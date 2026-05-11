from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from typing import Any

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.models import IntradayBar, IntradayContext


FINAL_DATA_TYPES = {"FINAL", "EOD", "CLOSE", "CONFIRMED_CLOSE"}


def _sort_bars(bars: list[IntradayBar]) -> list[IntradayBar]:
    return sorted(bars, key=lambda b: b.timestamp)


def _clamp(value: float, low: float = -1.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _vwap(bars: list[IntradayBar]) -> float | None:
    volume = sum(max(0.0, float(b.volume)) for b in bars)
    if volume <= 0.0:
        return None
    return sum(float(b.close) * max(0.0, float(b.volume)) for b in bars) / volume


def _positive_volume_bars(bars: list[IntradayBar]) -> list[IntradayBar]:
    return [bar for bar in bars if float(bar.volume or 0.0) > 0.0]


class IntradayMarketContext:
    """Builds point-in-time intraday context without looking past `timestamp`."""

    def __init__(self, config: DayTradingConfig, conn: sqlite3.Connection | None = None):
        self.config = config
        self.conn = conn

    def build(
        self,
        symbol: str,
        timestamp: datetime,
        bars_by_timeframe: dict[str, list[IntradayBar]],
        market_bars: list[IntradayBar] | None = None,
        sector_bars: list[IntradayBar] | None = None,
    ) -> IntradayContext:
        cfg = self.config
        primary = [b for b in _sort_bars(bars_by_timeframe.get(cfg.timeframe_primary, [])) if b.timestamp <= timestamp]
        confirm = [b for b in _sort_bars(bars_by_timeframe.get(cfg.timeframe_confirm, [])) if b.timestamp <= timestamp]
        risk_flags: list[str] = []
        stale_flags: list[str] = []
        missing_flags: list[str] = []
        reason_codes: list[str] = []
        raw: dict[str, Any] = {}
        if not primary:
            missing_flags.append("MISSING_PRIMARY_BARS")
            risk_flags.append("MISSING_PRIMARY_BARS")
            return self._empty_context(symbol, timestamp, risk_flags, stale_flags, missing_flags, reason_codes, raw)

        latest = primary[-1]
        primary_for_volume = _positive_volume_bars(primary) if cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"} else primary
        prev_for_volume = _positive_volume_bars(primary[-(cfg.breakout_lookback_bars + 1) : -1]) if cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"} else primary[-(cfg.breakout_lookback_bars + 1) : -1]
        vwap = _vwap(primary_for_volume)
        vwap_distance = ((latest.close - vwap) / vwap) if vwap and vwap > 0.0 else None
        prev = primary[-(cfg.breakout_lookback_bars + 1) : -1]
        prev_avg_volume = _avg([float(b.volume) for b in prev_for_volume])
        relative_volume = latest.volume / prev_avg_volume if prev_avg_volume > 0.0 else 0.0
        avg_trade_value = _avg([b.trade_value for b in primary_for_volume[-cfg.min_primary_bars :]])
        traded_value_score = _clamp(avg_trade_value / cfg.min_avg_trade_value - 1.0, -1.0, 1.0) if cfg.min_avg_trade_value else 0.0
        breakout_level = max((float(b.high) for b in prev), default=float(latest.close))
        breakout_score = _clamp((latest.close - breakout_level) / breakout_level * 20.0, -1.0, 1.0) if breakout_level > 0.0 else 0.0

        market_score = self._score_market_bars(market_bars, timestamp, missing_flags, risk_flags, reason_codes)
        sector_score = self._score_sector_bars(sector_bars, timestamp, missing_flags)
        trade_strength_score = self._score_trade_strength(symbol, timestamp, stale_flags, missing_flags, reason_codes)
        foreign_score, institution_score = self._score_investor_flows(symbol, timestamp, stale_flags, missing_flags, reason_codes)
        program_score = self._score_program_flows(symbol, timestamp, stale_flags, missing_flags, reason_codes)

        if cfg.require_vwap_above and (vwap is None or latest.close <= vwap):
            risk_flags.append("VWAP_NOT_CONFIRMED")
        if market_score < cfg.min_market_context_score:
            risk_flags.append("MARKET_CONTEXT_WEAK")
        if foreign_score < -0.20 and institution_score < -0.20:
            reason_codes.append("FOREIGN_INSTITUTION_NET_SELLING")

        total = (
            0.25 * (1.0 if vwap_distance is not None and vwap_distance > 0.0 else -0.5)
            + 0.20 * _clamp(relative_volume - 1.0, -1.0, 1.0)
            + 0.15 * traded_value_score
            + 0.15 * breakout_score
            + 0.15 * market_score
            + 0.05 * sector_score
            + 0.05 * trade_strength_score
            + 0.05 * foreign_score
            + 0.05 * institution_score
            + 0.05 * program_score
        )
        total = _clamp(total, -1.0, 1.0)
        if total < cfg.min_total_intraday_score:
            reason_codes.append("INTRADAY_CONTEXT_SCORE_LOW")
            risk_flags.append("INTRADAY_CONTEXT_SCORE_LOW")

        raw.update(
            {
                "primary_bar_count": len(primary),
                "confirm_bar_count": len(confirm),
                "market_proxy_symbol": cfg.market_proxy_symbol,
                "latest_primary_timestamp": latest.timestamp.isoformat(),
                "point_in_time_timestamp": timestamp.isoformat(),
            }
        )
        return IntradayContext(
            symbol=symbol,
            timestamp=timestamp,
            price=float(latest.close),
            vwap=vwap,
            vwap_distance_pct=vwap_distance,
            relative_volume=float(relative_volume),
            traded_value=float(latest.trade_value),
            traded_value_score=float(traded_value_score),
            breakout_score=float(breakout_score),
            market_context_score=float(market_score),
            sector_context_score=float(sector_score),
            trade_strength_score=float(trade_strength_score),
            foreign_flow_score=float(foreign_score),
            institution_flow_score=float(institution_score),
            program_flow_score=float(program_score),
            total_intraday_score=float(total),
            risk_flags=risk_flags,
            stale_data_flags=stale_flags,
            missing_data_flags=missing_flags,
            reason_codes=reason_codes,
            raw_metrics=raw,
        )

    def _empty_context(
        self,
        symbol: str,
        timestamp: datetime,
        risk_flags: list[str],
        stale_flags: list[str],
        missing_flags: list[str],
        reason_codes: list[str],
        raw: dict[str, Any],
    ) -> IntradayContext:
        return IntradayContext(
            symbol=symbol,
            timestamp=timestamp,
            price=0.0,
            vwap=None,
            vwap_distance_pct=None,
            relative_volume=0.0,
            traded_value=0.0,
            traded_value_score=0.0,
            breakout_score=0.0,
            market_context_score=-1.0,
            sector_context_score=0.0,
            trade_strength_score=0.0,
            foreign_flow_score=0.0,
            institution_flow_score=0.0,
            program_flow_score=0.0,
            total_intraday_score=-1.0,
            risk_flags=risk_flags,
            stale_data_flags=stale_flags,
            missing_data_flags=missing_flags,
            reason_codes=reason_codes,
            raw_metrics=raw,
        )

    def _score_market_bars(
        self,
        market_bars: list[IntradayBar] | None,
        timestamp: datetime,
        missing_flags: list[str],
        risk_flags: list[str],
        reason_codes: list[str],
    ) -> float:
        cfg = self.config
        bars = [b for b in _sort_bars(market_bars or []) if b.timestamp <= timestamp]
        if not bars:
            missing_flags.append("MISSING_MARKET_PROXY_DATA")
            if cfg.require_market_trend_data:
                risk_flags.append("MISSING_MARKET_PROXY_DATA")
                reason_codes.append("REJECT_MISSING_MARKET_PROXY_DATA")
            return -1.0 if cfg.require_market_trend_data else 0.0
        first = bars[0].close
        last = bars[-1].close
        ret = (last - first) / first if first > 0.0 else 0.0
        if ret <= -cfg.market_drop_limit_pct:
            reason_codes.append("MARKET_STRONG_DOWNTREND")
            return -1.0
        return _clamp(ret / max(cfg.market_drop_limit_pct, 0.001), -1.0, 1.0)

    def _score_sector_bars(self, sector_bars: list[IntradayBar] | None, timestamp: datetime, missing_flags: list[str]) -> float:
        bars = [b for b in _sort_bars(sector_bars or []) if b.timestamp <= timestamp]
        if not bars:
            missing_flags.append("MISSING_SECTOR_PROXY_DATA")
            return 0.0
        first = bars[0].close
        return _clamp(((bars[-1].close - first) / first) * 100.0, -1.0, 1.0) if first > 0.0 else 0.0

    def _latest_optional_row(self, table: str, symbol: str, timestamp: datetime) -> sqlite3.Row | None:
        if self.conn is None:
            return None
        return self.conn.execute(
            f"""
            SELECT *
            FROM {table}
            WHERE symbol=?
              AND timestamp<=?
              AND UPPER(data_type) NOT IN ({','.join('?' for _ in FINAL_DATA_TYPES)})
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol, timestamp.isoformat(), *sorted(FINAL_DATA_TYPES)),
        ).fetchone()

    def _apply_optional_policy(
        self,
        policy: str,
        flag: str,
        row: sqlite3.Row | None,
        timestamp: datetime,
        stale_flags: list[str],
        missing_flags: list[str],
        reason_codes: list[str],
    ) -> bool:
        if row is None:
            missing_flags.append(flag)
            if policy in {"reject", "fail_closed"}:
                reason_codes.append(f"REJECT_{flag}")
            return False
        row_ts = datetime.fromisoformat(str(row["timestamp"]))
        if timestamp - row_ts > timedelta(minutes=self.config.max_optional_data_age_minutes):
            stale_flags.append(flag.replace("MISSING", "STALE"))
            if policy in {"reject", "fail_closed"}:
                reason_codes.append(f"REJECT_STALE_{flag}")
                return False
        return True

    def _score_trade_strength(
        self,
        symbol: str,
        timestamp: datetime,
        stale_flags: list[str],
        missing_flags: list[str],
        reason_codes: list[str],
    ) -> float:
        row = self._latest_optional_row("intraday_trade_strength", symbol, timestamp)
        if not self._apply_optional_policy(self.config.trade_strength_data_policy, "MISSING_TRADE_STRENGTH", row, timestamp, stale_flags, missing_flags, reason_codes):
            return 0.0
        score = row["strength_score"] if row and row["strength_score"] is not None else None
        if score is not None:
            return _clamp(float(score))
        buy = float(row["buy_strength"] or 0.0)
        sell = float(row["sell_strength"] or 0.0)
        return _clamp((buy - sell) / max(abs(buy) + abs(sell), 1.0))

    def _score_investor_flows(
        self,
        symbol: str,
        timestamp: datetime,
        stale_flags: list[str],
        missing_flags: list[str],
        reason_codes: list[str],
    ) -> tuple[float, float]:
        row = self._latest_optional_row("intraday_investor_flows", symbol, timestamp)
        if not self._apply_optional_policy(self.config.investor_flow_data_policy, "MISSING_INVESTOR_FLOW", row, timestamp, stale_flags, missing_flags, reason_codes):
            return 0.0, 0.0
        foreign = float(row["foreign_net_buy_amount"] or 0.0)
        institution = float(row["institution_net_buy_amount"] or 0.0)
        scale = max(abs(foreign) + abs(institution), 1_000_000.0)
        return _clamp(foreign / scale), _clamp(institution / scale)

    def _score_program_flows(
        self,
        symbol: str,
        timestamp: datetime,
        stale_flags: list[str],
        missing_flags: list[str],
        reason_codes: list[str],
    ) -> float:
        row = self._latest_optional_row("intraday_program_flows", symbol, timestamp)
        if not self._apply_optional_policy(self.config.program_flow_data_policy, "MISSING_PROGRAM_FLOW", row, timestamp, stale_flags, missing_flags, reason_codes):
            return 0.0
        amount = float(row["program_net_buy_amount"] or 0.0)
        return _clamp(amount / max(abs(amount), 1_000_000.0))
