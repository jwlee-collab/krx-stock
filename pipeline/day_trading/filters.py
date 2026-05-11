from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.models import IntradayBar


@dataclass(frozen=True)
class IntradayFilterResult:
    passed: bool
    reason_codes: list[str]
    raw_metrics: dict[str, float | int | bool | str | None]


def _sort_bars(bars: list[IntradayBar]) -> list[IntradayBar]:
    return sorted(bars, key=lambda b: b.timestamp)


def _vwap(bars: list[IntradayBar]) -> float | None:
    volume = sum(max(0.0, float(b.volume)) for b in bars)
    if volume <= 0.0:
        return None
    return sum(float(b.close) * max(0.0, float(b.volume)) for b in bars) / volume


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _valid_ohlc(bar: IntradayBar) -> bool:
    values = [bar.open, bar.high, bar.low, bar.close]
    if any(v is None for v in values):
        return False
    if min(float(v) for v in values) <= 0.0:
        return False
    if float(bar.high) < max(float(bar.open), float(bar.close)):
        return False
    if float(bar.low) > min(float(bar.open), float(bar.close)):
        return False
    return True


def _is_no_trade_bar(bar: IntradayBar) -> bool:
    amount = float(bar.amount) if bar.amount is not None else 0.0
    return _valid_ohlc(bar) and float(bar.volume or 0.0) == 0.0 and amount <= 0.0


def _positive_volume_bars(bars: list[IntradayBar]) -> list[IntradayBar]:
    return [bar for bar in bars if float(bar.volume or 0.0) > 0.0]


def _drop_no_trade_bars(bars: list[IntradayBar]) -> list[IntradayBar]:
    return [bar for bar in bars if not _is_no_trade_bar(bar)]


def _no_trade_count(bars: list[IntradayBar]) -> int:
    return sum(1 for bar in bars if _is_no_trade_bar(bar))


def _timeframe_minutes(timeframe: str) -> int | None:
    value = timeframe.lower().strip()
    if value.endswith("m") and value[:-1].isdigit():
        return int(value[:-1])
    return None


def _has_duplicate_timestamps(bars: list[IntradayBar]) -> bool:
    seen = set()
    for bar in bars:
        if bar.timestamp in seen:
            return True
        seen.add(bar.timestamp)
    return False


def _invalid_ohlcv_reasons(bars: list[IntradayBar], *, zero_volume_bar_policy: str, timeframe_label: str) -> list[str]:
    for bar in bars:
        invalid_code = f"INVALID_{timeframe_label}_BAR"
        if not _valid_ohlc(bar):
            if bar.volume is not None and float(bar.volume) == 0.0:
                return [invalid_code, "ZERO_VOLUME_INVALID_OHLC"]
            return [invalid_code]
        if bar.volume is None or float(bar.volume) < 0.0:
            return [invalid_code]
        if bar.amount is not None and float(bar.amount) < 0.0:
            return [invalid_code]
        if float(bar.volume) == 0.0 and bar.amount is not None and float(bar.amount) > 0.0:
            return [invalid_code, "ZERO_VOLUME_POSITIVE_AMOUNT"]
        if float(bar.volume) == 0.0 and zero_volume_bar_policy == "strict_invalid":
            return [invalid_code, "ZERO_VOLUME_VALID_OHLC"]
    return []


def _has_intraday_gap(bars: list[IntradayBar], timeframe: str) -> bool:
    minutes = _timeframe_minutes(timeframe)
    if minutes is None or len(bars) < 2:
        return False
    expected = timedelta(minutes=minutes)
    for prev, cur in zip(bars, bars[1:]):
        if prev.timestamp.date() != cur.timestamp.date():
            continue
        if cur.timestamp - prev.timestamp > expected:
            return True
    return False


class IntradayFilter:
    def __init__(self, config: DayTradingConfig):
        self.config = config

    def evaluate(
        self,
        symbol: str,
        bars_by_timeframe: dict[str, list[IntradayBar]],
        market_bars: list[IntradayBar] | None = None,
    ) -> IntradayFilterResult:
        cfg = self.config
        primary = _sort_bars(bars_by_timeframe.get(cfg.timeframe_primary, []))
        confirm = _sort_bars(bars_by_timeframe.get(cfg.timeframe_confirm, []))
        if cfg.zero_volume_bar_policy == "drop_no_trade":
            primary = _drop_no_trade_bars(primary)
            confirm = _drop_no_trade_bars(confirm)
        reason_codes: list[str] = []
        metrics: dict[str, float | int | bool | str | None] = {
            "symbol": symbol,
            "zero_volume_bar_policy": cfg.zero_volume_bar_policy,
            "vwap_positive_volume_only": cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"},
            "no_trade_used_for_price_context": cfg.zero_volume_bar_policy == "no_trade_context",
        }

        if not primary:
            return IntradayFilterResult(False, ["MISSING_5M_DATA"], metrics)
        if not confirm:
            return IntradayFilterResult(False, ["MISSING_15M_DATA"], metrics)
        if len(primary) < cfg.min_primary_bars:
            return IntradayFilterResult(False, ["INSUFFICIENT_5M_BARS"], metrics)
        if len(confirm) < cfg.min_confirm_bars:
            return IntradayFilterResult(False, ["INSUFFICIENT_15M_BARS"], metrics)
        if _has_duplicate_timestamps(primary):
            return IntradayFilterResult(False, ["DUPLICATE_5M_TIMESTAMP"], metrics)
        if _has_duplicate_timestamps(confirm):
            return IntradayFilterResult(False, ["DUPLICATE_15M_TIMESTAMP"], metrics)
        invalid_primary = _invalid_ohlcv_reasons(primary, zero_volume_bar_policy=cfg.zero_volume_bar_policy, timeframe_label="5M")
        if invalid_primary:
            return IntradayFilterResult(False, invalid_primary, metrics)
        invalid_confirm = _invalid_ohlcv_reasons(confirm, zero_volume_bar_policy=cfg.zero_volume_bar_policy, timeframe_label="15M")
        if invalid_confirm:
            return IntradayFilterResult(False, invalid_confirm, metrics)
        if cfg.fail_closed_on_missing_data and _has_intraday_gap(primary, cfg.timeframe_primary):
            return IntradayFilterResult(False, ["MISSING_5M_BAR_GAP"], metrics)
        if cfg.fail_closed_on_missing_data and _has_intraday_gap(confirm, cfg.timeframe_confirm):
            return IntradayFilterResult(False, ["MISSING_15M_BAR_GAP"], metrics)

        latest = primary[-1]
        confirm_latest = confirm[-1]
        metrics.update(
            {
                "latest_timestamp": latest.timestamp.isoformat(),
                "confirm_latest_timestamp": confirm_latest.timestamp.isoformat(),
                "primary_bar_count": len(primary),
                "confirm_bar_count": len(confirm),
                "positive_volume_bar_count": len(_positive_volume_bars(primary)),
                "positive_volume_confirm_bar_count": len(_positive_volume_bars(confirm)),
                "no_trade_bar_count": _no_trade_count(primary),
                "no_trade_confirm_bar_count": _no_trade_count(confirm),
            }
        )
        if cfg.zero_volume_bar_policy == "no_trade_context":
            if _is_no_trade_bar(latest):
                return IntradayFilterResult(
                    False,
                    ["NO_TRADE_5M_BAR", "ZERO_VOLUME_VALID_OHLC", "NO_TRADE_CONTEXT_USED", "NO_TRADE_BAR_BLOCKED_ENTRY"],
                    metrics,
                )
            if _is_no_trade_bar(confirm_latest):
                return IntradayFilterResult(
                    False,
                    ["NO_TRADE_15M_BAR", "ZERO_VOLUME_VALID_OHLC", "NO_TRADE_CONTEXT_USED", "NO_TRADE_BAR_BLOCKED_ENTRY"],
                    metrics,
                )
        if any(v <= 0.0 for v in [latest.open, latest.high, latest.low, latest.close]) or latest.volume <= 0.0:
            return IntradayFilterResult(False, ["INVALID_5M_BAR"], metrics)
        if any(v <= 0.0 for v in [confirm_latest.open, confirm_latest.high, confirm_latest.low, confirm_latest.close]) or confirm_latest.volume <= 0.0:
            return IntradayFilterResult(False, ["INVALID_15M_BAR"], metrics)

        primary_prev = primary[-(cfg.breakout_lookback_bars + 1) : -1]
        confirm_prev = confirm[-(cfg.confirm_breakout_lookback_bars + 1) : -1]
        if not primary_prev or not confirm_prev:
            return IntradayFilterResult(False, ["INSUFFICIENT_BREAKOUT_LOOKBACK"], metrics)

        primary_for_volume = _positive_volume_bars(primary) if cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"} else primary
        primary_prev_for_volume = _positive_volume_bars(primary_prev) if cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"} else primary_prev
        avg_trade_value = _avg([b.trade_value for b in primary_for_volume[-cfg.min_primary_bars :]])
        latest_trade_value = latest.trade_value
        prev_avg_volume = _avg([float(b.volume) for b in primary_prev_for_volume])
        volume_surge_ratio = latest.volume / prev_avg_volume if prev_avg_volume > 0.0 else 0.0
        day_vwap = _vwap(primary_for_volume if cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"} else primary)
        primary_breakout_level = max(float(b.high) for b in primary_prev)
        confirm_breakout_level = max(float(b.high) for b in confirm_prev)
        primary_breakout = latest.close > primary_breakout_level
        confirm_breakout = confirm_latest.close > confirm_breakout_level

        metrics.update(
            {
                "latest_timestamp": latest.timestamp.isoformat(),
                "confirm_latest_timestamp": confirm_latest.timestamp.isoformat(),
                "latest_price": float(latest.close),
                "latest_trade_value": float(latest_trade_value),
                "avg_trade_value": float(avg_trade_value),
                "volume_surge_ratio": float(volume_surge_ratio),
                "vwap": float(day_vwap) if day_vwap is not None else None,
                "primary_breakout_level": float(primary_breakout_level),
                "confirm_breakout_level": float(confirm_breakout_level),
                "primary_breakout": bool(primary_breakout),
                "confirm_breakout": bool(confirm_breakout),
                "liquidity_pass": bool(avg_trade_value >= cfg.min_avg_trade_value and latest_trade_value >= cfg.min_latest_trade_value),
                "breakout_pass": bool(primary_breakout and (confirm_breakout or not cfg.require_confirm_breakout)),
                "volume_expansion_pass": bool(volume_surge_ratio >= cfg.min_volume_surge_ratio),
            }
        )

        if avg_trade_value < cfg.min_avg_trade_value:
            reason_codes.append("LOW_AVG_TRADE_VALUE")
        if latest_trade_value < cfg.min_latest_trade_value:
            reason_codes.append("LOW_LATEST_TRADE_VALUE")
        if cfg.require_vwap_above and (day_vwap is None or latest.close <= day_vwap):
            reason_codes.append("VWAP_NOT_CONFIRMED")
        if not primary_breakout:
            reason_codes.append("PRIMARY_BREAKOUT_MISSING")
        if cfg.require_confirm_breakout and not confirm_breakout:
            reason_codes.append("CONFIRM_BREAKOUT_MISSING")
        if volume_surge_ratio < cfg.min_volume_surge_ratio:
            reason_codes.append("VOLUME_SURGE_MISSING")

        if market_bars:
            ordered_market = _sort_bars(market_bars)
            first_close = ordered_market[0].close if ordered_market else 0.0
            last_close = ordered_market[-1].close if ordered_market else 0.0
            market_return = ((last_close - first_close) / first_close) if first_close > 0.0 else None
            metrics["market_intraday_return"] = market_return
            if market_return is not None and market_return <= -cfg.market_drop_limit_pct:
                reason_codes.append("MARKET_STRONG_DOWNTREND")
        elif cfg.require_market_trend_data:
            reason_codes.append("MISSING_MARKET_TREND_DATA")

        return IntradayFilterResult(not reason_codes, reason_codes, metrics)
