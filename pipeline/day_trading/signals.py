from __future__ import annotations

from datetime import datetime, timezone

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.context import IntradayMarketContext
from pipeline.day_trading.filters import IntradayFilter, _drop_no_trade_bars
from pipeline.day_trading.models import DaySignal, IntradayBar, SignalEvaluation


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


class DaySignalGenerator:
    def __init__(
        self,
        config: DayTradingConfig,
        intraday_filter: IntradayFilter | None = None,
        context_builder: IntradayMarketContext | None = None,
    ):
        self.config = config
        self.intraday_filter = intraday_filter or IntradayFilter(config)
        self.context_builder = context_builder or IntradayMarketContext(config)

    def generate(
        self,
        symbol: str,
        bars_by_timeframe: dict[str, list[IntradayBar]],
        source_universe: str,
        market_bars: list[IntradayBar] | None = None,
        created_at: datetime | None = None,
    ) -> SignalEvaluation:
        cfg = self.config
        created = created_at or datetime.now(timezone.utc)
        result = self.intraday_filter.evaluate(symbol, bars_by_timeframe, market_bars)
        if not result.passed:
            return SignalEvaluation(
                signal=None,
                rejected=True,
                reason_codes=result.reason_codes,
                raw_metrics=result.raw_metrics,
            )

        signal_bars_by_timeframe = dict(bars_by_timeframe)
        if cfg.zero_volume_bar_policy == "drop_no_trade":
            signal_bars_by_timeframe = {
                timeframe: _drop_no_trade_bars(list(bars))
                for timeframe, bars in bars_by_timeframe.items()
            }
        primary = sorted(signal_bars_by_timeframe[cfg.timeframe_primary], key=lambda b: b.timestamp)
        latest = primary[-1]
        context = self.context_builder.build(
            symbol=symbol,
            timestamp=latest.timestamp,
            bars_by_timeframe=signal_bars_by_timeframe,
            market_bars=market_bars,
        )
        if context.risk_flags or any(code.startswith("REJECT_") for code in context.reason_codes):
            return SignalEvaluation(
                signal=None,
                rejected=True,
                reason_codes=[*context.risk_flags, *context.reason_codes],
                raw_metrics={**result.raw_metrics, "intraday_context": context.to_dict()},
            )
        expected_entry = float(latest.close)
        volume_ratio = float(result.raw_metrics.get("volume_surge_ratio") or 0.0)
        breakout_level = float(result.raw_metrics.get("primary_breakout_level") or expected_entry)
        breakout_gap = (expected_entry - breakout_level) / breakout_level if breakout_level > 0.0 else 0.0
        confidence = _clamp(
            0.45
            + min(volume_ratio, 3.0) * 0.06
            + min(max(breakout_gap, 0.0), 0.03) * 3.0
            + max(context.total_intraday_score, -0.5) * 0.18,
            0.0,
            0.95,
        )
        raw_metrics = {
            **result.raw_metrics,
            "intraday_context": context.to_dict(),
        }
        signal = DaySignal(
            strategy_id=cfg.strategy_id,
            symbol=symbol,
            side="BUY",
            timestamp=latest.timestamp,
            expected_entry_price=expected_entry,
            stop_loss_price=expected_entry * (1.0 - cfg.stop_loss_pct),
            take_profit_price=expected_entry * (1.0 + cfg.take_profit_pct),
            confidence=confidence,
            signal_reason_codes=[
                "SWING_CANDIDATE",
                "VWAP_CONFIRMED",
                "PRIMARY_BREAKOUT",
                "CONFIRM_BREAKOUT",
                "VOLUME_SURGE",
                "INTRADAY_CONTEXT_CONFIRMED",
            ],
            raw_metrics=raw_metrics,
            mode=cfg.normalized_mode,
            source_universe=source_universe,
            created_at=created,
        )
        return SignalEvaluation(signal=signal, rejected=False, reason_codes=[], raw_metrics=result.raw_metrics)
