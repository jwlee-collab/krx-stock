from __future__ import annotations

from datetime import datetime, time

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.filters import _drop_no_trade_bars, _is_no_trade_bar, _positive_volume_bars, _sort_bars, _vwap
from pipeline.day_trading.models import DayExitSignal, DayPosition, IntradayBar


STOP_LOSS = "STOP_LOSS"
TAKE_PROFIT = "TAKE_PROFIT"
TRAILING_STOP = "TRAILING_STOP"
VWAP_BREAKDOWN = "VWAP_BREAKDOWN"
END_OF_DAY = "END_OF_DAY"
RISK_LIMIT = "RISK_LIMIT"
SIGNAL_INVALIDATED = "SIGNAL_INVALIDATED"
MANUAL_OR_UNKNOWN = "MANUAL_OR_UNKNOWN"


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(hour=int(hour), minute=int(minute))


class DayExitManager:
    def __init__(self, config: DayTradingConfig):
        self.config = config

    def evaluate(
        self,
        position: DayPosition,
        bars_by_timeframe: dict[str, list[IntradayBar]],
        now: datetime | None = None,
    ) -> DayExitSignal | None:
        cfg = self.config
        if position.strategy_id != cfg.strategy_id:
            return None
        primary = _sort_bars(bars_by_timeframe.get(cfg.timeframe_primary, []))
        if cfg.zero_volume_bar_policy == "drop_no_trade":
            primary = _drop_no_trade_bars(primary)
        if not primary:
            return None
        latest = primary[-1]
        ts = now or latest.timestamp
        if cfg.zero_volume_bar_policy == "no_trade_context" and _is_no_trade_bar(latest):
            return None
        position.highest_price = max(position.highest_price, float(latest.high), float(latest.close))
        metrics = {
            "latest_close": float(latest.close),
            "latest_high": float(latest.high),
            "latest_low": float(latest.low),
            "stop_loss_price": float(position.stop_loss_price),
            "take_profit_price": float(position.take_profit_price),
            "highest_price": float(position.highest_price),
        }

        if latest.low <= position.stop_loss_price:
            return DayExitSignal(cfg.strategy_id, position.symbol, ts, position.stop_loss_price, STOP_LOSS, metrics)
        if latest.high >= position.take_profit_price:
            return DayExitSignal(cfg.strategy_id, position.symbol, ts, position.take_profit_price, TAKE_PROFIT, metrics)
        if cfg.trailing_stop_enabled:
            trailing_price = position.highest_price * (1.0 - cfg.trailing_stop_pct)
            metrics["trailing_stop_price"] = trailing_price
            if latest.close <= trailing_price:
                return DayExitSignal(cfg.strategy_id, position.symbol, ts, latest.close, TRAILING_STOP, metrics)

        day_vwap = _vwap(_positive_volume_bars(primary)) if cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"} else _vwap(primary)
        metrics["vwap"] = day_vwap
        if cfg.require_vwap_above and day_vwap is not None and latest.close < day_vwap:
            return DayExitSignal(cfg.strategy_id, position.symbol, ts, latest.close, VWAP_BREAKDOWN, metrics)

        if cfg.no_overnight and ts.time() >= _parse_hhmm(cfg.force_exit_time):
            return DayExitSignal(cfg.strategy_id, position.symbol, ts, latest.close, END_OF_DAY, metrics)
        if cfg.no_overnight and ts.date().isoformat() > position.trade_date:
            return DayExitSignal(cfg.strategy_id, position.symbol, ts, latest.close, END_OF_DAY, metrics)
        return None
