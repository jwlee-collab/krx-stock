from __future__ import annotations

from datetime import datetime

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.models import DayEntryDecision, DaySignal
from pipeline.day_trading.positions import DayPositionTracker


class DayRiskManager:
    def __init__(self, config: DayTradingConfig):
        self.config = config

    def validate_entry(
        self,
        signal: DaySignal,
        tracker: DayPositionTracker,
        now: datetime,
        equity: float | None = None,
        day_start_equity: float | None = None,
        pending_order_symbols: set[str] | None = None,
    ) -> DayEntryDecision:
        cfg = self.config
        cfg.validate()
        pending_order_symbols = pending_order_symbols or set()
        equity_value = float(equity if equity is not None else cfg.initial_equity)
        day_start = float(day_start_equity if day_start_equity is not None else equity_value)
        trade_date = now.date().isoformat()

        if signal.strategy_id != cfg.strategy_id:
            return DayEntryDecision(False, "WRONG_STRATEGY_ID")
        if signal.symbol in pending_order_symbols:
            return DayEntryDecision(False, "DUPLICATE_PENDING_ORDER")
        if tracker.has_open_position(cfg.strategy_id, signal.symbol):
            return DayEntryDecision(False, "DUPLICATE_DAY_POSITION")
        if tracker.count_open(cfg.strategy_id) >= cfg.max_open_positions:
            return DayEntryDecision(False, "MAX_OPEN_POSITIONS")
        if tracker.count_entries(trade_date, cfg.strategy_id) >= cfg.max_trades_per_day:
            return DayEntryDecision(False, "MAX_TRADES_PER_DAY")
        if tracker.count_entries_for_symbol(trade_date, cfg.strategy_id, signal.symbol) >= cfg.max_trades_per_symbol_per_day:
            return DayEntryDecision(False, "MAX_TRADES_PER_SYMBOL_PER_DAY")

        daily_pnl = tracker.realized_pnl_for_date(trade_date, cfg.strategy_id)
        if day_start > 0.0 and daily_pnl / day_start <= -cfg.daily_loss_limit_pct:
            return DayEntryDecision(False, "DAILY_LOSS_LIMIT")
        if tracker.consecutive_losses(cfg.strategy_id) >= cfg.consecutive_loss_limit:
            return DayEntryDecision(False, "CONSECUTIVE_LOSS_LIMIT")
        if cfg.block_reentry_after_loss and tracker.had_loss_for_symbol_on_date(trade_date, cfg.strategy_id, signal.symbol):
            return DayEntryDecision(False, "LOSS_REENTRY_BLOCKED")

        notional = float(cfg.notional_per_trade)
        projected_total = tracker.open_notional(cfg.strategy_id) + notional
        if equity_value > 0.0 and notional / equity_value > cfg.max_symbol_exposure_pct + 1e-12:
            return DayEntryDecision(False, "MAX_SYMBOL_EXPOSURE")
        if equity_value > 0.0 and projected_total / equity_value > cfg.max_total_exposure_pct + 1e-12:
            return DayEntryDecision(False, "MAX_TOTAL_EXPOSURE")

        return DayEntryDecision(
            True,
            "APPROVED",
            notional=notional,
            raw_metrics={
                "trade_date": trade_date,
                "open_positions": tracker.count_open(cfg.strategy_id),
                "entries_today": tracker.count_entries(trade_date, cfg.strategy_id),
                "projected_total_exposure_pct": projected_total / equity_value if equity_value > 0.0 else None,
            },
        )
