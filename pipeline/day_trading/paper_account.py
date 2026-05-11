from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.models import DayPosition, DayTradeResult


@dataclass(frozen=True)
class PaperEntryCheck:
    approved: bool
    reason_code: str
    raw_metrics: dict[str, Any] = field(default_factory=dict)


class PaperAccount:
    """Small quote-only replay ledger; it never talks to a broker or account API."""

    def __init__(self, config: DayTradingConfig):
        self.config = config
        self.initial_cash_krw = float(config.paper_initial_cash_krw)
        self.cash_krw = float(config.paper_initial_cash_krw)
        self.reserved_cash_krw = 0.0
        self.open_position_value_krw = 0.0
        self.total_exposure_krw = 0.0
        self.realized_pnl_krw = 0.0
        self.unrealized_pnl_krw = 0.0
        self.fees_krw = 0.0
        self.tax_krw = 0.0
        self.slippage_cost_krw = 0.0
        self.max_intraday_exposure_krw = 0.0
        self.max_drawdown_krw = 0.0
        self.rejected_by_cash_count = 0
        self.rejected_by_exposure_count = 0
        self.rejected_by_daily_loss_count = 0
        self._open_entry_value_by_key: dict[tuple[str, str], float] = {}
        self._peak_equity_krw = self.initial_cash_krw
        self._last_mark_price_by_symbol: dict[str, float] = {}

    @property
    def ending_equity_krw(self) -> float:
        return self.cash_krw + self.open_position_value_krw

    @property
    def daily_return_pct(self) -> float:
        return (self.ending_equity_krw - self.initial_cash_krw) / self.initial_cash_krw if self.initial_cash_krw > 0.0 else 0.0

    def validate_entry(
        self,
        *,
        symbol: str,
        strategy_id: str,
        qty: int,
        entry_price: float,
        entry_fee_krw: float,
        now: datetime,
    ) -> PaperEntryCheck:
        position_value = float(qty) * float(entry_price)
        cash_required = position_value + float(entry_fee_krw)
        projected_exposure = self.total_exposure_krw + position_value
        raw = {
            "symbol": symbol,
            "strategy_id": strategy_id,
            "qty": qty,
            "entry_price": entry_price,
            "position_value_krw": position_value,
            "cash_required_krw": cash_required,
            "cash_krw": self.cash_krw,
            "projected_total_exposure_krw": projected_exposure,
            "paper_max_total_exposure_krw": self.config.paper_max_total_exposure_krw,
            "checked_at": now.isoformat(),
        }
        if qty <= 0:
            self.rejected_by_cash_count += 1
            return PaperEntryCheck(False, "PAPER_ZERO_QUANTITY", raw)
        if self.config.paper_reject_if_cash_insufficient and cash_required > self.cash_krw + 1e-9:
            self.rejected_by_cash_count += 1
            return PaperEntryCheck(False, "PAPER_CASH_INSUFFICIENT", raw)
        if self.config.paper_reject_if_exposure_exceeded and projected_exposure > float(self.config.paper_max_total_exposure_krw) + 1e-9:
            self.rejected_by_exposure_count += 1
            return PaperEntryCheck(False, "PAPER_EXPOSURE_EXCEEDED", raw)
        if self.realized_pnl_krw <= -float(self.config.paper_daily_loss_limit_krw):
            self.rejected_by_daily_loss_count += 1
            return PaperEntryCheck(False, "PAPER_DAILY_LOSS_LIMIT_KRW", raw)
        if self.daily_return_pct <= -float(self.config.paper_daily_loss_limit_pct):
            self.rejected_by_daily_loss_count += 1
            return PaperEntryCheck(False, "PAPER_DAILY_LOSS_LIMIT_PCT", raw)
        return PaperEntryCheck(True, "APPROVED", raw)

    def record_entry(self, position: DayPosition) -> None:
        key = (position.strategy_id, position.symbol)
        entry_value = float(position.qty) * float(position.entry_price)
        self._open_entry_value_by_key[key] = entry_value
        self.cash_krw -= entry_value + float(position.entry_fee_krw)
        self.fees_krw += float(position.entry_fee_krw)
        self.slippage_cost_krw += float(position.entry_slippage_cost_krw)
        self.total_exposure_krw += entry_value
        self.open_position_value_krw += entry_value
        self.max_intraday_exposure_krw = max(self.max_intraday_exposure_krw, self.total_exposure_krw)
        self._update_drawdown()

    def record_exit(self, trade: DayTradeResult) -> None:
        key = (trade.strategy_id, trade.symbol)
        entry_value = self._open_entry_value_by_key.pop(key, float(trade.entry_notional_krw))
        exit_value = float(trade.exit_notional_krw)
        self.cash_krw += exit_value - float(trade.exit_fee_krw) - float(trade.exit_tax_krw)
        self.realized_pnl_krw += float(trade.net_pnl)
        self.fees_krw += float(trade.exit_fee_krw)
        self.tax_krw += float(trade.exit_tax_krw)
        self.slippage_cost_krw += float(trade.exit_slippage_cost_krw)
        self.total_exposure_krw = max(0.0, self.total_exposure_krw - entry_value)
        self.open_position_value_krw = max(0.0, self.open_position_value_krw - entry_value)
        self._update_drawdown()

    def mark_to_market(self, positions: list[DayPosition], latest_price_by_symbol: dict[str, float]) -> None:
        total_value = 0.0
        unrealized = 0.0
        for position in positions:
            price = float(latest_price_by_symbol.get(position.symbol, position.entry_price))
            self._last_mark_price_by_symbol[position.symbol] = price
            current_value = float(position.qty) * price
            entry_value = float(position.qty) * float(position.entry_price)
            total_value += current_value
            unrealized += current_value - entry_value
        self.open_position_value_krw = total_value
        self.total_exposure_krw = total_value
        self.unrealized_pnl_krw = unrealized
        self.max_intraday_exposure_krw = max(self.max_intraday_exposure_krw, total_value)
        self._update_drawdown()

    def _update_drawdown(self) -> None:
        equity = self.ending_equity_krw
        self._peak_equity_krw = max(self._peak_equity_krw, equity)
        self.max_drawdown_krw = min(self.max_drawdown_krw, equity - self._peak_equity_krw)

    def summary(self, trades: list[DayTradeResult] | None = None) -> dict[str, Any]:
        trades = trades or []
        winning = [trade for trade in trades if float(trade.net_pnl) > 0.0]
        losing = [trade for trade in trades if float(trade.net_pnl) < 0.0]
        total_cost = self.fees_krw + self.tax_krw + self.slippage_cost_krw
        return {
            "initial_cash_krw": self.initial_cash_krw,
            "cash_krw": self.cash_krw,
            "ending_cash_krw": self.cash_krw,
            "reserved_cash_krw": self.reserved_cash_krw,
            "open_position_value_krw": self.open_position_value_krw,
            "total_exposure_krw": self.total_exposure_krw,
            "realized_pnl_krw": self.realized_pnl_krw,
            "unrealized_pnl_krw": self.unrealized_pnl_krw,
            "fees_krw": self.fees_krw,
            "tax_krw": self.tax_krw,
            "slippage_cost_krw": self.slippage_cost_krw,
            "total_cost_krw": total_cost,
            "ending_equity_krw": self.ending_equity_krw,
            "daily_return_pct": self.daily_return_pct,
            "max_intraday_exposure_krw": self.max_intraday_exposure_krw,
            "max_exposure_krw": self.max_intraday_exposure_krw,
            "exposure_limit_krw": float(self.config.paper_max_total_exposure_krw),
            "max_drawdown_krw": self.max_drawdown_krw,
            "total_trades": len(trades),
            "winning_trades": len(winning),
            "losing_trades": len(losing),
            "rejected_by_cash_count": self.rejected_by_cash_count,
            "rejected_by_exposure_count": self.rejected_by_exposure_count,
            "rejected_by_daily_loss_count": self.rejected_by_daily_loss_count,
            "cash_rejection_count": self.rejected_by_cash_count,
            "exposure_rejection_count": self.rejected_by_exposure_count,
            "daily_loss_rejection_count": self.rejected_by_daily_loss_count,
        }
