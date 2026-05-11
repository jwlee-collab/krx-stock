from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from pipeline.day_trading.models import DayPosition, DaySignal, DayTradeResult


class DayPositionTracker:
    def __init__(self) -> None:
        self._open: dict[tuple[str, str], DayPosition] = {}
        self._entry_records: list[DayPosition] = []
        self.closed_trades: list[DayTradeResult] = []

    def open_position(
        self,
        signal: DaySignal,
        qty: float,
        entry_price: float,
        opened_at: datetime,
        entry_cost: float = 0.0,
        entry_reference_price: float = 0.0,
        entry_fee_krw: float | None = None,
        entry_slippage_cost_krw: float = 0.0,
    ) -> DayPosition:
        key = (signal.strategy_id, signal.symbol)
        if key in self._open:
            raise ValueError(f"open position already exists for strategy={signal.strategy_id} symbol={signal.symbol}")
        position = DayPosition(
            strategy_id=signal.strategy_id,
            symbol=signal.symbol,
            qty=float(qty),
            entry_price=float(entry_price),
            stop_loss_price=float(signal.stop_loss_price),
            take_profit_price=float(signal.take_profit_price),
            opened_at=opened_at,
            trade_date=opened_at.date().isoformat(),
            entry_cost=float(entry_cost),
            entry_reference_price=float(entry_reference_price or entry_price),
            entry_fee_krw=float(entry_fee_krw if entry_fee_krw is not None else entry_cost),
            entry_slippage_cost_krw=float(entry_slippage_cost_krw),
            signal_reason_codes=list(signal.signal_reason_codes),
            raw_metrics=dict(signal.raw_metrics),
        )
        self._open[key] = position
        self._entry_records.append(replace(position))
        return position

    def add_position(self, position: DayPosition) -> None:
        key = (position.strategy_id, position.symbol)
        self._open[key] = position
        self._entry_records.append(replace(position))

    def close_position(
        self,
        strategy_id: str,
        symbol: str,
        exit_price: float,
        closed_at: datetime,
        reason: str,
        exit_cost: float = 0.0,
        exit_reference_price: float | None = None,
        exit_fee_krw: float | None = None,
        exit_tax_krw: float = 0.0,
        exit_slippage_cost_krw: float = 0.0,
    ) -> DayTradeResult | None:
        key = (strategy_id, symbol)
        position = self._open.get(key)
        if position is None:
            return None
        exit_price = float(exit_price)
        qty = float(position.qty)
        reference_entry_price = float(position.entry_reference_price or position.entry_price)
        reference_exit_price = float(exit_reference_price if exit_reference_price is not None else exit_price)
        entry_fee = float(position.entry_fee_krw if position.entry_fee_krw else position.entry_cost)
        exit_tax = float(exit_tax_krw)
        exit_fee = float(exit_fee_krw if exit_fee_krw is not None else max(0.0, float(exit_cost) - exit_tax))
        entry_slippage = float(position.entry_slippage_cost_krw)
        exit_slippage = float(exit_slippage_cost_krw)
        fees = entry_fee + exit_fee
        tax = exit_tax
        slippage_cost = entry_slippage + exit_slippage
        total_costs = fees + tax + slippage_cost
        gross_pnl = (reference_exit_price - reference_entry_price) * qty
        net_pnl = gross_pnl - total_costs
        basis = reference_entry_price * qty
        gross_return_pct = gross_pnl / basis if basis > 0.0 else 0.0
        net_return_pct = net_pnl / basis if basis > 0.0 else 0.0
        result = DayTradeResult(
            strategy_id=strategy_id,
            symbol=symbol,
            qty=qty,
            entry_time=position.opened_at,
            exit_time=closed_at,
            entry_price=position.entry_price,
            exit_price=exit_price,
            gross_pnl=gross_pnl,
            net_pnl=net_pnl,
            gross_return_pct=gross_return_pct,
            net_return_pct=net_return_pct,
            costs=total_costs,
            exit_reason=reason,
            signal_reason_codes=list(position.signal_reason_codes),
            entry_notional_krw=position.entry_price * qty,
            exit_notional_krw=exit_price * qty,
            fees_krw=fees,
            tax_krw=tax,
            slippage_cost_krw=slippage_cost,
            entry_fee_krw=entry_fee,
            exit_fee_krw=exit_fee,
            exit_tax_krw=tax,
            entry_slippage_cost_krw=entry_slippage,
            exit_slippage_cost_krw=exit_slippage,
        )
        position.status = "CLOSED"
        self.closed_trades.append(result)
        del self._open[key]
        return result

    def close_positions_for_strategy(
        self,
        strategy_id: str,
        exit_price_by_symbol: dict[str, float],
        closed_at: datetime,
        reason: str,
    ) -> list[DayTradeResult]:
        results: list[DayTradeResult] = []
        symbols = [symbol for sid, symbol in self._open if sid == strategy_id]
        for symbol in symbols:
            if symbol not in exit_price_by_symbol:
                continue
            result = self.close_position(strategy_id, symbol, exit_price_by_symbol[symbol], closed_at, reason)
            if result is not None:
                results.append(result)
        return results

    def get_open_positions(self, strategy_id: str | None = None) -> list[DayPosition]:
        if strategy_id is None:
            return list(self._open.values())
        return [p for (sid, _), p in self._open.items() if sid == strategy_id]

    def get_open_position(self, strategy_id: str, symbol: str) -> DayPosition | None:
        return self._open.get((strategy_id, symbol))

    def has_open_position(self, strategy_id: str, symbol: str) -> bool:
        return (strategy_id, symbol) in self._open

    def count_open(self, strategy_id: str) -> int:
        return len(self.get_open_positions(strategy_id))

    def count_entries(self, trade_date: str, strategy_id: str) -> int:
        return sum(1 for p in self._entry_records if p.strategy_id == strategy_id and p.trade_date == trade_date)

    def count_entries_for_symbol(self, trade_date: str, strategy_id: str, symbol: str) -> int:
        return sum(
            1
            for p in self._entry_records
            if p.strategy_id == strategy_id and p.trade_date == trade_date and p.symbol == symbol
        )

    def open_notional(self, strategy_id: str) -> float:
        return sum(p.notional for p in self.get_open_positions(strategy_id))

    def realized_pnl_for_date(self, trade_date: str, strategy_id: str) -> float:
        return sum(
            t.net_pnl
            for t in self.closed_trades
            if t.strategy_id == strategy_id and t.exit_time.date().isoformat() == trade_date
        )

    def consecutive_losses(self, strategy_id: str) -> int:
        count = 0
        for trade in reversed(self.closed_trades):
            if trade.strategy_id != strategy_id:
                continue
            if trade.net_pnl < 0.0:
                count += 1
            else:
                break
        return count

    def had_loss_for_symbol_on_date(self, trade_date: str, strategy_id: str, symbol: str) -> bool:
        return any(
            t.strategy_id == strategy_id
            and t.symbol == symbol
            and t.exit_time.date().isoformat() == trade_date
            and t.net_pnl < 0.0
            for t in self.closed_trades
        )
