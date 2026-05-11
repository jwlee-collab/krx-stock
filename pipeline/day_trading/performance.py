from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass

from pipeline.day_trading.models import DayTradeLogEvent, DayTradeResult


@dataclass(frozen=True)
class CostModel:
    commission_pct: float = 0.00015
    transaction_tax_pct: float = 0.00180
    slippage_pct: float = 0.00050

    def entry_fill_price(self, reference_price: float) -> float:
        return float(reference_price) * (1.0 + self.slippage_pct)

    def exit_fill_price(self, reference_price: float) -> float:
        return float(reference_price) * (1.0 - self.slippage_pct)

    def entry_cost(self, qty: float, entry_price: float) -> float:
        return self.entry_commission(qty, entry_price)

    def exit_cost(self, qty: float, exit_price: float) -> float:
        return self.exit_commission(qty, exit_price) + self.exit_tax(qty, exit_price)

    def entry_commission(self, qty: float, entry_price: float) -> float:
        return abs(float(qty) * float(entry_price)) * self.commission_pct

    def exit_commission(self, qty: float, exit_price: float) -> float:
        return abs(float(qty) * float(exit_price)) * self.commission_pct

    def exit_tax(self, qty: float, exit_price: float) -> float:
        return abs(float(qty) * float(exit_price)) * self.transaction_tax_pct

    def entry_slippage_cost(self, qty: float, reference_price: float, fill_price: float) -> float:
        return max(0.0, (float(fill_price) - float(reference_price)) * abs(float(qty)))

    def exit_slippage_cost(self, qty: float, reference_price: float, fill_price: float) -> float:
        return max(0.0, (float(reference_price) - float(fill_price)) * abs(float(qty)))

    def round_trip_cost_pct(self) -> float:
        return (2.0 * self.commission_pct) + self.transaction_tax_pct + (2.0 * self.slippage_pct)


def _max_drawdown(values: list[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    max_dd = 0.0
    for value in values:
        peak = max(peak, value)
        if peak > 0.0:
            max_dd = min(max_dd, (value - peak) / peak)
    return max_dd


class DayPerformanceAnalyzer:
    def __init__(self, initial_equity: float = 10_000_000.0, cost_model: CostModel | None = None):
        self.initial_equity = float(initial_equity)
        self.cost_model = cost_model or CostModel()

    def analyze(
        self,
        trades: list[DayTradeResult],
        log_events: list[DayTradeLogEvent] | None = None,
        slippage_scenarios: list[float] | None = None,
    ) -> dict:
        log_events = log_events or []
        slippage_scenarios = slippage_scenarios or [0.0, self.cost_model.slippage_pct, 0.0010, 0.0020]
        wins = [t.net_return_pct for t in trades if t.net_pnl > 0.0]
        losses = [t.net_return_pct for t in trades if t.net_pnl < 0.0]
        gross_returns = [t.gross_return_pct for t in trades]
        net_returns = [t.net_return_pct for t in trades]
        gross_profit = sum(t.net_pnl for t in trades if t.net_pnl > 0.0)
        gross_loss = abs(sum(t.net_pnl for t in trades if t.net_pnl < 0.0))
        equity = self.initial_equity
        curve = [equity]
        max_consecutive_losses = 0
        current_losses = 0
        by_hour: dict[str, list[float]] = defaultdict(list)
        by_symbol: dict[str, list[float]] = defaultdict(list)
        by_reason: dict[str, list[float]] = defaultdict(list)
        holding_minutes: list[float] = []
        for trade in trades:
            equity += trade.net_pnl
            curve.append(equity)
            if trade.net_pnl < 0.0:
                current_losses += 1
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
            else:
                current_losses = 0
            by_hour[f"{trade.entry_time.hour:02d}"].append(trade.net_return_pct)
            by_symbol[trade.symbol].append(trade.net_return_pct)
            for reason in trade.signal_reason_codes:
                by_reason[reason].append(trade.net_return_pct)
            holding_minutes.append((trade.exit_time - trade.entry_time).total_seconds() / 60.0)

        rejected_reasons: Counter[str] = Counter()
        for event in log_events:
            if event.event_type in {"SIGNAL_REJECTED", "RISK_REJECTED"}:
                for reason in event.reason_codes:
                    rejected_reasons[reason] += 1

        def _mean(vals: list[float]) -> float:
            return sum(vals) / len(vals) if vals else 0.0

        slippage_sensitivity = {}
        for slip in slippage_scenarios:
            round_trip = (2.0 * self.cost_model.commission_pct) + self.cost_model.transaction_tax_pct + (2.0 * float(slip))
            scenario_returns = [g - round_trip for g in gross_returns]
            slippage_sensitivity[f"{slip:.4f}"] = {
                "expectancy_per_trade": _mean(scenario_returns),
                "total_return_sum": sum(scenario_returns),
            }

        return {
            "total_trades": len(trades),
            "win_rate": len(wins) / len(trades) if trades else 0.0,
            "average_gain": _mean(wins),
            "average_loss": _mean(losses),
            "expectancy_per_trade": _mean(net_returns),
            "profit_factor": (gross_profit / gross_loss) if gross_loss > 0.0 else (math.inf if gross_profit > 0.0 else 0.0),
            "max_drawdown": _max_drawdown(curve),
            "max_consecutive_losses": max_consecutive_losses,
            "average_holding_minutes": _mean(holding_minutes),
            "performance_by_hour": {k: {"trades": len(v), "avg_return": _mean(v)} for k, v in sorted(by_hour.items())},
            "performance_by_symbol": {k: {"trades": len(v), "avg_return": _mean(v)} for k, v in sorted(by_symbol.items())},
            "performance_by_signal_reason": {k: {"trades": len(v), "avg_return": _mean(v)} for k, v in sorted(by_reason.items())},
            "rejected_signal_count": sum(rejected_reasons.values()),
            "rejected_by_reason": dict(rejected_reasons),
            "gross_return_sum": sum(gross_returns),
            "net_return_sum": sum(net_returns),
            "cost_impact": sum(gross_returns) - sum(net_returns),
            "gross_pnl_krw": sum(t.gross_pnl for t in trades),
            "net_pnl_krw": sum(t.net_pnl for t in trades),
            "fees_krw": sum(getattr(t, "fees_krw", 0.0) for t in trades),
            "tax_krw": sum(getattr(t, "tax_krw", 0.0) for t in trades),
            "slippage_cost_krw": sum(getattr(t, "slippage_cost_krw", 0.0) for t in trades),
            "total_cost_krw": sum(getattr(t, "costs", 0.0) for t in trades),
            "slippage_sensitivity": slippage_sensitivity,
        }
