from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DayValidationGateConfig:
    min_signal_only_days: int = 20
    min_paper_days: int = 20
    min_live_ready_days: int = 60
    min_trade_count: int = 30
    min_expectancy_per_trade: float = 0.0
    min_profit_factor: float = 1.20
    max_drawdown: float = -0.08
    max_consecutive_losses: int = 4
    max_daily_loss_limit_violations: int = 0
    max_backtest_paper_expectancy_gap: float = 0.003


class DayValidationGate:
    def __init__(self, config: DayValidationGateConfig | None = None):
        self.config = config or DayValidationGateConfig()

    def evaluate(
        self,
        performance: dict[str, Any],
        observed_days: int,
        data_quality_passed: bool,
        lookahead_passed: bool,
        market_proxy_available: bool,
        daily_loss_limit_violations: int = 0,
        paper_performance: dict[str, Any] | None = None,
        session_complete: bool | None = None,
        missing_force_exit_window: bool | None = None,
        open_position_count_at_end: int | None = None,
        paper_entry_count: int | None = None,
        paper_exit_count: int | None = None,
    ) -> dict[str, Any]:
        cfg = self.config
        reasons: list[str] = []
        total_trades = int(performance.get("total_trades", 0) or performance.get("trade_count", 0) or 0)
        expectancy = float(performance.get("expectancy_per_trade", 0.0) or 0.0)
        profit_factor = performance.get("profit_factor", 0.0)
        profit_factor_float = float(profit_factor) if profit_factor != float("inf") else float("inf")
        max_drawdown = float(performance.get("max_drawdown", 0.0) or 0.0)
        max_consecutive_losses = int(performance.get("max_consecutive_losses", 0) or 0)
        cost_impact_present = "cost_impact" in performance
        cost_impact = performance.get("cost_impact")

        if observed_days < cfg.min_signal_only_days:
            reasons.append("INSUFFICIENT_SIGNAL_ONLY_DAYS")
        if total_trades < cfg.min_trade_count:
            reasons.append("INSUFFICIENT_TRADE_SAMPLE")
        if expectancy <= cfg.min_expectancy_per_trade:
            reasons.append("NON_POSITIVE_COST_ADJUSTED_EXPECTANCY")
        if profit_factor_float < cfg.min_profit_factor:
            reasons.append("PROFIT_FACTOR_BELOW_THRESHOLD")
        if max_drawdown < cfg.max_drawdown:
            reasons.append("MAX_DRAWDOWN_TOO_DEEP")
        if max_consecutive_losses > cfg.max_consecutive_losses:
            reasons.append("MAX_CONSECUTIVE_LOSSES_EXCEEDED")
        if daily_loss_limit_violations > cfg.max_daily_loss_limit_violations:
            reasons.append("DAILY_LOSS_LIMIT_VIOLATED")
        if not lookahead_passed:
            reasons.append("LOOKAHEAD_VALIDATION_FAILED")
        if not data_quality_passed:
            reasons.append("DATA_QUALITY_FAILED")
        if not market_proxy_available:
            reasons.append("MARKET_PROXY_MISSING_OR_UNUSABLE")
        if session_complete is False:
            reasons.append("SESSION_INCOMPLETE")
        if missing_force_exit_window:
            reasons.append("FORCE_EXIT_WINDOW_MISSING")
        if open_position_count_at_end is not None and open_position_count_at_end > 0:
            reasons.append("OPEN_POSITIONS_REMAIN")
        if paper_entry_count is not None and paper_exit_count is not None and paper_exit_count < paper_entry_count:
            reasons.append("PAPER_EXITS_BELOW_ENTRIES")
        if not cost_impact_present:
            reasons.append("COST_IMPACT_UNAVAILABLE")
        elif total_trades > 0 and abs(float(cost_impact or 0.0)) <= 1e-12:
            reasons.append("COST_ADJUSTMENT_NOT_OBSERVABLE")

        if paper_performance is not None:
            paper_expectancy = float(paper_performance.get("expectancy_per_trade", 0.0) or 0.0)
            if abs(expectancy - paper_expectancy) > cfg.max_backtest_paper_expectancy_gap:
                reasons.append("BACKTEST_PAPER_EXPECTANCY_GAP_TOO_WIDE")

        if reasons:
            stage = "PAPER" if total_trades > 0 else "SIGNAL_ONLY"
            approved = False
        elif observed_days >= cfg.min_live_ready_days:
            stage = "LIVE_READY_CANDIDATE"
            approved = True
        elif observed_days >= cfg.min_paper_days:
            stage = "SMALL_LIVE_READY_CANDIDATE"
            approved = True
        else:
            stage = "PAPER_CANDIDATE"
            approved = False
            reasons.append("INSUFFICIENT_PAPER_OBSERVATION_DAYS")

        return {
            "approved": approved,
            "readiness_stage": stage,
            "reasons": reasons,
            "thresholds": {
                "min_signal_only_days": cfg.min_signal_only_days,
                "min_paper_days": cfg.min_paper_days,
                "min_live_ready_days": cfg.min_live_ready_days,
                "min_trade_count": cfg.min_trade_count,
                "min_expectancy_per_trade": cfg.min_expectancy_per_trade,
                "min_profit_factor": cfg.min_profit_factor,
                "max_drawdown": cfg.max_drawdown,
                "max_consecutive_losses": cfg.max_consecutive_losses,
                "max_daily_loss_limit_violations": cfg.max_daily_loss_limit_violations,
                "max_backtest_paper_expectancy_gap": cfg.max_backtest_paper_expectancy_gap,
            },
        }
