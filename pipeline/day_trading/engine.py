from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.exits import DayExitManager
from pipeline.day_trading.filters import _is_no_trade_bar, _sort_bars
from pipeline.day_trading.logging import DayTradeLogger
from pipeline.day_trading.models import IntradayBar
from pipeline.day_trading.paper_account import PaperAccount
from pipeline.day_trading.performance import CostModel
from pipeline.day_trading.positions import DayPositionTracker
from pipeline.day_trading.risk import DayRiskManager
from pipeline.day_trading.signals import DaySignalGenerator


class DayTradingEngine:
    def __init__(
        self,
        config: DayTradingConfig,
        universe_provider: Any,
        signal_generator: DaySignalGenerator | None = None,
        risk_manager: DayRiskManager | None = None,
        position_tracker: DayPositionTracker | None = None,
        exit_manager: DayExitManager | None = None,
        logger: DayTradeLogger | None = None,
        cost_model: CostModel | None = None,
        paper_account: PaperAccount | None = None,
    ):
        self.config = config
        self.universe_provider = universe_provider
        self.signal_generator = signal_generator or DaySignalGenerator(config)
        self.risk_manager = risk_manager or DayRiskManager(config)
        self.position_tracker = position_tracker or DayPositionTracker()
        self.exit_manager = exit_manager or DayExitManager(config)
        self.logger = logger or DayTradeLogger()
        self.cost_model = cost_model or CostModel(
            commission_pct=config.commission_pct,
            transaction_tax_pct=config.transaction_tax_pct,
            slippage_pct=config.slippage_pct,
        )
        self.paper_account = paper_account or PaperAccount(config)

    def run_once(
        self,
        as_of_date: str | None,
        intraday_data: dict[str, dict[str, list[IntradayBar]]],
        now: datetime | None = None,
        market_bars: list[IntradayBar] | None = None,
        equity: float | None = None,
        day_start_equity: float | None = None,
    ) -> dict[str, Any]:
        cfg = self.config
        mode = cfg.normalized_mode
        current_time = now or datetime.now(timezone.utc)
        if not cfg.enabled:
            self.logger.log_event(
                "ENGINE_SKIPPED",
                strategy_id=cfg.strategy_id,
                mode=mode,
                created_at=current_time,
                reason_codes=["DAY_TRADING_DISABLED"],
                message="day_trading.enabled is false",
            )
            return {"status": "skipped", "skip_reason": "DAY_TRADING_DISABLED", "signals": []}
        cfg.validate()
        if mode == "LIVE":
            raise RuntimeError("LIVE order execution is intentionally not implemented in the DAY engine")

        selection = (
            self.universe_provider.get_universe_selection(as_of_date)
            if hasattr(self.universe_provider, "get_universe_selection")
            else None
        )
        candidates = selection.candidates if selection is not None else self.universe_provider.get_candidates(as_of_date)
        self.logger.log_universe(
            cfg.strategy_id,
            mode,
            candidates,
            cfg.universe_source if selection is None else selection.source_universe,
            score_date=selection.score_date if selection is not None else None,
            trade_date=selection.trade_date if selection is not None else as_of_date,
            same_day_score_used=selection.same_day_score_used if selection is not None else False,
            lookahead_safe=selection.lookahead_safe if selection is not None else True,
            reason_codes=selection.reason_codes if selection is not None else [],
        )
        if selection is not None and not selection.lookahead_safe:
            self.logger.log_event(
                "LOOKAHEAD_RISK",
                strategy_id=cfg.strategy_id,
                mode=mode,
                created_at=current_time,
                reason_codes=selection.reason_codes,
                raw_metrics={
                    "trade_date": selection.trade_date,
                    "score_date": selection.score_date,
                    "same_day_score_used": selection.same_day_score_used,
                },
                message="DAY universe rejected because score_date is not point-in-time safe",
            )
        if not candidates:
            self.logger.log_event(
                "SIGNAL_REJECTED",
                strategy_id=cfg.strategy_id,
                mode=mode,
                created_at=current_time,
                reason_codes=["EMPTY_UNIVERSE", *(selection.reason_codes if selection is not None else [])],
                message="no DAY universe candidates",
            )
            return {"status": "ok", "candidate_count": 0, "signals": [], "orders": [], "closed": []}

        closed = self._process_exits(intraday_data, current_time)
        signals: list[dict[str, Any]] = []
        orders: list[dict[str, Any]] = []
        pending_order_symbols: set[str] = set()

        for symbol in candidates:
            evaluation = self.signal_generator.generate(
                symbol=symbol,
                bars_by_timeframe=intraday_data.get(symbol, {}),
                source_universe=cfg.universe_source if selection is None else selection.source_universe,
                market_bars=market_bars,
                created_at=current_time,
            )
            if evaluation.rejected or evaluation.signal is None:
                self.logger.log_event(
                    "SIGNAL_REJECTED",
                    strategy_id=cfg.strategy_id,
                    mode=mode,
                    symbol=symbol,
                    created_at=current_time,
                    reason_codes=evaluation.reason_codes,
                    raw_metrics=evaluation.raw_metrics,
                    message="DAY signal rejected by intraday filter",
                )
                continue

            decision = self.risk_manager.validate_entry(
                evaluation.signal,
                self.position_tracker,
                current_time,
                equity=equity,
                day_start_equity=day_start_equity,
                pending_order_symbols=pending_order_symbols,
            )
            if not decision.approved:
                self.logger.log_event(
                    "RISK_REJECTED",
                    strategy_id=cfg.strategy_id,
                    mode=mode,
                    symbol=symbol,
                    created_at=current_time,
                    reason_codes=[decision.reason_code],
                    raw_metrics=decision.raw_metrics,
                    message="DAY signal rejected by risk manager",
                )
                continue

            signal_dict = evaluation.signal.to_dict()
            self.logger.log_signal(signal_dict)
            signals.append(signal_dict)

            if mode == "SIGNAL_ONLY":
                continue

            entry_reference_price = float(evaluation.signal.expected_entry_price)
            entry_price = self.cost_model.entry_fill_price(entry_reference_price)
            qty = int(float(decision.notional) // entry_price) if entry_price > 0.0 else 0
            entry_fee = self.cost_model.entry_commission(qty, entry_price)
            entry_slippage_cost = self.cost_model.entry_slippage_cost(qty, entry_reference_price, entry_price)
            account_check = self.paper_account.validate_entry(
                symbol=symbol,
                strategy_id=cfg.strategy_id,
                qty=qty,
                entry_price=entry_price,
                entry_fee_krw=entry_fee,
                now=current_time,
            )
            if not account_check.approved:
                self.logger.log_event(
                    "RISK_REJECTED",
                    strategy_id=cfg.strategy_id,
                    mode=mode,
                    symbol=symbol,
                    created_at=current_time,
                    reason_codes=[account_check.reason_code],
                    raw_metrics=account_check.raw_metrics,
                    message="DAY paper account rejected entry",
                )
                continue
            position = self.position_tracker.open_position(
                evaluation.signal,
                qty,
                entry_price,
                current_time,
                entry_fee,
                entry_reference_price=entry_reference_price,
                entry_fee_krw=entry_fee,
                entry_slippage_cost_krw=entry_slippage_cost,
            )
            self.paper_account.record_entry(position)
            self.logger.log_paper_entry(position)
            pending_order_symbols.add(symbol)
            orders.append(
                {
                    "strategy_id": cfg.strategy_id,
                    "mode": mode,
                    "symbol": symbol,
                    "side": "BUY",
                    "qty": qty,
                    "price": entry_price,
                    "cost": entry_fee,
                    "entry_reference_price": entry_reference_price,
                    "notional_krw": position.notional,
                    "reason": "DAY_ENTRY",
                }
            )

        self._mark_paper_account(intraday_data)
        summary = {
            "status": "ok",
            "candidate_count": len(candidates),
            "signals": signals,
            "orders": orders,
            "closed": closed,
            "open_positions": self.position_tracker.count_open(cfg.strategy_id),
            "paper_account": self.paper_account.summary(self.position_tracker.closed_trades),
        }
        self.logger.log_daily_summary(cfg.strategy_id, mode, summary)
        return summary

    def _process_exits(
        self,
        intraday_data: dict[str, dict[str, list[IntradayBar]]],
        now: datetime,
    ) -> list[dict[str, Any]]:
        cfg = self.config
        closed: list[dict[str, Any]] = []
        for position in list(self.position_tracker.get_open_positions(cfg.strategy_id)):
            exit_signal = self.exit_manager.evaluate(position, intraday_data.get(position.symbol, {}), now=now)
            if exit_signal is None:
                if cfg.zero_volume_bar_policy == "no_trade_context":
                    primary = _sort_bars(intraday_data.get(position.symbol, {}).get(cfg.timeframe_primary, []))
                    if primary and _is_no_trade_bar(primary[-1]):
                        self.logger.log_event(
                            "EXIT_SKIPPED",
                            strategy_id=cfg.strategy_id,
                            mode=cfg.normalized_mode,
                            symbol=position.symbol,
                            created_at=now,
                            reason_codes=[
                                "NO_TRADE_5M_BAR",
                                "NO_TRADE_CONTEXT_USED",
                                "NO_TRADE_BAR_BLOCKED_EXIT",
                            ],
                            raw_metrics={"latest_timestamp": primary[-1].timestamp.isoformat()},
                            message="DAY exit blocked on no-trade bar",
                        )
                continue
            if cfg.normalized_mode == "SIGNAL_ONLY":
                self.logger.log_event(
                    "EXIT_SIGNAL",
                    strategy_id=cfg.strategy_id,
                    mode=cfg.normalized_mode,
                    symbol=position.symbol,
                    reason_codes=[exit_signal.reason],
                    raw_metrics=exit_signal.raw_metrics,
                    message="DAY exit signal generated",
                )
                continue
            exit_reference_price = float(exit_signal.expected_exit_price)
            exit_price = self.cost_model.exit_fill_price(exit_reference_price)
            exit_fee = self.cost_model.exit_commission(position.qty, exit_price)
            exit_tax = self.cost_model.exit_tax(position.qty, exit_price)
            exit_slippage_cost = self.cost_model.exit_slippage_cost(position.qty, exit_reference_price, exit_price)
            trade = self.position_tracker.close_position(
                cfg.strategy_id,
                position.symbol,
                exit_price,
                exit_signal.timestamp,
                exit_signal.reason,
                exit_cost=exit_fee + exit_tax,
                exit_reference_price=exit_reference_price,
                exit_fee_krw=exit_fee,
                exit_tax_krw=exit_tax,
                exit_slippage_cost_krw=exit_slippage_cost,
            )
            if trade is None:
                continue
            self.paper_account.record_exit(trade)
            self.logger.log_paper_exit(trade)
            closed.append(
                {
                    "strategy_id": trade.strategy_id,
                    "symbol": trade.symbol,
                    "exit_reason": trade.exit_reason,
                    "exit_price": trade.exit_price,
                    "net_pnl": trade.net_pnl,
                    "net_pnl_krw": trade.net_pnl,
                    "fees_krw": trade.fees_krw,
                    "tax_krw": trade.tax_krw,
                    "slippage_cost_krw": trade.slippage_cost_krw,
                }
            )
        return closed

    def _mark_paper_account(self, intraday_data: dict[str, dict[str, list[IntradayBar]]]) -> None:
        latest_price_by_symbol: dict[str, float] = {}
        for symbol, bars_by_timeframe in intraday_data.items():
            primary = _sort_bars(bars_by_timeframe.get(self.config.timeframe_primary, []))
            if primary:
                latest_price_by_symbol[symbol] = float(primary[-1].close)
        self.paper_account.mark_to_market(self.position_tracker.get_open_positions(self.config.strategy_id), latest_price_by_symbol)
