from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from pipeline.day_trading import (
    CostModel,
    DayPositionTracker,
    DaySignal,
    DayTradingConfig,
    DayTradingEngine,
    IntradayBar,
    StaticDayUniverseProvider,
)
from pipeline.day_trading.reporting import build_day_validation_markdown


def _bars(symbol: str, timeframe: str, closes: list[float], volumes: list[float] | None = None) -> list[IntradayBar]:
    start = datetime(2024, 1, 2, 9, 0)
    step = 5 if timeframe == "5m" else 15
    volumes = volumes or [1000.0 for _ in closes]
    return [
        IntradayBar(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=start + timedelta(minutes=step * idx),
            open=close - 0.2,
            high=close + 0.4,
            low=close - 0.6,
            close=close,
            volume=volumes[idx],
        )
        for idx, close in enumerate(closes)
    ]


def _passing_data(symbol: str = "005930") -> dict[str, list[IntradayBar]]:
    return {
        "5m": _bars(symbol, "5m", [100, 100.5, 101, 101.5, 102, 104], [1000, 1050, 980, 1000, 1100, 1800]),
        "15m": _bars(symbol, "15m", [100, 101, 102, 104], [3000, 3100, 3000, 5000]),
    }


def _market_bars() -> list[IntradayBar]:
    return _bars("069500", "5m", [1000, 1001, 1002, 1001, 1003, 1004], [1, 1, 1, 1, 1, 1])


def _signal(ts: datetime | None = None) -> DaySignal:
    now = ts or datetime(2024, 1, 2, 10, 0)
    return DaySignal(
        strategy_id="DAY",
        symbol="005930",
        side="BUY",
        timestamp=now,
        expected_entry_price=100.0,
        stop_loss_price=98.8,
        take_profit_price=102.4,
        confidence=0.7,
        signal_reason_codes=["PRIMARY_BREAKOUT"],
        raw_metrics={},
        mode="PAPER",
        source_universe="SWING_CANDIDATES",
        created_at=now,
    )


class DayPaperAccountTests(unittest.TestCase):
    def test_default_paper_capital_config_is_conservative(self) -> None:
        cfg = DayTradingConfig()
        self.assertEqual(cfg.paper_initial_cash_krw, 10_000_000.0)
        self.assertEqual(cfg.paper_notional_per_trade_krw, 1_500_000.0)
        self.assertEqual(cfg.paper_max_total_exposure_krw, 4_000_000.0)
        self.assertTrue(cfg.paper_reject_if_cash_insufficient)
        self.assertTrue(cfg.paper_reject_if_exposure_exceeded)

    def test_cash_insufficient_rejects_paper_entry(self) -> None:
        cfg = DayTradingConfig(
            enabled=True,
            mode="PAPER",
            min_avg_trade_value=1.0,
            min_latest_trade_value=1.0,
            max_symbol_exposure_pct=20_000.0,
            max_total_exposure_pct=20_000.0,
            paper_initial_cash_krw=100.0,
            paper_notional_per_trade_krw=1_000_000.0,
            paper_max_position_value_krw=1_000_000.0,
            paper_max_total_exposure_krw=1_000_000.0,
        )
        engine = DayTradingEngine(cfg, StaticDayUniverseProvider(["005930"]))
        engine.run_once(
            "2024-01-02",
            {"005930": _passing_data()},
            now=datetime(2024, 1, 2, 10, 0),
            market_bars=_market_bars(),
            equity=cfg.paper_initial_cash_krw,
            day_start_equity=cfg.paper_initial_cash_krw,
        )
        reasons = [reason for event in engine.logger.events for reason in event.reason_codes]
        self.assertIn("PAPER_CASH_INSUFFICIENT", reasons)
        self.assertEqual(engine.paper_account.summary()["cash_rejection_count"], 1)

    def test_exposure_limit_rejects_paper_entry(self) -> None:
        cfg = DayTradingConfig(
            enabled=True,
            mode="PAPER",
            min_avg_trade_value=1.0,
            min_latest_trade_value=1.0,
            max_symbol_exposure_pct=100.0,
            max_total_exposure_pct=100.0,
            paper_initial_cash_krw=10_000_000.0,
            paper_notional_per_trade_krw=1_000_000.0,
            paper_max_position_value_krw=1_000_000.0,
            paper_max_total_exposure_krw=500_000.0,
        )
        engine = DayTradingEngine(cfg, StaticDayUniverseProvider(["005930"]))
        engine.run_once(
            "2024-01-02",
            {"005930": _passing_data()},
            now=datetime(2024, 1, 2, 10, 0),
            market_bars=_market_bars(),
            equity=cfg.paper_initial_cash_krw,
            day_start_equity=cfg.paper_initial_cash_krw,
        )
        reasons = [reason for event in engine.logger.events for reason in event.reason_codes]
        self.assertIn("PAPER_EXPOSURE_EXCEEDED", reasons)

    def test_paper_entry_uses_integer_quantity(self) -> None:
        cfg = DayTradingConfig(
            enabled=True,
            mode="PAPER",
            min_avg_trade_value=1.0,
            min_latest_trade_value=1.0,
            max_symbol_exposure_pct=1.0,
            max_total_exposure_pct=1.0,
            paper_notional_per_trade_krw=1_000.0,
            paper_max_position_value_krw=1_000.0,
            paper_max_total_exposure_krw=10_000.0,
        )
        engine = DayTradingEngine(cfg, StaticDayUniverseProvider(["005930"]))
        engine.run_once(
            "2024-01-02",
            {"005930": _passing_data()},
            now=datetime(2024, 1, 2, 10, 0),
            market_bars=_market_bars(),
            equity=cfg.paper_initial_cash_krw,
            day_start_equity=cfg.paper_initial_cash_krw,
        )
        position = engine.position_tracker.get_open_position("DAY", "005930")
        self.assertIsNotNone(position)
        self.assertEqual(float(position.qty), float(int(position.qty)))
        self.assertLessEqual(position.notional, 1_000.0)

    def test_krw_pnl_and_costs_are_accounted(self) -> None:
        cfg = DayTradingConfig(enabled=True, mode="PAPER")
        cost_model = CostModel(cfg.commission_pct, cfg.transaction_tax_pct, cfg.slippage_pct)
        tracker = DayPositionTracker()
        signal = _signal()
        entry_price = cost_model.entry_fill_price(signal.expected_entry_price)
        qty = 10
        entry_fee = cost_model.entry_commission(qty, entry_price)
        entry_slippage = cost_model.entry_slippage_cost(qty, signal.expected_entry_price, entry_price)
        tracker.open_position(
            signal,
            qty=qty,
            entry_price=entry_price,
            opened_at=signal.timestamp,
            entry_cost=entry_fee,
            entry_reference_price=signal.expected_entry_price,
            entry_fee_krw=entry_fee,
            entry_slippage_cost_krw=entry_slippage,
        )
        exit_reference = 103.0
        exit_price = cost_model.exit_fill_price(exit_reference)
        exit_fee = cost_model.exit_commission(qty, exit_price)
        exit_tax = cost_model.exit_tax(qty, exit_price)
        exit_slippage = cost_model.exit_slippage_cost(qty, exit_reference, exit_price)
        trade = tracker.close_position(
            "DAY",
            "005930",
            exit_price,
            signal.timestamp + timedelta(minutes=20),
            "TAKE_PROFIT",
            exit_cost=exit_fee + exit_tax,
            exit_reference_price=exit_reference,
            exit_fee_krw=exit_fee,
            exit_tax_krw=exit_tax,
            exit_slippage_cost_krw=exit_slippage,
        )
        self.assertIsNotNone(trade)
        expected_gross = (exit_reference - signal.expected_entry_price) * qty
        expected_cost = entry_fee + exit_fee + exit_tax + entry_slippage + exit_slippage
        self.assertAlmostEqual(trade.gross_pnl, expected_gross)
        self.assertAlmostEqual(trade.costs, expected_cost)
        self.assertAlmostEqual(trade.net_pnl, expected_gross - expected_cost)
        self.assertGreater(trade.slippage_cost_krw, 0.0)

    def test_report_distinguishes_trade_returns_from_account_return(self) -> None:
        markdown = build_day_validation_markdown(
            {
                "start_date": "2026-05-11",
                "end_date": "2026-05-11",
                "performance": {"gross_return_sum": 0.01, "net_return_sum": 0.008, "total_trades": 1},
                "paper_account": {
                    "initial_cash_krw": 10_000_000,
                    "ending_cash_krw": 10_001_000,
                    "ending_equity_krw": 10_001_000,
                    "realized_pnl_krw": 1_000,
                    "daily_return_pct": 0.0001,
                    "fees_krw": 100,
                    "tax_krw": 50,
                    "slippage_cost_krw": 25,
                    "total_cost_krw": 175,
                },
                "trade_details": [
                    {
                        "symbol": "005930",
                        "entry_time": "2026-05-11T10:00:00",
                        "entry_price": 100.0,
                        "quantity": 10,
                        "notional_krw": 1000.0,
                        "exit_time": "2026-05-11T10:20:00",
                        "exit_price": 101.0,
                        "exit_reason": "TAKE_PROFIT",
                        "gross_pnl_krw": 10.0,
                        "net_pnl_krw": 8.0,
                        "gross_return_pct": 0.01,
                        "net_return_pct": 0.008,
                        "fees_krw": 1.0,
                        "tax_krw": 0.5,
                        "slippage_cost_krw": 0.5,
                        "signal_reason_codes": ["PRIMARY_BREAKOUT"],
                    }
                ],
            },
            data_quality={},
            gate_result={},
            universe_source="SWING_CANDIDATES",
            same_day_scores_allowed=False,
            market_proxy_symbol="069500",
        )
        self.assertIn("## Paper Account Summary", markdown)
        self.assertIn("daily_return_pct: 0.0001", markdown)
        self.assertIn("## Trade Details", markdown)
        self.assertIn("return_basis_note", markdown)


if __name__ == "__main__":
    unittest.main()
