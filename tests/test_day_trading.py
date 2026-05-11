from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from pipeline.day_trading import (
    CostModel,
    DayExitManager,
    DayPerformanceAnalyzer,
    DayPosition,
    DayPositionTracker,
    DayRiskManager,
    DaySignal,
    DaySignalGenerator,
    DayTradeLogger,
    DayTradingConfig,
    DayTradingEngine,
    IntradayFilter,
    IntradayBar,
    StaticDayUniverseProvider,
)
from scripts.run_day_replay_backtest import _build_policy_comparison_markdown


def _bars(symbol: str, timeframe: str, closes: list[float], volumes: list[float] | None = None) -> list[IntradayBar]:
    start = datetime(2024, 1, 2, 9, 0)
    step = 5 if timeframe == "5m" else 15
    volumes = volumes or [1000.0 for _ in closes]
    out: list[IntradayBar] = []
    for idx, close in enumerate(closes):
        ts = start + timedelta(minutes=step * idx)
        out.append(
            IntradayBar(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=ts,
                open=close - 0.2,
                high=close + 0.4,
                low=close - 0.6,
                close=close,
                volume=volumes[idx],
            )
        )
    return out


def _passing_data(symbol: str = "005930") -> dict[str, list[IntradayBar]]:
    return {
        "5m": _bars(symbol, "5m", [100, 100.5, 101, 101.5, 102, 104], [1000, 1050, 980, 1000, 1100, 1800]),
        "15m": _bars(symbol, "15m", [100, 101, 102, 104], [3000, 3100, 3000, 5000]),
    }


def _market_bars() -> list[IntradayBar]:
    return _bars("MARKET", "5m", [1000, 1001, 1002, 1001, 1003, 1004], [1, 1, 1, 1, 1, 1])


def _signal(symbol: str = "005930", ts: datetime | None = None) -> DaySignal:
    now = ts or datetime(2024, 1, 2, 10, 0)
    return DaySignal(
        strategy_id="DAY",
        symbol=symbol,
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


class DayTradingTests(unittest.TestCase):
    def test_default_config_is_disabled_signal_only(self) -> None:
        cfg = DayTradingConfig()
        self.assertFalse(cfg.enabled)
        self.assertEqual(cfg.mode, "SIGNAL_ONLY")
        self.assertEqual(cfg.strategy_id, "DAY")

    def test_empty_universe_generates_no_signal(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        engine = DayTradingEngine(cfg, StaticDayUniverseProvider([]))
        summary = engine.run_once("2024-01-02", {}, now=datetime(2024, 1, 2, 10, 0))
        self.assertEqual(summary["signals"], [])
        self.assertEqual(engine.logger.events[-1].reason_codes, ["EMPTY_UNIVERSE"])

    def test_missing_intraday_data_fail_closed(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        result = DaySignalGenerator(cfg).generate("005930", {}, "SWING_CANDIDATES")
        self.assertTrue(result.rejected)
        self.assertIn("MISSING_5M_DATA", result.reason_codes)

    def test_duplicate_intraday_timestamp_fail_closed(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        data = _passing_data()
        data["5m"].append(data["5m"][-1])
        result = DaySignalGenerator(cfg).generate("005930", data, "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertTrue(result.rejected)
        self.assertIn("DUPLICATE_5M_TIMESTAMP", result.reason_codes)

    def test_intraday_gap_fail_closed(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        primary = _bars(
            "005930",
            "5m",
            [100, 100.5, 101, 101.5, 102, 103, 104],
            [1000, 1050, 980, 1000, 1100, 1600, 2200],
        )
        data = {
            "5m": [bar for idx, bar in enumerate(primary) if idx != 3],
            "15m": _bars("005930", "15m", [100, 101, 102, 104], [3000, 3100, 3000, 5000]),
        }
        result = DaySignalGenerator(cfg).generate("005930", data, "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertTrue(result.rejected)
        self.assertIn("MISSING_5M_BAR_GAP", result.reason_codes)

    def test_invalid_earlier_intraday_bar_fail_closed(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        data = _passing_data()
        bad = data["5m"][0]
        data["5m"][0] = IntradayBar(
            bad.symbol,
            bad.timestamp,
            bad.timeframe,
            open=bad.open,
            high=bad.close - 1.0,
            low=bad.low,
            close=bad.close,
            volume=bad.volume,
        )
        result = DaySignalGenerator(cfg).generate("005930", data, "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertTrue(result.rejected)
        self.assertIn("INVALID_5M_BAR", result.reason_codes)

    def test_strict_zero_volume_normal_ohlc_is_invalid_bar(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        data = _passing_data()
        original = data["5m"][0]
        data["5m"][0] = IntradayBar(
            original.symbol,
            original.timestamp,
            original.timeframe,
            original.open,
            original.high,
            original.low,
            original.close,
            0.0,
            0.0,
        )
        result = DaySignalGenerator(cfg).generate("005930", data, "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertTrue(result.rejected)
        self.assertIn("INVALID_5M_BAR", result.reason_codes)
        self.assertIn("ZERO_VOLUME_VALID_OHLC", result.reason_codes)

    def test_no_trade_context_zero_volume_current_bar_blocks_entry(self) -> None:
        cfg = DayTradingConfig(
            enabled=True,
            min_avg_trade_value=1.0,
            min_latest_trade_value=1.0,
            zero_volume_bar_policy="no_trade_context",
        )
        data = _passing_data()
        latest = data["5m"][-1]
        data["5m"][-1] = IntradayBar(
            latest.symbol,
            latest.timestamp,
            latest.timeframe,
            latest.open,
            latest.high,
            latest.low,
            latest.close,
            0.0,
            0.0,
        )
        result = DaySignalGenerator(cfg).generate("005930", data, "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertTrue(result.rejected)
        self.assertIn("NO_TRADE_5M_BAR", result.reason_codes)
        self.assertIn("NO_TRADE_BAR_BLOCKED_ENTRY", result.reason_codes)
        self.assertNotIn("INVALID_5M_BAR", result.reason_codes)

    def test_no_trade_context_vwap_uses_positive_volume_bars_only(self) -> None:
        cfg = DayTradingConfig(
            enabled=True,
            min_avg_trade_value=1.0,
            min_latest_trade_value=1.0,
            zero_volume_bar_policy="no_trade_context",
        )
        data = {
            "5m": _bars("005930", "5m", [1000, 100, 101, 102, 103, 105], [0, 1000, 1000, 1000, 1000, 2500]),
            "15m": _bars("005930", "15m", [100, 101, 102, 105], [3000, 3100, 3000, 6000]),
        }
        result = IntradayFilter(cfg).evaluate("005930", data, market_bars=_market_bars())
        self.assertTrue(result.passed, result.reason_codes)
        self.assertLess(float(result.raw_metrics["vwap"]), 105.0)
        self.assertTrue(result.raw_metrics["vwap_positive_volume_only"])

    def test_no_trade_context_zero_volume_bar_does_not_trigger_exit(self) -> None:
        cfg = DayTradingConfig(enabled=True, zero_volume_bar_policy="no_trade_context")
        position = DayPosition("DAY", "005930", 1, 100, 98.8, 102.4, datetime(2024, 1, 2, 10, 0), "2024-01-02")
        data = {"5m": _bars("005930", "5m", [100, 97], [1000, 0])}
        data["5m"][-1] = IntradayBar("005930", datetime(2024, 1, 2, 10, 5), "5m", 97, 98, 96, 97, 0.0, 0.0)
        exit_signal = DayExitManager(cfg).evaluate(position, data)
        self.assertIsNone(exit_signal)

    def test_drop_no_trade_removes_zero_volume_rows_before_filtering(self) -> None:
        cfg = DayTradingConfig(
            enabled=True,
            min_avg_trade_value=1.0,
            min_latest_trade_value=1.0,
            zero_volume_bar_policy="drop_no_trade",
        )
        data = {
            "5m": _bars("005930", "5m", [99, 100, 100.5, 101, 101.5, 102, 104], [0, 1000, 1050, 980, 1000, 1100, 1800]),
            "15m": _bars("005930", "15m", [99, 100, 101, 102, 104], [0, 3000, 3100, 3000, 5000]),
        }
        result = IntradayFilter(cfg).evaluate("005930", data, market_bars=_market_bars())
        self.assertTrue(result.passed, result.reason_codes)
        self.assertEqual(result.raw_metrics["no_trade_bar_count"], 0)

    def test_zero_volume_policy_comparison_markdown_lists_all_policies(self) -> None:
        markdown = _build_policy_comparison_markdown(
            {
                "start_date": "2026-05-11",
                "end_date": "2026-05-11",
                "policy_comparison": [
                    {"policy": "strict_invalid"},
                    {"policy": "no_trade_context"},
                    {"policy": "drop_no_trade"},
                ],
            }
        )
        self.assertIn("strict_invalid", markdown)
        self.assertIn("no_trade_context", markdown)
        self.assertIn("drop_no_trade", markdown)

    def test_low_liquidity_rejects_signal(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1_000_000_000.0, min_latest_trade_value=1.0)
        result = DaySignalGenerator(cfg).generate("005930", _passing_data(), "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertTrue(result.rejected)
        self.assertIn("LOW_AVG_TRADE_VALUE", result.reason_codes)

    def test_vwap_failure_rejects_signal(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        data = {
            "5m": _bars("005930", "5m", [120, 121, 122, 123, 124, 119], [5000, 5000, 5000, 5000, 5000, 1000]),
            "15m": _bars("005930", "15m", [100, 101, 102, 104], [3000, 3000, 3000, 5000]),
        }
        result = DaySignalGenerator(cfg).generate("005930", data, "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertTrue(result.rejected)
        self.assertIn("VWAP_NOT_CONFIRMED", result.reason_codes)

    def test_breakout_on_5m_and_15m_generates_signal(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1.0, min_latest_trade_value=1.0)
        result = DaySignalGenerator(cfg).generate("005930", _passing_data(), "SWING_CANDIDATES", market_bars=_market_bars())
        self.assertFalse(result.rejected)
        self.assertIsNotNone(result.signal)
        self.assertEqual(result.signal.strategy_id, "DAY")
        self.assertIn("PRIMARY_BREAKOUT", result.signal.signal_reason_codes)
        self.assertIn("CONFIRM_BREAKOUT", result.signal.signal_reason_codes)

    def test_max_trades_per_day_limit(self) -> None:
        cfg = DayTradingConfig(enabled=True, mode="PAPER", max_trades_per_day=1, max_open_positions=3)
        tracker = DayPositionTracker()
        first = _signal("005930")
        tracker.open_position(first, qty=1, entry_price=100, opened_at=first.timestamp)
        tracker.close_position("DAY", "005930", 101, first.timestamp + timedelta(minutes=5), "TAKE_PROFIT")
        decision = DayRiskManager(cfg).validate_entry(_signal("000660"), tracker, datetime(2024, 1, 2, 10, 10))
        self.assertFalse(decision.approved)
        self.assertEqual(decision.reason_code, "MAX_TRADES_PER_DAY")

    def test_max_trades_per_symbol_per_day_limit(self) -> None:
        cfg = DayTradingConfig(enabled=True, mode="PAPER", max_trades_per_day=3, max_trades_per_symbol_per_day=1)
        tracker = DayPositionTracker()
        first = _signal("005930")
        tracker.open_position(first, qty=1, entry_price=100, opened_at=first.timestamp)
        tracker.close_position("DAY", "005930", 101, first.timestamp + timedelta(minutes=5), "TAKE_PROFIT")
        decision = DayRiskManager(cfg).validate_entry(_signal("005930"), tracker, datetime(2024, 1, 2, 10, 10))
        self.assertFalse(decision.approved)
        self.assertEqual(decision.reason_code, "MAX_TRADES_PER_SYMBOL_PER_DAY")

    def test_stop_loss_exit(self) -> None:
        cfg = DayTradingConfig(enabled=True)
        position = DayPosition("DAY", "005930", 1, 100, 98.8, 102.4, datetime(2024, 1, 2, 10, 0), "2024-01-02")
        data = {"5m": _bars("005930", "5m", [100, 99], [1000, 1000])}
        data["5m"][-1] = IntradayBar("005930", datetime(2024, 1, 2, 10, 5), "5m", 99, 99.5, 98.0, 99, 1000)
        exit_signal = DayExitManager(cfg).evaluate(position, data)
        self.assertIsNotNone(exit_signal)
        self.assertEqual(exit_signal.reason, "STOP_LOSS")

    def test_take_profit_exit(self) -> None:
        cfg = DayTradingConfig(enabled=True)
        position = DayPosition("DAY", "005930", 1, 100, 98.8, 102.4, datetime(2024, 1, 2, 10, 0), "2024-01-02")
        data = {"5m": _bars("005930", "5m", [100, 102], [1000, 1000])}
        data["5m"][-1] = IntradayBar("005930", datetime(2024, 1, 2, 10, 5), "5m", 102, 103.0, 101.5, 102.5, 1000)
        exit_signal = DayExitManager(cfg).evaluate(position, data)
        self.assertIsNotNone(exit_signal)
        self.assertEqual(exit_signal.reason, "TAKE_PROFIT")

    def test_force_end_of_day_exit(self) -> None:
        cfg = DayTradingConfig(enabled=True, force_exit_time="15:10")
        position = DayPosition("DAY", "005930", 1, 100, 90, 120, datetime(2024, 1, 2, 10, 0), "2024-01-02")
        data = {"5m": _bars("005930", "5m", [100, 101, 102], [1000, 1000, 1000])}
        data["5m"][-1] = IntradayBar("005930", datetime(2024, 1, 2, 15, 10), "5m", 102, 103, 101, 102, 1000)
        exit_signal = DayExitManager(cfg).evaluate(position, data)
        self.assertIsNotNone(exit_signal)
        self.assertEqual(exit_signal.reason, "END_OF_DAY")

    def test_day_exit_does_not_touch_swing_position_same_symbol(self) -> None:
        tracker = DayPositionTracker()
        opened = datetime(2024, 1, 2, 10, 0)
        tracker.add_position(DayPosition("SWING", "005930", 10, 90, 80, 120, opened, "2024-01-02"))
        tracker.add_position(DayPosition("DAY", "005930", 1, 100, 98, 103, opened, "2024-01-02"))
        result = tracker.close_position("DAY", "005930", 101, opened + timedelta(minutes=20), "TAKE_PROFIT")
        self.assertIsNotNone(result)
        self.assertIsNotNone(tracker.get_open_position("SWING", "005930"))
        self.assertIsNone(tracker.get_open_position("DAY", "005930"))

    def test_strategy_id_separation(self) -> None:
        tracker = DayPositionTracker()
        opened = datetime(2024, 1, 2, 10, 0)
        tracker.add_position(DayPosition("SWING", "005930", 10, 90, 80, 120, opened, "2024-01-02"))
        self.assertEqual(tracker.count_open("DAY"), 0)
        self.assertEqual(tracker.count_open("SWING"), 1)

    def test_live_mode_requires_explicit_approval(self) -> None:
        cfg = DayTradingConfig(enabled=True, mode="LIVE")
        with self.assertRaises(PermissionError):
            cfg.validate()

    def test_cost_adjusted_performance_is_below_gross(self) -> None:
        cfg = DayTradingConfig(enabled=True, mode="PAPER")
        cost_model = CostModel(cfg.commission_pct, cfg.transaction_tax_pct, cfg.slippage_pct)
        tracker = DayPositionTracker()
        sig = _signal("005930")
        entry = cost_model.entry_fill_price(sig.expected_entry_price)
        position = tracker.open_position(sig, qty=10, entry_price=entry, opened_at=sig.timestamp, entry_cost=cost_model.entry_cost(10, entry))
        exit_price = cost_model.exit_fill_price(103)
        tracker.close_position("DAY", position.symbol, exit_price, sig.timestamp + timedelta(minutes=20), "TAKE_PROFIT", cost_model.exit_cost(10, exit_price))
        report = DayPerformanceAnalyzer(cost_model=cost_model).analyze(tracker.closed_trades)
        self.assertGreater(report["gross_return_sum"], report["net_return_sum"])
        self.assertGreater(report["cost_impact"], 0.0)

    def test_rejection_reason_logged_and_analyzed(self) -> None:
        logger = DayTradeLogger()
        logger.log_event("SIGNAL_REJECTED", "DAY", "SIGNAL_ONLY", "005930", ["MISSING_5M_DATA"])
        report = DayPerformanceAnalyzer().analyze([], logger.events)
        self.assertEqual(report["rejected_signal_count"], 1)
        self.assertEqual(report["rejected_by_reason"]["MISSING_5M_DATA"], 1)


if __name__ == "__main__":
    unittest.main()
