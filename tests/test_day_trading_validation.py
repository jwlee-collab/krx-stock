from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from pipeline.day_trading.backtest import run_day_replay_backtest
from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.context import IntradayMarketContext
from pipeline.day_trading.data import load_intraday_bars_until, upsert_intraday_bars
from pipeline.day_trading.data_loader import load_intraday_prices_csv
from pipeline.day_trading.data_quality import validate_intraday_prices
from pipeline.day_trading.engine import DayTradingEngine
from pipeline.day_trading.models import IntradayBar
from pipeline.day_trading.universe import DayUniverseProvider, StaticDayUniverseProvider
from pipeline.day_trading.validation import DayValidationGate, DayValidationGateConfig
from pipeline.db import get_connection, init_db


def _bars(symbol: str, timeframe: str, closes: list[float], start: datetime | None = None) -> list[IntradayBar]:
    ts = start or datetime(2026, 5, 8, 9, 0)
    step = 5 if timeframe == "5m" else 15
    out: list[IntradayBar] = []
    for i, close in enumerate(closes):
        out.append(
            IntradayBar(
                symbol=symbol,
                timeframe=timeframe,
                timestamp=ts + timedelta(minutes=step * i),
                open=close - 0.2,
                high=close + 0.5,
                low=close - 0.5,
                close=close,
                volume=10_000 + i * 1_000,
                amount=close * (10_000 + i * 1_000),
                source="TEST",
            )
        )
    return out


class DayTradingValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_connection(":memory:")
        init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def _seed_score(self, date: str, symbol: str, rank: int = 1) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)",
            (symbol, date, 100, 101, 99, 100, 1000),
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank) VALUES(?,?,?,?)",
            (symbol, date, 1.0 / rank, rank),
        )
        self.conn.commit()

    def test_intraday_csv_load_and_duplicate_upsert(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "intraday.csv"
            csv_path.write_text(
                "symbol,timestamp,timeframe,open,high,low,close,volume,traded_value\n"
                "005930,2026-05-08T09:00:00,5m,100,101,99,100,1000,100000\n"
                "005930,2026-05-08T09:00:00,5m,100,102,99,101,1200,121200\n",
                encoding="utf-8",
            )
            result = load_intraday_prices_csv(self.conn, csv_path)
        self.assertEqual(result.input_rows, 2)
        self.assertEqual(result.invalid_rows, 0)
        row = self.conn.execute("SELECT COUNT(*) AS c, MAX(close) AS close FROM intraday_prices").fetchone()
        self.assertEqual(int(row["c"]), 1)
        self.assertEqual(float(row["close"]), 101.0)

    def test_data_quality_report_and_market_proxy_missing(self) -> None:
        upsert_intraday_bars(self.conn, _bars("005930", "5m", [100, 101, 102]))
        report = validate_intraday_prices(self.conn, market_proxy_symbol="MARKET")
        self.assertFalse(report["symbols"]["005930"]["day_strategy_usable"])
        self.assertFalse(report["market_proxy_available"])

    def test_data_quality_records_requested_missing_symbols(self) -> None:
        report = validate_intraday_prices(self.conn, symbols=["005930"])
        self.assertIn("005930", report["symbols"])
        self.assertFalse(report["symbols"]["005930"]["day_strategy_usable"])
        self.assertTrue(report["symbols"]["005930"]["missing_all_data"])

    def test_market_proxy_requires_primary_timeframe(self) -> None:
        upsert_intraday_bars(self.conn, _bars("005930", "5m", [100, 101, 102]))
        upsert_intraday_bars(self.conn, _bars("005930", "15m", [100, 101, 102]))
        upsert_intraday_bars(self.conn, _bars("MARKET", "15m", [1000, 1001, 1002]))
        report = validate_intraday_prices(self.conn, symbols=["005930"], market_proxy_symbol="MARKET")
        self.assertEqual(report["candidate_usable_symbol_count"], 1)
        self.assertFalse(report["market_proxy_available"])

    def test_intraday_loader_excludes_incomplete_confirm_timeframe(self) -> None:
        upsert_intraday_bars(self.conn, _bars("005930", "5m", [100, 101, 102, 103, 104]))
        upsert_intraday_bars(self.conn, _bars("005930", "15m", [100, 102]))
        data = load_intraday_bars_until(
            self.conn,
            ["005930"],
            "2026-05-08",
            ["5m", "15m"],
            datetime(2026, 5, 8, 9, 20),
            completed_timeframes=["15m"],
        )
        self.assertEqual(len(data["005930"]["5m"]), 5)
        self.assertEqual(len(data["005930"]["15m"]), 1)
        self.assertEqual(data["005930"]["15m"][0].timestamp, datetime(2026, 5, 8, 9, 0))

    def test_market_proxy_missing_fail_closed(self) -> None:
        cfg = DayTradingConfig(enabled=True, min_avg_trade_value=1, min_latest_trade_value=1, min_total_intraday_score=-1)
        engine = DayTradingEngine(cfg, StaticDayUniverseProvider(["005930"]))
        data = {"005930": {"5m": _bars("005930", "5m", [100, 101, 102, 103, 104, 106]), "15m": _bars("005930", "15m", [100, 102, 104, 106])}}
        result = engine.run_once("2026-05-08", data, now=datetime(2026, 5, 8, 10, 0))
        self.assertEqual(result["signals"], [])
        self.assertTrue(
            any("MISSING_MARKET_TREND_DATA" in event.reason_codes for event in engine.logger.events)
        )

    def test_universe_uses_previous_score_date_by_default(self) -> None:
        self._seed_score("2026-05-07", "005930")
        self._seed_score("2026-05-08", "000660")
        provider = DayUniverseProvider(self.conn, DayTradingConfig())
        selection = provider.get_universe_selection("2026-05-08")
        self.assertEqual(selection.score_date, "2026-05-07")
        self.assertEqual(selection.candidates, ["005930"])
        self.assertFalse(selection.same_day_score_used)

    def test_same_day_score_forbidden_without_override(self) -> None:
        self._seed_score("2026-05-08", "005930")
        selection = DayUniverseProvider(self.conn, DayTradingConfig()).get_universe_selection("2026-05-08")
        self.assertEqual(selection.candidates, [])
        self.assertIn("ONLY_SAME_DAY_SCORE_AVAILABLE_BUT_FORBIDDEN", selection.reason_codes)

    def test_same_day_score_allowed_only_with_override_and_recorded_in_log(self) -> None:
        self._seed_score("2026-05-08", "005930")
        cfg = DayTradingConfig(enabled=True, allow_same_day_scores=True, score_date_override="2026-05-08")
        provider = DayUniverseProvider(self.conn, cfg)
        selection = provider.get_universe_selection("2026-05-08")
        self.assertEqual(selection.candidates, ["005930"])
        self.assertTrue(selection.same_day_score_used)
        engine = DayTradingEngine(cfg, provider)
        engine.run_once("2026-05-08", {}, now=datetime(2026, 5, 8, 9, 0))
        universe_event = engine.logger.events[0]
        self.assertEqual(universe_event.raw_metrics["score_date"], "2026-05-08")
        self.assertTrue(universe_event.raw_metrics["same_day_score_used"])

    def test_intraday_context_uses_optional_flow_scores(self) -> None:
        cfg = DayTradingConfig(min_avg_trade_value=1, min_latest_trade_value=1, min_total_intraday_score=-1)
        self.conn.execute(
            """
            INSERT INTO intraday_investor_flows(
                symbol,timestamp,source,data_type,created_at,foreign_net_buy_amount,institution_net_buy_amount
            ) VALUES(?,?,?,?,?,?,?)
            """,
            ("005930", "2026-05-08T09:20:00", "TEST", "INTRADAY", "2026-05-08T09:20:00", 2_000_000, 1_000_000),
        )
        self.conn.execute(
            "INSERT INTO intraday_trade_strength(symbol,timestamp,source,data_type,created_at,buy_strength,sell_strength) VALUES(?,?,?,?,?,?,?)",
            ("005930", "2026-05-08T09:20:00", "TEST", "INTRADAY", "2026-05-08T09:20:00", 70, 30),
        )
        self.conn.execute(
            "INSERT INTO intraday_program_flows(symbol,timestamp,source,data_type,created_at,program_net_buy_amount) VALUES(?,?,?,?,?,?)",
            ("005930", "2026-05-08T09:20:00", "TEST", "INTRADAY", "2026-05-08T09:20:00", 3_000_000),
        )
        self.conn.commit()
        context = IntradayMarketContext(cfg, self.conn).build(
            "005930",
            datetime(2026, 5, 8, 9, 25),
            {"5m": _bars("005930", "5m", [100, 101, 102, 104, 106, 108]), "15m": _bars("005930", "15m", [100, 102, 104, 108])},
            market_bars=_bars("MARKET", "5m", [1000, 1001, 1002, 1003, 1004, 1005]),
        )
        self.assertGreater(context.foreign_flow_score, 0)
        self.assertGreater(context.institution_flow_score, 0)
        self.assertGreater(context.trade_strength_score, 0)
        self.assertGreater(context.program_flow_score, 0)

    def test_optional_flow_missing_reject_and_stale_reject(self) -> None:
        cfg = DayTradingConfig(investor_flow_data_policy="reject", min_total_intraday_score=-1)
        context = IntradayMarketContext(cfg, self.conn).build(
            "005930",
            datetime(2026, 5, 8, 9, 25),
            {"5m": _bars("005930", "5m", [100, 101, 102, 104, 106, 108]), "15m": _bars("005930", "15m", [100, 102, 104, 108])},
            market_bars=_bars("MARKET", "5m", [1000, 1001, 1002, 1003, 1004, 1005]),
        )
        self.assertIn("REJECT_MISSING_INVESTOR_FLOW", context.reason_codes)

        self.conn.execute(
            "INSERT INTO intraday_investor_flows(symbol,timestamp,source,data_type,created_at,foreign_net_buy_amount,institution_net_buy_amount) VALUES(?,?,?,?,?,?,?)",
            ("000660", "2026-05-08T09:00:00", "TEST", "INTRADAY", "2026-05-08T09:00:00", 1_000_000, 1_000_000),
        )
        self.conn.commit()
        stale = IntradayMarketContext(cfg, self.conn).build(
            "000660",
            datetime(2026, 5, 8, 9, 25),
            {"5m": _bars("000660", "5m", [100, 101, 102, 104, 106, 108]), "15m": _bars("000660", "15m", [100, 102, 104, 108])},
            market_bars=_bars("MARKET", "5m", [1000, 1001, 1002, 1003, 1004, 1005]),
        )
        self.assertIn("REJECT_STALE_MISSING_INVESTOR_FLOW", stale.reason_codes)

    def test_foreign_institution_double_net_sell_is_penalized(self) -> None:
        cfg = DayTradingConfig(min_total_intraday_score=-1)
        self.conn.execute(
            "INSERT INTO intraday_investor_flows(symbol,timestamp,source,data_type,created_at,foreign_net_buy_amount,institution_net_buy_amount) VALUES(?,?,?,?,?,?,?)",
            ("005930", "2026-05-08T09:20:00", "TEST", "INTRADAY", "2026-05-08T09:20:00", -2_000_000, -2_000_000),
        )
        self.conn.commit()
        context = IntradayMarketContext(cfg, self.conn).build(
            "005930",
            datetime(2026, 5, 8, 9, 25),
            {"5m": _bars("005930", "5m", [100, 101, 102, 104, 106, 108]), "15m": _bars("005930", "15m", [100, 102, 104, 108])},
            market_bars=_bars("MARKET", "5m", [1000, 1001, 1002, 1003, 1004, 1005]),
        )
        self.assertLess(context.foreign_flow_score, 0)
        self.assertLess(context.institution_flow_score, 0)
        self.assertIn("FOREIGN_INSTITUTION_NET_SELLING", context.reason_codes)

    def test_future_and_final_flow_data_not_used_in_replay_context(self) -> None:
        cfg = DayTradingConfig(investor_flow_data_policy="neutral", min_total_intraday_score=-1)
        self.conn.execute(
            "INSERT INTO intraday_investor_flows(symbol,timestamp,source,data_type,created_at,foreign_net_buy_amount,institution_net_buy_amount) VALUES(?,?,?,?,?,?,?)",
            ("005930", "2026-05-08T15:40:00", "TEST", "FINAL", "2026-05-08T15:40:00", 9_000_000, 9_000_000),
        )
        self.conn.execute(
            "INSERT INTO intraday_investor_flows(symbol,timestamp,source,data_type,created_at,foreign_net_buy_amount,institution_net_buy_amount) VALUES(?,?,?,?,?,?,?)",
            ("005930", "2026-05-08T09:30:00", "TEST", "INTRADAY", "2026-05-08T09:30:00", -1_000_000, -1_000_000),
        )
        self.conn.commit()
        context = IntradayMarketContext(cfg, self.conn).build(
            "005930",
            datetime(2026, 5, 8, 9, 25),
            {"5m": _bars("005930", "5m", [100, 101, 102, 104, 106, 108]), "15m": _bars("005930", "15m", [100, 102, 104, 108])},
            market_bars=_bars("MARKET", "5m", [1000, 1001, 1002, 1003, 1004, 1005]),
        )
        self.assertEqual(context.foreign_flow_score, 0.0)
        self.assertEqual(context.institution_flow_score, 0.0)

    def test_replay_backtest_point_in_time_validation(self) -> None:
        self._seed_score("2026-05-07", "005930")
        bars = _bars("005930", "5m", [100, 100.5, 101, 101.5, 102, 104, 105, 106, 107])
        confirm = _bars("005930", "15m", [100, 101, 102, 104, 106])
        market = _bars("MARKET", "5m", [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008])
        upsert_intraday_bars(self.conn, [*bars, *confirm, *market])
        cfg = DayTradingConfig(
            enabled=True,
            mode="PAPER",
            market_proxy_symbol="MARKET",
            min_avg_trade_value=1,
            min_latest_trade_value=1,
            min_total_intraday_score=-1,
        )
        result = run_day_replay_backtest(self.conn, "2026-05-08", "2026-05-08", cfg)
        self.assertEqual(result["lookahead_validation"]["future_candle_violations"], 0)
        self.assertEqual(result["lookahead_validation"]["lookahead_score_violations"], 0)

    def test_replay_does_not_use_incomplete_15m_confirm_bar(self) -> None:
        self._seed_score("2026-05-07", "005930")
        bars = _bars("005930", "5m", [100, 100.5, 101, 101.5, 102, 103, 104, 105, 106, 107])
        confirm = _bars("005930", "15m", [100, 101, 102, 108])
        market = _bars("MARKET", "5m", [1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009])
        upsert_intraday_bars(self.conn, [*bars, *confirm, *market])
        cfg = DayTradingConfig(
            enabled=True,
            mode="PAPER",
            market_proxy_symbol="MARKET",
            min_avg_trade_value=1,
            min_latest_trade_value=1,
            min_volume_surge_ratio=1.0,
            min_total_intraday_score=-1,
        )
        result = run_day_replay_backtest(self.conn, "2026-05-08", "2026-05-08", cfg)
        self.assertEqual(result["lookahead_validation"]["incomplete_confirm_candle_violations"], 0)
        self.assertFalse(any(event.event_type == "SIGNAL_CREATED" for event in result["log_events"]))
        self.assertTrue(
            any("INSUFFICIENT_15M_BARS" in event.reason_codes for event in result["log_events"])
        )

    def test_promotion_gate_blocks_and_approves(self) -> None:
        gate = DayValidationGate(DayValidationGateConfig(min_signal_only_days=1, min_paper_days=1, min_trade_count=2))
        bad = gate.evaluate(
            {"total_trades": 1, "expectancy_per_trade": -0.01, "profit_factor": 0.5, "max_drawdown": -0.01, "max_consecutive_losses": 1, "cost_impact": 0.001},
            observed_days=1,
            data_quality_passed=True,
            lookahead_passed=True,
            market_proxy_available=True,
        )
        self.assertFalse(bad["approved"])
        self.assertIn("INSUFFICIENT_TRADE_SAMPLE", bad["reasons"])
        good = gate.evaluate(
            {"total_trades": 3, "expectancy_per_trade": 0.002, "profit_factor": 1.5, "max_drawdown": -0.01, "max_consecutive_losses": 1, "cost_impact": 0.001},
            observed_days=2,
            data_quality_passed=True,
            lookahead_passed=True,
            market_proxy_available=True,
        )
        self.assertTrue(good["approved"])
        self.assertEqual(good["readiness_stage"], "SMALL_LIVE_READY_CANDIDATE")


if __name__ == "__main__":
    unittest.main()
