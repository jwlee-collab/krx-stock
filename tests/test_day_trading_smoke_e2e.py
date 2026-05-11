from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pipeline.db import get_connection, init_db


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures"
INTRADAY_CSV = FIXTURE_DIR / "day_intraday_smoke.csv"
DAILY_SCORES_SQL = FIXTURE_DIR / "day_daily_scores_smoke.sql"
FULL_SESSION_INTRADAY_CSV = FIXTURE_DIR / "day_intraday_full_session_smoke.csv"
FULL_SESSION_DAILY_SCORES_SQL = FIXTURE_DIR / "day_daily_scores_full_session_smoke.sql"


def _run_json(args: list[str]) -> dict:
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise AssertionError(f"command failed: {' '.join(args)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return json.loads(proc.stdout)


class DayTradingSmokeE2ETests(unittest.TestCase):
    def _prepare_db(self, db_path: Path, score_fixture: Path = DAILY_SCORES_SQL) -> None:
        conn = get_connection(db_path)
        init_db(conn)
        conn.executescript(score_fixture.read_text(encoding="utf-8"))
        conn.commit()
        conn.close()

    def test_cli_partial_session_smoke_generates_validation_report(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "day_smoke.sqlite"
            report_path = tmp / "day_validation_report.md"
            self._prepare_db(db_path)

            load_result = _run_json(
                [
                    "scripts/load_intraday_prices.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(INTRADAY_CSV),
                    "--source",
                    "SMOKE_FIXTURE",
                    "--validate",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(load_result["load"]["invalid_rows"], 0)
            self.assertEqual(load_result["load"]["valid_rows"], 34)
            self.assertTrue(load_result["quality"]["market_proxy_available"])

            quality = _run_json(
                [
                    "scripts/validate_intraday_prices.py",
                    "--db",
                    str(db_path),
                    "--symbols",
                    "005930",
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(quality["candidate_usable_symbol_count"], 1)
            self.assertTrue(quality["market_proxy_available"])
            self.assertTrue(quality["symbols"]["005930"]["day_strategy_usable"])

            summary = _run_json(
                [
                    "scripts/run_day_replay_backtest.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--enable-day-trading",
                    "--market-symbol",
                    "999999",
                    "--report-md",
                    str(report_path),
                ]
            )

            replay = summary["replay"]
            self.assertEqual(replay["score_dates"], {"2026-05-08": "2026-05-07"})
            self.assertEqual(replay["candidate_counts"], {"2026-05-08": 1})
            self.assertEqual(replay["lookahead_validation"]["future_candle_violations"], 0)
            self.assertEqual(replay["lookahead_validation"]["market_future_candle_violations"], 0)
            self.assertEqual(replay["lookahead_validation"]["incomplete_confirm_candle_violations"], 0)
            self.assertEqual(replay["lookahead_validation"]["lookahead_score_violations"], 0)
            self.assertIn("slippage_sensitivity", replay["performance"])
            self.assertIn("cost_impact", replay["performance"])
            self.assertIn("market_context_score", json.dumps(replay.get("log_events", []), ensure_ascii=False))
            self.assertFalse(replay["session_audit"]["session_complete"])
            self.assertEqual(replay["session_audit"]["open_position_count_at_end"], 1)
            self.assertTrue(replay["session_audit"]["missing_force_exit_window"])
            self.assertEqual(replay["event_counts"]["PAPER_ENTRY"], 1)
            self.assertEqual(replay["event_counts"].get("PAPER_EXIT", 0), 0)

            gate = summary["promotion_gate"]
            self.assertFalse(gate["approved"])
            self.assertIn("INSUFFICIENT_SIGNAL_ONLY_DAYS", gate["reasons"])
            self.assertIn("INSUFFICIENT_TRADE_SAMPLE", gate["reasons"])
            self.assertIn("SESSION_INCOMPLETE", gate["reasons"])
            self.assertIn("FORCE_EXIT_WINDOW_MISSING", gate["reasons"])
            self.assertIn("OPEN_POSITIONS_REMAIN", gate["reasons"])
            self.assertIn("PAPER_EXITS_BELOW_ENTRIES", gate["reasons"])
            self.assertEqual(summary["report_path"], str(report_path))

            report = report_path.read_text(encoding="utf-8")
            self.assertIn("DAY Strategy Validation Report", report)
            self.assertIn("candidate_usable_symbol_count: 1", report)
            self.assertIn("score_dates: {'2026-05-08': '2026-05-07'}", report)
            self.assertIn("lookahead_validation", report)
            self.assertIn("signal_created_count: 1", report)
            self.assertIn("paper_entry_count: 1", report)
            self.assertIn("paper_exit_count: 0", report)
            self.assertIn("session_complete: False", report)
            self.assertIn("open_position_count_at_end: 1", report)
            self.assertIn("cost_impact", report)
            self.assertIn("slippage_sensitivity", report)
            self.assertIn("INSUFFICIENT_TRADE_SAMPLE", report)

    def test_cli_full_session_smoke_exits_and_costs_are_audited(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "day_full_session_smoke.sqlite"
            report_path = tmp / "day_full_session_validation_report.md"
            self._prepare_db(db_path, FULL_SESSION_DAILY_SCORES_SQL)

            load_result = _run_json(
                [
                    "scripts/load_intraday_prices.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(FULL_SESSION_INTRADAY_CSV),
                    "--source",
                    "FULL_SESSION_SMOKE",
                    "--validate",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(load_result["load"]["invalid_rows"], 0)
            self.assertEqual(load_result["load"]["valid_rows"], 400)
            self.assertTrue(load_result["quality"]["market_proxy_available"])
            self.assertEqual(load_result["quality"]["candidate_usable_symbol_count"], 3)

            quality = _run_json(
                [
                    "scripts/validate_intraday_prices.py",
                    "--db",
                    str(db_path),
                    "--symbols",
                    "005930,000660,035420",
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(quality["candidate_usable_symbol_count"], 3)
            self.assertTrue(quality["market_proxy_available"])

            summary = _run_json(
                [
                    "scripts/run_day_replay_backtest.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--enable-day-trading",
                    "--market-symbol",
                    "999999",
                    "--report-md",
                    str(report_path),
                ]
            )
            replay = summary["replay"]
            self.assertEqual(replay["score_dates"], {"2026-05-08": "2026-05-07"})
            self.assertEqual(replay["candidate_counts"], {"2026-05-08": 3})
            self.assertEqual(replay["lookahead_validation"]["future_candle_violations"], 0)
            self.assertEqual(replay["lookahead_validation"]["market_future_candle_violations"], 0)
            self.assertEqual(replay["lookahead_validation"]["incomplete_confirm_candle_violations"], 0)
            self.assertEqual(replay["lookahead_validation"]["lookahead_score_violations"], 0)
            self.assertTrue(summary["data_quality"]["market_proxy_available"])

            event_counts = replay["event_counts"]
            self.assertGreaterEqual(event_counts["SIGNAL_CREATED"], 3)
            self.assertGreaterEqual(event_counts["PAPER_ENTRY"], 3)
            self.assertGreaterEqual(event_counts["PAPER_EXIT"], 3)
            exit_counts = replay["exit_reason_counts"]
            self.assertGreaterEqual(exit_counts["TAKE_PROFIT"], 1)
            self.assertGreaterEqual(exit_counts["STOP_LOSS"], 1)
            self.assertGreaterEqual(exit_counts["END_OF_DAY"], 1)

            audit = replay["session_audit"]
            self.assertTrue(audit["session_complete"])
            self.assertFalse(audit["missing_force_exit_window"])
            self.assertEqual(audit["last_bar_time"], "2026-05-08T15:10:00")
            self.assertEqual(audit["expected_force_exit_time"], "15:10")
            self.assertEqual(audit["open_position_count_at_end"], 0)
            self.assertEqual(audit["open_positions_at_end"], [])

            performance = replay["performance"]
            self.assertEqual(performance["total_trades"], 3)
            self.assertNotEqual(performance["cost_impact"], 0)
            self.assertNotEqual(performance["gross_return_sum"], performance["net_return_sum"])
            self.assertIn("slippage_sensitivity", performance)
            rejection_categories = replay["rejection_analysis"]["rejection_categories"]
            self.assertGreater(rejection_categories["breakout_rejections"], 0)
            self.assertGreater(rejection_categories["volume_expansion_rejections"], 0)
            self.assertGreater(rejection_categories["data_quality_rejections"], 0)
            self.assertGreater(rejection_categories["risk_limit_skips"], 0)
            self.assertIn("005930", replay["rejection_analysis"]["rejection_by_symbol"])

            gate = summary["promotion_gate"]
            self.assertFalse(gate["approved"])
            self.assertNotEqual(gate["readiness_stage"], "LIVE_READY_CANDIDATE")
            self.assertIn("INSUFFICIENT_SIGNAL_ONLY_DAYS", gate["reasons"])
            self.assertIn("INSUFFICIENT_TRADE_SAMPLE", gate["reasons"])

            report = report_path.read_text(encoding="utf-8")
            self.assertIn("signal_created_count: 3", report)
            self.assertIn("paper_entry_count: 3", report)
            self.assertIn("paper_exit_count: 3", report)
            self.assertIn("take_profit_exit_count: 1", report)
            self.assertIn("stop_loss_exit_count: 1", report)
            self.assertIn("end_of_day_exit_count: 1", report)
            self.assertIn("session_complete: True", report)
            self.assertIn("open_position_count_at_end: 0", report)
            self.assertIn("gross_return:", report)
            self.assertIn("net_return:", report)
            self.assertIn("cost_impact:", report)
            self.assertIn("slippage_sensitivity:", report)

    def test_cli_smoke_missing_market_proxy_blocks_gate(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "day_smoke_missing_market.sqlite"
            self._prepare_db(db_path)
            _run_json(
                [
                    "scripts/load_intraday_prices.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(INTRADAY_CSV),
                    "--source",
                    "SMOKE_FIXTURE",
                ]
            )
            summary = _run_json(
                [
                    "scripts/run_day_replay_backtest.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--enable-day-trading",
                    "--market-symbol",
                    "888888",
                ]
            )
            self.assertFalse(summary["data_quality"]["market_proxy_available"])
            self.assertFalse(summary["promotion_gate"]["approved"])
            self.assertIn("MARKET_PROXY_MISSING_OR_UNUSABLE", summary["promotion_gate"]["reasons"])
            self.assertIn("MISSING_MARKET_TREND_DATA", json.dumps(summary["replay"].get("log_events", []), ensure_ascii=False))


if __name__ == "__main__":
    unittest.main()
