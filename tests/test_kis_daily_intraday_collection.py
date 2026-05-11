from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

from pipeline.day_trading.data import upsert_intraday_bars
from pipeline.day_trading.models import IntradayBar
from pipeline.day_trading.reporting import build_day_validation_markdown
from pipeline.db import get_connection, init_db
from scripts.run_kis_daily_intraday_collection import (
    _build_coverage_audit,
    _delete_intraday_scope,
    _intraday_conflicts,
    _invalid_bar_breakdown,
)
from pipeline.day_trading.backtest import _invalid_rejection_diagnostics
from pipeline.day_trading.models import DayTradeLogEvent


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _insert_daily_price(conn: sqlite3.Connection, symbol: str, price_date: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO daily_prices(symbol,date,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?)
        """,
        (symbol, price_date, 100.0, 105.0, 95.0, 101.0, 1000000.0),
    )


def _insert_score(conn: sqlite3.Connection, symbol: str, score_date: str, rank: int) -> None:
    _insert_daily_price(conn, symbol, score_date)
    conn.execute(
        """
        INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
        VALUES(?,?,?,?)
        """,
        (symbol, score_date, 1.0 / rank, rank),
    )


class KisDailyIntradayCollectionTests(unittest.TestCase):
    def test_daily_collection_dry_run_uses_prior_score_date_and_proxy_target(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "market.sqlite"
            conn = get_connection(db_path)
            init_db(conn)
            _insert_score(conn, "005930", "2026-05-07", 1)
            _insert_score(conn, "000660", "2026-05-07", 2)
            _insert_score(conn, "005490", "2026-05-08", 1)
            conn.commit()
            conn.close()

            proc = _run(
                [
                    "scripts/run_kis_daily_intraday_collection.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-08",
                    "--market-symbol",
                    "069500",
                    "--top-n",
                    "10",
                    "--max-symbols",
                    "1",
                    "--dry-run",
                ]
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        plan = payload["collection_plan"]
        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(plan["score_date"], "2026-05-07")
        self.assertFalse(plan["same_day_score_used"])
        self.assertIn("MAX_SYMBOLS_BELOW_TOP_N_PARTIAL_UNIVERSE", payload["warnings"])
        self.assertTrue(payload["coverage_audit"]["partial_universe"])
        self.assertTrue(payload["coverage_audit"]["replay_collected_only"])
        self.assertEqual(plan["candidate_symbols"], ["005930"])
        self.assertIn({"date": "2026-05-08", "symbol": "069500", "source_type": "MARKET_PROXY", "score_date": None}, plan["targets"])

    def test_require_full_top_n_coverage_marks_missing_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "market.sqlite"
            conn = get_connection(db_path)
            init_db(conn)
            _insert_score(conn, "005930", "2026-05-07", 1)
            _insert_score(conn, "000660", "2026-05-07", 2)
            upsert_intraday_bars(
                conn,
                [
                    IntradayBar(
                        symbol="005930",
                        timeframe="5m",
                        timestamp=datetime.fromisoformat("2026-05-08T09:00:00"),
                        open=100.0,
                        high=101.0,
                        low=99.0,
                        close=100.0,
                        volume=1000.0,
                    )
                ],
            )
            plan = {
                "trade_date": "2026-05-08",
                "candidate_count": 2,
                "all_candidate_symbols": ["005930", "000660"],
                "candidate_symbols": ["005930"],
                "market_symbol": "069500",
            }
            coverage = _build_coverage_audit(
                conn,
                plan,
                timeframe="5m",
                replay_collected_only=False,
                require_full_top_n_coverage=True,
            )
            conn.close()
        self.assertEqual(coverage["collected_symbols"], ["005930"])
        self.assertEqual(coverage["missing_intraday_symbols"], ["000660"])
        self.assertEqual(coverage["missing_intraday_symbol_count"], 1)
        self.assertTrue(coverage["partial_universe"])
        self.assertTrue(coverage["require_full_top_n_coverage"])

    def test_daily_collection_blocks_same_day_only_scores_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "market.sqlite"
            conn = get_connection(db_path)
            init_db(conn)
            _insert_score(conn, "005930", "2026-05-08", 1)
            conn.commit()
            conn.close()

            proc = _run(
                [
                    "scripts/run_kis_daily_intraday_collection.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-08",
                    "--dry-run",
                ]
            )
        self.assertEqual(proc.returncode, 1)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "blocked")
        self.assertIn("ONLY_SAME_DAY_SCORE_AVAILABLE_BUT_FORBIDDEN", payload["blocked_reasons"])

    def test_daily_collection_does_not_create_missing_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "missing.sqlite"
            proc = _run(
                [
                    "scripts/run_kis_daily_intraday_collection.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-08",
                    "--dry-run",
                ]
            )
        self.assertEqual(proc.returncode, 2)
        self.assertFalse(db_path.exists())
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["blocked_reason"], "DB_MISSING")

    def test_intraday_conflicts_and_force_refresh_are_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "market.sqlite"
            conn = get_connection(db_path)
            init_db(conn)
            original = IntradayBar(
                symbol="005930",
                timeframe="5m",
                timestamp=datetime.fromisoformat("2026-05-08T09:00:00"),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0,
                volume=1000.0,
                amount=100000.0,
            )
            changed = IntradayBar(
                symbol="005930",
                timeframe="5m",
                timestamp=datetime.fromisoformat("2026-05-08T09:00:00"),
                open=100.0,
                high=102.0,
                low=99.0,
                close=101.0,
                volume=1000.0,
                amount=101000.0,
            )
            other_day = IntradayBar(
                symbol="005930",
                timeframe="5m",
                timestamp=datetime.fromisoformat("2026-05-09T09:00:00"),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.0,
                volume=1000.0,
                amount=100000.0,
            )
            upsert_intraday_bars(conn, [original, other_day])
            conflicts = _intraday_conflicts(conn, [changed])
            self.assertEqual(len(conflicts), 1)
            deleted = _delete_intraday_scope(conn, [changed])
            self.assertEqual(deleted, 1)
            remaining = conn.execute("SELECT COUNT(*) AS c FROM intraday_prices").fetchone()["c"]
            conn.close()
        self.assertEqual(remaining, 1)

    def test_invalid_bar_breakdown_includes_zero_volume_and_samples(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "market.sqlite"
            conn = get_connection(db_path)
            init_db(conn)
            bars = [
                IntradayBar(
                    symbol="005930",
                    timeframe="5m",
                    timestamp=datetime.fromisoformat("2026-05-08T09:00:00"),
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                    volume=0.0,
                ),
                IntradayBar(
                    symbol="005930",
                    timeframe="15m",
                    timestamp=datetime.fromisoformat("2026-05-08T09:00:00"),
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                    volume=0.0,
                ),
            ]
            upsert_intraday_bars(conn, bars)
            breakdown = _invalid_bar_breakdown(
                conn,
                trade_date="2026-05-08",
                symbols=["005930"],
                timeframes=["5m", "15m"],
            )
            conn.close()
        self.assertEqual(breakdown["zero_volume_count"], 2)
        self.assertEqual(breakdown["incomplete_aggregation_count"], 1)
        self.assertEqual(breakdown["invalid_bar_count_by_symbol"], {"005930": 3})
        self.assertEqual(breakdown["zero_volume_cause_counts"]["ZERO_VOLUME_VALID_PRICE"], 2)
        self.assertEqual(breakdown["zero_volume_cause_counts"]["INCOMPLETE_BAR"], 1)
        self.assertTrue(breakdown["invalid_bar_sample_rows"])

    def test_invalid_rejection_diagnostics_separates_repeated_events_from_unique_bars(self) -> None:
        events = [
            DayTradeLogEvent(
                event_type="SIGNAL_REJECTED",
                strategy_id="DAY",
                mode="PAPER",
                created_at=datetime.fromisoformat("2026-05-11T09:20:00"),
                symbol="005930",
                reason_codes=["INVALID_5M_BAR"],
                raw_metrics={"latest_timestamp": "2026-05-11T09:15:00"},
            ),
            DayTradeLogEvent(
                event_type="SIGNAL_REJECTED",
                strategy_id="DAY",
                mode="PAPER",
                created_at=datetime.fromisoformat("2026-05-11T09:25:00"),
                symbol="005930",
                reason_codes=["INVALID_5M_BAR"],
                raw_metrics={"latest_timestamp": "2026-05-11T09:15:00"},
            ),
            DayTradeLogEvent(
                event_type="SIGNAL_REJECTED",
                strategy_id="DAY",
                mode="PAPER",
                created_at=datetime.fromisoformat("2026-05-11T09:30:00"),
                symbol="000660",
                reason_codes=["INVALID_15M_BAR"],
                raw_metrics={"latest_timestamp": "2026-05-11T09:25:00", "confirm_latest_timestamp": "2026-05-11T09:00:00"},
            ),
        ]
        diagnostics = _invalid_rejection_diagnostics(events)
        self.assertEqual(diagnostics["invalid_event_count"], 3)
        self.assertEqual(diagnostics["invalid_unique_bar_count"], 2)
        self.assertEqual(diagnostics["invalid_repeated_evaluation_count"], 1)
        self.assertEqual(diagnostics["invalid_unique_symbol_count"], 2)

    def test_zero_signal_report_includes_blocking_reason_diagnostics(self) -> None:
        replay_result = {
            "start_date": "2026-05-11",
            "end_date": "2026-05-11",
            "score_dates": {"2026-05-11": "2026-05-08"},
            "candidate_counts": {"2026-05-11": 1},
            "lookahead_validation": {},
            "per_date_summary": {
                "2026-05-11": {
                    "candidate_count": 1,
                    "candidate_usable_symbol_count": 1,
                    "signal_count": 0,
                    "paper_entry_count": 0,
                    "paper_exit_count": 0,
                    "open_position_count_at_end": 0,
                    "session_complete": True,
                    "gross_return_sum": 0,
                    "net_return_sum": 0,
                    "cost_impact": 0,
                    "top_rejection_reasons": {"PRIMARY_BREAKOUT_MISSING": 10},
                }
            },
            "rejection_analysis": {
                "overall_rejection_reasons": {"PRIMARY_BREAKOUT_MISSING": 10},
                "rejection_categories": {"breakout_rejections": 10},
                "rejection_by_symbol": {"005930": {"PRIMARY_BREAKOUT_MISSING": 10}},
                "rejection_by_date": {"2026-05-11": {"PRIMARY_BREAKOUT_MISSING": 10}},
                "zero_signal_top_blocking_reasons_by_date": {"2026-05-11": {"PRIMARY_BREAKOUT_MISSING": 10}},
                "candidate_last_evaluated_at_by_date": {"2026-05-11": {"005930": "2026-05-11T15:30:00"}},
            },
            "data_availability": {},
            "performance": {},
            "session_audit": {"session_complete": True, "partial_session": False},
            "coverage_audit": {"requested_top_n": 1, "collected_symbol_count": 1, "partial_universe": False},
            "invalid_bar_analysis": {"zero_volume_count": 1, "invalid_bar_sample_rows": [{"symbol": "005930"}]},
        }
        md = build_day_validation_markdown(replay_result, {}, {"approved": False}, "SWING_CANDIDATES", False, "069500")
        self.assertIn("zero_signal_top_blocking_reasons_by_date", md)
        self.assertIn("candidate_last_evaluated_at_by_date", md)
        self.assertIn("Coverage Audit", md)
        self.assertIn("Invalid Bar Analysis", md)
        self.assertIn("zero_volume_count: 1", md)


if __name__ == "__main__":
    unittest.main()
