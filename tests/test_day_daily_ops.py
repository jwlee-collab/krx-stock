from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.run_kis_end_of_day_ops import _next_weekday, _score_ready_for_next_trade_date


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _run_json(args: list[str]) -> dict:
    proc = _run(args)
    if proc.returncode != 0:
        raise AssertionError(f"command failed: {' '.join(args)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return json.loads(proc.stdout)


def _bootstrap(db_path: Path) -> None:
    _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])


def _insert_daily_score(conn: sqlite3.Connection, symbol: str, score_date: str, rank: int = 1) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO daily_prices(symbol,date,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?)
        """,
        (symbol, score_date, 100.0, 105.0, 95.0, 101.0, 1000000.0),
    )
    conn.execute(
        """
        INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
        VALUES(?,?,?,?)
        """,
        (symbol, score_date, 0.9, rank),
    )


def _prepare_full_session_db(db_path: Path) -> None:
    _bootstrap(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript((FIXTURE_DIR / "day_daily_scores_full_session_smoke.sql").read_text(encoding="utf-8"))
    conn.commit()
    conn.close()
    proc = _run(
        [
            "scripts/load_intraday_prices.py",
            "--db",
            str(db_path),
            "--csv",
            str(FIXTURE_DIR / "day_intraday_full_session_smoke.csv"),
        ]
    )
    if proc.returncode != 0:
        raise AssertionError(proc.stdout + proc.stderr)


def _prepare_partial_session_db(db_path: Path) -> None:
    _bootstrap(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript((FIXTURE_DIR / "day_daily_scores_smoke.sql").read_text(encoding="utf-8"))
    conn.commit()
    conn.close()
    proc = _run(
        [
            "scripts/load_intraday_prices.py",
            "--db",
            str(db_path),
            "--csv",
            str(FIXTURE_DIR / "day_intraday_smoke.csv"),
        ]
    )
    if proc.returncode != 0:
        raise AssertionError(proc.stdout + proc.stderr)


class DayDailyOpsTests(unittest.TestCase):
    def test_daily_ops_dry_run_uses_prior_score_date_and_writes_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _bootstrap(db_path)
            conn = sqlite3.connect(db_path)
            _insert_daily_score(conn, "005930", "2026-05-07")
            conn.commit()
            conn.close()
            out_dir = tmp / "reports"
            proc = _run(
                [
                    "scripts/run_kis_daily_ops.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--max-symbols",
                    "1",
                    "--dry-run",
                    "--skip-rolling",
                    "--output-dir",
                    str(out_dir),
                    "--data-output-dir",
                    str(tmp / "data"),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["score_date_check"]["score_date"], "2026-05-07")
            self.assertEqual(payload["collection"]["status"], "dry_run")
            self.assertTrue((out_dir / "day_ops_status.json").exists())
            status = json.loads((out_dir / "day_ops_status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["score_date_used"], "2026-05-07")
            self.assertIn("complete_replayable_days", status)
            self.assertIn("excluded_partial_dates", status)
            self.assertIn("next_required_complete_days_for_3day_smoke", status)

    def test_daily_ops_blocks_same_day_score_only(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _bootstrap(db_path)
            conn = sqlite3.connect(db_path)
            _insert_daily_score(conn, "005930", "2026-05-08")
            conn.commit()
            conn.close()
            proc = _run(
                [
                    "scripts/run_kis_daily_ops.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--max-symbols",
                    "1",
                    "--dry-run",
                    "--output-dir",
                    str(tmp / "reports"),
                    "--data-output-dir",
                    str(tmp / "data"),
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "blocked")
            self.assertIn(payload["blocked_reason"], {"ONLY_SAME_DAY_SCORE_AVAILABLE_BUT_FORBIDDEN", "NO_PRIOR_SCORE_DATE"})

    def test_daily_ops_blocks_stale_score_date(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _bootstrap(db_path)
            conn = sqlite3.connect(db_path)
            _insert_daily_score(conn, "005930", "2026-05-01")
            conn.commit()
            conn.close()
            proc = _run(
                [
                    "scripts/run_kis_daily_ops.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-11",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--max-symbols",
                    "1",
                    "--dry-run",
                    "--max-score-age-days",
                    "3",
                    "--output-dir",
                    str(tmp / "reports"),
                    "--data-output-dir",
                    str(tmp / "data"),
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["blocked_reason"], "STALE_SCORE_DATE")
            self.assertTrue(payload["score_date_check"]["stale_score_blocked"])

    def test_daily_ops_allows_stale_score_only_with_explicit_flag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _bootstrap(db_path)
            conn = sqlite3.connect(db_path)
            _insert_daily_score(conn, "005930", "2026-05-01")
            conn.commit()
            conn.close()
            proc = _run(
                [
                    "scripts/run_kis_daily_ops.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-11",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--max-symbols",
                    "1",
                    "--dry-run",
                    "--allow-stale-score",
                    "--skip-rolling",
                    "--output-dir",
                    str(tmp / "reports"),
                    "--data-output-dir",
                    str(tmp / "data"),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertIn("STALE_SCORE_DATE_ALLOWED", payload["score_date_check"]["warnings"])
            self.assertTrue(payload["score_date_check"]["stale_score_allowed"])
            self.assertFalse(payload["score_date_check"]["stale_score_blocked"])

    def test_rolling_excludes_partial_sessions_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _prepare_partial_session_db(db_path)
            proc = _run(
                [
                    "scripts/run_day_rolling_replay.py",
                    "--db",
                    str(db_path),
                    "--end-date",
                    "2026-05-08",
                    "--windows",
                    "3",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--output-dir",
                    str(tmp / "rolling"),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            output = json.loads(proc.stdout)
            window = output["windows"][0]
            self.assertEqual(window["status"], "blocked")
            self.assertEqual(window["replayable_days"], 0)
            self.assertIn("2026-05-08", window["excluded_partial_dates"])
            self.assertIn("2026-05-08", window["partial_replayable_dates"])

    def test_rolling_includes_partial_sessions_only_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _prepare_partial_session_db(db_path)
            output = _run_json(
                [
                    "scripts/run_day_rolling_replay.py",
                    "--db",
                    str(db_path),
                    "--end-date",
                    "2026-05-08",
                    "--windows",
                    "3",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--include-partial-sessions",
                    "--output-dir",
                    str(tmp / "rolling"),
                ]
            )
            window = output["windows"][0]
            self.assertEqual(window["status"], "ok")
            self.assertEqual(window["replayable_days"], 1)
            self.assertEqual(window["partial_session_included_count"], 1)
            self.assertIn("profitability assessment is invalid", window["assessment_note"])
            gate = window["replay_summary"]["promotion_gate"]
            self.assertNotEqual(gate["readiness_stage"], "LIVE_READY_CANDIDATE")
            self.assertIn("SESSION_INCOMPLETE", gate["reasons"])

    def test_rolling_replay_reports_insufficient_profitability_sample(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _prepare_full_session_db(db_path)
            output = _run_json(
                [
                    "scripts/run_day_rolling_replay.py",
                    "--db",
                    str(db_path),
                    "--end-date",
                    "2026-05-08",
                    "--windows",
                    "3,5",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "3",
                    "--output-dir",
                    str(tmp / "rolling"),
                ]
            )
            self.assertEqual(output["status"], "ok")
            self.assertEqual(output["windows"][0]["replayable_days"], 1)
            self.assertIn("Insufficient", output["windows"][0]["assessment_note"])
            self.assertTrue((tmp / "rolling" / "day_rolling_summary_2026-05-08.md").exists())

    def test_rolling_policy_comparison_includes_three_zero_volume_policies(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _prepare_full_session_db(db_path)
            output = _run_json(
                [
                    "scripts/run_day_rolling_replay.py",
                    "--db",
                    str(db_path),
                    "--end-date",
                    "2026-05-08",
                    "--windows",
                    "3",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "3",
                    "--compare-zero-volume-policies",
                    "--output-dir",
                    str(tmp / "rolling"),
                ]
            )
            policies = {row["policy"] for row in output["windows"][0]["policy_comparison"]}
            self.assertEqual(policies, {"strict_invalid", "no_trade_context", "drop_no_trade"})

    def test_launchd_template_exists(self) -> None:
        self.assertTrue((ROOT / "ops" / "launchd" / "com.krxstock.kis-daily-ops.plist.template").exists())
        self.assertTrue((ROOT / "ops" / "README_kis_daily_ops.md").exists())

    def test_eod_ops_dry_run_replays_prior_score_before_daily_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _bootstrap(db_path)
            conn = sqlite3.connect(db_path)
            _insert_daily_score(conn, "005930", "2026-05-08")
            conn.commit()
            conn.close()
            proc = _run(
                [
                    "scripts/run_kis_end_of_day_ops.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-11",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--max-symbols",
                    "1",
                    "--refresh-daily-after-replay",
                    "--force-refresh",
                    "--dry-run",
                    "--output-dir",
                    str(tmp / "reports"),
                    "--data-output-dir",
                    str(tmp / "data"),
                    "--daily-prices-output",
                    str(tmp / "daily_prices_eod.csv"),
                    "--universe-csv",
                    str(FIXTURE_DIR / "day_intraday_smoke.csv"),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["score_date_used_for_replay"], "2026-05-08")
            self.assertFalse(payload["replay_uses_same_day_score"])
            self.assertEqual(payload["daily_refresh"]["status"], "dry_run")
            self.assertEqual(payload["daily_refresh"]["would_generate_daily_score_date"], "2026-05-11")
            self.assertEqual(payload["operation_order"], ["intraday_replay_before_daily_refresh", "daily_refresh_after_replay"])
            self.assertTrue((tmp / "reports" / "day_eod_ops_status.json").exists())

    def test_eod_ops_blocks_same_day_score_for_replay(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _bootstrap(db_path)
            conn = sqlite3.connect(db_path)
            _insert_daily_score(conn, "005930", "2026-05-11")
            conn.commit()
            conn.close()
            proc = _run(
                [
                    "scripts/run_kis_end_of_day_ops.py",
                    "--db",
                    str(db_path),
                    "--trade-date",
                    "2026-05-11",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--max-symbols",
                    "1",
                    "--dry-run",
                    "--output-dir",
                    str(tmp / "reports"),
                    "--data-output-dir",
                    str(tmp / "data"),
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = json.loads(proc.stdout)
            self.assertIn(payload["blocked_reason"], {"NO_PRIOR_SCORE_DATE", "SAME_DAY_SCORE_FORBIDDEN"})

    def test_score_ready_for_next_trade_date_after_generated_score(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            _bootstrap(db_path)
            conn = sqlite3.connect(db_path)
            _insert_daily_score(conn, "005930", "2026-05-11")
            conn.commit()
            readiness = _score_ready_for_next_trade_date(conn, "2026-05-11", "2026-05-12")
            conn.close()
            self.assertEqual(readiness["generated_daily_score_date"], "2026-05-11")
            self.assertEqual(readiness["refreshed_daily_price_date"], "2026-05-11")
            self.assertTrue(readiness["score_ready_for_next_trade_date"])

    def test_next_weekday_skips_weekend(self) -> None:
        self.assertEqual(_next_weekday("2026-05-08"), "2026-05-11")


if __name__ == "__main__":
    unittest.main()
