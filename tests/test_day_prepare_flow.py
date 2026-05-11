from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures"
INTRADAY_CSV = FIXTURE_DIR / "day_intraday_smoke.csv"


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _json_from_stdout(proc: subprocess.CompletedProcess[str]) -> dict:
    return json.loads(proc.stdout)


def _write_daily_prices(path: Path) -> None:
    path.write_text(
        "date,symbol,open,high,low,close,volume,traded_value\n"
        "2026-05-07,005930,80000,81000,79500,80500,1200000,96600000000\n",
        encoding="utf-8",
    )


def _write_daily_scores(path: Path) -> None:
    path.write_text("date,symbol,rank,score\n2026-05-07,005930,1,0.91\n", encoding="utf-8")


def _write_swing_prices(path: Path, price_date: str = "2026-05-07", symbol: str = "005930") -> None:
    path.write_text(
        "symbol,date,open,high,low,close,volume\n"
        f"{symbol},{price_date},100,105,95,101,1000000\n",
        encoding="utf-8",
    )


class DayPrepareFlowTests(unittest.TestCase):
    def test_prepare_blocks_when_db_missing_without_bootstrap(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "missing.sqlite"
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 2)
            self.assertFalse(db_path.exists())
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["status"], "blocked")
            self.assertEqual(payload["blocked_reason"], "DB_MISSING_AND_BOOTSTRAP_NOT_REQUESTED")

    def test_prepare_bootstrap_creates_schema_but_reports_no_data_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "market.sqlite"
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            self.assertTrue(db_path.exists())
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["status"], "blocked")
            self.assertEqual(payload["blocked_reason"], "DATA_NOT_REPLAYABLE_AFTER_PREPARE")
            self.assertEqual(payload["steps"]["bootstrap"]["status"], "ok")

    def test_prepare_missing_csv_is_explicitly_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            missing_prices = tmp / "missing_daily_prices.csv"
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--daily-prices-csv",
                    str(missing_prices),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["steps"]["daily_prices"]["status"], "blocked")
            self.assertEqual(payload["steps"]["daily_prices"]["reason"], "DAILY_PRICES_CSV_NOT_FOUND")

    def test_prepare_dry_run_does_not_modify_existing_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            daily_prices = tmp / "daily_prices.csv"
            _write_daily_prices(daily_prices)
            boot = _run(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            self.assertEqual(boot.returncode, 0)
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--daily-prices-csv",
                    str(daily_prices),
                    "--dry-run",
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 0)
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["steps"]["daily_prices"]["status"], "dry_run")
            conn = sqlite3.connect(db_path)
            count = conn.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
            conn.close()
            self.assertEqual(count, 0)

    def test_prepare_calls_plan_and_audit_without_replay_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            daily_prices = tmp / "daily_prices.csv"
            daily_scores = tmp / "daily_scores.csv"
            plan_md = tmp / "plan.md"
            audit_md = tmp / "audit.md"
            _write_daily_prices(daily_prices)
            _write_daily_scores(daily_scores)
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--daily-prices-csv",
                    str(daily_prices),
                    "--daily-scores-csv",
                    str(daily_scores),
                    "--intraday-csv",
                    str(INTRADAY_CSV),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                    "--dataset-plan-md",
                    str(plan_md),
                    "--audit-report-md",
                    str(audit_md),
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["status"], "ok")
            self.assertEqual(payload["steps"]["dataset_plan"]["status"], "ok")
            self.assertEqual(payload["steps"]["availability_audit"]["status"], "ok")
            self.assertEqual(payload["steps"]["replay"]["status"], "skipped")
            self.assertTrue(plan_md.exists())
            self.assertTrue(audit_md.exists())

    def test_prepare_run_replay_true_blocks_when_data_is_insufficient(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            daily_prices = tmp / "daily_prices.csv"
            daily_scores = tmp / "daily_scores.csv"
            _write_daily_prices(daily_prices)
            _write_daily_scores(daily_scores)
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--daily-prices-csv",
                    str(daily_prices),
                    "--daily-scores-csv",
                    str(daily_scores),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                    "--run-replay",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["status"], "blocked")
            self.assertEqual(payload["steps"]["replay"]["status"], "blocked")
            self.assertEqual(payload["steps"]["replay"]["blocked_reason"], "NO_REPLAY_DATES_WITH_INTRADAY_DATA")

    def test_audit_project_data_sources_reports_files_and_missing_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            data_dir = tmp / "data"
            data_dir.mkdir()
            (data_dir / "krx_source_universe_500.csv").write_text("symbol\n005930\n", encoding="utf-8")
            (data_dir / "intraday_candidates_5m.csv").write_text("symbol,timestamp\n005930,2026-05-08T09:05:00\n", encoding="utf-8")
            report_md = tmp / "sources.md"
            proc = _run(
                [
                    "scripts/audit_project_data_sources.py",
                    "--db",
                    str(tmp / "missing.sqlite"),
                    "--data-dir",
                    str(data_dir),
                    "--report-md",
                    str(report_md),
                ]
            )
            self.assertEqual(proc.returncode, 0)
            payload = _json_from_stdout(proc)
            self.assertFalse(payload["db_exists"])
            self.assertIn("DB_MISSING", payload["blocking_reasons"])
            self.assertTrue(payload["classified_files"]["universe_csv"])
            self.assertTrue(payload["classified_files"]["intraday_csv"])
            self.assertTrue(report_md.exists())

    def test_prepare_run_swing_pipeline_conflicts_with_direct_daily_scores(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            prices = tmp / "prices.csv"
            scores = tmp / "scores.csv"
            _write_swing_prices(prices)
            _write_daily_scores(scores)
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--run-swing-pipeline",
                    "--prices-csv",
                    str(prices),
                    "--daily-scores-csv",
                    str(scores),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 2)
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["blocked_reason"], "SWING_PIPELINE_AND_DAILY_SCORES_CSV_CONFLICT")

    def test_prepare_run_swing_pipeline_missing_prices_csv_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--run-swing-pipeline",
                    "--prices-csv",
                    str(tmp / "missing_prices.csv"),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["steps"]["swing_pipeline"]["reason"], "MISSING_PRICES_CSV")

    def test_prepare_run_swing_pipeline_blocks_when_no_scores_generated(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            prices = tmp / "prices.csv"
            universe = tmp / "universe.csv"
            _write_swing_prices(prices, symbol="005930")
            universe.write_text("symbol\n000660\n", encoding="utf-8")
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--run-swing-pipeline",
                    "--prices-csv",
                    str(prices),
                    "--universe-csv",
                    str(universe),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = _json_from_stdout(proc)
            self.assertIn("NO_DAILY_SCORES_GENERATED", payload["steps"]["swing_pipeline"]["blocked_reasons"])

    def test_prepare_run_swing_pipeline_dry_run_does_not_modify_db(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            prices = tmp / "prices.csv"
            _write_swing_prices(prices)
            boot = _run(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            self.assertEqual(boot.returncode, 0)
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--run-swing-pipeline",
                    "--prices-csv",
                    str(prices),
                    "--swing-pipeline-dry-run",
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = _json_from_stdout(proc)
            self.assertEqual(payload["status"], "blocked")
            self.assertEqual(payload["blocked_reason"], "DATA_NOT_REPLAYABLE_AFTER_PREPARE")
            self.assertEqual(payload["steps"]["swing_pipeline"]["status"], "dry_run")
            conn = sqlite3.connect(db_path)
            score_count = conn.execute("SELECT COUNT(*) FROM daily_scores").fetchone()[0]
            conn.close()
            self.assertEqual(score_count, 0)

    def test_prepare_run_swing_pipeline_generates_previous_score_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            prices = tmp / "prices.csv"
            _write_swing_prices(prices, price_date="2026-05-07")
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--run-swing-pipeline",
                    "--prices-csv",
                    str(prices),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = _json_from_stdout(proc)
            swing = payload["steps"]["swing_pipeline"]
            self.assertEqual(swing["status"], "ok")
            self.assertGreater(swing["daily_scores"]["row_count"], 0)
            self.assertEqual(
                swing["previous_score_date_summary"]["per_trade_date"]["2026-05-08"]["previous_score_date"],
                "2026-05-07",
            )

    def test_prepare_run_swing_pipeline_same_day_score_only_remains_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            prices = tmp / "prices.csv"
            _write_swing_prices(prices, price_date="2026-05-08")
            proc = _run(
                [
                    "scripts/prepare_day_replay_db.py",
                    "--db",
                    str(db_path),
                    "--bootstrap",
                    "--run-swing-pipeline",
                    "--prices-csv",
                    str(prices),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(proc.returncode, 1)
            payload = _json_from_stdout(proc)
            swing = payload["steps"]["swing_pipeline"]
            self.assertIn("SAME_DAY_SCORE_ONLY_FORBIDDEN", swing["blocked_reasons"])
            self.assertEqual(
                swing["previous_score_date_summary"]["per_trade_date"]["2026-05-08"]["same_day_only_forbidden"],
                True,
            )


if __name__ == "__main__":
    unittest.main()
