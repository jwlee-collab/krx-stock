from __future__ import annotations

import csv
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pipeline.db import get_connection


ROOT = Path(__file__).resolve().parents[1]


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


def _insert_price(conn: sqlite3.Connection, symbol: str, price_date: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO daily_prices(symbol,date,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?)
        """,
        (symbol, price_date, 100.0, 105.0, 95.0, 101.0, 1000000.0),
    )


class DayDatasetPlanTests(unittest.TestCase):
    def _prepare_db(self, db_path: Path) -> None:
        _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
        conn = get_connection(db_path)
        for price_date in ["2026-05-07", "2026-05-08", "2026-05-11"]:
            for symbol in ["005930", "000660", "035420"]:
                _insert_price(conn, symbol, price_date)
        conn.execute("INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank) VALUES('005930','2026-05-07',0.91,1)")
        conn.execute("INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank) VALUES('000660','2026-05-07',0.82,2)")
        conn.execute("INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank) VALUES('035420','2026-05-08',0.99,1)")
        conn.commit()
        conn.close()

    def test_plan_selects_previous_score_date_and_generates_manifest_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            report_path = tmp / "dataset_plan.md"
            required_csv = tmp / "required_intraday.csv"
            missing_csv = tmp / "missing_data.csv"
            self._prepare_db(db_path)

            output = _run_json(
                [
                    "scripts/plan_day_replay_dataset.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--report-md",
                    str(report_path),
                    "--required-intraday-csv",
                    str(required_csv),
                    "--missing-csv",
                    str(missing_csv),
                ]
            )
            plan = output["plan"]
            detail = plan["date_reports"]["2026-05-08"]
            self.assertEqual(detail["score_date"], "2026-05-07")
            self.assertFalse(detail["same_day_score_used"])
            self.assertEqual(detail["candidate_symbols"], ["005930"])
            self.assertEqual(plan["summary"]["total_required_candidate_symbols"], 1)
            self.assertEqual(plan["summary"]["missing_intraday_count"], 1)
            self.assertEqual(plan["summary"]["missing_market_proxy_count"], 1)

            with required_csv.open(newline="", encoding="utf-8") as f:
                required_rows = list(csv.DictReader(f))
            self.assertEqual(len(required_rows), 2)
            self.assertEqual(required_rows[0]["symbol"], "005930")
            self.assertEqual(required_rows[0]["source_type"], "CANDIDATE")
            self.assertEqual(required_rows[0]["score_date"], "2026-05-07")
            self.assertEqual(required_rows[1]["symbol"], "999999")
            self.assertEqual(required_rows[1]["source_type"], "MARKET_PROXY")

            with missing_csv.open(newline="", encoding="utf-8") as f:
                missing_rows = list(csv.DictReader(f))
            reasons = {row["reason"] for row in missing_rows}
            self.assertIn("MISSING_INTRADAY", reasons)
            self.assertIn("MISSING_MARKET_PROXY", reasons)
            self.assertTrue(report_path.exists())
            report = report_path.read_text(encoding="utf-8")
            self.assertIn("DAY Replay Dataset Plan", report)
            self.assertIn("top_missing_reasons", report)

    def test_market_proxy_required_for_every_trade_date(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            required_csv = tmp / "required_intraday.csv"
            self._prepare_db(db_path)
            _run_json(
                [
                    "scripts/plan_day_replay_dataset.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-11",
                    "--market-symbol",
                    "999999",
                    "--top-n",
                    "1",
                    "--required-intraday-csv",
                    str(required_csv),
                ]
            )
            with required_csv.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            market_rows = [row for row in rows if row["source_type"] == "MARKET_PROXY"]
            self.assertEqual([row["date"] for row in market_rows], ["2026-05-08", "2026-05-11"])

    def test_same_day_score_is_forbidden_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "same_day.sqlite"
            missing_csv = tmp / "missing.csv"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            conn = get_connection(db_path)
            _insert_price(conn, "005930", "2026-05-08")
            conn.execute("INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank) VALUES('005930','2026-05-08',0.9,1)")
            conn.commit()
            conn.close()

            output = _run_json(
                [
                    "scripts/plan_day_replay_dataset.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                    "--missing-csv",
                    str(missing_csv),
                ]
            )
            detail = output["plan"]["date_reports"]["2026-05-08"]
            self.assertIsNone(detail["score_date"])
            self.assertIn("NO_PRIOR_SCORE_DATE", detail["failure_reasons"])
            self.assertIn("SAME_DAY_SCORE_ONLY_FORBIDDEN", detail["failure_reasons"])
            with missing_csv.open(newline="", encoding="utf-8") as f:
                missing_rows = list(csv.DictReader(f))
            self.assertIn("NO_PRIOR_SCORE_DATE", {row["reason"] for row in missing_rows})

    def test_missing_db_is_blocked_and_not_created(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "missing.sqlite"
            proc = _run(
                [
                    "scripts/plan_day_replay_dataset.py",
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
            self.assertIn("DB does not exist", proc.stderr)


if __name__ == "__main__":
    unittest.main()
