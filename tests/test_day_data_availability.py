from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from pipeline.day_trading.availability import build_day_data_availability_report
from pipeline.day_trading.data_loader import load_intraday_prices_csv
from pipeline.db import get_connection, init_db


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures"
FULL_SESSION_INTRADAY_CSV = FIXTURE_DIR / "day_intraday_full_session_smoke.csv"
FULL_SESSION_DAILY_SCORES_SQL = FIXTURE_DIR / "day_daily_scores_full_session_smoke.sql"
PARTIAL_INTRADAY_CSV = FIXTURE_DIR / "day_intraday_smoke.csv"


class DayDataAvailabilityTests(unittest.TestCase):
    def _prepare_db(self, db_path: Path, score_sql: str) -> None:
        conn = get_connection(db_path)
        init_db(conn)
        conn.executescript(score_sql)
        conn.commit()
        conn.close()

    def test_availability_report_maps_previous_score_date_and_replayable_date(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "availability.sqlite"
            self._prepare_db(db_path, FULL_SESSION_DAILY_SCORES_SQL.read_text(encoding="utf-8"))
            conn = get_connection(db_path)
            load_intraday_prices_csv(conn, FULL_SESSION_INTRADAY_CSV, source="FULL_SESSION_SMOKE")
            report = build_day_data_availability_report(
                conn,
                start_date="2026-05-08",
                end_date="2026-05-08",
                market_proxy_symbol="999999",
            )
            self.assertEqual(report["replayable_dates"], ["2026-05-08"])
            detail = report["date_reports"]["2026-05-08"]
            self.assertEqual(detail["score_date"], "2026-05-07")
            self.assertTrue(detail["same_day_score_exists"])
            self.assertFalse(detail["same_day_score_only_forbidden"])
            self.assertEqual(detail["candidate_count"], 3)
            self.assertEqual(detail["candidate_usable_symbol_count"], 3)
            self.assertEqual(set(detail["candidate_intraday_overlap"]), {"005930", "000660", "035420"})
            self.assertTrue(detail["market_proxy_available"])
            conn.close()

    def test_same_day_only_score_is_unreplayable_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "same_day_only.sqlite"
            self._prepare_db(
                db_path,
                """
                INSERT OR IGNORE INTO daily_prices(symbol,date,open,high,low,close,volume)
                VALUES('005930','2026-05-08',100,101,99,100,1000);
                INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
                VALUES('005930','2026-05-08',0.9,1);
                """,
            )
            conn = get_connection(db_path)
            load_intraday_prices_csv(conn, PARTIAL_INTRADAY_CSV, source="PARTIAL_SMOKE")
            report = build_day_data_availability_report(
                conn,
                start_date="2026-05-08",
                end_date="2026-05-08",
                market_proxy_symbol="999999",
            )
            detail = report["date_reports"]["2026-05-08"]
            self.assertFalse(detail["replayable"])
            self.assertTrue(detail["same_day_score_only_forbidden"])
            self.assertIn("SAME_DAY_SCORE_ONLY_FORBIDDEN", detail["failure_reasons"])
            self.assertIn("NO_PRIOR_SCORE_DATE", detail["failure_reasons"])
            conn.close()

    def test_availability_cli_empty_db_reports_cleanly(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "empty.sqlite"
            report_path = Path(td) / "availability.md"
            proc = subprocess.run(
                [
                    sys.executable,
                    "scripts/audit_day_data_availability.py",
                    "--db",
                    str(db_path),
                    "--start-date",
                    "2026-05-08",
                    "--end-date",
                    "2026-05-08",
                    "--market-symbol",
                    "999999",
                    "--report-md",
                    str(report_path),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            data = json.loads(proc.stdout)
            self.assertEqual(data["summary"]["replayable_date_count"], 0)
            self.assertEqual(data["summary"]["unreplayable_date_count"], 1)
            self.assertIn("NO_INTRADAY_DATA", data["unreplayable_dates"]["2026-05-08"])
            self.assertTrue(report_path.exists())


if __name__ == "__main__":
    unittest.main()
