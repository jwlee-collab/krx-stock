from __future__ import annotations

import csv
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


def _run_json(args: list[str]) -> dict:
    proc = _run(args)
    if proc.returncode != 0:
        raise AssertionError(f"command failed: {' '.join(args)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    return json.loads(proc.stdout)


def _insert_daily_price(conn: sqlite3.Connection, symbol: str, price_date: str) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO daily_prices(symbol,date,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?)
        """,
        (symbol, price_date, 100.0, 105.0, 95.0, 101.0, 1000000.0),
    )


class DayDataBootstrapTests(unittest.TestCase):
    def test_bootstrap_market_db_dry_run_and_create_are_safe(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "market_pipeline.sqlite"
            dry = _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path), "--dry-run"])
            self.assertFalse(dry["db_exists_before"])
            self.assertTrue(dry["would_create_db"])
            self.assertFalse(db_path.exists())

            created = _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            self.assertTrue(created["created"])
            self.assertTrue(created["schema_ensured"])
            self.assertFalse(created["missing_required_tables"])
            for table in ["daily_prices", "intraday_prices", "daily_scores", "day_trade_logs", "day_paper_positions", "day_paper_orders"]:
                self.assertTrue(created["tables"][table]["exists"])
            self.assertTrue(db_path.exists())

            second = _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            self.assertFalse(second["created"])
            self.assertTrue(second["schema_ensured"])

    def test_load_daily_prices_csv_validates_deduplicates_and_summarizes_fk_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market_pipeline.sqlite"
            csv_path = tmp / "daily_prices.csv"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["date", "symbol", "open", "high", "low", "close", "volume", "traded_value", "market"],
                )
                writer.writeheader()
                writer.writerow({"date": "2026-05-07", "symbol": "5930", "open": "80000", "high": "81000", "low": "79500", "close": "80500", "volume": "1200000", "traded_value": "96600000000", "market": "KOSPI"})
                writer.writerow({"date": "2026-05-07", "symbol": "005930", "open": "80100", "high": "81200", "low": "79600", "close": "80600", "volume": "1300000", "traded_value": "104780000000", "market": "KOSPI"})
                writer.writerow({"date": "2026-05-07", "symbol": "000660", "open": "100000", "high": "99000", "low": "98000", "close": "98500", "volume": "900000", "traded_value": "88650000000", "market": "KOSPI"})

            loaded = _run_json(["scripts/load_daily_prices.py", "--db", str(db_path), "--csv", str(csv_path)])
            load = loaded["load"]
            self.assertEqual(load["input_rows"], 3)
            self.assertEqual(load["valid_rows"], 1)
            self.assertEqual(load["invalid_rows"], 1)
            self.assertEqual(load["duplicate_key_rows"], 1)
            self.assertEqual(load["inserted_or_updated_rows"], 1)
            self.assertIn("EXTRA_COLUMNS_IGNORED_BY_DAILY_PRICES_SCHEMA", load["warnings"])
            self.assertIn("DUPLICATE_DAILY_PRICE_KEYS_LAST_ROW_WINS", load["warnings"])
            self.assertIn("INVALID_DAILY_PRICE_ROWS_SKIPPED", load["warnings"])
            self.assertEqual(load["date_counts"], {"2026-05-07": 1})
            self.assertEqual(load["symbol_counts"], {"005930": 1})
            self.assertEqual(load["daily_scores_fk_ready_summary"]["price_key_count"], 1)

            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT close, volume FROM daily_prices WHERE symbol='005930' AND date='2026-05-07'").fetchone()
            conn.close()
            self.assertEqual(row, (80600.0, 1300000.0))

    def test_load_daily_prices_requires_existing_db_unless_explicit_bootstrap(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market_pipeline.sqlite"
            csv_path = tmp / "daily_prices.csv"
            csv_path.write_text(
                "date,symbol,open,high,low,close,volume\n2026-05-07,005930,80000,81000,79500,80500,1200000\n",
                encoding="utf-8",
            )
            blocked = _run(["scripts/load_daily_prices.py", "--db", str(db_path), "--csv", str(csv_path)])
            self.assertEqual(blocked.returncode, 2)
            self.assertFalse(db_path.exists())
            self.assertIn("DB does not exist", blocked.stderr)

            loaded = _run_json(
                [
                    "scripts/load_daily_prices.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(csv_path),
                    "--bootstrap-if-missing",
                ]
            )
            self.assertTrue(loaded["db_created_by_loader"])
            self.assertTrue(db_path.exists())
            self.assertEqual(loaded["load"]["valid_rows"], 1)

    def test_load_daily_scores_csv_and_previous_score_date_summary(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market_pipeline.sqlite"
            csv_path = tmp / "daily_scores.csv"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            conn = sqlite3.connect(db_path)
            _insert_daily_price(conn, "005930", "2026-05-07")
            _insert_daily_price(conn, "000660", "2026-05-07")
            conn.commit()
            conn.close()
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["score_date", "symbol", "rank", "score", "strategy", "reason"])
                writer.writeheader()
                writer.writerow({"score_date": "2026-05-07", "symbol": "005930", "rank": "1", "score": "0.91", "strategy": "SWING", "reason": "fixture"})
                writer.writerow({"score_date": "2026-05-07", "symbol": "000660", "rank": "", "score": "0.82", "strategy": "SWING", "reason": "rank-computed"})

            loaded = _run_json(
                [
                    "scripts/load_daily_scores.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(csv_path),
                    "--trade-start-date",
                    "2026-05-08",
                    "--trade-end-date",
                    "2026-05-08",
                ]
            )
            load = loaded["load"]
            self.assertEqual(load["input_rows"], 2)
            self.assertEqual(load["valid_rows"], 2)
            self.assertEqual(load["inserted_or_updated_rows"], 2)
            self.assertEqual(load["computed_rank_rows"], 1)
            self.assertIn("EXTRA_COLUMNS_IGNORED_BY_DAILY_SCORES_SCHEMA", load["warnings"])
            self.assertEqual(load["date_counts"], {"2026-05-07": 2})
            self.assertEqual(load["previous_score_date_summary"]["2026-05-08"]["previous_score_date"], "2026-05-07")
            self.assertTrue(load["previous_score_date_summary"]["2026-05-08"]["usable_by_default"])

    def test_same_day_only_scores_and_missing_intraday_are_not_replayable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market_pipeline.sqlite"
            csv_path = tmp / "daily_scores_same_day.csv"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            conn = sqlite3.connect(db_path)
            _insert_daily_price(conn, "005930", "2026-05-08")
            conn.commit()
            conn.close()
            csv_path.write_text("date,symbol,rank,score\n2026-05-08,005930,1,0.9\n", encoding="utf-8")
            loaded = _run_json(["scripts/load_daily_scores.py", "--db", str(db_path), "--csv", str(csv_path)])
            self.assertEqual(loaded["load"]["valid_rows"], 1)

            audit = _run_json(
                [
                    "scripts/audit_day_data_availability.py",
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
            detail = audit["date_reports"]["2026-05-08"]
            self.assertFalse(detail["replayable"])
            self.assertIn("SAME_DAY_SCORE_ONLY_FORBIDDEN", detail["failure_reasons"])
            self.assertIn("NO_INTRADAY_DATA", detail["failure_reasons"])
            self.assertIn("MARKET_PROXY_MISSING_OR_UNUSABLE", detail["failure_reasons"])

    def test_load_daily_scores_without_required_daily_prices_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market_pipeline.sqlite"
            csv_path = tmp / "daily_scores_without_prices.csv"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            csv_path.write_text("date,symbol,rank,score\n2026-05-07,005930,1,0.9\n", encoding="utf-8")
            proc = _run(["scripts/load_daily_scores.py", "--db", str(db_path), "--csv", str(csv_path)])
            self.assertEqual(proc.returncode, 1)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["status"], "blocked")
            self.assertEqual(payload["load"]["valid_rows"], 0)
            self.assertIn("DAILY_PRICES_REQUIRED_BEFORE_DAILY_SCORES_LOAD", payload["load"]["warnings"])
            self.assertEqual(payload["load"]["missing_daily_prices"], [{"symbol": "005930", "date": "2026-05-07"}])

    def test_previous_scores_create_candidates_but_intraday_and_market_proxy_are_required(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market_pipeline.sqlite"
            csv_path = tmp / "daily_scores_previous.csv"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            conn = sqlite3.connect(db_path)
            _insert_daily_price(conn, "005930", "2026-05-07")
            conn.commit()
            conn.close()
            csv_path.write_text("date,symbol,rank,score\n2026-05-07,005930,1,0.9\n", encoding="utf-8")
            _run_json(["scripts/load_daily_scores.py", "--db", str(db_path), "--csv", str(csv_path)])

            audit = _run_json(
                [
                    "scripts/audit_day_data_availability.py",
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
            detail = audit["date_reports"]["2026-05-08"]
            self.assertEqual(detail["score_date"], "2026-05-07")
            self.assertEqual(detail["candidate_count"], 1)
            self.assertFalse(detail["replayable"])
            self.assertIn("NO_CANDIDATE_INTRADAY_OVERLAP", detail["failure_reasons"])
            self.assertIn("NO_USABLE_CANDIDATE_INTRADAY", detail["failure_reasons"])
            self.assertIn("MARKET_PROXY_MISSING_OR_UNUSABLE", detail["failure_reasons"])

    def test_daily_prices_scores_and_intraday_loaders_make_audit_replayable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market_pipeline.sqlite"
            daily_prices_csv = tmp / "daily_prices.csv"
            daily_scores_csv = tmp / "daily_scores.csv"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
            daily_prices_csv.write_text(
                "date,symbol,open,high,low,close,volume,traded_value\n"
                "2026-05-07,005930,80000,81000,79500,80500,1200000,96600000000\n",
                encoding="utf-8",
            )
            daily_scores_csv.write_text("date,symbol,rank,score\n2026-05-07,005930,1,0.91\n", encoding="utf-8")
            _run_json(["scripts/load_daily_prices.py", "--db", str(db_path), "--csv", str(daily_prices_csv)])
            _run_json(["scripts/load_daily_scores.py", "--db", str(db_path), "--csv", str(daily_scores_csv)])
            _run_json(
                [
                    "scripts/load_intraday_prices.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(INTRADAY_CSV),
                    "--source",
                    "PARTIAL_SMOKE",
                    "--validate",
                    "--market-symbol",
                    "999999",
                ]
            )

            audit = _run_json(
                [
                    "scripts/audit_day_data_availability.py",
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
            detail = audit["date_reports"]["2026-05-08"]
            self.assertTrue(detail["replayable"])
            self.assertEqual(detail["score_date"], "2026-05-07")
            self.assertEqual(detail["candidate_count"], 1)
            self.assertEqual(detail["candidate_usable_symbol_count"], 1)
            self.assertTrue(detail["market_proxy_available"])

    def test_loaders_and_replay_do_not_create_missing_db_silently(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "missing.sqlite"
            intraday_proc = _run(
                [
                    "scripts/load_intraday_prices.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(INTRADAY_CSV),
                ]
            )
            self.assertEqual(intraday_proc.returncode, 2)
            self.assertFalse(db_path.exists())
            self.assertIn("DB does not exist", intraday_proc.stderr)

            replay_proc = _run(
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
                ]
            )
            self.assertEqual(replay_proc.returncode, 2)
            self.assertFalse(db_path.exists())
            self.assertIn("DB does not exist", replay_proc.stderr)

            run_once_proc = _run(
                [
                    "scripts/run_day_trading.py",
                    "--db",
                    str(db_path),
                    "--as-of-date",
                    "2026-05-08",
                    "--enable-day-trading",
                    "--mode",
                    "SIGNAL_ONLY",
                    "--market-symbol",
                    "999999",
                ]
            )
            self.assertEqual(run_once_proc.returncode, 2)
            self.assertFalse(db_path.exists())
            self.assertIn("DB does not exist", run_once_proc.stderr)

    def test_replay_existing_empty_db_reports_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "empty.sqlite"
            _run_json(["scripts/bootstrap_market_db.py", "--db", str(db_path)])
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
                ]
            )
            self.assertEqual(summary["status"], "blocked")
            self.assertEqual(summary["blocked_reason"], "NO_REPLAY_DATES_WITH_INTRADAY_DATA")
            self.assertEqual(summary["data_availability"]["summary"]["replayable_date_count"], 0)


if __name__ == "__main__":
    unittest.main()
