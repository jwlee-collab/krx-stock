from __future__ import annotations

import csv
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

from pipeline.day_trading.data import upsert_intraday_bars
from pipeline.day_trading.models import IntradayBar
from pipeline.db import get_connection, init_db


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )


def _write_intraday_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = ["symbol", "timestamp", "timeframe", "open", "high", "low", "close", "volume", "traded_value", "source"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class IntradayOhlcvConflictAuditTests(unittest.TestCase):
    def test_conflict_audit_detects_mismatch_and_creates_backup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "market.sqlite"
            csv_path = root / "incoming.csv"
            backup_path = root / "backup.csv"
            json_path = root / "audit.json"
            conn = get_connection(db_path)
            init_db(conn)
            upsert_intraday_bars(
                conn,
                [
                    IntradayBar(
                        symbol="005930",
                        timeframe="5m",
                        timestamp=datetime.fromisoformat("2026-05-11T09:00:00"),
                        open=100.0,
                        high=101.0,
                        low=99.0,
                        close=100.0,
                        volume=1000.0,
                        amount=100000.0,
                        source="EXISTING",
                    )
                ],
            )
            conn.close()
            _write_intraday_csv(
                csv_path,
                [
                    {
                        "symbol": "005930",
                        "timestamp": "2026-05-11T09:00:00",
                        "timeframe": "5m",
                        "open": 100,
                        "high": 102,
                        "low": 99,
                        "close": 101,
                        "volume": 1100,
                        "traded_value": 111100,
                        "source": "INCOMING",
                    }
                ],
            )
            proc = _run(
                [
                    "scripts/audit_intraday_ohlcv_conflicts.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(csv_path),
                    "--date",
                    "2026-05-11",
                    "--symbols",
                    "005930",
                    "--timeframes",
                    "5m",
                    "--market-symbol",
                    "005930",
                    "--backup-csv",
                    str(backup_path),
                    "--output-json",
                    str(json_path),
                    "--min-symbol-count",
                    "1",
                    "--expected-5m-count",
                    "1",
                    "--expected-15m-count",
                    "0",
                ]
            )
            self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
            payload = json.loads(proc.stdout)
            self.assertEqual(payload["comparison"]["conflict_row_count"], 1)
            self.assertEqual(payload["comparison"]["conflict_by_field"]["high"], 1)
            self.assertEqual(payload["comparison"]["conflict_by_field"]["close"], 1)
            self.assertEqual(payload["comparison"]["conflict_by_field"]["volume"], 1)
            self.assertTrue(payload["backup"]["created"])
            self.assertTrue(backup_path.exists())
            self.assertTrue(payload["safety"]["force_refresh_safe"])
            self.assertTrue(json_path.exists())

    def test_backup_scope_does_not_include_unrelated_date_or_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "market.sqlite"
            csv_path = root / "incoming.csv"
            backup_path = root / "backup.csv"
            conn = get_connection(db_path)
            init_db(conn)
            upsert_intraday_bars(
                conn,
                [
                    IntradayBar(
                        symbol="005930",
                        timeframe="5m",
                        timestamp=datetime.fromisoformat("2026-05-11T09:00:00"),
                        open=100,
                        high=101,
                        low=99,
                        close=100,
                        volume=1000,
                    ),
                    IntradayBar(
                        symbol="000660",
                        timeframe="5m",
                        timestamp=datetime.fromisoformat("2026-05-11T09:00:00"),
                        open=100,
                        high=101,
                        low=99,
                        close=100,
                        volume=1000,
                    ),
                    IntradayBar(
                        symbol="005930",
                        timeframe="5m",
                        timestamp=datetime.fromisoformat("2026-05-10T09:00:00"),
                        open=100,
                        high=101,
                        low=99,
                        close=100,
                        volume=1000,
                    ),
                ],
            )
            conn.close()
            _write_intraday_csv(
                csv_path,
                [
                    {
                        "symbol": "005930",
                        "timestamp": "2026-05-11T09:00:00",
                        "timeframe": "5m",
                        "open": 100,
                        "high": 101,
                        "low": 99,
                        "close": 100,
                        "volume": 1000,
                        "traded_value": "",
                        "source": "INCOMING",
                    }
                ],
            )
            proc = _run(
                [
                    "scripts/audit_intraday_ohlcv_conflicts.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(csv_path),
                    "--date",
                    "2026-05-11",
                    "--symbols",
                    "005930",
                    "--timeframes",
                    "5m",
                    "--market-symbol",
                    "005930",
                    "--backup-csv",
                    str(backup_path),
                    "--min-symbol-count",
                    "1",
                    "--expected-5m-count",
                    "1",
                    "--expected-15m-count",
                    "0",
                ]
            )
            with backup_path.open(newline="", encoding="utf-8") as f:
                backup_rows = list(csv.DictReader(f))
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(len(backup_rows), 1)
        self.assertEqual(backup_rows[0]["symbol"], "005930")
        self.assertEqual(backup_rows[0]["timestamp"], "2026-05-11T09:00:00")

    def test_unsafe_partial_session_blocks_force_refresh_recommendation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "market.sqlite"
            csv_path = root / "incoming.csv"
            backup_path = root / "backup.csv"
            conn = get_connection(db_path)
            init_db(conn)
            conn.close()
            _write_intraday_csv(
                csv_path,
                [
                    {
                        "symbol": "005930",
                        "timestamp": "2026-05-11T09:00:00",
                        "timeframe": "5m",
                        "open": 100,
                        "high": 101,
                        "low": 99,
                        "close": 100,
                        "volume": 1000,
                        "traded_value": "",
                        "source": "INCOMING",
                    }
                ],
            )
            proc = _run(
                [
                    "scripts/audit_intraday_ohlcv_conflicts.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(csv_path),
                    "--date",
                    "2026-05-11",
                    "--symbols",
                    "005930",
                    "--timeframes",
                    "5m,15m",
                    "--market-symbol",
                    "005930",
                    "--backup-csv",
                    str(backup_path),
                    "--min-symbol-count",
                    "1",
                    "--expected-5m-count",
                    "79",
                    "--expected-15m-count",
                    "27",
                ]
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertFalse(payload["safety"]["force_refresh_safe"])
        self.assertIn("expected_5m_count_ok", payload["safety"]["failed_conditions"])
        self.assertIn("expected_15m_count_ok", payload["safety"]["failed_conditions"])
        self.assertTrue(payload["incoming_partial_session"])

    def test_missing_market_proxy_blocks_force_refresh_recommendation(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            db_path = root / "market.sqlite"
            csv_path = root / "incoming.csv"
            backup_path = root / "backup.csv"
            conn = get_connection(db_path)
            init_db(conn)
            conn.close()
            _write_intraday_csv(
                csv_path,
                [
                    {
                        "symbol": "005930",
                        "timestamp": "2026-05-11T09:00:00",
                        "timeframe": "5m",
                        "open": 100,
                        "high": 101,
                        "low": 99,
                        "close": 100,
                        "volume": 1000,
                        "traded_value": "",
                        "source": "INCOMING",
                    }
                ],
            )
            proc = _run(
                [
                    "scripts/audit_intraday_ohlcv_conflicts.py",
                    "--db",
                    str(db_path),
                    "--csv",
                    str(csv_path),
                    "--date",
                    "2026-05-11",
                    "--timeframes",
                    "5m",
                    "--market-symbol",
                    "069500",
                    "--backup-csv",
                    str(backup_path),
                    "--min-symbol-count",
                    "1",
                    "--expected-5m-count",
                    "1",
                    "--expected-15m-count",
                    "0",
                ]
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertFalse(payload["safety"]["force_refresh_safe"])
        self.assertIn("market_proxy_included", payload["safety"]["failed_conditions"])


if __name__ == "__main__":
    unittest.main()
