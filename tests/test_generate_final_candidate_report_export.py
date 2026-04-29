import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.generate_final_candidate_report import _export_baseline_old_details


class BaselineOldDetailsExportTests(unittest.TestCase):
    def _setup_db(self, db_path: Path, with_trades: bool) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE backtest_runs (run_id TEXT PRIMARY KEY, initial_equity REAL)")
        conn.execute(
            "CREATE TABLE backtest_results (run_id TEXT, date TEXT, equity REAL, daily_return REAL, position_count INTEGER, max_single_position_weight REAL)"
        )
        conn.execute(
            "CREATE TABLE backtest_holdings (run_id TEXT, date TEXT, symbol TEXT, weight REAL, shares REAL, market_value REAL, entry_date TEXT, holding_days INTEGER, stopped INTEGER)"
        )
        conn.execute(
            "CREATE TABLE backtest_risk_events (run_id TEXT, date TEXT, symbol TEXT, event_type TEXT, trigger_price REAL, exit_price REAL, weight REAL, cash_effect REAL)"
        )
        if with_trades:
            conn.execute(
                "CREATE TABLE backtest_trades (run_id TEXT, date TEXT, symbol TEXT, action TEXT, weight_before REAL, weight_after REAL, trade_price REAL, reason TEXT, cash_after REAL)"
            )
        conn.execute("INSERT INTO backtest_runs (run_id, initial_equity) VALUES (?, ?)", ("run-1", 1000000.0))
        conn.execute(
            "INSERT INTO backtest_results VALUES (?, ?, ?, ?, ?, ?)",
            ("run-1", "2025-01-02", 1001000.0, 0.001, 1, 0.2),
        )
        conn.execute(
            "INSERT INTO backtest_holdings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("run-1", "2025-01-02", "005930", 0.2, 10, 200000, "2025-01-02", 0, 0),
        )
        if with_trades:
            conn.execute(
                "INSERT INTO backtest_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("run-1", "2025-01-02", "005930", "buy", 0.0, 0.2, 70000, "rebalance", 800000),
            )
        conn.commit()
        return conn

    def test_export_without_backtest_trades(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "synthetic.sqlite"
            outdir = Path(td) / "out"
            outdir.mkdir()
            conn = self._setup_db(db_path, with_trades=False)
            status = _export_baseline_old_details(conn, outdir, "run-1", {})

            trades_csv = outdir / "baseline_old_trades.csv"
            self.assertTrue(trades_csv.exists())
            with trades_csv.open(newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f))
            self.assertEqual(rows[0], ["date", "symbol", "action", "weight_before", "weight_after", "trade_price", "reason", "cash_after"])
            self.assertEqual(len(rows), 1)
            self.assertEqual(status["backtest_trades"]["status"], "unavailable")
            self.assertEqual(status["backtest_trades"]["unavailable_reason"], "backtest_trades table does not exist")

    def test_export_with_backtest_trades(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "synthetic.sqlite"
            outdir = Path(td) / "out"
            outdir.mkdir()
            conn = self._setup_db(db_path, with_trades=True)
            status = _export_baseline_old_details(conn, outdir, "run-1", {})

            trades_csv = outdir / "baseline_old_trades.csv"
            self.assertTrue(trades_csv.exists())
            with trades_csv.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["symbol"], "005930")
            self.assertEqual(status["backtest_trades"]["status"], "available")


if __name__ == "__main__":
    unittest.main()
