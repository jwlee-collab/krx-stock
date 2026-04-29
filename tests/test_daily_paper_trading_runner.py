import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import daily_paper_trading_runner as runner


class TestDailyPaperTradingRunner(unittest.TestCase):
    def _conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""CREATE TABLE backtest_runs (
            run_id TEXT, created_at TEXT, scoring_profile TEXT, top_n INTEGER,
            rebalance_frequency TEXT, min_holding_days INTEGER, keep_rank_threshold INTEGER,
            enable_position_stop_loss INTEGER, position_stop_loss_pct REAL,
            stop_loss_cash_mode TEXT, stop_loss_cooldown_days INTEGER,
            enable_trailing_stop INTEGER, market_filter_enabled INTEGER,
            entry_gate_enabled INTEGER, enable_overheat_entry_gate INTEGER,
            entry_quality_gate_enabled INTEGER, max_single_position_weight REAL
        )""")
        return conn

    def test_build_run_pipeline_command_contains_required_options(self):
        cmd = runner.build_run_pipeline_command(Path("a.db"), Path("u.csv"), "2026-01-01", "2026-01-02")
        joined = " ".join(cmd)
        for token in ["--source krx", "--universe-mode rolling_liquidity", "--universe-size 100", "--universe-lookback-days 20", "--scoring-version old", "--top-n 5", "--rebalance-frequency weekly", "--min-holding-days 10", "--keep-rank-threshold 9", "--enable-position-stop-loss", "--position-stop-loss-pct 0.10", "--stop-loss-cash-mode keep_cash", "--stop-loss-cooldown-days 0"]:
            self.assertIn(token, joined)

    def test_find_latest_baseline_old_run(self):
        conn = self._conn()
        conn.execute("INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("r1", "2026-01-01", "old", 5, "weekly", 10, 9, 1, 0.10, "keep_cash", 0, 0, 0, 0, 0, 0, 0.2))
        conn.execute("INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("r2", "2026-01-02", "old", 5, "weekly", 10, 9, 1, 0.10, "keep_cash", 0, 0, 0, 0, 0, 0, 0.2))
        got = runner.find_latest_baseline_old_run(conn)
        self.assertEqual(got["run_id"], "r2")

    def test_find_skips_invalid_newest(self):
        conn = self._conn()
        conn.execute("INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("ok", "2026-01-01", "old", 5, "weekly", 10, 9, 1, 0.10, "keep_cash", 0, 0, 0, 0, 0, 0, 0.2))
        conn.execute("INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("bad", "2026-01-03", "old", 5, "daily", 10, 9, 1, 0.10, "keep_cash", 0, 0, 0, 0, 0, 0, 0.2))
        got = runner.find_latest_baseline_old_run(conn)
        self.assertEqual(got["run_id"], "ok")

    def test_guardrail_weight_fail(self):
        with self.assertRaises(RuntimeError):
            runner.verify_baseline_old_guardrails({"scoring_profile":"old","top_n":5,"rebalance_frequency":"weekly","min_holding_days":10,"keep_rank_threshold":9,"enable_position_stop_loss":1,"position_stop_loss_pct":0.10,"stop_loss_cash_mode":"keep_cash","stop_loss_cooldown_days":0,"enable_trailing_stop":0,"market_filter_enabled":0,"entry_gate_enabled":0,"enable_overheat_entry_gate":0,"entry_quality_gate_enabled":0,"max_single_position_weight":0.3333})

    def test_guardrail_multiple_fail(self):
        with self.assertRaises(RuntimeError):
            runner.verify_baseline_old_guardrails({"scoring_profile":"old","top_n":5,"rebalance_frequency":"daily","min_holding_days":5,"keep_rank_threshold":9,"enable_position_stop_loss":0,"position_stop_loss_pct":0.10,"stop_loss_cash_mode":"keep_cash","stop_loss_cooldown_days":0,"enable_trailing_stop":0,"market_filter_enabled":0,"entry_gate_enabled":0,"enable_overheat_entry_gate":0,"entry_quality_gate_enabled":0,"max_single_position_weight":0.2})

    def test_query_templates_use_run_id(self):
        q = runner.run_id_filtered_queries("x")
        self.assertIn("run_id=?", q["latest_holdings_date"])
        self.assertIn("run_id=?", q["holdings_on_date"])
        self.assertIn("run_id=?", q["latest_result_date"])
        self.assertIn("run_id=?", q["risk_events"])

    def test_no_forbidden_paths_or_colab_import(self):
        src = Path("scripts/daily_paper_trading_runner.py").read_text(encoding="utf-8")
        self.assertNotIn("/content/drive", src)
        self.assertNotIn("/content/krx-stock", src)
        self.assertNotIn("google.colab", src)

    def test_argparse_required_three(self):
        ns = runner.parse_args(["--db","a","--universe-file","b","--reports-dir","c"])
        self.assertEqual(ns.db, "a")

    def test_skip_update_does_not_call_pipeline(self):
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            db = d / "x.db"
            uni = d / "u.csv"
            rep = d / "r"
            uni.write_text("symbol\n005930\n", encoding="utf-8")
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE daily_prices(symbol TEXT, date TEXT)")
            conn.execute("INSERT INTO daily_prices VALUES('005930','2026-01-01')")
            conn.execute("""CREATE TABLE backtest_runs (
                run_id TEXT, created_at TEXT, scoring_profile TEXT, top_n INTEGER,
                rebalance_frequency TEXT, min_holding_days INTEGER, keep_rank_threshold INTEGER,
                enable_position_stop_loss INTEGER, position_stop_loss_pct REAL,
                stop_loss_cash_mode TEXT, stop_loss_cooldown_days INTEGER,
                enable_trailing_stop INTEGER, market_filter_enabled INTEGER,
                entry_gate_enabled INTEGER, enable_overheat_entry_gate INTEGER,
                entry_quality_gate_enabled INTEGER, max_single_position_weight REAL
            )""")
            conn.execute("CREATE TABLE backtest_holdings(run_id TEXT,date TEXT,symbol TEXT,weight REAL)")
            conn.execute("INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("r1", "2026-01-01", "old", 5, "weekly", 10, 9, 1, 0.10, "keep_cash", 0, 0, 0, 0, 0, 0, 0.2))
            conn.execute("INSERT INTO backtest_holdings VALUES('r1','2026-01-01','005930',0.2)")
            conn.commit(); conn.close()
            with mock.patch("scripts.daily_paper_trading_runner.subprocess.run") as mrun:
                runner.main(["--db",str(db),"--universe-file",str(uni),"--reports-dir",str(rep),"--skip-update"])
                mrun.assert_not_called()

if __name__ == "__main__":
    unittest.main()
