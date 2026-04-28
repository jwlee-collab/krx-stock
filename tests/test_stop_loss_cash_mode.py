from __future__ import annotations

import unittest

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db


class StopLossRiskManagementTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_connection(":memory:")
        init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def _seed_prices_scores(self, dates: list[str], symbols: list[str], close_by_symbol_date: dict[tuple[str, str], float]) -> None:
        for sym in symbols:
            for d in dates:
                px = close_by_symbol_date[(sym, d)]
                self.conn.execute(
                    "INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)",
                    (sym, d, px, px, px, px, 1_000_000),
                )
                self.conn.execute(
                    "INSERT INTO daily_scores(symbol,date,score,rank) VALUES(?,?,?,?)",
                    (sym, d, 1.0, int(sym[1:])),
                )
        self.conn.commit()

    def test_keep_cash_reduces_total_weight_without_rebalancing(self) -> None:
        dates = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]
        symbols = ["S1", "S2", "S3", "S4", "S5"]
        prices: dict[tuple[str, str], float] = {}
        for sym in symbols:
            for d in dates:
                prices[(sym, d)] = 100.0
        prices[("S1", "2024-01-02")] = 80.0  # trigger -20% stop loss from entry

        self._seed_prices_scores(dates, symbols, prices)

        run_id = run_backtest(
            self.conn,
            top_n=5,
            rebalance_frequency="weekly",
            enable_position_stop_loss=True,
            position_stop_loss_pct=0.10,
            stop_loss_cash_mode="keep_cash",
        )

        day_holdings = self.conn.execute(
            "SELECT weight FROM backtest_holdings WHERE run_id=? AND date='2024-01-02' ORDER BY symbol",
            (run_id,),
        ).fetchall()
        total_weight = sum(float(r[0]) for r in day_holdings)
        self.assertLess(total_weight, 1.0)

        result_row = self.conn.execute(
            "SELECT exposure, max_single_position_weight FROM backtest_results WHERE run_id=? AND date='2024-01-03'",
            (run_id,),
        ).fetchone()
        self.assertIsNotNone(result_row)
        self.assertAlmostEqual(float(result_row[0]), 0.8, places=6)
        self.assertLessEqual(float(result_row[1]), 0.2 + 1e-12)

    def test_stop_loss_cooldown_blocks_reentry(self) -> None:
        dates = ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]
        symbols = ["S1", "S2", "S3", "S4", "S5", "S6"]
        prices: dict[tuple[str, str], float] = {}
        for sym in symbols:
            for d in dates:
                prices[(sym, d)] = 100.0
        prices[("S1", "2024-01-02")] = 80.0

        for sym in symbols:
            for d in dates:
                px = prices[(sym, d)]
                rank = 1 if sym == "S1" else int(sym[1:]) + 1
                self.conn.execute(
                    "INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)",
                    (sym, d, px, px, px, px, 1_000_000),
                )
                self.conn.execute(
                    "INSERT INTO daily_scores(symbol,date,score,rank) VALUES(?,?,?,?)",
                    (sym, d, 1.0, rank),
                )
        self.conn.commit()

        run_id = run_backtest(
            self.conn,
            top_n=5,
            rebalance_frequency="daily",
            enable_position_stop_loss=True,
            position_stop_loss_pct=0.10,
            stop_loss_cash_mode="rebalance_remaining",
            stop_loss_cooldown_days=3,
        )

        blocked_days = ["2024-01-03", "2024-01-04", "2024-01-05"]
        for d in blocked_days:
            row = self.conn.execute(
                "SELECT COUNT(1) FROM backtest_holdings WHERE run_id=? AND date=? AND symbol='S1'",
                (run_id, d),
            ).fetchone()
            self.assertEqual(int(row[0]), 0)


if __name__ == "__main__":
    unittest.main()
