from __future__ import annotations

import unittest

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db


class OverheatEntryGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_connection(":memory:")
        init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def _insert_row(self, symbol: str, date: str, rank: int, ret_5d: float, volume_z20: float) -> None:
        px = 100.0
        self.conn.execute(
            "INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES(?,?,?,?,?,?,?)",
            (symbol, date, px, px, px, px, 1_000_000),
        )
        self.conn.execute(
            "INSERT INTO daily_scores(symbol,date,score,rank) VALUES(?,?,?,?)",
            (symbol, date, 1.0, rank),
        )
        self.conn.execute(
            """
            INSERT INTO daily_features(
                symbol,date,ret_1d,ret_5d,momentum_20d,momentum_60d,sma_20_gap,sma_60_gap,
                range_pct,volatility_20d,volume_z20
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (symbol, date, 0.01, ret_5d, 0.01, 0.01, 0.01, 0.01, 0.05, 0.01, volume_z20),
        )

    def _run_with_gate(self) -> str:
        return run_backtest(
            self.conn,
            top_n=2,
            rebalance_frequency="daily",
            min_holding_days=3,
            keep_rank_threshold=2,
            stop_loss_cash_mode="keep_cash",
            enable_overheat_entry_gate=True,
            max_entry_ret_1d=0.08,
            max_entry_ret_5d=0.15,
            max_entry_range_pct=0.10,
            max_entry_volume_z20=3.0,
            enable_volume_surge_overheat_rule=True,
            volume_surge_threshold=3.0,
            volume_surge_ret_5d_threshold=0.10,
        )

    def test_gate_does_not_force_sell_existing_holdings(self) -> None:
        # day1/day2: A,C held. day3: B is overheat rank1, A rank2(existing), C rank3.
        for d in ["2024-01-01", "2024-01-02"]:
            self._insert_row("A", d, 1, 0.03, 0.5)
            self._insert_row("C", d, 2, 0.03, 0.5)
            self._insert_row("B", d, 3, 0.03, 0.5)
        self._insert_row("B", "2024-01-03", 1, 0.20, 4.0)
        self._insert_row("A", "2024-01-03", 2, 0.03, 0.5)
        self._insert_row("C", "2024-01-03", 3, 0.03, 0.5)
        self._insert_row("B", "2024-01-04", 1, 0.20, 4.0)
        self._insert_row("A", "2024-01-04", 2, 0.03, 0.5)
        self._insert_row("C", "2024-01-04", 3, 0.03, 0.5)
        self.conn.commit()

        run_id = self._run_with_gate()
        holdings_d3 = {
            r[0]
            for r in self.conn.execute(
                "SELECT symbol FROM backtest_holdings WHERE run_id=? AND date='2024-01-03'",
                (run_id,),
            ).fetchall()
        }
        self.assertIn("A", holdings_d3)

    def test_gate_rejects_new_entries_and_increases_cash_when_slots_not_filled(self) -> None:
        # day1/day2: only A exists so one slot remains cash. day3/day4: B,C appear but both overheat.
        self._insert_row("A", "2024-01-01", 1, 0.03, 0.5)
        self._insert_row("A", "2024-01-02", 1, 0.03, 0.5)
        self._insert_row("A", "2024-01-03", 1, 0.03, 0.5)
        self._insert_row("B", "2024-01-03", 2, 0.20, 4.0)
        self._insert_row("C", "2024-01-03", 3, 0.18, 3.5)
        self._insert_row("A", "2024-01-04", 1, 0.03, 0.5)
        self._insert_row("B", "2024-01-04", 2, 0.20, 4.0)
        self._insert_row("C", "2024-01-04", 3, 0.18, 3.5)
        self.conn.commit()

        run_id = self._run_with_gate()
        diag = self.conn.execute(
            """
            SELECT overheat_rejected_count, overheat_cash_days, average_cash_weight, average_exposure
            FROM backtest_runs WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
        self.assertIsNotNone(diag)
        self.assertGreaterEqual(int(diag[0]), 1)
        self.assertGreaterEqual(int(diag[1]), 1)
        self.assertGreater(float(diag[2]), 0.0)
        self.assertLess(float(diag[3]), 1.0)


if __name__ == "__main__":
    unittest.main()
