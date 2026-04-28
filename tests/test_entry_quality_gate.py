from __future__ import annotations

import unittest

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db


class EntryQualityGateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_connection(":memory:")
        init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def _insert_row(
        self,
        symbol: str,
        date: str,
        rank: int,
        ret_5d: float,
        range_pct: float,
        volatility_20d: float,
        momentum_20d: float,
    ) -> None:
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
            (symbol, date, 0.01, ret_5d, momentum_20d, 0.01, 0.01, 0.01, range_pct, volatility_20d, 0.5),
        )

    def _run_with_quality_gate(self) -> str:
        return run_backtest(
            self.conn,
            top_n=2,
            rebalance_frequency="daily",
            min_holding_days=3,
            keep_rank_threshold=2,
            stop_loss_cash_mode="keep_cash",
            enable_entry_quality_gate=True,
            enable_entry_range_rule=True,
            enable_entry_volatility_rule=True,
            enable_entry_ret5_minus_range_rule=True,
            enable_entry_range_to_ret5_rule=True,
            enable_entry_volatility_to_momentum20_rule=True,
            max_entry_quality_range_pct=0.16,
            max_entry_volatility_20d=0.06,
            min_entry_ret5_minus_range=0.0,
            max_entry_range_to_ret5=1.5,
            max_entry_volatility_to_momentum20=0.5,
        )

    def test_quality_gate_rejects_new_entries(self) -> None:
        self._insert_row("A", "2024-01-01", 1, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("A", "2024-01-02", 1, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("A", "2024-01-03", 1, 0.10, 0.08, 0.02, 0.10)
        # 신규 후보 B는 규칙 다수 위반
        self._insert_row("B", "2024-01-03", 2, 0.05, 0.20, 0.10, 0.10)
        self._insert_row("A", "2024-01-04", 1, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("B", "2024-01-04", 2, 0.05, 0.20, 0.10, 0.10)
        self.conn.commit()

        run_id = self._run_with_quality_gate()
        diag = self.conn.execute(
            """
            SELECT entry_quality_rejected_count, rejected_by_range_pct, rejected_by_volatility_20d,
                   rejected_by_ret5_minus_range, rejected_by_range_to_ret5,
                   rejected_by_volatility_to_momentum20
            FROM backtest_runs WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
        self.assertIsNotNone(diag)
        self.assertGreaterEqual(int(diag[0]), 1)
        self.assertGreaterEqual(int(diag[1]), 1)
        self.assertGreaterEqual(int(diag[2]), 1)
        self.assertGreaterEqual(int(diag[3]), 1)
        self.assertGreaterEqual(int(diag[4]), 1)
        self.assertGreaterEqual(int(diag[5]), 1)

    def test_quality_gate_does_not_force_sell_existing_holdings(self) -> None:
        for d in ["2024-01-01", "2024-01-02"]:
            self._insert_row("A", d, 1, 0.10, 0.08, 0.02, 0.10)
            self._insert_row("C", d, 2, 0.10, 0.08, 0.02, 0.10)
            self._insert_row("B", d, 3, 0.10, 0.08, 0.02, 0.10)
        # day3: B는 rank1이지만 난폭한 상승(품질 거부), A는 기존 보유
        self._insert_row("B", "2024-01-03", 1, 0.05, 0.20, 0.10, 0.10)
        self._insert_row("A", "2024-01-03", 2, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("C", "2024-01-03", 3, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("B", "2024-01-04", 1, 0.05, 0.20, 0.10, 0.10)
        self._insert_row("A", "2024-01-04", 2, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("C", "2024-01-04", 3, 0.10, 0.08, 0.02, 0.10)
        self.conn.commit()

        run_id = self._run_with_quality_gate()
        holdings_d3 = {
            r[0]
            for r in self.conn.execute(
                "SELECT symbol FROM backtest_holdings WHERE run_id=? AND date='2024-01-03'",
                (run_id,),
            ).fetchall()
        }
        self.assertIn("A", holdings_d3)

    def test_quality_gate_increases_cash_when_candidates_blocked(self) -> None:
        self._insert_row("A", "2024-01-01", 1, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("A", "2024-01-02", 1, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("A", "2024-01-03", 1, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("B", "2024-01-03", 2, 0.05, 0.20, 0.10, 0.10)
        self._insert_row("A", "2024-01-04", 1, 0.10, 0.08, 0.02, 0.10)
        self._insert_row("B", "2024-01-04", 2, 0.05, 0.20, 0.10, 0.10)
        self.conn.commit()

        run_id = self._run_with_quality_gate()
        diag = self.conn.execute(
            """
            SELECT entry_quality_cash_days, average_cash_weight, average_exposure
            FROM backtest_runs WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
        self.assertIsNotNone(diag)
        self.assertGreaterEqual(int(diag[0]), 1)
        self.assertGreater(float(diag[1]), 0.0)
        self.assertLess(float(diag[2]), 1.0)


if __name__ == "__main__":
    unittest.main()
