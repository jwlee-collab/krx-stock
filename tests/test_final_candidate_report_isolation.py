from __future__ import annotations

import unittest

from pipeline.db import get_connection, init_db
from scripts.generate_final_candidate_report import (
    _restore_daily_scores,
    _snapshot_daily_scores,
    _validate_candidate_outputs_not_identical,
)


class FinalCandidateReportIsolationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = get_connection(":memory:")
        init_db(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_snapshot_restore_daily_scores(self) -> None:
        self.conn.execute(
            "INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('AAA','2024-01-02',100,100,100,100,1000)"
        )
        self.conn.execute(
            "INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('BBB','2024-01-02',100,100,100,100,1000)"
        )
        self.conn.execute(
            "INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('ZZZ','2024-01-03',100,100,100,100,1000)"
        )
        self.conn.execute(
            "INSERT INTO daily_scores(symbol,date,score,rank) VALUES('AAA','2024-01-02',1.0,1)"
        )
        self.conn.execute(
            "INSERT INTO daily_scores(symbol,date,score,rank) VALUES('BBB','2024-01-02',0.9,2)"
        )
        self.conn.commit()

        _snapshot_daily_scores(self.conn, "tmp_final_scores_baseline_old")

        self.conn.execute("DELETE FROM daily_scores")
        self.conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('ZZZ','2024-01-03',5.0,1)")
        self.conn.commit()

        _restore_daily_scores(self.conn, "tmp_final_scores_baseline_old")
        rows = self.conn.execute("SELECT symbol, date, score, rank FROM daily_scores ORDER BY symbol").fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "AAA")
        self.assertEqual(rows[1]["symbol"], "BBB")

    def test_identical_paired_windows_raise_in_strict_mode(self) -> None:
        identical_rows = [
            {
                "candidate": "baseline_old",
                "eval_frequency": "monthly",
                "horizon_months": 3,
                "start_date": "2024-01-01",
                "end_date": "2024-03-29",
                "total_return": 0.1,
                "excess_return": 0.02,
                "max_drawdown": -0.2,
            },
            {
                "candidate": "aggressive_hybrid_v4",
                "eval_frequency": "monthly",
                "horizon_months": 3,
                "start_date": "2024-01-01",
                "end_date": "2024-03-29",
                "total_return": 0.1,
                "excess_return": 0.02,
                "max_drawdown": -0.2,
            },
        ]
        identical_monthly = [
            {"candidate": "baseline_old", "month": "2024-01", "strategy_return": 0.03},
            {"candidate": "aggressive_hybrid_v4", "month": "2024-01", "strategy_return": 0.03},
        ]

        with self.assertRaisesRegex(ValueError, "candidate outputs are identical"):
            _validate_candidate_outputs_not_identical(identical_rows, allow_smoke=False, monthly_rows=identical_monthly)

    def test_identical_paired_windows_warn_in_allow_smoke_mode(self) -> None:
        identical_rows = [
            {
                "candidate": "baseline_old",
                "eval_frequency": "monthly",
                "horizon_months": 3,
                "start_date": "2024-01-01",
                "end_date": "2024-03-29",
                "total_return": 0.1,
                "excess_return": 0.02,
                "max_drawdown": -0.2,
            },
            {
                "candidate": "aggressive_hybrid_v4",
                "eval_frequency": "monthly",
                "horizon_months": 3,
                "start_date": "2024-01-01",
                "end_date": "2024-03-29",
                "total_return": 0.1,
                "excess_return": 0.02,
                "max_drawdown": -0.2,
            },
        ]
        identical_monthly = [
            {"candidate": "baseline_old", "month": "2024-01", "strategy_return": 0.03},
            {"candidate": "aggressive_hybrid_v4", "month": "2024-01", "strategy_return": 0.03},
        ]

        result = _validate_candidate_outputs_not_identical(identical_rows, allow_smoke=True, monthly_rows=identical_monthly)
        self.assertEqual(result["status"], "warn")
        self.assertTrue(result["windows_identical"])
        self.assertTrue(result["monthly_identical"])


if __name__ == "__main__":
    unittest.main()
