from __future__ import annotations

import unittest

from pipeline.db import get_connection, init_db
from scripts.generate_final_candidate_report import (
    CandidateConfig,
    _all_comparable_windows_identical,
    _restore_candidate_scores,
    _snapshot_candidate_scores,
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
        self.conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('AAA','2024-01-02',1.0,1)")
        self.conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('BBB','2024-01-02',0.9,2)")
        self.conn.commit()

        table_name, columns = _snapshot_candidate_scores(
            self.conn,
            CandidateConfig(name="baseline_old", scoring_profile="old"),
        )

        self.conn.execute("DELETE FROM daily_scores")
        self.conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('ZZZ','2024-01-03',5.0,1)")
        self.conn.commit()

        _restore_candidate_scores(self.conn, table_name, columns)
        rows = self.conn.execute("SELECT symbol, date, score, rank FROM daily_scores ORDER BY symbol").fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "AAA")
        self.assertEqual(rows[1]["symbol"], "BBB")

    def test_identical_window_guard_true_and_false(self) -> None:
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
        self.assertTrue(_all_comparable_windows_identical(identical_rows))

        different_rows = [dict(r) for r in identical_rows]
        different_rows[1]["total_return"] = 0.10001
        self.assertFalse(_all_comparable_windows_identical(different_rows))


if __name__ == "__main__":
    unittest.main()
