from __future__ import annotations

import unittest
from pathlib import Path

from scripts.daily_paper_trading_runner import _build_parser


class TestDailyPaperTradingRunnerPaths(unittest.TestCase):
    def test_no_hardcoded_colab_paths_in_runner(self):
        runner_path = Path("scripts/daily_paper_trading_runner.py")
        source = runner_path.read_text(encoding="utf-8")
        self.assertNotIn("/content/drive", source)
        self.assertNotIn("/content/krx-stock", source)

    def test_required_path_options_work_without_optional_storage_options(self):
        parser = _build_parser()
        args = parser.parse_args(
            [
                "--db",
                "/tmp/work.db",
                "--universe-file",
                "/tmp/universe.csv",
                "--reports-dir",
                "/tmp/reports",
            ]
        )
        self.assertEqual(args.db, "/tmp/work.db")
        self.assertEqual(args.universe_file, "/tmp/universe.csv")
        self.assertEqual(args.reports_dir, "/tmp/reports")
        self.assertIsNone(args.restore_db_from)
        self.assertIsNone(args.backup_db)
        self.assertIsNone(args.backup_reports_dir)


if __name__ == "__main__":
    unittest.main()
