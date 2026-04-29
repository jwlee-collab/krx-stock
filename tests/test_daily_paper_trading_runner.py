import unittest
from unittest.mock import patch

from scripts import daily_paper_trading_runner as runner


class DailyPaperTradingRunnerTests(unittest.TestCase):
    def test_build_run_pipeline_command_includes_baseline_old_options(self):
        cmd = runner.build_run_pipeline_command("data/a.db", "u.csv")
        joined = " ".join(cmd)
        required = [
            "--source krx",
            "--universe-mode rolling_liquidity",
            "--universe-size 100",
            "--universe-lookback-days 20",
            "--scoring-version old",
            "--top-n 5",
            "--rebalance-frequency weekly",
            "--min-holding-days 10",
            "--keep-rank-threshold 9",
            "--enable-position-stop-loss",
            "--position-stop-loss-pct 0.10",
            "--stop-loss-cash-mode keep_cash",
            "--stop-loss-cooldown-days 0",
        ]
        for opt in required:
            self.assertIn(opt, joined)

    @patch("scripts.daily_paper_trading_runner.subprocess.run")
    def test_skip_update_does_not_call_run_pipeline(self, mock_run):
        with patch("scripts.daily_paper_trading_runner._latest_backtest_run") as latest, patch(
            "scripts.daily_paper_trading_runner.sqlite3.connect"
        ) as conn:
            latest.return_value = {"config_json": "{}"}
            db_mock = conn.return_value
            db_mock.execute.return_value.fetchone.return_value = object()
            with patch("scripts.daily_paper_trading_runner.verify_baseline_old_guardrails"):
                with patch("sys.argv", ["x", "--db", "d.db", "--reports-dir", "r", "--skip-update"]):
                    runner.main()
        mock_run.assert_not_called()

    def test_guardrail_fails_on_wrong_weight(self):
        bad = {
            "scoring_profile": "old",
            "universe_mode": "rolling_liquidity",
            "top_n": 5,
            "rebalance_frequency": "weekly",
            "min_holding_days": 10,
            "keep_rank_threshold": 9,
            "enable_position_stop_loss": True,
            "position_stop_loss_pct": 0.10,
            "stop_loss_cash_mode": "keep_cash",
            "max_single_position_weight": 0.3333,
        }
        with self.assertRaises(RuntimeError):
            runner.verify_baseline_old_guardrails(bad)

    def test_guardrail_fails_on_daily_rebalance_and_disabled_stop_loss(self):
        bad = {
            "scoring_profile": "old",
            "universe_mode": "rolling_liquidity",
            "top_n": 5,
            "rebalance_frequency": "daily",
            "min_holding_days": 5,
            "keep_rank_threshold": 9,
            "enable_position_stop_loss": False,
            "position_stop_loss_pct": 0.10,
            "stop_loss_cash_mode": "keep_cash",
            "max_single_position_weight": 0.2,
        }
        with self.assertRaises(RuntimeError):
            runner.verify_baseline_old_guardrails(bad)


if __name__ == "__main__":
    unittest.main()
