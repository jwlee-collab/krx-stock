from __future__ import annotations

import unittest

from scripts.run_start_window_robustness import (
    _compute_production_candidate_score,
    _compute_production_guardrail_pass,
    _compute_risk_adjusted_score,
)


class StartWindowRiskScoringTest(unittest.TestCase):
    def test_high_return_but_deep_mdd_fails_production_guardrail(self) -> None:
        guardrail_pass = _compute_production_guardrail_pass(
            evaluated_windows=12,
            mean_excess_return=0.12,
            win_rate_vs_benchmark=0.55,
            worst_mdd=-0.42,
            worst_total_return=-0.34,
            p25_excess_return=-0.10,
        )
        self.assertEqual(guardrail_pass, 0)

        candidate_score = _compute_production_candidate_score(
            production_guardrail_pass=guardrail_pass,
            risk_adjusted_score=0.25,
            stability_score=0.18,
        )
        self.assertIsNotNone(candidate_score)
        assert candidate_score is not None
        self.assertLess(candidate_score, -900.0)

    def test_lower_return_but_lower_drawdown_gets_higher_risk_adjusted_score(self) -> None:
        aggressive = _compute_risk_adjusted_score(
            mean_total_return=0.32,
            mean_excess_return=0.20,
            mean_sharpe=1.1,
            worst_mdd=-0.40,
            worst_total_return=-0.33,
            p25_excess_return=-0.18,
            std_total_return=0.28,
            mdd_breach_count_30=5,
            evaluated_windows=12,
        )
        defensive = _compute_risk_adjusted_score(
            mean_total_return=0.22,
            mean_excess_return=0.14,
            mean_sharpe=0.95,
            worst_mdd=-0.22,
            worst_total_return=-0.16,
            p25_excess_return=-0.04,
            std_total_return=0.15,
            mdd_breach_count_30=0,
            evaluated_windows=12,
        )
        self.assertIsNotNone(aggressive)
        self.assertIsNotNone(defensive)
        assert aggressive is not None and defensive is not None
        self.assertGreater(defensive, aggressive)


if __name__ == "__main__":
    unittest.main()
