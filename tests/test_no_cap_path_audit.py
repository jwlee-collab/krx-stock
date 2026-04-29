from __future__ import annotations

import argparse
import unittest

from scripts.run_broad_sector_guardrail_experiment import Candidate, _build_no_cap_path_audit


class TestNoCapPathAudit(unittest.TestCase):
    def _args(self) -> argparse.Namespace:
        return argparse.Namespace(
            universe_csv="/tmp/universe.csv",
            rolling_universe_top_n=500,
            start_date="2024-01-01",
            end_date="2024-12-31",
        )

    def test_clean_no_cap_pass_through_audit(self):
        audit = _build_no_cap_path_audit(
            no_cap_result={"rebalance_dates": [{"date": "2024-01-01"}]},
            no_cap_candidate=Candidate("baseline_old_no_sector_guardrail", "old", "none", None, None),
            args=self._args(),
        )
        self.assertTrue(audit["no_cap_path_audit_pass"])
        self.assertEqual(audit["no_cap_path_audit_fail_reasons"], [])

    def test_guardrail_applied_in_no_cap_fails_audit(self):
        audit = _build_no_cap_path_audit(
            no_cap_result={"rebalance_dates": []},
            no_cap_candidate=Candidate("bad_no_cap", "old", "hard_cap", 2, None),
            args=self._args(),
        )
        self.assertFalse(audit["no_cap_path_audit_pass"])
        self.assertIn("guardrail_selector_applied_in_no_cap", audit["no_cap_path_audit_fail_reasons"])
        self.assertIn("broad_sector_constraints_applied_in_no_cap", audit["no_cap_path_audit_fail_reasons"])

    def test_soft_penalty_path_level_suspect(self):
        audit = _build_no_cap_path_audit(
            no_cap_result={"rebalance_dates": []},
            no_cap_candidate=Candidate("bad_no_cap", "old", "none", None, 0.3),
            args=self._args(),
        )
        self.assertFalse(audit["no_cap_path_audit_pass"])
        self.assertIn("soft_penalty_applied_in_no_cap", audit["no_cap_path_audit_fail_reasons"])
        self.assertIn("soft_penalty_applied", audit["likely_path_level_suspects"])

    def test_missing_optional_fields_do_not_crash(self):
        args = argparse.Namespace(universe_csv=None, rolling_universe_top_n=300, start_date=None, end_date=None)
        audit = _build_no_cap_path_audit(
            no_cap_result={},
            no_cap_candidate=Candidate("baseline_old_no_sector_guardrail", "old", "none", None, None),
            args=args,
        )
        self.assertIn("config_snapshot", audit)
        self.assertTrue(audit["no_cap_path_audit_pass"])


if __name__ == "__main__":
    unittest.main()
