from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path

from scripts.run_broad_sector_guardrail_experiment import (
    _build_no_cap_path_audit,
    _resolve_sector_map_for_run,
    Candidate,
)


class TestNoCapSectorFileRequirements(unittest.TestCase):
    def _args(self, **overrides) -> argparse.Namespace:
        base = {
            "parity_only": True,
            "only_no_cap": True,
            "sector_file": None,
            "universe_csv": None,
            "rolling_universe_top_n": 500,
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
        }
        base.update(overrides)
        return argparse.Namespace(**base)

    def test_no_cap_diagnostic_only_without_sector_file(self):
        symbol_to_sector, _unknown_ratio, meta, _warnings = _resolve_sector_map_for_run(self._args(sector_file="/tmp/missing_sector_map.csv"))
        self.assertEqual(symbol_to_sector, {})
        self.assertFalse(meta["sector_file_required"])
        self.assertEqual(meta["sector_file_status"], "unavailable_not_required_for_no_cap")
        self.assertFalse(meta["sector_mapping_used_for_selection"])

    def test_cap_candidate_mode_without_sector_file_fails_fast(self):
        with self.assertRaises(FileNotFoundError) as ctx:
            _resolve_sector_map_for_run(self._args(parity_only=False, only_no_cap=False, sector_file="/tmp/missing_sector_map.csv"))
        self.assertIn("sector file is required for cap/soft-penalty guardrail candidates", str(ctx.exception))

    def test_no_cap_diagnostic_only_with_existing_sector_file_not_used(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "sector.csv"
            p.write_text("symbol,broad_sector\n005930,IT\n", encoding="utf-8")
            symbol_to_sector, _unknown_ratio, meta, _warnings = _resolve_sector_map_for_run(self._args(sector_file=str(p)))
        self.assertEqual(symbol_to_sector, {})
        self.assertFalse(meta["sector_file_required"])
        self.assertEqual(meta["sector_file_status"], "available_not_used_for_no_cap")
        self.assertFalse(meta["sector_file_used_for_no_cap"])
        self.assertFalse(meta["sector_mapping_used_for_selection"])

    def test_no_cap_path_audit_clean_when_sector_file_missing(self):
        _symbol_to_sector, _unknown_ratio, meta, _warnings = _resolve_sector_map_for_run(self._args(sector_file="/tmp/missing_sector_map.csv"))
        audit = _build_no_cap_path_audit(
            no_cap_result={"rebalance_dates": [{"date": "2024-01-01"}]},
            no_cap_candidate=Candidate("baseline_old_no_sector_guardrail", "old", "none", None, None),
            args=self._args(),
        )
        self.assertFalse(meta["sector_mapping_used_for_selection"])
        self.assertFalse(audit["selector_path"]["sector_mapping_used_for_selection"])
        self.assertFalse(audit["selector_path"]["symbol_to_sector_affects_selection"])
        self.assertFalse(audit["guardrail_flags"]["broad_sector_constraints_applied"])
        self.assertFalse(audit["guardrail_flags"]["soft_penalty_applied"])
        self.assertTrue(audit["no_cap_path_audit_pass"])


if __name__ == "__main__":
    unittest.main()
