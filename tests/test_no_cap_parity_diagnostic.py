from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.run_broad_sector_guardrail_experiment import _first_divergence_diagnostic


class TestNoCapParityDiagnostic(unittest.TestCase):
    def _write(self, path: Path, header: str, rows: list[str]) -> None:
        path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")

    def _base_call(self, ref: Path, out: Path, parity_pass: bool = True, holdings=None, daily=None):
        return _first_divergence_diagnostic(
            reference_dir=ref,
            outdir=out,
            no_cap_daily_rows=daily or [{"date": "2024-01-01", "equity": 100.0}, {"date": "2024-01-02", "equity": 101.0}],
            no_cap_holdings_rows=holdings or [{"date": "2024-01-01", "symbol": "AAA"}],
            no_cap_trade_rows=[],
            no_cap_stop_loss_rows=[],
            no_cap_rebalance_dates=[],
            no_cap_window_rows=[],
            no_cap_monthly_rows=[],
            no_cap_full_row={},
            parity_pass=parity_pass,
        )

    def test_perfect_parity(self):
        with tempfile.TemporaryDirectory() as d:
            ref = Path(d) / "ref"; out = Path(d) / "out"; ref.mkdir(); out.mkdir()
            self._write(ref / "baseline_old_equity_curve.csv", "date,equity", ["2024-01-01,100", "2024-01-02,101"])
            self._write(ref / "baseline_old_holdings.csv", "date,symbol", ["2024-01-01,AAA"])
            result = self._base_call(ref, out, parity_pass=True)
            self.assertTrue(result["parity_pass"])
            self.assertIsNone(result["first_mismatch_scope"])
            self.assertTrue(result["cap_interpretation_allowed"])

    def test_equity_first_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            ref = Path(d) / "ref"; out = Path(d) / "out"; ref.mkdir(); out.mkdir()
            self._write(ref / "baseline_old_equity_curve.csv", "date,equity", ["2024-01-01,100", "2024-01-02,101"])
            self._write(ref / "baseline_old_holdings.csv", "date,symbol", ["2024-01-01,AAA"])
            daily = [{"date": "2024-01-01", "equity": 100.0}, {"date": "2024-01-02", "equity": 103.0}]
            result = self._base_call(ref, out, parity_pass=False, daily=daily)
            self.assertEqual(result["first_mismatch_scope"], "equity_curve")
            self.assertEqual(result["first_mismatch_date"], "2024-01-02")
            self.assertFalse(result["decision_usable"])

    def test_holdings_first_mismatch(self):
        with tempfile.TemporaryDirectory() as d:
            ref = Path(d) / "ref"; out = Path(d) / "out"; ref.mkdir(); out.mkdir()
            self._write(ref / "baseline_old_equity_curve.csv", "date,equity", ["2024-01-01,100", "2024-01-02,101"])
            self._write(ref / "baseline_old_holdings.csv", "date,symbol", ["2024-01-01,AAA"])
            holdings = [{"date": "2024-01-01", "symbol": "BBB"}]
            result = self._base_call(ref, out, parity_pass=False, holdings=holdings)
            self.assertEqual(result["first_mismatch_scope"], "holdings")
            self.assertEqual(result["first_mismatch_date"], "2024-01-01")
            self.assertIn("selection", result["likely_cause_hint"])

    def test_missing_trades_not_failure(self):
        with tempfile.TemporaryDirectory() as d:
            ref = Path(d) / "ref"; out = Path(d) / "out"; ref.mkdir(); out.mkdir()
            self._write(ref / "baseline_old_equity_curve.csv", "date,equity", ["2024-01-01,100", "2024-01-02,101"])
            self._write(ref / "baseline_old_holdings.csv", "date,symbol", ["2024-01-01,AAA"])
            result = self._base_call(ref, out, parity_pass=True)
            self.assertIn("trades_comparison_unavailable_not_parity_failure", result["unavailable_artifacts"])
            self.assertTrue(result["parity_pass"])


if __name__ == "__main__":
    unittest.main()
