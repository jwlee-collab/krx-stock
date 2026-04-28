#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean


def _read_csv(path: Path) -> list[dict[str, object]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _discover_latest_report_dir(base: Path) -> Path:
    manifests = sorted(base.glob("**/manifest.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not manifests:
        raise ValueError(f"manifest.json not found under {base}")
    return manifests[0].parent


def _build_candidate_diff_audit(window_rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], dict[str, object]]:
    metrics = ["total_return", "benchmark_return", "excess_return", "max_drawdown"]
    baseline = {
        (r["eval_frequency"], int(r["horizon_months"]), r["start_date"], r["end_date"]): r
        for r in window_rows
        if r["candidate"] == "baseline_old"
    }
    aggressive = {
        (r["eval_frequency"], int(r["horizon_months"]), r["start_date"], r["end_date"]): r
        for r in window_rows
        if r["candidate"] == "aggressive_hybrid_v4"
    }
    paired = sorted(set(baseline) & set(aggressive))
    rows: list[dict[str, object]] = []
    summary: dict[str, object] = {"paired_window_count": len(paired), "metrics": {}}
    for metric in metrics:
        diffs = [float(aggressive[k][metric]) - float(baseline[k][metric]) for k in paired]
        nz = sum(1 for d in diffs if abs(d) > 1e-12)
        summary["metrics"][metric] = {
            "candidate_diff_nonzero_count": nz,
            "candidate_diff_zero_count": len(diffs) - nz,
            "max_abs_candidate_diff": max((abs(d) for d in diffs), default=0.0),
            "mean_abs_candidate_diff": mean([abs(d) for d in diffs]) if diffs else 0.0,
        }
        by_combo: dict[tuple[str, int], list[float]] = defaultdict(list)
        for k, d in zip(paired, diffs):
            by_combo[(k[0], k[1])].append(d)
        for (freq, horizon), vals in sorted(by_combo.items()):
            nz_combo = sum(1 for d in vals if abs(d) > 1e-12)
            rows.append(
                {
                    "metric": metric,
                    "eval_frequency": freq,
                    "horizon_months": horizon,
                    "paired_windows": len(vals),
                    "candidate_diff_nonzero_count": nz_combo,
                    "candidate_diff_zero_count": len(vals) - nz_combo,
                    "max_abs_candidate_diff": max((abs(d) for d in vals), default=0.0),
                    "mean_abs_candidate_diff": mean([abs(d) for d in vals]) if vals else 0.0,
                }
            )
    return rows, summary


def _build_zero_metric_audit(window_rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], list[str]]:
    metrics = ["total_return", "benchmark_return", "excess_return", "max_drawdown"]
    grouped: dict[tuple[str, str, int], list[dict[str, object]]] = defaultdict(list)
    warnings: list[str] = []
    out: list[dict[str, object]] = []
    for row in window_rows:
        grouped[(row["candidate"], row["eval_frequency"], int(row["horizon_months"]))].append(row)
    for (candidate, freq, horizon), rows in sorted(grouped.items()):
        for metric in metrics:
            vals = [float(r[metric]) if r.get(metric) not in (None, "") else float("nan") for r in rows]
            nan_count = sum(1 for v in vals if math.isnan(v))
            valid = [v for v in vals if not math.isnan(v)]
            zero_count = sum(1 for v in valid if abs(v) <= 1e-12)
            severity = "INFO" if metric == "max_drawdown" else ("WARN" if zero_count > 0 or nan_count > 0 else "INFO")
            out.append(
                {
                    "candidate": candidate,
                    "eval_frequency": freq,
                    "horizon_months": horizon,
                    "metric": metric,
                    "n_windows": len(rows),
                    "zero_count": zero_count,
                    "zero_rate": zero_count / len(rows) if rows else 0.0,
                    "nan_count": nan_count,
                    "min": min(valid) if valid else None,
                    "max": max(valid) if valid else None,
                    "mean": mean(valid) if valid else None,
                    "severity": severity,
                }
            )
            if severity == "WARN":
                warnings.append(
                    f"zero metric warning: candidate={candidate} freq={freq} horizon={horizon} metric={metric} zero_count={zero_count} nan_count={nan_count}"
                )
    return out, warnings


def _build_benchmark_sanity_audit(monthly_rows: list[dict[str, object]], equity_rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], dict[str, object], list[str]]:
    rows: list[dict[str, object]] = []
    warnings: list[str] = []
    by_month: dict[str, dict[str, float]] = defaultdict(dict)
    for row in monthly_rows:
        by_month[row["month"]][row["candidate"]] = float(row["benchmark_return"])

    inconsistent_months = 0
    max_month_diff = 0.0
    for month, vals in sorted(by_month.items()):
        numbers = list(vals.values())
        diff = (max(numbers) - min(numbers)) if numbers else 0.0
        status = "PASS" if diff <= 1e-12 else "WARN"
        if status == "WARN":
            inconsistent_months += 1
        max_month_diff = max(max_month_diff, diff)
        rows.append({"check_name": "monthly_benchmark_consistency", "scope": month, "status": status, "value": diff, "details": f"candidate_count={len(numbers)}"})

    by_candidate_monthly: dict[str, list[float]] = defaultdict(list)
    for row in monthly_rows:
        by_candidate_monthly[row["candidate"]].append(float(row["benchmark_return"]))
    monthly_compounded: dict[str, float] = {}
    for candidate, rets in sorted(by_candidate_monthly.items()):
        eq = 1.0
        for r in rets:
            eq *= (1.0 + r)
        monthly_compounded[candidate] = eq - 1.0

    by_candidate_eq: dict[str, list[float]] = defaultdict(list)
    for row in equity_rows:
        by_candidate_eq[row["candidate"]].append(float(row["benchmark_equity"]))
    equity_compounded: dict[str, float] = {}
    for candidate, eqs in sorted(by_candidate_eq.items()):
        if len(eqs) >= 2 and eqs[0] != 0:
            equity_compounded[candidate] = (eqs[-1] - eqs[0]) / eqs[0]
        else:
            equity_compounded[candidate] = 0.0

    for candidate in sorted(set(monthly_compounded) | set(equity_compounded)):
        diff = abs(monthly_compounded.get(candidate, 0.0) - equity_compounded.get(candidate, 0.0))
        rows.append(
            {
                "check_name": "monthly_vs_equity_compounded_return",
                "scope": candidate,
                "status": "PASS" if diff <= 1e-6 else "WARN",
                "value": diff,
                "details": f"monthly={monthly_compounded.get(candidate, 0.0):.12g},equity={equity_compounded.get(candidate, 0.0):.12g}",
            }
        )
        if diff > 1e-6:
            warnings.append(f"benchmark monthly/equity mismatch for {candidate}: {diff:.6g}")

    if monthly_compounded and abs(next(iter(monthly_compounded.values()))) < 1e-6:
        warnings.append("benchmark compounded return is near zero.")

    summary = {
        "inconsistent_month_count": inconsistent_months,
        "max_monthly_candidate_diff": max_month_diff,
        "monthly_compounded_by_candidate": monthly_compounded,
        "equity_compounded_by_candidate": equity_compounded,
    }
    return rows, summary, warnings


def _build_run_id_readiness_audit(full_rows: list[dict[str, object]], window_rows: list[dict[str, object]], worst_rows: list[dict[str, object]]) -> tuple[list[dict[str, object]], dict[str, object]]:
    checks = []
    full_ok = all(bool(r.get("run_id")) for r in full_rows) if full_rows else False
    window_ok = all(bool(r.get("run_id")) for r in window_rows) if window_rows else False
    worst_ok = all(bool(r.get("run_id")) for r in worst_rows) if worst_rows else False
    checks.append({"dataset": "full_period_results", "row_count": len(full_rows), "run_id_available": full_ok})
    checks.append({"dataset": "window_results", "row_count": len(window_rows), "run_id_available": window_ok})
    checks.append({"dataset": "worst_windows", "row_count": len(worst_rows), "run_id_available": worst_ok})
    summary = {
        "full_period_run_ids": sorted({r.get("run_id") for r in full_rows if r.get("run_id")}),
        "window_run_id_available": window_ok,
        "worst_window_run_id_available": worst_ok,
    }
    return checks, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Standalone QA audit for final candidate report outputs")
    parser.add_argument("--report-dir", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    report_dir = Path(args.report_dir) if args.report_dir else _discover_latest_report_dir(Path("reports/final_candidate_report"))
    output_dir = Path(args.output_dir) if args.output_dir else report_dir / "quality_audit"
    if output_dir.exists() and any(output_dir.iterdir()) and not args.overwrite:
        raise ValueError(f"output_dir is not empty, use --overwrite: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = json.loads((report_dir / "manifest.json").read_text(encoding="utf-8"))
    candidate_summary_rows = _read_csv(report_dir / "candidate_summary.csv")
    window_rows = _read_csv(report_dir / "window_results.csv")
    monthly_rows = _read_csv(report_dir / "monthly_returns.csv")
    equity_rows = _read_csv(report_dir / "equity_curve.csv")
    drawdown_rows = _read_csv(report_dir / "drawdown_curve.csv")
    worst_rows = _read_csv(report_dir / "worst_windows.csv")
    full_rows_path = report_dir / "full_period_results.csv"
    full_rows = _read_csv(full_rows_path) if full_rows_path.exists() else []

    candidate_diff_rows, candidate_diff_summary = _build_candidate_diff_audit(window_rows)
    zero_metric_rows, zero_warnings = _build_zero_metric_audit(window_rows)
    benchmark_rows, benchmark_summary, benchmark_warnings = _build_benchmark_sanity_audit(monthly_rows, equity_rows)
    run_id_rows, run_id_summary = _build_run_id_readiness_audit(full_rows, window_rows, worst_rows)

    qa_flags = []
    qa_flags.extend({"category": "zero_metric", "flag": w, "severity": "WARN"} for w in zero_warnings)
    qa_flags.extend({"category": "benchmark", "flag": w, "severity": "WARN"} for w in benchmark_warnings)
    qa_flags.append({"category": "benchmark", "flag": "Additional benchmark stress needed: valid495 EW, rolling liquidity top100 EW, KOSPI, KOSPI200, random top5 Monte Carlo.", "severity": "WARN"})
    qa_flags.append({"category": "cost_stress", "flag": "Cost/slippage stress needed: 0/5/10/20/30 bps one-way.", "severity": "WARN"})

    _write_csv(output_dir / "qa_flags.csv", qa_flags, ["category", "flag", "severity"])
    _write_csv(output_dir / "candidate_diff_audit.csv", candidate_diff_rows, ["metric", "eval_frequency", "horizon_months", "paired_windows", "candidate_diff_nonzero_count", "candidate_diff_zero_count", "max_abs_candidate_diff", "mean_abs_candidate_diff"])
    _write_csv(output_dir / "zero_metric_audit.csv", zero_metric_rows, ["candidate", "eval_frequency", "horizon_months", "metric", "n_windows", "zero_count", "zero_rate", "nan_count", "min", "max", "mean", "severity"])
    _write_csv(output_dir / "benchmark_sanity_audit.csv", benchmark_rows, ["check_name", "scope", "status", "value", "details"])
    _write_csv(output_dir / "run_id_readiness_audit.csv", run_id_rows, ["dataset", "row_count", "run_id_available"])

    report_lines = [
        "# Final Candidate QA Audit Report",
        "",
        f"- generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        f"- report_dir: {report_dir}",
        f"- source_manifest: {report_dir / 'manifest.json'}",
        "",
        "## Candidate diff audit",
        "- This count measures candidate-to-candidate difference, not zero-return windows.",
    ]
    for row in candidate_diff_rows:
        report_lines.append(f"- {row['metric']} {row['eval_frequency']} h={row['horizon_months']}m paired={row['paired_windows']} nonzero={row['candidate_diff_nonzero_count']} zero={row['candidate_diff_zero_count']} max_abs={float(row['max_abs_candidate_diff']):.6g}")
    report_lines.extend(["", "## Zero metric audit"])
    for row in zero_metric_rows:
        report_lines.append(f"- [{row['severity']}] {row['candidate']} {row['eval_frequency']} h={row['horizon_months']}m {row['metric']}: zero={row['zero_count']}, nan={row['nan_count']}, mean={row['mean']}")
    report_lines.extend(["", "## Benchmark sanity"])
    for row in benchmark_rows:
        report_lines.append(f"- {row['check_name']} [{row['status']}] scope={row['scope']} value={row['value']} details={row['details']}")
    report_lines.extend([
        "",
        "## Run ID readiness",
        f"- full_period_run_ids: {', '.join(run_id_summary['full_period_run_ids']) if run_id_summary['full_period_run_ids'] else 'none'}",
        f"- window_run_id_available: {run_id_summary['window_run_id_available']}",
        f"- worst_window_run_id_available: {run_id_summary['worst_window_run_id_available']}",
        "",
        "## Assumptions and warnings",
        "- universe is current-valid KOSPI 495, historical delisted names may be excluded",
        "- survivorship bias may exist",
        "- benchmark is universe equal-weight proxy",
        "- stop loss execution assumption used by current backtest",
        "- transaction cost/slippage stress required: 0/5/10/20/30 bps one-way",
        "- rolling liquidity universe may introduce universe drift",
        "- not a live trading system, research/reporting use only",
    ])
    (output_dir / "qa_audit_report.md").write_text("\n".join(report_lines), encoding="utf-8")

    qa_manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "report_dir": str(report_dir),
        "output_dir": str(output_dir),
        "source_manifest_path": str(report_dir / "manifest.json"),
        "source_manifest_created_at": manifest.get("created_at"),
        "inputs": {
            "candidate_summary_rows": len(candidate_summary_rows),
            "window_rows": len(window_rows),
            "monthly_rows": len(monthly_rows),
            "equity_rows": len(equity_rows),
            "drawdown_rows": len(drawdown_rows),
            "worst_rows": len(worst_rows),
            "full_period_rows": len(full_rows),
        },
        "candidate_diff_summary": candidate_diff_summary,
        "benchmark_sanity_summary": benchmark_summary,
        "run_id_readiness_summary": run_id_summary,
        "qa_flag_count": len(qa_flags),
        "files": {
            "qa_flags_csv": str(output_dir / "qa_flags.csv"),
            "candidate_diff_csv": str(output_dir / "candidate_diff_audit.csv"),
            "zero_metric_csv": str(output_dir / "zero_metric_audit.csv"),
            "benchmark_sanity_csv": str(output_dir / "benchmark_sanity_audit.csv"),
            "run_id_readiness_csv": str(output_dir / "run_id_readiness_audit.csv"),
            "qa_audit_report_md": str(output_dir / "qa_audit_report.md"),
            "qa_audit_manifest_json": str(output_dir / "qa_audit_manifest.json"),
        },
    }
    (output_dir / "qa_audit_manifest.json").write_text(json.dumps(qa_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"FINAL_CANDIDATE_QA_JSON={json.dumps(qa_manifest, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
