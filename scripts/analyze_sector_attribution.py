#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_REPORT_ROOT = Path("reports/final_candidate_report")
DEFAULT_SECTOR_FILE = Path("data/kospi_sector_map.csv")

CANDIDATES = ["baseline_old", "aggressive_hybrid_v4"]


@dataclass(frozen=True)
class FocusWindow:
    candidate: str
    focus_start_date: str
    focus_end_date: str
    selection_reason: str
    run_id: str | None
    eval_frequency: str | None = None
    horizon_months: int | None = None
    worst_metric: str | None = None


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in fieldnames})


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _norm_symbol(value: Any) -> str:
    s = "" if value is None else str(value).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return digits.zfill(6)
    return s.zfill(6) if s else ""


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _discover_report_dir(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit)
        if not path.exists():
            raise ValueError(f"--report-dir not found: {path}")
        return path

    if not DEFAULT_REPORT_ROOT.exists():
        raise ValueError(f"report root not found: {DEFAULT_REPORT_ROOT}")

    required = ["manifest.json", "full_period_results.csv", "window_results.csv", "worst_windows.csv"]
    fallback_required = ["manifest.json", "window_results.csv", "worst_windows.csv"]
    candidates_primary: list[Path] = []
    candidates_fallback: list[Path] = []

    for manifest in DEFAULT_REPORT_ROOT.glob("**/manifest.json"):
        parent = manifest.parent
        if "quality_audit" in parent.parts:
            continue
        if all((parent / name).exists() for name in required):
            candidates_primary.append(parent)
        elif all((parent / name).exists() for name in fallback_required):
            candidates_fallback.append(parent)

    pool = candidates_primary or candidates_fallback
    if not pool:
        raise ValueError("no valid final_candidate_report directory found")

    pool.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return pool[0]


def _load_sector_map(path: Path | None) -> tuple[dict[str, dict[str, str]], dict[str, Any], list[str]]:
    warnings: list[str] = []
    status = {
        "provided": bool(path),
        "exists": bool(path and path.exists()),
        "symbol_column": None,
        "sector_column": None,
        "name_column": None,
        "mapped_symbols": 0,
    }
    if path is None or not path.exists():
        warnings.append("sector-file missing; all symbols assigned UNKNOWN")
        return {}, status, warnings

    rows = _read_csv(path)
    if not rows:
        warnings.append("sector-file is empty; all symbols assigned UNKNOWN")
        return {}, status, warnings

    lower_map = {k.lower(): k for k in rows[0].keys()}
    symbol_col = lower_map.get("symbol")
    sector_col = lower_map.get("sector")
    industry_col = lower_map.get("industry")
    name_col = lower_map.get("name")
    status["symbol_column"] = symbol_col
    status["sector_column"] = sector_col or industry_col
    status["name_column"] = name_col

    if symbol_col is None:
        warnings.append("sector-file lacks symbol column; all symbols assigned UNKNOWN")
        return {}, status, warnings

    if sector_col is None and industry_col is None:
        warnings.append("sector-file lacks sector/industry column; all symbols assigned UNKNOWN")

    result: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = _norm_symbol(row.get(symbol_col))
        if not symbol:
            continue
        sector = (row.get(sector_col) if sector_col else None) or (row.get(industry_col) if industry_col else None) or "UNKNOWN"
        name = (row.get(name_col) if name_col else None) or ""
        result[symbol] = {
            "sector": str(sector).strip() or "UNKNOWN",
            "name": str(name).strip(),
        }

    status["mapped_symbols"] = len(result)
    return result, status, warnings


def _required_artifact_paths(report_dir: Path) -> dict[str, Path]:
    names = [
        "manifest.json",
        "full_period_results.csv",
        "window_results.csv",
        "worst_windows.csv",
        "monthly_returns.csv",
        "equity_curve.csv",
        "drawdown_curve.csv",
    ]
    paths = {name: report_dir / name for name in names}
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise ValueError(f"report artifacts missing: {', '.join(missing)}")
    return paths


def _resolve_focus_window(
    candidate: str,
    worst_rows: list[dict[str, str]],
    window_rows: list[dict[str, str]],
    full_rows: list[dict[str, str]],
    full_start: str,
    full_end: str,
    focus_start: str | None,
    focus_end: str | None,
) -> FocusWindow:
    if focus_start and focus_end:
        return FocusWindow(
            candidate=candidate,
            focus_start_date=focus_start,
            focus_end_date=focus_end,
            selection_reason="cli_override",
            run_id=None,
        )

    priorities = [(12, "max_drawdown"), (12, "total_return"), (6, "max_drawdown")]
    candidate_worst = [r for r in worst_rows if r.get("candidate") == candidate]

    def _match_worst(horizon: int, metric: str) -> dict[str, str] | None:
        for row in candidate_worst:
            if int(row.get("horizon_months") or 0) == horizon and row.get("worst_metric") == metric:
                return row
        return None

    for horizon, metric in priorities:
        row = _match_worst(horizon, metric)
        if row:
            return FocusWindow(
                candidate=candidate,
                focus_start_date=str(row.get("start_date") or full_start),
                focus_end_date=str(row.get("end_date") or full_end),
                selection_reason=f"worst_windows_h{horizon}_{metric}",
                run_id=str(row["run_id"]) if row.get("run_id") else None,
                eval_frequency=row.get("eval_frequency"),
                horizon_months=horizon,
                worst_metric=metric,
            )

    full_row = next((r for r in full_rows if r.get("candidate") == candidate), None)
    return FocusWindow(
        candidate=candidate,
        focus_start_date=str(full_row.get("start_date") if full_row else full_start),
        focus_end_date=str(full_row.get("end_date") if full_row else full_end),
        selection_reason="fallback_full_period",
        run_id=str(full_row["run_id"]) if full_row and full_row.get("run_id") else None,
        eval_frequency="full",
        horizon_months=None,
        worst_metric=None,
    )


def _resolve_run_id_from_windows(focus: FocusWindow, window_rows: list[dict[str, str]]) -> str | None:
    if focus.run_id:
        return focus.run_id

    matching = [
        row
        for row in window_rows
        if row.get("candidate") == focus.candidate
        and row.get("start_date") == focus.focus_start_date
        and row.get("end_date") == focus.focus_end_date
    ]
    if focus.eval_frequency is not None:
        matching = [r for r in matching if r.get("eval_frequency") == focus.eval_frequency]
    if focus.horizon_months is not None:
        matching = [r for r in matching if int(r.get("horizon_months") or 0) == focus.horizon_months]

    for row in matching:
        if row.get("run_id"):
            return str(row["run_id"])
    return None


def _load_holdings(conn: sqlite3.Connection, run_id: str, start: str, end: str) -> list[dict[str, Any]]:
    cols = _table_columns(conn, "backtest_holdings")
    if not cols:
        return []

    has_unrealized = "unrealized_return" in cols
    unrealized_expr = "unrealized_return" if has_unrealized else "NULL AS unrealized_return"
    rows = conn.execute(
        f"""
        SELECT date, symbol, weight, {unrealized_expr}
        FROM backtest_holdings
        WHERE run_id=? AND date BETWEEN ? AND ?
        ORDER BY date, symbol
        """,
        (run_id, start, end),
    ).fetchall()

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "date": str(r["date"]),
                "symbol": _norm_symbol(r["symbol"]),
                "weight": _to_float(r["weight"]),
                "unrealized_return": _to_float(r["unrealized_return"]),
            }
        )
    return out


def _load_stop_loss_counts(conn: sqlite3.Connection, run_id: str, start: str, end: str) -> dict[str, int]:
    cols = _table_columns(conn, "backtest_risk_events")
    if not cols:
        return {}
    required = {"run_id", "date", "symbol", "event_type"}
    if not required.issubset(cols):
        return {}
    rows = conn.execute(
        """
        SELECT symbol, COUNT(1) AS cnt
        FROM backtest_risk_events
        WHERE run_id=? AND date BETWEEN ? AND ? AND LOWER(event_type) LIKE '%stop_loss%'
        GROUP BY symbol
        """,
        (run_id, start, end),
    ).fetchall()
    return {_norm_symbol(r["symbol"]): int(r["cnt"]) for r in rows}


def _aggregate_candidate(
    candidate: str,
    run_id: str,
    start: str,
    end: str,
    holdings: list[dict[str, Any]],
    sector_map: dict[str, dict[str, str]],
    stop_counts: dict[str, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, float]]:
    per_symbol: dict[str, dict[str, Any]] = {}
    per_date_sector_weight: dict[tuple[str, str], float] = defaultdict(float)
    per_date_sector_symbols: dict[tuple[str, str], set[str]] = defaultdict(set)

    for row in holdings:
        symbol = row["symbol"]
        meta = sector_map.get(symbol, {"sector": "UNKNOWN", "name": ""})
        sector = meta.get("sector", "UNKNOWN") or "UNKNOWN"
        date = row["date"]
        weight = row["weight"]
        uret = row["unrealized_return"]
        contrib = weight * uret

        key = (date, sector)
        per_date_sector_weight[key] += weight
        per_date_sector_symbols[key].add(symbol)

        rec = per_symbol.setdefault(
            symbol,
            {
                "candidate": candidate,
                "run_id": run_id,
                "focus_start_date": start,
                "focus_end_date": end,
                "symbol": symbol,
                "name": meta.get("name", ""),
                "sector": sector,
                "dates": [],
                "weights": [],
                "unrealized_returns": [],
                "estimated_contributions": [],
            },
        )
        rec["dates"].append(date)
        rec["weights"].append(weight)
        rec["unrealized_returns"].append(uret)
        rec["estimated_contributions"].append(contrib)

    symbol_rows: list[dict[str, Any]] = []
    sector_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for symbol, rec in per_symbol.items():
        dates = sorted(set(rec["dates"]))
        row = {
            "candidate": candidate,
            "run_id": run_id,
            "focus_start_date": start,
            "focus_end_date": end,
            "symbol": symbol,
            "name": rec["name"],
            "sector": rec["sector"],
            "holding_days": len(dates),
            "first_holding_date": dates[0] if dates else None,
            "last_holding_date": dates[-1] if dates else None,
            "avg_weight": _mean(rec["weights"]),
            "max_weight": max(rec["weights"]) if rec["weights"] else 0.0,
            "avg_unrealized_return": _mean(rec["unrealized_returns"]),
            "min_unrealized_return": min(rec["unrealized_returns"]) if rec["unrealized_returns"] else 0.0,
            "max_unrealized_return": max(rec["unrealized_returns"]) if rec["unrealized_returns"] else 0.0,
            "estimated_contribution": sum(rec["estimated_contributions"]),
            "stop_loss_count": stop_counts.get(symbol, 0),
        }
        symbol_rows.append(row)
        sector_group[row["sector"]].append(row)

    total_avg_weight = sum(sum(float(s["avg_weight"]) for s in group) for group in sector_group.values())
    sector_concentration_score_by_sector: dict[str, float] = {}
    if total_avg_weight > 0:
        for sector, group in sector_group.items():
            share = sum(float(s["avg_weight"]) for s in group) / total_avg_weight
            sector_concentration_score_by_sector[sector] = share * share
    else:
        for sector in sector_group:
            sector_concentration_score_by_sector[sector] = 0.0

    summary_rows: list[dict[str, Any]] = []
    for sector, group in sorted(sector_group.items()):
        contribs = [float(r["estimated_contribution"]) for r in group]
        negatives = [v for v in contribs if v < 0]
        positives = [v for v in contribs if v > 0]
        all_unrealized = [float(r["avg_unrealized_return"]) for r in group]
        summary_rows.append(
            {
                "candidate": candidate,
                "run_id": run_id,
                "focus_start_date": start,
                "focus_end_date": end,
                "sector": sector,
                "holding_days": sum(int(r["holding_days"]) for r in group),
                "symbol_count": len(group),
                "avg_weight": _mean([float(r["avg_weight"]) for r in group]),
                "max_weight": max(float(r["max_weight"]) for r in group),
                "avg_unrealized_return": _mean(all_unrealized),
                "min_unrealized_return": min(all_unrealized) if all_unrealized else 0.0,
                "max_unrealized_return": max(all_unrealized) if all_unrealized else 0.0,
                "estimated_contribution": sum(contribs),
                "negative_contribution_sum": sum(negatives),
                "positive_contribution_sum": sum(positives),
                "stop_loss_count": sum(int(r["stop_loss_count"]) for r in group),
                "sector_concentration_score": sector_concentration_score_by_sector[sector],
            }
        )

    daily_sector_exposure: list[dict[str, Any]] = []
    daily_totals: dict[str, float] = defaultdict(float)
    for (date, _sector), weight in per_date_sector_weight.items():
        daily_totals[date] += weight

    per_date_hhi: dict[str, float] = defaultdict(float)
    max_sector_weight = 0.0
    for (date, sector), weight in sorted(per_date_sector_weight.items()):
        denom = daily_totals[date]
        sector_weight = (weight / denom) if denom else 0.0
        max_sector_weight = max(max_sector_weight, sector_weight)
        per_date_hhi[date] += sector_weight * sector_weight
        daily_sector_exposure.append(
            {
                "date": date,
                "candidate": candidate,
                "run_id": run_id,
                "sector": sector,
                "sector_weight": sector_weight,
                "symbol_count": len(per_date_sector_symbols[(date, sector)]),
            }
        )

    candidate_metrics = {
        "max_sector_weight": max_sector_weight,
        "avg_sector_hhi": _mean(list(per_date_hhi.values())),
        "max_sector_hhi": max(per_date_hhi.values()) if per_date_hhi else 0.0,
        "daily_points": len(per_date_hhi),
    }
    return summary_rows, symbol_rows, daily_sector_exposure, candidate_metrics


def _full_period_run_ids(full_rows: list[dict[str, str]], manifest: dict[str, Any]) -> dict[str, str | None]:
    out = {candidate: None for candidate in CANDIDATES}
    for row in full_rows:
        candidate = row.get("candidate")
        if candidate in out and row.get("run_id"):
            out[candidate] = str(row["run_id"])
    manifest_map = manifest.get("full_period_run_ids") or {}
    for candidate in CANDIDATES:
        if out[candidate] is None and manifest_map.get(candidate):
            out[candidate] = str(manifest_map[candidate])
    return out


def _sector_comparison(
    summary_rows: list[dict[str, Any]],
    daily_rows: list[dict[str, Any]],
    focus_map: dict[str, FocusWindow],
) -> list[dict[str, Any]]:
    baseline = "baseline_old"
    aggressive = "aggressive_hybrid_v4"
    if baseline not in focus_map or aggressive not in focus_map:
        return []

    b_focus = focus_map[baseline]
    a_focus = focus_map[aggressive]

    overlap_start = max(b_focus.focus_start_date, a_focus.focus_start_date)
    overlap_end = min(b_focus.focus_end_date, a_focus.focus_end_date)
    overlap_available = overlap_start <= overlap_end

    by_candidate_sector: dict[tuple[str, str], dict[str, Any]] = {}
    for row in summary_rows:
        by_candidate_sector[(row["candidate"], row["sector"])] = row

    daily_hhi_by_candidate: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    daily_sector_weight: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in daily_rows:
        candidate = row["candidate"]
        date = row["date"]
        if overlap_available and not (overlap_start <= date <= overlap_end):
            continue
        sector = row["sector"]
        weight = float(row["sector_weight"])
        daily_hhi_by_candidate[candidate][date] += weight * weight
        daily_sector_weight[(candidate, sector)].append(weight)

    sectors = sorted({r["sector"] for r in summary_rows if r["candidate"] in {baseline, aggressive}})
    rows: list[dict[str, Any]] = []
    for sector in sectors:
        b_summary = by_candidate_sector.get((baseline, sector), {})
        a_summary = by_candidate_sector.get((aggressive, sector), {})
        b_weights = daily_sector_weight.get((baseline, sector), [])
        a_weights = daily_sector_weight.get((aggressive, sector), [])

        rows.append(
            {
                "sector": sector,
                "baseline_focus_start": b_focus.focus_start_date,
                "baseline_focus_end": b_focus.focus_end_date,
                "aggressive_focus_start": a_focus.focus_start_date,
                "aggressive_focus_end": a_focus.focus_end_date,
                "overlap_start": overlap_start if overlap_available else "",
                "overlap_end": overlap_end if overlap_available else "",
                "avg_sector_weight_delta": _mean(a_weights) - _mean(b_weights),
                "max_sector_weight_delta": (max(a_weights) if a_weights else 0.0) - (max(b_weights) if b_weights else 0.0),
                "estimated_contribution_delta": _to_float(a_summary.get("estimated_contribution")) - _to_float(b_summary.get("estimated_contribution")),
                "sector_hhi_delta": _mean(list(daily_hhi_by_candidate.get(aggressive, {}).values()))
                - _mean(list(daily_hhi_by_candidate.get(baseline, {}).values())),
            }
        )

    return rows


def _stress_summary(
    conn: sqlite3.Connection,
    full_run_ids: dict[str, str | None],
    selected_candidates: list[str],
    sector_map: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    periods = [
        ("2024-07", "2024-07-01", "2024-07-31"),
        ("2024-08", "2024-08-01", "2024-08-31"),
        ("2024-09", "2024-09-01", "2024-09-30"),
        ("2024-07_to_2024-09", "2024-07-01", "2024-09-30"),
        ("2024-07_to_2025-03", "2024-07-01", "2025-03-31"),
    ]

    rows: list[dict[str, Any]] = []
    for candidate in selected_candidates:
        run_id = full_run_ids.get(candidate)
        if not run_id:
            continue
        for label, start, end in periods:
            holdings = _load_holdings(conn, run_id, start, end)
            if not holdings:
                continue
            by_sector_weights: dict[str, list[float]] = defaultdict(list)
            by_sector_contrib: dict[str, float] = defaultdict(float)
            by_sector_symbols: dict[str, set[str]] = defaultdict(set)
            for row in holdings:
                symbol = row["symbol"]
                sector = sector_map.get(symbol, {}).get("sector", "UNKNOWN") or "UNKNOWN"
                by_sector_weights[sector].append(row["weight"])
                by_sector_contrib[sector] += row["weight"] * row["unrealized_return"]
                by_sector_symbols[sector].add(symbol)

            for sector, weights in sorted(by_sector_weights.items()):
                rows.append(
                    {
                        "candidate": candidate,
                        "period": label,
                        "sector": sector,
                        "avg_weight": _mean(weights),
                        "max_weight": max(weights) if weights else 0.0,
                        "estimated_contribution": by_sector_contrib[sector],
                        "symbol_count": len(by_sector_symbols[sector]),
                    }
                )
    return rows


def _build_report_markdown(
    args: argparse.Namespace,
    report_dir: Path,
    focus_rows: list[FocusWindow],
    summary_rows: list[dict[str, Any]],
    symbol_rows: list[dict[str, Any]],
    comparison_rows: list[dict[str, Any]],
    stress_rows: list[dict[str, Any]],
    sector_status: dict[str, Any],
    warnings: list[str],
    candidate_metrics: dict[str, dict[str, float]],
) -> str:
    lines: list[str] = [
        "# Sector Attribution Report",
        "",
        "## Summary",
        f"- generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        f"- report_dir: {report_dir}",
        f"- db: {args.db}",
        f"- candidate_scope: {args.candidate}",
        f"- attribution_note: 본 리포트의 estimated_contribution은 weight * unrealized_return의 진단용 근사치이며, 회계 P&L과 다를 수 있습니다.",
    ]
    for warning in warnings:
        lines.append(f"- warning: {warning}")

    lines += [
        "",
        "## Inputs and assumptions",
        "- primary_inputs: manifest.json, full_period_results.csv, window_results.csv, worst_windows.csv, monthly_returns.csv, equity_curve.csv, drawdown_curve.csv.",
        "- focus_window_selection: worst_windows 우선순위(12m MDD -> 12m total_return -> 6m MDD), 미충족 시 full period fallback.",
        "- contribution_formula: estimated_contribution = sum(weight * unrealized_return).",
        "- stop_loss_count: backtest_risk_events.event_type LIKE '%stop_loss%' 존재 시 집계.",
    ]

    lines += ["", "## Sector file status", f"- sector_file: {args.sector_file}"]
    for key in ["provided", "exists", "symbol_column", "sector_column", "name_column", "mapped_symbols"]:
        lines.append(f"- {key}: {sector_status.get(key)}")

    lines += ["", "## Focus windows selected"]
    for row in focus_rows:
        lines.append(
            f"- {row.candidate}: {row.focus_start_date} ~ {row.focus_end_date}, reason={row.selection_reason}, run_id={row.run_id}"
        )

    lines += ["", "## Sector concentration by candidate"]
    for candidate, metrics in candidate_metrics.items():
        lines.append(
            f"- {candidate}: max_sector_weight={metrics.get('max_sector_weight', 0.0):.4f}, avg_sector_hhi={metrics.get('avg_sector_hhi', 0.0):.4f}, max_sector_hhi={metrics.get('max_sector_hhi', 0.0):.4f}"
        )

    lines += ["", "## Loss contribution by sector"]
    for candidate in sorted({row["candidate"] for row in summary_rows}):
        lines.append(f"### {candidate}")
        worst = sorted([r for r in summary_rows if r["candidate"] == candidate], key=lambda x: float(x["estimated_contribution"]))[:8]
        if not worst:
            lines.append("- no rows")
            continue
        for row in worst:
            lines.append(
                f"- {row['sector']}: est_contrib={float(row['estimated_contribution']):.4f}, neg_sum={float(row['negative_contribution_sum']):.4f}, avg_weight={float(row['avg_weight']):.4f}"
            )

    lines += ["", "## Worst symbols by estimated contribution"]
    for candidate in sorted({row["candidate"] for row in symbol_rows}):
        lines.append(f"### {candidate}")
        worst_symbols = sorted(
            [row for row in symbol_rows if row["candidate"] == candidate],
            key=lambda x: float(x["estimated_contribution"]),
        )[: max(5, min(args.top_n, 10))]
        if not worst_symbols:
            lines.append("- no rows")
            continue
        for row in worst_symbols:
            lines.append(
                f"- {row['symbol']} ({row['sector']}): est_contrib={float(row['estimated_contribution']):.4f}, avg_weight={float(row['avg_weight']):.4f}, stop_loss={int(row['stop_loss_count'])}"
            )

    lines += ["", "## 2024 stress sector attribution"]
    if not stress_rows:
        lines.append("- no rows")
    else:
        for candidate in sorted({r["candidate"] for r in stress_rows}):
            lines.append(f"### {candidate}")
            subset = [r for r in stress_rows if r["candidate"] == candidate and r["period"] in {"2024-07", "2024-08", "2024-09"}]
            worst = sorted(subset, key=lambda x: float(x["estimated_contribution"]))[:8]
            for row in worst:
                lines.append(
                    f"- {row['period']} / {row['sector']}: est_contrib={float(row['estimated_contribution']):.4f}, avg_weight={float(row['avg_weight']):.4f}"
                )

    lines += ["", "## Baseline_old vs aggressive_hybrid_v4 sector difference"]
    if not comparison_rows:
        lines.append("- comparison unavailable (candidate scope or overlap 부족)")
    else:
        for row in sorted(comparison_rows, key=lambda x: float(x["estimated_contribution_delta"]))[:12]:
            lines.append(
                f"- {row['sector']}: avg_weight_delta={float(row['avg_sector_weight_delta']):+.4f}, max_weight_delta={float(row['max_sector_weight_delta']):+.4f}, est_contrib_delta={float(row['estimated_contribution_delta']):+.4f}, hhi_delta={float(row['sector_hhi_delta']):+.4f}"
            )

    lines += ["", "## Interpretation"]
    unknown_only = summary_rows and all(row["sector"] == "UNKNOWN" for row in summary_rows)
    if unknown_only:
        lines.append("- 모든 섹터가 UNKNOWN으로 분류되어 있어, 우선 sector mapping 데이터 확보가 필요합니다.")

    for candidate in sorted({row["candidate"] for row in symbol_rows}):
        top5 = sorted([r for r in symbol_rows if r["candidate"] == candidate], key=lambda x: float(x["estimated_contribution"]))[:5]
        sector_count: dict[str, int] = defaultdict(int)
        for row in top5:
            sector_count[str(row["sector"])] += 1
        concentrated = [sector for sector, cnt in sector_count.items() if cnt >= 3]
        for sector in concentrated:
            lines.append(f"- {candidate}: top5 worst symbols 중 {sector}가 3개 이상으로 집중 리스크가 큽니다.")

    for row in comparison_rows:
        if float(row["avg_sector_weight_delta"]) > 0 and float(row["estimated_contribution_delta"]) < 0:
            lines.append(
                f"- aggressive_hybrid_v4가 {row['sector']}에서 baseline_old 대비 집중도↑ 및 손실기여↑ 패턴을 보여 baseline 승격 보류 근거가 됩니다."
            )
            break

    baseline_loss = {r["sector"] for r in summary_rows if r["candidate"] == "baseline_old" and float(r["estimated_contribution"]) < 0}
    aggressive_loss = {
        r["sector"] for r in summary_rows if r["candidate"] == "aggressive_hybrid_v4" and float(r["estimated_contribution"]) < 0
    }
    shared_loss = sorted(baseline_loss & aggressive_loss)
    if shared_loss:
        lines.append(
            "- 두 후보 모두 동일 손실 섹터가 존재해 scoring 단독 이슈보다는 market regime / universe-liquidity 노출 영향 가능성이 있습니다."
        )

    dominant_loss = sorted(summary_rows, key=lambda x: float(x["estimated_contribution"]))
    if dominant_loss:
        lines.append(
            "- 손실 기여가 특정 섹터에 집중되면 sector hard exclude보다 sector cap 또는 sector soft penalty를 우선 검토하세요."
        )

    lines += [
        "",
        "## Next recommended experiments",
        "- 진단 결과 상위 손실 섹터를 기준으로 sector cap(soft cap 포함) A/B 실험.",
        "- ranking 점수에 sector soft penalty를 소규모로 도입한 민감도 테스트.",
        "- 공통 손실 섹터가 유지되면 regime filter 및 유동성/유니버스 재점검 실험.",
    ]

    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze sector attribution from final candidate report outputs")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--report-dir", default=None)
    parser.add_argument("--sector-file", default=str(DEFAULT_SECTOR_FILE))
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--candidate", choices=["baseline_old", "aggressive_hybrid_v4", "all"], default="all")
    parser.add_argument("--focus-start-date")
    parser.add_argument("--focus-end-date")
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    if bool(args.focus_start_date) != bool(args.focus_end_date):
        raise ValueError("--focus-start-date and --focus-end-date must be provided together")

    report_dir = _discover_report_dir(args.report_dir)
    paths = _required_artifact_paths(report_dir)

    manifest = json.loads(paths["manifest.json"].read_text(encoding="utf-8"))
    full_rows = _read_csv(paths["full_period_results.csv"])
    window_rows = _read_csv(paths["window_results.csv"])
    worst_rows = _read_csv(paths["worst_windows.csv"])
    _ = _read_csv(paths["monthly_returns.csv"])
    _ = _read_csv(paths["equity_curve.csv"])
    _ = _read_csv(paths["drawdown_curve.csv"])

    full_start = str(manifest.get("date_range", {}).get("start_date") or "")
    full_end = str(manifest.get("date_range", {}).get("end_date") or "")
    if not full_start or not full_end:
        if full_rows:
            starts = [r.get("start_date") for r in full_rows if r.get("start_date")]
            ends = [r.get("end_date") for r in full_rows if r.get("end_date")]
            full_start = min(starts) if starts else ""
            full_end = max(ends) if ends else ""
    if not full_start or not full_end:
        raise ValueError("failed to resolve full-period date range")

    selected_candidates = [args.candidate] if args.candidate != "all" else CANDIDATES
    output_dir = Path(args.output_dir) if args.output_dir else (report_dir / "sector_attribution")
    if output_dir.exists() and args.overwrite:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    sector_file = Path(args.sector_file) if args.sector_file else None
    sector_map, sector_status, warnings = _load_sector_map(sector_file)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    focus_by_candidate: dict[str, FocusWindow] = {}
    full_run_ids = _full_period_run_ids(full_rows, manifest)

    summary_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    candidate_metrics: dict[str, dict[str, float]] = {}

    for candidate in selected_candidates:
        focus = _resolve_focus_window(
            candidate,
            worst_rows,
            window_rows,
            full_rows,
            full_start,
            full_end,
            args.focus_start_date,
            args.focus_end_date,
        )
        run_id = _resolve_run_id_from_windows(focus, window_rows)
        if run_id is None and focus.selection_reason == "fallback_full_period":
            run_id = full_run_ids.get(candidate)
        focus = FocusWindow(
            candidate=focus.candidate,
            focus_start_date=focus.focus_start_date,
            focus_end_date=focus.focus_end_date,
            selection_reason=focus.selection_reason,
            run_id=run_id,
            eval_frequency=focus.eval_frequency,
            horizon_months=focus.horizon_months,
            worst_metric=focus.worst_metric,
        )
        focus_by_candidate[candidate] = focus

        if not run_id:
            warnings.append(
                f"run_id unresolved for {candidate} ({focus.focus_start_date}~{focus.focus_end_date}); candidate skipped"
            )
            continue

        holdings = _load_holdings(conn, run_id, focus.focus_start_date, focus.focus_end_date)
        if not holdings:
            warnings.append(f"no holdings rows for candidate={candidate}, run_id={run_id}")
            continue

        stop_counts = _load_stop_loss_counts(conn, run_id, focus.focus_start_date, focus.focus_end_date)
        c_summary, c_symbols, c_daily, c_metrics = _aggregate_candidate(
            candidate,
            run_id,
            focus.focus_start_date,
            focus.focus_end_date,
            holdings,
            sector_map,
            stop_counts,
        )
        summary_rows.extend(c_summary)
        symbol_rows.extend(c_symbols)
        daily_rows.extend(c_daily)
        candidate_metrics[candidate] = c_metrics

    unknown_symbol_ratio = 0.0
    if symbol_rows:
        unknown_symbol_ratio = sum(1 for row in symbol_rows if row["sector"] == "UNKNOWN") / len(symbol_rows)
        if unknown_symbol_ratio >= 0.5:
            warnings.append(f"UNKNOWN sector ratio is high ({unknown_symbol_ratio:.2%})")

    comparison_rows = _sector_comparison(summary_rows, daily_rows, focus_by_candidate)
    stress_rows = _stress_summary(conn, full_run_ids, selected_candidates, sector_map)

    focus_csv_rows = [
        {
            "candidate": focus.candidate,
            "run_id": focus.run_id,
            "focus_start_date": focus.focus_start_date,
            "focus_end_date": focus.focus_end_date,
            "selection_reason": focus.selection_reason,
            "eval_frequency": focus.eval_frequency,
            "horizon_months": focus.horizon_months,
            "worst_metric": focus.worst_metric,
        }
        for focus in focus_by_candidate.values()
    ]

    _write_csv(
        output_dir / "sector_focus_windows.csv",
        focus_csv_rows,
        [
            "candidate",
            "run_id",
            "focus_start_date",
            "focus_end_date",
            "selection_reason",
            "eval_frequency",
            "horizon_months",
            "worst_metric",
        ],
    )
    _write_csv(
        output_dir / "sector_attribution_summary.csv",
        summary_rows,
        [
            "candidate",
            "run_id",
            "focus_start_date",
            "focus_end_date",
            "sector",
            "holding_days",
            "symbol_count",
            "avg_weight",
            "max_weight",
            "avg_unrealized_return",
            "min_unrealized_return",
            "max_unrealized_return",
            "estimated_contribution",
            "negative_contribution_sum",
            "positive_contribution_sum",
            "stop_loss_count",
            "sector_concentration_score",
        ],
    )
    _write_csv(
        output_dir / "sector_symbol_attribution.csv",
        sorted(symbol_rows, key=lambda x: float(x["estimated_contribution"])),
        [
            "candidate",
            "run_id",
            "focus_start_date",
            "focus_end_date",
            "symbol",
            "name",
            "sector",
            "holding_days",
            "first_holding_date",
            "last_holding_date",
            "avg_weight",
            "max_weight",
            "avg_unrealized_return",
            "min_unrealized_return",
            "max_unrealized_return",
            "estimated_contribution",
            "stop_loss_count",
        ],
    )
    _write_csv(
        output_dir / "daily_sector_exposure.csv",
        daily_rows,
        ["date", "candidate", "run_id", "sector", "sector_weight", "symbol_count"],
    )
    _write_csv(
        output_dir / "sector_comparison.csv",
        comparison_rows,
        [
            "sector",
            "baseline_focus_start",
            "baseline_focus_end",
            "aggressive_focus_start",
            "aggressive_focus_end",
            "overlap_start",
            "overlap_end",
            "avg_sector_weight_delta",
            "max_sector_weight_delta",
            "estimated_contribution_delta",
            "sector_hhi_delta",
        ],
    )
    _write_csv(
        output_dir / "sector_2024_stress_summary.csv",
        stress_rows,
        ["candidate", "period", "sector", "avg_weight", "max_weight", "estimated_contribution", "symbol_count"],
    )

    report_text = _build_report_markdown(
        args,
        report_dir,
        list(focus_by_candidate.values()),
        summary_rows,
        symbol_rows,
        comparison_rows,
        stress_rows,
        sector_status,
        warnings,
        candidate_metrics,
    )
    report_path = output_dir / "sector_attribution_report.md"
    report_path.write_text(report_text, encoding="utf-8")

    manifest_path = output_dir / "sector_attribution_manifest.json"
    manifest_payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "db": args.db,
        "report_dir": str(report_dir),
        "sector_file": args.sector_file,
        "selected_candidates": selected_candidates,
        "focus_windows": focus_csv_rows,
        "full_period_run_ids": full_run_ids,
        "candidate_metrics": candidate_metrics,
        "warnings": warnings,
        "unknown_symbol_ratio": unknown_symbol_ratio,
        "output_files": {
            "sector_focus_windows": str(output_dir / "sector_focus_windows.csv"),
            "sector_attribution_summary": str(output_dir / "sector_attribution_summary.csv"),
            "sector_symbol_attribution": str(output_dir / "sector_symbol_attribution.csv"),
            "daily_sector_exposure": str(output_dir / "daily_sector_exposure.csv"),
            "sector_comparison": str(output_dir / "sector_comparison.csv"),
            "sector_2024_stress_summary": str(output_dir / "sector_2024_stress_summary.csv"),
            "sector_attribution_report": str(report_path),
            "sector_attribution_manifest": str(manifest_path),
        },
    }
    manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(manifest_payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
