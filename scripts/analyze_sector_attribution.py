#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CANDIDATE_TO_PROFILE = {
    "baseline_old": "old",
    "aggressive_hybrid_v4": "hybrid_v4",
}


def _norm_symbol(value: Any) -> str:
    s = "" if value is None else str(value).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return digits.zfill(6)
    return s.zfill(6) if s else ""


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return rows


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _mean(values: list[float]) -> float | None:
    return (sum(values) / len(values)) if values else None


def _load_sector_map(sector_file: Path | None) -> tuple[dict[str, dict[str, str]], list[str]]:
    warnings: list[str] = []
    if sector_file is None or not sector_file.exists():
        warnings.append("sector-file unavailable; all symbols mapped to UNKNOWN")
        return {}, warnings

    rows = _read_csv(sector_file)
    if not rows:
        warnings.append(f"sector-file has no rows: {sector_file}")
        return {}, warnings

    keys = {k.lower(): k for k in rows[0].keys()}
    if "symbol" not in keys:
        warnings.append(f"sector-file missing symbol column: {sector_file}")
        return {}, warnings

    sector_key = keys.get("sector")
    industry_key = keys.get("industry")
    name_key = keys.get("name")
    if sector_key is None and industry_key is None:
        warnings.append("sector-file missing both sector and industry; using UNKNOWN")

    out: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = _norm_symbol(row.get(keys["symbol"]))
        if not symbol:
            continue
        sector_val = (row.get(sector_key) if sector_key else None) or (row.get(industry_key) if industry_key else None) or "UNKNOWN"
        name_val = (row.get(name_key) if name_key else None) or ""
        out[symbol] = {"sector": str(sector_val).strip() or "UNKNOWN", "name": str(name_val).strip()}
    return out, warnings


def _pick_focus_window(
    candidate: str,
    worst_rows: list[dict[str, str]],
    full_start: str,
    full_end: str,
    focus_start: str | None,
    focus_end: str | None,
) -> dict[str, str]:
    if focus_start and focus_end:
        return {
            "candidate": candidate,
            "focus_start_date": focus_start,
            "focus_end_date": focus_end,
            "selection_reason": "cli_override",
        }

    c_rows = [r for r in worst_rows if r.get("candidate") == candidate]
    priorities = [
        (12, "max_drawdown"),
        (12, "total_return"),
        (6, "max_drawdown"),
    ]
    for horizon, metric in priorities:
        for row in c_rows:
            if int(row.get("horizon_months") or 0) == horizon and row.get("worst_metric") == metric:
                return {
                    "candidate": candidate,
                    "focus_start_date": str(row["start_date"]),
                    "focus_end_date": str(row["end_date"]),
                    "selection_reason": f"worst_windows_h{horizon}_{metric}",
                }

    return {
        "candidate": candidate,
        "focus_start_date": full_start,
        "focus_end_date": full_end,
        "selection_reason": "fallback_full_period",
    }


def _resolve_run_id(
    conn: sqlite3.Connection,
    candidate: str,
    start_date: str,
    end_date: str,
    window_rows: list[dict[str, str]],
) -> str | None:
    candidates = [
        r.get("run_id")
        for r in window_rows
        if r.get("candidate") == candidate and r.get("start_date") == start_date and r.get("end_date") == end_date and r.get("run_id")
    ]
    if candidates:
        return str(candidates[0])

    scoring_profile = CANDIDATE_TO_PROFILE[candidate]
    row = conn.execute(
        """
        SELECT run_id
        FROM backtest_runs
        WHERE start_date=? AND end_date=? AND scoring_profile=?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (start_date, end_date, scoring_profile),
    ).fetchone()
    return str(row["run_id"]) if row else None


def _load_full_period_run_ids(
    conn: sqlite3.Connection,
    report_dir: Path,
    full_start: str,
    full_end: str,
    manifest: dict[str, Any],
    full_period_rows: list[dict[str, str]],
) -> dict[str, str | None]:
    out: dict[str, str | None] = {c: None for c in CANDIDATE_TO_PROFILE}
    map_from_manifest = manifest.get("full_period_run_ids") or {}
    for c in out:
        if map_from_manifest.get(c):
            out[c] = str(map_from_manifest[c])

    for row in full_period_rows:
        c = row.get("candidate")
        if c in out and row.get("run_id"):
            out[c] = str(row["run_id"])

    for c in out:
        if out[c]:
            continue
        prof = CANDIDATE_TO_PROFILE[c]
        db_row = conn.execute(
            "SELECT run_id FROM backtest_runs WHERE start_date=? AND end_date=? AND scoring_profile=? ORDER BY created_at DESC LIMIT 1",
            (full_start, full_end, prof),
        ).fetchone()
        if db_row:
            out[c] = str(db_row["run_id"])
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Analyze sector attribution for final candidate report windows")
    p.add_argument("--db", required=True)
    p.add_argument("--report-dir", required=True)
    p.add_argument("--sector-file", default="data/kospi_sector_map.csv")
    p.add_argument("--output-dir", default=None)
    p.add_argument("--candidate", choices=["baseline_old", "aggressive_hybrid_v4", "all"], default="all")
    p.add_argument("--focus-start-date")
    p.add_argument("--focus-end-date")
    p.add_argument("--top-n", type=int, default=30)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()

    report_dir = Path(args.report_dir)
    required = [
        "manifest.json",
        "window_results.csv",
        "worst_windows.csv",
        "monthly_returns.csv",
        "equity_curve.csv",
        "drawdown_curve.csv",
    ]
    for name in required:
        fp = report_dir / name
        if not fp.exists():
            raise ValueError(f"missing report artifact: {fp}")

    manifest = json.loads((report_dir / "manifest.json").read_text(encoding="utf-8"))
    window_rows = _read_csv(report_dir / "window_results.csv")
    worst_rows = _read_csv(report_dir / "worst_windows.csv")
    _ = _read_csv(report_dir / "monthly_returns.csv")
    _ = _read_csv(report_dir / "equity_curve.csv")
    _ = _read_csv(report_dir / "drawdown_curve.csv")

    full_period_rows: list[dict[str, str]] = []
    full_period_csv = report_dir / "full_period_results.csv"
    if full_period_csv.exists():
        full_period_rows = _read_csv(full_period_csv)

    full_start = str(manifest.get("date_range", {}).get("start_date") or "")
    full_end = str(manifest.get("date_range", {}).get("end_date") or "")
    if not full_start or not full_end:
        dates = sorted({r["date"] for r in _read_csv(report_dir / "equity_curve.csv")})
        if not dates:
            raise ValueError("cannot infer full period dates")
        full_start, full_end = dates[0], dates[-1]

    output_dir = Path(args.output_dir) if args.output_dir else report_dir / "sector_attribution"
    if output_dir.exists() and args.overwrite:
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    selected_candidates = [args.candidate] if args.candidate != "all" else list(CANDIDATE_TO_PROFILE.keys())
    if (args.focus_start_date and not args.focus_end_date) or (args.focus_end_date and not args.focus_start_date):
        raise ValueError("focus-start-date and focus-end-date must be provided together")

    sector_map, warnings = _load_sector_map(Path(args.sector_file) if args.sector_file else None)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    full_period_run_ids = _load_full_period_run_ids(conn, report_dir, full_start, full_end, manifest, full_period_rows)

    focus_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    symbol_rows: list[dict[str, Any]] = []

    for candidate in selected_candidates:
        focus = _pick_focus_window(candidate, worst_rows, full_start, full_end, args.focus_start_date, args.focus_end_date)
        run_id = _resolve_run_id(conn, candidate, focus["focus_start_date"], focus["focus_end_date"], window_rows)
        if run_id is None and focus["focus_start_date"] == full_start and focus["focus_end_date"] == full_end:
            run_id = full_period_run_ids.get(candidate)
        focus["run_id"] = run_id
        focus_rows.append(focus)
        if run_id is None:
            warnings.append(f"run_id unresolved for {candidate} {focus['focus_start_date']}~{focus['focus_end_date']}")
            continue

        h_rows = conn.execute(
            """
            SELECT date, symbol, weight, unrealized_return
            FROM backtest_holdings
            WHERE run_id=? AND date BETWEEN ? AND ?
            ORDER BY date, symbol
            """,
            (run_id, focus["focus_start_date"], focus["focus_end_date"]),
        ).fetchall()
        stop_rows = conn.execute(
            """
            SELECT date, symbol, event_type
            FROM backtest_risk_events
            WHERE run_id=? AND date BETWEEN ? AND ? AND event_type LIKE '%stop_loss%'
            """,
            (run_id, focus["focus_start_date"], focus["focus_end_date"]),
        ).fetchall()
        stop_by_symbol: dict[str, int] = defaultdict(int)
        for sr in stop_rows:
            sym = _norm_symbol(sr["symbol"])
            if sym:
                stop_by_symbol[sym] += 1

        per_symbol: dict[str, dict[str, Any]] = {}
        for r in h_rows:
            sym = _norm_symbol(r["symbol"])
            meta = sector_map.get(sym, {"sector": "UNKNOWN", "name": ""})
            d = str(r["date"])
            w = float(r["weight"])
            ur = float(r["unrealized_return"]) if r["unrealized_return"] is not None else 0.0
            contrib = w * ur

            rec = per_symbol.setdefault(
                sym,
                {
                    "candidate": candidate,
                    "run_id": run_id,
                    "symbol": sym,
                    "name": meta.get("name", ""),
                    "sector": meta.get("sector", "UNKNOWN") or "UNKNOWN",
                    "dates": [],
                    "weights": [],
                    "unrealized": [],
                    "contribs": [],
                },
            )
            rec["dates"].append(d)
            rec["weights"].append(w)
            rec["unrealized"].append(ur)
            rec["contribs"].append(contrib)

        per_sector: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for sym, rec in per_symbol.items():
            dates = sorted(set(rec["dates"]))
            out = {
                "candidate": candidate,
                "run_id": run_id,
                "focus_start_date": focus["focus_start_date"],
                "focus_end_date": focus["focus_end_date"],
                "symbol": sym,
                "name": rec["name"],
                "sector": rec["sector"],
                "holding_days": len(dates),
                "avg_weight": _mean(rec["weights"]) or 0.0,
                "max_weight": max(rec["weights"]) if rec["weights"] else 0.0,
                "entry_date": dates[0] if dates else None,
                "exit_date": dates[-1] if dates else None,
                "avg_unrealized_return": _mean(rec["unrealized"]) or 0.0,
                "min_unrealized_return": min(rec["unrealized"]) if rec["unrealized"] else 0.0,
                "max_unrealized_return": max(rec["unrealized"]) if rec["unrealized"] else 0.0,
                "estimated_contribution": sum(rec["contribs"]),
                "stop_loss_count": stop_by_symbol.get(sym, 0),
            }
            symbol_rows.append(out)
            per_sector[out["sector"]].append(out)

        sector_avg_weight_sum = sum(sum(float(x["avg_weight"]) for x in grp) for grp in per_sector.values())
        hhi = 0.0
        if sector_avg_weight_sum > 0:
            for grp in per_sector.values():
                share = sum(float(x["avg_weight"]) for x in grp) / sector_avg_weight_sum
                hhi += share * share

        for sector, grp in sorted(per_sector.items()):
            unrealized_vals = [float(x["avg_unrealized_return"]) for x in grp]
            contribs = [float(x["estimated_contribution"]) for x in grp]
            losses = [c for c in contribs if c < 0]
            gains = [c for c in contribs if c > 0]
            worst_symbols = sorted(grp, key=lambda x: float(x["estimated_contribution"]))[: args.top_n]
            summary_rows.append(
                {
                    "candidate": candidate,
                    "run_id": run_id,
                    "focus_start_date": focus["focus_start_date"],
                    "focus_end_date": focus["focus_end_date"],
                    "sector": sector,
                    "avg_weight": _mean([float(x["avg_weight"]) for x in grp]) or 0.0,
                    "max_weight": max(float(x["max_weight"]) for x in grp) if grp else 0.0,
                    "holding_days": sum(int(x["holding_days"]) for x in grp),
                    "symbol_count": len(grp),
                    "avg_unrealized_return": _mean(unrealized_vals) or 0.0,
                    "min_unrealized_return": min(unrealized_vals) if unrealized_vals else 0.0,
                    "max_unrealized_return": max(unrealized_vals) if unrealized_vals else 0.0,
                    "estimated_loss_contribution": sum(losses) if losses else 0.0,
                    "estimated_return_contribution": sum(gains) if gains else 0.0,
                    "stop_loss_count": sum(int(x["stop_loss_count"]) for x in grp),
                    "sector_concentration_score": hhi,
                    "worst_symbols_top_n": "|".join(x["symbol"] for x in worst_symbols),
                }
            )

    summary_path = output_dir / "sector_attribution_summary.csv"
    symbols_path = output_dir / "sector_symbol_attribution.csv"
    focus_path = output_dir / "sector_focus_windows.csv"
    report_path = output_dir / "sector_attribution_report.md"
    manifest_path = output_dir / "sector_attribution_manifest.json"

    _write_csv(
        summary_path,
        summary_rows,
        [
            "candidate", "run_id", "focus_start_date", "focus_end_date", "sector", "avg_weight", "max_weight", "holding_days",
            "symbol_count", "avg_unrealized_return", "min_unrealized_return", "max_unrealized_return",
            "estimated_loss_contribution", "estimated_return_contribution", "stop_loss_count", "sector_concentration_score", "worst_symbols_top_n",
        ],
    )
    _write_csv(
        symbols_path,
        symbol_rows,
        [
            "candidate", "run_id", "focus_start_date", "focus_end_date", "symbol", "name", "sector", "holding_days", "avg_weight", "max_weight",
            "entry_date", "exit_date", "avg_unrealized_return", "min_unrealized_return", "max_unrealized_return", "estimated_contribution", "stop_loss_count",
        ],
    )
    _write_csv(
        focus_path,
        focus_rows,
        ["candidate", "focus_start_date", "focus_end_date", "selection_reason", "run_id"],
    )

    def _top_loss_lines(candidate: str, n: int = 5) -> list[str]:
        rows = [r for r in summary_rows if r["candidate"] == candidate]
        rows = sorted(rows, key=lambda x: float(x["estimated_loss_contribution"]))[:n]
        return [f"- {r['sector']}: loss_contrib={float(r['estimated_loss_contribution']):.4f}, stop_loss_count={r['stop_loss_count']}" for r in rows]

    diff_lines: list[str] = []
    b = {r["sector"]: r for r in summary_rows if r["candidate"] == "baseline_old"}
    a = {r["sector"]: r for r in summary_rows if r["candidate"] == "aggressive_hybrid_v4"}
    for sector in sorted(set(b) | set(a)):
        bw = float(b.get(sector, {}).get("avg_weight", 0.0))
        aw = float(a.get(sector, {}).get("avg_weight", 0.0))
        diff_lines.append(f"- {sector}: avg_weight_diff(aggressive-baseline)={(aw-bw):+.4f}")

    unknown_ratio = 0.0
    if summary_rows:
        unknown_weight = sum(float(r["avg_weight"]) for r in summary_rows if r["sector"] == "UNKNOWN")
        total_weight = sum(float(r["avg_weight"]) for r in summary_rows)
        unknown_ratio = (unknown_weight / total_weight) if total_weight else 0.0

    interpretation = []
    if unknown_ratio > 0.2:
        interpretation.append("- UNKNOWN 섹터 비중이 높아(>20%) sector 데이터 보강이 우선입니다.")
    shared_loss = set(r["sector"] for r in summary_rows if float(r["estimated_loss_contribution"]) < 0 and r["candidate"] == "baseline_old") & set(
        r["sector"] for r in summary_rows if float(r["estimated_loss_contribution"]) < 0 and r["candidate"] == "aggressive_hybrid_v4"
    )
    if shared_loss:
        interpretation.append("- 두 후보가 동일 섹터에서 동시 손실을 보이면 scoring 이슈보다 regime/market exposure 영향 가능성이 큽니다.")
    if b and a:
        b_hhi = max(float(r["sector_concentration_score"]) for r in summary_rows if r["candidate"] == "baseline_old")
        a_hhi = max(float(r["sector_concentration_score"]) for r in summary_rows if r["candidate"] == "aggressive_hybrid_v4")
        if a_hhi > b_hhi + 0.03:
            interpretation.append("- aggressive_hybrid_v4의 섹터 집중도가 baseline_old 대비 높아 baseline 승격 보류 근거가 됩니다.")
    if not interpretation:
        interpretation.append("- 특정 섹터 편중 손실이 확인되면 hard exclude보다 sector cap/soft penalty를 우선 검토하세요.")

    lines = [
        "# Sector Attribution Report",
        "",
        "## Summary",
        f"- generated_at_utc: {datetime.now(timezone.utc).isoformat()}",
        f"- report_dir: {report_dir}",
        f"- db: {args.db}",
        f"- candidates: {', '.join(selected_candidates)}",
        f"- warnings: {len(warnings)}",
    ] + [f"- warning: {w}" for w in warnings]

    lines += ["", "## Focus windows selected"]
    for fr in focus_rows:
        lines.append(f"- {fr['candidate']}: {fr['focus_start_date']} ~ {fr['focus_end_date']} ({fr['selection_reason']}), run_id={fr.get('run_id')}")

    lines += ["", "## Sector concentration by candidate"]
    for c in selected_candidates:
        c_rows = [r for r in summary_rows if r["candidate"] == c]
        if not c_rows:
            lines.append(f"- {c}: unavailable")
            continue
        conc = max(float(r["sector_concentration_score"]) for r in c_rows)
        lines.append(f"- {c}: sector_concentration_score={conc:.4f}")

    lines += ["", "## Loss contribution by sector"]
    for c in selected_candidates:
        lines.append(f"### {c}")
        lines.extend(_top_loss_lines(c, n=8) or ["- unavailable"])

    lines += ["", "## Worst symbols by estimated contribution"]
    for c in selected_candidates:
        lines.append(f"### {c}")
        worst_syms = sorted([r for r in symbol_rows if r["candidate"] == c], key=lambda x: float(x["estimated_contribution"]))[: args.top_n]
        for r in worst_syms[:10]:
            lines.append(f"- {r['symbol']}({r['sector']}): est_contrib={float(r['estimated_contribution']):.4f}, stop_loss={r['stop_loss_count']}")
        if not worst_syms:
            lines.append("- unavailable")

    lines += ["", "## Baseline_old vs aggressive_hybrid_v4 sector difference"]
    lines.extend(diff_lines or ["- unavailable"])

    lines += ["", "## Interpretation"]
    lines.extend(interpretation)

    lines += [
        "",
        "## Next recommended experiments",
        "- 손실 기여 상위 섹터에 대해 hard exclude 대신 sector cap(예: 최대 비중 제한) 실험.",
        "- ranking 단계에 sector soft penalty를 적용한 A/B 백테스트.",
        "- 동일 섹터 동시 손실이 크면 regime filter는 그 다음 단계로 실험.",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")

    out_manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db": args.db,
        "report_dir": str(report_dir),
        "selected_candidates": selected_candidates,
        "full_period_run_ids": full_period_run_ids,
        "focus_windows": focus_rows,
        "sector_file": args.sector_file,
        "warnings": warnings,
        "output_files": {
            "sector_attribution_summary": str(summary_path),
            "sector_symbol_attribution": str(symbols_path),
            "sector_focus_windows": str(focus_path),
            "sector_attribution_report": str(report_path),
            "sector_attribution_manifest": str(manifest_path),
        },
    }
    manifest_path.write_text(json.dumps(out_manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(out_manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
