#!/usr/bin/env python3
from __future__ import annotations

"""Experiment 01A: holdings snapshot report-only script for frozen baseline reference run."""

import argparse
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

PERIOD_WINDOWS = [
    ("main_backtest", "2022-01-04", "2025-12-30"),
    ("validation_2026_q1", "2026-01-02", "2026-03-31"),
    ("recent_shadow_2026_04", "2026-04-01", "2026-04-29"),
]
STRESS_START = "2024-07-01"
STRESS_END = "2024-09-30"


HOLDINGS_CANDIDATE_COLUMNS = [
    "run_id", "date", "symbol", "weight", "entry_date", "entry_price", "current_price",
    "unrealized_return", "holding_days", "rank_at_entry", "score_at_entry", "entry_score", "entry_rank",
]
RESULTS_CANDIDATE_COLUMNS = [
    "run_id", "date", "equity", "daily_return", "position_count", "exposure", "cash_weight", "max_single_position_weight",
]


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _safe_row_count(conn: sqlite3.Connection, table: str) -> int | None:
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except sqlite3.Error:
        return None


def _period_label(date_value: str) -> str:
    for label, start, end in PERIOD_WINDOWS:
        if start <= date_value <= end:
            return label
    return "other"


def _is_stress(date_value: str) -> bool:
    return STRESS_START <= date_value <= STRESS_END


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    p = argparse.ArgumentParser(description="Experiment 01A holdings snapshot (report-only)")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    tables = ["backtest_holdings", "backtest_results", "backtest_risk_events", "backtest_runs"]
    table_schemas = {}
    table_row_counts = {}
    for t in tables:
        table_schemas[t] = _table_columns(conn, t)
        table_row_counts[t] = _safe_row_count(conn, t)

    holdings_available = table_schemas["backtest_holdings"]
    selected_holdings_cols = [c for c in HOLDINGS_CANDIDATE_COLUMNS if c in holdings_available]
    unavailable_holdings_cols = [c for c in HOLDINGS_CANDIDATE_COLUMNS if c not in holdings_available]

    results_available = table_schemas["backtest_results"]
    selected_results_cols = [c for c in RESULTS_CANDIDATE_COLUMNS if c in results_available]
    unavailable_results_cols = [c for c in RESULTS_CANDIDATE_COLUMNS if c not in results_available]

    if "date" not in selected_holdings_cols or "symbol" not in selected_holdings_cols:
        raise RuntimeError("backtest_holdings must include date and symbol for this report")
    if "date" not in selected_results_cols:
        raise RuntimeError("backtest_results must include date for this report")

    hq = f"SELECT {','.join(selected_holdings_cols)} FROM backtest_holdings WHERE run_id=? ORDER BY date, symbol"
    holdings_rows = [dict(r) for r in conn.execute(hq, (args.reference_run_id,)).fetchall()]
    for row in holdings_rows:
        row["period_label"] = _period_label(str(row["date"]))
        row["is_stress_2024_07_09"] = _is_stress(str(row["date"]))

    rq = f"SELECT {','.join(selected_results_cols)} FROM backtest_results WHERE run_id=? ORDER BY date"
    results_rows = [dict(r) for r in conn.execute(rq, (args.reference_run_id,)).fetchall()]
    for row in results_rows:
        row["period_label"] = _period_label(str(row["date"]))
        row["is_stress_2024_07_09"] = _is_stress(str(row["date"]))

    holdings_fields = selected_holdings_cols + ["period_label", "is_stress_2024_07_09"]
    results_fields = selected_results_cols + ["period_label", "is_stress_2024_07_09"]

    # Summaries
    per_day_counts: dict[str, int] = {}
    for r in holdings_rows:
        d = str(r["date"])
        per_day_counts[d] = per_day_counts.get(d, 0) + 1

    period_groups = ["main_backtest", "validation_2026_q1", "recent_shadow_2026_04", "other", "stress_2024_07_09"]
    period_summary_rows: list[dict[str, Any]] = []

    for period in period_groups:
        if period == "stress_2024_07_09":
            hrows = [r for r in holdings_rows if r["is_stress_2024_07_09"]]
            rrows = [r for r in results_rows if r["is_stress_2024_07_09"]]
        else:
            hrows = [r for r in holdings_rows if r["period_label"] == period]
            rrows = [r for r in results_rows if r["period_label"] == period]

        weights = [float(r["weight"]) for r in hrows if "weight" in r and r["weight"] is not None]
        unique_dates = sorted({str(r["date"]) for r in hrows})
        unique_symbols = {str(r["symbol"]) for r in hrows}
        avg_positions_per_day = (sum(per_day_counts[d] for d in unique_dates) / len(unique_dates)) if unique_dates else None

        def _avg(field: str) -> float | None:
            vals = [float(x[field]) for x in rrows if field in x and x[field] is not None]
            return (sum(vals) / len(vals)) if vals else None

        period_summary_rows.append({
            "period_label": period,
            "row_count": len(hrows),
            "unique_dates": len(unique_dates),
            "unique_symbols": len(unique_symbols),
            "avg_weight": (sum(weights) / len(weights)) if weights else None,
            "max_weight": max(weights) if weights else None,
            "min_weight": min(weights) if weights else None,
            "avg_positions_per_day": avg_positions_per_day,
            "avg_exposure": _avg("exposure") if "exposure" in selected_results_cols else None,
            "avg_position_count": _avg("position_count") if "position_count" in selected_results_cols else None,
            "avg_max_single_position_weight": _avg("max_single_position_weight") if "max_single_position_weight" in selected_results_cols else None,
            "start_date": unique_dates[0] if unique_dates else "",
            "end_date": unique_dates[-1] if unique_dates else "",
        })

    symbol_map: dict[str, dict[str, Any]] = {}
    for r in holdings_rows:
        sym = str(r["symbol"])
        d = str(r["date"])
        entry = symbol_map.setdefault(sym, {"symbol": sym, "dates": [], "weights": [], "periods": set(), "stress_days_count": 0})
        entry["dates"].append(d)
        if "weight" in r and r["weight"] is not None:
            entry["weights"].append(float(r["weight"]))
        entry["periods"].add(str(r["period_label"]))
        if r["is_stress_2024_07_09"]:
            entry["stress_days_count"] += 1

    symbol_summary_rows: list[dict[str, Any]] = []
    for sym, data in sorted(symbol_map.items()):
        dates = sorted(data["dates"])
        weights = data["weights"]
        symbol_summary_rows.append({
            "symbol": sym,
            "holding_days_count": len(dates),
            "first_seen_date": dates[0] if dates else "",
            "last_seen_date": dates[-1] if dates else "",
            "avg_weight": (sum(weights) / len(weights)) if weights else None,
            "max_weight": max(weights) if weights else None,
            "periods_seen": "|".join(sorted(data["periods"])),
            "stress_days_count": data["stress_days_count"],
        })

    run_id_safe = args.reference_run_id.replace("/", "_")
    holdings_file = out_dir / f"exp01a_holdings_snapshot_{run_id_safe}.csv"
    results_file = out_dir / f"exp01a_results_curve_{run_id_safe}.csv"
    period_file = out_dir / f"exp01a_holding_summary_by_period_{run_id_safe}.csv"
    symbol_file = out_dir / f"exp01a_symbol_summary_{run_id_safe}.csv"
    meta_file = out_dir / f"exp01a_schema_metadata_{run_id_safe}.json"

    _write_csv(holdings_file, holdings_rows, holdings_fields)
    _write_csv(results_file, results_rows, results_fields)
    _write_csv(period_file, period_summary_rows, list(period_summary_rows[0].keys()) if period_summary_rows else [])
    _write_csv(symbol_file, symbol_summary_rows, list(symbol_summary_rows[0].keys()) if symbol_summary_rows else [])

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reference_run_id": args.reference_run_id,
        "db_path": args.db,
        "table_row_counts": table_row_counts,
        "table_schemas": table_schemas,
        "selected_holdings_columns": selected_holdings_cols,
        "unavailable_holdings_columns": unavailable_holdings_cols,
        "selected_results_columns": selected_results_cols,
        "unavailable_results_columns": unavailable_results_cols,
        "output_file_paths": {
            "holdings_snapshot": str(holdings_file),
            "results_curve": str(results_file),
            "holding_summary_by_period": str(period_file),
            "symbol_summary": str(symbol_file),
            "schema_metadata": str(meta_file),
        },
        "output_row_counts": {
            "holdings_snapshot": len(holdings_rows),
            "results_curve": len(results_rows),
            "holding_summary_by_period": len(period_summary_rows),
            "symbol_summary": len(symbol_summary_rows),
        },
        "note": "report-only, no DB write, no strategy change, no backtest rerun",
    }
    meta_file.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    min_date = min((str(r["date"]) for r in holdings_rows), default="")
    max_date = max((str(r["date"]) for r in holdings_rows), default="")
    print("[Exp01A] Holdings snapshot report complete")
    print(f"- reference_run_id: {args.reference_run_id}")
    print(f"- holdings rows: {len(holdings_rows)}")
    print(f"- results rows: {len(results_rows)}")
    print(f"- date range: {min_date} ~ {max_date}")
    print(f"- unique symbols: {len(symbol_map)}")
    print("- period summary:")
    for row in period_summary_rows:
        print(
            f"  * {row['period_label']}: rows={row['row_count']}, dates={row['unique_dates']}, "
            f"symbols={row['unique_symbols']}, avg_weight={row['avg_weight']}"
        )
    print("- top held symbols by holding_days_count:")
    for row in sorted(symbol_summary_rows, key=lambda x: x["holding_days_count"], reverse=True)[:10]:
        print(f"  * {row['symbol']}: {row['holding_days_count']} days")
    print(f"- available holdings columns: {selected_holdings_cols}")
    print(f"- unavailable holdings columns: {unavailable_holdings_cols}")
    print("- next step recommendation: Exp01B feature attach")


if __name__ == "__main__":
    main()
