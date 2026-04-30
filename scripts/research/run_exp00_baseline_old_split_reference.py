#!/usr/bin/env python3
from __future__ import annotations

"""Experiment 00 research-only baseline_old split reference report."""

import argparse
import csv
import json
import math
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db

DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

BASELINE_PARAMS: dict[str, Any] = {
    "source": "krx",
    "scoring_profile": "old",
    "scoring_version": "old",
    "universe_mode": "rolling_liquidity",
    "universe_size": 100,
    "universe_lookback_days": 20,
    "top_n": 5,
    "rebalance_frequency": "weekly",
    "min_holding_days": 10,
    "keep_rank_threshold": 9,
    "enable_position_stop_loss": True,
    "position_stop_loss_pct": 0.10,
    "stop_loss_cash_mode": "keep_cash",
    "stop_loss_cooldown_days": 0,
    "enable_trailing_stop": False,
    "market_filter_enabled": False,
    "entry_gate_enabled": False,
    "enable_overheat_entry_gate": False,
    "enable_entry_quality_gate": False,
}


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _max_drawdown(equities: list[float]) -> float:
    peak = equities[0] if equities else 0.0
    mdd = 0.0
    for v in equities:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak else 0.0
        if dd < mdd:
            mdd = dd
    return mdd if equities else math.nan


def _period_rows(results: list[dict[str, Any]], start: str, end: str) -> list[dict[str, Any]]:
    return [r for r in results if start <= r["date"] <= end]


def _returns_from_nav(rows: list[dict[str, Any]], key_len: int) -> list[dict[str, Any]]:
    buckets: dict[str, list[float]] = {}
    for r in rows:
        buckets.setdefault(r["date"][:key_len], []).append(float(r["equity"]))
    out = []
    for k, vals in sorted(buckets.items()):
        if not vals or vals[0] == 0:
            ret = math.nan
        else:
            ret = vals[-1] / vals[0] - 1.0
        out.append({"period": k, "return": ret})
    return out


def _period_metrics(name: str, reference_run_id: str, period: str, expected_start: str, expected_end: str, rows: list[dict[str, Any]], monthly_map: dict[str, float], notes: list[str]) -> dict[str, Any]:
    if not rows:
        return {
            "strategy_name": name,
            "reference_run_id": reference_run_id,
            "period": period,
            "start_date": expected_start,
            "end_date": expected_end,
            "actual_start_date": "",
            "actual_end_date": "",
            "total_return": math.nan,
            "mdd": math.nan,
            "avg_exposure": math.nan,
            "avg_position_count": math.nan,
            "worst_month_return": math.nan,
            "final_nav": math.nan,
            "trading_days": 0,
            "notes": "; ".join(notes + ["no rows in date range"]),
        }
    equities = [float(r["equity"]) for r in rows]
    exposures = [float(r["exposure"]) for r in rows if r.get("exposure") is not None]
    pos_counts = [float(r["position_count"]) for r in rows if r.get("position_count") is not None]
    s, e = rows[0]["date"], rows[-1]["date"]
    months = [k for k in monthly_map if s[:7] <= k <= e[:7]]
    return {
        "strategy_name": name,
        "reference_run_id": reference_run_id,
        "period": period,
        "start_date": expected_start,
        "end_date": expected_end,
        "actual_start_date": s,
        "actual_end_date": e,
        "total_return": (equities[-1] / equities[0] - 1.0) if equities and equities[0] else math.nan,
        "mdd": _max_drawdown(equities),
        "avg_exposure": (sum(exposures) / len(exposures)) if exposures else math.nan,
        "avg_position_count": (sum(pos_counts) / len(pos_counts)) if pos_counts else math.nan,
        "worst_month_return": min((monthly_map[m] for m in months), default=math.nan),
        "final_nav": equities[-1],
        "trading_days": len(rows),
        "notes": "; ".join(notes),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Run Experiment 00 baseline_old split reference report")
    p.add_argument("--db", default="data/kospi_495_rolling_3y.db")
    p.add_argument("--output-dir", default="data/reports/research")
    p.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    p.add_argument("--rerun-baseline", action="store_true")
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection(args.db)
    init_db(conn)

    source = "existing_backtest_results"
    run_id = args.reference_run_id
    if args.rerun_baseline:
        run_id = run_backtest(
            conn,
            top_n=BASELINE_PARAMS["top_n"],
            rebalance_frequency=BASELINE_PARAMS["rebalance_frequency"],
            min_holding_days=BASELINE_PARAMS["min_holding_days"],
            keep_rank_threshold=BASELINE_PARAMS["keep_rank_threshold"],
            scoring_profile=BASELINE_PARAMS["scoring_profile"],
            market_filter_enabled=BASELINE_PARAMS["market_filter_enabled"],
            entry_gate_enabled=BASELINE_PARAMS["entry_gate_enabled"],
            enable_position_stop_loss=BASELINE_PARAMS["enable_position_stop_loss"],
            position_stop_loss_pct=BASELINE_PARAMS["position_stop_loss_pct"],
            stop_loss_cash_mode=BASELINE_PARAMS["stop_loss_cash_mode"],
            stop_loss_cooldown_days=BASELINE_PARAMS["stop_loss_cooldown_days"],
            enable_trailing_stop=BASELINE_PARAMS["enable_trailing_stop"],
            enable_overheat_entry_gate=BASELINE_PARAMS["enable_overheat_entry_gate"],
            enable_entry_quality_gate=BASELINE_PARAMS["enable_entry_quality_gate"],
        )
        source = "new_run_backtest"

    all_cols = _table_columns(conn, "backtest_results")
    required = ["date", "equity"]
    optional = [
        "daily_return", "exposure", "position_count", "turnover", "cash", "cash_weight",
        "portfolio_value", "holdings_value", "max_single_position_weight",
    ]
    missing = [c for c in required if c not in all_cols]
    if missing:
        raise RuntimeError(f"Missing required columns in backtest_results: {missing}; available={all_cols}")
    selected_cols = required + [c for c in optional if c in all_cols]

    q = f"SELECT {','.join(selected_cols)} FROM backtest_results WHERE run_id=? ORDER BY date"
    raw = conn.execute(q, (run_id,)).fetchall()
    rows = [{c: r[c] for c in selected_cols} for r in raw]

    monthly_rows = _returns_from_nav(rows, 7)
    yearly_rows = _returns_from_nav(rows, 4)
    monthly_map = {x["period"]: x["return"] for x in monthly_rows}

    period_defs = [
        ("full_available_to_2026_03_31", "2022-01-04", "2026-03-31"),
        ("main_backtest", "2022-01-04", "2025-12-30"),
        ("validation_2026_q1", "2026-01-02", "2026-03-31"),
        ("recent_shadow_2026_04", "2026-04-01", "2026-04-29"),
        ("stress_2024_07_09", "2024-07-01", "2024-09-30"),
        ("full_available_to_2026_04_29", "2022-01-04", "2026-04-29"),
    ]

    common_notes = [
        "frozen baseline_old reference split report",
        f"source={source}",
        "frozen_reason=rerun could not reproduce baseline_old reference",
        "rerun_mismatch_summary=existing DB reference run retained for reproducibility",
    ]
    split_rows = []
    for pname, start, end in period_defs:
        split_rows.append(_period_metrics("baseline_old", args.reference_run_id, pname, start, end, _period_rows(rows, start, end), monthly_map, common_notes))

    stress_rows = [r for r in split_rows if r["period"] == "stress_2024_07_09"]

    summary_file = out_dir / f"exp00_baseline_old_split_summary_{run_id}.csv"
    monthly_file = out_dir / f"exp00_baseline_old_monthly_returns_{run_id}.csv"
    yearly_file = out_dir / f"exp00_baseline_old_yearly_returns_{run_id}.csv"
    stress_file = out_dir / f"exp00_baseline_old_stress_2024_07_09_{run_id}.csv"
    meta_file = out_dir / f"exp00_baseline_old_metadata_{run_id}.json"

    for path, data in [(summary_file, split_rows), (monthly_file, monthly_rows), (yearly_file, yearly_rows), (stress_file, stress_rows)]:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(data[0].keys()) if data else ["empty"])
            w.writeheader()
            for row in data:
                w.writerow(row)

    metadata = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db_path": args.db,
        "strategy_name": "baseline_old",
        "reference_run_id": args.reference_run_id,
        "rerun_baseline": bool(args.rerun_baseline),
        "source": source,
        "frozen_baseline_reason": "Use existing baseline_old reference run in DB for reproducible split report",
        "rerun_mismatch_summary": "Fresh run_backtest results did not reproduce prior baseline_old reference metrics",
        "selected_backtest_results_columns": selected_cols,
        "required_columns": required,
        "optional_columns_selected_when_exists": optional,
    }
    meta_file.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
