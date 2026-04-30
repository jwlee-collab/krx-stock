#!/usr/bin/env python3
from __future__ import annotations

"""Experiment 00 research-only baseline_old split reference report.

Outputs are written under --output-dir using a unique run_id suffix:
- exp00_baseline_old_split_summary_<run_id>.csv
- exp00_baseline_old_monthly_returns_<run_id>.csv
- exp00_baseline_old_yearly_returns_<run_id>.csv
- exp00_baseline_old_stress_2024_07_09_<run_id>.csv
- exp00_baseline_old_position_contribution_<run_id>.csv
- exp00_baseline_old_metadata_<run_id>.json

This script is additive research tooling only and does not modify production/paper logic.
"""

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
from pipeline.dynamic_universe import build_rolling_liquidity_universe
from pipeline.features import generate_daily_features
from pipeline.scoring import generate_daily_scores


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


def _max_drawdown(equities: list[float]) -> float:
    if not equities:
        return float("nan")
    peak = equities[0]
    mdd = 0.0
    for v in equities:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak else 0.0
        if dd < mdd:
            mdd = dd
    return mdd


def _period_rows(results: list[sqlite3.Row], start: str, end: str) -> list[sqlite3.Row]:
    return [r for r in results if start <= r["date"] <= end]


def _period_metrics(name: str, period: str, rows: list[sqlite3.Row], monthly_map: dict[str, float], stop_loss_count: int, avg_exposure: float, turnover: float | None) -> dict[str, Any]:
    if not rows:
        return {
            "strategy_name": name, "period": period, "start_date": "", "end_date": "", "total_return": math.nan,
            "mdd": math.nan, "avg_exposure": avg_exposure, "stop_loss_count": stop_loss_count, "turnover": turnover,
            "worst_month_return": math.nan, "final_nav": math.nan, "trading_days": 0,
            "notes": "no rows in date range",
        }
    equities = [float(r["equity"]) for r in rows]
    start_date = rows[0]["date"]
    end_date = rows[-1]["date"]
    start_equity = equities[0] / (1.0 + float(rows[0]["daily_return"])) if rows[0]["daily_return"] is not None else equities[0]
    total_return = (equities[-1] - start_equity) / start_equity if start_equity else math.nan
    month_keys = [k for k in monthly_map.keys() if start_date[:7] <= k <= end_date[:7]]
    worst_month = min((monthly_map[k] for k in month_keys), default=math.nan)
    return {
        "strategy_name": name,
        "period": period,
        "start_date": start_date,
        "end_date": end_date,
        "total_return": total_return,
        "mdd": _max_drawdown(equities),
        "avg_exposure": avg_exposure,
        "stop_loss_count": stop_loss_count,
        "turnover": turnover,
        "worst_month_return": worst_month,
        "final_nav": equities[-1],
        "trading_days": len(rows),
        "notes": "research-only reference",
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Run Experiment 00 baseline_old split reference report")
    p.add_argument("--db", default="data/kospi_495_rolling_3y.db")
    p.add_argument("--universe-csv", default="data/kospi_valid_universe_495.csv")
    p.add_argument("--output-dir", default="data/reports/research")
    p.add_argument("--main-end", default="2025-12-30")
    p.add_argument("--validation-start", default="2026-01-02")
    p.add_argument("--validation-end", default="2026-03-31")
    p.add_argument("--recent-start", default="2026-04-01")
    p.add_argument("--recent-end", default="2026-04-29")
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection(args.db)
    init_db(conn)

    generate_daily_features(conn)
    build_rolling_liquidity_universe(
        conn,
        universe_size=BASELINE_PARAMS["universe_size"],
        lookback_days=BASELINE_PARAMS["universe_lookback_days"],
    )
    generate_daily_scores(
        conn,
        include_history=True,
        scoring_profile=BASELINE_PARAMS["scoring_profile"],
        universe_mode=BASELINE_PARAMS["universe_mode"],
        universe_size=BASELINE_PARAMS["universe_size"],
        universe_lookback_days=BASELINE_PARAMS["universe_lookback_days"],
    )

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

    rows = conn.execute("SELECT date,equity,daily_return,exposure FROM backtest_results WHERE run_id=? ORDER BY date", (run_id,)).fetchall()
    run_row = conn.execute("SELECT position_stop_loss_count, average_exposure FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone()
    stop_loss_count = int(run_row["position_stop_loss_count"]) if run_row else 0
    avg_exposure = float(run_row["average_exposure"]) if run_row else math.nan

    # Monthly/yearly returns from daily NAV.
    monthly: dict[str, list[float]] = {}
    yearly: dict[str, list[float]] = {}
    for r in rows:
        month = r["date"][:7]
        year = r["date"][:4]
        monthly.setdefault(month, []).append(float(r["daily_return"]))
        yearly.setdefault(year, []).append(float(r["daily_return"]))
    monthly_rows = [{"period": k, "return": math.prod(1.0 + x for x in v) - 1.0} for k, v in sorted(monthly.items())]
    yearly_rows = [{"period": k, "return": math.prod(1.0 + x for x in v) - 1.0} for k, v in sorted(yearly.items())]
    monthly_map = {x["period"]: x["return"] for x in monthly_rows}

    first_date = rows[0]["date"] if rows else ""
    split_rows = [
        _period_metrics("baseline_old", "full_available_to_2026_03_31", _period_rows(rows, first_date, args.validation_end), monthly_map, stop_loss_count, avg_exposure, None),
        _period_metrics("baseline_old", "main_backtest", _period_rows(rows, first_date, args.main_end), monthly_map, stop_loss_count, avg_exposure, None),
        _period_metrics("baseline_old", "validation_2026_q1", _period_rows(rows, args.validation_start, args.validation_end), monthly_map, stop_loss_count, avg_exposure, None),
        _period_metrics("baseline_old", "recent_shadow_2026_04", _period_rows(rows, args.recent_start, args.recent_end), monthly_map, stop_loss_count, avg_exposure, None),
        _period_metrics("baseline_old", "stress_2024_07_09", _period_rows(rows, "2024-07-01", "2024-09-30"), monthly_map, stop_loss_count, avg_exposure, None),
    ]
    stress_rows = [r for r in split_rows if r["period"] == "stress_2024_07_09"]

    contribution_file = out_dir / f"exp00_baseline_old_position_contribution_{run_id}.csv"
    with contribution_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["available", "reason"])
        w.writeheader()
        w.writerow({"available": "false", "reason": "position-level contribution not available from current backtest output"})

    summary_file = out_dir / f"exp00_baseline_old_split_summary_{run_id}.csv"
    monthly_file = out_dir / f"exp00_baseline_old_monthly_returns_{run_id}.csv"
    yearly_file = out_dir / f"exp00_baseline_old_yearly_returns_{run_id}.csv"
    stress_file = out_dir / f"exp00_baseline_old_stress_2024_07_09_{run_id}.csv"
    meta_file = out_dir / f"exp00_baseline_old_metadata_{run_id}.json"

    for path, data in [
        (summary_file, split_rows),
        (monthly_file, monthly_rows),
        (yearly_file, yearly_rows),
        (stress_file, stress_rows),
    ]:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(data[0].keys()) if data else ["empty"])
            w.writeheader()
            for row in data:
                w.writerow(row)

    metadata = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db_path": args.db,
        "universe_csv": args.universe_csv,
        "strategy_name": "baseline_old",
        "strategy_parameters": BASELINE_PARAMS,
        "split_dates": {
            "main_end": args.main_end,
            "validation_start": args.validation_start,
            "validation_end": args.validation_end,
            "recent_start": args.recent_start,
            "recent_end": args.recent_end,
            "stress_start": "2024-07-01",
            "stress_end": "2024-09-30",
        },
        "output_files": {
            "summary": str(summary_file), "monthly": str(monthly_file), "yearly": str(yearly_file),
            "stress": str(stress_file), "position_contribution": str(contribution_file), "metadata": str(meta_file),
        },
        "position_contribution": {"available": False, "reason": "position-level contribution not available from current backtest output"},
        "note": "research-only script; does not modify production/paper trading baseline behavior",
    }
    meta_file.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    # concise console summary
    print("period,start_date,end_date,total_return,mdd,avg_exposure,stop_loss_count,worst_month_return,final_nav")
    for r in split_rows:
        print(f"{r['period']},{r['start_date']},{r['end_date']},{r['total_return']},{r['mdd']},{r['avg_exposure']},{r['stop_loss_count']},{r['worst_month_return']},{r['final_nav']}")


if __name__ == "__main__":
    main()
