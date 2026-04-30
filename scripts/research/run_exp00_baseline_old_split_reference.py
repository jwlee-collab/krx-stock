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


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _pick_first(columns: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in columns:
            return c
    return None


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


def _period_metrics(
    name: str,
    period: str,
    rows: list[sqlite3.Row],
    monthly_map: dict[str, float],
    avg_exposure: float,
    stop_loss_count: float,
    turnover: float | None,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    period_notes = notes[:] if notes else []
    if not rows:
        return {
            "strategy_name": name, "period": period, "start_date": "", "end_date": "", "total_return": math.nan,
            "mdd": math.nan, "avg_exposure": avg_exposure, "stop_loss_count": stop_loss_count, "turnover": turnover,
            "worst_month_return": math.nan, "final_nav": math.nan, "trading_days": 0,
            "notes": "; ".join(period_notes + ["no rows in date range"]),
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
        "notes": "; ".join(period_notes + ["research-only reference"]),
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

    backtest_params = {
        "scoring_version": BASELINE_PARAMS["scoring_version"],
        "scoring_profile": BASELINE_PARAMS["scoring_profile"],
        "universe_mode": BASELINE_PARAMS["universe_mode"],
        "universe_size": BASELINE_PARAMS["universe_size"],
        "universe_lookback_days": BASELINE_PARAMS["universe_lookback_days"],
        "top_n": BASELINE_PARAMS["top_n"],
        "rebalance_frequency": BASELINE_PARAMS["rebalance_frequency"],
        "min_holding_days": BASELINE_PARAMS["min_holding_days"],
        "keep_rank_threshold": BASELINE_PARAMS["keep_rank_threshold"],
        "position_stop_loss": BASELINE_PARAMS["enable_position_stop_loss"],
        "position_stop_loss_pct": BASELINE_PARAMS["position_stop_loss_pct"],
        "stop_loss_cash_mode": BASELINE_PARAMS["stop_loss_cash_mode"],
        "stop_loss_cooldown_days": BASELINE_PARAMS["stop_loss_cooldown_days"],
        "trailing_stop": BASELINE_PARAMS["enable_trailing_stop"],
        "market_filter": BASELINE_PARAMS["market_filter_enabled"],
        "entry_gate": BASELINE_PARAMS["entry_gate_enabled"],
        "overheat_entry_gate": BASELINE_PARAMS["enable_overheat_entry_gate"],
        "entry_quality_gate": BASELINE_PARAMS["enable_entry_quality_gate"],
        "start_date": None,
        "end_date": None,
        "initial_capital": None,
    }
    print("[Exp00] run_backtest parameters:", json.dumps(backtest_params, ensure_ascii=False))

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
    result_cols = _table_columns(conn, "backtest_results")
    date_col = _pick_first(result_cols, ["date", "trading_date"])
    nav_col = _pick_first(result_cols, ["equity", "nav", "portfolio_value"])
    exposure_col = _pick_first(result_cols, ["exposure"])
    daily_ret_col = _pick_first(result_cols, ["daily_return", "return"])
    warnings: list[str] = []
    if date_col is None or nav_col is None:
        raise RuntimeError(f"Required columns not found in backtest_results. columns={result_cols}")
    select_cols = [date_col, nav_col]
    if daily_ret_col:
        select_cols.append(daily_ret_col)
    if exposure_col:
        select_cols.append(exposure_col)
    rows = conn.execute(
        f"SELECT {','.join(select_cols)} FROM backtest_results WHERE run_id=? ORDER BY {date_col}",
        (run_id,),
    ).fetchall()
    normalized_rows: list[dict[str, Any]] = []
    for r in rows:
        normalized_rows.append({
            "date": r[date_col],
            "equity": r[nav_col],
            "daily_return": r[daily_ret_col] if daily_ret_col else None,
            "exposure": r[exposure_col] if exposure_col else None,
        })
    rows = normalized_rows  # type: ignore[assignment]
    run_row = conn.execute("SELECT position_stop_loss_count, average_exposure FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone()
    full_stop_loss_count = int(run_row["position_stop_loss_count"]) if run_row and run_row["position_stop_loss_count"] is not None else math.nan

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
    def pavg_exposure(pr: list[dict[str, Any]]) -> tuple[float, list[str]]:
        vals = [float(x["exposure"]) for x in pr if x["exposure"] is not None]
        if not exposure_col:
            return math.nan, ["exposure column not available"]
        if not vals:
            return math.nan, ["no exposure values in period"]
        return sum(vals) / len(vals), []

    def pstop_loss_count(start: str, end: str) -> tuple[float, list[str]]:
        event_cols = _table_columns(conn, "backtest_events")
        if event_cols and {"run_id", "date"}.issubset(set(event_cols)):
            event_type_col = _pick_first(event_cols, ["event_type", "type", "action", "reason"])
            if event_type_col:
                q = f"SELECT COUNT(*) FROM backtest_events WHERE run_id=? AND date BETWEEN ? AND ? AND lower({event_type_col}) LIKE '%stop_loss%'"
                return float(conn.execute(q, (run_id, start, end)).fetchone()[0]), []
        trade_cols = _table_columns(conn, "backtest_trades")
        if trade_cols and {"run_id", "date"}.issubset(set(trade_cols)):
            reason_col = _pick_first(trade_cols, ["reason", "exit_reason", "trade_reason"])
            if reason_col:
                q = f"SELECT COUNT(*) FROM backtest_trades WHERE run_id=? AND date BETWEEN ? AND ? AND lower({reason_col}) LIKE '%stop_loss%'"
                return float(conn.execute(q, (run_id, start, end)).fetchone()[0]), []
        return math.nan, ["period-specific stop_loss_count unavailable (no usable event/trade table)"]

    def pturnover(start: str, end: str) -> tuple[float | None, list[str]]:
        trade_cols = _table_columns(conn, "backtest_trades")
        if trade_cols and {"run_id", "date"}.issubset(set(trade_cols)):
            qty_col = _pick_first(trade_cols, ["quantity", "qty", "shares"])
            price_col = _pick_first(trade_cols, ["price", "fill_price", "execution_price"])
            if qty_col and price_col:
                notional = float(conn.execute(f"SELECT COALESCE(SUM(ABS({qty_col}*{price_col})),0) FROM backtest_trades WHERE run_id=? AND date BETWEEN ? AND ?", (run_id, start, end)).fetchone()[0])
                pr = _period_rows(rows, start, end)
                avg_nav = sum(float(r["equity"]) for r in pr) / len(pr) if pr else 0.0
                return (notional / avg_nav) if avg_nav else math.nan, []
        return math.nan, ["period-specific turnover unavailable (no quantity/price trade data)"]

    period_defs = [
        ("full_available_to_2026_03_31", first_date, args.validation_end),
        ("main_backtest", first_date, args.main_end),
        ("validation_2026_q1", args.validation_start, args.validation_end),
        ("recent_shadow_2026_04", args.recent_start, args.recent_end),
        ("stress_2024_07_09", "2024-07-01", "2024-09-30"),
    ]
    split_rows = []
    for pname, pstart, pend in period_defs:
        pr = _period_rows(rows, pstart, pend)
        avg_exp, n1 = pavg_exposure(pr)
        sl_cnt, n2 = pstop_loss_count(pstart, pend)
        tov, n3 = pturnover(pstart, pend)
        split_rows.append(_period_metrics("baseline_old", pname, pr, monthly_map, avg_exp, sl_cnt, tov, n1 + n2 + n3))
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
    nav_curve_file = out_dir / f"exp00_baseline_old_nav_curve_{run_id}.csv"
    diagnostics_file = out_dir / f"exp00_baseline_old_backtest_run_diagnostics_{run_id}.csv"

    with nav_curve_file.open("w", newline="", encoding="utf-8") as f:
        cols = ["date", nav_col]
        if exposure_col:
            cols.append(exposure_col)
        if daily_ret_col:
            cols.append(daily_ret_col)
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            out = {"date": r["date"], nav_col: r["equity"]}
            if exposure_col:
                out[exposure_col] = r["exposure"]
            if daily_ret_col:
                out[daily_ret_col] = r["daily_return"]
            w.writerow(out)

    full_equities = [float(r["equity"]) for r in rows]
    first_nav = full_equities[0] if full_equities else math.nan
    final_nav = full_equities[-1] if full_equities else math.nan
    full_total_return = (final_nav / first_nav - 1.0) if first_nav and not math.isnan(first_nav) else math.nan
    if split_rows and split_rows[0]["total_return"] < 0:
        warnings.append("WARNING: Exp00 reproduced a negative baseline_old total return. This conflicts with prior reference. Check strategy parameters, scoring table, and backtest output selection before using this report.")
        print(warnings[-1])

    with diagnostics_file.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["run_id", "output_table", "date_col", "nav_col", "min_date", "max_date", "n_rows", "first_nav", "final_nav", "total_return_full_loaded_curve", "warning_if_any"])
        w.writeheader()
        w.writerow({
            "run_id": run_id, "output_table": "backtest_results", "date_col": date_col, "nav_col": nav_col,
            "min_date": first_date if rows else "", "max_date": rows[-1]["date"] if rows else "", "n_rows": len(rows),
            "first_nav": first_nav, "final_nav": final_nav, "total_return_full_loaded_curve": full_total_return,
            "warning_if_any": " | ".join(warnings),
        })

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
            "nav_curve": str(nav_curve_file), "backtest_run_diagnostics": str(diagnostics_file),
        },
        "run_backtest_parameters": backtest_params,
        "backtest_output_diagnostics": {
            "run_id": run_id,
            "output_table": "backtest_results",
            "selection_query": f"SELECT {','.join(select_cols)} FROM backtest_results WHERE run_id=? ORDER BY {date_col}",
            "date_col": date_col,
            "nav_col": nav_col,
            "loaded_nav_rows": len(rows),
            "nav_min_date": first_date if rows else "",
            "nav_max_date": rows[-1]["date"] if rows else "",
            "first_nav": first_nav,
            "final_nav": final_nav,
            "total_return_full_loaded_curve": full_total_return,
            "warnings": warnings,
            "run_row_position_stop_loss_count": full_stop_loss_count,
        },
        "db_coverage": {},
        "position_contribution": {"available": False, "reason": "position-level contribution not available from current backtest output"},
        "note": "research-only script; does not modify production/paper trading baseline behavior",
    }
    for table in ["daily_prices", "daily_scores"]:
        cols = _table_columns(conn, table)
        dcol = _pick_first(cols, ["date", "trading_date"])
        if not dcol:
            metadata["db_coverage"][table] = {"warning": "date column not found"}
            continue
        stats = conn.execute(f"SELECT MIN({dcol}), MAX({dcol}), COUNT(DISTINCT {dcol}), COUNT(*) FROM {table}").fetchone()
        metadata["db_coverage"][table] = {"min_date": stats[0], "max_date": stats[1], "distinct_dates": stats[2], "rows": stats[3]}
        print(f"[Exp00] {table} coverage: min={stats[0]}, max={stats[1]}, distinct_dates={stats[2]}, rows={stats[3]}")
        split_cov = {}
        for pname, pstart, pend in period_defs:
            c = conn.execute(f"SELECT COUNT(DISTINCT {dcol}), COUNT(*) FROM {table} WHERE {dcol} BETWEEN ? AND ?", (pstart, pend)).fetchone()
            split_cov[pname] = {"distinct_dates": c[0], "rows": c[1], "start": pstart, "end": pend}
        metadata["db_coverage"][f"{table}_by_split"] = split_cov
    meta_file.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    # concise console summary
    print("period,start_date,end_date,total_return,mdd,avg_exposure,stop_loss_count,worst_month_return,final_nav")
    for r in split_rows:
        print(f"{r['period']},{r['start_date']},{r['end_date']},{r['total_return']},{r['mdd']},{r['avg_exposure']},{r['stop_loss_count']},{r['worst_month_return']},{r['final_nav']}")


if __name__ == "__main__":
    main()
