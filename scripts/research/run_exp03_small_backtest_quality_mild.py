#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import shutil
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_START_DATE = "2022-02-03"
DEFAULT_END_DATE = "2026-04-29"
DEFAULT_CANDIDATE = "old_quality_mild_90_10"
BASELINE_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

PERIOD_DEFS = [
    ("full_available_to_2026_03_31", "2022-01-04", "2026-03-31"),
    ("main_backtest", "2022-01-04", "2025-12-30"),
    ("validation_2026_q1", "2026-01-02", "2026-03-31"),
    ("recent_shadow_2026_04", "2026-04-01", "2026-04-29"),
    ("stress_2024_07_09", "2024-07-01", "2024-09-30"),
    ("full_available_to_2026_04_29", "2022-01-04", "2026-04-29"),
]

BASELINE_FALLBACK = {
    "main_backtest": {"total_return": 2.454511143509467, "mdd": -0.34139230404085474},
    "validation_2026_q1": {"total_return": -0.0248309466740414, "mdd": -0.306184},
    "recent_shadow_2026_04": {"total_return": 0.2950930200253394, "mdd": -0.100226},
    "stress_2024_07_09": {"total_return": -0.2432450468509677, "mdd": -0.251041},
    "full_available_to_2026_04_29": {"total_return": 3.7921630281643584, "mdd": -0.34139230404085474},
}


def normalize_symbol(series: pd.Series) -> pd.Series:
    return series.astype("string").str.replace(".0", "", regex=False).str.strip().str.zfill(6)


def max_drawdown(eqs: list[float]) -> float:
    if not eqs:
        return math.nan
    peak, mdd = eqs[0], 0.0
    for v in eqs:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak else 0.0
        if dd < mdd:
            mdd = dd
    return mdd


def period_rows(rows: list[dict[str, Any]], start: str, end: str) -> list[dict[str, Any]]:
    return [r for r in rows if start <= r["date"] <= end]


def find_latest_shadow_csv(output_dir: Path) -> Path:
    files = sorted(output_dir.glob("exp02_shadow_scores_*.csv"), key=lambda p: p.stat().st_mtime)
    if not files:
        raise FileNotFoundError(f"No exp02_shadow_scores_*.csv found in {output_dir}")
    return files[-1]


def load_baseline_summary(output_dir: Path) -> tuple[dict[str, dict[str, float]], str]:
    patt = f"exp00_baseline_old_split_summary_{BASELINE_REFERENCE_RUN_ID}.csv"
    files = sorted(output_dir.glob(patt), key=lambda p: p.stat().st_mtime)
    if files:
        df = pd.read_csv(files[-1])
        m = {
            str(r["period"]): {"total_return": float(r["total_return"]), "mdd": float(r["mdd"])}
            for _, r in df.iterrows()
            if str(r.get("period", ""))
        }
        return m, str(files[-1])
    return BASELINE_FALLBACK, "hardcoded_fallback"


def main() -> None:
    ap = argparse.ArgumentParser(description="Exp03: research-only small backtest for quality_mild shadow candidate")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--shadow-scores-csv", default=None)
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    ap.add_argument("--candidate", default=DEFAULT_CANDIDATE)
    ap.add_argument("--start-date", default=DEFAULT_START_DATE)
    ap.add_argument("--end-date", default=DEFAULT_END_DATE)
    args = ap.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    candidate = args.candidate
    score_col = f"{candidate}_shadow_score"
    rank_col = f"{candidate}_shadow_rank"

    shadow_csv = Path(args.shadow_scores_csv) if args.shadow_scores_csv else find_latest_shadow_csv(out_dir)
    shadow_df = pd.read_csv(shadow_csv)
    required_cols = {"date", "symbol", score_col, rank_col}
    missing = sorted(required_cols - set(shadow_df.columns))
    if missing:
        raise ValueError(f"shadow csv missing columns: {missing}")

    shadow_df = shadow_df[["date", "symbol", score_col, rank_col]].copy()
    shadow_df["date"] = pd.to_datetime(shadow_df["date"]).dt.strftime("%Y-%m-%d")
    shadow_df = shadow_df[(shadow_df["date"] >= args.start_date) & (shadow_df["date"] <= args.end_date)]
    shadow_df["symbol"] = normalize_symbol(shadow_df["symbol"])
    shadow_df = shadow_df.rename(columns={score_col: "score", rank_col: "rank"})
    shadow_df["score"] = pd.to_numeric(shadow_df["score"], errors="coerce")
    shadow_df["rank"] = pd.to_numeric(shadow_df["rank"], errors="coerce")
    shadow_df = shadow_df.dropna(subset=["score", "rank"]).copy()

    exp03_run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    temp_db_path = out_dir / f"exp03_temp_backtest_{exp03_run_id}.db"
    shutil.copy2(args.db, temp_db_path)

    conn = get_connection(str(temp_db_path))
    init_db(conn)

    conn.execute("DELETE FROM daily_scores WHERE date >= ? AND date <= ?", (args.start_date, args.end_date))
    rows = [(str(r.symbol), str(r.date), float(r.score), int(r.rank)) for r in shadow_df.itertuples(index=False)]
    conn.executemany("INSERT INTO daily_scores(symbol,date,score,rank) VALUES (?,?,?,?)", rows)
    conn.commit()

    bt_run_id = run_backtest(
        conn,
        top_n=5,
        start_date="2022-01-04",
        end_date="2026-04-29",
        rebalance_frequency="weekly",
        min_holding_days=10,
        keep_rank_threshold=9,
        scoring_profile="old",
        enable_position_stop_loss=True,
        position_stop_loss_pct=0.10,
        stop_loss_cash_mode="keep_cash",
        stop_loss_cooldown_days=0,
        enable_trailing_stop=False,
        market_filter_enabled=False,
        entry_gate_enabled=False,
        enable_overheat_entry_gate=False,
        enable_entry_quality_gate=False,
    )

    q = "SELECT date,equity,daily_return,position_count,exposure FROM backtest_results WHERE run_id=? ORDER BY date"
    bt_df = pd.read_sql_query(q, conn, params=[bt_run_id])
    bt_df["date"] = pd.to_datetime(bt_df["date"]).dt.strftime("%Y-%m-%d")
    rows_all = bt_df.to_dict(orient="records")

    monthly = []
    yearly = []
    for key_len, target in [(7, monthly), (4, yearly)]:
        for k, g in bt_df.groupby(bt_df["date"].str[:key_len]):
            vals = g["equity"].tolist()
            ret = (vals[-1] / vals[0] - 1.0) if vals and vals[0] else math.nan
            target.append({"period": k, "return": ret})

    baseline_map, baseline_source = load_baseline_summary(out_dir)
    summary = []
    for pname, start, end in PERIOD_DEFS:
        pr = period_rows(rows_all, start, end)
        if pr:
            eq = [float(x["equity"]) for x in pr]
            exp = [float(x["exposure"]) for x in pr if x.get("exposure") is not None]
            pc = [float(x["position_count"]) for x in pr if x.get("position_count") is not None]
            total_ret = (eq[-1] / eq[0] - 1.0) if eq and eq[0] else math.nan
            mdd = max_drawdown(eq)
            final_nav = eq[-1]
            s, e = pr[0]["date"], pr[-1]["date"]
        else:
            total_ret = mdd = final_nav = math.nan
            exp, pc = [], []
            s = e = ""
        b = baseline_map.get(pname, {})
        bret = float(b.get("total_return", math.nan)) if b else math.nan
        bmdd = float(b.get("mdd", math.nan)) if b else math.nan
        summary.append({
            "strategy_name": "exp03_quality_mild_shadow_backtest",
            "candidate": candidate,
            "period": pname,
            "start_date": start,
            "end_date": end,
            "actual_start_date": s,
            "actual_end_date": e,
            "total_return": total_ret,
            "baseline_total_return": bret,
            "excess_return_vs_baseline": total_ret - bret if not math.isnan(total_ret) and not math.isnan(bret) else math.nan,
            "mdd": mdd,
            "baseline_mdd": bmdd,
            "mdd_diff_vs_baseline": mdd - bmdd if not math.isnan(mdd) and not math.isnan(bmdd) else math.nan,
            "avg_exposure": (sum(exp) / len(exp)) if exp else math.nan,
            "avg_position_count": (sum(pc) / len(pc)) if pc else math.nan,
            "final_nav": final_nav,
            "trading_days": len(pr),
            "notes": "research-only; temp DB daily_scores replaced by shadow candidate",
        })

    summary_df = pd.DataFrame(summary)
    vs_df = summary_df[["period", "total_return", "baseline_total_return", "excess_return_vs_baseline", "mdd", "baseline_mdd", "mdd_diff_vs_baseline"]].copy()

    paths = {
        "summary": out_dir / f"exp03_quality_mild_backtest_summary_{exp03_run_id}.csv",
        "monthly": out_dir / f"exp03_quality_mild_monthly_returns_{exp03_run_id}.csv",
        "yearly": out_dir / f"exp03_quality_mild_yearly_returns_{exp03_run_id}.csv",
        "nav": out_dir / f"exp03_quality_mild_nav_curve_{exp03_run_id}.csv",
        "vs": out_dir / f"exp03_quality_mild_vs_baseline_reference_{exp03_run_id}.csv",
        "meta": out_dir / f"exp03_quality_mild_metadata_{exp03_run_id}.json",
    }
    summary_df.to_csv(paths["summary"], index=False)
    pd.DataFrame(monthly).to_csv(paths["monthly"], index=False)
    pd.DataFrame(yearly).to_csv(paths["yearly"], index=False)
    bt_df.to_csv(paths["nav"], index=False)
    vs_df.to_csv(paths["vs"], index=False)

    meta = {
        "exp03_run_id": exp03_run_id,
        "candidate_backtest_run_id": bt_run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_db": args.db,
        "temp_db_used": True,
        "temp_db_path": str(temp_db_path),
        "temp_db_deleted": False,
        "shadow_scores_csv": str(shadow_csv),
        "candidate": candidate,
        "baseline_reference_run_id": BASELINE_REFERENCE_RUN_ID,
        "baseline_source": baseline_source,
        "periods": PERIOD_DEFS,
        "output_paths": {k: str(v) for k, v in paths.items()},
    }
    paths["meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"candidate run_id: {bt_run_id}")
    print("temp DB used: YES")
    for _, r in summary_df.iterrows():
        print(f"[{r['period']}] candidate_return={r['total_return']:.6f} baseline_return={r['baseline_total_return']:.6f} excess={r['excess_return_vs_baseline']:.6f} mdd={r['mdd']:.6f} baseline_mdd={r['baseline_mdd']:.6f}")
    for p in ["main_backtest", "validation_2026_q1", "stress_2024_07_09"]:
        row = summary_df[summary_df["period"] == p].iloc[0]
        trend = "개선" if float(row["excess_return_vs_baseline"]) >= 0 else "악화"
        print(f"{p}: baseline 대비 {trend} (excess_return={row['excess_return_vs_baseline']:.6f})")
    recent = summary_df[summary_df["period"] == "recent_shadow_2026_04"].iloc[0]
    print(f"recent 2026-04 무리한 차이 점검: excess_return={recent['excess_return_vs_baseline']:.6f}, mdd_diff={recent['mdd_diff_vs_baseline']:.6f}")

    main_ex = float(summary_df[summary_df["period"] == "main_backtest"]["excess_return_vs_baseline"].iloc[0])
    val_ex = float(summary_df[summary_df["period"] == "validation_2026_q1"]["excess_return_vs_baseline"].iloc[0])
    stress_ex = float(summary_df[summary_df["period"] == "stress_2024_07_09"]["excess_return_vs_baseline"].iloc[0])
    if main_ex < -0.10:
        rec = "1) candidate가 baseline보다 명확히 나쁘면 폐기"
    elif abs(main_ex) <= 0.05 and (val_ex > 0 or stress_ex > 0):
        rec = "2) main은 비슷하고 validation/stress가 개선되면 추가 robustness"
    else:
        rec = "3) main 수익률이 크게 훼손되면 중단"
    print(f"다음 단계 추천: {rec}")


if __name__ == "__main__":
    main()
