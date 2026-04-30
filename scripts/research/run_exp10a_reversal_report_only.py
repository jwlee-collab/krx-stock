#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

FEATURE_COLS = [
    "ret_1d",
    "ret_5d",
    "momentum_20d",
    "momentum_60d",
    "sma_20_gap",
    "sma_60_gap",
    "range_pct",
    "volatility_20d",
]
PRICE_COLS = ["open", "high", "low", "close", "volume"]

PERIODS = {
    "main_backtest": ("2022-01-04", "2025-12-30"),
    "validation_2026_q1": ("2026-01-02", "2026-03-31"),
    "recent_shadow_2026_04": ("2026-04-01", "2026-04-29"),
    "stress_2024_07_09": ("2024-07-01", "2024-09-30"),
}


def normalize_symbol(v: object) -> str:
    s = "" if v is None else str(v).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return s.zfill(6)
    return digits.zfill(6)[-6:]


def pct_rank(s: pd.Series) -> pd.Series:
    return s.rank(pct=True, method="average")


def topn_frame(df: pd.DataFrame, rank_col: str, top_n: int) -> pd.DataFrame:
    return df[df[rank_col] <= top_n].copy()


def summarize_group(df: pd.DataFrame, group_name: str) -> dict:
    n20 = df["next_20d_return"].dropna()
    return {
        "group": group_name,
        "row_count": int(len(df)),
        "unique_dates": int(df["date"].nunique()),
        "unique_symbols": int(df["symbol"].nunique()),
        "avg_next_5d_return": float(df["next_5d_return"].mean()),
        "median_next_5d_return": float(df["next_5d_return"].median()),
        "avg_next_20d_return": float(df["next_20d_return"].mean()),
        "median_next_20d_return": float(df["next_20d_return"].median()),
        "positive_rate_next_20d": float((n20 > 0).mean()) if len(n20) else np.nan,
        "avg_old_score_pct": float(df["old_score_pct"].mean()),
        "avg_reversal_score_v0": float(df["reversal_score_v0"].mean()),
        "avg_old_rank": float(df["old_rank"].mean()),
        "avg_reversal_rank_v0": float(df["reversal_rank_v0"].mean()),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Experiment 10A reversal style report-only / shadow research")
    p.add_argument("--db", default="data/kospi_495_rolling_3y.db")
    p.add_argument("--output-dir", default="data/reports/research")
    p.add_argument("--start-date", default="2022-02-03")
    p.add_argument("--end-date", default="2026-04-29")
    p.add_argument("--top-n", type=int, default=5)
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]

    with sqlite3.connect(args.db) as conn:
        scores = pd.read_sql_query(
            """
            SELECT date, symbol, score AS old_score, rank AS old_rank
            FROM daily_scores
            WHERE date >= ? AND date <= ?
            ORDER BY date, rank
            """,
            conn,
            params=(args.start_date, args.end_date),
        )
        if scores.empty:
            raise ValueError("No daily_scores rows found in requested range")

        features = pd.read_sql_query(
            f"SELECT date, symbol, {','.join(FEATURE_COLS)} FROM daily_features WHERE date >= ? AND date <= ?",
            conn,
            params=(args.start_date, args.end_date),
        )
        prices = pd.read_sql_query(
            f"SELECT date, symbol, {','.join(PRICE_COLS)} FROM daily_prices WHERE date >= ? AND date <= ?",
            conn,
            params=(args.start_date, args.end_date),
        )

    for df in (scores, features, prices):
        df["date"] = pd.to_datetime(df["date"])
        df["symbol"] = df["symbol"].map(normalize_symbol)

    df = scores.merge(features, on=["date", "symbol"], how="left")
    df = df.merge(prices, on=["date", "symbol"], how="left")

    df["intraday_range_pct"] = (df["high"] / df["low"] - 1).replace([np.inf, -np.inf], np.nan)
    df["close_to_open_return"] = (df["close"] / df["open"] - 1).replace([np.inf, -np.inf], np.nan)

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    df["next_1d_return"] = df.groupby("symbol")["close"].shift(-1) / df["close"] - 1
    df["next_5d_return"] = df.groupby("symbol")["close"].shift(-5) / df["close"] - 1
    df["next_20d_return"] = df.groupby("symbol")["close"].shift(-20) / df["close"] - 1

    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)
    df["old_score_pct"] = df.groupby("date")["old_score"].transform(pct_rank)

    # component percentile helpers
    df["mom20_pct"] = df.groupby("date")["momentum_20d"].transform(pct_rank)
    df["mom60_pct"] = df.groupby("date")["momentum_60d"].transform(pct_rank)
    df["ret5_pct"] = df.groupby("date")["ret_5d"].transform(pct_rank)
    df["c2o_pct"] = df.groupby("date")["close_to_open_return"].transform(pct_rank)
    df["sma20_pct"] = df.groupby("date")["sma_20_gap"].transform(pct_rank)
    df["sma60_pct"] = df.groupby("date")["sma_60_gap"].transform(pct_rank)
    df["vol_pct"] = df.groupby("date")["volatility_20d"].transform(pct_rank)
    df["range_pct_pct"] = df.groupby("date")["range_pct"].transform(pct_rank)
    df["intraday_range_pct_pct"] = df.groupby("date")["intraday_range_pct"].transform(pct_rank)

    mom20_c = df["mom20_pct"].clip(0.05, 0.95)
    mom60_c = df["mom60_pct"].clip(0.05, 0.95)
    df["pullback_component"] = pd.concat([1 - mom20_c, 1 - mom60_c], axis=1).mean(axis=1)
    df["recovery_component"] = pd.concat([df["ret5_pct"], df["c2o_pct"]], axis=1).mean(axis=1)
    df["trend_reclaim_component"] = pd.concat([df["sma20_pct"], df["sma60_pct"]], axis=1).mean(axis=1)
    df["stability_component"] = pd.concat([1 - df["vol_pct"], 1 - df["range_pct_pct"], 1 - df["intraday_range_pct_pct"]], axis=1).mean(axis=1)

    df["reversal_score_v0"] = (
        0.30 * df["pullback_component"]
        + 0.30 * df["recovery_component"]
        + 0.20 * df["trend_reclaim_component"]
        + 0.20 * df["stability_component"]
    )

    df["reversal_rank_v0"] = (
        df.groupby("date")["reversal_score_v0"].rank(method="first", ascending=False, na_option="bottom")
    )

    daily = []
    for d, g in df.groupby("date", sort=True):
        old = g.nsmallest(args.top_n, "old_rank")
        rev = g.nsmallest(args.top_n, "reversal_rank_v0")
        old_set, rev_set = set(old["symbol"]), set(rev["symbol"])
        overlap = old_set & rev_set
        added = sorted(rev_set - old_set)
        removed = sorted(old_set - rev_set)
        daily.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "old_topn_symbols": ",".join(sorted(old_set)),
                "reversal_topn_symbols": ",".join(sorted(rev_set)),
                "overlap_count": len(overlap),
                "overlap_rate": len(overlap) / args.top_n,
                "added_symbols": ",".join(added),
                "removed_symbols": ",".join(removed),
                "avg_old_rank_of_reversal_topn": float(rev["old_rank"].mean()),
                "avg_reversal_score_topn": float(rev["reversal_score_v0"].mean()),
                "avg_old_score_pct_reversal_topn": float(rev["old_score_pct"].mean()),
                "avg_next_5d_return_old_topn": float(old["next_5d_return"].mean()),
                "avg_next_5d_return_reversal_topn": float(rev["next_5d_return"].mean()),
                "avg_next_20d_return_old_topn": float(old["next_20d_return"].mean()),
                "avg_next_20d_return_reversal_topn": float(rev["next_20d_return"].mean()),
            }
        )
    daily_df = pd.DataFrame(daily)

    old_topn = topn_frame(df, "old_rank", args.top_n)
    rev_topn = topn_frame(df, "reversal_rank_v0", args.top_n)

    cand = []
    for name, sub in [("old_topn", old_topn), ("reversal_topn", rev_topn)]:
        s20 = sub["next_20d_return"].dropna()
        cand.append(
            {
                "group": name,
                "avg_next_1d_return": float(sub["next_1d_return"].mean()),
                "median_next_1d_return": float(sub["next_1d_return"].median()),
                "avg_next_5d_return": float(sub["next_5d_return"].mean()),
                "median_next_5d_return": float(sub["next_5d_return"].median()),
                "avg_next_20d_return": float(sub["next_20d_return"].mean()),
                "median_next_20d_return": float(sub["next_20d_return"].median()),
                "positive_rate_next_20d": float((s20 > 0).mean()) if len(s20) else np.nan,
                "row_count": int(len(sub)),
                "non_null_next_20d_count": int(sub["next_20d_return"].notna().sum()),
                "avg_ret_1d": float(sub["ret_1d"].mean()),
                "avg_ret_5d": float(sub["ret_5d"].mean()),
                "avg_momentum_20d": float(sub["momentum_20d"].mean()),
                "avg_momentum_60d": float(sub["momentum_60d"].mean()),
                "avg_sma_20_gap": float(sub["sma_20_gap"].mean()),
                "avg_sma_60_gap": float(sub["sma_60_gap"].mean()),
                "avg_range_pct": float(sub["range_pct"].mean()),
                "avg_volatility_20d": float(sub["volatility_20d"].mean()),
            }
        )
    cand_df = pd.DataFrame(cand)

    period_rows = []
    notes = [
        "report-only",
        "no DB write",
        "no strategy change",
        "no backtest rerun",
        "forward returns are diagnostic/lookahead-only",
        "reversal_score_v0 is shadow/research only",
    ]
    for period, (sdt, edt) in PERIODS.items():
        old_p = old_topn[(old_topn["date"] >= sdt) & (old_topn["date"] <= edt)]
        rev_p = rev_topn[(rev_topn["date"] >= sdt) & (rev_topn["date"] <= edt)]
        for gname, gdf in [("old_topn", old_p), ("reversal_topn", rev_p)]:
            row = summarize_group(gdf, gname)
            row["period"] = period
            period_rows.append(row)
        if period == "recent_shadow_2026_04":
            c = int(rev_p["next_20d_return"].notna().sum())
            if c < max(1, len(rev_p) // 2):
                notes.append(f"recent_shadow_2026_04 has low next_20d non-null count: {c}/{len(rev_p)}")
    period_df = pd.DataFrame(period_rows)[[
        "period", "group", "row_count", "unique_dates", "unique_symbols",
        "avg_next_5d_return", "median_next_5d_return", "avg_next_20d_return", "median_next_20d_return",
        "positive_rate_next_20d", "avg_old_score_pct", "avg_reversal_score_v0", "avg_old_rank", "avg_reversal_rank_v0"
    ]]

    recent_dates = sorted(df["date"].drop_duplicates())[-20:]
    recent_df = daily_df[daily_df["date"].isin([d.strftime("%Y-%m-%d") for d in recent_dates])][[
        "date", "old_topn_symbols", "reversal_topn_symbols", "overlap_count", "overlap_rate", "added_symbols", "removed_symbols"
    ]]

    files = {
        "scores": out_dir / f"exp10a_reversal_scores_{run_id}.csv",
        "daily_overlap": out_dir / f"exp10a_daily_topn_overlap_{run_id}.csv",
        "candidate_summary": out_dir / f"exp10a_candidate_forward_summary_{run_id}.csv",
        "period_summary": out_dir / f"exp10a_period_forward_summary_{run_id}.csv",
        "recent_comparison": out_dir / f"exp10a_recent_topn_comparison_{run_id}.csv",
        "metadata": out_dir / f"exp10a_metadata_{run_id}.json",
    }

    keep_cols = [
        "date", "symbol", "old_score", "old_rank", "old_score_pct", "reversal_score_v0", "reversal_rank_v0",
        "pullback_component", "recovery_component", "trend_reclaim_component", "stability_component",
        "next_1d_return", "next_5d_return", "next_20d_return",
    ] + FEATURE_COLS + PRICE_COLS + ["intraday_range_pct", "close_to_open_return"]

    out_scores = df[keep_cols].copy()
    out_scores["date"] = out_scores["date"].dt.strftime("%Y-%m-%d")
    out_scores.to_csv(files["scores"], index=False)
    daily_df.to_csv(files["daily_overlap"], index=False)
    cand_df.to_csv(files["candidate_summary"], index=False)
    period_df.to_csv(files["period_summary"], index=False)
    recent_df.to_csv(files["recent_comparison"], index=False)

    used_features = [c for c in FEATURE_COLS + ["intraday_range_pct", "close_to_open_return"] if c in df.columns]
    unavailable = [c for c in FEATURE_COLS if c not in df.columns]
    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "db_path": args.db,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "top_n": args.top_n,
        "row_counts": {
            "daily_scores_rows": int(len(scores)),
            "merged_rows": int(len(df)),
            "daily_overlap_rows": int(len(daily_df)),
            "candidate_summary_rows": int(len(cand_df)),
            "period_summary_rows": int(len(period_df)),
            "recent_comparison_rows": int(len(recent_df)),
        },
        "selected_features": used_features,
        "unavailable_features": unavailable,
        "component_definitions": {
            "pullback_component": "avg(1-clip(pct(momentum_20d),0.05,0.95), 1-clip(pct(momentum_60d),0.05,0.95))",
            "recovery_component": "avg(pct(ret_5d), pct(close_to_open_return))",
            "trend_reclaim_component": "avg(pct(sma_20_gap), pct(sma_60_gap))",
            "stability_component": "avg(1-pct(volatility_20d), 1-pct(range_pct), 1-pct(intraday_range_pct))",
            "reversal_score_v0": "0.30*pullback + 0.30*recovery + 0.20*trend_reclaim + 0.20*stability",
        },
        "output_paths": {k: str(v) for k, v in files.items()},
        "notes": notes,
    }
    files["metadata"].write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    avg_overlap = float(daily_df["overlap_rate"].mean()) if len(daily_df) else np.nan
    print("[Exp10A] Reversal Style Report-Only completed")
    print(f"- rows loaded: {len(scores):,} (daily_scores universe)")
    print(f"- date range: {args.start_date} ~ {args.end_date}")
    print(f"- avg overlap old vs reversal top{args.top_n}: {avg_overlap:.4f}")
    print("- old vs reversal forward summary (next_5d / next_20d avg):")
    for _, r in cand_df.iterrows():
        print(f"  - {r['group']}: {r['avg_next_5d_return']:.6f} / {r['avg_next_20d_return']:.6f}")
    for p_name in ["stress_2024_07_09", "validation_2026_q1"]:
        seg = period_df[period_df["period"] == p_name]
        print(f"- {p_name} comparison:")
        for _, r in seg.iterrows():
            print(f"  - {r['group']}: next_5d={r['avg_next_5d_return']:.6f}, next_20d={r['avg_next_20d_return']:.6f}")

    distinctness = "충분히 다름" if avg_overlap < 0.6 else "중복 높음"
    rev20 = float(cand_df.loc[cand_df['group'] == 'reversal_topn', 'avg_next_20d_return'].iloc[0])
    old20 = float(cand_df.loc[cand_df['group'] == 'old_topn', 'avg_next_20d_return'].iloc[0])
    promising = "유망" if rev20 > old20 else "보수적"
    print(f"- reversal vs old 차별성 판단: {distinctness}")
    print(f"- reversal next_20d forward 판단: {promising}")
    print("- next step recommendation:")
    print("  1) reversal forward return이 약하면 이 트랙 중단")
    print("  2) old와 overlap이 너무 높으면 의미 약함")
    print("  3) overlap이 낮고 validation/stress forward가 좋으면 Exp10B standalone small backtest 검토")


if __name__ == "__main__":
    main()
