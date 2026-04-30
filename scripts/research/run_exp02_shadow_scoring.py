#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_START_DATE = "2022-02-03"
DEFAULT_END_DATE = "2026-04-29"
DEFAULT_TOP_N = 5

FEATURE_COLUMNS = [
    "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap", "range_pct", "volatility_20d",
]
PRICE_COLUMNS = ["open", "high", "low", "close", "volume"]
OVERHEAT_FEATURES = ["ret_1d", "ret_5d", "range_pct", "volatility_20d", "close_to_open_return", "intraday_range_pct"]
QUALITY_CLIP_FEATURES = ["momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap"]
QUALITY_LOW_IS_BETTER_FEATURES = ["volatility_20d", "range_pct"]

CANDIDATES = {
    "old_penalty_mild_90_10": ("overheat_penalty_score", 0.90, 0.10),
    "old_penalty_mid_80_20": ("overheat_penalty_score", 0.80, 0.20),
    "old_quality_mild_90_10": ("entry_quality_score", 0.90, 0.10),
    "old_quality_mid_80_20": ("entry_quality_score", 0.80, 0.20),
}


def normalize_symbol(series: pd.Series) -> pd.Series:
    return series.astype("string").str.replace(".0", "", regex=False).str.strip().str.zfill(6)


def pct_rank_descending(series: pd.Series) -> pd.Series:
    return series.rank(method="average", pct=True, ascending=True)


def calc_feature_scores(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str], list[str]]:
    available_overheat = [f for f in OVERHEAT_FEATURES if f in df.columns]
    available_quality_clip = [f for f in QUALITY_CLIP_FEATURES if f in df.columns]
    available_quality_low = [f for f in QUALITY_LOW_IS_BETTER_FEATURES if f in df.columns]

    for feature in set(available_overheat + available_quality_clip + available_quality_low):
        pct_col = f"__pct_{feature}"
        df[pct_col] = df.groupby("date")[feature].transform(pct_rank_descending)

    overheat_parts: list[str] = []
    for feature in available_overheat:
        src = f"__pct_{feature}"
        dst = f"__overheat_{feature}"
        df[dst] = 1.0 - df[src]
        overheat_parts.append(dst)
    df["overheat_penalty_score"] = df[overheat_parts].mean(axis=1, skipna=True) if overheat_parts else pd.NA

    quality_parts: list[str] = []
    for feature in available_quality_clip:
        src = f"__pct_{feature}"
        dst = f"__quality_{feature}"
        df[dst] = df[src].clip(lower=0.2, upper=0.8)
        quality_parts.append(dst)
    for feature in available_quality_low:
        src = f"__pct_{feature}"
        dst = f"__quality_{feature}"
        df[dst] = 1.0 - df[src]
        quality_parts.append(dst)
    df["entry_quality_score"] = df[quality_parts].mean(axis=1, skipna=True) if quality_parts else pd.NA

    unavailable = [
        f for f in sorted(set(OVERHEAT_FEATURES + QUALITY_CLIP_FEATURES + QUALITY_LOW_IS_BETTER_FEATURES)) if f not in df.columns
    ]
    return df, available_overheat, (available_quality_clip + available_quality_low), unavailable


def topn_symbols(df: pd.DataFrame, rank_col: str, top_n: int) -> pd.DataFrame:
    topn = df[df[rank_col] <= top_n].sort_values(["date", rank_col, "symbol"])  # stable
    return topn.groupby("date")["symbol"].apply(list).reset_index(name="symbols")


def main() -> None:
    parser = argparse.ArgumentParser(description="Exp02 shadow scoring report-only")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--top-n", default=DEFAULT_TOP_N, type=int)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    score_q = """
    SELECT date, symbol, score, rank
    FROM daily_scores
    WHERE date >= ? AND date <= ?
    ORDER BY date, rank
    """
    scores = pd.read_sql_query(score_q, conn, params=[args.start_date, args.end_date])
    if scores.empty:
        raise ValueError("No daily_scores rows found in requested range")

    scores["date"] = pd.to_datetime(scores["date"]).dt.strftime("%Y-%m-%d")
    scores["symbol"] = normalize_symbol(scores["symbol"])
    scores = scores.rename(columns={"score": "old_score", "rank": "old_rank"})

    features_q = f"""
    SELECT date, symbol, {", ".join(FEATURE_COLUMNS)}
    FROM daily_features
    WHERE date >= ? AND date <= ?
    """
    features = pd.read_sql_query(features_q, conn, params=[args.start_date, args.end_date])
    features["date"] = pd.to_datetime(features["date"]).dt.strftime("%Y-%m-%d")
    features["symbol"] = normalize_symbol(features["symbol"])

    prices_q = f"""
    SELECT date, symbol, {", ".join(PRICE_COLUMNS)}
    FROM daily_prices
    WHERE date >= ? AND date <= ?
    """
    prices = pd.read_sql_query(prices_q, conn, params=[args.start_date, args.end_date])
    prices["date"] = pd.to_datetime(prices["date"]).dt.strftime("%Y-%m-%d")
    prices["symbol"] = normalize_symbol(prices["symbol"])
    conn.close()

    df = scores.merge(features, on=["date", "symbol"], how="left")
    df = df.merge(prices, on=["date", "symbol"], how="left")

    for col in ["old_score", "old_rank", *FEATURE_COLUMNS, *PRICE_COLUMNS]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["intraday_range_pct"] = (df["high"] / df["low"]) - 1.0
    df.loc[(df["low"] <= 0) | df["low"].isna() | df["high"].isna(), "intraday_range_pct"] = pd.NA
    df["close_to_open_return"] = (df["close"] / df["open"]) - 1.0
    df.loc[(df["open"] <= 0) | df["open"].isna() | df["close"].isna(), "close_to_open_return"] = pd.NA

    df["old_score_pct"] = df.groupby("date")["old_score"].transform(pct_rank_descending)
    df, selected_overheat_features, selected_quality_features, unavailable_features = calc_feature_scores(df)

    for candidate, (component, old_w, comp_w) in CANDIDATES.items():
        score_col = f"{candidate}_shadow_score"
        rank_col = f"{candidate}_shadow_rank"
        df[score_col] = old_w * df["old_score_pct"] + comp_w * df[component]
        df[rank_col] = df.groupby("date")[score_col].rank(method="min", ascending=False)

    old_topn_df = topn_symbols(df, "old_rank", args.top_n).rename(columns={"symbols": "old_topn_symbols"})

    overlap_rows = []
    for candidate in CANDIDATES:
        rank_col = f"{candidate}_shadow_rank"
        cand_topn = topn_symbols(df, rank_col, args.top_n).rename(columns={"symbols": "shadow_topn_symbols"})
        merged = old_topn_df.merge(cand_topn, on="date", how="inner")
        for _, row in merged.iterrows():
            old_set = row["old_topn_symbols"]
            shadow_set = row["shadow_topn_symbols"]
            overlap = sorted(set(old_set).intersection(shadow_set))
            added = [s for s in shadow_set if s not in old_set]
            removed = [s for s in old_set if s not in shadow_set]
            day_df = df[df["date"] == row["date"]]
            shadow_day = day_df[day_df["symbol"].isin(shadow_set)]
            old_day = day_df[day_df["symbol"].isin(old_set)]
            overlap_rows.append({
                "date": row["date"],
                "candidate": candidate,
                "old_topn_symbols": "|".join(old_set),
                "shadow_topn_symbols": "|".join(shadow_set),
                "overlap_count": len(overlap),
                "overlap_rate": len(overlap) / float(args.top_n),
                "added_symbols": "|".join(added),
                "removed_symbols": "|".join(removed),
                "avg_old_rank_of_shadow_topn": shadow_day["old_rank"].mean(),
                "avg_overheat_penalty_score_old_topn": old_day["overheat_penalty_score"].mean(),
                "avg_overheat_penalty_score_shadow_topn": shadow_day["overheat_penalty_score"].mean(),
                "avg_entry_quality_score_old_topn": old_day["entry_quality_score"].mean(),
                "avg_entry_quality_score_shadow_topn": shadow_day["entry_quality_score"].mean(),
            })
    overlap_df = pd.DataFrame(overlap_rows).sort_values(["date", "candidate"]).reset_index(drop=True)

    summary_rows = []
    for candidate, g in overlap_df.groupby("candidate"):
        sub = df[["date", "symbol", "ret_1d", "ret_5d", "momentum_20d", "range_pct", "volatility_20d", f"{candidate}_shadow_rank"]]
        top_sub = sub[sub[f"{candidate}_shadow_rank"] <= args.top_n]
        summary_rows.append({
            "candidate": candidate,
            "avg_overlap_rate": g["overlap_rate"].mean(),
            "median_overlap_rate": g["overlap_rate"].median(),
            "min_overlap_rate": g["overlap_rate"].min(),
            "pct_days_overlap_5_of_5": (g["overlap_count"] == 5).mean(),
            "pct_days_overlap_4_or_more": (g["overlap_count"] >= 4).mean(),
            "pct_days_overlap_3_or_less": (g["overlap_count"] <= 3).mean(),
            "avg_old_rank_of_shadow_topn": g["avg_old_rank_of_shadow_topn"].mean(),
            "avg_overheat_penalty_score_improvement": (g["avg_overheat_penalty_score_shadow_topn"] - g["avg_overheat_penalty_score_old_topn"]).mean(),
            "avg_entry_quality_score_improvement": (g["avg_entry_quality_score_shadow_topn"] - g["avg_entry_quality_score_old_topn"]).mean(),
            "avg_shadow_topn_ret_1d": top_sub["ret_1d"].mean(),
            "avg_shadow_topn_ret_5d": top_sub["ret_5d"].mean(),
            "avg_shadow_topn_momentum_20d": top_sub["momentum_20d"].mean(),
            "avg_shadow_topn_range_pct": top_sub["range_pct"].mean(),
            "avg_shadow_topn_volatility_20d": top_sub["volatility_20d"].mean(),
        })
    summary_df = pd.DataFrame(summary_rows).sort_values("avg_overlap_rate", ascending=False)

    recent_dates = sorted(df["date"].unique())[-20:]
    recent_df = overlap_df[overlap_df["date"].isin(recent_dates)].copy().sort_values(["date", "candidate"])

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    paths = {
        "shadow_scores": out_dir / f"exp02_shadow_scores_{run_id}.csv",
        "daily_topn_overlap": out_dir / f"exp02_daily_topn_overlap_{run_id}.csv",
        "candidate_summary": out_dir / f"exp02_candidate_summary_{run_id}.csv",
        "recent_topn_comparison": out_dir / f"exp02_recent_topn_comparison_{run_id}.csv",
        "metadata": out_dir / f"exp02_metadata_{run_id}.json",
    }

    feature_export_cols = [*FEATURE_COLUMNS, *PRICE_COLUMNS, "intraday_range_pct", "close_to_open_return"]
    out_cols = [
        "date", "symbol", "old_score", "old_rank", "old_score_pct", "overheat_penalty_score", "entry_quality_score",
        *[f"{c}_shadow_score" for c in CANDIDATES],
        *[f"{c}_shadow_rank" for c in CANDIDATES],
        *[c for c in feature_export_cols if c in df.columns],
    ]
    df[out_cols].sort_values(["date", "old_rank", "symbol"]).to_csv(paths["shadow_scores"], index=False)
    overlap_df.to_csv(paths["daily_topn_overlap"], index=False)
    summary_df.to_csv(paths["candidate_summary"], index=False)
    recent_df.to_csv(paths["recent_topn_comparison"], index=False)

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "db_path": args.db,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "top_n": args.top_n,
        "candidates": list(CANDIDATES.keys()),
        "selected_features_for_overheat_penalty_score": selected_overheat_features,
        "selected_features_for_entry_quality_score": selected_quality_features,
        "unavailable_features": unavailable_features,
        "row_counts": {
            "daily_scores_rows": int(scores.shape[0]),
            "merged_rows": int(df.shape[0]),
            "overlap_rows": int(overlap_df.shape[0]),
            "summary_rows": int(summary_df.shape[0]),
            "recent_rows": int(recent_df.shape[0]),
        },
        "output_file_paths": {k: str(v) for k, v in paths.items()},
        "note": [
            "report-only",
            "no DB write",
            "no strategy change",
            "no backtest rerun",
            "shadow scores are not production scores",
        ],
    }
    with open(paths["metadata"], "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    best_small = summary_df.sort_values("avg_overlap_rate", ascending=False).iloc[0]
    best_big = summary_df.sort_values("avg_overlap_rate", ascending=True).iloc[0]
    print("=" * 80)
    print("Exp02 Shadow Scoring (report-only) complete")
    print(f"rows loaded: {len(scores):,} (daily_scores universe)")
    print(f"date range: {df['date'].min()} ~ {df['date'].max()}")
    print("candidates summary:")
    print(summary_df[["candidate", "avg_overlap_rate", "avg_overheat_penalty_score_improvement", "avg_entry_quality_score_improvement"]].to_string(index=False))
    print(f"smallest change candidate: {best_small['candidate']} (avg_overlap={best_small['avg_overlap_rate']:.3f})")
    print(f"largest change candidate: {best_big['candidate']} (avg_overlap={best_big['avg_overlap_rate']:.3f})")
    print(f"overall avg overlap: {summary_df['avg_overlap_rate'].mean():.3f}")
    print("recent 5 trading days top_n changes:")
    print(recent_df.sort_values('date').groupby('candidate').tail(5)[['date', 'candidate', 'overlap_count', 'overlap_rate', 'added_symbols', 'removed_symbols']].to_string(index=False))
    print("next step recommendation: Exp03 small backtest should target only 1-2 candidates with non-low overlap and improved overheat score.")
    print("outputs:")
    for k, v in paths.items():
        print(f"- {k}: {v}")


if __name__ == "__main__":
    main()
