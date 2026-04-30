#!/usr/bin/env python3
from __future__ import annotations

"""Experiment 01C: winner/loser + stress + overheat summary from Exp01B snapshot (report-only)."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_INPUT_SNAPSHOT = "data/reports/research/exp01b_feature_forward_snapshot_3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316.csv"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

REQUIRED_COLUMNS = ["date", "symbol_norm", "period_label", "is_stress_2024_07_09", "next_5d_return", "next_20d_return"]
FEATURE_CANDIDATES = [
    "holding_score", "holding_rank", "daily_score", "daily_rank", "ret_1d", "ret_5d", "momentum_20d", "momentum_60d",
    "sma_20_gap", "sma_60_gap", "range_pct", "volatility_20d", "intraday_range_pct", "close_to_open_return", "unrealized_return",
]

OVERHEAT_BUCKETS = [
    ("ret_1d_ge_0p10", "ret_1d", 0.10, ">="),
    ("ret_1d_ge_0p20", "ret_1d", 0.20, ">="),
    ("ret_5d_ge_0p20", "ret_5d", 0.20, ">="),
    ("ret_5d_ge_0p40", "ret_5d", 0.40, ">="),
    ("range_pct_ge_0p12", "range_pct", 0.12, ">="),
    ("range_pct_ge_0p16", "range_pct", 0.16, ">="),
    ("intraday_range_pct_ge_0p12", "intraday_range_pct", 0.12, ">="),
    ("intraday_range_pct_ge_0p16", "intraday_range_pct", 0.16, ">="),
    ("volatility_20d_ge_0p06", "volatility_20d", 0.06, ">="),
    ("close_to_open_return_ge_0p10", "close_to_open_return", 0.10, ">="),
    ("close_to_open_return_ge_0p20", "close_to_open_return", 0.20, ">="),
    ("unrealized_return_ge_0p20", "unrealized_return", 0.20, ">="),
    ("unrealized_return_ge_0p50", "unrealized_return", 0.50, ">="),
]


def _positive_rate(series: pd.Series) -> float | None:
    valid = series.dropna()
    if valid.empty:
        return None
    return float((valid > 0).mean())


def _feature_mean_median(df: pd.DataFrame, feature: str, prefix: str) -> dict[str, Any]:
    if feature not in df.columns:
        return {}
    s = df[feature]
    return {f"{prefix}_{feature}_mean": s.mean(), f"{prefix}_{feature}_median": s.median()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Experiment 01C winner/stress/overheat summary (report-only)")
    parser.add_argument("--input-snapshot", default=DEFAULT_INPUT_SNAPSHOT)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(args.input_snapshot, dtype={"symbol": "string", "symbol_norm": "string"})
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].fillna("").str.zfill(6)
    df["symbol_norm"] = df["symbol_norm"].fillna("").str.zfill(6)

    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    selected_features = [c for c in FEATURE_CANDIDATES if c in df.columns]
    unavailable_features = [c for c in FEATURE_CANDIDATES if c not in df.columns]

    for col in selected_features + ["next_1d_return", "next_5d_return", "next_20d_return", "daily_score", "daily_rank"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["is_stress_2024_07_09"] = df["is_stress_2024_07_09"].astype(str).str.lower().isin(["true", "1", "t", "yes"])

    medium_base = df[df["next_20d_return"].notna()].copy()
    medium_lower = float(medium_base["next_20d_return"].quantile(0.30))
    medium_upper = float(medium_base["next_20d_return"].quantile(0.70))

    short_base = df[df["next_5d_return"].notna()].copy()
    short_lower = float(short_base["next_5d_return"].quantile(0.30))
    short_upper = float(short_base["next_5d_return"].quantile(0.70))

    df["medium_label"] = "medium_neutral"
    df.loc[df["next_20d_return"] <= medium_lower, "medium_label"] = "medium_loser"
    df.loc[df["next_20d_return"] >= medium_upper, "medium_label"] = "medium_winner"

    df["short_label"] = "short_neutral"
    df.loc[df["next_5d_return"] <= short_lower, "short_label"] = "short_loser"
    df.loc[df["next_5d_return"] >= short_upper, "short_label"] = "short_winner"

    wl_rows: list[dict[str, Any]] = []
    for feature in selected_features:
        s_all = df[feature].dropna()
        mw = df.loc[df["medium_label"] == "medium_winner", feature].dropna()
        ml = df.loc[df["medium_label"] == "medium_loser", feature].dropna()
        sw = df.loc[df["short_label"] == "short_winner", feature].dropna()
        sl = df.loc[df["short_label"] == "short_loser", feature].dropna()
        wl_rows.append({
            "feature": feature,
            "all_count": int(s_all.shape[0]), "all_mean": s_all.mean(), "all_median": s_all.median(),
            "medium_winner_count": int(mw.shape[0]), "medium_winner_mean": mw.mean(), "medium_winner_median": mw.median(),
            "medium_loser_count": int(ml.shape[0]), "medium_loser_mean": ml.mean(), "medium_loser_median": ml.median(),
            "medium_winner_minus_loser_mean": mw.mean() - ml.mean() if not mw.empty and not ml.empty else None,
            "medium_winner_minus_loser_median": mw.median() - ml.median() if not mw.empty and not ml.empty else None,
            "short_winner_count": int(sw.shape[0]), "short_winner_mean": sw.mean(), "short_winner_median": sw.median(),
            "short_loser_count": int(sl.shape[0]), "short_loser_mean": sl.mean(), "short_loser_median": sl.median(),
            "short_winner_minus_loser_mean": sw.mean() - sl.mean() if not sw.empty and not sl.empty else None,
            "short_winner_minus_loser_median": sw.median() - sl.median() if not sw.empty and not sl.empty else None,
        })
    winner_loser_df = pd.DataFrame(wl_rows)

    stress_df = df[df["is_stress_2024_07_09"]].copy()
    non_stress_df = df[~df["is_stress_2024_07_09"]].copy()

    stress_rows: list[dict[str, Any]] = []
    for name, sdf in [("stress", stress_df), ("non_stress", non_stress_df)]:
        row = {
            "section": "cohort_summary",
            "cohort": name,
            "row_count": int(sdf.shape[0]),
            "unique_dates": int(sdf["date"].nunique(dropna=True)),
            "unique_symbols": int(sdf["symbol_norm"].nunique(dropna=True)),
            "avg_next_1d_return": sdf["next_1d_return"].mean() if "next_1d_return" in sdf.columns else None,
            "avg_next_5d_return": sdf["next_5d_return"].mean(),
            "avg_next_20d_return": sdf["next_20d_return"].mean(),
            "median_next_20d_return": sdf["next_20d_return"].median(),
            "positive_rate_next_20d": _positive_rate(sdf["next_20d_return"]),
            "avg_daily_score": sdf["daily_score"].mean() if "daily_score" in sdf.columns else None,
            "median_daily_score": sdf["daily_score"].median() if "daily_score" in sdf.columns else None,
            "avg_daily_rank": sdf["daily_rank"].mean() if "daily_rank" in sdf.columns else None,
            "median_daily_rank": sdf["daily_rank"].median() if "daily_rank" in sdf.columns else None,
        }
        for feature in selected_features:
            row.update(_feature_mean_median(sdf, feature, name))
        stress_rows.append(row)

    diff_row: dict[str, Any] = {"section": "stress_minus_non_stress", "cohort": "stress_minus_non_stress"}
    for feature in selected_features + ["next_5d_return", "next_20d_return", "daily_score", "daily_rank"]:
        if feature in df.columns:
            diff_row[f"diff_mean_{feature}"] = stress_df[feature].mean() - non_stress_df[feature].mean()
            diff_row[f"diff_median_{feature}"] = stress_df[feature].median() - non_stress_df[feature].median()
    stress_rows.append(diff_row)
    stress_summary_df = pd.DataFrame(stress_rows)

    bucket_rows: list[dict[str, Any]] = []
    for bucket_name, feature, threshold, condition in OVERHEAT_BUCKETS:
        if feature not in df.columns:
            continue
        mask = df[feature] >= threshold
        bdf = df[mask]
        count = int(bdf.shape[0])
        non_null_20d = int(bdf["next_20d_return"].notna().sum())
        notes = "low_forward_20d_count" if non_null_20d < 20 else ""
        bucket_rows.append({
            "bucket_name": bucket_name,
            "feature": feature,
            "threshold": threshold,
            "condition": condition,
            "count": count,
            "count_pct": (count / len(df)) if len(df) else None,
            "avg_next_5d_return": bdf["next_5d_return"].mean(),
            "median_next_5d_return": bdf["next_5d_return"].median(),
            "avg_next_20d_return": bdf["next_20d_return"].mean(),
            "median_next_20d_return": bdf["next_20d_return"].median(),
            "positive_rate_next_20d": _positive_rate(bdf["next_20d_return"]),
            "stress_count": int(bdf["is_stress_2024_07_09"].sum()),
            "stress_count_pct": (float(bdf["is_stress_2024_07_09"].mean()) if count > 0 else None),
            "medium_winner_rate": (float((bdf["medium_label"] == "medium_winner").mean()) if count > 0 else None),
            "medium_loser_rate": (float((bdf["medium_label"] == "medium_loser").mean()) if count > 0 else None),
            "next_20d_non_null_count": non_null_20d,
            "notes": notes,
        })
    overheat_df = pd.DataFrame(bucket_rows)

    period_rows: list[dict[str, Any]] = []
    for label, gdf in df.groupby("period_label", dropna=False):
        period_rows.append({
            "period_label": label,
            "row_count": int(gdf.shape[0]),
            "unique_dates": int(gdf["date"].nunique(dropna=True)),
            "unique_symbols": int(gdf["symbol_norm"].nunique(dropna=True)),
            "avg_next_5d_return": gdf["next_5d_return"].mean(),
            "median_next_5d_return": gdf["next_5d_return"].median(),
            "avg_next_20d_return": gdf["next_20d_return"].mean(),
            "median_next_20d_return": gdf["next_20d_return"].median(),
            "positive_rate_next_20d": _positive_rate(gdf["next_20d_return"]),
            "next_20d_non_null_count": int(gdf["next_20d_return"].notna().sum()),
            "avg_daily_score": gdf["daily_score"].mean() if "daily_score" in gdf.columns else None,
            "median_daily_score": gdf["daily_score"].median() if "daily_score" in gdf.columns else None,
            "avg_daily_rank": gdf["daily_rank"].mean() if "daily_rank" in gdf.columns else None,
            "median_daily_rank": gdf["daily_rank"].median() if "daily_rank" in gdf.columns else None,
            "avg_ret_1d": gdf["ret_1d"].mean() if "ret_1d" in gdf.columns else None,
            "avg_ret_5d": gdf["ret_5d"].mean() if "ret_5d" in gdf.columns else None,
            "avg_momentum_20d": gdf["momentum_20d"].mean() if "momentum_20d" in gdf.columns else None,
            "avg_momentum_60d": gdf["momentum_60d"].mean() if "momentum_60d" in gdf.columns else None,
            "avg_range_pct": gdf["range_pct"].mean() if "range_pct" in gdf.columns else None,
            "avg_volatility_20d": gdf["volatility_20d"].mean() if "volatility_20d" in gdf.columns else None,
        })
    period_df = pd.DataFrame(period_rows)

    example_cols = [
        "date", "symbol", "symbol_norm", "period_label", "is_stress_2024_07_09", "weight", "daily_score", "daily_rank", "holding_score",
        "holding_rank", "next_5d_return", "next_20d_return", "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap",
        "range_pct", "volatility_20d", "intraday_range_pct", "close_to_open_return", "unrealized_return",
    ]
    example_cols_existing = [c for c in example_cols if c in df.columns]
    top = medium_base.nlargest(30, "next_20d_return")[example_cols_existing].copy()
    top.insert(0, "example_type", "top_winner")
    bottom = medium_base.nsmallest(30, "next_20d_return")[example_cols_existing].copy()
    bottom.insert(0, "example_type", "bottom_loser")
    examples_df = pd.concat([top, bottom], ignore_index=True)

    run_id = args.reference_run_id
    file_map = {
        "winner_loser_feature_summary": out_dir / f"exp01c_winner_loser_feature_summary_{run_id}.csv",
        "stress_feature_summary": out_dir / f"exp01c_stress_feature_summary_{run_id}.csv",
        "overheat_bucket_summary": out_dir / f"exp01c_overheat_bucket_summary_{run_id}.csv",
        "period_summary": out_dir / f"exp01c_period_summary_{run_id}.csv",
        "top_winner_loser_examples": out_dir / f"exp01c_top_winner_loser_examples_{run_id}.csv",
        "metadata": out_dir / f"exp01c_metadata_{run_id}.json",
    }

    winner_loser_df.to_csv(file_map["winner_loser_feature_summary"], index=False)
    stress_summary_df.to_csv(file_map["stress_feature_summary"], index=False)
    overheat_df.to_csv(file_map["overheat_bucket_summary"], index=False)
    period_df.to_csv(file_map["period_summary"], index=False)
    examples_df.to_csv(file_map["top_winner_loser_examples"], index=False)

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reference_run_id": run_id,
        "input_snapshot": str(args.input_snapshot),
        "output_files": {k: str(v) for k, v in file_map.items()},
        "row_counts": {
            "input_rows": int(df.shape[0]),
            "medium_base_rows": int(medium_base.shape[0]),
            "short_base_rows": int(short_base.shape[0]),
            "winner_loser_rows": int(winner_loser_df.shape[0]),
            "stress_summary_rows": int(stress_summary_df.shape[0]),
            "overheat_rows": int(overheat_df.shape[0]),
            "period_rows": int(period_df.shape[0]),
            "examples_rows": int(examples_df.shape[0]),
        },
        "selected_features": selected_features,
        "unavailable_features": unavailable_features,
        "winner_loser_thresholds": {
            "medium": {"lower_30pct": medium_lower, "upper_70pct": medium_upper},
            "short": {"lower_30pct": short_lower, "upper_70pct": short_upper},
        },
        "overheat_buckets_evaluated": [r[0] for r in OVERHEAT_BUCKETS if r[1] in df.columns],
        "notes": [
            "report-only",
            "no DB write",
            "no strategy change",
            "no backtest rerun",
            "no qcut/quantile",
            "forward returns are diagnostic/lookahead-only",
        ],
    }
    file_map["metadata"].write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    winner_top10 = winner_loser_df.sort_values("medium_winner_minus_loser_mean", ascending=False).head(10)
    loser_top10 = winner_loser_df.sort_values("medium_winner_minus_loser_mean", ascending=True).head(10)
    bad_buckets = overheat_df.sort_values("avg_next_20d_return", ascending=True).head(5) if not overheat_df.empty else pd.DataFrame()
    good_buckets = overheat_df.sort_values("avg_next_20d_return", ascending=False).head(5) if not overheat_df.empty else pd.DataFrame()

    print("=" * 88)
    print("Exp01C winner/stress/overheat summary (report-only)")
    print(f"input rows: {len(df)}")
    print(f"selected features ({len(selected_features)}): {selected_features}")
    print(f"medium threshold: lower30={medium_lower:.6f}, upper70={medium_upper:.6f}")
    print(f"short threshold: lower30={short_lower:.6f}, upper70={short_upper:.6f}")
    print("\n[winner side feature top 10 | medium_winner_minus_loser_mean]")
    print(winner_top10[["feature", "medium_winner_minus_loser_mean", "medium_winner_minus_loser_median"]].to_string(index=False))
    print("\n[loser side feature top 10 | medium_winner_minus_loser_mean ascending]")
    print(loser_top10[["feature", "medium_winner_minus_loser_mean", "medium_winner_minus_loser_median"]].to_string(index=False))
    if not overheat_df.empty:
        print("\n[overheat buckets with especially weak avg_next_20d_return]")
        print(bad_buckets[["bucket_name", "count", "avg_next_20d_return", "next_20d_non_null_count", "notes"]].to_string(index=False))
        print("\n[overheat buckets with relatively strong avg_next_20d_return]")
        print(good_buckets[["bucket_name", "count", "avg_next_20d_return", "next_20d_non_null_count", "notes"]].to_string(index=False))
    stress_core = stress_summary_df[stress_summary_df["section"] == "cohort_summary"][[
        "cohort", "row_count", "avg_next_5d_return", "avg_next_20d_return", "positive_rate_next_20d", "avg_daily_score", "avg_daily_rank",
    ]]
    print("\n[stress 핵심 요약]")
    print(stress_core.to_string(index=False))
    print("\n다음 단계 추천: Exp02 shadow scoring 설계로 이동하되, hard gate가 아니라 soft penalty 중심")
    print("=" * 88)


if __name__ == "__main__":
    main()
