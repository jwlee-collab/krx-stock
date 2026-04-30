#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

PRIMARY_FEATURE_CANDIDATES = [
    "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap",
    "rsi_14", "avg_volume_20d", "volume_ratio", "dollar_volume", "avg_dollar_volume_20d",
    "dollar_volume_ratio", "range_pct", "volatility_20d", "drawdown_20d",
]
DERIVED_FEATURE_CANDIDATES = [
    "ma5_gap", "ma20_gap", "ma5_above_ma20", "ret_20d", "high_20d_proximity",
    "drawdown_from_20d_high", "intraday_range_pct",
]


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    try:
        return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    except sqlite3.Error:
        return []


def pick_first(cols: list[str], choices: list[str]) -> str | None:
    for c in choices:
        if c in cols:
            return c
    return None


def label_period(date_str: str) -> str:
    if "2022-01-04" <= date_str <= "2025-12-30":
        return "main_backtest"
    if "2026-01-02" <= date_str <= "2026-03-31":
        return "validation_2026_q1"
    if "2026-04-01" <= date_str <= "2026-04-29":
        return "recent_shadow_2026_04"
    if "2024-07-01" <= date_str <= "2024-09-30":
        return "stress_2024_07_09"
    return "other"


def build_summary(df: pd.DataFrame, features: list[str]) -> pd.DataFrame:
    rows = []
    for f in features:
        s = pd.to_numeric(df[f], errors="coerce")
        st = df["short_term_winner"]
        sl = df["short_term_loser"]
        mt = df["medium_term_winner"]
        ml = df["medium_term_loser"]
        rows.append({
            "feature": f,
            "overall_mean": s.mean(),
            "overall_median": s.median(),
            "short_term_winner_mean": s[st].mean(),
            "short_term_winner_median": s[st].median(),
            "short_term_loser_mean": s[sl].mean(),
            "short_term_loser_median": s[sl].median(),
            "medium_term_winner_mean": s[mt].mean(),
            "medium_term_winner_median": s[mt].median(),
            "medium_term_loser_mean": s[ml].mean(),
            "medium_term_loser_median": s[ml].median(),
            "short_winner_minus_loser_mean": s[st].mean() - s[sl].mean(),
            "medium_winner_minus_loser_mean": s[mt].mean() - s[ml].mean(),
        })
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Exp01 baseline_old report-only feature diagnostics")
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    ap.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    args = ap.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata: dict = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reference_run_id": args.reference_run_id,
        "db_path": args.db,
        "notes": [
            "report-only diagnostics",
            "no DB write",
            "no strategy change",
            "forward returns are lookahead diagnostics only",
        ],
        "missing_columns": {},
        "unavailable_feature_columns": [],
    }

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    holdings_cols = table_columns(conn, "backtest_holdings")
    features_cols = table_columns(conn, "daily_features")
    prices_cols = table_columns(conn, "daily_prices")
    scores_cols = table_columns(conn, "daily_scores")
    _ = table_columns(conn, "backtest_results")
    _ = table_columns(conn, "backtest_risk_events")

    req_holdings = ["run_id", "date", "symbol"]
    selected_holdings = [c for c in req_holdings if c in holdings_cols] + [c for c in ["weight", "rank", "score"] if c in holdings_cols]
    miss_holdings = [c for c in req_holdings if c not in holdings_cols]
    if miss_holdings:
        metadata["missing_columns"]["backtest_holdings"] = miss_holdings
    if not all(c in selected_holdings for c in req_holdings):
        raise RuntimeError("backtest_holdings missing required columns run_id/date/symbol")

    hq = f"SELECT {','.join(selected_holdings)} FROM backtest_holdings WHERE run_id=?"
    holdings = pd.read_sql_query(hq, conn, params=[args.reference_run_id])

    feature_selected = [c for c in ["date", "symbol"] + PRIMARY_FEATURE_CANDIDATES if c in features_cols]
    missing_primary = [c for c in PRIMARY_FEATURE_CANDIDATES if c not in features_cols]
    df_features = pd.read_sql_query(f"SELECT {','.join(feature_selected)} FROM daily_features", conn) if {"date", "symbol"}.issubset(features_cols) else pd.DataFrame()

    score_col = pick_first(scores_cols, ["score", "total_score", "final_score"])
    rank_col = pick_first(scores_cols, ["rank", "score_rank"])
    score_selected = [c for c in ["date", "symbol", score_col, rank_col] if c]
    score_selected = [c for c in score_selected if c in scores_cols]
    if not {"date", "symbol"}.issubset(scores_cols):
        metadata["missing_columns"]["daily_scores"] = ["date", "symbol"]
        df_scores = pd.DataFrame(columns=["date", "symbol"])
    else:
        df_scores = pd.read_sql_query(f"SELECT {','.join(score_selected)} FROM daily_scores", conn)

    px_date_col = pick_first(prices_cols, ["date", "trade_date"])
    px_symbol_col = pick_first(prices_cols, ["symbol", "code", "ticker"])
    px_close_col = pick_first(prices_cols, ["close", "adj_close", "close_price"])
    px_high_col = pick_first(prices_cols, ["high", "high_price"])
    px_low_col = pick_first(prices_cols, ["low", "low_price"])
    needed_px = [px_date_col, px_symbol_col, px_close_col]
    if any(x is None for x in needed_px):
        metadata["missing_columns"]["daily_prices"] = ["date/trade_date", "symbol/code/ticker", "close/adj_close/close_price"]
        raise RuntimeError("daily_prices missing required columns for forward returns")

    px_sel = [c for c in [px_date_col, px_symbol_col, px_close_col, px_high_col, px_low_col] if c]
    px = pd.read_sql_query(f"SELECT {','.join(px_sel)} FROM daily_prices", conn)
    px = px.rename(columns={px_date_col: "date", px_symbol_col: "symbol", px_close_col: "close", px_high_col: "high", px_low_col: "low"})
    px = px.sort_values(["symbol", "date"])

    g = px.groupby("symbol", group_keys=False)
    px["next_1d_return"] = g["close"].shift(-1) / px["close"] - 1.0
    px["next_5d_return"] = g["close"].shift(-5) / px["close"] - 1.0
    px["next_20d_return"] = g["close"].shift(-20) / px["close"] - 1.0

    px["ma5"] = g["close"].transform(lambda x: x.rolling(5).mean())
    px["ma20"] = g["close"].transform(lambda x: x.rolling(20).mean())
    px["ret_20d"] = px["close"] / g["close"].shift(20) - 1.0
    px["high_20d"] = g["close"].transform(lambda x: x.rolling(20).max())
    px["ma5_gap"] = px["close"] / px["ma5"] - 1.0
    px["ma20_gap"] = px["close"] / px["ma20"] - 1.0
    px["ma5_above_ma20"] = (px["ma5"] > px["ma20"]).astype(float)
    px["high_20d_proximity"] = px["close"] / px["high_20d"]
    px["drawdown_from_20d_high"] = px["close"] / px["high_20d"] - 1.0
    if "high" in px.columns and "low" in px.columns:
        px["intraday_range_pct"] = px["high"] / px["low"] - 1.0

    merged = holdings.merge(df_features, on=["date", "symbol"], how="left") if not df_features.empty else holdings.copy()
    merged = merged.merge(df_scores, on=["date", "symbol"], how="left", suffixes=("", "_scoretbl"))
    merged = merged.merge(px[[c for c in ["date", "symbol", "next_1d_return", "next_5d_return", "next_20d_return", "ma5_gap", "ma20_gap", "ma5_above_ma20", "ret_20d", "high_20d_proximity", "drawdown_from_20d_high", "intraday_range_pct"] if c in px.columns]], on=["date", "symbol"], how="left")

    merged["period_label"] = merged["date"].map(label_period)

    q70_5 = merged["next_5d_return"].quantile(0.7)
    q30_5 = merged["next_5d_return"].quantile(0.3)
    q70_20 = merged["next_20d_return"].quantile(0.7)
    q30_20 = merged["next_20d_return"].quantile(0.3)
    merged["short_term_winner"] = merged["next_5d_return"] >= q70_5
    merged["short_term_loser"] = merged["next_5d_return"] <= q30_5
    merged["medium_term_winner"] = merged["next_20d_return"] >= q70_20
    merged["medium_term_loser"] = merged["next_20d_return"] <= q30_20

    selected_features = [f for f in PRIMARY_FEATURE_CANDIDATES + DERIVED_FEATURE_CANDIDATES if f in merged.columns]
    metadata["selected_feature_columns"] = selected_features
    metadata["unavailable_feature_columns"] = [f for f in PRIMARY_FEATURE_CANDIDATES + DERIVED_FEATURE_CANDIDATES if f not in merged.columns]
    metadata["unavailable_feature_columns"].extend(missing_primary)
    metadata["unavailable_feature_columns"] = sorted(set(metadata["unavailable_feature_columns"]))

    summary = build_summary(merged, selected_features)
    stress_summary = build_summary(merged[merged["period_label"] == "stress_2024_07_09"], selected_features)

    quantile_targets = [
        "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap", "rsi_14",
        "range_pct", "intraday_range_pct", "volatility_20d", "volume_ratio", "dollar_volume_ratio",
        "ma5_gap", "ma20_gap", "high_20d_proximity", "drawdown_from_20d_high",
    ]
    q_rows = []
    q_missing = []
    for f in quantile_targets:
        if f not in merged.columns:
            q_missing.append(f)
            continue
        v = pd.to_numeric(merged[f], errors="coerce")
        valid = merged[v.notna()].copy()
        if len(valid) < 10 or valid[f].nunique(dropna=True) < 5:
            q_missing.append(f"{f} (insufficient unique values)")
            continue
        valid["quantile"] = pd.qcut(valid[f], 5, labels=[1, 2, 3, 4, 5], duplicates="drop")
        grp = valid.groupby("quantile", dropna=True)
        tmp = grp[["next_1d_return", "next_5d_return", "next_20d_return"]].mean().reset_index()
        tmp["count"] = grp.size().values
        tmp["feature"] = f
        q_rows.append(tmp)
    quantile_df = pd.concat(q_rows, ignore_index=True) if q_rows else pd.DataFrame(columns=["feature", "quantile", "next_1d_return", "next_5d_return", "next_20d_return", "count"])

    buckets = []
    defs = [
        ("ret_1d>=0.10", "ret_1d", 0.10), ("ret_1d>=0.20", "ret_1d", 0.20),
        ("ret_5d>=0.20", "ret_5d", 0.20), ("ret_5d>=0.40", "ret_5d", 0.40),
        ("intraday_range_pct>=0.12", "intraday_range_pct", 0.12), ("intraday_range_pct>=0.16", "intraday_range_pct", 0.16),
        ("volatility_20d>=0.06", "volatility_20d", 0.06),
    ]
    for name, col, th in defs:
        if col not in merged.columns:
            continue
        sub = merged[pd.to_numeric(merged[col], errors="coerce") >= th]
        buckets.append({"bucket": name, "count": len(sub), "avg_next_5d_return": sub["next_5d_return"].mean(), "avg_next_20d_return": sub["next_20d_return"].mean(), "median_next_5d_return": sub["next_5d_return"].median(), "median_next_20d_return": sub["next_20d_return"].median(), "winner_rate_next_20d_positive": (sub["next_20d_return"] > 0).mean(), "stress_period_count": int((sub["period_label"] == "stress_2024_07_09").sum())})
    vol_col = "volume_ratio" if "volume_ratio" in merged.columns else ("dollar_volume_ratio" if "dollar_volume_ratio" in merged.columns else None)
    if vol_col:
        sub = merged[pd.to_numeric(merged[vol_col], errors="coerce") >= 2.0]
        buckets.append({"bucket": f"{vol_col}>=2.0", "count": len(sub), "avg_next_5d_return": sub["next_5d_return"].mean(), "avg_next_20d_return": sub["next_20d_return"].mean(), "median_next_5d_return": sub["next_5d_return"].median(), "median_next_20d_return": sub["next_20d_return"].median(), "winner_rate_next_20d_positive": (sub["next_20d_return"] > 0).mean(), "stress_period_count": int((sub["period_label"] == "stress_2024_07_09").sum())})
    bucket_df = pd.DataFrame(buckets)

    top_cols = [c for c in ["date", "symbol", "score", "rank", "weight", "next_5d_return", "next_20d_return"] + selected_features if c in merged.columns]
    top_w = merged.sort_values("next_20d_return", ascending=False).head(30)[top_cols].copy()
    top_w["example_type"] = "top_winner"
    top_l = merged.sort_values("next_20d_return", ascending=True).head(30)[top_cols].copy()
    top_l["example_type"] = "top_loser"
    examples = pd.concat([top_w, top_l], ignore_index=True)

    run_id = args.reference_run_id.replace("-", "")[:12]
    snapshot_file = out_dir / f"exp01_baseline_feature_snapshot_{run_id}.csv"
    winner_file = out_dir / f"exp01_winner_loser_feature_summary_{run_id}.csv"
    stress_file = out_dir / f"exp01_stress_2024_feature_summary_{run_id}.csv"
    quantile_file = out_dir / f"exp01_feature_quantile_forward_returns_{run_id}.csv"
    examples_file = out_dir / f"exp01_top_winner_loser_examples_{run_id}.csv"
    overheat_file = out_dir / f"exp01_overheat_bucket_summary_{run_id}.csv"
    meta_file = out_dir / f"exp01_metadata_{run_id}.json"

    merged.to_csv(snapshot_file, index=False)
    summary.to_csv(winner_file, index=False)
    stress_summary.to_csv(stress_file, index=False)
    quantile_df.to_csv(quantile_file, index=False)
    examples.to_csv(examples_file, index=False)
    bucket_df.to_csv(overheat_file, index=False)

    metadata["output_files"] = [str(p) for p in [snapshot_file, winner_file, stress_file, quantile_file, examples_file, overheat_file, meta_file]]
    metadata["row_counts"] = {
        "snapshot": len(merged), "winner_loser_summary": len(summary), "stress_summary": len(stress_summary),
        "quantile": len(quantile_df), "examples": len(examples), "overheat": len(bucket_df),
    }
    metadata["forward_return_calculation"] = "next_Nd_return = close(t+N)/close(t)-1 by symbol using daily_prices; lookahead diagnostics only"
    metadata["notes"].append(f"quantile_skipped={q_missing}")
    meta_file.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    sort_short = summary.sort_values("short_winner_minus_loser_mean", ascending=False)
    print("[Exp01] Snapshot rows:", len(merged))
    print("[Exp01] Used features:", ", ".join(selected_features))
    print("[Exp01] Unavailable features:", ", ".join(metadata["unavailable_feature_columns"]))
    print("[Exp01] Winner-high top10:", ", ".join(sort_short.head(10)["feature"].tolist()))
    print("[Exp01] Loser-high top10:", ", ".join(sort_short.tail(10)["feature"].tolist()))
    risky = bucket_df.sort_values("avg_next_20d_return").head(5)["bucket"].tolist() if not bucket_df.empty else []
    soft = bucket_df.sort_values("avg_next_20d_return", ascending=False).head(5)["bucket"].tolist() if not bucket_df.empty else []
    print("[Exp01] Hard-gate risk candidates:", ", ".join(risky))
    print("[Exp01] Soft-penalty candidates:", ", ".join(soft))
    stress_signal = stress_summary.sort_values("medium_winner_minus_loser_mean", ascending=False).head(10)["feature"].tolist() if not stress_summary.empty else []
    print("[Exp01] Stress notable features:", ", ".join(stress_signal))


if __name__ == "__main__":
    main()
