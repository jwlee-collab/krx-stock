#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_INPUT_SNAPSHOT = "data/reports/research/exp01b_feature_forward_snapshot_3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316.csv"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

REQUIRED_COLUMNS = [
    "date", "symbol_norm", "weight", "unrealized_return", "next_1d_return", "next_5d_return", "next_20d_return", "period_label", "is_stress_2024_07_09",
]

RULES = {
    "R1": ("trim_extreme_unrealized_50", lambda d: d["unrealized_return"] >= 0.50),
    "R2": ("trim_extreme_unrealized_50_and_weak_5d", lambda d: (d["unrealized_return"] >= 0.50) & (d["ret_5d"] <= 0)),
    "R3": ("trim_extreme_unrealized_50_and_intraday_reversal", lambda d: (d["unrealized_return"] >= 0.50) & (d["close_to_open_return"] <= -0.05)),
}

SPLITS = {
    "full_available_to_2026_03_31": ("2022-01-04", "2026-03-31"),
    "main_backtest": ("2022-01-04", "2025-12-30"),
    "validation_2026_q1": ("2026-01-02", "2026-03-31"),
    "recent_shadow_2026_04": ("2026-04-01", "2026-04-29"),
    "stress_2024_07_09": ("2024-07-01", "2024-09-30"),
    "full_available_to_2026_04_29": ("2022-01-04", "2026-04-29"),
}


def _load_snapshot(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"symbol": "string", "symbol_norm": "string"})
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].fillna("").str.zfill(6)
    df["symbol_norm"] = df["symbol_norm"].fillna("").str.zfill(6)
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["is_stress_2024_07_09"] = df["is_stress_2024_07_09"].astype(str).str.lower().isin(["true", "1", "t", "yes"])
    num_cols = set(REQUIRED_COLUMNS + ["daily_score", "daily_rank", "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap", "range_pct", "volatility_20d", "intraday_range_pct", "close_to_open_return"])
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _load_nav(db_path: str, run_id: str) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        cols = pd.read_sql_query("PRAGMA table_info(backtest_results)", conn)["name"].tolist()
        wanted = [c for c in ["date", "equity", "daily_return", "exposure", "position_count", "cash_weight", "max_single_position_weight"] if c in cols]
        q = f"SELECT {', '.join(wanted)} FROM backtest_results WHERE run_id = ? ORDER BY date"
        nav = pd.read_sql_query(q, conn, params=[run_id])
    nav["date"] = pd.to_datetime(nav["date"], errors="coerce")
    for c in wanted:
        if c != "date":
            nav[c] = pd.to_numeric(nav[c], errors="coerce")
    return nav


def _mdd(daily_returns: pd.Series) -> float:
    curve = (1.0 + daily_returns.fillna(0.0)).cumprod()
    dd = curve / curve.cummax() - 1.0
    return float(dd.min()) if len(dd) else 0.0


def _bucket_row(df: pd.DataFrame, name: str) -> dict[str, Any]:
    v20 = df["next_20d_return"].dropna()
    row = {
        "bucket": name, "row_count": int(len(df)), "unique_symbols": int(df["symbol_norm"].nunique()), "unique_dates": int(df["date"].nunique()),
        "avg_next_1d_return": df["next_1d_return"].mean(), "median_next_1d_return": df["next_1d_return"].median(),
        "avg_next_5d_return": df["next_5d_return"].mean(), "median_next_5d_return": df["next_5d_return"].median(),
        "avg_next_20d_return": df["next_20d_return"].mean(), "median_next_20d_return": df["next_20d_return"].median(),
        "positive_rate_next_20d": float((v20 > 0).mean()) if len(v20) else None,
        "severe_giveback_rate_next_20d": float((v20 <= -0.15).mean()) if len(v20) else None,
        "continuation_rate_next_20d": float((v20 >= 0.05).mean()) if len(v20) else None,
        "stress_count": int(df["is_stress_2024_07_09"].sum()),
        "stress_count_pct": float(df["is_stress_2024_07_09"].mean()) if len(df) else None,
    }
    for src, dst in [("daily_score", "avg_daily_score"), ("daily_rank", "avg_daily_rank"), ("ret_5d", "avg_ret_5d"), ("momentum_20d", "avg_momentum_20d"), ("momentum_60d", "avg_momentum_60d"), ("range_pct", "avg_range_pct"), ("volatility_20d", "avg_volatility_20d")]:
        row[dst] = df[src].mean() if src in df.columns else None
    return row




def _coerce_datetime_series(frame: pd.DataFrame, column: str) -> tuple[pd.Series, int]:
    col = frame[column]
    if isinstance(col, pd.DataFrame):
        col = col.iloc[:, 0]
    coerced = pd.to_datetime(col, errors="coerce")
    nat_count = int(coerced.isna().sum())
    return coerced, nat_count

def main() -> None:
    p = argparse.ArgumentParser(description="Exp04 Extreme Winner Micro-Rule Research Pack (report-only)")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--input-snapshot", default=DEFAULT_INPUT_SNAPSHOT)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    args = p.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_dir); out_dir.mkdir(parents=True, exist_ok=True)

    df = _load_snapshot(args.input_snapshot)
    nav = _load_nav(args.db, args.reference_run_id)

    # Part A
    buckets = [
        ("unrealized_return_lt_0", df["unrealized_return"] < 0),
        ("unrealized_return_0_to_0p20", (df["unrealized_return"] >= 0) & (df["unrealized_return"] < 0.20)),
        ("unrealized_return_0p20_to_0p50", (df["unrealized_return"] >= 0.20) & (df["unrealized_return"] < 0.50)),
        ("unrealized_return_0p50_to_1p00", (df["unrealized_return"] >= 0.50) & (df["unrealized_return"] < 1.00)),
        ("unrealized_return_ge_1p00", df["unrealized_return"] >= 1.00),
        ("unrealized_return_ge_0p20", df["unrealized_return"] >= 0.20),
        ("unrealized_return_ge_0p50", df["unrealized_return"] >= 0.50),
        ("unrealized_return_ge_1p00_dup", df["unrealized_return"] >= 1.00),
    ]
    if (df["unrealized_return"] >= 2.00).any():
        buckets.append(("unrealized_return_ge_2p00", df["unrealized_return"] >= 2.00))
    bucket_df = pd.DataFrame([_bucket_row(df[m], n) for n, m in buckets])

    # Part B
    ew = df[(df["unrealized_return"] >= 0.50) & df["next_20d_return"].notna()].copy()
    ew["label"] = "other"
    ew.loc[ew["next_20d_return"] <= -0.15, "label"] = "severe_giveback"
    ew.loc[ew["next_20d_return"] >= 0.05, "label"] = "continuation"
    severe = ew[ew["label"] == "severe_giveback"]
    cont = ew[ew["label"] == "continuation"]
    features = ["daily_score", "daily_rank", "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap", "range_pct", "volatility_20d", "intraday_range_pct", "close_to_open_return", "unrealized_return"]
    contrast_rows = []
    for f in features:
        if f not in ew.columns:
            continue
        contrast_rows.append({"feature": f, "severe_mean": severe[f].mean(), "severe_median": severe[f].median(), "continuation_mean": cont[f].mean(), "continuation_median": cont[f].median(), "severe_minus_continuation_mean": severe[f].mean() - cont[f].mean(), "severe_minus_continuation_median": severe[f].median() - cont[f].median()})
    contrast_df = pd.DataFrame(contrast_rows)

    # Part C/D/E
    nav_dates = nav["date"].drop_duplicates().sort_values().tolist()
    next_trade = {nav_dates[i]: nav_dates[i + 1] for i in range(len(nav_dates) - 1)}
    baseline = nav[["date", "daily_return"]].copy()
    baseline["date"] = pd.to_datetime(baseline["date"], errors="coerce")

    event_rows, nav_rows, summary_rows, example_rows = [], [], [], []
    dropped_event_nat_counts: dict[str, int] = {}
    for rule_key, (rule_name, fn) in RULES.items():
        mask = fn(df).fillna(False)
        events = df[mask].copy()
        events = events.loc[:, ~events.columns.duplicated()].copy()
        events["date"], dropped_nat_count = _coerce_datetime_series(events, "date")
        events = events[events["date"].notna()].copy()
        dropped_event_nat_counts[rule_name] = dropped_nat_count
        events["rule_name"] = rule_name
        events["adjustment_on_next_day"] = -(events["weight"].fillna(0.0) * events["next_1d_return"].fillna(0.0))
        events["next_trade_date"] = events["date"].map(next_trade)
        daily_adj = events.groupby("next_trade_date", dropna=True)["adjustment_on_next_day"].sum().rename("adj")

        cf = baseline.merge(daily_adj, how="left", left_on="date", right_index=True)
        cf["date"] = pd.to_datetime(cf["date"], errors="coerce")
        cf["adj"] = cf["adj"].fillna(0.0)
        cf["counterfactual_daily_return"] = cf["daily_return"].fillna(0.0) + cf["adj"]
        cf["counterfactual_equity"] = (1.0 + cf["counterfactual_daily_return"]).cumprod()
        cf["baseline_equity"] = (1.0 + cf["daily_return"].fillna(0.0)).cumprod()
        cf["rule_name"] = rule_name
        nav_rows.append(cf[["rule_name", "date", "daily_return", "adj", "counterfactual_daily_return", "baseline_equity", "counterfactual_equity"]])

        v20 = events["next_20d_return"].dropna()
        event_rows.append({
            "rule_name": rule_name, "event_count": int(len(events)), "unique_symbols": int(events["symbol_norm"].nunique()), "unique_dates": int(events["date"].nunique()),
            "avg_unrealized_return": events["unrealized_return"].mean(), "median_unrealized_return": events["unrealized_return"].median(),
            "avg_next_5d_return": events["next_5d_return"].mean(), "median_next_5d_return": events["next_5d_return"].median(),
            "avg_next_20d_return": events["next_20d_return"].mean(), "median_next_20d_return": events["next_20d_return"].median(),
            "severe_giveback_rate_next_20d": float((v20 <= -0.15).mean()) if len(v20) else None, "continuation_rate_next_20d": float((v20 >= 0.05).mean()) if len(v20) else None,
            "stress_event_count": int(events["is_stress_2024_07_09"].sum()), "events_by_period": json.dumps({str(k): int(v) for k, v in events.groupby("period_label").size().items()}, ensure_ascii=False),
            "dropped_event_date_nat_count": dropped_nat_count,
        })

        keep_cols = ["rule_name", "date", "next_trade_date", "symbol", "symbol_norm", "period_label", "is_stress_2024_07_09", "weight", "unrealized_return", "next_1d_return", "next_5d_return", "next_20d_return", "daily_score", "daily_rank", "ret_5d", "momentum_20d", "momentum_60d", "range_pct", "volatility_20d", "intraday_range_pct", "close_to_open_return"]
        k = [c for c in keep_cols if c in events.columns]
        example_rows.append(events.sort_values(["date", "unrealized_return"], ascending=[True, False]).head(50)[k])

        for period, (s, e) in SPLITS.items():
            sdt, edt = pd.Timestamp(s), pd.Timestamp(e)
            part = cf[(cf["date"] >= sdt) & (cf["date"] <= edt)]
            if part.empty:
                continue
            base_total = float((1.0 + part["daily_return"].fillna(0.0)).prod() - 1.0)
            cf_total = float((1.0 + part["counterfactual_daily_return"].fillna(0.0)).prod() - 1.0)
            summary_rows.append({
                "rule_name": rule_name, "period": period, "total_return": cf_total, "baseline_total_return": base_total,
                "excess_return_vs_baseline": cf_total - base_total, "mdd": _mdd(part["counterfactual_daily_return"]), "baseline_mdd": _mdd(part["daily_return"]),
                "mdd_diff_vs_baseline": _mdd(part["counterfactual_daily_return"]) - _mdd(part["daily_return"]),
                "trigger_count_in_period": int(events[(events["date"] >= sdt) & (events["date"] <= edt)].shape[0]), "avg_daily_adjustment": float(part["adj"].mean()),
                "notes": "backtest-lite approximation; next-day trim-to-cash only",
            })

    event_df = pd.DataFrame(event_rows)
    nav_df = pd.concat(nav_rows, ignore_index=True)
    summary_df = pd.DataFrame(summary_rows)
    examples_df = pd.concat(example_rows, ignore_index=True)

    files = {
        "bucket_summary": out_dir / f"exp04_extreme_winner_bucket_summary_{run_id}.csv",
        "feature_contrast": out_dir / f"exp04_extreme_winner_feature_contrast_{run_id}.csv",
        "event_summary": out_dir / f"exp04_micro_rule_event_summary_{run_id}.csv",
        "counterfactual_summary": out_dir / f"exp04_micro_rule_counterfactual_summary_{run_id}.csv",
        "counterfactual_nav": out_dir / f"exp04_micro_rule_counterfactual_nav_{run_id}.csv",
        "examples": out_dir / f"exp04_micro_rule_examples_{run_id}.csv",
        "metadata": out_dir / f"exp04_metadata_{run_id}.json",
    }
    bucket_df.to_csv(files["bucket_summary"], index=False)
    contrast_df.to_csv(files["feature_contrast"], index=False)
    event_df.to_csv(files["event_summary"], index=False)
    summary_df.to_csv(files["counterfactual_summary"], index=False)
    nav_df.to_csv(files["counterfactual_nav"], index=False)
    examples_df.to_csv(files["examples"], index=False)

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(), "reference_run_id": args.reference_run_id, "input_snapshot": args.input_snapshot, "db_path": args.db,
        "rule_definitions": {k: v[0] for k, v in RULES.items()}, "counterfactual_approximation_formula": "adjustment_on_next_day = -sum(weight*next_1d_return); counterfactual_daily_return = baseline_daily_return + adjustment",
        "output_files": {k: str(v) for k, v in files.items()},
        "row_counts": {"snapshot_rows": int(len(df)), "nav_rows": int(len(nav)), "extreme_rows_unrealized_ge_0p50": int((df["unrealized_return"] >= 0.50).sum())},
        "dropped_event_date_nat_counts": dropped_event_nat_counts,
        "notes": ["report-only", "no DB write", "no strategy change", "no backtest rerun", "no production change", "counterfactual is approximate", "this does not implement profit lock"],
    }
    files["metadata"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    ex = summary_df.pivot(index="rule_name", columns="period", values="excess_return_vs_baseline") if not summary_df.empty else pd.DataFrame()
    severe_rate = float((ew["next_20d_return"] <= -0.15).mean()) if len(ew) else float("nan")
    cont_rate = float((ew["next_20d_return"] >= 0.05).mean()) if len(ew) else float("nan")
    print("=== Exp04 Extreme Winner Micro-Rule Research Pack Summary ===")
    print(f"run_id={run_id}")
    print(f"unrealized_return>=0.50 rows: {int((df['unrealized_return'] >= 0.50).sum())}")
    print(f"severe_giveback_rate(next_20d<=-0.15): {severe_rate:.4f}")
    print(f"continuation_rate(next_20d>=0.05): {cont_rate:.4f}")
    for _, r in event_df.iterrows():
        print(f"{r['rule_name']} events: {int(r['event_count'])}")
    for rule in ex.index:
        print(f"{rule} excess(main/stress/full_2026_04_29)= {ex.at[rule, 'main_backtest'] if 'main_backtest' in ex.columns else None:.6f} / {ex.at[rule, 'stress_2024_07_09'] if 'stress_2024_07_09' in ex.columns else None:.6f} / {ex.at[rule, 'full_available_to_2026_04_29'] if 'full_available_to_2026_04_29' in ex.columns else None:.6f}")
    print("Rule judgment: main excess <= -0.10 => discard; stress gain with large main damage => discard; small main damage + stress/MDD improvement => candidate for further validation.")
    print("Next step recommendation: if all ambiguous, stop this track; only if promising, build a proper simulator.")


if __name__ == "__main__":
    main()
