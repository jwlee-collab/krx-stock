#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_INPUT_SNAPSHOT = "data/reports/research/exp01b_feature_forward_snapshot_3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316.csv"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"
INITIAL_EQUITY = 100000.0
CASH_DAYS = 5
RECON_WARN_THRESHOLD = 0.05

SPLITS = {
    "full_available_to_2026_03_31": ("2022-01-04", "2026-03-31"),
    "main_backtest": ("2022-01-04", "2025-12-30"),
    "validation_2026_q1": ("2026-01-02", "2026-03-31"),
    "recent_shadow_2026_04": ("2026-04-01", "2026-04-29"),
    "stress_2024_07_09": ("2024-07-01", "2024-09-30"),
    "full_available_to_2026_04_29": ("2022-01-04", "2026-04-29"),
}

RULES = {
    "trim_50_weak_5d": lambda d: (d["unrealized_return"] >= 0.50) & (d["ret_5d"] <= 0),
    "trim_50_intraday_reversal": lambda d: (d["unrealized_return"] >= 0.50) & (d["close_to_open_return"] <= -0.05),
}


def _mdd(daily_return: pd.Series) -> float:
    curve = (1.0 + daily_return.fillna(0.0)).cumprod()
    dd = curve / curve.cummax() - 1.0
    return float(dd.min()) if len(dd) else 0.0


def _load_db(db: str, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    with sqlite3.connect(db) as conn:
        h = pd.read_sql_query(
            "SELECT date, symbol, weight FROM backtest_holdings WHERE run_id=? ORDER BY date, symbol",
            conn,
            params=[run_id],
        )
        r = pd.read_sql_query(
            "SELECT date, equity, daily_return FROM backtest_results WHERE run_id=? ORDER BY date",
            conn,
            params=[run_id],
        )
    h["date"] = pd.to_datetime(h["date"], errors="coerce")
    h["symbol_norm"] = h["symbol"].astype("string").fillna("").str.zfill(6)
    h["weight"] = pd.to_numeric(h["weight"], errors="coerce").fillna(0.0)
    r["date"] = pd.to_datetime(r["date"], errors="coerce")
    r["daily_return"] = pd.to_numeric(r["daily_return"], errors="coerce").fillna(0.0)
    r["equity"] = pd.to_numeric(r["equity"], errors="coerce")
    return h, r


def _prepare_snapshot(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"symbol": "string", "symbol_norm": "string"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "symbol_norm" not in df.columns:
        df["symbol_norm"] = df["symbol"].astype("string")
    df["symbol_norm"] = df["symbol_norm"].fillna("").str.zfill(6)
    for c in ["unrealized_return", "ret_5d", "close_to_open_return", "next_1d_return", "next_5d_return", "next_20d_return"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _period_metrics(nav: pd.DataFrame, start: str, end: str) -> tuple[float, float]:
    sdt, edt = pd.Timestamp(start), pd.Timestamp(end)
    part = nav[(nav["date"] >= sdt) & (nav["date"] <= edt)]
    if part.empty:
        return 0.0, 0.0
    total = float((1.0 + part["daily_return"].fillna(0.0)).prod() - 1.0)
    return total, _mdd(part["daily_return"])


def main() -> None:
    p = argparse.ArgumentParser(description="Exp05 Extreme Winner Trim Rule Proper-ish Small Backtest")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--input-snapshot", default=DEFAULT_INPUT_SNAPSHOT)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    args = p.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    holdings, db_nav = _load_db(args.db, args.reference_run_id)
    snap = _prepare_snapshot(args.input_snapshot)

    if holdings.empty or db_nav.empty:
        raise ValueError("reference run data not found in backtest_holdings/backtest_results")

    base = holdings.merge(
        snap[["date", "symbol_norm", "next_1d_return"]].dropna(subset=["date", "symbol_norm"]),
        on=["date", "symbol_norm"],
        how="left",
    )
    base["next_1d_return"] = pd.to_numeric(base["next_1d_return"], errors="coerce").fillna(0.0)

    base["baseline_weighted_return"] = base["weight"] * base["next_1d_return"]
    by_day = base.groupby("date", as_index=False).agg(
        reconstructed_baseline_daily_return=("baseline_weighted_return", "sum")
    )
    by_day = by_day.sort_values("date")
    by_day["reconstructed_baseline_nav"] = INITIAL_EQUITY * (1.0 + by_day["reconstructed_baseline_daily_return"]).cumprod()

    dbv = db_nav[["date", "daily_return"]].rename(columns={"daily_return": "db_daily_return"})
    aligned = by_day.merge(dbv, on="date", how="inner")
    aligned["db_reference_nav"] = INITIAL_EQUITY * (1.0 + aligned["db_daily_return"].fillna(0.0)).cumprod()

    trade_dates = sorted(aligned["date"].dropna().unique().tolist())
    next_trade = {trade_dates[i]: trade_dates[i + 1] for i in range(len(trade_dates) - 1)}

    events_all = []
    rule_nav_parts = []
    summary_rows = []

    for rule_name, fn in RULES.items():
        ev = snap[fn(snap).fillna(False)].copy()
        ev = ev.dropna(subset=["date", "symbol_norm"])
        ev["cash_start_date"] = ev["date"].map(next_trade)
        idx_map = {d: i for i, d in enumerate(trade_dates)}

        def _cash_end(sd):
            if pd.isna(sd) or sd not in idx_map:
                return pd.NaT
            j = min(idx_map[sd] + CASH_DAYS - 1, len(trade_dates) - 1)
            return trade_dates[j]

        ev["cash_end_date"] = ev["cash_start_date"].map(_cash_end)
        ev["rule_name"] = rule_name

        events_all.append(ev[[
            "rule_name", "date", "symbol", "symbol_norm", "unrealized_return", "ret_5d", "close_to_open_return",
            "next_1d_return", "next_5d_return", "next_20d_return", "cash_start_date", "cash_end_date",
        ]].rename(columns={"date": "trigger_date"}))

        intervals = {(r.symbol_norm, r.cash_start_date, r.cash_end_date) for r in ev.itertuples() if pd.notna(r.cash_start_date) and pd.notna(r.cash_end_date)}
        sim = base.copy()
        sim["cash_flag"] = False
        for sym, sd, ed in intervals:
            m = (sim["symbol_norm"] == sym) & (sim["date"] >= sd) & (sim["date"] <= ed)
            sim.loc[m, "cash_flag"] = True
        sim["is_cash_treated"] = sim["cash_flag"]
        sim["cash_weight_added"] = sim["weight"].where(sim["is_cash_treated"], 0.0)
        sim["adjusted_position_return"] = sim["next_1d_return"].where(~sim["is_cash_treated"], 0.0)
        sim["weighted_adjusted_return"] = sim["weight"] * sim["adjusted_position_return"]
        daily = sim.groupby("date", as_index=False).agg(
            daily_return=("weighted_adjusted_return", "sum"),
            added_cash_weight=("cash_weight_added", "sum"),
            position_count=("symbol_norm", "count"),
        )
        daily = daily.sort_values("date")
        daily["nav"] = INITIAL_EQUITY * (1.0 + daily["daily_return"]).cumprod()
        daily = daily.rename(columns={"daily_return": f"{rule_name}_daily_return", "nav": f"{rule_name}_nav", "added_cash_weight": f"{rule_name}_added_cash_weight"})
        rule_nav_parts.append(daily)

        for period, (s, e) in SPLITS.items():
            part = daily[(daily["date"] >= pd.Timestamp(s)) & (daily["date"] <= pd.Timestamp(e))]
            if part.empty:
                continue
            total = float((1.0 + part[f"{rule_name}_daily_return"]).prod() - 1.0)
            mdd = _mdd(part[f"{rule_name}_daily_return"])
            rec_base_total, rec_base_mdd = _period_metrics(
                aligned.rename(columns={"reconstructed_baseline_daily_return": "daily_return"}),
                s,
                e,
            )
            db_total, db_mdd = _period_metrics(db_nav[["date", "daily_return"]], s, e)
            summary_rows.append({
                "rule_name": rule_name, "period": period, "total_return": total,
                "reconstructed_baseline_return": rec_base_total,
                "excess_vs_reconstructed_baseline": total - rec_base_total,
                "db_reference_baseline_return": db_total,
                "excess_vs_db_reference_baseline": total - db_total,
                "mdd": mdd,
                "reconstructed_baseline_mdd": rec_base_mdd,
                "db_reference_mdd": db_mdd,
                "trigger_count": int(ev[(ev["date"] >= pd.Timestamp(s)) & (ev["date"] <= pd.Timestamp(e))].shape[0]),
                "cash_days": CASH_DAYS,
                "avg_cash_weight_added": float(part[f"{rule_name}_added_cash_weight"].mean()),
                "notes": "research-only proper-ish simulator; trigger+1 then fixed 5-trading-day cash",
            })

    rec_check = []
    rec_nav = aligned[["date", "reconstructed_baseline_daily_return", "db_daily_return"]].rename(
        columns={"reconstructed_baseline_daily_return": "rec_daily_return"}
    )
    for period, (s, e) in SPLITS.items():
        rec_total, rec_mdd = _period_metrics(rec_nav.rename(columns={"rec_daily_return": "daily_return"}), s, e)
        db_total, db_mdd = _period_metrics(rec_nav.rename(columns={"db_daily_return": "daily_return"}), s, e)
        rec_check.append({"period": period, "reconstructed_baseline_return": rec_total, "db_reference_baseline_return": db_total, "return_diff": rec_total - db_total, "reconstructed_mdd": rec_mdd, "db_reference_mdd": db_mdd, "mdd_diff": rec_mdd - db_mdd})

    nav_out = aligned[["date", "reconstructed_baseline_nav"]].copy()
    for d in rule_nav_parts:
        nav_out = nav_out.merge(d, on="date", how="left")

    rec_df = pd.DataFrame(rec_check)
    sum_df = pd.DataFrame(summary_rows)
    evt_df = pd.concat(events_all, ignore_index=True) if events_all else pd.DataFrame()

    rec_path = out_dir / f"exp05_reconstructed_baseline_check_{run_id}.csv"
    sum_path = out_dir / f"exp05_trim_rule_summary_{run_id}.csv"
    nav_path = out_dir / f"exp05_trim_rule_nav_{run_id}.csv"
    evt_path = out_dir / f"exp05_trim_rule_events_{run_id}.csv"
    meta_path = out_dir / f"exp05_metadata_{run_id}.json"

    rec_df.to_csv(rec_path, index=False)
    sum_df.to_csv(sum_path, index=False)
    nav_out.to_csv(nav_path, index=False)
    evt_df.to_csv(evt_path, index=False)

    max_abs_diff = float(rec_df["return_diff"].abs().max()) if not rec_df.empty else 0.0
    untrusted = max_abs_diff >= RECON_WARN_THRESHOLD

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reference_run_id": args.reference_run_id,
        "approximation": "trigger next day -> fixed 5 trading day cash hold for symbol only",
        "reconstruction_warning_threshold": RECON_WARN_THRESHOLD,
        "max_abs_return_diff": max_abs_diff,
        "simulator_trustworthy": not untrusted,
        "notes": ["research-only", "no DB write", "no production/paper trading change", "no pipeline backtest change"],
        "outputs": {"reconstructed_check": str(rec_path), "summary": str(sum_path), "nav": str(nav_path), "events": str(evt_path)},
    }
    meta_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== Exp05 Extreme Winner Trim Rule Proper-ish Small Backtest ===")
    print(f"run_id={run_id}")
    print("[reconstructed baseline vs DB reference]")
    for r in rec_check:
        print(f"{r['period']}: return_diff={r['return_diff']:+.4%}, mdd_diff={r['mdd_diff']:+.4%}")
    if untrusted:
        print("WARNING: 이 simulator는 신뢰 불가")
    for rn in RULES.keys():
        for p in ["main_backtest", "stress_2024_07_09", "full_available_to_2026_04_29"]:
            row = sum_df[(sum_df["rule_name"] == rn) & (sum_df["period"] == p)]
            if row.empty:
                continue
            rr = row.iloc[0]
            print(f"{rn} {p}: total={rr['total_return']:+.2%}, excess_vs_reconstructed={rr['excess_vs_reconstructed_baseline']:+.2%}, mdd={rr['mdd']:+.2%}")
    print("[판단 가이드]")
    print("- main 수익률 훼손 여부 확인")
    print("- MDD 개선 여부 확인")
    print("- 결과가 애매하면 이 track 종료")
    print("- 결과가 좋고 reconstruction error가 작으면 robustness 검증 후보")


if __name__ == "__main__":
    main()
