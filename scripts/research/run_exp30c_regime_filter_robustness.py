from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

PERIODS = {
    "full_2018_to_2026": ("2018-01-02", "2026-12-31"),
    "pre_reference_2018_2021": ("2018-01-02", "2021-12-31"),
    "reference_2022_2026": ("2022-01-01", "2026-12-31"),
    "stress_covid_2020_02_04": ("2020-02-01", "2020-04-30"),
    "stress_2022_full_year": ("2022-01-01", "2022-12-31"),
    "stress_2024_07_09": ("2024-07-01", "2024-09-30"),
    "validation_2026_q1": ("2026-01-01", "2026-03-31"),
    "recent_2026_04": ("2026-04-01", "2026-04-30"),
}


def _latest_file(output_dir: Path, pattern: str) -> Path:
    files = sorted(output_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No file matched: {pattern} in {output_dir}")
    return files[0]


def _to_dt(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.normalize()
    return out.sort_values("date").reset_index(drop=True)


def _max_dd_info(df: pd.DataFrame, equity_col: str) -> dict:
    if df.empty:
        return {"max_drawdown": np.nan, "start": None, "trough": None, "recovery": None, "days_underwater": 0, "worst_10_dates": ""}
    eq = df[equity_col].astype(float)
    peak = eq.cummax()
    dd = eq / peak - 1.0
    idx_trough = dd.idxmin()
    trough_date = df.loc[idx_trough, "date"]
    peak_before = eq.loc[:idx_trough].idxmax()
    start_date = df.loc[peak_before, "date"]
    rec = df.loc[idx_trough:]
    rec_hit = rec[rec[equity_col] >= eq.loc[peak_before]]
    recovery_date = rec_hit.iloc[0]["date"] if not rec_hit.empty else pd.NaT
    days_under = int((dd < 0).sum())
    worst = df.assign(dd=dd).nsmallest(10, "dd")["date"].dt.strftime("%Y-%m-%d").tolist()
    return {
        "max_drawdown": float(dd.min()),
        "start": start_date,
        "trough": trough_date,
        "recovery": recovery_date,
        "days_underwater": days_under,
        "worst_10_dates": ",".join(worst),
    }


def _calc_ret(e: pd.Series) -> float:
    if e.empty:
        return np.nan
    return float(e.iloc[-1] / e.iloc[0] - 1.0)


def _summary_by_freq(sdf: pd.DataFrame, strategy: str, freq: str) -> pd.DataFrame:
    frame = sdf[["date", "equity"]].copy().set_index("date")
    out = []
    for key, grp in frame.groupby(pd.Grouper(freq=freq)):
        if grp.empty:
            continue
        peak = grp["equity"].cummax()
        dd = grp["equity"] / peak - 1
        out.append({"strategy": strategy, "period": str(key.date()), "return": _calc_ret(grp["equity"]), "mdd": float(dd.min())})
    return pd.DataFrame(out)


def _build_long_shadow(shadow_nav: pd.DataFrame) -> pd.DataFrame:
    cols = set(shadow_nav.columns)
    if {"date", "filter_name", "shadow_equity"}.issubset(cols):
        long_df = shadow_nav.copy()
        if "shadow_daily_return" not in long_df.columns:
            long_df["shadow_daily_return"] = long_df.groupby("filter_name")["shadow_equity"].pct_change().fillna(0.0)
        if "signal_raw" not in long_df.columns:
            long_df["signal_raw"] = 0
        return long_df

    if "date" not in cols:
        raise ValueError("shadow NAV CSV must include date column")

    date = shadow_nav[["date"]].copy()
    chunks = []
    for c in shadow_nav.columns:
        if c.endswith("_shadow_equity"):
            name = c[: -len("_shadow_equity")]
            row = date.copy()
            row["filter_name"] = name
            row["shadow_equity"] = shadow_nav[c]
            dr_col = f"{name}_shadow_daily_return"
            sig_col = f"{name}_signal_raw"
            exp_col = f"{name}_exposure_multiplier"
            row["shadow_daily_return"] = shadow_nav[dr_col] if dr_col in shadow_nav.columns else row["shadow_equity"].pct_change().fillna(0.0)
            row["signal_raw"] = shadow_nav[sig_col] if sig_col in shadow_nav.columns else 0
            row["exposure_multiplier"] = shadow_nav[exp_col] if exp_col in shadow_nav.columns else np.nan
            chunks.append(row)
    if not chunks:
        raise ValueError("Could not infer shadow_equity columns from wide schema")
    return pd.concat(chunks, ignore_index=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--shadow-nav-csv", default=None)
    p.add_argument("--shadow-summary-csv", default=None)
    p.add_argument("--daily-metrics-csv", default=None)
    p.add_argument("--output-dir", default="~/krx-stock-persist/reports/research")
    p.add_argument("--primary-filter", default="composite_defensive_off")
    p.add_argument("--secondary-filter", default="composite_defensive_half")
    args = p.parse_args()

    out_dir = Path(args.output_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())

    shadow_nav_csv = Path(args.shadow_nav_csv).expanduser() if args.shadow_nav_csv else _latest_file(out_dir, "exp30b_regime_shadow_nav_*.csv")
    shadow_summary_csv = Path(args.shadow_summary_csv).expanduser() if args.shadow_summary_csv else _latest_file(out_dir, "exp30b_regime_shadow_summary_*.csv")
    daily_metrics_csv = Path(args.daily_metrics_csv).expanduser() if args.daily_metrics_csv else _latest_file(out_dir, "exp30a_regime_daily_metrics_*.csv")
    ranking_csv = _latest_file(out_dir, "exp30b_regime_filter_ranking_*.csv")

    nav_raw = pd.read_csv(shadow_nav_csv)
    nav = _to_dt(_build_long_shadow(nav_raw))

    # baseline from nav file if present
    base_cols = set(nav_raw.columns)
    if {"date", "equity", "daily_return"}.issubset(base_cols) and "filter_name" in base_cols:
        b = nav_raw[["date", "equity", "daily_return"]].drop_duplicates().copy()
    else:
        bf = nav[nav["filter_name"] == "baseline"]
        if bf.empty:
            raise ValueError("Cannot find baseline data")
        b = bf[["date", "shadow_equity", "shadow_daily_return"]].rename(columns={"shadow_equity": "equity", "shadow_daily_return": "daily_return"})
    baseline = _to_dt(b)

    def strategy_df(name: str) -> pd.DataFrame:
        if name == "baseline":
            return baseline[["date", "equity", "daily_return"]].copy()
        s = nav[nav["filter_name"] == name].copy()
        if s.empty:
            raise ValueError(f"filter not found: {name}")
        s = s.rename(columns={"shadow_equity": "equity", "shadow_daily_return": "daily_return"})
        return s[["date", "equity", "daily_return", "signal_raw", "exposure_multiplier"]]

    strategies = {"baseline": strategy_df("baseline"), args.primary_filter: strategy_df(args.primary_filter), args.secondary_filter: strategy_df(args.secondary_filter)}

    yearly = pd.concat([_summary_by_freq(df, k, "Y") for k, df in strategies.items()], ignore_index=True)
    quarterly = pd.concat([_summary_by_freq(df, k, "Q") for k, df in strategies.items()], ignore_index=True)
    monthly = pd.concat([_summary_by_freq(df, k, "M") for k, df in strategies.items()], ignore_index=True)

    dd_rows = []
    for name, df in strategies.items():
        info = _max_dd_info(df, "equity")
        dd_rows.append({"strategy": name, **info})
    drawdown_df = pd.DataFrame(dd_rows)

    primary = strategies[args.primary_filter].copy()
    primary["signal"] = primary.get("signal_raw", pd.Series(0, index=primary.index)).fillna(0).astype(int)
    primary["year"] = primary["date"].dt.year
    primary["quarter"] = primary["date"].dt.to_period("Q").astype(str)
    sig_y = primary.groupby("year")["signal"].agg(signal_days="sum", total_days="count").reset_index()
    sig_y["signal_rate"] = sig_y["signal_days"] / sig_y["total_days"]
    sig_q = primary.groupby("quarter")["signal"].agg(signal_days="sum", total_days="count").reset_index()
    sig_q["signal_rate"] = sig_q["signal_days"] / sig_q["total_days"]
    streak = int(primary["signal"].groupby((primary["signal"] != primary["signal"].shift()).cumsum()).sum().max()) if not primary.empty else 0

    stress_rows = []
    for n, (s, e) in PERIODS.items():
        part = primary[(primary["date"] >= s) & (primary["date"] <= e)]
        stress_rows.append({"bucket": n, "signal_days": int(part["signal"].sum()), "total_days": int(len(part)), "signal_rate": float(part["signal"].mean()) if len(part) else np.nan})

    signal_df = pd.concat([
        sig_y.assign(level="year").rename(columns={"year": "bucket"}),
        sig_q.assign(level="quarter").rename(columns={"quarter": "bucket"}),
        pd.DataFrame(stress_rows).assign(level="stress"),
        pd.DataFrame([{"level": "overall", "bucket": "longest_consecutive_signal_streak", "signal_days": streak, "total_days": np.nan, "signal_rate": np.nan}]),
    ], ignore_index=True)

    # Decision summary (yearly decomposition)
    decisions = []
    baseline_y = _summary_by_freq(strategies["baseline"], "baseline", "Y")
    filter_y = _summary_by_freq(primary, args.primary_filter, "Y")
    by_year = baseline_y.merge(filter_y, on="period", suffixes=("_baseline", "_filter"))
    for _, r in by_year.iterrows():
        y = int(str(r["period"])[:4])
        sy = sig_y[sig_y["year"] == y]
        sr = float(sy.iloc[0]["signal_rate"]) if not sy.empty else np.nan
        excess = r["return_filter"] - r["return_baseline"]
        mdd_diff = r["mdd_filter"] - r["mdd_baseline"]
        note = "benefit" if (excess > 0 and mdd_diff >= 0) else "damage" if excess < 0 else "mixed"
        decisions.append({"year": y, "baseline_return": r["return_baseline"], "filter_return": r["return_filter"], "excess_return": excess, "baseline_mdd": r["mdd_baseline"], "filter_mdd": r["mdd_filter"], "mdd_diff": mdd_diff, "signal_rate": sr, "decision_note": note})

    dec_df = pd.DataFrame(decisions)

    # robustness rule
    ranking = pd.read_csv(ranking_csv)
    row = ranking[ranking["filter_name"] == args.primary_filter].head(1)
    if row.empty:
        raise ValueError("primary filter not found in ranking")
    r = row.iloc[0]
    warn = []
    if r.get("signal_rate", np.nan) > 0.40:
        warn.append("high_signal_rate")
    if (r.get("reference_mdd", np.nan) - (-0.3414)) < -0.10:
        warn.append("reference_mdd_worse_gt_10pp")
    top_year_share = (dec_df["excess_return"].max() / dec_df["excess_return"].clip(lower=0).sum()) if (dec_df["excess_return"].clip(lower=0).sum() > 0) else 0
    if top_year_share > 0.7:
        warn.append("single_year_dependency")
    stress_2024 = dec_df[dec_df["year"] == 2024]
    if not stress_2024.empty and float(stress_2024.iloc[0]["mdd_diff"]) < 0:
        warn.append("stress_2024_mdd_worse")

    candidate = bool(r["full_return"] > 0.9090 and r["full_mdd"] > -0.7620 and r["pre_reference_mdd"] > -0.7616 and r["reference_return"] >= 3.2058)
    reject = bool(r["full_return"] <= 0.9090 or r["pre_reference_mdd"] <= -0.7616 or r["reference_return"] < 3.0)
    status = "candidate" if candidate and not warn else "warning" if warn and not reject else "reject"

    decision_summary = pd.DataFrame([{
        "primary_filter": args.primary_filter,
        "full_return": r["full_return"],
        "full_mdd": r["full_mdd"],
        "pre_reference_mdd": r["pre_reference_mdd"],
        "reference_return": r["reference_return"],
        "signal_rate": r.get("signal_rate", np.nan),
        "warnings": ";".join(warn),
        "robustness_decision": status,
    }])

    outs = {
        "yearly": out_dir / f"exp30c_regime_filter_yearly_summary_{run_id}.csv",
        "quarterly": out_dir / f"exp30c_regime_filter_quarterly_summary_{run_id}.csv",
        "monthly": out_dir / f"exp30c_regime_filter_monthly_summary_{run_id}.csv",
        "drawdown": out_dir / f"exp30c_regime_filter_drawdown_summary_{run_id}.csv",
        "signal": out_dir / f"exp30c_regime_filter_signal_concentration_{run_id}.csv",
        "decision": out_dir / f"exp30c_regime_filter_decision_summary_{run_id}.csv",
        "meta": out_dir / f"exp30c_metadata_{run_id}.json",
    }
    yearly.to_csv(outs["yearly"], index=False)
    quarterly.to_csv(outs["quarterly"], index=False)
    monthly.to_csv(outs["monthly"], index=False)
    drawdown_df.to_csv(outs["drawdown"], index=False)
    signal_df.to_csv(outs["signal"], index=False)
    decision_summary.to_csv(outs["decision"], index=False)

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "input_files": {
            "shadow_nav_csv": str(shadow_nav_csv),
            "shadow_summary_csv": str(shadow_summary_csv),
            "shadow_ranking_csv": str(ranking_csv),
            "daily_metrics_csv": str(daily_metrics_csv),
        },
        "primary_filter": args.primary_filter,
        "secondary_filter": args.secondary_filter,
        "output_paths": {k: str(v) for k, v in outs.items() if k != "meta"},
        "notes": [
            "report-only",
            "no DB write",
            "no strategy change",
            "no backtest rerun",
            "NAV-level approximation inherited from Exp30B",
            "no production/paper trading change",
        ],
    }
    outs["meta"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[Primary filter yearly summary]")
    print(yearly[yearly["strategy"] == args.primary_filter].to_string(index=False))
    print("\n[Primary filter drawdown comparison]")
    print(drawdown_df[drawdown_df["strategy"].isin(["baseline", args.primary_filter])].to_string(index=False))
    print("\n[Signal concentration summary]")
    print(signal_df[signal_df["level"].isin(["year", "stress", "overall"])].to_string(index=False))
    print("\n[Robustness decision]")
    print(decision_summary.to_string(index=False))
    print("\n[Next step]")
    if status == "candidate":
        print("- Exp30D proper simulator / start-window robustness 설계")
    elif status == "warning":
        print("- 보류")
    else:
        print("- regime filter 트랙 종료")


if __name__ == "__main__":
    main()
