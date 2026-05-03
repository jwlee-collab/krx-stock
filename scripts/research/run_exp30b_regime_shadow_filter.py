from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PERIODS = {
    "full_2018_to_2026_04_29": ("2018-01-02", "2026-04-29"),
    "pre_reference_2018_2021": ("2018-01-02", "2021-12-30"),
    "reference_overlap_2022_2026_04_29": ("2022-01-04", "2026-04-29"),
    "main_backtest_2022_2025": ("2022-01-04", "2025-12-30"),
    "validation_2026_q1": ("2026-01-02", "2026-03-31"),
    "recent_shadow_2026_04": ("2026-04-01", "2026-04-29"),
    "stress_covid_2020_02_2020_04": ("2020-02-01", "2020-04-30"),
    "stress_2022_full_year": ("2022-01-04", "2022-12-29"),
    "stress_2024_07_09": ("2024-07-01", "2024-09-30"),
}


def _latest_file(output_dir: Path, pattern: str) -> Path:
    files = sorted(output_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No file matched: {pattern} in {output_dir}")
    return files[0]


def _max_drawdown_series(equity: pd.Series) -> pd.Series:
    peak = equity.cummax()
    return equity / peak - 1.0


def _period_slice(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    return df[(df["date"] >= start) & (df["date"] <= end)].copy()


def _calc_total_return(equity: pd.Series) -> float:
    if equity.empty:
        return np.nan
    return float(equity.iloc[-1] / equity.iloc[0] - 1.0)


def _decision_hint(
    full_excess: float,
    mdd_improve_pre: float,
    reference_excess: float,
    signal_rate: float,
) -> str:
    if np.isnan(signal_rate):
        return "insufficient_data"
    if signal_rate < 0.01:
        return "weak/rare signal"
    if signal_rate > 0.35 and full_excess < -0.05:
        return "too aggressive"
    if full_excess < -0.08 and mdd_improve_pre < 0.03:
        return "reject"
    if mdd_improve_pre >= 0.05 and reference_excess >= -0.03:
        return "candidate"
    return "mixed"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--daily-metrics-csv", default=None)
    parser.add_argument("--nav-csv", default=None)
    parser.add_argument("--output-dir", default="~/krx-stock-persist/reports/research")
    parser.add_argument("--start-date", default="2018-01-02")
    parser.add_argument("--end-date", default="2026-04-29")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    out_dir = Path(args.output_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    daily_csv = Path(args.daily_metrics_csv).expanduser() if args.daily_metrics_csv else _latest_file(out_dir, "exp30a_regime_daily_metrics_*.csv")
    nav_csv = Path(args.nav_csv).expanduser() if args.nav_csv else _latest_file(out_dir, "exp20b_nav_curve_*.csv")

    daily = pd.read_csv(daily_csv)
    nav = pd.read_csv(nav_csv)

    required_daily = {
        "date", "high_vol_ratio", "positive_momentum_20d_ratio", "above_sma20_ratio", "positive_momentum_60d_ratio", "above_sma60_ratio"
    }
    required_nav = {"date", "equity", "daily_return"}
    if miss := (required_daily - set(daily.columns)):
        raise ValueError(f"daily metrics csv missing columns: {miss}")
    if miss := (required_nav - set(nav.columns)):
        raise ValueError(f"nav csv missing columns: {miss}")

    base = nav[["date", "equity", "daily_return"]].copy()
    base["daily_return"] = base["daily_return"].astype(float)
    base["equity"] = base["equity"].astype(float)
    base = base[(base["date"] >= args.start_date) & (base["date"] <= args.end_date)].sort_values("date").reset_index(drop=True)
    base["baseline_cumulative_return"] = base["equity"] / base["equity"].iloc[0] - 1.0
    base["baseline_mdd"] = _max_drawdown_series(base["equity"])

    feat = daily[list(required_daily)].copy()
    merged = base.merge(feat, on="date", how="left")

    filters = {
        "crash_high_vol_off": (lambda df: df["high_vol_ratio"] > 0.30, 0.0),
        "crash_high_vol_half": (lambda df: df["high_vol_ratio"] > 0.30, 0.5),
        "breadth_weak_half": (lambda df: (df["positive_momentum_20d_ratio"] < 0.30) | (df["above_sma20_ratio"] < 0.30), 0.5),
        "long_breadth_weak_half": (lambda df: (df["positive_momentum_60d_ratio"] < 0.35) | (df["above_sma60_ratio"] < 0.35), 0.5),
        "composite_defensive_half": (
            lambda df: (
                (df["high_vol_ratio"] > 0.30)
                | ((df["positive_momentum_20d_ratio"] < 0.30) & (df["above_sma20_ratio"] < 0.35))
                | ((df["positive_momentum_60d_ratio"] < 0.35) & (df["above_sma60_ratio"] < 0.40))
            ),
            0.5,
        ),
        "composite_defensive_off": (
            lambda df: (
                (df["high_vol_ratio"] > 0.30)
                | ((df["positive_momentum_20d_ratio"] < 0.30) & (df["above_sma20_ratio"] < 0.35))
                | ((df["positive_momentum_60d_ratio"] < 0.35) & (df["above_sma60_ratio"] < 0.40))
            ),
            0.0,
        ),
    }

    nav_rows: list[pd.DataFrame] = []
    summary_rows: list[dict] = []
    ranking_rows: list[dict] = []
    signal_days_rows: list[dict] = []

    for filter_name, (signal_fn, defensive_mult) in filters.items():
        df = merged[["date", "equity", "daily_return", "baseline_cumulative_return", "baseline_mdd", "high_vol_ratio", "positive_momentum_20d_ratio", "above_sma20_ratio", "positive_momentum_60d_ratio", "above_sma60_ratio"]].copy()
        df["filter_name"] = filter_name
        df["signal_raw"] = signal_fn(df).fillna(False).astype(int)
        df["signal_applied_next_day"] = df["signal_raw"].shift(1).fillna(0).astype(int)
        df["exposure_multiplier"] = np.where(df["signal_applied_next_day"] == 1, defensive_mult, 1.0)
        df["shadow_daily_return"] = df["daily_return"] * df["exposure_multiplier"]
        df["shadow_equity"] = (1.0 + df["shadow_daily_return"]).cumprod() * df["equity"].iloc[0]
        df["shadow_mdd"] = _max_drawdown_series(df["shadow_equity"])
        df["added_cash_exposure"] = 1.0 - df["exposure_multiplier"]
        nav_rows.append(df[["date", "filter_name", "equity", "daily_return", "baseline_cumulative_return", "baseline_mdd", "signal_raw", "signal_applied_next_day", "exposure_multiplier", "shadow_daily_return", "shadow_equity", "shadow_mdd", "added_cash_exposure"]])

        for period, (start, end) in PERIODS.items():
            part = _period_slice(df, start, end)
            if part.empty:
                continue
            baseline_part = _period_slice(base, start, end)
            summary_rows.append({
                "filter_name": filter_name,
                "period": period,
                "total_return": _calc_total_return(part["shadow_equity"]),
                "baseline_return": _calc_total_return(baseline_part["equity"]),
                "excess_return_vs_baseline": _calc_total_return(part["shadow_equity"]) - _calc_total_return(baseline_part["equity"]),
                "mdd": float(part["shadow_mdd"].min()),
                "baseline_mdd": float(baseline_part["baseline_mdd"].min()),
                "mdd_diff_vs_baseline": float(part["shadow_mdd"].min() - baseline_part["baseline_mdd"].min()),
                "signal_days": int(part["signal_raw"].sum()),
                "signal_rate": float(part["signal_raw"].mean()),
                "avg_exposure_multiplier": float(part["exposure_multiplier"].mean()),
                "min_exposure_multiplier": float(part["exposure_multiplier"].min()),
                "avg_added_cash_exposure": float(part["added_cash_exposure"].mean()),
                "notes": "report-only NAV-level approximation; signals shifted +1 trading day",
            })

        for _, row in df[df["signal_raw"] == 1].iterrows():
            signal_days_rows.append({"filter_name": filter_name, "date": row["date"], "signal_raw": 1, "signal_applied_next_day": int(row["signal_applied_next_day"])})

    summary_df = pd.DataFrame(summary_rows)

    def get_metric(f: str, period: str, col: str) -> float:
        sub = summary_df[(summary_df["filter_name"] == f) & (summary_df["period"] == period)]
        return float(sub.iloc[0][col]) if not sub.empty else np.nan

    base_full = _period_slice(base, *PERIODS["full_2018_to_2026_04_29"])
    base_pre = _period_slice(base, *PERIODS["pre_reference_2018_2021"])
    base_ref = _period_slice(base, *PERIODS["reference_overlap_2022_2026_04_29"])
    base_covid = _period_slice(base, *PERIODS["stress_covid_2020_02_2020_04"])

    for fname in filters:
        full_return = get_metric(fname, "full_2018_to_2026_04_29", "total_return")
        full_mdd = get_metric(fname, "full_2018_to_2026_04_29", "mdd")
        pre_return = get_metric(fname, "pre_reference_2018_2021", "total_return")
        pre_mdd = get_metric(fname, "pre_reference_2018_2021", "mdd")
        ref_return = get_metric(fname, "reference_overlap_2022_2026_04_29", "total_return")
        ref_mdd = get_metric(fname, "reference_overlap_2022_2026_04_29", "mdd")
        covid_return = get_metric(fname, "stress_covid_2020_02_2020_04", "total_return")
        covid_mdd = get_metric(fname, "stress_covid_2020_02_2020_04", "mdd")
        signal_rate = get_metric(fname, "full_2018_to_2026_04_29", "signal_rate")
        avg_exp = get_metric(fname, "full_2018_to_2026_04_29", "avg_exposure_multiplier")

        ranking_rows.append({
            "filter_name": fname,
            "full_return": full_return,
            "full_mdd": full_mdd,
            "pre_reference_return": pre_return,
            "pre_reference_mdd": pre_mdd,
            "reference_return": ref_return,
            "reference_mdd": ref_mdd,
            "covid_stress_return": covid_return,
            "covid_stress_mdd": covid_mdd,
            "stress_2022_return": get_metric(fname, "stress_2022_full_year", "total_return"),
            "stress_2024_return": get_metric(fname, "stress_2024_07_09", "total_return"),
            "signal_rate": signal_rate,
            "avg_exposure_multiplier": avg_exp,
            "decision_hint": _decision_hint(
                full_return - _calc_total_return(base_full["equity"]),
                _period_slice(base_pre.assign(base_mdd=base_pre["baseline_mdd"]), *PERIODS["pre_reference_2018_2021"])["base_mdd"].min() - pre_mdd,
                ref_return - _calc_total_return(base_ref["equity"]),
                signal_rate,
            ),
        })

    nav_out = pd.concat(nav_rows, ignore_index=True)
    ranking_df = pd.DataFrame(ranking_rows).sort_values(["decision_hint", "full_return"], ascending=[True, False])
    signal_days_df = pd.DataFrame(signal_days_rows)

    paths = {
        "shadow_nav": out_dir / f"exp30b_regime_shadow_nav_{run_id}.csv",
        "summary": out_dir / f"exp30b_regime_shadow_summary_{run_id}.csv",
        "ranking": out_dir / f"exp30b_regime_filter_ranking_{run_id}.csv",
        "signal_days": out_dir / f"exp30b_regime_signal_days_{run_id}.csv",
        "metadata": out_dir / f"exp30b_metadata_{run_id}.json",
    }
    nav_out.to_csv(paths["shadow_nav"], index=False)
    summary_df.to_csv(paths["summary"], index=False)
    ranking_df.to_csv(paths["ranking"], index=False)
    signal_days_df.to_csv(paths["signal_days"], index=False)

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "input_daily_metrics_csv": str(daily_csv),
        "input_nav_csv": str(nav_csv),
        "filter_definitions": {
            k: {"defensive_multiplier": m} for k, (_, m) in filters.items()
        },
        "shift_rule": "signals applied next trading day (signal columns shifted by +1 trading day)",
        "output_paths": {k: str(v) for k, v in paths.items() if k != "metadata"},
        "notes": [
            "report-only",
            "no DB write",
            "no strategy change",
            "no backtest rerun",
            "NAV-level approximation",
            "no production/paper trading change",
        ],
    }
    paths["metadata"].write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[Baseline returns]")
    print(f"- full_2018_to_2026_04_29: {_calc_total_return(base_full['equity']):.4f}")
    print(f"- pre_reference_2018_2021: {_calc_total_return(base_pre['equity']):.4f}")
    print(f"- reference_overlap_2022_2026_04_29: {_calc_total_return(base_ref['equity']):.4f}")
    print(f"- stress_covid_2020_02_2020_04: {_calc_total_return(base_covid['equity']):.4f}")

    print("\n[Filter ranking table]")
    print(ranking_df[["filter_name", "full_return", "full_mdd", "pre_reference_mdd", "reference_return", "signal_rate", "decision_hint"]].to_string(index=False))

    pre_mdd_improve = []
    ref_return_damage = []
    baseline_pre_mdd = float(base_pre["baseline_mdd"].min()) if not base_pre.empty else np.nan
    baseline_ref_return = _calc_total_return(base_ref["equity"])
    for _, r in ranking_df.iterrows():
        pre_mdd_improve.append((r["filter_name"], baseline_pre_mdd - r["pre_reference_mdd"]))
        ref_return_damage.append((r["filter_name"], baseline_ref_return - r["reference_return"]))
    best_mdd = max(pre_mdd_improve, key=lambda x: x[1]) if pre_mdd_improve else (None, np.nan)
    least_damage = min(ref_return_damage, key=lambda x: x[1]) if ref_return_damage else (None, np.nan)
    print(f"\n[Best pre_reference MDD improvement] {best_mdd[0]} (improvement={best_mdd[1]:.4f})")
    print(f"[Least reference return damage] {least_damage[0]} (damage={least_damage[1]:.4f})")

    has_candidate = (ranking_df["decision_hint"] == "candidate").any()
    if has_candidate:
        print("[추천] 유망 filter가 있어 Exp30C robustness report-only 진행을 권장합니다.")
    else:
        print("[추천] 유망 filter가 부족해 regime filter 보류, simulator/attribution으로 이동을 권장합니다.")


if __name__ == "__main__":
    main()
