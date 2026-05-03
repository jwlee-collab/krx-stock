from __future__ import annotations

import argparse
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


PERIODS = {
    "pre_reference_2018_2021": ("2018-01-02", "2021-12-30"),
    "reference_2022_2026": ("2022-01-04", "2026-04-29"),
    "stress_covid_2020_02_04": ("2020-02-01", "2020-04-30"),
    "stress_2022_full_year": ("2022-01-04", "2022-12-29"),
    "stress_2024_07_09": ("2024-07-01", "2024-09-30"),
    "validation_2026_q1": ("2026-01-02", "2026-03-31"),
    "recent_shadow_2026_04": ("2026-04-01", "2026-04-29"),
}


def _connect_ro(path: str) -> sqlite3.Connection:
    p = Path(path).expanduser()
    conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _latest_file(output_dir: Path, pattern: str) -> Path:
    files = sorted(output_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        raise FileNotFoundError(f"No file matched: {pattern} in {output_dir}")
    return files[0]


def _max_drawdown_series(equity: pd.Series) -> pd.Series:
    rolling_peak = equity.cummax()
    return equity / rolling_peak - 1.0


def _rolling_mdd_from_returns(daily_returns: pd.Series, window: int) -> pd.Series:
    def f(x: np.ndarray) -> float:
        eq = np.cumprod(1.0 + x)
        peak = np.maximum.accumulate(eq)
        dd = eq / peak - 1.0
        return float(np.min(dd))

    return daily_returns.rolling(window, min_periods=window).apply(f, raw=True)


def _period_label(date_str: str) -> str:
    labels = [name for name, (s, e) in PERIODS.items() if s <= date_str <= e]
    return "|".join(labels) if labels else "outside_defined_periods"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--research-db", default="~/krx-stock-persist/data/kospi_495_rolling_2018_2026_research.db")
    parser.add_argument("--output-dir", default="~/krx-stock-persist/reports/research")
    parser.add_argument("--start-date", default="2018-01-02")
    parser.add_argument("--end-date", default="2026-04-29")
    parser.add_argument("--nav-csv", default=None)
    parser.add_argument("--rolling-window", type=int, default=20)
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    out_dir = Path(args.output_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    nav_csv = Path(args.nav_csv).expanduser() if args.nav_csv else _latest_file(out_dir, "exp20b_nav_curve_*.csv")
    _ = _latest_file(out_dir, "exp20b_backtest_summary_*.csv")

    nav = pd.read_csv(nav_csv)
    required_nav_cols = {"date", "equity", "daily_return"}
    missing_nav = required_nav_cols - set(nav.columns)
    if missing_nav:
        raise ValueError(f"NAV csv missing columns: {missing_nav}")
    nav = nav.loc[:, ["date", "equity", "daily_return"]].copy()
    nav = nav[(nav["date"] >= args.start_date) & (nav["date"] <= args.end_date)].sort_values("date").reset_index(drop=True)
    nav["strategy_daily_return"] = nav["daily_return"].astype(float)
    nav["strategy_20d_return"] = nav["equity"].shift(-args.rolling_window) / nav["equity"] - 1.0
    nav["strategy_60d_return"] = nav["equity"].shift(-60) / nav["equity"] - 1.0
    nav["strategy_drawdown"] = _max_drawdown_series(nav["equity"].astype(float))
    nav["strategy_20d_mdd"] = _rolling_mdd_from_returns(nav["strategy_daily_return"], args.rolling_window)
    nav["strategy_60d_mdd"] = _rolling_mdd_from_returns(nav["strategy_daily_return"], 60)

    conn = _connect_ro(args.research_db)
    cols = pd.read_sql_query("PRAGMA table_info(daily_features)", conn)["name"].tolist()
    has_volume_z20 = "volume_z20" in cols

    select_cols = [
        "date", "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap", "range_pct", "volatility_20d",
    ]
    if has_volume_z20:
        select_cols.append("volume_z20")

    query = f"""
    SELECT {", ".join(select_cols)}
    FROM daily_features
    WHERE date BETWEEN ? AND ?
    """
    feat = pd.read_sql_query(query, conn, params=[args.start_date, args.end_date])

    group = feat.groupby("date", sort=True)
    regime = pd.DataFrame({
        "universe_count": group.size(),
        "positive_ret_1d_ratio": group["ret_1d"].apply(lambda x: (x > 0).mean()),
        "positive_ret_5d_ratio": group["ret_5d"].apply(lambda x: (x > 0).mean()),
        "positive_momentum_20d_ratio": group["momentum_20d"].apply(lambda x: (x > 0).mean()),
        "positive_momentum_60d_ratio": group["momentum_60d"].apply(lambda x: (x > 0).mean()),
        "above_sma20_ratio": group["sma_20_gap"].apply(lambda x: (x > 0).mean()),
        "above_sma60_ratio": group["sma_60_gap"].apply(lambda x: (x > 0).mean()),
        "median_ret_5d": group["ret_5d"].median(),
        "median_momentum_20d": group["momentum_20d"].median(),
        "median_momentum_60d": group["momentum_60d"].median(),
        "median_sma_20_gap": group["sma_20_gap"].median(),
        "median_sma_60_gap": group["sma_60_gap"].median(),
        "median_volatility_20d": group["volatility_20d"].median(),
        "median_range_pct": group["range_pct"].median(),
        "mean_volatility_20d": group["volatility_20d"].mean(),
        "mean_range_pct": group["range_pct"].mean(),
        "high_vol_ratio": group["volatility_20d"].apply(lambda x: (x >= 0.06).mean()),
        "high_range_ratio": group["range_pct"].apply(lambda x: (x >= 0.12).mean()),
    }).reset_index()

    if has_volume_z20:
        regime["volume_z20_median"] = group["volume_z20"].median().values
        regime["volume_z20_high_ratio"] = group["volume_z20"].apply(lambda x: (x >= 2).mean()).values

    daily = regime.merge(nav[[
        "date", "strategy_daily_return", "strategy_20d_return", "strategy_60d_return", "strategy_drawdown", "strategy_20d_mdd", "strategy_60d_mdd",
    ]], on="date", how="inner").sort_values("date").reset_index(drop=True)

    daily["period"] = daily["date"].apply(_period_label)
    daily["bad_forward_20d"] = daily["strategy_20d_return"] <= -0.10
    daily["good_forward_20d"] = daily["strategy_20d_return"] >= 0.10
    daily["severe_drawdown_state"] = daily["strategy_drawdown"] <= -0.30
    daily["good_bad_group"] = np.where(daily["bad_forward_20d"], "bad_forward_20d", np.where(daily["good_forward_20d"], "good_forward_20d", "neutral"))

    period_rows = []
    for period_name in PERIODS:
        part = daily[daily["period"].str.contains(period_name)]
        if part.empty:
            continue
        period_rows.append({
            "period": period_name,
            "row_count": int(len(part)),
            "avg_strategy_daily_return": part["strategy_daily_return"].mean(),
            "avg_strategy_20d_return": part["strategy_20d_return"].mean(),
            "avg_strategy_drawdown": part["strategy_drawdown"].mean(),
            "avg_positive_momentum_20d_ratio": part["positive_momentum_20d_ratio"].mean(),
            "avg_positive_momentum_60d_ratio": part["positive_momentum_60d_ratio"].mean(),
            "avg_above_sma20_ratio": part["above_sma20_ratio"].mean(),
            "avg_above_sma60_ratio": part["above_sma60_ratio"].mean(),
            "avg_median_momentum_20d": part["median_momentum_20d"].mean(),
            "avg_median_momentum_60d": part["median_momentum_60d"].mean(),
            "avg_median_volatility_20d": part["median_volatility_20d"].mean(),
            "avg_high_vol_ratio": part["high_vol_ratio"].mean(),
            "avg_high_range_ratio": part["high_range_ratio"].mean(),
        })
    period_summary = pd.DataFrame(period_rows)

    metric_cols = [c for c in regime.columns if c != "date"] + ["strategy_daily_return", "strategy_20d_return", "strategy_60d_return", "strategy_drawdown"]
    contrast_rows = []
    for grp, part in daily.groupby("good_bad_group"):
        rec = {"group": grp, "row_count": int(len(part))}
        for m in metric_cols:
            rec[f"avg_{m}"] = part[m].mean()
            rec[f"median_{m}"] = part[m].median()
        contrast_rows.append(rec)
    contrast = pd.DataFrame(contrast_rows)

    roll_med = daily["median_volatility_20d"].rolling(args.rolling_window, min_periods=args.rolling_window).median()
    conditions = {
        "positive_momentum_20d_ratio_lt_0_40": daily["positive_momentum_20d_ratio"] < 0.40,
        "positive_momentum_20d_ratio_lt_0_30": daily["positive_momentum_20d_ratio"] < 0.30,
        "above_sma20_ratio_lt_0_40": daily["above_sma20_ratio"] < 0.40,
        "above_sma20_ratio_lt_0_30": daily["above_sma20_ratio"] < 0.30,
        "above_sma60_ratio_lt_0_40": daily["above_sma60_ratio"] < 0.40,
        "median_momentum_20d_lt_0": daily["median_momentum_20d"] < 0,
        "median_momentum_60d_lt_0": daily["median_momentum_60d"] < 0,
        "median_volatility_20d_gt_rolling_median": daily["median_volatility_20d"] > roll_med,
        "high_vol_ratio_gt_0_30": daily["high_vol_ratio"] > 0.30,
    }
    scan_rows = []
    for name, cond in conditions.items():
        part = daily[cond.fillna(False)]
        n = len(part)
        scan_rows.append({
            "condition_name": name,
            "trigger_days": int(n),
            "trigger_rate": float(n / len(daily)) if len(daily) else np.nan,
            "avg_strategy_next_20d_return": part["strategy_20d_return"].mean() if n else np.nan,
            "bad_forward_20d_rate": part["bad_forward_20d"].mean() if n else np.nan,
            "good_forward_20d_rate": part["good_forward_20d"].mean() if n else np.nan,
            "avg_strategy_drawdown": part["strategy_drawdown"].mean() if n else np.nan,
            "notes": "report-only threshold diagnostic; not used as live market filter",
        })
    scan = pd.DataFrame(scan_rows).sort_values(["bad_forward_20d_rate", "avg_strategy_next_20d_return"], ascending=[False, True])

    daily_path = out_dir / f"exp30a_regime_daily_metrics_{run_id}.csv"
    period_path = out_dir / f"exp30a_regime_period_summary_{run_id}.csv"
    contrast_path = out_dir / f"exp30a_regime_good_bad_contrast_{run_id}.csv"
    scan_path = out_dir / f"exp30a_regime_threshold_scan_{run_id}.csv"
    meta_path = out_dir / f"exp30a_metadata_{run_id}.json"

    daily.to_csv(daily_path, index=False)
    period_summary.to_csv(period_path, index=False)
    contrast.to_csv(contrast_path, index=False)
    scan.to_csv(scan_path, index=False)

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_db": str(Path(args.research_db).expanduser()),
        "nav_csv": str(nav_csv),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "selected_universe_metrics": [c for c in regime.columns if c != "date"],
        "labels_definition": {
            "periods": PERIODS,
            "bad_forward_20d": "strategy_20d_return <= -0.10",
            "good_forward_20d": "strategy_20d_return >= 0.10",
            "severe_drawdown_state": "strategy_drawdown <= -0.30",
        },
        "output_paths": {
            "daily_metrics": str(daily_path),
            "period_summary": str(period_path),
            "good_bad_contrast": str(contrast_path),
            "threshold_scan": str(scan_path),
        },
        "notes": [
            "report-only",
            "no DB write",
            "no strategy change",
            "no backtest rerun",
            "good/bad labels use future returns for diagnosis only",
            "no production/paper trading change",
        ],
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    pre = period_summary[period_summary["period"] == "pre_reference_2018_2021"]
    ref = period_summary[period_summary["period"] == "reference_2022_2026"]
    print(f"[Daily metrics row count] {len(daily)}")
    if not pre.empty and not ref.empty:
        pre_r = pre.iloc[0]
        ref_r = ref.iloc[0]
        print("[Pre vs Reference]")
        print(f"- avg_positive_momentum_20d_ratio: pre={pre_r['avg_positive_momentum_20d_ratio']:.4f}, ref={ref_r['avg_positive_momentum_20d_ratio']:.4f}")
        print(f"- avg_above_sma20_ratio: pre={pre_r['avg_above_sma20_ratio']:.4f}, ref={ref_r['avg_above_sma20_ratio']:.4f}")
        print(f"- avg_median_volatility_20d: pre={pre_r['avg_median_volatility_20d']:.4f}, ref={ref_r['avg_median_volatility_20d']:.4f}")
    print("[2018~2021 약세 구간 두드러진 metrics]")
    if not pre.empty:
        print(pre.to_dict(orient="records")[0])
    print("[2022 이후 강세 구간 두드러진 metrics]")
    if not ref.empty:
        print(ref.to_dict(orient="records")[0])
    print("[Threshold scan top candidates]")
    print(scan.head(5).to_string(index=False))
    top_bad_rate = scan["bad_forward_20d_rate"].max(skipna=True)
    if pd.notna(top_bad_rate) and top_bad_rate >= 0.55:
        print("[다음 단계 추천] 명확한 regime signal 후보가 있어 Exp30B regime shadow filter report-only를 권장합니다.")
    else:
        print("[다음 단계 추천] 명확한 regime signal이 부족해 regime detection 보류 후 simulator/attribution으로 이동을 권장합니다.")


if __name__ == "__main__":
    main()
