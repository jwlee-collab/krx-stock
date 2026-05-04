from __future__ import annotations

import argparse
import json
import sqlite3
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

HOLDING_COLUMNS = ["date", "symbol", "weight", "entry_date", "entry_price", "close", "unrealized_return", "rank", "score"]
RESULT_COLUMNS = ["date", "equity", "daily_return", "exposure", "position_count", "cash_weight", "max_single_position_weight"]


def table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]


def has_table(con: sqlite3.Connection, table: str) -> bool:
    q = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?"
    return con.execute(q, (table,)).fetchone() is not None


def pick_columns(existing: list[str], required: list[str]) -> tuple[list[str], list[str]]:
    selected = [c for c in required if c in existing]
    unavailable = [c for c in required if c not in existing]
    return selected, unavailable


def assign_periods(dates: pd.Series) -> pd.Series:
    labels: list[str] = []
    for d in dates:
        matched = [name for name, (s, e) in PERIODS.items() if pd.Timestamp(s) <= d <= pd.Timestamp(e)]
        labels.append("|".join(matched) if matched else "outside_defined_periods")
    return pd.Series(labels, index=dates.index)


def contribution_stats(series: pd.Series) -> dict:
    s = series.dropna()
    pos = s[s > 0].sort_values(ascending=False)
    neg = (-s[s < 0]).sort_values(ascending=False)

    def top_share(vals: pd.Series, n: int) -> float:
        return float(vals.head(n).sum() / vals.sum()) if len(vals) and vals.sum() > 0 else np.nan

    def num_for_half(vals: pd.Series) -> int:
        if len(vals) == 0 or vals.sum() <= 0:
            return 0
        return int((vals.cumsum() <= 0.5 * vals.sum()).sum() + 1)

    return {
        "top5_positive_share": top_share(pos, 5),
        "top5_negative_share": top_share(neg, 5),
        "symbols_for_50pct_positive": num_for_half(pos),
        "symbols_for_50pct_negative": num_for_half(neg),
    }


def top_winners_losers(symbol_sum: pd.Series, period: str) -> pd.DataFrame:
    pos = symbol_sum.sort_values(ascending=False).head(10).reset_index()
    pos.columns = ["symbol", "sum_position_contribution"]
    pos["bucket"] = period
    pos["side"] = "positive"

    neg = symbol_sum.sort_values(ascending=True).head(10).reset_index()
    neg.columns = ["symbol", "sum_position_contribution"]
    neg["bucket"] = period
    neg["side"] = "negative"
    return pd.concat([pos, neg], ignore_index=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--research-db", default="~/krx-stock-persist/data/kospi_495_rolling_2018_2026_research.db")
    p.add_argument("--output-dir", default="~/krx-stock-persist/reports/research")
    p.add_argument("--run-id", default=None)
    p.add_argument("--start-date", default="2018-01-02")
    p.add_argument("--end-date", default="2026-04-29")
    args = p.parse_args()

    db_path = Path(args.research_db).expanduser()
    out_dir = Path(args.output_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata: dict = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "research_db": str(db_path),
        "date_range": {"start_date": args.start_date, "end_date": args.end_date},
        "selected_columns": {},
        "unavailable_columns": {},
        "row_counts": {},
        "approximation_notes": [
            "contribution is approximate and uses weight * next close return",
            "position_contribution_next_day approximates next-day portfolio daily_return contribution",
        ],
        "notes": [
            "report-only",
            "no DB write",
            "no strategy change",
            "no backtest rerun",
            "contribution is approximate, based on weight * next close return",
            "no production/paper trading change",
        ],
    }

    with sqlite3.connect(db_path) as con:
        run_id = args.run_id
        if not run_id:
            row = con.execute("SELECT run_id FROM backtest_runs ORDER BY created_at DESC LIMIT 1").fetchone()
            if row is None:
                raise ValueError("No run_id found from backtest_runs")
            run_id = str(row[0])

        metadata["run_id"] = run_id

        h_cols_all = table_columns(con, "backtest_holdings")
        h_sel, h_unavail = pick_columns(h_cols_all, HOLDING_COLUMNS)
        if "date" not in h_sel or "symbol" not in h_sel or "weight" not in h_sel:
            raise ValueError("backtest_holdings must include date/symbol/weight columns")
        h_sql = f"SELECT {', '.join(h_sel)} FROM backtest_holdings WHERE run_id=? AND date BETWEEN ? AND ?"
        holdings = pd.read_sql_query(h_sql, con, params=(run_id, args.start_date, args.end_date))

        r_cols_all = table_columns(con, "backtest_results")
        r_sel, r_unavail = pick_columns(r_cols_all, RESULT_COLUMNS)
        if "date" not in r_sel:
            raise ValueError("backtest_results must include date")
        r_sql = f"SELECT {', '.join(r_sel)} FROM backtest_results WHERE run_id=? AND date BETWEEN ? AND ?"
        results = pd.read_sql_query(r_sql, con, params=(run_id, args.start_date, args.end_date))

        metadata["selected_columns"]["backtest_holdings"] = h_sel
        metadata["selected_columns"]["backtest_results"] = r_sel
        metadata["unavailable_columns"]["backtest_holdings"] = h_unavail
        metadata["unavailable_columns"]["backtest_results"] = r_unavail

        risk_events = pd.DataFrame()
        risk_unavailable_reason = None
        if has_table(con, "backtest_risk_events"):
            risk_cols = table_columns(con, "backtest_risk_events")
            risk_sel = [c for c in ["date", "symbol", "event_type"] if c in risk_cols]
            if "date" in risk_sel:
                risk_sql = f"SELECT {', '.join(risk_sel)} FROM backtest_risk_events WHERE run_id=? AND date BETWEEN ? AND ?"
                risk_events = pd.read_sql_query(risk_sql, con, params=(run_id, args.start_date, args.end_date))
            else:
                risk_unavailable_reason = "backtest_risk_events exists but date column missing"
        else:
            risk_unavailable_reason = "backtest_risk_events table unavailable"

    holdings["date"] = pd.to_datetime(holdings["date"]).dt.normalize()
    if "entry_date" in holdings.columns:
        holdings["entry_date"] = pd.to_datetime(holdings["entry_date"], errors="coerce").dt.normalize()

    for c in ["weight", "close", "unrealized_return", "rank", "score"]:
        if c in holdings.columns:
            holdings[c] = pd.to_numeric(holdings[c], errors="coerce")

    holdings = holdings.sort_values(["symbol", "date"]).reset_index(drop=True)
    holdings["next_close"] = holdings.groupby("symbol")["close"].shift(-1) if "close" in holdings.columns else np.nan
    holdings["symbol_next_1d_return"] = holdings["next_close"] / holdings["close"] - 1.0 if "close" in holdings.columns else np.nan
    holdings["position_contribution_next_day"] = holdings["weight"] * holdings["symbol_next_1d_return"]
    holdings["period_label"] = assign_periods(holdings["date"])

    results["date"] = pd.to_datetime(results["date"]).dt.normalize()
    res_pos_count = results[["date", "position_count"]] if "position_count" in results.columns else pd.DataFrame(columns=["date", "position_count"])
    snapshot = holdings.merge(res_pos_count, on="date", how="left")

    symbol_grp = snapshot.groupby("symbol", dropna=False)
    symbol_summary = symbol_grp.agg(
        holding_days=("date", "count"),
        first_date=("date", "min"),
        last_date=("date", "max"),
        avg_weight=("weight", "mean"),
        avg_rank=("rank", "mean"),
        avg_score=("score", "mean"),
        avg_unrealized_return=("unrealized_return", "mean"),
        max_unrealized_return=("unrealized_return", "max"),
        min_unrealized_return=("unrealized_return", "min"),
        sum_position_contribution=("position_contribution_next_day", "sum"),
        avg_position_contribution=("position_contribution_next_day", "mean"),
    ).reset_index()
    symbol_summary["positive_contribution_days"] = symbol_grp["position_contribution_next_day"].apply(lambda s: int((s > 0).sum())).values
    symbol_summary["negative_contribution_days"] = symbol_grp["position_contribution_next_day"].apply(lambda s: int((s < 0).sum())).values
    symbol_summary["periods_seen"] = symbol_grp["period_label"].apply(lambda s: "|".join(sorted(set("|".join(s.fillna("")).split("|")) - {""}))).values

    period_rows = []
    for p_name, p_df in snapshot.groupby("period_label"):
        ssum = p_df.groupby("symbol")["position_contribution_next_day"].sum()
        pstats = contribution_stats(ssum)
        period_rows.append(
            {
                "period": p_name,
                "row_count": int(len(p_df)),
                "unique_symbols": int(p_df["symbol"].nunique()),
                "avg_position_count": float(p_df["position_count"].mean()) if "position_count" in p_df else np.nan,
                "avg_weight": float(p_df["weight"].mean()),
                "avg_rank": float(p_df["rank"].mean()) if "rank" in p_df else np.nan,
                "avg_score": float(p_df["score"].mean()) if "score" in p_df else np.nan,
                "avg_unrealized_return": float(p_df["unrealized_return"].mean()) if "unrealized_return" in p_df else np.nan,
                "sum_position_contribution": float(p_df["position_contribution_next_day"].sum()),
                "avg_position_contribution": float(p_df["position_contribution_next_day"].mean()),
                "top_positive_symbol": ssum.idxmax() if len(ssum) else None,
                "top_negative_symbol": ssum.idxmin() if len(ssum) else None,
                "concentration_top5_positive_contribution": pstats["top5_positive_share"],
                "concentration_top5_negative_contribution": pstats["top5_negative_share"],
            }
        )
    period_summary = pd.DataFrame(period_rows)

    winners_losers = [top_winners_losers(symbol_summary.set_index("symbol")["sum_position_contribution"], "overall")]
    conc_rows = []
    overall_stats = contribution_stats(symbol_summary.set_index("symbol")["sum_position_contribution"])
    conc_rows.append({"bucket": "overall", **overall_stats})
    for p_name, p_df in snapshot.groupby("period_label"):
        ssum = p_df.groupby("symbol")["position_contribution_next_day"].sum()
        winners_losers.append(top_winners_losers(ssum, p_name))
        conc_rows.append({"bucket": p_name, **contribution_stats(ssum)})
    winners_losers_df = pd.concat(winners_losers, ignore_index=True)
    concentration_summary = pd.DataFrame(conc_rows)

    regime_rows = []
    for name in ["pre_reference_2018_2021", "reference_2022_2026"]:
        sub = snapshot[snapshot["period_label"].str.contains(name, na=False)]
        ssum = sub.groupby("symbol")["position_contribution_next_day"].sum() if not sub.empty else pd.Series(dtype=float)
        cstats = contribution_stats(ssum)
        regime_rows.append(
            {
                "regime": name,
                "unique_symbols": int(sub["symbol"].nunique()) if not sub.empty else 0,
                "avg_rank": float(sub["rank"].mean()) if "rank" in sub else np.nan,
                "avg_score": float(sub["score"].mean()) if "score" in sub else np.nan,
                "avg_unrealized_return": float(sub["unrealized_return"].mean()) if "unrealized_return" in sub else np.nan,
                "sum_position_contribution": float(sub["position_contribution_next_day"].sum()) if not sub.empty else 0.0,
                "top_winners": "|".join(ssum.sort_values(ascending=False).head(5).index.tolist()),
                "top_losers": "|".join(ssum.sort_values(ascending=True).head(5).index.tolist()),
                "top5_positive_share": cstats["top5_positive_share"],
                "top5_negative_share": cstats["top5_negative_share"],
            }
        )
    regime_comparison = pd.DataFrame(regime_rows)

    risk_summary_rows = []
    if not risk_events.empty:
        risk_events["date"] = pd.to_datetime(risk_events["date"]).dt.normalize()
        risk_events["period_label"] = assign_periods(risk_events["date"])
        if "event_type" in risk_events.columns:
            risk_summary_rows.extend([{"scope": "event_type", "key": k, "count": int(v)} for k, v in risk_events["event_type"].value_counts().items()])
        risk_summary_rows.extend([{"scope": "period", "key": k, "count": int(v)} for k, v in risk_events["period_label"].value_counts().items()])
        if "symbol" in risk_events.columns:
            risk_summary_rows.extend([{"scope": "symbol", "key": k, "count": int(v)} for k, v in risk_events["symbol"].value_counts().head(100).items()])

            px = holdings[["symbol", "date", "close"]].dropna().sort_values(["symbol", "date"]).copy()
            px["next_5d_return"] = px.groupby("symbol")["close"].shift(-5) / px["close"] - 1.0
            px["next_20d_return"] = px.groupby("symbol")["close"].shift(-20) / px["close"] - 1.0
            stop_df = risk_events[risk_events.get("event_type", "") == "stop_loss"].merge(px, on=["symbol", "date"], how="left")
            if not stop_df.empty:
                risk_summary_rows.append({"scope": "stop_loss_followup", "key": "avg_next_5d_return", "count": float(stop_df["next_5d_return"].mean())})
                risk_summary_rows.append({"scope": "stop_loss_followup", "key": "avg_next_20d_return", "count": float(stop_df["next_20d_return"].mean())})
    else:
        metadata.setdefault("unavailable", {})["backtest_risk_events"] = risk_unavailable_reason

    risk_summary = pd.DataFrame(risk_summary_rows)

    metadata["row_counts"] = {
        "holdings_rows": int(len(holdings)),
        "results_rows": int(len(results)),
        "snapshot_rows": int(len(snapshot)),
        "symbol_summary_rows": int(len(symbol_summary)),
        "period_summary_rows": int(len(period_summary)),
        "risk_event_summary_rows": int(len(risk_summary)),
    }

    run_id_safe = str(metadata["run_id"]).replace("/", "_")
    outputs = {
        "snapshot": out_dir / f"exp40a_position_attribution_snapshot_{run_id_safe}.csv",
        "symbol_summary": out_dir / f"exp40a_symbol_attribution_summary_{run_id_safe}.csv",
        "period_summary": out_dir / f"exp40a_period_attribution_summary_{run_id_safe}.csv",
        "winners_losers": out_dir / f"exp40a_top_winners_losers_{run_id_safe}.csv",
        "concentration": out_dir / f"exp40a_concentration_summary_{run_id_safe}.csv",
        "risk_event": out_dir / f"exp40a_risk_event_summary_{run_id_safe}.csv",
        "metadata": out_dir / f"exp40a_metadata_{run_id_safe}.json",
    }

    snapshot.to_csv(outputs["snapshot"], index=False)
    symbol_summary.to_csv(outputs["symbol_summary"], index=False)
    period_summary.to_csv(outputs["period_summary"], index=False)
    winners_losers_df.to_csv(outputs["winners_losers"], index=False)
    concentration_summary.to_csv(outputs["concentration"], index=False)
    risk_summary.to_csv(outputs["risk_event"], index=False)

    metadata["output_paths"] = {k: str(v) for k, v in outputs.items()}
    metadata["regime_comparison"] = regime_comparison.to_dict(orient="records")
    outputs["metadata"].write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    overall_symbol_sum = symbol_summary.set_index("symbol")["sum_position_contribution"]
    print(f"[Exp40A] run_id={run_id}")
    print(f"[Exp40A] holdings rows={len(holdings)} / results rows={len(results)}")
    print(f"[Exp40A] date range={args.start_date}~{args.end_date}")
    print("[Exp40A] top 10 positive contribution symbols")
    print(overall_symbol_sum.sort_values(ascending=False).head(10).to_string())
    print("[Exp40A] top 10 negative contribution symbols")
    print(overall_symbol_sum.sort_values(ascending=True).head(10).to_string())
    print("[Exp40A] 2018~2021 vs 2022~2026 attribution comparison")
    print(regime_comparison.to_string(index=False))
    print("[Exp40A] concentration summary")
    print(concentration_summary.to_string(index=False))
    print("[Exp40A] next-step recommendation")
    print("- attribution이 특정 winner 몇 개에 과도하게 의존하면 simulator/proper filter 우선")
    print("- regime별 loser 구조가 명확하면 Exp40B regime-aware simulator 설계")
    print("- 기여도 계산이 불안정하면 attribution schema 개선 필요")


if __name__ == "__main__":
    main()
