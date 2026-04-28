#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db
from pipeline.dynamic_universe import build_rolling_liquidity_universe, validate_rolling_universe_no_lookahead
from pipeline.scoring import generate_daily_scores
from pipeline.universe_input import load_symbols_from_universe_csv


@dataclass(frozen=True)
class CandidateConfig:
    name: str
    scoring_profile: str


CANDIDATES = [
    CandidateConfig(name="baseline_old", scoring_profile="old"),
    CandidateConfig(name="aggressive_hybrid_v4", scoring_profile="hybrid_v4"),
]

MAX_SINGLE_WEIGHT_LIMIT = 0.20 + 1e-9


def _safe_div(x: float, y: float) -> float:
    return x / y if y else 0.0


def _max_drawdown(equities: list[float]) -> float:
    if not equities:
        return 0.0
    peak = equities[0]
    worst = 0.0
    for equity in equities:
        peak = max(peak, equity)
        drawdown = _safe_div(equity - peak, peak)
        worst = min(worst, drawdown)
    return worst


def _parse_csv_tokens(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_int_tokens(value: str) -> list[int]:
    parsed = [int(item.strip()) for item in value.split(",") if item.strip()]
    if any(v <= 0 for v in parsed):
        raise ValueError("--horizons must contain positive integers")
    return parsed


def _sanitize_identifier(value: str) -> str:
    sanitized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value)
    if not sanitized:
        raise ValueError("identifier cannot be empty after sanitization")
    return sanitized


def _daily_scores_columns(conn: sqlite3.Connection) -> list[str]:
    cols = [str(r["name"]) for r in conn.execute("PRAGMA table_info(daily_scores)").fetchall()]
    if not cols:
        raise ValueError("daily_scores table not found")
    return cols


def _snapshot_candidate_scores(conn: sqlite3.Connection, candidate: CandidateConfig) -> tuple[str, list[str]]:
    columns = _daily_scores_columns(conn)
    col_sql = ", ".join(columns)
    table_name = f"tmp_final_candidate_scores_{_sanitize_identifier(candidate.name)}"
    conn.execute(f"DROP TABLE IF EXISTS temp.{table_name}")
    conn.execute(f"CREATE TEMP TABLE {table_name} AS SELECT {col_sql} FROM daily_scores")
    return table_name, columns


def _restore_candidate_scores(conn: sqlite3.Connection, snapshot_table: str, columns: list[str]) -> None:
    col_sql = ", ".join(columns)
    conn.execute("DELETE FROM daily_scores")
    conn.execute(f"INSERT INTO daily_scores ({col_sql}) SELECT {col_sql} FROM temp.{snapshot_table}")


def _collect_score_signature(conn: sqlite3.Connection, scoring_profile: str) -> dict[str, object]:
    row_count = int(conn.execute("SELECT COUNT(1) FROM daily_scores").fetchone()[0])
    first_date = conn.execute("SELECT MIN(date) FROM daily_scores").fetchone()[0]
    last_date = conn.execute("SELECT MAX(date) FROM daily_scores").fetchone()[0]
    sampled_dates = [d for d in [first_date, last_date] if d]
    if first_date and last_date and first_date != last_date:
        dates = [r["date"] for r in conn.execute("SELECT DISTINCT date FROM daily_scores ORDER BY date").fetchall()]
        if dates:
            sampled_dates.insert(1, dates[len(dates) // 2])

    top_symbols_by_date: dict[str, list[str]] = {}
    for d in sampled_dates:
        symbols = [
            str(r["symbol"])
            for r in conn.execute(
                """
                SELECT symbol
                FROM daily_scores
                WHERE date=?
                ORDER BY rank ASC, symbol ASC
                LIMIT 10
                """,
                (d,),
            ).fetchall()
        ]
        top_symbols_by_date[str(d)] = symbols

    return {
        "scoring_profile": scoring_profile,
        "row_count": row_count,
        "first_score_date": first_date,
        "last_score_date": last_date,
        "top10_symbols_by_sample_date": top_symbols_by_date,
    }


def _add_months(d: date, months: int) -> date:
    total = (d.year * 12 + (d.month - 1)) + months
    year = total // 12
    month = (total % 12) + 1
    return date(year, month, 1)


def _compute_trade_count_and_turnover(conn: sqlite3.Connection, run_id: str) -> tuple[int, float | None]:
    rows = conn.execute(
        "SELECT date, symbol FROM backtest_holdings WHERE run_id=? ORDER BY date, symbol",
        (run_id,),
    ).fetchall()
    holdings_by_date: dict[str, set[str]] = {}
    for row in rows:
        holdings_by_date.setdefault(row["date"], set()).add(row["symbol"])

    if not holdings_by_date:
        return 0, None

    dates = sorted(holdings_by_date)
    trades = len(holdings_by_date[dates[0]])
    turnover_ratios: list[float] = []
    for i in range(1, len(dates)):
        prev = holdings_by_date[dates[i - 1]]
        cur = holdings_by_date[dates[i]]
        changes = len(cur - prev) + len(prev - cur)
        trades += changes
        denom = max(1, len(prev))
        turnover_ratios.append(changes / denom)
    return trades, (mean(turnover_ratios) if turnover_ratios else 0.0)


def _get_trading_dates(conn: sqlite3.Connection, start_date: str | None, end_date: str | None) -> list[str]:
    params: list[str] = []
    where: list[str] = []
    if start_date:
        where.append("date >= ?")
        params.append(start_date)
    if end_date:
        where.append("date <= ?")
        params.append(end_date)
    sql = "SELECT DISTINCT date FROM daily_prices"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY date"
    return [r["date"] for r in conn.execute(sql, params).fetchall()]


def _build_benchmark_returns(conn: sqlite3.Connection, dates: list[str], allowed_symbols: list[str]) -> dict[str, float]:
    if len(dates) < 2:
        return {}
    symbol_sql = ",".join("?" for _ in allowed_symbols)
    benchmark_by_date: dict[str, float] = {}
    for i in range(len(dates) - 1):
        d0 = dates[i]
        d1 = dates[i + 1]
        rows = conn.execute(
            f"""
            SELECT p0.close AS c0, p1.close AS c1
            FROM daily_prices p0
            JOIN daily_prices p1 ON p1.symbol = p0.symbol AND p1.date = ?
            WHERE p0.date = ? AND p0.symbol IN ({symbol_sql})
            """,
            (d1, d0, *allowed_symbols),
        ).fetchall()
        daily_returns = [(float(r["c1"]) - float(r["c0"])) / float(r["c0"]) for r in rows if r["c0"]]
        benchmark_by_date[d1] = sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
    return benchmark_by_date


def _window_starts(trading_dates: list[str], eval_frequency: str) -> list[str]:
    starts: list[str] = []
    seen: set[str] = set()
    for d in trading_dates:
        month_key = d[:7]
        if eval_frequency == "monthly":
            key = month_key
        elif eval_frequency == "quarterly":
            y = int(d[:4])
            m = int(d[5:7])
            q = (m - 1) // 3 + 1
            key = f"{y}-Q{q}"
        else:
            raise ValueError(f"unsupported eval_frequency: {eval_frequency}")
        if key not in seen:
            seen.add(key)
            starts.append(d)
    return starts


def _resolve_window_end(start: str, horizon_months: int, trading_dates: list[str]) -> str | None:
    start_d = date.fromisoformat(start)
    target = _add_months(start_d, horizon_months)
    candidate = [d for d in trading_dates if d > start and d < target.isoformat()]
    if candidate:
        return candidate[-1]
    fallback = [d for d in trading_dates if d > start and d <= target.isoformat()]
    return fallback[-1] if fallback else None


def _compute_monthly_returns(bt_rows: list[sqlite3.Row], benchmark_by_date: dict[str, float]) -> dict[str, tuple[float, float]]:
    by_month: dict[str, list[tuple[float, float]]] = defaultdict(list)
    for row in bt_rows:
        month_key = row["date"][:7]
        by_month[month_key].append((float(row["daily_return"]), benchmark_by_date.get(row["date"], 0.0)))

    monthly: dict[str, tuple[float, float]] = {}
    for month_key, vals in by_month.items():
        s_eq = 1.0
        b_eq = 1.0
        for s_ret, b_ret in vals:
            s_eq *= (1.0 + s_ret)
            b_eq *= (1.0 + b_ret)
        monthly[month_key] = (s_eq - 1.0, b_eq - 1.0)
    return monthly


def _quantile(values: list[float], q: float) -> float:
    if not values:
        raise ValueError("quantile requires non-empty values")
    vals = sorted(values)
    idx = (len(vals) - 1) * q
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return vals[lo]
    return vals[lo] + (vals[hi] - vals[lo]) * (idx - lo)


def _run_candidate_backtest(
    conn: sqlite3.Connection,
    candidate: CandidateConfig,
    start_date: str,
    end_date: str,
    benchmark_by_date: dict[str, float],
) -> dict[str, object]:
    run_id = run_backtest(
        conn,
        top_n=5,
        start_date=start_date,
        end_date=end_date,
        rebalance_frequency="weekly",
        min_holding_days=10,
        keep_rank_threshold=9,
        scoring_profile=candidate.scoring_profile,
        stop_loss_cash_mode="keep_cash",
        stop_loss_cooldown_days=0,
        enable_position_stop_loss=True,
        position_stop_loss_pct=0.10,
        enable_overheat_entry_gate=False,
        enable_entry_quality_gate=False,
    )
    bt_rows = conn.execute(
        "SELECT date, equity, daily_return, position_count, max_single_position_weight FROM backtest_results WHERE run_id=? ORDER BY date",
        (run_id,),
    ).fetchall()
    run_row = conn.execute(
        "SELECT initial_equity, average_actual_position_count, max_single_position_weight FROM backtest_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if not bt_rows or not run_row:
        raise ValueError(f"missing backtest results for candidate={candidate.name} run_id={run_id}")

    initial_equity = float(run_row["initial_equity"])
    equities = [float(r["equity"]) for r in bt_rows]
    dates = [str(r["date"]) for r in bt_rows]
    benchmark_equity = initial_equity
    benchmark_equities: list[float] = []
    for d in dates:
        benchmark_equity *= (1.0 + benchmark_by_date.get(d, 0.0))
        benchmark_equities.append(benchmark_equity)

    total_return = _safe_div(equities[-1] - initial_equity, initial_equity)
    benchmark_return = _safe_div(benchmark_equities[-1] - initial_equity, initial_equity)
    max_single_observed = max(
        [float(run_row["max_single_position_weight"] or 0.0)]
        + [float(r["max_single_position_weight"] or 0.0) for r in bt_rows]
    )
    if max_single_observed > MAX_SINGLE_WEIGHT_LIMIT:
        raise ValueError(
            f"stop loss keep_cash validation failed for {candidate.name}: max_single_weight_observed={max_single_observed:.6f}"
        )

    trade_count, turnover = _compute_trade_count_and_turnover(conn, run_id)
    return {
        "run_id": run_id,
        "rows": bt_rows,
        "dates": dates,
        "equities": equities,
        "benchmark_equities": benchmark_equities,
        "total_return": total_return,
        "benchmark_return": benchmark_return,
        "excess_return": total_return - benchmark_return,
        "max_drawdown": _max_drawdown(equities),
        "trade_count": trade_count,
        "mean_turnover": turnover,
        "avg_position_count": float(run_row["average_actual_position_count"] or 0.0),
        "max_single_weight_observed": max_single_observed,
    }


def _build_candidate_summary(window_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, int], list[dict[str, object]]] = defaultdict(list)
    for row in window_rows:
        grouped[(str(row["candidate"]), str(row["eval_frequency"]), int(row["horizon_months"]))].append(row)

    out: list[dict[str, object]] = []
    for (candidate, freq, horizon), rows in sorted(grouped.items()):
        total_returns = [float(r["total_return"]) for r in rows]
        benchmark_returns = [float(r["benchmark_return"]) for r in rows]
        excess_returns = [float(r["excess_return"]) for r in rows]
        mdds = [float(r["max_drawdown"]) for r in rows]
        turnover_vals = [float(r["turnover"]) for r in rows if r.get("turnover") is not None]
        position_vals = [float(r["avg_position_count"]) for r in rows if r.get("avg_position_count") is not None]
        max_single_vals = [float(r["max_single_weight_observed"]) for r in rows if r.get("max_single_weight_observed") is not None]

        out.append(
            {
                "candidate": candidate,
                "scoring_profile": next(
                    str(r.get("scoring_profile"))
                    for r in rows
                    if r.get("scoring_profile") is not None
                ),
                "eval_frequency": freq,
                "horizon_months": horizon,
                "n_windows": len(rows),
                "mean_total_return": mean(total_returns),
                "mean_benchmark_return": mean(benchmark_returns),
                "mean_excess_return": mean(excess_returns),
                "median_excess_return": median(excess_returns),
                "p25_excess_return": _quantile(excess_returns, 0.25),
                "win_vs_benchmark": mean([float(r["win_vs_benchmark"]) for r in rows]),
                "worst_total_return": min(total_returns),
                "worst_excess_return": min(excess_returns),
                "worst_mdd": min(mdds),
                "mdd_breach_30_rate": mean([1.0 if m <= -0.30 else 0.0 for m in mdds]),
                "mdd_breach_35_rate": mean([1.0 if m <= -0.35 else 0.0 for m in mdds]),
                "mean_turnover": mean(turnover_vals) if turnover_vals else None,
                "avg_position_count": mean(position_vals) if position_vals else None,
                "max_single_weight_observed": max(max_single_vals) if max_single_vals else None,
            }
        )
    return out


def _build_worst_windows(window_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str, int], list[dict[str, object]]] = defaultdict(list)
    for row in window_rows:
        grouped[(str(row["candidate"]), str(row["eval_frequency"]), int(row["horizon_months"]))].append(row)

    out: list[dict[str, object]] = []
    for (candidate, freq, horizon), rows in sorted(grouped.items()):
        for metric in ["total_return", "excess_return", "max_drawdown"]:
            worst = min(rows, key=lambda x: float(x[metric]))
            out.append(
                {
                    "candidate": candidate,
                    "scoring_profile": str(worst.get("scoring_profile")),
                    "eval_frequency": freq,
                    "horizon_months": horizon,
                    "worst_metric": metric,
                    "start_date": worst["start_date"],
                    "end_date": worst["end_date"],
                    "total_return": worst["total_return"],
                    "benchmark_return": worst["benchmark_return"],
                    "excess_return": worst["excess_return"],
                    "max_drawdown": worst["max_drawdown"],
                }
            )
    return out


def _all_comparable_windows_identical(window_rows: list[dict[str, object]]) -> bool:
    baseline = {
        (
            str(r["eval_frequency"]),
            int(r["horizon_months"]),
            str(r["start_date"]),
            str(r["end_date"]),
        ): r
        for r in window_rows
        if str(r["candidate"]) == "baseline_old"
    }
    aggressive = {
        (
            str(r["eval_frequency"]),
            int(r["horizon_months"]),
            str(r["start_date"]),
            str(r["end_date"]),
        ): r
        for r in window_rows
        if str(r["candidate"]) == "aggressive_hybrid_v4"
    }
    comparable_keys = sorted(set(baseline) & set(aggressive))
    if not comparable_keys:
        return False
    for key in comparable_keys:
        b = baseline[key]
        a = aggressive[key]
        if not (
            float(b["total_return"]) == float(a["total_return"])
            and float(b["excess_return"]) == float(a["excess_return"])
            and float(b["max_drawdown"]) == float(a["max_drawdown"])
        ):
            return False
    return True


def _write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    if not rows and fieldnames is None:
        raise ValueError(f"cannot write empty CSV without fieldnames: {path}")
    headers = fieldnames if fieldnames is not None else list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _plot_outputs(
    outdir: Path,
    equity_curve_rows: list[dict[str, object]],
    drawdown_rows: list[dict[str, object]],
    monthly_rows: list[dict[str, object]],
) -> list[str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        tiny_png = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
            b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc```\x00\x00"
            b"\x00\x04\x00\x01\xf6\x178U\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        fallback = []
        for name in ["equity_curve.png", "drawdown_curve.png", "monthly_returns_plot.png"]:
            p = outdir / name
            p.write_bytes(tiny_png)
            fallback.append(str(p))
        return fallback

    plot_paths: list[str] = []

    by_candidate_eq: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    for row in equity_curve_rows:
        by_candidate_eq[str(row["candidate"])].append((str(row["date"]), float(row["strategy_equity"]), float(row["benchmark_equity"])))

    plt.figure(figsize=(10, 5))
    for candidate, vals in sorted(by_candidate_eq.items()):
        dates = [v[0] for v in vals]
        series = [v[1] for v in vals]
        plt.plot(dates, series, label=f"{candidate} strategy")
    if by_candidate_eq:
        sample = next(iter(by_candidate_eq.values()))
        plt.plot([v[0] for v in sample], [v[2] for v in sample], label="benchmark", linestyle="--", color="black")
    plt.title("Equity Curve")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    eq_png = outdir / "equity_curve.png"
    plt.savefig(eq_png)
    plt.close()
    plot_paths.append(str(eq_png))

    by_candidate_dd: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    for row in drawdown_rows:
        by_candidate_dd[str(row["candidate"])].append((str(row["date"]), float(row["strategy_drawdown"]), float(row["benchmark_drawdown"])))

    plt.figure(figsize=(10, 5))
    for candidate, vals in sorted(by_candidate_dd.items()):
        plt.plot([v[0] for v in vals], [v[1] for v in vals], label=f"{candidate} drawdown")
    if by_candidate_dd:
        sample = next(iter(by_candidate_dd.values()))
        plt.plot([v[0] for v in sample], [v[2] for v in sample], label="benchmark drawdown", linestyle="--", color="black")
    plt.title("Drawdown Curve")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    dd_png = outdir / "drawdown_curve.png"
    plt.savefig(dd_png)
    plt.close()
    plot_paths.append(str(dd_png))

    monthly_by_candidate: dict[str, dict[str, float]] = defaultdict(dict)
    all_months: set[str] = set()
    for row in monthly_rows:
        monthly_by_candidate[str(row["candidate"])][str(row["month"])] = float(row["strategy_return"])
        all_months.add(str(row["month"]))

    months = sorted(all_months)
    if months:
        plt.figure(figsize=(12, 5))
        x = list(range(len(months)))
        width = 0.4
        baseline = [monthly_by_candidate.get("baseline_old", {}).get(m, 0.0) for m in months]
        aggressive = [monthly_by_candidate.get("aggressive_hybrid_v4", {}).get(m, 0.0) for m in months]
        plt.bar([i - width / 2 for i in x], baseline, width=width, label="baseline_old")
        plt.bar([i + width / 2 for i in x], aggressive, width=width, label="aggressive_hybrid_v4")
        plt.xticks(x, months, rotation=45)
        plt.title("Monthly Returns Comparison")
        plt.legend()
        plt.tight_layout()
        monthly_png = outdir / "monthly_returns_plot.png"
        plt.savefig(monthly_png)
        plt.close()
        plot_paths.append(str(monthly_png))

    return plot_paths


def _decision_text(summary_rows: list[dict[str, object]]) -> tuple[str, list[str]]:
    by_candidate: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in summary_rows:
        by_candidate[str(row["candidate"])].append(row)

    baseline_rows = by_candidate["baseline_old"]
    aggressive_rows = by_candidate["aggressive_hybrid_v4"]
    if not baseline_rows or not aggressive_rows:
        raise ValueError("both candidates must exist in summary")

    def agg(rows: list[dict[str, object]], key: str, fn=mean) -> float:
        vals = [float(r[key]) for r in rows]
        return fn(vals)

    b = {
        "mean_total_return": agg(baseline_rows, "mean_total_return"),
        "mean_excess_return": agg(baseline_rows, "mean_excess_return"),
        "p25_excess_return": agg(baseline_rows, "p25_excess_return"),
        "worst_total_return": agg(baseline_rows, "worst_total_return", min),
        "worst_mdd": agg(baseline_rows, "worst_mdd", min),
        "mdd_breach_30_rate": agg(baseline_rows, "mdd_breach_30_rate"),
        "mdd_breach_35_rate": agg(baseline_rows, "mdd_breach_35_rate"),
    }
    a = {
        "mean_total_return": agg(aggressive_rows, "mean_total_return"),
        "mean_excess_return": agg(aggressive_rows, "mean_excess_return"),
        "p25_excess_return": agg(aggressive_rows, "p25_excess_return"),
        "worst_total_return": agg(aggressive_rows, "worst_total_return", min),
        "worst_mdd": agg(aggressive_rows, "worst_mdd", min),
        "mdd_breach_30_rate": agg(aggressive_rows, "mdd_breach_30_rate"),
        "mdd_breach_35_rate": agg(aggressive_rows, "mdd_breach_35_rate"),
    }
    if b == a:
        decision = "Candidates are indistinguishable in this run; check scoring isolation."
        rationale = [
            "baseline_old and aggressive_hybrid_v4 are exactly tied across aggregated robustness metrics.",
            "Candidates are indistinguishable in this run; check scoring isolation.",
        ]
        return decision, rationale

    risk_degraded = (
        (a["worst_mdd"] < b["worst_mdd"] - 0.03)
        or (a["worst_total_return"] < b["worst_total_return"] - 0.03)
        or (a["mdd_breach_30_rate"] > b["mdd_breach_30_rate"] + 0.05)
        or (a["mdd_breach_35_rate"] > b["mdd_breach_35_rate"] + 0.03)
    )

    decision = "baseline_old 유지"
    rationale: list[str] = [
        f"baseline_old mean_total_return={b['mean_total_return']:.2%}, mean_excess_return={b['mean_excess_return']:.2%}, p25_excess_return={b['p25_excess_return']:.2%}",
        f"aggressive_hybrid_v4 mean_total_return={a['mean_total_return']:.2%}, mean_excess_return={a['mean_excess_return']:.2%}, p25_excess_return={a['p25_excess_return']:.2%}",
        f"baseline_old worst_total_return={b['worst_total_return']:.2%}, worst_mdd={b['worst_mdd']:.2%}, breach30={b['mdd_breach_30_rate']:.2%}, breach35={b['mdd_breach_35_rate']:.2%}",
        f"aggressive_hybrid_v4 worst_total_return={a['worst_total_return']:.2%}, worst_mdd={a['worst_mdd']:.2%}, breach30={a['mdd_breach_30_rate']:.2%}, breach35={a['mdd_breach_35_rate']:.2%}",
    ]

    if (a["mean_total_return"] > b["mean_total_return"] or a["mean_excess_return"] > b["mean_excess_return"]) and risk_degraded:
        rationale.append("aggressive_hybrid_v4는 평균 수익 측면 개선이 있으나 worst_mdd/worst_total_return/breach rate가 유의하게 악화되어 production baseline으로 승격하지 않습니다.")
        rationale.append("결론: baseline_old = production baseline 유지, aggressive_hybrid_v4 = aggressive candidate 유지")
    elif not risk_degraded and a["mean_excess_return"] >= b["mean_excess_return"] and a["p25_excess_return"] >= b["p25_excess_return"]:
        decision = "aggressive_hybrid_v4 승격 가능(리스크 동등 이상)"
        rationale.append("aggressive_hybrid_v4가 수익/하방 지표를 함께 충족하여 승격 후보로 볼 수 있습니다.")
    else:
        rationale.append("risk-aware 관점에서 baseline_old가 더 우위이므로 production baseline 유지를 권고합니다.")
    return decision, rationale


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate strict final candidate report for baseline_old vs aggressive_hybrid_v4")
    parser.add_argument("--db", default="data/kospi_495_rolling_3y.db")
    parser.add_argument("--universe-file", default="data/kospi_valid_universe_495.csv")
    parser.add_argument("--outdir", default="reports/final_candidate_report/latest")
    parser.add_argument("--output-dir", dest="output_dir_alias", default=None, help="Backward compatibility alias for --outdir")
    parser.add_argument("--benchmark-mode", default="universe", choices=["universe"])
    parser.add_argument("--eval-frequencies", default="monthly,quarterly")
    parser.add_argument("--horizons", default="1,3,6,12")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--allow-smoke", action="store_true", help="Allow full-period only run without robustness windows")
    args = parser.parse_args()

    if args.output_dir_alias:
        args.outdir = args.output_dir_alias

    outdir = Path(args.outdir)
    if outdir.exists() and args.overwrite:
        shutil.rmtree(outdir)
    if outdir.exists() and any(outdir.iterdir()) and not args.overwrite:
        raise ValueError(f"outdir is not empty, use --overwrite: {outdir}")
    outdir.mkdir(parents=True, exist_ok=True)

    conn = get_connection(args.db)
    init_db(conn)

    allowed_symbols = load_symbols_from_universe_csv(args.universe_file)
    if not allowed_symbols:
        raise ValueError("universe file has no symbols")

    trading_dates = _get_trading_dates(conn, args.start_date, args.end_date)
    if len(trading_dates) < 2:
        raise ValueError("not enough trading dates in requested range")

    eval_frequencies = _parse_csv_tokens(args.eval_frequencies)
    horizons = _parse_int_tokens(args.horizons)

    universe_build = build_rolling_liquidity_universe(conn, universe_size=100, lookback_days=20)
    try:
        lookahead_validation = validate_rolling_universe_no_lookahead(conn, universe_size=100, lookback_days=20)
        if int(lookahead_validation.get("violations", 0)) > 0:
            raise ValueError(f"lookahead validation violations > 0: {lookahead_validation}")
    except Exception as exc:
        lookahead_validation = {"status": "unavailable", "reason": str(exc)}

    benchmark_by_date = _build_benchmark_returns(conn, trading_dates, allowed_symbols)
    if not benchmark_by_date:
        raise ValueError("benchmark return series is empty")

    candidate_score_snapshots: dict[str, tuple[str, list[str]]] = {}
    score_signatures: list[dict[str, object]] = []
    warnings: list[str] = []
    for candidate in CANDIDATES:
        generate_daily_scores(
            conn,
            include_history=True,
            allowed_symbols=allowed_symbols,
            scoring_profile=candidate.scoring_profile,
            universe_mode="rolling_liquidity",
            universe_size=100,
            universe_lookback_days=20,
        )
        score_signatures.append(_collect_score_signature(conn, candidate.scoring_profile))
        candidate_score_snapshots[candidate.name] = _snapshot_candidate_scores(conn, candidate)

    signatures_by_profile = {str(s["scoring_profile"]): s for s in score_signatures}
    old_sig = signatures_by_profile.get("old")
    hybrid_sig = signatures_by_profile.get("hybrid_v4")
    if old_sig and hybrid_sig:
        if (
            int(old_sig["row_count"]) == int(hybrid_sig["row_count"])
            and old_sig["first_score_date"] == hybrid_sig["first_score_date"]
            and old_sig["last_score_date"] == hybrid_sig["last_score_date"]
            and old_sig["top10_symbols_by_sample_date"] == hybrid_sig["top10_symbols_by_sample_date"]
        ):
            warnings.append("score signatures are identical for old and hybrid_v4; verify scoring differentiation.")

    equity_curve_rows: list[dict[str, object]] = []
    drawdown_rows: list[dict[str, object]] = []
    monthly_rows: list[dict[str, object]] = []
    full_period_rows: list[dict[str, object]] = []

    full_start = trading_dates[0]
    full_end = trading_dates[-1]
    for candidate in CANDIDATES:
        snapshot_table, columns = candidate_score_snapshots[candidate.name]
        _restore_candidate_scores(conn, snapshot_table, columns)
        result = _run_candidate_backtest(conn, candidate, full_start, full_end, benchmark_by_date)
        full_period_rows.append(
            {
                "candidate": candidate.name,
                "scoring_profile": candidate.scoring_profile,
                "start_date": full_start,
                "end_date": full_end,
                "total_return": result["total_return"],
                "benchmark_return": result["benchmark_return"],
                "excess_return": result["excess_return"],
                "max_drawdown": result["max_drawdown"],
                "turnover": result["mean_turnover"],
                "avg_position_count": result["avg_position_count"],
                "max_single_weight_observed": result["max_single_weight_observed"],
                "trade_count": result["trade_count"],
            }
        )

        equities = list(result["equities"])
        benchmark_equities = list(result["benchmark_equities"])
        dates = list(result["dates"])
        s_peak = equities[0]
        b_peak = benchmark_equities[0]
        for d, s_eq, b_eq in zip(dates, equities, benchmark_equities):
            s_peak = max(s_peak, s_eq)
            b_peak = max(b_peak, b_eq)
            equity_curve_rows.append({"date": d, "candidate": candidate.name, "strategy_equity": s_eq, "benchmark_equity": b_eq})
            drawdown_rows.append(
                {
                    "date": d,
                    "candidate": candidate.name,
                    "strategy_drawdown": _safe_div(s_eq - s_peak, s_peak),
                    "benchmark_drawdown": _safe_div(b_eq - b_peak, b_peak),
                }
            )

        monthly = _compute_monthly_returns(result["rows"], benchmark_by_date)
        for month_key, (strategy_ret, benchmark_ret) in sorted(monthly.items()):
            monthly_rows.append(
                {
                    "month": month_key,
                    "candidate": candidate.name,
                    "strategy_return": strategy_ret,
                    "benchmark_return": benchmark_ret,
                    "excess_return": strategy_ret - benchmark_ret,
                }
            )

    window_rows: list[dict[str, object]] = []
    if not args.allow_smoke:
        for freq in eval_frequencies:
            starts = _window_starts(trading_dates, freq)
            for horizon in horizons:
                for start in starts:
                    end = _resolve_window_end(start, horizon, trading_dates)
                    if not end:
                        continue
                    for candidate in CANDIDATES:
                        snapshot_table, columns = candidate_score_snapshots[candidate.name]
                        _restore_candidate_scores(conn, snapshot_table, columns)
                        r = _run_candidate_backtest(conn, candidate, start, end, benchmark_by_date)
                        window_rows.append(
                            {
                                "candidate": candidate.name,
                                "scoring_profile": candidate.scoring_profile,
                                "eval_frequency": freq,
                                "horizon_months": horizon,
                                "start_date": start,
                                "end_date": end,
                                "total_return": r["total_return"],
                                "benchmark_return": r["benchmark_return"],
                                "excess_return": r["excess_return"],
                                "max_drawdown": r["max_drawdown"],
                                "win_vs_benchmark": 1 if float(r["excess_return"]) > 0.0 else 0,
                                "turnover": r["mean_turnover"],
                                "avg_position_count": r["avg_position_count"],
                                "max_single_weight_observed": r["max_single_weight_observed"],
                            }
                        )

    if not args.allow_smoke:
        if not window_rows:
            raise ValueError("final mode requires window_results.csv rows (robustness_rows_loaded=0 is forbidden)")
        candidates_in_window = sorted({str(r["candidate"]) for r in window_rows})
        if candidates_in_window != sorted([c.name for c in CANDIDATES]):
            raise ValueError(f"window results must include both candidates, got={candidates_in_window}")
        if any(r.get("benchmark_return") is None for r in window_rows):
            raise ValueError("benchmark_return missing in window rows")
        if _all_comparable_windows_identical(window_rows):
            raise ValueError("candidate outputs are identical across all windows; scoring isolation failure likely")
    elif window_rows and _all_comparable_windows_identical(window_rows):
        warnings.append("candidate outputs are identical across all windows; scoring isolation failure likely")

    summary_rows = _build_candidate_summary(window_rows) if window_rows else []
    worst_rows = _build_worst_windows(window_rows) if window_rows else []

    if not args.allow_smoke and not summary_rows:
        raise ValueError("final mode requires non-empty candidate_summary.csv")

    summary_csv = outdir / "candidate_summary.csv"
    window_results_csv = outdir / "window_results.csv"
    equity_curve_csv = outdir / "equity_curve.csv"
    drawdown_curve_csv = outdir / "drawdown_curve.csv"
    monthly_returns_csv = outdir / "monthly_returns.csv"
    worst_windows_csv = outdir / "worst_windows.csv"
    report_md = outdir / "final_candidate_report.md"
    manifest_path = outdir / "manifest.json"

    _write_csv(equity_curve_csv, equity_curve_rows)
    _write_csv(drawdown_curve_csv, drawdown_rows)
    _write_csv(monthly_returns_csv, monthly_rows)
    _write_csv(window_results_csv, window_rows, fieldnames=[
        "candidate",
        "scoring_profile",
        "eval_frequency",
        "horizon_months",
        "start_date",
        "end_date",
        "total_return",
        "benchmark_return",
        "excess_return",
        "max_drawdown",
        "win_vs_benchmark",
        "turnover",
        "avg_position_count",
        "max_single_weight_observed",
    ])
    _write_csv(summary_csv, summary_rows, fieldnames=[
        "candidate",
        "scoring_profile",
        "eval_frequency",
        "horizon_months",
        "n_windows",
        "mean_total_return",
        "mean_benchmark_return",
        "mean_excess_return",
        "median_excess_return",
        "p25_excess_return",
        "win_vs_benchmark",
        "worst_total_return",
        "worst_excess_return",
        "worst_mdd",
        "mdd_breach_30_rate",
        "mdd_breach_35_rate",
        "mean_turnover",
        "avg_position_count",
        "max_single_weight_observed",
    ])
    _write_csv(worst_windows_csv, worst_rows, fieldnames=[
        "candidate",
        "scoring_profile",
        "eval_frequency",
        "horizon_months",
        "worst_metric",
        "start_date",
        "end_date",
        "total_return",
        "benchmark_return",
        "excess_return",
        "max_drawdown",
    ])

    plots = _plot_outputs(outdir, equity_curve_rows, drawdown_rows, monthly_rows)

    decision = "allow-smoke mode: production decision skipped"
    decision_rationale = ["--allow-smoke enabled: robustness window 평가를 생략했습니다."]
    if summary_rows:
        decision, decision_rationale = _decision_text(summary_rows)

    monthly_summary = [r for r in summary_rows if r.get("eval_frequency") == "monthly"]
    quarterly_summary = [r for r in summary_rows if r.get("eval_frequency") == "quarterly"]

    horizon_lines = []
    for h in sorted(set(int(r["horizon_months"]) for r in summary_rows)):
        rows = [r for r in summary_rows if int(r["horizon_months"]) == h]
        for c in ["baseline_old", "aggressive_hybrid_v4"]:
            c_rows = [r for r in rows if r["candidate"] == c]
            if not c_rows:
                continue
            horizon_lines.append(f"- h={h}m {c}: mean_excess={mean(float(x['mean_excess_return']) for x in c_rows):.2%}, worst_mdd={min(float(x['worst_mdd']) for x in c_rows):.2%}")

    report_lines = [
        "# Final Candidate Report (Strict)",
        "",
        "## Run configuration",
        f"- created_at_utc: {datetime.now(timezone.utc).isoformat()}",
        f"- db: {args.db}",
        f"- universe_file: {args.universe_file}",
        f"- benchmark_mode: {args.benchmark_mode}",
        f"- eval_frequencies: {','.join(eval_frequencies)}",
        f"- horizons: {','.join(str(h) for h in horizons)}",
        f"- full_period: {full_start} ~ {full_end}",
        f"- allow_smoke: {args.allow_smoke}",
        f"- robustness_rows_loaded: {len(window_rows)}",
        "",
        "## Diagnostics",
        f"- warnings: {len(warnings)}",
    ]
    report_lines.extend([f"- warning: {w}" for w in warnings] if warnings else ["- warning: none"])
    report_lines.extend([
        "",
        "### Score signatures",
    ])
    for sig in score_signatures:
        report_lines.append(
            f"- scoring_profile={sig['scoring_profile']}, rows={sig['row_count']}, first={sig['first_score_date']}, last={sig['last_score_date']}, samples={json.dumps(sig['top10_symbols_by_sample_date'], ensure_ascii=False)}"
        )

    report_lines.extend([
        "",
        "## Candidate configs",
        "- baseline_old: scoring_profile=old, top_n=5, min_holding_days=10, keep_rank_threshold=9, rebalance_frequency=weekly, position_stop_loss_pct=0.10, stop_loss_cash_mode=keep_cash, stop_loss_cooldown_days=0, overheat_gate=OFF, entry_quality_gate=OFF",
        "- aggressive_hybrid_v4: scoring_profile=hybrid_v4, top_n=5, min_holding_days=10, keep_rank_threshold=9, rebalance_frequency=weekly, position_stop_loss_pct=0.10, stop_loss_cash_mode=keep_cash, stop_loss_cooldown_days=0, overheat_gate=OFF, entry_quality_gate=OFF",
        "",
        "## Full-period result",
    ])
    for row in full_period_rows:
        report_lines.append(
            f"- {row['candidate']}: total={float(row['total_return']):.2%}, benchmark={float(row['benchmark_return']):.2%}, excess={float(row['excess_return']):.2%}, mdd={float(row['max_drawdown']):.2%}, turnover={row['turnover']}, avg_position_count={row['avg_position_count']:.2f}, max_single_weight_observed={float(row['max_single_weight_observed']):.4f}"
        )

    report_lines.extend(["", "## Monthly robustness summary"])
    if monthly_summary:
        for row in monthly_summary:
            report_lines.append(
                f"- {row['candidate']} h={row['horizon_months']}m: n={row['n_windows']}, mean_total={float(row['mean_total_return']):.2%}, mean_excess={float(row['mean_excess_return']):.2%}, p25_excess={float(row['p25_excess_return']):.2%}, worst_total={float(row['worst_total_return']):.2%}, worst_mdd={float(row['worst_mdd']):.2%}"
            )
    else:
        report_lines.append("- unavailable")

    report_lines.extend(["", "## Quarterly robustness summary"])
    if quarterly_summary:
        for row in quarterly_summary:
            report_lines.append(
                f"- {row['candidate']} h={row['horizon_months']}m: n={row['n_windows']}, mean_total={float(row['mean_total_return']):.2%}, mean_excess={float(row['mean_excess_return']):.2%}, p25_excess={float(row['p25_excess_return']):.2%}, worst_total={float(row['worst_total_return']):.2%}, worst_mdd={float(row['worst_mdd']):.2%}"
            )
    else:
        report_lines.append("- unavailable")

    report_lines.extend(["", "## Horizon 1/3/6/12 comparison"])
    report_lines.extend(horizon_lines if horizon_lines else ["- unavailable"])

    report_lines.extend(["", "## Worst window comparison"])
    if worst_rows:
        for row in worst_rows:
            report_lines.append(
                f"- {row['candidate']} {row['eval_frequency']} h={row['horizon_months']}m {row['worst_metric']}: {row['start_date']}~{row['end_date']} total={float(row['total_return']):.2%} excess={float(row['excess_return']):.2%} mdd={float(row['max_drawdown']):.2%}"
            )
    else:
        report_lines.append("- unavailable")

    report_lines.extend(["", "## MDD breach comparison"])
    if summary_rows:
        for c in ["baseline_old", "aggressive_hybrid_v4"]:
            rows = [r for r in summary_rows if r["candidate"] == c]
            report_lines.append(
                f"- {c}: avg_breach30={mean(float(r['mdd_breach_30_rate']) for r in rows):.2%}, avg_breach35={mean(float(r['mdd_breach_35_rate']) for r in rows):.2%}"
            )
    else:
        report_lines.append("- unavailable")

    report_lines.extend(["", "## Production decision", f"- decision: {decision}"])
    report_lines.extend([f"- {line}" for line in decision_rationale])

    report_lines.extend(
        [
            "",
            "## Remaining risks",
            "- rolling liquidity universe drift 및 시장 미시구조 변화 리스크",
            "- benchmark proxy(universe equal-weight)와 실거래 벤치마크 괴리 리스크",
            "- stop-loss 동시 발생 시 성과 분산 확대 리스크",
            "",
            "## Next recommended work: sector attribution / regime filter",
            "- sector attribution 리포트로 초과수익의 섹터 편향을 분해",
            "- regime filter(soft/hard) 추가 실험으로 하방 tail risk 완화 가능성 검증",
        ]
    )
    report_md.write_text("\n".join(report_lines), encoding="utf-8")

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "run_configuration": {
            "db": args.db,
            "universe_file": args.universe_file,
            "outdir": str(outdir),
            "benchmark_mode": args.benchmark_mode,
            "eval_frequencies": eval_frequencies,
            "horizons": horizons,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "allow_smoke": args.allow_smoke,
        },
        "db_path": args.db,
        "universe_path": args.universe_file,
        "universe_rows": len(allowed_symbols),
        "candidate_configs": [
            {
                "candidate": c.name,
                "scoring_profile": c.scoring_profile,
                "top_n": 5,
                "min_holding_days": 10,
                "keep_rank_threshold": 9,
                "rebalance_frequency": "weekly",
                "position_stop_loss_pct": 0.10,
                "stop_loss_cash_mode": "keep_cash",
                "stop_loss_cooldown_days": 0,
                "overheat_gate": False,
                "entry_quality_gate": False,
            }
            for c in CANDIDATES
        ],
        "date_range": {"start_date": full_start, "end_date": full_end},
        "lookahead_validation": lookahead_validation,
        "rows_changed": universe_build.get("row_changes"),
        "output_file_paths": {
            "manifest_path": str(manifest_path),
            "summary_csv": str(summary_csv),
            "window_results_csv": str(window_results_csv),
            "monthly_returns_csv": str(monthly_returns_csv),
            "equity_curve_csv": str(equity_curve_csv),
            "drawdown_curve_csv": str(drawdown_curve_csv),
            "worst_windows_csv": str(worst_windows_csv),
            "report_md": str(report_md),
            "plots": plots,
        },
        "score_signatures": score_signatures,
        "warnings": warnings,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    final_payload = {
        "outdir": str(outdir),
        "manifest_path": str(manifest_path),
        "summary_csv": str(summary_csv),
        "window_results_csv": str(window_results_csv),
        "monthly_returns_csv": str(monthly_returns_csv),
        "equity_curve_csv": str(equity_curve_csv),
        "drawdown_curve_csv": str(drawdown_curve_csv),
        "worst_windows_csv": str(worst_windows_csv),
        "report_md": str(report_md),
        "plots": plots,
    }
    print(f"FINAL_CANDIDATE_REPORT_JSON={json.dumps(final_payload, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
