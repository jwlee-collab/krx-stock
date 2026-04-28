#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db
from pipeline.dynamic_universe import build_rolling_liquidity_universe, validate_rolling_universe_no_lookahead
from pipeline.scoring import generate_daily_scores
from pipeline.universe_input import load_symbols_from_universe_csv

TRADING_DAYS = 252


@dataclass(frozen=True)
class CandidateConfig:
    name: str
    scoring_profile: str


CANDIDATES = [
    CandidateConfig(name="baseline_old", scoring_profile="old"),
    CandidateConfig(name="aggressive_hybrid_v4", scoring_profile="hybrid_v4"),
]


def _safe_div(x: float, y: float) -> float:
    return x / y if y else 0.0


def _annualized_return(total_return: float, periods: int) -> float:
    if periods <= 0:
        return 0.0
    years = periods / TRADING_DAYS
    if years <= 0.0 or (1.0 + total_return) <= 0.0:
        return 0.0
    return (1.0 + total_return) ** (1.0 / years) - 1.0


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


def _volatility(daily_returns: list[float]) -> float:
    if len(daily_returns) < 2:
        return 0.0
    avg = sum(daily_returns) / len(daily_returns)
    var = sum((ret - avg) ** 2 for ret in daily_returns) / (len(daily_returns) - 1)
    return math.sqrt(var) * math.sqrt(TRADING_DAYS)


def _sharpe(daily_returns: list[float]) -> float:
    vol = _volatility(daily_returns)
    if vol == 0.0 or not daily_returns:
        return 0.0
    avg = sum(daily_returns) / len(daily_returns)
    return (avg * TRADING_DAYS) / vol


def _trade_count_from_holdings(conn: sqlite3.Connection, run_id: str) -> int:
    rows = conn.execute(
        "SELECT date, symbol FROM backtest_holdings WHERE run_id=? ORDER BY date, symbol",
        (run_id,),
    ).fetchall()
    holdings_by_date: dict[str, set[str]] = {}
    for row in rows:
        holdings_by_date.setdefault(row["date"], set()).add(row["symbol"])

    if not holdings_by_date:
        return 0

    dates = sorted(holdings_by_date)
    trades = len(holdings_by_date[dates[0]])
    for i in range(1, len(dates)):
        prev = holdings_by_date[dates[i - 1]]
        cur = holdings_by_date[dates[i]]
        trades += len(cur - prev) + len(prev - cur)
    return trades


def _build_benchmark_returns(
    conn: sqlite3.Connection,
    dates: list[str],
    allowed_symbols: list[str],
) -> dict[str, float]:
    if len(dates) < 2:
        return {}

    benchmark_by_date: dict[str, float] = {}
    symbol_sql = ",".join("?" for _ in allowed_symbols)
    for i in range(len(dates) - 1):
        d0 = dates[i]
        d1 = dates[i + 1]
        rows = conn.execute(
            f"""
            SELECT p0.close AS c0, p1.close AS c1
            FROM daily_prices p0
            JOIN daily_prices p1
              ON p1.symbol = p0.symbol
             AND p1.date = ?
            WHERE p0.date = ?
              AND p0.symbol IN ({symbol_sql})
            """,
            (d1, d0, *allowed_symbols),
        ).fetchall()
        daily_returns = [(float(r["c1"]) - float(r["c0"])) / float(r["c0"]) for r in rows if r["c0"]]
        benchmark_by_date[d1] = sum(daily_returns) / len(daily_returns) if daily_returns else 0.0
    return benchmark_by_date


def _monthly_returns(rows: list[sqlite3.Row], benchmark_by_date: dict[str, float]) -> dict[str, tuple[float, float]]:
    by_month: dict[str, list[tuple[float, float]]] = {}
    for row in rows:
        date = row["date"]
        month_key = date[:7]
        by_month.setdefault(month_key, []).append((float(row["daily_return"]), benchmark_by_date.get(date, 0.0)))

    monthly: dict[str, tuple[float, float]] = {}
    for month, vals in by_month.items():
        strategy_eq = 1.0
        benchmark_eq = 1.0
        for strategy_ret, benchmark_ret in vals:
            strategy_eq *= (1.0 + strategy_ret)
            benchmark_eq *= (1.0 + benchmark_ret)
        monthly[month] = (strategy_eq - 1.0, benchmark_eq - 1.0)
    return monthly


def _read_optional_robustness_rows(output_dir: Path) -> list[dict[str, str]]:
    pattern_list = [
        "start_window_robustness_strategy_summary_*.csv",
        "start_window_robustness_period_summary_*.csv",
        "robustness_stability_*.csv",
        "robustness_experiments_*.csv",
    ]
    rows: list[dict[str, str]] = []
    for pattern in pattern_list:
        for csv_path in sorted(output_dir.glob(pattern)):
            try:
                with csv_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        row_copy = dict(row)
                        row_copy["__source_file"] = csv_path.name
                        rows.append(row_copy)
            except Exception:
                continue
    return rows


def _pick_candidate_rows(
    robustness_rows: list[dict[str, str]],
    scoring_profile: str,
) -> list[dict[str, str]]:
    matched: list[dict[str, str]] = []
    for row in robustness_rows:
        scoring = (row.get("scoring_version") or row.get("scoring") or "").strip()
        if scoring != scoring_profile:
            continue
        matched.append(row)
    return matched


def _summarize_robustness(matched_rows: list[dict[str, str]]) -> dict[str, float | int]:
    def _to_float(value: str | None) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    returns = [_to_float(r.get("mean_total_return")) for r in matched_rows]
    excess = [_to_float(r.get("mean_excess_return")) for r in matched_rows]
    win_rate = [_to_float(r.get("win_rate_vs_benchmark")) for r in matched_rows]
    worst_mdd = [_to_float(r.get("worst_mdd")) for r in matched_rows]
    worst_total = [_to_float(r.get("worst_total_return")) for r in matched_rows]

    def _avg(vals: list[float | None]) -> float | None:
        only = [v for v in vals if v is not None]
        return sum(only) / len(only) if only else None

    def _min(vals: list[float | None]) -> float | None:
        only = [v for v in vals if v is not None]
        return min(only) if only else None

    return {
        "matched_row_count": len(matched_rows),
        "mean_total_return": _avg(returns) or 0.0,
        "mean_excess_return": _avg(excess) or 0.0,
        "win_rate_vs_benchmark": _avg(win_rate) or 0.0,
        "worst_mdd": _min(worst_mdd) or 0.0,
        "worst_total_return": _min(worst_total) or 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate final comparison report for baseline_old vs aggressive_hybrid_v4")
    parser.add_argument("--db", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--universe-file", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection(args.db)
    init_db(conn)

    allowed_symbols = load_symbols_from_universe_csv(args.universe_file)
    if not allowed_symbols:
        raise ValueError("--universe-file must contain at least one symbol")

    universe_build = build_rolling_liquidity_universe(conn, universe_size=100, lookback_days=20)
    lookahead_validation = validate_rolling_universe_no_lookahead(conn, universe_size=100, lookback_days=20)

    summary_rows: list[dict[str, float | int | str]] = []
    equity_curve_rows: list[dict[str, float | str]] = []
    monthly_rows: list[dict[str, float | str]] = []
    candidate_payloads: dict[str, dict[str, float | int | str]] = {}

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
        run_id = run_backtest(
            conn,
            top_n=5,
            start_date=args.start_date,
            end_date=args.end_date,
            rebalance_frequency="weekly",
            min_holding_days=10,
            keep_rank_threshold=9,
            scoring_profile=candidate.scoring_profile,
            stop_loss_cash_mode="keep_cash",
            enable_position_stop_loss=True,
            position_stop_loss_pct=0.10,
            enable_overheat_entry_gate=False,
            enable_entry_quality_gate=False,
        )

        bt_rows = conn.execute(
            "SELECT date, equity, daily_return, position_count, cash_weight, max_single_position_weight FROM backtest_results WHERE run_id=? ORDER BY date",
            (run_id,),
        ).fetchall()
        if not bt_rows:
            raise ValueError(f"backtest_results missing for {candidate.name}")

        run_row = conn.execute(
            """
            SELECT initial_equity, position_stop_loss_count, average_actual_position_count,
                   average_cash_weight, max_single_position_weight
            FROM backtest_runs WHERE run_id=?
            """,
            (run_id,),
        ).fetchone()
        if not run_row:
            raise ValueError(f"backtest_runs missing for run_id={run_id}")

        dates = [row["date"] for row in bt_rows]
        benchmark_by_date = _build_benchmark_returns(conn, dates, allowed_symbols)

        benchmark_equity = float(run_row["initial_equity"])
        benchmark_returns: list[float] = []
        for date in dates:
            benchmark_daily = benchmark_by_date.get(date, 0.0)
            benchmark_equity *= (1.0 + benchmark_daily)
            benchmark_returns.append(benchmark_daily)

        daily_returns = [float(row["daily_return"]) for row in bt_rows]
        equities = [float(row["equity"]) for row in bt_rows]
        total_return = _safe_div(equities[-1] - float(run_row["initial_equity"]), float(run_row["initial_equity"]))
        benchmark_return = _safe_div(benchmark_equity - float(run_row["initial_equity"]), float(run_row["initial_equity"]))

        trade_count = _trade_count_from_holdings(conn, run_id)
        wins = [ret for ret in daily_returns if ret > 0.0]
        win_rate_daily = _safe_div(len(wins), len(daily_returns))

        summary = {
            "candidate": candidate.name,
            "run_id": run_id,
            "scoring_profile": candidate.scoring_profile,
            "top_n": 5,
            "min_holding_days": 10,
            "keep_rank_threshold": 9,
            "rebalance_frequency": "weekly",
            "position_stop_loss_pct": 0.10,
            "stop_loss_cash_mode": "keep_cash",
            "total_return": total_return,
            "benchmark_return": benchmark_return,
            "excess_return": total_return - benchmark_return,
            "annualized_return": _annualized_return(total_return, len(daily_returns)),
            "max_drawdown": _max_drawdown(equities),
            "sharpe": _sharpe(daily_returns),
            "volatility": _volatility(daily_returns),
            "win_rate_daily": win_rate_daily,
            "trade_count": trade_count,
            "position_stop_loss_count": int(run_row["position_stop_loss_count"] or 0),
            "average_position_count": float(run_row["average_actual_position_count"] or 0.0),
            "average_cash_weight": float(run_row["average_cash_weight"] or 0.0),
            "max_single_position_weight": float(run_row["max_single_position_weight"] or 0.0),
        }
        summary_rows.append(summary)
        candidate_payloads[candidate.name] = summary

        benchmark_equity_running = float(run_row["initial_equity"])
        for row in bt_rows:
            date = row["date"]
            benchmark_daily = benchmark_by_date.get(date, 0.0)
            benchmark_equity_running *= (1.0 + benchmark_daily)
            equity_curve_rows.append(
                {
                    "date": date,
                    "candidate": candidate.name,
                    "strategy_equity": float(row["equity"]),
                    "benchmark_equity": benchmark_equity_running,
                }
            )

        monthly = _monthly_returns(bt_rows, benchmark_by_date)
        for month, (strategy_ret, bench_ret) in sorted(monthly.items()):
            monthly_rows.append(
                {
                    "month": month,
                    "candidate": candidate.name,
                    "strategy_return": strategy_ret,
                    "benchmark_return": bench_ret,
                    "excess_return": strategy_ret - bench_ret,
                }
            )

    robustness_rows = _read_optional_robustness_rows(output_dir)
    robustness_summary: dict[str, dict[str, float | int]] = {}
    for candidate in CANDIDATES:
        matched = _pick_candidate_rows(robustness_rows, candidate.scoring_profile)
        robustness_summary[candidate.name] = _summarize_robustness(matched)

    summary_csv = output_dir / "final_candidate_summary.csv"
    with summary_csv.open("w", encoding="utf-8", newline="") as f:
        if summary_rows:
            writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)

    curve_csv = output_dir / "final_candidate_equity_curve.csv"
    with curve_csv.open("w", encoding="utf-8", newline="") as f:
        if equity_curve_rows:
            writer = csv.DictWriter(f, fieldnames=list(equity_curve_rows[0].keys()))
            writer.writeheader()
            writer.writerows(equity_curve_rows)

    monthly_csv = output_dir / "final_candidate_monthly_returns.csv"
    with monthly_csv.open("w", encoding="utf-8", newline="") as f:
        if monthly_rows:
            writer = csv.DictWriter(f, fieldnames=list(monthly_rows[0].keys()))
            writer.writeheader()
            writer.writerows(monthly_rows)

    baseline = candidate_payloads["baseline_old"]
    aggressive = candidate_payloads["aggressive_hybrid_v4"]

    lines = [
        "# Final Candidate Report",
        "",
        f"- Generated at: {datetime.utcnow().isoformat()}Z",
        f"- Date range: {args.start_date} ~ {args.end_date}",
        f"- Universe file: {args.universe_file}",
        f"- Rolling universe build rows_changed: {universe_build['row_changes']}",
        f"- Rolling universe lookahead violations: {lookahead_validation['violations']}",
        "",
        "## Final baseline candidate",
        f"- Candidate: baseline_old (scoring=old)",
        f"- total_return={baseline['total_return']:.2%}, excess_return={baseline['excess_return']:.2%}, max_drawdown={baseline['max_drawdown']:.2%}",
        f"- sharpe={baseline['sharpe']:.2f}, volatility={baseline['volatility']:.2%}, win_rate_daily={baseline['win_rate_daily']:.2%}",
        "",
        "## Aggressive candidate",
        f"- Candidate: aggressive_hybrid_v4 (scoring=hybrid_v4)",
        f"- total_return={aggressive['total_return']:.2%}, excess_return={aggressive['excess_return']:.2%}, max_drawdown={aggressive['max_drawdown']:.2%}",
        f"- sharpe={aggressive['sharpe']:.2f}, volatility={aggressive['volatility']:.2%}, win_rate_daily={aggressive['win_rate_daily']:.2%}",
        "",
        "## Why old remains baseline",
        "- old는 동일 규칙(top_n=5, weekly, min_holding_days=10)에서 안정적인 낙폭과 tail 방어를 유지하는지 우선 확인 대상입니다.",
        f"- robustness reference: mean_total_return={robustness_summary['baseline_old']['mean_total_return']:.2%}, "
        f"worst_mdd={robustness_summary['baseline_old']['worst_mdd']:.2%}, "
        f"worst_total_return={robustness_summary['baseline_old']['worst_total_return']:.2%}",
        "",
        "## Why hybrid_v4 is not baseline yet",
        "- hybrid_v4는 평균 수익률 개선 여지가 있으나 낙폭/하방 꼬리 위험이 baseline 대비 큰지 점검이 필요합니다.",
        f"- robustness reference: mean_total_return={robustness_summary['aggressive_hybrid_v4']['mean_total_return']:.2%}, "
        f"worst_mdd={robustness_summary['aggressive_hybrid_v4']['worst_mdd']:.2%}, "
        f"worst_total_return={robustness_summary['aggressive_hybrid_v4']['worst_total_return']:.2%}",
        "",
        "## 2024 failure window comparison",
        "- 2024 구간 월별/분기별 하방 구간은 final_candidate_monthly_returns.csv에서 두 후보의 음수 월 누적과 benchmark 대비 underperformance를 비교하세요.",
        "- 필요 시 start_window_robustness_period_summary_* CSV를 함께 열어 같은 기간의 window 단위 worst metrics를 교차 확인하세요.",
        "",
        "## Remaining risks",
        "- universe drift: rolling_liquidity top100 구성 종목 변화가 tail risk를 키울 수 있습니다.",
        "- benchmark proxy risk: benchmark는 universe-file 기반 equal-weight proxy로 계산되었습니다.",
        "- stop-loss clustering: 급락장에 position_stop_loss 동시 발생으로 현금비중 급증 가능성이 남아 있습니다.",
        "",
        "## Next recommended work: sector data / sector attribution / regime filter",
        "1. sector data 연동으로 전략 수익의 섹터 편향을 분해합니다.",
        "2. sector attribution 리포트를 추가해 초과수익의 설명력을 높입니다.",
        "3. regime filter를 후보(soft/hard)로 실험해 tail risk 완화 여부를 검증합니다.",
        "",
        "## Robustness CSV integration status",
        f"- robustness_rows_loaded={len(robustness_rows)}",
        f"- baseline_old matched_rows={robustness_summary['baseline_old']['matched_row_count']}",
        f"- aggressive_hybrid_v4 matched_rows={robustness_summary['aggressive_hybrid_v4']['matched_row_count']}",
    ]

    report_md = output_dir / "final_candidate_report.md"
    report_md.write_text("\n".join(lines), encoding="utf-8")

    print(
        json.dumps(
            {
                "summary_csv": str(summary_csv),
                "curve_csv": str(curve_csv),
                "monthly_csv": str(monthly_csv),
                "report_md": str(report_md),
                "candidates": summary_rows,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
