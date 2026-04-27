#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
import calendar
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db
from pipeline.scoring import generate_daily_scores, normalize_scoring_profile
from pipeline.universe_input import load_symbols_from_universe_csv, parse_symbols_arg

TRADING_DAYS = 252


@dataclass
class ExperimentResult:
    batch_id: str
    run_id: str
    start_date: str
    end_date: str
    period_months: int
    top_n: int
    min_holding_days: int
    keep_rank_threshold: int
    keep_rank_offset: int
    scoring_version: str
    rebalance_frequency: str
    market_filter_enabled: int
    market_filter_ma20_reduce_by: int
    market_filter_ma60_mode: str
    ma20_trigger_count: int
    ma60_trigger_count: int
    reduced_target_count_days: int
    blocked_new_buy_days: int
    cash_mode_days: int
    total_return: float
    max_drawdown: float
    sharpe: float
    trade_count: int
    candidate_avg_return: float
    excess_return_vs_universe: float
    robustness_score: float



def _parse_int_list(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _parse_str_list(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _subtract_months(d: date, months: int) -> date:
    month_idx = d.month - 1 - months
    year = d.year + month_idx // 12
    month = month_idx % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(d.day, last_day)
    return date(year, month, day)


def _safe_div(x: float, y: float) -> float:
    return x / y if y else 0.0


def _max_drawdown(equities: list[float]) -> float:
    if not equities:
        return 0.0
    peak = equities[0]
    mdd = 0.0
    for v in equities:
        if v > peak:
            peak = v
        dd = _safe_div(v - peak, peak)
        if dd < mdd:
            mdd = dd
    return mdd


def _volatility(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(var) * math.sqrt(TRADING_DAYS)


def _sharpe(returns: list[float]) -> float:
    vol = _volatility(returns)
    if vol == 0.0 or not returns:
        return 0.0
    avg = sum(returns) / len(returns)
    return (avg * TRADING_DAYS) / vol


def _get_dates(conn: sqlite3.Connection) -> list[str]:
    return [r["date"] for r in conn.execute("SELECT DISTINCT date FROM daily_prices ORDER BY date").fetchall()]


def _build_target_holdings(
    ranked_symbols: list[str],
    rank_by_symbol: dict[str, int],
    current_symbols: set[str],
    entry_index_by_symbol: dict[str, int],
    current_day_index: int,
    top_n: int,
    min_holding_days: int,
    keep_rank_threshold: int,
) -> set[str]:
    keep_due_rank = {sym for sym in current_symbols if rank_by_symbol.get(sym, 10**9) <= keep_rank_threshold}
    keep_due_holding = {
        sym
        for sym in current_symbols
        if (current_day_index - entry_index_by_symbol.get(sym, current_day_index)) < min_holding_days
    }
    kept = keep_due_rank | keep_due_holding
    target = list(kept)
    for sym in ranked_symbols:
        if sym in kept:
            continue
        if len(target) >= top_n:
            break
        target.append(sym)
    return set(target)


def _simulate_holdings(
    conn: sqlite3.Connection,
    dates: list[str],
    top_n: int,
    min_holding_days: int,
    keep_rank_threshold: int,
    rebalance_frequency: str,
) -> list[set[str]]:
    current_holdings: set[str] = set()
    entry_index_by_symbol: dict[str, int] = {}
    holdings_by_day: list[set[str]] = []
    prev_date: str | None = None

    for i, d0 in enumerate(dates[:-1]):
        should_rebalance = True
        if rebalance_frequency == "weekly" and prev_date is not None:
            curr_week = datetime.strptime(d0, "%Y-%m-%d").date().isocalendar()[:2]
            prev_week = datetime.strptime(prev_date, "%Y-%m-%d").date().isocalendar()[:2]
            should_rebalance = curr_week != prev_week

        if should_rebalance:
            ranked_rows = conn.execute(
                "SELECT symbol, rank FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC",
                (d0,),
            ).fetchall()
            ranked_symbols = [r["symbol"] for r in ranked_rows]
            rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked_rows}
            target = _build_target_holdings(
                ranked_symbols,
                rank_by_symbol,
                current_holdings,
                entry_index_by_symbol,
                i,
                top_n,
                min_holding_days,
                keep_rank_threshold,
            )
            for sym in target - current_holdings:
                entry_index_by_symbol[sym] = i
            for sym in current_holdings - target:
                entry_index_by_symbol.pop(sym, None)
            current_holdings = target

        holdings_by_day.append(set(current_holdings))
        prev_date = d0

    return holdings_by_day


def _estimate_trade_count(holdings_by_day: list[set[str]]) -> int:
    if not holdings_by_day:
        return 0
    trades = len(holdings_by_day[0])
    for i in range(1, len(holdings_by_day)):
        prev = holdings_by_day[i - 1]
        cur = holdings_by_day[i]
        trades += len(cur - prev) + len(prev - cur)
    return trades


def _compute_candidate_avg_return(conn: sqlite3.Connection, eval_dates: list[str]) -> float:
    if not eval_dates:
        return 0.0
    equity = 1.0
    for i in range(len(eval_dates) - 1):
        d0 = eval_dates[i]
        d1 = eval_dates[i + 1]
        rows = conn.execute(
            """
            SELECT p0.close AS c0, p1.close AS c1
            FROM daily_scores s
            JOIN daily_prices p0 ON p0.symbol=s.symbol AND p0.date=s.date
            JOIN daily_prices p1 ON p1.symbol=s.symbol AND p1.date=?
            WHERE s.date=?
            """,
            (d1, d0),
        ).fetchall()
        rets = [(r["c1"] - r["c0"]) / r["c0"] for r in rows if r["c0"]]
        r = sum(rets) / len(rets) if rets else 0.0
        equity *= (1.0 + r)
    return equity - 1.0


def _compute_robustness_score(total_return: float, mdd: float, sharpe: float, trade_count: int, excess: float) -> float:
    return (
        (0.35 * total_return)
        + (0.30 * sharpe)
        + (0.25 * excess)
        + (0.10 * mdd)
        - (0.001 * trade_count)
    )


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Run robustness experiments over multiple parameter combinations")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--output-dir", default="data/reports")
    p.add_argument("--period-months", default="3,6,12")
    p.add_argument("--top-n-values", default="3,5,10")
    p.add_argument("--min-holding-days-values", default="5,10")
    p.add_argument("--keep-rank-offsets", default="2,4", help="keep_rank_threshold = top_n + offset")
    p.add_argument(
        "--scoring-versions",
        default="old,hybrid_v4",
        help="Comma-separated scoring profiles for the experiment set (default focuses on old,hybrid_v4)",
    )
    p.add_argument(
        "--include-trend-v2",
        action="store_true",
        help="Add trend_v2 as an auxiliary profile on top of --scoring-versions",
    )
    p.add_argument("--rebalance-frequency", choices=["daily", "weekly"], default="daily")
    p.add_argument("--symbols", default="", help="Comma-separated KRX 6-digit symbols")
    p.add_argument("--universe-file", help="CSV path containing at least a symbol column")
    p.add_argument("--initial-equity", type=float, default=100000.0)
    p.add_argument("--market-filter-modes", default="off,on", help="Comma-separated experiment modes: off,on")
    p.add_argument("--market-filter-ma20-reduce-by", type=int, default=1)
    p.add_argument("--market-filter-ma60-mode", choices=["none", "block_new_buys", "cash"], default="block_new_buys")
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)

    all_dates = _get_dates(conn)
    if len(all_dates) < 2:
        raise ValueError("실험 실행을 위해 최소 2거래일 이상의 daily_prices 데이터가 필요합니다.")

    end_date = all_dates[-1]
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    periods = _parse_int_list(args.period_months)
    top_ns = _parse_int_list(args.top_n_values)
    min_holding_days_values = _parse_int_list(args.min_holding_days_values)
    keep_offsets = _parse_int_list(args.keep_rank_offsets)
    scoring_versions = [normalize_scoring_profile(v) for v in _parse_str_list(args.scoring_versions)]
    if args.include_trend_v2:
        scoring_versions.append(normalize_scoring_profile("trend_v2"))
    scoring_versions = list(dict.fromkeys(scoring_versions))

    market_filter_modes = [x.strip().lower() for x in args.market_filter_modes.split(",") if x.strip()]
    market_filter_modes = list(dict.fromkeys(market_filter_modes))
    invalid_market_modes = [m for m in market_filter_modes if m not in {"off", "on"}]
    if invalid_market_modes:
        raise ValueError(f"invalid --market-filter-modes: {invalid_market_modes}")

    selected_symbols = parse_symbols_arg(args.symbols)
    if args.universe_file:
        selected_symbols = load_symbols_from_universe_csv(args.universe_file)
        print(f"[universe] loaded symbols={len(selected_symbols)} from file={args.universe_file}")
        if Path(args.universe_file).name == "kospi100_manual.csv":
            if len(selected_symbols) != 100:
                raise ValueError(
                    f"[universe] expected 100 symbols for {args.universe_file}, got {len(selected_symbols)}"
                )
            print("[universe] verified symbols=100 for kospi100_manual.csv")

    batch_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO robustness_experiment_batches(
            batch_id, created_at, db_path, end_date, rebalance_frequency, notes
        ) VALUES(?,?,?,?,?,?)
        """,
        (
            batch_id,
            created_at,
            str(Path(args.db)),
            end_date,
            args.rebalance_frequency,
            "auto-generated by scripts/run_robustness_experiments.py",
        ),
    )

    detailed_results: list[ExperimentResult] = []

    for period_m in periods:
        start_dt = _subtract_months(end_dt, period_m)
        start_date = start_dt.isoformat()

        available_dates = [d for d in all_dates if start_date <= d <= end_date]
        if len(available_dates) < 2:
            print(f"[skip] period={period_m}m has insufficient dates ({len(available_dates)})")
            continue

        for top_n in top_ns:
            for min_holding_days in min_holding_days_values:
                for keep_offset in keep_offsets:
                    keep_rank_threshold = top_n + keep_offset
                    for scoring_version in scoring_versions:
                        for market_mode in market_filter_modes:
                            market_filter_enabled = market_mode == "on"
                            generate_daily_scores(
                                conn,
                                include_history=True,
                                allowed_symbols=selected_symbols or None,
                                scoring_profile=scoring_version,
                            )
                            run_id = run_backtest(
                                conn,
                                top_n=top_n,
                                start_date=start_date,
                                end_date=end_date,
                                initial_equity=args.initial_equity,
                                rebalance_frequency=args.rebalance_frequency,
                                min_holding_days=min_holding_days,
                                keep_rank_threshold=keep_rank_threshold,
                                scoring_profile=scoring_version,
                                market_filter_enabled=market_filter_enabled,
                                market_filter_ma20_reduce_by=args.market_filter_ma20_reduce_by,
                                market_filter_ma60_mode=args.market_filter_ma60_mode,
                            )

                            bt_rows = conn.execute(
                                "SELECT date,equity,daily_return FROM backtest_results WHERE run_id=? ORDER BY date",
                                (run_id,),
                            ).fetchall()
                            if not bt_rows:
                                continue

                            returns = [float(r["daily_return"]) for r in bt_rows]
                            equities = [float(r["equity"]) for r in bt_rows]
                            total_return = _safe_div(equities[-1] - args.initial_equity, args.initial_equity)
                            mdd = _max_drawdown(equities)
                            sharpe = _sharpe(returns)
                            market_diag = conn.execute(
                                """
                                SELECT ma20_trigger_count, ma60_trigger_count, reduced_target_count_days,
                                       blocked_new_buy_days, cash_mode_days
                                FROM backtest_runs
                                WHERE run_id=?
                                """,
                                (run_id,),
                            ).fetchone()

                            holdings_by_day = _simulate_holdings(
                                conn,
                                available_dates,
                                top_n=top_n,
                                min_holding_days=min_holding_days,
                                keep_rank_threshold=keep_rank_threshold,
                                rebalance_frequency=args.rebalance_frequency,
                            )
                            trade_count = _estimate_trade_count(holdings_by_day)

                            candidate_avg_return = _compute_candidate_avg_return(conn, available_dates)
                            excess_return = total_return - candidate_avg_return
                            robust_score = _compute_robustness_score(
                                total_return=total_return,
                                mdd=mdd,
                                sharpe=sharpe,
                                trade_count=trade_count,
                                excess=excess_return,
                            )

                            result = ExperimentResult(
                                batch_id=batch_id,
                                run_id=run_id,
                                start_date=start_date,
                                end_date=end_date,
                                period_months=period_m,
                                top_n=top_n,
                                min_holding_days=min_holding_days,
                                keep_rank_threshold=keep_rank_threshold,
                                keep_rank_offset=keep_offset,
                                scoring_version=scoring_version,
                                rebalance_frequency=args.rebalance_frequency,
                                market_filter_enabled=int(market_filter_enabled),
                                market_filter_ma20_reduce_by=int(max(0, args.market_filter_ma20_reduce_by)),
                                market_filter_ma60_mode=args.market_filter_ma60_mode,
                                ma20_trigger_count=int(market_diag["ma20_trigger_count"]) if market_diag else 0,
                                ma60_trigger_count=int(market_diag["ma60_trigger_count"]) if market_diag else 0,
                                reduced_target_count_days=int(market_diag["reduced_target_count_days"]) if market_diag else 0,
                                blocked_new_buy_days=int(market_diag["blocked_new_buy_days"]) if market_diag else 0,
                                cash_mode_days=int(market_diag["cash_mode_days"]) if market_diag else 0,
                                total_return=total_return,
                                max_drawdown=mdd,
                                sharpe=sharpe,
                                trade_count=trade_count,
                                candidate_avg_return=candidate_avg_return,
                                excess_return_vs_universe=excess_return,
                                robustness_score=robust_score,
                            )
                            detailed_results.append(result)

                            conn.execute(
                            """
                            INSERT INTO robustness_experiment_results(
                                batch_id, run_id, start_date, end_date, period_months,
                                top_n, min_holding_days, keep_rank_threshold, keep_rank_offset,
                                scoring_version, rebalance_frequency,
                                market_filter_enabled, market_filter_ma20_reduce_by, market_filter_ma60_mode,
                                ma20_trigger_count, ma60_trigger_count, reduced_target_count_days, blocked_new_buy_days, cash_mode_days,
                                total_return, max_drawdown, sharpe, trade_count,
                                candidate_avg_return, excess_return_vs_universe, robustness_score
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                result.batch_id,
                                result.run_id,
                                result.start_date,
                                result.end_date,
                                result.period_months,
                                result.top_n,
                                result.min_holding_days,
                                result.keep_rank_threshold,
                                result.keep_rank_offset,
                                result.scoring_version,
                                result.rebalance_frequency,
                                result.market_filter_enabled,
                                result.market_filter_ma20_reduce_by,
                                result.market_filter_ma60_mode,
                                result.ma20_trigger_count,
                                result.ma60_trigger_count,
                                result.reduced_target_count_days,
                                result.blocked_new_buy_days,
                                result.cash_mode_days,
                                result.total_return,
                                result.max_drawdown,
                                result.sharpe,
                                result.trade_count,
                                result.candidate_avg_return,
                                result.excess_return_vs_universe,
                                result.robustness_score,
                            ),
                        )
                            conn.commit()
                            print(
                                "[ok]",
                                f"period={period_m}m top_n={top_n} hold={min_holding_days} keep={keep_rank_threshold} score={scoring_version} mfilter={market_mode}",
                                f"run_id={run_id[:8]} total={total_return:.2%} sharpe={sharpe:.2f} excess={excess_return:.2%}",
                            )

    if not detailed_results:
        raise ValueError("실험 결과가 없습니다. 입력 기간/데이터를 확인하세요.")

    sorted_detailed = sorted(detailed_results, key=lambda x: x.robustness_score, reverse=True)

    stable_groups: dict[str, list[ExperimentResult]] = defaultdict(list)
    for r in detailed_results:
        key = f"top_n={r.top_n}|hold={r.min_holding_days}|keep_offset={r.keep_rank_offset}|scoring={r.scoring_version}|mfilter={r.market_filter_enabled}|ma20cut={r.market_filter_ma20_reduce_by}|ma60={r.market_filter_ma60_mode}"
        stable_groups[key].append(r)

    stability_rows: list[dict[str, float | int | str]] = []
    for key, group in stable_groups.items():
        total_returns = [g.total_return for g in group]
        sharpes = [g.sharpe for g in group]
        mdds = [g.max_drawdown for g in group]
        excesses = [g.excess_return_vs_universe for g in group]
        trades = [g.trade_count for g in group]

        mean_total = sum(total_returns) / len(total_returns)
        mean_sharpe = sum(sharpes) / len(sharpes)
        worst_mdd = min(mdds)
        mean_excess = sum(excesses) / len(excesses)
        mean_trade = sum(trades) / len(trades)
        std_total = math.sqrt(sum((x - mean_total) ** 2 for x in total_returns) / len(total_returns))

        stability_score = (
            (0.35 * mean_total)
            + (0.30 * mean_sharpe)
            + (0.25 * mean_excess)
            + (0.10 * worst_mdd)
            - (0.15 * std_total)
            - (0.001 * mean_trade)
        )

        row = {
            "batch_id": batch_id,
            "stability_group_key": key,
            "num_periods": len(group),
            "mean_total_return": mean_total,
            "std_total_return": std_total,
            "mean_sharpe": mean_sharpe,
            "worst_mdd": worst_mdd,
            "mean_excess_return_vs_universe": mean_excess,
            "mean_trade_count": mean_trade,
            "stability_score": stability_score,
        }
        stability_rows.append(row)

        conn.execute(
            """
            INSERT INTO robustness_experiment_stability(
                batch_id, stability_group_key, num_periods,
                mean_total_return, std_total_return, mean_sharpe,
                worst_mdd, mean_excess_return_vs_universe,
                mean_trade_count, stability_score
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row["batch_id"],
                row["stability_group_key"],
                row["num_periods"],
                row["mean_total_return"],
                row["std_total_return"],
                row["mean_sharpe"],
                row["worst_mdd"],
                row["mean_excess_return_vs_universe"],
                row["mean_trade_count"],
                row["stability_score"],
            ),
        )
    conn.commit()

    stability_rows.sort(key=lambda x: float(x["stability_score"]), reverse=True)

    output_dir = Path(args.output_dir)
    detail_csv = output_dir / f"robustness_experiments_{batch_id}.csv"
    stability_csv = output_dir / f"robustness_stability_{batch_id}.csv"
    summary_md = output_dir / f"robustness_summary_{batch_id}.md"

    detail_dicts = [r.__dict__ for r in sorted_detailed]
    _write_csv(detail_csv, detail_dicts, list(detail_dicts[0].keys()))
    _write_csv(stability_csv, stability_rows, list(stability_rows[0].keys()))

    best_stable = stability_rows[0]
    top_details = sorted_detailed[:10]
    lines = [
        f"# Robustness Experiment Summary ({batch_id})",
        "",
        f"- End date: `{end_date}`",
        f"- Periods: `{periods}` months",
        f"- Top-N candidates: `{top_ns}`",
        f"- Min holding days: `{min_holding_days_values}`",
        f"- Keep-rank offsets: `{keep_offsets}` (keep_rank_threshold = top_n + offset)",
        f"- Scoring versions: `{scoring_versions}`",
        f"- Market filter modes: `{market_filter_modes}` (ma20_reduce_by={args.market_filter_ma20_reduce_by}, ma60_mode={args.market_filter_ma60_mode})",
        f"- Rebalance frequency: `{args.rebalance_frequency}`",
        f"- Universe filter input: `{'--universe-file' if args.universe_file else '--symbols' if selected_symbols else 'all symbols in DB'}`",
        f"- Universe size: `{len(selected_symbols) if selected_symbols else 'ALL'}`",
        "",
        "## 한눈에 해석",
        f"- 가장 안정적인 설정(기간 평균 기준): **{best_stable['stability_group_key']}**",
        f"- 위 설정은 평균 총수익률 `{best_stable['mean_total_return']:.2%}`, 최악 MDD `{best_stable['worst_mdd']:.2%}`, 평균 샤프 `{best_stable['mean_sharpe']:.2f}`를 기록했습니다.",
        f"- 후보군 평균 대비 평균 초과수익은 `{best_stable['mean_excess_return_vs_universe']:.2%}` 입니다.",
        "",
        "## 상위 10개 개별 실험(robustness_score 기준)",
        "",
        "|rank|period|top_n|min_hold|keep_threshold|scoring|market_filter|ma20_hit|ma60_hit|reduce_days|block_days|cash_days|total_return|MDD|sharpe|trades|excess_vs_universe|score|",
        "|---:|---:|---:|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, r in enumerate(top_details, start=1):
        lines.append(
            f"|{idx}|{r.period_months}m|{r.top_n}|{r.min_holding_days}|{r.keep_rank_threshold}|{r.scoring_version}|"
            f"{'ON' if r.market_filter_enabled else 'OFF'}({r.market_filter_ma60_mode})|"
            f"{r.ma20_trigger_count}|{r.ma60_trigger_count}|{r.reduced_target_count_days}|{r.blocked_new_buy_days}|{r.cash_mode_days}|"
            f"{r.total_return:.2%}|{r.max_drawdown:.2%}|{r.sharpe:.2f}|{r.trade_count}|{r.excess_return_vs_universe:.2%}|{r.robustness_score:.4f}|"
        )

    summary_md.parent.mkdir(parents=True, exist_ok=True)
    summary_md.write_text("\n".join(lines), encoding="utf-8")

    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "experiments": len(detailed_results),
                "detail_csv": str(detail_csv),
                "stability_csv": str(stability_csv),
                "summary_md": str(summary_md),
                "best_stable_config": best_stable,
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
