#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import csv
import json
import math
import sqlite3
import sys
import uuid
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
from pipeline.scoring import generate_daily_scores, normalize_scoring_profile
from pipeline.universe_input import load_symbols_from_universe_csv, load_symbols_with_market_scope, parse_symbols_arg

TRADING_DAYS = 252


@dataclass
class WindowResult:
    batch_id: str
    run_id: str | None
    evaluation_start_date: str
    evaluation_end_date: str | None
    period_months: int
    window_label: str
    top_n: int
    min_holding_days: int
    keep_rank_threshold: int
    scoring_version: str
    entry_gate_enabled: int
    market_scope: str
    position_stop_loss_enabled: int
    position_stop_loss_pct: float
    portfolio_dd_cut_enabled: int
    portfolio_dd_cut_pct: float
    total_return: float | None
    max_drawdown: float | None
    sharpe: float | None
    excess_return_vs_universe: float | None
    trade_count: int | None
    average_actual_position_count: float | None
    position_stop_loss_count: int | None
    portfolio_dd_cut_count: int | None
    robustness_score: float | None
    skipped: int
    skip_reason: str


@dataclass
class StrategySummary:
    batch_id: str
    strategy_key: str
    top_n: int
    min_holding_days: int
    keep_rank_threshold: int
    scoring_version: str
    entry_gate_enabled: int
    market_scope: str
    position_stop_loss_enabled: int
    position_stop_loss_pct: float
    portfolio_dd_cut_enabled: int
    portfolio_dd_cut_pct: float
    num_windows: int
    evaluated_windows: int
    skipped_windows: int
    mean_total_return: float
    median_total_return: float
    std_total_return: float
    positive_return_rate: float
    win_rate_vs_universe: float
    mean_excess_return_vs_universe: float
    mean_sharpe: float
    median_sharpe: float
    worst_1m_return: float | None
    worst_3m_return: float | None
    worst_6m_return: float | None
    worst_12m_return: float | None
    worst_mdd: float
    mean_mdd: float
    average_trade_count: float
    average_position_count: float
    stability_score: float


def _parse_int_list(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _parse_str_list(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _parse_float_list(value: str) -> list[float]:
    return [float(x.strip()) for x in value.split(",") if x.strip()]


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
    avg = sum(returns) / len(returns)
    var = sum((r - avg) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(var) * math.sqrt(TRADING_DAYS)


def _sharpe(returns: list[float]) -> float:
    vol = _volatility(returns)
    if vol == 0.0 or not returns:
        return 0.0
    return (sum(returns) / len(returns) * TRADING_DAYS) / vol


def _add_months(d: date, months: int) -> date:
    month_idx = d.month - 1 + months
    year = d.year + month_idx // 12
    month = month_idx % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last_day))


def _get_dates(conn: sqlite3.Connection) -> list[str]:
    return [r["date"] for r in conn.execute("SELECT DISTINCT date FROM daily_prices ORDER BY date").fetchall()]


def _nearest_trading_on_or_after(dates: list[str], target: str) -> str | None:
    for d in dates:
        if d >= target:
            return d
    return None


def _nearest_trading_on_or_before(dates: list[str], target: str) -> str | None:
    for d in reversed(dates):
        if d <= target:
            return d
    return None


def _generate_schedule_dates(min_date: date, max_date: date, frequency: str) -> list[date]:
    result: list[date] = []
    cur = date(min_date.year, min_date.month, 1)
    while cur <= max_date:
        if frequency == "monthly":
            result.append(cur)
            month = cur.month + 1
            year = cur.year + (1 if month > 12 else 0)
            month = 1 if month > 12 else month
            cur = date(year, month, 1)
        elif frequency == "quarterly":
            if cur.month in {1, 4, 7, 10}:
                result.append(cur)
            month = cur.month + 1
            year = cur.year + (1 if month > 12 else 0)
            month = 1 if month > 12 else month
            cur = date(year, month, 1)
        else:
            raise ValueError(f"invalid frequency: {frequency}")
    return [d for d in result if min_date <= d <= max_date]


def _compute_candidate_avg_return(conn: sqlite3.Connection, eval_dates: list[str]) -> float:
    if len(eval_dates) < 2:
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


def _build_target_holdings(
    ranked_rows: list[sqlite3.Row],
    rank_by_symbol: dict[str, int],
    current_symbols: set[str],
    entry_index_by_symbol: dict[str, int],
    current_day_index: int,
    top_n: int,
    min_holding_days: int,
    keep_rank_threshold: int,
    entry_gate_enabled: bool = False,
    min_entry_score: float = 0.0,
    require_positive_momentum20: bool = False,
    require_positive_momentum60: bool = False,
    require_above_sma20: bool = False,
    require_above_sma60: bool = False,
) -> set[str]:
    keep_due_rank = {sym for sym in current_symbols if rank_by_symbol.get(sym, 10**9) <= keep_rank_threshold}
    keep_due_holding = {
        sym for sym in current_symbols if (current_day_index - entry_index_by_symbol.get(sym, current_day_index)) < min_holding_days
    }
    kept = keep_due_rank | keep_due_holding
    target = list(kept)
    for row in ranked_rows:
        sym = row["symbol"]
        if sym in kept:
            continue
        if len(target) >= top_n:
            break
        if entry_gate_enabled:
            score = float(row["score"]) if row["score"] is not None else 0.0
            m20 = row["momentum_20d"]
            m60 = row["momentum_60d"]
            g20 = row["sma_20_gap"]
            g60 = row["sma_60_gap"]
            if score < float(min_entry_score):
                continue
            if require_positive_momentum20 and (m20 is None or float(m20) <= 0.0):
                continue
            if require_positive_momentum60 and (m60 is None or float(m60) <= 0.0):
                continue
            if require_above_sma20 and (g20 is None or float(g20) <= 0.0):
                continue
            if require_above_sma60 and (g60 is None or float(g60) <= 0.0):
                continue
        target.append(sym)
    return set(target)


def _simulate_holdings(
    conn: sqlite3.Connection,
    dates: list[str],
    top_n: int,
    min_holding_days: int,
    keep_rank_threshold: int,
    rebalance_frequency: str,
    entry_gate_enabled: bool = False,
    min_entry_score: float = 0.0,
    require_positive_momentum20: bool = False,
    require_positive_momentum60: bool = False,
    require_above_sma20: bool = False,
    require_above_sma60: bool = False,
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
                """
                SELECT s.symbol, s.rank, s.score,
                       f.momentum_20d, f.momentum_60d, f.sma_20_gap, f.sma_60_gap
                FROM daily_scores s
                LEFT JOIN daily_features f
                  ON f.symbol = s.symbol AND f.date = s.date
                WHERE s.date=?
                ORDER BY s.rank ASC, s.symbol ASC
                """,
                (d0,),
            ).fetchall()
            rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked_rows}
            target = _build_target_holdings(
                ranked_rows,
                rank_by_symbol,
                current_holdings,
                entry_index_by_symbol,
                i,
                top_n,
                min_holding_days,
                keep_rank_threshold,
                entry_gate_enabled=entry_gate_enabled,
                min_entry_score=min_entry_score,
                require_positive_momentum20=require_positive_momentum20,
                require_positive_momentum60=require_positive_momentum60,
                require_above_sma20=require_above_sma20,
                require_above_sma60=require_above_sma60,
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


def _compute_robustness_score(total_return: float, mdd: float, sharpe: float, trades: int, excess: float) -> float:
    return (0.35 * total_return) + (0.30 * sharpe) + (0.25 * excess) + (0.10 * mdd) - (0.001 * trades)


def _stddev(values: list[float]) -> float:
    if not values:
        return 0.0
    avg = mean(values)
    return math.sqrt(sum((x - avg) ** 2 for x in values) / len(values))


def _create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS start_window_robustness_batches (
            batch_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            db_path TEXT NOT NULL,
            start_dates_spec TEXT NOT NULL,
            period_months_spec TEXT NOT NULL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            run_id TEXT,
            evaluation_start_date TEXT NOT NULL,
            evaluation_end_date TEXT,
            period_months INTEGER NOT NULL,
            window_label TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            min_holding_days INTEGER NOT NULL,
            keep_rank_threshold INTEGER NOT NULL,
            scoring_version TEXT NOT NULL,
            entry_gate_enabled INTEGER NOT NULL,
            market_scope TEXT NOT NULL,
            position_stop_loss_enabled INTEGER NOT NULL,
            position_stop_loss_pct REAL NOT NULL,
            portfolio_dd_cut_enabled INTEGER NOT NULL,
            portfolio_dd_cut_pct REAL NOT NULL,
            total_return REAL,
            max_drawdown REAL,
            sharpe REAL,
            excess_return_vs_universe REAL,
            trade_count INTEGER,
            average_actual_position_count REAL,
            position_stop_loss_count INTEGER,
            portfolio_dd_cut_count INTEGER,
            robustness_score REAL,
            skipped INTEGER NOT NULL,
            skip_reason TEXT NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES start_window_robustness_batches(batch_id)
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_strategy_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            strategy_key TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            min_holding_days INTEGER NOT NULL,
            keep_rank_threshold INTEGER NOT NULL,
            scoring_version TEXT NOT NULL,
            entry_gate_enabled INTEGER NOT NULL,
            market_scope TEXT NOT NULL,
            position_stop_loss_enabled INTEGER NOT NULL,
            position_stop_loss_pct REAL NOT NULL,
            portfolio_dd_cut_enabled INTEGER NOT NULL,
            portfolio_dd_cut_pct REAL NOT NULL,
            num_windows INTEGER NOT NULL,
            evaluated_windows INTEGER NOT NULL,
            skipped_windows INTEGER NOT NULL,
            mean_total_return REAL NOT NULL,
            median_total_return REAL NOT NULL,
            std_total_return REAL NOT NULL,
            positive_return_rate REAL NOT NULL,
            win_rate_vs_universe REAL NOT NULL,
            mean_excess_return_vs_universe REAL NOT NULL,
            mean_sharpe REAL NOT NULL,
            median_sharpe REAL NOT NULL,
            worst_1m_return REAL,
            worst_3m_return REAL,
            worst_6m_return REAL,
            worst_12m_return REAL,
            worst_mdd REAL NOT NULL,
            mean_mdd REAL NOT NULL,
            average_trade_count REAL NOT NULL,
            average_position_count REAL NOT NULL,
            stability_score REAL NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES start_window_robustness_batches(batch_id)
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_period_summary (
            period_summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            period_months INTEGER NOT NULL,
            num_windows INTEGER NOT NULL,
            evaluated_windows INTEGER NOT NULL,
            skipped_windows INTEGER NOT NULL,
            mean_total_return REAL NOT NULL,
            positive_return_rate REAL NOT NULL,
            win_rate_vs_universe REAL NOT NULL,
            worst_total_return REAL,
            mean_sharpe REAL NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES start_window_robustness_batches(batch_id)
        );

        CREATE INDEX IF NOT EXISTS idx_swrr_batch ON start_window_robustness_results(batch_id, period_months, skipped);
        CREATE INDEX IF NOT EXISTS idx_swss_batch_score ON start_window_robustness_strategy_summary(batch_id, stability_score DESC);
        """
    )
    conn.commit()


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Run start-date rolling-window robustness evaluations")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--output-dir", default="data/reports")
    p.add_argument("--start-dates", default="")
    p.add_argument("--start-date-frequency", choices=["monthly", "quarterly"], default=None)
    p.add_argument("--min-start-date", default=None)
    p.add_argument("--max-start-date", default=None)
    p.add_argument("--period-months", default="1,3,6,12")
    p.add_argument("--top-n-values", default="3,5,10")
    p.add_argument("--min-holding-days-values", default="5,10")
    p.add_argument("--keep-rank-offsets", default="2,4")
    p.add_argument("--scoring-versions", default="old,hybrid_v4")
    p.add_argument("--rebalance-frequency", choices=["daily", "weekly"], default="daily")
    p.add_argument("--symbols", default="")
    p.add_argument("--universe-file")
    p.add_argument("--initial-equity", type=float, default=100000.0)
    p.add_argument("--universe-mode", choices=["static", "rolling_liquidity"], default="static")
    p.add_argument("--universe-size", type=int, default=100)
    p.add_argument("--universe-lookback-days", type=int, default=20)
    p.add_argument("--market-scopes", default="ALL")
    p.add_argument("--market-filter-modes", default="off", help="Comma-separated: off,on")
    p.add_argument("--market-filter-ma20-reduce-by", type=int, default=1)
    p.add_argument("--market-filter-ma60-mode", choices=["none", "block_new_buys", "cash"], default="block_new_buys")
    p.add_argument("--entry-gate-modes", default="off,on")
    p.add_argument("--min-entry-score-values", default="0.0")
    p.add_argument("--entry-gate-rule-set", choices=["none", "basic_trend"], default="none")
    p.add_argument("--require-positive-momentum20", action="store_true")
    p.add_argument("--require-positive-momentum60", action="store_true")
    p.add_argument("--require-above-sma20", action="store_true")
    p.add_argument("--require-above-sma60", action="store_true")
    p.add_argument("--position-stop-loss-modes", default="off,on")
    p.add_argument("--position-stop-loss-pct-values", default="0.08,0.10")
    p.add_argument("--portfolio-dd-cut-modes", default="off,on")
    p.add_argument("--portfolio-dd-cut-pct-values", default="0.10")
    p.add_argument("--portfolio-dd-cooldown-days-values", default="20")
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)
    _create_tables(conn)

    all_dates = _get_dates(conn)
    if len(all_dates) < 2:
        raise ValueError("start-window robustness 실행을 위해 최소 2거래일 이상의 daily_prices 데이터가 필요합니다.")

    min_db_date = all_dates[0]
    max_db_date = all_dates[-1]

    min_start = date.fromisoformat(args.min_start_date) if args.min_start_date else date.fromisoformat(min_db_date)
    max_start = date.fromisoformat(args.max_start_date) if args.max_start_date else date.fromisoformat(max_db_date)
    if min_start > max_start:
        raise ValueError("--min-start-date must be <= --max-start-date")

    explicit_starts = [s.strip() for s in args.start_dates.split(",") if s.strip()]
    schedule_starts: list[str] = []
    if args.start_date_frequency:
        candidate_dates = _generate_schedule_dates(min_start, max_start, args.start_date_frequency)
        for d in candidate_dates:
            trading = _nearest_trading_on_or_after(all_dates, d.isoformat())
            if trading and min_start.isoformat() <= trading <= max_start.isoformat():
                schedule_starts.append(trading)

    for d in explicit_starts:
        trading = _nearest_trading_on_or_after(all_dates, d)
        if trading:
            schedule_starts.append(trading)

    if not schedule_starts:
        raise ValueError("No evaluation start dates resolved. Check --start-dates / --start-date-frequency / min/max bounds.")

    start_dates = sorted(set([d for d in schedule_starts if min_start.isoformat() <= d <= max_start.isoformat()]))
    if not start_dates:
        raise ValueError("No start dates remain after min/max filters.")

    periods = _parse_int_list(args.period_months)
    top_ns = _parse_int_list(args.top_n_values)
    min_holding_days_values = _parse_int_list(args.min_holding_days_values)
    keep_offsets = _parse_int_list(args.keep_rank_offsets)
    scoring_versions = [normalize_scoring_profile(v) for v in _parse_str_list(args.scoring_versions)]
    market_scopes = list(dict.fromkeys([x.strip().upper() for x in args.market_scopes.split(",") if x.strip()]))
    market_filter_modes = list(dict.fromkeys([x.strip().lower() for x in args.market_filter_modes.split(",") if x.strip()]))
    entry_gate_modes = list(dict.fromkeys([x.strip().lower() for x in args.entry_gate_modes.split(",") if x.strip()]))
    min_entry_scores = _parse_float_list(args.min_entry_score_values)
    position_stop_loss_modes = list(dict.fromkeys([x.strip().lower() for x in args.position_stop_loss_modes.split(",") if x.strip()]))
    position_stop_loss_pct_values = _parse_float_list(args.position_stop_loss_pct_values)
    portfolio_dd_cut_modes = list(dict.fromkeys([x.strip().lower() for x in args.portfolio_dd_cut_modes.split(",") if x.strip()]))
    portfolio_dd_cut_pct_values = _parse_float_list(args.portfolio_dd_cut_pct_values)
    portfolio_dd_cooldown_days_values = _parse_int_list(args.portfolio_dd_cooldown_days_values)

    selected_symbols = parse_symbols_arg(args.symbols)
    selected_symbol_to_market: dict[str, str] = {s: "UNKNOWN" for s in selected_symbols}
    if args.universe_file:
        selected_symbols = load_symbols_from_universe_csv(args.universe_file)
        selected_symbols, selected_symbol_to_market = load_symbols_with_market_scope(args.universe_file, "ALL")

    lookahead_info = {"checked_rows": 0, "violations": 0}
    if args.universe_mode == "rolling_liquidity":
        build_rolling_liquidity_universe(conn, universe_size=args.universe_size, lookback_days=args.universe_lookback_days)
        lookahead_info = validate_rolling_universe_no_lookahead(
            conn,
            universe_size=args.universe_size,
            lookback_days=args.universe_lookback_days,
        )

    batch_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO start_window_robustness_batches(batch_id, created_at, db_path, start_dates_spec, period_months_spec, notes)
        VALUES(?,?,?,?,?,?)
        """,
        (
            batch_id,
            datetime.now(timezone.utc).isoformat(),
            str(Path(args.db)),
            json.dumps(start_dates, ensure_ascii=False),
            args.period_months,
            f"script=scripts/run_start_window_robustness.py universe_mode={args.universe_mode}",
        ),
    )
    conn.commit()

    detailed_results: list[WindowResult] = []

    for top_n in top_ns:
        for min_holding_days in min_holding_days_values:
            for keep_offset in keep_offsets:
                keep_rank_threshold = top_n + keep_offset
                for scoring_version in scoring_versions:
                    for market_scope in market_scopes:
                        if args.universe_file:
                            scoped_symbols, _ = load_symbols_with_market_scope(args.universe_file, market_scope)
                        elif selected_symbols:
                            scoped_symbols = selected_symbols
                        else:
                            scoped_symbols = []
                        for entry_mode in entry_gate_modes:
                            entry_gate_enabled = entry_mode == "on"
                            for min_entry_score in min_entry_scores:
                                for market_filter_mode in market_filter_modes:
                                    market_filter_enabled = market_filter_mode == "on"
                                    for pos_sl_mode in position_stop_loss_modes:
                                        enable_position_stop_loss = pos_sl_mode == "on"
                                        for pos_sl_pct in position_stop_loss_pct_values:
                                            for port_dd_mode in portfolio_dd_cut_modes:
                                                enable_portfolio_dd_cut = port_dd_mode == "on"
                                                for port_dd_cut_pct in portfolio_dd_cut_pct_values:
                                                    for port_dd_cd_days in portfolio_dd_cooldown_days_values:
                                                        require_positive_momentum20 = args.require_positive_momentum20
                                                        require_positive_momentum60 = args.require_positive_momentum60
                                                        require_above_sma20 = args.require_above_sma20
                                                        require_above_sma60 = args.require_above_sma60
                                                        if args.entry_gate_rule_set == "basic_trend":
                                                            require_positive_momentum20 = True
                                                            require_above_sma20 = True

                                                    generate_daily_scores(
                                                        conn,
                                                        include_history=True,
                                                        allowed_symbols=scoped_symbols or None,
                                                        scoring_profile=scoring_version,
                                                        universe_mode=args.universe_mode,
                                                        universe_size=args.universe_size,
                                                        universe_lookback_days=args.universe_lookback_days,
                                                    )

                                                    for evaluation_start_date in start_dates:
                                                        start_dt = date.fromisoformat(evaluation_start_date)
                                                        for period_m in periods:
                                                            window_label = f"{evaluation_start_date}_fwd_{period_m}m"
                                                            desired_end = _add_months(start_dt, period_m)
                                                            if desired_end.isoformat() > max_db_date:
                                                                detailed_results.append(
                                                                    WindowResult(
                                                                        batch_id=batch_id,
                                                                        run_id=None,
                                                                        evaluation_start_date=evaluation_start_date,
                                                                        evaluation_end_date=None,
                                                                        period_months=period_m,
                                                                        window_label=window_label,
                                                                        top_n=top_n,
                                                                        min_holding_days=min_holding_days,
                                                                        keep_rank_threshold=keep_rank_threshold,
                                                                        scoring_version=scoring_version,
                                                                        entry_gate_enabled=int(entry_gate_enabled),
                                                                        market_scope=market_scope,
                                                                        position_stop_loss_enabled=int(enable_position_stop_loss),
                                                                        position_stop_loss_pct=float(pos_sl_pct),
                                                                        portfolio_dd_cut_enabled=int(enable_portfolio_dd_cut),
                                                                        portfolio_dd_cut_pct=float(port_dd_cut_pct),
                                                                        total_return=None,
                                                                        max_drawdown=None,
                                                                        sharpe=None,
                                                                        excess_return_vs_universe=None,
                                                                        trade_count=None,
                                                                        average_actual_position_count=None,
                                                                        position_stop_loss_count=None,
                                                                        portfolio_dd_cut_count=None,
                                                                        robustness_score=None,
                                                                        skipped=1,
                                                                        skip_reason="insufficient_future_data",
                                                                    )
                                                                )
                                                                continue
                                                            evaluation_end_date = _nearest_trading_on_or_before(all_dates, desired_end.isoformat())
                                                            if evaluation_end_date is None or evaluation_end_date <= evaluation_start_date:
                                                                detailed_results.append(
                                                                    WindowResult(
                                                                        batch_id=batch_id,
                                                                        run_id=None,
                                                                        evaluation_start_date=evaluation_start_date,
                                                                        evaluation_end_date=evaluation_end_date,
                                                                        period_months=period_m,
                                                                        window_label=window_label,
                                                                        top_n=top_n,
                                                                        min_holding_days=min_holding_days,
                                                                        keep_rank_threshold=keep_rank_threshold,
                                                                        scoring_version=scoring_version,
                                                                        entry_gate_enabled=int(entry_gate_enabled),
                                                                        market_scope=market_scope,
                                                                        position_stop_loss_enabled=int(enable_position_stop_loss),
                                                                        position_stop_loss_pct=float(pos_sl_pct),
                                                                        portfolio_dd_cut_enabled=int(enable_portfolio_dd_cut),
                                                                        portfolio_dd_cut_pct=float(port_dd_cut_pct),
                                                                        total_return=None,
                                                                        max_drawdown=None,
                                                                        sharpe=None,
                                                                        excess_return_vs_universe=None,
                                                                        trade_count=None,
                                                                        average_actual_position_count=None,
                                                                        position_stop_loss_count=None,
                                                                        portfolio_dd_cut_count=None,
                                                                        robustness_score=None,
                                                                        skipped=1,
                                                                        skip_reason="insufficient_future_data",
                                                                    )
                                                                )
                                                                continue

                                                            eval_dates = [d for d in all_dates if evaluation_start_date <= d <= evaluation_end_date]
                                                            if len(eval_dates) < 2:
                                                                detailed_results.append(
                                                                    WindowResult(
                                                                        batch_id=batch_id,
                                                                        run_id=None,
                                                                        evaluation_start_date=evaluation_start_date,
                                                                        evaluation_end_date=evaluation_end_date,
                                                                        period_months=period_m,
                                                                        window_label=window_label,
                                                                        top_n=top_n,
                                                                        min_holding_days=min_holding_days,
                                                                        keep_rank_threshold=keep_rank_threshold,
                                                                        scoring_version=scoring_version,
                                                                        entry_gate_enabled=int(entry_gate_enabled),
                                                                        market_scope=market_scope,
                                                                        position_stop_loss_enabled=int(enable_position_stop_loss),
                                                                        position_stop_loss_pct=float(pos_sl_pct),
                                                                        portfolio_dd_cut_enabled=int(enable_portfolio_dd_cut),
                                                                        portfolio_dd_cut_pct=float(port_dd_cut_pct),
                                                                        total_return=None,
                                                                        max_drawdown=None,
                                                                        sharpe=None,
                                                                        excess_return_vs_universe=None,
                                                                        trade_count=None,
                                                                        average_actual_position_count=None,
                                                                        position_stop_loss_count=None,
                                                                        portfolio_dd_cut_count=None,
                                                                        robustness_score=None,
                                                                        skipped=1,
                                                                        skip_reason="insufficient_trading_days",
                                                                    )
                                                                )
                                                                continue

                                                            run_id = run_backtest(
                                                                conn,
                                                                top_n=top_n,
                                                                start_date=evaluation_start_date,
                                                                end_date=evaluation_end_date,
                                                                initial_equity=args.initial_equity,
                                                                rebalance_frequency=args.rebalance_frequency,
                                                                min_holding_days=min_holding_days,
                                                                keep_rank_threshold=keep_rank_threshold,
                                                                scoring_profile=scoring_version,
                                                                market_filter_enabled=False,
                                                                market_filter_ma20_reduce_by=args.market_filter_ma20_reduce_by,
                                                                market_filter_ma60_mode=args.market_filter_ma60_mode,
                                                                entry_gate_enabled=entry_gate_enabled,
                                                                min_entry_score=min_entry_score,
                                                                require_positive_momentum20=require_positive_momentum20,
                                                                require_positive_momentum60=require_positive_momentum60,
                                                                require_above_sma20=require_above_sma20,
                                                                require_above_sma60=require_above_sma60,
                                                                enable_position_stop_loss=enable_position_stop_loss,
                                                                position_stop_loss_pct=pos_sl_pct,
                                                                enable_portfolio_dd_cut=enable_portfolio_dd_cut,
                                                                portfolio_dd_cut_pct=port_dd_cut_pct,
                                                                portfolio_dd_cooldown_days=port_dd_cd_days,
                                                            )

                                                            bt_rows = conn.execute(
                                                                "SELECT equity,daily_return FROM backtest_results WHERE run_id=? ORDER BY date",
                                                                (run_id,),
                                                            ).fetchall()
                                                            if not bt_rows:
                                                                detailed_results.append(
                                                                    WindowResult(
                                                                        batch_id=batch_id,
                                                                        run_id=run_id,
                                                                        evaluation_start_date=evaluation_start_date,
                                                                        evaluation_end_date=evaluation_end_date,
                                                                        period_months=period_m,
                                                                        window_label=window_label,
                                                                        top_n=top_n,
                                                                        min_holding_days=min_holding_days,
                                                                        keep_rank_threshold=keep_rank_threshold,
                                                                        scoring_version=scoring_version,
                                                                        entry_gate_enabled=int(entry_gate_enabled),
                                                                        market_scope=market_scope,
                                                                        position_stop_loss_enabled=int(enable_position_stop_loss),
                                                                        position_stop_loss_pct=float(pos_sl_pct),
                                                                        portfolio_dd_cut_enabled=int(enable_portfolio_dd_cut),
                                                                        portfolio_dd_cut_pct=float(port_dd_cut_pct),
                                                                        total_return=None,
                                                                        max_drawdown=None,
                                                                        sharpe=None,
                                                                        excess_return_vs_universe=None,
                                                                        trade_count=None,
                                                                        average_actual_position_count=None,
                                                                        position_stop_loss_count=None,
                                                                        portfolio_dd_cut_count=None,
                                                                        robustness_score=None,
                                                                        skipped=1,
                                                                        skip_reason="no_backtest_rows",
                                                                    )
                                                                )
                                                                continue

                                                            returns = [float(r["daily_return"]) for r in bt_rows]
                                                            equities = [float(r["equity"]) for r in bt_rows]
                                                            total_return = _safe_div(equities[-1] - args.initial_equity, args.initial_equity)
                                                            mdd = _max_drawdown(equities)
                                                            sharpe = _sharpe(returns)
                                                            candidate_avg_return = _compute_candidate_avg_return(conn, eval_dates)
                                                            excess = total_return - candidate_avg_return
                                                            holdings_by_day = _simulate_holdings(
                                                                conn,
                                                                eval_dates,
                                                                top_n=top_n,
                                                                min_holding_days=min_holding_days,
                                                                keep_rank_threshold=keep_rank_threshold,
                                                                rebalance_frequency=args.rebalance_frequency,
                                                                entry_gate_enabled=entry_gate_enabled,
                                                                min_entry_score=min_entry_score,
                                                                require_positive_momentum20=require_positive_momentum20,
                                                                require_positive_momentum60=require_positive_momentum60,
                                                                require_above_sma20=require_above_sma20,
                                                                require_above_sma60=require_above_sma60,
                                                            )
                                                            trade_count = _estimate_trade_count(holdings_by_day)
                                                            bt_meta = conn.execute(
                                                                """
                                                                SELECT average_actual_position_count,
                                                                       position_stop_loss_count, portfolio_dd_cut_count
                                                                FROM backtest_runs WHERE run_id=?
                                                                """,
                                                                (run_id,),
                                                            ).fetchone()
                                                            avg_pos = float(bt_meta["average_actual_position_count"]) if bt_meta else 0.0
                                                            pos_sl_count = int(bt_meta["position_stop_loss_count"]) if bt_meta else 0
                                                            dd_cut_count = int(bt_meta["portfolio_dd_cut_count"]) if bt_meta else 0
                                                            robust = _compute_robustness_score(total_return, mdd, sharpe, trade_count, excess)

                                                            detailed_results.append(
                                                                WindowResult(
                                                                    batch_id=batch_id,
                                                                    run_id=run_id,
                                                                    evaluation_start_date=evaluation_start_date,
                                                                    evaluation_end_date=evaluation_end_date,
                                                                    period_months=period_m,
                                                                    window_label=window_label,
                                                                    top_n=top_n,
                                                                    min_holding_days=min_holding_days,
                                                                    keep_rank_threshold=keep_rank_threshold,
                                                                    scoring_version=scoring_version,
                                                                    entry_gate_enabled=int(entry_gate_enabled),
                                                                    market_scope=market_scope,
                                                                    position_stop_loss_enabled=int(enable_position_stop_loss),
                                                                    position_stop_loss_pct=float(pos_sl_pct),
                                                                    portfolio_dd_cut_enabled=int(enable_portfolio_dd_cut),
                                                                    portfolio_dd_cut_pct=float(port_dd_cut_pct),
                                                                    total_return=total_return,
                                                                    max_drawdown=mdd,
                                                                    sharpe=sharpe,
                                                                    excess_return_vs_universe=excess,
                                                                    trade_count=trade_count,
                                                                    average_actual_position_count=avg_pos,
                                                                    position_stop_loss_count=pos_sl_count,
                                                                    portfolio_dd_cut_count=dd_cut_count,
                                                                    robustness_score=robust,
                                                                    skipped=0,
                                                                    skip_reason="",
                                                                )
                                                            )

    if not detailed_results:
        raise ValueError("No robustness windows were produced.")

    insert_rows = [
        (
            r.batch_id,
            r.run_id,
            r.evaluation_start_date,
            r.evaluation_end_date,
            r.period_months,
            r.window_label,
            r.top_n,
            r.min_holding_days,
            r.keep_rank_threshold,
            r.scoring_version,
            r.entry_gate_enabled,
            r.market_scope,
            r.position_stop_loss_enabled,
            r.position_stop_loss_pct,
            r.portfolio_dd_cut_enabled,
            r.portfolio_dd_cut_pct,
            r.total_return,
            r.max_drawdown,
            r.sharpe,
            r.excess_return_vs_universe,
            r.trade_count,
            r.average_actual_position_count,
            r.position_stop_loss_count,
            r.portfolio_dd_cut_count,
            r.robustness_score,
            r.skipped,
            r.skip_reason,
        )
        for r in detailed_results
    ]

    conn.executemany(
        """
        INSERT INTO start_window_robustness_results(
            batch_id, run_id, evaluation_start_date, evaluation_end_date, period_months, window_label,
            top_n, min_holding_days, keep_rank_threshold, scoring_version, entry_gate_enabled, market_scope,
            position_stop_loss_enabled, position_stop_loss_pct, portfolio_dd_cut_enabled, portfolio_dd_cut_pct,
            total_return, max_drawdown, sharpe, excess_return_vs_universe, trade_count,
            average_actual_position_count, position_stop_loss_count, portfolio_dd_cut_count,
            robustness_score, skipped, skip_reason
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        insert_rows,
    )

    groups: dict[tuple, list[WindowResult]] = defaultdict(list)
    for r in detailed_results:
        key = (
            r.top_n,
            r.min_holding_days,
            r.keep_rank_threshold,
            r.scoring_version,
            r.entry_gate_enabled,
            r.market_scope,
            r.position_stop_loss_enabled,
            r.position_stop_loss_pct,
            r.portfolio_dd_cut_enabled,
            r.portfolio_dd_cut_pct,
        )
        groups[key].append(r)

    summary_rows: list[StrategySummary] = []
    for key, rows in groups.items():
        valid = [r for r in rows if not r.skipped]
        returns = [float(r.total_return) for r in valid if r.total_return is not None]
        sharpes = [float(r.sharpe) for r in valid if r.sharpe is not None]
        mdds = [float(r.max_drawdown) for r in valid if r.max_drawdown is not None]
        excess = [float(r.excess_return_vs_universe) for r in valid if r.excess_return_vs_universe is not None]
        trades = [int(r.trade_count) for r in valid if r.trade_count is not None]
        positions = [float(r.average_actual_position_count) for r in valid if r.average_actual_position_count is not None]

        per_worst: dict[int, float | None] = {}
        for p in [1, 3, 6, 12]:
            vals = [float(r.total_return) for r in valid if r.period_months == p and r.total_return is not None]
            per_worst[p] = min(vals) if vals else None

        mean_total = mean(returns) if returns else 0.0
        std_total = _stddev(returns)
        mean_sharpe = mean(sharpes) if sharpes else 0.0
        mean_excess = mean(excess) if excess else 0.0
        worst_mdd = min(mdds) if mdds else 0.0
        mean_mdd = mean(mdds) if mdds else 0.0
        avg_trade = mean(trades) if trades else 0.0
        avg_pos = mean(positions) if positions else 0.0
        stability_score = (0.35 * mean_total) + (0.30 * mean_sharpe) + (0.25 * mean_excess) + (0.10 * worst_mdd) - (0.15 * std_total) - (0.001 * avg_trade)

        summary_rows.append(
            StrategySummary(
                batch_id=batch_id,
                strategy_key=(
                    f"top_n={key[0]}|hold={key[1]}|keep={key[2]}|score={key[3]}|entry={key[4]}|scope={key[5]}|"
                    f"pos_sl={key[6]}:{key[7]:.2f}|dd_cut={key[8]}:{key[9]:.2f}"
                ),
                top_n=key[0],
                min_holding_days=key[1],
                keep_rank_threshold=key[2],
                scoring_version=key[3],
                entry_gate_enabled=key[4],
                market_scope=key[5],
                position_stop_loss_enabled=key[6],
                position_stop_loss_pct=key[7],
                portfolio_dd_cut_enabled=key[8],
                portfolio_dd_cut_pct=key[9],
                num_windows=len(rows),
                evaluated_windows=len(valid),
                skipped_windows=len(rows) - len(valid),
                mean_total_return=mean_total,
                median_total_return=median(returns) if returns else 0.0,
                std_total_return=std_total,
                positive_return_rate=(sum(1 for x in returns if x > 0) / len(returns)) if returns else 0.0,
                win_rate_vs_universe=(sum(1 for x in excess if x > 0) / len(excess)) if excess else 0.0,
                mean_excess_return_vs_universe=mean_excess,
                mean_sharpe=mean_sharpe,
                median_sharpe=median(sharpes) if sharpes else 0.0,
                worst_1m_return=per_worst[1],
                worst_3m_return=per_worst[3],
                worst_6m_return=per_worst[6],
                worst_12m_return=per_worst[12],
                worst_mdd=worst_mdd,
                mean_mdd=mean_mdd,
                average_trade_count=avg_trade,
                average_position_count=avg_pos,
                stability_score=stability_score,
            )
        )

    conn.executemany(
        """
        INSERT INTO start_window_robustness_strategy_summary(
            batch_id, strategy_key, top_n, min_holding_days, keep_rank_threshold, scoring_version, entry_gate_enabled,
            market_scope, position_stop_loss_enabled, position_stop_loss_pct, portfolio_dd_cut_enabled, portfolio_dd_cut_pct,
            num_windows, evaluated_windows, skipped_windows, mean_total_return, median_total_return, std_total_return,
            positive_return_rate, win_rate_vs_universe, mean_excess_return_vs_universe, mean_sharpe, median_sharpe,
            worst_1m_return, worst_3m_return, worst_6m_return, worst_12m_return, worst_mdd, mean_mdd,
            average_trade_count, average_position_count, stability_score
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                s.batch_id,
                s.strategy_key,
                s.top_n,
                s.min_holding_days,
                s.keep_rank_threshold,
                s.scoring_version,
                s.entry_gate_enabled,
                s.market_scope,
                s.position_stop_loss_enabled,
                s.position_stop_loss_pct,
                s.portfolio_dd_cut_enabled,
                s.portfolio_dd_cut_pct,
                s.num_windows,
                s.evaluated_windows,
                s.skipped_windows,
                s.mean_total_return,
                s.median_total_return,
                s.std_total_return,
                s.positive_return_rate,
                s.win_rate_vs_universe,
                s.mean_excess_return_vs_universe,
                s.mean_sharpe,
                s.median_sharpe,
                s.worst_1m_return,
                s.worst_3m_return,
                s.worst_6m_return,
                s.worst_12m_return,
                s.worst_mdd,
                s.mean_mdd,
                s.average_trade_count,
                s.average_position_count,
                s.stability_score,
            )
            for s in summary_rows
        ],
    )

    period_rows: list[dict] = []
    for p_month in sorted(set(periods)):
        sub = [r for r in detailed_results if r.period_months == p_month]
        valid = [r for r in sub if not r.skipped]
        returns = [float(r.total_return) for r in valid if r.total_return is not None]
        sharpes = [float(r.sharpe) for r in valid if r.sharpe is not None]
        excess = [float(r.excess_return_vs_universe) for r in valid if r.excess_return_vs_universe is not None]
        period_rows.append(
            {
                "batch_id": batch_id,
                "period_months": p_month,
                "num_windows": len(sub),
                "evaluated_windows": len(valid),
                "skipped_windows": len(sub) - len(valid),
                "mean_total_return": mean(returns) if returns else 0.0,
                "positive_return_rate": (sum(1 for x in returns if x > 0) / len(returns)) if returns else 0.0,
                "win_rate_vs_universe": (sum(1 for x in excess if x > 0) / len(excess)) if excess else 0.0,
                "worst_total_return": min(returns) if returns else None,
                "mean_sharpe": mean(sharpes) if sharpes else 0.0,
            }
        )

    conn.executemany(
        """
        INSERT INTO start_window_robustness_period_summary(
            batch_id, period_months, num_windows, evaluated_windows, skipped_windows,
            mean_total_return, positive_return_rate, win_rate_vs_universe, worst_total_return, mean_sharpe
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                r["batch_id"],
                r["period_months"],
                r["num_windows"],
                r["evaluated_windows"],
                r["skipped_windows"],
                r["mean_total_return"],
                r["positive_return_rate"],
                r["win_rate_vs_universe"],
                r["worst_total_return"],
                r["mean_sharpe"],
            )
            for r in period_rows
        ],
    )
    conn.commit()

    sorted_summary = sorted(summary_rows, key=lambda x: x.stability_score, reverse=True)
    output_dir = Path(args.output_dir)
    detail_csv = output_dir / f"start_window_robustness_results_{batch_id}.csv"
    strategy_csv = output_dir / f"start_window_robustness_strategy_summary_{batch_id}.csv"
    period_csv = output_dir / f"start_window_robustness_period_summary_{batch_id}.csv"
    summary_md = output_dir / f"start_window_robustness_summary_{batch_id}.md"

    detail_dicts = [r.__dict__ for r in detailed_results]
    summary_dicts = [s.__dict__ for s in sorted_summary]
    _write_csv(detail_csv, detail_dicts, list(detail_dicts[0].keys()))
    _write_csv(strategy_csv, summary_dicts, list(summary_dicts[0].keys()))
    _write_csv(period_csv, period_rows, list(period_rows[0].keys()))

    weakest_short = [s for s in sorted_summary if s.worst_1m_return is not None]
    weakest_short_txt = "N/A"
    if weakest_short:
        weak = min(weakest_short, key=lambda x: x.worst_1m_return if x.worst_1m_return is not None else 999)
        weakest_short_txt = f"{weak.strategy_key} (worst_1m={weak.worst_1m_return:.2%})"

    worst_window = [r for r in detailed_results if not r.skipped and r.total_return is not None]
    worst_window_txt = "N/A"
    if worst_window:
        ww = min(worst_window, key=lambda x: x.total_return if x.total_return is not None else 999)
        worst_window_txt = f"{ww.window_label} / {ww.scoring_version} / top_n={ww.top_n}: {ww.total_return:.2%}"

    largest_mdd = [r for r in detailed_results if not r.skipped and r.max_drawdown is not None]
    largest_mdd_txt = "N/A"
    if largest_mdd:
        wm = min(largest_mdd, key=lambda x: x.max_drawdown if x.max_drawdown is not None else 999)
        largest_mdd_txt = f"{wm.window_label} / {wm.scoring_version} / top_n={wm.top_n}: {wm.max_drawdown:.2%}"

    lines = [
        f"# Start-window Robustness Summary ({batch_id})",
        "",
        "## 실행 개요",
        f"- start dates: `{start_dates}`",
        f"- period months: `{periods}`",
        f"- rolling universe lookahead validation: checked={lookahead_info['checked_rows']}, violations={lookahead_info['violations']}",
        "",
        "## 가장 안정적인 설정 Top 10",
        "",
        "|rank|strategy_key|num_windows|evaluated|mean_ret|std_ret|pos_rate|win_vs_universe|mean_sharpe|stability|",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, s in enumerate(sorted_summary[:10], start=1):
        lines.append(
            f"|{idx}|{s.strategy_key}|{s.num_windows}|{s.evaluated_windows}|{s.mean_total_return:.2%}|{s.std_total_return:.2%}|{s.positive_return_rate:.2%}|{s.win_rate_vs_universe:.2%}|{s.mean_sharpe:.2f}|{s.stability_score:.4f}|"
        )

    lines.extend(
        [
            "",
            "## 기간별 요약",
            "",
            "|period|mean_return|positive_rate|win_vs_universe|worst_return|mean_sharpe|evaluated/skipped|",
            "|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for r in period_rows:
        worst = "N/A" if r["worst_total_return"] is None else f"{r['worst_total_return']:.2%}"
        lines.append(
            f"|{r['period_months']}m|{r['mean_total_return']:.2%}|{r['positive_return_rate']:.2%}|{r['win_rate_vs_universe']:.2%}|{worst}|{r['mean_sharpe']:.2f}|{r['evaluated_windows']}/{r['skipped_windows']}|"
        )

    lines.extend(
        [
            "",
            "## 해석 포인트",
            f"- 1개월 성과 반복 약세 여부: {weakest_short_txt}",
            "- 3/6/12개월 안정성은 period summary의 mean/positive/worst를 함께 확인하세요.",
            "- 특정 시작일 편중 여부는 동일 strategy_key의 std_total_return과 worst window를 함께 확인하세요.",
            f"- 후보군 평균 대비 이긴 비율은 `win_rate_vs_universe` 컬럼으로 제공합니다.",
            f"- 최악의 시작일/기간 조합: {worst_window_txt}",
            f"- MDD가 가장 컸던 설정/윈도우: {largest_mdd_txt}",
            "",
        ]
    )
    summary_md.parent.mkdir(parents=True, exist_ok=True)
    summary_md.write_text("\n".join(lines), encoding="utf-8")

    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "detail_csv": str(detail_csv),
                "strategy_csv": str(strategy_csv),
                "period_csv": str(period_csv),
                "summary_md": str(summary_md),
                "lookahead_validation": lookahead_info,
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
