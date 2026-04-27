#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import json
import math
import sqlite3
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db
from pipeline.dynamic_universe import build_rolling_liquidity_universe, validate_rolling_universe_no_lookahead
from pipeline.scoring import generate_daily_scores, normalize_scoring_profile
from pipeline.universe_input import load_symbols_from_universe_csv, load_symbols_with_market_scope, parse_symbols_arg

TRADING_DAYS = 252
SUPPORTED_SKIP_REASONS = {
    "insufficient_future_data",
    "insufficient_warmup_data",
    "no_trading_days",
    "no_scores",
    "no_tradable_universe",
    "invalid_config",
    "backtest_error",
}
STABILITY_WEIGHTS = {1: 0.10, 3: 0.20, 6: 0.30, 12: 0.40}


@dataclass
class WindowResult:
    batch_id: str
    config_id: str
    requested_start_date: str
    actual_start_date: str | None
    requested_end_date: str
    actual_end_date: str | None
    period_months: int
    actual_trading_days: int
    skipped: int
    skip_reason: str | None
    top_n: int
    min_holding_days: int
    keep_rank_offset: int
    keep_rank_threshold: int
    scoring_version: str
    market_scope: str
    benchmark_mode: str
    benchmark_return: float | None
    excess_return: float | None
    win_vs_benchmark: int | None
    total_return: float | None
    max_drawdown: float | None
    sharpe: float | None
    trade_count: int | None
    position_stop_loss_enabled: int
    position_stop_loss_pct: float | None
    portfolio_dd_cut_enabled: int
    portfolio_dd_cut_pct: float | None
    run_id: str | None


def _parse_int_list(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _parse_str_list(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _parse_date_list(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _parse_optional_float_list(value: str) -> list[float | None]:
    out: list[float | None] = []
    for x in _parse_str_list(value):
        if x.lower() in {"none", "off", "null", "na"}:
            out.append(None)
        else:
            out.append(float(x))
    return out


def _safe_div(x: float, y: float) -> float:
    return x / y if y else 0.0


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


def _add_months(d: date, months: int) -> date:
    month_idx = d.month - 1 + months
    year = d.year + month_idx // 12
    month = month_idx % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(d.day, last_day)
    return date(year, month, day)


def _find_first_trading_on_or_after(all_dates: list[str], target: str) -> str | None:
    for d in all_dates:
        if d >= target:
            return d
    return None


def _find_last_trading_on_or_before(all_dates: list[str], target: str) -> str | None:
    for d in reversed(all_dates):
        if d <= target:
            return d
    return None


def _count_warmup_days(all_dates: list[str], actual_start_date: str) -> int:
    return len([d for d in all_dates if d < actual_start_date])


def _get_dates(conn: sqlite3.Connection) -> list[str]:
    return [r["date"] for r in conn.execute("SELECT DISTINCT date FROM daily_prices ORDER BY date").fetchall()]


def _get_window_dates(all_dates: list[str], start_date: str, end_date: str) -> list[str]:
    return [d for d in all_dates if start_date <= d <= end_date]


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _generate_anchor_dates(freq: str, min_date: str, max_date: str) -> list[str]:
    if freq == "none":
        return []
    start = datetime.strptime(min_date, "%Y-%m-%d").date().replace(day=1)
    stop = datetime.strptime(max_date, "%Y-%m-%d").date()
    step = 1 if freq == "monthly" else 3
    out: list[str] = []
    cursor = start
    while cursor <= stop:
        out.append(cursor.isoformat())
        cursor = _add_months(cursor, step)
    return out


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS start_window_robustness_batches (
            batch_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            db_path TEXT NOT NULL,
            min_start_date TEXT,
            max_start_date TEXT,
            start_date_frequency TEXT NOT NULL,
            min_warmup_days INTEGER NOT NULL,
            benchmark_mode TEXT NOT NULL,
            commission_bps REAL,
            tax_bps REAL,
            slippage_bps REAL,
            cost_model TEXT,
            stability_score_description TEXT,
            score_rows_used INTEGER,
            universe_rows_used INTEGER,
            score_generation_scope TEXT,
            universe_generation_scope TEXT,
            lookahead_checked INTEGER,
            lookahead_violations INTEGER
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            config_id TEXT NOT NULL,
            requested_start_date TEXT NOT NULL,
            actual_start_date TEXT,
            requested_end_date TEXT NOT NULL,
            actual_end_date TEXT,
            actual_trading_days INTEGER NOT NULL,
            period_months INTEGER NOT NULL,
            skipped INTEGER NOT NULL,
            skip_reason TEXT,
            top_n INTEGER NOT NULL,
            min_holding_days INTEGER NOT NULL,
            keep_rank_offset INTEGER NOT NULL,
            keep_rank_threshold INTEGER NOT NULL,
            scoring_version TEXT NOT NULL,
            market_scope TEXT NOT NULL,
            benchmark_mode TEXT NOT NULL,
            benchmark_return REAL,
            excess_return REAL,
            win_vs_benchmark INTEGER,
            total_return REAL,
            max_drawdown REAL,
            sharpe REAL,
            trade_count INTEGER,
            position_stop_loss_enabled INTEGER NOT NULL,
            position_stop_loss_pct REAL,
            portfolio_dd_cut_enabled INTEGER NOT NULL,
            portfolio_dd_cut_pct REAL,
            run_id TEXT
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_strategy_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            config_id TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            min_holding_days INTEGER NOT NULL,
            keep_rank_offset INTEGER NOT NULL,
            keep_rank_threshold INTEGER NOT NULL,
            scoring_version TEXT NOT NULL,
            market_scope TEXT NOT NULL,
            position_stop_loss_enabled INTEGER NOT NULL,
            position_stop_loss_pct REAL,
            portfolio_dd_cut_enabled INTEGER NOT NULL,
            portfolio_dd_cut_pct REAL,
            num_windows INTEGER NOT NULL,
            evaluated_windows INTEGER NOT NULL,
            skipped_windows INTEGER NOT NULL,
            evaluated_ratio REAL NOT NULL,
            mean_total_return REAL,
            mean_excess_return REAL,
            mean_sharpe REAL,
            worst_mdd REAL,
            win_rate REAL,
            win_vs_benchmark_rate REAL,
            stability_score REAL,
            benchmark_mode TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_period_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            period_months INTEGER NOT NULL,
            num_windows INTEGER NOT NULL,
            evaluated_windows INTEGER NOT NULL,
            skipped_windows INTEGER NOT NULL,
            evaluated_ratio REAL NOT NULL,
            mean_total_return REAL,
            mean_excess_return REAL,
            mean_sharpe REAL,
            worst_mdd REAL,
            win_rate REAL,
            win_vs_benchmark_rate REAL,
            benchmark_mode TEXT NOT NULL
        );
        """
    )
    conn.commit()


def _estimate_trade_count(conn: sqlite3.Connection, run_id: str) -> int:
    try:
        rows = conn.execute(
            "SELECT date, group_concat(symbol) AS syms FROM backtest_holdings WHERE run_id=? GROUP BY date ORDER BY date",
            (run_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    if not rows:
        return 0
    trade_count = 0
    prev: set[str] = set()
    for r in rows:
        curr = set((r["syms"] or "").split(",")) if r["syms"] else set()
        curr.discard("")
        trade_count += len(curr - prev) + len(prev - curr)
        prev = curr
    return trade_count


def _calculate_benchmark_return(
    conn: sqlite3.Connection,
    benchmark_mode: str,
    window_dates: list[str],
    source_symbols: list[str] | None,
    universe_size: int,
    universe_lookback_days: int,
) -> float | None:
    if benchmark_mode == "none":
        return None
    if len(window_dates) < 2:
        return None

    date_pairs = list(zip(window_dates[:-1], window_dates[1:]))
    daily_avg: list[float] = []
    for d0, d1 in date_pairs:
        if benchmark_mode == "source_universe_equal_weight":
            if not source_symbols:
                rows = conn.execute(
                    """
                    SELECT p1.symbol, p0.close AS c0, p1.close AS c1
                    FROM daily_prices p1 JOIN daily_prices p0
                    ON p0.symbol=p1.symbol AND p0.date=?
                    WHERE p1.date=?
                    """,
                    (d0, d1),
                ).fetchall()
            else:
                placeholders = ",".join("?" for _ in source_symbols)
                rows = conn.execute(
                    f"""
                    SELECT p1.symbol, p0.close AS c0, p1.close AS c1
                    FROM daily_prices p1 JOIN daily_prices p0
                    ON p0.symbol=p1.symbol AND p0.date=?
                    WHERE p1.date=? AND p1.symbol IN ({placeholders})
                    """,
                    [d0, d1, *source_symbols],
                ).fetchall()
        elif benchmark_mode == "rolling_universe_equal_weight":
            rows = conn.execute(
                """
                SELECT p1.symbol, p0.close AS c0, p1.close AS c1
                FROM daily_universe u
                JOIN daily_prices p1 ON p1.symbol=u.symbol AND p1.date=?
                JOIN daily_prices p0 ON p0.symbol=u.symbol AND p0.date=?
                WHERE u.date=? AND u.universe_mode='rolling_liquidity' AND u.universe_size=? AND u.lookback_days=?
                """,
                (d1, d0, d0, int(universe_size), int(universe_lookback_days)),
            ).fetchall()
        elif benchmark_mode == "kospi_index":
            return None
        else:
            return None

        rets = [float(r["c1"]) / float(r["c0"]) - 1.0 for r in rows if r["c0"] and r["c1"]]
        if not rets:
            return None
        daily_avg.append(sum(rets) / len(rets))

    eq = 1.0
    for r in daily_avg:
        eq *= 1.0 + r
    return eq - 1.0


def _build_config_id(d: dict) -> str:
    raw = json.dumps(d, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def main() -> None:
    p = argparse.ArgumentParser(description="Forward rolling-window robustness by start-date anchors")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--output-dir", default="data/reports")
    p.add_argument("--start-dates", default="")
    p.add_argument("--start-date-frequency", choices=["none", "monthly", "quarterly"], default="monthly")
    p.add_argument("--min-start-date", required=True)
    p.add_argument("--max-start-date", required=True)
    p.add_argument("--period-months", default="1,3,6,12")
    p.add_argument("--top-n-values", default="3,5")
    p.add_argument("--min-holding-days-values", default="3,5")
    p.add_argument("--keep-rank-offsets", default="2")
    p.add_argument("--scoring-versions", default="old")
    p.add_argument("--rebalance-frequency", choices=["daily", "weekly"], default="daily")
    p.add_argument("--symbols", default="")
    p.add_argument("--universe-file")
    p.add_argument("--universe-mode", choices=["static", "rolling_liquidity"], default="static")
    p.add_argument("--universe-size", type=int, default=100)
    p.add_argument("--universe-lookback-days", type=int, default=20)
    p.add_argument("--market-scopes", default="ALL")
    p.add_argument("--entry-gate-modes", default="off")
    p.add_argument("--market-filter-modes", default="off")
    p.add_argument("--market-filter-ma20-reduce-by", type=int, default=1)
    p.add_argument("--market-filter-ma60-mode", choices=["none", "block_new_buys", "cash"], default="block_new_buys")
    p.add_argument("--position-stop-loss-modes", default="off")
    p.add_argument("--position-stop-loss-pcts", default="none")
    p.add_argument("--portfolio-dd-cut-modes", default="off")
    p.add_argument("--portfolio-dd-cut-pcts", default="none")
    p.add_argument("--portfolio-dd-cooldown-days", type=int, default=20)
    p.add_argument("--benchmark-mode", choices=["kospi_index", "source_universe_equal_weight", "rolling_universe_equal_weight", "none"], default="none")
    p.add_argument("--min-warmup-days", type=int, default=60)
    p.add_argument("--allow-lookahead-violations", action="store_true")
    p.add_argument("--commission-bps", type=float, default=0.0)
    p.add_argument("--tax-bps", type=float, default=0.0)
    p.add_argument("--slippage-bps", type=float, default=0.0)
    p.add_argument("--cost-model", default="unspecified")
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)
    _ensure_tables(conn)

    all_dates = _get_dates(conn)
    if len(all_dates) < 2:
        raise ValueError("Need at least 2 trading dates")

    selected_symbols = parse_symbols_arg(args.symbols)
    selected_symbol_to_market: dict[str, str] = {s: "UNKNOWN" for s in selected_symbols}
    if args.universe_file:
        selected_symbols = load_symbols_from_universe_csv(args.universe_file)
        all_symbols, selected_symbol_to_market = load_symbols_with_market_scope(args.universe_file, "ALL")
        selected_symbols = all_symbols

    if args.universe_mode == "rolling_liquidity":
        summary = build_rolling_liquidity_universe(conn, args.universe_size, args.universe_lookback_days)
        lookahead = validate_rolling_universe_no_lookahead(conn, args.universe_size, args.universe_lookback_days)
        print(f"[daily_universe] {summary}")
        print(f"[lookahead] checked={lookahead['checked_rows']} violations={lookahead['violations']}")
        if lookahead["violations"] > 0 and not args.allow_lookahead_violations:
            raise ValueError("lookahead violations detected; rerun with --allow-lookahead-violations to continue")
    else:
        lookahead = {"checked_rows": 0, "violations": 0}

    requested_dates = set(_parse_date_list(args.start_dates))
    requested_dates.update(_generate_anchor_dates(args.start_date_frequency, args.min_start_date, args.max_start_date))
    requested_dates = {d for d in requested_dates if args.min_start_date <= d <= args.max_start_date}
    requested_dates_sorted = sorted(requested_dates)
    if not requested_dates_sorted:
        raise ValueError("No requested start dates generated")

    # start/end are both inclusive in this script.
    mapped: dict[str, list[str]] = defaultdict(list)
    for req in requested_dates_sorted:
        actual = _find_first_trading_on_or_after(all_dates, req)
        mapped[actual or "NONE"].append(req)

    dedup_pairs: list[tuple[str, str]] = []
    duplicate_logs: list[str] = []
    for actual, reqs in mapped.items():
        if actual == "NONE":
            for r in reqs:
                dedup_pairs.append((r, ""))
            continue
        reqs_sorted = sorted(reqs)
        dedup_pairs.append((reqs_sorted[0], actual))
        if len(reqs_sorted) > 1:
            duplicate_logs.append(f"actual_start_date={actual} <= requested={reqs_sorted}")

    periods = _parse_int_list(args.period_months)
    top_ns = _parse_int_list(args.top_n_values)
    holds = _parse_int_list(args.min_holding_days_values)
    keep_offsets = _parse_int_list(args.keep_rank_offsets)
    scoring_versions = [normalize_scoring_profile(v) for v in _parse_str_list(args.scoring_versions)]
    market_scopes = [x.strip().upper() for x in args.market_scopes.split(",") if x.strip()]
    entry_modes = [x.strip().lower() for x in args.entry_gate_modes.split(",") if x.strip()]
    market_filter_modes = [x.strip().lower() for x in args.market_filter_modes.split(",") if x.strip()]
    pos_modes = [x.strip().lower() for x in args.position_stop_loss_modes.split(",") if x.strip()]
    pos_pcts = _parse_optional_float_list(args.position_stop_loss_pcts)
    dd_modes = [x.strip().lower() for x in args.portfolio_dd_cut_modes.split(",") if x.strip()]
    dd_pcts = _parse_optional_float_list(args.portfolio_dd_cut_pcts)

    batch_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    all_results: list[WindowResult] = []
    score_rows_used = 0
    universe_rows_used = 0
    score_generation_scope = ""
    universe_generation_scope = ""

    for scoring_version in scoring_versions:
        for market_scope in market_scopes:
            if args.universe_file:
                scoped_symbols, _ = load_symbols_with_market_scope(args.universe_file, market_scope)
            elif selected_symbols:
                scoped_symbols = selected_symbols
            else:
                scoped_symbols = []

            # stale daily_scores mixing guard: regenerate for this exact scope.
            conn.execute("DELETE FROM daily_scores")
            conn.commit()
            generate_daily_scores(
                conn,
                include_history=True,
                allowed_symbols=scoped_symbols or None,
                scoring_profile=scoring_version,
                universe_mode=args.universe_mode,
                universe_size=args.universe_size,
                universe_lookback_days=args.universe_lookback_days,
            )
            scope_where = "ALL" if not scoped_symbols else f"symbols={len(scoped_symbols)}"
            score_generation_scope = (
                f"scoring_version={scoring_version};market_scope={market_scope};universe_mode={args.universe_mode};"
                f"universe_size={args.universe_size};universe_lookback_days={args.universe_lookback_days};{scope_where}"
            )
            score_rows_used = conn.execute("SELECT COUNT(*) AS c FROM daily_scores").fetchone()["c"]
            if args.universe_mode == "rolling_liquidity":
                universe_rows_used = conn.execute(
                    "SELECT COUNT(*) AS c FROM daily_universe WHERE universe_mode='rolling_liquidity' AND universe_size=? AND lookback_days=?",
                    (args.universe_size, args.universe_lookback_days),
                ).fetchone()["c"]
            universe_generation_scope = (
                f"universe_mode={args.universe_mode};universe_size={args.universe_size};"
                f"universe_lookback_days={args.universe_lookback_days};market_scope={market_scope}"
            )

            for top_n in top_ns:
                for hold_days in holds:
                    for keep_offset in keep_offsets:
                        keep_threshold = top_n + keep_offset
                        for period_m in periods:
                            for req_start, actual_start in dedup_pairs:
                                start_d = datetime.strptime(req_start, "%Y-%m-%d").date()
                                req_end = (_add_months(start_d, period_m) - timedelta(days=1)).isoformat()
                                if not actual_start:
                                    all_results.append(WindowResult(
                                        batch_id, "", req_start, None, req_end, None, period_m, 0, 1,
                                        "no_trading_days", top_n, hold_days, keep_offset, keep_threshold,
                                        scoring_version, market_scope, args.benchmark_mode, None, None, None,
                                        None, None, None, None, 0, None, 0, None, None
                                    ))
                                    continue

                                if keep_threshold < top_n:
                                    all_results.append(WindowResult(
                                        batch_id, "", req_start, actual_start, req_end, None, period_m, 0, 1,
                                        "invalid_config", top_n, hold_days, keep_offset, keep_threshold,
                                        scoring_version, market_scope, args.benchmark_mode, None, None, None,
                                        None, None, None, None, 0, None, 0, None, None
                                    ))
                                    continue

                                warmup_days = _count_warmup_days(all_dates, actual_start)
                                actual_end = _find_last_trading_on_or_before(all_dates, req_end)
                                window_dates = _get_window_dates(all_dates, actual_start, actual_end) if actual_end else []

                                if warmup_days < args.min_warmup_days:
                                    all_results.append(WindowResult(
                                        batch_id, "", req_start, actual_start, req_end, actual_end, period_m, len(window_dates), 1,
                                        "insufficient_warmup_data", top_n, hold_days, keep_offset, keep_threshold,
                                        scoring_version, market_scope, args.benchmark_mode, None, None, None,
                                        None, None, None, None, 0, None, 0, None, None
                                    ))
                                    continue
                                if not actual_end or len(window_dates) < 2:
                                    all_results.append(WindowResult(
                                        batch_id, "", req_start, actual_start, req_end, actual_end, period_m, len(window_dates), 1,
                                        "insufficient_future_data", top_n, hold_days, keep_offset, keep_threshold,
                                        scoring_version, market_scope, args.benchmark_mode, None, None, None,
                                        None, None, None, None, 0, None, 0, None, None
                                    ))
                                    continue

                                ds_count = conn.execute(
                                    "SELECT COUNT(*) AS c FROM daily_scores WHERE date=?",
                                    (actual_start,),
                                ).fetchone()["c"]
                                if ds_count == 0:
                                    all_results.append(WindowResult(
                                        batch_id, "", req_start, actual_start, req_end, actual_end, period_m, len(window_dates), 1,
                                        "no_scores", top_n, hold_days, keep_offset, keep_threshold,
                                        scoring_version, market_scope, args.benchmark_mode, None, None, None,
                                        None, None, None, None, 0, None, 0, None, None
                                    ))
                                    continue

                                for entry_mode in entry_modes:
                                    for mf_mode in market_filter_modes:
                                        for pos_mode in pos_modes:
                                            for pos_pct in pos_pcts:
                                                for dd_mode in dd_modes:
                                                    for dd_pct in dd_pcts:
                                                        pos_enabled = pos_mode == "on" and pos_pct is not None
                                                        dd_enabled = dd_mode == "on" and dd_pct is not None
                                                        config = {
                                                            "top_n": top_n,
                                                            "min_holding_days": hold_days,
                                                            "keep_rank_offset": keep_offset,
                                                            "keep_rank_threshold": keep_threshold,
                                                            "scoring_version": scoring_version,
                                                            "entry_gate_mode": entry_mode,
                                                            "market_filter_mode": mf_mode,
                                                            "position_stop_loss_enabled": pos_enabled,
                                                            "position_stop_loss_pct": pos_pct,
                                                            "portfolio_dd_cut_enabled": dd_enabled,
                                                            "portfolio_dd_cut_pct": dd_pct,
                                                            "portfolio_dd_cooldown_days": args.portfolio_dd_cooldown_days,
                                                            "market_scope": market_scope,
                                                        }
                                                        config_id = _build_config_id(config)
                                                        try:
                                                            run_id = run_backtest(
                                                                conn,
                                                                top_n=top_n,
                                                                start_date=actual_start,
                                                                end_date=actual_end,
                                                                rebalance_frequency=args.rebalance_frequency,
                                                                min_holding_days=hold_days,
                                                                keep_rank_threshold=keep_threshold,
                                                                scoring_profile=scoring_version,
                                                                market_filter_enabled=(mf_mode == "on"),
                                                                market_filter_ma20_reduce_by=args.market_filter_ma20_reduce_by,
                                                                market_filter_ma60_mode=args.market_filter_ma60_mode,
                                                                entry_gate_enabled=(entry_mode == "on"),
                                                                enable_position_stop_loss=pos_enabled,
                                                                position_stop_loss_pct=float(pos_pct or 0.0),
                                                                enable_portfolio_dd_cut=dd_enabled,
                                                                portfolio_dd_cut_pct=float(dd_pct or 0.0),
                                                                portfolio_dd_cooldown_days=args.portfolio_dd_cooldown_days,
                                                            )
                                                        except Exception:
                                                            all_results.append(WindowResult(
                                                                batch_id, config_id, req_start, actual_start, req_end, actual_end, period_m,
                                                                len(window_dates), 1, "backtest_error", top_n, hold_days, keep_offset,
                                                                keep_threshold, scoring_version, market_scope, args.benchmark_mode,
                                                                None, None, None, None, None, None, None,
                                                                int(pos_enabled), pos_pct, int(dd_enabled), dd_pct, None
                                                            ))
                                                            continue

                                                        bt_rows = conn.execute(
                                                            "SELECT date,equity,daily_return FROM backtest_results WHERE run_id=? ORDER BY date",
                                                            (run_id,),
                                                        ).fetchall()
                                                        if not bt_rows:
                                                            all_results.append(WindowResult(
                                                                batch_id, config_id, req_start, actual_start, req_end, actual_end, period_m,
                                                                len(window_dates), 1, "no_tradable_universe", top_n, hold_days,
                                                                keep_offset, keep_threshold, scoring_version, market_scope,
                                                                args.benchmark_mode, None, None, None, None, None, None, None,
                                                                int(pos_enabled), pos_pct, int(dd_enabled), dd_pct, run_id
                                                            ))
                                                            continue

                                                        rets = [float(r["daily_return"]) for r in bt_rows]
                                                        equities = [float(r["equity"]) for r in bt_rows]
                                                        total_return = _safe_div(equities[-1] - 100000.0, 100000.0)
                                                        mdd = _max_drawdown(equities)
                                                        sharpe = _sharpe(rets)
                                                        trade_count = _estimate_trade_count(conn, run_id)

                                                        bench = _calculate_benchmark_return(
                                                            conn, args.benchmark_mode, window_dates,
                                                            scoped_symbols or None, args.universe_size, args.universe_lookback_days,
                                                        )
                                                        excess = (total_return - bench) if bench is not None else None
                                                        win_vs_bench = int(total_return > bench) if bench is not None else None

                                                        all_results.append(WindowResult(
                                                            batch_id, config_id, req_start, actual_start, req_end, actual_end, period_m,
                                                            len(window_dates), 0, None, top_n, hold_days, keep_offset,
                                                            keep_threshold, scoring_version, market_scope, args.benchmark_mode,
                                                            bench, excess, win_vs_bench, total_return, mdd, sharpe,
                                                            trade_count, int(pos_enabled), pos_pct, int(dd_enabled), dd_pct, run_id
                                                        ))

    conn.execute(
        """
        INSERT INTO start_window_robustness_batches(
            batch_id,created_at,db_path,min_start_date,max_start_date,start_date_frequency,min_warmup_days,
            benchmark_mode,commission_bps,tax_bps,slippage_bps,cost_model,stability_score_description,
            score_rows_used,universe_rows_used,score_generation_scope,universe_generation_scope,
            lookahead_checked,lookahead_violations
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            batch_id, created_at, str(Path(args.db)), args.min_start_date, args.max_start_date,
            args.start_date_frequency, int(args.min_warmup_days), args.benchmark_mode,
            float(args.commission_bps), float(args.tax_bps), float(args.slippage_bps), args.cost_model,
            "stability_score = Σ_h w_h * z_h, weights: 1M=0.10,3M=0.20,6M=0.30,12M=0.40; z_h uses evaluated windows only",
            int(score_rows_used), int(universe_rows_used), score_generation_scope, universe_generation_scope,
            int(lookahead.get("checked_rows", 0)), int(lookahead.get("violations", 0)),
        ),
    )

    rows_dict = [r.__dict__ for r in all_results]
    conn.executemany(
        """
        INSERT INTO start_window_robustness_results(
            batch_id,config_id,requested_start_date,actual_start_date,requested_end_date,actual_end_date,
            actual_trading_days,period_months,skipped,skip_reason,top_n,min_holding_days,keep_rank_offset,
            keep_rank_threshold,scoring_version,market_scope,benchmark_mode,benchmark_return,excess_return,
            win_vs_benchmark,total_return,max_drawdown,sharpe,trade_count,position_stop_loss_enabled,
            position_stop_loss_pct,portfolio_dd_cut_enabled,portfolio_dd_cut_pct,run_id
        ) VALUES(
            :batch_id,:config_id,:requested_start_date,:actual_start_date,:requested_end_date,:actual_end_date,
            :actual_trading_days,:period_months,:skipped,:skip_reason,:top_n,:min_holding_days,:keep_rank_offset,
            :keep_rank_threshold,:scoring_version,:market_scope,:benchmark_mode,:benchmark_return,:excess_return,
            :win_vs_benchmark,:total_return,:max_drawdown,:sharpe,:trade_count,:position_stop_loss_enabled,
            :position_stop_loss_pct,:portfolio_dd_cut_enabled,:portfolio_dd_cut_pct,:run_id
        )
        """,
        rows_dict,
    )

    grouped: dict[str, list[WindowResult]] = defaultdict(list)
    for r in all_results:
        key = "|".join([r.config_id, str(r.top_n), str(r.min_holding_days), str(r.keep_rank_offset), str(r.keep_rank_threshold), r.scoring_version, r.market_scope, str(r.position_stop_loss_enabled), str(r.position_stop_loss_pct), str(r.portfolio_dd_cut_enabled), str(r.portfolio_dd_cut_pct)])
        grouped[key].append(r)

    strategy_rows: list[dict] = []
    for _, group in grouped.items():
        template = group[0]
        evaluated = [g for g in group if g.skipped == 0]
        num = len(group)
        ev = len(evaluated)
        skipped = num - ev
        total_vals = [g.total_return for g in evaluated if g.total_return is not None]
        excess_vals = [g.excess_return for g in evaluated if g.excess_return is not None]
        sharpe_vals = [g.sharpe for g in evaluated if g.sharpe is not None]
        mdds = [g.max_drawdown for g in evaluated if g.max_drawdown is not None]
        wins = [1 if (g.total_return or 0.0) > 0 else 0 for g in evaluated]
        wins_b = [g.win_vs_benchmark for g in evaluated if g.win_vs_benchmark is not None]

        per_period_mean: dict[int, float] = {}
        for h in STABILITY_WEIGHTS:
            hv = [g.total_return for g in evaluated if g.period_months == h and g.total_return is not None]
            if hv:
                per_period_mean[h] = sum(hv) / len(hv)
        stability = 0.0
        weight_sum = 0.0
        for h, w in STABILITY_WEIGHTS.items():
            if h in per_period_mean:
                stability += w * per_period_mean[h]
                weight_sum += w
        stability_score = stability / weight_sum if weight_sum else None

        strategy_rows.append({
            "batch_id": batch_id,
            "config_id": template.config_id,
            "top_n": template.top_n,
            "min_holding_days": template.min_holding_days,
            "keep_rank_offset": template.keep_rank_offset,
            "keep_rank_threshold": template.keep_rank_threshold,
            "scoring_version": template.scoring_version,
            "market_scope": template.market_scope,
            "position_stop_loss_enabled": template.position_stop_loss_enabled,
            "position_stop_loss_pct": template.position_stop_loss_pct,
            "portfolio_dd_cut_enabled": template.portfolio_dd_cut_enabled,
            "portfolio_dd_cut_pct": template.portfolio_dd_cut_pct,
            "num_windows": num,
            "evaluated_windows": ev,
            "skipped_windows": skipped,
            "evaluated_ratio": _safe_div(ev, num),
            "mean_total_return": (sum(total_vals) / len(total_vals)) if total_vals else None,
            "mean_excess_return": (sum(excess_vals) / len(excess_vals)) if excess_vals else None,
            "mean_sharpe": (sum(sharpe_vals) / len(sharpe_vals)) if sharpe_vals else None,
            "worst_mdd": min(mdds) if mdds else None,
            "win_rate": (sum(wins) / len(wins)) if wins else None,
            "win_vs_benchmark_rate": (sum(wins_b) / len(wins_b)) if wins_b else None,
            "stability_score": stability_score,
            "benchmark_mode": args.benchmark_mode,
        })

    conn.executemany(
        """
        INSERT INTO start_window_robustness_strategy_summary(
            batch_id,config_id,top_n,min_holding_days,keep_rank_offset,keep_rank_threshold,scoring_version,market_scope,
            position_stop_loss_enabled,position_stop_loss_pct,portfolio_dd_cut_enabled,portfolio_dd_cut_pct,
            num_windows,evaluated_windows,skipped_windows,evaluated_ratio,mean_total_return,mean_excess_return,mean_sharpe,
            worst_mdd,win_rate,win_vs_benchmark_rate,stability_score,benchmark_mode
        ) VALUES(
            :batch_id,:config_id,:top_n,:min_holding_days,:keep_rank_offset,:keep_rank_threshold,:scoring_version,:market_scope,
            :position_stop_loss_enabled,:position_stop_loss_pct,:portfolio_dd_cut_enabled,:portfolio_dd_cut_pct,
            :num_windows,:evaluated_windows,:skipped_windows,:evaluated_ratio,:mean_total_return,:mean_excess_return,:mean_sharpe,
            :worst_mdd,:win_rate,:win_vs_benchmark_rate,:stability_score,:benchmark_mode
        )
        """,
        strategy_rows,
    )

    period_rows: list[dict] = []
    for p_m in sorted(set(r.period_months for r in all_results)):
        group = [r for r in all_results if r.period_months == p_m]
        evaluated = [g for g in group if g.skipped == 0]
        num = len(group)
        ev = len(evaluated)
        total_vals = [g.total_return for g in evaluated if g.total_return is not None]
        excess_vals = [g.excess_return for g in evaluated if g.excess_return is not None]
        sharpe_vals = [g.sharpe for g in evaluated if g.sharpe is not None]
        mdds = [g.max_drawdown for g in evaluated if g.max_drawdown is not None]
        wins = [1 if (g.total_return or 0.0) > 0 else 0 for g in evaluated]
        wins_b = [g.win_vs_benchmark for g in evaluated if g.win_vs_benchmark is not None]
        period_rows.append({
            "batch_id": batch_id,
            "period_months": p_m,
            "num_windows": num,
            "evaluated_windows": ev,
            "skipped_windows": num - ev,
            "evaluated_ratio": _safe_div(ev, num),
            "mean_total_return": (sum(total_vals) / len(total_vals)) if total_vals else None,
            "mean_excess_return": (sum(excess_vals) / len(excess_vals)) if excess_vals else None,
            "mean_sharpe": (sum(sharpe_vals) / len(sharpe_vals)) if sharpe_vals else None,
            "worst_mdd": min(mdds) if mdds else None,
            "win_rate": (sum(wins) / len(wins)) if wins else None,
            "win_vs_benchmark_rate": (sum(wins_b) / len(wins_b)) if wins_b else None,
            "benchmark_mode": args.benchmark_mode,
        })

    conn.executemany(
        """
        INSERT INTO start_window_robustness_period_summary(
            batch_id,period_months,num_windows,evaluated_windows,skipped_windows,evaluated_ratio,
            mean_total_return,mean_excess_return,mean_sharpe,worst_mdd,win_rate,win_vs_benchmark_rate,benchmark_mode
        ) VALUES(
            :batch_id,:period_months,:num_windows,:evaluated_windows,:skipped_windows,:evaluated_ratio,
            :mean_total_return,:mean_excess_return,:mean_sharpe,:worst_mdd,:win_rate,:win_vs_benchmark_rate,:benchmark_mode
        )
        """,
        period_rows,
    )
    conn.commit()

    output_dir = Path(args.output_dir)
    detail_csv = output_dir / f"start_window_robustness_detailed_{batch_id}.csv"
    strategy_csv = output_dir / f"start_window_robustness_strategy_summary_{batch_id}.csv"
    period_csv = output_dir / f"start_window_robustness_period_summary_{batch_id}.csv"
    md_path = output_dir / f"start_window_robustness_summary_{batch_id}.md"

    _write_csv(detail_csv, rows_dict, list(rows_dict[0].keys()) if rows_dict else [])
    _write_csv(strategy_csv, strategy_rows, list(strategy_rows[0].keys()) if strategy_rows else [])
    _write_csv(period_csv, period_rows, list(period_rows[0].keys()) if period_rows else [])

    duplicate_text = "\n".join([f"- {x}" for x in duplicate_logs]) if duplicate_logs else "- none"
    skipped_by_reason = Counter([r.skip_reason for r in all_results if r.skipped and r.skip_reason])
    top_stable = sorted([x for x in strategy_rows if x["stability_score"] is not None], key=lambda x: x["stability_score"], reverse=True)[:10]
    worst_windows = sorted([r for r in all_results if r.skipped == 0 and r.total_return is not None], key=lambda x: x.total_return)[:10]
    max_mdd_cfg = sorted([x for x in strategy_rows if x["worst_mdd"] is not None], key=lambda x: x["worst_mdd"])[:10]

    lines = [
        f"# Start-date Rolling-window Robustness Summary ({batch_id})",
        "",
        "## Batch metadata",
        f"- DB path: `{args.db}`",
        f"- date range: `{args.min_start_date}` ~ `{args.max_start_date}`",
        f"- benchmark mode: `{args.benchmark_mode}`",
        f"- cost model: `{args.cost_model}` (commission_bps={args.commission_bps}, tax_bps={args.tax_bps}, slippage_bps={args.slippage_bps})",
        f"- scoring versions: `{scoring_versions}`",
        f"- universe mode/size/lookback: `{args.universe_mode}/{args.universe_size}/{args.universe_lookback_days}`",
        f"- start date generation mode: explicit + `{args.start_date_frequency}`",
        f"- min_warmup_days: `{args.min_warmup_days}`",
        f"- window boundary convention: `window_start` and `window_end` are **inclusive** trading-date filters.",
        "",
        "## Duplicate mapped start dates",
        duplicate_text,
        "",
        "## Top stable configs",
        "|rank|config_id|stability|mean_return|mean_excess|worst_mdd|eval_ratio|",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for i, r in enumerate(top_stable, 1):
        lines.append(f"|{i}|{r['config_id']}|{r['stability_score']:.4f}|{(r['mean_total_return'] if r['mean_total_return'] is not None else float('nan')):.2%}|{(r['mean_excess_return'] if r['mean_excess_return'] is not None else float('nan')):.2%}|{(r['worst_mdd'] if r['worst_mdd'] is not None else float('nan')):.2%}|{r['evaluated_ratio']:.2%}|")

    lines += ["", "## Period summary for 1M/3M/6M/12M", "|period|eval/total|mean_return|mean_excess|mean_sharpe|worst_mdd|", "|---:|---:|---:|---:|---:|---:|"]
    for r in sorted(period_rows, key=lambda x: x["period_months"]):
        lines.append(f"|{r['period_months']}M|{r['evaluated_windows']}/{r['num_windows']}|{(r['mean_total_return'] if r['mean_total_return'] is not None else float('nan')):.2%}|{(r['mean_excess_return'] if r['mean_excess_return'] is not None else float('nan')):.2%}|{(r['mean_sharpe'] if r['mean_sharpe'] is not None else float('nan')):.2f}|{(r['worst_mdd'] if r['worst_mdd'] is not None else float('nan')):.2%}|")

    lines += [
        "",
        "## 1M weakness warning",
        "- 1M window는 노이즈가 크므로 탈락 기준이 아니라 **경고 신호**로만 해석합니다.",
        "",
        "## Worst windows",
        "|actual_start_date|actual_end_date|period_months|config|total_return|benchmark_return|excess_return|max_drawdown|",
        "|---|---|---:|---|---:|---:|---:|---:|",
    ]
    for r in worst_windows:
        lines.append(f"|{r.actual_start_date}|{r.actual_end_date}|{r.period_months}|{r.config_id}|{r.total_return:.2%}|{(r.benchmark_return if r.benchmark_return is not None else float('nan')):.2%}|{(r.excess_return if r.excess_return is not None else float('nan')):.2%}|{(r.max_drawdown if r.max_drawdown is not None else float('nan')):.2%}|")

    lines += ["", "## Max MDD configs", "|config|worst_mdd|", "|---|---:|"]
    for r in max_mdd_cfg:
        lines.append(f"|{r['config_id']}|{r['worst_mdd']:.2%}|")

    lines += ["", "## Skipped windows summary by skip_reason"]
    for reason, cnt in sorted(skipped_by_reason.items()):
        lines.append(f"- `{reason}`: {cnt}")

    lines += [
        "",
        "## Lookahead validation result",
        f"- checked={lookahead.get('checked_rows', 0)}, violations={lookahead.get('violations', 0)}",
    ]
    if lookahead.get("violations", 0) > 0:
        lines.append("- ⚠️ lookahead violation detected; 결과 해석 시 주의 필요")

    lines += [
        "",
        "## Warnings",
        "- monthly starts + 6M/12M horizon은 구간이 강하게 overlap될 수 있어 독립 표본으로 해석하면 안 됩니다.",
    ]
    if args.cost_model == "unspecified" or (args.commission_bps == 0 and args.tax_bps == 0 and args.slippage_bps == 0):
        lines.append("- ⚠️ 비용 모델이 0 또는 unspecified 입니다. 실전 해석 시 과대평가 가능성이 있습니다.")
    if args.benchmark_mode == "none":
        lines.append("- ⚠️ benchmark가 없어 excess_return / win_vs_benchmark는 NA 입니다.")
    if any((r.skip_reason == "backtest_error") for r in all_results):
        lines.append("- ⚠️ backtest_error가 발생한 window가 있습니다. 데이터 부족 skip과 분리해서 확인하세요.")

    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text("\n".join(lines), encoding="utf-8")

    detailed_db_count = conn.execute("SELECT COUNT(*) AS c FROM start_window_robustness_results WHERE batch_id=?", (batch_id,)).fetchone()["c"]
    strategy_db_count = conn.execute("SELECT COUNT(*) AS c FROM start_window_robustness_strategy_summary WHERE batch_id=?", (batch_id,)).fetchone()["c"]
    period_db_count = conn.execute("SELECT COUNT(*) AS c FROM start_window_robustness_period_summary WHERE batch_id=?", (batch_id,)).fetchone()["c"]

    print(f"[counts] batch_id={batch_id} detailed={detailed_db_count} strategy_summary={strategy_db_count} period_summary={period_db_count}")
    print(f"[csv-sqlite-check] detailed_csv={len(rows_dict)} detailed_db={detailed_db_count}")
    print(f"[csv-sqlite-check] strategy_csv={len(strategy_rows)} strategy_db={strategy_db_count}")
    print(f"[csv-sqlite-check] period_csv={len(period_rows)} period_db={period_db_count}")

    print(
        json.dumps(
            {
                "batch_id": batch_id,
                "detail_csv": str(detail_csv),
                "strategy_summary_csv": str(strategy_csv),
                "period_summary_csv": str(period_csv),
                "summary_md": str(md_path),
                "duplicates_removed": len(duplicate_logs),
                "skip_reasons_supported": sorted(SUPPORTED_SKIP_REASONS),
            },
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
