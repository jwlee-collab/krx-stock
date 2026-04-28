#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import csv
import json
import sqlite3
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass, asdict
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

STABILITY_WEIGHTS: dict[int, float] = {1: 0.10, 3: 0.20, 6: 0.30, 12: 0.40}
NONE_TOKENS = {"", "none", "off", "null", "na", "n/a"}


@dataclass
class EvaluationConfig:
    config_id: str
    top_n: int
    min_holding_days: int
    keep_rank_offset: int
    keep_rank_threshold: int
    scoring_version: str
    market_scope: str
    entry_gate_mode: str
    market_filter_mode: str
    position_stop_loss_enabled: int
    position_stop_loss_pct: float | None
    stop_loss_cash_mode: str
    stop_loss_cooldown_days: int
    portfolio_dd_cut_enabled: int
    portfolio_dd_cut_pct: float | None
    portfolio_dd_cooldown_days: int
    overheat_entry_gate_enabled: int
    overheat_ret_1d_rule_enabled: int
    overheat_ret_5d_rule_enabled: int
    overheat_range_rule_enabled: int
    overheat_volume_z20_rule_enabled: int
    volume_surge_overheat_rule_enabled: int


@dataclass
class WindowResult:
    batch_id: str
    config_id: str
    run_id: str | None
    requested_start_date: str
    actual_start_date: str | None
    requested_end_date: str
    actual_end_date: str | None
    period_months: int
    window_label: str
    top_n: int
    min_holding_days: int
    keep_rank_offset: int
    keep_rank_threshold: int
    scoring_version: str
    market_scope: str
    entry_gate_mode: str
    market_filter_mode: str
    benchmark_mode: str
    position_stop_loss_enabled: int
    position_stop_loss_pct: float | None
    stop_loss_cash_mode: str
    stop_loss_cooldown_days: int
    portfolio_dd_cut_enabled: int
    portfolio_dd_cut_pct: float | None
    portfolio_dd_cooldown_days: int
    overheat_entry_gate_enabled: int
    max_entry_ret_1d: float
    max_entry_ret_5d: float
    max_entry_range_pct: float
    max_entry_volume_z20: float
    volume_surge_rule_enabled: int
    volume_surge_threshold: float
    volume_surge_ret_5d_threshold: float
    overheat_rejected_count: int | None
    overheat_cash_days: int | None
    overheat_rejected_by_ret_1d: int | None
    overheat_rejected_by_ret_5d: int | None
    overheat_rejected_by_range_pct: int | None
    overheat_rejected_by_volume_z20: int | None
    overheat_rejected_by_volume_surge_rule: int | None
    total_return: float | None
    benchmark_return: float | None
    excess_return: float | None
    sharpe: float | None
    max_drawdown: float | None
    trade_count: int | None
    average_actual_position_count: float | None
    position_stop_loss_count: int | None
    portfolio_dd_cut_count: int | None
    win_vs_benchmark: int | None
    cost_metadata_json: str | None
    skipped: int
    skip_reason: str


@dataclass
class StrategySummary:
    batch_id: str
    config_id: str
    strategy_key: str
    top_n: int
    min_holding_days: int
    keep_rank_offset: int
    keep_rank_threshold: int
    scoring_version: str
    market_scope: str
    entry_gate_mode: str
    market_filter_mode: str
    benchmark_mode: str
    position_stop_loss_enabled: int
    position_stop_loss_pct: float | None
    stop_loss_cash_mode: str
    stop_loss_cooldown_days: int
    portfolio_dd_cut_enabled: int
    portfolio_dd_cut_pct: float | None
    portfolio_dd_cooldown_days: int
    overheat_entry_gate_enabled: int
    max_entry_ret_1d: float
    max_entry_ret_5d: float
    max_entry_range_pct: float
    max_entry_volume_z20: float
    volume_surge_rule_enabled: int
    volume_surge_threshold: float
    volume_surge_ret_5d_threshold: float
    overheat_rejected_count_mean: float | None
    overheat_cash_days_mean: float | None
    overheat_rejected_by_ret_1d_mean: float | None
    overheat_rejected_by_ret_5d_mean: float | None
    overheat_rejected_by_range_pct_mean: float | None
    overheat_rejected_by_volume_z20_mean: float | None
    overheat_rejected_by_volume_surge_rule_mean: float | None
    num_windows: int
    evaluated_windows: int
    skipped_windows: int
    evaluated_ratio: float
    mean_total_return: float | None
    median_total_return: float | None
    std_total_return: float | None
    mean_benchmark_return: float | None
    mean_excess_return: float | None
    win_rate_vs_benchmark: float | None
    mean_sharpe: float | None
    worst_mdd: float | None
    stability_score: float | None
    evaluated_1m_windows: int
    evaluated_3m_windows: int
    evaluated_6m_windows: int
    evaluated_12m_windows: int
    horizon_coverage_ratio: float
    missing_horizon_penalty_applied: int


def _parse_int_list(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _parse_str_list(value: str) -> list[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def _safe_div(x: float, y: float) -> float:
    return x / y if y else 0.0


def _add_months(d: date, months: int) -> date:
    month_idx = d.month - 1 + months
    year = d.year + month_idx // 12
    month = month_idx % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last_day))


def _stddev(values: list[float]) -> float | None:
    if len(values) < 2:
        return 0.0 if values else None
    avg = mean(values)
    return (sum((x - avg) ** 2 for x in values) / len(values)) ** 0.5


def _sharpe(daily_returns: list[float]) -> float:
    if not daily_returns:
        return 0.0
    avg = mean(daily_returns)
    var = sum((r - avg) ** 2 for r in daily_returns) / max(1, len(daily_returns) - 1)
    vol = var**0.5
    if vol == 0:
        return 0.0
    return (avg * 252) / (vol * (252**0.5))


def _max_drawdown(equities: list[float]) -> float:
    if not equities:
        return 0.0
    peak = equities[0]
    worst = 0.0
    for e in equities:
        peak = max(peak, e)
        dd = _safe_div(e - peak, peak)
        worst = min(worst, dd)
    return worst


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
        elif frequency == "quarterly" and cur.month in {1, 4, 7, 10}:
            result.append(cur)
        month = cur.month + 1
        year = cur.year + (1 if month > 12 else 0)
        month = 1 if month > 12 else month
        cur = date(year, month, 1)
    return [d for d in result if min_date <= d <= max_date]


def _parse_toggle_pcts(values: str | None, modes: str, label: str) -> list[tuple[int, float | None]]:
    if values and values.strip():
        out: list[tuple[int, float | None]] = []
        for token in _parse_str_list(values):
            t = token.lower()
            if t in NONE_TOKENS:
                out.append((0, None))
            else:
                try:
                    out.append((1, float(token)))
                except ValueError as e:
                    raise ValueError(f"invalid {label} value: {token}") from e
        return list(dict.fromkeys(out))

    out = []
    for m in _parse_str_list(modes):
        mode = m.lower()
        if mode in {"off", "none"}:
            out.append((0, None))
        elif mode == "on":
            out.append((1, 0.10))
        else:
            raise ValueError(f"invalid {label} mode: {m}")
    return list(dict.fromkeys(out))


def _get_dates(conn: sqlite3.Connection) -> list[str]:
    return [r[0] for r in conn.execute("SELECT DISTINCT date FROM daily_prices ORDER BY date").fetchall()]


def _compute_universe_benchmark_return(conn: sqlite3.Connection, eval_dates: list[str]) -> float:
    if len(eval_dates) < 2:
        return 0.0
    eq = 1.0
    for i in range(len(eval_dates) - 1):
        d0, d1 = eval_dates[i], eval_dates[i + 1]
        rows = conn.execute(
            """
            SELECT p0.close, p1.close
            FROM daily_scores s
            JOIN daily_prices p0 ON p0.symbol=s.symbol AND p0.date=s.date
            JOIN daily_prices p1 ON p1.symbol=s.symbol AND p1.date=?
            WHERE s.date=?
            """,
            (d1, d0),
        ).fetchall()
        rets = [(r[1] - r[0]) / r[0] for r in rows if r[0]]
        eq *= 1 + (mean(rets) if rets else 0.0)
    return eq - 1.0


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = set(_table_columns(conn, table))
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def _ensure_tables(conn: sqlite3.Connection, reset: bool = False) -> None:
    tables = [
        "start_window_robustness_results",
        "start_window_robustness_strategy_summary",
        "start_window_robustness_period_summary",
        "start_window_robustness_batches",
    ]
    if reset:
        conn.executescript("\n".join([f"DROP TABLE IF EXISTS {t};" for t in tables]))

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS start_window_robustness_batches (
            batch_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            db_path TEXT NOT NULL,
            start_dates_spec TEXT NOT NULL,
            period_months_spec TEXT NOT NULL,
            benchmark_mode TEXT NOT NULL,
            min_warmup_days INTEGER NOT NULL,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            config_id TEXT NOT NULL,
            run_id TEXT,
            requested_start_date TEXT NOT NULL,
            actual_start_date TEXT,
            requested_end_date TEXT NOT NULL,
            actual_end_date TEXT,
            period_months INTEGER NOT NULL,
            window_label TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            min_holding_days INTEGER NOT NULL,
            keep_rank_offset INTEGER NOT NULL,
            keep_rank_threshold INTEGER NOT NULL,
            scoring_version TEXT NOT NULL,
            market_scope TEXT NOT NULL,
            entry_gate_mode TEXT NOT NULL,
            market_filter_mode TEXT NOT NULL,
            benchmark_mode TEXT NOT NULL,
            position_stop_loss_enabled INTEGER NOT NULL,
            position_stop_loss_pct REAL,
            stop_loss_cash_mode TEXT NOT NULL DEFAULT 'rebalance_remaining',
            stop_loss_cooldown_days INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cut_enabled INTEGER NOT NULL,
            portfolio_dd_cut_pct REAL,
            portfolio_dd_cooldown_days INTEGER NOT NULL,
            overheat_entry_gate_enabled INTEGER NOT NULL DEFAULT 0,
            max_entry_ret_1d REAL NOT NULL DEFAULT 0.08,
            max_entry_ret_5d REAL NOT NULL DEFAULT 0.15,
            max_entry_range_pct REAL NOT NULL DEFAULT 0.10,
            max_entry_volume_z20 REAL NOT NULL DEFAULT 3.0,
            volume_surge_rule_enabled INTEGER NOT NULL DEFAULT 0,
            volume_surge_threshold REAL NOT NULL DEFAULT 3.0,
            volume_surge_ret_5d_threshold REAL NOT NULL DEFAULT 0.10,
            overheat_rejected_count INTEGER,
            overheat_cash_days INTEGER,
            overheat_rejected_by_ret_1d INTEGER,
            overheat_rejected_by_ret_5d INTEGER,
            overheat_rejected_by_range_pct INTEGER,
            overheat_rejected_by_volume_z20 INTEGER,
            overheat_rejected_by_volume_surge_rule INTEGER,
            total_return REAL,
            benchmark_return REAL,
            excess_return REAL,
            sharpe REAL,
            max_drawdown REAL,
            trade_count INTEGER,
            average_actual_position_count REAL,
            position_stop_loss_count INTEGER,
            portfolio_dd_cut_count INTEGER,
            win_vs_benchmark INTEGER,
            cost_metadata_json TEXT,
            skipped INTEGER NOT NULL,
            skip_reason TEXT NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES start_window_robustness_batches(batch_id)
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_strategy_summary (
            summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            config_id TEXT NOT NULL,
            strategy_key TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            min_holding_days INTEGER NOT NULL,
            keep_rank_offset INTEGER NOT NULL,
            keep_rank_threshold INTEGER NOT NULL,
            scoring_version TEXT NOT NULL,
            market_scope TEXT NOT NULL,
            entry_gate_mode TEXT NOT NULL,
            market_filter_mode TEXT NOT NULL,
            benchmark_mode TEXT NOT NULL,
            position_stop_loss_enabled INTEGER NOT NULL,
            position_stop_loss_pct REAL,
            stop_loss_cash_mode TEXT NOT NULL DEFAULT 'rebalance_remaining',
            stop_loss_cooldown_days INTEGER NOT NULL DEFAULT 0,
            portfolio_dd_cut_enabled INTEGER NOT NULL,
            portfolio_dd_cut_pct REAL,
            portfolio_dd_cooldown_days INTEGER NOT NULL,
            overheat_entry_gate_enabled INTEGER NOT NULL DEFAULT 0,
            max_entry_ret_1d REAL NOT NULL DEFAULT 0.08,
            max_entry_ret_5d REAL NOT NULL DEFAULT 0.15,
            max_entry_range_pct REAL NOT NULL DEFAULT 0.10,
            max_entry_volume_z20 REAL NOT NULL DEFAULT 3.0,
            volume_surge_rule_enabled INTEGER NOT NULL DEFAULT 0,
            volume_surge_threshold REAL NOT NULL DEFAULT 3.0,
            volume_surge_ret_5d_threshold REAL NOT NULL DEFAULT 0.10,
            overheat_rejected_count_mean REAL,
            overheat_cash_days_mean REAL,
            overheat_rejected_by_ret_1d_mean REAL,
            overheat_rejected_by_ret_5d_mean REAL,
            overheat_rejected_by_range_pct_mean REAL,
            overheat_rejected_by_volume_z20_mean REAL,
            overheat_rejected_by_volume_surge_rule_mean REAL,
            num_windows INTEGER NOT NULL,
            evaluated_windows INTEGER NOT NULL,
            skipped_windows INTEGER NOT NULL,
            evaluated_ratio REAL NOT NULL,
            mean_total_return REAL,
            median_total_return REAL,
            std_total_return REAL,
            mean_benchmark_return REAL,
            mean_excess_return REAL,
            win_rate_vs_benchmark REAL,
            mean_sharpe REAL,
            worst_mdd REAL,
            stability_score REAL,
            evaluated_1m_windows INTEGER NOT NULL,
            evaluated_3m_windows INTEGER NOT NULL,
            evaluated_6m_windows INTEGER NOT NULL,
            evaluated_12m_windows INTEGER NOT NULL,
            horizon_coverage_ratio REAL NOT NULL,
            missing_horizon_penalty_applied INTEGER NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES start_window_robustness_batches(batch_id)
        );

        CREATE TABLE IF NOT EXISTS start_window_robustness_period_summary (
            period_summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            period_months INTEGER NOT NULL,
            num_windows INTEGER NOT NULL,
            evaluated_windows INTEGER NOT NULL,
            skipped_windows INTEGER NOT NULL,
            mean_total_return REAL,
            mean_benchmark_return REAL,
            mean_excess_return REAL,
            mean_sharpe REAL,
            FOREIGN KEY (batch_id) REFERENCES start_window_robustness_batches(batch_id)
        );
        """
    )

    required_results_cols = {"config_id", "requested_start_date", "actual_start_date", "benchmark_mode", "cost_metadata_json"}
    cols = set(_table_columns(conn, "start_window_robustness_results"))
    missing = required_results_cols - cols
    if missing:
        raise RuntimeError(
            "Incompatible existing start_window_robustness_results schema. "
            f"Missing columns: {sorted(missing)}. Use --reset-start-window-tables for development DB reset."
        )
    _ensure_column(
        conn,
        "start_window_robustness_results",
        "stop_loss_cash_mode",
        "stop_loss_cash_mode TEXT NOT NULL DEFAULT 'rebalance_remaining'",
    )
    _ensure_column(
        conn,
        "start_window_robustness_results",
        "stop_loss_cooldown_days",
        "stop_loss_cooldown_days INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(conn, "start_window_robustness_results", "overheat_entry_gate_enabled", "overheat_entry_gate_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "start_window_robustness_results", "max_entry_ret_1d", "max_entry_ret_1d REAL NOT NULL DEFAULT 0.08")
    _ensure_column(conn, "start_window_robustness_results", "max_entry_ret_5d", "max_entry_ret_5d REAL NOT NULL DEFAULT 0.15")
    _ensure_column(conn, "start_window_robustness_results", "max_entry_range_pct", "max_entry_range_pct REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "start_window_robustness_results", "max_entry_volume_z20", "max_entry_volume_z20 REAL NOT NULL DEFAULT 3.0")
    _ensure_column(conn, "start_window_robustness_results", "volume_surge_rule_enabled", "volume_surge_rule_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "start_window_robustness_results", "volume_surge_threshold", "volume_surge_threshold REAL NOT NULL DEFAULT 3.0")
    _ensure_column(conn, "start_window_robustness_results", "volume_surge_ret_5d_threshold", "volume_surge_ret_5d_threshold REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "start_window_robustness_results", "overheat_rejected_count", "overheat_rejected_count INTEGER")
    _ensure_column(conn, "start_window_robustness_results", "overheat_cash_days", "overheat_cash_days INTEGER")
    _ensure_column(conn, "start_window_robustness_results", "overheat_rejected_by_ret_1d", "overheat_rejected_by_ret_1d INTEGER")
    _ensure_column(conn, "start_window_robustness_results", "overheat_rejected_by_ret_5d", "overheat_rejected_by_ret_5d INTEGER")
    _ensure_column(conn, "start_window_robustness_results", "overheat_rejected_by_range_pct", "overheat_rejected_by_range_pct INTEGER")
    _ensure_column(conn, "start_window_robustness_results", "overheat_rejected_by_volume_z20", "overheat_rejected_by_volume_z20 INTEGER")
    _ensure_column(conn, "start_window_robustness_results", "overheat_rejected_by_volume_surge_rule", "overheat_rejected_by_volume_surge_rule INTEGER")
    _ensure_column(
        conn,
        "start_window_robustness_strategy_summary",
        "stop_loss_cash_mode",
        "stop_loss_cash_mode TEXT NOT NULL DEFAULT 'rebalance_remaining'",
    )
    _ensure_column(
        conn,
        "start_window_robustness_strategy_summary",
        "stop_loss_cooldown_days",
        "stop_loss_cooldown_days INTEGER NOT NULL DEFAULT 0",
    )
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_entry_gate_enabled", "overheat_entry_gate_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "max_entry_ret_1d", "max_entry_ret_1d REAL NOT NULL DEFAULT 0.08")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "max_entry_ret_5d", "max_entry_ret_5d REAL NOT NULL DEFAULT 0.15")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "max_entry_range_pct", "max_entry_range_pct REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "max_entry_volume_z20", "max_entry_volume_z20 REAL NOT NULL DEFAULT 3.0")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "volume_surge_rule_enabled", "volume_surge_rule_enabled INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "volume_surge_threshold", "volume_surge_threshold REAL NOT NULL DEFAULT 3.0")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "volume_surge_ret_5d_threshold", "volume_surge_ret_5d_threshold REAL NOT NULL DEFAULT 0.10")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_rejected_count_mean", "overheat_rejected_count_mean REAL")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_cash_days_mean", "overheat_cash_days_mean REAL")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_rejected_by_ret_1d_mean", "overheat_rejected_by_ret_1d_mean REAL")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_rejected_by_ret_5d_mean", "overheat_rejected_by_ret_5d_mean REAL")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_rejected_by_range_pct_mean", "overheat_rejected_by_range_pct_mean REAL")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_rejected_by_volume_z20_mean", "overheat_rejected_by_volume_z20_mean REAL")
    _ensure_column(conn, "start_window_robustness_strategy_summary", "overheat_rejected_by_volume_surge_rule_mean", "overheat_rejected_by_volume_surge_rule_mean REAL")
    conn.commit()


def main() -> None:
    p = argparse.ArgumentParser(description="Run start-date rolling-window robustness evaluations (config-first)")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--output-dir", default="data/reports")
    p.add_argument("--start-dates", default="")
    p.add_argument("--start-date-frequency", choices=["monthly", "quarterly"], default=None)
    p.add_argument("--min-start-date", default=None)
    p.add_argument("--max-start-date", default=None)
    p.add_argument("--period-months", default="1,3,6,12")
    p.add_argument("--top-n-values", default="3,5")
    p.add_argument("--min-holding-days-values", default="3,5")
    p.add_argument("--keep-rank-offsets", default="2,4")
    p.add_argument("--keep-rank-threshold-values", default="")
    p.add_argument("--scoring-versions", default="old,hybrid_v4")
    p.add_argument("--rebalance-frequency", choices=["daily", "weekly"], default="daily")
    p.add_argument("--symbols", default="")
    p.add_argument("--universe-file")
    p.add_argument("--initial-equity", type=float, default=100000.0)
    p.add_argument("--universe-mode", choices=["static", "rolling_liquidity"], default="static")
    p.add_argument("--universe-size", type=int, default=100)
    p.add_argument("--universe-lookback-days", type=int, default=20)
    p.add_argument("--market-scopes", default="ALL")
    p.add_argument("--market-filter-modes", default="off")
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
    p.add_argument("--position-stop-loss-pcts", default="")
    p.add_argument("--stop-loss-cash-modes", default="rebalance_remaining")
    p.add_argument("--stop-loss-cooldown-days-values", default="0")
    p.add_argument("--portfolio-dd-cut-modes", default="off,on")
    p.add_argument("--portfolio-dd-cut-pcts", default="")
    p.add_argument("--portfolio-dd-cooldown-days-values", default="20")
    p.add_argument("--overheat-entry-gate-modes", default="off,on")
    p.add_argument("--overheat-ret-1d-rule-modes", default="off,on")
    p.add_argument("--overheat-ret-5d-rule-modes", default="off,on")
    p.add_argument("--overheat-range-rule-modes", default="off,on")
    p.add_argument("--overheat-volume-z20-rule-modes", default="off,on")
    p.add_argument("--volume-surge-overheat-rule-modes", default="off,on")
    p.add_argument("--max-entry-ret-1d", type=float, default=0.08)
    p.add_argument("--max-entry-ret-5d", type=float, default=0.15)
    p.add_argument("--max-entry-range-pct", type=float, default=0.10)
    p.add_argument("--max-entry-volume-z20", type=float, default=3.0)
    p.add_argument("--enable-volume-surge-overheat-rule", action="store_true")
    p.add_argument("--volume-surge-threshold", type=float, default=3.0)
    p.add_argument("--volume-surge-ret-5d-threshold", type=float, default=0.10)
    p.add_argument("--benchmark-mode", choices=["none", "universe"], default="universe")
    p.add_argument("--min-warmup-days", type=int, default=20)
    p.add_argument("--reset-start-window-tables", action="store_true")
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)
    _ensure_tables(conn, reset=args.reset_start_window_tables)

    all_dates = _get_dates(conn)
    if len(all_dates) < 2:
        raise ValueError("start-window robustness requires at least 2 daily_prices dates")
    min_db_date, max_db_date = all_dates[0], all_dates[-1]

    min_start = date.fromisoformat(args.min_start_date) if args.min_start_date else date.fromisoformat(min_db_date)
    max_start = date.fromisoformat(args.max_start_date) if args.max_start_date else date.fromisoformat(max_db_date)

    schedule_starts: list[str] = []
    if args.start_date_frequency:
        for d in _generate_schedule_dates(min_start, max_start, args.start_date_frequency):
            if min_start <= d <= max_start:
                schedule_starts.append(d.isoformat())
    for d in _parse_str_list(args.start_dates):
        if min_start.isoformat() <= d <= max_start.isoformat():
            schedule_starts.append(d)
    start_dates = sorted(set(schedule_starts))
    if not start_dates:
        raise ValueError("No evaluation start dates resolved")

    periods = _parse_int_list(args.period_months)
    top_ns = _parse_int_list(args.top_n_values)
    min_holds = _parse_int_list(args.min_holding_days_values)
    keep_offsets = _parse_int_list(args.keep_rank_offsets)
    keep_threshold_values = _parse_int_list(args.keep_rank_threshold_values) if args.keep_rank_threshold_values.strip() else []
    scoring_versions = [normalize_scoring_profile(v) for v in _parse_str_list(args.scoring_versions)]
    market_scopes = list(dict.fromkeys([x.upper() for x in _parse_str_list(args.market_scopes)]))
    entry_modes = [x.lower() for x in _parse_str_list(args.entry_gate_modes)]
    market_filter_modes = [x.lower() for x in _parse_str_list(args.market_filter_modes)]
    pos_sl_grid = _parse_toggle_pcts(args.position_stop_loss_pcts, args.position_stop_loss_modes, "position-stop-loss")
    stop_loss_cash_modes = _parse_str_list(args.stop_loss_cash_modes)
    stop_loss_cooldown_days_values = _parse_int_list(args.stop_loss_cooldown_days_values)
    for mode in stop_loss_cash_modes:
        if mode not in {"rebalance_remaining", "keep_cash"}:
            raise ValueError(f"invalid stop-loss cash mode: {mode}")
    dd_cut_grid = _parse_toggle_pcts(args.portfolio_dd_cut_pcts, args.portfolio_dd_cut_modes, "portfolio-dd-cut")
    dd_cooldowns = _parse_int_list(args.portfolio_dd_cooldown_days_values)
    overheat_modes = [x.lower() for x in _parse_str_list(args.overheat_entry_gate_modes)]
    invalid_overheat_modes = [m for m in overheat_modes if m not in {"off", "on"}]
    if invalid_overheat_modes:
        raise ValueError(f"invalid overheat-entry-gate mode(s): {invalid_overheat_modes}")
    overheat_ret_1d_modes = [x.lower() for x in _parse_str_list(args.overheat_ret_1d_rule_modes)]
    overheat_ret_5d_modes = [x.lower() for x in _parse_str_list(args.overheat_ret_5d_rule_modes)]
    overheat_range_modes = [x.lower() for x in _parse_str_list(args.overheat_range_rule_modes)]
    overheat_volume_z20_modes = [x.lower() for x in _parse_str_list(args.overheat_volume_z20_rule_modes)]
    volume_surge_overheat_modes = [x.lower() for x in _parse_str_list(args.volume_surge_overheat_rule_modes)]
    for label, modes in [
        ("overheat-ret-1d-rule", overheat_ret_1d_modes),
        ("overheat-ret-5d-rule", overheat_ret_5d_modes),
        ("overheat-range-rule", overheat_range_modes),
        ("overheat-volume-z20-rule", overheat_volume_z20_modes),
        ("volume-surge-overheat-rule", volume_surge_overheat_modes),
    ]:
        invalid = [m for m in modes if m not in {"off", "on"}]
        if invalid:
            raise ValueError(f"invalid {label} mode(s): {invalid}")

    selected_symbols = parse_symbols_arg(args.symbols)
    if args.universe_file and not selected_symbols:
        selected_symbols = load_symbols_from_universe_csv(args.universe_file)

    lookahead_info = {"checked_rows": 0, "violations": 0}
    if args.universe_mode == "rolling_liquidity":
        build_rolling_liquidity_universe(conn, universe_size=args.universe_size, lookback_days=args.universe_lookback_days)
        lookahead_info = validate_rolling_universe_no_lookahead(conn, universe_size=args.universe_size, lookback_days=args.universe_lookback_days)
        if lookahead_info.get("violations", 0) > 0:
            raise RuntimeError(f"lookahead validation failed: {lookahead_info}")

    configs: list[EvaluationConfig] = []
    for top_n in top_ns:
        threshold_grid = keep_threshold_values if keep_threshold_values else [top_n + off for off in keep_offsets]
        for keep_threshold in threshold_grid:
            keep_offset = keep_threshold - top_n
            for min_holding_days in min_holds:
                for scoring_version in scoring_versions:
                    for market_scope in market_scopes:
                        for entry_mode in entry_modes:
                            for market_filter_mode in market_filter_modes:
                                for sl_enabled, sl_pct in pos_sl_grid:
                                    for stop_loss_cash_mode in stop_loss_cash_modes:
                                        for stop_loss_cooldown_days in stop_loss_cooldown_days_values:
                                            for dd_enabled, dd_pct in dd_cut_grid:
                                                for cooldown in dd_cooldowns:
                                                    for overheat_mode in overheat_modes:
                                                        for ret_1d_mode in overheat_ret_1d_modes:
                                                            for ret_5d_mode in overheat_ret_5d_modes:
                                                                for range_mode in overheat_range_modes:
                                                                    for volz_mode in overheat_volume_z20_modes:
                                                                        for surge_mode in volume_surge_overheat_modes:
                                                                            cfg = EvaluationConfig(
                                                                                config_id=(
                                                                                    f"top={top_n}|hold={min_holding_days}|keep={keep_threshold}|score={scoring_version}|"
                                                                                    f"scope={market_scope}|entry={entry_mode}|mf={market_filter_mode}|"
                                                                                    f"pos_sl={sl_enabled}:{sl_pct}|sl_cash={stop_loss_cash_mode}|"
                                                                                    f"sl_cd={stop_loss_cooldown_days}|dd_cut={dd_enabled}:{dd_pct}|dd_cd={cooldown}|"
                                                                                    f"overheat={overheat_mode}|ret1d={ret_1d_mode}|ret5d={ret_5d_mode}|"
                                                                                    f"range={range_mode}|volz={volz_mode}|surge={surge_mode}"
                                                                                ),
                                                                                top_n=top_n,
                                                                                min_holding_days=min_holding_days,
                                                                                keep_rank_offset=keep_offset,
                                                                                keep_rank_threshold=keep_threshold,
                                                                                scoring_version=scoring_version,
                                                                                market_scope=market_scope,
                                                                                entry_gate_mode=entry_mode,
                                                                                market_filter_mode=market_filter_mode,
                                                                                position_stop_loss_enabled=sl_enabled,
                                                                                position_stop_loss_pct=sl_pct,
                                                                                stop_loss_cash_mode=stop_loss_cash_mode,
                                                                                stop_loss_cooldown_days=stop_loss_cooldown_days,
                                                                                portfolio_dd_cut_enabled=dd_enabled,
                                                                                portfolio_dd_cut_pct=dd_pct,
                                                                                portfolio_dd_cooldown_days=cooldown,
                                                                                overheat_entry_gate_enabled=1 if overheat_mode == "on" else 0,
                                                                                overheat_ret_1d_rule_enabled=1 if ret_1d_mode == "on" else 0,
                                                                                overheat_ret_5d_rule_enabled=1 if ret_5d_mode == "on" else 0,
                                                                                overheat_range_rule_enabled=1 if range_mode == "on" else 0,
                                                                                overheat_volume_z20_rule_enabled=1 if volz_mode == "on" else 0,
                                                                                volume_surge_overheat_rule_enabled=1 if surge_mode == "on" else 0,
                                                                            )
                                                                            configs.append(cfg)

    generated_profiles: set[tuple[str, str]] = set()
    batch_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO start_window_robustness_batches(batch_id, created_at, db_path, start_dates_spec, period_months_spec, benchmark_mode, min_warmup_days, notes)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            batch_id,
            datetime.now(timezone.utc).isoformat(),
            str(Path(args.db)),
            json.dumps(start_dates, ensure_ascii=False),
            args.period_months,
            args.benchmark_mode,
            args.min_warmup_days,
            "config-first evaluation",
        ),
    )

    results: list[WindowResult] = []
    for cfg in configs:
        if cfg.keep_rank_threshold < cfg.top_n:
            for s in start_dates:
                for p_month in periods:
                    req_end = _add_months(date.fromisoformat(s), p_month).isoformat()
                    results.append(
                        WindowResult(
                            batch_id=batch_id, config_id=cfg.config_id, run_id=None,
                            requested_start_date=s, actual_start_date=s, requested_end_date=req_end, actual_end_date=None,
                            period_months=p_month, window_label=f"{s}_fwd_{p_month}m", top_n=cfg.top_n,
                            min_holding_days=cfg.min_holding_days, keep_rank_offset=cfg.keep_rank_offset,
                            keep_rank_threshold=cfg.keep_rank_threshold, scoring_version=cfg.scoring_version,
                            market_scope=cfg.market_scope, entry_gate_mode=cfg.entry_gate_mode,
                            market_filter_mode=cfg.market_filter_mode, benchmark_mode=args.benchmark_mode,
                            position_stop_loss_enabled=cfg.position_stop_loss_enabled,
                            position_stop_loss_pct=cfg.position_stop_loss_pct,
                            stop_loss_cash_mode=cfg.stop_loss_cash_mode,
                            stop_loss_cooldown_days=cfg.stop_loss_cooldown_days,
                            portfolio_dd_cut_enabled=cfg.portfolio_dd_cut_enabled,
                            portfolio_dd_cut_pct=cfg.portfolio_dd_cut_pct,
                            portfolio_dd_cooldown_days=cfg.portfolio_dd_cooldown_days,
                            overheat_entry_gate_enabled=cfg.overheat_entry_gate_enabled,
                            max_entry_ret_1d=args.max_entry_ret_1d,
                            max_entry_ret_5d=args.max_entry_ret_5d,
                            max_entry_range_pct=args.max_entry_range_pct,
                            max_entry_volume_z20=args.max_entry_volume_z20,
                            volume_surge_rule_enabled=cfg.volume_surge_overheat_rule_enabled,
                            volume_surge_threshold=args.volume_surge_threshold,
                            volume_surge_ret_5d_threshold=args.volume_surge_ret_5d_threshold,
                            overheat_rejected_count=None,
                            overheat_cash_days=None,
                            overheat_rejected_by_ret_1d=None,
                            overheat_rejected_by_ret_5d=None,
                            overheat_rejected_by_range_pct=None,
                            overheat_rejected_by_volume_z20=None,
                            overheat_rejected_by_volume_surge_rule=None,
                            total_return=None, benchmark_return=None, excess_return=None, sharpe=None, max_drawdown=None,
                            trade_count=None, average_actual_position_count=None, position_stop_loss_count=None,
                            portfolio_dd_cut_count=None, win_vs_benchmark=None, cost_metadata_json=None,
                            skipped=1, skip_reason="invalid_config",
                        )
                    )
            continue

        scope_symbols = selected_symbols
        if args.universe_file:
            scope_symbols, _ = load_symbols_with_market_scope(args.universe_file, cfg.market_scope)

        profile_key = (cfg.scoring_version, cfg.market_scope)
        if profile_key not in generated_profiles:
            generate_daily_scores(
                conn,
                include_history=True,
                allowed_symbols=scope_symbols or None,
                scoring_profile=cfg.scoring_version,
                universe_mode=args.universe_mode,
                universe_size=args.universe_size,
                universe_lookback_days=args.universe_lookback_days,
            )
            generated_profiles.add(profile_key)

        for s in start_dates:
            actual_start = _nearest_trading_on_or_after(all_dates, s)
            warmup_days = len([d for d in all_dates if actual_start and d < actual_start])
            for p_month in periods:
                req_end = _add_months(date.fromisoformat(s), p_month).isoformat()
                actual_end = _nearest_trading_on_or_before(all_dates, req_end)
                window_kwargs = dict(
                    batch_id=batch_id, config_id=cfg.config_id,
                    requested_start_date=s, actual_start_date=actual_start,
                    requested_end_date=req_end, actual_end_date=actual_end,
                    period_months=p_month, window_label=f"{s}_fwd_{p_month}m",
                    top_n=cfg.top_n, min_holding_days=cfg.min_holding_days, keep_rank_offset=cfg.keep_rank_offset,
                    keep_rank_threshold=cfg.keep_rank_threshold, scoring_version=cfg.scoring_version,
                    market_scope=cfg.market_scope, entry_gate_mode=cfg.entry_gate_mode,
                    market_filter_mode=cfg.market_filter_mode, benchmark_mode=args.benchmark_mode,
                    position_stop_loss_enabled=cfg.position_stop_loss_enabled,
                    position_stop_loss_pct=cfg.position_stop_loss_pct,
                    stop_loss_cash_mode=cfg.stop_loss_cash_mode,
                    stop_loss_cooldown_days=cfg.stop_loss_cooldown_days,
                    portfolio_dd_cut_enabled=cfg.portfolio_dd_cut_enabled,
                    portfolio_dd_cut_pct=cfg.portfolio_dd_cut_pct,
                    portfolio_dd_cooldown_days=cfg.portfolio_dd_cooldown_days,
                    overheat_entry_gate_enabled=cfg.overheat_entry_gate_enabled,
                    max_entry_ret_1d=args.max_entry_ret_1d,
                    max_entry_ret_5d=args.max_entry_ret_5d,
                    max_entry_range_pct=args.max_entry_range_pct,
                    max_entry_volume_z20=args.max_entry_volume_z20,
                    volume_surge_rule_enabled=cfg.volume_surge_overheat_rule_enabled,
                    volume_surge_threshold=args.volume_surge_threshold,
                    volume_surge_ret_5d_threshold=args.volume_surge_ret_5d_threshold,
                )
                result_metric_kwargs = dict(
                    overheat_rejected_count=None,
                    overheat_cash_days=None,
                    overheat_rejected_by_ret_1d=None,
                    overheat_rejected_by_ret_5d=None,
                    overheat_rejected_by_range_pct=None,
                    overheat_rejected_by_volume_z20=None,
                    overheat_rejected_by_volume_surge_rule=None,
                    total_return=None,
                    benchmark_return=None,
                    excess_return=None,
                    sharpe=None,
                    max_drawdown=None,
                    trade_count=None,
                    average_actual_position_count=None,
                    position_stop_loss_count=None,
                    portfolio_dd_cut_count=None,
                    win_vs_benchmark=None,
                    cost_metadata_json=None,
                )
                if actual_start is None:
                    results.append(
                        WindowResult(
                            **window_kwargs,
                            run_id=None,
                            **result_metric_kwargs,
                            skipped=1,
                            skip_reason="insufficient_future_data",
                        )
                    )
                    continue
                if warmup_days < args.min_warmup_days:
                    results.append(
                        WindowResult(
                            **window_kwargs,
                            run_id=None,
                            **result_metric_kwargs,
                            skipped=1,
                            skip_reason="insufficient_warmup_data",
                        )
                    )
                    continue
                if req_end > max_db_date or actual_end is None or actual_end <= actual_start:
                    results.append(
                        WindowResult(
                            **window_kwargs,
                            run_id=None,
                            **result_metric_kwargs,
                            skipped=1,
                            skip_reason="insufficient_future_data",
                        )
                    )
                    continue

                score_cnt = conn.execute("SELECT COUNT(1) FROM daily_scores WHERE date=?", (actual_start,)).fetchone()[0]
                if score_cnt == 0:
                    results.append(
                        WindowResult(
                            **window_kwargs,
                            run_id=None,
                            **result_metric_kwargs,
                            skipped=1,
                            skip_reason="no_scores",
                        )
                    )
                    continue
                if scope_symbols:
                    placeholders = ",".join(["?"] * len(scope_symbols))
                    tradable = conn.execute(
                        f"SELECT COUNT(1) FROM daily_scores WHERE date=? AND symbol IN ({placeholders})",
                        (actual_start, *scope_symbols),
                    ).fetchone()[0]
                    if tradable == 0:
                        results.append(
                            WindowResult(
                                **window_kwargs,
                                run_id=None,
                                **result_metric_kwargs,
                                skipped=1,
                                skip_reason="no_tradable_universe",
                            )
                        )
                        continue

                try:
                    run_id = run_backtest(
                        conn,
                        top_n=cfg.top_n,
                        start_date=actual_start,
                        end_date=actual_end,
                        initial_equity=args.initial_equity,
                        rebalance_frequency=args.rebalance_frequency,
                        min_holding_days=cfg.min_holding_days,
                        keep_rank_threshold=cfg.keep_rank_threshold,
                        scoring_profile=cfg.scoring_version,
                        market_filter_enabled=(cfg.market_filter_mode == "on"),
                        market_filter_ma20_reduce_by=args.market_filter_ma20_reduce_by,
                        market_filter_ma60_mode=args.market_filter_ma60_mode,
                        entry_gate_enabled=(cfg.entry_gate_mode == "on"),
                        min_entry_score=float(_parse_str_list(args.min_entry_score_values)[0]),
                        require_positive_momentum20=args.require_positive_momentum20,
                        require_positive_momentum60=args.require_positive_momentum60,
                        require_above_sma20=args.require_above_sma20,
                        require_above_sma60=args.require_above_sma60,
                        enable_position_stop_loss=bool(cfg.position_stop_loss_enabled),
                        position_stop_loss_pct=(cfg.position_stop_loss_pct or 0.1),
                        stop_loss_cash_mode=cfg.stop_loss_cash_mode,
                        stop_loss_cooldown_days=cfg.stop_loss_cooldown_days,
                        enable_portfolio_dd_cut=bool(cfg.portfolio_dd_cut_enabled),
                        portfolio_dd_cut_pct=(cfg.portfolio_dd_cut_pct or 0.1),
                        portfolio_dd_cooldown_days=cfg.portfolio_dd_cooldown_days,
                        enable_overheat_entry_gate=bool(cfg.overheat_entry_gate_enabled),
                        enable_overheat_ret_1d_rule=bool(cfg.overheat_ret_1d_rule_enabled),
                        enable_overheat_ret_5d_rule=bool(cfg.overheat_ret_5d_rule_enabled),
                        enable_overheat_range_rule=bool(cfg.overheat_range_rule_enabled),
                        enable_overheat_volume_z20_rule=bool(cfg.overheat_volume_z20_rule_enabled),
                        max_entry_ret_1d=args.max_entry_ret_1d,
                        max_entry_ret_5d=args.max_entry_ret_5d,
                        max_entry_range_pct=args.max_entry_range_pct,
                        max_entry_volume_z20=args.max_entry_volume_z20,
                        enable_volume_surge_overheat_rule=bool(cfg.volume_surge_overheat_rule_enabled),
                        volume_surge_threshold=args.volume_surge_threshold,
                        volume_surge_ret_5d_threshold=args.volume_surge_ret_5d_threshold,
                    )
                except Exception:
                    results.append(
                        WindowResult(
                            **window_kwargs,
                            run_id=None,
                            **result_metric_kwargs,
                            skipped=1,
                            skip_reason="backtest_error",
                        )
                    )
                    continue

                bt_rows = conn.execute("SELECT equity,daily_return FROM backtest_results WHERE run_id=? ORDER BY date", (run_id,)).fetchall()
                if not bt_rows:
                    results.append(
                        WindowResult(
                            **window_kwargs,
                            run_id=run_id,
                            **result_metric_kwargs,
                            skipped=1,
                            skip_reason="no_backtest_rows",
                        )
                    )
                    continue

                equities = [float(r[0]) for r in bt_rows]
                d_returns = [float(r[1]) for r in bt_rows]
                total_return = _safe_div(equities[-1] - args.initial_equity, args.initial_equity)
                eval_dates = [d for d in all_dates if actual_start <= d <= actual_end]
                benchmark_return = None
                if args.benchmark_mode == "universe":
                    benchmark_return = _compute_universe_benchmark_return(conn, eval_dates)
                excess = None if benchmark_return is None else total_return - benchmark_return
                bt_meta = conn.execute(
                    """
                    SELECT average_actual_position_count, position_stop_loss_count, portfolio_dd_cut_count,
                           market_filter_enabled, entry_gate_enabled,
                           overheat_rejected_count, overheat_cash_days,
                           overheat_rejected_by_ret_1d, overheat_rejected_by_ret_5d,
                           overheat_rejected_by_range_pct, overheat_rejected_by_volume_z20,
                           overheat_rejected_by_volume_surge_rule
                    FROM backtest_runs WHERE run_id=?
                    """,
                    (run_id,),
                ).fetchone()
                cost_meta = {
                    "cost_model": "none",
                    "commission": None,
                    "slippage": None,
                }
                results.append(
                    WindowResult(
                        **window_kwargs,
                        run_id=run_id,
                        total_return=total_return,
                        benchmark_return=benchmark_return,
                        excess_return=excess,
                        sharpe=_sharpe(d_returns),
                        max_drawdown=_max_drawdown(equities),
                        trade_count=len(bt_rows),
                        average_actual_position_count=float(bt_meta[0]) if bt_meta else None,
                        position_stop_loss_count=int(bt_meta[1]) if bt_meta else None,
                        portfolio_dd_cut_count=int(bt_meta[2]) if bt_meta else None,
                        overheat_rejected_count=int(bt_meta[5]) if bt_meta else None,
                        overheat_cash_days=int(bt_meta[6]) if bt_meta else None,
                        overheat_rejected_by_ret_1d=int(bt_meta[7]) if bt_meta else None,
                        overheat_rejected_by_ret_5d=int(bt_meta[8]) if bt_meta else None,
                        overheat_rejected_by_range_pct=int(bt_meta[9]) if bt_meta else None,
                        overheat_rejected_by_volume_z20=int(bt_meta[10]) if bt_meta else None,
                        overheat_rejected_by_volume_surge_rule=int(bt_meta[11]) if bt_meta else None,
                        win_vs_benchmark=(1 if (excess is not None and excess > 0) else (0 if excess is not None else None)),
                        cost_metadata_json=json.dumps(cost_meta, ensure_ascii=False),
                        skipped=0,
                        skip_reason="",
                    )
                )

    result_fields = list(asdict(results[0]).keys())
    conn.executemany(
        f"INSERT INTO start_window_robustness_results({','.join(result_fields)}) VALUES ({','.join(['?']*len(result_fields))})",
        [tuple(asdict(r).values()) for r in results],
    )

    grouped: dict[str, list[WindowResult]] = defaultdict(list)
    for r in results:
        grouped[r.config_id].append(r)

    summaries: list[StrategySummary] = []
    for config_id, rows in grouped.items():
        cfg = rows[0]
        valid = [r for r in rows if r.skipped == 0]
        rets = [r.total_return for r in valid if r.total_return is not None]
        bench = [r.benchmark_return for r in valid if r.benchmark_return is not None]
        excess = [r.excess_return for r in valid if r.excess_return is not None]
        sharpes = [r.sharpe for r in valid if r.sharpe is not None]
        mdds = [r.max_drawdown for r in valid if r.max_drawdown is not None]
        overheat_rejected = [r.overheat_rejected_count for r in valid if r.overheat_rejected_count is not None]
        overheat_cash = [r.overheat_cash_days for r in valid if r.overheat_cash_days is not None]
        overheat_ret_1d = [r.overheat_rejected_by_ret_1d for r in valid if r.overheat_rejected_by_ret_1d is not None]
        overheat_ret_5d = [r.overheat_rejected_by_ret_5d for r in valid if r.overheat_rejected_by_ret_5d is not None]
        overheat_range = [r.overheat_rejected_by_range_pct for r in valid if r.overheat_rejected_by_range_pct is not None]
        overheat_volz = [r.overheat_rejected_by_volume_z20 for r in valid if r.overheat_rejected_by_volume_z20 is not None]
        overheat_surge = [r.overheat_rejected_by_volume_surge_rule for r in valid if r.overheat_rejected_by_volume_surge_rule is not None]

        eval_counts = {h: len([r for r in valid if r.period_months == h]) for h in STABILITY_WEIGHTS}
        coverage = sum(1 for h in STABILITY_WEIGHTS if eval_counts[h] > 0) / len(STABILITY_WEIGHTS)

        stability = None
        if valid:
            stability = 0.0
            for h, w in STABILITY_WEIGHTS.items():
                vals = [r.total_return for r in valid if r.period_months == h and r.total_return is not None]
                stability += w * (mean(vals) if vals else 0.0)

        summaries.append(
            StrategySummary(
                batch_id=batch_id,
                config_id=config_id,
                strategy_key=config_id,
                top_n=cfg.top_n,
                min_holding_days=cfg.min_holding_days,
                keep_rank_offset=cfg.keep_rank_offset,
                keep_rank_threshold=cfg.keep_rank_threshold,
                scoring_version=cfg.scoring_version,
                market_scope=cfg.market_scope,
                entry_gate_mode=cfg.entry_gate_mode,
                market_filter_mode=cfg.market_filter_mode,
                benchmark_mode=cfg.benchmark_mode,
                position_stop_loss_enabled=cfg.position_stop_loss_enabled,
                position_stop_loss_pct=cfg.position_stop_loss_pct,
                stop_loss_cash_mode=cfg.stop_loss_cash_mode,
                stop_loss_cooldown_days=cfg.stop_loss_cooldown_days,
                portfolio_dd_cut_enabled=cfg.portfolio_dd_cut_enabled,
                portfolio_dd_cut_pct=cfg.portfolio_dd_cut_pct,
                portfolio_dd_cooldown_days=cfg.portfolio_dd_cooldown_days,
                overheat_entry_gate_enabled=cfg.overheat_entry_gate_enabled,
                max_entry_ret_1d=cfg.max_entry_ret_1d,
                max_entry_ret_5d=cfg.max_entry_ret_5d,
                max_entry_range_pct=cfg.max_entry_range_pct,
                max_entry_volume_z20=cfg.max_entry_volume_z20,
                volume_surge_rule_enabled=cfg.volume_surge_rule_enabled,
                volume_surge_threshold=cfg.volume_surge_threshold,
                volume_surge_ret_5d_threshold=cfg.volume_surge_ret_5d_threshold,
                overheat_rejected_count_mean=mean(overheat_rejected) if overheat_rejected else None,
                overheat_cash_days_mean=mean(overheat_cash) if overheat_cash else None,
                overheat_rejected_by_ret_1d_mean=mean(overheat_ret_1d) if overheat_ret_1d else None,
                overheat_rejected_by_ret_5d_mean=mean(overheat_ret_5d) if overheat_ret_5d else None,
                overheat_rejected_by_range_pct_mean=mean(overheat_range) if overheat_range else None,
                overheat_rejected_by_volume_z20_mean=mean(overheat_volz) if overheat_volz else None,
                overheat_rejected_by_volume_surge_rule_mean=mean(overheat_surge) if overheat_surge else None,
                num_windows=len(rows),
                evaluated_windows=len(valid),
                skipped_windows=len(rows) - len(valid),
                evaluated_ratio=_safe_div(len(valid), len(rows)),
                mean_total_return=mean(rets) if rets else None,
                median_total_return=median(rets) if rets else None,
                std_total_return=_stddev(rets),
                mean_benchmark_return=mean(bench) if bench else None,
                mean_excess_return=mean(excess) if excess else None,
                win_rate_vs_benchmark=(_safe_div(sum(1 for x in excess if x > 0), len(excess)) if excess else None),
                mean_sharpe=mean(sharpes) if sharpes else None,
                worst_mdd=min(mdds) if mdds else None,
                stability_score=stability,
                evaluated_1m_windows=eval_counts[1],
                evaluated_3m_windows=eval_counts[3],
                evaluated_6m_windows=eval_counts[6],
                evaluated_12m_windows=eval_counts[12],
                horizon_coverage_ratio=coverage,
                missing_horizon_penalty_applied=1 if coverage < 1 else 0,
            )
        )

    sum_fields = list(asdict(summaries[0]).keys())
    conn.executemany(
        f"INSERT INTO start_window_robustness_strategy_summary({','.join(sum_fields)}) VALUES ({','.join(['?']*len(sum_fields))})",
        [tuple(asdict(s).values()) for s in summaries],
    )

    period_rows = []
    for p_month in sorted(set(periods)):
        rows = [r for r in results if r.period_months == p_month]
        valid = [r for r in rows if r.skipped == 0]
        period_rows.append(
            {
                "batch_id": batch_id,
                "period_months": p_month,
                "num_windows": len(rows),
                "evaluated_windows": len(valid),
                "skipped_windows": len(rows) - len(valid),
                "mean_total_return": mean([r.total_return for r in valid if r.total_return is not None]) if valid else None,
                "mean_benchmark_return": mean([r.benchmark_return for r in valid if r.benchmark_return is not None]) if valid and args.benchmark_mode != "none" else None,
                "mean_excess_return": mean([r.excess_return for r in valid if r.excess_return is not None]) if valid and args.benchmark_mode != "none" else None,
                "mean_sharpe": mean([r.sharpe for r in valid if r.sharpe is not None]) if valid else None,
            }
        )
    conn.executemany(
        """
        INSERT INTO start_window_robustness_period_summary(
            batch_id,period_months,num_windows,evaluated_windows,skipped_windows,
            mean_total_return,mean_benchmark_return,mean_excess_return,mean_sharpe
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        [tuple(r[k] for k in ["batch_id", "period_months", "num_windows", "evaluated_windows", "skipped_windows", "mean_total_return", "mean_benchmark_return", "mean_excess_return", "mean_sharpe"]) for r in period_rows],
    )
    conn.commit()

    out = Path(args.output_dir)
    detail_csv = out / f"start_window_robustness_results_{batch_id}.csv"
    summary_csv = out / f"start_window_robustness_strategy_summary_{batch_id}.csv"
    period_csv = out / f"start_window_robustness_period_summary_{batch_id}.csv"
    md = out / f"start_window_robustness_summary_{batch_id}.md"

    detail_rows = [asdict(r) for r in results]
    summary_rows = [asdict(s) for s in sorted(summaries, key=lambda x: (x.stability_score is not None, x.stability_score), reverse=True)]
    _write_csv(detail_csv, detail_rows, list(detail_rows[0].keys()))
    _write_csv(summary_csv, summary_rows, list(summary_rows[0].keys()))
    _write_csv(period_csv, period_rows, list(period_rows[0].keys()))

    db_count = conn.execute("SELECT COUNT(1) FROM start_window_robustness_results WHERE batch_id=?", (batch_id,)).fetchone()[0]
    csv_count = max(0, sum(1 for _ in detail_csv.open("r", encoding="utf-8")) - 1)
    if db_count != csv_count:
        raise RuntimeError(f"CSV/SQLite row-count mismatch: db={db_count}, csv={csv_count}")

    lines = [
        f"# Start-window Robustness Summary ({batch_id})",
        "",
        "## Policy",
        f"- benchmark_mode: `{args.benchmark_mode}`",
        f"- min_warmup_days: `{args.min_warmup_days}`",
        "- stability_score uses fixed weights 1/3/6/12m = 0.10/0.20/0.30/0.40 without missing-horizon renormalization.",
        "- configs with missing 6m/12m coverage are marked as insufficient horizon coverage.",
        "",
        "## Top stable configs",
        "",
        "|rank|config|stability|eval|coverage|note|",
        "|---:|---|---:|---:|---:|---|",
    ]
    rank = 0
    for s in summary_rows:
        coverage_ok = s["evaluated_6m_windows"] > 0 and s["evaluated_12m_windows"] > 0
        note = "ok" if coverage_ok else "insufficient horizon coverage"
        if not coverage_ok:
            continue
        rank += 1
        stability_txt = "NA" if s["stability_score"] is None else f"{s['stability_score']:.4f}"
        lines.append(
            f"|{rank}|`{s['config_id']}`|{stability_txt}|{s['evaluated_windows']}/{s['num_windows']}|{s['horizon_coverage_ratio']:.2f}|{note}|"
        )
        if rank >= 10:
            break
    if rank == 0:
        lines.append("|1|N/A|NA|0/0|0.00|insufficient horizon coverage|")

    md.parent.mkdir(parents=True, exist_ok=True)
    md.write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({
        "batch_id": batch_id,
        "results_csv": str(detail_csv),
        "summary_csv": str(summary_csv),
        "period_csv": str(period_csv),
        "summary_md": str(md),
        "csv_row_count": csv_count,
        "sqlite_row_count": db_count,
        "lookahead_validation": lookahead_info,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
