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
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db
from pipeline.scoring import DEFAULT_SCORING_PROFILE, generate_daily_scores, normalize_scoring_profile
from pipeline.universe_input import load_symbols_from_universe_csv

TRADING_DAYS = 252


@dataclass
class BacktestAnalysis:
    run_id: str
    metrics: dict[str, Any]
    contributions: list[dict[str, Any]]


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


def _is_week_boundary(curr: str, prev: str | None) -> bool:
    if prev is None:
        return True
    c = datetime.strptime(curr, "%Y-%m-%d").date().isocalendar()[:2]
    p = datetime.strptime(prev, "%Y-%m-%d").date().isocalendar()[:2]
    return c != p


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


def _simulate_holdings(conn: sqlite3.Connection, run_id: str) -> tuple[list[str], list[set[str]]]:
    run = conn.execute(
        """
        SELECT top_n,
               COALESCE(rebalance_frequency,'daily') AS rebalance_frequency,
               COALESCE(min_holding_days,0) AS min_holding_days,
               COALESCE(keep_rank_threshold, top_n) AS keep_rank_threshold
        FROM backtest_runs
        WHERE run_id=?
        """,
        (run_id,),
    ).fetchone()
    if not run:
        raise ValueError(f"run_id not found: {run_id}")

    result_dates = [
        r["date"]
        for r in conn.execute("SELECT date FROM backtest_results WHERE run_id=? ORDER BY date", (run_id,)).fetchall()
    ]
    if not result_dates:
        return [], []
    prev_dates = [conn.execute("SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d,)).fetchone()["d"] for d in result_dates]

    top_n = int(run["top_n"])
    rebalance_frequency = str(run["rebalance_frequency"])
    min_holding_days = int(run["min_holding_days"])
    keep_rank_threshold = int(run["keep_rank_threshold"])

    current_holdings: set[str] = set()
    entry_index_by_symbol: dict[str, int] = {}
    holdings_by_day: list[set[str]] = []

    for i, d0 in enumerate(prev_dates):
        if not d0:
            holdings_by_day.append(set())
            continue

        prev_d0 = prev_dates[i - 1] if i > 0 else None
        should_rebalance = rebalance_frequency == "daily" or (rebalance_frequency == "weekly" and _is_week_boundary(d0, prev_d0))

        if should_rebalance:
            ranked = conn.execute(
                "SELECT symbol, rank FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC",
                (d0,),
            ).fetchall()
            ranked_symbols = [r["symbol"] for r in ranked]
            rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked}
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

    return result_dates, holdings_by_day


def _compute_candidate_avg_return(conn: sqlite3.Connection, symbols: set[str], dates: list[str]) -> float:
    if len(dates) < 2 or not symbols:
        return 0.0
    equity = 1.0
    placeholders = ",".join("?" for _ in symbols)
    symbol_params = sorted(symbols)
    for i in range(len(dates) - 1):
        d0 = dates[i]
        d1 = dates[i + 1]
        rows = conn.execute(
            f"""
            SELECT p0.close AS c0, p1.close AS c1
            FROM daily_prices p0
            JOIN daily_prices p1 ON p1.symbol=p0.symbol
            WHERE p0.date=? AND p1.date=? AND p0.symbol IN ({placeholders}) AND p1.symbol IN ({placeholders})
            """,
            [d0, d1, *symbol_params, *symbol_params],
        ).fetchall()
        rets = [(r["c1"] - r["c0"]) / r["c0"] for r in rows if r["c0"]]
        daily_ret = sum(rets) / len(rets) if rets else 0.0
        equity *= 1.0 + daily_ret
    return equity - 1.0


def _analyze_symbol_contribution(
    conn: sqlite3.Connection,
    result_dates: list[str],
    holdings_by_day: list[set[str]],
) -> list[dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    prev_holdings: set[str] = set()

    for i, d1 in enumerate(result_dates):
        d0 = conn.execute("SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d1,)).fetchone()["d"]
        if not d0:
            continue
        current = holdings_by_day[i] if i < len(holdings_by_day) else set()
        n = len(current)
        for sym in current:
            rec = stats.setdefault(sym, {"symbol": sym, "holding_days": 0, "selection_count": 0, "estimated_contribution_return": 0.0})
            rec["holding_days"] += 1
            if sym not in prev_holdings:
                rec["selection_count"] += 1

            row = conn.execute(
                "SELECT p0.close AS c0, p1.close AS c1 FROM daily_prices p0 JOIN daily_prices p1 ON p1.symbol=p0.symbol WHERE p0.symbol=? AND p0.date=? AND p1.date=?",
                (sym, d0, d1),
            ).fetchone()
            if row and row["c0"] and n > 0:
                sym_ret = (row["c1"] - row["c0"]) / row["c0"]
                rec["estimated_contribution_return"] += (sym_ret / n)

        prev_holdings = set(current)

    rows = list(stats.values())
    rows.sort(key=lambda x: float(x["estimated_contribution_return"]), reverse=True)
    total_positive = sum(max(float(x["estimated_contribution_return"]), 0.0) for x in rows)
    for rec in rows:
        rec["contribution_share"] = _safe_div(max(float(rec["estimated_contribution_return"]), 0.0), total_positive)
    return rows


def _estimate_trade_count(holdings_by_day: list[set[str]]) -> int:
    if not holdings_by_day:
        return 0
    trades = len(holdings_by_day[0])
    for i in range(1, len(holdings_by_day)):
        prev = holdings_by_day[i - 1]
        cur = holdings_by_day[i]
        trades += len(cur - prev) + len(prev - cur)
    return trades


def _run_and_analyze(
    conn: sqlite3.Connection,
    label: str,
    symbols: set[str],
    top_n: int,
    start_date: str | None,
    end_date: str | None,
    initial_equity: float,
    rebalance_frequency: str,
    min_holding_days: int,
    keep_rank_threshold: int | None,
    scoring_profile: str,
) -> BacktestAnalysis | None:
    if not symbols:
        return None

    generate_daily_scores(
        conn,
        include_history=True,
        allowed_symbols=sorted(symbols),
        scoring_profile=scoring_profile,
    )
    run_id = run_backtest(
        conn,
        top_n=top_n,
        start_date=start_date,
        end_date=end_date,
        initial_equity=initial_equity,
        rebalance_frequency=rebalance_frequency,
        min_holding_days=min_holding_days,
        keep_rank_threshold=keep_rank_threshold,
        scoring_profile=scoring_profile,
    )

    results = conn.execute(
        "SELECT date, equity, daily_return, position_count FROM backtest_results WHERE run_id=? ORDER BY date",
        (run_id,),
    ).fetchall()
    returns = [float(r["daily_return"]) for r in results]
    equities = [float(r["equity"]) for r in results]
    avg_holdings = sum(int(r["position_count"]) for r in results) / len(results) if results else 0.0

    result_dates, holdings_by_day = _simulate_holdings(conn, run_id)
    contributions = _analyze_symbol_contribution(conn, result_dates, holdings_by_day)
    top3 = contributions[:3]
    total_positive = sum(max(float(c["estimated_contribution_return"]), 0.0) for c in contributions)
    top3_contrib = sum(max(float(c["estimated_contribution_return"]), 0.0) for c in top3)

    candidate_avg_return = _compute_candidate_avg_return(conn, symbols, [r["date"] for r in results])
    total_return = _safe_div((equities[-1] if equities else initial_equity) - initial_equity, initial_equity)

    metrics = {
        "label": label,
        "run_id": run_id,
        "symbol_count": len(symbols),
        "top_n": top_n,
        "total_return": total_return,
        "max_drawdown": _max_drawdown(equities),
        "sharpe": _sharpe(returns),
        "candidate_avg_return": candidate_avg_return,
        "excess_return_vs_universe": total_return - candidate_avg_return,
        "trade_count": _estimate_trade_count(holdings_by_day),
        "avg_holdings": avg_holdings,
        "top3_contribution_share": _safe_div(top3_contrib, total_positive),
    }

    return BacktestAnalysis(run_id=run_id, metrics=metrics, contributions=contributions)


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _persist_to_sqlite(conn: sqlite3.Connection, report_id: str, universe_rows: list[dict[str, Any]], topn_rows: list[dict[str, Any]], contrib_rows: list[dict[str, Any]]) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS universe_sensitivity_reports (
            report_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            notes TEXT
        );
        CREATE TABLE IF NOT EXISTS universe_sensitivity_metrics (
            report_id TEXT NOT NULL,
            scope_label TEXT NOT NULL,
            run_id TEXT NOT NULL,
            symbol_count INTEGER NOT NULL,
            top_n INTEGER NOT NULL,
            total_return REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            sharpe REAL NOT NULL,
            candidate_avg_return REAL NOT NULL,
            excess_return_vs_universe REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            avg_holdings REAL NOT NULL,
            top3_contribution_share REAL NOT NULL,
            PRIMARY KEY (report_id, scope_label),
            FOREIGN KEY (report_id) REFERENCES universe_sensitivity_reports(report_id)
        );
        CREATE TABLE IF NOT EXISTS topn_sensitivity_metrics (
            report_id TEXT NOT NULL,
            universe_scope TEXT NOT NULL,
            run_id TEXT NOT NULL,
            top_n INTEGER NOT NULL,
            symbol_count INTEGER NOT NULL,
            total_return REAL NOT NULL,
            max_drawdown REAL NOT NULL,
            sharpe REAL NOT NULL,
            candidate_avg_return REAL NOT NULL,
            excess_return_vs_universe REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            avg_holdings REAL NOT NULL,
            top3_contribution_share REAL NOT NULL,
            PRIMARY KEY (report_id, universe_scope, top_n),
            FOREIGN KEY (report_id) REFERENCES universe_sensitivity_reports(report_id)
        );
        CREATE TABLE IF NOT EXISTS symbol_contribution_metrics (
            report_id TEXT NOT NULL,
            universe_scope TEXT NOT NULL,
            symbol TEXT NOT NULL,
            holding_days INTEGER NOT NULL,
            selection_count INTEGER NOT NULL,
            estimated_contribution_return REAL NOT NULL,
            contribution_share REAL NOT NULL,
            PRIMARY KEY (report_id, universe_scope, symbol),
            FOREIGN KEY (report_id) REFERENCES universe_sensitivity_reports(report_id)
        );
        """
    )
    conn.execute(
        "INSERT INTO universe_sensitivity_reports(report_id, created_at, notes) VALUES(?,?,?)",
        (report_id, datetime.utcnow().isoformat(), "universe/top_n sensitivity diagnostics"),
    )
    conn.executemany(
        """
        INSERT INTO universe_sensitivity_metrics(
            report_id,scope_label,run_id,symbol_count,top_n,total_return,max_drawdown,sharpe,
            candidate_avg_return,excess_return_vs_universe,trade_count,avg_holdings,top3_contribution_share
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                report_id,
                r["label"],
                r["run_id"],
                int(r["symbol_count"]),
                int(r["top_n"]),
                float(r["total_return"]),
                float(r["max_drawdown"]),
                float(r["sharpe"]),
                float(r["candidate_avg_return"]),
                float(r["excess_return_vs_universe"]),
                int(r["trade_count"]),
                float(r["avg_holdings"]),
                float(r["top3_contribution_share"]),
            )
            for r in universe_rows
        ],
    )
    conn.executemany(
        """
        INSERT INTO topn_sensitivity_metrics(
            report_id,universe_scope,run_id,top_n,symbol_count,total_return,max_drawdown,sharpe,
            candidate_avg_return,excess_return_vs_universe,trade_count,avg_holdings,top3_contribution_share
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                report_id,
                r["universe_scope"],
                r["run_id"],
                int(r["top_n"]),
                int(r["symbol_count"]),
                float(r["total_return"]),
                float(r["max_drawdown"]),
                float(r["sharpe"]),
                float(r["candidate_avg_return"]),
                float(r["excess_return_vs_universe"]),
                int(r["trade_count"]),
                float(r["avg_holdings"]),
                float(r["top3_contribution_share"]),
            )
            for r in topn_rows
        ],
    )
    conn.executemany(
        """
        INSERT INTO symbol_contribution_metrics(
            report_id,universe_scope,symbol,holding_days,selection_count,estimated_contribution_return,contribution_share
        ) VALUES(?,?,?,?,?,?,?)
        """,
        [
            (
                report_id,
                r.get("universe_scope", "primary"),
                r["symbol"],
                int(r["holding_days"]),
                int(r["selection_count"]),
                float(r["estimated_contribution_return"]),
                float(r["contribution_share"]),
            )
            for r in contrib_rows
        ],
    )
    conn.commit()


def main() -> None:
    p = argparse.ArgumentParser(description="Universe sensitivity / concentration diagnostics")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--old-universe-file", required=True)
    p.add_argument("--new-universe-file", required=True)
    p.add_argument("--output-dir", default="data/reports/universe_sensitivity")
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--initial-equity", type=float, default=100000.0)
    p.add_argument("--rebalance-frequency", choices=["daily", "weekly"], default="daily")
    p.add_argument("--min-holding-days", type=int, default=5)
    p.add_argument("--keep-rank-threshold", type=int)
    p.add_argument("--scoring-version", default=DEFAULT_SCORING_PROFILE)
    p.add_argument("--universe-top-n", type=int, default=3)
    p.add_argument("--top-n-values", default="3,5,10")
    p.add_argument("--topn-universe-scope", choices=["old", "new", "overlap"], default="new")
    p.add_argument("--skip-sqlite", action="store_true")
    args = p.parse_args()

    scoring_profile = normalize_scoring_profile(args.scoring_version)
    conn = get_connection(args.db)
    init_db(conn)

    old_symbols = set(load_symbols_from_universe_csv(args.old_universe_file))
    new_symbols = set(load_symbols_from_universe_csv(args.new_universe_file))
    overlap = old_symbols & new_symbols
    removed = old_symbols - new_symbols
    added = new_symbols - old_symbols

    compare_row = {
        "old_universe_file": args.old_universe_file,
        "new_universe_file": args.new_universe_file,
        "old_count": len(old_symbols),
        "new_count": len(new_symbols),
        "overlap_count": len(overlap),
        "removed_count": len(removed),
        "added_count": len(added),
        "removed_symbols": ",".join(sorted(removed)),
        "added_symbols": ",".join(sorted(added)),
        "overlap_ratio": _safe_div(len(overlap), len(old_symbols | new_symbols)),
    }

    scopes = {
        "old_universe": old_symbols,
        "new_universe": new_symbols,
        "overlap_universe_only": overlap,
        "removed_symbols_only": removed,
        "added_symbols_only": added,
    }

    universe_rows: list[dict[str, Any]] = []
    analyses: dict[str, BacktestAnalysis] = {}
    for label, symbols in scopes.items():
        analysis = _run_and_analyze(
            conn=conn,
            label=label,
            symbols=symbols,
            top_n=args.universe_top_n,
            start_date=args.start_date,
            end_date=args.end_date,
            initial_equity=args.initial_equity,
            rebalance_frequency=args.rebalance_frequency,
            min_holding_days=args.min_holding_days,
            keep_rank_threshold=args.keep_rank_threshold,
            scoring_profile=scoring_profile,
        )
        if analysis is None:
            continue
        analyses[label] = analysis
        universe_rows.append(analysis.metrics)

    scope_map = {
        "old": old_symbols,
        "new": new_symbols,
        "overlap": overlap,
    }
    topn_scope_symbols = scope_map[args.topn_universe_scope]
    top_n_values = [int(x.strip()) for x in args.top_n_values.split(",") if x.strip()]

    topn_rows: list[dict[str, Any]] = []
    topn_analyses: list[BacktestAnalysis] = []
    for top_n in top_n_values:
        analysis = _run_and_analyze(
            conn=conn,
            label=f"{args.topn_universe_scope}_top_n_{top_n}",
            symbols=topn_scope_symbols,
            top_n=top_n,
            start_date=args.start_date,
            end_date=args.end_date,
            initial_equity=args.initial_equity,
            rebalance_frequency=args.rebalance_frequency,
            min_holding_days=args.min_holding_days,
            keep_rank_threshold=args.keep_rank_threshold,
            scoring_profile=scoring_profile,
        )
        if analysis is None:
            continue
        topn_analyses.append(analysis)
        row = dict(analysis.metrics)
        row["universe_scope"] = args.topn_universe_scope
        topn_rows.append(row)

    topn_rows.sort(key=lambda x: int(x["top_n"]))
    baseline = topn_rows[0] if topn_rows else None
    for row in topn_rows:
        if baseline:
            row["delta_return_vs_baseline"] = float(row["total_return"]) - float(baseline["total_return"])
            row["delta_mdd_vs_baseline"] = float(row["max_drawdown"]) - float(baseline["max_drawdown"])
            row["delta_excess_vs_baseline"] = float(row["excess_return_vs_universe"]) - float(baseline["excess_return_vs_universe"])
        else:
            row["delta_return_vs_baseline"] = 0.0
            row["delta_mdd_vs_baseline"] = 0.0
            row["delta_excess_vs_baseline"] = 0.0

    primary_label = "new_universe" if "new_universe" in analyses else (next(iter(analyses)) if analyses else None)
    primary_contrib = analyses[primary_label].contributions if primary_label else []
    top10 = primary_contrib[:10]
    bottom10 = list(reversed(primary_contrib[-10:])) if primary_contrib else []

    top3_share = 0.0
    if primary_contrib:
        total = sum(max(float(r["estimated_contribution_return"]), 0.0) for r in primary_contrib)
        top3 = sum(max(float(r["estimated_contribution_return"]), 0.0) for r in primary_contrib[:3])
        top3_share = _safe_div(top3, total)

    concentration_summary = {
        "primary_scope": primary_label,
        "is_concentrated": bool(top3_share >= 0.5),
        "top_n3_too_concentrated": bool(any(r["top_n"] == 3 and float(r["top3_contribution_share"]) >= 0.5 for r in topn_rows)),
        "top_n5_or_10_more_stable": bool(
            any(r["top_n"] in {5, 10} and float(r["max_drawdown"]) > float(next((x for x in topn_rows if x["top_n"] == 3), {"max_drawdown": 0})["max_drawdown"]) for r in topn_rows)
        ),
        "top3_contribution_share_primary": top3_share,
    }

    report_id = datetime.utcnow().strftime("univsens_%Y%m%d_%H%M%S")
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(out_dir / "universe_comparison.csv", [compare_row], list(compare_row.keys()))
    if universe_rows:
        _write_csv(out_dir / "universe_sensitivity_metrics.csv", universe_rows, list(universe_rows[0].keys()))
    if topn_rows:
        _write_csv(out_dir / "topn_sensitivity_metrics.csv", topn_rows, list(topn_rows[0].keys()))
    if top10:
        _write_csv(out_dir / "symbol_contribution_top10.csv", top10, list(top10[0].keys()))
    if bottom10:
        _write_csv(out_dir / "symbol_contribution_bottom10.csv", bottom10, list(bottom10[0].keys()))

    summary = {
        "report_id": report_id,
        "universe_comparison": compare_row,
        "universe_metrics": universe_rows,
        "topn_metrics": topn_rows,
        "concentration_risk": concentration_summary,
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    if not args.skip_sqlite:
        contrib_rows = [dict(r, universe_scope=primary_label or "primary") for r in primary_contrib]
        _persist_to_sqlite(conn, report_id, universe_rows, topn_rows, contrib_rows)

    print(json.dumps({"report_id": report_id, "output_dir": str(out_dir), "primary_scope": primary_label}, ensure_ascii=False))


if __name__ == "__main__":
    main()
