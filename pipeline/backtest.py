from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone


def _is_week_boundary(date_str: str, prev_date: str | None) -> bool:
    if prev_date is None:
        return True
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    p = datetime.strptime(prev_date, "%Y-%m-%d").date()
    return d.isocalendar()[:2] != p.isocalendar()[:2]


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
    keep_due_rank = {
        sym
        for sym in current_symbols
        if rank_by_symbol.get(sym, 10**9) <= keep_rank_threshold
    }
    keep_due_holding_period = {
        sym
        for sym in current_symbols
        if (current_day_index - entry_index_by_symbol.get(sym, current_day_index)) < min_holding_days
    }
    kept = set(keep_due_rank) | set(keep_due_holding_period)

    target = list(kept)
    for sym in ranked_symbols:
        if sym in kept:
            continue
        if len(target) >= top_n:
            break
        target.append(sym)
    return set(target)


def run_backtest(
    conn: sqlite3.Connection,
    top_n: int = 5,
    start_date: str | None = None,
    end_date: str | None = None,
    initial_equity: float = 100000.0,
    rebalance_frequency: str = "daily",
    min_holding_days: int = 0,
    keep_rank_threshold: int | None = None,
    scoring_profile: str = "improved_v1",
) -> str:
    """Equal-weight long backtest using daily_scores and next-day close returns."""
    if rebalance_frequency not in {"daily", "weekly"}:
        raise ValueError("rebalance_frequency must be one of: daily, weekly")

    if keep_rank_threshold is None:
        keep_rank_threshold = top_n

    run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    all_dates = [
        r["date"]
        for r in conn.execute(
            "SELECT DISTINCT date FROM daily_prices ORDER BY date"
        ).fetchall()
    ]

    if start_date:
        all_dates = [d for d in all_dates if d >= start_date]
    if end_date:
        all_dates = [d for d in all_dates if d <= end_date]

    if len(all_dates) < 2:
        raise ValueError("Need at least 2 dates for backtest")

    conn.execute(
        """
        INSERT INTO backtest_runs(
            run_id,created_at,top_n,start_date,end_date,initial_equity,
            rebalance_frequency,min_holding_days,keep_rank_threshold,scoring_profile
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            created_at,
            top_n,
            start_date,
            end_date,
            float(initial_equity),
            rebalance_frequency,
            int(min_holding_days),
            int(keep_rank_threshold),
            scoring_profile,
        ),
    )

    equity = initial_equity
    result_rows: list[tuple] = []

    current_holdings: set[str] = set()
    entry_index_by_symbol: dict[str, int] = {}

    for i in range(len(all_dates) - 1):
        d0 = all_dates[i]
        d1 = all_dates[i + 1]
        prev_d0 = all_dates[i - 1] if i > 0 else None

        should_rebalance = (
            rebalance_frequency == "daily"
            or (rebalance_frequency == "weekly" and _is_week_boundary(d0, prev_d0))
        )

        if should_rebalance:
            ranked_rows = conn.execute(
                "SELECT symbol, rank FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC",
                (d0,),
            ).fetchall()
            ranked_symbols = [r["symbol"] for r in ranked_rows]
            rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked_rows}

            target_holdings = _build_target_holdings(
                ranked_symbols=ranked_symbols,
                rank_by_symbol=rank_by_symbol,
                current_symbols=current_holdings,
                entry_index_by_symbol=entry_index_by_symbol,
                current_day_index=i,
                top_n=top_n,
                min_holding_days=int(min_holding_days),
                keep_rank_threshold=int(keep_rank_threshold),
            )

            for sym in (target_holdings - current_holdings):
                entry_index_by_symbol[sym] = i
            for sym in (current_holdings - target_holdings):
                entry_index_by_symbol.pop(sym, None)
            current_holdings = target_holdings

        if not current_holdings:
            daily_ret = 0.0
            pos_count = 0
        else:
            returns = []
            for sym in sorted(current_holdings):
                row0 = conn.execute(
                    "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d0)
                ).fetchone()
                row1 = conn.execute(
                    "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d1)
                ).fetchone()
                if row0 and row1 and row0["close"]:
                    returns.append((row1["close"] - row0["close"]) / row0["close"])
            daily_ret = (sum(returns) / len(returns)) if returns else 0.0
            pos_count = len(returns)

        equity *= 1.0 + daily_ret
        result_rows.append((run_id, d1, equity, daily_ret, pos_count))

    conn.executemany(
        """
        INSERT INTO backtest_results(run_id,date,equity,daily_return,position_count)
        VALUES(?,?,?,?,?)
        """,
        result_rows,
    )
    conn.commit()
    return run_id
