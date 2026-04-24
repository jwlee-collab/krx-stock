from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def _is_same_iso_week(left: str, right: str) -> bool:
    l = datetime.strptime(left, "%Y-%m-%d").date().isocalendar()[:2]
    r = datetime.strptime(right, "%Y-%m-%d").date().isocalendar()[:2]
    return l == r


def _trading_days_between(conn: sqlite3.Connection, start_date: str, end_date: str) -> int:
    row = conn.execute(
        "SELECT COUNT(DISTINCT date) AS c FROM daily_prices WHERE date>=? AND date<=?",
        (start_date, end_date),
    ).fetchone()
    return int(row["c"] or 0)


def run_paper_trading_cycle(
    conn: sqlite3.Connection,
    as_of_date: str | None = None,
    target_positions: int = 5,
    notional_per_position: float = 10000.0,
    rebalance_frequency: str = "daily",
    min_holding_days: int = 0,
    keep_rank_threshold: int | None = None,
) -> dict:
    """Rebalance paper portfolio to top-ranked symbols for a date."""
    if rebalance_frequency not in {"daily", "weekly"}:
        raise ValueError("rebalance_frequency must be one of: daily, weekly")

    if keep_rank_threshold is None:
        keep_rank_threshold = target_positions

    if as_of_date is None:
        row = conn.execute("SELECT MAX(date) AS d FROM daily_scores").fetchone()
        as_of_date = row["d"] if row else None
    if not as_of_date:
        raise ValueError("No score date available")

    ts = datetime.now(timezone.utc).isoformat()

    if rebalance_frequency == "weekly":
        last = conn.execute(
            "SELECT MAX(as_of_date) AS d FROM paper_rebalance_log WHERE rebalance_frequency='weekly'"
        ).fetchone()
        last_date = last["d"] if last else None
        if last_date and _is_same_iso_week(last_date, as_of_date):
            return {
                "as_of_date": as_of_date,
                "target_count": 0,
                "sold": 0,
                "bought": 0,
                "open_positions": conn.execute("SELECT COUNT(*) c FROM paper_positions").fetchone()["c"],
                "skipped": True,
                "skip_reason": "weekly_rebalance_already_executed_in_same_iso_week",
            }

    ranked_rows = conn.execute(
        "SELECT symbol, rank FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC",
        (as_of_date,),
    ).fetchall()
    rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked_rows}

    current_rows = conn.execute("SELECT symbol, qty, entry_date FROM paper_positions").fetchall()
    current_syms = {r["symbol"] for r in current_rows}

    protected: set[str] = set()
    for row in current_rows:
        sym = row["symbol"]
        rank = rank_by_symbol.get(sym, 10**9)
        holding_days = _trading_days_between(conn, row["entry_date"], as_of_date) if row["entry_date"] else 0
        if rank <= int(keep_rank_threshold) or holding_days <= int(min_holding_days):
            protected.add(sym)

    target_list = list(protected)
    for row in ranked_rows:
        sym = row["symbol"]
        if sym in target_list:
            continue
        if len(target_list) >= target_positions:
            break
        target_list.append(sym)

    target_syms = set(target_list)

    to_sell = sorted(current_syms - target_syms)
    to_buy = sorted(target_syms - current_syms)

    sold = 0
    for sym in to_sell:
        pos = conn.execute("SELECT qty FROM paper_positions WHERE symbol=?", (sym,)).fetchone()
        px = conn.execute(
            "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, as_of_date)
        ).fetchone()
        if pos and px:
            conn.execute(
                "INSERT INTO paper_orders(created_at,symbol,side,qty,price,reason) VALUES(?,?,?,?,?,?)",
                (ts, sym, "SELL", pos["qty"], px["close"], "rebalance_out"),
            )
            conn.execute("DELETE FROM paper_positions WHERE symbol=?", (sym,))
            sold += 1

    bought = 0
    for sym in to_buy:
        px = conn.execute(
            "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, as_of_date)
        ).fetchone()
        if px and px["close"] > 0:
            qty = notional_per_position / px["close"]
            conn.execute(
                "INSERT INTO paper_orders(created_at,symbol,side,qty,price,reason) VALUES(?,?,?,?,?,?)",
                (ts, sym, "BUY", qty, px["close"], "rebalance_in"),
            )
            conn.execute(
                """
                INSERT INTO paper_positions(symbol,qty,entry_price,entry_date,updated_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    qty=excluded.qty,
                    entry_price=excluded.entry_price,
                    updated_at=excluded.updated_at
                """,
                (sym, qty, px["close"], as_of_date, ts),
            )
            bought += 1

    conn.execute(
        "INSERT OR REPLACE INTO paper_rebalance_log(as_of_date, executed_at, rebalance_frequency) VALUES(?,?,?)",
        (as_of_date, ts, rebalance_frequency),
    )

    conn.commit()
    return {
        "as_of_date": as_of_date,
        "target_count": len(target_syms),
        "sold": sold,
        "bought": bought,
        "open_positions": conn.execute("SELECT COUNT(*) c FROM paper_positions").fetchone()["c"],
        "skipped": False,
        "rebalance_frequency": rebalance_frequency,
        "min_holding_days": int(min_holding_days),
        "keep_rank_threshold": int(keep_rank_threshold),
    }
