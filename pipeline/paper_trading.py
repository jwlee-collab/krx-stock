from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def run_paper_trading_cycle(
    conn: sqlite3.Connection,
    as_of_date: str | None = None,
    target_positions: int = 5,
    notional_per_position: float = 10000.0,
) -> dict:
    """Rebalance paper portfolio to top-ranked symbols for a date."""
    if as_of_date is None:
        row = conn.execute("SELECT MAX(date) AS d FROM daily_scores").fetchone()
        as_of_date = row["d"] if row else None
    if not as_of_date:
        raise ValueError("No score date available")

    ts = datetime.now(timezone.utc).isoformat()

    target = conn.execute(
        "SELECT symbol FROM daily_scores WHERE date=? ORDER BY rank ASC LIMIT ?",
        (as_of_date, target_positions),
    ).fetchall()
    target_syms = {r["symbol"] for r in target}

    current = conn.execute("SELECT symbol, qty FROM paper_positions").fetchall()
    current_syms = {r["symbol"] for r in current}

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
                INSERT INTO paper_positions(symbol,qty,entry_price,updated_at)
                VALUES(?,?,?,?)
                ON CONFLICT(symbol) DO UPDATE SET
                    qty=excluded.qty,
                    entry_price=excluded.entry_price,
                    updated_at=excluded.updated_at
                """,
                (sym, qty, px["close"], ts),
            )
            bought += 1

    conn.commit()
    return {
        "as_of_date": as_of_date,
        "target_count": len(target_syms),
        "sold": sold,
        "bought": bought,
        "open_positions": conn.execute("SELECT COUNT(*) c FROM paper_positions").fetchone()["c"],
    }
