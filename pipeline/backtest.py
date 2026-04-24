from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone


def run_backtest(
    conn: sqlite3.Connection,
    top_n: int = 5,
    start_date: str | None = None,
    end_date: str | None = None,
    initial_equity: float = 100000.0,
) -> str:
    """Equal-weight long backtest using daily_scores and next-day close returns."""
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
        "INSERT INTO backtest_runs(run_id,created_at,top_n,start_date,end_date) VALUES(?,?,?,?,?)",
        (run_id, created_at, top_n, start_date, end_date),
    )

    equity = initial_equity
    result_rows: list[tuple] = []

    for i in range(len(all_dates) - 1):
        d0 = all_dates[i]
        d1 = all_dates[i + 1]

        picks = conn.execute(
            "SELECT symbol FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC LIMIT ?",
            (d0, top_n),
        ).fetchall()
        symbols = [p["symbol"] for p in picks]
        if not symbols:
            daily_ret = 0.0
            pos_count = 0
        else:
            returns = []
            for sym in symbols:
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
