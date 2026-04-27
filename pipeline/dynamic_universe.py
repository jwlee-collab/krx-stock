from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def build_rolling_liquidity_universe(
    conn: sqlite3.Connection,
    universe_size: int = 100,
    lookback_days: int = 20,
) -> dict[str, int | str]:
    """
    Build daily rolling-liquidity universe.

    Look-ahead prevention rule:
    - Universe of date t uses only data up to t-1.
    - Concretely, avg_dollar_volume is computed from the previous `lookback_days`
      rows per symbol and excludes date t itself.
    """
    if universe_size <= 0:
        raise ValueError("universe_size must be > 0")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be > 0")

    mode = "rolling_liquidity"
    created_at = datetime.now(timezone.utc).isoformat()
    before = conn.total_changes

    conn.execute(
        "DELETE FROM daily_universe WHERE universe_mode=? AND universe_size=? AND lookback_days=?",
        (mode, int(universe_size), int(lookback_days)),
    )

    conn.execute(
        """
        INSERT INTO daily_universe(
            date, symbol, universe_rank, avg_dollar_volume,
            universe_mode, universe_size, lookback_days, created_at
        )
        WITH base AS (
            SELECT
                symbol,
                date,
                close * volume AS dollar_volume,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS rn
            FROM daily_prices
        ),
        hist AS (
            SELECT
                cur.date AS date,
                cur.symbol AS symbol,
                AVG(prev.dollar_volume) AS avg_dollar_volume,
                COUNT(prev.dollar_volume) AS sample_cnt
            FROM base cur
            JOIN base prev
              ON prev.symbol = cur.symbol
             AND prev.rn BETWEEN (cur.rn - ?) AND (cur.rn - 1)
            GROUP BY cur.date, cur.symbol
        ),
        ranked AS (
            SELECT
                date,
                symbol,
                avg_dollar_volume,
                ROW_NUMBER() OVER (
                    PARTITION BY date
                    ORDER BY avg_dollar_volume DESC, symbol ASC
                ) AS universe_rank
            FROM hist
            WHERE sample_cnt = ?
        )
        SELECT
            date,
            symbol,
            universe_rank,
            avg_dollar_volume,
            ?,
            ?,
            ?,
            ?
        FROM ranked
        WHERE universe_rank <= ?
        """,
        (
            int(lookback_days),
            int(lookback_days),
            mode,
            int(universe_size),
            int(lookback_days),
            created_at,
            int(universe_size),
        ),
    )

    conn.commit()
    return {
        "universe_mode": mode,
        "universe_size": int(universe_size),
        "lookback_days": int(lookback_days),
        "row_changes": conn.total_changes - before,
    }


def validate_rolling_universe_no_lookahead(
    conn: sqlite3.Connection,
    universe_size: int,
    lookback_days: int,
    sample_limit: int = 50,
) -> dict[str, int]:
    """
    Validation for look-ahead rule:
    Recompute avg from [t-lookback, t-1] only and compare with stored avg.
    """
    rows = conn.execute(
        """
        SELECT date, symbol, avg_dollar_volume
        FROM daily_universe
        WHERE universe_mode='rolling_liquidity'
          AND universe_size=?
          AND lookback_days=?
        ORDER BY date, universe_rank
        LIMIT ?
        """,
        (int(universe_size), int(lookback_days), int(sample_limit)),
    ).fetchall()

    checked = 0
    violations = 0
    for row in rows:
        rec = conn.execute(
            """
            WITH base AS (
                SELECT
                    date,
                    close * volume AS dollar_volume,
                    ROW_NUMBER() OVER (ORDER BY date) AS rn
                FROM daily_prices
                WHERE symbol=?
            ),
            cur AS (
                SELECT rn
                FROM base
                WHERE date=?
            )
            SELECT AVG(prev.dollar_volume) AS avg_dv
            FROM base prev, cur
            WHERE prev.rn BETWEEN (cur.rn - ?) AND (cur.rn - 1)
            """,
            (row["symbol"], row["date"], int(lookback_days)),
        ).fetchone()
        if rec and rec["avg_dv"] is not None:
            checked += 1
            if abs(float(rec["avg_dv"]) - float(row["avg_dollar_volume"])) > 1e-6:
                violations += 1

    return {"checked_rows": checked, "violations": violations}
