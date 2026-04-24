from __future__ import annotations

import sqlite3
from collections import defaultdict


def score_formula(ret_1d: float | None, ret_5d: float | None, momentum_20d: float | None, range_pct: float | None, volume_z20: float | None) -> float:
    """Unified score formula for both latest and historical scoring."""
    r1 = ret_1d or 0.0
    r5 = ret_5d or 0.0
    m20 = momentum_20d or 0.0
    vol = volume_z20 or 0.0
    rng = range_pct or 0.0
    return (0.20 * r1) + (0.35 * r5) + (0.35 * m20) + (0.10 * vol) - (0.05 * rng)


def _rank_desc(values: list[tuple[str, float]]) -> list[tuple[str, float, int]]:
    sorted_vals = sorted(values, key=lambda t: t[1], reverse=True)
    ranked: list[tuple[str, float, int]] = []
    prev_score: float | None = None
    rank = 0
    for i, (symbol, score) in enumerate(sorted_vals, start=1):
        if prev_score is None or score != prev_score:
            rank = i
            prev_score = score
        ranked.append((symbol, score, rank))
    return ranked


def generate_daily_scores(
    conn: sqlite3.Connection,
    as_of_date: str | None = None,
    include_history: bool = False,
    allowed_symbols: list[str] | None = None,
) -> int:
    """Generate scores for one date (latest or provided) or all dates (historical mode)."""
    score_date: str | None = as_of_date
    symbol_filter_sql = ""
    symbol_filter_params: list[str] = []
    if allowed_symbols is not None:
        if not allowed_symbols:
            return 0
        symbol_filter_sql = f" AND symbol IN ({','.join('?' for _ in allowed_symbols)})"
        symbol_filter_params = allowed_symbols

    if include_history:
        rows = conn.execute(
            f"""
            SELECT symbol,date,ret_1d,ret_5d,momentum_20d,range_pct,volume_z20
            FROM daily_features
            WHERE 1=1 {symbol_filter_sql}
            ORDER BY date, symbol
            """,
            symbol_filter_params,
        ).fetchall()
    else:
        target = as_of_date
        if target is None:
            v = conn.execute("SELECT MAX(date) AS d FROM daily_features").fetchone()
            target = v["d"] if v else None
        score_date = target
        if not target:
            return 0
        rows = conn.execute(
            f"""
            SELECT symbol,date,ret_1d,ret_5d,momentum_20d,range_pct,volume_z20
            FROM daily_features
            WHERE date = ? {symbol_filter_sql}
            ORDER BY symbol
            """,
            [target, *symbol_filter_params],
        ).fetchall()

    by_date: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in rows:
        s = score_formula(r["ret_1d"], r["ret_5d"], r["momentum_20d"], r["range_pct"], r["volume_z20"])
        by_date[r["date"]].append((r["symbol"], s))

    out_rows: list[tuple] = []
    for date, vals in by_date.items():
        for symbol, score, rank in _rank_desc(vals):
            out_rows.append((symbol, date, score, rank))

    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO daily_scores(symbol,date,score,rank)
        VALUES(?,?,?,?)
        ON CONFLICT(symbol,date) DO UPDATE SET
            score=excluded.score,
            rank=excluded.rank
        """,
        out_rows,
    )

    if allowed_symbols is not None:
        if include_history:
            conn.execute(
                f"DELETE FROM daily_scores WHERE symbol NOT IN ({','.join('?' for _ in allowed_symbols)})",
                allowed_symbols,
            )
        else:
            if score_date:
                conn.execute(
                    f"DELETE FROM daily_scores WHERE date=? AND symbol NOT IN ({','.join('?' for _ in allowed_symbols)})",
                    [score_date, *allowed_symbols],
                )
    conn.commit()
    return conn.total_changes - before
