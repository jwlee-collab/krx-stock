#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_connection, init_db
from pipeline.universe_input import load_symbols_from_universe_csv, parse_symbols_arg


def main() -> None:
    p = argparse.ArgumentParser(description="Inspect dynamic daily_universe table")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--universe-mode", choices=["rolling_liquidity"], default="rolling_liquidity")
    p.add_argument("--universe-size", type=int, default=100)
    p.add_argument("--universe-lookback-days", type=int, default=20)
    p.add_argument("--date", default=None, help="Inspect specific date (YYYY-MM-DD). default latest")
    p.add_argument("--compare-static-universe-file", default=None)
    p.add_argument("--compare-static-symbols", default="")
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)

    base_params = (args.universe_mode, args.universe_size, args.universe_lookback_days)
    summary_row = conn.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            MIN(date) AS min_date,
            MAX(date) AS max_date,
            COUNT(DISTINCT date) AS distinct_dates
        FROM daily_universe
        WHERE universe_mode=? AND universe_size=? AND lookback_days=?
        """,
        base_params,
    ).fetchone()

    date_counts = conn.execute(
        """
        SELECT date, COUNT(*) AS universe_count
        FROM daily_universe
        WHERE universe_mode=? AND universe_size=? AND lookback_days=?
        GROUP BY date
        ORDER BY date
        """,
        base_params,
    ).fetchall()

    inspect_date = args.date
    if not inspect_date:
        max_row = conn.execute(
            """
            SELECT MAX(date) AS d
            FROM daily_universe
            WHERE universe_mode=? AND universe_size=? AND lookback_days=?
            """,
            base_params,
        ).fetchone()
        inspect_date = max_row["d"] if max_row else None

    top10 = []
    if inspect_date:
        top10 = conn.execute(
            """
            SELECT date, symbol, universe_rank, avg_dollar_volume
            FROM daily_universe
            WHERE universe_mode=? AND universe_size=? AND lookback_days=? AND date=?
            ORDER BY universe_rank
            LIMIT 10
            """,
            (*base_params, inspect_date),
        ).fetchall()

    size_violations = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM (
            SELECT date, COUNT(*) AS c
            FROM daily_universe
            WHERE universe_mode=? AND universe_size=? AND lookback_days=?
            GROUP BY date
            HAVING c > ?
        ) t
        """,
        (*base_params, args.universe_size),
    ).fetchone()["n"]

    static_symbols = set(parse_symbols_arg(args.compare_static_symbols))
    if args.compare_static_universe_file:
        static_symbols = set(load_symbols_from_universe_csv(args.compare_static_universe_file))

    rolling_set = set()
    if inspect_date:
        rows = conn.execute(
            """
            SELECT symbol
            FROM daily_universe
            WHERE universe_mode=? AND universe_size=? AND lookback_days=? AND date=?
            """,
            (*base_params, inspect_date),
        ).fetchall()
        rolling_set = {r["symbol"] for r in rows}

    comparison = None
    if static_symbols and rolling_set:
        overlap = rolling_set & static_symbols
        comparison = {
            "inspect_date": inspect_date,
            "rolling_count": len(rolling_set),
            "static_count": len(static_symbols),
            "overlap_count": len(overlap),
            "rolling_only_count": len(rolling_set - static_symbols),
            "static_only_count": len(static_symbols - rolling_set),
            "is_different": rolling_set != static_symbols,
        }

    out = {
        "daily_universe_summary": dict(summary_row) if summary_row else {},
        "universe_count_by_date_head": [dict(r) for r in date_counts[:10]],
        "universe_count_by_date_tail": [dict(r) for r in date_counts[-10:]],
        "inspect_date": inspect_date,
        "top10_for_date": [dict(r) for r in top10],
        "universe_size": args.universe_size,
        "size_violations_over_limit": int(size_violations),
        "rolling_vs_static_comparison": comparison,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
