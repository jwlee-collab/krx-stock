#!/usr/bin/env python3
from __future__ import annotations

import csv
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_connection, init_db
from pipeline.dynamic_universe import build_rolling_liquidity_universe
from pipeline.ingest import ingest_daily_prices_csv


def _write_sample_prices(path: Path, symbols: list[str], dates: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["symbol", "date", "open", "high", "low", "close", "volume"]
        )
        writer.writeheader()
        for s_idx, symbol in enumerate(symbols):
            for d_idx, dt in enumerate(dates):
                base = 1000 + s_idx * 10 + d_idx
                writer.writerow(
                    {
                        "symbol": symbol,
                        "date": dt,
                        "open": base,
                        "high": base + 5,
                        "low": base - 3,
                        "close": base + 1,
                        "volume": 10000 + (s_idx * 100) + d_idx,
                    }
                )


def _fetch_one(conn: sqlite3.Connection, query: str, params: tuple = ()) -> sqlite3.Row:
    row = conn.execute(query, params).fetchone()
    if row is None:
        raise RuntimeError(f"query returned no rows: {query}")
    return row


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "smoke.db"
        csv_path = root / "sample_prices.csv"

        symbols = ["000001", "000002", "000003", "000004", "000005", "000006"]
        dates = ["2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07", "2025-01-08"]
        _write_sample_prices(csv_path, symbols, dates)

        conn = get_connection(str(db_path))
        init_db(conn)
        ingest_daily_prices_csv(conn, csv_path)

        prices_before = _fetch_one(
            conn,
            "SELECT COUNT(DISTINCT symbol) AS c FROM daily_prices",
        )
        if int(prices_before["c"]) != len(symbols):
            raise AssertionError(f"expected {len(symbols)} symbols in daily_prices, got {int(prices_before['c'])}")

        build_rolling_liquidity_universe(conn, universe_size=3, lookback_days=2)

        prices_after = _fetch_one(
            conn,
            "SELECT COUNT(DISTINCT symbol) AS c FROM daily_prices",
        )
        if int(prices_after["c"]) != len(symbols):
            raise AssertionError("daily_prices symbol count changed after daily_universe build")

        universe_per_date = conn.execute(
            """
            SELECT date, COUNT(*) AS c
            FROM daily_universe
            WHERE universe_mode='rolling_liquidity' AND universe_size=3 AND lookback_days=2
            GROUP BY date
            ORDER BY date
            """
        ).fetchall()

        if not universe_per_date:
            raise AssertionError("daily_universe rows were not created")
        for row in universe_per_date:
            if int(row["c"]) != 3:
                raise AssertionError(f"date={row['date']} expected 3 symbols, got {int(row['c'])}")

        print(
            "[smoke] ok "
            f"daily_prices_distinct_symbols={int(prices_after['c'])} "
            f"daily_universe_dates={len(universe_per_date)} "
            "daily_universe_per_date=3"
        )


if __name__ == "__main__":
    main()
