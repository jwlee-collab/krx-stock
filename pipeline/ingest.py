from __future__ import annotations

import csv
import sqlite3
from pathlib import Path


def ingest_daily_prices_csv(conn: sqlite3.Connection, csv_path: str | Path) -> int:
    """Ingest OHLCV rows from CSV into daily_prices.

    Expected header: symbol,date,open,high,low,close,volume
    """
    inserted = 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"symbol", "date", "open", "high", "low", "close", "volume"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")

        rows = [
            (
                r["symbol"].strip().upper(),
                r["date"].strip(),
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]),
            )
            for r in reader
        ]

    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO daily_prices(symbol,date,open,high,low,close,volume)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(symbol,date) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            volume=excluded.volume
        """,
        rows,
    )
    conn.commit()
    inserted = conn.total_changes - before
    return inserted
