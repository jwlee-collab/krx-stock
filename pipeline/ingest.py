from __future__ import annotations

import csv
import sqlite3
from datetime import date, datetime
from pathlib import Path


def normalize_krx_symbol(symbol: str) -> str:
    """Normalize stock code to 6-digit KRX format."""
    raw = symbol.strip()
    if not raw:
        raise ValueError("symbol must not be empty")
    if not raw.isdigit():
        raise ValueError(f"KRX symbol must be numeric: {symbol}")
    return raw.zfill(6)


def _to_yyyymmdd(value: str) -> str:
    if "-" in value:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y%m%d")
    datetime.strptime(value, "%Y%m%d")
    return value


def _to_iso_date(value: str) -> str:
    if "-" in value:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def _upsert_price_rows(conn: sqlite3.Connection, rows: list[tuple]) -> int:
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
    return conn.total_changes - before


def ingest_daily_prices_csv(conn: sqlite3.Connection, csv_path: str | Path) -> int:
    """Ingest OHLCV rows from CSV into daily_prices.

    Expected header: symbol,date,open,high,low,close,volume
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"symbol", "date", "open", "high", "low", "close", "volume"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")

        rows = [
            (
                normalize_krx_symbol(r["symbol"]),
                _to_iso_date(r["date"]),
                float(r["open"]),
                float(r["high"]),
                float(r["low"]),
                float(r["close"]),
                float(r["volume"]),
            )
            for r in reader
        ]

    return _upsert_price_rows(conn, rows)


def resolve_krx_symbols(markets: list[str] | None = None, as_of_date: str | None = None) -> list[str]:
    """Resolve KRX tickers for requested markets using pykrx."""
    try:
        from pykrx import stock
    except ImportError as e:
        raise ImportError("pykrx is required for KRX data ingestion. Install with: pip install pykrx") from e

    target_date = _to_yyyymmdd(as_of_date) if as_of_date else date.today().strftime("%Y%m%d")
    selected = markets or ["KOSPI", "KOSDAQ"]

    symbols: set[str] = set()
    for market in selected:
        symbols.update(stock.get_market_ticker_list(date=target_date, market=market))
    return sorted(normalize_krx_symbol(s) for s in symbols)


def ingest_krx_prices(
    conn: sqlite3.Connection,
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> int:
    """Fetch and ingest KRX OHLCV data from pykrx into SQLite."""
    try:
        from pykrx import stock
    except ImportError as e:
        raise ImportError("pykrx is required for KRX data ingestion. Install with: pip install pykrx") from e

    start = _to_yyyymmdd(start_date)
    end = _to_yyyymmdd(end_date)

    upsert_rows: list[tuple] = []
    for symbol in sorted({normalize_krx_symbol(s) for s in symbols}):
        df = stock.get_market_ohlcv_by_date(start, end, symbol)
        if df.empty:
            continue

        for dt, row in df.iterrows():
            iso_date = dt.strftime("%Y-%m-%d")
            upsert_rows.append(
                (
                    symbol,
                    iso_date,
                    float(row["시가"]),
                    float(row["고가"]),
                    float(row["저가"]),
                    float(row["종가"]),
                    float(row["거래량"]),
                )
            )

    return _upsert_price_rows(conn, upsert_rows)
