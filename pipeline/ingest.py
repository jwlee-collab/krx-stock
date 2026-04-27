from __future__ import annotations

import csv
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path
from urllib.request import Request, urlopen


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


def _validate_markets(markets: list[str] | None) -> list[str]:
    selected = [m.upper() for m in (markets or ["KOSPI", "KOSDAQ"])]
    supported = {"KOSPI", "KOSDAQ"}
    invalid = [m for m in selected if m not in supported]
    if invalid:
        raise ValueError(f"Unsupported market values: {invalid}. Use KOSPI, KOSDAQ, or ALL.")
    return selected


def _kind_market_type(market: str) -> str:
    mapping = {
        "KOSPI": "stockMkt",
        "KOSDAQ": "kosdaqMkt",
    }
    return mapping[market]


def _fetch_kind_symbols(market: str) -> set[str]:
    market_type = _kind_market_type(market)
    url = f"https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType={market_type}"
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=15) as resp:
        body = resp.read()

    decoded = body.decode("euc-kr", errors="replace")
    reader = csv.DictReader(decoded.splitlines())
    symbols: set[str] = set()
    for row in reader:
        code = (row.get("종목코드") or "").strip()
        if code.isdigit():
            symbols.add(normalize_krx_symbol(code))
    return symbols


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
    """Resolve KRX tickers for requested markets.

    Primary: pykrx market ticker API.
    Fallback: public KRX KIND corporation list download (no login).
    """
    target_date = _to_yyyymmdd(as_of_date) if as_of_date else date.today().strftime("%Y%m%d")
    selected = _validate_markets(markets)

    pykrx_stock = None
    pykrx_error = None
    try:
        from pykrx import stock as pykrx_stock
    except ImportError:
        pykrx_error = "pykrx is not installed"
    except Exception as e:  # pragma: no cover - defensive guard for pykrx runtime failures
        pykrx_error = str(e)

    symbols: set[str] = set()
    fallback_errors: list[str] = []
    for market in selected:
        market_symbols: set[str] = set()
        if pykrx_stock is not None:
            try:
                tickers = pykrx_stock.get_market_ticker_list(date=target_date, market=market)
                market_symbols.update(normalize_krx_symbol(s) for s in tickers if s)
            except Exception as e:  # pragma: no cover - network/runtime dependent
                pykrx_error = str(e)

        if not market_symbols:
            try:
                market_symbols = _fetch_kind_symbols(market)
            except Exception as e:  # pragma: no cover - network/runtime dependent
                fallback_errors.append(f"{market}:{e}")
        symbols.update(market_symbols)

    if not symbols:
        pykrx_hint = f" pykrx_error={pykrx_error}" if pykrx_error else ""
        fallback_hint = f" fallback_error={'; '.join(fallback_errors)}" if fallback_errors else ""
        raise RuntimeError(
            f"Failed to resolve market symbols for markets={selected}. "
            "Both pykrx and public KIND fallback returned 0 symbols."
            f"{pykrx_hint}{fallback_hint}"
        )
    return sorted(symbols)


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

    unique_symbols = sorted({normalize_krx_symbol(s) for s in symbols})
    print(f"[ingest] requested_symbols={len(unique_symbols)} start={_to_iso_date(start)} end={_to_iso_date(end)}")

    upsert_rows: list[tuple] = []
    symbols_with_data = 0
    symbols_without_data = 0
    failed_symbols: list[str] = []

    for idx, symbol in enumerate(unique_symbols, start=1):
        df = None
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                df = stock.get_market_ohlcv_by_date(start, end, symbol)
                last_error = None
                break
            except Exception as exc:  # pragma: no cover - network/runtime dependent
                last_error = exc
                if attempt < 3:
                    time.sleep(0.35 * attempt)
        if last_error is not None:
            failed_symbols.append(symbol)
            print(f"[ingest] warning symbol={symbol} idx={idx}/{len(unique_symbols)} error={last_error}")
            continue
        if df is None or df.empty:
            symbols_without_data += 1
            continue

        symbols_with_data += 1

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

    changes = _upsert_price_rows(conn, upsert_rows)
    print(
        f"[ingest] fetched_symbols_with_data={symbols_with_data} "
        f"symbols_without_data={symbols_without_data} failed_symbols={len(failed_symbols)} rows_upserted={changes}"
    )
    if failed_symbols:
        preview = ",".join(failed_symbols[:10])
        suffix = "..." if len(failed_symbols) > 10 else ""
        print(f"[ingest] failed_symbol_preview={preview}{suffix}")
    return changes
