from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

from pipeline.day_trading.models import IntradayBar


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _timeframe_minutes(timeframe: str) -> int | None:
    value = timeframe.lower().strip()
    if value.endswith("m") and value[:-1].isdigit():
        return int(value[:-1])
    return None


def _bar_completed_by(bar: IntradayBar, until: datetime, timeframe: str) -> bool:
    minutes = _timeframe_minutes(timeframe)
    if minutes is None:
        return bar.timestamp <= until
    return bar.timestamp + timedelta(minutes=minutes) <= until


def upsert_intraday_bars(conn: sqlite3.Connection, bars: list[IntradayBar]) -> int:
    created_at = datetime.now(timezone.utc).isoformat()
    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO intraday_prices(symbol,timeframe,timestamp,date,time,open,high,low,close,volume,amount,source,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol,timeframe,timestamp) DO UPDATE SET
            date=excluded.date,
            time=excluded.time,
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            volume=excluded.volume,
            amount=excluded.amount,
            source=excluded.source,
            created_at=excluded.created_at
        """,
        [
            (
                b.symbol,
                b.timeframe,
                b.timestamp.isoformat(),
                b.timestamp.date().isoformat(),
                b.timestamp.time().replace(microsecond=0).isoformat(),
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.amount,
                b.source or "UNKNOWN",
                created_at,
            )
            for b in bars
        ],
    )
    conn.commit()
    return conn.total_changes - before


def load_intraday_bars(
    conn: sqlite3.Connection,
    symbols: list[str],
    trade_date: str,
    timeframes: list[str],
) -> dict[str, dict[str, list[IntradayBar]]]:
    if not symbols:
        return {}
    rows = conn.execute(
        f"""
        SELECT symbol,timeframe,timestamp,open,high,low,close,volume,amount
        FROM intraday_prices
        WHERE symbol IN ({",".join("?" for _ in symbols)})
          AND timeframe IN ({",".join("?" for _ in timeframes)})
          AND substr(timestamp,1,10)=?
        ORDER BY symbol,timeframe,timestamp
        """,
        [*symbols, *timeframes, trade_date],
    ).fetchall()
    out: dict[str, dict[str, list[IntradayBar]]] = {}
    for row in rows:
        out.setdefault(row["symbol"], {}).setdefault(row["timeframe"], []).append(
            IntradayBar(
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                timestamp=_parse_ts(row["timestamp"]),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
                amount=float(row["amount"]) if row["amount"] is not None else None,
            )
        )
    return out


def load_intraday_bars_until(
    conn: sqlite3.Connection,
    symbols: list[str],
    trade_date: str,
    timeframes: list[str],
    until: datetime,
    completed_timeframes: list[str] | None = None,
) -> dict[str, dict[str, list[IntradayBar]]]:
    data = load_intraday_bars(conn, symbols, trade_date, timeframes)
    require_completed = set(completed_timeframes or [])
    out: dict[str, dict[str, list[IntradayBar]]] = {}
    for symbol, by_tf in data.items():
        for timeframe, bars in by_tf.items():
            if timeframe in require_completed:
                filtered = [b for b in bars if _bar_completed_by(b, until, timeframe)]
            else:
                filtered = [b for b in bars if b.timestamp <= until]
            out.setdefault(symbol, {})[timeframe] = filtered
    return out
