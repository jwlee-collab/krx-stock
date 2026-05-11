from __future__ import annotations

import csv
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline.ingest import normalize_krx_symbol


@dataclass(frozen=True)
class DailyPriceRecord:
    symbol: str
    price_date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class DailyPriceLoadResult:
    input_rows: int
    valid_rows: int
    invalid_rows: int
    inserted_or_updated_rows: int
    duplicate_key_rows: int
    dry_run: bool
    date_counts: dict[str, int]
    symbol_counts: dict[str, int]
    daily_scores_fk_ready_summary: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    failed_rows: list[dict[str, Any]] = field(default_factory=list)
    ignored_columns: list[str] = field(default_factory=list)


def _parse_date(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise ValueError("missing date")
    if "-" in value:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def _parse_float(row: dict[str, str], key: str) -> float:
    raw = (row.get(key) or "").strip()
    if raw == "":
        raise ValueError(f"missing {key}")
    return float(raw.replace(",", ""))


def _validate_ohlcv(record: DailyPriceRecord) -> None:
    if min(record.open, record.high, record.low, record.close) <= 0.0:
        raise ValueError("OHLC must be positive")
    if record.high < max(record.open, record.close):
        raise ValueError("high must be >= max(open, close)")
    if record.low > min(record.open, record.close):
        raise ValueError("low must be <= min(open, close)")
    if record.volume < 0.0:
        raise ValueError("volume must be non-negative")


def load_daily_prices_csv(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    dry_run: bool = False,
) -> DailyPriceLoadResult:
    path = Path(csv_path)
    failed_rows: list[dict[str, Any]] = []
    parsed: list[DailyPriceRecord] = []
    input_rows = 0
    ignored_columns: list[str] = []

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"date", "symbol", "open", "high", "low", "close", "volume"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")
        ignored_columns = sorted(set(reader.fieldnames or []) - required)
        for line_no, row in enumerate(reader, start=2):
            input_rows += 1
            try:
                record = DailyPriceRecord(
                    symbol=normalize_krx_symbol(row.get("symbol") or ""),
                    price_date=_parse_date(row.get("date") or ""),
                    open=_parse_float(row, "open"),
                    high=_parse_float(row, "high"),
                    low=_parse_float(row, "low"),
                    close=_parse_float(row, "close"),
                    volume=_parse_float(row, "volume"),
                )
                _validate_ohlcv(record)
                parsed.append(record)
            except Exception as exc:
                failed_rows.append({"line": line_no, "error": str(exc), "row": dict(row)})

    deduped: dict[tuple[str, str], DailyPriceRecord] = {}
    duplicate_key_rows = 0
    for record in parsed:
        key = (record.symbol, record.price_date)
        if key in deduped:
            duplicate_key_rows += 1
        deduped[key] = record
    valid_records = list(deduped.values())

    changes = 0
    if valid_records and not dry_run:
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
            [
                (
                    record.symbol,
                    record.price_date,
                    record.open,
                    record.high,
                    record.low,
                    record.close,
                    record.volume,
                )
                for record in valid_records
            ],
        )
        conn.commit()
        changes = conn.total_changes - before

    date_counts = dict(Counter(record.price_date for record in valid_records))
    symbol_counts = dict(Counter(record.symbol for record in valid_records))
    warnings: list[str] = []
    if ignored_columns:
        warnings.append("EXTRA_COLUMNS_IGNORED_BY_DAILY_PRICES_SCHEMA")
    if duplicate_key_rows:
        warnings.append("DUPLICATE_DAILY_PRICE_KEYS_LAST_ROW_WINS")
    if failed_rows:
        warnings.append("INVALID_DAILY_PRICE_ROWS_SKIPPED")

    return DailyPriceLoadResult(
        input_rows=input_rows,
        valid_rows=len(valid_records),
        invalid_rows=len(failed_rows),
        inserted_or_updated_rows=changes,
        duplicate_key_rows=duplicate_key_rows,
        dry_run=bool(dry_run),
        date_counts=date_counts,
        symbol_counts=symbol_counts,
        daily_scores_fk_ready_summary={
            "price_key_count": len(valid_records),
            "date_count": len(date_counts),
            "symbol_count": len(symbol_counts),
            "dates": sorted(date_counts),
            "symbols": sorted(symbol_counts),
            "note": "daily_scores rows require matching daily_prices(symbol,date) keys before load",
        },
        warnings=warnings,
        failed_rows=failed_rows,
        ignored_columns=ignored_columns,
    )
