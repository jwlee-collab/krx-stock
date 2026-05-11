from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from pipeline.day_trading.data import upsert_intraday_bars
from pipeline.day_trading.models import IntradayBar


def _normalize_intraday_symbol(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        raise ValueError(f"invalid symbol: {raw}")
    if value.isdigit():
        return value.zfill(6)
    normalized = value.upper()
    if not all(ch.isalnum() or ch in {"_", "-", "."} for ch in normalized):
        raise ValueError(f"invalid symbol: {raw}")
    return normalized


@dataclass(frozen=True)
class IntradayLoadResult:
    input_rows: int
    valid_rows: int
    invalid_rows: int
    inserted_or_updated_rows: int
    dry_run: bool
    failed_rows: list[dict] = field(default_factory=list)


def _parse_float(row: dict[str, str], key: str, required: bool = True) -> float | None:
    raw = (row.get(key) or "").strip()
    if raw == "":
        if required:
            raise ValueError(f"missing {key}")
        return None
    return float(raw.replace(",", ""))


def _parse_timestamp(raw: str) -> datetime:
    value = raw.strip()
    if not value:
        raise ValueError("missing timestamp")
    return datetime.fromisoformat(value)


def load_intraday_prices_csv(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    default_timeframe: str = "5m",
    source: str = "CSV",
    dry_run: bool = False,
) -> IntradayLoadResult:
    path = Path(csv_path)
    failed_rows: list[dict] = []
    bars: list[IntradayBar] = []
    input_rows = 0
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"symbol", "timestamp", "open", "high", "low", "close", "volume"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")
        for line_no, row in enumerate(reader, start=2):
            input_rows += 1
            try:
                symbol = _normalize_intraday_symbol(row.get("symbol"))
                ts = _parse_timestamp(row.get("timestamp") or "")
                timeframe = (row.get("timeframe") or row.get("interval") or default_timeframe).strip()
                open_px = _parse_float(row, "open")
                high_px = _parse_float(row, "high")
                low_px = _parse_float(row, "low")
                close_px = _parse_float(row, "close")
                volume = _parse_float(row, "volume")
                amount = _parse_float(row, "traded_value", required=False)
                if amount is None:
                    amount = _parse_float(row, "amount", required=False)
                row_source = (row.get("source") or source).strip() or source
                if min(open_px, high_px, low_px, close_px) <= 0.0:
                    raise ValueError("OHLC must be positive")
                if high_px < max(open_px, close_px) or low_px > min(open_px, close_px):
                    raise ValueError("invalid OHLC range")
                if volume is None or volume < 0.0:
                    raise ValueError("volume must be non-negative")
                if amount is not None and amount < 0.0:
                    raise ValueError("traded_value/amount must be non-negative")
                bars.append(
                    IntradayBar(
                        symbol=symbol,
                        timestamp=ts,
                        timeframe=timeframe,
                        open=float(open_px),
                        high=float(high_px),
                        low=float(low_px),
                        close=float(close_px),
                        volume=float(volume),
                        amount=amount,
                        source=row_source,
                    )
                )
            except Exception as exc:
                failed_rows.append({"line": line_no, "error": str(exc), "row": dict(row)})
    changes = 0 if dry_run else upsert_intraday_bars(conn, bars)
    return IntradayLoadResult(
        input_rows=input_rows,
        valid_rows=len(bars),
        invalid_rows=len(failed_rows),
        inserted_or_updated_rows=changes,
        dry_run=bool(dry_run),
        failed_rows=failed_rows,
    )
