#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_connection


OHLCV_FIELDS = ["open", "high", "low", "close", "volume", "traded_value"]
DEFAULT_TIMEFRAMES = ["5m", "15m"]


@dataclass(frozen=True)
class IntradayCsvRow:
    symbol: str
    timeframe: str
    timestamp: str
    date: str
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    traded_value: float | None
    source: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.symbol, self.timeframe, self.timestamp)


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _normalize_symbol(raw: str | None) -> str:
    value = (raw or "").strip()
    if not value:
        raise ValueError("missing symbol")
    return value.zfill(6) if value.isdigit() else value.upper()


def _parse_timestamp(raw: str | None) -> tuple[str, str, str]:
    value = (raw or "").strip()
    if not value:
        raise ValueError("missing timestamp")
    ts = datetime.fromisoformat(value)
    return ts.isoformat(), ts.date().isoformat(), ts.time().isoformat()


def _parse_float(row: dict[str, str], key: str, *, required: bool = True) -> float | None:
    raw = (row.get(key) or "").strip()
    if raw == "":
        if required:
            raise ValueError(f"missing {key}")
        return None
    return float(raw.replace(",", ""))


def _load_incoming_csv(
    csv_path: Path,
    *,
    audit_date: str,
    symbols: set[str] | None,
    timeframes: set[str],
) -> tuple[dict[tuple[str, str, str], IntradayCsvRow], list[dict[str, Any]], Counter[str]]:
    rows: dict[tuple[str, str, str], IntradayCsvRow] = {}
    failed: list[dict[str, Any]] = []
    source_summary: Counter[str] = Counter()
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"symbol", "timestamp", "open", "high", "low", "close", "volume"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")
        for line_no, raw in enumerate(reader, start=2):
            try:
                symbol = _normalize_symbol(raw.get("symbol"))
                timeframe = (raw.get("timeframe") or raw.get("interval") or "").strip() or "5m"
                timestamp, row_date, row_time = _parse_timestamp(raw.get("timestamp"))
                if row_date != audit_date:
                    continue
                if symbols is not None and symbol not in symbols:
                    continue
                if timeframe not in timeframes:
                    continue
                traded_value = _parse_float(raw, "traded_value", required=False)
                if traded_value is None:
                    traded_value = _parse_float(raw, "amount", required=False)
                row = IntradayCsvRow(
                    symbol=symbol,
                    timeframe=timeframe,
                    timestamp=timestamp,
                    date=row_date,
                    time=row_time,
                    open=float(_parse_float(raw, "open")),
                    high=float(_parse_float(raw, "high")),
                    low=float(_parse_float(raw, "low")),
                    close=float(_parse_float(raw, "close")),
                    volume=float(_parse_float(raw, "volume")),
                    traded_value=traded_value,
                    source=(raw.get("source") or "CSV").strip() or "CSV",
                )
                rows[row.key] = row
                source_summary[row.source] += 1
            except Exception as exc:
                failed.append({"line": line_no, "error": str(exc), "row": dict(raw)})
    return rows, failed, source_summary


def _db_rows(
    conn: sqlite3.Connection,
    *,
    audit_date: str,
    symbols: set[str],
    timeframes: set[str],
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], Counter[str]]:
    if not symbols:
        return {}, Counter()
    placeholders_symbols = ",".join("?" for _ in symbols)
    placeholders_timeframes = ",".join("?" for _ in timeframes)
    params: list[Any] = [audit_date, *sorted(timeframes), *sorted(symbols)]
    rows = conn.execute(
        f"""
        SELECT symbol,timeframe,timestamp,date,time,open,high,low,close,volume,amount,source,created_at
        FROM intraday_prices
        WHERE COALESCE(date, substr(timestamp,1,10))=?
          AND timeframe IN ({placeholders_timeframes})
          AND symbol IN ({placeholders_symbols})
        ORDER BY symbol,timeframe,timestamp
        """,
        params,
    ).fetchall()
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    source_summary: Counter[str] = Counter()
    for row in rows:
        item = dict(row)
        key = (str(item["symbol"]), str(item["timeframe"]), str(item["timestamp"]))
        out[key] = item
        source_summary[str(item.get("source") or "UNKNOWN")] += 1
    return out, source_summary


def _write_backup_csv(path: Path, rows: dict[tuple[str, str, str], dict[str, Any]]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "timestamp",
        "timeframe",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "traded_value",
        "source",
        "date",
        "time",
        "created_at",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted(rows.values(), key=lambda r: (str(r["symbol"]), str(r["timeframe"]), str(r["timestamp"]))):
            writer.writerow(
                {
                    "symbol": row["symbol"],
                    "timestamp": row["timestamp"],
                    "timeframe": row["timeframe"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "traded_value": row["amount"],
                    "source": row["source"],
                    "date": row["date"],
                    "time": row["time"],
                    "created_at": row["created_at"],
                }
            )
    return {"path": str(path), "row_count": len(rows), "created": True}


def _float_equal(a: Any, b: Any, tolerance: float = 1e-9) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tolerance


def _compare(
    existing: dict[tuple[str, str, str], dict[str, Any]],
    incoming: dict[tuple[str, str, str], IntradayCsvRow],
    *,
    max_samples: int,
) -> dict[str, Any]:
    existing_keys = set(existing)
    incoming_keys = set(incoming)
    matched = sorted(existing_keys & incoming_keys)
    conflict_by_symbol: Counter[str] = Counter()
    conflict_by_timeframe: Counter[str] = Counter()
    conflict_by_field: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    for key in matched:
        old = existing[key]
        new = incoming[key]
        field_diffs: dict[str, dict[str, Any]] = {}
        comparisons = {
            "open": (old["open"], new.open),
            "high": (old["high"], new.high),
            "low": (old["low"], new.low),
            "close": (old["close"], new.close),
            "volume": (old["volume"], new.volume),
            "traded_value": (old["amount"], new.traded_value),
        }
        for field, (old_value, new_value) in comparisons.items():
            if not _float_equal(old_value, new_value):
                field_diffs[field] = {"existing": old_value, "incoming": new_value}
                conflict_by_field[field] += 1
        if field_diffs:
            symbol, timeframe, timestamp = key
            conflict_by_symbol[symbol] += 1
            conflict_by_timeframe[timeframe] += 1
            if len(samples) < max_samples:
                samples.append(
                    {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "timestamp": timestamp,
                        "field_diffs": field_diffs,
                        "existing_source": old.get("source"),
                        "incoming_source": new.source,
                    }
                )
    missing_in_db = sorted(incoming_keys - existing_keys)
    missing_in_incoming = sorted(existing_keys - incoming_keys)
    return {
        "compared_row_count": len(matched),
        "matched_key_count": len(matched),
        "missing_in_db_count": len(missing_in_db),
        "missing_in_incoming_count": len(missing_in_incoming),
        "missing_in_db_sample": [{"symbol": a, "timeframe": b, "timestamp": c} for a, b, c in missing_in_db[:max_samples]],
        "missing_in_incoming_sample": [{"symbol": a, "timeframe": b, "timestamp": c} for a, b, c in missing_in_incoming[:max_samples]],
        "conflict_row_count": sum(conflict_by_symbol.values()),
        "conflict_symbol_count": len(conflict_by_symbol),
        "conflict_timeframe_count": len(conflict_by_timeframe),
        "conflict_by_symbol": dict(conflict_by_symbol),
        "conflict_by_timeframe": dict(conflict_by_timeframe),
        "conflict_by_field": dict(conflict_by_field),
        "sample_conflicts": samples,
    }


def _timestamp_range(rows: dict[tuple[str, str, str], Any]) -> dict[str, Any]:
    timestamps = sorted(key[2] for key in rows)
    return {"first_timestamp": timestamps[0] if timestamps else None, "last_timestamp": timestamps[-1] if timestamps else None}


def _incoming_session_summary(incoming: dict[tuple[str, str, str], IntradayCsvRow]) -> dict[str, Any]:
    counts: dict[str, Counter[str]] = {}
    timestamps: dict[str, dict[str, list[str]]] = {}
    for row in incoming.values():
        counts.setdefault(row.symbol, Counter())[row.timeframe] += 1
        timestamps.setdefault(row.symbol, {}).setdefault(row.timeframe, []).append(row.timestamp)
    by_symbol: dict[str, Any] = {}
    for symbol in sorted(counts):
        by_tf = {}
        for timeframe, count in sorted(counts[symbol].items()):
            ts = sorted(timestamps[symbol][timeframe])
            by_tf[timeframe] = {"count": count, "first": ts[0] if ts else None, "last": ts[-1] if ts else None}
        by_symbol[symbol] = by_tf
    return {"symbol_count": len(counts), "by_symbol": by_symbol}


def _safety_conditions(
    *,
    incoming: dict[tuple[str, str, str], IntradayCsvRow],
    audit_date: str,
    market_symbol: str,
    backup: dict[str, Any],
    min_symbol_count: int,
    expected_5m_count: int,
    expected_15m_count: int,
) -> dict[str, Any]:
    summary = _incoming_session_summary(incoming)
    symbols = set(summary["by_symbol"])
    condition_details: dict[str, Any] = {}
    condition_details["market_proxy_included"] = market_symbol in symbols
    condition_details["incoming_symbol_count_ok"] = len(symbols) >= min_symbol_count
    condition_details["backup_created"] = bool(backup.get("created")) and Path(str(backup.get("path"))).exists()
    condition_details["scope_is_single_date"] = all(row.date == audit_date for row in incoming.values())
    condition_details["whole_db_delete"] = False
    condition_details["same_day_score_change"] = False
    bad_5m = {
        symbol: frames.get("5m", {}).get("count", 0)
        for symbol, frames in summary["by_symbol"].items()
        if frames.get("5m", {}).get("count", 0) != expected_5m_count
    }
    bad_15m = {
        symbol: frames.get("15m", {}).get("count", 0)
        for symbol, frames in summary["by_symbol"].items()
        if frames.get("15m", {}).get("count", 0) != expected_15m_count
    }
    condition_details["expected_5m_count_ok"] = not bad_5m
    condition_details["expected_15m_count_ok"] = not bad_15m
    condition_details["bad_5m_counts"] = bad_5m
    condition_details["bad_15m_counts"] = bad_15m
    condition_details["incoming_partial_session"] = bool(bad_5m or bad_15m)
    failed = [
        key
        for key, value in condition_details.items()
        if key.endswith("_ok") and not value
    ]
    if not condition_details["market_proxy_included"]:
        failed.append("market_proxy_included")
    if not condition_details["backup_created"]:
        failed.append("backup_created")
    if not condition_details["scope_is_single_date"]:
        failed.append("scope_is_single_date")
    if condition_details["whole_db_delete"]:
        failed.append("whole_db_delete")
    if condition_details["same_day_score_change"]:
        failed.append("same_day_score_change")
    return {
        "force_refresh_safe": not failed,
        "failed_conditions": failed,
        "conditions": condition_details,
        "incoming_session": summary,
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# Intraday OHLCV Conflict Audit ({report['date']})",
        "",
        f"- status: {report['status']}",
        f"- db: {report['db']}",
        f"- csv: {report['csv']}",
        f"- existing_row_count: {report['existing_row_count']}",
        f"- incoming_row_count: {report['incoming_row_count']}",
        f"- compared_row_count: {report['comparison']['compared_row_count']}",
        f"- conflict_row_count: {report['comparison']['conflict_row_count']}",
        f"- missing_in_db_count: {report['comparison']['missing_in_db_count']}",
        f"- missing_in_incoming_count: {report['comparison']['missing_in_incoming_count']}",
        f"- force_refresh_safe: {report['safety']['force_refresh_safe']}",
        f"- failed_conditions: {report['safety']['failed_conditions']}",
        f"- backup_csv: {report['backup'].get('path')}",
        "",
        "## Conflict By Field",
        "",
    ]
    for key, value in sorted(report["comparison"]["conflict_by_field"].items()):
        lines.append(f"- {key}: {value}")
    if not report["comparison"]["conflict_by_field"]:
        lines.append("- none")
    lines.extend(["", "## Conflict By Symbol", ""])
    for key, value in sorted(report["comparison"]["conflict_by_symbol"].items()):
        lines.append(f"- {key}: {value}")
    if not report["comparison"]["conflict_by_symbol"]:
        lines.append("- none")
    lines.extend(["", "## Conflict By Timeframe", ""])
    for key, value in sorted(report["comparison"]["conflict_by_timeframe"].items()):
        lines.append(f"- {key}: {value}")
    if not report["comparison"]["conflict_by_timeframe"]:
        lines.append("- none")
    lines.extend(["", "## Sample Conflicts", ""])
    for sample in report["comparison"]["sample_conflicts"]:
        lines.append(f"- {sample['symbol']} {sample['timeframe']} {sample['timestamp']}: {sorted(sample['field_diffs'])}")
    if not report["comparison"]["sample_conflicts"]:
        lines.append("- none")
    lines.extend(
        [
            "",
            "This report only compares quote data rows. It does not change strategy rules, scores, orders, or account state.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit incoming intraday CSV rows against existing DB OHLCV rows")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--symbols", default=None, help="Optional comma-separated symbols; defaults to incoming CSV symbols")
    parser.add_argument("--timeframes", default="5m,15m")
    parser.add_argument("--market-symbol", default="069500")
    parser.add_argument("--output-md", default=None)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--backup-csv", required=True)
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--expected-5m-count", type=int, default=79)
    parser.add_argument("--expected-15m-count", type=int, default=27)
    parser.add_argument("--min-symbol-count", type=int, default=21)
    args = parser.parse_args()

    db_path = Path(args.db)
    csv_path = Path(args.csv)
    if not db_path.exists():
        print(json.dumps({"status": "blocked", "blocked_reason": "DB_MISSING", "db": str(db_path)}, ensure_ascii=False, indent=2))
        raise SystemExit(2)
    if not csv_path.exists():
        print(json.dumps({"status": "blocked", "blocked_reason": "CSV_MISSING", "csv": str(csv_path)}, ensure_ascii=False, indent=2))
        raise SystemExit(2)

    symbols_arg = _split_csv(args.symbols)
    symbols = {_normalize_symbol(symbol) for symbol in symbols_arg} if symbols_arg else None
    timeframes = set(_split_csv(args.timeframes) or DEFAULT_TIMEFRAMES)
    incoming, failed_rows, incoming_sources = _load_incoming_csv(
        csv_path,
        audit_date=args.date,
        symbols=symbols,
        timeframes=timeframes,
    )
    incoming_symbols = {key[0] for key in incoming}
    scoped_symbols = symbols or incoming_symbols
    conn = get_connection(db_path)
    existing, existing_sources = _db_rows(conn, audit_date=args.date, symbols=scoped_symbols, timeframes=timeframes)
    conn.close()
    backup = _write_backup_csv(Path(args.backup_csv), existing)
    comparison = _compare(existing, incoming, max_samples=args.sample_limit)
    safety = _safety_conditions(
        incoming=incoming,
        audit_date=args.date,
        market_symbol=_normalize_symbol(args.market_symbol),
        backup=backup,
        min_symbol_count=args.min_symbol_count,
        expected_5m_count=args.expected_5m_count,
        expected_15m_count=args.expected_15m_count,
    )
    report = {
        "status": "ok",
        "db": str(db_path),
        "csv": str(csv_path),
        "date": args.date,
        "symbols": sorted(scoped_symbols),
        "timeframes": sorted(timeframes),
        "existing_row_count": len(existing),
        "incoming_row_count": len(incoming),
        "failed_incoming_rows": failed_rows[: args.sample_limit],
        "failed_incoming_row_count": len(failed_rows),
        "existing_source_summary": dict(existing_sources),
        "incoming_source_summary": dict(incoming_sources),
        "existing_timestamp_range": _timestamp_range(existing),
        "incoming_timestamp_range": _timestamp_range(incoming),
        "incoming_session_complete": not safety["conditions"]["incoming_partial_session"],
        "incoming_partial_session": safety["conditions"]["incoming_partial_session"],
        "backup": backup,
        "comparison": comparison,
        "safety": safety,
    }
    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    if args.output_md:
        out = Path(args.output_md)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_markdown(report), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
