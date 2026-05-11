from __future__ import annotations

import csv
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DailyScoreRecord:
    symbol: str
    score_date: str
    score: float
    rank: int


@dataclass(frozen=True)
class DailyScoreLoadResult:
    input_rows: int
    valid_rows: int
    invalid_rows: int
    inserted_or_updated_rows: int
    duplicate_key_rows: int
    computed_rank_rows: int
    score_filled_zero_rows: int
    dry_run: bool
    date_counts: dict[str, int]
    previous_score_date_summary: dict[str, dict[str, Any]]
    warnings: list[str] = field(default_factory=list)
    failed_rows: list[dict[str, Any]] = field(default_factory=list)
    ignored_columns: list[str] = field(default_factory=list)
    missing_daily_prices: list[dict[str, str]] = field(default_factory=list)


def _parse_date(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise ValueError("missing date/score_date")
    return date.fromisoformat(value).isoformat()


def _parse_symbol(raw: str) -> str:
    symbol = raw.strip().zfill(6)
    if not symbol or not symbol.isdigit():
        raise ValueError(f"invalid symbol: {raw}")
    return symbol


def _parse_float(raw: str | None, field_name: str) -> float | None:
    value = (raw or "").strip()
    if value == "":
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {raw}") from exc


def _parse_int(raw: str | None, field_name: str) -> int | None:
    value = (raw or "").strip()
    if value == "":
        return None
    try:
        return int(float(value.replace(",", "")))
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}: {raw}") from exc


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _previous_score_date(conn: sqlite3.Connection, trade_date: str) -> str | None:
    row = conn.execute("SELECT MAX(date) AS score_date FROM daily_scores WHERE date < ?", (trade_date,)).fetchone()
    if not row:
        return None
    value = row["score_date"] if isinstance(row, sqlite3.Row) else row[0]
    return str(value) if value else None


def _same_day_score_exists(conn: sqlite3.Connection, trade_date: str) -> bool:
    row = conn.execute("SELECT 1 FROM daily_scores WHERE date=? LIMIT 1", (trade_date,)).fetchone()
    return row is not None


def build_previous_score_date_summary(
    conn: sqlite3.Connection,
    trade_start_date: str | None = None,
    trade_end_date: str | None = None,
    fallback_score_dates: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    if trade_start_date and trade_end_date:
        trade_dates = _date_range(trade_start_date, trade_end_date)
    else:
        trade_dates = sorted(set(fallback_score_dates or []))
    summary: dict[str, dict[str, Any]] = {}
    for trade_date in trade_dates:
        previous = _previous_score_date(conn, trade_date)
        same_day_exists = _same_day_score_exists(conn, trade_date)
        summary[trade_date] = {
            "trade_date": trade_date,
            "previous_score_date": previous,
            "same_day_score_exists": same_day_exists,
            "same_day_only_forbidden": bool(same_day_exists and not previous),
            "usable_by_default": previous is not None,
        }
    return summary


def _daily_price_key_exists(conn: sqlite3.Connection, symbol: str, score_date: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM daily_prices WHERE symbol=? AND date=? LIMIT 1",
        (symbol, score_date),
    ).fetchone()
    return row is not None


def load_daily_scores_csv(
    conn: sqlite3.Connection,
    csv_path: str | Path,
    dry_run: bool = False,
    trade_start_date: str | None = None,
    trade_end_date: str | None = None,
) -> DailyScoreLoadResult:
    path = Path(csv_path)
    failed_rows: list[dict[str, Any]] = []
    raw_records: list[dict[str, Any]] = []
    input_rows = 0
    computed_rank_rows = 0
    score_filled_zero_rows = 0
    ignored_columns: list[str] = []

    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        has_date = "date" in fieldnames or "score_date" in fieldnames
        if not has_date:
            raise ValueError("CSV missing date or score_date column")
        if "symbol" not in fieldnames:
            raise ValueError("CSV missing symbol column")
        if "rank" not in fieldnames and "score" not in fieldnames:
            raise ValueError("CSV requires rank or score column")
        ignored_columns = sorted(set(fieldnames) - {"date", "score_date", "symbol", "rank", "score"})
        for line_no, row in enumerate(reader, start=2):
            input_rows += 1
            try:
                score_date = _parse_date(row.get("score_date") or row.get("date") or "")
                symbol = _parse_symbol(row.get("symbol") or "")
                rank = _parse_int(row.get("rank"), "rank")
                score = _parse_float(row.get("score"), "score")
                if rank is not None and rank <= 0:
                    raise ValueError("rank must be positive")
                if score is None and rank is None:
                    raise ValueError("rank or score is required")
                raw_records.append({"symbol": symbol, "score_date": score_date, "rank": rank, "score": score})
            except Exception as exc:
                failed_rows.append({"line": line_no, "error": str(exc), "row": dict(row)})

    by_date: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in raw_records:
        by_date[record["score_date"]].append(record)
    for score_date, records in by_date.items():
        used_ranks = {record["rank"] for record in records if record["rank"] is not None}
        next_rank = 1
        for record in sorted(
            [r for r in records if r["rank"] is None],
            key=lambda r: (-(r["score"] or 0.0), r["symbol"]),
        ):
            while next_rank in used_ranks:
                next_rank += 1
            record["rank"] = next_rank
            used_ranks.add(next_rank)
            computed_rank_rows += 1
        for record in records:
            if record["score"] is None:
                record["score"] = 0.0
                score_filled_zero_rows += 1

    deduped: dict[tuple[str, str], DailyScoreRecord] = {}
    duplicate_key_rows = 0
    for record in raw_records:
        key = (record["symbol"], record["score_date"])
        if key in deduped:
            duplicate_key_rows += 1
        deduped[key] = DailyScoreRecord(
            symbol=record["symbol"],
            score_date=record["score_date"],
            score=float(record["score"]),
            rank=int(record["rank"]),
        )

    valid_records = list(deduped.values())
    records_with_daily_prices: list[DailyScoreRecord] = []
    missing_daily_prices: list[dict[str, str]] = []
    for record in valid_records:
        if _daily_price_key_exists(conn, record.symbol, record.score_date):
            records_with_daily_prices.append(record)
        else:
            missing_daily_prices.append({"symbol": record.symbol, "date": record.score_date})
    if missing_daily_prices:
        for item in missing_daily_prices[:20]:
            failed_rows.append(
                {
                    "line": None,
                    "error": "missing daily_prices row required by daily_scores foreign key",
                    "row": item,
                }
            )
        if len(missing_daily_prices) > 20:
            failed_rows.append(
                {
                    "line": None,
                    "error": f"{len(missing_daily_prices) - 20} additional daily_prices keys missing",
                    "row": {},
                }
            )
        valid_records = records_with_daily_prices

    changes = 0
    if valid_records and not dry_run:
        before = conn.total_changes
        conn.executemany(
            """
            INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
            VALUES(?,?,?,?)
            """,
            [(record.symbol, record.score_date, record.score, record.rank) for record in valid_records],
        )
        conn.commit()
        changes = conn.total_changes - before

    loaded_dates = sorted({record.score_date for record in valid_records})
    previous_summary = build_previous_score_date_summary(
        conn,
        trade_start_date=trade_start_date,
        trade_end_date=trade_end_date,
        fallback_score_dates=loaded_dates,
    )
    warnings: list[str] = []
    if score_filled_zero_rows:
        warnings.append("SCORE_MISSING_FILLED_ZERO_FOR_RANK_ONLY_ROWS")
    if ignored_columns:
        warnings.append("EXTRA_COLUMNS_IGNORED_BY_DAILY_SCORES_SCHEMA")
    if any(item.get("same_day_only_forbidden") for item in previous_summary.values()):
        warnings.append("SAME_DAY_SCORE_ONLY_FORBIDDEN_FOR_AT_LEAST_ONE_TRADE_DATE")
    if missing_daily_prices:
        warnings.append("DAILY_PRICES_REQUIRED_BEFORE_DAILY_SCORES_LOAD")

    date_counts = dict(Counter(record.score_date for record in valid_records))
    return DailyScoreLoadResult(
        input_rows=input_rows,
        valid_rows=len(valid_records),
        invalid_rows=len(failed_rows),
        inserted_or_updated_rows=changes,
        duplicate_key_rows=duplicate_key_rows,
        computed_rank_rows=computed_rank_rows,
        score_filled_zero_rows=score_filled_zero_rows,
        dry_run=bool(dry_run),
        date_counts=date_counts,
        previous_score_date_summary=previous_summary,
        warnings=warnings,
        failed_rows=failed_rows,
        ignored_columns=ignored_columns,
        missing_daily_prices=missing_daily_prices,
    )
