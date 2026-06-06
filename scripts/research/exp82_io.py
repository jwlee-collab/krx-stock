from __future__ import annotations

import csv
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from exp82_types import Cell, Config, OutputPaths, PriceRow, Record

FORWARD_COLS = (
    "forward_1d_return",
    "forward_3d_return",
    "forward_5d_return",
    "forward_10d_return",
    "forward_3d_min_low_return",
    "forward_5d_min_low_return",
    "forward_5d_mdd",
    "next_day_crash_flag",
    "next_5d_crash_flag",
    "next_5d_winner_flag",
    "outcome_status",
)


def read_csv_rows(path: Path) -> list[Record]:
    with path.expanduser().open("r", encoding="utf-8-sig", newline="") as handle:
        return [
            {str(key): "" if value is None else value for key, value in row.items() if key is not None}
            for row in csv.DictReader(handle)
        ]


def write_csv_rows(path: Path, rows: list[Record], preferred_fields: list[str]) -> None:
    fields = fieldnames(rows, preferred_fields)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: stringify(row.get(field)) for field in fields})


def append_csv_rows(path: Path, rows: list[Record], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: stringify(row.get(field)) for field in fields})


def fieldnames(rows: list[Record], preferred_fields: list[str]) -> list[str]:
    seen = set(preferred_fields)
    fields = list(preferred_fields)
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    return fields


def output_paths(out_dir: Path) -> OutputPaths:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return OutputPaths(
        snapshot=out_dir / f"exp82_shadow_label_snapshot_{stamp}.csv",
        snapshot_latest=out_dir / "exp82_shadow_label_latest.csv",
        tracking=out_dir / f"exp82_shadow_forward_tracking_{stamp}.csv",
        tracking_latest=out_dir / "exp82_shadow_forward_tracking_latest.csv",
        summary=out_dir / f"exp82_label_performance_summary_{stamp}.csv",
        summary_latest=out_dir / "exp82_label_performance_summary_latest.csv",
        dashboard=out_dir / f"exp82_dashboard_{stamp}.md",
        dashboard_latest=out_dir / "exp82_dashboard_latest.md",
        metadata=out_dir / f"exp82_metadata_{stamp}.json",
        metadata_latest=out_dir / "exp82_metadata_latest.json",
        ledger=out_dir / "exp82_shadow_label_ledger.csv",
    )


def load_latest_snapshot(cfg: Config) -> tuple[list[Record], str]:
    rows = read_csv_rows(cfg.exp81_latest)
    if not rows:
        return [], cfg.as_of_date or ""
    as_of = cfg.as_of_date or stringify(rows[0].get("as_of_date"))
    return [row for row in rows if stringify(row.get("as_of_date")) == as_of], as_of


def load_history_rows(cfg: Config, candidate_n: int, exclude_date: str) -> list[Record]:
    forward = {row_key(row): row for row in read_csv_rows(cfg.exp80a_forward)}
    rows: list[Record] = []
    for row in read_csv_rows(cfg.exp80a_snapshot):
        if stringify(row.get("analysis_included")) not in {"", "1"}:
            continue
        if stringify(row.get("date")) == exclude_date:
            continue
        if int_value(row.get("candidate_rank")) > candidate_n:
            continue
        merged = dict(row)
        fwd = forward.get(row_key(row))
        if fwd is not None:
            for col in FORWARD_COLS:
                if col in fwd:
                    merged[col] = fwd[col]
        rows.append(merged)
    return rows


def load_exp80d_best_index(path: Path) -> dict[tuple[str, str], Record]:
    best: dict[tuple[str, str], Record] = {}
    for row in read_csv_rows(path):
        if row.get("timing_rule") != "range_cooldown_then_reclaim":
            continue
        key = row_key(row)
        current = best.get(key)
        if current is None or int_value(row.get("top_k")) > int_value(current.get("top_k")):
            best[key] = row
    return best


def load_prices(db: Path, symbols: set[str]) -> dict[str, list[PriceRow]]:
    if not symbols:
        return {}
    uri = f"file:{quote(str(db.expanduser().resolve()), safe='/')}?mode=ro"
    placeholders = ",".join("?" for _ in symbols)
    query = f"SELECT symbol,date,high,low,close FROM daily_prices WHERE symbol IN ({placeholders}) ORDER BY symbol,date"
    rows: dict[str, list[PriceRow]] = {}
    with sqlite3.connect(uri, uri=True) as conn:
        conn.execute("PRAGMA query_only=ON")
        for symbol, date, high, low, close in conn.execute(query, sorted(symbols)):
            rows.setdefault(str(symbol).zfill(6), []).append(PriceRow(str(date), float(high), float(low), float(close)))
    return rows


def db_latest_date(db: Path) -> str:
    uri = f"file:{quote(str(db.expanduser().resolve()), safe='/')}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.execute("PRAGMA query_only=ON")
        row = conn.execute("SELECT MAX(date) FROM daily_scores").fetchone()
    return stringify(row[0])


def latest_report_meta(reports_dir: Path) -> Record:
    paths = sorted(reports_dir.expanduser().glob("*_paper_report_summary.json"))
    if not paths:
        return {"paper_latest_date": "", "paper_status": "missing"}
    path = paths[-1]
    data = json.loads(path.read_text(encoding="utf-8"))
    report_date = re.sub(r"_paper_report_summary\.json$", "", path.name)
    return {"paper_latest_date": report_date, "paper_latest_signal_date": stringify(data.get("latest_signal_date") or report_date), "paper_status": "available"}


def ops_meta_for_date(path: Path, as_of_date: str) -> Record:
    if not path.expanduser().exists():
        return {"ops_ledger_status": "unavailable", "ops_warning": "ops ledger missing"}
    for row in read_csv_rows(path):
        dates = {stringify(row.get("expected_trade_date")), stringify(row.get("run_date")), stringify(row.get("paper_report_date"))}
        if as_of_date not in dates:
            continue
        stale = "STALE" in stringify(row.get("stale_check")).upper()
        invalid = stringify(row.get("invalid_fallback_flag")).lower() == "true"
        warning = "; ".join([name for name, bad in (("stale warning", stale), ("invalid fallback", invalid)) if bad])
        return {"ops_ledger_status": stringify(row.get("status") or "unknown"), "ops_warning": warning or "none"}
    return {"ops_ledger_status": "missing_date", "ops_warning": f"no ops ledger row for {as_of_date}"}


def load_json(path: Path) -> Record:
    if not path.expanduser().exists():
        return {}
    return json.loads(path.expanduser().read_text(encoding="utf-8"))


def row_key(row: Record) -> tuple[str, str]:
    return stringify(row.get("as_of_date") or row.get("date")), stringify(row.get("symbol")).zfill(6)


def int_value(value: Cell) -> int:
    text = stringify(value).strip()
    if text == "":
        return 999999
    try:
        return int(float(text))
    except ValueError:
        return 999999


def stringify(value: Cell) -> str:
    return "" if value is None else str(value)
