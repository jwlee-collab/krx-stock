#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.availability import build_day_data_availability_report


def _readonly_conn(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_count(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None
    if not exists:
        return {"exists": False, "row_count": 0, "date_count": 0, "min_date": None, "max_date": None}
    date_expr = "COALESCE(date, substr(timestamp,1,10))" if table == "intraday_prices" else "date"
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT {date_expr}) AS date_count,
               MIN({date_expr}) AS min_date,
               MAX({date_expr}) AS max_date
        FROM {table}
        """
    ).fetchone()
    return {
        "exists": True,
        "row_count": int(row["row_count"] or 0),
        "date_count": int(row["date_count"] or 0),
        "min_date": row["min_date"],
        "max_date": row["max_date"],
    }


def _list_files(data_dir: Path) -> list[dict[str, Any]]:
    if not data_dir.exists():
        return []
    files = []
    for path in sorted(p for p in data_dir.iterdir() if p.is_file()):
        files.append({"path": str(path), "name": path.name, "suffix": path.suffix.lower(), "size_bytes": path.stat().st_size})
    return files


def _classify_files(files: list[dict[str, Any]]) -> dict[str, list[str]]:
    names = [str(item["path"]) for item in files]
    lower = [(path, path.lower()) for path in names]
    return {
        "universe_csv": [path for path, low in lower if low.endswith(".csv") and "universe" in low],
        "daily_price_csv": [path for path, low in lower if low.endswith(".csv") and ("daily_price" in low or "daily_prices" in low)],
        "daily_score_csv": [path for path, low in lower if low.endswith(".csv") and ("daily_score" in low or "daily_scores" in low)],
        "intraday_csv": [path for path, low in lower if low.endswith(".csv") and "intraday" in low],
        "market_proxy_csv": [path for path, low in lower if low.endswith(".csv") and ("market_proxy" in low or "proxy" in low)],
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Project Data Source Audit",
        "",
        "## Files",
        f"- data_dir: {report.get('data_dir')}",
        f"- data_file_count: {len(report.get('data_files', []))}",
        f"- classified_files: {report.get('classified_files', {})}",
        "",
        "## DB",
        f"- db_path: {report.get('db_path')}",
        f"- db_exists: {report.get('db_exists')}",
        f"- tables: {report.get('tables', {})}",
        "",
        "## DAY Replay Readiness",
        f"- replay_possible_now: {report.get('replay_possible_now')}",
        f"- blocking_reasons: {report.get('blocking_reasons', [])}",
        f"- availability_summary: {report.get('availability_summary', {})}",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit local project data files and DB readiness for DAY replay")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--market-symbol", default=None)
    parser.add_argument("--report-md", default=None)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    db_path = Path(args.db)
    files = _list_files(data_dir)
    classified = _classify_files(files)
    report: dict[str, Any] = {
        "data_dir": str(data_dir),
        "data_files": files,
        "classified_files": classified,
        "db_path": str(db_path),
        "db_exists": db_path.exists(),
        "tables": {},
        "availability_summary": None,
        "replay_possible_now": False,
        "blocking_reasons": [],
    }
    if not db_path.exists():
        report["blocking_reasons"].append("DB_MISSING")
    else:
        conn = _readonly_conn(db_path)
        try:
            for table in ["daily_prices", "daily_features", "daily_scores", "daily_universe", "intraday_prices"]:
                report["tables"][table] = _table_count(conn, table)
            if args.start_date and args.end_date and args.market_symbol:
                availability = build_day_data_availability_report(
                    conn,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    market_proxy_symbol=args.market_symbol,
                )
                report["availability_summary"] = availability.get("summary", {})
                report["replay_possible_now"] = bool(availability.get("replayable_dates"))
                if not report["replay_possible_now"]:
                    report["blocking_reasons"].append("NO_REPLAYABLE_DATES")
            else:
                report["blocking_reasons"].append("DATE_RANGE_OR_MARKET_SYMBOL_NOT_PROVIDED")
        finally:
            conn.close()
    if not classified["universe_csv"]:
        report["blocking_reasons"].append("NO_UNIVERSE_CSV_DISCOVERED")
    if not classified["intraday_csv"]:
        report["blocking_reasons"].append("NO_INTRADAY_CSV_DISCOVERED")
    if args.report_md:
        out = Path(args.report_md)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_markdown(report), encoding="utf-8")
        report["report_path"] = str(out)
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
