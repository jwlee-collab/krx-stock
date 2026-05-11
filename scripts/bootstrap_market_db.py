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

from pipeline.db import get_connection, init_db


REQUIRED_DAY_TABLES = [
    "daily_prices",
    "intraday_prices",
    "daily_scores",
    "day_trade_logs",
    "day_paper_positions",
    "day_paper_orders",
    "intraday_trade_strength",
    "intraday_investor_flows",
    "intraday_program_flows",
    "intraday_market_context",
]


def _connect_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _table_report(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    existing = {str(row["name"]) for row in rows}
    report: dict[str, dict[str, Any]] = {}
    for table in REQUIRED_DAY_TABLES:
        columns: list[str] = []
        if table in existing:
            columns = [str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        report[table] = {
            "exists": table in existing,
            "columns": columns,
        }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap the SQLite market DB schema for DAY replay validation")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--dry-run", action="store_true", help="Report what would happen without creating or mutating a DB")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_exists_before = db_path.exists()
    report: dict[str, Any] = {
        "db_path": str(db_path),
        "db_exists_before": db_exists_before,
        "dry_run": bool(args.dry_run),
        "created": False,
        "schema_ensured": False,
    }

    if args.dry_run:
        report["would_create_db"] = not db_exists_before
        if db_exists_before:
            conn = _connect_readonly(db_path)
            report["tables"] = _table_report(conn)
            conn.close()
        else:
            report["tables"] = {table: {"exists": False, "columns": []} for table in REQUIRED_DAY_TABLES}
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    init_db(conn)
    conn.commit()
    report["created"] = not db_exists_before and db_path.exists()
    report["schema_ensured"] = True
    report["tables"] = _table_report(conn)
    report["missing_required_tables"] = [
        table for table, table_info in report["tables"].items() if not table_info.get("exists")
    ]
    conn.close()
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
