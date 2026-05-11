#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.daily_prices_loader import load_daily_prices_csv
from pipeline.db import get_connection, init_db


def _fail_json(message: str, **extra: object) -> None:
    payload = {"status": "error", "message": message, **extra}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load daily OHLCV CSV rows into daily_prices")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--csv", required=True, help="CSV with date,symbol,open,high,low,close,volume")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bootstrap-if-missing", action="store_true", help="Explicitly create and initialize the DB if it does not exist")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_exists_before = db_path.exists()
    if not db_exists_before and not args.bootstrap_if_missing:
        _fail_json(
            "DB does not exist; run scripts/bootstrap_market_db.py first or pass --bootstrap-if-missing explicitly",
            db_path=str(db_path),
            db_exists_before=False,
        )
    if not db_exists_before and args.dry_run:
        _fail_json(
            "dry-run will not create a missing DB; run bootstrap first or remove --dry-run with --bootstrap-if-missing",
            db_path=str(db_path),
            db_exists_before=False,
        )

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    init_db(conn)
    result = load_daily_prices_csv(conn, args.csv, dry_run=args.dry_run)
    conn.close()
    payload = {
        "status": "ok",
        "db_path": str(db_path),
        "db_exists_before": db_exists_before,
        "db_created_by_loader": not db_exists_before and db_path.exists(),
        "load": result.__dict__,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
