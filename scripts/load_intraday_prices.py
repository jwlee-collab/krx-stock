#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.data_loader import load_intraday_prices_csv
from pipeline.day_trading.data_quality import validate_intraday_prices
from pipeline.db import get_connection, init_db


def _fail_json(message: str, **extra: object) -> None:
    payload = {"status": "error", "message": message, **extra}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Load intraday OHLCV CSV rows into intraday_prices")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--csv", required=True, help="CSV with symbol,timestamp,open,high,low,close,volume")
    parser.add_argument("--default-timeframe", default="5m")
    parser.add_argument("--source", default="CSV")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bootstrap-if-missing", action="store_true", help="Explicitly create and initialize the DB if it does not exist")
    parser.add_argument("--validate", action="store_true", help="Run data quality validation after load/dry-run")
    parser.add_argument("--market-symbol", default=None)
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
    result = load_intraday_prices_csv(
        conn,
        args.csv,
        default_timeframe=args.default_timeframe,
        source=args.source,
        dry_run=args.dry_run,
    )
    out = {
        "status": "ok",
        "db_path": str(db_path),
        "db_exists_before": db_exists_before,
        "db_created_by_loader": not db_exists_before and db_path.exists(),
        "load": result.__dict__,
    }
    if args.validate and not args.dry_run:
        out["quality"] = validate_intraday_prices(conn, market_proxy_symbol=args.market_symbol)
    conn.close()
    print(json.dumps(out, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
