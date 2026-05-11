#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.data_quality import validate_intraday_prices
from pipeline.db import get_connection, init_db


def _fail_json(message: str, **extra: object) -> None:
    payload = {"status": "error", "message": message, **extra}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate intraday_prices data quality for DAY strategy")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--market-symbol", default=None)
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    db_path = Path(args.db)
    if not db_path.exists():
        _fail_json(
            "DB does not exist; run scripts/bootstrap_market_db.py and data loaders before validation",
            db_path=str(db_path),
            db_exists_before=False,
        )
    conn = get_connection(db_path)
    init_db(conn)
    report = validate_intraday_prices(
        conn,
        symbols=symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        market_proxy_symbol=args.market_symbol,
    )
    conn.close()
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
