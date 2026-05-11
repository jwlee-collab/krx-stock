#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.availability import (
    build_day_data_availability_markdown,
    build_day_data_availability_report,
    write_day_data_availability_report,
)
from pipeline.db import get_connection, init_db


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit DAY intraday data availability and replay readiness")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--market-symbol", default=None)
    parser.add_argument("--max-universe-symbols", type=int, default=20)
    parser.add_argument("--report-md", default=None)
    args = parser.parse_args()

    db_path = Path(args.db)
    db_exists_before_open = db_path.exists()
    conn = get_connection(db_path if db_exists_before_open else ":memory:")
    init_db(conn)
    report = build_day_data_availability_report(
        conn,
        start_date=args.start_date,
        end_date=args.end_date,
        market_proxy_symbol=args.market_symbol,
        max_universe_symbols=args.max_universe_symbols,
    )
    report["db_path"] = str(db_path)
    report["db_exists_before_open"] = db_exists_before_open
    if args.report_md:
        markdown = build_day_data_availability_markdown(report)
        report["report_path"] = str(write_day_data_availability_report(args.report_md, markdown))
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
