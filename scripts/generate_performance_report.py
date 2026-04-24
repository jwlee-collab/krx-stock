#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_connection, init_db
from pipeline.performance_report import generate_performance_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate backtest performance comparison report")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--run-id", default=None, help="Target backtest run_id. If omitted, latest run is used.")
    parser.add_argument("--benchmark", default="KOSPI", choices=["KOSPI", "KOSPI200"], help="Benchmark index preference")
    parser.add_argument("--output-dir", default="data/reports")
    args = parser.parse_args()

    conn = get_connection(args.db)
    init_db(conn)

    result = generate_performance_report(
        conn,
        output_dir=args.output_dir,
        run_id=args.run_id,
        benchmark=args.benchmark,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
