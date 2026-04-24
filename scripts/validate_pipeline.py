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
from pipeline.validator import validate_pipeline


def main() -> None:
    p = argparse.ArgumentParser(description="Validate full SQLite pipeline")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--top-n", type=int, default=3)
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)
    result = validate_pipeline(conn, top_n=args.top_n)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
