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
from pipeline.ingest import ingest_daily_prices_csv
from pipeline.features import generate_daily_features
from pipeline.scoring import generate_daily_scores
from pipeline.backtest import run_backtest
from pipeline.paper_trading import run_paper_trading_cycle


def main() -> None:
    p = argparse.ArgumentParser(description="Run full SQLite market pipeline")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--prices-csv", required=True)
    p.add_argument("--top-n", type=int, default=3)
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)

    ing = ingest_daily_prices_csv(conn, args.prices_csv)
    feat = generate_daily_features(conn)
    score = generate_daily_scores(conn, include_history=True)
    run_id = run_backtest(conn, top_n=args.top_n)
    paper = run_paper_trading_cycle(conn, target_positions=args.top_n)

    summary = {
        "db": args.db,
        "ingest_changes": ing,
        "feature_changes": feat,
        "score_changes": score,
        "backtest_run_id": run_id,
        "paper": paper,
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
