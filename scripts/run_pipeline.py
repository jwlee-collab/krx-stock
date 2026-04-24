#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_connection, init_db
from pipeline.ingest import ingest_daily_prices_csv, ingest_krx_prices, resolve_krx_symbols
from pipeline.features import generate_daily_features
from pipeline.scoring import generate_daily_scores
from pipeline.backtest import run_backtest
from pipeline.paper_trading import run_paper_trading_cycle


def _parse_markets(value: str) -> list[str]:
    if value.upper() == "ALL":
        return ["KOSPI", "KOSDAQ"]
    return [x.strip().upper() for x in value.split(",") if x.strip()]


def _parse_symbols(value: str | None) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def main() -> None:
    p = argparse.ArgumentParser(description="Run full SQLite KRX market pipeline")
    p.add_argument("--db", default="data/market_pipeline.db")
    p.add_argument("--source", choices=["csv", "krx"], default="krx")

    # CSV mode
    p.add_argument("--prices-csv", help="CSV path for symbol,date,open,high,low,close,volume")

    # KRX(pykrx) mode
    p.add_argument("--symbols", help="Comma-separated KRX 6-digit symbols (e.g. 005930,000660)")
    p.add_argument("--market", default="ALL", help="KOSPI, KOSDAQ, or ALL")
    p.add_argument("--start-date", default="2025-01-01", help="YYYY-MM-DD")
    p.add_argument("--end-date", default=None, help="YYYY-MM-DD")

    p.add_argument("--top-n", type=int, default=3)
    args = p.parse_args()

    conn = get_connection(args.db)
    init_db(conn)

    if args.source == "csv":
        if not args.prices_csv:
            raise ValueError("--prices-csv is required when --source csv")
        ing = ingest_daily_prices_csv(conn, args.prices_csv)
    else:
        markets = _parse_markets(args.market)
        symbols = _parse_symbols(args.symbols)
        if not symbols:
            symbols = resolve_krx_symbols(markets=markets, as_of_date=args.end_date)
        end_date = args.end_date or date.today().isoformat()
        ing = ingest_krx_prices(conn, symbols=symbols, start_date=args.start_date, end_date=end_date)

    feat = generate_daily_features(conn)
    score = generate_daily_scores(conn, include_history=True)
    run_id = run_backtest(conn, top_n=args.top_n)
    paper = run_paper_trading_cycle(conn, target_positions=args.top_n)

    summary = {
        "db": args.db,
        "source": args.source,
        "ingest_changes": ing,
        "feature_changes": feat,
        "score_changes": score,
        "backtest_run_id": run_id,
        "paper": paper,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
