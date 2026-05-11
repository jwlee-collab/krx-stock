#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.dataset_plan import (
    build_day_replay_dataset_plan,
    build_day_replay_dataset_plan_markdown,
    write_day_replay_dataset_plan_report,
    write_missing_data_csv,
    write_required_intraday_csv,
)
from pipeline.db import get_connection


def _fail_json(message: str, **extra: object) -> None:
    payload = {"status": "blocked", "message": message, **extra}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Plan required data for long-range DAY replay validation")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--market-symbol", required=True)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--report-md", default=None)
    parser.add_argument("--required-intraday-csv", default=None)
    parser.add_argument("--missing-csv", default=None)
    parser.add_argument("--allow-same-day-score", action="store_true")
    parser.add_argument("--timeframe", default="5m")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        _fail_json(
            "DB does not exist; run scripts/bootstrap_market_db.py and loaders before planning long-range replay data",
            db_path=str(db_path),
            db_exists_before=False,
        )

    conn = get_connection(db_path)
    plan = build_day_replay_dataset_plan(
        conn,
        start_date=args.start_date,
        end_date=args.end_date,
        market_symbol=args.market_symbol,
        top_n=args.top_n,
        timeframe=args.timeframe,
        allow_same_day_score=args.allow_same_day_score,
    )
    conn.close()
    output = {"status": "ok", "db_path": str(db_path), "plan": plan}
    if args.required_intraday_csv:
        output["required_intraday_csv"] = str(write_required_intraday_csv(args.required_intraday_csv, plan["required_intraday"]))
    if args.missing_csv:
        output["missing_csv"] = str(write_missing_data_csv(args.missing_csv, plan["missing_data"]))
    if args.report_md:
        markdown = build_day_replay_dataset_plan_markdown(plan)
        output["report_path"] = str(write_day_replay_dataset_plan_report(args.report_md, markdown))
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
