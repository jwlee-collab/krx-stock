#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading import DayTradingConfig, DayTradingEngine, DayTradeLogger, DayUniverseProvider
from pipeline.day_trading.context import IntradayMarketContext
from pipeline.day_trading.data import load_intraday_bars_until
from pipeline.day_trading.signals import DaySignalGenerator
from pipeline.db import get_connection, init_db


def _fail_json(message: str, **extra: object) -> None:
    payload = {"status": "error", "message": message, **extra}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run DAY intraday strategy in SIGNAL_ONLY or PAPER mode")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--as-of-date", default=None, help="YYYY-MM-DD trade date, defaults to today's date")
    parser.add_argument("--enable-day-trading", action="store_true", help="Required to run; default stays disabled")
    parser.add_argument("--mode", choices=["SIGNAL_ONLY", "PAPER", "LIVE"], default="SIGNAL_ONLY")
    parser.add_argument("--max-universe-symbols", type=int, default=20)
    parser.add_argument("--max-open-positions", type=int, default=2)
    parser.add_argument("--notional-per-trade", type=float, default=1_000_000.0)
    parser.add_argument("--initial-equity", type=float, default=10_000_000.0)
    parser.add_argument("--min-avg-trade-value", type=float, default=50_000_000.0)
    parser.add_argument("--min-latest-trade-value", type=float, default=30_000_000.0)
    parser.add_argument("--market-symbol", default=None, help="Optional intraday market proxy symbol for fail-closed market trend checks")
    parser.add_argument("--allow-same-day-scores", action="store_true", help="Allow same-day pre-market scores; default forbids same-day scores")
    parser.add_argument("--score-date-override", default=None, help="Explicit score_date override, required for reviewed same-day score use")
    parser.add_argument("--allow-live-trading", action="store_true", help="Still requires a separate reviewed live executor")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        _fail_json(
            "DB does not exist; run scripts/bootstrap_market_db.py and data loaders before DAY trading",
            db_path=str(db_path),
            db_exists_before=False,
        )

    conn = get_connection(db_path)
    init_db(conn)
    as_of_date = args.as_of_date or date.today().isoformat()
    current_time = datetime.now()

    config = DayTradingConfig(
        enabled=bool(args.enable_day_trading),
        mode=args.mode,
        max_universe_symbols=args.max_universe_symbols,
        max_open_positions=args.max_open_positions,
        notional_per_trade=args.notional_per_trade,
        initial_equity=args.initial_equity,
        paper_initial_cash_krw=args.initial_equity,
        paper_notional_per_trade_krw=args.notional_per_trade,
        paper_max_position_value_krw=args.notional_per_trade,
        paper_max_open_positions=args.max_open_positions,
        paper_max_total_exposure_krw=args.initial_equity * 0.40,
        min_avg_trade_value=args.min_avg_trade_value,
        min_latest_trade_value=args.min_latest_trade_value,
        market_proxy_symbol=args.market_symbol,
        allow_same_day_scores=bool(args.allow_same_day_scores),
        score_date_override=args.score_date_override,
        allow_live_trading=bool(args.allow_live_trading),
    )
    provider = DayUniverseProvider(conn, config)
    candidates = provider.get_candidates(as_of_date)
    intraday_data = load_intraday_bars_until(
        conn,
        candidates,
        as_of_date,
        [config.timeframe_primary, config.timeframe_confirm],
        current_time,
        completed_timeframes=[config.timeframe_confirm],
    )
    market_bars = None
    if args.market_symbol:
        market_data = load_intraday_bars_until(
            conn,
            [args.market_symbol],
            as_of_date,
            [config.timeframe_primary],
            current_time,
        )
        market_bars = market_data.get(args.market_symbol, {}).get(config.timeframe_primary)
    logger = DayTradeLogger(conn)
    signal_generator = DaySignalGenerator(config, context_builder=IntradayMarketContext(config, conn))
    engine = DayTradingEngine(config, provider, signal_generator=signal_generator, logger=logger)
    summary = engine.run_once(
        as_of_date=as_of_date,
        intraday_data=intraday_data,
        now=current_time,
        market_bars=market_bars,
        equity=config.paper_initial_cash_krw,
        day_start_equity=config.paper_initial_cash_krw,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
