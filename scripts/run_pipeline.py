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
from pipeline.scoring import DEFAULT_SCORING_PROFILE, SUPPORTED_SCORING_PROFILES, generate_daily_scores
from pipeline.universe_filter import UniverseFilterConfig, filter_universe
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
    p.add_argument("--rebalance-frequency", choices=["daily", "weekly"], default="daily")
    p.add_argument("--min-holding-days", type=int, default=5)
    p.add_argument("--keep-rank-threshold", type=int, default=None, help="Keep existing holdings while rank <= this threshold (default: top_n)")
    p.add_argument("--disable-universe-filter", action="store_true", help="Disable pre-scoring universe filter")
    p.add_argument("--min-close-price", type=float, default=3000.0)
    p.add_argument("--min-avg-dollar-volume-20d", type=float, default=1_000_000_000.0)
    p.add_argument("--min-avg-volume-20d", type=float, default=100_000.0)
    p.add_argument("--min-data-days-60d", type=int, default=60)
    p.add_argument("--shock-lookback-days", type=int, default=20)
    p.add_argument("--shock-abs-return-threshold", type=float, default=0.18)
    p.add_argument("--shock-max-hits", type=int, default=1)
    p.add_argument("--scoring-profile", choices=sorted(SUPPORTED_SCORING_PROFILES), default=DEFAULT_SCORING_PROFILE)
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
            if not symbols:
                raise RuntimeError(
                    "시장 유니버스 자동 수집 결과가 0개입니다. "
                    "--market 값을 확인하거나 네트워크/pykrx 설치 상태를 점검하세요."
                )
            print(f"[universe] auto-resolved symbols={len(symbols)} markets={markets}")
        end_date = args.end_date or date.today().isoformat()
        ing = ingest_krx_prices(conn, symbols=symbols, start_date=args.start_date, end_date=end_date)
        if ing == 0:
            raise RuntimeError(
                "가격 데이터 적재 건수가 0입니다. "
                "시장 휴장일 범위이거나 네트워크 응답 문제일 수 있습니다. "
                "입력 값(--start-date, --end-date, --market/--symbols)을 확인하세요."
            )

    feat = generate_daily_features(conn)

    universe_summary = None
    selected_symbols = None
    apply_filter = (not args.disable_universe_filter) and args.source == "krx" and not _parse_symbols(args.symbols)
    if apply_filter:
        cfg = UniverseFilterConfig(
            min_close_price=args.min_close_price,
            min_avg_dollar_volume_20d=args.min_avg_dollar_volume_20d,
            min_avg_volume_20d=args.min_avg_volume_20d,
            min_data_days_60d=args.min_data_days_60d,
            shock_lookback_days=args.shock_lookback_days,
            shock_abs_return_threshold=args.shock_abs_return_threshold,
            shock_max_hits=args.shock_max_hits,
        )
        universe_summary = filter_universe(conn, cfg)
        selected_symbols = universe_summary["selected_symbols"]
        print(
            f"[universe_filter] before={universe_summary['before_count']} after={universe_summary['after_count']} removed={universe_summary['removed_count']}"
        )
        print(f"[universe_filter] removed_by_reason={json.dumps(universe_summary['removed_by_reason'], ensure_ascii=False)}")
        if universe_summary["after_count"] == 0:
            summary = {
                "db": args.db,
                "source": args.source,
                "ingest_changes": ing,
                "feature_changes": feat,
                "universe_filter": universe_summary,
                "status": "skipped",
                "skip_reason": (
                    "유니버스 필터 결과 후보군이 0개여서 scoring/backtest/paper trading을 실행하지 않았습니다. "
                    "필터 임계값 또는 데이터 기간을 완화해 재실행하세요."
                ),
            }
            print(json.dumps(summary, indent=2, ensure_ascii=False))
            return

    score = generate_daily_scores(
        conn,
        include_history=True,
        allowed_symbols=selected_symbols,
        scoring_profile=args.scoring_profile,
    )
    run_id = run_backtest(
        conn,
        top_n=args.top_n,
        rebalance_frequency=args.rebalance_frequency,
        min_holding_days=args.min_holding_days,
        keep_rank_threshold=args.keep_rank_threshold,
        scoring_profile=args.scoring_profile,
    )
    paper = run_paper_trading_cycle(
        conn,
        target_positions=args.top_n,
        rebalance_frequency=args.rebalance_frequency,
        min_holding_days=args.min_holding_days,
        keep_rank_threshold=args.keep_rank_threshold,
    )

    summary = {
        "db": args.db,
        "source": args.source,
        "ingest_changes": ing,
        "feature_changes": feat,
        "universe_filter": universe_summary,
        "score_changes": score,
        "scoring_profile": args.scoring_profile,
        "backtest_run_id": run_id,
        "paper": paper,
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        raise SystemExit(1)
