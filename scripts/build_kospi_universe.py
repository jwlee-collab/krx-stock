#!/usr/bin/env python3
"""Build KOSPI-only source universe CSV (target: 500 symbols) using pykrx."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build KOSPI-only source universe csv")
    p.add_argument("--output", default="data/kospi_source_universe_500.csv")
    p.add_argument("--target-size", type=int, default=500)
    p.add_argument("--lookback-days", type=int, default=20)
    p.add_argument("--as-of-date", default=None, help="YYYY-MM-DD (default: yesterday)")
    return p.parse_args()


def _to_yyyymmdd(s: str) -> str:
    return datetime.strptime(s, "%Y-%m-%d").strftime("%Y%m%d")


def _candidate_dates(as_of: date, lookback_days: int) -> list[str]:
    span = max(lookback_days * 4, lookback_days + 10)
    start = as_of - timedelta(days=span)
    out: list[str] = []
    d = start
    while d <= as_of:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def main() -> None:
    args = parse_args()

    try:
        from pykrx import stock
    except Exception as e:  # pragma: no cover - runtime dependency path
        raise SystemExit(
            "pykrx 가 필요합니다. Colab/로컬에서 `pip install pykrx` 후 다시 실행하세요. "
            f"(import error: {e})"
        )

    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else (date.today() - timedelta(days=1))
    as_of_yyyymmdd = as_of.strftime("%Y%m%d")

    tickers = stock.get_market_ticker_list(date=as_of_yyyymmdd, market="KOSPI")
    if not tickers:
        raise SystemExit(f"KOSPI 티커 수집 실패: as_of={as_of.isoformat()}")

    sums: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)

    valid_days = 0
    for d in reversed(_candidate_dates(as_of, args.lookback_days)):
        df = stock.get_market_ohlcv_by_ticker(date=d, market="KOSPI")
        if df is None or df.empty or "거래대금" not in df.columns:
            continue
        valid_days += 1
        for sym, row in df.iterrows():
            value = float(row.get("거래대금", 0.0) or 0.0)
            sums[str(sym).zfill(6)] += value
            counts[str(sym).zfill(6)] += 1
        if valid_days >= args.lookback_days:
            break

    if valid_days == 0:
        raise SystemExit("거래대금 데이터를 가져오지 못했습니다. 네트워크/pykrx 상태를 확인하세요.")

    scored: list[tuple[str, float]] = []
    for sym in tickers:
        s = str(sym).zfill(6)
        avg = sums[s] / counts[s] if counts[s] else 0.0
        scored.append((s, avg))
    scored.sort(key=lambda x: (-x[1], x[0]))
    selected = scored[: min(args.target_size, len(scored))]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    note = f"pykrx_kospi_avg_trading_value_{valid_days}d_asof_{as_of.isoformat()}"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "name", "market", "note"])
        w.writeheader()
        for sym, _ in selected:
            name = stock.get_market_ticker_name(sym) or ""
            w.writerow({"symbol": sym, "name": name, "market": "KOSPI", "note": note})

    print(f"[done] output={out_path}")
    print(f"[done] as_of={as_of.isoformat()} lookback_days={valid_days}")
    print(f"[done] selected={len(selected)} / kospi_listed={len(tickers)} target={args.target_size}")
    if len(selected) < args.target_size:
        print("[warn] target-size보다 상장 종목 수가 적거나 데이터 수집 가능한 종목이 부족합니다.")


if __name__ == "__main__":
    main()
