#!/usr/bin/env python3
"""Validate whether symbols in a universe CSV have real OHLCV rows."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate universe CSV against pykrx OHLCV availability")
    parser.add_argument("--file", required=True, help="Input universe CSV path")
    parser.add_argument("--start-date", required=True, help="Validation start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Validation end date (YYYY-MM-DD)")
    parser.add_argument("--min-price-rows", type=int, default=1, help="Minimum OHLCV row count to classify as valid")
    parser.add_argument("--output-valid", required=True, help="Output CSV path for symbols with price data")
    parser.add_argument("--output-invalid", required=True, help="Output CSV path for symbols without enough price data")
    return parser.parse_args()


def _normalize_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    normalized: dict[str, dict[str, str]] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).zfill(6)
        if not (len(symbol) == 6 and symbol.isdigit()):
            continue
        normalized[symbol] = {
            "symbol": symbol,
            "name": str(row.get("name") or ""),
            "market": str(row.get("market") or ""),
            "note": str(row.get("note") or ""),
        }
    return sorted(normalized.values(), key=lambda x: x["symbol"])


def _count_rows(symbol: str, start_yyyymmdd: str, end_yyyymmdd: str) -> int:
    from pykrx import stock

    df = stock.get_market_ohlcv_by_date(start_yyyymmdd, end_yyyymmdd, symbol)
    if df is None or df.empty:
        return 0
    return int(len(df))


def _write(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> None:
    args = parse_args()
    start_yyyymmdd = args.start_date.replace("-", "")
    end_yyyymmdd = args.end_date.replace("-", "")

    if int(args.min_price_rows) < 1:
        raise SystemExit(f"--min-price-rows must be >= 1 (got {args.min_price_rows})")

    source_rows = _normalize_rows(Path(args.file))
    valid_rows: list[dict[str, str]] = []
    invalid_rows: list[dict[str, str]] = []

    for row in source_rows:
        symbol = row["symbol"]
        price_rows = _count_rows(symbol=symbol, start_yyyymmdd=start_yyyymmdd, end_yyyymmdd=end_yyyymmdd)
        merged = {
            **row,
            "price_rows": str(price_rows),
            "validation_start_date": args.start_date,
            "validation_end_date": args.end_date,
            "min_price_rows": str(args.min_price_rows),
        }
        if price_rows >= int(args.min_price_rows):
            valid_rows.append(merged)
        else:
            invalid_rows.append(merged)

    _write(
        path=Path(args.output_valid),
        fieldnames=["symbol", "name", "market", "note", "price_rows", "validation_start_date", "validation_end_date", "min_price_rows"],
        rows=valid_rows,
    )
    _write(
        path=Path(args.output_invalid),
        fieldnames=["symbol", "name", "market", "note", "price_rows", "validation_start_date", "validation_end_date", "min_price_rows"],
        rows=invalid_rows,
    )

    print(f"file={args.file}")
    print(f"start_date={args.start_date}")
    print(f"end_date={args.end_date}")
    print(f"candidate_symbols={len(source_rows)}")
    print(f"valid_symbols={len(valid_rows)}")
    print(f"invalid_symbols={len(invalid_rows)}")
    print(f"output_valid={args.output_valid}")
    print(f"output_invalid={args.output_invalid}")


if __name__ == "__main__":
    main()
