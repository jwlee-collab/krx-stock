#!/usr/bin/env python3
"""Validate static universe CSV for dynamic universe experiments."""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate KRX source universe CSV")
    parser.add_argument("--file", required=True, help="Path to universe csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    path = Path(args.file)
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    has_symbol = "symbol" in fieldnames
    symbols = [str(r.get("symbol", "")).zfill(6) for r in rows] if has_symbol else []
    unique_count = len(set(symbols)) if has_symbol else 0
    duplicate_count = len(symbols) - unique_count if has_symbol else 0
    six_digit_ok = all(re.fullmatch(r"\d{6}", s or "") is not None for s in symbols) if has_symbol else False

    print(f"file={path}")
    print(f"rows={len(rows)}")
    print(f"columns={fieldnames}")
    print(f"has_symbol_column={has_symbol}")
    print(f"unique_symbols={unique_count}")
    print(f"duplicate_symbols={duplicate_count}")
    print(f"all_symbols_are_6_digit={six_digit_ok}")

    if "market" in fieldnames:
        market_counter = Counter((r.get("market") or "").strip().upper() for r in rows)
        print("market_distribution=")
        for k, v in sorted(market_counter.items()):
            print(f"  {k or '<EMPTY>'}: {v}")
    else:
        print("market_distribution=no_market_column")


if __name__ == "__main__":
    main()
