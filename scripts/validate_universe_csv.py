#!/usr/bin/env python3
"""Validate static universe CSV for dynamic universe experiments."""

from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path


SYMBOL_RE = re.compile(r"^\d{6}$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate KRX source universe CSV")
    parser.add_argument("--file", required=True, help="Path to universe csv")
    parser.add_argument("--expected-rows", type=int, default=None, help="Optional expected row count")
    parser.add_argument("--require-kospi-only", action="store_true", help="Fail if market is not all KOSPI")
    return parser.parse_args()


def has_utf8_bom(path: Path) -> bool:
    with path.open("rb") as f:
        head = f.read(3)
    return head == b"\xef\xbb\xbf"


def main() -> None:
    args = parse_args()
    path = Path(args.file)

    bom = has_utf8_bom(path)
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    has_symbol = "symbol" in fieldnames
    symbols = [str(r.get("symbol", "")).zfill(6) for r in rows] if has_symbol else []
    unique_count = len(set(symbols)) if has_symbol else 0
    duplicate_count = len(symbols) - unique_count if has_symbol else 0
    six_digit_ok = all(SYMBOL_RE.fullmatch(s or "") is not None for s in symbols) if has_symbol else False

    market_values = [str((r.get("market") or "")).strip().upper() for r in rows]
    market_counter = Counter(market_values) if "market" in fieldnames else Counter()
    market_all_kospi = bool(rows) and all(m == "KOSPI" for m in market_values) if "market" in fieldnames else False

    print(f"file={path}")
    print(f"rows={len(rows)}")
    print(f"columns={fieldnames}")
    print(f"has_symbol_column={has_symbol}")
    print(f"unique_symbols={unique_count}")
    print(f"duplicate_symbols={duplicate_count}")
    print(f"all_symbols_are_6_digit={six_digit_ok}")
    print(f"utf8_bom_present={bom}")

    if "market" in fieldnames:
        print("market_distribution=")
        for k, v in sorted(market_counter.items()):
            print(f"  {k or '<EMPTY>'}: {v}")
        print(f"market_all_kospi={market_all_kospi}")
    else:
        print("market_distribution=no_market_column")
        print("market_all_kospi=False")

    errors: list[str] = []
    if not has_symbol:
        errors.append("missing symbol column")
    if not six_digit_ok:
        errors.append("symbol format check failed (expected 6 digits)")
    if duplicate_count != 0:
        errors.append("duplicate symbols found")
    if bom:
        errors.append("utf-8 BOM detected")
    if args.expected_rows is not None and len(rows) != args.expected_rows:
        errors.append(f"expected_rows={args.expected_rows}, actual_rows={len(rows)}")
    if args.require_kospi_only and not market_all_kospi:
        errors.append("market column is not all KOSPI")

    if errors:
        print("validation=FAIL")
        for e in errors:
            print(f"error={e}")
        raise SystemExit(1)

    print("validation=PASS")


if __name__ == "__main__":
    main()
