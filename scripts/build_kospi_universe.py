#!/usr/bin/env python3
"""Build KOSPI-only source universe CSV with resilient fallback paths."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any


DEFAULT_FALLBACK_CSV = Path("data/kospi_source_universe_500.csv")
DEFAULT_EXCLUDED_CSV = Path("data/kospi_source_universe_excluded_no_price.csv")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build KOSPI-only source universe csv")
    p.add_argument("--output", default="data/kospi_source_universe_500.csv")
    p.add_argument("--target-size", type=int, default=500)
    p.add_argument("--min-size", type=int, default=500)
    p.add_argument("--allow-partial", action="store_true")
    p.add_argument("--lookback-days", type=int, default=20)
    p.add_argument("--as-of-date", default=None, help="YYYY-MM-DD (default: yesterday)")
    p.add_argument("--fallback-csv", default=str(DEFAULT_FALLBACK_CSV))
    p.add_argument("--validate-price-data", action="store_true", help="Keep only symbols that have OHLCV rows in validation period")
    p.add_argument("--validation-start-date", default=None, help="YYYY-MM-DD (default: as_of_date - 30 days)")
    p.add_argument("--validation-end-date", default=None, help="YYYY-MM-DD (default: as_of_date)")
    p.add_argument("--min-price-rows", type=int, default=1, help="Minimum OHLCV row count in validation period")
    p.add_argument("--excluded-output", default=str(DEFAULT_EXCLUDED_CSV), help="CSV path for symbols excluded by price validation")
    return p.parse_args()


def _candidate_dates(as_of: date, lookback_days: int) -> list[str]:
    span = max(lookback_days * 4, lookback_days + 10)
    start = as_of - timedelta(days=span)
    out: list[str] = []
    d = start
    while d <= as_of:
        out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def _read_fallback_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    out: list[dict[str, str]] = []
    for r in rows:
        sym = str(r.get("symbol", "")).zfill(6)
        if not (len(sym) == 6 and sym.isdigit()):
            continue
        market = str(r.get("market") or "KOSPI").strip().upper() or "KOSPI"
        if market != "KOSPI":
            continue
        out.append({"symbol": sym, "name": str(r.get("name") or ""), "market": "KOSPI", "note": str(r.get("note") or "fallback_csv")})
    uniq: dict[str, dict[str, str]] = {}
    for row in out:
        uniq[row["symbol"]] = row
    return sorted(uniq.values(), key=lambda x: x["symbol"])


def _fetch_with_pykrx(as_of: date, lookback_days: int) -> tuple[list[dict[str, str]], str]:
    from pykrx import stock

    as_of_yyyymmdd = as_of.strftime("%Y%m%d")
    tickers = stock.get_market_ticker_list(date=as_of_yyyymmdd, market="KOSPI")
    if not tickers:
        raise RuntimeError(f"KOSPI ticker list is empty (as_of={as_of.isoformat()})")

    sums: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)

    valid_days = 0
    for d in reversed(_candidate_dates(as_of, lookback_days)):
        df = stock.get_market_ohlcv_by_ticker(date=d, market="KOSPI")
        if df is None or df.empty or "거래대금" not in df.columns:
            continue
        valid_days += 1
        for sym, row in df.iterrows():
            value = float(row.get("거래대금", 0.0) or 0.0)
            s = str(sym).zfill(6)
            sums[s] += value
            counts[s] += 1
        if valid_days >= lookback_days:
            break

    if valid_days == 0:
        raise RuntimeError("거래대금 데이터를 확보하지 못했습니다.")

    scored: list[tuple[str, float]] = []
    for sym in tickers:
        s = str(sym).zfill(6)
        avg = sums[s] / counts[s] if counts[s] else 0.0
        scored.append((s, avg))
    scored.sort(key=lambda x: (-x[1], x[0]))

    note = f"pykrx_kospi_avg_trading_value_{valid_days}d_asof_{as_of.isoformat()}"
    rows = []
    for sym, _ in scored:
        name = stock.get_market_ticker_name(sym) or ""
        rows.append({"symbol": sym, "name": name, "market": "KOSPI", "note": note})
    return rows, "pykrx"


def _fetch_with_fdr() -> tuple[list[dict[str, str]], str]:
    import FinanceDataReader as fdr

    df = fdr.StockListing("KOSPI")
    if df is None or df.empty or "Code" not in df.columns:
        raise RuntimeError("FinanceDataReader StockListing('KOSPI') 결과가 비어 있습니다.")

    rows = []
    for _, r in df.iterrows():
        sym = str(r.get("Code", "")).zfill(6)
        if not (len(sym) == 6 and sym.isdigit()):
            continue
        rows.append(
            {
                "symbol": sym,
                "name": str(r.get("Name") or ""),
                "market": "KOSPI",
                "note": "fdr_stocklisting_kospi",
            }
        )
    uniq = {x["symbol"]: x for x in rows}
    return sorted(uniq.values(), key=lambda x: x["symbol"]), "fdr"


def _finalize_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    uniq = {r["symbol"]: r for r in rows if str(r.get("market", "")).upper() == "KOSPI"}
    return sorted(uniq.values(), key=lambda x: x["symbol"])


def _count_price_rows(symbol: str, start_yyyymmdd: str, end_yyyymmdd: str) -> int:
    from pykrx import stock

    df = stock.get_market_ohlcv_by_date(start_yyyymmdd, end_yyyymmdd, symbol)
    if df is None or df.empty:
        return 0
    return int(len(df))


def _validate_price_data(
    rows: list[dict[str, str]],
    start_date: date,
    end_date: date,
    min_price_rows: int,
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    start_yyyymmdd = start_date.strftime("%Y%m%d")
    end_yyyymmdd = end_date.strftime("%Y%m%d")
    valid_rows: list[dict[str, str]] = []
    invalid_rows: list[dict[str, Any]] = []

    for row in rows:
        symbol = str(row.get("symbol", "")).zfill(6)
        price_rows = _count_price_rows(symbol=symbol, start_yyyymmdd=start_yyyymmdd, end_yyyymmdd=end_yyyymmdd)
        if price_rows >= min_price_rows:
            valid_rows.append(row)
            continue
        invalid_rows.append(
            {
                "symbol": symbol,
                "name": str(row.get("name", "")),
                "market": str(row.get("market", "KOSPI")),
                "note": str(row.get("note", "")),
                "price_rows": str(price_rows),
                "validation_start_date": start_date.isoformat(),
                "validation_end_date": end_date.isoformat(),
            }
        )
    return valid_rows, invalid_rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> None:
    args = parse_args()
    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else (date.today() - timedelta(days=1))
    validation_start_date = (
        datetime.strptime(args.validation_start_date, "%Y-%m-%d").date()
        if args.validation_start_date
        else (as_of - timedelta(days=30))
    )
    validation_end_date = datetime.strptime(args.validation_end_date, "%Y-%m-%d").date() if args.validation_end_date else as_of
    if validation_start_date > validation_end_date:
        raise SystemExit(
            f"validation date range is invalid: start={validation_start_date.isoformat()} end={validation_end_date.isoformat()}"
        )
    if int(args.min_price_rows) < 1:
        raise SystemExit(f"--min-price-rows must be >= 1 (got {args.min_price_rows})")

    errors: list[str] = []
    source = ""
    rows: list[dict[str, str]] = []

    try:
        rows, source = _fetch_with_pykrx(as_of=as_of, lookback_days=args.lookback_days)
    except Exception as e:
        errors.append(f"pykrx failed: {e}")

    if not rows:
        fallback_rows = _read_fallback_csv(Path(args.fallback_csv))
        if fallback_rows:
            rows = fallback_rows
            source = f"fallback_csv:{args.fallback_csv}"
        else:
            errors.append(f"fallback csv unavailable or invalid: {args.fallback_csv}")

    if not rows:
        try:
            rows, source = _fetch_with_fdr()
        except Exception as e:
            errors.append(f"FinanceDataReader failed: {e}")

    if not rows:
        raise SystemExit(
            "KOSPI 후보군 생성에 실패했습니다.\n"
            "시도 순서: pykrx -> existing fallback csv -> FinanceDataReader\n"
            + "\n".join(f"- {x}" for x in errors)
        )

    candidates = _finalize_rows(rows)
    selected = candidates[: min(args.target_size, len(candidates))]
    excluded_rows: list[dict[str, Any]] = []

    if args.validate_price_data:
        valid_rows, excluded_rows = _validate_price_data(
            rows=candidates,
            start_date=validation_start_date,
            end_date=validation_end_date,
            min_price_rows=int(args.min_price_rows),
        )
        candidates = valid_rows
        selected = candidates[: min(args.target_size, len(candidates))]
        excluded_path = Path(args.excluded_output)
        _write_csv(
            path=excluded_path,
            fieldnames=["symbol", "name", "market", "note", "price_rows", "validation_start_date", "validation_end_date"],
            rows=excluded_rows,
        )
        print(f"[done] candidate_symbols_before_price_validation={len(valid_rows) + len(excluded_rows)}")
        print(f"[done] symbols_with_price_data={len(valid_rows)}")
        print(f"[done] symbols_without_price_data={len(excluded_rows)}")
        print(f"[done] excluded_output path={excluded_path}")

    if len(candidates) < int(args.min_size):
        raise SystemExit(
            f"확보 종목 수({len(candidates)})가 --min-size({args.min_size}) 미만이라 실패합니다. "
            f"source={source} target_size={args.target_size} validate_price_data={'on' if args.validate_price_data else 'off'}"
        )

    if len(selected) < int(args.target_size) and not args.allow_partial:
        raise SystemExit(
            f"strict mode 실패: selected={len(selected)} < target_size={args.target_size}. "
            "--allow-partial 옵션으로 partial 모드를 사용하세요."
        )

    out_path = Path(args.output)
    _write_csv(path=out_path, fieldnames=["symbol", "name", "market", "note"], rows=selected)

    is_partial = len(selected) < int(args.target_size)
    print(f"[done] output={out_path}")
    print(f"[done] source={source}")
    print(f"[done] output_rows={len(selected)}")
    print(f"[done] selected_rows={len(selected)} target_size={args.target_size} min_size={args.min_size}")
    print(f"[done] partial_mode={'on' if args.allow_partial else 'off'} partial_result={'yes' if is_partial else 'no'}")
    if errors:
        print("[warn] fallback logs:")
        for x in errors:
            print(f"  - {x}")


if __name__ == "__main__":
    main()
