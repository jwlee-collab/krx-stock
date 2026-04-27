#!/usr/bin/env python3
"""Build KOSPI-only source universe CSV with resilient fallback paths."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path


DEFAULT_FALLBACK_CSV = Path("data/kospi_source_universe_500.csv")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build KOSPI-only source universe csv")
    p.add_argument("--output", default="data/kospi_source_universe_500.csv")
    p.add_argument("--target-size", type=int, default=500)
    p.add_argument("--min-size", type=int, default=500)
    p.add_argument("--allow-partial", action="store_true")
    p.add_argument("--lookback-days", type=int, default=20)
    p.add_argument("--as-of-date", default=None, help="YYYY-MM-DD (default: yesterday)")
    p.add_argument("--fallback-csv", default=str(DEFAULT_FALLBACK_CSV))
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


def _finalize_rows(rows: list[dict[str, str]], target_size: int) -> list[dict[str, str]]:
    uniq = {r["symbol"]: r for r in rows if str(r.get("market", "")).upper() == "KOSPI"}
    ordered = sorted(uniq.values(), key=lambda x: x["symbol"])
    return ordered[: min(target_size, len(ordered))]


def main() -> None:
    args = parse_args()
    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else (date.today() - timedelta(days=1))

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

    selected = _finalize_rows(rows, args.target_size)

    if len(selected) < int(args.min_size):
        raise SystemExit(
            f"확보 종목 수({len(selected)})가 --min-size({args.min_size}) 미만이라 실패합니다. "
            f"source={source} target_size={args.target_size}"
        )

    if len(selected) < int(args.target_size) and not args.allow_partial:
        raise SystemExit(
            f"strict mode 실패: selected={len(selected)} < target_size={args.target_size}. "
            "--allow-partial 옵션으로 partial 모드를 사용하세요."
        )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "name", "market", "note"])
        w.writeheader()
        for row in selected:
            w.writerow(row)

    is_partial = len(selected) < int(args.target_size)
    print(f"[done] output={out_path}")
    print(f"[done] source={source}")
    print(f"[done] selected_rows={len(selected)} target_size={args.target_size} min_size={args.min_size}")
    print(f"[done] partial_mode={'on' if args.allow_partial else 'off'} partial_result={'yes' if is_partial else 'no'}")
    if errors:
        print("[warn] fallback logs:")
        for x in errors:
            print(f"  - {x}")


if __name__ == "__main__":
    main()
