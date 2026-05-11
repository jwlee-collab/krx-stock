#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.ingest import normalize_krx_symbol


NAVER_DAILY_CHART_URL = "https://fchart.stock.naver.com/sise.nhn"


@dataclass(frozen=True)
class PublicDailyPrice:
    date: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    traded_value: float
    source: str = "NAVER_FCHART_DAY"


def _parse_date(value: str) -> str:
    if "-" in value:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    return datetime.strptime(value, "%Y%m%d").date().isoformat()


def _read_symbols_from_universe(path: Path, max_symbols: int | None = None) -> list[str]:
    symbols: list[str] = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "symbol" not in (reader.fieldnames or []):
            raise ValueError("universe CSV must contain a symbol column")
        for row in reader:
            raw = (row.get("symbol") or "").strip()
            if not raw:
                continue
            symbols.append(normalize_krx_symbol(raw))
            if max_symbols is not None and len(symbols) >= max_symbols:
                break
    return symbols


def _symbols_from_args(symbols: str | None, universe_csv: str | None, max_symbols: int | None) -> list[str]:
    if symbols:
        out = [normalize_krx_symbol(s) for s in symbols.split(",") if s.strip()]
    elif universe_csv:
        out = _read_symbols_from_universe(Path(universe_csv), max_symbols=max_symbols)
    else:
        raise ValueError("provide --symbols or --universe-csv")
    if max_symbols is not None:
        out = out[:max_symbols]
    if not out:
        raise ValueError("no symbols selected")
    return out


def parse_naver_daily_chart_xml(
    xml_text: str,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[PublicDailyPrice]:
    normalized = normalize_krx_symbol(symbol)
    start_iso = _parse_date(start_date) if start_date else None
    end_iso = _parse_date(end_date) if end_date else None
    root = ElementTree.fromstring(xml_text)
    rows: list[PublicDailyPrice] = []
    for item in root.findall(".//item"):
        raw = item.attrib.get("data", "")
        parts = raw.split("|")
        if len(parts) != 6:
            continue
        d, open_px, high_px, low_px, close_px, volume = parts
        if "null" in {p.lower() for p in parts}:
            continue
        iso_date = _parse_date(d)
        if start_iso and iso_date < start_iso:
            continue
        if end_iso and iso_date > end_iso:
            continue
        close = float(close_px)
        vol = float(volume)
        rows.append(
            PublicDailyPrice(
                date=iso_date,
                symbol=normalized,
                open=float(open_px),
                high=float(high_px),
                low=float(low_px),
                close=close,
                volume=vol,
                traded_value=close * vol,
            )
        )
    return rows


def fetch_naver_daily_prices(
    symbol: str,
    count: int,
    start_date: str | None = None,
    end_date: str | None = None,
    timeout: int = 15,
) -> list[PublicDailyPrice]:
    normalized = normalize_krx_symbol(symbol)
    query = urlencode({"symbol": normalized, "timeframe": "day", "count": int(count), "requestType": 0})
    req = Request(f"{NAVER_DAILY_CHART_URL}?{query}", headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("euc-kr", errors="replace")
    return parse_naver_daily_chart_xml(body, normalized, start_date=start_date, end_date=end_date)


def write_daily_prices_csv(path: str | Path, rows: list[PublicDailyPrice]) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["date", "symbol", "open", "high", "low", "close", "volume", "traded_value", "source"],
        )
        writer.writeheader()
        for row in sorted(rows, key=lambda r: (r.date, r.symbol)):
            writer.writerow(row.__dict__)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch public no-secret daily OHLCV CSV for KRX symbols")
    parser.add_argument("--symbols", default=None, help="Comma-separated KRX symbols")
    parser.add_argument("--universe-csv", default=None, help="CSV with symbol column")
    parser.add_argument("--max-symbols", type=int, default=20)
    parser.add_argument("--count", type=int, default=90, help="Number of recent daily bars to request per symbol")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--sleep-sec", type=float, default=0.05)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    symbols = _symbols_from_args(args.symbols, args.universe_csv, args.max_symbols)
    all_rows: list[PublicDailyPrice] = []
    failed: list[dict[str, str]] = []
    for idx, symbol in enumerate(symbols):
        try:
            rows = fetch_naver_daily_prices(
                symbol,
                count=args.count,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            all_rows.extend(rows)
        except Exception as exc:
            failed.append({"symbol": symbol, "error": str(exc)})
        if args.sleep_sec > 0 and idx < len(symbols) - 1:
            time.sleep(args.sleep_sec)

    payload = {
        "status": "ok" if all_rows else "blocked",
        "source": "NAVER_FCHART_DAY",
        "symbols_requested": len(symbols),
        "symbols_with_rows": len({r.symbol for r in all_rows}),
        "row_count": len(all_rows),
        "min_date": min((r.date for r in all_rows), default=None),
        "max_date": max((r.date for r in all_rows), default=None),
        "failed": failed,
        "output": None,
        "dry_run": bool(args.dry_run),
        "notes": [
            "Public no-secret daily quote fetch only; no broker, account, order, credential, or secret path.",
            "Do not interpret downloaded rows as strategy profitability.",
        ],
    }
    if all_rows and not args.dry_run:
        payload["output"] = str(write_daily_prices_csv(args.output, all_rows))
    elif all_rows:
        payload["output"] = str(args.output)

    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    if not all_rows:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
