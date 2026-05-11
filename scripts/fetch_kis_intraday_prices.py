#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.data_loader import load_intraday_prices_csv
from pipeline.day_trading.kis_client import (
    DEFAULT_KIS_TOKEN_CACHE_PATH,
    KIS_INDEX_MINUTE_PATH,
    KIS_STOCK_MINUTE_PATH,
    KisClientError,
    KisEndpointBlocked,
    KisQuotationClient,
    RequiredIntradayRequest,
    aggregate_bars,
    kis_rows_from_payload,
    load_kis_env,
    parse_kis_minute_payload,
    parse_kis_minute_rows_with_diagnostics,
    parse_required_intraday_csv,
    raise_for_kis_response_error,
    write_intraday_bars_csv,
)
from pipeline.day_trading.models import IntradayBar
from pipeline.db import get_connection, init_db


def _redact(text: str, secrets: list[str]) -> str:
    out = text
    for secret in secrets:
        if secret:
            out = out.replace(secret, "***REDACTED***")
    return out


def _unique_candidate_symbols(requests: list[RequiredIntradayRequest]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for req in requests:
        if req.source_type != "CANDIDATE":
            continue
        symbol = req.symbol.zfill(6)
        if symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    return out


def _required_dates_by_symbol(requests: list[RequiredIntradayRequest]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for req in requests:
        symbol = req.symbol.zfill(6) if req.source_type == "CANDIDATE" else req.symbol
        out.setdefault(symbol, set()).add(req.date)
    return out


def _market_proxy_requests(requests: list[RequiredIntradayRequest]) -> list[RequiredIntradayRequest]:
    return [req for req in requests if req.source_type == "MARKET_PROXY"]


def _is_placeholder_market_proxy_symbol(symbol: str | None) -> bool:
    value = (symbol or "").strip().upper()
    return value in {"", "MARKET_PROXY"}


def _remap_symbol(bars: list[IntradayBar], output_symbol: str, source: str | None = None) -> list[IntradayBar]:
    return [
        IntradayBar(
            symbol=output_symbol,
            timestamp=bar.timestamp,
            timeframe=bar.timeframe,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            amount=bar.amount,
            source=source or bar.source,
        )
        for bar in bars
    ]


def _filter_to_required_dates(bars: list[IntradayBar], symbol: str, dates_by_symbol: dict[str, set[str]]) -> list[IntradayBar]:
    required_dates = dates_by_symbol.get(symbol)
    if not required_dates:
        return bars
    return [bar for bar in bars if bar.timestamp.date().isoformat() in required_dates]


def _raw_minute_row_key(row: dict[str, Any]) -> str:
    date = row.get("stck_bsop_date") or row.get("bsop_date") or row.get("date") or ""
    hour = row.get("stck_cntg_hour") or row.get("cntg_hour") or row.get("hour") or row.get("time") or ""
    return f"{date}:{hour}"


def _row_date(row: dict[str, Any]) -> str | None:
    raw_date = row.get("stck_bsop_date") or row.get("bsop_date") or row.get("date")
    if raw_date in (None, ""):
        return None
    value = str(raw_date).replace("-", "").strip()
    if len(value) != 8:
        return None
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def _filter_raw_rows_to_required_dates(rows: list[dict[str, Any]], required_dates: set[str] | None) -> list[dict[str, Any]]:
    if not required_dates:
        return rows
    return [row for row in rows if _row_date(row) in required_dates]


def _earliest_required_page_timestamp(page_raw: list[IntradayBar], required_dates: set[str] | None) -> Any:
    if required_dates:
        required = [bar.timestamp for bar in page_raw if bar.timestamp.date().isoformat() in required_dates]
        if required:
            return min(required)
    return min((bar.timestamp for bar in page_raw), default=None)


def _collect_stock_symbol(
    client: KisQuotationClient,
    symbol: str,
    *,
    timeframe: str,
    confirm_timeframe: str | None,
    input_hour: str,
    session_start: str,
    max_pages: int,
    required_dates_by_symbol: dict[str, set[str]],
) -> tuple[list[IntradayBar], dict[str, Any]]:
    page_hour = input_hour
    raw_rows_by_timestamp: dict[str, dict[str, Any]] = {}
    pages: list[dict[str, Any]] = []
    required_dates = required_dates_by_symbol.get(symbol)
    for _ in range(max_pages):
        payload = client.inquire_stock_minute(symbol, input_hour=page_hour)
        raise_for_kis_response_error(payload)
        page_rows = kis_rows_from_payload(payload)
        page_raw = parse_kis_minute_payload(payload, symbol=symbol, source="KIS_QUOTATION_STOCK_RAW")
        page_required = [
            bar for bar in page_raw if not required_dates or bar.timestamp.date().isoformat() in required_dates
        ]
        new_count = 0
        for row in page_rows:
            key = _raw_minute_row_key(row)
            if key not in raw_rows_by_timestamp:
                raw_rows_by_timestamp[key] = row
                new_count += 1
        pages.append(
            {
                "input_hour": page_hour,
                "raw_bar_count": len(page_raw),
                "required_date_bar_count": len(page_required),
                "new_bar_count": new_count,
                "first_timestamp": page_raw[0].timestamp.isoformat() if page_raw else None,
                "last_timestamp": page_raw[-1].timestamp.isoformat() if page_raw else None,
                "first_required_timestamp": page_required[0].timestamp.isoformat() if page_required else None,
                "last_required_timestamp": page_required[-1].timestamp.isoformat() if page_required else None,
            }
        )
        if not page_raw or new_count == 0:
            break
        earliest = _earliest_required_page_timestamp(page_raw, required_dates)
        if earliest is not None and earliest.strftime("%H%M%S") <= session_start:
            break
        if earliest is None:
            break
        page_hour = (earliest.replace(second=0, microsecond=0) - timedelta(minutes=1)).strftime("%H%M%S")
    raw, parse_diagnostics = parse_kis_minute_rows_with_diagnostics(
        _filter_raw_rows_to_required_dates(list(raw_rows_by_timestamp.values()), required_dates),
        symbol=symbol,
        source="KIS_QUOTATION_STOCK_RAW",
    )
    timeframes = [timeframe]
    if confirm_timeframe and confirm_timeframe not in timeframes:
        timeframes.append(confirm_timeframe)
    filtered: list[IntradayBar] = []
    timeframe_counts: dict[str, dict[str, Any]] = {}
    for output_timeframe in timeframes:
        bars = aggregate_bars(raw, output_timeframe)
        matched = _filter_to_required_dates(bars, symbol, required_dates_by_symbol)
        filtered.extend(matched)
        timeframe_counts[output_timeframe] = {
            "aggregated_bar_count": len(bars),
            "matched_required_date_bar_count": len(matched),
            "returned_dates": sorted({bar.timestamp.date().isoformat() for bar in bars}),
        }
    return filtered, {
        "symbol": symbol,
        "endpoint": KIS_STOCK_MINUTE_PATH,
        "raw_bar_count": len(raw),
        "matched_required_date_bar_count": len(filtered),
        "timeframes": timeframe_counts,
        "returned_dates": sorted({bar.timestamp.date().isoformat() for bar in raw}),
        "required_dates": sorted(required_dates_by_symbol.get(symbol, set())),
        "parse_diagnostics": parse_diagnostics.to_dict(),
        "pages": pages,
    }


def _collect_market_proxy(
    client: KisQuotationClient,
    *,
    output_symbol: str,
    index_code: str,
    timeframe: str,
    confirm_timeframe: str | None,
    input_hour: str,
    session_start: str,
    max_pages: int,
    required_dates_by_symbol: dict[str, set[str]],
) -> tuple[list[IntradayBar], dict[str, Any]]:
    page_hour = input_hour
    raw_rows_by_timestamp: dict[str, dict[str, Any]] = {}
    pages: list[dict[str, Any]] = []
    required_dates = required_dates_by_symbol.get(output_symbol)
    for _ in range(max_pages):
        payload = client.inquire_index_minute(index_code, input_hour=page_hour)
        raise_for_kis_response_error(payload)
        page_rows = kis_rows_from_payload(payload)
        page_raw = parse_kis_minute_payload(payload, symbol=output_symbol, source="KIS_QUOTATION_INDEX_RAW")
        page_required = [
            bar for bar in page_raw if not required_dates or bar.timestamp.date().isoformat() in required_dates
        ]
        new_count = 0
        for row in page_rows:
            key = _raw_minute_row_key(row)
            if key not in raw_rows_by_timestamp:
                raw_rows_by_timestamp[key] = row
                new_count += 1
        pages.append(
            {
                "input_hour": page_hour,
                "raw_bar_count": len(page_raw),
                "required_date_bar_count": len(page_required),
                "new_bar_count": new_count,
                "first_timestamp": page_raw[0].timestamp.isoformat() if page_raw else None,
                "last_timestamp": page_raw[-1].timestamp.isoformat() if page_raw else None,
                "first_required_timestamp": page_required[0].timestamp.isoformat() if page_required else None,
                "last_required_timestamp": page_required[-1].timestamp.isoformat() if page_required else None,
            }
        )
        if not page_raw or new_count == 0:
            break
        earliest = _earliest_required_page_timestamp(page_raw, required_dates)
        if earliest is not None and earliest.strftime("%H%M%S") <= session_start:
            break
        if earliest is None:
            break
        page_hour = (earliest.replace(second=0, microsecond=0) - timedelta(minutes=1)).strftime("%H%M%S")
    raw, parse_diagnostics = parse_kis_minute_rows_with_diagnostics(
        _filter_raw_rows_to_required_dates(list(raw_rows_by_timestamp.values()), required_dates),
        symbol=output_symbol,
        source="KIS_QUOTATION_INDEX_RAW",
    )
    timeframes = [timeframe]
    if confirm_timeframe and confirm_timeframe not in timeframes:
        timeframes.append(confirm_timeframe)
    filtered: list[IntradayBar] = []
    timeframe_counts: dict[str, dict[str, Any]] = {}
    for output_timeframe in timeframes:
        bars = aggregate_bars(raw, output_timeframe)
        matched = _filter_to_required_dates(bars, output_symbol, required_dates_by_symbol)
        filtered.extend(matched)
        timeframe_counts[output_timeframe] = {
            "aggregated_bar_count": len(bars),
            "matched_required_date_bar_count": len(matched),
            "returned_dates": sorted({bar.timestamp.date().isoformat() for bar in bars}),
        }
    return filtered, {
        "symbol": output_symbol,
        "index_code": index_code,
        "endpoint": KIS_INDEX_MINUTE_PATH,
        "raw_bar_count": len(raw),
        "matched_required_date_bar_count": len(filtered),
        "timeframes": timeframe_counts,
        "returned_dates": sorted({bar.timestamp.date().isoformat() for bar in raw}),
        "required_dates": sorted(required_dates_by_symbol.get(output_symbol, set())),
        "parse_diagnostics": parse_diagnostics.to_dict(),
        "pages": pages,
    }


def _load_output_to_db(db_path: Path, csv_path: Path, timeframe: str) -> dict[str, Any]:
    if not db_path.exists():
        return {"status": "blocked", "blocked_reason": "DB_MISSING_FOR_INTRADAY_LOAD", "db_path": str(db_path)}
    conn = get_connection(db_path)
    init_db(conn)
    result = load_intraday_prices_csv(conn, csv_path, default_timeframe=timeframe, source="KIS_QUOTATION", dry_run=False)
    conn.close()
    return {"status": "ok", "db_path": str(db_path), "load": result.__dict__}


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch KIS quote-only minute bars from a DAY required intraday manifest")
    parser.add_argument("--required-intraday-csv", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--db", default=None, help="Optional existing DB path to load generated intraday CSV into")
    parser.add_argument("--market-symbol", default="999999")
    parser.add_argument("--market-index-code", default="0001")
    parser.add_argument("--market-proxy-symbol", default=None, help="ETF/stock symbol to fetch as market proxy when --market-proxy-source ETF")
    parser.add_argument("--market-proxy-source", choices=["INDEX", "ETF"], default="INDEX")
    parser.add_argument("--replace-market-proxy-symbol", action="store_true", help="Allow replacing a concrete MARKET_PROXY manifest symbol with --market-proxy-symbol")
    parser.add_argument("--market-proxy-output-symbol", default=None, help="Optional alias symbol to write for the collected market proxy; defaults to the fetched ETF symbol")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--confirm-timeframe", default="15m", help="Optional derived confirm timeframe to write alongside primary bars")
    parser.add_argument("--no-confirm-timeframe", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.12)
    parser.add_argument("--env", choices=["paper", "real"], default=None)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--input-hour", default="153000")
    parser.add_argument("--session-start", default="090000")
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--force-refresh-token", action="store_true")
    parser.add_argument("--skip-market-proxy", action="store_true")
    args = parser.parse_args()

    env_result = load_kis_env(args.env_file, env_override=args.env)
    if env_result.status != "ok" or env_result.config is None:
        print(json.dumps({"status": "blocked", "blocked_reason": env_result.reason, **env_result.to_dict()}, ensure_ascii=False, indent=2))
        raise SystemExit(2)
    secrets = [env_result.config.app_key, env_result.config.app_secret]

    required_path = Path(args.required_intraday_csv)
    if not required_path.exists():
        print(
            json.dumps(
                {"status": "blocked", "blocked_reason": "REQUIRED_INTRADAY_CSV_NOT_FOUND", "path": str(required_path)},
                ensure_ascii=False,
                indent=2,
            )
        )
        raise SystemExit(2)

    requests = parse_required_intraday_csv(
        required_path,
        timeframe=args.timeframe,
        start_date=args.start_date,
        end_date=args.end_date,
        max_symbols=args.max_symbols,
    )
    candidate_symbols = _unique_candidate_symbols(requests)
    proxy_reqs = _market_proxy_requests(requests)
    proxy_manifest_symbol = proxy_reqs[0].symbol if proxy_reqs else args.market_symbol
    proxy_fetch_symbol = args.market_proxy_symbol or ("069500" if args.market_proxy_source == "ETF" else proxy_manifest_symbol)
    if args.market_proxy_source == "ETF":
        if _is_placeholder_market_proxy_symbol(proxy_manifest_symbol) or args.replace_market_proxy_symbol or proxy_manifest_symbol == proxy_fetch_symbol:
            proxy_output_symbol = args.market_proxy_output_symbol or proxy_fetch_symbol
        else:
            proxy_output_symbol = proxy_manifest_symbol
    else:
        proxy_output_symbol = proxy_manifest_symbol
    required_dates = _required_dates_by_symbol(requests)
    if proxy_reqs:
        proxy_dates = {req.date for req in proxy_reqs}
        required_dates.setdefault(proxy_manifest_symbol, set()).update(proxy_dates)
        required_dates.setdefault(proxy_output_symbol, set()).update(proxy_dates)
        required_dates.setdefault(proxy_fetch_symbol, set()).update(proxy_dates)

    output: dict[str, Any] = {
        "status": "ok",
        "env": env_result.config.safe_summary(),
        "dry_run": args.dry_run,
        "required_intraday_csv": str(required_path),
        "output_csv": str(args.output_csv),
        "timeframe": args.timeframe,
        "confirm_timeframe": None if args.no_confirm_timeframe else args.confirm_timeframe,
        "session_start": args.session_start,
        "input_hour": args.input_hour,
        "max_pages": args.max_pages,
        "candidate_symbol_count": len(candidate_symbols),
        "market_proxy_required": bool(proxy_reqs) and not args.skip_market_proxy,
        "market_proxy_source": args.market_proxy_source,
        "market_proxy_manifest_symbol": proxy_manifest_symbol,
        "market_proxy_fetch_symbol": proxy_fetch_symbol,
        "market_proxy_output_symbol": proxy_output_symbol,
        "collection": {"candidates": [], "market_proxy": None},
        "db_load": {"status": "skipped"},
    }
    if args.dry_run:
        output["status"] = "dry_run"
        output["candidate_symbols"] = candidate_symbols
        output["market_index_code"] = args.market_index_code
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return

    client = KisQuotationClient(
        env_result.config,
        dry_run=False,
        sleep_seconds=args.sleep_seconds,
        token_cache_path=args.token_cache,
    )
    try:
        client.get_access_token(force_refresh=args.force_refresh_token)
        output["token_status"] = "ok"
    except Exception as exc:
        output["status"] = "blocked"
        output["token_status"] = "blocked"
        output["blocked_reason"] = _redact(str(exc), secrets)
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1)

    collected: list[IntradayBar] = []
    blocked_reasons: list[str] = []
    for symbol in candidate_symbols:
        try:
            bars, summary = _collect_stock_symbol(
                client,
                symbol,
                timeframe=args.timeframe,
                confirm_timeframe=None if args.no_confirm_timeframe else args.confirm_timeframe,
                input_hour=args.input_hour,
                session_start=args.session_start,
                max_pages=args.max_pages,
                required_dates_by_symbol=required_dates,
            )
            collected.extend(bars)
            output["collection"]["candidates"].append(summary)
            if not bars:
                blocked_reasons.append(f"NO_INTRADAY_ROWS_FOR_REQUIRED_DATES:{symbol}")
        except (KisClientError, KisEndpointBlocked) as exc:
            output["collection"]["candidates"].append(
                {"symbol": symbol, "status": "blocked", "blocked_reason": _redact(str(exc), secrets)}
            )
            blocked_reasons.append(f"CANDIDATE_COLLECTION_FAILED:{symbol}")

    if proxy_reqs and not args.skip_market_proxy:
        try:
            if args.market_proxy_source == "ETF":
                if proxy_output_symbol == proxy_manifest_symbol and proxy_fetch_symbol != proxy_manifest_symbol and not args.replace_market_proxy_symbol:
                    output["collection"]["market_proxy"] = {
                        "status": "blocked",
                        "blocked_reason": "MARKET_PROXY_SYMBOL_REPLACEMENT_REQUIRES_EXPLICIT_FLAG",
                        "manifest_symbol": proxy_manifest_symbol,
                        "requested_proxy_symbol": proxy_fetch_symbol,
                    }
                    blocked_reasons.append("MARKET_PROXY_SYMBOL_REPLACEMENT_REQUIRES_EXPLICIT_FLAG")
                    proxy_bars = []
                    proxy_summary = {}
                else:
                    proxy_bars, proxy_summary = _collect_stock_symbol(
                        client,
                        proxy_fetch_symbol.zfill(6),
                        timeframe=args.timeframe,
                        confirm_timeframe=None if args.no_confirm_timeframe else args.confirm_timeframe,
                        input_hour=args.input_hour,
                        session_start=args.session_start,
                        max_pages=args.max_pages,
                        required_dates_by_symbol=required_dates,
                    )
                    proxy_bars = _remap_symbol(proxy_bars, proxy_output_symbol, source="KIS_QUOTATION_MARKET_PROXY_ETF")
                    proxy_summary.update(
                        {
                            "source_type": "ETF",
                            "proxy_fetch_symbol": proxy_fetch_symbol.zfill(6),
                            "proxy_output_symbol": proxy_output_symbol,
                            "manifest_symbol": proxy_manifest_symbol,
                        }
                    )
            else:
                proxy_bars, proxy_summary = _collect_market_proxy(
                    client,
                    output_symbol=proxy_output_symbol,
                    index_code=args.market_index_code,
                    timeframe=args.timeframe,
                    confirm_timeframe=None if args.no_confirm_timeframe else args.confirm_timeframe,
                    input_hour=args.input_hour,
                    session_start=args.session_start,
                    max_pages=args.max_pages,
                    required_dates_by_symbol=required_dates,
                )
            collected.extend(proxy_bars)
            if proxy_summary:
                output["collection"]["market_proxy"] = proxy_summary
            if not proxy_bars:
                blocked_reasons.append("NO_MARKET_PROXY_ROWS_FOR_REQUIRED_DATES")
        except (KisClientError, KisEndpointBlocked) as exc:
            output["collection"]["market_proxy"] = {"status": "blocked", "blocked_reason": _redact(str(exc), secrets)}
            blocked_reasons.append("MARKET_PROXY_COLLECTION_FAILED")

    if collected:
        out_path = write_intraday_bars_csv(args.output_csv, collected)
        output["output_csv"] = str(out_path)
        output["written_rows"] = len(collected)
        output["written_dates"] = sorted({bar.timestamp.date().isoformat() for bar in collected})
        if args.db:
            output["db_load"] = _load_output_to_db(Path(args.db), out_path, args.timeframe)
    else:
        output["written_rows"] = 0

    if blocked_reasons:
        output["status"] = "blocked"
        output["blocked_reasons"] = sorted(set(blocked_reasons))
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1)
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
