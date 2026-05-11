#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.kis_client import (
    DEFAULT_KIS_TOKEN_CACHE_PATH,
    KIS_INDEX_MINUTE_PATH,
    KIS_INDEX_MINUTE_TR_ID,
    KIS_STOCK_MINUTE_PATH,
    KIS_STOCK_MINUTE_TR_ID,
    KisClientError,
    KisEndpointBlocked,
    KisQuotationClient,
    aggregate_bars,
    load_kis_env,
    parse_kis_minute_payload,
    raise_for_kis_response_error,
    response_summary,
)


def _redact(text: str, secrets: list[str]) -> str:
    out = text
    for secret in secrets:
        if secret:
            out = out.replace(secret, "***REDACTED***")
    return out


def _bar_summary(bars: list[Any]) -> dict[str, Any]:
    if not bars:
        return {"row_count": 0}
    return {
        "row_count": len(bars),
        "first_timestamp": bars[0].timestamp.isoformat(),
        "last_timestamp": bars[-1].timestamp.isoformat(),
        "dates": sorted({b.timestamp.date().isoformat() for b in bars}),
    }


def _probe_stock(client: KisQuotationClient, symbol: str, input_hour: str, secrets: list[str]) -> dict[str, Any]:
    try:
        payload = client.inquire_stock_minute(symbol, input_hour=input_hour)
        raise_for_kis_response_error(payload)
        raw_bars = parse_kis_minute_payload(payload, symbol=symbol.zfill(6), source="KIS_QUOTATION_STOCK_RAW")
        five_minute_bars = aggregate_bars(raw_bars, "5m")
        return {
            "status": "ok",
            "endpoint": KIS_STOCK_MINUTE_PATH,
            "tr_id": KIS_STOCK_MINUTE_TR_ID,
            "response": response_summary(payload),
            "raw_minute": _bar_summary(raw_bars),
            "five_minute": _bar_summary(five_minute_bars),
            "historical_date_support": "unknown_from_probe; compare returned dates with requested trade dates",
        }
    except (KisClientError, KisEndpointBlocked) as exc:
        return {
            "status": "blocked",
            "endpoint": KIS_STOCK_MINUTE_PATH,
            "tr_id": KIS_STOCK_MINUTE_TR_ID,
            "blocked_reason": _redact(str(exc), secrets),
        }


def _probe_index(client: KisQuotationClient, index_code: str, market_symbol: str, input_hour: str, secrets: list[str]) -> dict[str, Any]:
    try:
        payload = client.inquire_index_minute(index_code, input_hour=input_hour)
        raise_for_kis_response_error(payload)
        raw_bars = parse_kis_minute_payload(payload, symbol=market_symbol, source="KIS_QUOTATION_INDEX_RAW")
        five_minute_bars = aggregate_bars(raw_bars, "5m")
        return {
            "status": "ok",
            "endpoint": KIS_INDEX_MINUTE_PATH,
            "tr_id": KIS_INDEX_MINUTE_TR_ID,
            "index_code": index_code,
            "output_symbol": market_symbol,
            "response": response_summary(payload),
            "raw_minute": _bar_summary(raw_bars),
            "five_minute": _bar_summary(five_minute_bars),
        }
    except (KisClientError, KisEndpointBlocked) as exc:
        return {
            "status": "blocked",
            "endpoint": KIS_INDEX_MINUTE_PATH,
            "tr_id": KIS_INDEX_MINUTE_TR_ID,
            "index_code": index_code,
            "output_symbol": market_symbol,
            "blocked_reason": _redact(str(exc), secrets),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe KIS quote-only intraday quotation endpoints without printing secrets")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--env", choices=["paper", "real"], default=None)
    parser.add_argument("--symbol", default="005930")
    parser.add_argument("--market-symbol", default="999999")
    parser.add_argument("--market-index-code", default="0001")
    parser.add_argument("--input-hour", default="153000")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-market-proxy", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--force-refresh-token", action="store_true")
    args = parser.parse_args()

    env_result = load_kis_env(args.env_file, env_override=args.env)
    if env_result.status != "ok" or env_result.config is None:
        print(json.dumps({"status": "blocked", "blocked_reason": env_result.reason, **env_result.to_dict()}, ensure_ascii=False, indent=2))
        raise SystemExit(2)

    secrets = [env_result.config.app_key, env_result.config.app_secret]
    client = KisQuotationClient(
        env_result.config,
        dry_run=args.dry_run,
        sleep_seconds=args.sleep_seconds,
        token_cache_path=args.token_cache,
    )
    output: dict[str, Any] = {
        "status": "ok",
        "env": env_result.config.safe_summary(),
        "dry_run": args.dry_run,
        "token_status": "not_requested" if args.dry_run else "pending",
        "stock_probe": None,
        "market_proxy_probe": None,
    }
    if args.dry_run:
        output["token_status"] = "dry_run"
        output["stock_probe"] = {
            "status": "dry_run",
            "endpoint": KIS_STOCK_MINUTE_PATH,
            "tr_id": KIS_STOCK_MINUTE_TR_ID,
            "symbol": args.symbol.zfill(6),
        }
        if not args.skip_market_proxy:
            output["market_proxy_probe"] = {
                "status": "dry_run",
                "endpoint": KIS_INDEX_MINUTE_PATH,
                "tr_id": KIS_INDEX_MINUTE_TR_ID,
                "index_code": args.market_index_code,
                "output_symbol": args.market_symbol,
            }
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return

    try:
        client.get_access_token(force_refresh=args.force_refresh_token)
        output["token_status"] = "ok"
    except Exception as exc:
        output["status"] = "blocked"
        output["token_status"] = "blocked"
        output["blocked_reason"] = _redact(str(exc), secrets)
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1)

    output["stock_probe"] = _probe_stock(client, args.symbol, args.input_hour, secrets)
    if not args.skip_market_proxy:
        output["market_proxy_probe"] = _probe_index(client, args.market_index_code, args.market_symbol, args.input_hour, secrets)
    if output["stock_probe"]["status"] != "ok":
        output["status"] = "blocked"
        output["blocked_reason"] = "STOCK_INTRADAY_PROBE_FAILED"
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1)
    if output["market_proxy_probe"] and output["market_proxy_probe"]["status"] != "ok":
        output["status"] = "partial"
        output["blocked_reason"] = "MARKET_PROXY_INTRADAY_PROBE_FAILED"
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
