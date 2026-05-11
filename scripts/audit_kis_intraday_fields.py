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

from pipeline.day_trading.kis_client import (  # noqa: E402
    DEFAULT_KIS_TOKEN_CACHE_PATH,
    KIS_STOCK_MINUTE_PATH,
    KIS_STOCK_MINUTE_TR_ID,
    KisClientError,
    KisEndpointBlocked,
    KisQuotationClient,
    audit_kis_intraday_rows,
    kis_rows_from_payload,
    load_kis_env,
    parse_kis_minute_rows_with_diagnostics,
    raise_for_kis_response_error,
    response_summary,
)


def _redact(text: str, secrets: list[str]) -> str:
    out = text
    for secret in secrets:
        if secret:
            out = out.replace(secret, "***REDACTED***")
    return out


def _write_json(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _markdown_section(title: str, section: dict[str, Any]) -> list[str]:
    audit = section.get("field_audit", {})
    diagnostics = section.get("parse_diagnostics", {})
    return [
        f"## {title}",
        f"- status: {section.get('status')}",
        f"- endpoint: {section.get('endpoint')}",
        f"- tr_id: {section.get('tr_id')}",
        f"- symbol: {section.get('symbol')}",
        f"- response_summary: {section.get('response')}",
        f"- row_count: {audit.get('row_count')}",
        f"- field_names: {audit.get('field_names', [])}",
        f"- field_non_null_count: {audit.get('field_non_null_count', {})}",
        f"- field_numeric_parseable: {audit.get('field_numeric_parseable', {})}",
        f"- volume_candidate_fields: {audit.get('volume_candidate_fields', {})}",
        f"- amount_candidate_fields: {audit.get('amount_candidate_fields', {})}",
        f"- price_candidate_fields: {audit.get('price_candidate_fields', {})}",
        f"- timestamp_candidate_fields: {audit.get('timestamp_candidate_fields', {})}",
        f"- parse_diagnostics: {diagnostics}",
        f"- sample_rows: {audit.get('sample_rows', [])}",
        "",
    ]


def _write_markdown(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    lines = [
        "# KIS Intraday Field Audit",
        "",
        "This report contains sanitized quotation response samples only. App keys, app secrets, and access tokens are never written.",
        "",
        "## Summary",
        f"- status: {payload.get('status')}",
        f"- env: {payload.get('env')}",
        f"- requested_date: {payload.get('requested_date')}",
        "",
    ]
    if payload.get("stock"):
        lines.extend(_markdown_section("Stock Symbol", payload["stock"]))
    if payload.get("market_proxy"):
        lines.extend(_markdown_section("Market Proxy Symbol", payload["market_proxy"]))
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")


def _audit_symbol(client: KisQuotationClient, symbol: str, *, input_hour: str, max_rows: int, secrets: list[str]) -> dict[str, Any]:
    try:
        payload = client.inquire_stock_minute(symbol.zfill(6), input_hour=input_hour)
        raise_for_kis_response_error(payload)
        rows = kis_rows_from_payload(payload)
        bars, diagnostics = parse_kis_minute_rows_with_diagnostics(rows, symbol=symbol.zfill(6), source="KIS_FIELD_AUDIT")
        return {
            "status": "ok",
            "endpoint": KIS_STOCK_MINUTE_PATH,
            "tr_id": KIS_STOCK_MINUTE_TR_ID,
            "symbol": symbol.zfill(6),
            "response": response_summary(payload),
            "field_audit": audit_kis_intraday_rows(rows, max_rows=max_rows),
            "parse_diagnostics": diagnostics.to_dict(),
            "parsed_bar_count": len(bars),
            "returned_dates": sorted({bar.timestamp.date().isoformat() for bar in bars}),
        }
    except (KisClientError, KisEndpointBlocked) as exc:
        return {
            "status": "blocked",
            "endpoint": KIS_STOCK_MINUTE_PATH,
            "tr_id": KIS_STOCK_MINUTE_TR_ID,
            "symbol": symbol.zfill(6),
            "blocked_reason": _redact(str(exc), secrets),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit sanitized KIS quote-only intraday raw fields without printing secrets")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--env", choices=["paper", "real"], default=None)
    parser.add_argument("--symbol", default="005930")
    parser.add_argument("--market-symbol", default=None)
    parser.add_argument("--date", default=None, help="Optional expected trade date for report comparison; KIS endpoint may still return current-day rows")
    parser.add_argument("--input-hour", default="153000")
    parser.add_argument("--max-rows", type=int, default=10)
    parser.add_argument("--output-md", default=None)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--sleep-seconds", type=float, default=0.12)
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--force-refresh-token", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    env_result = load_kis_env(args.env_file, env_override=args.env)
    if env_result.status != "ok" or env_result.config is None:
        payload = {"status": "blocked", "blocked_reason": env_result.reason, **env_result.to_dict()}
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(2)
    payload: dict[str, Any] = {
        "status": "ok",
        "env": env_result.config.safe_summary(),
        "dry_run": args.dry_run,
        "requested_date": args.date,
        "stock": None,
        "market_proxy": None,
    }
    if args.dry_run:
        payload["status"] = "dry_run"
        payload["stock"] = {"status": "dry_run", "endpoint": KIS_STOCK_MINUTE_PATH, "tr_id": KIS_STOCK_MINUTE_TR_ID, "symbol": args.symbol.zfill(6)}
        if args.market_symbol:
            payload["market_proxy"] = {
                "status": "dry_run",
                "endpoint": KIS_STOCK_MINUTE_PATH,
                "tr_id": KIS_STOCK_MINUTE_TR_ID,
                "symbol": args.market_symbol.zfill(6),
            }
        _write_json(args.output_json, payload)
        _write_markdown(args.output_md, payload)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        return

    secrets = [env_result.config.app_key, env_result.config.app_secret]
    client = KisQuotationClient(
        env_result.config,
        sleep_seconds=args.sleep_seconds,
        token_cache_path=args.token_cache,
    )
    try:
        client.get_access_token(force_refresh=args.force_refresh_token)
        payload["token_status"] = "ok"
    except Exception as exc:
        payload["status"] = "blocked"
        payload["token_status"] = "blocked"
        payload["blocked_reason"] = _redact(str(exc), secrets)
        print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1)

    payload["stock"] = _audit_symbol(client, args.symbol, input_hour=args.input_hour, max_rows=args.max_rows, secrets=secrets)
    if args.market_symbol:
        payload["market_proxy"] = _audit_symbol(client, args.market_symbol, input_hour=args.input_hour, max_rows=args.max_rows, secrets=secrets)
    if payload["stock"]["status"] != "ok":
        payload["status"] = "blocked"
        payload["blocked_reason"] = "STOCK_FIELD_AUDIT_FAILED"
    _write_json(args.output_json, payload)
    _write_markdown(args.output_md, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    if payload["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
