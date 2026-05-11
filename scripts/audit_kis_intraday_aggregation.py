#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.kis_client import (  # noqa: E402
    DEFAULT_KIS_TOKEN_CACHE_PATH,
    KisClientError,
    KisEndpointBlocked,
    KisQuotationClient,
    aggregate_bars,
    audit_kis_intraday_rows,
    kis_rows_from_payload,
    load_kis_env,
    parse_kis_minute_rows_with_diagnostics,
    raise_for_kis_response_error,
)
from pipeline.day_trading.models import IntradayBar  # noqa: E402


def _json_dump(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def _redact(text: str, secrets: list[str]) -> str:
    out = text
    for secret in secrets:
        if secret:
            out = out.replace(secret, "***REDACTED***")
    return out


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _raw_timestamp(row: dict[str, Any]) -> datetime | None:
    raw_date = row.get("stck_bsop_date") or row.get("bsop_date") or row.get("date")
    raw_time = row.get("stck_cntg_hour") or row.get("cntg_hour") or row.get("hour") or row.get("time")
    if raw_date is None or raw_time is None:
        return None
    date_value = str(raw_date).replace("-", "").strip()
    time_value = str(raw_time).replace(":", "").strip().zfill(6)
    if len(date_value) != 8 or len(time_value) < 4:
        return None
    try:
        return datetime.strptime(f"{date_value}{time_value[:6]}", "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _raw_row_key(row: dict[str, Any]) -> str:
    ts = _raw_timestamp(row)
    return ts.isoformat() if ts is not None else json.dumps(row, sort_keys=True, ensure_ascii=False)


def _raw_volume(row: dict[str, Any]) -> float | None:
    for key in ("cntg_vol", "tr_vol", "volume", "trading_volume", "acml_vol"):
        parsed = _to_float(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _raw_amount(row: dict[str, Any]) -> float | None:
    for key in ("tr_pbmn", "cntg_tr_pbmn", "amount", "traded_value", "trading_value", "acml_tr_pbmn"):
        parsed = _to_float(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _raw_prices(row: dict[str, Any]) -> list[float | None]:
    return [
        _to_float(row.get("stck_oprc") or row.get("oprc") or row.get("open")),
        _to_float(row.get("stck_hgpr") or row.get("hgpr") or row.get("high")),
        _to_float(row.get("stck_lwpr") or row.get("lwpr") or row.get("low")),
        _to_float(row.get("stck_prpr") or row.get("prpr") or row.get("close")),
    ]


def _in_session(ts: datetime, session_start: str, session_end: str) -> bool:
    start = time.fromisoformat(f"{session_start[:2]}:{session_start[2:4]}:{session_start[4:6]}")
    end = time.fromisoformat(f"{session_end[:2]}:{session_end[2:4]}:{session_end[4:6]}")
    return start <= ts.time() <= end


def _safe_row(row: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "stck_bsop_date",
        "stck_cntg_hour",
        "stck_oprc",
        "stck_hgpr",
        "stck_lwpr",
        "stck_prpr",
        "cntg_vol",
        "acml_vol",
        "tr_pbmn",
        "acml_tr_pbmn",
    ]
    return {key: row.get(key) for key in keys if key in row}


def _fetch_raw_rows(
    client: KisQuotationClient,
    symbol: str,
    *,
    trade_date: str,
    input_hour: str,
    session_start: str,
    max_pages: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    page_hour = input_hour
    rows_by_key: dict[str, dict[str, Any]] = {}
    pages: list[dict[str, Any]] = []
    duplicate_timestamp_count = 0
    for _ in range(max_pages):
        payload = client.inquire_stock_minute(symbol, input_hour=page_hour)
        raise_for_kis_response_error(payload)
        rows = kis_rows_from_payload(payload)
        parsed_ts = [(row, _raw_timestamp(row)) for row in rows]
        page_trade_date_ts = [ts for _row, ts in parsed_ts if ts is not None and ts.date().isoformat() == trade_date]
        new_count = 0
        for row, ts in parsed_ts:
            if ts is None or ts.date().isoformat() != trade_date:
                continue
            key = ts.isoformat()
            if key in rows_by_key:
                duplicate_timestamp_count += 1
                continue
            rows_by_key[key] = row
            new_count += 1
        page_summary = {
            "input_hour": page_hour,
            "payload_row_count": len(rows),
            "trade_date_row_count": len(page_trade_date_ts),
            "new_trade_date_row_count": new_count,
            "earliest_trade_date_timestamp": min(page_trade_date_ts).isoformat() if page_trade_date_ts else None,
            "latest_trade_date_timestamp": max(page_trade_date_ts).isoformat() if page_trade_date_ts else None,
        }
        pages.append(page_summary)
        if not rows or new_count == 0 and not page_trade_date_ts:
            break
        if not page_trade_date_ts:
            all_ts = [ts for _row, ts in parsed_ts if ts is not None]
            if all_ts and max(all_ts).date().isoformat() < trade_date:
                break
            if all_ts:
                page_hour = (min(all_ts).replace(second=0, microsecond=0) - timedelta(minutes=1)).strftime("%H%M%S")
                continue
            break
        earliest = min(page_trade_date_ts)
        if earliest.strftime("%H%M%S") <= session_start:
            break
        page_hour = (earliest.replace(second=0, microsecond=0) - timedelta(minutes=1)).strftime("%H%M%S")
    rows_out = [rows_by_key[key] for key in sorted(rows_by_key)]
    return rows_out, {
        "pages": pages,
        "duplicate_timestamp_count": duplicate_timestamp_count,
        "raw_returned_date_set": sorted({str(_raw_timestamp(row).date()) for row in rows_out if _raw_timestamp(row) is not None}),
    }


def _bar_validity(bar: IntradayBar) -> list[str]:
    reasons: list[str] = []
    values = [bar.open, bar.high, bar.low, bar.close]
    if min(float(v) for v in values) <= 0.0:
        reasons.append("INVALID_OHLC_NON_POSITIVE")
    elif float(bar.high) < max(float(bar.open), float(bar.close)) or float(bar.low) > min(float(bar.open), float(bar.close)):
        reasons.append("INVALID_OHLC_RANGE")
    if float(bar.volume) == 0.0:
        reasons.append("ZERO_VOLUME_VALID_PRICE" if min(float(v) for v in values) > 0.0 else "ZERO_VOLUME_ZERO_PRICE")
    elif float(bar.volume) < 0.0:
        reasons.append("NEGATIVE_VOLUME")
    if bar.amount is not None and float(bar.volume) == 0.0 and float(bar.amount) > 0.0:
        reasons.append("ZERO_VOLUME_POSITIVE_AMOUNT")
    return reasons


def _time_range(rows: list[Any], getter) -> dict[str, str | None]:
    values = [getter(row) for row in rows]
    values = [value for value in values if value is not None]
    return {
        "first": min(values).isoformat() if values else None,
        "last": max(values).isoformat() if values else None,
    }


def _missing_minute_gaps(bars: list[IntradayBar], *, sample_limit: int) -> dict[str, Any]:
    ordered = sorted(bars, key=lambda bar: bar.timestamp)
    gaps: list[dict[str, Any]] = []
    for prev, cur in zip(ordered, ordered[1:]):
        delta = cur.timestamp - prev.timestamp
        if prev.timestamp.date() == cur.timestamp.date() and delta > timedelta(minutes=1):
            gaps.append({"from": prev.timestamp.isoformat(), "to": cur.timestamp.isoformat(), "minutes": int(delta.total_seconds() // 60)})
    return {"count": len(gaps), "samples": gaps[:sample_limit]}


def _audit_bars(bars: list[IntradayBar], *, sample_limit: int) -> dict[str, Any]:
    zero_bars = [bar for bar in bars if float(bar.volume) == 0.0]
    invalid_counts: Counter[str] = Counter()
    invalid_samples: list[dict[str, Any]] = []
    by_timestamp: Counter[str] = Counter()
    for bar in bars:
        reasons = _bar_validity(bar)
        for reason in reasons:
            invalid_counts[reason] += 1
            by_timestamp[bar.timestamp.isoformat()] += 1
            if len(invalid_samples) < sample_limit:
                invalid_samples.append(
                    {
                        "symbol": bar.symbol,
                        "timestamp": bar.timestamp.isoformat(),
                        "timeframe": bar.timeframe,
                        "reason": reason,
                        "open": bar.open,
                        "high": bar.high,
                        "low": bar.low,
                        "close": bar.close,
                        "volume": bar.volume,
                        "amount": bar.amount,
                        "source": bar.source,
                    }
                )
    return {
        "count": len(bars),
        "timestamp_range": _time_range(bars, lambda bar: bar.timestamp),
        "zero_volume_count": len(zero_bars),
        "volume_sum": sum(float(bar.volume) for bar in bars),
        "traded_value_sum": sum(float(bar.trade_value) for bar in bars),
        "invalid_count_by_reason": dict(invalid_counts),
        "invalid_sample_rows": invalid_samples,
        "first_zero_volume_timestamp": zero_bars[0].timestamp.isoformat() if zero_bars else None,
        "last_zero_volume_timestamp": zero_bars[-1].timestamp.isoformat() if zero_bars else None,
        "top_invalid_time_buckets": dict(by_timestamp.most_common(10)),
    }


def _audit_symbol(
    client: KisQuotationClient,
    symbol: str,
    *,
    trade_date: str,
    input_hour: str,
    session_start: str,
    session_end: str,
    max_pages: int,
    sample_limit: int,
) -> dict[str, Any]:
    raw_rows, fetch_summary = _fetch_raw_rows(
        client,
        symbol,
        trade_date=trade_date,
        input_hour=input_hour,
        session_start=session_start,
        max_pages=max_pages,
    )
    session_rows: list[dict[str, Any]] = []
    out_of_session_rows: list[dict[str, Any]] = []
    for row in raw_rows:
        ts = _raw_timestamp(row)
        if ts is not None and _in_session(ts, session_start, session_end):
            session_rows.append(row)
        else:
            out_of_session_rows.append(row)
    normalized, diagnostics = parse_kis_minute_rows_with_diagnostics(
        session_rows,
        symbol=symbol,
        source="KIS_QUOTATION_STOCK_RAW",
    )
    five = aggregate_bars(normalized, "5m")
    fifteen = aggregate_bars(normalized, "15m")

    raw_zero = 0
    raw_zero_valid_price = 0
    raw_zero_zero_price = 0
    raw_volume_sum = 0.0
    for row in session_rows:
        volume = _raw_volume(row)
        prices = _raw_prices(row)
        if volume is not None:
            raw_volume_sum += float(volume)
        if volume == 0.0:
            raw_zero += 1
            if any(value in (None, 0.0) for value in prices):
                raw_zero_zero_price += 1
            else:
                raw_zero_valid_price += 1

    normalized_audit = _audit_bars(normalized, sample_limit=sample_limit)
    five_audit = _audit_bars(five, sample_limit=sample_limit)
    fifteen_audit = _audit_bars(fifteen, sample_limit=sample_limit)
    cause_counts: Counter[str] = Counter()
    cause_counts["RAW_ZERO_VOLUME"] = raw_zero
    cause_counts["NORMALIZED_ZERO_VOLUME"] = normalized_audit["zero_volume_count"]
    cause_counts["AGGREGATED_ZERO_VOLUME"] = five_audit["zero_volume_count"] + fifteen_audit["zero_volume_count"]
    cause_counts["ZERO_VOLUME_VALID_PRICE"] = (
        raw_zero_valid_price
        + normalized_audit["invalid_count_by_reason"].get("ZERO_VOLUME_VALID_PRICE", 0)
        + five_audit["invalid_count_by_reason"].get("ZERO_VOLUME_VALID_PRICE", 0)
        + fifteen_audit["invalid_count_by_reason"].get("ZERO_VOLUME_VALID_PRICE", 0)
    )
    cause_counts["ZERO_VOLUME_ZERO_PRICE"] = raw_zero_zero_price
    cause_counts["OUT_OF_SESSION_ROW"] = len(out_of_session_rows)
    cause_counts["DUPLICATE_ROW_DROPPED"] = fetch_summary["duplicate_timestamp_count"]
    cause_counts["MISSING_RAW_ROWS"] = _missing_minute_gaps(normalized, sample_limit=sample_limit)["count"]
    if diagnostics.cumulative_volume_diff_used:
        cause_counts["CUMULATIVE_DIFF_ZERO"] += normalized_audit["invalid_count_by_reason"].get("ZERO_VOLUME_VALID_PRICE", 0)
    if diagnostics.negative_cumulative_diff_count:
        cause_counts["CUMULATIVE_DIFF_NEGATIVE"] += diagnostics.negative_cumulative_diff_count
    if diagnostics.raw_volume_field_used is None:
        cause_counts["UNKNOWN_VOLUME_MAPPING"] += len(session_rows)
    cause_counts["INCOMPLETE_BAR"] += sum(1 for bar in fifteen if len([b for b in five if bar.timestamp <= b.timestamp < bar.timestamp + timedelta(minutes=15)]) < 3)

    invalid_sample_rows = (
        normalized_audit["invalid_sample_rows"] + five_audit["invalid_sample_rows"] + fifteen_audit["invalid_sample_rows"]
    )[:sample_limit]
    return {
        "symbol": symbol,
        "raw_row_count": len(session_rows),
        "raw_total_row_count_for_trade_date": len(raw_rows),
        "out_of_session_row_count": len(out_of_session_rows),
        "normalized_row_count": len(normalized),
        "aggregated_5m_count": len(five),
        "aggregated_15m_count": len(fifteen),
        "raw_timestamp_range": _time_range(session_rows, _raw_timestamp),
        "normalized_timestamp_range": normalized_audit["timestamp_range"],
        "raw_zero_volume_count": raw_zero,
        "normalized_zero_volume_count": normalized_audit["zero_volume_count"],
        "aggregated_5m_zero_volume_count": five_audit["zero_volume_count"],
        "aggregated_15m_zero_volume_count": fifteen_audit["zero_volume_count"],
        "raw_volume_sum": raw_volume_sum,
        "normalized_volume_sum": normalized_audit["volume_sum"],
        "aggregated_5m_volume_sum": five_audit["volume_sum"],
        "aggregated_15m_volume_sum": fifteen_audit["volume_sum"],
        "raw_traded_value_sum": _raw_amount(session_rows[-1]) if session_rows else 0.0,
        "normalized_traded_value_sum": normalized_audit["traded_value_sum"],
        "aggregated_5m_traded_value_sum": five_audit["traded_value_sum"],
        "aggregated_15m_traded_value_sum": fifteen_audit["traded_value_sum"],
        "duplicate_timestamp_count": fetch_summary["duplicate_timestamp_count"],
        "missing_timestamp_gaps": _missing_minute_gaps(normalized, sample_limit=sample_limit),
        "invalid_ohlc_count": (
            normalized_audit["invalid_count_by_reason"].get("INVALID_OHLC_NON_POSITIVE", 0)
            + normalized_audit["invalid_count_by_reason"].get("INVALID_OHLC_RANGE", 0)
            + five_audit["invalid_count_by_reason"].get("INVALID_OHLC_NON_POSITIVE", 0)
            + five_audit["invalid_count_by_reason"].get("INVALID_OHLC_RANGE", 0)
            + fifteen_audit["invalid_count_by_reason"].get("INVALID_OHLC_NON_POSITIVE", 0)
            + fifteen_audit["invalid_count_by_reason"].get("INVALID_OHLC_RANGE", 0)
        ),
        "zero_volume_with_valid_ohlc_count": cause_counts["ZERO_VOLUME_VALID_PRICE"],
        "zero_volume_with_all_prices_nonzero_count": cause_counts["ZERO_VOLUME_VALID_PRICE"],
        "zero_volume_with_all_prices_zero_count": cause_counts["ZERO_VOLUME_ZERO_PRICE"],
        "invalid_sample_rows": invalid_sample_rows,
        "first_zero_volume_timestamp": normalized_audit["first_zero_volume_timestamp"] or five_audit["first_zero_volume_timestamp"],
        "last_zero_volume_timestamp": normalized_audit["last_zero_volume_timestamp"] or fifteen_audit["last_zero_volume_timestamp"],
        "top_invalid_time_buckets": dict(
            Counter(normalized_audit["top_invalid_time_buckets"])
            + Counter(five_audit["top_invalid_time_buckets"])
            + Counter(fifteen_audit["top_invalid_time_buckets"])
        ),
        "zero_volume_cause_counts": {key: value for key, value in sorted(cause_counts.items()) if value},
        "parse_diagnostics": diagnostics.to_dict(),
        "field_audit": audit_kis_intraday_rows(session_rows, max_rows=sample_limit),
        "fetch_summary": fetch_summary,
        "raw_sample_rows": [_safe_row(row) for row in session_rows[:sample_limit]],
    }


def _write_markdown(path: str | Path, report: dict[str, Any]) -> None:
    lines = [
        f"# KIS Intraday Aggregation Audit ({report.get('trade_date')})",
        "",
        "## Summary",
        f"- status: {report.get('status')}",
        f"- symbols: {report.get('symbols')}",
        f"- market_symbol: {report.get('market_symbol')}",
        f"- session: {report.get('session_start')}~{report.get('session_end')}",
        "",
    ]
    for symbol, audit in sorted((report.get("symbol_audits") or {}).items()):
        lines.extend(
            [
                f"## {symbol}",
                f"- raw_row_count: {audit.get('raw_row_count')}",
                f"- normalized_row_count: {audit.get('normalized_row_count')}",
                f"- aggregated_5m_count: {audit.get('aggregated_5m_count')}",
                f"- aggregated_15m_count: {audit.get('aggregated_15m_count')}",
                f"- raw_timestamp_range: {audit.get('raw_timestamp_range')}",
                f"- normalized_timestamp_range: {audit.get('normalized_timestamp_range')}",
                f"- raw_zero_volume_count: {audit.get('raw_zero_volume_count')}",
                f"- normalized_zero_volume_count: {audit.get('normalized_zero_volume_count')}",
                f"- aggregated_5m_zero_volume_count: {audit.get('aggregated_5m_zero_volume_count')}",
                f"- aggregated_15m_zero_volume_count: {audit.get('aggregated_15m_zero_volume_count')}",
                f"- raw_volume_sum: {audit.get('raw_volume_sum')}",
                f"- normalized_volume_sum: {audit.get('normalized_volume_sum')}",
                f"- aggregated_5m_volume_sum: {audit.get('aggregated_5m_volume_sum')}",
                f"- aggregated_15m_volume_sum: {audit.get('aggregated_15m_volume_sum')}",
                f"- raw_traded_value_sum: {audit.get('raw_traded_value_sum')}",
                f"- normalized_traded_value_sum: {audit.get('normalized_traded_value_sum')}",
                f"- aggregated_5m_traded_value_sum: {audit.get('aggregated_5m_traded_value_sum')}",
                f"- aggregated_15m_traded_value_sum: {audit.get('aggregated_15m_traded_value_sum')}",
                f"- duplicate_timestamp_count: {audit.get('duplicate_timestamp_count')}",
                f"- missing_timestamp_gaps: {audit.get('missing_timestamp_gaps')}",
                f"- invalid_ohlc_count: {audit.get('invalid_ohlc_count')}",
                f"- zero_volume_cause_counts: {audit.get('zero_volume_cause_counts')}",
                f"- first_zero_volume_timestamp: {audit.get('first_zero_volume_timestamp')}",
                f"- last_zero_volume_timestamp: {audit.get('last_zero_volume_timestamp')}",
                f"- top_invalid_time_buckets: {audit.get('top_invalid_time_buckets')}",
                f"- parse_diagnostics: {audit.get('parse_diagnostics')}",
                f"- invalid_sample_rows: {audit.get('invalid_sample_rows')}",
                "",
            ]
        )
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit raw KIS rows against normalized 1m and aggregated 5m/15m bars")
    parser.add_argument("--db", default="data/market_pipeline.db", help="Accepted for workflow symmetry; not modified")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--market-symbol", default="069500")
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--output-md", default=None)
    parser.add_argument("--output-json", default=None)
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--use-cache", action="store_true")
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.12)
    parser.add_argument("--input-hour", default="153000")
    parser.add_argument("--session-start", default="090000")
    parser.add_argument("--session-end", default="153000")
    parser.add_argument("--max-pages", type=int, default=20)
    args = parser.parse_args()

    symbols = [symbol.strip().zfill(6) for symbol in args.symbols.split(",") if symbol.strip()]
    if args.market_symbol:
        symbols.append(args.market_symbol.strip().zfill(6))
    symbols = list(dict.fromkeys(symbols))
    if args.max_symbols is not None:
        symbols = symbols[: int(args.max_symbols)]
        if args.market_symbol and args.market_symbol.strip().zfill(6) not in symbols:
            symbols.append(args.market_symbol.strip().zfill(6))

    report: dict[str, Any] = {
        "status": "ok",
        "trade_date": args.trade_date,
        "symbols": symbols,
        "market_symbol": args.market_symbol,
        "session_start": args.session_start,
        "session_end": args.session_end,
        "dry_run": args.dry_run,
        "db": args.db,
    }
    if args.dry_run:
        report["status"] = "dry_run"
        _json_dump(report)
        return
    if not symbols:
        _json_dump({**report, "status": "blocked", "blocked_reason": "NO_SYMBOLS"})
        raise SystemExit(2)

    env_result = load_kis_env(args.env_file)
    if env_result.status != "ok" or env_result.config is None:
        _json_dump({**report, "status": "blocked", "blocked_reason": env_result.reason, **env_result.to_dict()})
        raise SystemExit(2)
    secrets = [env_result.config.app_key, env_result.config.app_secret]
    client = KisQuotationClient(
        env_result.config,
        sleep_seconds=args.sleep_seconds,
        token_cache_path=args.token_cache if args.use_cache else args.token_cache,
    )
    try:
        client.get_access_token(force_refresh=False)
    except Exception as exc:
        _json_dump({**report, "status": "blocked", "blocked_reason": _redact(str(exc), secrets)})
        raise SystemExit(1)

    symbol_audits: dict[str, Any] = {}
    blocked_symbols: dict[str, str] = {}
    for symbol in symbols:
        try:
            symbol_audits[symbol] = _audit_symbol(
                client,
                symbol,
                trade_date=args.trade_date,
                input_hour=args.input_hour,
                session_start=args.session_start,
                session_end=args.session_end,
                max_pages=args.max_pages,
                sample_limit=args.sample_limit,
            )
        except (KisClientError, KisEndpointBlocked) as exc:
            blocked_symbols[symbol] = _redact(str(exc), secrets)
    report["symbol_audits"] = symbol_audits
    report["blocked_symbols"] = blocked_symbols
    if blocked_symbols and not symbol_audits:
        report["status"] = "blocked"
        report["blocked_reason"] = "ALL_SYMBOL_AUDITS_BLOCKED"

    if args.output_json:
        out_json = Path(args.output_json)
        out_json.parent.mkdir(parents=True, exist_ok=True)
        out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        report["output_json"] = str(out_json)
    if args.output_md:
        _write_markdown(args.output_md, report)
        report["output_md"] = args.output_md
    _json_dump(report)
    if report["status"] == "blocked":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
