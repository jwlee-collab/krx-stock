from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import datetime, time, timedelta
from typing import Any


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _timeframe_minutes(timeframe: str) -> int | None:
    value = timeframe.lower().strip()
    if value.endswith("m") and value[:-1].isdigit():
        return int(value[:-1])
    return None


def _in_regular_krx_session(ts: datetime) -> bool:
    t = ts.time()
    return time(9, 0) <= t <= time(15, 30)


def validate_intraday_prices(
    conn: sqlite3.Connection,
    symbols: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    required_timeframes: list[str] | None = None,
    market_proxy_symbol: str | None = None,
    max_missing_bars_per_symbol: int = 0,
) -> dict[str, Any]:
    required_timeframes = required_timeframes or ["5m", "15m"]
    requested_symbols = list(dict.fromkeys(symbols or []))
    query_symbols = list(requested_symbols)
    if market_proxy_symbol and query_symbols and market_proxy_symbol not in query_symbols:
        query_symbols.append(market_proxy_symbol)
    where = []
    params: list[str] = []
    if query_symbols:
        where.append(f"symbol IN ({','.join('?' for _ in query_symbols)})")
        params.extend(query_symbols)
    if start_date:
        where.append("COALESCE(date, substr(timestamp,1,10)) >= ?")
        params.append(start_date)
    if end_date:
        where.append("COALESCE(date, substr(timestamp,1,10)) <= ?")
        params.append(end_date)
    predicate = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT symbol,timeframe,timestamp,open,high,low,close,volume,amount
        FROM intraday_prices
        {predicate}
        ORDER BY symbol,timeframe,timestamp
        """,
        params,
    ).fetchall()

    by_key: dict[tuple[str, str], list[sqlite3.Row]] = {}
    timestamp_counts: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        key = (str(row["symbol"]), str(row["timeframe"]))
        by_key.setdefault(key, []).append(row)
        timestamp_counts[(str(row["symbol"]), str(row["timeframe"]), str(row["timestamp"]))] += 1

    symbol_reports: dict[str, dict[str, Any]] = {}
    total_duplicates = sum(max(0, count - 1) for count in timestamp_counts.values())
    for (symbol, timeframe), group in sorted(by_key.items()):
        failure_reasons: Counter[str] = Counter()
        timestamps = [_parse_ts(str(r["timestamp"])) for r in group]
        missing_bars = 0
        tf_minutes = _timeframe_minutes(timeframe)
        if tf_minutes and len(timestamps) >= 2:
            expected_delta = timedelta(minutes=tf_minutes)
            for prev, cur in zip(timestamps, timestamps[1:]):
                if prev.date() != cur.date():
                    continue
                gap = cur - prev
                if gap > expected_delta:
                    missing_bars += max(0, int(gap.total_seconds() // expected_delta.total_seconds()) - 1)
        for row, ts in zip(group, timestamps):
            open_px = row["open"]
            high_px = row["high"]
            low_px = row["low"]
            close_px = row["close"]
            volume = row["volume"]
            amount = row["amount"]
            if any(v is None for v in [open_px, high_px, low_px, close_px]):
                failure_reasons["missing_ohlc"] += 1
                continue
            if min(float(open_px), float(high_px), float(low_px), float(close_px)) <= 0.0:
                failure_reasons["non_positive_ohlc"] += 1
            if float(high_px) < max(float(open_px), float(close_px)) or float(low_px) > min(float(open_px), float(close_px)):
                failure_reasons["invalid_ohlc_range"] += 1
            if volume is None or float(volume) < 0.0:
                failure_reasons["invalid_volume"] += 1
            if amount is not None and float(amount) < 0.0:
                failure_reasons["invalid_traded_value"] += 1
            if not _in_regular_krx_session(ts):
                failure_reasons["outside_krx_regular_session"] += 1
            if ts.tzinfo is not None:
                failure_reasons["timezone_aware_timestamp"] += 1
        duplicate_count = sum(
            max(0, timestamp_counts[(symbol, timeframe, str(r["timestamp"]))] - 1)
            for r in group
        )
        if duplicate_count:
            failure_reasons["duplicate_timestamp"] += duplicate_count
        if missing_bars > max_missing_bars_per_symbol:
            failure_reasons["missing_bars"] += missing_bars
        key_report = {
            "symbol": symbol,
            "timeframe": timeframe,
            "bar_count": len(group),
            "missing_bar_count": missing_bars,
            "duplicate_count": duplicate_count,
            "first_timestamp": min(timestamps).isoformat() if timestamps else None,
            "last_timestamp": max(timestamps).isoformat() if timestamps else None,
            "failure_reasons": dict(failure_reasons),
            "day_strategy_usable": not failure_reasons,
        }
        symbol_reports.setdefault(symbol, {"timeframes": {}, "day_strategy_usable": True})
        symbol_reports[symbol]["timeframes"][timeframe] = key_report
        symbol_reports[symbol]["day_strategy_usable"] = (
            symbol_reports[symbol]["day_strategy_usable"] and key_report["day_strategy_usable"]
        )
    for symbol in requested_symbols:
        if symbol not in symbol_reports:
            symbol_reports[symbol] = {
                "timeframes": {},
                "day_strategy_usable": False,
                "missing_required_timeframes": list(required_timeframes),
                "missing_all_data": True,
                "can_build_15m_from_5m": False,
            }

    for symbol, report in symbol_reports.items():
        missing_required = [tf for tf in required_timeframes if tf not in report["timeframes"]]
        if missing_required:
            report["day_strategy_usable"] = False
            report["missing_required_timeframes"] = missing_required
        five_min = report["timeframes"].get("5m")
        report["can_build_15m_from_5m"] = bool(five_min and int(five_min["bar_count"]) >= 3)

    primary_timeframe = required_timeframes[0] if required_timeframes else "5m"
    market_proxy_available = None
    if market_proxy_symbol:
        market_report = symbol_reports.get(market_proxy_symbol, {})
        primary_report = market_report.get("timeframes", {}).get(primary_timeframe)
        market_proxy_available = bool(primary_report and primary_report.get("day_strategy_usable"))
    candidate_usable_count = sum(
        1
        for symbol, report in symbol_reports.items()
        if symbol != market_proxy_symbol and report["day_strategy_usable"]
    )

    return {
        "row_count": len(rows),
        "symbol_count": len(symbol_reports),
        "duplicate_count": total_duplicates,
        "required_timeframes": required_timeframes,
        "market_proxy_symbol": market_proxy_symbol,
        "market_proxy_available": market_proxy_available,
        "symbols": symbol_reports,
        "day_strategy_usable_symbol_count": sum(1 for r in symbol_reports.values() if r["day_strategy_usable"]),
        "candidate_usable_symbol_count": candidate_usable_count,
    }
