from __future__ import annotations

import sqlite3
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.data_quality import validate_intraday_prices
from pipeline.day_trading.universe import DayUniverseProvider


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    out = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _available_intraday_dates(conn: sqlite3.Connection, start_date: str | None, end_date: str | None) -> list[str]:
    where = []
    params: list[str] = []
    if start_date:
        where.append("COALESCE(date, substr(timestamp,1,10))>=?")
        params.append(start_date)
    if end_date:
        where.append("COALESCE(date, substr(timestamp,1,10))<=?")
        params.append(end_date)
    predicate = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT DISTINCT COALESCE(date, substr(timestamp,1,10)) AS d
        FROM intraday_prices
        {predicate}
        ORDER BY d
        """,
        params,
    ).fetchall()
    return [str(r["d"]) for r in rows]


def _available_score_dates(conn: sqlite3.Connection, start_date: str | None, end_date: str | None) -> list[str]:
    where = []
    params: list[str] = []
    if start_date:
        where.append("date>=?")
        params.append(start_date)
    if end_date:
        where.append("date<=?")
        params.append(end_date)
    predicate = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(f"SELECT DISTINCT date FROM daily_scores {predicate} ORDER BY date", params).fetchall()
    return [str(r["date"]) for r in rows]


def _timeframe_counts(conn: sqlite3.Connection, trade_date: str, symbols: list[str] | None = None) -> dict[str, dict[str, int]]:
    where = ["COALESCE(date, substr(timestamp,1,10))=?"]
    params: list[str] = [trade_date]
    if symbols:
        where.append(f"symbol IN ({','.join('?' for _ in symbols)})")
        params.extend(symbols)
    rows = conn.execute(
        f"""
        SELECT symbol,timeframe,COUNT(*) AS c
        FROM intraday_prices
        WHERE {' AND '.join(where)}
        GROUP BY symbol,timeframe
        ORDER BY symbol,timeframe
        """,
        params,
    ).fetchall()
    out: dict[str, dict[str, int]] = {}
    for row in rows:
        out.setdefault(str(row["symbol"]), {})[str(row["timeframe"])] = int(row["c"])
    return out


def _daily_score_exists(conn: sqlite3.Connection, score_date: str) -> bool:
    row = conn.execute("SELECT 1 FROM daily_scores WHERE date=? LIMIT 1", (score_date,)).fetchone()
    return row is not None


def build_day_data_availability_report(
    conn: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
    market_proxy_symbol: str | None = None,
    max_universe_symbols: int = 20,
) -> dict[str, Any]:
    intraday_dates = _available_intraday_dates(conn, start_date, end_date)
    score_dates_in_range = _available_score_dates(conn, start_date, end_date)
    score_dates_all = _available_score_dates(conn, None, None)
    if start_date and end_date:
        dates_to_check = sorted(set(intraday_dates) | set(score_dates_in_range))
        if not dates_to_check:
            dates_to_check = _date_range(start_date, end_date)
    else:
        dates_to_check = intraday_dates

    cfg = DayTradingConfig(max_universe_symbols=max_universe_symbols, market_proxy_symbol=market_proxy_symbol)
    provider = DayUniverseProvider(conn, cfg)
    date_reports: dict[str, dict[str, Any]] = {}
    replayable_dates: list[str] = []
    unreplayable_dates: dict[str, list[str]] = {}

    for trade_date in dates_to_check:
        selection = provider.get_universe_selection(trade_date)
        candidates = list(selection.candidates)
        symbols_for_quality = candidates if candidates else ["__NO_DAY_CANDIDATES__"]
        quality = validate_intraday_prices(
            conn,
            symbols=symbols_for_quality,
            start_date=trade_date,
            end_date=trade_date,
            market_proxy_symbol=market_proxy_symbol,
        )
        counts = _timeframe_counts(conn, trade_date, sorted(set(candidates + ([market_proxy_symbol] if market_proxy_symbol else []))))
        candidate_intraday_overlap = [symbol for symbol in candidates if symbol in counts]
        same_day_score_exists = _daily_score_exists(conn, trade_date)
        same_day_only_forbidden = (
            not selection.score_date
            and same_day_score_exists
            and "ONLY_SAME_DAY_SCORE_AVAILABLE_BUT_FORBIDDEN" in selection.reason_codes
        )
        failure_reasons: list[str] = []
        if trade_date not in intraday_dates:
            failure_reasons.append("NO_INTRADAY_DATA")
        if not selection.score_date:
            failure_reasons.append("NO_PRIOR_SCORE_DATE")
        if same_day_only_forbidden:
            failure_reasons.append("SAME_DAY_SCORE_ONLY_FORBIDDEN")
        if not candidates:
            failure_reasons.append("EMPTY_CANDIDATES")
        if not candidate_intraday_overlap:
            failure_reasons.append("NO_CANDIDATE_INTRADAY_OVERLAP")
        if quality.get("candidate_usable_symbol_count", 0) <= 0:
            failure_reasons.append("NO_USABLE_CANDIDATE_INTRADAY")
        if market_proxy_symbol and not quality.get("market_proxy_available"):
            failure_reasons.append("MARKET_PROXY_MISSING_OR_UNUSABLE")

        quality_failures = Counter()
        for symbol_report in quality.get("symbols", {}).values():
            for timeframe_report in symbol_report.get("timeframes", {}).values():
                quality_failures.update(timeframe_report.get("failure_reasons", {}))
        date_report = {
            "trade_date": trade_date,
            "intraday_available": trade_date in intraday_dates,
            "score_date": selection.score_date,
            "same_day_score_exists": same_day_score_exists,
            "same_day_score_only_forbidden": same_day_only_forbidden,
            "candidate_count": len(candidates),
            "candidate_symbols": candidates,
            "candidate_intraday_overlap": candidate_intraday_overlap,
            "candidate_usable_symbol_count": quality.get("candidate_usable_symbol_count", 0),
            "market_proxy_available": quality.get("market_proxy_available") if market_proxy_symbol else None,
            "timeframe_counts": counts,
            "quality_failure_summary": dict(quality_failures),
            "replayable": not failure_reasons,
            "failure_reasons": failure_reasons,
        }
        date_reports[trade_date] = date_report
        if date_report["replayable"]:
            replayable_dates.append(trade_date)
        else:
            unreplayable_dates[trade_date] = failure_reasons

    return {
        "start_date": start_date,
        "end_date": end_date,
        "market_proxy_symbol": market_proxy_symbol,
        "intraday_dates": intraday_dates,
        "daily_score_dates": score_dates_all,
        "replayable_dates": replayable_dates,
        "unreplayable_dates": unreplayable_dates,
        "date_reports": date_reports,
        "summary": {
            "intraday_date_count": len(intraday_dates),
            "daily_score_date_count": len(score_dates_all),
            "checked_date_count": len(dates_to_check),
            "replayable_date_count": len(replayable_dates),
            "unreplayable_date_count": len(unreplayable_dates),
        },
    }


def build_day_data_availability_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# DAY Data Availability Audit ({report.get('start_date') or 'BEGIN'} ~ {report.get('end_date') or 'END'})",
        "",
        "## Summary",
    ]
    for key, value in report.get("summary", {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            f"- market_proxy_symbol: {report.get('market_proxy_symbol') or 'MISSING'}",
            f"- intraday_dates: {report.get('intraday_dates', [])}",
            f"- daily_score_dates: {report.get('daily_score_dates', [])}",
            f"- replayable_dates: {report.get('replayable_dates', [])}",
            f"- unreplayable_dates: {report.get('unreplayable_dates', {})}",
            "",
            "## Date Detail",
            "| date | replayable | score_date | candidates | usable | market_proxy | failures | overlap | quality_failures |",
            "| --- | --- | --- | ---: | ---: | --- | --- | --- | --- |",
        ]
    )
    for trade_date, detail in sorted(report.get("date_reports", {}).items()):
        lines.append(
            "| "
            + " | ".join(
                [
                    trade_date,
                    str(detail.get("replayable")),
                    str(detail.get("score_date")),
                    str(detail.get("candidate_count", 0)),
                    str(detail.get("candidate_usable_symbol_count", 0)),
                    str(detail.get("market_proxy_available")),
                    str(detail.get("failure_reasons", [])),
                    str(detail.get("candidate_intraday_overlap", [])),
                    str(detail.get("quality_failure_summary", {})),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Required Data",
            "- intraday_prices: symbol,timestamp,date,time,timeframe,open,high,low,close,volume,amount/source",
            "- daily_scores: prior score_date rows for the DAY trade_date, ranked by rank",
            "- market proxy: primary 5m data for the requested market_proxy_symbol",
            "- optional context: intraday provisional investor/program/trade-strength rows with point-in-time timestamp",
        ]
    )
    return "\n".join(lines) + "\n"


def write_day_data_availability_report(path: str | Path, markdown: str) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(markdown, encoding="utf-8")
    return out
