from __future__ import annotations

import csv
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from pipeline.day_trading.data_quality import validate_intraday_prices


@dataclass(frozen=True)
class RequiredIntradayRow:
    date: str
    symbol: str
    timeframe: str
    source_type: str
    score_date: str | None
    rank: int | None
    score: float | None
    required_reason: str


@dataclass(frozen=True)
class MissingDataRow:
    date: str
    symbol: str
    data_type: str
    timeframe: str | None
    reason: str


def _date_range(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _weekdays(start_date: str, end_date: str) -> list[str]:
    return [d for d in _date_range(start_date, end_date) if date.fromisoformat(d).weekday() < 5]


def _distinct_dates(conn: sqlite3.Connection, table: str, start_date: str, end_date: str) -> list[str]:
    if table == "intraday_prices":
        date_expr = "COALESCE(date, substr(timestamp,1,10))"
    else:
        date_expr = "date"
    rows = conn.execute(
        f"""
        SELECT DISTINCT {date_expr} AS d
        FROM {table}
        WHERE {date_expr} >= ? AND {date_expr} <= ?
        ORDER BY d
        """,
        (start_date, end_date),
    ).fetchall()
    return [str(row["d"]) for row in rows]


def _infer_trade_dates(conn: sqlite3.Connection, start_date: str, end_date: str) -> tuple[list[str], str, bool]:
    daily_price_dates = _distinct_dates(conn, "daily_prices", start_date, end_date)
    if daily_price_dates:
        return daily_price_dates, "daily_prices", False
    score_dates = _distinct_dates(conn, "daily_scores", start_date, end_date)
    if score_dates:
        return score_dates, "daily_scores", True
    intraday_dates = _distinct_dates(conn, "intraday_prices", start_date, end_date)
    if intraday_dates:
        return intraday_dates, "intraday_prices", True
    return _weekdays(start_date, end_date), "weekdays", True


def _previous_score_date(conn: sqlite3.Connection, trade_date: str, allow_same_day_score: bool) -> tuple[str | None, bool, bool]:
    op = "<=" if allow_same_day_score else "<"
    row = conn.execute(f"SELECT MAX(date) AS d FROM daily_scores WHERE date {op} ?", (trade_date,)).fetchone()
    score_date = str(row["d"]) if row and row["d"] else None
    same_day_used = bool(score_date and score_date == trade_date)
    same_day_exists = conn.execute("SELECT 1 FROM daily_scores WHERE date=? LIMIT 1", (trade_date,)).fetchone() is not None
    return score_date, same_day_used, same_day_exists


def _candidates_for_score_date(conn: sqlite3.Connection, score_date: str, top_n: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT symbol, rank, score
        FROM daily_scores
        WHERE date=?
        ORDER BY rank ASC, score DESC, symbol ASC
        LIMIT ?
        """,
        (score_date, int(top_n)),
    ).fetchall()
    return [
        {
            "symbol": str(row["symbol"]),
            "rank": int(row["rank"]) if row["rank"] is not None else None,
            "score": float(row["score"]) if row["score"] is not None else None,
        }
        for row in rows
    ]


def _daily_price_exists(conn: sqlite3.Connection, symbol: str, price_date: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM daily_prices WHERE symbol=? AND date=? LIMIT 1",
        (symbol, price_date),
    ).fetchone() is not None


def _intraday_count(conn: sqlite3.Connection, trade_date: str, symbol: str, timeframe: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM intraday_prices
        WHERE symbol=?
          AND timeframe=?
          AND COALESCE(date, substr(timestamp,1,10))=?
        """,
        (symbol, timeframe, trade_date),
    ).fetchone()
    return int(row["c"] or 0) if row else 0


def _bars_per_day_estimate(timeframe: str) -> int:
    value = timeframe.strip().lower()
    if value == "5m":
        return 78
    if value == "15m":
        return 26
    if value.endswith("m") and value[:-1].isdigit() and int(value[:-1]) > 0:
        return max(1, 390 // int(value[:-1]))
    return 0


def build_day_replay_dataset_plan(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    market_symbol: str,
    top_n: int = 50,
    timeframe: str = "5m",
    allow_same_day_score: bool = False,
) -> dict[str, Any]:
    trade_dates, calendar_source, calendar_inferred = _infer_trade_dates(conn, start_date, end_date)
    required_rows: list[RequiredIntradayRow] = []
    missing_rows: list[MissingDataRow] = []
    date_reports: dict[str, dict[str, Any]] = {}
    reason_counts: Counter[str] = Counter()
    existing_intraday_count = 0
    existing_market_proxy_count = 0
    bars_estimate = _bars_per_day_estimate(timeframe)

    for trade_date in trade_dates:
        score_date, same_day_used, same_day_exists = _previous_score_date(conn, trade_date, allow_same_day_score)
        candidates: list[dict[str, Any]] = []
        date_reasons: list[str] = []
        if not score_date:
            date_reasons.append("NO_PRIOR_SCORE_DATE")
            reason_counts["NO_PRIOR_SCORE_DATE"] += 1
            missing_rows.append(MissingDataRow(trade_date, "", "DAILY_SCORE", None, "NO_PRIOR_SCORE_DATE"))
            if same_day_exists and not allow_same_day_score:
                date_reasons.append("SAME_DAY_SCORE_ONLY_FORBIDDEN")
                reason_counts["SAME_DAY_SCORE_ONLY_FORBIDDEN"] += 1
                missing_rows.append(MissingDataRow(trade_date, "", "DAILY_SCORE", None, "SAME_DAY_SCORE_ONLY_FORBIDDEN"))
        else:
            candidates = _candidates_for_score_date(conn, score_date, top_n)
            if same_day_used:
                date_reasons.append("SAME_DAY_SCORE_USED")
                reason_counts["SAME_DAY_SCORE_USED"] += 1
            if not candidates:
                date_reasons.append("EMPTY_CANDIDATES")
                reason_counts["EMPTY_CANDIDATES"] += 1
                missing_rows.append(MissingDataRow(trade_date, "", "DAILY_SCORE", None, "EMPTY_CANDIDATES"))

        for candidate in candidates:
            symbol = str(candidate["symbol"])
            required_rows.append(
                RequiredIntradayRow(
                    date=trade_date,
                    symbol=symbol,
                    timeframe=timeframe,
                    source_type="CANDIDATE",
                    score_date=score_date,
                    rank=candidate.get("rank"),
                    score=candidate.get("score"),
                    required_reason="TOP_N_SWING_CANDIDATE",
                )
            )
            if score_date and not _daily_price_exists(conn, symbol, score_date):
                reason_counts["MISSING_DAILY_PRICE"] += 1
                missing_rows.append(MissingDataRow(trade_date, symbol, "DAILY_PRICE", None, "MISSING_DAILY_PRICE"))
            bars = _intraday_count(conn, trade_date, symbol, timeframe)
            if bars <= 0:
                reason_counts["MISSING_INTRADAY"] += 1
                missing_rows.append(MissingDataRow(trade_date, symbol, "INTRADAY", timeframe, "MISSING_INTRADAY"))
            else:
                existing_intraday_count += 1

        required_rows.append(
            RequiredIntradayRow(
                date=trade_date,
                symbol=market_symbol,
                timeframe=timeframe,
                source_type="MARKET_PROXY",
                score_date=None,
                rank=None,
                score=None,
                required_reason="MARKET_CONTEXT",
            )
        )
        market_bars = _intraday_count(conn, trade_date, market_symbol, timeframe)
        if market_bars <= 0:
            reason_counts["MISSING_MARKET_PROXY"] += 1
            missing_rows.append(MissingDataRow(trade_date, market_symbol, "INTRADAY", timeframe, "MISSING_MARKET_PROXY"))
        else:
            existing_market_proxy_count += 1

        quality = validate_intraday_prices(
            conn,
            symbols=[str(c["symbol"]) for c in candidates] if candidates else ["__NO_DAY_CANDIDATES__"],
            start_date=trade_date,
            end_date=trade_date,
            required_timeframes=[timeframe],
            market_proxy_symbol=market_symbol,
        )
        quality_failures = Counter()
        for symbol_report in quality.get("symbols", {}).values():
            for timeframe_report in symbol_report.get("timeframes", {}).values():
                quality_failures.update(timeframe_report.get("failure_reasons", {}))
        if quality_failures:
            reason_counts["INTRADAY_QUALITY_FAIL"] += sum(quality_failures.values())

        date_reports[trade_date] = {
            "trade_date": trade_date,
            "score_date": score_date,
            "same_day_score_used": same_day_used,
            "same_day_score_exists": same_day_exists,
            "candidate_count": len(candidates),
            "candidate_symbols": [str(c["symbol"]) for c in candidates],
            "candidate_intraday_existing_count": sum(
                1 for c in candidates if _intraday_count(conn, trade_date, str(c["symbol"]), timeframe) > 0
            ),
            "market_proxy_intraday_available": market_bars > 0,
            "candidate_usable_symbol_count": int(quality.get("candidate_usable_symbol_count", 0)),
            "quality_failure_summary": dict(quality_failures),
            "replay_candidate": bool(score_date and candidates),
            "failure_reasons": date_reasons,
        }

    required_candidate_rows = [r for r in required_rows if r.source_type == "CANDIDATE"]
    required_market_rows = [r for r in required_rows if r.source_type == "MARKET_PROXY"]
    missing_reason_counts = Counter(row.reason for row in missing_rows)
    dates_with_prior_score = [d for d, r in date_reports.items() if r.get("score_date")]
    dates_without_prior_score = [d for d, r in date_reports.items() if not r.get("score_date")]
    total_required_symbol_days = len(required_rows)
    summary = {
        "requested_days": len(_date_range(start_date, end_date)),
        "inferred_trade_dates": len(trade_dates),
        "replay_candidate_dates": sum(1 for r in date_reports.values() if r.get("replay_candidate")),
        "dates_with_prior_score": len(dates_with_prior_score),
        "dates_without_prior_score": len(dates_without_prior_score),
        "total_required_candidate_symbols": len(required_candidate_rows),
        "total_required_market_proxy_rows_estimate": len(required_market_rows) * bars_estimate,
        "total_required_intraday_rows_estimate": total_required_symbol_days * bars_estimate,
        "existing_intraday_coverage": {
            "candidate_symbol_days": existing_intraday_count,
            "market_proxy_days": existing_market_proxy_count,
            "required_symbol_days": total_required_symbol_days,
            "coverage_ratio": (existing_intraday_count + existing_market_proxy_count) / total_required_symbol_days
            if total_required_symbol_days
            else 0.0,
        },
        "missing_intraday_count": missing_reason_counts.get("MISSING_INTRADAY", 0),
        "missing_market_proxy_count": missing_reason_counts.get("MISSING_MARKET_PROXY", 0),
        "missing_daily_prices_count": missing_reason_counts.get("MISSING_DAILY_PRICE", 0),
        "missing_daily_scores_count": missing_reason_counts.get("NO_PRIOR_SCORE_DATE", 0),
        "top_missing_reasons": dict(missing_reason_counts.most_common(10)),
        "bars_per_day_estimate": bars_estimate,
    }
    return {
        "start_date": start_date,
        "end_date": end_date,
        "market_symbol": market_symbol,
        "top_n": int(top_n),
        "timeframe": timeframe,
        "allow_same_day_score": bool(allow_same_day_score),
        "calendar_source": calendar_source,
        "calendar_inferred": calendar_inferred,
        "trade_dates": trade_dates,
        "dates_with_prior_score": dates_with_prior_score,
        "dates_without_prior_score": dates_without_prior_score,
        "date_reports": date_reports,
        "required_intraday": [r.__dict__ for r in required_rows],
        "missing_data": [r.__dict__ for r in missing_rows],
        "summary": summary,
        "estimation_note": "Intraday row estimates use regular KRX session assumptions: 5m ~= 78 bars/day, 15m ~= 26 bars/day.",
    }


def write_required_intraday_csv(path: str | Path, rows: list[dict[str, Any]]) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    columns = ["date", "symbol", "timeframe", "source_type", "score_date", "rank", "score", "required_reason"]
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})
    return out


def write_missing_data_csv(path: str | Path, rows: list[dict[str, Any]]) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    columns = ["date", "symbol", "data_type", "timeframe", "reason"]
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})
    return out


def build_day_replay_dataset_plan_markdown(plan: dict[str, Any]) -> str:
    summary = plan.get("summary", {})
    lines = [
        f"# DAY Replay Dataset Plan ({plan.get('start_date')} ~ {plan.get('end_date')})",
        "",
        "## Configuration",
        f"- market_symbol: {plan.get('market_symbol')}",
        f"- top_n: {plan.get('top_n')}",
        f"- timeframe: {plan.get('timeframe')}",
        f"- allow_same_day_score: {plan.get('allow_same_day_score')}",
        f"- calendar_source: {plan.get('calendar_source')}",
        f"- calendar_inferred: {plan.get('calendar_inferred')}",
        f"- estimation_note: {plan.get('estimation_note')}",
        "",
        "## Summary",
    ]
    for key, value in summary.items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            f"- dates_with_prior_score: {plan.get('dates_with_prior_score', [])}",
            f"- dates_without_prior_score: {plan.get('dates_without_prior_score', [])}",
            "",
            "## Date Detail",
            "| date | score_date | candidates | usable | market_proxy | replay_candidate | failures |",
            "| --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for trade_date, detail in sorted(plan.get("date_reports", {}).items()):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(trade_date),
                    str(detail.get("score_date")),
                    str(detail.get("candidate_count", 0)),
                    str(detail.get("candidate_usable_symbol_count", 0)),
                    str(detail.get("market_proxy_intraday_available")),
                    str(detail.get("replay_candidate")),
                    str(detail.get("failure_reasons", [])),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Missing Data",
            f"- top_missing_reasons: {summary.get('top_missing_reasons', {})}",
            f"- missing_intraday_count: {summary.get('missing_intraday_count', 0)}",
            f"- missing_market_proxy_count: {summary.get('missing_market_proxy_count', 0)}",
            f"- missing_daily_prices_count: {summary.get('missing_daily_prices_count', 0)}",
            f"- missing_daily_scores_count: {summary.get('missing_daily_scores_count', 0)}",
            "",
            "## Next Commands",
            "1. Bootstrap DB if needed: `python3 scripts/bootstrap_market_db.py --db data/market_pipeline.db`",
            "2. Load daily prices: `python3 scripts/load_daily_prices.py --db data/market_pipeline.db --csv data/daily_prices.csv`",
            "3. Load daily scores: `python3 scripts/load_daily_scores.py --db data/market_pipeline.db --csv data/daily_scores.csv --trade-start-date START --trade-end-date END`",
            "4. Use the required intraday CSV to collect candidate and market proxy intraday bars.",
            "5. Load intraday bars, run availability audit, then start with a 3-5 trading day replay before expanding.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_day_replay_dataset_plan_report(path: str | Path, markdown: str) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(markdown, encoding="utf-8")
    return out
