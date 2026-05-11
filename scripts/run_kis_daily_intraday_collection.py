#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.availability import (  # noqa: E402
    build_day_data_availability_markdown,
    build_day_data_availability_report,
    write_day_data_availability_report,
)
from pipeline.day_trading.backtest import run_day_replay_backtest  # noqa: E402
from pipeline.day_trading.config import DayTradingConfig  # noqa: E402
from pipeline.day_trading.data_loader import load_intraday_prices_csv  # noqa: E402
from pipeline.day_trading.data_quality import validate_intraday_prices  # noqa: E402
from pipeline.day_trading.kis_client import (  # noqa: E402
    DEFAULT_KIS_TOKEN_CACHE_PATH,
    KisClientError,
    KisEndpointBlocked,
    KisQuotationClient,
    load_kis_env,
    write_intraday_bars_csv,
)
from pipeline.day_trading.models import IntradayBar  # noqa: E402
from pipeline.day_trading.reporting import build_day_validation_markdown, write_day_validation_report  # noqa: E402
from pipeline.day_trading.universe import DayUniverseProvider  # noqa: E402
from pipeline.day_trading.validation import DayValidationGate  # noqa: E402
from pipeline.db import get_connection, init_db  # noqa: E402
from scripts.run_day_replay_backtest import _build_policy_comparison_markdown, _summarize_policy_result  # noqa: E402
from scripts.fetch_kis_intraday_prices import _collect_stock_symbol, _redact, _remap_symbol  # noqa: E402

ZERO_VOLUME_POLICIES = ["strict_invalid", "no_trade_context", "drop_no_trade"]
PAPER_ACCOUNT_ARG_NAMES = [
    "paper_initial_cash_krw",
    "paper_notional_per_trade_krw",
    "paper_max_total_exposure_krw",
    "paper_max_position_value_krw",
    "paper_daily_loss_limit_krw",
    "paper_daily_loss_limit_pct",
]


def _json_dump(payload: dict[str, Any], *, stream: Any = None) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream or sys.stdout)


def _paper_config_kwargs(args: argparse.Namespace) -> dict[str, float]:
    return {
        name: float(value)
        for name in PAPER_ACCOUNT_ARG_NAMES
        if (value := getattr(args, name, None)) is not None
    }


def _fail(status_code: int, payload: dict[str, Any]) -> None:
    _json_dump(payload)
    raise SystemExit(status_code)


def _build_collection_plan(
    conn,
    *,
    trade_date: str,
    market_symbol: str,
    top_n: int,
    max_symbols: int | None,
) -> dict[str, Any]:
    cfg = DayTradingConfig(max_universe_symbols=top_n, market_proxy_symbol=market_symbol)
    provider = DayUniverseProvider(conn, cfg)
    selection = provider.get_universe_selection(trade_date)
    all_candidates = list(selection.candidates)
    candidates = list(all_candidates)
    if max_symbols is not None:
        candidates = candidates[: int(max_symbols)]
    blocked_reasons: list[str] = []
    if not selection.score_date:
        blocked_reasons.extend(selection.reason_codes or ["NO_PRIOR_SCORE_DATE"])
    if selection.same_day_score_used:
        blocked_reasons.append("SAME_DAY_SCORE_FORBIDDEN")
    if not candidates:
        blocked_reasons.append("EMPTY_CANDIDATES")
    targets = [
        {"date": trade_date, "symbol": symbol, "source_type": "CANDIDATE", "score_date": selection.score_date}
        for symbol in candidates
    ]
    targets.append({"date": trade_date, "symbol": market_symbol, "source_type": "MARKET_PROXY", "score_date": None})
    return {
        "trade_date": trade_date,
        "score_date": selection.score_date,
        "same_day_score_used": selection.same_day_score_used,
        "lookahead_safe": selection.lookahead_safe,
        "candidate_count": len(selection.candidates),
        "all_candidate_symbols": all_candidates,
        "selected_candidate_count": len(candidates),
        "candidate_symbols": candidates,
        "market_symbol": market_symbol,
        "targets": targets,
        "blocked_reasons": sorted(set(blocked_reasons)),
    }


def _required_dates_by_symbol(symbols: list[str], market_symbol: str, trade_date: str) -> dict[str, set[str]]:
    out = {symbol: {trade_date} for symbol in symbols}
    out[market_symbol] = {trade_date}
    return out


def _intraday_conflicts(conn, bars: list[IntradayBar], *, max_rows: int = 20) -> list[dict[str, Any]]:
    conflicts: list[dict[str, Any]] = []
    for bar in bars:
        row = conn.execute(
            """
            SELECT open,high,low,close,volume,amount
            FROM intraday_prices
            WHERE symbol=? AND timeframe=? AND timestamp=?
            """,
            (bar.symbol, bar.timeframe, bar.timestamp.isoformat()),
        ).fetchone()
        if row is None:
            continue
        existing = {
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "amount": float(row["amount"]) if row["amount"] is not None else None,
        }
        incoming = {
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close),
            "volume": float(bar.volume),
            "amount": float(bar.amount) if bar.amount is not None else None,
        }
        if existing != incoming:
            conflicts.append(
                {
                    "symbol": bar.symbol,
                    "timeframe": bar.timeframe,
                    "timestamp": bar.timestamp.isoformat(),
                    "existing": existing,
                    "incoming": incoming,
                }
            )
            if len(conflicts) >= max_rows:
                break
    return conflicts


def _delete_intraday_scope(conn, bars: list[IntradayBar]) -> int:
    before = conn.total_changes
    scope = sorted({(bar.symbol, bar.timeframe, bar.timestamp.date().isoformat()) for bar in bars})
    for symbol, timeframe, trade_date in scope:
        conn.execute(
            """
            DELETE FROM intraday_prices
            WHERE symbol=? AND timeframe=? AND COALESCE(date, substr(timestamp,1,10))=?
            """,
            (symbol, timeframe, trade_date),
        )
    conn.commit()
    return conn.total_changes - before


def _collection_session_summary(bars: list[IntradayBar], trade_date: str, market_symbol: str) -> dict[str, Any]:
    by_symbol: dict[str, dict[str, list[IntradayBar]]] = {}
    for bar in bars:
        if bar.timestamp.date().isoformat() != trade_date:
            continue
        by_symbol.setdefault(bar.symbol, {}).setdefault(bar.timeframe, []).append(bar)
    symbol_summary: dict[str, Any] = {}
    partial_symbols: list[str] = []
    for symbol, by_tf in sorted(by_symbol.items()):
        primary = sorted(by_tf.get("5m", []), key=lambda b: b.timestamp)
        if not primary:
            partial_symbols.append(symbol)
            symbol_summary[symbol] = {"bar_count_5m": 0, "first": None, "last": None, "partial_session": True}
            continue
        first = primary[0].timestamp.time().isoformat()
        last = primary[-1].timestamp.time().isoformat()
        partial = first > "09:00:00" or last < "15:10:00"
        if partial:
            partial_symbols.append(symbol)
        symbol_summary[symbol] = {
            "bar_count_5m": len(primary),
            "first": primary[0].timestamp.isoformat(),
            "last": primary[-1].timestamp.isoformat(),
            "partial_session": partial,
        }
    return {
        "symbol_count": len(symbol_summary),
        "market_symbol": market_symbol,
        "market_proxy_collected": market_symbol in symbol_summary,
        "partial_session": bool(partial_symbols),
        "partial_symbols": partial_symbols,
        "symbols": symbol_summary,
    }


def _existing_intraday_symbols(conn, trade_date: str, symbols: list[str], timeframe: str) -> list[str]:
    if not symbols:
        return []
    rows = conn.execute(
        f"""
        SELECT DISTINCT symbol
        FROM intraday_prices
        WHERE COALESCE(date, substr(timestamp,1,10))=?
          AND timeframe=?
          AND symbol IN ({",".join("?" for _ in symbols)})
        ORDER BY symbol
        """,
        [trade_date, timeframe, *symbols],
    ).fetchall()
    return [str(row["symbol"]) for row in rows]


def _build_coverage_audit(conn, plan: dict[str, Any], *, timeframe: str, replay_collected_only: bool, require_full_top_n_coverage: bool) -> dict[str, Any]:
    requested = list(plan.get("all_candidate_symbols", plan.get("candidate_symbols", [])))
    collected = _existing_intraday_symbols(conn, str(plan["trade_date"]), requested, timeframe)
    missing = [symbol for symbol in requested if symbol not in set(collected)]
    requested_count = len(requested)
    collected_count = len(collected)
    return {
        "requested_top_n": int(plan.get("candidate_count", 0)),
        "selected_for_collection_count": requested_count,
        "collected_symbol_count": collected_count,
        "collected_symbols": collected,
        "missing_intraday_symbols": missing,
        "missing_intraday_symbol_count": len(missing),
        "intraday_coverage_ratio": (collected_count / requested_count) if requested_count else 0.0,
        "partial_universe": bool(collected_count < int(plan.get("candidate_count", 0))),
        "replay_collected_only": bool(replay_collected_only),
        "require_full_top_n_coverage": bool(require_full_top_n_coverage),
        "market_proxy_symbol": plan.get("market_symbol"),
    }


def _invalid_bar_breakdown(conn, *, trade_date: str, symbols: list[str], timeframes: list[str], max_samples: int = 10) -> dict[str, Any]:
    if not symbols:
        return {
            "invalid_bar_count_by_symbol": {},
            "invalid_bar_count_by_timeframe": {},
            "invalid_bar_count_by_reason": {},
            "invalid_bar_sample_rows": [],
            "zero_volume_count": 0,
            "invalid_ohlc_count": 0,
            "missing_price_count": 0,
            "incomplete_aggregation_count": 0,
            "zero_volume_positive_amount_count": 0,
            "positive_volume_zero_amount_count": 0,
            "estimated_traded_value_count": 0,
            "cumulative_volume_diff_used": False,
            "cumulative_amount_diff_used": False,
            "negative_cumulative_diff_count": 0,
            "raw_volume_field_used": None,
            "raw_amount_field_used": None,
            "raw_price_fields_used": {},
            "invalid_mapping_sample_rows": [],
            "invalid_bar_count_by_timestamp": {},
            "zero_volume_cause_counts": {},
            "first_invalid_timestamp": None,
            "last_invalid_timestamp": None,
        }
    rows = conn.execute(
        f"""
        SELECT symbol,timeframe,timestamp,open,high,low,close,volume,amount,source
        FROM intraday_prices
        WHERE COALESCE(date, substr(timestamp,1,10))=?
          AND timeframe IN ({",".join("?" for _ in timeframes)})
          AND symbol IN ({",".join("?" for _ in symbols)})
        ORDER BY symbol,timeframe,timestamp
        """,
        [trade_date, *timeframes, *symbols],
    ).fetchall()
    by_symbol: Counter[str] = Counter()
    by_timeframe: Counter[str] = Counter()
    by_reason: Counter[str] = Counter()
    by_timestamp: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    mapping_samples: list[dict[str, Any]] = []
    invalid_timestamps: list[str] = []
    zero_volume_positive_amount_count = 0
    positive_volume_zero_amount_count = 0
    estimated_traded_value_count = 0
    cumulative_volume_diff_used = False
    cumulative_amount_diff_used = False
    raw_volume_fields: Counter[str] = Counter()
    raw_amount_fields: Counter[str] = Counter()
    zero_volume_causes: Counter[str] = Counter()

    def record(row: Any, reason: str) -> None:
        symbol = str(row["symbol"])
        timeframe = str(row["timeframe"])
        ts = str(row["timestamp"])
        by_symbol[symbol] += 1
        by_timeframe[timeframe] += 1
        by_reason[reason] += 1
        by_timestamp[ts] += 1
        invalid_timestamps.append(ts)
        if len(samples) < max_samples:
            samples.append(
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": ts,
                    "reason": reason,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "amount": row["amount"],
                    "source": row["source"],
                }
            )

    for row in rows:
        values = [row["open"], row["high"], row["low"], row["close"]]
        if any(v is None for v in values):
            record(row, "missing_price")
            continue
        open_px = float(row["open"])
        high_px = float(row["high"])
        low_px = float(row["low"])
        close_px = float(row["close"])
        volume = row["volume"]
        amount = row["amount"]
        source = str(row["source"] or "")
        for part in source.split("|"):
            if part.startswith("VOL_FIELD="):
                raw_volume_fields[part.removeprefix("VOL_FIELD=")] += 1
            elif part.startswith("AMOUNT_FIELD="):
                raw_amount_fields[part.removeprefix("AMOUNT_FIELD=")] += 1
        if "ESTIMATED_TRADE_VALUE" in source:
            estimated_traded_value_count += 1
        if "CUMULATIVE_VOLUME_DIFF" in source:
            cumulative_volume_diff_used = True
        if "CUMULATIVE_AMOUNT_DIFF" in source:
            cumulative_amount_diff_used = True
        if min(open_px, high_px, low_px, close_px) <= 0.0:
            record(row, "non_positive_ohlc")
        elif high_px < max(open_px, close_px) or low_px > min(open_px, close_px):
            record(row, "invalid_ohlc")
        if volume is None:
            record(row, "missing_volume")
        elif float(volume) == 0.0:
            record(row, "zero_volume")
            if min(open_px, high_px, low_px, close_px) == 0.0:
                zero_volume_causes["ZERO_VOLUME_ZERO_PRICE"] += 1
            else:
                zero_volume_causes["ZERO_VOLUME_VALID_PRICE"] += 1
            if "VOL_FIELD=NONE" in source:
                zero_volume_causes["UNKNOWN_VOLUME_MAPPING"] += 1
            if "CUMULATIVE_VOLUME_DIFF" in source:
                zero_volume_causes["CUMULATIVE_DIFF_ZERO"] += 1
        elif float(volume) < 0.0:
            record(row, "negative_volume")
        if volume is not None and amount is not None:
            if float(volume) == 0.0 and float(amount) > 0.0:
                zero_volume_positive_amount_count += 1
                if len(mapping_samples) < max_samples:
                    mapping_samples.append(
                        {
                            "symbol": row["symbol"],
                            "timeframe": row["timeframe"],
                            "timestamp": row["timestamp"],
                            "reason": "zero_volume_positive_amount",
                            "volume": row["volume"],
                            "amount": row["amount"],
                            "source": row["source"],
                        }
                    )
            elif float(volume) > 0.0 and float(amount) == 0.0:
                positive_volume_zero_amount_count += 1
                if len(mapping_samples) < max_samples:
                    mapping_samples.append(
                        {
                            "symbol": row["symbol"],
                            "timeframe": row["timeframe"],
                            "timestamp": row["timestamp"],
                            "reason": "positive_volume_zero_amount",
                            "volume": row["volume"],
                            "amount": row["amount"],
                            "source": row["source"],
                        }
                    )

    five_rows = conn.execute(
        f"""
        SELECT symbol,timestamp
        FROM intraday_prices
        WHERE COALESCE(date, substr(timestamp,1,10))=?
          AND timeframe='5m'
          AND symbol IN ({",".join("?" for _ in symbols)})
        ORDER BY symbol,timestamp
        """,
        [trade_date, *symbols],
    ).fetchall()
    five_by_symbol: dict[str, list[str]] = {}
    for row in five_rows:
        five_by_symbol.setdefault(str(row["symbol"]), []).append(str(row["timestamp"]))
    for row in rows:
        if str(row["timeframe"]) != "15m":
            continue
        ts = str(row["timestamp"])
        start = datetime.fromisoformat(ts)
        end = start + timedelta(minutes=15)
        count = sum(1 for raw in five_by_symbol.get(str(row["symbol"]), []) if start <= datetime.fromisoformat(raw) < end)
        if count < 3:
            record(row, "incomplete_aggregation")
            zero_volume_causes["INCOMPLETE_BAR"] += 1

    return {
        "invalid_bar_count_by_symbol": dict(by_symbol),
        "invalid_bar_count_by_timeframe": dict(by_timeframe),
        "invalid_bar_count_by_reason": dict(by_reason),
        "invalid_bar_sample_rows": samples,
        "zero_volume_count": int(by_reason.get("zero_volume", 0)),
        "invalid_ohlc_count": int(by_reason.get("invalid_ohlc", 0) + by_reason.get("non_positive_ohlc", 0)),
        "missing_price_count": int(by_reason.get("missing_price", 0)),
        "incomplete_aggregation_count": int(by_reason.get("incomplete_aggregation", 0)),
        "zero_volume_positive_amount_count": zero_volume_positive_amount_count,
        "positive_volume_zero_amount_count": positive_volume_zero_amount_count,
        "estimated_traded_value_count": estimated_traded_value_count,
        "cumulative_volume_diff_used": cumulative_volume_diff_used,
        "cumulative_amount_diff_used": cumulative_amount_diff_used,
        "negative_cumulative_diff_count": 0,
        "raw_volume_field_used": dict(raw_volume_fields),
        "raw_amount_field_used": dict(raw_amount_fields),
        "raw_price_fields_used": {},
        "invalid_mapping_sample_rows": mapping_samples,
        "invalid_bar_count_by_timestamp": dict(by_timestamp),
        "zero_volume_cause_counts": dict(zero_volume_causes),
        "first_invalid_timestamp": min(invalid_timestamps) if invalid_timestamps else None,
        "last_invalid_timestamp": max(invalid_timestamps) if invalid_timestamps else None,
    }


def _combine_parse_diagnostics(collection_detail: dict[str, Any]) -> dict[str, Any]:
    diagnostics: list[dict[str, Any]] = []
    for detail in collection_detail.get("candidates", []):
        diag = detail.get("parse_diagnostics")
        if isinstance(diag, dict):
            diagnostics.append(diag)
    market_diag = (collection_detail.get("market_proxy") or {}).get("parse_diagnostics")
    if isinstance(market_diag, dict):
        diagnostics.append(market_diag)
    raw_volume_fields: Counter[str] = Counter()
    raw_amount_fields: Counter[str] = Counter()
    price_fields: dict[str, str | None] = {}
    invalid_mapping_samples: list[dict[str, Any]] = []
    zero_volume_causes: Counter[str] = Counter()
    for diag in diagnostics:
        if diag.get("raw_volume_field_used"):
            raw_volume_fields[str(diag["raw_volume_field_used"])] += 1
        if diag.get("raw_amount_field_used"):
            raw_amount_fields[str(diag["raw_amount_field_used"])] += 1
        for key, value in (diag.get("raw_price_fields_used") or {}).items():
            price_fields.setdefault(str(key), value)
        invalid_mapping_samples.extend(diag.get("invalid_mapping_sample_rows") or [])
        if int(diag.get("negative_cumulative_diff_count", 0)):
            zero_volume_causes["CUMULATIVE_DIFF_NEGATIVE"] += int(diag.get("negative_cumulative_diff_count", 0))
    return {
        "raw_volume_field_used": dict(raw_volume_fields),
        "raw_amount_field_used": dict(raw_amount_fields),
        "raw_price_fields_used": price_fields,
        "cumulative_volume_diff_used": any(bool(diag.get("cumulative_volume_diff_used")) for diag in diagnostics),
        "cumulative_amount_diff_used": any(bool(diag.get("cumulative_amount_diff_used")) for diag in diagnostics),
        "estimated_traded_value_count": sum(int(diag.get("estimated_traded_value_count", 0)) for diag in diagnostics),
        "negative_cumulative_diff_count": sum(int(diag.get("negative_cumulative_diff_count", 0)) for diag in diagnostics),
        "zero_volume_positive_amount_count": sum(int(diag.get("zero_volume_positive_amount_count", 0)) for diag in diagnostics),
        "positive_volume_zero_amount_count": sum(int(diag.get("positive_volume_zero_amount_count", 0)) for diag in diagnostics),
        "invalid_mapping_sample_rows": invalid_mapping_samples[:10],
        "parse_zero_volume_cause_counts": dict(zero_volume_causes),
    }


def _run_replay_and_gate(conn, args: argparse.Namespace) -> dict[str, Any]:
    replay_top_n = int(args.top_n)
    candidate_overrides = None
    if getattr(args, "replay_collected_only_effective", False):
        replay_top_n = len(args.replay_candidate_symbols)
        candidate_overrides = {args.trade_date: list(args.replay_candidate_symbols)}
    cfg = DayTradingConfig(
        enabled=True,
        mode="PAPER",
        market_proxy_symbol=args.market_symbol,
        max_universe_symbols=replay_top_n,
        zero_volume_bar_policy=args.zero_volume_bar_policy,
        **_paper_config_kwargs(args),
    )
    quality = validate_intraday_prices(
        conn,
        start_date=args.trade_date,
        end_date=args.trade_date,
        market_proxy_symbol=args.market_symbol,
    )
    availability = build_day_data_availability_report(
        conn,
        start_date=args.trade_date,
        end_date=args.trade_date,
        market_proxy_symbol=args.market_symbol,
        max_universe_symbols=replay_top_n,
    )
    replay = run_day_replay_backtest(conn, args.trade_date, args.trade_date, config=cfg, candidate_overrides_by_date=candidate_overrides)
    replay["coverage_audit"] = getattr(args, "coverage_audit", {})
    replay["invalid_bar_analysis"] = getattr(args, "invalid_bar_analysis", {})
    replay["data_availability"] = {
        "summary": availability.get("summary", {}),
        "replayable_dates": availability.get("replayable_dates", []),
        "unreplayable_dates": availability.get("unreplayable_dates", {}),
    }
    for trade_date, date_summary in replay.get("per_date_summary", {}).items():
        date_availability = availability.get("date_reports", {}).get(trade_date, {})
        date_summary["candidate_usable_symbol_count"] = date_availability.get("candidate_usable_symbol_count", 0)
        date_summary["replayable_by_data_audit"] = date_availability.get("replayable", False)
        date_summary["data_audit_failure_reasons"] = date_availability.get("failure_reasons", [])
    lookahead = replay.get("lookahead_validation", {})
    audit = replay.get("session_audit", {})
    event_counts = replay.get("event_counts", {})
    gate = DayValidationGate().evaluate(
        replay.get("performance", {}),
        observed_days=len(replay.get("candidate_counts", {})),
        data_quality_passed=quality.get("candidate_usable_symbol_count", 0) > 0,
        lookahead_passed=(lookahead.get("future_candle_violations", 0) == 0 and lookahead.get("lookahead_score_violations", 0) == 0),
        market_proxy_available=bool(quality.get("market_proxy_available")),
        session_complete=audit.get("session_complete"),
        missing_force_exit_window=audit.get("missing_force_exit_window"),
        open_position_count_at_end=audit.get("open_position_count_at_end"),
        paper_entry_count=event_counts.get("PAPER_ENTRY", 0),
        paper_exit_count=event_counts.get("PAPER_EXIT", 0),
    )
    if args.replay_report_md:
        md = build_day_validation_markdown(
            replay,
            quality,
            gate,
            universe_source=cfg.universe_source,
            same_day_scores_allowed=cfg.allow_same_day_scores,
            market_proxy_symbol=cfg.market_proxy_symbol,
        )
        write_day_validation_report(args.replay_report_md, md)
    return {"data_quality": quality, "data_availability": availability, "replay": replay, "promotion_gate": gate}


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect one DAY trade_date of KIS quote-only intraday data and optionally replay it")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--market-symbol", default="069500")
    parser.add_argument("--market-proxy-source", choices=["ETF"], default="ETF")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--confirm-timeframe", default="15m")
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--audit-report-md", default=None)
    parser.add_argument("--replay-report-md", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.12)
    parser.add_argument("--skip-replay", action="store_true")
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--replay-collected-only", action="store_true", help="Replay only symbols collected by this run; marks the report as partial_universe")
    parser.add_argument("--require-full-top-n-coverage", action="store_true", help="Block replay unless every requested top-n candidate has intraday coverage")
    parser.add_argument("--zero-volume-bar-policy", choices=ZERO_VOLUME_POLICIES, default="strict_invalid")
    parser.add_argument("--compare-zero-volume-policies", action="store_true", help="Replay the collected day under all zero-volume policies")
    parser.add_argument("--paper-initial-cash-krw", dest="paper_initial_cash_krw", type=float, default=None)
    parser.add_argument("--paper-notional-per-trade-krw", dest="paper_notional_per_trade_krw", type=float, default=None)
    parser.add_argument("--paper-max-total-exposure-krw", dest="paper_max_total_exposure_krw", type=float, default=None)
    parser.add_argument("--paper-max-position-value-krw", dest="paper_max_position_value_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-krw", dest="paper_daily_loss_limit_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-pct", dest="paper_daily_loss_limit_pct", type=float, default=None)
    parser.add_argument("--env", choices=["paper", "real"], default=None)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--input-hour", default="153000")
    parser.add_argument("--session-start", default="090000")
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--force-refresh-token", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    output_csv = Path(args.output_csv or f"data/intraday_kis_{args.trade_date}.csv")
    summary: dict[str, Any] = {
        "status": "ok",
        "db": str(db_path),
        "trade_date": args.trade_date,
        "market_symbol": args.market_symbol,
        "market_proxy_source": args.market_proxy_source,
        "output_csv": str(output_csv),
        "dry_run": args.dry_run,
        "force_refresh": args.force_refresh,
        "replay_collected_only": bool(args.replay_collected_only),
        "require_full_top_n_coverage": bool(args.require_full_top_n_coverage),
        "zero_volume_bar_policy": args.zero_volume_bar_policy,
        "compare_zero_volume_policies": bool(args.compare_zero_volume_policies),
    }
    if not db_path.exists():
        _fail(2, {**summary, "status": "blocked", "blocked_reason": "DB_MISSING"})

    conn = get_connection(db_path)
    init_db(conn)
    plan = _build_collection_plan(
        conn,
        trade_date=args.trade_date,
        market_symbol=args.market_symbol,
        top_n=args.top_n,
        max_symbols=args.max_symbols,
    )
    warnings: list[str] = []
    if args.max_symbols is not None and int(args.max_symbols) < int(args.top_n):
        warnings.append("MAX_SYMBOLS_BELOW_TOP_N_PARTIAL_UNIVERSE")
    args.replay_collected_only_effective = bool(args.replay_collected_only or (args.max_symbols is not None and int(args.max_symbols) < int(args.top_n) and not args.require_full_top_n_coverage))
    if args.replay_collected_only_effective and not args.replay_collected_only:
        warnings.append("REPLAY_COLLECTED_ONLY_AUTO_ENABLED_FOR_SMOKE")
    summary["warnings"] = warnings
    summary["collection_plan"] = plan
    if plan["blocked_reasons"]:
        conn.close()
        _fail(1, {**summary, "status": "blocked", "blocked_reasons": plan["blocked_reasons"]})
    if args.dry_run:
        summary["coverage_audit"] = {
            "requested_top_n": int(plan.get("candidate_count", 0)),
            "selected_for_collection_count": int(plan.get("selected_candidate_count", 0)),
            "collected_symbol_count": 0,
            "collected_symbols": [],
            "missing_intraday_symbols": list(plan.get("all_candidate_symbols", plan.get("candidate_symbols", []))),
            "missing_intraday_symbol_count": int(plan.get("candidate_count", 0)),
            "intraday_coverage_ratio": 0.0,
            "partial_universe": bool(int(plan.get("selected_candidate_count", 0)) < int(plan.get("candidate_count", 0))),
            "replay_collected_only": bool(args.replay_collected_only_effective),
            "require_full_top_n_coverage": bool(args.require_full_top_n_coverage),
            "market_proxy_symbol": args.market_symbol,
        }
        conn.close()
        _json_dump({**summary, "status": "dry_run"})
        return

    env_result = load_kis_env(args.env_file, env_override=args.env)
    if env_result.status != "ok" or env_result.config is None:
        conn.close()
        _fail(2, {**summary, "status": "blocked", "blocked_reason": env_result.reason, **env_result.to_dict()})
    secrets = [env_result.config.app_key, env_result.config.app_secret]
    summary["env"] = env_result.config.safe_summary()

    client = KisQuotationClient(
        env_result.config,
        dry_run=False,
        sleep_seconds=args.sleep_seconds,
        token_cache_path=args.token_cache,
    )
    try:
        client.get_access_token(force_refresh=args.force_refresh_token)
        summary["token_status"] = "ok"
    except Exception as exc:
        conn.close()
        _fail(1, {**summary, "status": "blocked", "token_status": "blocked", "blocked_reason": _redact(str(exc), secrets)})

    required_dates = _required_dates_by_symbol(plan["candidate_symbols"], args.market_symbol, args.trade_date)
    collected: list[IntradayBar] = []
    collection_detail: dict[str, Any] = {"candidates": [], "market_proxy": None}
    blocked_reasons: list[str] = []
    for symbol in plan["candidate_symbols"]:
        try:
            bars, detail = _collect_stock_symbol(
                client,
                symbol,
                timeframe=args.timeframe,
                confirm_timeframe=args.confirm_timeframe,
                input_hour=args.input_hour,
                session_start=args.session_start,
                max_pages=args.max_pages,
                required_dates_by_symbol=required_dates,
            )
            collected.extend(bars)
            collection_detail["candidates"].append(detail)
            if not bars:
                blocked_reasons.append(f"NO_INTRADAY_ROWS_FOR_REQUIRED_DATES:{symbol}")
        except (KisClientError, KisEndpointBlocked) as exc:
            blocked_reasons.append(f"CANDIDATE_COLLECTION_FAILED:{symbol}")
            collection_detail["candidates"].append({"symbol": symbol, "status": "blocked", "blocked_reason": _redact(str(exc), secrets)})

    try:
        proxy_bars, proxy_detail = _collect_stock_symbol(
            client,
            args.market_symbol,
            timeframe=args.timeframe,
            confirm_timeframe=args.confirm_timeframe,
            input_hour=args.input_hour,
            session_start=args.session_start,
            max_pages=args.max_pages,
            required_dates_by_symbol=required_dates,
        )
        proxy_bars = _remap_symbol(proxy_bars, args.market_symbol, source="KIS_QUOTATION_MARKET_PROXY_ETF")
        collected.extend(proxy_bars)
        proxy_detail.update({"source_type": "ETF", "proxy_output_symbol": args.market_symbol})
        collection_detail["market_proxy"] = proxy_detail
        if not proxy_bars:
            blocked_reasons.append("MARKET_PROXY_MISSING_OR_UNUSABLE")
    except (KisClientError, KisEndpointBlocked) as exc:
        blocked_reasons.append("MARKET_PROXY_MISSING_OR_UNUSABLE")
        collection_detail["market_proxy"] = {"status": "blocked", "blocked_reason": _redact(str(exc), secrets)}

    summary["collection"] = collection_detail
    summary["collection_session"] = _collection_session_summary(collected, args.trade_date, args.market_symbol)
    if blocked_reasons:
        conn.close()
        _fail(1, {**summary, "status": "blocked", "blocked_reasons": sorted(set(blocked_reasons))})

    conflicts = _intraday_conflicts(conn, collected)
    summary["intraday_conflict_count"] = len(conflicts)
    if conflicts and not args.force_refresh:
        conn.close()
        _fail(
            1,
            {
                **summary,
                "status": "blocked",
                "blocked_reason": "INTRADAY_OHLCV_CONFLICT",
                "conflicts_sample": conflicts,
            },
        )
    if args.force_refresh:
        summary["force_refresh_deleted_rows"] = _delete_intraday_scope(conn, collected)

    write_intraday_bars_csv(output_csv, collected)
    load_result = load_intraday_prices_csv(conn, output_csv, default_timeframe=args.timeframe, source="KIS_QUOTATION_DAILY_COLLECTION")
    summary["db_load"] = load_result.__dict__

    quality = validate_intraday_prices(
        conn,
        start_date=args.trade_date,
        end_date=args.trade_date,
        market_proxy_symbol=args.market_symbol,
    )
    summary["data_quality"] = quality
    coverage_audit = _build_coverage_audit(
        conn,
        plan,
        timeframe=args.timeframe,
        replay_collected_only=bool(args.replay_collected_only_effective),
        require_full_top_n_coverage=bool(args.require_full_top_n_coverage),
    )
    coverage_audit["market_proxy_available"] = bool(quality.get("market_proxy_available"))
    summary["coverage_audit"] = coverage_audit
    args.coverage_audit = coverage_audit
    args.replay_candidate_symbols = list(coverage_audit["collected_symbols"])
    if args.require_full_top_n_coverage and int(coverage_audit["missing_intraday_symbol_count"]) > 0:
        summary["status"] = "blocked"
        summary["blocked_reason"] = "FULL_TOP_N_INTRADAY_COVERAGE_REQUIRED"
        conn.close()
        _json_dump(summary)
        raise SystemExit(1)
    if args.replay_collected_only_effective and not args.replay_candidate_symbols:
        summary["status"] = "blocked"
        summary["blocked_reason"] = "NO_COLLECTED_CANDIDATES_FOR_REPLAY"
        conn.close()
        _json_dump(summary)
        raise SystemExit(1)
    invalid_breakdown_symbols = sorted(set(plan.get("candidate_symbols", []) + [args.market_symbol]))
    invalid_breakdown = _invalid_bar_breakdown(
        conn,
        trade_date=args.trade_date,
        symbols=invalid_breakdown_symbols,
        timeframes=[args.timeframe, args.confirm_timeframe],
    )
    parse_breakdown = _combine_parse_diagnostics(collection_detail)
    invalid_breakdown.update(parse_breakdown)
    cause_counts = Counter(invalid_breakdown.get("zero_volume_cause_counts") or {})
    cause_counts.update(parse_breakdown.get("parse_zero_volume_cause_counts") or {})
    invalid_breakdown["zero_volume_cause_counts"] = dict(cause_counts)
    summary["invalid_bar_analysis"] = invalid_breakdown
    args.invalid_bar_analysis = invalid_breakdown
    availability = build_day_data_availability_report(
        conn,
        start_date=args.trade_date,
        end_date=args.trade_date,
        market_proxy_symbol=args.market_symbol,
        max_universe_symbols=(len(args.replay_candidate_symbols) if args.replay_collected_only_effective else args.top_n),
    )
    summary["availability"] = {
        "summary": availability.get("summary", {}),
        "replayable_dates": availability.get("replayable_dates", []),
        "unreplayable_dates": availability.get("unreplayable_dates", {}),
        "date_report": availability.get("date_reports", {}).get(args.trade_date, {}),
    }
    if args.audit_report_md:
        write_day_data_availability_report(args.audit_report_md, build_day_data_availability_markdown(availability))
        summary["audit_report_path"] = args.audit_report_md

    if args.skip_replay:
        summary["replay"] = {"status": "skipped", "skip_reason": "SKIP_REPLAY_REQUESTED"}
    elif args.trade_date in availability.get("replayable_dates", []):
        if args.compare_zero_volume_policies:
            replay_by_policy: dict[str, Any] = {}
            comparison_rows: list[dict[str, Any]] = []
            original_policy = args.zero_volume_bar_policy
            original_report = args.replay_report_md
            args.replay_report_md = None
            for policy in ZERO_VOLUME_POLICIES:
                args.zero_volume_bar_policy = policy
                bundle = _run_replay_and_gate(conn, args)
                comparison_rows.append(_summarize_policy_result(policy, bundle["replay"], bundle["promotion_gate"]))
                replay_by_policy[policy] = {
                    "event_counts": bundle["replay"].get("event_counts", {}),
                    "session_audit": bundle["replay"].get("session_audit", {}),
                    "performance": bundle["replay"].get("performance", {}),
                    "paper_account": bundle["replay"].get("paper_account", {}),
                    "trade_details": bundle["replay"].get("trade_details", []),
                    "rejection_analysis": bundle["replay"].get("rejection_analysis", {}),
                    "zero_volume_policy_summary": bundle["replay"].get("zero_volume_policy_summary", {}),
                    "promotion_gate": bundle["promotion_gate"],
                }
            args.zero_volume_bar_policy = original_policy
            args.replay_report_md = original_report
            summary["replay"] = {"status": "ok", "policy_comparison": comparison_rows, "policy_details": replay_by_policy}
            if args.replay_report_md:
                report_path = Path(args.replay_report_md)
                report_path.parent.mkdir(parents=True, exist_ok=True)
                report_path.write_text(
                    _build_policy_comparison_markdown(
                        {
                            "start_date": args.trade_date,
                            "end_date": args.trade_date,
                            "policy_comparison": comparison_rows,
                        }
                    ),
                    encoding="utf-8",
                )
                summary["replay_report_path"] = args.replay_report_md
        else:
            replay_bundle = _run_replay_and_gate(conn, args)
            replay_bundle["replay"]["coverage_audit"] = coverage_audit
            replay_bundle["replay"]["invalid_bar_analysis"] = invalid_breakdown
            summary["replay"] = {
                "status": replay_bundle["replay"].get("status"),
                "score_dates": replay_bundle["replay"].get("score_dates"),
                "event_counts": replay_bundle["replay"].get("event_counts", {}),
                "session_audit": replay_bundle["replay"].get("session_audit", {}),
                "per_date_summary": replay_bundle["replay"].get("per_date_summary", {}),
                "rejection_analysis": replay_bundle["replay"].get("rejection_analysis", {}),
                "performance": replay_bundle["replay"].get("performance", {}),
                "paper_account": replay_bundle["replay"].get("paper_account", {}),
                "trade_details": replay_bundle["replay"].get("trade_details", []),
                "coverage_audit": coverage_audit,
                "invalid_bar_analysis": invalid_breakdown,
                "zero_volume_policy_summary": replay_bundle["replay"].get("zero_volume_policy_summary", {}),
                "promotion_gate": replay_bundle["promotion_gate"],
            }
            if args.replay_report_md:
                summary["replay_report_path"] = args.replay_report_md
    else:
        summary["replay"] = {
            "status": "blocked",
            "blocked_reason": "DATA_NOT_REPLAYABLE_AFTER_COLLECTION",
            "unreplayable_dates": availability.get("unreplayable_dates", {}),
        }

    if summary.get("replay", {}).get("status") == "blocked":
        summary["status"] = "blocked"
    summary["top_blocking_reasons"] = dict(Counter(summary.get("replay", {}).get("rejection_analysis", {}).get("overall_rejection_reasons", {})).most_common(5))
    conn.close()
    _json_dump(summary)


if __name__ == "__main__":
    main()
