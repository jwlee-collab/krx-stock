#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.backtest import run_day_replay_backtest
from pipeline.day_trading.availability import build_day_data_availability_report
from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.data_quality import validate_intraday_prices
from pipeline.day_trading.reporting import build_day_validation_markdown, write_day_validation_report
from pipeline.day_trading.validation import DayValidationGate
from pipeline.db import get_connection, init_db


ZERO_VOLUME_POLICIES = ["strict_invalid", "no_trade_context", "drop_no_trade"]
PAPER_ACCOUNT_ARG_NAMES = [
    "paper_initial_cash_krw",
    "paper_notional_per_trade_krw",
    "paper_max_total_exposure_krw",
    "paper_max_position_value_krw",
    "paper_daily_loss_limit_krw",
    "paper_daily_loss_limit_pct",
]


def _fail_json(message: str, **extra: object) -> None:
    payload = {"status": "error", "message": message, **extra}
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
    raise SystemExit(2)


def _event_count(replay: dict[str, object], event_type: str) -> int:
    return int((replay.get("event_counts") or {}).get(event_type, 0))  # type: ignore[union-attr]


def _policy_metric_counts(replay: dict[str, object]) -> dict[str, int]:
    counts = {
        "vwap_available_count": 0,
        "liquidity_pass_count": 0,
        "breakout_pass_count": 0,
        "volume_expansion_pass_count": 0,
    }
    for event in replay.get("log_events", []) or []:
        raw = getattr(event, "raw_metrics", {}) or {}
        nested = raw.get("raw_metrics") if isinstance(raw, dict) else None
        metrics = nested if isinstance(nested, dict) else raw
        if not isinstance(metrics, dict):
            continue
        if metrics.get("vwap") is not None:
            counts["vwap_available_count"] += 1
        if metrics.get("liquidity_pass"):
            counts["liquidity_pass_count"] += 1
        if metrics.get("breakout_pass"):
            counts["breakout_pass_count"] += 1
        if metrics.get("volume_expansion_pass"):
            counts["volume_expansion_pass_count"] += 1
    return counts


def _paper_config_kwargs(args: argparse.Namespace) -> dict[str, float]:
    return {
        name: float(value)
        for name in PAPER_ACCOUNT_ARG_NAMES
        if (value := getattr(args, name, None)) is not None
    }


def _evaluate_gate(replay: dict[str, object], quality: dict[str, object]) -> dict[str, object]:
    lookahead = replay.get("lookahead_validation", {}) or {}
    audit = replay.get("session_audit", {}) or {}
    event_counts = replay.get("event_counts", {}) or {}
    return DayValidationGate().evaluate(
        replay.get("performance", {}) or {},
        observed_days=len(replay.get("candidate_counts", {}) or {}),
        data_quality_passed=int(quality.get("candidate_usable_symbol_count", 0) or 0) > 0,
        lookahead_passed=(int(lookahead.get("future_candle_violations", 0) or 0) == 0 and int(lookahead.get("lookahead_score_violations", 0) or 0) == 0),
        market_proxy_available=bool(quality.get("market_proxy_available")),
        session_complete=audit.get("session_complete"),
        missing_force_exit_window=audit.get("missing_force_exit_window"),
        open_position_count_at_end=audit.get("open_position_count_at_end"),
        paper_entry_count=event_counts.get("PAPER_ENTRY", 0),
        paper_exit_count=event_counts.get("PAPER_EXIT", 0),
    )


def _build_policy_comparison_markdown(summary: dict[str, object]) -> str:
    rows = summary.get("policy_comparison", []) or []
    lines = [
        f"# DAY Zero-Volume Policy Comparison ({summary.get('start_date')} ~ {summary.get('end_date')})",
        "",
        "This report compares data-quality policies for KIS zero-volume bars. It is not a profitability claim.",
        "",
        "| policy | signals | entries | exits | gross | net | cost | account_pnl_krw | account_return_pct | invalid_5m | invalid_15m | no_trade_5m | no_trade_15m | positive_bars | no_trade_bars | open_end | session_complete | promotion_stage |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("policy")),
                    str(row.get("signal_created_count", 0)),
                    str(row.get("paper_entry_count", 0)),
                    str(row.get("paper_exit_count", 0)),
                    str(row.get("gross_return_sum", 0.0)),
                    str(row.get("net_return_sum", 0.0)),
                    str(row.get("cost_impact", 0.0)),
                    str((row.get("paper_account") or {}).get("realized_pnl_krw", 0.0)),
                    str((row.get("paper_account") or {}).get("daily_return_pct", 0.0)),
                    str(row.get("INVALID_5M_BAR", 0)),
                    str(row.get("INVALID_15M_BAR", 0)),
                    str(row.get("NO_TRADE_5M_BAR", 0)),
                    str(row.get("NO_TRADE_15M_BAR", 0)),
                    str(row.get("positive_volume_bar_count", 0)),
                    str(row.get("no_trade_bar_count", 0)),
                    str(row.get("open_position_count_at_end", 0)),
                    str(row.get("session_complete")),
                    str(row.get("promotion_gate", {}).get("readiness_stage")),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Details",
        ]
    )
    for row in rows:
        lines.extend(
            [
                f"### {row.get('policy')}",
                f"- top_rejection_reasons: {row.get('top_rejection_reasons', {})}",
                f"- zero_volume_policy_summary: {row.get('zero_volume_policy_summary', {})}",
                f"- paper_account: {row.get('paper_account', {})}",
                f"- vwap_available_count: {row.get('vwap_available_count', 0)}",
                f"- liquidity_pass_count: {row.get('liquidity_pass_count', 0)}",
                f"- breakout_pass_count: {row.get('breakout_pass_count', 0)}",
                f"- volume_expansion_pass_count: {row.get('volume_expansion_pass_count', 0)}",
                f"- promotion_gate_reasons: {row.get('promotion_gate', {}).get('reasons', [])}",
                "",
            ]
        )
    return "\n".join(lines)


def _summarize_policy_result(policy: str, replay: dict[str, object], gate: dict[str, object]) -> dict[str, object]:
    perf = replay.get("performance", {}) or {}
    rejection = replay.get("rejection_analysis", {}) or {}
    reasons = rejection.get("overall_rejection_reasons", {}) if isinstance(rejection, dict) else {}
    audit = replay.get("session_audit", {}) or {}
    zero_summary = replay.get("zero_volume_policy_summary", {}) or {}
    metric_counts = _policy_metric_counts(replay)
    return {
        "policy": policy,
        "signal_created_count": _event_count(replay, "SIGNAL_CREATED"),
        "paper_entry_count": _event_count(replay, "PAPER_ENTRY"),
        "paper_exit_count": _event_count(replay, "PAPER_EXIT"),
        "gross_return_sum": perf.get("gross_return_sum", 0.0),
        "net_return_sum": perf.get("net_return_sum", 0.0),
        "cost_impact": perf.get("cost_impact", 0.0),
        "INVALID_5M_BAR": reasons.get("INVALID_5M_BAR", 0) if isinstance(reasons, dict) else 0,
        "INVALID_15M_BAR": reasons.get("INVALID_15M_BAR", 0) if isinstance(reasons, dict) else 0,
        "NO_TRADE_5M_BAR": reasons.get("NO_TRADE_5M_BAR", 0) if isinstance(reasons, dict) else 0,
        "NO_TRADE_15M_BAR": reasons.get("NO_TRADE_15M_BAR", 0) if isinstance(reasons, dict) else 0,
        "positive_volume_bar_count": zero_summary.get("positive_volume_bar_count", 0),
        "no_trade_bar_count": zero_summary.get("no_trade_bar_count", 0),
        "open_position_count_at_end": audit.get("open_position_count_at_end"),
        "session_complete": audit.get("session_complete"),
        "top_rejection_reasons": dict(list((reasons or {}).items())[:10]) if isinstance(reasons, dict) else {},
        "zero_volume_policy_summary": zero_summary,
        "paper_account": replay.get("paper_account", {}),
        "trade_details": replay.get("trade_details", []),
        "promotion_gate": gate,
        **metric_counts,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run point-in-time DAY replay backtest and validation gate")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--enable-day-trading", action="store_true", help="Required; default remains disabled")
    parser.add_argument("--market-symbol", required=True, help="Market proxy symbol stored in intraday_prices")
    parser.add_argument("--max-universe-symbols", type=int, default=20)
    parser.add_argument("--allow-same-day-scores", action="store_true")
    parser.add_argument("--score-date-override", default=None)
    parser.add_argument("--zero-volume-bar-policy", choices=ZERO_VOLUME_POLICIES, default="strict_invalid")
    parser.add_argument("--compare-zero-volume-policies", action="store_true", help="Run strict_invalid, no_trade_context, and drop_no_trade on the same replay range")
    parser.add_argument("--paper-initial-cash-krw", dest="paper_initial_cash_krw", type=float, default=None)
    parser.add_argument("--paper-notional-per-trade-krw", dest="paper_notional_per_trade_krw", type=float, default=None)
    parser.add_argument("--paper-max-total-exposure-krw", dest="paper_max_total_exposure_krw", type=float, default=None)
    parser.add_argument("--paper-max-position-value-krw", dest="paper_max_position_value_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-krw", dest="paper_daily_loss_limit_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-pct", dest="paper_daily_loss_limit_pct", type=float, default=None)
    parser.add_argument("--report-md", default=None)
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        _fail_json(
            "DB does not exist; run scripts/bootstrap_market_db.py and data loaders before replay",
            db_path=str(db_path),
            db_exists_before=False,
        )
    conn = get_connection(db_path)
    init_db(conn)
    quality = validate_intraday_prices(
        conn,
        start_date=args.start_date,
        end_date=args.end_date,
        market_proxy_symbol=args.market_symbol,
    )
    availability = build_day_data_availability_report(
        conn,
        start_date=args.start_date,
        end_date=args.end_date,
        market_proxy_symbol=args.market_symbol,
        max_universe_symbols=args.max_universe_symbols,
    )
    policies = ZERO_VOLUME_POLICIES if args.compare_zero_volume_policies else [args.zero_volume_bar_policy]
    if args.compare_zero_volume_policies:
        comparison: list[dict[str, object]] = []
        replay_status_by_policy: dict[str, object] = {}
        gate_results: dict[str, object] = {}
        for policy in policies:
            cfg = DayTradingConfig(
                enabled=bool(args.enable_day_trading),
                mode="PAPER",
                market_proxy_symbol=args.market_symbol,
                max_universe_symbols=args.max_universe_symbols,
                allow_same_day_scores=bool(args.allow_same_day_scores),
                score_date_override=args.score_date_override,
                zero_volume_bar_policy=policy,
                **_paper_config_kwargs(args),
            )
            replay = run_day_replay_backtest(conn, args.start_date, args.end_date, config=cfg)
            replay["data_availability"] = {
                "summary": availability.get("summary", {}),
                "replayable_dates": availability.get("replayable_dates", []),
                "unreplayable_dates": availability.get("unreplayable_dates", {}),
            }
            gate = _evaluate_gate(replay, quality)
            replay_status_by_policy[policy] = {
                "status": replay.get("status"),
                "event_counts": replay.get("event_counts", {}),
                "session_audit": replay.get("session_audit", {}),
                "performance": replay.get("performance", {}),
                "zero_volume_policy_summary": replay.get("zero_volume_policy_summary", {}),
            }
            gate_results[policy] = gate
            comparison.append(_summarize_policy_result(policy, replay, gate))
        summary = {
            "status": "ok",
            "start_date": args.start_date,
            "end_date": args.end_date,
            "market_symbol": args.market_symbol,
            "data_quality": quality,
            "data_availability": availability,
            "policy_comparison": comparison,
            "replay_status_by_policy": replay_status_by_policy,
            "promotion_gate_by_policy": gate_results,
        }
        if args.report_md:
            report_path = Path(args.report_md)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(_build_policy_comparison_markdown(summary), encoding="utf-8")
            summary["report_path"] = str(report_path)
        conn.close()
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        return

    cfg = DayTradingConfig(
        enabled=bool(args.enable_day_trading),
        mode="PAPER",
        market_proxy_symbol=args.market_symbol,
        max_universe_symbols=args.max_universe_symbols,
        allow_same_day_scores=bool(args.allow_same_day_scores),
        score_date_override=args.score_date_override,
        zero_volume_bar_policy=args.zero_volume_bar_policy,
        **_paper_config_kwargs(args),
    )
    replay = run_day_replay_backtest(conn, args.start_date, args.end_date, config=cfg)
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
    gate = _evaluate_gate(replay, quality)
    status = "ok" if replay.get("candidate_counts") else "blocked"
    summary = {
        "status": status,
        "blocked_reason": None if status == "ok" else "NO_REPLAY_DATES_WITH_INTRADAY_DATA",
        "data_quality": quality,
        "data_availability": availability,
        "replay": replay,
        "promotion_gate": gate,
    }
    if args.report_md:
        md = build_day_validation_markdown(
            replay,
            quality,
            gate,
            universe_source=cfg.universe_source,
            same_day_scores_allowed=cfg.allow_same_day_scores,
            market_proxy_symbol=cfg.market_proxy_symbol,
        )
        summary["report_path"] = str(write_day_validation_report(args.report_md, md))
    conn.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
