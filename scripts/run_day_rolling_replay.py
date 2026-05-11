#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.availability import build_day_data_availability_report  # noqa: E402
from pipeline.day_trading.backtest import run_day_replay_backtest  # noqa: E402
from pipeline.day_trading.config import DayTradingConfig  # noqa: E402
from pipeline.day_trading.data_quality import validate_intraday_prices  # noqa: E402
from pipeline.day_trading.reporting import build_day_validation_markdown, write_day_validation_report  # noqa: E402
from pipeline.db import get_connection, init_db  # noqa: E402
from scripts.run_day_replay_backtest import (  # noqa: E402
    PAPER_ACCOUNT_ARG_NAMES,
    ZERO_VOLUME_POLICIES,
    _build_policy_comparison_markdown,
    _evaluate_gate,
    _summarize_policy_result,
)


def _json_dump(payload: dict[str, Any], *, stream: Any = None) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream or sys.stdout)


def _parse_windows(raw: str) -> list[int]:
    windows = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        windows.append(int(value))
    return sorted(dict.fromkeys(windows))


def _paper_config_kwargs(args: argparse.Namespace) -> dict[str, float]:
    return {
        name: float(value)
        for name in PAPER_ACCOUNT_ARG_NAMES
        if (value := getattr(args, name, None)) is not None
    }


def _intraday_dates(conn, end_date: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT COALESCE(date, substr(timestamp,1,10)) AS d
        FROM intraday_prices
        WHERE COALESCE(date, substr(timestamp,1,10))<=?
        ORDER BY d
        """,
        (end_date,),
    ).fetchall()
    return [str(row["d"]) for row in rows]


def _calendar_start_for_empty_window(end_date: str, window: int) -> str:
    return (date.fromisoformat(end_date) - timedelta(days=max(1, window * 2))).isoformat()


def _compact_replay_summary(replay: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
    perf = replay.get("performance", {})
    event_counts = replay.get("event_counts", {})
    audit = replay.get("session_audit", {})
    rejection = replay.get("rejection_analysis", {}).get("overall_rejection_reasons", {})
    zero_policy = replay.get("zero_volume_policy_summary", {})
    paper_account = replay.get("paper_account", {})
    return {
        "status": replay.get("status"),
        "signal_count": int(event_counts.get("SIGNAL_CREATED", 0)),
        "entry_count": int(event_counts.get("PAPER_ENTRY", 0)),
        "exit_count": int(event_counts.get("PAPER_EXIT", 0)),
        "gross_return_sum": perf.get("gross_return_sum", 0.0),
        "net_return_sum": perf.get("net_return_sum", 0.0),
        "cost_impact": perf.get("cost_impact", 0.0),
        "win_rate": perf.get("win_rate", 0.0),
        "profit_factor": perf.get("profit_factor", 0.0),
        "max_drawdown": perf.get("max_drawdown", 0.0),
        "average_trade_return": perf.get("expectancy_per_trade", 0.0),
        "paper_account": paper_account,
        "top_rejection_reasons": dict(list(rejection.items())[:10]),
        "open_position_count_at_end": audit.get("open_position_count_at_end"),
        "session_complete": audit.get("session_complete"),
        "zero_volume_policy_summary": {
            "zero_volume_bar_policy": zero_policy.get("zero_volume_bar_policy"),
            "no_trade_bar_count": zero_policy.get("no_trade_bar_count", 0),
            "positive_volume_bar_count": zero_policy.get("positive_volume_bar_count", 0),
            "strict_invalid_count": zero_policy.get("strict_invalid_count", 0),
            "no_trade_context_count": zero_policy.get("no_trade_context_count", 0),
            "dropped_no_trade_count": zero_policy.get("dropped_no_trade_count", 0),
        },
        "promotion_gate": gate,
    }


def _partial_session_reasons(replay: dict[str, Any], trade_date: str) -> list[str]:
    audit = replay.get("session_audit", {}) or {}
    per_date = (replay.get("per_date_summary", {}) or {}).get(trade_date, {}) or {}
    reasons: list[str] = []
    if audit.get("session_complete"):
        return reasons
    if audit.get("partial_session") or per_date.get("partial_session"):
        reasons.append("PARTIAL_SESSION")
    if trade_date in (audit.get("partial_session_dates") or []):
        reasons.append("PARTIAL_SESSION_DATE")
    if trade_date in (audit.get("missing_force_exit_dates") or []):
        reasons.append("MISSING_FORCE_EXIT_WINDOW")
    if audit.get("open_position_count_at_end", 0):
        reasons.append("OPEN_POSITIONS_REMAIN")
    if per_date.get("first_bar_time") is None:
        reasons.append("NO_CANDIDATE_PRIMARY_BARS")
    if per_date.get("market_first_bar_time") is None:
        reasons.append("NO_MARKET_PROXY_PRIMARY_BARS")
    if not reasons:
        reasons.append("SESSION_INCOMPLETE")
    return sorted(set(reasons))


def _probe_session_status(
    conn,
    *,
    trade_date: str,
    market_symbol: str,
    top_n: int,
    paper_config: dict[str, float] | None,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if trade_date in cache:
        return cache[trade_date]
    cfg = DayTradingConfig(
        enabled=True,
        mode="PAPER",
        market_proxy_symbol=market_symbol,
        max_universe_symbols=top_n,
        zero_volume_bar_policy="strict_invalid",
        **(paper_config or {}),
    )
    replay = run_day_replay_backtest(conn, trade_date, trade_date, config=cfg)
    audit = replay.get("session_audit", {}) or {}
    per_date = (replay.get("per_date_summary", {}) or {}).get(trade_date, {}) or {}
    status = {
        "trade_date": trade_date,
        "session_complete": bool(audit.get("session_complete")),
        "partial_session": bool(audit.get("partial_session") or per_date.get("partial_session")),
        "reasons": _partial_session_reasons(replay, trade_date),
        "first_bar_time": per_date.get("first_bar_time"),
        "last_bar_time": per_date.get("last_bar_time"),
        "market_first_bar_time": per_date.get("market_first_bar_time"),
        "market_last_bar_time": per_date.get("market_last_bar_time"),
        "open_position_count_at_end": audit.get("open_position_count_at_end"),
        "paper_entry_count": replay.get("event_counts", {}).get("PAPER_ENTRY", 0),
        "paper_exit_count": replay.get("event_counts", {}).get("PAPER_EXIT", 0),
    }
    cache[trade_date] = status
    return status


def _session_filter(
    conn,
    *,
    replayable_dates: list[str],
    market_symbol: str,
    top_n: int,
    include_partial_sessions: bool,
    paper_config: dict[str, float] | None,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    statuses = {
        trade_date: _probe_session_status(
            conn,
            trade_date=trade_date,
            market_symbol=market_symbol,
            top_n=top_n,
            paper_config=paper_config,
            cache=cache,
        )
        for trade_date in replayable_dates
    }
    complete_dates = [trade_date for trade_date in replayable_dates if statuses[trade_date].get("session_complete")]
    partial_dates = [trade_date for trade_date in replayable_dates if not statuses[trade_date].get("session_complete")]
    included_dates = list(replayable_dates if include_partial_sessions else complete_dates)
    excluded_dates = [] if include_partial_sessions else partial_dates
    excluded_reasons = {
        trade_date: statuses[trade_date].get("reasons", [])
        for trade_date in excluded_dates
    }
    included_partial_dates = [trade_date for trade_date in included_dates if trade_date in partial_dates]
    return {
        "session_status_by_date": statuses,
        "complete_replayable_dates": complete_dates,
        "partial_replayable_dates": partial_dates,
        "included_replayable_dates": included_dates,
        "included_partial_session_dates": included_partial_dates,
        "excluded_partial_dates": excluded_dates,
        "excluded_partial_reasons": excluded_reasons,
        "excluded_partial_session_count": len(excluded_dates),
        "remaining_complete_replayable_days": len(complete_dates),
        "partial_session_included_count": len(included_partial_dates),
    }


def _write_rolling_markdown(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# DAY Rolling Replay Report ({payload.get('end_date')})",
        "",
        "Rolling reports summarize accumulated replay readiness. They are not profitability claims.",
        "",
        "## Summary",
        f"- market_symbol: {payload.get('market_symbol')}",
        f"- zero_volume_bar_policy: {payload.get('zero_volume_bar_policy')}",
        f"- compare_zero_volume_policies: {payload.get('compare_zero_volume_policies')}",
        f"- include_partial_sessions: {payload.get('include_partial_sessions')}",
        f"- exclude_partial_sessions: {payload.get('exclude_partial_sessions')}",
        f"- complete_replayable_days: {payload.get('complete_replayable_days', [])}",
        f"- partial_replayable_days: {payload.get('partial_replayable_days', [])}",
        f"- excluded_partial_dates: {payload.get('excluded_partial_dates', [])}",
        "",
        "## Windows",
        "| window | start | end | requested | complete | included | excluded_partial | status | signals | entries | exits | net | cost | gate | notes |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for item in payload.get("windows", []):
        summary = item.get("replay_summary", {})
        gate = summary.get("promotion_gate", {}) if isinstance(summary, dict) else {}
        lines.append(
            "| "
            + " | ".join(
                [
                    str(item.get("window")),
                    str(item.get("start_date")),
                    str(item.get("end_date")),
                    str(item.get("requested_days")),
                    str(item.get("remaining_complete_replayable_days", 0)),
                    str(item.get("replayable_days")),
                    str(item.get("excluded_partial_session_count", 0)),
                    str(item.get("status")),
                    str(summary.get("signal_count", 0) if isinstance(summary, dict) else 0),
                    str(summary.get("entry_count", 0) if isinstance(summary, dict) else 0),
                    str(summary.get("exit_count", 0) if isinstance(summary, dict) else 0),
                    str(summary.get("net_return_sum", 0.0) if isinstance(summary, dict) else 0.0),
                    str(summary.get("cost_impact", 0.0) if isinstance(summary, dict) else 0.0),
                    str(gate.get("readiness_stage")),
                    str(item.get("assessment_note")),
                ]
            )
            + " |"
        )
    lines.extend(["", "## Detail"])
    for item in payload.get("windows", []):
        lines.extend(
            [
                f"### Window {item.get('window')}",
                f"- replayable_dates: {item.get('replayable_dates', [])}",
                f"- complete_replayable_dates: {item.get('complete_replayable_dates', [])}",
                f"- partial_replayable_dates: {item.get('partial_replayable_dates', [])}",
                f"- excluded_partial_session_count: {item.get('excluded_partial_session_count', 0)}",
                f"- excluded_partial_dates: {item.get('excluded_partial_dates', [])}",
                f"- excluded_partial_reasons: {item.get('excluded_partial_reasons', {})}",
                f"- remaining_complete_replayable_days: {item.get('remaining_complete_replayable_days', 0)}",
                f"- partial_session_included_count: {item.get('partial_session_included_count', 0)}",
                f"- unreplayable_dates: {item.get('unreplayable_dates', {})}",
                f"- market_proxy_available_days: {item.get('market_proxy_available_days', 0)}",
                f"- top_rejection_reasons: {(item.get('replay_summary') or {}).get('top_rejection_reasons', {})}",
                f"- zero_volume_policy_summary: {(item.get('replay_summary') or {}).get('zero_volume_policy_summary', {})}",
                f"- policy_comparison: {item.get('policy_comparison', [])}",
                "",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_window(
    conn,
    *,
    end_date: str,
    window: int,
    market_symbol: str,
    top_n: int,
    zero_volume_policy: str,
    compare_policies: bool,
    include_partial_sessions: bool,
    min_replayable_days: int,
    output_dir: Path,
    dry_run: bool,
    session_cache: dict[str, dict[str, Any]],
    paper_config: dict[str, float] | None,
) -> dict[str, Any]:
    available_dates = _intraday_dates(conn, end_date)
    selected_dates = available_dates[-window:]
    start_date = selected_dates[0] if selected_dates else _calendar_start_for_empty_window(end_date, window)
    availability = build_day_data_availability_report(
        conn,
        start_date=start_date,
        end_date=end_date,
        market_proxy_symbol=market_symbol,
        max_universe_symbols=top_n,
    )
    availability_replayable_dates = availability.get("replayable_dates", [])
    session_filter = _session_filter(
        conn,
        replayable_dates=list(availability_replayable_dates),
        market_symbol=market_symbol,
        top_n=top_n,
        include_partial_sessions=include_partial_sessions,
        paper_config=paper_config,
        cache=session_cache,
    )
    replayable_dates = session_filter["included_replayable_dates"]
    market_proxy_available_days = sum(
        1
        for detail in availability.get("date_reports", {}).values()
        if detail.get("market_proxy_available")
    )
    item: dict[str, Any] = {
        "window": window,
        "start_date": start_date,
        "end_date": end_date,
        "requested_days": window,
        "availability_replayable_days": len(availability_replayable_dates),
        "replayable_days": len(replayable_dates),
        "replayable_dates": replayable_dates,
        "complete_replayable_dates": session_filter["complete_replayable_dates"],
        "partial_replayable_dates": session_filter["partial_replayable_dates"],
        "excluded_partial_session_count": session_filter["excluded_partial_session_count"],
        "excluded_partial_dates": session_filter["excluded_partial_dates"],
        "excluded_partial_reasons": session_filter["excluded_partial_reasons"],
        "remaining_complete_replayable_days": session_filter["remaining_complete_replayable_days"],
        "included_partial_session_dates": session_filter["included_partial_session_dates"],
        "partial_session_included_count": session_filter["partial_session_included_count"],
        "include_partial_sessions": include_partial_sessions,
        "exclude_partial_sessions": not include_partial_sessions,
        "unreplayable_dates": availability.get("unreplayable_dates", {}),
        "market_proxy_available_days": market_proxy_available_days,
    }
    if len(replayable_dates) < min_replayable_days:
        item.update(
            {
                "status": "blocked",
                "blocked_reason": "INSUFFICIENT_REPLAYABLE_DAYS",
                "assessment_note": "Not enough replayable days for rolling assessment.",
            }
        )
        return item
    if len(replayable_dates) < window:
        item["assessment_note"] = "Insufficient for profitability assessment; smoke/coverage only."
    else:
        item["assessment_note"] = "Rolling window complete enough for preliminary analysis, not LIVE readiness."
    if item["partial_session_included_count"]:
        item["assessment_note"] = "Partial sessions included; profitability assessment is invalid."
    if dry_run:
        item["status"] = "dry_run"
        return item

    if compare_policies:
        rows = []
        for policy in ZERO_VOLUME_POLICIES:
            cfg = DayTradingConfig(
                enabled=True,
                mode="PAPER",
                market_proxy_symbol=market_symbol,
                max_universe_symbols=top_n,
                zero_volume_bar_policy=policy,
                **(paper_config or {}),
            )
            replay = run_day_replay_backtest(conn, start_date, end_date, config=cfg, trade_dates=list(replayable_dates))
            quality = validate_intraday_prices(conn, start_date=start_date, end_date=end_date, market_proxy_symbol=market_symbol)
            gate = _evaluate_gate(replay, quality)
            rows.append(_summarize_policy_result(policy, replay, gate))
        report_path = output_dir / f"day_rolling_{window}d_{end_date}_zero_volume_compare.md"
        report_path.write_text(
            _build_policy_comparison_markdown(
                {
                    "start_date": start_date,
                    "end_date": end_date,
                    "policy_comparison": rows,
                }
            ),
            encoding="utf-8",
        )
        item.update({"status": "ok", "policy_comparison": rows, "report_path": str(report_path)})
        return item

    cfg = DayTradingConfig(
        enabled=True,
        mode="PAPER",
        market_proxy_symbol=market_symbol,
        max_universe_symbols=top_n,
        zero_volume_bar_policy=zero_volume_policy,
        **(paper_config or {}),
    )
    replay = run_day_replay_backtest(conn, start_date, end_date, config=cfg, trade_dates=list(replayable_dates))
    quality = validate_intraday_prices(conn, start_date=start_date, end_date=end_date, market_proxy_symbol=market_symbol)
    gate = _evaluate_gate(replay, quality)
    report_path = output_dir / f"day_rolling_{window}d_{end_date}.md"
    write_day_validation_report(
        report_path,
        build_day_validation_markdown(
            replay,
            quality,
            gate,
            universe_source=cfg.universe_source,
            same_day_scores_allowed=cfg.allow_same_day_scores,
            market_proxy_symbol=cfg.market_proxy_symbol,
        ),
    )
    item.update({"status": "ok", "replay_summary": _compact_replay_summary(replay, gate), "report_path": str(report_path)})
    return item


def main() -> None:
    parser = argparse.ArgumentParser(description="Run rolling DAY replay reports over accumulated intraday DB data")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--windows", default="3,5,20,60")
    parser.add_argument("--market-symbol", default="069500")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--zero-volume-bar-policy", choices=ZERO_VOLUME_POLICIES, default="strict_invalid")
    parser.add_argument("--compare-zero-volume-policies", action="store_true")
    parser.add_argument("--paper-initial-cash-krw", dest="paper_initial_cash_krw", type=float, default=None)
    parser.add_argument("--paper-notional-per-trade-krw", dest="paper_notional_per_trade_krw", type=float, default=None)
    parser.add_argument("--paper-max-total-exposure-krw", dest="paper_max_total_exposure_krw", type=float, default=None)
    parser.add_argument("--paper-max-position-value-krw", dest="paper_max_position_value_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-krw", dest="paper_daily_loss_limit_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-pct", dest="paper_daily_loss_limit_pct", type=float, default=None)
    parser.set_defaults(include_partial_sessions=False)
    parser.add_argument("--include-partial-sessions", dest="include_partial_sessions", action="store_true", help="Include partial sessions for debugging only; profitability assessment remains invalid")
    parser.add_argument("--exclude-partial-sessions", dest="include_partial_sessions", action="store_false", help="Exclude partial sessions from rolling replay (default)")
    parser.add_argument("--output-dir", default="reports/rolling")
    parser.add_argument("--min-replayable-days", type=int, default=1)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        _json_dump({"status": "blocked", "blocked_reason": "DB_MISSING", "db": str(db_path)}, stream=sys.stderr)
        raise SystemExit(2)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = get_connection(db_path)
    init_db(conn)
    payload: dict[str, Any] = {
        "status": "ok",
        "db": str(db_path),
        "end_date": args.end_date,
        "windows_requested": _parse_windows(args.windows),
        "market_symbol": args.market_symbol,
        "top_n": args.top_n,
        "zero_volume_bar_policy": args.zero_volume_bar_policy,
        "compare_zero_volume_policies": bool(args.compare_zero_volume_policies),
        "include_partial_sessions": bool(args.include_partial_sessions),
        "exclude_partial_sessions": not bool(args.include_partial_sessions),
        "dry_run": bool(args.dry_run),
        "windows": [],
    }
    session_cache: dict[str, dict[str, Any]] = {}
    paper_config = _paper_config_kwargs(args)
    for window in payload["windows_requested"]:
        payload["windows"].append(
            _run_window(
                conn,
                end_date=args.end_date,
                window=int(window),
                market_symbol=args.market_symbol,
                top_n=args.top_n,
                zero_volume_policy=args.zero_volume_bar_policy,
                compare_policies=bool(args.compare_zero_volume_policies),
                include_partial_sessions=bool(args.include_partial_sessions),
                min_replayable_days=max(1, int(args.min_replayable_days)),
                output_dir=output_dir,
                dry_run=bool(args.dry_run),
                session_cache=session_cache,
                paper_config=paper_config,
            )
        )
    conn.close()
    complete_dates = sorted(
        {
            str(trade_date)
            for item in payload["windows"]
            for trade_date in item.get("complete_replayable_dates", [])
        }
    )
    partial_dates = sorted(
        {
            str(trade_date)
            for item in payload["windows"]
            for trade_date in item.get("partial_replayable_dates", [])
        }
    )
    excluded_dates = sorted(
        {
            str(trade_date)
            for item in payload["windows"]
            for trade_date in item.get("excluded_partial_dates", [])
        }
    )
    payload["complete_replayable_days"] = complete_dates
    payload["complete_replayable_day_count"] = len(complete_dates)
    payload["partial_replayable_days"] = partial_dates
    payload["partial_replayable_day_count"] = len(partial_dates)
    payload["excluded_partial_dates"] = excluded_dates
    payload["blocked_windows"] = [item for item in payload["windows"] if item.get("status") == "blocked"]
    payload["report_path"] = str(output_dir / f"day_rolling_summary_{args.end_date}.md")
    _write_rolling_markdown(Path(payload["report_path"]), payload)
    _json_dump(payload)


if __name__ == "__main__":
    main()
