#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.config import DayTradingConfig  # noqa: E402
from pipeline.day_trading.kis_client import DEFAULT_KIS_TOKEN_CACHE_PATH, load_kis_env  # noqa: E402
from pipeline.day_trading.universe import DayUniverseProvider  # noqa: E402
from pipeline.db import get_connection, init_db  # noqa: E402


def _json_dump(payload: dict[str, Any], *, stream: Any = None) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream or sys.stdout)


def _parse_windows(raw: str) -> list[int]:
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def _run_json_command(args: list[str]) -> tuple[int, dict[str, Any], str]:
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    payload: dict[str, Any]
    try:
        payload = json.loads(proc.stdout) if proc.stdout.strip() else {}
    except json.JSONDecodeError:
        payload = {"raw_stdout": proc.stdout[-4000:]}
    return proc.returncode, payload, proc.stderr[-4000:]


def _append_paper_account_args(command: list[str], args: argparse.Namespace) -> None:
    mappings = [
        ("--paper-initial-cash-krw", "paper_initial_cash_krw"),
        ("--paper-notional-per-trade-krw", "paper_notional_per_trade_krw"),
        ("--paper-max-total-exposure-krw", "paper_max_total_exposure_krw"),
        ("--paper-max-position-value-krw", "paper_max_position_value_krw"),
        ("--paper-daily-loss-limit-krw", "paper_daily_loss_limit_krw"),
        ("--paper-daily-loss-limit-pct", "paper_daily_loss_limit_pct"),
    ]
    for option, attr in mappings:
        value = getattr(args, attr, None)
        if value is not None:
            command.extend([option, str(value)])


def _auto_trade_date() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()


def _hhmm_to_minutes(value: str) -> int:
    hour, minute = value.split(":", 1)
    return int(hour) * 60 + int(minute)


def _score_date_check(
    conn,
    *,
    trade_date: str,
    market_symbol: str,
    top_n: int,
    max_score_staleness_days: int,
    allow_stale_score: bool,
) -> dict[str, Any]:
    cfg = DayTradingConfig(max_universe_symbols=top_n, market_proxy_symbol=market_symbol)
    selection = DayUniverseProvider(conn, cfg).get_universe_selection(trade_date)
    blocked: list[str] = []
    warnings: list[str] = []
    if not selection.score_date:
        blocked.extend(selection.reason_codes or ["NO_PRIOR_SCORE_DATE"])
    if selection.same_day_score_used:
        blocked.append("SAME_DAY_SCORE_FORBIDDEN")
    if not selection.candidates:
        blocked.append("EMPTY_CANDIDATES")
    if len(selection.candidates) < top_n:
        warnings.append("INSUFFICIENT_CANDIDATES")
    score_age_days = None
    stale_score_blocked = False
    if selection.score_date:
        score_age_days = (datetime.fromisoformat(trade_date).date() - datetime.fromisoformat(selection.score_date).date()).days
        if score_age_days > max_score_staleness_days:
            if allow_stale_score:
                warnings.append("STALE_SCORE_DATE_ALLOWED")
            else:
                blocked.append("STALE_SCORE_DATE")
                stale_score_blocked = True
    return {
        "trade_date": trade_date,
        "score_date": selection.score_date,
        "score_date_used": selection.score_date,
        "score_age_days": score_age_days,
        "score_staleness_days": score_age_days,
        "max_score_staleness_days": max_score_staleness_days,
        "stale_score_allowed": bool(allow_stale_score),
        "stale_score_blocked": stale_score_blocked,
        "same_day_score_used": selection.same_day_score_used,
        "lookahead_safe": selection.lookahead_safe,
        "candidate_count": len(selection.candidates),
        "candidate_symbols": list(selection.candidates),
        "blocked_reasons": sorted(set(blocked)),
        "warnings": warnings,
    }


def _primary_blocked_reason(reasons: list[str]) -> str | None:
    priority = [
        "ONLY_SAME_DAY_SCORE_AVAILABLE_BUT_FORBIDDEN",
        "SAME_DAY_SCORE_FORBIDDEN",
        "NO_PRIOR_SCORE_DATE",
        "STALE_SCORE_DATE",
        "EMPTY_CANDIDATES",
        "INSUFFICIENT_CANDIDATES",
    ]
    for reason in priority:
        if reason in reasons:
            return reason
    return reasons[0] if reasons else None


def _next_actions_for_blocked_reason(reason: str | None) -> list[str]:
    if reason in {"NO_PRIOR_SCORE_DATE", "ONLY_SAME_DAY_SCORE_AVAILABLE_BUT_FORBIDDEN", "SAME_DAY_SCORE_FORBIDDEN"}:
        return [
            "Update daily_prices for dates before the DAY trade_date, e.g. scripts/fetch_public_daily_prices.py or an approved daily_prices CSV.",
            "Regenerate SWING daily_scores with scripts/prepare_day_replay_db.py --run-swing-pipeline.",
            "Re-run scripts/run_kis_daily_ops.py after a prior score_date exists.",
        ]
    if reason == "STALE_SCORE_DATE":
        return [
            "Fetch or load the latest daily_prices before the trade_date, e.g. scripts/fetch_public_daily_prices.py or an approved daily_prices CSV.",
            "Regenerate daily_features and daily_scores through the SWING pipeline.",
            "Re-run daily ops, or use --allow-stale-score only for explicit diagnostics.",
        ]
    if reason == "EMPTY_CANDIDATES":
        return [
            "Check daily_scores rank/score output for the prior score_date.",
            "Confirm the SWING pipeline produced enough ranked candidates.",
        ]
    if reason == "BEFORE_POST_CLOSE_COLLECTION_WINDOW":
        return [
            "Run after the configured KST post-close guard time.",
            "Use --allow-partial-session-collection only for explicit partial-session debugging.",
        ]
    return []


def _rolling_status_summary(rolling: dict[str, Any], windows_requested: list[int]) -> dict[str, Any]:
    windows = rolling.get("windows", []) if isinstance(rolling, dict) else []
    complete_dates: set[str] = set()
    partial_dates: set[str] = set()
    excluded_dates: set[str] = set()
    generated: list[int] = []
    blocked: list[dict[str, Any]] = []
    insufficient_reasons: dict[str, str] = {}
    for item in windows:
        complete_dates.update(str(d) for d in item.get("complete_replayable_dates", []) or [])
        partial_dates.update(str(d) for d in item.get("partial_replayable_dates", []) or [])
        excluded_dates.update(str(d) for d in item.get("excluded_partial_dates", []) or [])
        if item.get("status") == "ok":
            generated.append(int(item.get("window", 0)))
        if item.get("status") == "blocked":
            blocked.append({"window": item.get("window"), "reason": item.get("blocked_reason")})
        if int(item.get("remaining_complete_replayable_days", 0) or 0) < int(item.get("requested_days", 0) or 0):
            insufficient_reasons[str(item.get("window"))] = item.get("blocked_reason") or item.get("assessment_note") or "INSUFFICIENT_COMPLETE_REPLAYABLE_DAYS"
    complete_count = len(complete_dates)
    return {
        "complete_replayable_days": sorted(complete_dates),
        "complete_replayable_day_count": complete_count,
        "partial_replayable_days": sorted(partial_dates),
        "partial_replayable_day_count": len(partial_dates),
        "excluded_partial_dates": sorted(excluded_dates),
        "rolling_windows_requested": windows_requested,
        "rolling_windows_generated": generated,
        "rolling_windows_blocked": blocked,
        "insufficient_complete_days_reasons": insufficient_reasons,
        "next_required_complete_days_for_3day_smoke": max(0, 3 - complete_count),
        "next_required_complete_days_for_20day_analysis": max(0, 20 - complete_count),
    }


def _write_status_files(output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "day_ops_status.json"
    md_path = output_dir / "day_ops_status.md"
    score_check = payload.get("score_date_check", {})
    rolling_summary = _rolling_status_summary(
        payload.get("rolling", {}) if isinstance(payload.get("rolling"), dict) else {},
        payload.get("rolling_windows_requested", []),
    )
    status = {
        "last_run_at": payload.get("run_at"),
        "last_trade_date": payload.get("trade_date"),
        "trade_date": payload.get("trade_date"),
        "replay_trade_date": payload.get("trade_date"),
        "score_date_used": score_check.get("score_date"),
        "score_date_used_for_replay": score_check.get("score_date"),
        "replay_uses_same_day_score": bool(score_check.get("same_day_score_used")),
        "score_staleness_days": score_check.get("score_staleness_days"),
        "stale_score_allowed": score_check.get("stale_score_allowed"),
        "stale_score_blocked": score_check.get("stale_score_blocked"),
        "daily_refresh_enabled": False,
        "daily_refresh_ran_after_replay": False,
        "refreshed_daily_price_date": None,
        "generated_daily_score_date": None,
        "next_trade_date_candidate": None,
        "score_ready_for_next_trade_date": None,
        "score_staleness_days_before_refresh": score_check.get("score_staleness_days"),
        "score_staleness_days_after_refresh": None,
        "daily_refresh_blocked_reason": None,
        "collected_symbols": payload.get("collection", {}).get("collection_plan", {}).get("candidate_symbols", []),
        "market_symbol": payload.get("market_symbol"),
        "rows_inserted": payload.get("collection", {}).get("db_load", {}).get("inserted_or_updated_rows", 0),
        "rows_updated": payload.get("collection", {}).get("db_load", {}).get("inserted_or_updated_rows", 0),
        "invalid_rows": payload.get("collection", {}).get("db_load", {}).get("invalid_rows", 0),
        "audit_status": payload.get("collection", {}).get("availability", {}).get("summary", {}),
        "replay_status": payload.get("collection", {}).get("replay", {}).get("status"),
        "signal_count": payload.get("collection", {}).get("replay", {}).get("event_counts", {}).get("SIGNAL_CREATED", 0),
        "entry_count": payload.get("collection", {}).get("replay", {}).get("event_counts", {}).get("PAPER_ENTRY", 0),
        "exit_count": payload.get("collection", {}).get("replay", {}).get("event_counts", {}).get("PAPER_EXIT", 0),
        "open_position_count_at_end": payload.get("collection", {}).get("replay", {}).get("session_audit", {}).get("open_position_count_at_end"),
        "zero_volume_policy_default": payload.get("zero_volume_bar_policy"),
        "policy_compare_status": "enabled" if payload.get("compare_zero_volume_policies") else "disabled",
        "rolling_reports_generated": [item.get("report_path") for item in payload.get("rolling", {}).get("windows", []) if item.get("report_path")],
        **rolling_summary,
        "blocked_reason": payload.get("blocked_reason"),
        "next_actions": payload.get("next_actions", []),
    }
    json_path.write_text(json.dumps(status, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    lines = [
        f"# DAY Daily Ops Status ({status['last_trade_date']})",
        "",
        f"- last_run_at: {status['last_run_at']}",
        f"- score_date_used: {status['score_date_used']}",
        f"- score_date_used_for_replay: {status['score_date_used_for_replay']}",
        f"- replay_trade_date: {status['replay_trade_date']}",
        f"- replay_uses_same_day_score: {status['replay_uses_same_day_score']}",
        f"- score_staleness_days: {status['score_staleness_days']}",
        f"- stale_score_allowed: {status['stale_score_allowed']}",
        f"- stale_score_blocked: {status['stale_score_blocked']}",
        f"- daily_refresh_enabled: {status['daily_refresh_enabled']}",
        f"- daily_refresh_ran_after_replay: {status['daily_refresh_ran_after_replay']}",
        f"- refreshed_daily_price_date: {status['refreshed_daily_price_date']}",
        f"- generated_daily_score_date: {status['generated_daily_score_date']}",
        f"- next_trade_date_candidate: {status['next_trade_date_candidate']}",
        f"- score_ready_for_next_trade_date: {status['score_ready_for_next_trade_date']}",
        f"- market_symbol: {status['market_symbol']}",
        f"- replay_status: {status['replay_status']}",
        f"- signal/entry/exit: {status['signal_count']}/{status['entry_count']}/{status['exit_count']}",
        f"- open_position_count_at_end: {status['open_position_count_at_end']}",
        f"- zero_volume_policy_default: {status['zero_volume_policy_default']}",
        f"- policy_compare_status: {status['policy_compare_status']}",
        f"- complete_replayable_days: {status['complete_replayable_days']}",
        f"- partial_replayable_days: {status['partial_replayable_days']}",
        f"- excluded_partial_dates: {status['excluded_partial_dates']}",
        f"- rolling_windows_requested: {status['rolling_windows_requested']}",
        f"- rolling_windows_generated: {status['rolling_windows_generated']}",
        f"- rolling_windows_blocked: {status['rolling_windows_blocked']}",
        f"- insufficient_complete_days_reasons: {status['insufficient_complete_days_reasons']}",
        f"- next_required_complete_days_for_3day_smoke: {status['next_required_complete_days_for_3day_smoke']}",
        f"- next_required_complete_days_for_20day_analysis: {status['next_required_complete_days_for_20day_analysis']}",
        f"- blocked_reason: {status['blocked_reason']}",
        f"- next_actions: {status['next_actions']}",
        "",
        "This status file is operational metadata and is not a profitability claim.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"status_json": str(json_path), "status_md": str(md_path)}


def _compact_collection(payload: dict[str, Any]) -> dict[str, Any]:
    replay = payload.get("replay", {}) or {}
    return {
        "status": payload.get("status"),
        "blocked_reason": payload.get("blocked_reason"),
        "blocked_reasons": payload.get("blocked_reasons", []),
        "warnings": payload.get("warnings", []),
        "collection_plan": payload.get("collection_plan", {}),
        "collection_session": payload.get("collection_session", {}),
        "db_load": payload.get("db_load", {}),
        "data_quality": {
            "row_count": payload.get("data_quality", {}).get("row_count"),
            "candidate_usable_symbol_count": payload.get("data_quality", {}).get("candidate_usable_symbol_count"),
            "market_proxy_available": payload.get("data_quality", {}).get("market_proxy_available"),
        },
        "coverage_audit": payload.get("coverage_audit", {}),
        "availability": payload.get("availability", {}),
        "replay": {
            "status": replay.get("status"),
            "event_counts": replay.get("event_counts", {}),
            "session_audit": replay.get("session_audit", {}),
            "performance": replay.get("performance", {}),
            "paper_account": replay.get("paper_account", {}),
            "trade_details": replay.get("trade_details", []),
            "policy_comparison": replay.get("policy_comparison", []),
        },
        "top_blocking_reasons": payload.get("top_blocking_reasons", {}),
        "audit_report_path": payload.get("audit_report_path"),
        "replay_report_path": payload.get("replay_report_path"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run quote-only KIS daily collection, replay, and rolling reports")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--auto-trade-date", action="store_true")
    parser.add_argument("--market-symbol", default="069500")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--max-symbols", type=int, default=20)
    parser.add_argument("--require-full-top-n-coverage", action="store_true")
    parser.add_argument("--zero-volume-bar-policy", default="strict_invalid", choices=["strict_invalid", "no_trade_context", "drop_no_trade"])
    parser.add_argument("--compare-zero-volume-policies", action="store_true")
    parser.add_argument("--paper-initial-cash-krw", dest="paper_initial_cash_krw", type=float, default=None)
    parser.add_argument("--paper-notional-per-trade-krw", dest="paper_notional_per_trade_krw", type=float, default=None)
    parser.add_argument("--paper-max-total-exposure-krw", dest="paper_max_total_exposure_krw", type=float, default=None)
    parser.add_argument("--paper-max-position-value-krw", dest="paper_max_position_value_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-krw", dest="paper_daily_loss_limit_krw", type=float, default=None)
    parser.add_argument("--paper-daily-loss-limit-pct", dest="paper_daily_loss_limit_pct", type=float, default=None)
    parser.add_argument("--rolling-windows", default="3,5,20,60")
    parser.add_argument("--output-dir", default="reports/daily_ops")
    parser.add_argument("--data-output-dir", default="data/intraday")
    parser.add_argument("--sleep-seconds", type=float, default=0.3)
    parser.add_argument("--force-refresh", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-replay", action="store_true")
    parser.add_argument("--skip-rolling", action="store_true")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--max-score-staleness-days", type=int, default=3)
    parser.add_argument("--max-score-age-days", dest="max_score_staleness_days", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--allow-stale-score", action="store_true")
    parser.set_defaults(include_partial_sessions=False)
    parser.add_argument("--include-partial-sessions", dest="include_partial_sessions", action="store_true", help="Include partial sessions in rolling replay for debugging only")
    parser.add_argument("--exclude-partial-sessions", dest="include_partial_sessions", action="store_false", help="Exclude partial sessions from rolling replay (default)")
    parser.add_argument("--earliest-run-time", default="15:45", help="KST guard for current-day non-dry-run collection")
    parser.add_argument("--allow-partial-session-collection", action="store_true")
    args = parser.parse_args()

    trade_date = args.trade_date or (_auto_trade_date() if args.auto_trade_date else None)
    output_dir = Path(args.output_dir)
    data_output_dir = Path(args.data_output_dir)
    payload: dict[str, Any] = {
        "status": "ok",
        "run_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
        "db": args.db,
        "trade_date": trade_date,
        "market_symbol": args.market_symbol,
        "top_n": args.top_n,
        "max_symbols": args.max_symbols,
        "zero_volume_bar_policy": args.zero_volume_bar_policy,
        "compare_zero_volume_policies": bool(args.compare_zero_volume_policies),
        "rolling_windows_requested": _parse_windows(args.rolling_windows),
        "max_score_staleness_days": args.max_score_staleness_days,
        "allow_stale_score": bool(args.allow_stale_score),
        "include_partial_sessions": bool(args.include_partial_sessions),
        "dry_run": bool(args.dry_run),
    }
    if not trade_date:
        payload.update({"status": "blocked", "blocked_reason": "TRADE_DATE_REQUIRED", "next_actions": ["Pass --trade-date YYYY-MM-DD or --auto-trade-date."]})
        output_paths = _write_status_files(output_dir, payload)
        _json_dump({**payload, "status_files": output_paths})
        raise SystemExit(2)
    db_path = Path(args.db)
    if not db_path.exists():
        payload.update({"status": "blocked", "blocked_reason": "DB_MISSING", "next_actions": ["Run scripts/bootstrap_market_db.py and load daily_prices/daily_scores before daily ops."]})
        output_paths = _write_status_files(output_dir, payload)
        _json_dump({**payload, "status_files": output_paths})
        raise SystemExit(2)
    env_result = load_kis_env(args.env_file)
    payload["env_status"] = env_result.to_dict()
    if env_result.status != "ok" and not args.dry_run:
        payload.update({"status": "blocked", "blocked_reason": env_result.reason, "next_actions": ["Check .env via scripts/check_kis_env.py without printing secrets."]})
        output_paths = _write_status_files(output_dir, payload)
        _json_dump({**payload, "status_files": output_paths})
        raise SystemExit(2)

    conn = get_connection(db_path)
    init_db(conn)
    score_check = _score_date_check(
        conn,
        trade_date=trade_date,
        market_symbol=args.market_symbol,
        top_n=args.top_n,
        max_score_staleness_days=args.max_score_staleness_days,
        allow_stale_score=bool(args.allow_stale_score),
    )
    conn.close()
    payload["score_date_check"] = score_check
    if score_check["blocked_reasons"]:
        blocked_reason = _primary_blocked_reason(score_check["blocked_reasons"])
        payload.update({"status": "blocked", "blocked_reason": blocked_reason, "next_actions": _next_actions_for_blocked_reason(blocked_reason)})
        output_paths = _write_status_files(output_dir, payload)
        _json_dump({**payload, "status_files": output_paths})
        raise SystemExit(1)
    now_kst = datetime.now(ZoneInfo("Asia/Seoul"))
    if (
        not args.dry_run
        and not args.allow_partial_session_collection
        and trade_date == now_kst.date().isoformat()
        and (_hhmm_to_minutes(f"{now_kst.hour:02d}:{now_kst.minute:02d}") < _hhmm_to_minutes(args.earliest_run_time))
    ):
        payload.update(
            {
                "status": "blocked",
                "blocked_reason": "BEFORE_POST_CLOSE_COLLECTION_WINDOW",
                "next_actions": _next_actions_for_blocked_reason("BEFORE_POST_CLOSE_COLLECTION_WINDOW"),
                "current_kst": now_kst.isoformat(),
                "earliest_run_time": args.earliest_run_time,
            }
        )
        output_paths = _write_status_files(output_dir, payload)
        _json_dump({**payload, "status_files": output_paths})
        raise SystemExit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    data_output_dir.mkdir(parents=True, exist_ok=True)
    collection_args = [
        "scripts/run_kis_daily_intraday_collection.py",
        "--db",
        str(db_path),
        "--trade-date",
        trade_date,
        "--market-symbol",
        args.market_symbol,
        "--top-n",
        str(args.top_n),
        "--max-symbols",
        str(args.max_symbols),
        "--output-csv",
        str(data_output_dir / f"intraday_kis_{trade_date}.csv"),
        "--audit-report-md",
        str(output_dir / f"day_data_availability_{trade_date}.md"),
        "--replay-report-md",
        str(output_dir / f"day_replay_{trade_date}.md"),
        "--sleep-seconds",
        str(args.sleep_seconds),
        "--env-file",
        args.env_file,
        "--token-cache",
        args.token_cache,
        "--zero-volume-bar-policy",
        args.zero_volume_bar_policy,
    ]
    if args.require_full_top_n_coverage:
        collection_args.append("--require-full-top-n-coverage")
    if args.compare_zero_volume_policies:
        collection_args.append("--compare-zero-volume-policies")
    if args.force_refresh:
        collection_args.append("--force-refresh")
    if args.dry_run:
        collection_args.append("--dry-run")
    if args.skip_replay:
        collection_args.append("--skip-replay")
    _append_paper_account_args(collection_args, args)
    code, collection_payload, stderr_tail = _run_json_command(collection_args)
    payload["collection_returncode"] = code
    payload["collection"] = _compact_collection(collection_payload)
    if stderr_tail:
        payload["collection_stderr_tail"] = stderr_tail
    if code != 0:
        blocked_reason = collection_payload.get("blocked_reason") or "KIS_DAILY_COLLECTION_FAILED"
        payload.update(
            {
                "status": "blocked",
                "blocked_reason": blocked_reason,
                "next_actions": _next_actions_for_blocked_reason(blocked_reason),
            }
        )
    elif not args.skip_rolling:
        rolling_args = [
            "scripts/run_day_rolling_replay.py",
            "--db",
            str(db_path),
            "--end-date",
            trade_date,
            "--windows",
            args.rolling_windows,
            "--market-symbol",
            args.market_symbol,
            "--top-n",
            str(args.top_n),
            "--zero-volume-bar-policy",
            args.zero_volume_bar_policy,
            "--output-dir",
            str(output_dir / "rolling"),
        ]
        if args.compare_zero_volume_policies:
            rolling_args.append("--compare-zero-volume-policies")
        if args.include_partial_sessions:
            rolling_args.append("--include-partial-sessions")
        else:
            rolling_args.append("--exclude-partial-sessions")
        _append_paper_account_args(rolling_args, args)
        if args.dry_run:
            rolling_args.append("--dry-run")
        r_code, rolling_payload, rolling_stderr = _run_json_command(rolling_args)
        payload["rolling_returncode"] = r_code
        payload["rolling"] = rolling_payload
        if rolling_stderr:
            payload["rolling_stderr_tail"] = rolling_stderr
        if r_code != 0:
            payload.update({"status": "blocked", "blocked_reason": rolling_payload.get("blocked_reason") or "ROLLING_REPLAY_FAILED"})
    else:
        payload["rolling"] = {"status": "skipped", "skip_reason": "SKIP_ROLLING_REQUESTED"}

    output_paths = _write_status_files(output_dir, payload)
    payload["status_files"] = output_paths
    _json_dump(payload)
    if payload["status"] == "blocked":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
