#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.kis_client import DEFAULT_KIS_TOKEN_CACHE_PATH, load_kis_env  # noqa: E402
from pipeline.day_trading.universe import DayUniverseProvider  # noqa: E402
from pipeline.day_trading.config import DayTradingConfig  # noqa: E402
from pipeline.db import get_connection, init_db  # noqa: E402


def _json_dump(payload: dict[str, Any], *, stream: Any = None) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str), file=stream or sys.stdout)


def _run_json_command(args: list[str]) -> tuple[int, dict[str, Any], str]:
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    try:
        payload = json.loads(proc.stdout) if proc.stdout.strip() else {}
    except json.JSONDecodeError:
        payload = {"raw_stdout": proc.stdout[-4000:]}
    return proc.returncode, payload, proc.stderr[-4000:]


def _auto_trade_date() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()


def _next_weekday(value: str) -> str:
    cur = date.fromisoformat(value) + timedelta(days=1)
    while cur.weekday() >= 5:
        cur += timedelta(days=1)
    return cur.isoformat()


def _score_selection(conn, trade_date: str, market_symbol: str, top_n: int) -> dict[str, Any]:
    cfg = DayTradingConfig(max_universe_symbols=top_n, market_proxy_symbol=market_symbol)
    selection = DayUniverseProvider(conn, cfg).get_universe_selection(trade_date)
    staleness = None
    if selection.score_date:
        staleness = (date.fromisoformat(trade_date) - date.fromisoformat(selection.score_date)).days
    return {
        "trade_date": trade_date,
        "score_date": selection.score_date,
        "score_date_used_for_replay": selection.score_date,
        "score_staleness_days": staleness,
        "same_day_score_used": selection.same_day_score_used,
        "lookahead_safe": selection.lookahead_safe,
        "candidate_count": len(selection.candidates),
        "reason_codes": selection.reason_codes,
    }


def _score_date_exists(conn, score_date: str) -> bool:
    row = conn.execute("SELECT 1 FROM daily_scores WHERE date=? LIMIT 1", (score_date,)).fetchone()
    return row is not None


def _daily_price_date_exists(conn, price_date: str) -> bool:
    row = conn.execute("SELECT 1 FROM daily_prices WHERE date=? LIMIT 1", (price_date,)).fetchone()
    return row is not None


def _score_ready_for_next_trade_date(conn, trade_date: str, next_trade_date: str) -> dict[str, Any]:
    row = conn.execute("SELECT MAX(date) AS d FROM daily_scores WHERE date < ?", (next_trade_date,)).fetchone()
    previous = (row["d"] if "d" in row.keys() else row[0]) if hasattr(row, "keys") and row else (row[0] if row else None)
    return {
        "next_trade_date_candidate": next_trade_date,
        "generated_daily_score_date": trade_date if _score_date_exists(conn, trade_date) else None,
        "refreshed_daily_price_date": trade_date if _daily_price_date_exists(conn, trade_date) else None,
        "previous_score_date_for_next_trade_date": previous,
        "score_ready_for_next_trade_date": previous == trade_date,
    }


def _next_actions_for_daily_refresh(reason: str | None) -> list[str]:
    if reason in {"DAILY_REFRESH_DISABLED", None}:
        return []
    if reason == "DAILY_PRICE_FETCH_FAILED":
        return [
            "Check public daily data availability for the post-close trade_date.",
            "If public fetch remains unavailable, provide an approved daily_prices CSV and rerun prepare_day_replay_db.py.",
        ]
    if reason == "DAILY_PRICE_FOR_TRADE_DATE_MISSING":
        return [
            "Confirm the public source has published the trade_date close.",
            "Retry after the source updates, or load a vetted daily_prices CSV.",
        ]
    if reason in {"DAILY_FEATURES_GENERATION_FAILED", "DAILY_SCORES_GENERATION_FAILED", "NEXT_DAY_SCORE_DATE_NOT_GENERATED"}:
        return [
            "Run scripts/prepare_day_replay_db.py --run-swing-pipeline with the refreshed daily_prices CSV.",
            "Verify daily_scores contains trade_date D before the next DAY run.",
        ]
    return []


def _write_status_files(output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "day_eod_ops_status.json"
    md_path = output_dir / "day_eod_ops_status.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    lines = [
        f"# DAY End-of-Day Ops Status ({payload.get('trade_date')})",
        "",
        f"- status: {payload.get('status')}",
        f"- blocked_reason: {payload.get('blocked_reason')}",
        f"- replay_trade_date: {payload.get('replay_trade_date')}",
        f"- score_date_used_for_replay: {payload.get('score_date_used_for_replay')}",
        f"- replay_uses_same_day_score: {payload.get('replay_uses_same_day_score')}",
        f"- daily_refresh_enabled: {payload.get('daily_refresh_enabled')}",
        f"- daily_refresh_ran_after_replay: {payload.get('daily_refresh_ran_after_replay')}",
        f"- refreshed_daily_price_date: {payload.get('refreshed_daily_price_date')}",
        f"- generated_daily_score_date: {payload.get('generated_daily_score_date')}",
        f"- next_trade_date_candidate: {payload.get('next_trade_date_candidate')}",
        f"- score_ready_for_next_trade_date: {payload.get('score_ready_for_next_trade_date')}",
        f"- score_staleness_days_before_refresh: {payload.get('score_staleness_days_before_refresh')}",
        f"- score_staleness_days_after_refresh: {payload.get('score_staleness_days_after_refresh')}",
        f"- daily_refresh_blocked_reason: {payload.get('daily_refresh_blocked_reason')}",
        f"- complete_replayable_days: {payload.get('rolling_summary', {}).get('complete_replayable_days', [])}",
        f"- partial_replayable_days: {payload.get('rolling_summary', {}).get('partial_replayable_days', [])}",
        f"- excluded_partial_dates: {payload.get('rolling_summary', {}).get('excluded_partial_dates', [])}",
        f"- next_actions: {payload.get('next_actions', [])}",
        "",
        "D replay uses only score_date < D. Any score generated for D is for the next trade date only.",
        "This status file is operational metadata and is not a profitability claim.",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"status_json": str(json_path), "status_md": str(md_path)}


def _compact_daily_ops(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": payload.get("status"),
        "blocked_reason": payload.get("blocked_reason"),
        "score_date_check": payload.get("score_date_check", {}),
        "collection": payload.get("collection", {}),
        "rolling": payload.get("rolling", {}),
        "status_files": payload.get("status_files", {}),
    }


def _daily_refresh(
    *,
    db_path: Path,
    trade_date: str,
    next_trade_date: str,
    daily_prices_output: str,
    universe_csv: str,
    market_symbol: str,
    top_n: int,
    sleep_seconds: float,
    daily_max_symbols: int,
    dry_run: bool,
) -> dict[str, Any]:
    if dry_run:
        return {
            "status": "dry_run",
            "daily_refresh_ran_after_replay": False,
            "daily_prices_output": daily_prices_output,
            "would_fetch_date": trade_date,
            "would_generate_daily_score_date": trade_date,
            "next_trade_date_candidate": next_trade_date,
        }
    fetch_args = [
        "scripts/fetch_public_daily_prices.py",
        "--universe-csv",
        universe_csv,
        "--max-symbols",
        str(daily_max_symbols),
        "--count",
        "5",
        "--start-date",
        trade_date,
        "--end-date",
        trade_date,
        "--output",
        daily_prices_output,
        "--sleep-sec",
        str(sleep_seconds),
    ]
    code, fetch_payload, fetch_stderr = _run_json_command(fetch_args)
    if code != 0:
        return {
            "status": "blocked",
            "daily_refresh_blocked_reason": "DAILY_PRICE_FETCH_FAILED",
            "fetch": fetch_payload,
            "stderr_tail": fetch_stderr,
        }
    if fetch_payload.get("max_date") != trade_date:
        return {
            "status": "blocked",
            "daily_refresh_blocked_reason": "DAILY_PRICE_FOR_TRADE_DATE_MISSING",
            "fetch": fetch_payload,
        }
    prepare_args = [
        "scripts/prepare_day_replay_db.py",
        "--db",
        str(db_path),
        "--run-swing-pipeline",
        "--prices-csv",
        daily_prices_output,
        "--universe-csv",
        universe_csv,
        "--pipeline-source",
        "csv",
        "--start-date",
        trade_date,
        "--end-date",
        next_trade_date,
        "--market-symbol",
        market_symbol,
        "--top-n",
        str(top_n),
        "--dataset-plan-md",
        f"reports/day_dataset_plan_eod_{trade_date}_{next_trade_date}.md",
        "--audit-report-md",
        f"reports/day_data_availability_eod_{trade_date}_{next_trade_date}.md",
    ]
    p_code, prepare_payload, prepare_stderr = _run_json_command(prepare_args)
    if p_code != 0 or prepare_payload.get("status") == "blocked":
        reason = prepare_payload.get("blocked_reason") or prepare_payload.get("swing_pipeline", {}).get("reason") or "DAILY_SCORES_GENERATION_FAILED"
        return {
            "status": "blocked",
            "daily_refresh_blocked_reason": reason,
            "fetch": fetch_payload,
            "prepare": prepare_payload,
            "stderr_tail": prepare_stderr,
        }
    conn = get_connection(db_path)
    init_db(conn)
    readiness = _score_ready_for_next_trade_date(conn, trade_date, next_trade_date)
    conn.close()
    if not readiness["refreshed_daily_price_date"]:
        return {"status": "blocked", "daily_refresh_blocked_reason": "DAILY_PRICE_FOR_TRADE_DATE_MISSING", "fetch": fetch_payload, "prepare": prepare_payload, **readiness}
    if not readiness["generated_daily_score_date"]:
        return {"status": "blocked", "daily_refresh_blocked_reason": "NEXT_DAY_SCORE_DATE_NOT_GENERATED", "fetch": fetch_payload, "prepare": prepare_payload, **readiness}
    return {
        "status": "ok",
        "daily_refresh_ran_after_replay": True,
        "fetch": fetch_payload,
        "prepare": prepare_payload,
        **readiness,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run quote-only KIS end-of-day DAY ops and next-day SWING score refresh")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--trade-date", default=None)
    parser.add_argument("--auto-trade-date", action="store_true")
    parser.add_argument("--market-symbol", default="069500")
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--max-symbols", type=int, default=20)
    parser.add_argument("--require-full-top-n-coverage", action="store_true")
    parser.add_argument("--zero-volume-bar-policy", default="strict_invalid", choices=["strict_invalid", "no_trade_context", "drop_no_trade"])
    parser.add_argument("--compare-zero-volume-policies", action="store_true")
    parser.add_argument("--rolling-windows", default="3,5,20,60")
    parser.add_argument("--output-dir", default="reports/daily_ops")
    parser.add_argument("--data-output-dir", default="data/intraday")
    parser.add_argument("--daily-prices-output", default="data/public_daily_prices_eod.csv")
    parser.add_argument("--universe-csv", default="data/krx_source_universe_500.csv")
    parser.add_argument("--refresh-daily-after-replay", action="store_true")
    parser.add_argument("--skip-daily-refresh", action="store_true")
    parser.add_argument("--skip-intraday-collection", action="store_true")
    parser.add_argument("--skip-replay", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-refresh", action="store_true", help="Pass scoped intraday force-refresh through to daily KIS ops")
    parser.set_defaults(include_partial_sessions=False)
    parser.add_argument("--include-partial-sessions", dest="include_partial_sessions", action="store_true", help="Include partial sessions in rolling replay for debugging only")
    parser.add_argument("--exclude-partial-sessions", dest="include_partial_sessions", action="store_false", help="Exclude partial sessions from rolling replay (default)")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--sleep-seconds", type=float, default=0.3)
    parser.add_argument("--daily-max-symbols", type=int, default=None, help="Max symbols for public daily refresh; defaults to max(top_n,max_symbols)")
    parser.add_argument("--max-score-staleness-days", type=int, default=3)
    parser.add_argument("--allow-stale-score", action="store_true")
    args = parser.parse_args()

    trade_date = args.trade_date or (_auto_trade_date() if args.auto_trade_date else None)
    output_dir = Path(args.output_dir)
    db_path = Path(args.db)
    payload: dict[str, Any] = {
        "status": "ok",
        "run_at": datetime.now(ZoneInfo("Asia/Seoul")).isoformat(),
        "db": str(db_path),
        "trade_date": trade_date,
        "replay_trade_date": trade_date,
        "market_symbol": args.market_symbol,
        "top_n": args.top_n,
        "max_symbols": args.max_symbols,
        "zero_volume_bar_policy": args.zero_volume_bar_policy,
        "compare_zero_volume_policies": bool(args.compare_zero_volume_policies),
        "daily_refresh_enabled": bool(args.refresh_daily_after_replay and not args.skip_daily_refresh),
        "include_partial_sessions": bool(args.include_partial_sessions),
        "daily_refresh_ran_after_replay": False,
        "dry_run": bool(args.dry_run),
        "operation_order": [],
    }
    if not trade_date:
        payload.update({"status": "blocked", "blocked_reason": "TRADE_DATE_REQUIRED", "next_actions": ["Pass --trade-date YYYY-MM-DD or --auto-trade-date."]})
        payload["status_files"] = _write_status_files(output_dir, payload)
        _json_dump(payload)
        raise SystemExit(2)
    if not db_path.exists():
        payload.update({"status": "blocked", "blocked_reason": "DB_MISSING", "next_actions": ["Run scripts/bootstrap_market_db.py and load daily_prices/daily_scores first."]})
        payload["status_files"] = _write_status_files(output_dir, payload)
        _json_dump(payload)
        raise SystemExit(2)
    env_result = load_kis_env(args.env_file)
    payload["env_status"] = env_result.to_dict()
    if env_result.status != "ok" and not args.dry_run and not args.skip_intraday_collection:
        payload.update({"status": "blocked", "blocked_reason": env_result.reason, "next_actions": ["Check .env via scripts/check_kis_env.py without printing secrets."]})
        payload["status_files"] = _write_status_files(output_dir, payload)
        _json_dump(payload)
        raise SystemExit(2)

    conn = get_connection(db_path)
    init_db(conn)
    before = _score_selection(conn, trade_date, args.market_symbol, args.top_n)
    conn.close()
    payload.update(
        {
            "score_date_used_for_replay": before.get("score_date_used_for_replay"),
            "replay_uses_same_day_score": bool(before.get("same_day_score_used")),
            "score_staleness_days_before_refresh": before.get("score_staleness_days"),
            "score_date_check_before_refresh": before,
        }
    )
    if before.get("same_day_score_used"):
        payload.update({"status": "blocked", "blocked_reason": "SAME_DAY_SCORE_FORBIDDEN", "next_actions": ["Regenerate or load a prior daily_scores date; do not use same-day post-close scores for replay."]})
        payload["status_files"] = _write_status_files(output_dir, payload)
        _json_dump(payload)
        raise SystemExit(1)
    if not before.get("score_date"):
        payload.update({"status": "blocked", "blocked_reason": "NO_PRIOR_SCORE_DATE", "next_actions": ["Load or generate daily_scores for a date before the replay trade_date."]})
        payload["status_files"] = _write_status_files(output_dir, payload)
        _json_dump(payload)
        raise SystemExit(1)
    if int(before.get("score_staleness_days") or 0) > args.max_score_staleness_days and not args.allow_stale_score:
        payload.update({"status": "blocked", "blocked_reason": "STALE_SCORE_DATE", "next_actions": ["Run end-of-day daily refresh after replay, or refresh daily_prices/daily_scores before the next session."]})
        payload["status_files"] = _write_status_files(output_dir, payload)
        _json_dump(payload)
        raise SystemExit(1)

    next_trade_date = _next_weekday(trade_date)
    payload["next_trade_date_candidate"] = next_trade_date

    if args.skip_intraday_collection:
        payload["intraday_ops"] = {"status": "skipped", "reason": "SKIP_INTRADAY_COLLECTION_REQUESTED"}
    else:
        daily_ops_args = [
            "scripts/run_kis_daily_ops.py",
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
            "--zero-volume-bar-policy",
            args.zero_volume_bar_policy,
            "--rolling-windows",
            args.rolling_windows,
            "--output-dir",
            args.output_dir,
            "--data-output-dir",
            args.data_output_dir,
            "--env-file",
            args.env_file,
            "--token-cache",
            args.token_cache,
            "--sleep-seconds",
            str(args.sleep_seconds),
            "--max-score-staleness-days",
            str(args.max_score_staleness_days),
        ]
        if args.include_partial_sessions:
            daily_ops_args.append("--include-partial-sessions")
        else:
            daily_ops_args.append("--exclude-partial-sessions")
        if args.require_full_top_n_coverage:
            daily_ops_args.append("--require-full-top-n-coverage")
        if args.compare_zero_volume_policies:
            daily_ops_args.append("--compare-zero-volume-policies")
        if args.skip_replay:
            daily_ops_args.append("--skip-replay")
        if args.allow_stale_score:
            daily_ops_args.append("--allow-stale-score")
        if args.force_refresh:
            daily_ops_args.append("--force-refresh")
        if args.dry_run:
            daily_ops_args.append("--dry-run")
        payload["operation_order"].append("intraday_replay_before_daily_refresh")
        code, daily_payload, stderr_tail = _run_json_command(daily_ops_args)
        payload["intraday_ops_returncode"] = code
        payload["intraday_ops"] = _compact_daily_ops(daily_payload)
        if stderr_tail:
            payload["intraday_ops_stderr_tail"] = stderr_tail
        if code != 0:
            payload.update({"status": "blocked", "blocked_reason": daily_payload.get("blocked_reason") or "DAILY_INTRADAY_OPS_FAILED"})

    rolling = payload.get("intraday_ops", {}).get("rolling", {}) if isinstance(payload.get("intraday_ops"), dict) else {}
    payload["rolling_summary"] = {
        "complete_replayable_days": rolling.get("complete_replayable_days", []),
        "partial_replayable_days": rolling.get("partial_replayable_days", []),
        "excluded_partial_dates": rolling.get("excluded_partial_dates", []),
        "blocked_windows": rolling.get("blocked_windows", []),
    }

    refresh_result: dict[str, Any]
    if args.skip_daily_refresh or not args.refresh_daily_after_replay:
        refresh_result = {
            "status": "skipped",
            "daily_refresh_blocked_reason": "DAILY_REFRESH_DISABLED",
            "daily_refresh_ran_after_replay": False,
        }
    elif payload.get("status") == "blocked":
        refresh_result = {
            "status": "skipped",
            "daily_refresh_blocked_reason": "REPLAY_OR_COLLECTION_BLOCKED",
            "daily_refresh_ran_after_replay": False,
        }
    else:
        payload["operation_order"].append("daily_refresh_after_replay")
        refresh_result = _daily_refresh(
            db_path=db_path,
            trade_date=trade_date,
            next_trade_date=next_trade_date,
            daily_prices_output=args.daily_prices_output,
            universe_csv=args.universe_csv,
            market_symbol=args.market_symbol,
            top_n=args.top_n,
            sleep_seconds=args.sleep_seconds,
            daily_max_symbols=int(args.daily_max_symbols or max(args.top_n, args.max_symbols)),
            dry_run=bool(args.dry_run),
        )
        if refresh_result.get("status") == "blocked":
            payload.update({"status": "blocked", "blocked_reason": refresh_result.get("daily_refresh_blocked_reason")})
    payload["daily_refresh"] = refresh_result
    payload["daily_refresh_ran_after_replay"] = bool(refresh_result.get("daily_refresh_ran_after_replay"))
    payload["daily_refresh_blocked_reason"] = refresh_result.get("daily_refresh_blocked_reason")
    payload["refreshed_daily_price_date"] = refresh_result.get("refreshed_daily_price_date")
    payload["generated_daily_score_date"] = refresh_result.get("generated_daily_score_date")
    payload["score_ready_for_next_trade_date"] = refresh_result.get("score_ready_for_next_trade_date")
    payload["score_staleness_days_after_refresh"] = 1 if refresh_result.get("score_ready_for_next_trade_date") else None
    if payload.get("blocked_reason") and "next_actions" not in payload:
        payload["next_actions"] = _next_actions_for_daily_refresh(payload.get("blocked_reason"))
    payload["status_files"] = _write_status_files(output_dir, payload)
    _json_dump(payload)
    if payload["status"] == "blocked":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
