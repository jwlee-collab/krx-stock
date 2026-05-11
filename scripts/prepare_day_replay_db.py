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

from pipeline.day_trading.availability import (
    build_day_data_availability_markdown,
    build_day_data_availability_report,
    write_day_data_availability_report,
)
from pipeline.day_trading.daily_prices_loader import load_daily_prices_csv
from pipeline.day_trading.daily_scores_loader import load_daily_scores_csv
from pipeline.day_trading.data_loader import load_intraday_prices_csv
from pipeline.day_trading.dataset_plan import (
    build_day_replay_dataset_plan,
    build_day_replay_dataset_plan_markdown,
    write_day_replay_dataset_plan_report,
)
from pipeline.day_trading.backtest import run_day_replay_backtest
from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.data_quality import validate_intraday_prices
from pipeline.day_trading.reporting import build_day_validation_markdown, write_day_validation_report
from pipeline.day_trading.validation import DayValidationGate
from pipeline.db import get_connection, init_db
from pipeline.features import generate_daily_features
from pipeline.ingest import ingest_daily_prices_csv
from pipeline.scoring import DEFAULT_SCORING_PROFILE, generate_daily_scores
from pipeline.universe_input import load_symbols_from_universe_csv


def _step(status: str, **extra: Any) -> dict[str, Any]:
    return {"status": status, **extra}


def _missing_file_step(path: str | None, label: str) -> dict[str, Any]:
    if not path:
        return _step("skipped", reason=f"{label.upper()}_CSV_NOT_PROVIDED")
    return _step("blocked", reason=f"{label.upper()}_CSV_NOT_FOUND", path=path)


def _evaluate_gate(replay: dict[str, Any], quality: dict[str, Any]) -> dict[str, Any]:
    lookahead = replay.get("lookahead_validation", {})
    audit = replay.get("session_audit", {})
    event_counts = replay.get("event_counts", {})
    return DayValidationGate().evaluate(
        replay.get("performance", {}),
        observed_days=len(replay.get("candidate_counts", {})),
        data_quality_passed=quality.get("candidate_usable_symbol_count", 0) > 0,
        lookahead_passed=(
            lookahead.get("future_candle_violations", 0) == 0
            and lookahead.get("lookahead_score_violations", 0) == 0
        ),
        market_proxy_available=bool(quality.get("market_proxy_available")),
        session_complete=audit.get("session_complete"),
        missing_force_exit_window=audit.get("missing_force_exit_window"),
        open_position_count_at_end=audit.get("open_position_count_at_end"),
        paper_entry_count=event_counts.get("PAPER_ENTRY", 0),
        paper_exit_count=event_counts.get("PAPER_EXIT", 0),
    )


def _table_summary(conn, table: str, date_expr: str = "date") -> dict[str, Any]:
    exists = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None
    if not exists:
        return {"exists": False, "row_count": 0, "date_count": 0, "min_date": None, "max_date": None}
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS row_count,
               COUNT(DISTINCT {date_expr}) AS date_count,
               MIN({date_expr}) AS min_date,
               MAX({date_expr}) AS max_date
        FROM {table}
        """
    ).fetchone()
    return {
        "exists": True,
        "row_count": int(row["row_count"] or 0),
        "date_count": int(row["date_count"] or 0),
        "min_date": row["min_date"],
        "max_date": row["max_date"],
    }


def _score_symbol_count_summary(conn) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT date, COUNT(*) AS symbol_count
        FROM daily_scores
        GROUP BY date
        ORDER BY date
        """
    ).fetchall()
    counts = {str(row["date"]): int(row["symbol_count"] or 0) for row in rows}
    values = list(counts.values())
    return {
        "by_date": counts,
        "min_symbols_per_date": min(values) if values else 0,
        "max_symbols_per_date": max(values) if values else 0,
        "sample": dict(list(counts.items())[:10]),
    }


def _date_range(start_date: str, end_date: str) -> list[str]:
    from datetime import date, timedelta

    cur = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    out: list[str] = []
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _previous_score_summary(conn, start_date: str, end_date: str) -> dict[str, Any]:
    per_date: dict[str, dict[str, Any]] = {}
    without_prior = 0
    same_day_only = 0
    for trade_date in _date_range(start_date, end_date):
        previous = conn.execute("SELECT MAX(date) AS d FROM daily_scores WHERE date < ?", (trade_date,)).fetchone()["d"]
        same_day_exists = conn.execute("SELECT 1 FROM daily_scores WHERE date=? LIMIT 1", (trade_date,)).fetchone() is not None
        item = {
            "previous_score_date": previous,
            "same_day_score_exists": same_day_exists,
            "same_day_only_forbidden": bool(same_day_exists and not previous),
            "usable_by_default": previous is not None,
        }
        if not previous:
            without_prior += 1
        if item["same_day_only_forbidden"]:
            same_day_only += 1
        per_date[trade_date] = item
    return {
        "per_trade_date": per_date,
        "dates_without_prior_score_count": without_prior,
        "same_day_score_only_forbidden_count": same_day_only,
    }


def _run_swing_pipeline_step(
    conn,
    prices_csv: str | None,
    universe_csv: str | None,
    pipeline_source: str,
    dry_run: bool,
    start_date: str,
    end_date: str,
    min_score_dates: int | None,
    min_score_symbols_per_date: int | None,
    require_generated_daily_scores: bool,
) -> dict[str, Any]:
    if pipeline_source != "csv":
        return _step("blocked", reason="PIPELINE_SOURCE_REQUIRES_EXTERNAL_API_OR_UNSUPPORTED", pipeline_source=pipeline_source)
    if not prices_csv:
        return _step("blocked", reason="MISSING_PRICES_CSV")
    prices_path = Path(prices_csv)
    if not prices_path.exists():
        return _step("blocked", reason="MISSING_PRICES_CSV", path=str(prices_path))
    allowed_symbols = None
    if universe_csv:
        universe_path = Path(universe_csv)
        if not universe_path.exists():
            return _step("blocked", reason="MISSING_UNIVERSE_CSV", path=str(universe_path))
        allowed_symbols = load_symbols_from_universe_csv(universe_path)
    if dry_run:
        return _step(
            "dry_run",
            prices_csv=str(prices_path),
            universe_csv=universe_csv,
            allowed_symbol_count=len(allowed_symbols or []),
            would_generate=["daily_prices", "daily_features", "daily_scores"],
        )

    try:
        ingest_changes = ingest_daily_prices_csv(conn, prices_path)
        feature_changes = generate_daily_features(conn)
        score_changes = generate_daily_scores(
            conn,
            include_history=True,
            allowed_symbols=allowed_symbols,
            scoring_profile=DEFAULT_SCORING_PROFILE,
        )
    except Exception as exc:
        return _step("blocked", reason="SWING_PIPELINE_FAILED", error=str(exc))

    prices_summary = _table_summary(conn, "daily_prices")
    features_summary = _table_summary(conn, "daily_features")
    scores_summary = _table_summary(conn, "daily_scores")
    score_counts = _score_symbol_count_summary(conn)
    previous_summary = _previous_score_summary(conn, start_date, end_date)
    blocked_reasons: list[str] = []
    if require_generated_daily_scores and int(scores_summary["row_count"]) <= 0:
        blocked_reasons.append("NO_DAILY_SCORES_GENERATED")
    if min_score_dates is not None and int(scores_summary["date_count"]) < int(min_score_dates):
        blocked_reasons.append("MIN_SCORE_DATES_NOT_MET")
    if (
        min_score_symbols_per_date is not None
        and int(score_counts["min_symbols_per_date"]) < int(min_score_symbols_per_date)
    ):
        blocked_reasons.append("MIN_SCORE_SYMBOLS_PER_DATE_NOT_MET")
    if previous_summary["same_day_score_only_forbidden_count"] > 0:
        blocked_reasons.append("SAME_DAY_SCORE_ONLY_FORBIDDEN")
    return _step(
        "blocked" if blocked_reasons else "ok",
        reason=blocked_reasons[0] if blocked_reasons else None,
        blocked_reasons=blocked_reasons,
        prices_csv=str(prices_path),
        universe_csv=universe_csv,
        allowed_symbol_count=len(allowed_symbols or []),
        ingest_changes=ingest_changes,
        feature_changes=feature_changes,
        score_changes=score_changes,
        daily_prices=prices_summary,
        daily_features=features_summary,
        daily_scores=scores_summary,
        daily_scores_symbol_counts=score_counts,
        previous_score_date_summary=previous_summary,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare a local DB for DAY replay validation from CSV inputs")
    parser.add_argument("--db", default="data/market_pipeline.db")
    parser.add_argument("--daily-prices-csv", default=None)
    parser.add_argument("--daily-scores-csv", default=None)
    parser.add_argument("--intraday-csv", default=None)
    parser.add_argument("--run-swing-pipeline", action="store_true")
    parser.add_argument("--prices-csv", default=None, help="CSV passed to the existing SWING daily_prices pipeline")
    parser.add_argument("--universe-csv", default=None, help="Optional symbol universe CSV for SWING scoring")
    parser.add_argument("--pipeline-source", default="csv", choices=["csv", "krx"])
    parser.add_argument("--skip-direct-daily-scores-load", action="store_true")
    parser.add_argument("--swing-pipeline-dry-run", action="store_true")
    parser.add_argument("--require-generated-daily-scores", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--min-score-dates", type=int, default=None)
    parser.add_argument("--min-score-symbols-per-date", type=int, default=None)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--market-symbol", required=True)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--bootstrap", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--audit-report-md", default=None)
    parser.add_argument("--dataset-plan-md", default=None)
    parser.add_argument("--replay-report-md", default=None)
    parser.add_argument("--run-replay", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_exists_before = db_path.exists()
    output: dict[str, Any] = {
        "status": "ok",
        "db_path": str(db_path),
        "db_exists_before": db_exists_before,
        "dry_run": bool(args.dry_run),
        "run_replay": bool(args.run_replay),
        "run_swing_pipeline": bool(args.run_swing_pipeline),
        "pipeline_source": args.pipeline_source,
        "prices_csv": args.prices_csv or args.daily_prices_csv,
        "universe_csv": args.universe_csv,
        "require_generated_daily_scores": bool(args.require_generated_daily_scores),
        "steps": {},
    }

    if args.run_swing_pipeline and args.daily_scores_csv and not args.skip_direct_daily_scores_load:
        output["status"] = "blocked"
        output["blocked_reason"] = "SWING_PIPELINE_AND_DAILY_SCORES_CSV_CONFLICT"
        output["steps"]["argument_validation"] = _step(
            "blocked",
            reason="Use either --run-swing-pipeline or --daily-scores-csv, or pass --skip-direct-daily-scores-load",
        )
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(2)

    if not db_exists_before and not args.bootstrap:
        output["status"] = "blocked"
        output["blocked_reason"] = "DB_MISSING_AND_BOOTSTRAP_NOT_REQUESTED"
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(2)

    if args.bootstrap:
        output["steps"]["bootstrap"] = _step(
            "dry_run" if args.dry_run else "ok",
            would_create_db=not db_exists_before,
            schema_ensured=not args.dry_run,
        )
        if not args.dry_run:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = get_connection(db_path)
            init_db(conn)
            conn.commit()
            conn.close()
    else:
        output["steps"]["bootstrap"] = _step("skipped", reason="BOOTSTRAP_NOT_REQUESTED")

    if not db_path.exists():
        output["status"] = "blocked"
        output["blocked_reason"] = "DB_NOT_AVAILABLE_AFTER_BOOTSTRAP_STEP"
        for label, path in [
            ("daily_prices", args.daily_prices_csv),
            ("daily_scores", args.daily_scores_csv),
            ("intraday", args.intraday_csv),
        ]:
            output["steps"][label] = _missing_file_step(path, label) if path else _step("skipped", reason=f"{label.upper()}_CSV_NOT_PROVIDED")
        output["steps"]["dataset_plan"] = _step("skipped", reason="DB_NOT_AVAILABLE")
        output["steps"]["availability_audit"] = _step("skipped", reason="DB_NOT_AVAILABLE")
        output["steps"]["replay"] = _step("skipped", reason="DB_NOT_AVAILABLE")
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(2)

    conn = get_connection(db_path)
    init_db(conn)

    try:
        if args.run_swing_pipeline and args.daily_prices_csv and not args.prices_csv:
            output["steps"]["daily_prices"] = _step("skipped", reason="DAILY_PRICES_CSV_USED_AS_SWING_PRICES_CSV")
        elif args.daily_prices_csv:
            path = Path(args.daily_prices_csv)
            if not path.exists():
                output["steps"]["daily_prices"] = _missing_file_step(args.daily_prices_csv, "daily_prices")
                output["status"] = "blocked"
            else:
                result = load_daily_prices_csv(conn, path, dry_run=args.dry_run)
                output["steps"]["daily_prices"] = _step("dry_run" if args.dry_run else "ok", load=result.__dict__)
        else:
            output["steps"]["daily_prices"] = _step("skipped", reason="DAILY_PRICES_CSV_NOT_PROVIDED")

        if args.run_swing_pipeline:
            swing_prices_csv = args.prices_csv or args.daily_prices_csv
            swing_step = _run_swing_pipeline_step(
                conn,
                prices_csv=swing_prices_csv,
                universe_csv=args.universe_csv,
                pipeline_source=args.pipeline_source,
                dry_run=bool(args.dry_run or args.swing_pipeline_dry_run),
                start_date=args.start_date,
                end_date=args.end_date,
                min_score_dates=args.min_score_dates,
                min_score_symbols_per_date=args.min_score_symbols_per_date,
                require_generated_daily_scores=bool(args.require_generated_daily_scores),
            )
            output["steps"]["swing_pipeline"] = swing_step
            if swing_step["status"] == "blocked":
                output["status"] = "blocked"
                output["blocked_reason"] = swing_step.get("reason") or "SWING_PIPELINE_BLOCKED"
        else:
            output["steps"]["swing_pipeline"] = _step("skipped", reason="RUN_SWING_PIPELINE_NOT_REQUESTED")

        if args.daily_scores_csv and args.skip_direct_daily_scores_load:
            output["steps"]["daily_scores"] = _step("skipped", reason="SKIP_DIRECT_DAILY_SCORES_LOAD_REQUESTED")
        elif args.daily_scores_csv:
            path = Path(args.daily_scores_csv)
            if not path.exists():
                output["steps"]["daily_scores"] = _missing_file_step(args.daily_scores_csv, "daily_scores")
                output["status"] = "blocked"
            else:
                result = load_daily_scores_csv(
                    conn,
                    path,
                    dry_run=args.dry_run,
                    trade_start_date=args.start_date,
                    trade_end_date=args.end_date,
                )
                step_status = "dry_run" if args.dry_run else "ok"
                if result.input_rows > 0 and result.valid_rows == 0:
                    step_status = "blocked"
                    output["status"] = "blocked"
                output["steps"]["daily_scores"] = _step(step_status, load=result.__dict__)
        else:
            output["steps"]["daily_scores"] = _step("skipped", reason="DAILY_SCORES_CSV_NOT_PROVIDED")

        if args.intraday_csv:
            path = Path(args.intraday_csv)
            if not path.exists():
                output["steps"]["intraday"] = _missing_file_step(args.intraday_csv, "intraday")
                output["status"] = "blocked"
            else:
                result = load_intraday_prices_csv(conn, path, source="DAY_REPLAY_PREP", dry_run=args.dry_run)
                output["steps"]["intraday"] = _step("dry_run" if args.dry_run else "ok", load=result.__dict__)
        else:
            output["steps"]["intraday"] = _step("skipped", reason="INTRADAY_CSV_NOT_PROVIDED")

        output["steps"]["database_summary"] = _step(
            "ok",
            daily_prices=_table_summary(conn, "daily_prices"),
            daily_features=_table_summary(conn, "daily_features"),
            daily_scores=_table_summary(conn, "daily_scores"),
            intraday_prices=_table_summary(conn, "intraday_prices"),
            daily_scores_symbol_counts=_score_symbol_count_summary(conn),
            previous_score_date_summary=_previous_score_summary(conn, args.start_date, args.end_date),
        )

        plan = build_day_replay_dataset_plan(
            conn,
            args.start_date,
            args.end_date,
            args.market_symbol,
            top_n=args.top_n,
        )
        output["steps"]["dataset_plan"] = _step("ok", summary=plan.get("summary", {}))
        if args.dataset_plan_md:
            markdown = build_day_replay_dataset_plan_markdown(plan)
            output["steps"]["dataset_plan"]["report_path"] = str(
                write_day_replay_dataset_plan_report(args.dataset_plan_md, markdown)
            )

        availability = build_day_data_availability_report(
            conn,
            start_date=args.start_date,
            end_date=args.end_date,
            market_proxy_symbol=args.market_symbol,
            max_universe_symbols=args.top_n,
        )
        output["steps"]["availability_audit"] = _step("ok", summary=availability.get("summary", {}))
        if args.audit_report_md:
            markdown = build_day_data_availability_markdown(availability)
            output["steps"]["availability_audit"]["report_path"] = str(
                write_day_data_availability_report(args.audit_report_md, markdown)
            )
        if (
            output["status"] == "ok"
            and not args.dry_run
            and int(availability.get("summary", {}).get("replayable_date_count", 0)) <= 0
        ):
            output["status"] = "blocked"
            output["blocked_reason"] = "DATA_NOT_REPLAYABLE_AFTER_PREPARE"

        if args.run_replay:
            cfg = DayTradingConfig(
                enabled=True,
                mode="PAPER",
                market_proxy_symbol=args.market_symbol,
                max_universe_symbols=args.top_n,
            )
            quality = validate_intraday_prices(
                conn,
                start_date=args.start_date,
                end_date=args.end_date,
                market_proxy_symbol=args.market_symbol,
            )
            replay = run_day_replay_backtest(conn, args.start_date, args.end_date, config=cfg)
            replay["data_availability"] = {
                "summary": availability.get("summary", {}),
                "replayable_dates": availability.get("replayable_dates", []),
                "unreplayable_dates": availability.get("unreplayable_dates", {}),
            }
            gate = _evaluate_gate(replay, quality)
            replay_status = "ok" if replay.get("candidate_counts") else "blocked"
            output["steps"]["replay"] = _step(
                replay_status,
                blocked_reason=None if replay_status == "ok" else "NO_REPLAY_DATES_WITH_INTRADAY_DATA",
                candidate_counts=replay.get("candidate_counts", {}),
                event_counts=replay.get("event_counts", {}),
                promotion_gate=gate,
            )
            if replay_status == "blocked":
                output["status"] = "blocked"
            if args.replay_report_md:
                markdown = build_day_validation_markdown(
                    replay,
                    quality,
                    gate,
                    universe_source=cfg.universe_source,
                    same_day_scores_allowed=cfg.allow_same_day_scores,
                    market_proxy_symbol=cfg.market_proxy_symbol,
                )
                output["steps"]["replay"]["report_path"] = str(write_day_validation_report(args.replay_report_md, markdown))
        else:
            output["steps"]["replay"] = _step("skipped", reason="RUN_REPLAY_NOT_REQUESTED")
    finally:
        conn.close()

    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    if output["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
