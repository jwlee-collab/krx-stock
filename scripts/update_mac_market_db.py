#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path


PIPELINE_TIMEOUT_SECONDS = 3600

BASELINE_ARGS = [
    "--source", "krx",
    "--scoring-version", "old",
    "--universe-mode", "rolling_liquidity",
    "--universe-size", "100",
    "--universe-lookback-days", "20",
    "--top-n", "5",
    "--rebalance-frequency", "weekly",
    "--min-holding-days", "10",
    "--keep-rank-threshold", "9",
    "--enable-position-stop-loss",
    "--position-stop-loss-pct", "0.10",
    "--stop-loss-cash-mode", "keep_cash",
    "--stop-loss-cooldown-days", "0",
]


TABLES_TO_SUMMARIZE = [
    "daily_prices",
    "daily_features",
    "daily_scores",
    "daily_universe",
    "backtest_results",
    "backtest_holdings",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mac MVP: update KOSPI paper-trading market DB")
    p.add_argument("--db", default="~/krx-stock-persist/data/kospi_495_rolling_3y.db")
    p.add_argument("--universe", default="~/krx-stock-persist/data/kospi_valid_universe_495.csv")
    p.add_argument("--logs-dir", default="~/krx-stock-persist/logs")
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force", action="store_true", help="Run pipeline even when DB is already up to date")
    return p.parse_args()


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    raise SystemExit(1)


def read_log_tail(log_path: Path, max_chars: int = 20000) -> str:
    if not log_path.exists():
        return ""
    try:
        text = log_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    return text[-max_chars:]


def classify_pipeline_failure(log_path: Path, *, timed_out: bool = False) -> str:
    tail = read_log_tail(log_path).lower()
    pykrx_markers = [
        "pykrx",
        "get_market_ohlcv_by_ticker",
        "none of [index(['시가'",
        "none of [index(['고가'",
        "none of [index(['저가'",
        "none of [index(['종가'",
        "keyerror",
    ]
    if any(marker in tail for marker in pykrx_markers):
        return "pykrx/provider failure"
    if timed_out:
        return "pipeline timeout"
    if "timeoutexpired" in tail or "timed out" in tail:
        return "pipeline timeout"
    return "pipeline failure"


def fail_pipeline(reason: str, log_path: Path, **fields: object) -> None:
    parts = [
        "stage=baseline_old pipeline",
        f"reason={reason}",
        f"log={log_path}",
        "production_fallback_applied=false",
        "fallback_available_but_not_applied_to_production_db=true",
    ]
    parts.extend(f"{key}={value}" for key, value in fields.items())
    fail("; ".join(parts))


def is_weekday(value: str) -> bool:
    try:
        return date.fromisoformat(value).weekday() < 5
    except ValueError:
        return True


def ensure_exists(path: Path, description: str) -> None:
    if not path.exists():
        fail(f"{description} not found: {path}")


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def table_stats(conn: sqlite3.Connection, table: str) -> dict[str, str | int | None]:
    row = conn.execute(
        f"SELECT MIN(date) AS min_date, MAX(date) AS max_date, COUNT(*) AS row_count FROM {table}"
    ).fetchone()
    return {
        "min": row["min_date"],
        "max": row["max_date"],
        "count": int(row["row_count"]),
    }


def print_stats(conn: sqlite3.Connection, label: str) -> None:
    print(f"[{label}] table stats")
    for table in TABLES_TO_SUMMARIZE:
        stats = table_stats(conn, table)
        print(f"- {table}: min={stats['min']} max={stats['max']} count={stats['count']}")


def fetch_latest_signal_date(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT MAX(s.date) AS latest_signal_date
        FROM daily_scores s
        JOIN daily_universe u
          ON s.date = u.date
         AND s.symbol = u.symbol
        WHERE u.universe_mode = 'rolling_liquidity'
          AND u.universe_size = 100
          AND u.lookback_days = 20
        """
    ).fetchone()
    return row["latest_signal_date"] if row else None


def fetch_latest_baseline_run(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT run_id, created_at
        FROM backtest_runs
        WHERE scoring_profile = 'old'
          AND top_n = 5
          AND rebalance_frequency = 'weekly'
          AND min_holding_days = 10
          AND keep_rank_threshold = 9
          AND market_filter_enabled = 0
          AND entry_gate_enabled = 0
          AND enable_overheat_entry_gate = 0
          AND entry_quality_gate_enabled = 0
          AND enable_position_stop_loss = 1
          AND ABS(position_stop_loss_pct - 0.10) < 1e-9
          AND stop_loss_cash_mode = 'keep_cash'
          AND stop_loss_cooldown_days = 0
          AND enable_trailing_stop = 0
          AND max_single_position_weight <= 0.20
        ORDER BY datetime(created_at) DESC
        LIMIT 1
        """
    ).fetchone()


def latest_holdings_date_for_run(conn: sqlite3.Connection, run_id: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) AS latest_holdings_date FROM backtest_holdings WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    return row["latest_holdings_date"] if row else None


def latest_results_date_for_run(conn: sqlite3.Connection, run_id: str) -> str | None:
    row = conn.execute(
        "SELECT MAX(date) AS latest_results_date FROM backtest_results WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    return row["latest_results_date"] if row else None


def main() -> None:
    args = parse_args()
    db_path = Path(args.db).expanduser()
    universe_path = Path(args.universe).expanduser()
    logs_dir = Path(args.logs_dir).expanduser()
    repo_root = Path(__file__).resolve().parents[1]

    ensure_exists(db_path, "DB file")
    ensure_exists(universe_path, "Universe file")
    logs_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_path = logs_dir / f"mac_market_db_update_{timestamp}.log"

    with connect_db(db_path) as conn:
        before_signal = fetch_latest_signal_date(conn)
        print_stats(conn, "before")
        if before_signal:
            print(f"[before] latest_signal_date={before_signal}")

        precheck_selected = fetch_latest_baseline_run(conn)
        precheck_run_id = precheck_selected["run_id"] if precheck_selected else None
        precheck_results_date = (
            latest_results_date_for_run(conn, precheck_run_id) if precheck_run_id else None
        )
        precheck_holdings_date = (
            latest_holdings_date_for_run(conn, precheck_run_id) if precheck_run_id else None
        )

    print(f"[precheck] latest_signal_date={before_signal}")
    print(f"[precheck] latest_baseline_run_id={precheck_run_id}")
    print(f"[precheck] latest_results_date={precheck_results_date}")
    print(f"[precheck] latest_holdings_date={precheck_holdings_date}")

    # A DB is internally consistent when latest_signal/results/holdings dates match.
    # But for scheduled daily ops, that is not enough: if --end-date is newer than
    # the DB latest date, we must still run the pipeline.
    target_end_date = args.end_date

    is_internally_consistent = (
        before_signal is not None
        and precheck_run_id is not None
        and precheck_results_date == before_signal
        and precheck_holdings_date == before_signal
        and precheck_results_date == precheck_holdings_date
    )

    is_fresh_enough_for_requested_end = (
        target_end_date is None
        or (before_signal is not None and before_signal >= target_end_date)
    )

    is_noop_ready = is_internally_consistent and is_fresh_enough_for_requested_end

    if is_noop_ready and not args.force:
        print("PASS: Mac market DB update completed. Database was already up to date.")
        print(f"Latest signal date: {before_signal}")
        print(f"Latest results date: {precheck_results_date}")
        print(f"Latest holdings date: {precheck_holdings_date}")
        return

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_pipeline.py"),
        "--db", str(db_path),
        "--universe-file", str(universe_path),
    ] + BASELINE_ARGS

    if args.start_date:
        cmd.extend(["--start-date", args.start_date])
    if args.end_date:
        cmd.extend(["--end-date", args.end_date])

    print(f"[log] writing pipeline output to {log_path}")
    print(f"[cmd] {' '.join(cmd)}")

    if not args.dry_run:
        try:
            with log_path.open("w", encoding="utf-8") as lf:
                proc = subprocess.run(
                    cmd,
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=PIPELINE_TIMEOUT_SECONDS,
                )
        except subprocess.TimeoutExpired:
            fail_pipeline(
                classify_pipeline_failure(log_path, timed_out=True),
                log_path,
                timeout_seconds=PIPELINE_TIMEOUT_SECONDS,
            )

        if proc.returncode != 0:
            fail_pipeline(
                classify_pipeline_failure(log_path),
                log_path,
                returncode=proc.returncode,
            )

    with connect_db(db_path) as conn:
        print_stats(conn, "after")
        selected = fetch_latest_baseline_run(conn)
        if not selected:
            fail("baseline_old run creation failed (no matching run found in backtest_runs)")

        selected_run_id = selected["run_id"]
        print(f"selected/latest baseline_old run_id={selected_run_id}")

        after_signal = fetch_latest_signal_date(conn)
        if not after_signal:
            fail("latest_signal_date using daily_scores JOIN daily_universe is missing")

        print(f"latest_signal_date using daily_scores JOIN daily_universe={after_signal}")

        if before_signal and after_signal < before_signal:
            fail(
                f"latest_signal_date moved backward (before={before_signal}, after={after_signal})"
            )

        latest_results_date = latest_results_date_for_run(conn, selected_run_id)
        if not latest_results_date:
            fail(f"latest_results_date missing for selected run_id={selected_run_id}")

        latest_holdings_date = latest_holdings_date_for_run(conn, selected_run_id)
        if not latest_holdings_date:
            fail(f"latest_holdings_date missing for selected run_id={selected_run_id}")

        print(f"latest_results_date using selected_run_id backtest_results={latest_results_date}")
        print(f"latest_holdings_date using selected_run_id backtest_holdings={latest_holdings_date}")

        if latest_holdings_date != latest_results_date:
            fail(
                "selected run has mismatched max date between backtest_results and backtest_holdings "
                f"(results={latest_results_date}, holdings={latest_holdings_date})"
            )

        if latest_results_date < after_signal:
            fail(
                "selected run backtest_results max date is older than latest_signal_date "
                f"(latest_signal_date={after_signal}, results_max={latest_results_date})"
            )

        if args.end_date and after_signal < args.end_date:
            if is_weekday(args.end_date):
                fail(
                    "stage=baseline_old freshness_check; reason=stale latest_signal_date; "
                    f"target_date={args.end_date}; latest_signal_date={after_signal}; "
                    "calendar_validation=weekday_only_uncertain; success_report_allowed=false"
                )
            print(
                "WARN: latest_signal_date is older than requested end-date "
                f"(end-date={args.end_date}, latest_signal_date={after_signal}); "
                "calendar_validation=weekday_only_uncertain"
            )

    if before_signal and after_signal == before_signal:
        print("PASS: Mac market DB update completed. Database was already up to date.")
    else:
        print("PASS: Mac market DB update completed.")
    print(f"Latest signal date: {after_signal}")
    print(f"Latest results date: {latest_results_date}")
    print(f"Latest holdings date: {latest_holdings_date}")


if __name__ == "__main__":
    try:
        main()
    except ModuleNotFoundError as exc:
        if exc.name == "pykrx":
            fail("pykrx is not installed. Please install pykrx before running this script.")
        raise
    except Exception as exc:
        fail(str(exc))
