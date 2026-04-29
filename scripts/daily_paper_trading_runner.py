#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from pathlib import Path


BASELINE_OLD_CONFIG = {
    "source": "krx",
    "universe_mode": "rolling_liquidity",
    "universe_size": 100,
    "universe_lookback_days": 20,
    "scoring_version": "old",
    "top_n": 5,
    "rebalance_frequency": "weekly",
    "min_holding_days": 10,
    "keep_rank_threshold": 9,
    "enable_position_stop_loss": True,
    "position_stop_loss_pct": 0.10,
    "stop_loss_cash_mode": "keep_cash",
    "stop_loss_cooldown_days": 0,
    "enable_trailing_stop": False,
    "market_filter": False,
    "entry_gate": False,
    "overheat_entry_gate": False,
    "entry_quality_gate": False,
}


def build_run_pipeline_command(db: str, universe_file: str | None = None) -> list[str]:
    cmd = [
        sys.executable,
        "scripts/run_pipeline.py",
        "--db",
        db,
        "--source",
        "krx",
        "--universe-mode",
        "rolling_liquidity",
        "--universe-size",
        "100",
        "--universe-lookback-days",
        "20",
        "--scoring-version",
        "old",
        "--top-n",
        "5",
        "--rebalance-frequency",
        "weekly",
        "--min-holding-days",
        "10",
        "--keep-rank-threshold",
        "9",
        "--enable-position-stop-loss",
        "--position-stop-loss-pct",
        "0.10",
        "--stop-loss-cash-mode",
        "keep_cash",
        "--stop-loss-cooldown-days",
        "0",
    ]
    if universe_file:
        cmd.extend(["--universe-file", universe_file])
    return cmd


def _latest_backtest_run(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM backtest_runs ORDER BY created_at DESC, rowid DESC LIMIT 1"
    ).fetchone()
    if row is None:
        raise RuntimeError("No backtest_runs found for guardrail verification")
    return row


def verify_baseline_old_guardrails(config: dict, has_daily_universe: bool = True) -> None:
    mismatches = []

    def expect(key: str, expected):
        actual = config.get(key)
        if actual != expected:
            mismatches.append(f"{key}: expected={expected!r}, actual={actual!r}")

    expect("scoring_profile", "old")
    um = config.get("universe_mode")
    if um != "rolling_liquidity" and not has_daily_universe:
        mismatches.append("universe_mode must be rolling_liquidity or latest daily_universe must be present")
    expect("top_n", 5)
    expect("rebalance_frequency", "weekly")
    expect("min_holding_days", 10)
    expect("keep_rank_threshold", 9)
    expect("enable_position_stop_loss", True)
    expect("position_stop_loss_pct", 0.10)
    expect("stop_loss_cash_mode", "keep_cash")

    max_w = config.get("max_single_position_weight")
    if max_w is None or float(max_w) > 0.20:
        mismatches.append(f"max_single_position_weight must be <= 0.20, actual={max_w!r}")

    if mismatches:
        raise RuntimeError("Baseline old guardrail mismatch: " + "; ".join(mismatches))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--universe-file")
    p.add_argument("--reports-dir", required=True)
    p.add_argument("--skip-update", action="store_true")
    p.add_argument("--no-display", action="store_true")
    args = p.parse_args()

    Path(args.reports_dir).mkdir(parents=True, exist_ok=True)
    effective = dict(BASELINE_OLD_CONFIG)
    print("[baseline_old] effective_config(before):")
    print(json.dumps(effective, ensure_ascii=False, indent=2))

    if not args.skip_update:
        cmd = build_run_pipeline_command(args.db, args.universe_file)
        print("[run] " + " ".join(cmd))
        subprocess.run(cmd, check=True)
    else:
        print("[skip-update] pipeline update skipped: no run_pipeline invocation")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    row = _latest_backtest_run(conn)
    cfg = json.loads(row["config_json"]) if "config_json" in row.keys() and row["config_json"] else {}
    latest_daily_universe = conn.execute("SELECT 1 FROM daily_universe ORDER BY date DESC LIMIT 1").fetchone() is not None
    verify_baseline_old_guardrails(cfg, has_daily_universe=latest_daily_universe)

    log_keys = [
        "universe_mode",
        "universe_size",
        "scoring_version",
        "top_n",
        "rebalance_frequency",
        "min_holding_days",
        "keep_rank_threshold",
        "position_stop_loss_enabled",
        "position_stop_loss_pct",
        "stop_loss_cash_mode",
    ]
    after = {k: cfg.get(k) for k in log_keys}
    print("[baseline_old] effective_config(after):")
    print(json.dumps(after, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
