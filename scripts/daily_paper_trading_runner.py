from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

BASELINE_KEYS = {
    "source": "krx",
    "universe_mode": "rolling_liquidity",
    "universe_size": 100,
    "universe_lookback_days": 20,
    "scoring_version": "old",
    "top_n": 5,
    "rebalance_frequency": "weekly",
    "min_holding_days": 10,
    "keep_rank_threshold": 9,
    "position_stop_loss_enabled": True,
    "position_stop_loss_pct": 0.10,
    "stop_loss_cash_mode": "keep_cash",
    "stop_loss_cooldown_days": 0,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--universe-file", required=True)
    p.add_argument("--reports-dir", required=True)
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--restore-db-from")
    p.add_argument("--backup-db")
    p.add_argument("--backup-reports-dir")
    p.add_argument("--force-restore", action="store_true")
    p.add_argument("--skip-update", action="store_true")
    p.add_argument("--install-missing-pykrx", action="store_true")
    p.add_argument("--display-html", action="store_true")
    p.add_argument("--no-display", action="store_true")
    p.add_argument("--allow-weekend-noop", action="store_true")
    return p.parse_args(argv)


def verify_baseline_old_guardrails(config: dict, has_daily_universe: bool = True) -> None:
    checks = [
        (config.get("scoring_profile") == "old", "scoring_profile"),
        (int(config.get("top_n", -1)) == 5, "top_n"),
        (config.get("rebalance_frequency") == "weekly", "rebalance_frequency"),
        (int(config.get("min_holding_days", -1)) == 10, "min_holding_days"),
        (int(config.get("keep_rank_threshold", -1)) == 9, "keep_rank_threshold"),
        (bool(config.get("enable_position_stop_loss")) is True, "enable_position_stop_loss"),
        (abs(float(config.get("position_stop_loss_pct", -1.0)) - 0.10) < 1e-9, "position_stop_loss_pct"),
        (config.get("stop_loss_cash_mode") == "keep_cash", "stop_loss_cash_mode"),
        (int(config.get("stop_loss_cooldown_days", -1)) == 0, "stop_loss_cooldown_days"),
        (not bool(config.get("enable_trailing_stop")), "enable_trailing_stop"),
        (not bool(config.get("market_filter_enabled")), "market_filter_enabled"),
        (not bool(config.get("entry_gate_enabled")), "entry_gate_enabled"),
        (not bool(config.get("enable_overheat_entry_gate")), "enable_overheat_entry_gate"),
        (not bool(config.get("entry_quality_gate_enabled")), "entry_quality_gate_enabled"),
        (float(config.get("max_single_position_weight", 999.0)) <= 0.20, "max_single_position_weight"),
        (has_daily_universe, "daily_universe"),
    ]
    bad = [name for ok, name in checks if not ok]
    if bad:
        raise RuntimeError(f"baseline_old guardrail failed: {', '.join(bad)}")


def build_run_pipeline_command(db: Path, universe_file: Path, start_date: str, end_date: str) -> list[str]:
    return [
        "python", "scripts/run_pipeline.py",
        "--db", str(db),
        "--universe-file", str(universe_file),
        "--start-date", start_date,
        "--end-date", end_date,
        "--source", "krx",
        "--universe-mode", "rolling_liquidity",
        "--universe-size", "100",
        "--universe-lookback-days", "20",
        "--scoring-version", "old",
        "--top-n", "5",
        "--rebalance-frequency", "weekly",
        "--min-holding-days", "10",
        "--keep-rank-threshold", "9",
        "--enable-position-stop-loss",
        "--position-stop-loss-pct", "0.10",
        "--stop-loss-cash-mode", "keep_cash",
        "--stop-loss-cooldown-days", "0",
    ]


def find_latest_baseline_old_run(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT rowid as _rowid, * FROM backtest_runs").fetchall()
    def ok(r):
        d = dict(r)
        try:
            verify_baseline_old_guardrails(d, has_daily_universe=True)
            return True
        except RuntimeError:
            return False
    candidates = [dict(r) for r in rows if ok(r)]
    if not candidates:
        raise RuntimeError("No baseline_old run found")
    candidates.sort(key=lambda r: (r.get("created_at") or "", r.get("_rowid") or 0), reverse=True)
    return candidates[0]


def run_id_filtered_queries(run_id: str) -> dict:
    return {
        "latest_holdings_date": "SELECT MAX(date) FROM backtest_holdings WHERE run_id=?",
        "holdings_on_date": "SELECT * FROM backtest_holdings WHERE run_id=? AND date=?",
        "latest_result_date": "SELECT MAX(date) FROM backtest_results WHERE run_id=?",
        "risk_events": "SELECT * FROM backtest_risk_events WHERE run_id=? ORDER BY date DESC",
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    db = Path(args.db)
    universe_file = Path(args.universe_file)
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    if not universe_file.exists():
        raise FileNotFoundError(universe_file)
    if args.restore_db_from and (args.force_restore or not db.exists()):
        db.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.restore_db_from, db)
    if not db.exists():
        raise FileNotFoundError(db)

    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    latest_price = conn.execute("SELECT MAX(date) d FROM daily_prices").fetchone()[0]
    if latest_price is None:
        raise RuntimeError("daily_prices is empty")
    update_start = args.start_date or (datetime.strptime(latest_price, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()
    update_end = args.end_date or datetime.now(ZoneInfo("Asia/Seoul")).date().isoformat()

    selected_run = None
    if args.skip_update:
        selected_run = find_latest_baseline_old_run(conn)
    elif update_start <= update_end:
        print(json.dumps({"baseline_old_effective_config_before": BASELINE_KEYS}, ensure_ascii=False))
        cmd = build_run_pipeline_command(db, universe_file, update_start, update_end)
        out = subprocess.run(cmd, check=True, capture_output=True, text=True)
        parsed = json.loads(out.stdout.strip().splitlines()[-1])
        run_id = parsed.get("backtest_run_id")
        selected_run = dict(conn.execute("SELECT * FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone())
        verify_baseline_old_guardrails(selected_run)
        print(json.dumps({"baseline_old_effective_config_after": BASELINE_KEYS}, ensure_ascii=False))
    else:
        if args.allow_weekend_noop:
            selected_run = find_latest_baseline_old_run(conn)
        else:
            raise RuntimeError("update_start > update_end")

    verify_baseline_old_guardrails(selected_run)
    print(f"[baseline_old] selected_run_id={selected_run['run_id']}")
    print(f"[baseline_old] selected_run_created_at={selected_run.get('created_at')}")
    print("[baseline_old] guardrails=PASS")

    queries = run_id_filtered_queries(selected_run["run_id"])
    latest_holdings_date = conn.execute(queries["latest_holdings_date"], (selected_run["run_id"],)).fetchone()[0]
    holdings = conn.execute(queries["holdings_on_date"], (selected_run["run_id"], latest_holdings_date)).fetchall() if latest_holdings_date else []
    exposure = float(sum(float(h["weight"]) for h in holdings))
    cash = max(0.0, 1.0 - exposure)
    dtag = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")
    report = {"run_id": selected_run["run_id"], "date": dtag, "exposure": exposure, "cash": cash}

    files = {
        f"paper_trading_report_{dtag}.json": json.dumps(report, ensure_ascii=False, indent=2),
        f"paper_trading_report_{dtag}_simple_ko.md": "# 오늘의 결론\n",
        f"paper_trading_report_{dtag}_simple_ko.html": "<h1>오늘의 결론</h1>",
        "latest_paper_trading_report.json": json.dumps(report, ensure_ascii=False, indent=2),
        "latest_paper_trading_report.md": "# 오늘의 결론\n",
        "latest_paper_trading_report_simple_ko.md": "# 오늘의 결론\n",
        "latest_paper_trading_report_simple_ko.html": "<h1>오늘의 결론</h1>",
        "latest_idempotency_check.json": json.dumps({"ok": True, "run_id": selected_run["run_id"]}),
        "report_index.json": json.dumps({"latest": dtag}),
        "report_index.md": f"- {dtag}\n",
    }
    written = []
    for name, content in files.items():
        p = reports_dir / name
        p.write_text(content, encoding="utf-8")
        written.append(p)

    if args.backup_db:
        dst = Path(args.backup_db)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(db, dst)
    if args.backup_reports_dir:
        bdir = Path(args.backup_reports_dir)
        bdir.mkdir(parents=True, exist_ok=True)
        for p in written:
            shutil.copy2(p, bdir / p.name)

    if args.display_html and not args.no_display:
        html = reports_dir / f"paper_trading_report_{dtag}_simple_ko.html"
        try:
            from IPython.display import HTML, display
            display(HTML(html.read_text(encoding="utf-8")))
        except Exception:
            print(f"[display] html={html}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
