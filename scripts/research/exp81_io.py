from __future__ import annotations

import csv
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from exp81_types import Cell, Config, OutputPaths, Record

FORWARD_COLS = (
    "forward_1d_return",
    "forward_3d_return",
    "forward_5d_return",
    "forward_10d_return",
    "next_day_crash_flag",
    "next_5d_crash_flag",
    "next_5d_winner_flag",
    "outcome_status",
)


def read_csv_rows(path: Path) -> list[Record]:
    with path.expanduser().open("r", encoding="utf-8-sig", newline="") as handle:
        return [
            {str(key): "" if value is None else value for key, value in row.items() if key is not None}
            for row in csv.DictReader(handle)
        ]


def write_csv_rows(path: Path, rows: list[Record], preferred_fields: list[str]) -> None:
    fields = fieldnames(rows, preferred_fields)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: stringify(row.get(field)) for field in fields})


def fieldnames(rows: list[Record], preferred_fields: list[str]) -> list[str]:
    seen = set(preferred_fields)
    fields = list(preferred_fields)
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    return fields


def output_paths(out_dir: Path) -> OutputPaths:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return OutputPaths(
        dashboard_csv=out_dir / f"exp81_candidate_shadow_dashboard_{stamp}.csv",
        dashboard_latest_csv=out_dir / "exp81_candidate_shadow_latest.csv",
        dashboard_md=out_dir / f"exp81_dashboard_{stamp}.md",
        dashboard_latest_md=out_dir / "exp81_dashboard_latest.md",
        metadata_json=out_dir / f"exp81_metadata_{stamp}.json",
        metadata_latest_json=out_dir / "exp81_metadata_latest.json",
    )


def resolve_as_of_date(cfg: Config) -> tuple[str, Record]:
    db_latest = db_latest_date(cfg.db)
    report_meta = latest_report_meta(cfg.reports_dir)
    as_of = cfg.as_of_date or stringify(report_meta.get("latest_signal_date")) or db_latest
    if as_of == "":
        as_of = db_latest
    return as_of, {"db_latest_date": db_latest, **report_meta}


def db_latest_date(db: Path) -> str:
    uri = f"file:{quote(str(db.expanduser().resolve()), safe='/')}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.execute("PRAGMA query_only=ON")
        row = conn.execute("SELECT MAX(date) FROM daily_scores").fetchone()
    return stringify(row[0])


def latest_report_meta(reports_dir: Path) -> Record:
    paths = sorted(reports_dir.expanduser().glob("*_paper_report_summary.json"))
    if not paths:
        return {"latest_report_date": "", "latest_signal_date": "", "latest_report_path": "", "report_status": "missing"}
    path = paths[-1]
    data = json.loads(path.read_text(encoding="utf-8"))
    report_date = re.sub(r"_paper_report_summary\.json$", "", path.name)
    return {
        "latest_report_date": report_date,
        "latest_signal_date": stringify(data.get("latest_signal_date") or data.get("target_date") or report_date),
        "latest_report_path": str(path),
        "report_status": "available",
    }


def load_candidate_rows(cfg: Config, as_of_date: str) -> tuple[list[Record], str]:
    rows = snapshot_candidates(cfg, as_of_date)
    if rows:
        return rows, "exp80a_pool100_snapshot"
    return rebuild_candidates_from_db(cfg, as_of_date), "db_reconstructed_candidate_proxy"


def rebuild_candidates_from_db(cfg: Config, as_of_date: str) -> list[Record]:
    uri = f"file:{quote(str(cfg.db.expanduser().resolve()), safe='/')}?mode=ro"
    with sqlite3.connect(uri, uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        run_id = selected_run_id(conn, as_of_date)
        held = current_holdings(conn, as_of_date, run_id)
        names = daily_universe_names(conn, as_of_date)
        features = daily_feature_rows(conn, as_of_date)
        rows: list[Record] = []
        candidate_rank = 0
        for score in conn.execute(score_query(), (as_of_date,)):
            symbol = stringify(score["symbol"]).zfill(6)
            if symbol in held:
                continue
            candidate_rank += 1
            if candidate_rank > cfg.candidate_n:
                break
            rows.append(candidate_row(as_of_date, symbol, candidate_rank, score, run_id, names, features))
        return rows


def selected_run_id(conn: sqlite3.Connection, as_of_date: str) -> str:
    run_sql = (
        "SELECT run_id FROM backtest_runs "
        "WHERE scoring_profile='old' AND top_n=5 AND rebalance_frequency='weekly' "
        "AND min_holding_days=10 AND keep_rank_threshold=9 "
        "AND enable_position_stop_loss=1 AND stop_loss_cash_mode='keep_cash' "
        "ORDER BY datetime(created_at) DESC LIMIT 1"
    )
    row = conn.execute(run_sql).fetchone()
    return "" if row is None else stringify(row[0])


def table_names(conn: sqlite3.Connection) -> set[str]:
    return {stringify(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def current_holdings(conn: sqlite3.Connection, as_of_date: str, run_id: str) -> set[str]:
    if not run_id or "backtest_holdings" not in table_names(conn):
        return set()
    return {
        stringify(row[0]).zfill(6)
        for row in conn.execute("SELECT symbol FROM backtest_holdings WHERE run_id=? AND date=?", (run_id, as_of_date))
    }


def daily_universe_names(conn: sqlite3.Connection, as_of_date: str) -> dict[str, str]:
    cols = set(columns(conn, "daily_universe"))
    name_col = next((col for col in ("symbol_name", "name", "company_name") if col in cols), "")
    if not name_col:
        return {}
    return {
        stringify(row["symbol"]).zfill(6): stringify(row[name_col])
        for row in conn.execute(f'SELECT symbol,"{name_col}" FROM daily_universe WHERE date=?', (as_of_date,))
    }


def daily_feature_rows(conn: sqlite3.Connection, as_of_date: str) -> dict[str, Record]:
    if "daily_features" not in table_names(conn):
        return {}
    wanted = ["symbol", *[col for col in feature_columns() if col in columns(conn, "daily_features")]]
    query = f"SELECT {','.join(wanted)} FROM daily_features WHERE date=?"
    return {stringify(row["symbol"]).zfill(6): dict(row) for row in conn.execute(query, (as_of_date,))}


def feature_columns() -> tuple[str, ...]:
    return ("ret_1d", "ret_5d", "range_pct", "volume_z20", "traded_value_z20", "high_to_close_giveback", "sma5_gap")


def score_query() -> str:
    return (
        "SELECT ds.symbol, ds.rank, ds.score "
        "FROM daily_scores ds JOIN daily_universe du ON du.date=ds.date AND du.symbol=ds.symbol "
        "WHERE ds.date=? ORDER BY ds.rank, ds.symbol"
    )


def candidate_row(date: str, symbol: str, rank: int, score: sqlite3.Row, run_id: str, names: dict[str, str], features: dict[str, Record]) -> Record:
    row: Record = {
        "date": date,
        "symbol": symbol,
        "symbol_name": names.get(symbol, ""),
        "candidate_rank": rank,
        "original_rank": int_value(score["rank"]),
        "score": float_value(score["score"]),
        "is_current_holding": 0,
        "selected_run_id": run_id,
        **features.get(symbol, {}),
    }
    row.update(hot_flags(row))
    return row


def hot_flags(row: Record) -> Record:
    flags = {
        "hot_ret_1d_flag": (float_value(row.get("ret_1d")) or -999.0) >= 0.08,
        "hot_ret_5d_flag": (float_value(row.get("ret_5d")) or -999.0) >= 0.15,
        "hot_range_flag": (float_value(row.get("range_pct")) or -999.0) >= 0.10,
        "hot_volume_flag": (float_value(row.get("volume_z20")) or -999.0) >= 3.0 or (float_value(row.get("traded_value_z20")) or -999.0) >= 3.0,
        "hot_giveback_flag": (float_value(row.get("high_to_close_giveback")) or 999.0) <= -0.05,
        "overextended_sma5_flag": (float_value(row.get("sma5_gap")) or -999.0) >= 0.12,
    }
    out: Record = {name: int(value) for name, value in flags.items()}
    out["headline_proxy_score"] = sum(int(value) for value in flags.values())
    return out


def snapshot_candidates(cfg: Config, as_of_date: str) -> list[Record]:
    forward = {row_key(row): row for row in read_csv_rows(cfg.exp80a_forward)}
    rows: list[Record] = []
    for row in read_csv_rows(cfg.exp80a_snapshot):
        if stringify(row.get("date")) != as_of_date:
            continue
        if stringify(row.get("analysis_included")) not in {"", "1"}:
            continue
        merged = dict(row)
        fwd = forward.get(row_key(row))
        if fwd is not None:
            for col in FORWARD_COLS:
                if col in fwd:
                    merged[col] = fwd[col]
        rows.append(merged)
    return sorted(rows, key=lambda row: int_value(row.get("candidate_rank")))[: cfg.candidate_n]


def load_exp80d_best_index(path: Path) -> dict[tuple[str, str], Record]:
    best: dict[tuple[str, str], Record] = {}
    for row in read_csv_rows(path):
        if row.get("timing_rule") != "range_cooldown_then_reclaim":
            continue
        key = row_key(row)
        current = best.get(key)
        if current is None or int_value(row.get("top_k")) > int_value(current.get("top_k")):
            best[key] = row
    return best


def load_exp80c_action_index(path: Path) -> dict[tuple[str, str], Record]:
    best: dict[tuple[str, str], Record] = {}
    for row in read_csv_rows(path):
        if row.get("rule") != "hybrid_delay_candidate_flag":
            continue
        key = row_key(row)
        current = best.get(key)
        if current is None or int_value(row.get("top_k")) > int_value(current.get("top_k")):
            best[key] = row
    return best


def ops_meta_for_date(path: Path, as_of_date: str) -> Record:
    if not path.expanduser().exists():
        return {"ops_ledger_status": "unavailable", "ops_warning": "ops ledger missing"}
    for row in read_csv_rows(path):
        dates = {stringify(row.get("expected_trade_date")), stringify(row.get("run_date")), stringify(row.get("paper_report_date"))}
        if as_of_date not in dates:
            continue
        stale = "STALE" in stringify(row.get("stale_check")).upper()
        invalid = stringify(row.get("invalid_fallback_flag")).lower() == "true"
        status = stringify(row.get("status") or "unknown")
        warning = ops_warning(status, stale, invalid)
        return {"ops_ledger_status": status, "ops_warning": warning, "ops_row": json.dumps(row, ensure_ascii=False)}
    return {"ops_ledger_status": "missing_date", "ops_warning": f"no ops ledger row for {as_of_date}"}


def ops_warning(status: str, stale: bool, invalid: bool) -> str:
    warnings: list[str] = []
    if status.lower() not in {"ok", "success", "completed", "done", ""}:
        warnings.append(f"status={status}")
    if stale:
        warnings.append("stale warning")
    if invalid:
        warnings.append("invalid fallback quarantined")
    return "; ".join(warnings) if warnings else "none"


def row_key(row: Record) -> tuple[str, str]:
    return stringify(row.get("date")), stringify(row.get("symbol")).zfill(6)


def int_value(value: Cell) -> int:
    text = stringify(value).strip()
    if text == "":
        return 999999
    try:
        return int(float(text))
    except ValueError:
        return 999999


def float_value(value: Cell) -> float | None:
    text = stringify(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def stringify(value: Cell) -> str:
    return "" if value is None else str(value)
