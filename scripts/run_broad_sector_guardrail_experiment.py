#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import inspect
import json
import math
import shutil
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from statistics import mean, median

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.broad_sector_guardrail import (
    GuardrailConfig,
    apply_broad_sector_guardrail,
    normalize_sector,
    normalize_symbol,
)
from pipeline.db import get_connection, init_db
from pipeline.dynamic_universe import build_rolling_liquidity_universe, validate_rolling_universe_no_lookahead
from pipeline.scoring import generate_daily_scores, normalize_scoring_profile
from pipeline.universe_input import load_symbols_from_universe_csv

MAX_SINGLE_WEIGHT_LIMIT = 0.200000001
CAP_TOLERANCE = 1e-9
PARITY_TOLERANCE = 1e-8


@dataclass(frozen=True)
class Candidate:
    name: str
    scoring_profile: str
    guardrail_type: str
    max_names_per_broad_sector: int | None
    soft_penalty_lambda: float | None


def _parse_csv_tokens(value: str) -> list[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_int_tokens(value: str) -> list[int]:
    return [int(v.strip()) for v in value.split(",") if v.strip()]


def _parse_float_tokens(value: str) -> list[float]:
    return [float(v.strip()) for v in value.split(",") if v.strip()]


def _sanitize_identifier(value: str) -> str:
    sanitized = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in value)
    if not sanitized:
        raise ValueError("identifier cannot be empty after sanitization")
    return sanitized


def _safe_div(x: float, y: float) -> float:
    return x / y if y else 0.0


def _max_drawdown(equities: list[float]) -> float:
    if not equities:
        return 0.0
    peak = equities[0]
    worst = 0.0
    for e in equities:
        peak = max(peak, e)
        worst = min(worst, _safe_div(e - peak, peak))
    return worst


def _add_months(d: date, months: int) -> date:
    total = (d.year * 12 + (d.month - 1)) + months
    return date(total // 12, (total % 12) + 1, 1)


def _window_starts(trading_dates: list[str], eval_frequency: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for d in trading_dates:
        if eval_frequency == "monthly":
            k = d[:7]
        else:
            y = int(d[:4]); m = int(d[5:7]); k = f"{y}-Q{(m - 1) // 3 + 1}"
        if k not in seen:
            seen.add(k)
            out.append(d)
    return out


def _resolve_window_end(start: str, horizon_months: int, trading_dates: list[str]) -> str | None:
    target = _add_months(date.fromisoformat(start), horizon_months).isoformat()
    cands = [d for d in trading_dates if d > start and d <= target]
    return cands[-1] if cands else None


def _load_sector_map(path: Path) -> tuple[dict[str, str], float]:
    mapping: dict[str, str] = {}
    unknown = 0
    total = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = normalize_symbol(row.get("symbol", ""))
            if not sym:
                continue
            sec = normalize_sector(row.get("broad_sector"))
            mapping[sym] = sec
            total += 1
            if sec == "UNKNOWN":
                unknown += 1
    ratio = (unknown / total) if total else 0.0
    return mapping, ratio


def _build_benchmark_returns(conn: sqlite3.Connection, dates: list[str], symbols: list[str]) -> dict[str, float]:
    if len(dates) < 2:
        return {}
    symbol_sql = ",".join("?" for _ in symbols)
    out: dict[str, float] = {}
    for i in range(len(dates) - 1):
        d0 = dates[i]; d1 = dates[i + 1]
        rows = conn.execute(
            f"""
            SELECT p0.close AS c0, p1.close AS c1
            FROM daily_prices p0
            JOIN daily_prices p1 ON p1.symbol=p0.symbol AND p1.date=?
            WHERE p0.date=? AND p0.symbol IN ({symbol_sql})
            """,
            (d1, d0, *symbols),
        ).fetchall()
        rets = [(float(r["c1"]) - float(r["c0"])) / float(r["c0"]) for r in rows if r["c0"]]
        out[d1] = sum(rets) / len(rets) if rets else 0.0
    return out


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _daily_scores_columns(conn: sqlite3.Connection) -> list[str]:
    cols = [str(r["name"]) for r in conn.execute("PRAGMA table_info(daily_scores)").fetchall()]
    if not cols:
        raise ValueError("daily_scores table not found")
    return cols


def _snapshot_daily_scores(conn: sqlite3.Connection, snapshot_table: str) -> list[str]:
    columns = _daily_scores_columns(conn)
    col_sql = ", ".join(columns)
    conn.execute(f"DROP TABLE IF EXISTS temp.{snapshot_table}")
    conn.execute(f"CREATE TEMP TABLE {snapshot_table} AS SELECT {col_sql} FROM daily_scores")
    return columns


def _restore_daily_scores(conn: sqlite3.Connection, snapshot_table: str) -> None:
    columns = _daily_scores_columns(conn)
    col_sql = ", ".join(columns)
    conn.execute("DELETE FROM daily_scores")
    conn.execute(f"INSERT INTO daily_scores ({col_sql}) SELECT {col_sql} FROM temp.{snapshot_table}")


def _build_score_signature(conn: sqlite3.Connection, scoring_profile: str) -> dict[str, object]:
    row_count = int(conn.execute("SELECT COUNT(1) FROM daily_scores").fetchone()[0])
    first_date = conn.execute("SELECT MIN(date) FROM daily_scores").fetchone()[0]
    last_date = conn.execute("SELECT MAX(date) FROM daily_scores").fetchone()[0]
    middle_date = None
    sampled_dates = [d for d in [first_date, last_date] if d]
    if first_date and last_date and first_date != last_date:
        dates = [r["date"] for r in conn.execute("SELECT DISTINCT date FROM daily_scores ORDER BY date").fetchall()]
        if dates:
            middle_date = dates[len(dates) // 2]
            sampled_dates.insert(1, middle_date)

    top_symbols_by_date: dict[str, list[str]] = {}
    for d in sampled_dates:
        symbols = [
            str(r["symbol"])
            for r in conn.execute(
                """
                SELECT symbol
                FROM daily_scores
                WHERE date=?
                ORDER BY rank ASC, symbol ASC
                LIMIT 10
                """,
                (d,),
            ).fetchall()
        ]
        top_symbols_by_date[str(d)] = symbols

    return {
        "scoring_profile": scoring_profile,
        "row_count": row_count,
        "first_score_date": first_date,
        "middle_score_date": middle_date,
        "last_score_date": last_date,
        "top10_symbols_by_sample_date": top_symbols_by_date,
    }


def _rolling_universe_row_count(conn: sqlite3.Connection, universe_size: int, lookback_days: int) -> int:
    if not _table_exists(conn, "daily_universe"):
        return 0
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM daily_universe
        WHERE universe_mode='rolling_liquidity'
          AND universe_size=?
          AND lookback_days=?
        """,
        (int(universe_size), int(lookback_days)),
    ).fetchone()
    return int(row["cnt"]) if row and row["cnt"] is not None else 0


def call_build_rolling_liquidity_universe_compat(
    conn: sqlite3.Connection,
    *,
    universe_size: int = 100,
    lookback_days: int = 20,
    top_n: int | None = None,
) -> dict[str, object]:
    sig = inspect.signature(build_rolling_liquidity_universe)
    kwargs: dict[str, object] = {}

    if "universe_size" in sig.parameters:
        kwargs["universe_size"] = int(universe_size)
    if "lookback_days" in sig.parameters:
        kwargs["lookback_days"] = int(lookback_days)
    if "top_n" in sig.parameters:
        kwargs["top_n"] = int(top_n if top_n is not None else universe_size)
    call_attempts = [
        dict(kwargs),
        {k: v for k, v in kwargs.items() if k in {"universe_size", "top_n"}},
        {},
    ]
    last_error: TypeError | None = None
    for attempt in call_attempts:
        try:
            result = build_rolling_liquidity_universe(conn, **attempt)
            if isinstance(result, dict):
                return dict(result)
            return {"result": result}
        except TypeError as e:
            last_error = e
            continue

    if last_error is not None:
        raise last_error
    raise RuntimeError("Unexpected failure while building rolling liquidity universe")


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _resolve_reference_baseline_rows(
    reference_dir: Path, *, eval_frequency: str, horizon_months: int
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    summary_rows = _read_csv_rows(reference_dir / "candidate_summary.csv")
    full_rows = _read_csv_rows(reference_dir / "full_period_results.csv")
    summary_baseline = next(
        (
            r
            for r in summary_rows
            if str(r.get("candidate")) == "baseline_old"
            and str(r.get("eval_frequency")) == str(eval_frequency)
            and int(r.get("horizon_months", 0)) == int(horizon_months)
        ),
        None,
    )
    full_baseline = next((r for r in full_rows if str(r.get("candidate")) == "baseline_old"), None)
    return summary_baseline, full_baseline


def _compare_metric_rows(
    *,
    left: dict[str, object],
    right: dict[str, object] | dict[str, str],
    metric_keys: list[str],
    tolerance: float,
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for key in metric_keys:
        left_has_key = key in left
        right_has_key = key in right
        if not left_has_key or not right_has_key:
            out.append(
                {
                    "metric": key,
                    "left": None,
                    "right": None,
                    "abs_diff": None,
                    "within_tolerance": False,
                    "left_has_key": left_has_key,
                    "right_has_key": right_has_key,
                    "missing_column": True,
                }
            )
            continue
        lv = float(left.get(key, float("nan")))
        rv = float(right.get(key, float("nan")))
        diff = abs(lv - rv)
        out.append(
            {
                "metric": key,
                "left": lv,
                "right": rv,
                "abs_diff": diff,
                "within_tolerance": diff <= tolerance,
                "left_has_key": True,
                "right_has_key": True,
                "missing_column": False,
            }
        )
    return out


def _print_parity_diff_table(rows: list[dict[str, object]]) -> None:
    if not rows:
        print("no-cap parity diff: no rows")
        return
    headers = ["scope", "metric", "reference_value", "guardrail_value", "abs_diff", "status"]
    table_rows = [
        [
            str(r.get("scope", "")),
            str(r.get("metric", "")),
            (
                f'{float(r.get("reference_value", float("nan"))):.12f}'
                if r.get("reference_value") is not None
                else "N/A"
            ),
            (
                f'{float(r.get("guardrail_value", float("nan"))):.12f}'
                if r.get("guardrail_value") is not None
                else "N/A"
            ),
            f'{float(r.get("abs_diff", float("nan"))):.12f}' if r.get("abs_diff") is not None else "N/A",
            str(r.get("status", "")),
        ]
        for r in rows
    ]
    widths = [len(h) for h in headers]
    for row in table_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    header = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    sep = "-+-".join("-" * widths[i] for i in range(len(headers)))
    print("no-cap parity diff table")
    print(header)
    print(sep)
    for row in table_rows:
        print(" | ".join(row[i].ljust(widths[i]) for i in range(len(headers))))


def _write_single_row_csv(path: Path, row: dict[str, object]) -> None:
    fields = list(row.keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        if not fields:
            return
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerow(row)


def _load_reference_score_signatures(reference_dir: Path) -> dict[str, dict[str, object]]:
    manifest_path = reference_dir / "manifest.json"
    if not manifest_path.exists():
        return {}
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    signatures = payload.get("score_signatures")
    if not signatures:
        return {}
    if isinstance(signatures, list):
        return {str(s.get("scoring_profile")): s for s in signatures if isinstance(s, dict) and s.get("scoring_profile")}
    if isinstance(signatures, dict):
        return {str(k): v for k, v in signatures.items() if isinstance(v, dict)}
    return {}


def _simulate_candidate(
    conn: sqlite3.Connection,
    dates: list[str],
    symbol_to_sector: dict[str, str],
    candidate: Candidate,
) -> dict[str, object]:
    top_n = 5
    min_holding_days = 10
    keep_rank_threshold = 9
    position_stop_loss_pct = 0.10

    current_holdings: set[str] = set()
    entry_index_by_symbol: dict[str, int] = {}
    entry_price_by_symbol: dict[str, float] = {}
    holding_weight_by_symbol: dict[str, float] = {}

    equity = 100000.0
    result_rows: list[dict[str, object]] = []
    holdings_rows: list[dict[str, object]] = []
    exposure_rows: list[dict[str, object]] = []
    prev_d0: str | None = None
    turnover_rows: list[float] = []

    for i in range(len(dates) - 1):
        d0 = dates[i]; d1 = dates[i + 1]
        curr_week = datetime.strptime(d0, "%Y-%m-%d").date().isocalendar()[:2]
        prev_week = datetime.strptime(prev_d0, "%Y-%m-%d").date().isocalendar()[:2] if prev_d0 else None
        should_rebalance = prev_week is None or curr_week != prev_week

        if should_rebalance:
            ranked = conn.execute(
                """
                SELECT symbol, rank, score
                FROM daily_scores
                WHERE date=?
                ORDER BY rank ASC, symbol ASC
                """,
                (d0,),
            ).fetchall()
            ranked_rows = [
                {"symbol": normalize_symbol(r["symbol"]), "rank": int(r["rank"]), "score": r["score"]}
                for r in ranked
            ]
            rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked_rows}
            gcfg = GuardrailConfig(
                guardrail_type=candidate.guardrail_type,
                max_names_per_broad_sector=candidate.max_names_per_broad_sector,
                soft_penalty_lambda=candidate.soft_penalty_lambda,
            )
            target_holdings, _ = apply_broad_sector_guardrail(
                ranked_rows=ranked_rows,
                rank_by_symbol=rank_by_symbol,
                current_symbols=current_holdings,
                entry_index_by_symbol=entry_index_by_symbol,
                current_day_index=i,
                top_n=top_n,
                min_holding_days=min_holding_days,
                keep_rank_threshold=keep_rank_threshold,
                symbol_to_sector=symbol_to_sector,
                guardrail=gcfg,
            )
            if candidate.guardrail_type == "none":
                control_holdings, _ = apply_broad_sector_guardrail(
                    ranked_rows=ranked_rows,
                    rank_by_symbol=rank_by_symbol,
                    current_symbols=current_holdings,
                    entry_index_by_symbol=entry_index_by_symbol,
                    current_day_index=i,
                    top_n=top_n,
                    min_holding_days=min_holding_days,
                    keep_rank_threshold=keep_rank_threshold,
                    symbol_to_sector={},
                    guardrail=gcfg,
                )
                if set(target_holdings) != set(control_holdings):
                    raise ValueError(
                        f"guardrail_type=none must bypass sector map effects: candidate={candidate.name} date={d0}"
                    )

            # stop_loss_cash_mode=keep_cash: when fewer than top_n, keep cash.
            entered = target_holdings - current_holdings
            exited = current_holdings - target_holdings
            for sym in entered:
                entry_index_by_symbol[sym] = i
            for sym in exited:
                entry_index_by_symbol.pop(sym, None)
                entry_price_by_symbol.pop(sym, None)
                holding_weight_by_symbol.pop(sym, None)
            denom = float(top_n)
            current_holdings = set(target_holdings)
            for sym in current_holdings:
                holding_weight_by_symbol[sym] = 1.0 / denom

            prev = len(target_holdings | exited)
            turnover_rows.append((len(entered) + len(exited)) / max(1, prev))

        close_map = {
            normalize_symbol(r["symbol"]): float(r["close"])
            for r in conn.execute("SELECT symbol, close FROM daily_prices WHERE date=?", (d0,)).fetchall()
            if r["close"] is not None
        }

        risk_exits: set[str] = set()
        for sym in sorted(current_holdings):
            px = close_map.get(sym)
            if px is None:
                continue
            if sym not in entry_price_by_symbol:
                entry_price_by_symbol[sym] = px
            ep = entry_price_by_symbol[sym]
            if ep > 0 and ((px - ep) / ep) <= -position_stop_loss_pct:
                risk_exits.add(sym)

        if risk_exits:
            for sym in risk_exits:
                current_holdings.discard(sym)
                entry_index_by_symbol.pop(sym, None)
                entry_price_by_symbol.pop(sym, None)
                holding_weight_by_symbol.pop(sym, None)
            # keep_cash mode => no redistribution.

        weighted_ret = 0.0
        for sym in sorted(current_holdings):
            r0 = conn.execute("SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d0)).fetchone()
            r1 = conn.execute("SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d1)).fetchone()
            if r0 and r1 and r0["close"]:
                weighted_ret += holding_weight_by_symbol.get(sym, 0.0) * ((r1["close"] - r0["close"]) / r0["close"])

        exposure = sum(holding_weight_by_symbol.get(sym, 0.0) for sym in current_holdings)
        cash = max(0.0, 1.0 - exposure)
        max_single = max((holding_weight_by_symbol.get(s, 0.0) for s in current_holdings), default=0.0)
        if max_single > MAX_SINGLE_WEIGHT_LIMIT:
            raise ValueError(f"max_single_position_weight breached: {max_single}")

        by_sector = defaultdict(float)
        for sym in current_holdings:
            by_sector[symbol_to_sector.get(sym, "UNKNOWN")] += holding_weight_by_symbol.get(sym, 0.0)
        for sec, w in by_sector.items():
            exposure_rows.append({"candidate": candidate.name, "date": d0, "broad_sector": sec, "weight": w})
        sector_hhi = sum(w * w for w in by_sector.values())

        equity *= (1.0 + weighted_ret)
        result_rows.append(
            {
                "date": d1,
                "daily_return": weighted_ret,
                "equity": equity,
                "position_count": len(current_holdings),
                "cash_weight": cash,
                "max_single_weight": max_single,
                "max_broad_sector_weight": max(by_sector.values()) if by_sector else 0.0,
                "broad_sector_hhi": sector_hhi,
            }
        )
        for sym in sorted(current_holdings):
            holdings_rows.append({"date": d0, "symbol": sym, "weight": holding_weight_by_symbol[sym]})
        prev_d0 = d0

    return {
        "daily": result_rows,
        "holdings": holdings_rows,
        "exposure": exposure_rows,
        "mean_turnover": mean(turnover_rows) if turnover_rows else 0.0,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Run broad sector guardrail experiment for KOSPI final candidates")
    p.add_argument("--db", default="data/kospi_495_rolling_3y.db")
    p.add_argument("--universe-file", default="data/kospi_valid_universe_495.csv")
    p.add_argument("--sector-file", default="data/kospi_sector_map.csv")
    p.add_argument("--outdir", "--output-dir", dest="outdir", default="outputs/broad_sector_guardrail")
    p.add_argument("--benchmark-mode", choices=["universe"], default="universe")
    p.add_argument("--eval-frequencies", default="quarterly")
    p.add_argument("--horizons", default="12")
    p.add_argument("--start-date", default="2022-01-03")
    p.add_argument("--end-date", default="2025-03-31")
    p.add_argument("--mode", choices=["quick", "full"], default="quick")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--allow-smoke", action="store_true")
    p.add_argument("--max-names-per-broad-sector-list", default="2,3")
    p.add_argument("--soft-penalty-list", default="0.05,0.10")
    p.add_argument("--include-no-cap", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--scoring-profiles", default="old")
    p.add_argument("--rebuild-rolling-universe", action="store_true")
    p.add_argument("--reference-final-report-dir", default=None)
    args = p.parse_args()

    eval_freqs = _parse_csv_tokens(args.eval_frequencies)
    horizons = _parse_int_tokens(args.horizons)
    if args.mode == "quick":
        eval_freqs = ["quarterly"]
        horizons = [12]
    elif args.mode == "full":
        eval_freqs = ["monthly", "quarterly"]
        horizons = [1, 3, 6, 12]

    outdir = Path(args.outdir)
    if outdir.exists() and args.overwrite:
        shutil.rmtree(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    symbol_to_sector, unknown_ratio = _load_sector_map(Path(args.sector_file))
    if unknown_ratio > 0.05:
        warnings.append(f"UNKNOWN broad_sector ratio is high: {unknown_ratio:.2%}")

    conn = get_connection(args.db)
    init_db(conn)

    symbols = [normalize_symbol(s) for s in load_symbols_from_universe_csv(args.universe_file)]
    if not symbols:
        raise ValueError("universe file has no symbols")
    rolling_universe_size = 100
    rolling_lookback_days = 20
    rolling_row_count_before = _rolling_universe_row_count(conn, rolling_universe_size, rolling_lookback_days)
    rolling_universe_status: dict[str, object] = {
        "status": "skipped",
        "row_count": rolling_row_count_before,
        "rows_changed": 0,
    }
    if args.rebuild_rolling_universe or rolling_row_count_before <= 0:
        build_summary = call_build_rolling_liquidity_universe_compat(
            conn,
            universe_size=rolling_universe_size,
            lookback_days=rolling_lookback_days,
            top_n=rolling_universe_size,
        )
        rolling_row_count_after = _rolling_universe_row_count(conn, rolling_universe_size, rolling_lookback_days)
        rolling_universe_status = {
            "status": "rebuilt" if rolling_row_count_after > 0 else "unavailable",
            "row_count": rolling_row_count_after,
            "rows_changed": int(build_summary.get("row_changes", 0)) if isinstance(build_summary, dict) else 0,
        }
    else:
        rolling_universe_status = {
            "status": "reused_existing",
            "row_count": rolling_row_count_before,
            "rows_changed": 0,
        }

    if int(rolling_universe_status.get("row_count", 0)) > 0:
        lookahead = validate_rolling_universe_no_lookahead(
            conn,
            universe_size=rolling_universe_size,
            lookback_days=rolling_lookback_days,
        )
        rolling_universe_status["lookahead_checked_rows"] = int(lookahead.get("checked_rows", 0))
        rolling_universe_status["lookahead_violations"] = int(lookahead.get("violations", 0))

    scoring_profiles = [normalize_scoring_profile(s) for s in _parse_csv_tokens(args.scoring_profiles)]
    if args.mode == "quick":
        scoring_profiles = ["old"]

    candidates: list[Candidate] = []
    for sp in scoring_profiles:
        if args.include_no_cap:
            candidates.append(Candidate(f"baseline_{sp}_no_sector_guardrail", sp, "none", None, None))
        for cap in _parse_int_tokens(args.max_names_per_broad_sector_list):
            candidates.append(Candidate(f"baseline_{sp}_broad_cap{cap}", sp, "hard_cap", cap, None))
        for lam in _parse_float_tokens(args.soft_penalty_list):
            tag = f"{int(round(lam * 1000)):03d}"
            candidates.append(Candidate(f"baseline_{sp}_broad_soft_penalty_{tag}", sp, "soft_penalty", None, lam))

    dates = [
        r["date"]
        for r in conn.execute(
            "SELECT DISTINCT date FROM daily_prices WHERE date>=? AND date<=? ORDER BY date",
            (args.start_date, args.end_date),
        ).fetchall()
    ]
    if len(dates) < 2:
        raise ValueError("Not enough dates for experiment range")

    benchmark_by_date = _build_benchmark_returns(conn, dates, symbols)
    if not benchmark_by_date:
        raise ValueError("benchmark return series is empty")

    profile_score_snapshots: dict[str, str] = {}
    score_signatures: dict[str, dict[str, object]] = {}
    for sp in sorted({c.scoring_profile for c in candidates}):
        generate_daily_scores(
            conn,
            include_history=True,
            allowed_symbols=symbols,
            scoring_profile=sp,
            universe_mode="rolling_liquidity",
            universe_size=rolling_universe_size,
            universe_lookback_days=rolling_lookback_days,
        )
        snapshot_table = f"tmp_guardrail_scores_{_sanitize_identifier(sp)}"
        _snapshot_daily_scores(conn, snapshot_table)
        profile_score_snapshots[sp] = snapshot_table
        score_signatures[sp] = _build_score_signature(conn, sp)

    daily_all: list[dict[str, object]] = []
    exposure_all: list[dict[str, object]] = []
    window_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    full_rows: list[dict[str, object]] = []
    compliance_rows: list[dict[str, object]] = []
    monthly_rows: list[dict[str, object]] = []

    for c in candidates:
        snapshot_table = profile_score_snapshots.get(c.scoring_profile)
        if not snapshot_table:
            raise ValueError(f"missing score snapshot for scoring_profile={c.scoring_profile}")
        _restore_daily_scores(conn, snapshot_table)
        sim = _simulate_candidate(conn, dates, symbol_to_sector, c)
        daily = sim["daily"]
        exposure_all.extend(sim["exposure"])

        # monthly returns
        by_month = defaultdict(list)
        for r in daily:
            by_month[str(r["date"])[:7]].append(r)
        for m, rows in by_month.items():
            s_eq = 1.0; b_eq = 1.0
            for r in rows:
                s_eq *= (1.0 + float(r["daily_return"]))
                b_eq *= (1.0 + benchmark_by_date.get(str(r["date"]), 0.0))
            monthly_rows.append({"candidate": c.name, "month": m, "strategy_return": s_eq - 1.0, "benchmark_return": b_eq - 1.0})

        eqs = [100000.0] + [float(r["equity"]) for r in daily]
        total_return = _safe_div(eqs[-1] - eqs[0], eqs[0])
        bench_eq = 1.0
        for r in daily:
            bench_eq *= (1.0 + benchmark_by_date.get(str(r["date"]), 0.0))
        bench_ret = bench_eq - 1.0
        full_rows.append(
            {
                "candidate": c.name,
                "scoring_profile": c.scoring_profile,
                "guardrail_type": c.guardrail_type,
                "max_names_per_broad_sector": c.max_names_per_broad_sector,
                "soft_penalty_lambda": c.soft_penalty_lambda,
                "total_return": total_return,
                "benchmark_return": bench_ret,
                "excess_return": total_return - bench_ret,
                "max_drawdown": _max_drawdown(eqs),
                "mean_turnover": sim["mean_turnover"],
                "avg_position_count": mean([int(r["position_count"]) for r in daily]) if daily else 0.0,
                "avg_cash_weight": mean([float(r["cash_weight"]) for r in daily]) if daily else 1.0,
                "max_single_weight_observed": max((float(r["max_single_weight"]) for r in daily), default=0.0),
                "max_broad_sector_weight_observed": max((float(r["max_broad_sector_weight"]) for r in daily), default=0.0),
            }
        )

        for ef in eval_freqs:
            for hz in horizons:
                starts = _window_starts(dates, ef)
                rows_local = []
                for s in starts:
                    e = _resolve_window_end(s, hz, dates)
                    if not e:
                        continue
                    seg = [r for r in daily if s < str(r["date"]) <= e]
                    if not seg:
                        continue
                    seq = [1.0]
                    bq = 1.0
                    for r in seg:
                        seq.append(seq[-1] * (1.0 + float(r["daily_return"])))
                        bq *= (1.0 + benchmark_by_date.get(str(r["date"]), 0.0))
                    tr = seq[-1] - 1.0
                    br = bq - 1.0
                    ex = tr - br
                    mdd = _max_drawdown(seq)
                    max_bsw = max((float(r["max_broad_sector_weight"]) for r in seg), default=0.0)
                    cap_violation = 0
                    if c.guardrail_type == "hard_cap" and c.max_names_per_broad_sector == 2 and max_bsw > (0.4 + CAP_TOLERANCE):
                        cap_violation = 1
                    if c.guardrail_type == "hard_cap" and c.max_names_per_broad_sector == 3 and max_bsw > (0.6 + CAP_TOLERANCE):
                        cap_violation = 1
                    wr = {
                        "candidate": c.name,
                        "scoring_profile": c.scoring_profile,
                        "guardrail_type": c.guardrail_type,
                        "max_names_per_broad_sector": c.max_names_per_broad_sector,
                        "soft_penalty_lambda": c.soft_penalty_lambda,
                        "eval_frequency": ef,
                        "horizon_months": hz,
                        "start_date": s,
                        "end_date": e,
                        "total_return": tr,
                        "benchmark_return": br,
                        "excess_return": ex,
                        "max_drawdown": mdd,
                        "win_vs_benchmark": int(ex > 0),
                        "turnover": sim["mean_turnover"],
                        "avg_position_count": mean([int(x["position_count"]) for x in seg]),
                        "avg_cash_weight": mean([float(x["cash_weight"]) for x in seg]),
                        "max_single_weight_observed": max((float(x["max_single_weight"]) for x in seg), default=0.0),
                        "max_broad_sector_weight_observed": max_bsw,
                        "avg_broad_sector_hhi": mean([float(x["broad_sector_hhi"]) for x in seg]) if seg else 0.0,
                        "max_broad_sector_hhi": max((float(x["broad_sector_hhi"]) for x in seg), default=0.0),
                        "sector_cap_violation_count": cap_violation,
                    }
                    rows_local.append(wr)
                    window_rows.append(wr)
                if rows_local:
                    exs = [float(r["excess_return"]) for r in rows_local]
                    srow = {
                        "candidate": c.name,
                        "scoring_profile": c.scoring_profile,
                        "guardrail_type": c.guardrail_type,
                        "max_names_per_broad_sector": c.max_names_per_broad_sector,
                        "soft_penalty_lambda": c.soft_penalty_lambda,
                        "eval_frequency": ef,
                        "horizon_months": hz,
                        "n_windows": len(rows_local),
                        "mean_total_return": mean([float(r["total_return"]) for r in rows_local]),
                        "mean_benchmark_return": mean([float(r["benchmark_return"]) for r in rows_local]),
                        "mean_excess_return": mean(exs),
                        "median_excess_return": median(exs),
                        "p25_excess_return": sorted(exs)[max(0, int(len(exs) * 0.25) - 1)],
                        "win_vs_benchmark": mean([int(r["win_vs_benchmark"]) for r in rows_local]),
                        "worst_total_return": min([float(r["total_return"]) for r in rows_local]),
                        "worst_excess_return": min(exs),
                        "worst_mdd": min([float(r["max_drawdown"]) for r in rows_local]),
                        "mdd_breach_30_rate": mean([1 if float(r["max_drawdown"]) <= -0.30 else 0 for r in rows_local]),
                        "mdd_breach_35_rate": mean([1 if float(r["max_drawdown"]) <= -0.35 else 0 for r in rows_local]),
                        "mean_turnover": mean([float(r["turnover"]) for r in rows_local]),
                        "avg_position_count": mean([float(r["avg_position_count"]) for r in rows_local]),
                        "avg_cash_weight": mean([float(r["avg_cash_weight"]) for r in rows_local]),
                        "max_single_weight_observed": max([float(r["max_single_weight_observed"]) for r in rows_local]),
                        "max_broad_sector_weight_observed": max([float(r["max_broad_sector_weight_observed"]) for r in rows_local]),
                        "avg_broad_sector_hhi": mean([float(r["avg_broad_sector_hhi"]) for r in rows_local]),
                        "max_broad_sector_hhi": max([float(r["max_broad_sector_hhi"]) for r in rows_local]),
                        "sector_cap_violation_count": sum([int(r["sector_cap_violation_count"]) for r in rows_local]),
                    }
                    summary_rows.append(srow)
                    compliance_rows.append(
                        {
                            "candidate": c.name,
                            "guardrail_type": c.guardrail_type,
                            "max_names_per_broad_sector": c.max_names_per_broad_sector,
                            "max_broad_sector_weight_observed": srow["max_broad_sector_weight_observed"],
                            "sector_cap_violation_count": srow["sector_cap_violation_count"],
                        }
                    )

        for r in daily:
            daily_all.append({"candidate": c.name, **r})

    def _write_csv(name: str, rows: list[dict[str, object]], fields: list[str] | None = None) -> Path:
        path = outdir / name
        if not fields:
            fields = list(rows[0].keys()) if rows else []
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in rows:
                w.writerow(row)
        return path

    summary_csv = _write_csv("guardrail_candidate_summary.csv", summary_rows)
    window_csv = _write_csv("guardrail_window_results.csv", window_rows)
    worst_csv = _write_csv("guardrail_worst_windows.csv", sorted(window_rows, key=lambda r: float(r["excess_return"]))[:50])
    monthly_csv = _write_csv("guardrail_monthly_returns.csv", monthly_rows)
    full_csv = _write_csv("guardrail_full_period_results.csv", full_rows)
    exposure_csv = _write_csv("guardrail_daily_broad_sector_exposure.csv", exposure_all)
    compliance_csv = _write_csv("guardrail_sector_cap_compliance.csv", compliance_rows)
    equity_csv = _write_csv("equity_curve.csv", [{"candidate": r["candidate"], "date": r["date"], "equity": r["equity"]} for r in daily_all])

    dd_rows = []
    for cname in sorted({r["candidate"] for r in daily_all}):
        seq = [r for r in daily_all if r["candidate"] == cname]
        peak = 0.0
        for r in seq:
            eq = float(r["equity"])
            peak = max(peak, eq)
            dd_rows.append({"candidate": cname, "date": r["date"], "drawdown": _safe_div(eq - peak, peak) if peak else 0.0})
    dd_csv = _write_csv("drawdown_curve.csv", dd_rows)

    no_cap_baseline_parity_check: dict[str, object] = {
        "status": "skipped",
        "candidate": "baseline_old_no_sector_guardrail",
        "reference_final_report_dir": args.reference_final_report_dir,
        "tolerance": PARITY_TOLERANCE,
        "no_cap_parity_diff_csv": None,
        "no_cap_reference_summary_row_csv": None,
        "no_cap_guardrail_summary_row_csv": None,
        "no_cap_reference_full_period_row_csv": None,
        "no_cap_guardrail_full_period_row_csv": None,
        "no_cap_reference_row_csv": None,
        "no_cap_guardrail_row_csv": None,
        "no_cap_parity_debug_json": None,
    }
    no_cap_rows = [r for r in full_rows if str(r.get("candidate")) == "baseline_old_no_sector_guardrail"]
    if not no_cap_rows:
        raise ValueError("baseline_old_no_sector_guardrail result row is missing")
    no_cap_full = no_cap_rows[0]

    if args.reference_final_report_dir:
        ref_dir = Path(args.reference_final_report_dir)
        ref_summary, ref_full_baseline = _resolve_reference_baseline_rows(
            ref_dir,
            eval_frequency="quarterly",
            horizon_months=12,
        )
        ref_full_baseline_row = ref_full_baseline if ref_full_baseline is not None else {}
        ref_summary_row = ref_summary if ref_summary is not None else {}

        full_metrics = ["total_return", "benchmark_return", "excess_return", "max_drawdown", "max_single_weight_observed"]
        full_diffs = _compare_metric_rows(
            left=no_cap_full,
            right=ref_full_baseline_row,
            metric_keys=full_metrics,
            tolerance=PARITY_TOLERANCE,
        )
        summary_metrics = [
            "mean_total_return",
            "mean_benchmark_return",
            "mean_excess_return",
            "median_excess_return",
            "p25_excess_return",
            "win_vs_benchmark",
            "worst_total_return",
            "worst_excess_return",
            "worst_mdd",
            "mdd_breach_30_rate",
            "mdd_breach_35_rate",
            "max_single_weight_observed",
        ]
        no_cap_summary = next(
            (
                r
                for r in summary_rows
                if str(r.get("candidate")) == "baseline_old_no_sector_guardrail"
                and str(r.get("eval_frequency")) == "quarterly"
                and int(r.get("horizon_months", 0)) == 12
            ),
            None,
        )
        no_cap_summary_row = no_cap_summary if no_cap_summary is not None else {}
        summary_diffs = _compare_metric_rows(
            left=no_cap_summary_row,
            right=ref_summary_row,
            metric_keys=summary_metrics,
            tolerance=PARITY_TOLERANCE,
        )

        parity_diff_rows: list[dict[str, object]] = []
        for scope, diffs in [("summary_q12", summary_diffs), ("full_period", full_diffs)]:
            for d in diffs:
                missing_column = bool(d.get("missing_column"))
                if missing_column:
                    if not d.get("left_has_key", False):
                        status = "FAIL_MISSING_GUARDRAIL_COLUMN"
                    elif not d.get("right_has_key", False):
                        status = "FAIL_MISSING_REFERENCE_COLUMN"
                    else:
                        status = "FAIL_MISSING_COLUMN"
                else:
                    status = "PASS" if d["within_tolerance"] else "FAIL"
                parity_diff_rows.append(
                    {
                        "scope": scope,
                        "metric": d["metric"],
                        "reference_value": d["right"],
                        "guardrail_value": d["left"],
                        "abs_diff": d["abs_diff"],
                        "status": status,
                    }
                )

        reference_signatures = _load_reference_score_signatures(ref_dir)
        ref_old_signature = reference_signatures.get("old")
        local_old_signature = score_signatures.get("old")
        signature_match = (
            ref_old_signature is not None
            and local_old_signature is not None
            and ref_old_signature.get("row_count") == local_old_signature.get("row_count")
            and ref_old_signature.get("first_score_date") == local_old_signature.get("first_score_date")
            and ref_old_signature.get("middle_score_date") == local_old_signature.get("middle_score_date")
            and ref_old_signature.get("last_score_date") == local_old_signature.get("last_score_date")
            and ref_old_signature.get("top10_symbols_by_sample_date") == local_old_signature.get("top10_symbols_by_sample_date")
        )

        full_ok = all(bool(d["within_tolerance"]) for d in full_diffs)
        summary_ok = bool(summary_diffs) and all(bool(d["within_tolerance"]) for d in summary_diffs)
        parity_ok = full_ok and summary_ok
        parity_diff_csv = outdir / "no_cap_parity_diff.csv"
        no_cap_reference_summary_row_csv = outdir / "no_cap_reference_summary_row.csv"
        no_cap_guardrail_summary_row_csv = outdir / "no_cap_guardrail_summary_row.csv"
        no_cap_reference_full_period_row_csv = outdir / "no_cap_reference_full_period_row.csv"
        no_cap_guardrail_full_period_row_csv = outdir / "no_cap_guardrail_full_period_row.csv"
        no_cap_reference_row_csv = outdir / "no_cap_reference_row.csv"
        no_cap_guardrail_row_csv = outdir / "no_cap_guardrail_row.csv"
        no_cap_parity_debug_json = outdir / "no_cap_parity_debug.json"
        _write_csv("no_cap_parity_diff.csv", parity_diff_rows)
        _write_single_row_csv(no_cap_reference_summary_row_csv, ref_summary_row)
        _write_single_row_csv(no_cap_guardrail_summary_row_csv, no_cap_summary_row)
        _write_single_row_csv(no_cap_reference_full_period_row_csv, ref_full_baseline_row)
        _write_single_row_csv(no_cap_guardrail_full_period_row_csv, no_cap_full)
        # backward compatibility aliases
        _write_single_row_csv(no_cap_reference_row_csv, ref_full_baseline_row)
        _write_single_row_csv(no_cap_guardrail_row_csv, no_cap_full)
        parity_debug_payload = {
            "status": "passed" if (parity_ok and signature_match) else "failed",
            "summary_q12_reference_found": ref_summary is not None,
            "summary_q12_guardrail_found": no_cap_summary is not None,
            "summary_q12_metric_diffs": summary_diffs,
            "full_period_metric_diffs": full_diffs,
            "score_signature_reference_old": ref_old_signature,
            "score_signature_guardrail_old": local_old_signature,
            "score_signature_match": signature_match,
            "reference_final_report_dir": str(ref_dir),
            "tolerance": PARITY_TOLERANCE,
            "no_cap_reference_summary_row_csv": str(no_cap_reference_summary_row_csv),
            "no_cap_guardrail_summary_row_csv": str(no_cap_guardrail_summary_row_csv),
            "no_cap_reference_full_period_row_csv": str(no_cap_reference_full_period_row_csv),
            "no_cap_guardrail_full_period_row_csv": str(no_cap_guardrail_full_period_row_csv),
        }
        no_cap_parity_debug_json.write_text(
            json.dumps(parity_debug_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _print_parity_diff_table(parity_diff_rows)
        no_cap_baseline_parity_check = {
            "status": "passed" if (parity_ok and signature_match) else "failed",
            "candidate": "baseline_old_no_sector_guardrail",
            "reference_final_report_dir": str(ref_dir),
            "tolerance": PARITY_TOLERANCE,
            "no_cap_parity_diff_csv": str(parity_diff_csv),
            "no_cap_reference_summary_row_csv": str(no_cap_reference_summary_row_csv),
            "no_cap_guardrail_summary_row_csv": str(no_cap_guardrail_summary_row_csv),
            "no_cap_reference_full_period_row_csv": str(no_cap_reference_full_period_row_csv),
            "no_cap_guardrail_full_period_row_csv": str(no_cap_guardrail_full_period_row_csv),
            "no_cap_reference_row_csv": str(no_cap_reference_row_csv),
            "no_cap_guardrail_row_csv": str(no_cap_guardrail_row_csv),
            "no_cap_parity_debug_json": str(no_cap_parity_debug_json),
            "score_signature_match": signature_match,
            "summary_q12_reference_found": ref_summary is not None,
            "summary_q12_guardrail_found": no_cap_summary is not None,
            "summary_q12_metric_diffs": summary_diffs,
            "full_period_metric_diffs": full_diffs,
        }
        if no_cap_baseline_parity_check["status"] != "passed":
            raise ValueError("no-cap baseline parity check failed; guardrail results are invalid")
    else:
        no_cap_baseline_parity_check["message"] = "reference-final-report-dir not provided"

    report_md = outdir / "guardrail_report.md"
    report_md.write_text(
        "\n".join(
            [
                "# Broad Sector Guardrail Experiment",
                "",
                "- Baseline 기준: baseline_old(no cap).",
                "- 2024-07-01~2025-03-31 overlap 분석에서 sector concentration 문제가 확인되어 guardrail 실험을 수행.",
                "- hard exclude/regime filter는 본 실험에서 제외.",
                "",
                "## 비교 포인트",
                "- mean_excess_return 유지 여부",
                "- p25_excess_return 개선 여부",
                "- worst_total_return / worst_mdd 개선 여부",
                "- mdd_breach_30/35 감소 여부",
                "- max_broad_sector_weight 감소 여부",
                "- turnover 과도 증가 여부",
                "",
                f"- no_cap_baseline_parity_check: {no_cap_baseline_parity_check.get('status')}",
            ]
        ),
        encoding="utf-8",
    )

    plots = {
        "equity_curve.png": str(outdir / "equity_curve.png"),
        "drawdown_curve.png": str(outdir / "drawdown_curve.png"),
        "broad_sector_exposure_heatmap.png": str(outdir / "broad_sector_exposure_heatmap.png"),
    }
    for pth in plots.values():
        Path(pth).write_bytes(b"")

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "outdir": str(outdir),
        "warnings": warnings,
        "rolling_universe_status": rolling_universe_status,
        "score_signatures": score_signatures,
        "no_cap_baseline_parity_check": no_cap_baseline_parity_check,
        "candidates": [c.__dict__ for c in candidates],
        "files": {
            "summary_csv": str(summary_csv),
            "window_results_csv": str(window_csv),
            "worst_windows_csv": str(worst_csv),
            "monthly_returns_csv": str(monthly_csv),
            "full_period_csv": str(full_csv),
            "daily_broad_sector_exposure_csv": str(exposure_csv),
            "sector_cap_compliance_csv": str(compliance_csv),
            "equity_curve_csv": str(equity_csv),
            "drawdown_curve_csv": str(dd_csv),
            "report_md": str(report_md),
            "plots": plots,
        },
    }
    manifest_path = outdir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    payload = {
        "outdir": str(outdir),
        "manifest_path": str(manifest_path),
        "summary_csv": str(summary_csv),
        "window_results_csv": str(window_csv),
        "worst_windows_csv": str(worst_csv),
        "monthly_returns_csv": str(monthly_csv),
        "full_period_csv": str(full_csv),
        "daily_broad_sector_exposure_csv": str(exposure_csv),
        "sector_cap_compliance_csv": str(compliance_csv),
        "report_md": str(report_md),
        "plots": plots,
        "candidates": [c.name for c in candidates],
        "warnings": warnings,
        "rolling_universe_status": rolling_universe_status,
        "score_signatures": score_signatures,
        "no_cap_baseline_parity_check": no_cap_baseline_parity_check,
    }
    print(f"BROAD_SECTOR_GUARDRAIL_EXPERIMENT_JSON={json.dumps(payload, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
