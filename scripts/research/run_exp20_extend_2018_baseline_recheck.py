#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.backtest import run_backtest
from pipeline.db import get_connection, init_db
from pipeline.dynamic_universe import build_rolling_liquidity_universe
from pipeline.features import generate_daily_features
from pipeline.ingest import ingest_krx_prices, normalize_krx_symbol
from pipeline.scoring import generate_daily_scores

DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

BASELINE_PARAMS: dict[str, Any] = {
    "scoring_profile": "old",
    "scoring_version": "old",
    "universe_mode": "rolling_liquidity",
    "universe_size": 100,
    "universe_lookback_days": 20,
    "top_n": 5,
    "rebalance_frequency": "weekly",
    "min_holding_days": 10,
    "keep_rank_threshold": 9,
    "enable_position_stop_loss": True,
    "position_stop_loss_pct": 0.10,
    "stop_loss_cash_mode": "keep_cash",
    "stop_loss_cooldown_days": 0,
    "enable_trailing_stop": False,
    "market_filter_enabled": False,
    "entry_gate_enabled": False,
    "enable_overheat_entry_gate": False,
    "enable_entry_quality_gate": False,
}

FROZEN_REFERENCE = {
    "reference_overlap_2022_2026_04_29": {"total_return": 3.7921630281643584, "mdd": -0.34139230404085474},
    "reference_overlap_2022_2026_03_31": {"total_return": 2.4012550830632016, "mdd": -0.34139230404085474},
    "main_backtest_2022_2025": {"total_return": 2.454511143509467, "mdd": -0.34139230404085474},
    "validation_2026_q1": {"total_return": -0.0248309466740414, "mdd": -0.306184},
    "recent_shadow_2026_04": {"total_return": 0.2950930200253394, "mdd": -0.100226},
    "stress_2024_07_09": {"total_return": -0.2432450468509677, "mdd": -0.251041},
}

PERIODS = [
    ("full_2018_to_2026_04_29", "2018-01-02", "2026-04-29"),
    ("pre_reference_2018_2021", "2018-01-02", "2021-12-30"),
    ("reference_overlap_2022_2026_04_29", "2022-01-04", "2026-04-29"),
    ("reference_overlap_2022_2026_03_31", "2022-01-04", "2026-03-31"),
    ("main_backtest_2022_2025", "2022-01-04", "2025-12-30"),
    ("validation_2026_q1", "2026-01-02", "2026-03-31"),
    ("recent_shadow_2026_04", "2026-04-01", "2026-04-29"),
    ("stress_covid_2020_02_2020_04", "2020-02-01", "2020-04-30"),
    ("stress_2022_full_year", "2022-01-04", "2022-12-29"),
    ("stress_2024_07_09", "2024-07-01", "2024-09-30"),
]


def _read_symbols(path: Path, limit: int | None = None) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        key = "symbol" if "symbol" in (reader.fieldnames or []) else (reader.fieldnames or [""])[0]
        symbols = sorted({normalize_krx_symbol((r.get(key) or "").strip()) for r in reader if (r.get(key) or "").strip()})
    return symbols[:limit] if limit else symbols


def _max_drawdown(vals: list[float]) -> float:
    if not vals:
        return math.nan
    peak, mdd = vals[0], 0.0
    for v in vals:
        if v > peak:
            peak = v
        mdd = min(mdd, (v - peak) / peak if peak else 0.0)
    return mdd


def _coverage(conn: sqlite3.Connection, table: str) -> dict[str, Any]:
    row = conn.execute(f"SELECT MIN(date) mn, MAX(date) mx, COUNT(*) c, COUNT(DISTINCT date) d, COUNT(DISTINCT symbol) s FROM {table}").fetchone()
    return {"table": table, "min_date": row["mn"], "max_date": row["mx"], "row_count": row["c"], "distinct_dates": row["d"], "symbol_count": row["s"]}


def _period_metrics(rows: list[sqlite3.Row], period: str, start: str, end: str) -> dict[str, Any]:
    sub = [r for r in rows if start <= r["date"] <= end]
    if not sub:
        return {"strategy_name": "baseline_old", "run_id": "", "period": period, "start_date": start, "end_date": end, "actual_start_date": "", "actual_end_date": "", "total_return": math.nan, "mdd": math.nan, "avg_exposure": math.nan, "avg_position_count": math.nan, "final_nav": math.nan, "trading_days": 0, "notes": "no rows"}
    eq = [float(r["equity"]) for r in sub]
    exp = [float(r["exposure"]) for r in sub]
    pc = [float(r["position_count"]) for r in sub]
    return {"strategy_name": "baseline_old", "run_id": sub[0]["run_id"], "period": period, "start_date": start, "end_date": end, "actual_start_date": sub[0]["date"], "actual_end_date": sub[-1]["date"], "total_return": eq[-1]/eq[0]-1.0, "mdd": _max_drawdown(eq), "avg_exposure": sum(exp)/len(exp), "avg_position_count": sum(pc)/len(pc), "final_nav": eq[-1], "trading_days": len(sub), "notes": "research-only; survivorship bias possible"}


def _returns_by_bucket(rows: list[sqlite3.Row], key_len: int) -> list[dict[str, Any]]:
    b: dict[str, list[float]] = {}
    for r in rows:
        b.setdefault(r["date"][:key_len], []).append(float(r["equity"]))
    out = []
    for k, vals in sorted(b.items()):
        out.append({"period": k, "return": vals[-1]/vals[0]-1.0 if vals and vals[0] else math.nan})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-db", default="data/kospi_495_rolling_3y.db")
    ap.add_argument("--research-db", default="data/kospi_495_rolling_2018_2026_research.db")
    ap.add_argument("--universe-csv", default="data/kospi_valid_universe_495.csv")
    ap.add_argument("--output-dir", default="data/reports/research")
    ap.add_argument("--start-date", default="2018-01-02")
    ap.add_argument("--end-date", default="2026-04-29")
    ap.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    ap.add_argument("--skip-ingest", action="store_true")
    ap.add_argument("--symbols-limit", type=int)
    ap.add_argument("--force-rebuild", action="store_true")
    args = ap.parse_args()

    source_db = Path(args.source_db)
    research_db = Path(args.research_db)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.force_rebuild and research_db.exists():
        research_db.unlink()
    if not research_db.exists():
        research_db.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_db, research_db)

    conn = get_connection(research_db)
    init_db(conn)

    symbols = _read_symbols(Path(args.universe_csv), args.symbols_limit)
    if not symbols:
        raise RuntimeError("No symbols loaded from universe csv")

    cov_price = _coverage(conn, "daily_prices")
    needs_ingest = (cov_price["min_date"] is None or cov_price["min_date"] > args.start_date or cov_price["max_date"] < args.end_date or cov_price["symbol_count"] < len(symbols))
    if needs_ingest and not args.skip_ingest:
        ingest_krx_prices(conn, symbols, args.start_date, args.end_date)

    conn.execute(f"DELETE FROM daily_prices WHERE symbol NOT IN ({','.join('?' for _ in symbols)})", symbols)
    conn.commit()

    generate_daily_features(conn, start_date=args.start_date, end_date=args.end_date)
    build_rolling_liquidity_universe(conn, universe_size=BASELINE_PARAMS["universe_size"], lookback_days=BASELINE_PARAMS["universe_lookback_days"])
    generate_daily_scores(conn, include_history=True, scoring_profile="old", universe_mode="rolling_liquidity", universe_size=100, universe_lookback_days=20)

    run_id = run_backtest(conn, start_date=args.start_date, end_date=args.end_date, top_n=5, rebalance_frequency="weekly", min_holding_days=10, keep_rank_threshold=9, scoring_profile="old", market_filter_enabled=False, entry_gate_enabled=False, enable_position_stop_loss=True, position_stop_loss_pct=0.10, stop_loss_cash_mode="keep_cash", stop_loss_cooldown_days=0, enable_trailing_stop=False, enable_overheat_entry_gate=False, enable_entry_quality_gate=False)

    nav_rows = conn.execute("SELECT run_id,date,equity,exposure,position_count FROM backtest_results WHERE run_id=? ORDER BY date", (run_id,)).fetchall()
    coverage_rows = [_coverage(conn, "daily_prices"), _coverage(conn, "daily_features"), _coverage(conn, "daily_scores")]
    score_extra = conn.execute("SELECT MIN(rank) mn_rank, MAX(rank) mx_rank, AVG(cnt) avg_rows_per_date FROM (SELECT date, COUNT(*) cnt FROM daily_scores GROUP BY date)").fetchone()
    coverage_rows.append({"table": "daily_scores_rank", "min_date": "", "max_date": "", "row_count": "", "distinct_dates": "", "symbol_count": "", "min_rank": score_extra["mn_rank"], "max_rank": score_extra["mx_rank"], "avg_rows_per_date": score_extra["avg_rows_per_date"]})

    summary = []
    for p, s, e in PERIODS:
        summary.append(_period_metrics(nav_rows, p, s, e))

    ref_checks = []
    repro_fail = False
    repro_warning = False
    sum_map = {r["period"]: r for r in summary}
    for period, ref in FROZEN_REFERENCE.items():
        ext = sum_map.get(period, {})
        rd = (ext.get("total_return", math.nan) - ref["total_return"]) if ext else math.nan
        md = (ext.get("mdd", math.nan) - ref["mdd"]) if ext else math.nan
        status = "PASS"
        if period == "reference_overlap_2022_2026_04_29" and (not math.isnan(rd)) and abs(rd) >= 0.50:
            status = "FAIL"; repro_fail = True
        elif period == "main_backtest_2022_2025" and (not math.isnan(rd)) and abs(rd) >= 0.50:
            status = "WARNING"; repro_warning = True
        ref_checks.append({"period": period, "extended_run_return": ext.get("total_return"), "frozen_reference_return": ref["total_return"], "return_diff": rd, "extended_run_mdd": ext.get("mdd"), "frozen_reference_mdd": ref["mdd"], "mdd_diff": md, "reproducibility_status": status})

    monthly = _returns_by_bucket(nav_rows, 7)
    yearly = _returns_by_bucket(nav_rows, 4)
    nav_curve = [{"date": r["date"], "equity": r["equity"], "exposure": r["exposure"], "position_count": r["position_count"]} for r in nav_rows]

    def dump_csv(path: Path, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

    dump_csv(out_dir / f"exp20_extended_2018_baseline_summary_{run_id}.csv", summary)
    dump_csv(out_dir / f"exp20_extended_2018_monthly_returns_{run_id}.csv", monthly)
    dump_csv(out_dir / f"exp20_extended_2018_yearly_returns_{run_id}.csv", yearly)
    dump_csv(out_dir / f"exp20_extended_2018_nav_curve_{run_id}.csv", nav_curve)
    dump_csv(out_dir / f"exp20_reference_overlap_check_{run_id}.csv", ref_checks)
    dump_csv(out_dir / f"exp20_data_coverage_{run_id}.csv", coverage_rows)

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(), "source_db": str(source_db), "research_db": str(research_db),
        "universe_csv": args.universe_csv, "start_date": args.start_date, "end_date": args.end_date,
        "symbol_count": len(symbols), "run_id": run_id, "reference_run_id": args.reference_run_id,
        "baseline_parameters": BASELINE_PARAMS, "coverage_summary": coverage_rows,
        "reference_reproducibility_results": ref_checks,
        "survivorship_bias_warning": "current 495 universe extended backward may introduce survivorship bias",
        "output_paths": [str(out_dir / f"exp20_extended_2018_baseline_summary_{run_id}.csv")],
        "notes": ["research-only", "no production DB modification", "no paper trading change", "no live trading/order", "current 495 universe extended backward may introduce survivorship bias", "low reproducibility: expanded result reliability lowered" if (repro_fail or repro_warning) else "reference overlap reproducibility acceptable"],
    }
    (out_dir / f"exp20_metadata_{run_id}.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    find = {r["period"]: r for r in summary}
    repro_status = "FAIL" if repro_fail else ("WARNING" if repro_warning else "PASS")
    print("\n[EXP20 SUMMARY]")
    print(f"research_db={research_db}")
    print(f"coverage_prices={coverage_rows[0]}")
    print(f"coverage_features={coverage_rows[1]}")
    print(f"coverage_scores={coverage_rows[2]}")
    print(f"backtest_run_id={run_id}")
    for key in ["full_2018_to_2026_04_29", "pre_reference_2018_2021", "reference_overlap_2022_2026_04_29", "stress_covid_2020_02_2020_04", "stress_2022_full_year", "stress_2024_07_09"]:
        v = find[key]; print(f"{key}: return={v['total_return']:.6f} mdd={v['mdd']:.6f}")
    print(f"reference_reproducibility={repro_status}")
    if repro_fail:
        print("final_judgement=reference overlap reproducibility FAIL -> 확장 결과 신뢰 낮음, simulator/data generation 재점검 필요")
    elif find["full_2018_to_2026_04_29"]["total_return"] > 0:
        print("final_judgement=2018 확장에서도 baseline_old 강세 -> baseline 유지, 전략 변경 없음")
    else:
        print("final_judgement=2018 확장에서 성과 약화 -> 2022 이후 regime 특화 가능성, production은 paper observation 유지")


if __name__ == "__main__":
    main()
