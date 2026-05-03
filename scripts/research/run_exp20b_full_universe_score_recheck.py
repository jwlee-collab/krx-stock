from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from pipeline.backtest import run_backtest
from pipeline.scoring import score_formula_v1

REFERENCE_METRICS = {
    "reference_overlap_2022_2026_04_29": {"return": 3.7921630281643584, "mdd": -0.34139230404085474},
    "reference_overlap_2022_2026_03_31": {"return": 2.4012550830632016, "mdd": -0.34139230404085474},
    "main_backtest_2022_2025": {"return": 2.454511143509467, "mdd": -0.34139230404085474},
    "validation_2026_q1": {"return": -0.0248309466740414, "mdd": -0.306184},
    "recent_shadow_2026_04": {"return": 0.2950930200253394, "mdd": -0.100226},
    "stress_2024_07_09": {"return": -0.2432450468509677, "mdd": -0.251041},
}

SPLITS = [
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

CHECK_DATES = ["2022-01-04", "2024-07-01", "2024-09-30", "2025-12-30", "2026-03-31", "2026-04-29"]


def _connect(path: str, ro: bool = False) -> sqlite3.Connection:
    if ro:
        conn = sqlite3.connect(f"file:{Path(path).expanduser()}?mode=ro", uri=True)
    else:
        conn = sqlite3.connect(Path(path).expanduser())
    conn.row_factory = sqlite3.Row
    return conn


def _coverage(conn: sqlite3.Connection, start: str, end: str) -> dict[str, float | int | str | None]:
    base = conn.execute(
        """
        SELECT MIN(date) min_date, MAX(date) max_date, COUNT(*) n_rows,
               COUNT(DISTINCT date) n_dates, COUNT(DISTINCT symbol) n_symbols
        FROM daily_scores WHERE date BETWEEN ? AND ?
        """,
        (start, end),
    ).fetchone()
    by_date = conn.execute(
        "SELECT date, COUNT(*) c FROM daily_scores WHERE date BETWEEN ? AND ? GROUP BY date",
        (start, end),
    ).fetchall()
    counts = [int(r["c"]) for r in by_date]
    return {
        "min_date": base["min_date"], "max_date": base["max_date"], "n_rows": int(base["n_rows"] or 0),
        "n_dates": int(base["n_dates"] or 0), "n_symbols": int(base["n_symbols"] or 0),
        "avg_rows_per_date": float(mean(counts)) if counts else 0.0,
        "min_rows_per_date": min(counts) if counts else 0,
        "max_rows_per_date": max(counts) if counts else 0,
    }


def _regenerate_scores(conn: sqlite3.Connection, start: str, end: str, symbols_limit: int | None) -> int:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS daily_scores(symbol TEXT NOT NULL, date TEXT NOT NULL, score REAL, rank INTEGER, PRIMARY KEY(symbol,date))"
    )
    q = """
        SELECT symbol, date, ret_1d, ret_5d, momentum_20d, range_pct, volume_z20
        FROM daily_features
        WHERE date BETWEEN ? AND ?
    """
    params: list[object] = [start, end]
    if symbols_limit:
        q += " AND symbol IN (SELECT symbol FROM daily_features WHERE date BETWEEN ? AND ? GROUP BY symbol ORDER BY symbol LIMIT ?)"
        params += [start, end, int(symbols_limit)]
    q += " ORDER BY date, symbol"
    rows = conn.execute(q, params).fetchall()
    by_date: dict[str, list[tuple[str, float]]] = {}
    for r in rows:
        s = score_formula_v1(r["ret_1d"], r["ret_5d"], r["momentum_20d"], r["range_pct"], r["volume_z20"])
        by_date.setdefault(r["date"], []).append((r["symbol"], float(s)))
    conn.execute("DELETE FROM daily_scores WHERE date BETWEEN ? AND ?", (start, end))
    out: list[tuple[str, str, float, int]] = []
    for d, items in by_date.items():
        items = sorted(items, key=lambda x: x[1], reverse=True)
        prev = None
        rank = 0
        for i, (sym, sc) in enumerate(items, start=1):
            if prev is None or sc != prev:
                rank = i
                prev = sc
            out.append((sym, d, sc, rank))
    conn.executemany("INSERT INTO daily_scores(symbol,date,score,rank) VALUES(?,?,?,?)", out)
    conn.commit()
    return len(out)


def _overlap(source: sqlite3.Connection, research: sqlite3.Connection) -> list[dict[str, object]]:
    out = []
    for d in CHECK_DATES:
        rec = {"date": d}
        for k in [5, 10, 20]:
            a = {r[0] for r in source.execute("SELECT symbol FROM daily_scores WHERE date=? ORDER BY rank,symbol LIMIT ?", (d, k)).fetchall()}
            b = {r[0] for r in research.execute("SELECT symbol FROM daily_scores WHERE date=? ORDER BY rank,symbol LIMIT ?", (d, k)).fetchall()}
            ov = len(a & b)
            rec[f"top{k}_source_n"] = len(a)
            rec[f"top{k}_research_n"] = len(b)
            rec[f"top{k}_overlap_n"] = ov
            rec[f"top{k}_overlap_ratio"] = (ov / k) if k else 0.0
        out.append(rec)
    return out


def _max_drawdown(equity: list[float]) -> float:
    peak = equity[0]
    mdd = 0.0
    for v in equity:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, (v - peak) / peak)
    return mdd


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader(); w.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--research-db", default="~/krx-stock-persist/data/kospi_495_rolling_2018_2026_research.db")
    p.add_argument("--source-db", default="~/krx-stock-persist/data/kospi_495_rolling_3y.db")
    p.add_argument("--output-dir", default="~/krx-stock-persist/reports/research")
    p.add_argument("--start-date", default="2018-01-02")
    p.add_argument("--end-date", default="2026-04-29")
    p.add_argument("--reference-run-id", default="3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316")
    p.add_argument("--symbols-limit", type=int)
    p.add_argument("--skip-backtest", action="store_true")
    args = p.parse_args()

    run_id = str(uuid.uuid4())
    out_dir = Path(args.output_dir).expanduser(); out_dir.mkdir(parents=True, exist_ok=True)
    research = _connect(args.research_db)
    source = _connect(args.source_db, ro=True)

    before = _coverage(research, args.start_date, args.end_date)
    inserted = _regenerate_scores(research, args.start_date, args.end_date, args.symbols_limit)
    after = _coverage(research, args.start_date, args.end_date)
    overlaps = _overlap(source, research)

    bt_run_id = None
    summary_rows: list[dict[str, object]] = []
    nav_rows: list[dict[str, object]] = []
    yearly_rows: list[dict[str, object]] = []
    ref_rows: list[dict[str, object]] = []
    if not args.skip_backtest:
        bt_run_id = run_backtest(
            research, top_n=5, start_date=args.start_date, end_date=args.end_date, rebalance_frequency="weekly",
            min_holding_days=10, keep_rank_threshold=9, scoring_profile="old", market_filter_enabled=False,
            entry_gate_enabled=False, enable_position_stop_loss=True, position_stop_loss_pct=0.10,
            stop_loss_cash_mode="keep_cash", stop_loss_cooldown_days=0, enable_trailing_stop=False,
            enable_overheat_entry_gate=False, enable_entry_quality_gate=False,
        )
        bt = research.execute("SELECT date,equity,daily_return FROM backtest_results WHERE run_id=? ORDER BY date", (bt_run_id,)).fetchall()
        eq = [float(r["equity"]) for r in bt]
        nav_rows = [{"date": r["date"], "equity": float(r["equity"]), "daily_return": float(r["daily_return"])} for r in bt]
        for name, s, e in SPLITS:
            part = [r for r in bt if s <= r["date"] <= e]
            if not part:
                continue
            ret = float(part[-1]["equity"]) / float(part[0]["equity"]) - 1.0
            mdd = _max_drawdown([float(x["equity"]) for x in part])
            row = {"split": name, "start": s, "end": e, "return": ret, "mdd": mdd}
            summary_rows.append(row)
            if name in REFERENCE_METRICS:
                ref = REFERENCE_METRICS[name]
                ref_rows.append({**row, "reference_return": ref["return"], "reference_mdd": ref["mdd"], "return_diff": ret - ref["return"], "mdd_diff": mdd - ref["mdd"]})
        y = research.execute("SELECT substr(date,1,4) y, MIN(equity) mn, MAX(equity) mx FROM backtest_results WHERE run_id=? GROUP BY substr(date,1,4) ORDER BY y", (bt_run_id,)).fetchall()
        yearly_rows = [{"year": r["y"], "year_return": (float(r["mx"]) / float(r["mn"]) - 1.0) if float(r["mn"]) else 0.0} for r in y]

    _write_csv(out_dir / f"exp20b_full_universe_score_coverage_{run_id}.csv", [{"phase": "before", **before}, {"phase": "after", **after}])
    _write_csv(out_dir / f"exp20b_score_overlap_vs_source_{run_id}.csv", overlaps)
    _write_csv(out_dir / f"exp20b_backtest_summary_{run_id}.csv", summary_rows)
    _write_csv(out_dir / f"exp20b_reference_overlap_check_{run_id}.csv", ref_rows)
    _write_csv(out_dir / f"exp20b_yearly_returns_{run_id}.csv", yearly_rows)
    _write_csv(out_dir / f"exp20b_nav_curve_{run_id}.csv", nav_rows)

    meta = {
        "created_at": datetime.now(timezone.utc).isoformat(), "research_db": str(Path(args.research_db).expanduser()),
        "source_db": str(Path(args.source_db).expanduser()), "score_generation_mode": "full_daily_features_universe",
        "selected_scoring_formula": "pipeline.scoring.score_formula_v1(old)", "score_coverage": {"before": before, "after": after},
        "score_overlap_results": overlaps, "backtest_run_id": bt_run_id, "reference_reproducibility_result": ref_rows,
        "survivorship_bias_warning": "Universe survivorship bias may remain in 2018~2026 history.",
        "notes": ["research-only", "source DB read-only", "production DB not modified", "no paper trading change", "daily_scores regenerated only inside research DB"],
    }
    (out_dir / f"exp20b_metadata_{run_id}.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[Score coverage before]", before)
    print("[Score coverage after]", after)
    print(f"[Rows inserted] {inserted}")
    for row in overlaps:
        print(f"[Overlap {row['date']}] top5={row['top5_overlap_n']}/5 top10={row['top10_overlap_n']}/10 top20={row['top20_overlap_n']}/20")
    print(f"[Backtest run_id] {bt_run_id}")
    if ref_rows:
        for r in ref_rows:
            print(f"[Reference compare {r['split']}] return={r['return']:.6f} vs ref={r['reference_return']:.6f} diff={r['return_diff']:.6f}; mdd={r['mdd']:.6f} vs ref={r['reference_mdd']:.6f} diff={r['mdd_diff']:.6f}")
    print("[Final 판정]")
    print("- 2022~2026 overlap이 복구되면 2018~2021 결과를 사용할 수 있음")
    print("- overlap이 여전히 실패하면 backtest logic/universe usage 문제로 전환")
    print("- top score overlap이 낮으면 scoring formula/feature 차이 문제")


if __name__ == "__main__":
    main()
