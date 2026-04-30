#!/usr/bin/env python3
from __future__ import annotations

"""Experiment 01B: Attach features/scores/prices/forward returns to frozen baseline holdings (report-only)."""

import argparse
import csv
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

DEFAULT_DB = "data/kospi_495_rolling_3y.db"
DEFAULT_OUTPUT_DIR = "data/reports/research"
DEFAULT_REFERENCE_RUN_ID = "3f3cc4bf-bbe4-4cb7-b0ef-cdc2d8020316"

PERIOD_WINDOWS = [
    ("main_backtest", "2022-01-04", "2025-12-30"),
    ("validation_2026_q1", "2026-01-02", "2026-03-31"),
    ("recent_shadow_2026_04", "2026-04-01", "2026-04-29"),
]
STRESS_START = "2024-07-01"
STRESS_END = "2024-09-30"

HOLDINGS_CANDIDATE_COLUMNS = [
    "run_id", "date", "symbol", "weight", "entry_date", "entry_price", "close", "unrealized_return", "rank", "score",
]
FEATURE_CANDIDATES = [
    "ret_1d", "ret_5d", "momentum_20d", "momentum_60d", "sma_20_gap", "sma_60_gap", "rsi_14", "avg_volume_20d",
    "volume_ratio", "dollar_volume", "avg_dollar_volume_20d", "dollar_volume_ratio", "range_pct", "volatility_20d", "drawdown_20d",
]
PRICE_CANDIDATE_COLUMNS = ["open", "high", "low", "close", "volume"]


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _normalize_symbol(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text.zfill(6)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_div_return(numerator: Any, denominator: Any) -> float | None:
    n = _to_float(numerator)
    d = _to_float(denominator)
    if n is None or d is None or d == 0:
        return None
    return (n / d) - 1.0


def _period_label(date_value: str) -> str:
    for label, start, end in PERIOD_WINDOWS:
        if start <= date_value <= end:
            return label
    return "other"


def _is_stress(date_value: str) -> bool:
    return STRESS_START <= date_value <= STRESS_END


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _numeric_stats(values: list[Any]) -> tuple[float | None, float | None, float | None, float | None]:
    nums: list[float] = []
    for v in values:
        fv = _to_float(v)
        if fv is not None and not math.isnan(fv):
            nums.append(fv)
    if not nums:
        return None, None, None, None
    return sum(nums) / len(nums), median(nums), min(nums), max(nums)


def _aggregate_summary(rows: list[dict[str, Any]], label: str) -> dict[str, Any]:
    def vals(name: str) -> list[float]:
        out: list[float] = []
        for r in rows:
            v = _to_float(r.get(name))
            if v is not None and not math.isnan(v):
                out.append(v)
        return out

    next20 = vals("next_20d_return")
    daily_score = vals("daily_score")
    daily_rank = vals("daily_rank")
    return {
        "period_label": label,
        "row_count": len(rows),
        "unique_dates": len({str(r.get("date", "")) for r in rows if r.get("date")}),
        "unique_symbols": len({str(r.get("symbol_norm", "")) for r in rows if r.get("symbol_norm")}),
        "avg_weight": (sum(vals("weight")) / len(vals("weight"))) if vals("weight") else None,
        "avg_unrealized_return": (sum(vals("unrealized_return")) / len(vals("unrealized_return"))) if vals("unrealized_return") else None,
        "avg_daily_score": (sum(daily_score) / len(daily_score)) if daily_score else None,
        "median_daily_score": median(daily_score) if daily_score else None,
        "avg_daily_rank": (sum(daily_rank) / len(daily_rank)) if daily_rank else None,
        "median_daily_rank": median(daily_rank) if daily_rank else None,
        "avg_next_1d_return": (sum(vals("next_1d_return")) / len(vals("next_1d_return"))) if vals("next_1d_return") else None,
        "median_next_1d_return": median(vals("next_1d_return")) if vals("next_1d_return") else None,
        "avg_next_5d_return": (sum(vals("next_5d_return")) / len(vals("next_5d_return"))) if vals("next_5d_return") else None,
        "median_next_5d_return": median(vals("next_5d_return")) if vals("next_5d_return") else None,
        "avg_next_20d_return": (sum(next20) / len(next20)) if next20 else None,
        "median_next_20d_return": median(next20) if next20 else None,
        "positive_rate_next_20d": (sum(1 for x in next20 if x > 0) / len(next20)) if next20 else None,
        "forward_20d_non_null_count": len(next20),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Experiment 01B feature-forward attach (report-only)")
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--reference-run-id", default=DEFAULT_REFERENCE_RUN_ID)
    args = p.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    holdings_columns = _table_columns(conn, "backtest_holdings")
    features_columns = _table_columns(conn, "daily_features")
    scores_columns = _table_columns(conn, "daily_scores")
    prices_columns = _table_columns(conn, "daily_prices")

    for table, cols in {
        "backtest_holdings": holdings_columns,
        "daily_features": features_columns,
        "daily_scores": scores_columns,
        "daily_prices": prices_columns,
    }.items():
        if not cols:
            raise RuntimeError(f"Required table not found or empty schema: {table}")

    selected_holdings_cols = [c for c in HOLDINGS_CANDIDATE_COLUMNS if c in holdings_columns]
    if "date" not in selected_holdings_cols or "symbol" not in selected_holdings_cols:
        raise RuntimeError("backtest_holdings must include date and symbol")
    selected_feature_cols = [c for c in FEATURE_CANDIDATES if c in features_columns]
    unavailable_feature_cols = [c for c in FEATURE_CANDIDATES if c not in features_columns]

    score_select_cols = [c for c in ["date", "symbol", "score", "rank"] if c in scores_columns]
    price_select_cols = [c for c in ["date", "symbol", *PRICE_CANDIDATE_COLUMNS] if c in prices_columns]

    hq = f"SELECT {','.join(selected_holdings_cols)} FROM backtest_holdings WHERE run_id=? ORDER BY date, symbol"
    holdings_rows = [dict(r) for r in conn.execute(hq, (args.reference_run_id,)).fetchall()]

    for r in holdings_rows:
        r["symbol_norm"] = _normalize_symbol(r.get("symbol"))
        r["period_label"] = _period_label(str(r.get("date", "")))
        r["is_stress_2024_07_09"] = _is_stress(str(r.get("date", "")))
        if "rank" in r:
            r["holding_rank"] = r.pop("rank")
        if "score" in r:
            r["holding_score"] = r.pop("score")

    fq = f"SELECT date,symbol,{','.join(selected_feature_cols)} FROM daily_features" if selected_feature_cols else "SELECT date,symbol FROM daily_features"
    features_map: dict[tuple[str, str], dict[str, Any]] = {}
    for row in conn.execute(fq):
        rr = dict(row)
        key = (str(rr["date"]), _normalize_symbol(rr["symbol"]))
        features_map[key] = {c: rr.get(c) for c in selected_feature_cols}

    sq = f"SELECT {','.join(score_select_cols)} FROM daily_scores"
    scores_map: dict[tuple[str, str], dict[str, Any]] = {}
    for row in conn.execute(sq):
        rr = dict(row)
        key = (str(rr["date"]), _normalize_symbol(rr["symbol"]))
        scores_map[key] = {
            "daily_score": rr.get("score") if "score" in rr else None,
            "daily_rank": rr.get("rank") if "rank" in rr else None,
        }

    pq = f"SELECT {','.join(price_select_cols)} FROM daily_prices"
    prices_map: dict[tuple[str, str], dict[str, Any]] = {}
    price_series: dict[str, list[tuple[str, float]]] = {}
    for row in conn.execute(pq):
        rr = dict(row)
        d = str(rr["date"])
        sym = _normalize_symbol(rr["symbol"])
        prices_map[(d, sym)] = {
            "price_open": rr.get("open") if "open" in rr else None,
            "price_high": rr.get("high") if "high" in rr else None,
            "price_low": rr.get("low") if "low" in rr else None,
            "price_close": rr.get("close") if "close" in rr else None,
            "price_volume": rr.get("volume") if "volume" in rr else None,
        }
        c = _to_float(rr.get("close")) if "close" in rr else None
        if c is not None:
            price_series.setdefault(sym, []).append((d, c))

    forward_map: dict[tuple[str, str], dict[str, float | None]] = {}
    for sym, series in price_series.items():
        ordered = sorted(series, key=lambda x: x[0])
        closes = [x[1] for x in ordered]
        dates = [x[0] for x in ordered]
        for i, (d, c0) in enumerate(zip(dates, closes)):
            row = {"next_1d_return": None, "next_5d_return": None, "next_20d_return": None}
            for step, col in [(1, "next_1d_return"), (5, "next_5d_return"), (20, "next_20d_return")]:
                j = i + step
                if j < len(closes) and c0 != 0:
                    row[col] = (closes[j] / c0) - 1.0
            forward_map[(d, sym)] = row

    snapshot_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []

    for base in holdings_rows:
        row = dict(base)
        key = (str(row["date"]), str(row["symbol_norm"]))
        feat = features_map.get(key, {})
        scr = scores_map.get(key, {})
        prc = prices_map.get(key, {})
        fwd = forward_map.get(key, {})
        row.update({c: feat.get(c) for c in selected_feature_cols})
        row.update({"daily_score": scr.get("daily_score"), "daily_rank": scr.get("daily_rank")})
        row.update({
            "price_open": prc.get("price_open"), "price_high": prc.get("price_high"), "price_low": prc.get("price_low"),
            "price_close": prc.get("price_close"), "price_volume": prc.get("price_volume"),
        })
        row.update({
            "intraday_range_pct": _safe_div_return(row.get("price_high"), row.get("price_low")),
            "close_to_open_return": _safe_div_return(row.get("price_close"), row.get("price_open")),
        })
        row.update({
            "next_1d_return": fwd.get("next_1d_return"), "next_5d_return": fwd.get("next_5d_return"), "next_20d_return": fwd.get("next_20d_return"),
        })
        snapshot_rows.append(row)

        miss_feat = any(row.get(c) is None for c in selected_feature_cols) if selected_feature_cols else True
        missing_info = {
            "date": row.get("date"), "symbol": row.get("symbol"), "symbol_norm": row.get("symbol_norm"),
            "missing_daily_features": miss_feat,
            "missing_daily_scores": row.get("daily_score") is None and row.get("daily_rank") is None,
            "missing_daily_prices": row.get("price_close") is None,
            "missing_forward_1d": row.get("next_1d_return") is None,
            "missing_forward_5d": row.get("next_5d_return") is None,
            "missing_forward_20d": row.get("next_20d_return") is None,
            "period_label": row.get("period_label"),
            "is_stress_2024_07_09": row.get("is_stress_2024_07_09"),
        }
        if any(bool(missing_info[k]) for k in [
            "missing_daily_features", "missing_daily_scores", "missing_daily_prices", "missing_forward_1d", "missing_forward_5d", "missing_forward_20d",
        ]):
            missing_rows.append(missing_info)

    coverage_targets = [
        *selected_feature_cols, "holding_score", "holding_rank", "daily_score", "daily_rank", "intraday_range_pct", "close_to_open_return",
        "next_1d_return", "next_5d_return", "next_20d_return",
    ]
    coverage_rows = []
    for col in coverage_targets:
        vals = [r.get(col) for r in snapshot_rows]
        non_null = sum(1 for v in vals if v is not None)
        null_count = len(snapshot_rows) - non_null
        avg, med, mn, mx = _numeric_stats(vals)
        coverage_rows.append({
            "column": col,
            "non_null_count": non_null,
            "null_count": null_count,
            "coverage_rate": (non_null / len(snapshot_rows)) if snapshot_rows else None,
            "mean": avg,
            "median": med,
            "min": mn,
            "max": mx,
        })

    period_rows = []
    for label in [x[0] for x in PERIOD_WINDOWS] + ["other"]:
        period_rows.append(_aggregate_summary([r for r in snapshot_rows if r.get("period_label") == label], label))
    period_rows.append(_aggregate_summary([r for r in snapshot_rows if r.get("is_stress_2024_07_09")], "stress_2024_07_09"))

    run_id_safe = args.reference_run_id.replace("/", "_")
    snapshot_file = out_dir / f"exp01b_feature_forward_snapshot_{run_id_safe}.csv"
    coverage_file = out_dir / f"exp01b_attach_coverage_summary_{run_id_safe}.csv"
    period_file = out_dir / f"exp01b_period_forward_summary_{run_id_safe}.csv"
    missing_file = out_dir / f"exp01b_missing_join_rows_{run_id_safe}.csv"
    metadata_file = out_dir / f"exp01b_metadata_{run_id_safe}.json"

    snapshot_fields = list(snapshot_rows[0].keys()) if snapshot_rows else []
    coverage_fields = list(coverage_rows[0].keys()) if coverage_rows else []
    period_fields = list(period_rows[0].keys()) if period_rows else []
    missing_fields = [
        "date", "symbol", "symbol_norm", "missing_daily_features", "missing_daily_scores", "missing_daily_prices",
        "missing_forward_1d", "missing_forward_5d", "missing_forward_20d", "period_label", "is_stress_2024_07_09",
    ]

    _write_csv(snapshot_file, snapshot_rows, snapshot_fields)
    _write_csv(coverage_file, coverage_rows, coverage_fields)
    _write_csv(period_file, period_rows, period_fields)
    _write_csv(missing_file, missing_rows, missing_fields)

    join_cov = {
        "daily_features": sum(1 for r in snapshot_rows if all(r.get(c) is not None for c in selected_feature_cols)) / len(snapshot_rows) if snapshot_rows and selected_feature_cols else 0.0,
        "daily_scores": sum(1 for r in snapshot_rows if (r.get("daily_score") is not None or r.get("daily_rank") is not None)) / len(snapshot_rows) if snapshot_rows else 0.0,
        "daily_prices": sum(1 for r in snapshot_rows if r.get("price_close") is not None) / len(snapshot_rows) if snapshot_rows else 0.0,
    }
    forward_cov = {
        "next_1d_return": sum(1 for r in snapshot_rows if r.get("next_1d_return") is not None) / len(snapshot_rows) if snapshot_rows else 0.0,
        "next_5d_return": sum(1 for r in snapshot_rows if r.get("next_5d_return") is not None) / len(snapshot_rows) if snapshot_rows else 0.0,
        "next_20d_return": sum(1 for r in snapshot_rows if r.get("next_20d_return") is not None) / len(snapshot_rows) if snapshot_rows else 0.0,
    }

    metadata = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "reference_run_id": args.reference_run_id,
        "db_path": args.db,
        "selected_holdings_columns": selected_holdings_cols,
        "selected_daily_features_columns": selected_feature_cols,
        "unavailable_daily_features_columns": unavailable_feature_cols,
        "selected_daily_scores_columns": score_select_cols,
        "selected_daily_prices_columns": price_select_cols,
        "output_file_paths": {
            "snapshot": str(snapshot_file),
            "attach_coverage_summary": str(coverage_file),
            "period_forward_summary": str(period_file),
            "missing_join_rows": str(missing_file),
            "metadata": str(metadata_file),
        },
        "output_row_counts": {
            "snapshot": len(snapshot_rows),
            "attach_coverage_summary": len(coverage_rows),
            "period_forward_summary": len(period_rows),
            "missing_join_rows": len(missing_rows),
        },
        "join_coverage": join_cov,
        "forward_return_coverage": forward_cov,
        "note": [
            "report-only",
            "no DB write",
            "no strategy change",
            "no backtest rerun",
            "forward returns are diagnostic/lookahead-only and not used for trading",
            "lookahead diagnostic only",
        ],
    }
    metadata_file.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[Exp01B] Feature/forward attach report complete")
    print(f"- reference_run_id: {args.reference_run_id}")
    print(f"- snapshot rows: {len(snapshot_rows)}")
    print(f"- daily_features join coverage: {join_cov['daily_features']:.4f}")
    print(f"- daily_scores join coverage: {join_cov['daily_scores']:.4f}")
    print(f"- daily_prices join coverage: {join_cov['daily_prices']:.4f}")
    print(f"- next_1d coverage: {forward_cov['next_1d_return']:.4f}")
    print(f"- next_5d coverage: {forward_cov['next_5d_return']:.4f}")
    print(f"- next_20d coverage: {forward_cov['next_20d_return']:.4f}")
    print("- period forward summary:")
    for r in period_rows:
        print(
            f"  * {r['period_label']}: rows={r['row_count']}, dates={r['unique_dates']}, symbols={r['unique_symbols']}, "
            f"avg_next_20d={r['avg_next_20d_return']}, positive_rate_next_20d={r['positive_rate_next_20d']}"
        )
    print(f"- unavailable features: {unavailable_feature_cols}")
    print("- next step recommendation: Exp01C winner/loser + stress + overheat summary")


if __name__ == "__main__":
    main()
