#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
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
    build_rolling_liquidity_universe(conn, symbols=symbols, top_n=100)
    validate_rolling_universe_no_lookahead(conn)

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

    daily_all: list[dict[str, object]] = []
    exposure_all: list[dict[str, object]] = []
    window_rows: list[dict[str, object]] = []
    summary_rows: list[dict[str, object]] = []
    full_rows: list[dict[str, object]] = []
    compliance_rows: list[dict[str, object]] = []
    monthly_rows: list[dict[str, object]] = []

    for c in candidates:
        generate_daily_scores(conn, scoring_profile=c.scoring_profile)
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
    }
    print(f"BROAD_SECTOR_GUARDRAIL_EXPERIMENT_JSON={json.dumps(payload, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
