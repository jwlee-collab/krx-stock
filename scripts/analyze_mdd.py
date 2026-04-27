#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path
from typing import Any


def _load_equity_curve(conn: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT date, equity, daily_return, position_count
        FROM backtest_results
        WHERE run_id=?
        ORDER BY date
        """,
        (run_id,),
    ).fetchall()


def _compute_mdd(curve: list[sqlite3.Row]) -> dict[str, Any]:
    if not curve:
        raise ValueError("No backtest_results rows found for run_id")

    running_peak = float(curve[0]["equity"])
    running_peak_date = str(curve[0]["date"])
    mdd = 0.0
    trough_date = str(curve[0]["date"])
    peak_date = running_peak_date

    for row in curve:
        date = str(row["date"])
        eq = float(row["equity"])
        if eq > running_peak:
            running_peak = eq
            running_peak_date = date
        dd = (eq - running_peak) / running_peak if running_peak > 0 else 0.0
        if dd < mdd:
            mdd = dd
            trough_date = date
            peak_date = running_peak_date

    peak_equity = None
    trough_equity = None
    for row in curve:
        if row["date"] == peak_date and peak_equity is None:
            peak_equity = float(row["equity"])
        if row["date"] == trough_date:
            trough_equity = float(row["equity"])

    recovery_date = None
    reached_trough = False
    for row in curve:
        if row["date"] == trough_date:
            reached_trough = True
            continue
        if reached_trough and peak_equity is not None and float(row["equity"]) >= peak_equity:
            recovery_date = str(row["date"])
            break

    return {
        "peak_date": peak_date,
        "trough_date": trough_date,
        "recovery_date": recovery_date,
        "mdd_pct": mdd,
        "peak_equity": peak_equity,
        "trough_equity": trough_equity,
    }


def _load_mdd_holdings(conn: sqlite3.Connection, run_id: str, peak_date: str, trough_date: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT run_id, date, symbol, weight, entry_date, entry_price, close, unrealized_return, rank, score
        FROM backtest_holdings
        WHERE run_id=?
          AND date BETWEEN ? AND ?
        ORDER BY date, symbol
        """,
        (run_id, peak_date, trough_date),
    ).fetchall()


def _build_symbol_contribution(holdings: list[sqlite3.Row], peak_equity: float | None) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[sqlite3.Row]] = {}
    for row in holdings:
        by_symbol.setdefault(str(row["symbol"]), []).append(row)

    contribution_rows: list[dict[str, Any]] = []
    for symbol, rows in sorted(by_symbol.items()):
        rows = sorted(rows, key=lambda r: str(r["date"]))
        holding_days = len({str(r["date"]) for r in rows})
        first_close = float(rows[0]["close"]) if rows[0]["close"] is not None else None
        last_close = float(rows[-1]["close"]) if rows[-1]["close"] is not None else None
        price_change_pct = None
        if first_close is not None and last_close is not None and first_close > 0:
            price_change_pct = (last_close - first_close) / first_close
        avg_weight = sum(float(r["weight"]) for r in rows) / len(rows)
        estimated_contribution_pct = (price_change_pct * avg_weight) if price_change_pct is not None else None
        estimated_contribution_notional = (
            peak_equity * estimated_contribution_pct
            if peak_equity is not None and estimated_contribution_pct is not None
            else None
        )
        contribution_rows.append(
            {
                "symbol": symbol,
                "holding_days": holding_days,
                "peak_to_trough_price_change_pct": price_change_pct,
                "avg_weight": avg_weight,
                "estimated_loss_contribution_pct": estimated_contribution_pct,
                "estimated_loss_contribution_notional": estimated_contribution_notional,
                "peak_interval_close": first_close,
                "trough_interval_close": last_close,
            }
        )

    contribution_rows.sort(
        key=lambda r: r["estimated_loss_contribution_pct"] if r["estimated_loss_contribution_pct"] is not None else 1.0
    )
    return contribution_rows


def _load_risk_events(conn: sqlite3.Connection, run_id: str, peak_date: str, trough_date: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT date, symbol, event_type, trigger_price, reference_price, return_pct, action
        FROM backtest_risk_events
        WHERE run_id=?
          AND date BETWEEN ? AND ?
        ORDER BY date, event_id
        """,
        (run_id, peak_date, trough_date),
    ).fetchall()


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze MDD window from stored backtest holdings")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--run-id", required=True, help="Backtest run_id")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    curve = _load_equity_curve(conn, args.run_id)
    mdd = _compute_mdd(curve)
    holdings = _load_mdd_holdings(conn, args.run_id, mdd["peak_date"], mdd["trough_date"])
    contributions = _build_symbol_contribution(holdings, mdd["peak_equity"])
    risk_events = _load_risk_events(conn, args.run_id, mdd["peak_date"], mdd["trough_date"])

    risk_event_summary: dict[str, int] = {}
    for evt in risk_events:
        et = str(evt["event_type"])
        risk_event_summary[et] = risk_event_summary.get(et, 0) + 1

    holdings_csv_rows = [
        {
            "run_id": r["run_id"],
            "date": r["date"],
            "symbol": r["symbol"],
            "weight": r["weight"],
            "entry_date": r["entry_date"],
            "entry_price": r["entry_price"],
            "close": r["close"],
            "unrealized_return": r["unrealized_return"],
            "rank": r["rank"],
            "score": r["score"],
        }
        for r in holdings
    ]

    summary_payload = {
        "run_id": args.run_id,
        "mdd_peak_date": mdd["peak_date"],
        "mdd_trough_date": mdd["trough_date"],
        "mdd_recovery_date": mdd["recovery_date"],
        "mdd_pct": mdd["mdd_pct"],
        "peak_equity": mdd["peak_equity"],
        "trough_equity": mdd["trough_equity"],
        "mdd_window_holding_rows": len(holdings),
        "mdd_window_symbols": len({r['symbol'] for r in holdings}),
        "risk_events_in_mdd_window": risk_event_summary,
        "has_risk_events": bool(risk_events),
    }

    json_path = output_dir / f"mdd_summary_{args.run_id}.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(summary_payload, f, ensure_ascii=False, indent=2)

    holdings_csv_path = output_dir / f"mdd_holdings_{args.run_id}.csv"
    _write_csv(
        holdings_csv_path,
        holdings_csv_rows,
        ["run_id", "date", "symbol", "weight", "entry_date", "entry_price", "close", "unrealized_return", "rank", "score"],
    )

    contrib_csv_path = output_dir / f"mdd_symbol_contribution_{args.run_id}.csv"
    _write_csv(
        contrib_csv_path,
        contributions,
        [
            "symbol",
            "holding_days",
            "peak_to_trough_price_change_pct",
            "avg_weight",
            "estimated_loss_contribution_pct",
            "estimated_loss_contribution_notional",
            "peak_interval_close",
            "trough_interval_close",
        ],
    )

    report_path = output_dir / f"mdd_report_{args.run_id}.md"
    top_losses = contributions[:10]
    with report_path.open("w", encoding="utf-8") as f:
        f.write(f"# MDD Analysis Report ({args.run_id})\n\n")
        f.write("## MDD Summary\n")
        f.write(f"- Peak date: {mdd['peak_date']}\n")
        f.write(f"- Trough date: {mdd['trough_date']}\n")
        f.write(f"- Recovery date: {mdd['recovery_date'] or 'N/A'}\n")
        f.write(f"- MDD: {mdd['mdd_pct']:.2%}\n")
        f.write(f"- Peak equity: {mdd['peak_equity']}\n")
        f.write(f"- Trough equity: {mdd['trough_equity']}\n\n")

        f.write("## Holdings in MDD Window\n")
        f.write(f"- Holding rows: {len(holdings)}\n")
        f.write(f"- Unique symbols: {len({r['symbol'] for r in holdings})}\n\n")

        f.write("## Top Estimated Loss Contributions\n")
        f.write("| Symbol | Holding Days | Peak→Trough Price Change | Avg Weight | Est. Contribution |\n")
        f.write("|---|---:|---:|---:|---:|\n")
        for row in top_losses:
            pchg = row["peak_to_trough_price_change_pct"]
            ec = row["estimated_loss_contribution_pct"]
            f.write(
                "| {symbol} | {days} | {pchg} | {w:.2%} | {ec} |\n".format(
                    symbol=row["symbol"],
                    days=row["holding_days"],
                    pchg=f"{pchg:.2%}" if pchg is not None else "N/A",
                    w=float(row["avg_weight"]),
                    ec=f"{ec:.2%}" if ec is not None else "N/A",
                )
            )

        f.write("\n## Risk Events (Stop Loss / Risk Cut)\n")
        if risk_events:
            for et, cnt in sorted(risk_event_summary.items()):
                f.write(f"- {et}: {cnt}\n")
        else:
            f.write("- No risk events found in MDD window.\n")

    print(json.dumps(summary_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
