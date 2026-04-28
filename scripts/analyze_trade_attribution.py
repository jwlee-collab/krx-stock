#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from statistics import median
from typing import Any

FEATURE_COLUMNS = [
    "ret_1d",
    "ret_5d",
    "momentum_20d",
    "momentum_60d",
    "sma_20_gap",
    "sma_60_gap",
    "range_pct",
    "volatility_20d",
    "volume_z20",
]

RISK_RATIO_COLUMNS = ["range_to_ret5", "volatility_to_momentum20", "ret5_minus_range"]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if abs(denominator) < 1e-12:
        return None
    return float(numerator) / abs(float(denominator))


def _load_holding_rows(conn: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT date, symbol, weight, entry_date, entry_price, close, rank, score
        FROM backtest_holdings
        WHERE run_id=?
        ORDER BY symbol, date
        """,
        (run_id,),
    ).fetchall()


def _load_feature_map(conn: sqlite3.Connection) -> dict[tuple[str, str], sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT symbol, date, ret_1d, ret_5d, momentum_20d, momentum_60d,
               sma_20_gap, sma_60_gap, range_pct, volatility_20d, volume_z20
        FROM daily_features
        """
    ).fetchall()
    return {(str(r["symbol"]), str(r["date"])): r for r in rows}


def _load_risk_events(conn: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT date, symbol, event_type
        FROM backtest_risk_events
        WHERE run_id=?
        ORDER BY date, event_id
        """,
        (run_id,),
    ).fetchall()


def _load_price_map(conn: sqlite3.Connection) -> dict[str, list[sqlite3.Row]]:
    rows = conn.execute(
        """
        SELECT symbol, date, close
        FROM daily_prices
        ORDER BY symbol, date
        """
    ).fetchall()
    by_symbol: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        by_symbol.setdefault(str(row["symbol"]), []).append(row)
    return by_symbol


def _find_universe_sector_map() -> tuple[dict[str, str], str]:
    data_dir = Path("data")
    if not data_dir.exists():
        return {}, "sector unavailable"

    for csv_path in sorted(data_dir.glob("*.csv")):
        try:
            with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                names = {name.strip().lower() for name in reader.fieldnames if name}
                if "symbol" not in names or "sector" not in names:
                    continue

                symbol_key = next(name for name in reader.fieldnames if name and name.strip().lower() == "symbol")
                sector_key = next(name for name in reader.fieldnames if name and name.strip().lower() == "sector")
                sector_by_symbol: dict[str, str] = {}
                for row in reader:
                    sym = (row.get(symbol_key) or "").strip()
                    sec = (row.get(sector_key) or "").strip()
                    if sym and sec:
                        sector_by_symbol[sym] = sec
                if sector_by_symbol:
                    return sector_by_symbol, f"sector source: {csv_path}"
        except OSError:
            continue

    return {}, "sector unavailable"


def _episode_rows(
    holding_rows: list[sqlite3.Row],
    feature_map: dict[tuple[str, str], sqlite3.Row],
    stop_events: dict[tuple[str, str], int],
) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[sqlite3.Row]] = {}
    for row in holding_rows:
        by_symbol.setdefault(str(row["symbol"]), []).append(row)

    episodes: list[dict[str, Any]] = []
    for symbol, rows in by_symbol.items():
        rows = sorted(rows, key=lambda r: str(r["date"]))
        cur: list[sqlite3.Row] = []
        prev_date: str | None = None
        for row in rows:
            date = str(row["date"])
            continuous = bool(prev_date and (str(row["entry_date"] or "") == str(cur[0]["entry_date"] or "")))
            if prev_date is not None and (not continuous or date <= prev_date):
                if cur:
                    episodes.append(_build_episode(symbol, cur, feature_map, stop_events))
                cur = [row]
            else:
                cur.append(row)
            prev_date = date
        if cur:
            episodes.append(_build_episode(symbol, cur, feature_map, stop_events))

    episodes.sort(key=lambda r: (str(r["entry_date"]), str(r["symbol"])))
    return episodes


def _build_episode(
    symbol: str,
    rows: list[sqlite3.Row],
    feature_map: dict[tuple[str, str], sqlite3.Row],
    stop_events: dict[tuple[str, str], int],
) -> dict[str, Any]:
    rows = sorted(rows, key=lambda r: str(r["date"]))
    entry_row = rows[0]
    exit_row = rows[-1]

    entry_date = str(entry_row["date"])
    exit_date = str(exit_row["date"])
    entry_price = float(entry_row["entry_price"]) if entry_row["entry_price"] is not None else None
    exit_price = float(exit_row["close"]) if exit_row["close"] is not None else None

    episode_return = None
    if entry_price is not None and exit_price is not None and entry_price > 0:
        episode_return = (exit_price - entry_price) / entry_price

    avg_weight = sum(float(r["weight"]) for r in rows) / len(rows)
    est_contrib = avg_weight * episode_return if episode_return is not None else None

    rel_returns: list[float] = []
    if entry_price is not None and entry_price > 0:
        for r in rows:
            c = r["close"]
            if c is not None:
                rel_returns.append((float(c) - entry_price) / entry_price)

    mae = min(rel_returns) if rel_returns else None
    mfe = max(rel_returns) if rel_returns else None

    feature_row = feature_map.get((symbol, entry_date))
    feature_values = {k: (float(feature_row[k]) if feature_row and feature_row[k] is not None else None) for k in FEATURE_COLUMNS}

    range_to_ret5 = _safe_ratio(feature_values["range_pct"], feature_values["ret_5d"])
    vol_to_mom20 = _safe_ratio(feature_values["volatility_20d"], feature_values["momentum_20d"])
    ret5_minus_range = (
        feature_values["ret_5d"] - feature_values["range_pct"]
        if feature_values["ret_5d"] is not None and feature_values["range_pct"] is not None
        else None
    )

    stop_loss_count = 0
    for r in rows:
        stop_loss_count += stop_events.get((symbol, str(r["date"])), 0)

    return {
        "symbol": symbol,
        "entry_date": entry_date,
        "exit_date": exit_date,
        "holding_days": len(rows),
        "entry_rank": entry_row["rank"],
        "entry_score": entry_row["score"],
        "entry_price": entry_price,
        "exit_price": exit_price,
        "episode_return": episode_return,
        "avg_weight": avg_weight,
        "estimated_contribution": est_contrib,
        "stop_loss_count": stop_loss_count,
        "stopped": int(stop_loss_count > 0),
        "max_adverse_excursion": mae,
        "max_favorable_excursion": mfe,
        **feature_values,
        "range_to_ret5": range_to_ret5,
        "volatility_to_momentum20": vol_to_mom20,
        "ret5_minus_range": ret5_minus_range,
    }


def _build_stop_event_index(events: list[sqlite3.Row]) -> dict[tuple[str, str], int]:
    out: dict[tuple[str, str], int] = {}
    for e in events:
        event_type = str(e["event_type"] or "")
        if "stop_loss" not in event_type:
            continue
        symbol = e["symbol"]
        if symbol is None:
            continue
        key = (str(symbol), str(e["date"]))
        out[key] = out.get(key, 0) + 1
    return out


def _mean(values: list[float]) -> float | None:
    return (sum(values) / len(values)) if values else None


def _summarize_features(episodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = {
        "gain": [e for e in episodes if e["episode_return"] is not None and float(e["episode_return"]) > 0],
        "loss": [e for e in episodes if e["episode_return"] is not None and float(e["episode_return"]) <= 0],
        "stopped": [e for e in episodes if int(e["stopped"]) == 1],
    }
    features = FEATURE_COLUMNS + RISK_RATIO_COLUMNS + ["episode_return", "estimated_contribution", "holding_days"]
    rows: list[dict[str, Any]] = []
    for feature in features:
        row: dict[str, Any] = {"feature": feature}
        for label, grp in groups.items():
            vals = [float(e[feature]) for e in grp if e.get(feature) is not None]
            row[f"{label}_count"] = len(vals)
            row[f"{label}_mean"] = _mean(vals)
            row[f"{label}_median"] = median(vals) if vals else None
        rows.append(row)
    return rows


def _stop_loss_after_returns(
    events: list[sqlite3.Row],
    price_map: dict[str, list[sqlite3.Row]],
) -> list[dict[str, Any]]:
    horizons = [5, 10, 20]
    rows: list[dict[str, Any]] = []
    for e in events:
        event_type = str(e["event_type"] or "")
        symbol = e["symbol"]
        if symbol is None or "stop_loss" not in event_type:
            continue
        sym = str(symbol)
        event_date = str(e["date"])
        series = price_map.get(sym, [])
        idx = next((i for i, r in enumerate(series) if str(r["date"]) == event_date), None)
        if idx is None:
            continue
        base_close = float(series[idx]["close"]) if series[idx]["close"] is not None else None
        if base_close is None or base_close <= 0:
            continue

        row: dict[str, Any] = {"symbol": sym, "event_date": event_date, "event_type": event_type}
        for h in horizons:
            target_idx = idx + h
            value = None
            if target_idx < len(series):
                target_close = series[target_idx]["close"]
                if target_close is not None:
                    value = (float(target_close) - base_close) / base_close
            row[f"ret_{h}d_after_stop"] = value
        rows.append(row)
    return rows


def _sector_attribution(episodes: list[dict[str, Any]], sector_by_symbol: dict[str, str]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for e in episodes:
        sec = sector_by_symbol.get(str(e["symbol"]), "UNKNOWN")
        grouped.setdefault(sec, []).append(e)

    out: list[dict[str, Any]] = []
    for sector, group in sorted(grouped.items()):
        ep_returns = [float(r["episode_return"]) for r in group if r["episode_return"] is not None]
        contribs = [float(r["estimated_contribution"]) for r in group if r["estimated_contribution"] is not None]
        out.append(
            {
                "sector": sector,
                "episode_count": len(group),
                "avg_episode_return": _mean(ep_returns),
                "median_episode_return": median(ep_returns) if ep_returns else None,
                "sum_estimated_contribution": sum(contribs) if contribs else None,
                "stopped_episode_count": sum(int(r["stopped"]) for r in group),
            }
        )
    return out


def _load_run_row(conn: sqlite3.Connection, run_id: str) -> sqlite3.Row:
    run = conn.execute("SELECT * FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone()
    if run is None:
        raise ValueError(f"run_id not found: {run_id}")
    return run


def main() -> None:
    parser = argparse.ArgumentParser(description="Trade/Episode attribution analyzer")
    parser.add_argument("--db", required=True, help="SQLite DB path")
    parser.add_argument("--run-id", required=True, help="Backtest run_id")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    run_row = _load_run_row(conn, args.run_id)
    holdings = _load_holding_rows(conn, args.run_id)
    events = _load_risk_events(conn, args.run_id)
    feature_map = _load_feature_map(conn)
    stop_event_index = _build_stop_event_index(events)
    episodes = _episode_rows(holdings, feature_map, stop_event_index)

    feature_summary = _summarize_features(episodes)
    price_map = _load_price_map(conn)
    stop_after_rows = _stop_loss_after_returns(events, price_map)

    sector_by_symbol, sector_note = _find_universe_sector_map()
    sector_rows: list[dict[str, Any]] = []
    if sector_by_symbol:
        sector_rows = _sector_attribution(episodes, sector_by_symbol)

    episode_path = out_dir / f"trade_episode_attribution_{args.run_id}.csv"
    feature_path = out_dir / f"trade_feature_summary_{args.run_id}.csv"
    stop_path = out_dir / f"stop_loss_after_return_{args.run_id}.csv"
    report_path = out_dir / f"trade_attribution_report_{args.run_id}.md"

    _write_csv(
        episode_path,
        episodes,
        [
            "symbol",
            "entry_date",
            "exit_date",
            "holding_days",
            "entry_rank",
            "entry_score",
            "entry_price",
            "exit_price",
            "episode_return",
            "avg_weight",
            "estimated_contribution",
            "stop_loss_count",
            "stopped",
            "max_adverse_excursion",
            "max_favorable_excursion",
            *FEATURE_COLUMNS,
            *RISK_RATIO_COLUMNS,
        ],
    )
    _write_csv(feature_path, feature_summary, list(feature_summary[0].keys()) if feature_summary else ["feature"])
    _write_csv(
        stop_path,
        stop_after_rows,
        ["symbol", "event_date", "event_type", "ret_5d_after_stop", "ret_10d_after_stop", "ret_20d_after_stop"],
    )

    stop_ret_5 = [float(r["ret_5d_after_stop"]) for r in stop_after_rows if r["ret_5d_after_stop"] is not None]
    stop_ret_10 = [float(r["ret_10d_after_stop"]) for r in stop_after_rows if r["ret_10d_after_stop"] is not None]
    stop_ret_20 = [float(r["ret_20d_after_stop"]) for r in stop_after_rows if r["ret_20d_after_stop"] is not None]

    with report_path.open("w", encoding="utf-8") as f:
        f.write(f"# Trade Attribution Report ({args.run_id})\n\n")
        f.write("## Run Configuration\n")
        f.write(f"- top_n: {run_row['top_n']}\n")
        f.write(f"- rebalance_frequency: {run_row['rebalance_frequency']}\n")
        f.write(f"- min_holding_days: {run_row['min_holding_days']}\n")
        f.write(f"- keep_rank_threshold: {run_row['keep_rank_threshold']}\n")
        f.write(f"- scoring_profile: {run_row['scoring_profile']}\n")
        f.write(f"- position_stop_loss_pct: {run_row['position_stop_loss_pct']}\n")
        f.write(f"- stop_loss_cash_mode: {run_row['stop_loss_cash_mode']}\n\n")

        f.write("## Episode Summary\n")
        f.write(f"- total_episodes: {len(episodes)}\n")
        f.write(
            f"- loss_episodes: {sum(1 for e in episodes if e['episode_return'] is not None and float(e['episode_return']) <= 0)}\n"
        )
        f.write(
            f"- stopped_episodes: {sum(1 for e in episodes if int(e['stopped']) == 1)}\n"
        )
        avg_ep = _mean([float(e["episode_return"]) for e in episodes if e["episode_return"] is not None])
        f.write(f"- average_episode_return: {avg_ep if avg_ep is not None else 'N/A'}\n\n")

        f.write("## Stop Loss After Returns\n")
        f.write(f"- stop_event_count: {len(stop_after_rows)}\n")
        f.write(f"- avg_ret_5d_after_stop: {_mean(stop_ret_5)}\n")
        f.write(f"- avg_ret_10d_after_stop: {_mean(stop_ret_10)}\n")
        f.write(f"- avg_ret_20d_after_stop: {_mean(stop_ret_20)}\n\n")

        f.write("## Feature Diagnostic (gain vs loss vs stopped)\n")
        f.write("- See CSV: trade_feature_summary_<run_id>.csv\n\n")

        f.write("## Sector Attribution\n")
        if not sector_rows:
            f.write(f"- {sector_note}\n")
        else:
            f.write(f"- {sector_note}\n")
            f.write("\n| sector | episode_count | avg_episode_return | median_episode_return | sum_estimated_contribution | stopped_episode_count |\n")
            f.write("|---|---:|---:|---:|---:|---:|\n")
            for row in sector_rows:
                f.write(
                    "| {sector} | {episode_count} | {avg_episode_return} | {median_episode_return} | {sum_estimated_contribution} | {stopped_episode_count} |\n".format(
                        **row
                    )
                )


if __name__ == "__main__":
    main()
