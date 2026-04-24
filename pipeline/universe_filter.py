from __future__ import annotations

import sqlite3
from dataclasses import dataclass


@dataclass(frozen=True)
class UniverseFilterConfig:
    min_close_price: float = 3000.0
    min_avg_dollar_volume_20d: float = 1_000_000_000.0
    min_avg_volume_20d: float = 100_000.0
    min_data_days_60d: int = 60
    shock_lookback_days: int = 20
    shock_abs_return_threshold: float = 0.18
    shock_max_hits: int = 1


def _latest_date(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT MAX(date) AS d FROM daily_prices").fetchone()
    return row["d"] if row else None


def filter_universe(conn: sqlite3.Connection, config: UniverseFilterConfig) -> dict:
    """Filter symbols using latest snapshot and recent history.

    Returns:
        {
          "as_of_date": "YYYY-MM-DD",
          "before_count": int,
          "after_count": int,
          "selected_symbols": list[str],
          "removed_count": int,
          "removed_by_reason": {reason: count}
        }
    """
    as_of_date = _latest_date(conn)
    if not as_of_date:
        return {
            "as_of_date": None,
            "before_count": 0,
            "after_count": 0,
            "selected_symbols": [],
            "removed_count": 0,
            "removed_by_reason": {},
        }

    base_rows = conn.execute(
        """
        SELECT symbol, close
        FROM daily_prices
        WHERE date = ?
        """,
        (as_of_date,),
    ).fetchall()
    before_symbols = {r["symbol"] for r in base_rows}
    close_by_symbol = {r["symbol"]: float(r["close"]) for r in base_rows}

    dates_20 = [
        r["date"]
        for r in conn.execute(
            "SELECT DISTINCT date FROM daily_prices WHERE date <= ? ORDER BY date DESC LIMIT 20",
            (as_of_date,),
        ).fetchall()
    ]
    dates_60 = [
        r["date"]
        for r in conn.execute(
            "SELECT DISTINCT date FROM daily_prices WHERE date <= ? ORDER BY date DESC LIMIT 60",
            (as_of_date,),
        ).fetchall()
    ]

    avg20_by_symbol = {
        r["symbol"]: (float(r["avg_volume"]), float(r["avg_dollar_volume"]))
        for r in conn.execute(
            f"""
            SELECT symbol,
                   AVG(volume) AS avg_volume,
                   AVG(close * volume) AS avg_dollar_volume
            FROM daily_prices
            WHERE date IN ({','.join('?' for _ in dates_20)})
            GROUP BY symbol
            """,
            dates_20,
        ).fetchall()
    } if dates_20 else {}

    cnt60_by_symbol = {
        r["symbol"]: int(r["cnt"])
        for r in conn.execute(
            f"""
            SELECT symbol, COUNT(*) AS cnt
            FROM daily_prices
            WHERE date IN ({','.join('?' for _ in dates_60)})
            GROUP BY symbol
            """,
            dates_60,
        ).fetchall()
    } if dates_60 else {}

    shock_rows = conn.execute(
        """
        SELECT symbol, date, close
        FROM daily_prices
        WHERE date <= ?
        ORDER BY symbol, date
        """,
        (as_of_date,),
    ).fetchall()
    closes_by_symbol: dict[str, list[tuple[str, float]]] = {}
    for r in shock_rows:
        closes_by_symbol.setdefault(r["symbol"], []).append((r["date"], float(r["close"])))

    shock_hits_by_symbol: dict[str, int] = {}
    for symbol, hist in closes_by_symbol.items():
        if len(hist) < 2:
            shock_hits_by_symbol[symbol] = 0
            continue
        rets = []
        for i in range(1, len(hist)):
            prev = hist[i - 1][1]
            cur = hist[i][1]
            if prev == 0:
                continue
            rets.append(abs((cur - prev) / prev))
        lookback = rets[-config.shock_lookback_days :] if config.shock_lookback_days > 0 else rets
        shock_hits_by_symbol[symbol] = sum(1 for x in lookback if x >= config.shock_abs_return_threshold)

    removed_by_reason = {
        "low_close_price": set(),
        "low_avg_dollar_volume_20d": set(),
        "low_avg_volume_20d": set(),
        "insufficient_data_60d": set(),
        "too_many_shock_moves": set(),
    }

    selected = set(before_symbols)
    for symbol in before_symbols:
        close_px = close_by_symbol.get(symbol)
        if close_px is None or close_px < config.min_close_price:
            removed_by_reason["low_close_price"].add(symbol)

        avg_volume, avg_dollar = avg20_by_symbol.get(symbol, (0.0, 0.0))
        if avg_dollar < config.min_avg_dollar_volume_20d:
            removed_by_reason["low_avg_dollar_volume_20d"].add(symbol)
        if avg_volume < config.min_avg_volume_20d:
            removed_by_reason["low_avg_volume_20d"].add(symbol)

        cnt60 = cnt60_by_symbol.get(symbol, 0)
        if cnt60 < config.min_data_days_60d:
            removed_by_reason["insufficient_data_60d"].add(symbol)

        shock_hits = shock_hits_by_symbol.get(symbol, 0)
        if shock_hits > config.shock_max_hits:
            removed_by_reason["too_many_shock_moves"].add(symbol)

    removed_union = set().union(*removed_by_reason.values())
    selected -= removed_union

    return {
        "as_of_date": as_of_date,
        "before_count": len(before_symbols),
        "after_count": len(selected),
        "selected_symbols": sorted(selected),
        "removed_count": len(removed_union),
        "removed_by_reason": {k: len(v) for k, v in removed_by_reason.items()},
    }

