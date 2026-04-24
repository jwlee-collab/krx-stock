from __future__ import annotations

import sqlite3


def _safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b in (None, 0):
        return None
    return a / b


def generate_daily_features(conn: sqlite3.Connection, start_date: str | None = None, end_date: str | None = None) -> int:
    """Generate feature rows using OHLCV history and store in daily_features."""
    where = []
    params: list[str] = []
    if start_date:
        where.append("date >= ?")
        params.append(start_date)
    if end_date:
        where.append("date <= ?")
        params.append(end_date)
    predicate = f"WHERE {' AND '.join(where)}" if where else ""

    rows = conn.execute(
        f"""
        SELECT symbol, date, open, high, low, close, volume
        FROM daily_prices
        {predicate}
        ORDER BY symbol, date
        """,
        params,
    ).fetchall()

    by_symbol: dict[str, list[sqlite3.Row]] = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    feature_rows: list[tuple] = []
    for symbol, hist in by_symbol.items():
        closes = [r["close"] for r in hist]
        volumes = [r["volume"] for r in hist]
        for i, r in enumerate(hist):
            ret_1d = _safe_div(closes[i] - closes[i - 1], closes[i - 1]) if i >= 1 else None
            ret_5d = _safe_div(closes[i] - closes[i - 5], closes[i - 5]) if i >= 5 else None
            momentum_20d = _safe_div(closes[i] - closes[i - 20], closes[i - 20]) if i >= 20 else None
            momentum_60d = _safe_div(closes[i] - closes[i - 60], closes[i - 60]) if i >= 60 else None
            sma20 = (sum(closes[i - 19 : i + 1]) / 20.0) if i >= 19 else None
            sma60 = (sum(closes[i - 59 : i + 1]) / 60.0) if i >= 59 else None
            sma_20_gap = _safe_div(closes[i], sma20) - 1.0 if sma20 else None
            sma_60_gap = _safe_div(closes[i], sma60) - 1.0 if sma60 else None
            range_pct = _safe_div(r["high"] - r["low"], r["close"])
            volatility_20d = None
            if i >= 20:
                ret_window: list[float] = []
                for j in range(i - 19, i + 1):
                    if closes[j - 1]:
                        ret_window.append((closes[j] - closes[j - 1]) / closes[j - 1])
                if len(ret_window) >= 2:
                    mean_r = sum(ret_window) / len(ret_window)
                    var_r = sum((x - mean_r) ** 2 for x in ret_window) / len(ret_window)
                    volatility_20d = var_r**0.5

            if i >= 19:
                window = volumes[i - 19 : i + 1]
                mean = sum(window) / len(window)
                var = sum((x - mean) ** 2 for x in window) / len(window)
                std = var**0.5
                volume_z20 = (r["volume"] - mean) / std if std > 0 else 0.0
            else:
                volume_z20 = None

            feature_rows.append(
                (
                    symbol,
                    r["date"],
                    ret_1d,
                    ret_5d,
                    momentum_20d,
                    momentum_60d,
                    sma_20_gap,
                    sma_60_gap,
                    range_pct,
                    volatility_20d,
                    volume_z20,
                )
            )

    before = conn.total_changes
    conn.executemany(
        """
        INSERT INTO daily_features(
            symbol,date,ret_1d,ret_5d,momentum_20d,momentum_60d,sma_20_gap,sma_60_gap,range_pct,volatility_20d,volume_z20
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol,date) DO UPDATE SET
            ret_1d=excluded.ret_1d,
            ret_5d=excluded.ret_5d,
            momentum_20d=excluded.momentum_20d,
            momentum_60d=excluded.momentum_60d,
            sma_20_gap=excluded.sma_20_gap,
            sma_60_gap=excluded.sma_60_gap,
            range_pct=excluded.range_pct,
            volatility_20d=excluded.volatility_20d,
            volume_z20=excluded.volume_z20
        """,
        feature_rows,
    )
    conn.commit()
    return conn.total_changes - before
