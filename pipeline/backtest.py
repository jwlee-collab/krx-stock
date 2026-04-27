from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone


def _is_week_boundary(date_str: str, prev_date: str | None) -> bool:
    if prev_date is None:
        return True
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    p = datetime.strptime(prev_date, "%Y-%m-%d").date()
    return d.isocalendar()[:2] != p.isocalendar()[:2]


def _build_target_holdings(
    ranked_symbols: list[str],
    rank_by_symbol: dict[str, int],
    current_symbols: set[str],
    entry_index_by_symbol: dict[str, int],
    current_day_index: int,
    top_n: int,
    min_holding_days: int,
    keep_rank_threshold: int,
) -> set[str]:
    keep_due_rank = {
        sym
        for sym in current_symbols
        if rank_by_symbol.get(sym, 10**9) <= keep_rank_threshold
    }
    keep_due_holding_period = {
        sym
        for sym in current_symbols
        if (current_day_index - entry_index_by_symbol.get(sym, current_day_index)) < min_holding_days
    }
    kept = set(keep_due_rank) | set(keep_due_holding_period)

    target = list(kept)
    for sym in ranked_symbols:
        if sym in kept:
            continue
        if len(target) >= top_n:
            break
        target.append(sym)
    return set(target)


def _build_proxy_market_close_by_date(conn: sqlite3.Connection, dates: list[str]) -> dict[str, float]:
    rows = conn.execute(
        """
        SELECT date, AVG(close) AS market_close
        FROM daily_prices
        WHERE date BETWEEN ? AND ?
        GROUP BY date
        ORDER BY date
        """,
        (dates[0], dates[-1]),
    ).fetchall()
    return {r["date"]: float(r["market_close"]) for r in rows if r["market_close"] is not None}


def _build_market_regime_by_date(
    conn: sqlite3.Connection,
    dates: list[str],
) -> dict[str, dict[str, float | bool | None]]:
    if not dates:
        return {}

    close_by_date = _build_proxy_market_close_by_date(conn, dates)
    ordered_dates = [d for d in dates if d in close_by_date]
    closes = [close_by_date[d] for d in ordered_dates]

    regime_by_date: dict[str, dict[str, float | bool | None]] = {}
    for idx, d in enumerate(ordered_dates):
        ma20 = (sum(closes[idx - 19 : idx + 1]) / 20.0) if idx >= 19 else None
        ma60 = (sum(closes[idx - 59 : idx + 1]) / 60.0) if idx >= 59 else None
        c = closes[idx]
        regime_by_date[d] = {
            "market_proxy_value": c,
            "market_proxy_ma20": ma20,
            "market_proxy_ma60": ma60,
            "below_ma20": (ma20 is not None and c < ma20),
            "below_ma60": (ma60 is not None and c < ma60),
        }

    return regime_by_date


def run_backtest(
    conn: sqlite3.Connection,
    top_n: int = 5,
    start_date: str | None = None,
    end_date: str | None = None,
    initial_equity: float = 100000.0,
    rebalance_frequency: str = "daily",
    min_holding_days: int = 0,
    keep_rank_threshold: int | None = None,
    scoring_profile: str = "improved_v1",
    market_filter_enabled: bool = False,
    market_filter_ma20_reduce_by: int = 1,
    market_filter_ma60_mode: str = "block_new_buys",
) -> str:
    """Equal-weight long backtest using daily_scores and next-day close returns."""
    if rebalance_frequency not in {"daily", "weekly"}:
        raise ValueError("rebalance_frequency must be one of: daily, weekly")
    if market_filter_ma60_mode not in {"none", "block_new_buys", "cash"}:
        raise ValueError("market_filter_ma60_mode must be one of: none, block_new_buys, cash")

    if keep_rank_threshold is None:
        keep_rank_threshold = top_n

    run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    all_dates = [
        r["date"]
        for r in conn.execute(
            "SELECT DISTINCT date FROM daily_prices ORDER BY date"
        ).fetchall()
    ]

    if start_date:
        all_dates = [d for d in all_dates if d >= start_date]
    if end_date:
        all_dates = [d for d in all_dates if d <= end_date]

    if len(all_dates) < 2:
        raise ValueError("Need at least 2 dates for backtest")

    regime_by_date = _build_market_regime_by_date(conn, all_dates)

    conn.execute(
        """
        INSERT INTO backtest_runs(
            run_id,created_at,top_n,start_date,end_date,initial_equity,
            rebalance_frequency,min_holding_days,keep_rank_threshold,scoring_profile,
            market_filter_enabled,market_filter_ma20_reduce_by,market_filter_ma60_mode,
            ma20_trigger_count,ma60_trigger_count,reduced_target_count_days,blocked_new_buy_days,cash_mode_days
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            run_id,
            created_at,
            top_n,
            start_date,
            end_date,
            float(initial_equity),
            rebalance_frequency,
            int(min_holding_days),
            int(keep_rank_threshold),
            scoring_profile,
            int(bool(market_filter_enabled)),
            int(max(0, market_filter_ma20_reduce_by)),
            market_filter_ma60_mode,
            0,
            0,
            0,
            0,
            0,
        ),
    )

    equity = initial_equity
    result_rows: list[tuple] = []

    current_holdings: set[str] = set()
    entry_index_by_symbol: dict[str, int] = {}
    market_filter_event_rows: list[tuple] = []
    ma20_trigger_count = 0
    ma60_trigger_count = 0
    reduced_target_count_days = 0
    blocked_new_buy_days = 0
    cash_mode_days = 0

    for i in range(len(all_dates) - 1):
        d0 = all_dates[i]
        d1 = all_dates[i + 1]
        prev_d0 = all_dates[i - 1] if i > 0 else None

        should_rebalance = (
            rebalance_frequency == "daily"
            or (rebalance_frequency == "weekly" and _is_week_boundary(d0, prev_d0))
        )

        if should_rebalance:
            ranked_rows = conn.execute(
                "SELECT symbol, rank FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC",
                (d0,),
            ).fetchall()
            ranked_symbols = [r["symbol"] for r in ranked_rows]
            rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked_rows}

            effective_top_n = int(top_n)
            block_new_buys = False
            action = "none"
            regime = {
                "market_proxy_value": None,
                "market_proxy_ma20": None,
                "market_proxy_ma60": None,
                "below_ma20": False,
                "below_ma60": False,
            }
            if market_filter_enabled:
                regime = regime_by_date.get(d0, regime)
                if regime["below_ma20"]:
                    effective_top_n = max(0, effective_top_n - int(max(0, market_filter_ma20_reduce_by)))
                    ma20_trigger_count += 1
                    reduced_target_count_days += 1
                    action = "reduce_holdings"
                if regime["below_ma60"]:
                    ma60_trigger_count += 1
                    if market_filter_ma60_mode == "cash":
                        effective_top_n = 0
                        cash_mode_days += 1
                        action = "cash"
                    elif market_filter_ma60_mode == "block_new_buys":
                        block_new_buys = True
                        blocked_new_buy_days += 1
                        action = "block_new_buys"

            target_holdings = _build_target_holdings(
                ranked_symbols=ranked_symbols,
                rank_by_symbol=rank_by_symbol,
                current_symbols=current_holdings,
                entry_index_by_symbol=entry_index_by_symbol,
                current_day_index=i,
                top_n=effective_top_n,
                min_holding_days=int(min_holding_days),
                keep_rank_threshold=int(keep_rank_threshold),
            )
            if block_new_buys:
                target_holdings = target_holdings & current_holdings

            if market_filter_enabled and (regime["below_ma20"] or regime["below_ma60"]):
                market_filter_event_rows.append(
                    (
                        run_id,
                        d0,
                        regime["market_proxy_value"],
                        regime["market_proxy_ma20"],
                        regime["market_proxy_ma60"],
                        int(bool(regime["below_ma20"])),
                        int(bool(regime["below_ma60"])),
                        int(top_n),
                        len(target_holdings),
                        market_filter_ma60_mode,
                        action,
                    )
                )

            for sym in (target_holdings - current_holdings):
                entry_index_by_symbol[sym] = i
            for sym in (current_holdings - target_holdings):
                entry_index_by_symbol.pop(sym, None)
            current_holdings = target_holdings

        if not current_holdings:
            daily_ret = 0.0
            pos_count = 0
        else:
            returns = []
            for sym in sorted(current_holdings):
                row0 = conn.execute(
                    "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d0)
                ).fetchone()
                row1 = conn.execute(
                    "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d1)
                ).fetchone()
                if row0 and row1 and row0["close"]:
                    returns.append((row1["close"] - row0["close"]) / row0["close"])
            daily_ret = (sum(returns) / len(returns)) if returns else 0.0
            pos_count = len(returns)

        equity *= 1.0 + daily_ret
        result_rows.append((run_id, d1, equity, daily_ret, pos_count))

    conn.executemany(
        """
        INSERT INTO backtest_results(run_id,date,equity,daily_return,position_count)
        VALUES(?,?,?,?,?)
        """,
        result_rows,
    )
    if market_filter_event_rows:
        conn.executemany(
            """
            INSERT INTO backtest_market_filter_events(
                run_id,date,market_proxy_value,market_proxy_ma20,market_proxy_ma60,
                below_ma20,below_ma60,original_target_count,adjusted_target_count,ma60_mode,action
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            market_filter_event_rows,
        )
    conn.execute(
        """
        UPDATE backtest_runs
        SET ma20_trigger_count=?,
            ma60_trigger_count=?,
            reduced_target_count_days=?,
            blocked_new_buy_days=?,
            cash_mode_days=?
        WHERE run_id=?
        """,
        (
            int(ma20_trigger_count),
            int(ma60_trigger_count),
            int(reduced_target_count_days),
            int(blocked_new_buy_days),
            int(cash_mode_days),
            run_id,
        ),
    )
    conn.commit()
    return run_id
