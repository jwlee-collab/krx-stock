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
    ranked_rows: list[sqlite3.Row],
    rank_by_symbol: dict[str, int],
    current_symbols: set[str],
    entry_index_by_symbol: dict[str, int],
    current_day_index: int,
    top_n: int,
    min_holding_days: int,
    keep_rank_threshold: int,
    entry_gate_enabled: bool = False,
    min_entry_score: float = 0.0,
    require_positive_momentum20: bool = False,
    require_positive_momentum60: bool = False,
    require_above_sma20: bool = False,
    require_above_sma60: bool = False,
    blocked_new_entries: set[str] | None = None,
    enable_overheat_entry_gate: bool = False,
    enable_overheat_ret_1d_rule: bool = False,
    enable_overheat_ret_5d_rule: bool = False,
    enable_overheat_range_rule: bool = False,
    enable_overheat_volume_z20_rule: bool = False,
    max_entry_ret_1d: float = 0.08,
    max_entry_ret_5d: float = 0.15,
    max_entry_range_pct: float = 0.10,
    max_entry_volume_z20: float = 3.0,
    enable_volume_surge_overheat_rule: bool = False,
    volume_surge_threshold: float = 3.0,
    volume_surge_ret_5d_threshold: float = 0.10,
    enable_entry_quality_gate: bool = False,
    enable_entry_range_rule: bool = False,
    enable_entry_volatility_rule: bool = False,
    enable_entry_ret5_minus_range_rule: bool = False,
    enable_entry_range_to_ret5_rule: bool = False,
    enable_entry_volatility_to_momentum20_rule: bool = False,
    max_entry_quality_range_pct: float = 0.16,
    max_entry_volatility_20d: float = 0.06,
    min_entry_ret5_minus_range: float = 0.0,
    max_entry_range_to_ret5: float = 1.5,
    max_entry_volatility_to_momentum20: float = 0.5,
) -> tuple[set[str], dict[str, int]]:
    blocked_new_entries = blocked_new_entries or set()
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
    overheat_diag = {
        "overheat_rejected_count": 0,
        "overheat_rejected_by_ret_1d": 0,
        "overheat_rejected_by_ret_5d": 0,
        "overheat_rejected_by_range_pct": 0,
        "overheat_rejected_by_volume_z20": 0,
        "overheat_rejected_by_volume_surge_rule": 0,
        "entry_quality_rejected_count": 0,
        "rejected_by_range_pct": 0,
        "rejected_by_volatility_20d": 0,
        "rejected_by_ret5_minus_range": 0,
        "rejected_by_range_to_ret5": 0,
        "rejected_by_volatility_to_momentum20": 0,
    }
    for row in ranked_rows:
        sym = row["symbol"]
        if sym in kept:
            continue
        if sym in blocked_new_entries:
            continue
        if len(target) >= top_n:
            break
        if entry_gate_enabled:
            score = float(row["score"]) if row["score"] is not None else 0.0
            m20 = row["momentum_20d"]
            m60 = row["momentum_60d"]
            g20 = row["sma_20_gap"]
            g60 = row["sma_60_gap"]
            if score < float(min_entry_score):
                continue
            if require_positive_momentum20 and (m20 is None or float(m20) <= 0.0):
                continue
            if require_positive_momentum60 and (m60 is None or float(m60) <= 0.0):
                continue
            if require_above_sma20 and (g20 is None or float(g20) <= 0.0):
                continue
            if require_above_sma60 and (g60 is None or float(g60) <= 0.0):
                continue
        if enable_overheat_entry_gate:
            ret_1d = row["ret_1d"]
            ret_5d = row["ret_5d"]
            range_pct = row["range_pct"]
            volume_z20 = row["volume_z20"]
            rejected = False
            if enable_overheat_ret_1d_rule and ret_1d is not None and float(ret_1d) > float(max_entry_ret_1d):
                rejected = True
                overheat_diag["overheat_rejected_by_ret_1d"] += 1
            if enable_overheat_ret_5d_rule and ret_5d is not None and float(ret_5d) > float(max_entry_ret_5d):
                rejected = True
                overheat_diag["overheat_rejected_by_ret_5d"] += 1
            if enable_overheat_range_rule and range_pct is not None and float(range_pct) > float(max_entry_range_pct):
                rejected = True
                overheat_diag["overheat_rejected_by_range_pct"] += 1
            if enable_overheat_volume_z20_rule and volume_z20 is not None and float(volume_z20) > float(max_entry_volume_z20):
                rejected = True
                overheat_diag["overheat_rejected_by_volume_z20"] += 1
            if (
                enable_volume_surge_overheat_rule
                and volume_z20 is not None
                and ret_5d is not None
                and float(volume_z20) > float(volume_surge_threshold)
                and float(ret_5d) > float(volume_surge_ret_5d_threshold)
            ):
                rejected = True
                overheat_diag["overheat_rejected_by_volume_surge_rule"] += 1
            if rejected:
                overheat_diag["overheat_rejected_count"] += 1
                continue
        if enable_entry_quality_gate:
            ret_5d = row["ret_5d"]
            range_pct = row["range_pct"]
            volatility_20d = row["volatility_20d"]
            momentum_20d = row["momentum_20d"]
            ret_5d_val = float(ret_5d) if ret_5d is not None else None
            range_pct_val = float(range_pct) if range_pct is not None else None
            volatility_20d_val = float(volatility_20d) if volatility_20d is not None else None
            momentum_20d_val = float(momentum_20d) if momentum_20d is not None else None
            ret5_minus_range = (
                (ret_5d_val - range_pct_val)
                if ret_5d_val is not None and range_pct_val is not None
                else None
            )
            range_to_ret5 = (
                (range_pct_val / abs(ret_5d_val))
                if range_pct_val is not None and ret_5d_val is not None and abs(ret_5d_val) > 0.0
                else None
            )
            volatility_to_momentum20 = (
                (volatility_20d_val / abs(momentum_20d_val))
                if volatility_20d_val is not None and momentum_20d_val is not None and abs(momentum_20d_val) > 0.0
                else None
            )
            quality_rejected = False
            if (
                enable_entry_range_rule
                and (
                    range_pct_val is None
                    or range_pct_val > float(max_entry_quality_range_pct)
                )
            ):
                quality_rejected = True
                overheat_diag["rejected_by_range_pct"] += 1
            if (
                enable_entry_volatility_rule
                and (
                    volatility_20d_val is None
                    or volatility_20d_val > float(max_entry_volatility_20d)
                )
            ):
                quality_rejected = True
                overheat_diag["rejected_by_volatility_20d"] += 1
            if (
                enable_entry_ret5_minus_range_rule
                and (
                    ret5_minus_range is None
                    or ret5_minus_range < float(min_entry_ret5_minus_range)
                )
            ):
                quality_rejected = True
                overheat_diag["rejected_by_ret5_minus_range"] += 1
            if (
                enable_entry_range_to_ret5_rule
                and (
                    range_to_ret5 is None
                    or range_to_ret5 > float(max_entry_range_to_ret5)
                )
            ):
                quality_rejected = True
                overheat_diag["rejected_by_range_to_ret5"] += 1
            if (
                enable_entry_volatility_to_momentum20_rule
                and (
                    volatility_to_momentum20 is None
                    or volatility_to_momentum20 > float(max_entry_volatility_to_momentum20)
                )
            ):
                quality_rejected = True
                overheat_diag["rejected_by_volatility_to_momentum20"] += 1
            if quality_rejected:
                overheat_diag["entry_quality_rejected_count"] += 1
                continue
        target.append(sym)
    return set(target), overheat_diag


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
    entry_gate_enabled: bool = False,
    min_entry_score: float = 0.0,
    require_positive_momentum20: bool = False,
    require_positive_momentum60: bool = False,
    require_above_sma20: bool = False,
    require_above_sma60: bool = False,
    enable_position_stop_loss: bool = False,
    position_stop_loss_pct: float = 0.08,
    enable_trailing_stop: bool = False,
    trailing_stop_pct: float = 0.10,
    enable_portfolio_dd_cut: bool = False,
    portfolio_dd_cut_pct: float = 0.10,
    portfolio_dd_cooldown_days: int = 20,
    stop_loss_cash_mode: str = "rebalance_remaining",
    stop_loss_cooldown_days: int = 0,
    enable_overheat_entry_gate: bool = False,
    enable_overheat_ret_1d_rule: bool = False,
    enable_overheat_ret_5d_rule: bool = False,
    enable_overheat_range_rule: bool = False,
    enable_overheat_volume_z20_rule: bool = False,
    max_entry_ret_1d: float = 0.08,
    max_entry_ret_5d: float = 0.15,
    max_entry_range_pct: float = 0.10,
    max_entry_volume_z20: float = 3.0,
    enable_volume_surge_overheat_rule: bool = False,
    volume_surge_threshold: float = 3.0,
    volume_surge_ret_5d_threshold: float = 0.10,
    enable_entry_quality_gate: bool = False,
    enable_entry_range_rule: bool = False,
    enable_entry_volatility_rule: bool = False,
    enable_entry_ret5_minus_range_rule: bool = False,
    enable_entry_range_to_ret5_rule: bool = False,
    enable_entry_volatility_to_momentum20_rule: bool = False,
    max_entry_quality_range_pct: float = 0.16,
    max_entry_volatility_20d: float = 0.06,
    min_entry_ret5_minus_range: float = 0.0,
    max_entry_range_to_ret5: float = 1.5,
    max_entry_volatility_to_momentum20: float = 0.5,
) -> str:
    """Equal-weight long backtest using daily_scores and next-day close returns."""
    if rebalance_frequency not in {"daily", "weekly"}:
        raise ValueError("rebalance_frequency must be one of: daily, weekly")
    if market_filter_ma60_mode not in {"none", "block_new_buys", "cash"}:
        raise ValueError("market_filter_ma60_mode must be one of: none, block_new_buys, cash")
    if stop_loss_cash_mode not in {"rebalance_remaining", "keep_cash"}:
        raise ValueError("stop_loss_cash_mode must be one of: rebalance_remaining, keep_cash")
    if position_stop_loss_pct < 0.0 or trailing_stop_pct < 0.0 or portfolio_dd_cut_pct < 0.0:
        raise ValueError("risk cut percentages must be >= 0")

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

    initial_run_values = (
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
        int(bool(entry_gate_enabled)),
        float(min_entry_score),
        int(bool(require_positive_momentum20)),
        int(bool(require_positive_momentum60)),
        int(bool(require_above_sma20)),
        int(bool(require_above_sma60)),
        0,
        0,
        0.0,
        0,
        0,
        int(bool(enable_position_stop_loss)),
        float(position_stop_loss_pct),
        int(bool(enable_trailing_stop)),
        float(trailing_stop_pct),
        int(bool(enable_portfolio_dd_cut)),
        float(portfolio_dd_cut_pct),
        int(max(0, portfolio_dd_cooldown_days)),
        0,
        0,
        0,
        0,
        0,
        stop_loss_cash_mode,
        int(max(0, stop_loss_cooldown_days)),
        int(bool(enable_overheat_entry_gate)),
        int(bool(enable_overheat_entry_gate)),
        int(bool(enable_overheat_ret_1d_rule)),
        int(bool(enable_overheat_ret_5d_rule)),
        int(bool(enable_overheat_range_rule)),
        int(bool(enable_overheat_volume_z20_rule)),
        float(max_entry_ret_1d),
        float(max_entry_ret_5d),
        float(max_entry_range_pct),
        float(max_entry_volume_z20),
        int(bool(enable_volume_surge_overheat_rule)),
        int(bool(enable_volume_surge_overheat_rule)),
        float(volume_surge_threshold),
        float(volume_surge_ret_5d_threshold),
        int(bool(enable_entry_quality_gate)),
        int(bool(enable_entry_range_rule)),
        int(bool(enable_entry_volatility_rule)),
        int(bool(enable_entry_ret5_minus_range_rule)),
        int(bool(enable_entry_range_to_ret5_rule)),
        int(bool(enable_entry_volatility_to_momentum20_rule)),
        float(max_entry_quality_range_pct),
        float(max_entry_volatility_20d),
        float(min_entry_ret5_minus_range),
        float(max_entry_range_to_ret5),
        float(max_entry_volatility_to_momentum20),
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0.0,
        0.0,
        0.0,
        0.0,
    )
    conn.execute(
        f"""
        INSERT INTO backtest_runs(
            run_id,created_at,top_n,start_date,end_date,initial_equity,
            rebalance_frequency,min_holding_days,keep_rank_threshold,scoring_profile,
            market_filter_enabled,market_filter_ma20_reduce_by,market_filter_ma60_mode,
            ma20_trigger_count,ma60_trigger_count,reduced_target_count_days,blocked_new_buy_days,cash_mode_days,
            entry_gate_enabled,min_entry_score,require_positive_momentum20,require_positive_momentum60,
            require_above_sma20,require_above_sma60,entry_gate_rejected_count,entry_gate_cash_days,
            average_actual_position_count,min_actual_position_count,max_actual_position_count,
            enable_position_stop_loss,position_stop_loss_pct,enable_trailing_stop,trailing_stop_pct,
            enable_portfolio_dd_cut,portfolio_dd_cut_pct,portfolio_dd_cooldown_days,
            position_stop_loss_count,trailing_stop_count,portfolio_dd_cut_count,
            portfolio_dd_cooldown_days_count,risk_cut_cash_days,
            stop_loss_cash_mode,stop_loss_cooldown_days,
            enable_overheat_entry_gate,overheat_entry_gate_enabled,
            enable_overheat_ret_1d_rule,enable_overheat_ret_5d_rule,
            enable_overheat_range_rule,enable_overheat_volume_z20_rule,
            max_entry_ret_1d,max_entry_ret_5d,max_entry_range_pct,max_entry_volume_z20,
            enable_volume_surge_overheat_rule,volume_surge_rule_enabled,volume_surge_threshold,volume_surge_ret_5d_threshold,
            entry_quality_gate_enabled,
            enable_entry_range_rule,enable_entry_volatility_rule,enable_entry_ret5_minus_range_rule,
            enable_entry_range_to_ret5_rule,enable_entry_volatility_to_momentum20_rule,
            max_entry_quality_range_pct,max_entry_volatility_20d,min_entry_ret5_minus_range,
            max_entry_range_to_ret5,max_entry_volatility_to_momentum20,
            entry_quality_rejected_count,entry_quality_cash_days,
            rejected_by_range_pct,rejected_by_volatility_20d,rejected_by_ret5_minus_range,
            rejected_by_range_to_ret5,rejected_by_volatility_to_momentum20,
            overheat_rejected_count,overheat_cash_days,
            overheat_rejected_by_ret_1d,overheat_rejected_by_ret_5d,overheat_rejected_by_range_pct,
            overheat_rejected_by_volume_z20,overheat_rejected_by_volume_surge_rule,
            average_cash_weight,average_exposure,min_exposure,max_single_position_weight
        ) VALUES({",".join("?" for _ in initial_run_values)})
        """,
        initial_run_values,
    )

    equity = initial_equity
    result_rows: list[tuple] = []
    holdings_rows: list[tuple] = []

    current_holdings: set[str] = set()
    entry_index_by_symbol: dict[str, int] = {}
    market_filter_event_rows: list[tuple] = []
    ma20_trigger_count = 0
    ma60_trigger_count = 0
    reduced_target_count_days = 0
    blocked_new_buy_days = 0
    cash_mode_days = 0
    entry_gate_rejected_count = 0
    entry_gate_cash_days = 0
    overheat_rejected_count = 0
    overheat_cash_days = 0
    overheat_rejected_by_ret_1d = 0
    overheat_rejected_by_ret_5d = 0
    overheat_rejected_by_range_pct = 0
    overheat_rejected_by_volume_z20 = 0
    overheat_rejected_by_volume_surge_rule = 0
    entry_quality_rejected_count = 0
    entry_quality_cash_days = 0
    rejected_by_range_pct = 0
    rejected_by_volatility_20d = 0
    rejected_by_ret5_minus_range = 0
    rejected_by_range_to_ret5 = 0
    rejected_by_volatility_to_momentum20 = 0
    risk_event_rows: list[tuple] = []
    position_stop_loss_count = 0
    trailing_stop_count = 0
    portfolio_dd_cut_count = 0
    portfolio_dd_cooldown_days_count = 0
    risk_cut_cash_days = 0
    entry_price_by_symbol: dict[str, float] = {}
    peak_price_by_symbol: dict[str, float] = {}
    holding_weight_by_symbol: dict[str, float] = {}
    stop_loss_cooldown_until_idx_by_symbol: dict[str, int] = {}
    portfolio_peak_equity = float(initial_equity)
    dd_cooldown_until_idx = -1

    def _append_holdings_snapshot(snapshot_date: str) -> None:
        if not current_holdings:
            return
        score_rows = conn.execute(
            f"""
            SELECT symbol, rank, score
            FROM daily_scores
            WHERE date=? AND symbol IN ({",".join("?" for _ in current_holdings)})
            """,
            (snapshot_date, *sorted(current_holdings)),
        ).fetchall()
        score_by_symbol = {
            r["symbol"]: (
                int(r["rank"]) if r["rank"] is not None else None,
                float(r["score"]) if r["score"] is not None else None,
            )
            for r in score_rows
        }
        current_close_rows = conn.execute(
            "SELECT symbol, close FROM daily_prices WHERE date=?",
            (snapshot_date,),
        ).fetchall()
        close_by_symbol = {r["symbol"]: float(r["close"]) for r in current_close_rows if r["close"] is not None}
        for sym in sorted(current_holdings):
            weight = holding_weight_by_symbol.get(sym, 0.0)
            close_px = close_by_symbol.get(sym)
            entry_px = entry_price_by_symbol.get(sym)
            entry_idx = entry_index_by_symbol.get(sym)
            entry_date = all_dates[entry_idx] if entry_idx is not None and 0 <= entry_idx < len(all_dates) else None
            unrealized_return = None
            if close_px is not None and entry_px is not None and entry_px > 0:
                unrealized_return = (close_px - entry_px) / entry_px
            rank_score = score_by_symbol.get(sym, (None, None))
            holdings_rows.append(
                (
                    run_id,
                    snapshot_date,
                    sym,
                    weight,
                    entry_date,
                    entry_px,
                    close_px,
                    unrealized_return,
                    rank_score[0],
                    rank_score[1],
                )
            )

    for i in range(len(all_dates) - 1):
        d0 = all_dates[i]
        d1 = all_dates[i + 1]
        prev_d0 = all_dates[i - 1] if i > 0 else None
        new_entries_for_day: set[str] = set()
        removed_by_risk_for_day: set[str] = set()

        should_rebalance = (
            rebalance_frequency == "daily"
            or (rebalance_frequency == "weekly" and _is_week_boundary(d0, prev_d0))
        )

        if should_rebalance:
            ranked_rows = conn.execute(
                """
                SELECT s.symbol, s.rank, s.score,
                       f.momentum_20d, f.momentum_60d, f.sma_20_gap, f.sma_60_gap,
                       f.ret_1d, f.ret_5d, f.range_pct, f.volatility_20d, f.volume_z20
                FROM daily_scores s
                LEFT JOIN daily_features f
                  ON f.symbol = s.symbol AND f.date = s.date
                WHERE s.date=?
                ORDER BY s.rank ASC, s.symbol ASC
                """,
                (d0,),
            ).fetchall()
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
            if enable_portfolio_dd_cut and i <= dd_cooldown_until_idx:
                block_new_buys = True
                action = "risk_cut_block_new_buys"

            target_holdings, overheat_diag = _build_target_holdings(
                ranked_rows=ranked_rows,
                rank_by_symbol=rank_by_symbol,
                current_symbols=current_holdings,
                entry_index_by_symbol=entry_index_by_symbol,
                current_day_index=i,
                top_n=effective_top_n,
                min_holding_days=int(min_holding_days),
                keep_rank_threshold=int(keep_rank_threshold),
                entry_gate_enabled=entry_gate_enabled,
                min_entry_score=min_entry_score,
                require_positive_momentum20=require_positive_momentum20,
                require_positive_momentum60=require_positive_momentum60,
                require_above_sma20=require_above_sma20,
                require_above_sma60=require_above_sma60,
                blocked_new_entries={
                    sym
                    for sym, cooldown_until_idx in stop_loss_cooldown_until_idx_by_symbol.items()
                    if cooldown_until_idx >= i and sym not in current_holdings
                },
                enable_overheat_entry_gate=enable_overheat_entry_gate,
                enable_overheat_ret_1d_rule=enable_overheat_ret_1d_rule,
                enable_overheat_ret_5d_rule=enable_overheat_ret_5d_rule,
                enable_overheat_range_rule=enable_overheat_range_rule,
                enable_overheat_volume_z20_rule=enable_overheat_volume_z20_rule,
                max_entry_ret_1d=max_entry_ret_1d,
                max_entry_ret_5d=max_entry_ret_5d,
                max_entry_range_pct=max_entry_range_pct,
                max_entry_volume_z20=max_entry_volume_z20,
                enable_volume_surge_overheat_rule=enable_volume_surge_overheat_rule,
                volume_surge_threshold=volume_surge_threshold,
                volume_surge_ret_5d_threshold=volume_surge_ret_5d_threshold,
                enable_entry_quality_gate=enable_entry_quality_gate,
                enable_entry_range_rule=enable_entry_range_rule,
                enable_entry_volatility_rule=enable_entry_volatility_rule,
                enable_entry_ret5_minus_range_rule=enable_entry_ret5_minus_range_rule,
                enable_entry_range_to_ret5_rule=enable_entry_range_to_ret5_rule,
                enable_entry_volatility_to_momentum20_rule=enable_entry_volatility_to_momentum20_rule,
                max_entry_quality_range_pct=max_entry_quality_range_pct,
                max_entry_volatility_20d=max_entry_volatility_20d,
                min_entry_ret5_minus_range=min_entry_ret5_minus_range,
                max_entry_range_to_ret5=max_entry_range_to_ret5,
                max_entry_volatility_to_momentum20=max_entry_volatility_to_momentum20,
            )
            overheat_rejected_count += int(overheat_diag["overheat_rejected_count"])
            overheat_rejected_by_ret_1d += int(overheat_diag["overheat_rejected_by_ret_1d"])
            overheat_rejected_by_ret_5d += int(overheat_diag["overheat_rejected_by_ret_5d"])
            overheat_rejected_by_range_pct += int(overheat_diag["overheat_rejected_by_range_pct"])
            overheat_rejected_by_volume_z20 += int(overheat_diag["overheat_rejected_by_volume_z20"])
            overheat_rejected_by_volume_surge_rule += int(overheat_diag["overheat_rejected_by_volume_surge_rule"])
            entry_quality_rejected_count += int(overheat_diag["entry_quality_rejected_count"])
            rejected_by_range_pct += int(overheat_diag["rejected_by_range_pct"])
            rejected_by_volatility_20d += int(overheat_diag["rejected_by_volatility_20d"])
            rejected_by_ret5_minus_range += int(overheat_diag["rejected_by_ret5_minus_range"])
            rejected_by_range_to_ret5 += int(overheat_diag["rejected_by_range_to_ret5"])
            rejected_by_volatility_to_momentum20 += int(overheat_diag["rejected_by_volatility_to_momentum20"])
            if block_new_buys:
                target_holdings = target_holdings & current_holdings
            if entry_gate_enabled:
                open_slots = max(0, effective_top_n - len(current_holdings))
                rejects = 0
                fills = 0
                for row in ranked_rows:
                    sym = row["symbol"]
                    if sym in current_holdings:
                        continue
                    score = float(row["score"]) if row["score"] is not None else 0.0
                    m20 = row["momentum_20d"]
                    m60 = row["momentum_60d"]
                    g20 = row["sma_20_gap"]
                    g60 = row["sma_60_gap"]
                    passed = True
                    if score < float(min_entry_score):
                        passed = False
                    if require_positive_momentum20 and (m20 is None or float(m20) <= 0.0):
                        passed = False
                    if require_positive_momentum60 and (m60 is None or float(m60) <= 0.0):
                        passed = False
                    if require_above_sma20 and (g20 is None or float(g20) <= 0.0):
                        passed = False
                    if require_above_sma60 and (g60 is None or float(g60) <= 0.0):
                        passed = False
                    if passed:
                        fills += 1
                        if fills >= open_slots:
                            break
                    else:
                        rejects += 1
                entry_gate_rejected_count += rejects

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
                new_entries_for_day.add(sym)
            for sym in (current_holdings - target_holdings):
                entry_index_by_symbol.pop(sym, None)
                entry_price_by_symbol.pop(sym, None)
                peak_price_by_symbol.pop(sym, None)
                holding_weight_by_symbol.pop(sym, None)
            current_holdings = target_holdings
            if current_holdings:
                allocation_slots = int(effective_top_n) if 0 < len(current_holdings) < int(effective_top_n) else len(current_holdings)
                equal_weight = 1.0 / float(allocation_slots)
            else:
                equal_weight = 0.0
            for sym in current_holdings:
                holding_weight_by_symbol[sym] = equal_weight

        current_close_rows = conn.execute(
            "SELECT symbol, close FROM daily_prices WHERE date=?",
            (d0,),
        ).fetchall()
        close_by_symbol = {r["symbol"]: float(r["close"]) for r in current_close_rows if r["close"] is not None}

        for sym in sorted(current_holdings):
            px = close_by_symbol.get(sym)
            if px is None:
                continue
            if sym in new_entries_for_day or sym not in entry_price_by_symbol:
                entry_price_by_symbol[sym] = px
                peak_price_by_symbol[sym] = px
            else:
                peak_price_by_symbol[sym] = max(peak_price_by_symbol.get(sym, px), px)

        risk_exits: set[str] = set()
        for sym in sorted(current_holdings):
            px = close_by_symbol.get(sym)
            if px is None:
                continue
            entry_px = entry_price_by_symbol.get(sym, px)
            if enable_position_stop_loss and entry_px > 0:
                ret = (px - entry_px) / entry_px
                if ret <= -float(position_stop_loss_pct):
                    risk_exits.add(sym)
                    position_stop_loss_count += 1
                    cooldown_until_idx = i + int(max(0, stop_loss_cooldown_days))
                    stop_loss_cooldown_until_idx_by_symbol[sym] = cooldown_until_idx
                    cooldown_until_date = (
                        all_dates[cooldown_until_idx] if cooldown_until_idx < len(all_dates) else all_dates[-1]
                    )
                    risk_event_rows.append(
                        (
                            run_id,
                            d0,
                            sym,
                            "position_stop_loss",
                            px,
                            entry_px,
                            ret,
                            "sell_position",
                            created_at,
                            cooldown_until_date,
                        )
                    )
                    continue
            if enable_trailing_stop:
                peak_px = peak_price_by_symbol.get(sym, px)
                if peak_px > 0:
                    dd_from_peak = (px - peak_px) / peak_px
                    if dd_from_peak <= -float(trailing_stop_pct):
                        risk_exits.add(sym)
                        trailing_stop_count += 1
                        risk_event_rows.append(
                            (run_id, d0, sym, "trailing_stop", px, peak_px, dd_from_peak, "sell_position", created_at, None)
                        )

        if risk_exits:
            for sym in risk_exits:
                entry_index_by_symbol.pop(sym, None)
                entry_price_by_symbol.pop(sym, None)
                peak_price_by_symbol.pop(sym, None)
                holding_weight_by_symbol.pop(sym, None)
            current_holdings -= risk_exits
            if current_holdings and stop_loss_cash_mode == "rebalance_remaining":
                equal_weight = 1.0 / float(len(current_holdings))
                for sym in current_holdings:
                    holding_weight_by_symbol[sym] = equal_weight
            removed_by_risk_for_day = set(risk_exits)

        total_weight = sum(holding_weight_by_symbol.get(sym, 0.0) for sym in current_holdings)
        max_single_weight = max((holding_weight_by_symbol.get(sym, 0.0) for sym in current_holdings), default=0.0)
        cash_weight = max(0.0, 1.0 - total_weight)
        if not current_holdings:
            daily_ret = 0.0
            pos_count = 0
        else:
            _append_holdings_snapshot(d0)
            weighted_ret = 0.0
            for sym in sorted(current_holdings):
                row0 = conn.execute(
                    "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d0)
                ).fetchone()
                row1 = conn.execute(
                    "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d1)
                ).fetchone()
                if row0 and row1 and row0["close"]:
                    sym_ret = (row1["close"] - row0["close"]) / row0["close"]
                    weighted_ret += holding_weight_by_symbol.get(sym, 0.0) * sym_ret
            daily_ret = weighted_ret
            pos_count = len(current_holdings)
        if entry_gate_enabled and pos_count < int(top_n):
            entry_gate_cash_days += 1
        if enable_overheat_entry_gate and pos_count < int(top_n):
            overheat_cash_days += 1
        if enable_entry_quality_gate and pos_count < int(top_n):
            entry_quality_cash_days += 1
        if removed_by_risk_for_day and pos_count < int(top_n):
            risk_cut_cash_days += 1

        equity *= 1.0 + daily_ret
        result_rows.append((run_id, d1, equity, daily_ret, pos_count, total_weight, cash_weight, max_single_weight))
        portfolio_peak_equity = max(portfolio_peak_equity, equity)
        if enable_portfolio_dd_cut and portfolio_peak_equity > 0:
            portfolio_dd = (equity - portfolio_peak_equity) / portfolio_peak_equity
            if portfolio_dd <= -float(portfolio_dd_cut_pct):
                if (i + 1) > dd_cooldown_until_idx:
                    portfolio_dd_cut_count += 1
                    dd_cooldown_until_idx = i + int(max(0, portfolio_dd_cooldown_days))
                    portfolio_dd_cooldown_days_count += int(max(0, portfolio_dd_cooldown_days))
                    risk_event_rows.append(
                        (
                            run_id,
                            d1,
                            None,
                            "portfolio_dd_cut",
                            equity,
                            portfolio_peak_equity,
                            portfolio_dd,
                            "pause_new_buys",
                            created_at,
                            None,
                        )
                    )

        if enable_portfolio_dd_cut and i < dd_cooldown_until_idx:
            risk_cut_cash_days += 1

    if all_dates:
        _append_holdings_snapshot(all_dates[-1])

    conn.executemany(
        """
        INSERT INTO backtest_results(
            run_id,date,equity,daily_return,position_count,exposure,cash_weight,max_single_position_weight
        )
        VALUES(?,?,?,?,?,?,?,?)
        """,
        result_rows,
    )
    conn.execute("DELETE FROM backtest_holdings WHERE run_id=?", (run_id,))
    if holdings_rows:
        conn.executemany(
            """
            INSERT INTO backtest_holdings(
                run_id,date,symbol,weight,entry_date,entry_price,close,unrealized_return,rank,score
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            holdings_rows,
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
    if risk_event_rows:
        conn.executemany(
            """
            INSERT INTO backtest_risk_events(
                run_id,date,symbol,event_type,trigger_price,reference_price,return_pct,action,created_at,stop_loss_cooldown_until
            ) VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            risk_event_rows,
        )
    conn.execute(
        """
        UPDATE backtest_runs
        SET ma20_trigger_count=?,
            ma60_trigger_count=?,
            reduced_target_count_days=?,
            blocked_new_buy_days=?,
            cash_mode_days=?,
            entry_gate_rejected_count=?,
            entry_gate_cash_days=?,
            average_actual_position_count=?,
            min_actual_position_count=?,
            max_actual_position_count=?,
            overheat_rejected_count=?,
            overheat_cash_days=?,
            overheat_rejected_by_ret_1d=?,
            overheat_rejected_by_ret_5d=?,
            overheat_rejected_by_range_pct=?,
            overheat_rejected_by_volume_z20=?,
            overheat_rejected_by_volume_surge_rule=?,
            entry_quality_rejected_count=?,
            entry_quality_cash_days=?,
            rejected_by_range_pct=?,
            rejected_by_volatility_20d=?,
            rejected_by_ret5_minus_range=?,
            rejected_by_range_to_ret5=?,
            rejected_by_volatility_to_momentum20=?,
            position_stop_loss_count=?,
            trailing_stop_count=?,
            portfolio_dd_cut_count=?,
            portfolio_dd_cooldown_days_count=?,
            risk_cut_cash_days=?,
            average_cash_weight=?,
            average_exposure=?,
            min_exposure=?,
            max_single_position_weight=?
        WHERE run_id=?
        """,
        (
            int(ma20_trigger_count),
            int(ma60_trigger_count),
            int(reduced_target_count_days),
            int(blocked_new_buy_days),
            int(cash_mode_days),
            int(entry_gate_rejected_count),
            int(entry_gate_cash_days),
            (sum(r[4] for r in result_rows) / len(result_rows)) if result_rows else 0.0,
            min((r[4] for r in result_rows), default=0),
            max((r[4] for r in result_rows), default=0),
            int(overheat_rejected_count),
            int(overheat_cash_days),
            int(overheat_rejected_by_ret_1d),
            int(overheat_rejected_by_ret_5d),
            int(overheat_rejected_by_range_pct),
            int(overheat_rejected_by_volume_z20),
            int(overheat_rejected_by_volume_surge_rule),
            int(entry_quality_rejected_count),
            int(entry_quality_cash_days),
            int(rejected_by_range_pct),
            int(rejected_by_volatility_20d),
            int(rejected_by_ret5_minus_range),
            int(rejected_by_range_to_ret5),
            int(rejected_by_volatility_to_momentum20),
            int(position_stop_loss_count),
            int(trailing_stop_count),
            int(portfolio_dd_cut_count),
            int(portfolio_dd_cooldown_days_count),
            int(risk_cut_cash_days),
            (sum(r[6] for r in result_rows) / len(result_rows)) if result_rows else 0.0,
            (sum(r[5] for r in result_rows) / len(result_rows)) if result_rows else 0.0,
            min((r[5] for r in result_rows), default=0.0),
            max((r[7] for r in result_rows), default=0.0),
            run_id,
        ),
    )
    conn.commit()
    return run_id
