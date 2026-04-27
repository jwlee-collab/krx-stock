from __future__ import annotations

import csv
import math
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

TRADING_DAYS = 252


@dataclass
class SeriesResult:
    key: str
    label: str
    returns: list[float]
    equities: list[float]
    holdings: list[int]
    trades: int


def _to_yyyymmdd(value: str) -> str:
    return value.replace("-", "")


def _safe_div(x: float, y: float) -> float:
    return x / y if y else 0.0


def _max_drawdown(equities: list[float]) -> float:
    if not equities:
        return 0.0
    peak = equities[0]
    mdd = 0.0
    for v in equities:
        if v > peak:
            peak = v
        dd = _safe_div(v - peak, peak)
        if dd < mdd:
            mdd = dd
    return mdd


def _annualized_return(total_return: float, n_returns: int) -> float:
    if n_returns <= 0:
        return 0.0
    years = n_returns / TRADING_DAYS
    return (1.0 + total_return) ** (1.0 / years) - 1.0 if years > 0 else 0.0


def _volatility(returns: list[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    return math.sqrt(var) * math.sqrt(TRADING_DAYS)


def _sharpe(returns: list[float]) -> float:
    vol = _volatility(returns)
    if vol == 0.0 or not returns:
        return 0.0
    avg = sum(returns) / len(returns)
    return (avg * TRADING_DAYS) / vol


def _estimate_trades(holdings_by_day: list[set[str]]) -> int:
    if not holdings_by_day:
        return 0
    trades = len(holdings_by_day[0])
    for i in range(1, len(holdings_by_day)):
        prev = holdings_by_day[i - 1]
        cur = holdings_by_day[i]
        trades += len(cur - prev) + len(prev - cur)
    return trades


def _compute_metrics(series: SeriesResult, actual_initial: float) -> dict[str, float | int | str]:
    first_equity = series.equities[0] if series.equities else actual_initial
    ending = series.equities[-1] if series.equities else actual_initial
    total = _safe_div(ending - actual_initial, actual_initial)
    return {
        "strategy_key": series.key,
        "strategy_label": series.label,
        "actual_initial_capital": actual_initial,
        "first_recorded_equity": first_equity,
        "ending_equity": ending,
        "total_return": total,
        "annualized_return": _annualized_return(total, len(series.returns)),
        "max_drawdown": _max_drawdown(series.equities),
        "volatility": _volatility(series.returns),
        "sharpe": _sharpe(series.returns),
        "trade_count": int(series.trades),
        "avg_holdings": sum(series.holdings) / len(series.holdings) if series.holdings else 0.0,
    }


def _get_latest_run_id(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT run_id FROM backtest_runs ORDER BY created_at DESC LIMIT 1").fetchone()
    if not row:
        raise ValueError("No backtest_runs found")
    return row["run_id"]


def _get_latest_run_id_by_freq(conn: sqlite3.Connection, rebalance_frequency: str) -> str | None:
    row = conn.execute(
        "SELECT run_id FROM backtest_runs WHERE COALESCE(rebalance_frequency,'daily')=? ORDER BY created_at DESC LIMIT 1",
        (rebalance_frequency,),
    ).fetchone()
    return row["run_id"] if row else None


def _get_run_dates(conn: sqlite3.Connection, run_id: str) -> list[str]:
    rows = conn.execute("SELECT date FROM backtest_results WHERE run_id=? ORDER BY date", (run_id,)).fetchall()
    dates = [r["date"] for r in rows]
    if not dates:
        raise ValueError(f"No backtest_results found for run_id={run_id}")
    return dates




def _get_scoring_profile(conn: sqlite3.Connection, run_id: str) -> str:
    row = conn.execute("SELECT COALESCE(scoring_profile, 'old') AS scoring_profile FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone()
    return str(row["scoring_profile"]) if row else "old"


def _get_market_filter_label(conn: sqlite3.Connection, run_id: str) -> str:
    row = conn.execute(
        """
        SELECT COALESCE(market_filter_enabled,0) AS enabled,
               COALESCE(market_filter_ma20_reduce_by,1) AS ma20_cut,
               COALESCE(market_filter_ma60_mode,'block_new_buys') AS ma60_mode
        FROM backtest_runs WHERE run_id=?
        """,
        (run_id,),
    ).fetchone()
    if not row or int(row["enabled"]) == 0:
        return "mfilter=OFF"
    return f"mfilter=ON(ma20_cut={int(row['ma20_cut'])},ma60={row['ma60_mode']})"

def _get_initial_equity(conn: sqlite3.Connection, run_id: str) -> float:
    run = conn.execute("SELECT initial_equity FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone()
    if run and run["initial_equity"] is not None:
        return float(run["initial_equity"])
    first = conn.execute(
        "SELECT equity, daily_return FROM backtest_results WHERE run_id=? ORDER BY date LIMIT 1", (run_id,)
    ).fetchone()
    if not first:
        return 100000.0
    return float(first["equity"]) / (1.0 + float(first["daily_return"]))


def _week_changed(curr: str, prev: str | None) -> bool:
    if prev is None:
        return True
    c = datetime.strptime(curr, "%Y-%m-%d").date().isocalendar()[:2]
    p = datetime.strptime(prev, "%Y-%m-%d").date().isocalendar()[:2]
    return c != p


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
        sym for sym in current_symbols if rank_by_symbol.get(sym, 10**9) <= keep_rank_threshold
    }
    keep_due_holding = {
        sym
        for sym in current_symbols
        if (current_day_index - entry_index_by_symbol.get(sym, current_day_index)) < min_holding_days
    }
    kept = keep_due_rank | keep_due_holding
    target = list(kept)
    for sym in ranked_symbols:
        if sym in kept:
            continue
        if len(target) >= top_n:
            break
        target.append(sym)
    return set(target)


def _simulate_holdings_from_run(conn: sqlite3.Connection, run_id: str, dates: list[str]) -> list[set[str]]:
    run = conn.execute(
        """
        SELECT top_n, COALESCE(rebalance_frequency,'daily') AS rebalance_frequency,
               COALESCE(min_holding_days,0) AS min_holding_days,
               COALESCE(keep_rank_threshold, top_n) AS keep_rank_threshold,
               COALESCE(market_filter_enabled,0) AS market_filter_enabled,
               COALESCE(market_filter_ma20_reduce_by,1) AS market_filter_ma20_reduce_by,
               COALESCE(market_filter_ma60_mode,'block_new_buys') AS market_filter_ma60_mode
        FROM backtest_runs WHERE run_id=?
        """,
        (run_id,),
    ).fetchone()
    if not run:
        raise ValueError(f"run_id not found: {run_id}")

    top_n = int(run["top_n"])
    frequency = run["rebalance_frequency"]
    min_holding_days = int(run["min_holding_days"])
    keep_rank_threshold = int(run["keep_rank_threshold"])

    prev_dates = [conn.execute("SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d,)).fetchone()["d"] for d in dates]

    market_filter_enabled = int(run["market_filter_enabled"]) == 1
    market_filter_ma20_reduce_by = int(run["market_filter_ma20_reduce_by"])
    market_filter_ma60_mode = str(run["market_filter_ma60_mode"])

    market_rows = conn.execute(
        """
        SELECT date, AVG(close) AS market_close
        FROM daily_prices
        GROUP BY date
        ORDER BY date
        """
    ).fetchall()
    market_close_by_date = {r["date"]: float(r["market_close"]) for r in market_rows if r["market_close"] is not None}
    ordered_market_dates = [d for d in sorted(market_close_by_date) if d in prev_dates]
    regime_by_date: dict[str, dict[str, bool]] = {}
    closes = [market_close_by_date[d] for d in ordered_market_dates]
    for idx, d in enumerate(ordered_market_dates):
        ma20 = (sum(closes[idx - 19 : idx + 1]) / 20.0) if idx >= 19 else None
        ma60 = (sum(closes[idx - 59 : idx + 1]) / 60.0) if idx >= 59 else None
        c = closes[idx]
        regime_by_date[d] = {
            "below_ma20": (ma20 is not None and c < ma20),
            "below_ma60": (ma60 is not None and c < ma60),
        }

    current_holdings: set[str] = set()
    entry_index_by_symbol: dict[str, int] = {}
    holdings_by_day: list[set[str]] = []

    for i, d0 in enumerate(prev_dates):
        if not d0:
            holdings_by_day.append(set())
            continue

        prev_d0 = prev_dates[i - 1] if i > 0 else None
        should_rebalance = frequency == "daily" or (frequency == "weekly" and _week_changed(d0, prev_d0))

        if should_rebalance:
            ranked = conn.execute(
                "SELECT symbol, rank FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC",
                (d0,),
            ).fetchall()
            ranked_symbols = [r["symbol"] for r in ranked]
            rank_by_symbol = {r["symbol"]: int(r["rank"]) for r in ranked}
            effective_top_n = top_n
            block_new_buys = False
            if market_filter_enabled:
                regime = regime_by_date.get(d0, {"below_ma20": False, "below_ma60": False})
                if regime["below_ma20"]:
                    effective_top_n = max(0, effective_top_n - max(0, market_filter_ma20_reduce_by))
                if regime["below_ma60"]:
                    if market_filter_ma60_mode == "cash":
                        effective_top_n = 0
                    elif market_filter_ma60_mode == "block_new_buys":
                        block_new_buys = True

            target = _build_target_holdings(
                ranked_symbols,
                rank_by_symbol,
                current_holdings,
                entry_index_by_symbol,
                i,
                effective_top_n,
                min_holding_days,
                keep_rank_threshold,
            )
            if block_new_buys:
                target = target & current_holdings
            for sym in target - current_holdings:
                entry_index_by_symbol[sym] = i
            for sym in current_holdings - target:
                entry_index_by_symbol.pop(sym, None)
            current_holdings = target

        holdings_by_day.append(set(current_holdings))
    return holdings_by_day


def _load_strategy_series(conn: sqlite3.Connection, run_id: str, key: str, label: str) -> tuple[SeriesResult, list[str]]:
    rows = conn.execute(
        "SELECT date,equity,daily_return,position_count FROM backtest_results WHERE run_id=? ORDER BY date",
        (run_id,),
    ).fetchall()
    dates = [r["date"] for r in rows]
    holdings_by_day = _simulate_holdings_from_run(conn, run_id, dates)
    return (
        SeriesResult(
            key=key,
            label=label,
            returns=[float(r["daily_return"]) for r in rows],
            equities=[float(r["equity"]) for r in rows],
            holdings=[int(r["position_count"]) for r in rows],
            trades=_estimate_trades(holdings_by_day),
        ),
        dates,
    )


def _build_equal_weight_universe(conn: sqlite3.Connection, dates: list[str], initial_equity: float) -> SeriesResult:
    equity = initial_equity
    equities, returns, holdings = [], [], []
    holding_sets: list[set[str]] = []
    prev_dates = [conn.execute("SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d,)).fetchone()["d"] for d in dates]
    for d0, d1 in zip(prev_dates, dates):
        if not d0:
            returns.append(0.0); holdings.append(0); equities.append(equity); holding_sets.append(set()); continue
        candidates = conn.execute("SELECT symbol FROM daily_scores WHERE date=?", (d0,)).fetchall()
        symbols = [r["symbol"] for r in candidates]
        realized, valid = [], set()
        for sym in symbols:
            row0 = conn.execute("SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d0)).fetchone()
            row1 = conn.execute("SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d1)).fetchone()
            if row0 and row1 and row0["close"]:
                realized.append((row1["close"] - row0["close"]) / row0["close"])
                valid.add(sym)
        ret = sum(realized) / len(realized) if realized else 0.0
        equity *= 1.0 + ret
        returns.append(ret); holdings.append(len(valid)); equities.append(equity); holding_sets.append(valid)
    return SeriesResult("equal_weight_universe", "equal_weight_universe", returns, equities, holdings, _estimate_trades(holding_sets))


def _build_proxy_benchmark(conn: sqlite3.Connection, dates: list[str], initial_equity: float) -> SeriesResult:
    equity = initial_equity
    equities, returns, holdings = [], [], []
    prev_dates = [conn.execute("SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d,)).fetchone()["d"] for d in dates]
    for d0, d1 in zip(prev_dates, dates):
        if not d0:
            returns.append(0.0); holdings.append(0); equities.append(equity); continue
        rows = conn.execute(
            """
            SELECT p0.close AS c0, p1.close AS c1
            FROM daily_prices p0
            JOIN daily_prices p1 ON p1.symbol=p0.symbol
            WHERE p0.date=? AND p1.date=?
            """,
            (d0, d1),
        ).fetchall()
        rets = [(r["c1"] - r["c0"]) / r["c0"] for r in rows if r["c0"]]
        ret = sum(rets) / len(rets) if rets else 0.0
        equity *= 1.0 + ret
        returns.append(ret); holdings.append(len(rets)); equities.append(equity)
    return SeriesResult("benchmark_kospi", "benchmark_kospi (universe proxy)", returns, equities, holdings, 0)


def _build_benchmark_series(conn: sqlite3.Connection, dates: list[str], initial_equity: float, benchmark: str) -> tuple[SeriesResult, str, str]:
    code_map = {"KOSPI": "1001", "KOSPI200": "1028"}
    code = code_map.get(benchmark.upper(), "1001")
    try:
        from pykrx import stock

        df = stock.get_index_ohlcv_by_date(_to_yyyymmdd(dates[0]), _to_yyyymmdd(dates[-1]), code)
        if df.empty:
            raise ValueError("empty benchmark dataframe")
        close_by_date = {idx.strftime("%Y-%m-%d"): float(row["종가"]) for idx, row in df.iterrows()}
        equity = initial_equity
        returns, equities = [], []
        for i, d1 in enumerate(dates):
            d0 = dates[i - 1] if i > 0 else None
            if not d0 or d0 not in close_by_date or d1 not in close_by_date or close_by_date[d0] == 0:
                ret = 0.0
            else:
                ret = (close_by_date[d1] - close_by_date[d0]) / close_by_date[d0]
            equity *= 1.0 + ret
            returns.append(ret); equities.append(equity)
        return SeriesResult("benchmark_kospi", f"benchmark_kospi ({benchmark.upper()} index)", returns, equities, [1 for _ in dates], 0), benchmark.upper(), f"pykrx_index_{code}"
    except Exception:
        return _build_proxy_benchmark(conn, dates, initial_equity), benchmark.upper(), "proxy_equal_weight_all_prices"


def _monthly_returns(dates: list[str], returns: list[float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for d, r in zip(dates, returns):
        month = d[:7]
        out.setdefault(month, 1.0)
        out[month] *= 1.0 + r
    return {m: v - 1.0 for m, v in out.items()}


def _fmt_money(v: float) -> str:
    return f"{v:,.0f}"


def _fmt_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def generate_performance_report(
    conn: sqlite3.Connection,
    output_dir: str | Path,
    run_id: str | None = None,
    benchmark: str = "KOSPI",
    baseline_run_id: str | None = None,
    improved_run_id: str | None = None,
    improved_new_run_id: str | None = None,
) -> dict[str, str | int]:
    # Backward compatibility: run_id means improved target if explicit ids absent.
    improved_target = improved_run_id or run_id or _get_latest_run_id(conn)
    baseline_target = baseline_run_id or _get_latest_run_id_by_freq(conn, "daily") or improved_target

    improved_profile = _get_scoring_profile(conn, improved_target)
    improved_series, improved_dates = _load_strategy_series(
        conn, improved_target, key="improved_strategy_old", label=f"improved_strategy_old ({improved_profile}, {_get_market_filter_label(conn, improved_target)})"
    )
    baseline_profile = _get_scoring_profile(conn, baseline_target)
    baseline_series, baseline_dates = _load_strategy_series(
        conn, baseline_target, key="baseline_strategy", label=f"baseline_strategy ({baseline_profile}, {_get_market_filter_label(conn, baseline_target)})"
    )

    strategy_series_map: dict[str, tuple[SeriesResult, list[str]]] = {
        "baseline_strategy": (baseline_series, baseline_dates),
        "improved_strategy_old": (improved_series, improved_dates),
    }
    if improved_new_run_id:
        improved_new_profile = _get_scoring_profile(conn, improved_new_run_id)
        improved_new_series, improved_new_dates = _load_strategy_series(
            conn, improved_new_run_id, key="improved_strategy_new", label=f"improved_strategy_new ({improved_new_profile}, {_get_market_filter_label(conn, improved_new_run_id)})"
        )
        strategy_series_map["improved_strategy_new"] = (improved_new_series, improved_new_dates)

    common_dates = sorted(set.intersection(*[set(v[1]) for v in strategy_series_map.values()]))
    if not common_dates:
        raise ValueError("No overlapping dates across selected strategy runs")

    def _slice(series: SeriesResult, dates: list[str]) -> SeriesResult:
        idx = {d: i for i, d in enumerate(dates)}
        selected = [idx[d] for d in common_dates]
        return SeriesResult(
            key=series.key,
            label=series.label,
            returns=[series.returns[i] for i in selected],
            equities=[series.equities[i] for i in selected],
            holdings=[series.holdings[i] for i in selected],
            trades=series.trades,
        )

    baseline_series = _slice(baseline_series, baseline_dates)
    improved_series = _slice(improved_series, improved_dates)
    improved_new_series = None
    if improved_new_run_id:
        raw_series, raw_dates = strategy_series_map["improved_strategy_new"]
        improved_new_series = _slice(raw_series, raw_dates)
    initial_equity = _get_initial_equity(conn, improved_target)

    equal_series = _build_equal_weight_universe(conn, common_dates, initial_equity)
    benchmark_series, benchmark_name, benchmark_source = _build_benchmark_series(conn, common_dates, initial_equity, benchmark)

    report_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()
    notes = (
        "benchmark_source=proxy_equal_weight_all_prices (index unavailable in current environment)"
        if benchmark_source.startswith("proxy")
        else "benchmark_source=pykrx index data"
    )
    conn.execute(
        """
        INSERT INTO performance_report_runs(
          report_id,base_run_id,created_at,benchmark_name,benchmark_source,start_date,end_date,notes
        ) VALUES(?,?,?,?,?,?,?,?)
        """,
        (report_id, improved_target, created_at, benchmark_name, benchmark_source, common_dates[0], common_dates[-1], notes),
    )

    metrics = [_compute_metrics(baseline_series, initial_equity), _compute_metrics(improved_series, initial_equity)]
    if improved_new_series is not None:
        metrics.append(_compute_metrics(improved_new_series, initial_equity))
    metrics.extend([_compute_metrics(equal_series, initial_equity), _compute_metrics(benchmark_series, initial_equity)])

    conn.executemany(
        """
        INSERT INTO performance_report_summary(
          report_id,strategy_key,strategy_label,actual_initial_capital,first_recorded_equity,
          ending_equity,total_return,annualized_return,max_drawdown,volatility,sharpe,trade_count,avg_holdings
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                report_id, m["strategy_key"], m["strategy_label"], m["actual_initial_capital"],
                m["first_recorded_equity"], m["ending_equity"], m["total_return"], m["annualized_return"],
                m["max_drawdown"], m["volatility"], m["sharpe"], m["trade_count"], m["avg_holdings"],
            )
            for m in metrics
        ],
    )

    conn.executemany(
        "INSERT INTO performance_report_curve(report_id,date,strategy_equity,equal_weight_equity,benchmark_equity) VALUES(?,?,?,?,?)",
        [
            (report_id, d, improved_series.equities[i], equal_series.equities[i], benchmark_series.equities[i])
            for i, d in enumerate(common_dates)
        ],
    )

    baseline_monthly = _monthly_returns(common_dates, baseline_series.returns)
    improved_monthly = _monthly_returns(common_dates, improved_series.returns)
    improved_new_monthly = _monthly_returns(common_dates, improved_new_series.returns) if improved_new_series else {}
    equal_monthly = _monthly_returns(common_dates, equal_series.returns)
    benchmark_monthly = _monthly_returns(common_dates, benchmark_series.returns)
    months = sorted(set(baseline_monthly) | set(improved_monthly) | set(improved_new_monthly) | set(equal_monthly) | set(benchmark_monthly))
    conn.executemany(
        "INSERT INTO performance_report_monthly(report_id,month,strategy_return,equal_weight_return,benchmark_return) VALUES(?,?,?,?,?)",
        [(report_id, m, improved_monthly.get(m, 0.0), equal_monthly.get(m, 0.0), benchmark_monthly.get(m, 0.0)) for m in months],
    )
    conn.commit()

    out_dir = Path(output_dir); out_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = out_dir / f"performance_comparison_{improved_target}.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "strategy_key", "전략명", "실제 초기 자본", "첫 기록 시점 자산", "마지막 자산", "총 수익률", "연환산 수익률",
            "최대 낙폭(MDD)", "변동성(연환산)", "샤프비율", "거래 횟수", "평균 보유 종목 수",
            "실제 초기 자본(표시)", "첫 기록 시점 자산(표시)", "마지막 자산(표시)", "총 수익률(표시)",
            "연환산 수익률(표시)", "최대 낙폭(MDD)(표시)", "변동성(표시)",
        ])
        for m in metrics:
            writer.writerow([
                m["strategy_key"], m["strategy_label"], m["actual_initial_capital"], m["first_recorded_equity"],
                m["ending_equity"], m["total_return"], m["annualized_return"], m["max_drawdown"], m["volatility"],
                m["sharpe"], m["trade_count"], m["avg_holdings"], _fmt_money(float(m["actual_initial_capital"])),
                _fmt_money(float(m["first_recorded_equity"])), _fmt_money(float(m["ending_equity"])),
                _fmt_pct(float(m["total_return"])), _fmt_pct(float(m["annualized_return"])),
                _fmt_pct(float(m["max_drawdown"])), _fmt_pct(float(m["volatility"])),
            ])

    curve_csv = out_dir / f"equity_curve_comparison_{improved_target}.csv"
    with curve_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        curve_header = ["date", "baseline_strategy", "improved_strategy_old"]
        if improved_new_series:
            curve_header.append("improved_strategy_new")
        curve_header.extend(["equal_weight_universe", "benchmark_kospi"])
        writer.writerow(curve_header)
        for i, d in enumerate(common_dates):
            row = [d, baseline_series.equities[i], improved_series.equities[i]]
            if improved_new_series:
                row.append(improved_new_series.equities[i])
            row.extend([equal_series.equities[i], benchmark_series.equities[i]])
            writer.writerow(row)

    monthly_csv = out_dir / f"monthly_returns_{improved_target}.csv"
    with monthly_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        header = ["month", "baseline_strategy", "improved_strategy_old"]
        if improved_new_series:
            header.append("improved_strategy_new")
        header.extend(["equal_weight_universe", "benchmark_kospi"])
        header_display = ["baseline_strategy(표시)", "improved_strategy_old(표시)"]
        if improved_new_series:
            header_display.append("improved_strategy_new(표시)")
        header_display.extend(["equal_weight_universe(표시)", "benchmark_kospi(표시)"])
        writer.writerow(header + header_display)
        for month in months:
            b0 = baseline_monthly.get(month, 0.0)
            b1 = improved_monthly.get(month, 0.0)
            b2 = improved_new_monthly.get(month, 0.0)
            e = equal_monthly.get(month, 0.0)
            k = benchmark_monthly.get(month, 0.0)
            row = [month, b0, b1]
            if improved_new_series:
                row.append(b2)
            row.extend([e, k, _fmt_pct(b0), _fmt_pct(b1)])
            if improved_new_series:
                row.append(_fmt_pct(b2))
            row.extend([_fmt_pct(e), _fmt_pct(k)])
            writer.writerow(row)

    return {
        "report_id": report_id,
        "run_id": improved_target,
        "baseline_run_id": baseline_target,
        "improved_run_id": improved_target,
        "improved_new_run_id": improved_new_run_id,
        "baseline_scoring_profile": baseline_profile,
        "improved_old_scoring_profile": improved_profile,
        "improved_new_scoring_profile": improved_new_profile if improved_new_run_id else None,
        "summary_csv": str(summary_csv),
        "curve_csv": str(curve_csv),
        "monthly_csv": str(monthly_csv),
        "benchmark_name": benchmark_name,
        "benchmark_source": benchmark_source,
        "rows": len(common_dates),
    }
