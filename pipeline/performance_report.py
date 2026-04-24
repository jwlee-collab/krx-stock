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
    if years <= 0:
        return 0.0
    return (1.0 + total_return) ** (1.0 / years) - 1.0


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


def _compute_metrics(
    series: SeriesResult,
    actual_initial: float,
) -> dict[str, float | int | str]:
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
    row = conn.execute(
        "SELECT run_id FROM backtest_runs ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        raise ValueError("No backtest_runs found")
    return row["run_id"]


def _get_run_dates(conn: sqlite3.Connection, run_id: str) -> list[str]:
    rows = conn.execute(
        "SELECT date FROM backtest_results WHERE run_id=? ORDER BY date", (run_id,)
    ).fetchall()
    dates = [r["date"] for r in rows]
    if not dates:
        raise ValueError(f"No backtest_results found for run_id={run_id}")
    return dates


def _get_initial_equity(conn: sqlite3.Connection, run_id: str) -> float:
    run = conn.execute(
        "SELECT initial_equity FROM backtest_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    if run and run["initial_equity"] is not None:
        return float(run["initial_equity"])

    first = conn.execute(
        "SELECT equity, daily_return FROM backtest_results WHERE run_id=? ORDER BY date LIMIT 1",
        (run_id,),
    ).fetchone()
    if not first:
        return 100000.0
    return float(first["equity"]) / (1.0 + float(first["daily_return"]))


def _load_strategy_series(conn: sqlite3.Connection, run_id: str) -> tuple[SeriesResult, list[str]]:
    rows = conn.execute(
        "SELECT date,equity,daily_return,position_count FROM backtest_results WHERE run_id=? ORDER BY date",
        (run_id,),
    ).fetchall()
    dates = [r["date"] for r in rows]

    run = conn.execute("SELECT top_n FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone()
    top_n = int(run["top_n"]) if run else 0
    holding_sets: list[set[str]] = []
    for d in dates:
        prev_date = conn.execute(
            "SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d,)
        ).fetchone()["d"]
        if not prev_date:
            holding_sets.append(set())
            continue
        picks = conn.execute(
            "SELECT symbol FROM daily_scores WHERE date=? ORDER BY rank ASC, symbol ASC LIMIT ?",
            (prev_date, top_n),
        ).fetchall()
        holding_sets.append({p["symbol"] for p in picks})

    return (
        SeriesResult(
            key="strategy",
            label="strategy (score top-N)",
            returns=[float(r["daily_return"]) for r in rows],
            equities=[float(r["equity"]) for r in rows],
            holdings=[int(r["position_count"]) for r in rows],
            trades=_estimate_trades(holding_sets),
        ),
        dates,
    )


def _build_equal_weight_universe(conn: sqlite3.Connection, dates: list[str], initial_equity: float) -> SeriesResult:
    equity = initial_equity
    equities: list[float] = []
    returns: list[float] = []
    holdings: list[int] = []
    holding_sets: list[set[str]] = []

    prev_dates = [conn.execute("SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d,)).fetchone()["d"] for d in dates]
    for d0, d1 in zip(prev_dates, dates):
        if not d0:
            returns.append(0.0)
            holdings.append(0)
            equities.append(equity)
            holding_sets.append(set())
            continue

        candidates = conn.execute("SELECT symbol FROM daily_scores WHERE date=?", (d0,)).fetchall()
        symbols = [r["symbol"] for r in candidates]
        realized: list[float] = []
        valid_symbols: set[str] = set()
        for sym in symbols:
            row0 = conn.execute(
                "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d0)
            ).fetchone()
            row1 = conn.execute(
                "SELECT close FROM daily_prices WHERE symbol=? AND date=?", (sym, d1)
            ).fetchone()
            if row0 and row1 and row0["close"]:
                realized.append((row1["close"] - row0["close"]) / row0["close"])
                valid_symbols.add(sym)

        ret = sum(realized) / len(realized) if realized else 0.0
        equity *= 1.0 + ret
        returns.append(ret)
        holdings.append(len(valid_symbols))
        equities.append(equity)
        holding_sets.append(valid_symbols)

    return SeriesResult(
        key="equal_weight_universe",
        label="equal_weight_universe",
        returns=returns,
        equities=equities,
        holdings=holdings,
        trades=_estimate_trades(holding_sets),
    )


def _build_proxy_benchmark(conn: sqlite3.Connection, dates: list[str], initial_equity: float) -> SeriesResult:
    equity = initial_equity
    equities: list[float] = []
    returns: list[float] = []
    holdings: list[int] = []

    prev_dates = [conn.execute("SELECT MAX(date) AS d FROM daily_prices WHERE date < ?", (d,)).fetchone()["d"] for d in dates]
    for d0, d1 in zip(prev_dates, dates):
        if not d0:
            returns.append(0.0)
            holdings.append(0)
            equities.append(equity)
            continue

        rows = conn.execute(
            """
            SELECT p0.symbol AS symbol, p0.close AS c0, p1.close AS c1
            FROM daily_prices p0
            JOIN daily_prices p1 ON p1.symbol=p0.symbol
            WHERE p0.date=? AND p1.date=?
            """,
            (d0, d1),
        ).fetchall()
        rets = [(r["c1"] - r["c0"]) / r["c0"] for r in rows if r["c0"]]
        ret = sum(rets) / len(rets) if rets else 0.0
        equity *= 1.0 + ret
        returns.append(ret)
        holdings.append(len(rets))
        equities.append(equity)

    return SeriesResult(
        key="benchmark_kospi",
        label="benchmark_kospi (universe proxy)",
        returns=returns,
        equities=equities,
        holdings=holdings,
        trades=0,
    )


def _build_benchmark_series(
    conn: sqlite3.Connection,
    dates: list[str],
    initial_equity: float,
    benchmark: str,
) -> tuple[SeriesResult, str, str]:
    code_map = {"KOSPI": "1001", "KOSPI200": "1028"}
    code = code_map.get(benchmark.upper(), "1001")

    try:
        from pykrx import stock

        start = _to_yyyymmdd(dates[0])
        end = _to_yyyymmdd(dates[-1])
        df = stock.get_index_ohlcv_by_date(start, end, code)
        if df.empty:
            raise ValueError("empty benchmark dataframe")

        close_by_date = {idx.strftime("%Y-%m-%d"): float(row["종가"]) for idx, row in df.iterrows()}
        equity = initial_equity
        returns: list[float] = []
        equities: list[float] = []
        for i, d1 in enumerate(dates):
            d0 = dates[i - 1] if i > 0 else None
            if not d0 or d0 not in close_by_date or d1 not in close_by_date or close_by_date[d0] == 0:
                ret = 0.0
            else:
                ret = (close_by_date[d1] - close_by_date[d0]) / close_by_date[d0]
            equity *= 1.0 + ret
            returns.append(ret)
            equities.append(equity)

        return (
            SeriesResult(
                key="benchmark_kospi",
                label=f"benchmark_kospi ({benchmark.upper()} index)",
                returns=returns,
                equities=equities,
                holdings=[1 for _ in dates],
                trades=0,
            ),
            benchmark.upper(),
            f"pykrx_index_{code}",
        )
    except Exception:
        proxy = _build_proxy_benchmark(conn, dates, initial_equity)
        return proxy, benchmark.upper(), "proxy_equal_weight_all_prices"


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
) -> dict[str, str | int]:
    target_run_id = run_id or _get_latest_run_id(conn)
    dates = _get_run_dates(conn, target_run_id)
    initial_equity = _get_initial_equity(conn, target_run_id)

    strategy_series, strategy_dates = _load_strategy_series(conn, target_run_id)
    equal_series = _build_equal_weight_universe(conn, strategy_dates, initial_equity)
    benchmark_series, benchmark_name, benchmark_source = _build_benchmark_series(
        conn, strategy_dates, initial_equity, benchmark
    )

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
        (report_id, target_run_id, created_at, benchmark_name, benchmark_source, dates[0], dates[-1], notes),
    )

    metrics = [
        _compute_metrics(strategy_series, initial_equity),
        _compute_metrics(equal_series, initial_equity),
        _compute_metrics(benchmark_series, initial_equity),
    ]

    conn.executemany(
        """
        INSERT INTO performance_report_summary(
          report_id,strategy_key,strategy_label,actual_initial_capital,first_recorded_equity,
          ending_equity,total_return,annualized_return,max_drawdown,volatility,sharpe,trade_count,avg_holdings
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                report_id,
                m["strategy_key"],
                m["strategy_label"],
                m["actual_initial_capital"],
                m["first_recorded_equity"],
                m["ending_equity"],
                m["total_return"],
                m["annualized_return"],
                m["max_drawdown"],
                m["volatility"],
                m["sharpe"],
                m["trade_count"],
                m["avg_holdings"],
            )
            for m in metrics
        ],
    )

    curve_rows = [
        (
            report_id,
            d,
            strategy_series.equities[i],
            equal_series.equities[i],
            benchmark_series.equities[i],
        )
        for i, d in enumerate(strategy_dates)
    ]
    conn.executemany(
        "INSERT INTO performance_report_curve(report_id,date,strategy_equity,equal_weight_equity,benchmark_equity) VALUES(?,?,?,?,?)",
        curve_rows,
    )

    strategy_monthly = _monthly_returns(strategy_dates, strategy_series.returns)
    equal_monthly = _monthly_returns(strategy_dates, equal_series.returns)
    benchmark_monthly = _monthly_returns(strategy_dates, benchmark_series.returns)
    months = sorted(set(strategy_monthly) | set(equal_monthly) | set(benchmark_monthly))
    conn.executemany(
        "INSERT INTO performance_report_monthly(report_id,month,strategy_return,equal_weight_return,benchmark_return) VALUES(?,?,?,?,?)",
        [
            (
                report_id,
                month,
                strategy_monthly.get(month, 0.0),
                equal_monthly.get(month, 0.0),
                benchmark_monthly.get(month, 0.0),
            )
            for month in months
        ],
    )
    conn.commit()

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_csv = out_dir / f"performance_comparison_{target_run_id}.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "strategy_key",
                "전략명",
                "실제 초기 자본",
                "첫 기록 시점 자산",
                "마지막 자산",
                "총 수익률",
                "연환산 수익률",
                "최대 낙폭(MDD)",
                "변동성(연환산)",
                "샤프비율",
                "거래 횟수",
                "평균 보유 종목 수",
                "실제 초기 자본(표시)",
                "첫 기록 시점 자산(표시)",
                "마지막 자산(표시)",
                "총 수익률(표시)",
                "연환산 수익률(표시)",
                "최대 낙폭(MDD)(표시)",
                "변동성(표시)",
            ]
        )
        for m in metrics:
            writer.writerow(
                [
                    m["strategy_key"],
                    m["strategy_label"],
                    m["actual_initial_capital"],
                    m["first_recorded_equity"],
                    m["ending_equity"],
                    m["total_return"],
                    m["annualized_return"],
                    m["max_drawdown"],
                    m["volatility"],
                    m["sharpe"],
                    m["trade_count"],
                    m["avg_holdings"],
                    _fmt_money(float(m["actual_initial_capital"])),
                    _fmt_money(float(m["first_recorded_equity"])),
                    _fmt_money(float(m["ending_equity"])),
                    _fmt_pct(float(m["total_return"])),
                    _fmt_pct(float(m["annualized_return"])),
                    _fmt_pct(float(m["max_drawdown"])),
                    _fmt_pct(float(m["volatility"])),
                ]
            )

    curve_csv = out_dir / f"equity_curve_comparison_{target_run_id}.csv"
    with curve_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "strategy", "equal_weight_universe", "benchmark_kospi"])
        for i, d in enumerate(strategy_dates):
            writer.writerow([d, strategy_series.equities[i], equal_series.equities[i], benchmark_series.equities[i]])

    monthly_csv = out_dir / f"monthly_returns_{target_run_id}.csv"
    with monthly_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "month",
                "strategy",
                "equal_weight_universe",
                "benchmark_kospi",
                "strategy(표시)",
                "equal_weight_universe(표시)",
                "benchmark_kospi(표시)",
            ]
        )
        for month in months:
            s = strategy_monthly.get(month, 0.0)
            e = equal_monthly.get(month, 0.0)
            b = benchmark_monthly.get(month, 0.0)
            writer.writerow([month, s, e, b, _fmt_pct(s), _fmt_pct(e), _fmt_pct(b)])

    return {
        "report_id": report_id,
        "run_id": target_run_id,
        "summary_csv": str(summary_csv),
        "curve_csv": str(curve_csv),
        "monthly_csv": str(monthly_csv),
        "benchmark_name": benchmark_name,
        "benchmark_source": benchmark_source,
        "rows": len(strategy_dates),
    }
