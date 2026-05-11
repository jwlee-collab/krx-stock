from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from dataclasses import replace
from datetime import datetime, time, timedelta
from typing import Any

from pipeline.day_trading.config import DayTradingConfig
from pipeline.day_trading.context import IntradayMarketContext
from pipeline.day_trading.data import load_intraday_bars
from pipeline.day_trading.engine import DayTradingEngine
from pipeline.day_trading.filters import _drop_no_trade_bars, _is_no_trade_bar
from pipeline.day_trading.logging import DayTradeLogger
from pipeline.day_trading.models import IntradayBar
from pipeline.day_trading.paper_account import PaperAccount
from pipeline.day_trading.performance import CostModel, DayPerformanceAnalyzer
from pipeline.day_trading.positions import DayPositionTracker
from pipeline.day_trading.signals import DaySignalGenerator
from pipeline.day_trading.universe import DayUniverseProvider
from pipeline.day_trading.universe import StaticDayUniverseProvider
from pipeline.day_trading.models import UniverseSelection


def _bars_until(bars: list[IntradayBar], ts: datetime) -> list[IntradayBar]:
    return [b for b in sorted(bars, key=lambda x: x.timestamp) if b.timestamp <= ts]


def _timeframe_minutes(timeframe: str) -> int | None:
    value = timeframe.lower().strip()
    if value.endswith("m") and value[:-1].isdigit():
        return int(value[:-1])
    return None


def _bar_completed_by(bar: IntradayBar, ts: datetime, timeframe: str) -> bool:
    minutes = _timeframe_minutes(timeframe)
    if minutes is None:
        return bar.timestamp <= ts
    return bar.timestamp + timedelta(minutes=minutes) <= ts


def _bars_completed_by(bars: list[IntradayBar], ts: datetime, timeframe: str) -> list[IntradayBar]:
    return [b for b in sorted(bars, key=lambda x: x.timestamp) if _bar_completed_by(b, ts, timeframe)]


def _apply_zero_volume_bar_policy_to_loaded_data(
    full_data: dict[str, dict[str, list[IntradayBar]]],
    policy: str,
) -> dict[str, dict[str, list[IntradayBar]]]:
    if policy != "drop_no_trade":
        return full_data
    return {
        symbol: {
            timeframe: _drop_no_trade_bars(list(bars))
            for timeframe, bars in by_timeframe.items()
        }
        for symbol, by_timeframe in full_data.items()
    }


def _zero_volume_policy_summary(
    full_data: dict[str, dict[str, list[IntradayBar]]],
    policy: str,
    *,
    events: list[Any] | None = None,
) -> dict[str, Any]:
    no_trade_by_symbol: dict[str, int] = {}
    positive_by_symbol: dict[str, int] = {}
    no_trade_by_timeframe: Counter[str] = Counter()
    positive_by_timeframe: Counter[str] = Counter()
    no_trade_total = 0
    positive_total = 0
    for symbol, by_timeframe in full_data.items():
        symbol_no_trade = 0
        symbol_positive = 0
        for timeframe, bars in by_timeframe.items():
            for bar in bars:
                if _is_no_trade_bar(bar):
                    no_trade_total += 1
                    symbol_no_trade += 1
                    no_trade_by_timeframe[timeframe] += 1
                elif float(bar.volume or 0.0) > 0.0:
                    positive_total += 1
                    symbol_positive += 1
                    positive_by_timeframe[timeframe] += 1
        if symbol_no_trade:
            no_trade_by_symbol[symbol] = symbol_no_trade
        if symbol_positive:
            positive_by_symbol[symbol] = symbol_positive

    event_counter: Counter[str] = Counter()
    for event in events or []:
        event_counter.update(getattr(event, "reason_codes", []) or [])

    dropped_count = no_trade_total if policy == "drop_no_trade" else 0
    return {
        "zero_volume_bar_policy": policy,
        "strict_invalid_count": no_trade_total if policy == "strict_invalid" else 0,
        "no_trade_context_count": no_trade_total if policy == "no_trade_context" else 0,
        "dropped_no_trade_count": dropped_count,
        "no_trade_bar_count": no_trade_total,
        "positive_volume_bar_count": positive_total,
        "no_trade_bar_count_by_symbol": dict(sorted(no_trade_by_symbol.items())),
        "positive_volume_bar_count_by_symbol": dict(sorted(positive_by_symbol.items())),
        "no_trade_bar_count_by_timeframe": dict(sorted(no_trade_by_timeframe.items())),
        "positive_volume_bar_count_by_timeframe": dict(sorted(positive_by_timeframe.items())),
        "no_trade_blocked_entry_count": int(event_counter.get("NO_TRADE_BAR_BLOCKED_ENTRY", 0)),
        "no_trade_blocked_exit_count": int(event_counter.get("NO_TRADE_BAR_BLOCKED_EXIT", 0)),
        "vwap_positive_volume_only": policy in {"no_trade_context", "drop_no_trade"},
        "no_trade_used_for_price_context": policy == "no_trade_context",
        "policy_caveat": "Zero-volume bars are a data-quality policy comparison input, not profitability evidence.",
    }


def _count_rejections(events: list[Any]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for event in events:
        if event.event_type in {"SIGNAL_REJECTED", "RISK_REJECTED"}:
            counts.update(event.reason_codes)
    return counts


def _count_rejections_by_symbol(events: list[Any]) -> dict[str, dict[str, int]]:
    out: dict[str, Counter[str]] = defaultdict(Counter)
    for event in events:
        if event.event_type not in {"SIGNAL_REJECTED", "RISK_REJECTED"} or not event.symbol:
            continue
        out[str(event.symbol)].update(event.reason_codes)
    return {symbol: dict(counter) for symbol, counter in sorted(out.items())}


def _last_evaluation_by_symbol(events: list[Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for event in events:
        if event.event_type not in {"SIGNAL_REJECTED", "RISK_REJECTED", "SIGNAL_CREATED"} or not event.symbol:
            continue
        raw_metrics = getattr(event, "raw_metrics", {}) or {}
        latest_ts = raw_metrics.get("latest_timestamp")
        if latest_ts is None:
            nested = raw_metrics.get("raw_metrics")
            if isinstance(nested, dict):
                latest_ts = nested.get("latest_timestamp")
        if latest_ts is None:
            event_time = getattr(event, "created_at", None)
            latest_ts = event_time.isoformat() if event_time is not None else None
        if latest_ts is not None:
            out[str(event.symbol)] = str(latest_ts)
    return dict(sorted(out.items()))


def _event_metric(event: Any, key: str) -> Any:
    raw_metrics = getattr(event, "raw_metrics", {}) or {}
    if key in raw_metrics:
        return raw_metrics.get(key)
    nested = raw_metrics.get("raw_metrics")
    if isinstance(nested, dict) and key in nested:
        return nested.get(key)
    return None


def _invalid_rejection_diagnostics(events: list[Any]) -> dict[str, Any]:
    invalid_events: list[tuple[str, str, str, str]] = []
    reason_counter: Counter[str] = Counter()
    by_symbol_events: Counter[str] = Counter()
    unique_by_symbol: dict[str, set[tuple[str, str, str, str]]] = defaultdict(set)
    last_by_symbol: dict[str, str] = {}

    for event in events:
        if event.event_type not in {"SIGNAL_REJECTED", "RISK_REJECTED"}:
            continue
        symbol = str(event.symbol or "UNKNOWN")
        latest_ts = _event_metric(event, "latest_timestamp")
        confirm_ts = _event_metric(event, "confirm_latest_timestamp")
        created_at = getattr(event, "created_at", None)
        fallback_ts = created_at.isoformat() if created_at is not None else "UNKNOWN"
        if latest_ts is not None:
            last_by_symbol[symbol] = str(latest_ts)
        elif confirm_ts is not None:
            last_by_symbol[symbol] = str(confirm_ts)
        else:
            last_by_symbol[symbol] = fallback_ts
        for reason in event.reason_codes:
            if not str(reason).startswith("INVALID_"):
                continue
            timeframe = "15m" if "15M" in str(reason) else "5m" if "5M" in str(reason) else "unknown"
            bar_ts = str(confirm_ts if timeframe == "15m" and confirm_ts is not None else latest_ts or fallback_ts)
            key = (symbol, timeframe, bar_ts, str(reason))
            invalid_events.append(key)
            reason_counter[str(reason)] += 1
            by_symbol_events[symbol] += 1
            unique_by_symbol[symbol].add(key)

    unique_keys = set(invalid_events)
    unique_by_symbol_counts = {symbol: len(keys) for symbol, keys in sorted(unique_by_symbol.items())}
    return {
        "invalid_event_count": len(invalid_events),
        "invalid_unique_bar_count": len(unique_keys),
        "invalid_unique_symbol_count": len(unique_by_symbol),
        "invalid_repeated_evaluation_count": max(0, len(invalid_events) - len(unique_keys)),
        "rejection_events_by_symbol": dict(sorted(by_symbol_events.items())),
        "rejection_unique_bars_by_symbol": unique_by_symbol_counts,
        "last_evaluated_timestamp_by_symbol": dict(sorted(last_by_symbol.items())),
        "top_10_repeated_rejection_reasons": dict(reason_counter.most_common(10)),
    }


def _count_rejection_categories(events: list[Any]) -> dict[str, int]:
    categories: Counter[str] = Counter()
    for event in events:
        if event.event_type == "RISK_REJECTED":
            categories["risk_limit_skips"] += len(event.reason_codes) or 1
        if event.event_type not in {"SIGNAL_REJECTED", "RISK_REJECTED"}:
            continue
        for reason in event.reason_codes:
            if "MARKET" in reason and ("MISSING" in reason or "PROXY" in reason):
                categories["market_proxy_missing"] += 1
            if "VWAP" in reason:
                categories["vwap_rejections"] += 1
            if "TRADE_VALUE" in reason or "LIQUIDITY" in reason:
                categories["liquidity_rejections"] += 1
            if "BREAKOUT" in reason:
                categories["breakout_rejections"] += 1
            if "VOLUME_SURGE" in reason:
                categories["volume_expansion_rejections"] += 1
            if any(token in reason for token in ["MISSING_", "INSUFFICIENT_", "DUPLICATE_", "INVALID_"]):
                categories["data_quality_rejections"] += 1
            if reason.startswith("NO_TRADE_") or reason.startswith("ZERO_VOLUME_"):
                categories["no_trade_rejections"] += 1
    return dict(categories)


def _parse_hhmm(value: str) -> time:
    hour, minute = value.split(":", 1)
    return time(hour=int(hour), minute=int(minute))


def _trade_details(trades: list[Any]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for trade in trades:
        details.append(
            {
                "strategy_id": trade.strategy_id,
                "symbol": trade.symbol,
                "entry_time": trade.entry_time.isoformat(),
                "entry_price": trade.entry_price,
                "quantity": int(trade.qty) if float(trade.qty).is_integer() else trade.qty,
                "notional_krw": trade.entry_notional_krw,
                "exit_time": trade.exit_time.isoformat(),
                "exit_price": trade.exit_price,
                "exit_reason": trade.exit_reason,
                "gross_pnl_krw": trade.gross_pnl,
                "net_pnl_krw": trade.net_pnl,
                "gross_return_pct": trade.gross_return_pct,
                "net_return_pct": trade.net_return_pct,
                "fees_krw": trade.fees_krw,
                "tax_krw": trade.tax_krw,
                "slippage_cost_krw": trade.slippage_cost_krw,
                "total_cost_krw": trade.costs,
                "signal_reason_codes": list(trade.signal_reason_codes),
            }
        )
    return details


class ReplayUniverseProvider:
    def __init__(self, base_provider: DayUniverseProvider, candidate_overrides_by_date: dict[str, list[str]] | None = None):
        self.base_provider = base_provider
        self.candidate_overrides_by_date = candidate_overrides_by_date or {}

    def get_universe_selection(self, trade_date: str | None = None) -> UniverseSelection:
        selection = self.base_provider.get_universe_selection(trade_date)
        if trade_date and trade_date in self.candidate_overrides_by_date:
            return UniverseSelection(
                trade_date=selection.trade_date,
                score_date=selection.score_date,
                candidates=list(self.candidate_overrides_by_date[trade_date]),
                source_universe=selection.source_universe,
                same_day_score_used=selection.same_day_score_used,
                lookahead_safe=selection.lookahead_safe,
                reason_codes=[*selection.reason_codes, "REPLAY_COLLECTED_ONLY_UNIVERSE"],
            )
        return selection

    def get_candidates(self, as_of_date: str | None = None) -> list[str]:
        return self.get_universe_selection(as_of_date).candidates


def run_day_backtest(
    bars_by_symbol: dict[str, dict[str, list[IntradayBar]]],
    swing_candidates_by_date: dict[str, list[str]],
    config: DayTradingConfig | None = None,
    market_bars_by_date: dict[str, list[IntradayBar]] | None = None,
    initial_equity: float | None = None,
) -> dict[str, Any]:
    cfg = config or DayTradingConfig()
    if not cfg.enabled:
        return {"status": "skipped", "skip_reason": "DAY_TRADING_DISABLED", "performance": {}}
    if cfg.normalized_mode == "LIVE":
        raise PermissionError("DAY backtest cannot run in LIVE mode")

    cfg = replace(cfg, mode="PAPER")
    bars_by_symbol = _apply_zero_volume_bar_policy_to_loaded_data(bars_by_symbol, cfg.zero_volume_bar_policy)
    tracker = DayPositionTracker()
    logger = DayTradeLogger()
    initial = float(initial_equity if initial_equity is not None else cfg.paper_initial_cash_krw)
    equity = initial
    cost_model = CostModel(cfg.commission_pct, cfg.transaction_tax_pct, cfg.slippage_pct)
    paper_account = PaperAccount(cfg)

    timestamps_by_date: dict[str, list[datetime]] = defaultdict(list)
    for bars_by_timeframe in bars_by_symbol.values():
        for bar in bars_by_timeframe.get(cfg.timeframe_primary, []):
            timestamps_by_date[bar.timestamp.date().isoformat()].append(bar.timestamp)

    for trade_date in sorted(swing_candidates_by_date):
        candidates = swing_candidates_by_date.get(trade_date, [])
        provider = StaticDayUniverseProvider(candidates)
        engine = DayTradingEngine(
            cfg,
            provider,
            position_tracker=tracker,
            logger=logger,
            cost_model=cost_model,
            paper_account=paper_account,
        )
        day_start_equity = equity
        for ts in sorted(set(timestamps_by_date.get(trade_date, []))):
            snapshot: dict[str, dict[str, list[IntradayBar]]] = {}
            for symbol, bars_by_timeframe in bars_by_symbol.items():
                snapshot[symbol] = {
                    cfg.timeframe_primary: _bars_until(bars_by_timeframe.get(cfg.timeframe_primary, []), ts),
                    cfg.timeframe_confirm: _bars_completed_by(
                        bars_by_timeframe.get(cfg.timeframe_confirm, []),
                        ts,
                        cfg.timeframe_confirm,
                    ),
                }
            engine.run_once(
                as_of_date=trade_date,
                intraday_data=snapshot,
                now=ts,
                market_bars=_bars_until(market_bars_by_date.get(trade_date, []), ts) if market_bars_by_date else None,
                equity=equity,
                day_start_equity=day_start_equity,
            )
            equity = initial + sum(t.net_pnl for t in tracker.closed_trades)

    analyzer = DayPerformanceAnalyzer(initial_equity=initial, cost_model=cost_model)
    paper_account_summary = paper_account.summary(tracker.closed_trades)
    return {
        "status": "ok",
        "trade_count": len(tracker.closed_trades),
        "trades": tracker.closed_trades,
        "trade_details": _trade_details(tracker.closed_trades),
        "log_events": logger.events,
        "performance": analyzer.analyze(tracker.closed_trades, logger.events),
        "paper_account": paper_account_summary,
        "zero_volume_policy_summary": _zero_volume_policy_summary(bars_by_symbol, cfg.zero_volume_bar_policy, events=logger.events),
    }


def run_day_replay_backtest(
    conn: sqlite3.Connection,
    start_date: str,
    end_date: str,
    config: DayTradingConfig | None = None,
    initial_equity: float | None = None,
    candidate_overrides_by_date: dict[str, list[str]] | None = None,
    trade_dates: list[str] | None = None,
) -> dict[str, Any]:
    cfg = config or DayTradingConfig()
    if not cfg.enabled:
        return {"status": "skipped", "skip_reason": "DAY_TRADING_DISABLED", "performance": {}}
    if cfg.normalized_mode == "LIVE":
        raise PermissionError("DAY replay backtest cannot run in LIVE mode")

    cfg = replace(cfg, mode="PAPER")
    provider = ReplayUniverseProvider(DayUniverseProvider(conn, cfg), candidate_overrides_by_date)
    tracker = DayPositionTracker()
    logger = DayTradeLogger()
    cost_model = CostModel(cfg.commission_pct, cfg.transaction_tax_pct, cfg.slippage_pct)
    paper_account = PaperAccount(cfg)
    context_builder = IntradayMarketContext(cfg, conn)
    signal_generator = DaySignalGenerator(cfg, context_builder=context_builder)
    engine = DayTradingEngine(
        cfg,
        provider,
        signal_generator=signal_generator,
        position_tracker=tracker,
        logger=logger,
        cost_model=cost_model,
        paper_account=paper_account,
    )
    initial = float(initial_equity if initial_equity is not None else cfg.paper_initial_cash_krw)
    equity = initial
    all_dates = [
        str(r["d"])
        for r in conn.execute(
            """
            SELECT DISTINCT COALESCE(date, substr(timestamp,1,10)) AS d
            FROM intraday_prices
            WHERE COALESCE(date, substr(timestamp,1,10))>=?
              AND COALESCE(date, substr(timestamp,1,10))<=?
            ORDER BY d
            """,
            (start_date, end_date),
        ).fetchall()
    ]
    if trade_dates is not None:
        allowed_dates = set(trade_dates)
        all_dates = [trade_date for trade_date in all_dates if trade_date in allowed_dates]
    candidate_counts: dict[str, int] = {}
    score_dates: dict[str, str | None] = {}
    last_bar_by_date: dict[str, str | None] = {}
    first_bar_by_date: dict[str, str | None] = {}
    market_first_bar_by_date: dict[str, str | None] = {}
    market_last_bar_by_date: dict[str, str | None] = {}
    per_date_summary: dict[str, dict[str, Any]] = {}
    zero_policy_summary_by_date: dict[str, dict[str, Any]] = {}
    replay_validation = {
        "future_candle_violations": 0,
        "market_future_candle_violations": 0,
        "incomplete_confirm_candle_violations": 0,
        "lookahead_score_violations": 0,
    }

    for trade_date in all_dates:
        selection = provider.get_universe_selection(trade_date)
        candidate_counts[trade_date] = len(selection.candidates)
        score_dates[trade_date] = selection.score_date
        if selection.same_day_score_used:
            replay_validation["lookahead_score_violations"] += 1
        symbols = list(selection.candidates)
        if cfg.market_proxy_symbol:
            symbols_with_market = sorted(set(symbols + [cfg.market_proxy_symbol]))
        else:
            symbols_with_market = symbols
        full_data = load_intraday_bars(
            conn,
            symbols_with_market,
            trade_date,
            [cfg.timeframe_primary, cfg.timeframe_confirm],
        )
        original_full_data = full_data
        zero_policy_summary_by_date[trade_date] = _zero_volume_policy_summary(original_full_data, cfg.zero_volume_bar_policy)
        full_data = _apply_zero_volume_bar_policy_to_loaded_data(original_full_data, cfg.zero_volume_bar_policy)
        market_full = (
            full_data.get(cfg.market_proxy_symbol, {}).get(cfg.timeframe_primary, [])
            if cfg.market_proxy_symbol
            else None
        )
        if market_full:
            market_first_bar_by_date[trade_date] = min(b.timestamp for b in market_full).isoformat()
            market_last_bar_by_date[trade_date] = max(b.timestamp for b in market_full).isoformat()
        else:
            market_first_bar_by_date[trade_date] = None
            market_last_bar_by_date[trade_date] = None
        timestamps = sorted(
            {
                b.timestamp
                for symbol in symbols
                for b in full_data.get(symbol, {}).get(cfg.timeframe_primary, [])
            }
        )
        first_bar_by_date[trade_date] = timestamps[0].isoformat() if timestamps else None
        last_bar_by_date[trade_date] = timestamps[-1].isoformat() if timestamps else None
        day_start_equity = equity
        event_start = len(logger.events)
        trade_start = len(tracker.closed_trades)
        for ts in timestamps:
            snapshot: dict[str, dict[str, list[IntradayBar]]] = {}
            for symbol in symbols:
                snapshot[symbol] = {
                    cfg.timeframe_primary: _bars_until(full_data.get(symbol, {}).get(cfg.timeframe_primary, []), ts),
                    cfg.timeframe_confirm: _bars_completed_by(
                        full_data.get(symbol, {}).get(cfg.timeframe_confirm, []),
                        ts,
                        cfg.timeframe_confirm,
                    ),
                }
                if any(b.timestamp > ts for b in snapshot[symbol][cfg.timeframe_primary]):
                    replay_validation["future_candle_violations"] += 1
                if any(b.timestamp > ts for b in snapshot[symbol][cfg.timeframe_confirm]):
                    replay_validation["future_candle_violations"] += 1
                if any(not _bar_completed_by(b, ts, cfg.timeframe_confirm) for b in snapshot[symbol][cfg.timeframe_confirm]):
                    replay_validation["incomplete_confirm_candle_violations"] += 1
            market_bars = _bars_until(market_full or [], ts) if market_full is not None else None
            if market_bars and any(b.timestamp > ts for b in market_bars):
                replay_validation["market_future_candle_violations"] += 1
            engine.run_once(
                as_of_date=trade_date,
                intraday_data=snapshot,
                now=ts,
                market_bars=market_bars,
                equity=equity,
                day_start_equity=day_start_equity,
            )
            equity = initial + sum(t.net_pnl for t in tracker.closed_trades)
        day_events = logger.events[event_start:]
        day_trades = tracker.closed_trades[trade_start:]
        day_event_counts = Counter(event.event_type for event in day_events)
        day_open_positions = [p for p in tracker.get_open_positions(cfg.strategy_id) if p.trade_date == trade_date]
        day_rejections = _count_rejections(day_events)
        last_bar = last_bar_by_date.get(trade_date)
        first_bar = first_bar_by_date.get(trade_date)
        market_first_bar = market_first_bar_by_date.get(trade_date)
        market_last_bar = market_last_bar_by_date.get(trade_date)
        missing_force_exit = last_bar is None or datetime.fromisoformat(last_bar).time() < _parse_hhmm(cfg.force_exit_time)
        session_start_clock = time(9, 0)
        partial_session = bool(
            (first_bar and datetime.fromisoformat(first_bar).time() > session_start_clock)
            or (cfg.market_proxy_symbol and not market_first_bar)
            or (market_first_bar and datetime.fromisoformat(market_first_bar).time() > session_start_clock)
            or missing_force_exit
        )
        per_date_summary[trade_date] = {
            "candidate_count": len(selection.candidates),
            "score_date": selection.score_date,
            "same_day_score_used": selection.same_day_score_used,
            "signal_count": int(day_event_counts.get("SIGNAL_CREATED", 0)),
            "paper_entry_count": int(day_event_counts.get("PAPER_ENTRY", 0)),
            "paper_exit_count": int(day_event_counts.get("PAPER_EXIT", 0)),
            "open_position_count_at_end": len(day_open_positions),
            "session_complete": bool(last_bar and not missing_force_exit and not partial_session and not day_open_positions),
            "partial_session": partial_session,
            "first_bar_time": first_bar,
            "last_bar_time": last_bar,
            "market_first_bar_time": market_first_bar,
            "market_last_bar_time": market_last_bar,
            "gross_return_sum": sum(t.gross_return_pct for t in day_trades),
            "net_return_sum": sum(t.net_return_pct for t in day_trades),
            "cost_impact": sum(t.gross_return_pct for t in day_trades) - sum(t.net_return_pct for t in day_trades),
            "gross_pnl_krw": sum(t.gross_pnl for t in day_trades),
            "net_pnl_krw": sum(t.net_pnl for t in day_trades),
            "fees_krw": sum(t.fees_krw for t in day_trades),
            "tax_krw": sum(t.tax_krw for t in day_trades),
            "slippage_cost_krw": sum(t.slippage_cost_krw for t in day_trades),
            "trade_details": _trade_details(day_trades),
            "top_rejection_reasons": dict(day_rejections.most_common(10)),
            "top_signal_zero_blocking_reasons": dict(day_rejections.most_common(5))
            if int(day_event_counts.get("SIGNAL_CREATED", 0)) == 0
            else {},
            "rejection_by_symbol": _count_rejections_by_symbol(day_events),
            "candidate_last_evaluated_at": _last_evaluation_by_symbol(day_events),
            "invalid_rejection_diagnostics": _invalid_rejection_diagnostics(day_events),
            "zero_volume_policy_summary": _zero_volume_policy_summary(original_full_data, cfg.zero_volume_bar_policy, events=day_events),
        }

    analyzer = DayPerformanceAnalyzer(initial_equity=initial, cost_model=cost_model)
    paper_account_summary = paper_account.summary(tracker.closed_trades)
    event_counts = Counter(event.event_type for event in logger.events)
    exit_reason_counts = Counter(trade.exit_reason for trade in tracker.closed_trades)
    open_positions = tracker.get_open_positions(cfg.strategy_id)
    force_exit_clock = _parse_hhmm(cfg.force_exit_time)
    missing_force_exit_dates = [
        trade_date
        for trade_date, last_bar in last_bar_by_date.items()
        if last_bar is None or datetime.fromisoformat(last_bar).time() < force_exit_clock
    ]
    session_start_clock = time(9, 0)
    partial_session_dates = [
        trade_date
        for trade_date in last_bar_by_date
        if (
            (first_bar_by_date.get(trade_date) and datetime.fromisoformat(str(first_bar_by_date[trade_date])).time() > session_start_clock)
            or (
                cfg.market_proxy_symbol
                and (
                    not market_first_bar_by_date.get(trade_date)
                    or datetime.fromisoformat(str(market_first_bar_by_date[trade_date])).time() > session_start_clock
                )
            )
            or trade_date in missing_force_exit_dates
        )
    ]
    paper_entry_count = int(event_counts.get("PAPER_ENTRY", 0))
    paper_exit_count = int(event_counts.get("PAPER_EXIT", 0))
    open_position_count = len(open_positions)
    session_complete = (
        bool(all_dates)
        and not missing_force_exit_dates
        and not partial_session_dates
        and open_position_count == 0
        and paper_exit_count >= paper_entry_count
    )
    session_audit = {
        "session_complete": bool(session_complete),
        "last_bar_time": max((v for v in last_bar_by_date.values() if v), default=None),
        "last_bar_time_by_date": last_bar_by_date,
        "first_bar_time_by_date": first_bar_by_date,
        "market_first_bar_time_by_date": market_first_bar_by_date,
        "market_last_bar_time_by_date": market_last_bar_by_date,
        "partial_session": bool(partial_session_dates),
        "partial_session_dates": partial_session_dates,
        "expected_force_exit_time": cfg.force_exit_time,
        "missing_force_exit_window": bool(missing_force_exit_dates),
        "missing_force_exit_dates": missing_force_exit_dates,
        "open_position_count_at_end": open_position_count,
        "open_positions_at_end": [
            {
                "strategy_id": p.strategy_id,
                "symbol": p.symbol,
                "qty": p.qty,
                "entry_price": p.entry_price,
                "opened_at": p.opened_at.isoformat(),
                "trade_date": p.trade_date,
                "stop_loss_price": p.stop_loss_price,
                "take_profit_price": p.take_profit_price,
            }
            for p in open_positions
        ],
    }
    policy_event_summary = _zero_volume_policy_summary({}, cfg.zero_volume_bar_policy, events=logger.events)
    total_no_trade = sum(int(summary.get("no_trade_bar_count", 0)) for summary in zero_policy_summary_by_date.values())
    total_positive = sum(int(summary.get("positive_volume_bar_count", 0)) for summary in zero_policy_summary_by_date.values())
    no_trade_by_symbol: Counter[str] = Counter()
    positive_by_symbol: Counter[str] = Counter()
    no_trade_by_timeframe: Counter[str] = Counter()
    positive_by_timeframe: Counter[str] = Counter()
    for summary in zero_policy_summary_by_date.values():
        no_trade_by_symbol.update(summary.get("no_trade_bar_count_by_symbol", {}) or {})
        positive_by_symbol.update(summary.get("positive_volume_bar_count_by_symbol", {}) or {})
        no_trade_by_timeframe.update(summary.get("no_trade_bar_count_by_timeframe", {}) or {})
        positive_by_timeframe.update(summary.get("positive_volume_bar_count_by_timeframe", {}) or {})
    zero_volume_policy_summary = {
        "zero_volume_bar_policy": cfg.zero_volume_bar_policy,
        "strict_invalid_count": total_no_trade if cfg.zero_volume_bar_policy == "strict_invalid" else 0,
        "no_trade_context_count": total_no_trade if cfg.zero_volume_bar_policy == "no_trade_context" else 0,
        "dropped_no_trade_count": total_no_trade if cfg.zero_volume_bar_policy == "drop_no_trade" else 0,
        "no_trade_bar_count": total_no_trade,
        "positive_volume_bar_count": total_positive,
        "no_trade_bar_count_by_symbol": dict(sorted(no_trade_by_symbol.items())),
        "positive_volume_bar_count_by_symbol": dict(sorted(positive_by_symbol.items())),
        "no_trade_bar_count_by_timeframe": dict(sorted(no_trade_by_timeframe.items())),
        "positive_volume_bar_count_by_timeframe": dict(sorted(positive_by_timeframe.items())),
        "no_trade_blocked_entry_count": policy_event_summary.get("no_trade_blocked_entry_count", 0),
        "no_trade_blocked_exit_count": policy_event_summary.get("no_trade_blocked_exit_count", 0),
        "vwap_positive_volume_only": cfg.zero_volume_bar_policy in {"no_trade_context", "drop_no_trade"},
        "no_trade_used_for_price_context": cfg.zero_volume_bar_policy == "no_trade_context",
        "policy_caveat": "Zero-volume policy comparison is a data-quality check; one-day smoke output is not profitability evidence.",
        "by_date": zero_policy_summary_by_date,
    }
    return {
        "status": "ok",
        "start_date": start_date,
        "end_date": end_date,
        "candidate_counts": candidate_counts,
        "score_dates": score_dates,
        "trades": tracker.closed_trades,
        "trade_details": _trade_details(tracker.closed_trades),
        "trade_count": len(tracker.closed_trades),
        "log_events": logger.events,
        "event_counts": dict(event_counts),
        "exit_reason_counts": dict(exit_reason_counts),
        "session_audit": session_audit,
        "zero_volume_policy_summary": zero_volume_policy_summary,
        "per_date_summary": per_date_summary,
        "rejection_analysis": {
            "overall_rejection_reasons": dict(_count_rejections(logger.events).most_common()),
            "rejection_categories": _count_rejection_categories(logger.events),
            "rejection_by_symbol": _count_rejections_by_symbol(logger.events),
            "rejection_by_date": {
                date: summary.get("top_rejection_reasons", {})
                for date, summary in per_date_summary.items()
            },
            "zero_signal_top_blocking_reasons_by_date": {
                date: summary.get("top_signal_zero_blocking_reasons", {})
                for date, summary in per_date_summary.items()
                if int(summary.get("signal_count", 0)) == 0
            },
            "candidate_last_evaluated_at_by_date": {
                date: summary.get("candidate_last_evaluated_at", {})
                for date, summary in per_date_summary.items()
            },
            "invalid_rejection_diagnostics": _invalid_rejection_diagnostics(logger.events),
            "invalid_rejection_diagnostics_by_date": {
                date: summary.get("invalid_rejection_diagnostics", {})
                for date, summary in per_date_summary.items()
            },
        },
        "performance": analyzer.analyze(tracker.closed_trades, logger.events),
        "paper_account": paper_account_summary,
        "lookahead_validation": replay_validation,
        "execution_assumption": "PAPER uses adverse slippage on entry/exit plus commission and transaction tax; no perfect fills assumed.",
    }
