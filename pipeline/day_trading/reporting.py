from __future__ import annotations

from pathlib import Path
from typing import Any


def _event_count(replay_result: dict[str, Any], event_type: str) -> int:
    event_counts = replay_result.get("event_counts") or {}
    if event_type in event_counts:
        return int(event_counts[event_type])
    count = 0
    for event in replay_result.get("log_events", []):
        if getattr(event, "event_type", None) == event_type:
            count += 1
    return count


def build_day_validation_markdown(
    replay_result: dict[str, Any],
    data_quality: dict[str, Any],
    gate_result: dict[str, Any],
    universe_source: str,
    same_day_scores_allowed: bool,
    market_proxy_symbol: str | None,
) -> str:
    perf = replay_result.get("performance", {})
    lookahead = replay_result.get("lookahead_validation", {})
    exit_counts = replay_result.get("exit_reason_counts", {})
    audit = replay_result.get("session_audit", {})
    per_date = replay_result.get("per_date_summary", {})
    rejection_analysis = replay_result.get("rejection_analysis", {})
    data_availability = replay_result.get("data_availability", {})
    coverage = replay_result.get("coverage_audit", {})
    invalid_bar_analysis = replay_result.get("invalid_bar_analysis", {})
    zero_policy = replay_result.get("zero_volume_policy_summary", {})
    paper_account = replay_result.get("paper_account", {}) or {}
    trade_details = replay_result.get("trade_details", []) or []
    lines = [
        f"# DAY Strategy Validation Report ({replay_result.get('start_date')} ~ {replay_result.get('end_date')})",
        "",
        "## Configuration",
        f"- universe_source: {universe_source}",
        f"- same_day_scores_allowed: {same_day_scores_allowed}",
        f"- market_proxy_symbol: {market_proxy_symbol or 'MISSING'}",
        f"- execution_assumption: {replay_result.get('execution_assumption', 'PAPER conservative fills')}",
        "",
        "## Data Quality",
        f"- intraday_rows: {data_quality.get('row_count', 0)}",
        f"- symbol_count: {data_quality.get('symbol_count', 0)}",
        f"- usable_symbol_count: {data_quality.get('day_strategy_usable_symbol_count', 0)}",
        f"- candidate_usable_symbol_count: {data_quality.get('candidate_usable_symbol_count', 0)}",
        f"- market_proxy_available: {data_quality.get('market_proxy_available')}",
        f"- duplicate_count: {data_quality.get('duplicate_count', 0)}",
        "",
        "## Universe And Lookahead",
        f"- score_dates: {replay_result.get('score_dates', {})}",
        f"- candidate_counts: {replay_result.get('candidate_counts', {})}",
        f"- lookahead_validation: {lookahead}",
        "",
        "## Coverage Audit",
        f"- requested_top_n: {coverage.get('requested_top_n')}",
        f"- collected_symbol_count: {coverage.get('collected_symbol_count')}",
        f"- collected_symbols: {coverage.get('collected_symbols', [])}",
        f"- missing_intraday_symbol_count: {coverage.get('missing_intraday_symbol_count')}",
        f"- missing_intraday_symbols: {coverage.get('missing_intraday_symbols', [])}",
        f"- intraday_coverage_ratio: {coverage.get('intraday_coverage_ratio')}",
        f"- partial_universe: {coverage.get('partial_universe')}",
        f"- replay_collected_only: {coverage.get('replay_collected_only')}",
        f"- require_full_top_n_coverage: {coverage.get('require_full_top_n_coverage')}",
        f"- market_proxy_symbol: {coverage.get('market_proxy_symbol', market_proxy_symbol or 'MISSING')}",
        f"- market_proxy_available: {coverage.get('market_proxy_available', data_quality.get('market_proxy_available'))}",
        "",
        "## Invalid Bar Analysis",
        f"- invalid_bar_count_by_symbol: {invalid_bar_analysis.get('invalid_bar_count_by_symbol', {})}",
        f"- invalid_bar_count_by_timeframe: {invalid_bar_analysis.get('invalid_bar_count_by_timeframe', {})}",
        f"- invalid_bar_count_by_reason: {invalid_bar_analysis.get('invalid_bar_count_by_reason', {})}",
        f"- zero_volume_count: {invalid_bar_analysis.get('zero_volume_count', 0)}",
        f"- invalid_ohlc_count: {invalid_bar_analysis.get('invalid_ohlc_count', 0)}",
        f"- missing_price_count: {invalid_bar_analysis.get('missing_price_count', 0)}",
        f"- incomplete_aggregation_count: {invalid_bar_analysis.get('incomplete_aggregation_count', 0)}",
        f"- zero_volume_positive_amount_count: {invalid_bar_analysis.get('zero_volume_positive_amount_count', 0)}",
        f"- positive_volume_zero_amount_count: {invalid_bar_analysis.get('positive_volume_zero_amount_count', 0)}",
        f"- estimated_traded_value_count: {invalid_bar_analysis.get('estimated_traded_value_count', 0)}",
        f"- cumulative_volume_diff_used: {invalid_bar_analysis.get('cumulative_volume_diff_used')}",
        f"- cumulative_amount_diff_used: {invalid_bar_analysis.get('cumulative_amount_diff_used')}",
        f"- negative_cumulative_diff_count: {invalid_bar_analysis.get('negative_cumulative_diff_count', 0)}",
        f"- raw_volume_field_used: {invalid_bar_analysis.get('raw_volume_field_used')}",
        f"- raw_amount_field_used: {invalid_bar_analysis.get('raw_amount_field_used')}",
        f"- raw_price_fields_used: {invalid_bar_analysis.get('raw_price_fields_used', {})}",
        f"- invalid_mapping_sample_rows: {invalid_bar_analysis.get('invalid_mapping_sample_rows', [])}",
        f"- invalid_bar_count_by_timestamp: {invalid_bar_analysis.get('invalid_bar_count_by_timestamp', {})}",
        f"- zero_volume_cause_counts: {invalid_bar_analysis.get('zero_volume_cause_counts', {})}",
        f"- first_invalid_timestamp: {invalid_bar_analysis.get('first_invalid_timestamp')}",
        f"- last_invalid_timestamp: {invalid_bar_analysis.get('last_invalid_timestamp')}",
        f"- invalid_bar_sample_rows: {invalid_bar_analysis.get('invalid_bar_sample_rows', [])}",
        "",
        "## Zero-Volume Policy",
        f"- zero_volume_bar_policy: {zero_policy.get('zero_volume_bar_policy')}",
        f"- strict_invalid_count: {zero_policy.get('strict_invalid_count', 0)}",
        f"- no_trade_context_count: {zero_policy.get('no_trade_context_count', 0)}",
        f"- dropped_no_trade_count: {zero_policy.get('dropped_no_trade_count', 0)}",
        f"- no_trade_bar_count: {zero_policy.get('no_trade_bar_count', 0)}",
        f"- positive_volume_bar_count: {zero_policy.get('positive_volume_bar_count', 0)}",
        f"- no_trade_bar_count_by_symbol: {zero_policy.get('no_trade_bar_count_by_symbol', {})}",
        f"- positive_volume_bar_count_by_symbol: {zero_policy.get('positive_volume_bar_count_by_symbol', {})}",
        f"- no_trade_blocked_entry_count: {zero_policy.get('no_trade_blocked_entry_count', 0)}",
        f"- no_trade_blocked_exit_count: {zero_policy.get('no_trade_blocked_exit_count', 0)}",
        f"- vwap_positive_volume_only: {zero_policy.get('vwap_positive_volume_only')}",
        f"- no_trade_used_for_price_context: {zero_policy.get('no_trade_used_for_price_context')}",
        f"- policy_caveat: {zero_policy.get('policy_caveat', 'Zero-volume policy output is for data quality analysis, not profitability claims.')}",
        "",
        "## Per-Date Summary",
    ]
    if per_date:
        lines.append("| date | candidates | usable | signals | entries | exits | open_end | complete | gross | net | cost | top_rejections |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | --- |")
        for date, summary in sorted(per_date.items()):
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(date),
                        str(summary.get("candidate_count", 0)),
                        str(summary.get("candidate_usable_symbol_count", 0)),
                        str(summary.get("signal_count", 0)),
                        str(summary.get("paper_entry_count", 0)),
                        str(summary.get("paper_exit_count", 0)),
                        str(summary.get("open_position_count_at_end", 0)),
                        str(summary.get("session_complete")),
                        str(summary.get("gross_return_sum", 0.0)),
                        str(summary.get("net_return_sum", 0.0)),
                        str(summary.get("cost_impact", 0.0)),
                        str(summary.get("top_rejection_reasons", {})),
                    ]
                )
                + " |"
            )
    else:
        lines.append("- no replay dates")
    lines.extend(
        [
            "",
            "## Rejection Analysis",
            f"- overall_rejection_reasons: {rejection_analysis.get('overall_rejection_reasons', {})}",
            f"- rejection_categories: {rejection_analysis.get('rejection_categories', {})}",
            f"- rejection_by_symbol: {rejection_analysis.get('rejection_by_symbol', {})}",
            f"- rejection_by_date: {rejection_analysis.get('rejection_by_date', {})}",
            f"- zero_signal_top_blocking_reasons_by_date: {rejection_analysis.get('zero_signal_top_blocking_reasons_by_date', {})}",
            f"- candidate_last_evaluated_at_by_date: {rejection_analysis.get('candidate_last_evaluated_at_by_date', {})}",
            f"- invalid_rejection_diagnostics: {rejection_analysis.get('invalid_rejection_diagnostics', {})}",
            f"- invalid_rejection_diagnostics_by_date: {rejection_analysis.get('invalid_rejection_diagnostics_by_date', {})}",
            "",
            "## Data Availability By Replay Range",
            f"- summary: {data_availability.get('summary', {})}",
            f"- replayable_dates: {data_availability.get('replayable_dates', [])}",
            f"- unreplayable_dates: {data_availability.get('unreplayable_dates', {})}",
            "",
        "## Signals And Trades",
        f"- signal_created_count: {_event_count(replay_result, 'SIGNAL_CREATED')}",
        f"- paper_entry_count: {_event_count(replay_result, 'PAPER_ENTRY')}",
        f"- paper_exit_count: {_event_count(replay_result, 'PAPER_EXIT')}",
        f"- take_profit_exit_count: {exit_counts.get('TAKE_PROFIT', 0)}",
        f"- stop_loss_exit_count: {exit_counts.get('STOP_LOSS', 0)}",
        f"- end_of_day_exit_count: {exit_counts.get('END_OF_DAY', 0)}",
        f"- rejected_signal_count: {perf.get('rejected_signal_count', 0)}",
        f"- rejected_by_reason: {perf.get('rejected_by_reason', {})}",
        f"- total_trades: {perf.get('total_trades', 0)}",
        ]
    )
    lines.extend(
        [
            "",
            "## Paper Account Summary",
            f"- initial_cash_krw: {paper_account.get('initial_cash_krw', 0.0)}",
            f"- ending_cash_krw: {paper_account.get('ending_cash_krw', 0.0)}",
            f"- ending_equity_krw: {paper_account.get('ending_equity_krw', 0.0)}",
            f"- realized_pnl_krw: {paper_account.get('realized_pnl_krw', 0.0)}",
            f"- unrealized_pnl_krw: {paper_account.get('unrealized_pnl_krw', 0.0)}",
            f"- daily_return_pct: {paper_account.get('daily_return_pct', 0.0)}",
            f"- total_trades: {paper_account.get('total_trades', perf.get('total_trades', 0))}",
            f"- winning_trades: {paper_account.get('winning_trades', 0)}",
            f"- losing_trades: {paper_account.get('losing_trades', 0)}",
            f"- fees_krw: {paper_account.get('fees_krw', perf.get('fees_krw', 0.0))}",
            f"- tax_krw: {paper_account.get('tax_krw', perf.get('tax_krw', 0.0))}",
            f"- slippage_cost_krw: {paper_account.get('slippage_cost_krw', perf.get('slippage_cost_krw', 0.0))}",
            f"- total_cost_krw: {paper_account.get('total_cost_krw', perf.get('total_cost_krw', 0.0))}",
            f"- max_exposure_krw: {paper_account.get('max_exposure_krw', 0.0)}",
            f"- exposure_limit_krw: {paper_account.get('exposure_limit_krw', 0.0)}",
            f"- cash_rejection_count: {paper_account.get('cash_rejection_count', 0)}",
            f"- exposure_rejection_count: {paper_account.get('exposure_rejection_count', 0)}",
            f"- daily_loss_rejection_count: {paper_account.get('daily_loss_rejection_count', 0)}",
            "",
            "## Trade Details",
        ]
    )
    if trade_details:
        lines.append("| symbol | entry_time | entry_price | quantity | notional_krw | exit_time | exit_price | exit_reason | gross_pnl_krw | net_pnl_krw | gross_return_pct | net_return_pct | fees_krw | tax_krw | slippage_cost_krw | signal_reason_codes |")
        lines.append("| --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |")
        for trade in trade_details:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(trade.get("symbol")),
                        str(trade.get("entry_time")),
                        str(trade.get("entry_price")),
                        str(trade.get("quantity")),
                        str(trade.get("notional_krw")),
                        str(trade.get("exit_time")),
                        str(trade.get("exit_price")),
                        str(trade.get("exit_reason")),
                        str(trade.get("gross_pnl_krw")),
                        str(trade.get("net_pnl_krw")),
                        str(trade.get("gross_return_pct")),
                        str(trade.get("net_return_pct")),
                        str(trade.get("fees_krw")),
                        str(trade.get("tax_krw")),
                        str(trade.get("slippage_cost_krw")),
                        str(trade.get("signal_reason_codes", [])),
                    ]
                )
                + " |"
            )
    else:
        lines.append("- no closed PAPER trades")
    lines.extend(
        [
        "",
        "## Session Audit",
        f"- session_complete: {audit.get('session_complete')}",
        f"- partial_session: {audit.get('partial_session')}",
        f"- partial_session_dates: {audit.get('partial_session_dates', [])}",
        f"- last_bar_time: {audit.get('last_bar_time')}",
        f"- first_bar_time_by_date: {audit.get('first_bar_time_by_date', {})}",
        f"- market_first_bar_time_by_date: {audit.get('market_first_bar_time_by_date', {})}",
        f"- market_last_bar_time_by_date: {audit.get('market_last_bar_time_by_date', {})}",
        f"- expected_force_exit_time: {audit.get('expected_force_exit_time')}",
        f"- missing_force_exit_window: {audit.get('missing_force_exit_window')}",
        f"- open_position_count_at_end: {audit.get('open_position_count_at_end')}",
        f"- open_positions_at_end: {audit.get('open_positions_at_end', [])}",
        ]
    )
    lines.extend(
        [
        "",
        "## Performance",
        f"- win_rate: {perf.get('win_rate', 0.0)}",
        f"- average_gain: {perf.get('average_gain', 0.0)}",
        f"- average_loss: {perf.get('average_loss', 0.0)}",
        f"- expectancy_per_trade: {perf.get('expectancy_per_trade', 0.0)}",
        f"- profit_factor: {perf.get('profit_factor', 0.0)}",
        f"- max_drawdown: {perf.get('max_drawdown', 0.0)}",
        f"- max_consecutive_losses: {perf.get('max_consecutive_losses', 0)}",
        f"- average_holding_minutes: {perf.get('average_holding_minutes', 0.0)}",
        "- return_basis_note: gross_return_sum/net_return_sum are trade return aggregates; daily_return_pct is account-equity based.",
        f"- gross_return: {perf.get('gross_return_sum', 0.0)}",
        f"- net_return: {perf.get('net_return_sum', 0.0)}",
        f"- gross_return_sum: {perf.get('gross_return_sum', 0.0)}",
        f"- net_return_sum: {perf.get('net_return_sum', 0.0)}",
        f"- cost_impact: {perf.get('cost_impact', 0.0)}",
        f"- gross_pnl_krw: {perf.get('gross_pnl_krw', 0.0)}",
        f"- net_pnl_krw: {perf.get('net_pnl_krw', 0.0)}",
        f"- fees_krw: {perf.get('fees_krw', 0.0)}",
        f"- tax_krw: {perf.get('tax_krw', 0.0)}",
        f"- slippage_cost_krw: {perf.get('slippage_cost_krw', 0.0)}",
        f"- total_cost_krw: {perf.get('total_cost_krw', 0.0)}",
        f"- slippage_sensitivity: {perf.get('slippage_sensitivity', {})}",
        ]
    )
    lines.extend(
        [
        "",
        "## Promotion Gate",
        f"- approved: {gate_result.get('approved')}",
        f"- readiness_stage: {gate_result.get('readiness_stage')}",
        f"- reasons: {gate_result.get('reasons', [])}",
        "",
        "## Recommended Next Actions",
        ]
    )
    reasons = gate_result.get("reasons", [])
    if not reasons:
        lines.append("- Continue PAPER monitoring; do not enable LIVE without human review.")
    else:
        for reason in reasons:
            lines.append(f"- Address gate failure: {reason}")
    return "\n".join(lines) + "\n"


def write_day_validation_report(path: str | Path, markdown: str) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(markdown, encoding="utf-8")
    return out
