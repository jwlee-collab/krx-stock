from __future__ import annotations

from statistics import median

from exp81_rules import classify
from exp82_io import row_key, stringify
from exp82_types import LABEL_ORDER, PriceRow, Record


def build_monitor_rows(
    latest: list[Record],
    history: list[Record],
    recorded_at: str,
    exp80d_index: dict[tuple[str, str], Record],
    prices: dict[str, list[PriceRow]],
) -> tuple[list[Record], list[Record], list[Record]]:
    snapshot = [monitor_row(row, recorded_at, exp80d_index, prices, "exp81_latest", True) for row in latest]
    tracking_history = [monitor_row(row, recorded_at, exp80d_index, prices, "exp80a_reclassified_history", False) for row in history]
    tracking = sorted([*tracking_history, *snapshot], key=lambda row: (str(row.get("as_of_date")), int(fnum(row.get("candidate_rank")) or 999999)))
    summary = performance_summary(tracking)
    return snapshot, tracking, summary


def monitor_row(
    row: Record,
    recorded_at: str,
    exp80d_index: dict[tuple[str, str], Record],
    prices: dict[str, list[PriceRow]],
    source: str,
    preserve_label: bool,
) -> Record:
    as_of_date = stringify(row.get("as_of_date") or row.get("date"))
    symbol = stringify(row.get("symbol")).zfill(6)
    label, reason = label_reason(row, preserve_label)
    exp80d = exp80d_index.get((as_of_date, symbol), {})
    out: Record = {
        "recorded_at": recorded_at,
        "as_of_date": as_of_date,
        "symbol": symbol,
        "symbol_name": row.get("symbol_name"),
        "candidate_rank": row.get("candidate_rank"),
        "original_rank": row.get("original_rank"),
        "score": row.get("score"),
        "shadow_label": label,
        "shadow_reason": reason,
        "headline_proxy_score": row.get("headline_proxy_score"),
        "hot_range_flag": row.get("hot_range_flag"),
        "hot_giveback_flag": row.get("hot_giveback_flag"),
        "hot_ret_1d_flag": row.get("hot_ret_1d_flag"),
        "hot_ret_5d_flag": row.get("hot_ret_5d_flag"),
        "hot_volume_flag": row.get("hot_volume_flag"),
        "overextended_sma5_flag": row.get("overextended_sma5_flag"),
        "range_pct": row.get("range_pct"),
        "ret_1d": row.get("ret_1d"),
        "ret_5d": row.get("ret_5d"),
        "volume_z20": row.get("volume_z20"),
        "sma5_gap": row.get("sma5_gap"),
        "exp80d_best_rule": "range_cooldown_then_reclaim",
        "exp80d_best_entry_status": exp80d.get("entry_status") or row.get("exp80d_best_rule_entry_status"),
        "exp80d_best_entry_date": exp80d.get("entry_date") or row.get("exp80d_best_rule_entry_date"),
        "exp80d_best_entry_to_5d_return": exp80d.get("entry_to_5d_return") or row.get("exp80d_best_rule_entry_to_5d_return"),
        "monitor_source": source,
    }
    out.update(outcome_from_prices(row, prices))
    return out


def label_reason(row: Record, preserve_label: bool) -> tuple[str, str]:
    if preserve_label and stringify(row.get("shadow_label")):
        return stringify(row.get("shadow_label")), stringify(row.get("shadow_reason"))
    return classify(row)


def outcome_from_prices(row: Record, prices: dict[str, list[PriceRow]]) -> Record:
    as_of_date = stringify(row.get("as_of_date") or row.get("date"))
    symbol = stringify(row.get("symbol")).zfill(6)
    series = prices.get(symbol, [])
    date_index = {price.date: idx for idx, price in enumerate(series)}
    idx = date_index.get(as_of_date)
    computed = compute_outcome(series, idx)
    if computed["outcome_status"] == "PENDING":
        return fallback_outcome(row, computed)
    return computed


def compute_outcome(series: list[PriceRow], idx: int | None) -> Record:
    if idx is None:
        return {"outcome_status": "PENDING"}
    base = series[idx].close
    f1 = close_return(series, idx, base, 1)
    f3 = close_return(series, idx, base, 3)
    f5 = close_return(series, idx, base, 5)
    f10 = close_return(series, idx, base, 10)
    low3 = min_low_return(series, idx, base, 3)
    low5 = min_low_return(series, idx, base, 5)
    status = "COMPLETE_5D" if f5 is not None and low5 is not None else "COMPLETE_3D" if f3 is not None else "COMPLETE_1D" if f1 is not None else "PENDING"
    return {
        "outcome_status": status,
        "forward_1d_return": f1,
        "forward_3d_return": f3,
        "forward_5d_return": f5,
        "forward_10d_return": f10,
        "forward_3d_min_low_return": low3,
        "forward_5d_min_low_return": low5,
        "forward_5d_mdd": low5,
        "next_day_crash_flag": bool_flag(f1 is not None and f1 <= -0.05) if f1 is not None else None,
        "next_5d_crash_flag": bool_flag(low5 is not None and low5 <= -0.10) if low5 is not None else None,
        "next_5d_winner_flag": bool_flag(f5 is not None and f5 >= 0.05) if f5 is not None else None,
    }


def fallback_outcome(row: Record, computed: Record) -> Record:
    out = dict(computed)
    for col in ("forward_1d_return", "forward_3d_return", "forward_5d_return", "forward_10d_return", "forward_3d_min_low_return", "forward_5d_min_low_return", "forward_5d_mdd", "next_day_crash_flag", "next_5d_crash_flag", "next_5d_winner_flag"):
        out[col] = row.get(col)
    status = stringify(row.get("outcome_status"))
    out["outcome_status"] = status.upper() if status else "PENDING"
    return out


def performance_summary(rows: list[Record]) -> list[Record]:
    out: list[Record] = []
    for label in LABEL_ORDER:
        subset = [row for row in rows if row.get("shadow_label") == label]
        completed_1d = [row for row in subset if fnum(row.get("forward_1d_return")) is not None]
        completed_5d = [row for row in subset if fnum(row.get("forward_5d_return")) is not None]
        out.append(summary_row(label, subset, completed_1d, completed_5d))
    return out


def summary_row(label: str, rows: list[Record], completed_1d: list[Record], completed_5d: list[Record]) -> Record:
    return {
        "shadow_label": label,
        "row_count": len(rows),
        "completed_1d_count": len(completed_1d),
        "completed_5d_count": len(completed_5d),
        "pending_count": len(rows) - len(completed_5d),
        "avg_forward_1d_return": avg(completed_1d, "forward_1d_return"),
        "avg_forward_3d_return": avg(rows, "forward_3d_return"),
        "avg_forward_5d_return": avg(completed_5d, "forward_5d_return"),
        "median_forward_5d_return": med(completed_5d, "forward_5d_return"),
        "next_day_crash_rate": flag_rate(completed_1d, "next_day_crash_flag"),
        "next_5d_crash_rate": flag_rate(completed_5d, "next_5d_crash_flag"),
        "next_5d_winner_rate": flag_rate(completed_5d, "next_5d_winner_flag"),
        "avg_forward_5d_mdd": avg(completed_5d, "forward_5d_mdd"),
        "avg_headline_proxy_score": avg(rows, "headline_proxy_score"),
        "avg_range_pct": avg(rows, "range_pct"),
        "avg_volume_z20": avg(rows, "volume_z20"),
    }


def close_return(series: list[PriceRow], idx: int, base: float, days: int) -> float | None:
    target = idx + days
    return None if target >= len(series) else series[target].close / base - 1.0


def min_low_return(series: list[PriceRow], idx: int, base: float, days: int) -> float | None:
    if idx + days >= len(series):
        return None
    lows = [series[pos].low for pos in range(idx + 1, idx + days + 1)]
    return min(lows) / base - 1.0 if lows else None


def avg(rows: list[Record], col: str) -> float | None:
    values = [value for row in rows if (value := fnum(row.get(col))) is not None]
    return None if not values else sum(values) / len(values)


def med(rows: list[Record], col: str) -> float | None:
    values = [value for row in rows if (value := fnum(row.get(col))) is not None]
    return None if not values else float(median(values))


def flag_rate(rows: list[Record], col: str) -> float | None:
    values = [int(value) for row in rows if (value := fnum(row.get(col))) is not None]
    return None if not values else sum(values) / len(values)


def bool_flag(value: bool) -> int:
    return 1 if value else 0


def fnum(value: object) -> float | None:
    text = stringify(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None
