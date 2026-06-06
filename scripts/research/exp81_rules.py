from __future__ import annotations

from exp81_io import row_key, stringify
from exp81_types import Cell, Record, ShadowLabel


def build_dashboard_rows(
    candidates: list[Record],
    as_of_date: str,
    source: str,
    exp80d_index: dict[tuple[str, str], Record],
    exp80c_index: dict[tuple[str, str], Record],
) -> list[Record]:
    rows: list[Record] = []
    for candidate in candidates:
        row = dashboard_row(candidate, as_of_date, source, exp80d_index, exp80c_index)
        rows.append(row)
    return rows


def dashboard_row(
    candidate: Record,
    as_of_date: str,
    source: str,
    exp80d_index: dict[tuple[str, str], Record],
    exp80c_index: dict[tuple[str, str], Record],
) -> Record:
    key = row_key(candidate)
    exp80d = exp80d_index.get(key, {})
    exp80c = exp80c_index.get(key, {})
    label, reason = classify(candidate)
    row: Record = {
        "as_of_date": as_of_date,
        "symbol": stringify(candidate.get("symbol")).zfill(6),
        "symbol_name": candidate.get("symbol_name"),
        "candidate_rank": candidate.get("candidate_rank"),
        "original_rank": candidate.get("original_rank"),
        "score": candidate.get("score"),
        "current_holding_flag": candidate.get("is_current_holding") or 0,
        "candidate_source": candidate_source(source, candidate),
        "shadow_label": label,
        "shadow_reason": reason,
        "headline_proxy_score": candidate.get("headline_proxy_score"),
        "hot_range_flag": candidate.get("hot_range_flag"),
        "hot_giveback_flag": candidate.get("hot_giveback_flag"),
        "hot_ret_1d_flag": candidate.get("hot_ret_1d_flag"),
        "hot_ret_5d_flag": candidate.get("hot_ret_5d_flag"),
        "hot_volume_flag": candidate.get("hot_volume_flag"),
        "overextended_sma5_flag": candidate.get("overextended_sma5_flag"),
        "range_pct": candidate.get("range_pct"),
        "ret_1d": candidate.get("ret_1d"),
        "ret_5d": candidate.get("ret_5d"),
        "volume_z20": candidate.get("volume_z20"),
        "sma5_gap": candidate.get("sma5_gap"),
        "forward_1d_return": candidate.get("forward_1d_return"),
        "forward_3d_return": candidate.get("forward_3d_return"),
        "forward_5d_return": candidate.get("forward_5d_return"),
        "exp80d_best_rule_entry_status": exp80d.get("entry_status"),
        "exp80d_best_rule_entry_date": exp80d.get("entry_date"),
        "exp80d_best_rule_entry_to_5d_return": exp80d.get("entry_to_5d_return"),
        "exp80c_action_label": exp80c.get("action_label"),
        "outcome_status": candidate.get("outcome_status"),
    }
    return row


def classify(row: Record) -> tuple[ShadowLabel, str]:
    missing = missing_feature(row)
    if missing:
        return "PENDING_DATA", missing
    headline = fnum(row.get("headline_proxy_score")) or 0.0
    rank = fnum(row.get("candidate_rank")) or 999999.0
    hot_range_flag = flag(row, "hot_range_flag")
    high_risk = hot_range_flag and flag(row, "hot_giveback_flag")
    high_risk = high_risk or headline >= 3.0
    high_risk = high_risk or (flag(row, "hot_ret_5d_flag") and flag(row, "hot_volume_flag"))
    if high_risk:
        return "HIGH_RISK_OVERHEAT", risk_reason(row, "high risk overheat")
    if hot_range_flag and headline <= 1.0 and rank <= 5.0:
        return "DELAY_REVIEW", risk_reason(row, "hot range but top-ranked; verify 1-2D path")
    if hot_range_flag:
        return "COOLDOWN_RECLAIM_WATCH", risk_reason(row, "wait for range cooldown and reclaim")
    if rank > 10.0 or headline >= 2.0 or flag(row, "hot_giveback_flag") or flag(row, "overextended_sma5_flag"):
        return "LOWER_PRIORITY", risk_reason(row, "reference candidate; rank or risk is less favorable")
    return "IMMEDIATE_CANDIDATE", risk_reason(row, "low overheat flags; shadow immediate bucket")


def missing_feature(row: Record) -> str:
    required = ("headline_proxy_score", "hot_range_flag", "hot_giveback_flag", "hot_ret_5d_flag", "hot_volume_flag")
    missing = [name for name in required if stringify(row.get(name)) == ""]
    return "" if not missing else "missing feature: " + ",".join(missing)


def risk_reason(row: Record, prefix: str) -> str:
    flags: list[str] = []
    for name in ("hot_range_flag", "hot_giveback_flag", "hot_ret_1d_flag", "hot_ret_5d_flag", "hot_volume_flag", "overextended_sma5_flag"):
        if flag(row, name):
            flags.append(name.replace("_flag", ""))
    forward = "forward pending" if stringify(row.get("forward_5d_return")) == "" else "forward known"
    flag_text = "flags=" + ",".join(flags) if flags else "flags=none"
    return f"{prefix}; {flag_text}; headline={stringify(row.get('headline_proxy_score'))}; {forward}"


def candidate_source(source: str, row: Record) -> str:
    rank = fnum(row.get("candidate_rank")) or 999999.0
    if rank <= 5.0:
        bucket = "top5_current_candidate_proxy"
    elif rank <= 10.0:
        bucket = "top10_report_candidate_proxy"
    else:
        bucket = "reference_candidate_proxy"
    return f"{source}:{bucket}"


def flag(row: Record, name: str) -> bool:
    value = stringify(row.get(name)).strip().lower()
    return value in {"1", "1.0", "true", "yes"}


def fnum(value: Cell) -> float | None:
    text = stringify(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None
