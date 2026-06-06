from __future__ import annotations

from typing import Union

Cell = Union[str, int, float, bool, None]
Record = dict[str, Cell]
LABEL_ORDER = (
    "IMMEDIATE_CANDIDATE",
    "DELAY_REVIEW",
    "COOLDOWN_RECLAIM_WATCH",
    "HIGH_RISK_OVERHEAT",
    "LOWER_PRIORITY",
    "PENDING_DATA",
)


def markdown_report(rows: list[Record], perf: list[Record], quality: Record) -> str:
    counts = label_counts(rows)
    lines = [
        "# Exp83 Candidate Shadow Daily Summary",
        "",
        "## 1. Header",
        header_lines(quality),
        "",
        "## 2. Label Counts",
        counts_table(counts),
        "",
        "## 3. Immediate Candidates",
        candidate_table(rows, perf, "IMMEDIATE_CANDIDATE", 5, "historical immediate bucket"),
        "",
        "## 4. Delay Review",
        candidate_table(rows, perf, "DELAY_REVIEW", 5, "wait for 1-2D confirmation"),
        "",
        "## 5. Cooldown / Reclaim Watch",
        watch_table(rows, "COOLDOWN_RECLAIM_WATCH", 5),
        "",
        "## 6. High Risk Overheat",
        risk_table(rows, perf, 10),
        "",
        "## 7. Lower Priority",
        candidate_table(rows, perf, "LOWER_PRIORITY", 10, "reference only"),
        "",
        "## 8. Pending Data",
        candidate_table(rows, perf, "PENDING_DATA", 10, "feature or forward data pending"),
        "",
        "## 9. Label Performance Summary",
        performance_table(perf),
        "",
        "## 10. Interpretation",
        interpretation_lines(),
        "",
        "## 11. Safety Flags",
        "- production_change=false",
        "- paper_trading_change=false",
        "- real_order_change=false",
    ]
    return "\n".join(lines) + "\n"


def header_lines(quality: Record) -> str:
    return "\n".join(
        [
            f"- as_of_date={quality.get('as_of_date')}",
            f"- paper latest_signal_date={quality.get('paper_latest_signal_date')}",
            f"- paper latest_holdings_date={quality.get('paper_latest_holdings_date')}",
            f"- ops ledger status={quality.get('ops_ledger_status')}",
            f"- stale/missing warning={quality.get('stale_or_missing_warning')}",
        ]
    )


def counts_table(counts: dict[str, int]) -> str:
    rows = ["| label | count |", "|---|---:|"]
    rows.extend(f"| {label} | {counts.get(label, 0)} |" for label in LABEL_ORDER)
    return "\n".join(rows)


def candidate_table(rows: list[Record], perf: list[Record], label: str, limit: int, note_prefix: str) -> str:
    subset = [row for row in rows if row.get("shadow_label") == label][:limit]
    if not subset:
        return f"_No `{label}` rows._"
    perf_note = label_note(perf, label)
    header = "| name | rank | score | headline | reason | note |"
    sep = "|---|---:|---:|---:|---|---|"
    body = [
        f"| {row.get('symbol_name') or row.get('symbol')} | {row.get('original_rank')} | {num(row.get('score'))} | "
        f"{row.get('headline_proxy_score')} | {short(row.get('shadow_reason'))} | {note_prefix}; {perf_note} |"
        for row in subset
    ]
    return "\n".join([header, sep, *body])


def watch_table(rows: list[Record], label: str, limit: int) -> str:
    subset = [row for row in rows if row.get("shadow_label") == label][:limit]
    if not subset:
        return f"_No `{label}` rows._"
    header = "| name | reason | wait condition | hot flags |"
    sep = "|---|---|---|---|"
    body = [
        f"| {row.get('symbol_name') or row.get('symbol')} | {short(row.get('shadow_reason'))} | "
        f"range <10% and reclaim/positive close | {hot_flags(row)} |"
        for row in subset
    ]
    return "\n".join([header, sep, *body])


def risk_table(rows: list[Record], perf: list[Record], limit: int) -> str:
    subset = [row for row in rows if row.get("shadow_label") == "HIGH_RISK_OVERHEAT"][:limit]
    if not subset:
        return "_No `HIGH_RISK_OVERHEAT` rows._"
    risk_note = label_note(perf, "HIGH_RISK_OVERHEAT")
    header = "| name | reason | hot flags | risk note |"
    sep = "|---|---|---|---|"
    body = [
        f"| {row.get('symbol_name') or row.get('symbol')} | {short(row.get('shadow_reason'))} | "
        f"{hot_flags(row)} | {risk_note} |"
        for row in subset
    ]
    return "\n".join([header, sep, *body])


def performance_table(perf: list[Record]) -> str:
    wanted = {"IMMEDIATE_CANDIDATE", "HIGH_RISK_OVERHEAT", "LOWER_PRIORITY"}
    rows = ["| label | n | avg 5D | 5D crash | 5D winner |", "|---|---:|---:|---:|---:|"]
    for row in [item for item in perf if item.get("shadow_label") in wanted]:
        rows.append(
            f"| {row.get('shadow_label')} | {row.get('completed_5d_count')}/{row.get('row_count')} | "
            f"{pct(row.get('avg_forward_5d_return'))} | {pct(row.get('next_5d_crash_rate'))} | "
            f"{pct(row.get('next_5d_winner_rate'))} |"
        )
    return "\n".join(rows)


def interpretation_lines() -> str:
    return "\n".join(
        [
            "- 이 리포트는 매수 추천이 아닙니다.",
            "- baseline_old 공식 paper trading을 변경하지 않습니다.",
            "- 후보군을 즉시/대기/고위험/참고로 나누는 report-only shadow classification입니다.",
        ]
    )


def label_counts(rows: list[Record]) -> dict[str, int]:
    return {label: sum(1 for row in rows if row.get("shadow_label") == label) for label in LABEL_ORDER}


def label_note(perf: list[Record], label: str) -> str:
    row = next((item for item in perf if item.get("shadow_label") == label), {})
    return f"avg5D {pct(row.get('avg_forward_5d_return'))}, crash {pct(row.get('next_5d_crash_rate'))}, winner {pct(row.get('next_5d_winner_rate'))}, n={row.get('completed_5d_count') or 0}"


def hot_flags(row: Record) -> str:
    pairs = (("hot_range_flag", "range"), ("hot_giveback_flag", "giveback"), ("hot_ret_5d_flag", "ret5"), ("hot_volume_flag", "volume"))
    flags = [name for col, name in pairs if str(row.get(col)).lower() in {"1", "1.0", "true"}]
    return ",".join(flags) if flags else "none"


def short(value: object) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= 90 else text[:87] + "..."


def pct(value: object) -> str:
    parsed = fnum(value)
    return "n/a" if parsed is None else f"{parsed * 100:.2f}%"


def num(value: object) -> str:
    parsed = fnum(value)
    return "n/a" if parsed is None else f"{parsed:.4f}"


def fnum(value: object) -> float | None:
    text = "" if value is None else str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None
