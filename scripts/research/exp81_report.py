from __future__ import annotations

import json
from pathlib import Path

from exp81_io import stringify
from exp81_rules import fnum
from exp81_types import Config, OutputPaths, Record, WATCH_SYMBOLS

LABEL_ORDER = (
    "IMMEDIATE_CANDIDATE",
    "DELAY_REVIEW",
    "COOLDOWN_RECLAIM_WATCH",
    "HIGH_RISK_OVERHEAT",
    "LOWER_PRIORITY",
    "PENDING_DATA",
)


def build_metadata(cfg: Config, as_of_date: str, rows: list[Record], quality: Record, paths: OutputPaths) -> Record:
    return {
        "experiment": "Exp81 candidate shadow dashboard",
        "production_change": False,
        "paper_trading_change": False,
        "real_order_change": False,
        "db_write": False,
        "broker_order_api": False,
        "telegram_sent": False,
        "db_open_mode": "sqlite_uri_mode_ro_and_query_only",
        "as_of_date": as_of_date,
        "candidate_n": cfg.candidate_n,
        "rows": len(rows),
        "label_counts": json.dumps(label_counts(rows), ensure_ascii=False),
        "data_quality": json.dumps(quality, ensure_ascii=False),
        "inputs": json.dumps(input_paths(cfg), ensure_ascii=False),
        "outputs": json.dumps(paths_as_dict(paths), ensure_ascii=False),
        "dry_run": cfg.dry_run,
    }


def input_paths(cfg: Config) -> dict[str, str]:
    return {
        "db": str(cfg.db),
        "reports_dir": str(cfg.reports_dir),
        "ops_ledger": str(cfg.ops_ledger),
        "exp80a_forward": str(cfg.exp80a_forward),
        "exp80a_snapshot": str(cfg.exp80a_snapshot),
        "exp80c_assignments": str(cfg.exp80c_assignments),
        "exp80d_entry_paths": str(cfg.exp80d_entry_paths),
        "exp80d_rule_summary": str(cfg.exp80d_rule_summary),
    }


def paths_as_dict(paths: OutputPaths) -> dict[str, str]:
    return {
        "dashboard_csv": str(paths.dashboard_csv),
        "dashboard_latest_csv": str(paths.dashboard_latest_csv),
        "dashboard_md": str(paths.dashboard_md),
        "dashboard_latest_md": str(paths.dashboard_latest_md),
        "metadata_json": str(paths.metadata_json),
        "metadata_latest_json": str(paths.metadata_latest_json),
    }


def write_report(path: Path, latest_path: Path, rows: list[Record], metadata: Record, quality: Record) -> None:
    text = report_text(rows, metadata, quality)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")


def report_text(rows: list[Record], metadata: Record, quality: Record) -> str:
    counts = label_counts(rows)
    lines = [
        "# Exp81 Candidate Shadow Dashboard",
        "",
        "## 1. Executive Summary",
        executive_summary(metadata, counts),
        "",
        "## 2. Immediate Candidates",
        label_table(rows, "IMMEDIATE_CANDIDATE", 15),
        "",
        "## 3. Delay / Reclaim Watch",
        label_table(rows, "DELAY_REVIEW", 10) + "\n" + label_table(rows, "COOLDOWN_RECLAIM_WATCH", 10),
        "",
        "## 4. High Risk Overheat",
        label_table(rows, "HIGH_RISK_OVERHEAT", 15),
        "",
        "## 5. Lower Priority / Reference",
        label_table(rows, "LOWER_PRIORITY", 15),
        "",
        "## 6. Recent Case Study",
        watch_table(rows),
        "",
        "## 7. Ops/Data Quality",
        quality_lines(quality),
        "",
        "## 8. Recommended Next Shadow Steps",
        "- Exp82: daily shadow monitoring으로 label별 다음날/5일 outcome 추적.",
        "- Exp83: candidate label forward tracking으로 missed winner와 avoided crash를 일별 저장.",
        "- Exp84: external news/theme feasibility를 report-only로 검토.",
        "- production/paper 적용 금지. 이 대시보드는 shadow 분류입니다.",
        "",
        "## Safety Flags",
        "- production_change=false",
        "- paper_trading_change=false",
        "- real_order_change=false",
    ]
    return "\n".join(lines) + "\n"


def executive_summary(metadata: Record, counts: dict[str, int]) -> str:
    parts = [f"- as_of_date={metadata.get('as_of_date')}, candidates={metadata.get('rows')}."]
    parts.append("- 오늘 바로 매수 추천이 아니라 baseline_old 후보군의 report-only shadow classification입니다.")
    parts.append("- " + ", ".join(f"{label}={counts.get(label, 0)}" for label in LABEL_ORDER))
    return "\n".join(parts)


def label_table(rows: list[Record], label: str, limit: int) -> str:
    subset = [row for row in rows if row.get("shadow_label") == label][:limit]
    if not subset:
        return f"_No `{label}` rows._"
    header = "| rank | name | flags | 5D/Range | reason |"
    sep = "|---:|---|---|---:|---|"
    body = [
        f"| {row.get('candidate_rank')} | {row.get('symbol_name') or row.get('symbol')} | {flag_text(row)} | "
        f"{pct(row.get('ret_5d'))}/{pct(row.get('range_pct'))} | {short_reason(row)} |"
        for row in subset
    ]
    return "\n".join([f"### {label}", header, sep, *body])


def watch_table(rows: list[Record]) -> str:
    watch = {symbol: name for symbol, name in WATCH_SYMBOLS}
    subset = [row for row in rows if stringify(row.get("symbol")).zfill(6) in watch]
    if not subset:
        return "_No watched symbols in current candidate set._"
    header = "| name | rank | label | flags | 5D/Range | note |"
    sep = "|---|---:|---|---|---:|---|"
    body = [
        f"| {watch[stringify(row.get('symbol')).zfill(6)]} | {row.get('candidate_rank')} | {row.get('shadow_label')} | "
        f"{flag_text(row)} | {pct(row.get('ret_5d'))}/{pct(row.get('range_pct'))} | {short_reason(row)} |"
        for row in subset[:20]
    ]
    return "\n".join([header, sep, *body])


def quality_lines(quality: Record) -> str:
    return "\n".join(
        [
            f"- latest_report_date={quality.get('latest_report_date')}",
            f"- latest_signal_date={quality.get('latest_signal_date')}",
            f"- db_latest_date={quality.get('db_latest_date')}",
            f"- ops_ledger_status={quality.get('ops_ledger_status')}",
            f"- ops_warning={quality.get('ops_warning')}",
            f"- candidate_source={quality.get('candidate_source')}",
            f"- stale_or_missing_warning={quality.get('stale_or_missing_warning')}",
        ]
    )


def label_counts(rows: list[Record]) -> dict[str, int]:
    return {label: sum(1 for row in rows if row.get("shadow_label") == label) for label in LABEL_ORDER}


def flag_text(row: Record) -> str:
    names = []
    for col, name in (("hot_range_flag", "range"), ("hot_giveback_flag", "giveback"), ("hot_ret_5d_flag", "ret5"), ("hot_volume_flag", "vol")):
        if stringify(row.get(col)).lower() in {"1", "1.0", "true"}:
            names.append(name)
    return ",".join(names) if names else "none"


def short_reason(row: Record) -> str:
    reason = stringify(row.get("shadow_reason"))
    return reason if len(reason) <= 90 else reason[:87] + "..."


def pct(value: object) -> str:
    parsed = fnum(value)
    return "n/a" if parsed is None else f"{parsed * 100:.1f}%"
