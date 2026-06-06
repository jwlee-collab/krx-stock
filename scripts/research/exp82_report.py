from __future__ import annotations

import json
from pathlib import Path

from exp82_metrics import fnum
from exp82_types import Config, LABEL_ORDER, OutputPaths, Record, WATCH_SYMBOLS


def build_metadata(cfg: Config, as_of_date: str, snapshot: list[Record], tracking: list[Record], summary: list[Record], quality: Record, paths: OutputPaths) -> Record:
    return {
        "experiment": "Exp82 candidate shadow label forward monitor",
        "production_change": False,
        "paper_trading_change": False,
        "real_order_change": False,
        "db_write": False,
        "broker_order_api": False,
        "telegram_sent": False,
        "db_open_mode": "sqlite_uri_mode_ro_and_query_only",
        "as_of_date": as_of_date,
        "snapshot_rows": len(snapshot),
        "tracking_rows": len(tracking),
        "summary_rows": len(summary),
        "label_counts": json.dumps(label_counts(snapshot), ensure_ascii=False),
        "data_quality": json.dumps(quality, ensure_ascii=False),
        "inputs": json.dumps(input_paths(cfg), ensure_ascii=False),
        "outputs": json.dumps(paths_as_dict(paths), ensure_ascii=False),
        "append_ledger": cfg.append_ledger,
        "dry_run": cfg.dry_run,
    }


def input_paths(cfg: Config) -> dict[str, str]:
    return {
        "db": str(cfg.db),
        "reports_dir": str(cfg.reports_dir),
        "ops_ledger": str(cfg.ops_ledger),
        "exp81_latest": str(cfg.exp81_latest),
        "exp81_metadata": str(cfg.exp81_metadata),
        "exp80a_forward": str(cfg.exp80a_forward),
        "exp80a_snapshot": str(cfg.exp80a_snapshot),
        "exp80d_entry_paths": str(cfg.exp80d_entry_paths),
        "exp80d_rule_summary": str(cfg.exp80d_rule_summary),
    }


def paths_as_dict(paths: OutputPaths) -> dict[str, str]:
    return {
        "snapshot": str(paths.snapshot),
        "snapshot_latest": str(paths.snapshot_latest),
        "tracking": str(paths.tracking),
        "tracking_latest": str(paths.tracking_latest),
        "summary": str(paths.summary),
        "summary_latest": str(paths.summary_latest),
        "dashboard": str(paths.dashboard),
        "dashboard_latest": str(paths.dashboard_latest),
        "metadata": str(paths.metadata),
        "metadata_latest": str(paths.metadata_latest),
        "ledger": str(paths.ledger),
    }


def write_report(path: Path, latest_path: Path, snapshot: list[Record], summary: list[Record], quality: Record, metadata: Record) -> None:
    text = report_text(snapshot, summary, quality, metadata)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")


def report_text(snapshot: list[Record], summary: list[Record], quality: Record, metadata: Record) -> str:
    lines = [
        "# Exp82 Candidate Shadow Label Forward Monitor",
        "",
        "## 1. Executive Summary",
        executive_summary(snapshot, metadata),
        "",
        "## 2. Latest Candidate Labels",
        latest_tables(snapshot),
        "",
        "## 3. Label Forward Performance",
        summary_table(summary),
        "",
        "## 4. Pending Outcome Tracker",
        pending_table(snapshot),
        "",
        "## 5. 2026-06-04 Case Study",
        watch_table(snapshot),
        "",
        "## 6. Ops/Data Quality",
        quality_lines(quality),
        "",
        "## 7. Next Steps",
        "- Exp83: daily shadow dashboard integration into Hermes-readable file.",
        "- Exp84: external news/theme feasibility.",
        "- Exp85: shadow label forward observation for 20 trading days.",
        "- production/paper 적용 금지. 이 파일은 report-only shadow monitor입니다.",
        "",
        "## Safety Flags",
        "- production_change=false",
        "- paper_trading_change=false",
        "- real_order_change=false",
    ]
    return "\n".join(lines) + "\n"


def executive_summary(snapshot: list[Record], metadata: Record) -> str:
    counts = label_counts(snapshot)
    lines = [f"- latest as_of_date={metadata.get('as_of_date')}, latest candidates={len(snapshot)}."]
    lines.append("- 매수 추천이 아니라 report-only shadow classification forward monitor입니다.")
    lines.append("- " + ", ".join(f"{label}={counts.get(label, 0)}" for label in LABEL_ORDER))
    return "\n".join(lines)


def latest_tables(snapshot: list[Record]) -> str:
    chunks: list[str] = []
    for label in LABEL_ORDER:
        rows = [row for row in snapshot if row.get("shadow_label") == label]
        chunks.append(label_table(label, rows[:12]))
    return "\n\n".join(chunks)


def label_table(label: str, rows: list[Record]) -> str:
    if not rows:
        return f"### {label}\n_No rows._"
    header = "| rank | name | 5D/Range | outcome |"
    sep = "|---:|---|---:|---|"
    body = [
        f"| {row.get('candidate_rank')} | {row.get('symbol_name') or row.get('symbol')} | "
        f"{pct(row.get('ret_5d'))}/{pct(row.get('range_pct'))} | {row.get('outcome_status')} |"
        for row in rows
    ]
    return "\n".join([f"### {label}", header, sep, *body])


def summary_table(summary: list[Record]) -> str:
    header = "| label | rows | done 5D | pending | avg 5D | 5D crash | 5D winner | avg range |"
    sep = "|---|---:|---:|---:|---:|---:|---:|---:|"
    body = [
        f"| {row.get('shadow_label')} | {row.get('row_count')} | {row.get('completed_5d_count')} | "
        f"{row.get('pending_count')} | {pct(row.get('avg_forward_5d_return'))} | "
        f"{pct(row.get('next_5d_crash_rate'))} | {pct(row.get('next_5d_winner_rate'))} | {pct(row.get('avg_range_pct'))} |"
        for row in summary
    ]
    return "\n".join([header, sep, *body])


def pending_table(snapshot: list[Record]) -> str:
    rows = [row for row in snapshot if row.get("outcome_status") != "COMPLETE_5D"]
    if not rows:
        return "_No pending latest candidates._"
    header = "| name | label | rank | status |"
    sep = "|---|---|---:|---|"
    body = [f"| {row.get('symbol_name') or row.get('symbol')} | {row.get('shadow_label')} | {row.get('candidate_rank')} | {row.get('outcome_status')} |" for row in rows[:20]]
    return "\n".join([header, sep, *body])


def watch_table(snapshot: list[Record]) -> str:
    watch = {symbol: name for symbol, name in WATCH_SYMBOLS}
    rows = [row for row in snapshot if str(row.get("symbol")).zfill(6) in watch]
    if not rows:
        return "_No watched names in latest snapshot._"
    header = "| name | rank | label | ret5/range | outcome |"
    sep = "|---|---:|---|---:|---|"
    body = [
        f"| {watch[str(row.get('symbol')).zfill(6)]} | {row.get('candidate_rank')} | {row.get('shadow_label')} | "
        f"{pct(row.get('ret_5d'))}/{pct(row.get('range_pct'))} | {row.get('outcome_status')} |"
        for row in rows
    ]
    return "\n".join([header, sep, *body])


def quality_lines(quality: Record) -> str:
    return "\n".join(
        [
            f"- db_latest_date={quality.get('db_latest_date')}",
            f"- paper_latest_date={quality.get('paper_latest_date')}",
            f"- paper_latest_signal_date={quality.get('paper_latest_signal_date')}",
            f"- ops_ledger_status={quality.get('ops_ledger_status')}",
            f"- ops_warning={quality.get('ops_warning')}",
            f"- stale_or_missing_report_warning={quality.get('stale_or_missing_report_warning')}",
            f"- invalid_fallback_or_quarantine_warning={quality.get('invalid_fallback_or_quarantine_warning')}",
        ]
    )


def label_counts(rows: list[Record]) -> dict[str, int]:
    return {label: sum(1 for row in rows if row.get("shadow_label") == label) for label in LABEL_ORDER}


def pct(value: object) -> str:
    parsed = fnum(value)
    return "n/a" if parsed is None else f"{parsed * 100:.1f}%"
