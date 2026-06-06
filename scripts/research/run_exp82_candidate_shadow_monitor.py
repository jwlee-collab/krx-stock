#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from exp82_io import (
    append_csv_rows,
    db_latest_date,
    latest_report_meta,
    load_exp80d_best_index,
    load_history_rows,
    load_json,
    load_latest_snapshot,
    load_prices,
    ops_meta_for_date,
    output_paths,
    write_csv_rows,
)
from exp82_metrics import build_monitor_rows
from exp82_report import build_metadata, paths_as_dict, write_report
from exp82_types import (
    DEFAULT_DB,
    DEFAULT_EXP80A_FORWARD,
    DEFAULT_EXP80A_SNAPSHOT,
    DEFAULT_EXP80D_ENTRY_PATHS,
    DEFAULT_EXP80D_RULE_SUMMARY,
    DEFAULT_EXP81_LATEST,
    DEFAULT_EXP81_METADATA,
    DEFAULT_OPS_LEDGER,
    DEFAULT_OUT_DIR,
    DEFAULT_REPORTS_DIR,
    Config,
    OutputPaths,
    Record,
)


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Exp82 candidate shadow label forward monitor")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--ops-ledger", default=DEFAULT_OPS_LEDGER)
    parser.add_argument("--exp81-latest", default=DEFAULT_EXP81_LATEST)
    parser.add_argument("--exp81-metadata", default=DEFAULT_EXP81_METADATA)
    parser.add_argument("--exp80a-forward", default=DEFAULT_EXP80A_FORWARD)
    parser.add_argument("--exp80a-snapshot", default=DEFAULT_EXP80A_SNAPSHOT)
    parser.add_argument("--exp80d-entry-paths", default=DEFAULT_EXP80D_ENTRY_PATHS)
    parser.add_argument("--exp80d-rule-summary", default=DEFAULT_EXP80D_RULE_SUMMARY)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--append-ledger", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return Config(
        db=Path(str(args.db)).expanduser(),
        reports_dir=Path(str(args.reports_dir)).expanduser(),
        ops_ledger=Path(str(args.ops_ledger)).expanduser(),
        exp81_latest=Path(str(args.exp81_latest)).expanduser(),
        exp81_metadata=Path(str(args.exp81_metadata)).expanduser(),
        exp80a_forward=Path(str(args.exp80a_forward)).expanduser(),
        exp80a_snapshot=Path(str(args.exp80a_snapshot)).expanduser(),
        exp80d_entry_paths=Path(str(args.exp80d_entry_paths)).expanduser(),
        exp80d_rule_summary=Path(str(args.exp80d_rule_summary)).expanduser(),
        out_dir=Path(str(args.out_dir)).expanduser(),
        as_of_date=None if args.as_of_date is None else str(args.as_of_date),
        append_ledger=bool(args.append_ledger),
        dry_run=bool(args.dry_run),
    )


def run(cfg: Config) -> Record:
    latest, as_of_date = load_latest_snapshot(cfg)
    candidate_n = max([int(float(str(row.get("candidate_rank") or 0))) for row in latest], default=20)
    history = load_history_rows(cfg, candidate_n, as_of_date)
    symbols = {str(row.get("symbol")).zfill(6) for row in [*latest, *history]}
    prices = load_prices(cfg.db, symbols)
    recorded_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    snapshot, tracking, summary = build_monitor_rows(latest, history, recorded_at, load_exp80d_best_index(cfg.exp80d_entry_paths), prices)
    quality = quality_meta(cfg, as_of_date, candidate_n)
    paths = output_paths(cfg.out_dir)
    metadata = build_metadata(cfg, as_of_date, snapshot, tracking, summary, quality, paths)
    if not cfg.dry_run:
        write_outputs(paths, snapshot, tracking, summary, metadata, quality, cfg.append_ledger)
    return {"dry_run": cfg.dry_run, "as_of_date": as_of_date, "snapshot_rows": len(snapshot), "tracking_rows": len(tracking), "outputs": paths_as_dict(paths)}


def quality_meta(cfg: Config, as_of_date: str, candidate_n: int) -> Record:
    report_meta = latest_report_meta(cfg.reports_dir)
    ops_meta = ops_meta_for_date(cfg.ops_ledger, as_of_date)
    exp81_meta = load_json(cfg.exp81_metadata)
    paper_signal = str(report_meta.get("paper_latest_signal_date") or "")
    stale = "none" if paper_signal in {"", as_of_date} else f"paper_latest_signal_date {paper_signal} != as_of_date {as_of_date}"
    invalid = "invalid fallback/quarantine" if "invalid" in str(ops_meta.get("ops_warning")).lower() else "none"
    return {
        "db_latest_date": db_latest_date(cfg.db),
        "candidate_n": candidate_n,
        "exp81_metadata_as_of_date": exp81_meta.get("as_of_date", ""),
        **report_meta,
        **ops_meta,
        "stale_or_missing_report_warning": stale,
        "invalid_fallback_or_quarantine_warning": invalid,
    }


def write_outputs(paths: OutputPaths, snapshot: list[Record], tracking: list[Record], summary: list[Record], metadata: Record, quality: Record, append_ledger: bool) -> None:
    write_csv_rows(paths.snapshot, snapshot, tracking_fields())
    write_csv_rows(paths.snapshot_latest, snapshot, tracking_fields())
    write_csv_rows(paths.tracking, tracking, tracking_fields())
    write_csv_rows(paths.tracking_latest, tracking, tracking_fields())
    write_csv_rows(paths.summary, summary, summary_fields())
    write_csv_rows(paths.summary_latest, summary, summary_fields())
    if append_ledger:
        append_csv_rows(paths.ledger, snapshot, tracking_fields())
    paths.metadata.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(metadata, ensure_ascii=False, indent=2)
    paths.metadata.write_text(text, encoding="utf-8")
    paths.metadata_latest.write_text(text, encoding="utf-8")
    write_report(paths.dashboard, paths.dashboard_latest, snapshot, summary, quality, metadata)


def tracking_fields() -> list[str]:
    return [
        "recorded_at", "as_of_date", "symbol", "symbol_name", "candidate_rank", "original_rank", "score",
        "shadow_label", "shadow_reason", "headline_proxy_score", "hot_range_flag", "hot_giveback_flag",
        "hot_ret_1d_flag", "hot_ret_5d_flag", "hot_volume_flag", "overextended_sma5_flag", "range_pct",
        "ret_1d", "ret_5d", "volume_z20", "sma5_gap", "exp80d_best_rule", "exp80d_best_entry_status",
        "exp80d_best_entry_date", "exp80d_best_entry_to_5d_return", "outcome_status", "forward_1d_return",
        "forward_3d_return", "forward_5d_return", "forward_10d_return", "forward_3d_min_low_return",
        "forward_5d_min_low_return", "next_day_crash_flag", "next_5d_crash_flag", "next_5d_winner_flag",
        "forward_5d_mdd", "monitor_source",
    ]


def summary_fields() -> list[str]:
    return [
        "shadow_label", "row_count", "completed_1d_count", "completed_5d_count", "pending_count",
        "avg_forward_1d_return", "avg_forward_3d_return", "avg_forward_5d_return", "median_forward_5d_return",
        "next_day_crash_rate", "next_5d_crash_rate", "next_5d_winner_rate", "avg_forward_5d_mdd",
        "avg_headline_proxy_score", "avg_range_pct", "avg_volume_z20",
    ]


def main() -> int:
    print(json.dumps(run(parse_args()), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
