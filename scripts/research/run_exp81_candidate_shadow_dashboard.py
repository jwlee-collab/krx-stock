#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///

from __future__ import annotations

import argparse
import json
from pathlib import Path

from exp81_io import (
    load_candidate_rows,
    load_exp80c_action_index,
    load_exp80d_best_index,
    ops_meta_for_date,
    output_paths,
    read_csv_rows,
    resolve_as_of_date,
    write_csv_rows,
)
from exp81_report import build_metadata, paths_as_dict, write_report
from exp81_rules import build_dashboard_rows, fnum
from exp81_types import (
    DEFAULT_DB,
    DEFAULT_EXP80A_FORWARD,
    DEFAULT_EXP80A_SNAPSHOT,
    DEFAULT_EXP80C_ASSIGNMENTS,
    DEFAULT_EXP80D_ENTRY_PATHS,
    DEFAULT_EXP80D_RULE_SUMMARY,
    DEFAULT_OPS_LEDGER,
    DEFAULT_OUT_DIR,
    DEFAULT_REPORTS_DIR,
    Config,
    OutputPaths,
    Record,
)


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Exp81 candidate shadow dashboard")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--ops-ledger", default=DEFAULT_OPS_LEDGER)
    parser.add_argument("--exp80a-forward", default=DEFAULT_EXP80A_FORWARD)
    parser.add_argument("--exp80a-snapshot", default=DEFAULT_EXP80A_SNAPSHOT)
    parser.add_argument("--exp80c-assignments", default=DEFAULT_EXP80C_ASSIGNMENTS)
    parser.add_argument("--exp80d-entry-paths", default=DEFAULT_EXP80D_ENTRY_PATHS)
    parser.add_argument("--exp80d-rule-summary", default=DEFAULT_EXP80D_RULE_SUMMARY)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--candidate-n", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return Config(
        db=Path(str(args.db)).expanduser(),
        reports_dir=Path(str(args.reports_dir)).expanduser(),
        ops_ledger=Path(str(args.ops_ledger)).expanduser(),
        exp80a_forward=Path(str(args.exp80a_forward)).expanduser(),
        exp80a_snapshot=Path(str(args.exp80a_snapshot)).expanduser(),
        exp80c_assignments=Path(str(args.exp80c_assignments)).expanduser(),
        exp80d_entry_paths=Path(str(args.exp80d_entry_paths)).expanduser(),
        exp80d_rule_summary=Path(str(args.exp80d_rule_summary)).expanduser(),
        out_dir=Path(str(args.out_dir)).expanduser(),
        as_of_date=None if args.as_of_date is None else str(args.as_of_date),
        candidate_n=int(args.candidate_n),
        dry_run=bool(args.dry_run),
    )


def run(cfg: Config) -> Record:
    as_of_date, report_meta = resolve_as_of_date(cfg)
    candidates, source = load_candidate_rows(cfg, as_of_date)
    rows = build_dashboard_rows(
        candidates,
        as_of_date,
        source,
        load_exp80d_best_index(cfg.exp80d_entry_paths),
        load_exp80c_action_index(cfg.exp80c_assignments),
    )
    quality = quality_meta(cfg, as_of_date, source, report_meta)
    paths = output_paths(cfg.out_dir)
    metadata = build_metadata(cfg, as_of_date, rows, quality, paths)
    if not cfg.dry_run:
        write_outputs(paths, rows, metadata, quality)
    return {"dry_run": cfg.dry_run, "as_of_date": as_of_date, "rows": len(rows), "outputs": paths_as_dict(paths)}


def quality_meta(cfg: Config, as_of_date: str, source: str, report_meta: Record) -> Record:
    ops_meta = ops_meta_for_date(cfg.ops_ledger, as_of_date)
    latest_signal = str(report_meta.get("latest_signal_date") or "")
    stale = "none" if latest_signal in {"", as_of_date} else f"latest_signal_date {latest_signal} != as_of_date {as_of_date}"
    return {
        **report_meta,
        **ops_meta,
        "candidate_source": source,
        "stale_or_missing_warning": stale,
        "exp80d_best_rule": best_rule(cfg.exp80d_rule_summary),
    }


def best_rule(path: Path) -> str:
    rows = [row for row in read_csv_rows(path) if row.get("timing_rule") != "baseline_immediate"]
    if not rows:
        return "unavailable"
    row = max(rows, key=lambda item: fnum(item.get("tradeoff_score")) or -999999.0)
    return f"{row.get('timing_rule')} top_k={row.get('top_k')} tradeoff={row.get('tradeoff_score')}"


def write_outputs(paths: OutputPaths, rows: list[Record], metadata: Record, quality: Record) -> None:
    write_csv_rows(paths.dashboard_csv, rows, dashboard_fields())
    write_csv_rows(paths.dashboard_latest_csv, rows, dashboard_fields())
    paths.metadata_json.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(metadata, ensure_ascii=False, indent=2)
    paths.metadata_json.write_text(text, encoding="utf-8")
    paths.metadata_latest_json.write_text(text, encoding="utf-8")
    write_report(paths.dashboard_md, paths.dashboard_latest_md, rows, metadata, quality)


def dashboard_fields() -> list[str]:
    return [
        "as_of_date",
        "symbol",
        "symbol_name",
        "candidate_rank",
        "original_rank",
        "score",
        "current_holding_flag",
        "candidate_source",
        "shadow_label",
        "shadow_reason",
        "headline_proxy_score",
        "hot_range_flag",
        "hot_giveback_flag",
        "hot_ret_1d_flag",
        "hot_ret_5d_flag",
        "hot_volume_flag",
        "overextended_sma5_flag",
        "range_pct",
        "ret_1d",
        "ret_5d",
        "volume_z20",
        "sma5_gap",
        "forward_1d_return",
        "forward_3d_return",
        "forward_5d_return",
        "exp80d_best_rule_entry_status",
        "exp80d_best_rule_entry_date",
        "exp80d_best_rule_entry_to_5d_return",
        "exp80c_action_label",
        "outcome_status",
    ]


def main() -> int:
    print(json.dumps(run(parse_args()), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
