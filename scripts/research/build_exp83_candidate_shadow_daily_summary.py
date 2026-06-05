#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = []
# ///

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

from exp83_report import label_counts, markdown_report

Cell = Union[str, int, float, bool, None]
Record = dict[str, Cell]


@dataclass(frozen=True)
class Config:
    exp82_latest: Path
    exp82_performance: Path
    exp82_metadata: Path
    paper_reports_dir: Path
    ops_ledger: Path
    out_dir: Path
    hermes_dir: Path
    as_of_date: str | None
    dry_run: bool


@dataclass(frozen=True)
class OutputPaths:
    markdown: Path
    markdown_latest: Path
    metadata: Path
    metadata_latest: Path
    hermes_markdown: Path


DEFAULT_EXP82_LATEST = "~/krx-stock-persist/reports/research/exp82/exp82_shadow_label_latest.csv"
DEFAULT_EXP82_PERFORMANCE = "~/krx-stock-persist/reports/research/exp82/exp82_label_performance_summary_latest.csv"
DEFAULT_EXP82_METADATA = "~/krx-stock-persist/reports/research/exp82/exp82_metadata_latest.json"
DEFAULT_PAPER_REPORTS_DIR = "~/krx-stock-persist/reports/paper_trading"
DEFAULT_OPS_LEDGER = "~/krx-stock-persist/reports/paper_trading/krx_daily_ops_ledger.csv"
DEFAULT_OUT_DIR = "~/krx-stock-persist/reports/research/exp83"
DEFAULT_HERMES_DIR = "~/.hermes/krx"


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Build Exp83 candidate shadow daily summary")
    parser.add_argument("--exp82-latest", default=DEFAULT_EXP82_LATEST)
    parser.add_argument("--exp82-performance", default=DEFAULT_EXP82_PERFORMANCE)
    parser.add_argument("--exp82-metadata", default=DEFAULT_EXP82_METADATA)
    parser.add_argument("--paper-reports-dir", default=DEFAULT_PAPER_REPORTS_DIR)
    parser.add_argument("--ops-ledger", default=DEFAULT_OPS_LEDGER)
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    parser.add_argument("--hermes-dir", default=DEFAULT_HERMES_DIR)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    return Config(
        exp82_latest=Path(str(args.exp82_latest)).expanduser(),
        exp82_performance=Path(str(args.exp82_performance)).expanduser(),
        exp82_metadata=Path(str(args.exp82_metadata)).expanduser(),
        paper_reports_dir=Path(str(args.paper_reports_dir)).expanduser(),
        ops_ledger=Path(str(args.ops_ledger)).expanduser(),
        out_dir=Path(str(args.out_dir)).expanduser(),
        hermes_dir=Path(str(args.hermes_dir)).expanduser(),
        as_of_date=None if args.as_of_date is None else str(args.as_of_date),
        dry_run=bool(args.dry_run),
    )


def run(cfg: Config) -> Record:
    rows = read_csv_rows(cfg.exp82_latest)
    as_of_date = cfg.as_of_date or first_as_of_date(rows) or metadata_as_of_date(cfg.exp82_metadata)
    latest_rows = [row for row in rows if stringify(row.get("as_of_date")) == as_of_date]
    perf = read_csv_rows(cfg.exp82_performance)
    quality = quality_meta(cfg, as_of_date)
    paths = output_paths(cfg.out_dir, cfg.hermes_dir)
    text = markdown_report(latest_rows, perf, quality)
    metadata = build_metadata(cfg, latest_rows, perf, quality, paths)
    if not cfg.dry_run:
        write_outputs(paths, text, metadata)
    return {"dry_run": cfg.dry_run, "as_of_date": as_of_date, "rows": len(latest_rows), "outputs": paths_as_dict(paths)}


def read_csv_rows(path: Path) -> list[Record]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [
            {str(key): "" if value is None else value for key, value in row.items() if key is not None}
            for row in csv.DictReader(handle)
        ]


def output_paths(out_dir: Path, hermes_dir: Path) -> OutputPaths:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return OutputPaths(
        markdown=out_dir / f"exp83_candidate_shadow_daily_summary_{stamp}.md",
        markdown_latest=out_dir / "exp83_candidate_shadow_daily_summary_latest.md",
        metadata=out_dir / f"exp83_candidate_shadow_daily_summary_{stamp}.json",
        metadata_latest=out_dir / "exp83_candidate_shadow_daily_summary_latest.json",
        hermes_markdown=hermes_dir / "latest_candidate_shadow_summary.md",
    )


def quality_meta(cfg: Config, as_of_date: str) -> Record:
    paper = latest_paper_summary(cfg.paper_reports_dir)
    ops = ops_meta_for_date(cfg.ops_ledger, as_of_date)
    stale = stale_warning(as_of_date, paper)
    return {
        "as_of_date": as_of_date,
        "paper_latest_report_date": paper.get("paper_latest_report_date"),
        "paper_latest_signal_date": paper.get("latest_signal_date"),
        "paper_latest_holdings_date": paper.get("latest_holdings_date"),
        "ops_ledger_status": ops.get("ops_ledger_status"),
        "ops_warning": ops.get("ops_warning"),
        "stale_or_missing_warning": stale,
    }


def latest_paper_summary(reports_dir: Path) -> Record:
    paths = sorted(reports_dir.glob("*_paper_report_summary.json"))
    if not paths:
        return {"paper_latest_report_date": "", "latest_signal_date": "", "latest_holdings_date": "", "paper_status": "missing"}
    path = paths[-1]
    data = json.loads(path.read_text(encoding="utf-8"))
    report_date = re.sub(r"_paper_report_summary\.json$", "", path.name)
    return {
        "paper_latest_report_date": report_date,
        "latest_signal_date": stringify(data.get("latest_signal_date") or report_date),
        "latest_holdings_date": stringify(data.get("latest_holdings_date") or data.get("holdings_date") or ""),
        "paper_status": "available",
        "paper_summary_path": str(path),
    }


def ops_meta_for_date(path: Path, as_of_date: str) -> Record:
    if not path.exists():
        return {"ops_ledger_status": "unavailable", "ops_warning": "ops ledger missing"}
    for row in read_csv_rows(path):
        dates = {stringify(row.get("expected_trade_date")), stringify(row.get("run_date")), stringify(row.get("paper_report_date"))}
        if as_of_date not in dates:
            continue
        stale = "STALE" in stringify(row.get("stale_check")).upper()
        invalid = stringify(row.get("invalid_fallback_flag")).lower() == "true"
        warning = "; ".join(name for name, bad in (("stale warning", stale), ("invalid fallback", invalid)) if bad)
        return {"ops_ledger_status": stringify(row.get("status") or "unknown"), "ops_warning": warning or "none"}
    return {"ops_ledger_status": "missing_date", "ops_warning": f"no ops ledger row for {as_of_date}"}


def stale_warning(as_of_date: str, paper: Record) -> str:
    signal = stringify(paper.get("latest_signal_date"))
    holdings = stringify(paper.get("latest_holdings_date"))
    if signal == "" or holdings == "":
        return "missing paper latest_signal_date/latest_holdings_date"
    return "none" if signal == as_of_date else f"paper latest_signal_date {signal} != as_of_date {as_of_date}"


def build_metadata(cfg: Config, rows: list[Record], perf: list[Record], quality: Record, paths: OutputPaths) -> Record:
    return {
        "experiment": "Exp83 candidate shadow daily report integration",
        "production_change": False,
        "paper_trading_change": False,
        "real_order_change": False,
        "telegram_sent": False,
        "broker_order_api": False,
        "db_write": False,
        "rows": len(rows),
        "label_counts": label_counts(rows),
        "performance_rows": len(perf),
        "quality": quality,
        "inputs": input_paths(cfg),
        "outputs": paths_as_dict(paths),
        "dry_run": cfg.dry_run,
    }


def input_paths(cfg: Config) -> dict[str, str]:
    return {
        "exp82_latest": str(cfg.exp82_latest),
        "exp82_performance": str(cfg.exp82_performance),
        "exp82_metadata": str(cfg.exp82_metadata),
        "paper_reports_dir": str(cfg.paper_reports_dir),
        "ops_ledger": str(cfg.ops_ledger),
    }


def paths_as_dict(paths: OutputPaths) -> dict[str, str]:
    return {
        "markdown": str(paths.markdown),
        "markdown_latest": str(paths.markdown_latest),
        "metadata": str(paths.metadata),
        "metadata_latest": str(paths.metadata_latest),
        "hermes_markdown": str(paths.hermes_markdown),
    }


def write_outputs(paths: OutputPaths, text: str, metadata: Record) -> None:
    paths.markdown.parent.mkdir(parents=True, exist_ok=True)
    paths.hermes_markdown.parent.mkdir(parents=True, exist_ok=True)
    paths.markdown.write_text(text, encoding="utf-8")
    paths.markdown_latest.write_text(text, encoding="utf-8")
    paths.hermes_markdown.write_text(text, encoding="utf-8")
    data = json.dumps(metadata, ensure_ascii=False, indent=2)
    paths.metadata.write_text(data, encoding="utf-8")
    paths.metadata_latest.write_text(data, encoding="utf-8")


def first_as_of_date(rows: list[Record]) -> str:
    return stringify(rows[0].get("as_of_date")) if rows else ""


def metadata_as_of_date(path: Path) -> str:
    if not path.exists():
        return ""
    return stringify(json.loads(path.read_text(encoding="utf-8")).get("as_of_date"))


def stringify(value: Cell) -> str:
    return "" if value is None else str(value)


def main() -> int:
    print(json.dumps(run(parse_args()), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
