#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Portable daily paper trading runner (local/Colab/Docker/agent compatible)."
    )
    p.add_argument("--db", required=True, help="Working SQLite DB path")
    p.add_argument("--universe-file", required=True, help="Universe CSV path")
    p.add_argument("--reports-dir", required=True, help="Report output directory")

    p.add_argument("--restore-db-from", help="Optional source DB path to restore from before run")
    p.add_argument("--backup-db", help="Optional destination DB path for backup after run")
    p.add_argument("--backup-reports-dir", help="Optional destination directory for report backups")
    p.add_argument("--force-restore", action="store_true", help="Restore even when --db already exists")

    p.add_argument("--skip-update", action="store_true")
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--install-missing-pykrx", action="store_true")
    p.add_argument("--display-html", action="store_true")
    p.add_argument("--no-display", action="store_true")
    return p


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _restore_db_if_requested(args: argparse.Namespace) -> None:
    if not args.restore_db_from:
        return
    restore_src = Path(args.restore_db_from).expanduser().resolve()
    db_path = Path(args.db).expanduser().resolve()
    if not restore_src.exists():
        raise FileNotFoundError(f"--restore-db-from does not exist: {restore_src}")
    if db_path.exists() and not args.force_restore:
        print(f"[restore] skipped existing db={db_path} (use --force-restore to overwrite)")
        return
    _copy_file(restore_src, db_path)
    print(f"[restore] copied {restore_src} -> {db_path}")


def _run_pipeline(args: argparse.Namespace) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_pipeline.py"),
        "--db",
        str(Path(args.db)),
        "--source",
        "krx",
        "--universe-file",
        str(Path(args.universe_file)),
    ]
    if args.start_date:
        cmd += ["--start-date", args.start_date]
    if args.end_date:
        cmd += ["--end-date", args.end_date]
    print(f"[run] {' '.join(cmd)}")
    return subprocess.run(cmd, check=False, text=True)


def _backup_outputs_if_requested(args: argparse.Namespace) -> None:
    db_path = Path(args.db).expanduser().resolve()
    reports_dir = Path(args.reports_dir).expanduser().resolve()

    if args.backup_db:
        backup_db = Path(args.backup_db).expanduser().resolve()
        _copy_file(db_path, backup_db)
        print(f"[backup] copied db {db_path} -> {backup_db}")

    if args.backup_reports_dir and reports_dir.exists():
        backup_reports_dir = Path(args.backup_reports_dir).expanduser().resolve()
        backup_reports_dir.mkdir(parents=True, exist_ok=True)
        for file_path in reports_dir.glob("*"):
            if file_path.is_file():
                shutil.copy2(file_path, backup_reports_dir / file_path.name)
        print(f"[backup] copied reports {reports_dir} -> {backup_reports_dir}")


def _display_report_if_requested(args: argparse.Namespace) -> None:
    reports_dir = Path(args.reports_dir).expanduser().resolve()
    html_files = sorted(reports_dir.glob("*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not html_files:
        return
    latest_html = html_files[0]

    if args.no_display:
        print(f"[report] latest_html={latest_html}")
        return

    if args.display_html:
        try:
            from IPython.display import HTML, display

            display(HTML(latest_html.read_text(encoding="utf-8")))
            print(f"[report] displayed_html={latest_html}")
            return
        except Exception:
            pass

    print(f"[report] latest_html={latest_html}")


def main() -> None:
    args = _build_parser().parse_args()

    Path(args.reports_dir).expanduser().resolve().mkdir(parents=True, exist_ok=True)
    _restore_db_if_requested(args)

    result = _run_pipeline(args)
    if result.returncode != 0:
        raise SystemExit(result.returncode)

    _backup_outputs_if_requested(args)
    _display_report_if_requested(args)


if __name__ == "__main__":
    main()
