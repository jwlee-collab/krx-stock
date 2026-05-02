#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

DEFAULT_DB = "~/krx-stock-persist/data/kospi_495_rolling_3y.db"
DEFAULT_REPORTS_DIR = "~/krx-stock-persist/reports/paper_trading"
DEFAULT_LOGS_DIR = "~/krx-stock-persist/logs"
REQUIRED_SUMMARY_KEYS = [
    "selected_run_id",
    "selected_run_created_at",
    "latest_signal_date",
    "latest_holdings_date",
    "previous_holdings_date",
    "holdings_count",
    "actual_exposure",
    "actual_cash",
    "new_candidate_count",
    "sold_count",
    "added_count",
    "kept_count",
    "generated_at",
]
REQUIRED_SECTIONS = [
    "오늘의 결론",
    "현재 포트폴리오",
    "전일 대비 변화",
    "정리/매도 내역",
    "신규 편입 내역",
    "현재 보유",
    "신규 후보",
    "참고용 후보",
    "운영 규칙",
    "손익 기준 설명",
    "과열도 설명",
]


def _expand(path: str) -> Path:
    return Path(path).expanduser().resolve()


def _fail(msg: str, failures: list[str]) -> None:
    failures.append(msg)


def _find_report_files(reports_dir: Path, as_of_date: str | None) -> tuple[Path | None, Path | None, Path | None]:
    if as_of_date:
        stem = f"{as_of_date}_paper_report"
        return (
            reports_dir / f"{stem}_summary.json",
            reports_dir / f"{stem}.html",
            reports_dir / f"{stem}.md",
        )

    json_files = sorted(reports_dir.glob("*_paper_report_summary.json"))
    if not json_files:
        return None, None, None
    summary = json_files[-1]
    date_prefix = summary.name.replace("_paper_report_summary.json", "")
    return (
        summary,
        reports_dir / f"{date_prefix}_paper_report.html",
        reports_dir / f"{date_prefix}_paper_report.md",
    )


def _validate_baseline_old(conn: sqlite3.Connection, run_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM backtest_runs
        WHERE run_id = ?
          AND scoring_profile = 'old'
          AND top_n = 5
          AND rebalance_frequency = 'weekly'
          AND min_holding_days = 10
          AND keep_rank_threshold = 9
          AND enable_position_stop_loss = 1
          AND ABS(position_stop_loss_pct - 0.10) < 1e-9
          AND stop_loss_cash_mode = 'keep_cash'
          AND enable_trailing_stop = 0
          AND market_filter_enabled = 0
          AND entry_gate_enabled = 0
          AND enable_overheat_entry_gate = 0
          AND entry_quality_gate_enabled = 0
          AND max_single_position_weight <= 0.20
        """,
        (run_id,),
    ).fetchone()
    return bool(row)


def _extract_table_block(html: str, section: str) -> str:
    marker = f"<h2>{section}</h2>"
    start = html.find(marker)
    if start < 0:
        return ""
    next_h2 = html.find("<h2>", start + len(marker))
    return html[start: next_h2 if next_h2 >= 0 else len(html)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    ap.add_argument("--logs-dir", default=DEFAULT_LOGS_DIR)
    ap.add_argument("--as-of-date", default=None)
    ap.add_argument("--summary-json", default=None)
    ap.add_argument("--html", default=None)
    ap.add_argument("--markdown", default=None)
    args = ap.parse_args()

    failures: list[str] = []
    db_path = _expand(args.db)
    reports_dir = _expand(args.reports_dir)
    logs_dir = _expand(args.logs_dir)

    summary_path = _expand(args.summary_json) if args.summary_json else None
    html_path = _expand(args.html) if args.html else None
    md_path = _expand(args.markdown) if args.markdown else None

    if not summary_path or not html_path or not md_path:
        auto_summary, auto_html, auto_md = _find_report_files(reports_dir, args.as_of_date)
        summary_path = summary_path or auto_summary
        html_path = html_path or auto_html
        md_path = md_path or auto_md

    # 1) 파일 존재 검증
    if not summary_path or not summary_path.exists():
        _fail(f"summary JSON 파일이 없습니다: {summary_path}", failures)
    if not html_path or not html_path.exists():
        _fail(f"HTML report 파일이 없습니다: {html_path}", failures)
    if not md_path or not md_path.exists():
        _fail(f"Markdown report 파일이 없습니다: {md_path}", failures)
    if not logs_dir.exists() or not any(logs_dir.glob("*.log")):
        _fail(f"log 파일이 없습니다: {logs_dir}", failures)

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    # 2) 필수 키 검증
    for k in REQUIRED_SUMMARY_KEYS:
        if k not in summary:
            _fail(f"summary 필수 키 누락: {k}", failures)

    run_id = str(summary.get("selected_run_id", ""))
    latest_signal_date = summary.get("latest_signal_date")
    latest_holdings_date = summary.get("latest_holdings_date")
    previous_holdings_date = summary.get("previous_holdings_date")

    conn = sqlite3.connect(str(db_path))
    try:
        # 3) baseline_old run_id 검증
        if not _validate_baseline_old(conn, run_id):
            _fail("selected_run_id가 baseline_old 조건을 만족하지 않습니다", failures)

        # 4) latest_signal_date 검증
        expected_signal = conn.execute(
            """
            SELECT MAX(ds.date)
            FROM daily_scores ds
            JOIN daily_universe du
              ON du.date = ds.date
             AND du.symbol = ds.symbol
            """
        ).fetchone()[0]
        if latest_signal_date != expected_signal:
            _fail(f"latest_signal_date 불일치: summary={latest_signal_date}, expected={expected_signal}", failures)

        # 5) latest_holdings_date 검증
        expected_latest_holdings = conn.execute(
            "SELECT MAX(date) FROM backtest_holdings WHERE run_id = ?", (run_id,)
        ).fetchone()[0]
        if latest_holdings_date != expected_latest_holdings:
            _fail(
                f"latest_holdings_date 불일치: summary={latest_holdings_date}, expected={expected_latest_holdings}",
                failures,
            )

        # 6) previous_holdings_date 검증
        expected_prev = conn.execute(
            "SELECT MAX(date) FROM backtest_holdings WHERE run_id = ? AND date < ?",
            (run_id, latest_holdings_date),
        ).fetchone()[0]
        if previous_holdings_date != expected_prev:
            _fail(
                f"previous_holdings_date 불일치: summary={previous_holdings_date}, expected={expected_prev}",
                failures,
            )

        # 7,8) exposure/cash, holdings_count
        rows = conn.execute(
            "SELECT symbol, weight FROM backtest_holdings WHERE run_id=? AND date=?",
            (run_id, latest_holdings_date),
        ).fetchall()
        exposure = sum(float(r[1] or 0.0) for r in rows)
        cash = 1.0 - exposure
        tol = 1e-9
        if abs(float(summary.get("actual_exposure", 0.0)) - exposure) > tol:
            _fail(
                f"actual_exposure 불일치: summary={summary.get('actual_exposure')}, expected={exposure}", failures
            )
        if abs(float(summary.get("actual_cash", 0.0)) - cash) > tol:
            _fail(f"actual_cash 불일치: summary={summary.get('actual_cash')}, expected={cash}", failures)
        if int(summary.get("holdings_count", -1)) != len(rows):
            _fail(f"holdings_count 불일치: summary={summary.get('holdings_count')}, expected={len(rows)}", failures)

        # 9) 전일 대비 변화
        cur = {str(r[0]) for r in rows}
        prev_rows = conn.execute(
            "SELECT symbol FROM backtest_holdings WHERE run_id=? AND date=?",
            (run_id, previous_holdings_date),
        ).fetchall() if previous_holdings_date else []
        prev = {str(r[0]) for r in prev_rows}
        sold_count, added_count, kept_count = len(prev - cur), len(cur - prev), len(cur & prev)
        if int(summary.get("sold_count", -1)) != sold_count:
            _fail(f"sold_count 불일치: summary={summary.get('sold_count')}, expected={sold_count}", failures)
        if int(summary.get("added_count", -1)) != added_count:
            _fail(f"added_count 불일치: summary={summary.get('added_count')}, expected={added_count}", failures)
        if int(summary.get("kept_count", -1)) != kept_count:
            _fail(f"kept_count 불일치: summary={summary.get('kept_count')}, expected={kept_count}", failures)

        # 10) 신규 후보 검증 (preview가 있을 때)
        preview = summary.get("top_new_candidates_preview") or []
        if isinstance(preview, list):
            eligible = conn.execute(
                """
                SELECT ds.symbol
                FROM daily_scores ds
                JOIN daily_universe du
                  ON du.date = ds.date
                 AND du.symbol = ds.symbol
                WHERE ds.date = ?
                """,
                (latest_signal_date,),
            ).fetchall()
            eligible_set = {str(r[0]) for r in eligible}
            for item in preview:
                symbol = str(item.get("symbol", ""))
                if symbol in cur:
                    _fail(f"신규 후보에 현재 보유 종목 포함: {symbol}", failures)
                if symbol not in eligible_set:
                    _fail(f"신규 후보가 eligible 후보가 아님: {symbol}", failures)

    finally:
        conn.close()

    # 11) HTML 검증
    html_text = html_path.read_text(encoding="utf-8")
    for section in REQUIRED_SECTIONS:
        if section not in html_text:
            _fail(f"HTML 필수 섹션 누락: {section}", failures)
    if "현재 포트폴리오" in html_text and "<table" in _extract_table_block(html_text, "현재 포트폴리오"):
        _fail("HTML 현재 포트폴리오 섹션에 2열 표가 있으면 안 됩니다", failures)
    if "summary-line" not in html_text and "summary-card" not in html_text:
        _fail("HTML에 compact summary line/card가 없습니다", failures)
    new_block = _extract_table_block(html_text, "신규 후보")
    if "요약" in new_block:
        _fail("신규 후보 표에 요약 컬럼이 있으면 안 됩니다", failures)
    if "제안비중" in new_block:
        _fail("신규 후보 표에 제안비중 컬럼이 있으면 안 됩니다", failures)

    # 12) Markdown 검증
    md_text = md_path.read_text(encoding="utf-8")
    if not md_text.strip():
        _fail("Markdown 파일이 비어 있습니다", failures)
    for section in REQUIRED_SECTIONS:
        if section not in md_text:
            _fail(f"Markdown 필수 섹션 누락: {section}", failures)

    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        return 1

    print("PASS: Mac paper report validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
