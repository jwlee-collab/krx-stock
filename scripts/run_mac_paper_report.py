#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import html
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_DB = "~/krx-stock-persist/data/kospi_495_rolling_3y.db"
DEFAULT_UNIVERSE = "~/krx-stock-persist/data/kospi_valid_universe_495.csv"
DEFAULT_REPORTS_DIR = "~/krx-stock-persist/reports/paper_trading"
DEFAULT_LOGS_DIR = "~/krx-stock-persist/logs"


@dataclass
class RunSelection:
    run_id: str
    created_at: str


def _expand(path: str) -> Path:
    return Path(path).expanduser().resolve()


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _require_columns(conn: sqlite3.Connection, table: str, required: list[str]) -> None:
    cols = set(_table_columns(conn, table))
    if not cols:
        raise RuntimeError(f"필수 테이블 누락: {table}")
    missing = [c for c in required if c not in cols]
    if missing:
        raise RuntimeError(f"필수 컬럼 누락: table={table}, missing={missing}")


def _col_expr(col: str, available_cols: set[str], fallback: str = "NULL") -> str:
    return col if col in available_cols else fallback


def _load_symbol_names(universe_csv: Path) -> dict[str, str]:
    if not universe_csv.exists():
        raise RuntimeError(f"유니버스 CSV 파일이 없습니다: {universe_csv}")
    with universe_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        if "symbol" not in headers:
            raise RuntimeError("유니버스 CSV에 symbol 컬럼이 없습니다")
        name_col = "name" if "name" in headers else ("company_name" if "company_name" in headers else None)
        out: dict[str, str] = {}
        for row in reader:
            sym = (row.get("symbol") or "").strip()
            if not sym:
                continue
            nm = (row.get(name_col) or "").strip() if name_col else ""
            out[sym] = nm
        return out


def _select_baseline_old_run(conn: sqlite3.Connection) -> RunSelection:
    sql = """
    SELECT run_id, created_at
    FROM backtest_runs
    WHERE scoring_profile='old'
      AND top_n=5
      AND rebalance_frequency='weekly'
      AND min_holding_days=10
      AND keep_rank_threshold=9
      AND enable_position_stop_loss=1
      AND ABS(position_stop_loss_pct - 0.10) < 1e-9
      AND stop_loss_cash_mode='keep_cash'
      AND enable_trailing_stop=0
      AND market_filter_enabled=0
      AND entry_gate_enabled=0
      AND enable_overheat_entry_gate=0
      AND entry_quality_gate_enabled=0
      AND max_single_position_weight <= 0.20
    ORDER BY datetime(created_at) DESC
    LIMIT 1
    """
    row = conn.execute(sql).fetchone()
    if not row:
        raise RuntimeError("baseline_old 조건을 만족하는 backtest_runs를 찾지 못했습니다")
    return RunSelection(run_id=str(row[0]), created_at=str(row[1]))


def _trading_days_between(conn: sqlite3.Connection, start_date: str, end_date: str) -> int | None:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM (SELECT DISTINCT date FROM daily_prices WHERE date BETWEEN ? AND ?)
        """,
        (start_date, end_date),
    ).fetchone()
    return int(row[0]) if row is not None else None


def _compute_holding_days(
    conn: sqlite3.Connection,
    run_id: str,
    symbol: str,
    latest_holdings_date: str,
    holding_row: sqlite3.Row,
    holdings_cols: set[str],
) -> str:
    if "holding_days" in holdings_cols and holding_row["holding_days"] is not None:
        return str(holding_row["holding_days"])

    for date_col in ["entry_date", "entry_signal_date", "opened_date", "buy_date"]:
        if date_col in holdings_cols and holding_row[date_col]:
            td = _trading_days_between(conn, str(holding_row[date_col]), latest_holdings_date)
            return str(td) if td is not None else "계산 불가"

    seq_row = conn.execute(
        """
        SELECT MIN(date)
        FROM backtest_holdings
        WHERE run_id=? AND symbol=? AND date<=?
        """,
        (run_id, symbol, latest_holdings_date),
    ).fetchone()
    if seq_row and seq_row[0]:
        td = _trading_days_between(conn, str(seq_row[0]), latest_holdings_date)
        return str(td) if td is not None else "계산 불가"
    return "계산 불가"


def _symbol_name(symbol_names: dict[str, str], symbol: str) -> str:
    return symbol_names.get(symbol) or f"{symbol} (종목명 미확인)"


def _fmt_weight_pct(value: Any) -> str:
    return f"{float(value) * 100:.1f}%" if value is not None else "N/A"


def _fmt_signed_pct(value: Any) -> str:
    if value is None:
        return "N/A"
    return f"{float(value) * 100:+.1f}%"


def _html_table(headers: list[str], rows: list[list[str]]) -> str:
    thead = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{html.escape(c)}</td>" for c in row)
        body_rows.append(f"<tr>{cells}</tr>")
    tbody = "".join(body_rows) if body_rows else f"<tr><td colspan='{len(headers)}'>없음</td></tr>"
    return f"<table><thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>"


def main() -> int:
    parser = argparse.ArgumentParser(description="Mac 전용 paper trading report-only 생성기")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--universe", default=DEFAULT_UNIVERSE)
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR)
    parser.add_argument("--logs-dir", default=DEFAULT_LOGS_DIR)
    parser.add_argument("--as-of-date", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = _expand(args.db)
    universe_path = _expand(args.universe)
    reports_dir = _expand(args.reports_dir)
    logs_dir = _expand(args.logs_dir)

    if not db_path.exists():
        raise RuntimeError(f"DB 파일이 없습니다: {db_path}")

    symbol_names = _load_symbol_names(universe_path)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    _require_columns(conn, "daily_scores", ["date", "symbol"])
    _require_columns(conn, "daily_universe", ["date", "symbol"])
    _require_columns(conn, "backtest_runs", ["run_id", "created_at"])
    _require_columns(conn, "backtest_holdings", ["run_id", "date", "symbol", "weight"])
    _require_columns(conn, "daily_prices", ["date"])

    holdings_cols = set(_table_columns(conn, "backtest_holdings"))
    scores_cols = set(_table_columns(conn, "daily_scores"))
    results_cols = set(_table_columns(conn, "backtest_results"))
    risk_cols = set(_table_columns(conn, "backtest_risk_events"))

    selected = _select_baseline_old_run(conn)

    date_where = ""
    params: list[Any] = []
    if args.as_of_date:
        date_where = " AND ds.date <= ?"
        params.append(args.as_of_date)

    latest_signal_date = conn.execute(
        f"""
        SELECT MAX(ds.date)
        FROM daily_scores ds
        JOIN daily_universe du ON du.date=ds.date AND du.symbol=ds.symbol
        WHERE 1=1 {date_where}
        """,
        params,
    ).fetchone()[0]
    if not latest_signal_date:
        raise RuntimeError("daily_scores JOIN daily_universe 기준 latest_signal_date를 찾지 못했습니다")

    hrow = conn.execute(
        "SELECT MAX(date) FROM backtest_holdings WHERE run_id=? AND date <= ?",
        (selected.run_id, latest_signal_date),
    ).fetchone()
    latest_holdings_date = hrow[0]
    if not latest_holdings_date:
        raise RuntimeError("선택 run_id 내 latest_holdings_date를 찾지 못했습니다")

    prow = conn.execute(
        "SELECT MAX(date) FROM backtest_holdings WHERE run_id=? AND date < ?",
        (selected.run_id, latest_holdings_date),
    ).fetchone()
    previous_holdings_date = prow[0]

    holding_select = [
        "symbol",
        "weight",
        f"{_col_expr('holding_days', holdings_cols)} AS holding_days",
        f"{_col_expr('pnl_pct', holdings_cols)} AS pnl_pct",
        f"{_col_expr('entry_date', holdings_cols)} AS entry_date",
        f"{_col_expr('entry_signal_date', holdings_cols)} AS entry_signal_date",
        f"{_col_expr('opened_date', holdings_cols)} AS opened_date",
        f"{_col_expr('buy_date', holdings_cols)} AS buy_date",
        f"{_col_expr('rank', holdings_cols)} AS holding_rank",
        f"{_col_expr('score', holdings_cols)} AS holding_score",
        f"{_col_expr('pnl', holdings_cols)} AS pnl",
        f"{_col_expr('return', holdings_cols)} AS holding_return",
    ]
    holdings = conn.execute(
        f"SELECT {', '.join(holding_select)} FROM backtest_holdings WHERE run_id=? AND date=? ORDER BY weight DESC, symbol ASC",
        (selected.run_id, latest_holdings_date),
    ).fetchall()
    if not holdings:
        raise RuntimeError("현재 보유 종목이 없습니다")

    actual_exposure = float(sum(float(r["weight"] or 0.0) for r in holdings))
    actual_cash = 1.0 - actual_exposure

    current_symbols = {str(r["symbol"]) for r in holdings}
    candidate_select = [
        "ds.symbol",
        f"{_col_expr('rank', scores_cols, '999999')} AS rank",
        f"{_col_expr('score', scores_cols)} AS score",
    ]
    candidates = conn.execute(
        f"""
        SELECT {', '.join(candidate_select)}
        FROM daily_scores ds
        JOIN daily_universe du ON du.date=ds.date AND du.symbol=ds.symbol
        WHERE ds.date=?
        ORDER BY rank ASC, ds.symbol ASC
        """,
        (latest_signal_date,),
    ).fetchall()
    eligible_new = [r for r in candidates if str(r["symbol"]) not in current_symbols]
    suggested_weight = (actual_exposure / 5.0) if actual_exposure > 0 else 0.2

    prev_symbols = set()
    if previous_holdings_date:
        prev_symbols = {
            str(r[0])
            for r in conn.execute(
                "SELECT symbol FROM backtest_holdings WHERE run_id=? AND date=?", (selected.run_id, previous_holdings_date)
            ).fetchall()
        }

    sold_symbols = sorted(prev_symbols - current_symbols)
    added_symbols = sorted(current_symbols - prev_symbols)
    kept_symbols = sorted(current_symbols & prev_symbols)

    reports_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / f"mac_paper_report_{latest_signal_date}.log"
    logging.basicConfig(filename=str(log_path), level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info("table_columns backtest_holdings=%s", sorted(holdings_cols))
    logging.info("table_columns daily_scores=%s", sorted(scores_cols))
    logging.info("table_columns backtest_results=%s", sorted(results_cols))
    logging.info("table_columns backtest_risk_events=%s", sorted(risk_cols))

    md_path = reports_dir / f"{latest_signal_date}_paper_report.md"
    html_path = reports_dir / f"{latest_signal_date}_paper_report.html"
    summary_path = reports_dir / f"{latest_signal_date}_paper_report_summary.json"

    md = []
    md.append(f"# Paper Trading 리포트 ({latest_signal_date})")
    md.append("\n## 1. 오늘의 결론")
    md.append(f"- baseline_old run_id `{selected.run_id}` 기준으로 리포트를 생성했습니다.")
    md.append("\n## 2. 현재 포트폴리오")
    md.append(f"- 보유 종목 수: {len(holdings)}")
    md.append(f"- 주식 비중: {actual_exposure:.4f}")
    md.append(f"- 현금 비중: {actual_cash:.4f}")
    md.append(f"- 종목당 기본 목표 비중: {(actual_exposure/len(holdings)):.4f}")
    md.append("\n## 3. 전일 대비 변화")
    md.append(f"- 직전 보유일: {previous_holdings_date or 'N/A'}")
    md.append(f"- 현재 보유일: {latest_holdings_date}")
    md.append(f"- 정리 종목 수: {len(sold_symbols)}")
    md.append(f"- 신규 편입 수: {len(added_symbols)}")
    md.append(f"- 유지 종목 수: {len(kept_symbols)}")
    md.append("\n## 4. 정리/매도 내역")
    md.append("- 참고: 체결가 기반 확정손익이 아닐 수 있습니다.")
    for sym in sold_symbols:
        md.append(f"- {_symbol_name(symbol_names, sym)} | 정리일 {latest_holdings_date} | 정리사유 리밸런싱 | 비중 N/A | 손익 N/A | 참고 risk_event 또는 평가손익")
    md.append("\n## 5. 신규 편입 내역")
    for sym in added_symbols:
        md.append(f"- {_symbol_name(symbol_names, sym)}")
    md.append("\n## 6. 현재 보유 종목")
    for r in holdings:
        sym = str(r["symbol"])
        holding_days = _compute_holding_days(conn, selected.run_id, sym, latest_holdings_date, r, holdings_cols)
        pnl_like = r["pnl_pct"]
        if pnl_like is None:
            pnl_like = r["holding_return"]
        if pnl_like is None:
            pnl_like = r["pnl"]
        rank_like = r["holding_rank"]
        md.append(
            f"- {_symbol_name(symbol_names, sym)} | 판단 유지 | 비중 {float(r['weight']):.4f} | 보유일수 {holding_days} | 손익 {pnl_like if pnl_like is not None else 'N/A'} | 최근순위 {rank_like if rank_like is not None else 'N/A'} | 과열도 N/A | 요약 기존보유 | 주의 N/A | 이유 baseline_old"
        )
    md.append("\n## 7. 신규 매수 후보")
    for r in eligible_new[:10]:
        sym = str(r["symbol"])
        md.append(f"- {_symbol_name(symbol_names, sym)} | 제안비중 {suggested_weight:.4f} | 전체순위 {r['rank'] if r['rank'] is not None else 'N/A'} | 점수 {r['score'] if r['score'] is not None else 'N/A'} | 과열도 N/A | 요약 후보 | 주의 N/A | 이유 유니버스+스코어")
    md.append("\n## 8. 참고용 후보 5개")
    for r in eligible_new[:5]:
        md.append(f"- {_symbol_name(symbol_names, str(r['symbol']))} (rank={r['rank'] if r['rank'] is not None else 'N/A'}, score={r['score'] if r['score'] is not None else 'N/A'})")
    md.append("\n## 9. 운영 규칙")
    md.append("- 본 스크립트는 DB 읽기 전용이며 주문/자동매매/DB업데이트를 수행하지 않습니다.")
    md.append("\n## 10. 손익 기준 설명")
    md.append("- risk event가 있으면 event 수익률을 우선 참고하고, 없으면 직전 보유일 마지막 평가손익을 참고합니다.")
    md.append("\n## 11. 과열도 설명")
    md.append("- 과열도 데이터가 없으면 N/A로 표시합니다.")

    sold_rows = [
        [
            _symbol_name(symbol_names, sym),
            latest_holdings_date,
            "리밸런싱",
            "N/A",
            "N/A",
            "risk_event 또는 평가손익 참고",
        ]
        for sym in sold_symbols
    ]
    added_rows = [[_symbol_name(symbol_names, sym)] for sym in added_symbols]
    holdings_rows = []
    for r in holdings:
        sym = str(r["symbol"])
        holding_days = _compute_holding_days(conn, selected.run_id, sym, latest_holdings_date, r, holdings_cols)
        pnl_like = r["pnl_pct"]
        if pnl_like is None:
            pnl_like = r["holding_return"]
        if pnl_like is None:
            pnl_like = r["pnl"]
        rank_like = r["holding_rank"]
        holdings_rows.append(
            [
                _symbol_name(symbol_names, sym),
                "유지",
                _fmt_weight_pct(r["weight"]),
                str(holding_days),
                _fmt_signed_pct(pnl_like),
                str(rank_like) if rank_like is not None else "N/A",
                "N/A",
                "기존보유",
                "N/A",
                "baseline_old",
            ]
        )
    candidate_rows = [
        [
            _symbol_name(symbol_names, str(r["symbol"])),
            _fmt_weight_pct(suggested_weight),
            str(r["rank"]) if r["rank"] is not None else "N/A",
            str(r["score"]) if r["score"] is not None else "N/A",
            "N/A",
            "후보",
            "N/A",
            "유니버스+스코어",
        ]
        for r in eligible_new[:10]
    ]
    ref_rows = [
        [
            _symbol_name(symbol_names, str(r["symbol"])),
            str(r["rank"]) if r["rank"] is not None else "N/A",
            str(r["score"]) if r["score"] is not None else "N/A",
        ]
        for r in eligible_new[:5]
    ]

    html_doc = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Paper Trading 리포트 ({html.escape(latest_signal_date)})</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; line-height: 1.5; }}
    h1, h2 {{ margin-top: 1.2em; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 16px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px 10px; text-align: left; }}
    th {{ background: #f5f7fa; }}
    .note {{ background: #fff8e1; border: 1px solid #f2d98b; padding: 10px; border-radius: 6px; margin: 10px 0 18px; }}
  </style>
</head>
<body>
  <h1>Paper Trading 리포트 ({html.escape(latest_signal_date)})</h1>
  <h2>1. 오늘의 결론</h2>
  <p>baseline_old run_id <code>{html.escape(selected.run_id)}</code> 기준으로 리포트를 생성했습니다.</p>
  <h2>2. 현재 포트폴리오</h2>
  {_html_table(["항목", "값"], [["보유 종목 수", str(len(holdings))], ["주식 비중", _fmt_weight_pct(actual_exposure)], ["현금 비중", _fmt_weight_pct(actual_cash)], ["종목당 기본 목표 비중", _fmt_weight_pct(actual_exposure/len(holdings))]])}
  <h2>3. 전일 대비 변화</h2>
  {_html_table(["항목", "값"], [["직전 보유일", previous_holdings_date or "N/A"], ["현재 보유일", latest_holdings_date], ["정리 종목 수", str(len(sold_symbols))], ["신규 편입 수", str(len(added_symbols))], ["유지 종목 수", str(len(kept_symbols))]])}
  <h2>4. 정리/매도 내역</h2>
  {_html_table(["종목", "정리일", "정리사유", "비중", "손익", "참고"], sold_rows)}
  <div class="note">참고: 체결가 기반 확정손익이 아닐 수 있습니다.</div>
  <h2>5. 신규 편입 내역</h2>
  {_html_table(["종목"], added_rows)}
  <h2>6. 현재 보유 종목</h2>
  {_html_table(["종목", "판단", "비중", "보유일수", "손익", "최근순위", "과열도", "요약", "주의", "이유"], holdings_rows)}
  <h2>7. 신규 매수 후보</h2>
  {_html_table(["종목", "제안비중", "전체순위", "점수", "과열도", "요약", "주의", "이유"], candidate_rows)}
  <h2>8. 참고용 후보 5개</h2>
  {_html_table(["종목", "rank", "score"], ref_rows)}
  <h2>9. 운영 규칙</h2>
  <p>본 스크립트는 DB 읽기 전용이며 주문/자동매매/DB업데이트를 수행하지 않습니다.</p>
  <h2>10. 손익 기준 설명</h2>
  <div class="note">risk event가 있으면 event 수익률을 우선 참고하고, 없으면 직전 보유일 마지막 평가손익을 참고합니다.</div>
  <h2>11. 과열도 설명</h2>
  <div class="note">과열도 데이터가 없으면 N/A로 표시합니다.</div>
</body>
</html>
"""

    summary = {
        "selected_run_id": selected.run_id,
        "selected_run_created_at": selected.created_at,
        "latest_signal_date": latest_signal_date,
        "latest_holdings_date": latest_holdings_date,
        "previous_holdings_date": previous_holdings_date,
        "holdings_count": len(holdings),
        "actual_exposure": actual_exposure,
        "actual_cash": actual_cash,
        "new_candidate_count": len(eligible_new),
        "sold_count": len(sold_symbols),
        "added_count": len(added_symbols),
        "kept_count": len(kept_symbols),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    if not args.dry_run:
        md_path.write_text("\n".join(md) + "\n", encoding="utf-8")
        html_path.write_text(html_doc, encoding="utf-8")
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logging.info("report generated: %s", summary)

    print(f"Markdown: {md_path}")
    print(f"HTML: {html_path}")
    print(f"JSON: {summary_path}")
    print(f"Log: {log_path}")
    print("예시 실행: python scripts/run_mac_paper_report.py --as-of-date 2026-04-30 --dry-run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
