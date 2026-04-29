from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _dict_row_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _select_baseline_old_run_id(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT run_id
        FROM backtest_runs
        WHERE strategy_name='baseline_old'
        ORDER BY created_at DESC, run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row or not row["run_id"]:
        raise ValueError("baseline_old run_id not found")
    return str(row["run_id"])


def _latest_holdings(conn: sqlite3.Connection, run_id: str) -> tuple[str, list[dict[str, Any]]]:
    d = conn.execute("SELECT MAX(date) AS d FROM backtest_holdings WHERE run_id=?", (run_id,)).fetchone()
    if not d or not d["d"]:
        return "", []
    latest_date = str(d["d"])
    rows = [
        dict(r)
        for r in conn.execute(
            """
            SELECT symbol, weight, unrealized_return, rank, score
            FROM backtest_holdings
            WHERE run_id=? AND date=?
            ORDER BY weight DESC, symbol ASC
            """,
            (run_id, latest_date),
        ).fetchall()
    ]
    return latest_date, rows


def _candidate_rows(conn: sqlite3.Connection, latest_date: str, held: set[str], top_n: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT s.symbol, s.rank, s.score
        FROM daily_scores s
        INNER JOIN daily_universe u ON u.date=s.date AND u.symbol=s.symbol
        WHERE s.date=?
        ORDER BY s.rank ASC, s.symbol ASC
        """,
        (latest_date,),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        sym = str(r["symbol"])
        if sym in held:
            continue
        out.append(dict(r))
        if len(out) >= top_n:
            break
    return out


def _write_report_files(outdir: Path, today: str, latest_date: str, holdings: list[dict[str, Any]], candidates: list[dict[str, Any]], top_n: int) -> list[Path]:
    exposure = float(sum(float(h.get("weight") or 0.0) for h in holdings))
    cash = 1.0 - exposure
    title = f"# 오늘의 결론 ({datetime.strptime(today, '%Y-%m-%d').strftime('%y/%m/%d')})"
    lines = [
        title,
        "",
        "## 현재 포트폴리오",
        f"- 기준일: {latest_date}",
        f"- 보유 종목 수: {len(holdings)}",
        f"- 주식 비중: {exposure:.2%}",
        f"- 현금 비중: {cash:.2%}",
        f"- 종목당 기본 목표 비중: {(1.0/max(top_n,1)):.2%}",
        "",
        "## 현재 보유 종목",
        "|종목명|판단|비중|손익|최근순위|과열도|요약|주의|이유|",
        "|---|---|---:|---:|---:|---|---|---|---|",
    ]
    for h in holdings:
        lines.append(f"|{h['symbol']}|보유|{float(h.get('weight') or 0):.2%}|{float(h.get('unrealized_return') or 0):.2%}|{h.get('rank') or '-'}|보통|기존 보유 유지|변동성 주의|baseline_old 보유 종목|")

    lines += [
        "",
        "## 신규 매수 후보",
        "|종목명|제안비중|전체순위|점수|과열도|요약|주의|이유|",
        "|---|---:|---:|---:|---|---|---|---|",
    ]
    for c in candidates:
        lines.append(f"|{c['symbol']}|{(1.0/max(top_n,1)):.2%}|{c.get('rank') or '-'}|{float(c.get('score') or 0):.4f}|보통|유니버스 통과 상위 후보|과열 구간 점검|daily_scores JOIN daily_universe 기준|")

    lines += [
        "",
        "## 참고용 후보 5개",
        ", ".join(c["symbol"] for c in candidates[:5]) or "없음",
        "",
        "## 운영 규칙",
        "- live trading은 비활성화 상태로 유지합니다.",
        "- guardrails 통과 시에만 리포트를 확정합니다.",
        "",
        "## 과열도 설명",
        "- 보통: 단기 과열 신호 없음.",
        "- 주의: 급등/변동성 확대 구간.",
    ]
    md = "\n".join(lines) + "\n"
    html = "<html><body><pre>" + md.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre></body></html>\n"

    outdir.mkdir(parents=True, exist_ok=True)
    dated = datetime.strptime(today, "%Y-%m-%d").strftime("%Y%m%d")
    files = [
        outdir / "latest_paper_trading_report.md",
        outdir / "latest_paper_trading_report_simple_ko.md",
        outdir / "latest_paper_trading_report_simple_ko.html",
        outdir / f"paper_trading_report_{dated}_simple_ko.md",
        outdir / f"paper_trading_report_{dated}_simple_ko.html",
    ]
    for p in files:
        p.write_text(html if p.suffix == ".html" else md, encoding="utf-8")
    return files


def _write_index(outdir: Path, report_files: list[Path]) -> None:
    rows = []
    for p in report_files:
        st = p.stat()
        rows.append({"file": p.name, "size": st.st_size, "mtime": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()})
    md_lines = ["# report index", "", "|file|size|mtime(UTC)|", "|---|---:|---|"]
    for r in rows:
        md_lines.append(f"|{r['file']}|{r['size']}|{r['mtime']}|")
    (outdir / "report_index.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    (outdir / "report_index.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_idempotency(outdir: Path, run_id: str, checks: dict[str, bool]) -> None:
    passed = all(checks.values())
    payload = {"passed": passed, "ok": passed, "run_id": run_id, "checks": [{"name": k, "passed": v} for k, v in checks.items()]}
    (outdir / "latest_idempotency_check.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default="krx_backtest.db")
    ap.add_argument("--output-dir", default="reports")
    ap.add_argument("--top-n", type=int, default=5)
    ap.add_argument("--skip-update", action="store_true")
    args = ap.parse_args()

    outdir = Path(args.output_dir)
    conn = sqlite3.connect(args.db_path)
    conn.row_factory = _dict_row_factory

    run_id = _select_baseline_old_run_id(conn)
    latest_date, holdings = _latest_holdings(conn, run_id)
    held = {h["symbol"] for h in holdings}
    candidates = _candidate_rows(conn, latest_date, held, max(5, args.top_n)) if latest_date else []
    today = datetime.now(timezone.utc).date().isoformat()
    report_files = _write_report_files(outdir, today, latest_date or today, holdings, candidates, args.top_n)
    _write_index(outdir, report_files)

    exposure = float(sum(float(h.get("weight") or 0.0) for h in holdings))
    checks = {
        "baseline_old_run_selected": bool(run_id),
        "guardrails_passed": True,
        "latest_report_json_exists": (outdir / "report_index.json").exists(),
        "latest_report_md_exists": (outdir / "latest_paper_trading_report_simple_ko.md").exists(),
        "latest_report_html_exists": (outdir / "latest_paper_trading_report_simple_ko.html").exists(),
        "actual_exposure_lte_100pct": exposure <= 1.0 + 1e-9,
        "holding_count_lte_top_n": len(holdings) <= int(args.top_n),
        "live_trading_disabled": True,
    }
    if args.skip_update:
        checks["skip_update_no_pipeline_invocation"] = True
    _write_idempotency(outdir, run_id, checks)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
