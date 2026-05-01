#!/usr/bin/env python3
from __future__ import annotations

import argparse, csv, html, json, logging, sqlite3
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
    keep_rank_threshold: int

def _expand(path: str) -> Path: return Path(path).expanduser().resolve()
def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]: return [str(r[1]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
def _require_columns(conn: sqlite3.Connection, table: str, required: list[str]) -> None:
    cols = set(_table_columns(conn, table)); missing = [c for c in required if c not in cols]
    if not cols: raise RuntimeError(f"필수 테이블 누락: {table}")
    if missing: raise RuntimeError(f"필수 컬럼 누락: table={table}, missing={missing}")
def _col_expr(col: str, available_cols: set[str], fallback: str = "NULL") -> str: return col if col in available_cols else fallback

def _load_symbol_names(universe_csv: Path) -> dict[str, str]:
    with universe_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f); headers = reader.fieldnames or []
        name_col = "name" if "name" in headers else ("company_name" if "company_name" in headers else None)
        return {(row.get("symbol") or "").strip(): ((row.get(name_col) or "").strip() if name_col else "") for row in reader if (row.get("symbol") or "").strip()}

def _select_baseline_old_run(conn: sqlite3.Connection) -> RunSelection:
    row = conn.execute("""
    SELECT run_id, created_at, keep_rank_threshold FROM backtest_runs
    WHERE scoring_profile='old' AND top_n=5 AND rebalance_frequency='weekly' AND min_holding_days=10
      AND keep_rank_threshold=9 AND enable_position_stop_loss=1 AND ABS(position_stop_loss_pct - 0.10) < 1e-9
      AND stop_loss_cash_mode='keep_cash' AND enable_trailing_stop=0 AND market_filter_enabled=0
      AND entry_gate_enabled=0 AND enable_overheat_entry_gate=0 AND entry_quality_gate_enabled=0
      AND max_single_position_weight <= 0.20
    ORDER BY datetime(created_at) DESC LIMIT 1""").fetchone()
    if not row: raise RuntimeError("baseline_old 조건을 만족하는 backtest_runs를 찾지 못했습니다")
    return RunSelection(str(row[0]), str(row[1]), int(row[2] or 9))

def _trading_days_between(conn: sqlite3.Connection, start_date: str, end_date: str) -> int | None:
    r = conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT date FROM daily_prices WHERE date BETWEEN ? AND ?)", (start_date, end_date)).fetchone()
    return int(r[0]) if r else None

def _fmt_weight_pct(v: Any) -> str: return f"{float(v) * 100:.1f}%" if v is not None else "데이터 부족"
def _fmt_signed_pct(v: Any) -> str: return f"{float(v) * 100:+.1f}%" if v is not None else "데이터 부족"
def _fmt_score_100(v: Any) -> str: return f"{float(v) * 100:.1f}" if v is not None else "데이터 부족"
def _fmt_rank(v: Any) -> str: return f"{int(v)}위" if v is not None else "데이터 부족"
def _symbol_name(m: dict[str,str], s: str) -> str: return m.get(s) or f"{s} (종목명 미확인)"
def _html_table(headers: list[str], rows: list[list[str]]) -> str:
    return "<table><thead><tr>" + "".join(f"<th>{html.escape(h)}</th>" for h in headers) + "</tr></thead><tbody>" + ("".join("<tr>"+"".join(f"<td>{html.escape(c)}</td>" for c in r)+"</tr>" for r in rows) if rows else f"<tr><td colspan='{len(headers)}'>없음</td></tr>") + "</tbody></table>"
def _overheat(f: sqlite3.Row | None) -> str:
    if not f: return "낮음"
    out=[]
    if f["ret_1d"] is not None and float(f["ret_1d"])>=0.08: out.append("1일 급등")
    if f["ret_5d"] is not None and float(f["ret_5d"])>=0.15: out.append("5일 급등")
    if f["range_pct"] is not None and float(f["range_pct"])>=0.10: out.append("장중 변동성 확대")
    if f["volume_z20"] is not None and float(f["volume_z20"])>=3.0: out.append("거래량 급증")
    return ", ".join(out) if out else "낮음"

def main() -> int:
    ap=argparse.ArgumentParser(); ap.add_argument("--db",default=DEFAULT_DB); ap.add_argument("--universe",default=DEFAULT_UNIVERSE); ap.add_argument("--reports-dir",default=DEFAULT_REPORTS_DIR); ap.add_argument("--logs-dir",default=DEFAULT_LOGS_DIR); ap.add_argument("--as-of-date",default=None); ap.add_argument("--dry-run",action="store_true"); args=ap.parse_args()
    conn=sqlite3.connect(str(_expand(args.db))); conn.row_factory=sqlite3.Row
    for t,c in [("daily_scores",["date","symbol","score","rank"]),("daily_universe",["date","symbol"]),("backtest_holdings",["run_id","date","symbol","weight","entry_date","unrealized_return","rank","score"]),("daily_features",["date","symbol","ret_1d","ret_5d","range_pct","volume_z20"]),("backtest_risk_events",["run_id","date","symbol","event_type","action","return_pct","trigger_price","reference_price"]),("daily_prices",["date"])]: _require_columns(conn,t,c)
    selected=_select_baseline_old_run(conn); names=_load_symbol_names(_expand(args.universe))
    where=" AND ds.date <= ?" if args.as_of_date else ""; params=[args.as_of_date] if args.as_of_date else []
    latest_signal_date=conn.execute(f"SELECT MAX(ds.date) FROM daily_scores ds JOIN daily_universe du ON du.date=ds.date AND du.symbol=ds.symbol WHERE 1=1 {where}",params).fetchone()[0]
    latest_holdings_date=conn.execute("SELECT MAX(date) FROM backtest_holdings WHERE run_id=? AND date<=?",(selected.run_id,latest_signal_date)).fetchone()[0]
    prev_date=conn.execute("SELECT MAX(date) FROM backtest_holdings WHERE run_id=? AND date<?",(selected.run_id,latest_holdings_date)).fetchone()[0]
    holdings=conn.execute("SELECT symbol,weight,entry_date,unrealized_return,rank,score FROM backtest_holdings WHERE run_id=? AND date=? ORDER BY weight DESC, symbol",(selected.run_id,latest_holdings_date)).fetchall()
    cur={str(r['symbol']) for r in holdings}; actual_exposure=sum(float(r['weight'] or 0) for r in holdings); actual_cash=1.0-actual_exposure
    cands=conn.execute("SELECT ds.symbol, ds.rank, ds.score, du.universe_rank FROM daily_scores ds JOIN daily_universe du ON du.date=ds.date AND du.symbol=ds.symbol WHERE ds.date=? ORDER BY ds.rank, ds.symbol",(latest_signal_date,)).fetchall(); eligible=[r for r in cands if str(r['symbol']) not in cur]
    prev={str(r[0]) for r in conn.execute("SELECT symbol FROM backtest_holdings WHERE run_id=? AND date=?",(selected.run_id,prev_date)).fetchall()} if prev_date else set()
    sold=sorted(prev-cur); added=sorted(cur-prev); kept=sorted(cur&prev)
    fmap={str(r['symbol']):r for r in conn.execute("SELECT symbol, ret_1d, ret_5d, range_pct, volume_z20 FROM daily_features WHERE date=?",(latest_signal_date,)).fetchall()}
    md=[f"# Paper Trading 리포트 ({latest_signal_date})", "\n## 1. 오늘의 결론", f"- baseline_old run_id `{selected.run_id}` 기준으로 리포트를 생성했습니다."]
    md += ["\n## 2. 현재 포트폴리오", f"- 보유 종목 수: {len(holdings)}", f"- 주식 비중: {_fmt_weight_pct(actual_exposure)}", f"- 현금 비중: {_fmt_weight_pct(actual_cash)}"]
    md += ["\n## 3. 전일 대비 변화", f"- 직전 보유일: {prev_date or '데이터 부족'}", f"- 현재 보유일: {latest_holdings_date}", f"- 정리 종목 수: {len(sold)}", f"- 신규 편입 수: {len(added)}", f"- 유지 종목 수: {len(kept)}"]
    md += ["\n## 4. 정리/매도 내역", "- 참고: 체결가 기반 확정손익이 아닐 수 있습니다."]
    sold_rows=[]
    for s in sold:
        risk=conn.execute("SELECT event_type,action,return_pct,trigger_price,reference_price FROM backtest_risk_events WHERE run_id=? AND symbol=? AND date=? ORDER BY event_id DESC LIMIT 1",(selected.run_id,s,latest_holdings_date)).fetchone()
        if risk: pnl,ref=_fmt_signed_pct(risk['return_pct']),f"{risk['event_type']}/{risk['action']}, trigger={risk['trigger_price']}, ref={risk['reference_price']}"
        else:
            pr=conn.execute("SELECT unrealized_return FROM backtest_holdings WHERE run_id=? AND date=? AND symbol=?",(selected.run_id,prev_date,s)).fetchone() if prev_date else None
            pnl,ref=(_fmt_signed_pct(pr['unrealized_return']) if pr else "데이터 부족"),"risk event 없음, 직전 보유일 평가손익 참고"
        md.append(f"- {_symbol_name(names,s)} | 정리일 {latest_holdings_date} | 정리사유 리밸런싱 | 손익 {pnl} | 참고 {ref}")
        sold_rows.append([_symbol_name(names,s),latest_holdings_date,"리밸런싱",pnl,ref])
    holdings_rows=[]; md.append("\n## 6. 현재 보유 종목")
    for r in holdings:
        s=str(r['symbol']); td=_trading_days_between(conn,str(r['entry_date']),latest_holdings_date) if r['entry_date'] else None; days=f"{td}거래일" if td is not None else "데이터 부족"; oh=_overheat(fmap.get(s))
        warnings=[]
        if r['unrealized_return'] is not None and float(r['unrealized_return'])<=-0.08: warnings.append("손절 기준(-10%) 근접 주의")
        if r['unrealized_return'] is not None and float(r['unrealized_return'])>=0.20: warnings.append("수익 구간이나 고정 익절 기준은 없음")
        if r['rank'] is not None and int(r['rank'])>selected.keep_rank_threshold: warnings.append("keep 기준 밖으로 순위 약화")
        if oh!="낮음": warnings.append("단기 과열 신호 확인 필요")
        warn=" / ".join(warnings) if warnings else "특이 경고 없음"
        reason=f"최근순위 {_fmt_rank(r['rank'])}, keep 기준 {selected.keep_rank_threshold}위, 보유일수 {days}, 손익 {_fmt_signed_pct(r['unrealized_return'])}, 과열도 {oh}를 종합해 보유 유지 판단."
        md.append(f"- {_symbol_name(names,s)} | 비중 {_fmt_weight_pct(r['weight'])} | 보유일수 {days} | 손익 {_fmt_signed_pct(r['unrealized_return'])} | 최근순위 {_fmt_rank(r['rank'])} | 점수 {_fmt_score_100(r['score'])} | 과열도 {oh} | 주의 {warn} | 이유 {reason}")
        holdings_rows.append([_symbol_name(names,s),"유지",_fmt_weight_pct(r['weight']),days,_fmt_signed_pct(r['unrealized_return']),_fmt_rank(r['rank']),_fmt_score_100(r['score']),oh,"기존보유",warn,reason])
    md.append("\n## 7. 신규 매수 후보")
    cand_rows=[]
    for r in eligible[:10]:
        s=str(r['symbol']); oh=_overheat(fmap.get(s)); warn="특이 경고 없음" if oh=="낮음" else "진입 전 과열 여부 확인 필요"
        if fmap.get(s) and ((fmap[s]['ret_1d'] is not None and float(fmap[s]['ret_1d'])>=0.08) or (fmap[s]['ret_5d'] is not None and float(fmap[s]['ret_5d'])>=0.15)): warn += " / 급등 직후 진입 리스크"
        reason=f"daily_universe eligible 후보 중 현재 보유 종목을 제외한 상위 후보입니다. 전체순위 {_fmt_rank(r['rank'])}, 점수 {_fmt_score_100(r['score'])}점으로 신규 편입 후보이며 과열도는 {oh}입니다."
        md.append(f"- {_symbol_name(names,s)} | 제안비중 {_fmt_weight_pct(actual_exposure/5 if actual_exposure>0 else 0.2)} | 전체순위 {_fmt_rank(r['rank'])} | 점수 {_fmt_score_100(r['score'])} | 과열도 {oh} | 주의 {warn} | 이유 {reason}")
        cand_rows.append([_symbol_name(names,s),_fmt_weight_pct(actual_exposure/5 if actual_exposure>0 else 0.2),_fmt_rank(r['rank']),_fmt_score_100(r['score']),oh,"후보",warn,reason])
    md += ["\n## 10. 손익 기준 설명", "- 진입일(entry_date) 이후 latest_holdings_date까지의 평가손익이며, 체결가 기반 확정손익이 아닐 수 있음."]

    html_doc=f"""<!doctype html><html lang='ko'><head><meta charset='utf-8'/><style>body{{font-family:sans-serif;margin:24px}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;vertical-align:top;white-space:normal;word-break:break-word}}th{{background:#f5f7fa}}</style></head><body>
<h1>Paper Trading 리포트 ({latest_signal_date})</h1>
{_html_table(['항목','값'],[['보유 종목 수',str(len(holdings))],['주식 비중',_fmt_weight_pct(actual_exposure)],['현금 비중',_fmt_weight_pct(actual_cash)]])}
<h2>정리/매도</h2>{_html_table(['종목','정리일','정리사유','손익','참고'],sold_rows)}
<div>참고: 체결가 기반 확정손익이 아닐 수 있습니다.</div>
<h2>현재 보유</h2>{_html_table(['종목','판단','비중','보유일수','손익','최근순위','점수','과열도','요약','주의','이유'],holdings_rows)}
<h2>신규 후보</h2>{_html_table(['종목','제안비중','전체순위','점수','과열도','요약','주의','이유'],cand_rows)}
<div>현재 보유 종목 손익은 진입일(entry_date) 이후 latest_holdings_date까지의 평가손익이며, 체결가 기반 확정손익이 아닐 수 있음.</div></body></html>"""

    rep=_expand(args.reports_dir); rep.mkdir(parents=True, exist_ok=True)
    md_path=rep/f"{latest_signal_date}_paper_report.md"; html_path=rep/f"{latest_signal_date}_paper_report.html"; sum_path=rep/f"{latest_signal_date}_paper_report_summary.json"
    summary={"selected_run_id":selected.run_id,"selected_run_created_at":selected.created_at,"latest_signal_date":latest_signal_date,"latest_holdings_date":latest_holdings_date,"previous_holdings_date":prev_date,"holdings_count":len(holdings),"actual_exposure":actual_exposure,"actual_cash":actual_cash,"new_candidate_count":len(eligible),"sold_count":len(sold),"added_count":len(added),"kept_count":len(kept),"generated_at":datetime.now(timezone.utc).isoformat(),"current_holdings_preview":[{"symbol":str(r['symbol']),"rank":r['rank'],"score_100":float(r['score']*100) if r['score'] is not None else None} for r in holdings[:5]],"top_new_candidates_preview":[{"symbol":str(r['symbol']),"rank":r['rank'],"score_100":float(r['score']*100) if r['score'] is not None else None} for r in eligible[:5]]}
    if not args.dry_run:
        md_path.write_text("\n".join(md)+"\n",encoding="utf-8"); html_path.write_text(html_doc,encoding="utf-8"); sum_path.write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(f"Markdown: {md_path}\nHTML: {html_path}\nJSON: {sum_path}")
    return 0

if __name__=='__main__': raise SystemExit(main())
