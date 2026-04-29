import json
import sqlite3
from pathlib import Path

from scripts.daily_paper_trading_runner import main


def _prep_db(db: Path) -> None:
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE backtest_runs(run_id TEXT, strategy_name TEXT, created_at TEXT)")
    conn.execute("CREATE TABLE backtest_holdings(run_id TEXT, date TEXT, symbol TEXT, weight REAL, unrealized_return REAL, rank INTEGER, score REAL)")
    conn.execute("CREATE TABLE daily_scores(symbol TEXT, date TEXT, rank INTEGER, score REAL)")
    conn.execute("CREATE TABLE daily_universe(date TEXT, symbol TEXT)")
    conn.execute("INSERT INTO backtest_runs VALUES('rid-1','baseline_old','2026-04-29T00:00:00Z')")
    conn.execute("INSERT INTO backtest_holdings VALUES('rid-1','2026-04-28','AAA',0.2,0.01,1,1.5)")
    conn.execute("INSERT INTO backtest_holdings VALUES('rid-1','2026-04-28','BBB',0.2,-0.02,2,1.3)")
    conn.execute("INSERT INTO daily_scores VALUES('AAA','2026-04-28',1,1.5)")
    conn.execute("INSERT INTO daily_scores VALUES('CCC','2026-04-28',2,1.4)")
    conn.execute("INSERT INTO daily_scores VALUES('DDD','2026-04-28',3,1.2)")
    conn.execute("INSERT INTO daily_universe VALUES('2026-04-28','AAA')")
    conn.execute("INSERT INTO daily_universe VALUES('2026-04-28','CCC')")
    conn.execute("INSERT INTO daily_universe VALUES('2026-04-28','DDD')")
    conn.commit()
    conn.close()


def test_reports_and_idempotency(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    out = tmp_path / "out"
    _prep_db(db)
    monkeypatch.setattr("sys.argv", ["x", "--db-path", str(db), "--output-dir", str(out), "--skip-update"])
    assert main() == 0

    idem = json.loads((out / "latest_idempotency_check.json").read_text(encoding="utf-8"))
    assert "passed" in idem
    names = {c["name"] for c in idem["checks"]}
    assert "skip_update_no_pipeline_invocation" in names

    md = (out / "latest_paper_trading_report_simple_ko.md").read_text(encoding="utf-8")
    html = (out / "latest_paper_trading_report_simple_ko.html").read_text(encoding="utf-8")
    for token in ["오늘의 결론", "현재 포트폴리오", "현재 보유 종목", "신규 매수 후보", "운영 규칙"]:
        assert token in md
        assert token in html

    idx = (out / "report_index.md").read_text(encoding="utf-8")
    assert "latest_paper_trading_report_simple_ko.html" in idx
