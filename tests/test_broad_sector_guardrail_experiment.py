from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.db import get_connection, init_db
from scripts.run_broad_sector_guardrail_experiment import (
    Candidate,
    _build_score_signature,
    _simulate_candidate,
)


def test_guardrail_none_bypasses_sector_target_builder() -> None:
    conn = get_connection(":memory:")
    init_db(conn)
    conn.execute("INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('AAA','2024-01-01',100,100,100,100,1000)")
    conn.execute("INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('BBB','2024-01-01',100,100,100,100,1000)")
    conn.execute("INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('AAA','2024-01-08',101,101,101,101,1000)")
    conn.execute("INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('BBB','2024-01-08',102,102,102,102,1000)")
    conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('AAA','2024-01-01',1.0,1)")
    conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('BBB','2024-01-01',0.9,2)")
    conn.commit()

    candidate = Candidate("baseline_old_no_sector_guardrail", "old", "none", None, None)

    def _fake_apply(*, symbol_to_sector, **kwargs):
        if symbol_to_sector:
            return {"AAA"}, 4
        return {"BBB"}, 4

    with patch("scripts.run_broad_sector_guardrail_experiment.apply_broad_sector_guardrail", side_effect=_fake_apply):
        try:
            _simulate_candidate(conn, ["2024-01-01", "2024-01-08"], {"AAA": "A", "BBB": "B"}, candidate)
            assert False, "Expected ValueError for guardrail_type=none sector influence"
        except ValueError as exc:
            assert "must bypass sector map effects" in str(exc)


def test_score_signature_has_required_fields() -> None:
    conn = get_connection(":memory:")
    init_db(conn)
    conn.execute("INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('AAA','2024-01-01',100,100,100,100,1000)")
    conn.execute("INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('BBB','2024-01-15',100,100,100,100,1000)")
    conn.execute("INSERT INTO daily_prices(symbol,date,open,high,low,close,volume) VALUES('CCC','2024-01-31',100,100,100,100,1000)")
    conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('AAA','2024-01-01',1.0,1)")
    conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('BBB','2024-01-15',0.9,2)")
    conn.execute("INSERT INTO daily_scores(symbol,date,score,rank) VALUES('CCC','2024-01-31',0.8,3)")
    conn.commit()

    sig = _build_score_signature(conn, "old")
    for key in [
        "scoring_profile",
        "row_count",
        "first_score_date",
        "middle_score_date",
        "last_score_date",
        "top10_symbols_by_sample_date",
    ]:
        assert key in sig
    assert sig["scoring_profile"] == "old"
    assert int(sig["row_count"]) == 3
