import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.broad_sector_guardrail import GuardrailConfig, apply_broad_sector_guardrail


def _rows(items):
    return [
        {"symbol": sym, "rank": i + 1, "score": float(100 - i)}
        for i, sym in enumerate(items)
    ]


def test_hard_cap2_limits_same_sector_and_fills_with_others():
    ranked = _rows(["000001", "000002", "000003", "000004", "000005", "000006"])
    sector = {
        "000001": "A",
        "000002": "A",
        "000003": "A",
        "000004": "A",
        "000005": "B",
        "000006": "C",
    }
    tgt, cash_slots = apply_broad_sector_guardrail(
        ranked_rows=ranked,
        current_symbols=set(),
        rank_by_symbol={r["symbol"]: r["rank"] for r in ranked},
        entry_index_by_symbol={},
        current_day_index=0,
        top_n=5,
        min_holding_days=10,
        keep_rank_threshold=9,
        symbol_to_sector=sector,
        guardrail=GuardrailConfig("hard_cap", max_names_per_broad_sector=2),
    )
    a_count = sum(1 for s in tgt if sector[s] == "A")
    assert a_count == 2
    assert "000005" in tgt and "000006" in tgt
    assert cash_slots == 1


def test_hard_cap2_allows_cash_when_no_alternative_sector():
    ranked = _rows(["000001", "000002", "000003", "000004", "000005"])
    sector = {sym: "A" for sym in [r["symbol"] for r in ranked]}
    tgt, cash_slots = apply_broad_sector_guardrail(
        ranked_rows=ranked,
        current_symbols=set(),
        rank_by_symbol={r["symbol"]: r["rank"] for r in ranked},
        entry_index_by_symbol={},
        current_day_index=0,
        top_n=5,
        min_holding_days=10,
        keep_rank_threshold=9,
        symbol_to_sector=sector,
        guardrail=GuardrailConfig("hard_cap", max_names_per_broad_sector=2),
    )
    assert len(tgt) == 2
    assert cash_slots == 3
