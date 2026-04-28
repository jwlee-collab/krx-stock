from __future__ import annotations

from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True)
class GuardrailConfig:
    guardrail_type: str  # none | hard_cap | soft_penalty
    max_names_per_broad_sector: int | None = None
    soft_penalty_lambda: float | None = None


def normalize_symbol(symbol: str) -> str:
    return str(symbol).strip().zfill(6)


def normalize_sector(value: str | None) -> str:
    text = (value or "").strip()
    return text if text else "UNKNOWN"


def _score_tuple(row: dict, sector_count: int, penalty_lambda: float | None) -> tuple[float, int, str]:
    score = row.get("score")
    rank = int(row.get("rank") or 10**9)
    symbol = str(row.get("symbol"))
    numeric = float(score) if score is not None else -1e18
    if penalty_lambda and penalty_lambda > 0:
        numeric -= float(penalty_lambda) * float(sector_count)
    return (numeric, -rank, symbol)


def apply_broad_sector_guardrail(
    *,
    ranked_rows: list[dict],
    current_symbols: set[str],
    rank_by_symbol: dict[str, int],
    entry_index_by_symbol: dict[str, int],
    current_day_index: int,
    top_n: int,
    min_holding_days: int,
    keep_rank_threshold: int,
    symbol_to_sector: dict[str, str],
    guardrail: GuardrailConfig,
) -> tuple[set[str], int]:
    keep_due_rank = {sym for sym in current_symbols if rank_by_symbol.get(sym, 10**9) <= keep_rank_threshold}
    keep_due_holding = {
        sym
        for sym in current_symbols
        if (current_day_index - entry_index_by_symbol.get(sym, current_day_index)) < min_holding_days
    }
    target: list[str] = list(keep_due_rank | keep_due_holding)

    # Guardrail priority over keep rules.
    if guardrail.guardrail_type == "hard_cap" and guardrail.max_names_per_broad_sector:
        while True:
            counts = Counter(symbol_to_sector.get(sym, "UNKNOWN") for sym in target)
            violators = [s for s, c in counts.items() if c > guardrail.max_names_per_broad_sector]
            if not violators:
                break
            for sector in violators:
                sector_syms = [s for s in target if symbol_to_sector.get(s, "UNKNOWN") == sector]
                sector_syms.sort(key=lambda s: (rank_by_symbol.get(s, 10**9), s), reverse=True)
                remove_n = len(sector_syms) - int(guardrail.max_names_per_broad_sector)
                for sym in sector_syms[:remove_n]:
                    if sym in target:
                        target.remove(sym)

    chosen = set(target)
    if len(chosen) >= top_n:
        return set(target[:top_n]), max(0, top_n - len(target[:top_n]))

    remaining = [r for r in ranked_rows if str(r["symbol"]) not in chosen]
    if guardrail.guardrail_type == "soft_penalty":
        lam = float(guardrail.soft_penalty_lambda or 0.0)
        while len(target) < top_n and remaining:
            counts = Counter(symbol_to_sector.get(sym, "UNKNOWN") for sym in target)
            best_idx = 0
            best_key = None
            for idx, row in enumerate(remaining):
                sym = str(row["symbol"])
                sec = symbol_to_sector.get(sym, "UNKNOWN")
                key = _score_tuple(row, counts[sec], lam)
                if best_key is None or key > best_key:
                    best_key = key
                    best_idx = idx
            row = remaining.pop(best_idx)
            target.append(str(row["symbol"]))
    else:
        cap = guardrail.max_names_per_broad_sector if guardrail.guardrail_type == "hard_cap" else None
        counts = Counter(symbol_to_sector.get(sym, "UNKNOWN") for sym in target)
        for row in remaining:
            if len(target) >= top_n:
                break
            sym = str(row["symbol"])
            sec = symbol_to_sector.get(sym, "UNKNOWN")
            if cap is not None and counts[sec] >= int(cap):
                continue
            target.append(sym)
            counts[sec] += 1

    if len(target) > top_n:
        target = target[:top_n]
    return set(target), max(0, top_n - len(target))
