from __future__ import annotations

import csv
from pathlib import Path

from pipeline.ingest import normalize_krx_symbol


def parse_symbols_arg(symbols_arg: str | None) -> list[str]:
    """Parse comma-separated symbols and normalize as 6-digit KRX codes."""
    if not symbols_arg:
        return []
    parsed = [normalize_krx_symbol(x) for x in symbols_arg.split(",") if x.strip()]
    return sorted(set(parsed))


def load_symbols_from_universe_csv(path: str | Path) -> list[str]:
    """Load candidate symbols from CSV with at least `symbol` column.

    Extra columns (e.g., name, market, note) are ignored.
    """
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"universe file not found: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if "symbol" not in fieldnames:
            raise ValueError(f"CSV missing required column: symbol (columns={fieldnames})")

        symbols: list[str] = []
        for idx, row in enumerate(reader, start=2):
            raw_symbol = (row.get("symbol") or "").strip()
            if not raw_symbol:
                continue
            try:
                symbols.append(normalize_krx_symbol(raw_symbol))
            except ValueError as exc:
                raise ValueError(f"invalid symbol at {csv_path}:{idx}: {raw_symbol}") from exc

    unique_symbols = sorted(set(symbols))
    if not unique_symbols:
        raise ValueError(f"no valid symbols found in universe file: {csv_path}")
    return unique_symbols
