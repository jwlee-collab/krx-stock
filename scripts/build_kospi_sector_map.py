#!/usr/bin/env python3
"""Build KOSPI sector mapping CSV for a validated universe file."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_UNIVERSE = Path("data/kospi_valid_universe_495.csv")
DEFAULT_OUTPUT = Path("data/kospi_sector_map.csv")


@dataclass
class ProviderResult:
    rows: dict[str, dict[str, str]]
    warnings: list[str]
    provider_ok: bool


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build sector map for KOSPI valid universe")
    p.add_argument("--universe-file", default=str(DEFAULT_UNIVERSE))
    p.add_argument("--output", default=str(DEFAULT_OUTPUT))
    p.add_argument("--source", choices=["fdr", "pykrx", "manual", "auto"], default="auto")
    p.add_argument("--fallback-sector", default="UNKNOWN")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--allow-partial", action="store_true")
    return p.parse_args()


def _norm_symbol(value: Any) -> str:
    s = "" if value is None else str(value).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(6)


def _read_universe(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    warnings: list[str] = []
    if not path.exists():
        raise SystemExit(f"universe file not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        raise SystemExit(f"universe file is empty: {path}")

    lower = {k.lower(): k for k in rows[0].keys()}
    symbol_col = lower.get("symbol")
    name_col = lower.get("name")
    if symbol_col is None:
        raise SystemExit("universe file requires 'symbol' column")

    unique_rows: dict[str, dict[str, str]] = {}
    duplicate_count = 0
    for row in rows:
        symbol = _norm_symbol(row.get(symbol_col))
        if not symbol:
            continue
        name = str(row.get(name_col) or "").strip() if name_col else ""
        if symbol in unique_rows:
            duplicate_count += 1
            if not unique_rows[symbol]["name"] and name:
                unique_rows[symbol]["name"] = name
            continue
        unique_rows[symbol] = {"symbol": symbol, "name": name}

    if duplicate_count:
        warnings.append(f"deduplicated symbols={duplicate_count}")

    out = sorted(unique_rows.values(), key=lambda x: x["symbol"])
    if not out:
        raise SystemExit("no valid symbols after normalization")
    return out, warnings


def _fetch_fdr() -> ProviderResult:
    warnings: list[str] = []
    try:
        import FinanceDataReader as fdr
    except Exception as e:  # pragma: no cover - depends on env
        return ProviderResult(rows={}, warnings=[f"FinanceDataReader import failed: {e}"], provider_ok=False)

    try:
        df = fdr.StockListing("KOSPI")
    except Exception as e:  # pragma: no cover - depends on network
        return ProviderResult(rows={}, warnings=[f"FinanceDataReader StockListing failed: {e}"], provider_ok=False)

    if df is None or df.empty:
        return ProviderResult(rows={}, warnings=["FinanceDataReader StockListing returned empty dataset"], provider_ok=False)

    columns = {str(c).lower(): c for c in df.columns}
    code_col = columns.get("code")
    name_col = columns.get("name")
    sector_col = columns.get("sector")
    industry_col = columns.get("industry")
    market_col = columns.get("market")

    if code_col is None:
        return ProviderResult(rows={}, warnings=["FinanceDataReader dataset has no Code column"], provider_ok=False)

    rows: dict[str, dict[str, str]] = {}
    for _, r in df.iterrows():
        symbol = _norm_symbol(r.get(code_col))
        if not symbol:
            continue
        rows[symbol] = {
            "name": str(r.get(name_col) or "").strip() if name_col else "",
            "market": str(r.get(market_col) or "KOSPI").strip() if market_col else "KOSPI",
            "sector": str(r.get(sector_col) or "").strip() if sector_col else "",
            "industry": str(r.get(industry_col) or "").strip() if industry_col else "",
            "source": "fdr",
            "source_detail": "FinanceDataReader.StockListing('KOSPI')",
        }

    if sector_col is None:
        warnings.append("FinanceDataReader dataset has no sector column; fallback-sector used")
    if industry_col is None:
        warnings.append("FinanceDataReader dataset has no industry column")

    return ProviderResult(rows=rows, warnings=warnings, provider_ok=True)


def _fetch_pykrx() -> ProviderResult:
    warnings: list[str] = []
    try:
        from pykrx import stock
    except Exception as e:  # pragma: no cover - depends on env
        return ProviderResult(rows={}, warnings=[f"pykrx import failed: {e}"], provider_ok=False)

    try:
        tickers = stock.get_market_ticker_list(market="KOSPI")
    except Exception as e:  # pragma: no cover - depends on network
        return ProviderResult(rows={}, warnings=[f"pykrx ticker list failed: {e}"], provider_ok=False)

    if not tickers:
        return ProviderResult(rows={}, warnings=["pykrx returned no KOSPI tickers"], provider_ok=False)

    rows: dict[str, dict[str, str]] = {}
    for t in tickers:
        symbol = _norm_symbol(t)
        if not symbol:
            continue
        name = ""
        try:
            name = str(stock.get_market_ticker_name(symbol) or "").strip()
        except Exception:
            name = ""
        rows[symbol] = {
            "name": name,
            "market": "KOSPI",
            "sector": "",
            "industry": "",
            "source": "pykrx",
            "source_detail": "pykrx.get_market_ticker_list + get_market_ticker_name",
        }

    warnings.append("pykrx metadata does not provide reliable sector/industry; fallback-sector used")
    return ProviderResult(rows=rows, warnings=warnings, provider_ok=True)


def _build_rows(universe_rows: list[dict[str, str]], source: str, fallback_sector: str) -> tuple[list[dict[str, str]], list[str], str]:
    warnings: list[str] = []

    fdr = ProviderResult(rows={}, warnings=[], provider_ok=False)
    pyk = ProviderResult(rows={}, warnings=[], provider_ok=False)

    if source in {"fdr", "auto"}:
        fdr = _fetch_fdr()
        warnings.extend(fdr.warnings)
    if source in {"pykrx", "auto"}:
        pyk = _fetch_pykrx()
        warnings.extend(pyk.warnings)

    used_source = source
    out: list[dict[str, str]] = []
    updated_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for u in universe_rows:
        symbol = u["symbol"]
        base_name = str(u.get("name") or "").strip()

        meta: dict[str, str] = {}
        if source == "fdr":
            meta = fdr.rows.get(symbol, {})
        elif source == "pykrx":
            meta = pyk.rows.get(symbol, {})
        elif source == "manual":
            meta = {}
        else:  # auto
            if symbol in fdr.rows:
                meta = fdr.rows[symbol]
                used_source = "auto(fdr>pykrx>manual)"
            elif symbol in pyk.rows:
                meta = pyk.rows[symbol]
                used_source = "auto(fdr>pykrx>manual)"
            else:
                meta = {}
                used_source = "auto(fdr>pykrx>manual)"

        name = str(meta.get("name") or base_name).strip()
        market = str(meta.get("market") or "KOSPI").strip() or "KOSPI"
        sector = str(meta.get("sector") or "").strip()
        industry = str(meta.get("industry") or "").strip()

        if not sector:
            sector = fallback_sector
        if not industry:
            industry = fallback_sector

        row_source = str(meta.get("source") or "manual")
        row_source_detail = str(meta.get("source_detail") or "universe_file")

        if sector != fallback_sector:
            mapping_status = "mapped"
            note = ""
        elif row_source == "manual":
            mapping_status = "unknown"
            note = "sector unavailable; fallback sector applied"
        else:
            mapping_status = "fallback"
            note = "external metadata missing sector/industry; fallback sector applied"

        out.append(
            {
                "symbol": symbol,
                "name": name,
                "market": market,
                "sector": sector,
                "industry": industry,
                "source": row_source,
                "source_detail": row_source_detail,
                "updated_at_utc": updated_at_utc,
                "mapping_status": mapping_status,
                "note": note,
            }
        )

    return out, warnings, used_source


def _write_csv(path: Path, rows: list[dict[str, str]], overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise SystemExit(f"output exists; use --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "name",
        "market",
        "sector",
        "industry",
        "source",
        "source_detail",
        "updated_at_utc",
        "mapping_status",
        "note",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> None:
    args = parse_args()
    universe_file = Path(args.universe_file)
    output_path = Path(args.output)

    universe_rows, warnings = _read_universe(universe_file)
    output_rows, collect_warnings, used_source = _build_rows(
        universe_rows=universe_rows,
        source=args.source,
        fallback_sector=str(args.fallback_sector),
    )
    warnings.extend(collect_warnings)

    expected_rows = len(universe_rows)
    actual_rows = len(output_rows)
    if actual_rows != expected_rows:
        mismatch = f"output rows mismatch: expected={expected_rows} actual={actual_rows}"
        warnings.append(mismatch)
        if not args.allow_partial:
            raise SystemExit(mismatch)

    _write_csv(path=output_path, rows=output_rows, overwrite=bool(args.overwrite))

    mapped_rows = sum(1 for r in output_rows if r.get("mapping_status") == "mapped")
    unknown_rows = sum(1 for r in output_rows if r.get("sector") == str(args.fallback_sector))
    unknown_ratio = (unknown_rows / actual_rows) if actual_rows else 1.0

    summary = {
        "universe_rows": expected_rows,
        "output_rows": actual_rows,
        "mapped_rows": mapped_rows,
        "unknown_rows": unknown_rows,
        "unknown_ratio": round(unknown_ratio, 6),
        "output": str(output_path),
        "source": used_source,
        "warnings": warnings,
    }
    print(f"[done] wrote sector map rows={actual_rows} output={output_path}")
    print("KOSPI_SECTOR_MAP_JSON=" + json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
