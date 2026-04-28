#!/usr/bin/env python3
"""Build KOSPI sector mapping CSV for a validated universe file."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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
    p.add_argument(
        "--as-of-date",
        default=None,
        help="Reference date for pykrx lookups (YYYY-MM-DD or YYYYMMDD). Defaults to recent business day.",
    )
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


def _parse_as_of_date(value: str | None) -> date | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise SystemExit(f"invalid --as-of-date format: {value} (use YYYY-MM-DD or YYYYMMDD)")


def _find_latest_validation_end_date(universe_path: Path) -> date | None:
    if not universe_path.exists():
        return None
    try:
        with universe_path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
    except Exception:
        return None
    if not rows:
        return None
    lower = {k.lower(): k for k in rows[0].keys()}
    date_cols = [lower.get("validation_end_date"), lower.get("end_date"), lower.get("date")]
    parsed: list[date] = []
    for col in [c for c in date_cols if c]:
        for row in rows:
            v = str(row.get(col) or "").strip()
            if not v:
                continue
            for fmt in ("%Y-%m-%d", "%Y%m%d"):
                try:
                    parsed.append(datetime.strptime(v, fmt).date())
                    break
                except ValueError:
                    continue
    return max(parsed) if parsed else None


def _default_recent_business_day(base: date) -> date:
    d = base
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


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


def _fetch_pykrx(as_of: date, universe_path: Path) -> ProviderResult:
    warnings: list[str] = []
    try:
        from pykrx import stock
    except Exception as e:  # pragma: no cover - depends on env
        return ProviderResult(rows={}, warnings=[f"pykrx import failed: {e}"], provider_ok=False)

    include_sector_names = [
        "음식료품",
        "섬유의복",
        "종이목재",
        "화학",
        "의약품",
        "비금속광물",
        "철강금속",
        "기계",
        "전기전자",
        "의료정밀",
        "운수장비",
        "유통업",
        "전기가스업",
        "건설업",
        "운수창고업",
        "통신업",
        "금융업",
        "은행",
        "증권",
        "보험",
        "서비스업",
        "제조업",
        "기타 업종지수",
    ]
    exclude_keywords = [
        "코스피",
        "KOSPI",
        "KRX300",
        "대형주",
        "중형주",
        "소형주",
        "고배당",
        "가치주",
        "성장주",
        "동일가중",
        "레버리지",
        "인버스",
        "ESG",
        "배당",
        "테마",
    ]

    # Do not use today directly; use user date or recent business day with fallback dates.
    offsets = [0, 1, 2, 3, 5, 10]
    chosen_date: date | None = None
    index_tickers: list[str] = []
    last_error = ""
    validation_date = _find_latest_validation_end_date(universe_path)
    bases = [("as_of", as_of)]
    if validation_date and validation_date != as_of:
        bases.append(("validation_end_date", validation_date))

    for base_label, base_date in bases:
        for offset in offsets:
            d = base_date - timedelta(days=offset)
            ymd = d.strftime("%Y%m%d")
            try:
                index_tickers = stock.get_index_ticker_list(ymd, market="KOSPI")
                if index_tickers:
                    chosen_date = d
                    break
                warnings.append(f"pykrx retry ({base_label}) date={ymd}: empty index ticker list")
            except Exception as e:  # pragma: no cover - depends on network
                last_error = str(e)
                warnings.append(f"pykrx retry ({base_label}) date={ymd}: get_index_ticker_list failed ({e})")
        if chosen_date is not None:
            break
    if chosen_date is None:
        return ProviderResult(rows={}, warnings=warnings + [f"pykrx index lookup failed: {last_error}"], provider_ok=False)

    rows: dict[str, dict[str, str]] = {}
    sector_candidates_by_symbol: dict[str, list[str]] = {}
    names_by_symbol: dict[str, str] = {}
    ymd = chosen_date.strftime("%Y%m%d")
    for index_ticker in index_tickers:
        try:
            index_name = str(stock.get_index_ticker_name(index_ticker) or "").strip()
        except Exception as e:
            warnings.append(f"index name lookup failed index={index_ticker}: {e}")
            continue

        include = index_name in include_sector_names
        exclude = any(keyword in index_name for keyword in exclude_keywords)
        if not include or exclude:
            continue
        try:
            members = stock.get_index_portfolio_deposit_file(index_ticker, ymd)
        except Exception as e:
            warnings.append(f"portfolio lookup failed index={index_ticker}({index_name}) date={ymd}: {e}")
            continue

        for raw_symbol in members:
            symbol = _norm_symbol(raw_symbol)
            if not symbol:
                continue
            sector_candidates_by_symbol.setdefault(symbol, [])
            if index_name not in sector_candidates_by_symbol[symbol]:
                sector_candidates_by_symbol[symbol].append(index_name)
            if symbol not in names_by_symbol:
                try:
                    names_by_symbol[symbol] = str(stock.get_market_ticker_name(symbol) or "").strip()
                except Exception:
                    names_by_symbol[symbol] = ""

    include_priority = {name: i for i, name in enumerate(include_sector_names)}
    for symbol, candidates in sector_candidates_by_symbol.items():
        ordered = sorted(candidates, key=lambda x: include_priority.get(x, 999))
        primary = ordered[0] if ordered else ""
        status = "mapped" if len(ordered) <= 1 else "conflict_resolved"
        rows[symbol] = {
            "name": names_by_symbol.get(symbol, ""),
            "market": "KOSPI",
            "sector": primary,
            "industry": primary,
            "sector_candidates": ";".join(ordered),
            "source": "pykrx_index_sector",
            "source_detail": f"pykrx index portfolio date={ymd}",
            "mapping_status": status,
        }

    warnings.append(f"pykrx index mapping as_of_date={ymd} mapped_symbols={len(rows)}")
    return ProviderResult(rows=rows, warnings=warnings, provider_ok=True)


def _build_rows(
    universe_rows: list[dict[str, str]],
    source: str,
    fallback_sector: str,
    as_of: date,
    universe_path: Path,
) -> tuple[list[dict[str, str]], list[str], str]:
    warnings: list[str] = []

    fdr = ProviderResult(rows={}, warnings=[], provider_ok=False)
    pyk = ProviderResult(rows={}, warnings=[], provider_ok=False)

    if source in {"fdr", "auto"}:
        fdr = _fetch_fdr()
        warnings.extend(fdr.warnings)
    if source in {"pykrx", "auto"}:
        pyk = _fetch_pykrx(as_of=as_of, universe_path=universe_path)
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
            if symbol in pyk.rows:
                meta = pyk.rows[symbol]
                used_source = "auto(pykrx_index_sector>fdr>manual)"
            elif symbol in fdr.rows:
                meta = fdr.rows[symbol]
                used_source = "auto(pykrx_index_sector>fdr>manual)"
            else:
                meta = {}
                used_source = "auto(pykrx_index_sector>fdr>manual)"

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
        sector_candidates = str(meta.get("sector_candidates") or sector)
        mapping_status_hint = str(meta.get("mapping_status") or "")

        if mapping_status_hint:
            mapping_status = mapping_status_hint
            note = ""
        elif sector != fallback_sector:
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
                "sector_candidates": sector_candidates,
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
        "sector_candidates",
        "source",
        "source_detail",
        "updated_at_utc",
        "mapping_status",
        "note",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for row in rows:
            row = dict(row)
            row["symbol"] = _norm_symbol(row.get("symbol"))
            w.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> None:
    args = parse_args()
    universe_file = Path(args.universe_file)
    output_path = Path(args.output)

    universe_rows, warnings = _read_universe(universe_file)
    cli_as_of = _parse_as_of_date(args.as_of_date)
    fallback_validation_date = _find_latest_validation_end_date(universe_file)
    today_utc = datetime.now(timezone.utc).date()
    as_of_date = cli_as_of or _default_recent_business_day(today_utc)
    if cli_as_of is None and fallback_validation_date and fallback_validation_date < as_of_date:
        warnings.append(
            f"--as-of-date not provided; computed recent business day={as_of_date.isoformat()}, universe validation_end_date max={fallback_validation_date.isoformat()}"
        )

    output_rows, collect_warnings, used_source = _build_rows(
        universe_rows=universe_rows,
        source=args.source,
        fallback_sector=str(args.fallback_sector),
        as_of=as_of_date,
        universe_path=universe_file,
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
