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
DEFAULT_KIND_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
CSV_ENCODINGS = ["cp949", "utf-8-sig", "utf-8"]


@dataclass
class ProviderResult:
    rows: dict[str, dict[str, str]]
    warnings: list[str]
    provider_ok: bool
    source_detail: str = ""


def map_kind_sector_to_broad_sector(sector: str) -> str:
    raw = str(sector or "").strip()
    if not raw:
        return "기타"
    if raw.upper() == "UNKNOWN":
        return "UNKNOWN"

    text = raw.replace(" ", "")
    mapping_rules: list[tuple[str, list[str]]] = [
        ("전기전자/IT하드웨어", ["반도체", "전자부품", "통신및방송장비", "컴퓨터", "전기장비", "절연선", "전동기", "배터리", "전지"]),
        ("자동차/운수장비", ["자동차", "자동차부품", "운수장비", "선박", "항공기"]),
        ("기계/장비", ["특수목적용기계", "일반목적용기계", "기계장비", "기계"]),
        ("건설/엔지니어링", ["건축기술", "엔지니어링", "건물건설", "토목건설", "건설업"]),
        ("금융", ["금융지원서비스업", "기타금융업", "보험업", "은행", "증권", "신탁"]),
        ("화학/소재", ["화학", "기초화학", "기타화학", "의약화학", "플라스틱", "고무", "정유"]),
        ("철강/금속", ["1차철강", "철강", "비철금속", "금속제품"]),
        ("헬스케어", ["의약품", "의료용기기", "바이오", "자연과학연구개발", "연구개발"]),
        ("소비재", ["식품", "음료", "알코올", "담배", "의복", "가구", "귀금속", "장신용품"]),
        ("유통/상사", ["종합소매", "도매", "상품중개", "유통업"]),
        ("운송", ["항공여객", "해상운송", "도로화물", "육상여객", "운수창고", "운수창고업"]),
        ("에너지/유틸리티", ["전기가스", "연료용가스", "전력", "에너지", "가스"]),
        ("소프트웨어/커뮤니케이션", ["소프트웨어", "인터넷", "자료처리", "포털", "방송", "통신"]),
        ("부동산", ["부동산", "리츠"]),
    ]
    for broad_sector, keywords in mapping_rules:
        if any(keyword in text for keyword in keywords):
            return broad_sector
    return "기타"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build sector map for KOSPI valid universe")
    p.add_argument("--universe-file", default=str(DEFAULT_UNIVERSE))
    p.add_argument("--output", default=str(DEFAULT_OUTPUT))
    p.add_argument("--source", choices=["kind", "krx-file", "fdr", "pykrx", "manual", "auto"], default="auto")
    p.add_argument("--input-sector-file", default=None, help="Path to user-downloaded KRX/KIND sector file (CSV/XLS/XLSX)")
    p.add_argument("--kind-url", default=DEFAULT_KIND_URL, help="KIND corpList download URL")
    p.add_argument("--encoding", default="auto", help="CSV encoding (auto|cp949|utf-8-sig|utf-8)")
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
    market_col = lower.get("market")
    if symbol_col is None:
        raise SystemExit("universe file requires 'symbol' column")

    unique_rows: dict[str, dict[str, str]] = {}
    duplicate_count = 0
    for row in rows:
        symbol = _norm_symbol(row.get(symbol_col))
        if not symbol:
            continue
        name = str(row.get(name_col) or "").strip() if name_col else ""
        market = str(row.get(market_col) or "KOSPI").strip() if market_col else "KOSPI"
        if symbol in unique_rows:
            duplicate_count += 1
            if not unique_rows[symbol]["name"] and name:
                unique_rows[symbol]["name"] = name
            if not unique_rows[symbol]["market"] and market:
                unique_rows[symbol]["market"] = market
            continue
        unique_rows[symbol] = {"symbol": symbol, "name": name, "market": market or "KOSPI"}

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
    except Exception as e:  # pragma: no cover
        return ProviderResult(rows={}, warnings=[f"FinanceDataReader import failed: {e}"], provider_ok=False)

    try:
        df = fdr.StockListing("KOSPI")
    except Exception as e:  # pragma: no cover
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

    return ProviderResult(rows=rows, warnings=warnings, provider_ok=True, source_detail="FinanceDataReader.StockListing('KOSPI')")


def _fetch_pykrx(as_of: date, universe_path: Path) -> ProviderResult:
    warnings: list[str] = []
    try:
        from pykrx import stock
    except Exception as e:  # pragma: no cover
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
    exclude_keywords = ["코스피", "KOSPI", "KRX300", "대형주", "중형주", "소형주", "고배당", "가치주", "성장주", "동일가중", "레버리지", "인버스", "ESG", "배당", "테마"]

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
            except Exception as e:  # pragma: no cover
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
        rows[symbol] = {
            "name": names_by_symbol.get(symbol, ""),
            "market": "KOSPI",
            "sector": primary,
            "industry": primary,
            "sector_candidates": ";".join(ordered),
            "index_ticker": "",
            "index_name": primary,
            "as_of_date": chosen_date.isoformat(),
            "source": "pykrx",
            "source_detail": f"pykrx index portfolio date={ymd}",
        }

    warnings.append(f"pykrx index mapping as_of_date={ymd} mapped_symbols={len(rows)}")
    return ProviderResult(rows=rows, warnings=warnings, provider_ok=True, source_detail=f"pykrx index portfolio date={ymd}")


def _read_csv_with_fallback(path: Path, encoding_opt: str) -> tuple[Any, str]:
    import pandas as pd

    if encoding_opt != "auto":
        return pd.read_csv(path, dtype=str, encoding=encoding_opt), encoding_opt

    errors: list[str] = []
    for enc in CSV_ENCODINGS:
        try:
            return pd.read_csv(path, dtype=str, encoding=enc), enc
        except Exception as e:
            errors.append(f"{enc}:{e}")
    raise ValueError("csv encoding detection failed: " + " | ".join(errors))


def _resolve_col(columns: list[str], candidates: list[str]) -> str | None:
    normalized = {c.strip().lower(): c for c in columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def _fetch_kind(kind_url: str) -> ProviderResult:
    warnings: list[str] = []
    try:
        import pandas as pd
    except Exception as e:  # pragma: no cover
        return ProviderResult(rows={}, warnings=[f"pandas import failed: {e}"], provider_ok=False)

    try:
        frames = pd.read_html(kind_url)
    except Exception as e:
        return ProviderResult(rows={}, warnings=[f"KIND read_html failed: {e}"], provider_ok=False, source_detail=kind_url)

    if not frames:
        return ProviderResult(rows={}, warnings=["KIND read_html returned no tables"], provider_ok=False, source_detail=kind_url)

    df = frames[0]
    cols = [str(c) for c in df.columns]
    symbol_col = _resolve_col(cols, ["종목코드", "단축코드", "code", "ticker", "symbol"])
    name_col = _resolve_col(cols, ["회사명", "종목명", "한글 종목명", "name"])
    sector_col = _resolve_col(cols, ["업종", "업종명", "산업", "산업명", "KRX업종", "sector"])

    if symbol_col is None or sector_col is None:
        return ProviderResult(rows={}, warnings=[f"KIND columns not found symbol_col={symbol_col} sector_col={sector_col}"], provider_ok=False, source_detail=kind_url)

    rows: dict[str, dict[str, str]] = {}
    for _, r in df.iterrows():
        symbol = _norm_symbol(r.get(symbol_col))
        if not symbol:
            continue
        sector = str(r.get(sector_col) or "").strip()
        rows[symbol] = {
            "name": str(r.get(name_col) or "").strip() if name_col else "",
            "market": "",
            "sector": sector,
            "industry": sector,
            "source": "kind",
            "source_detail": kind_url,
        }

    return ProviderResult(rows=rows, warnings=warnings, provider_ok=True, source_detail=kind_url)


def _fetch_krx_file(input_file: Path, encoding_opt: str) -> ProviderResult:
    warnings: list[str] = []
    try:
        import pandas as pd
    except Exception as e:  # pragma: no cover
        return ProviderResult(rows={}, warnings=[f"pandas import failed: {e}"], provider_ok=False, source_detail=str(input_file))

    if not input_file.exists():
        return ProviderResult(rows={}, warnings=[f"input sector file not found: {input_file}"], provider_ok=False, source_detail=str(input_file))

    ext = input_file.suffix.lower()
    try:
        if ext == ".csv":
            df, chosen_encoding = _read_csv_with_fallback(input_file, encoding_opt)
            warnings.append(f"krx-file csv encoding={chosen_encoding}")
        elif ext in {".xls", ".xlsx"}:
            try:
                df = pd.read_excel(input_file, dtype=str)
            except Exception:
                frames = pd.read_html(str(input_file))
                if not frames:
                    raise
                df = frames[0]
        else:
            return ProviderResult(rows={}, warnings=[f"unsupported input-sector-file extension: {ext}"], provider_ok=False, source_detail=str(input_file))
    except Exception as e:
        return ProviderResult(rows={}, warnings=[f"failed to read input-sector-file: {e}"], provider_ok=False, source_detail=str(input_file))

    cols = [str(c) for c in df.columns]
    symbol_col = _resolve_col(cols, ["종목코드", "단축코드", "표준코드", "code", "ticker", "symbol"])
    name_col = _resolve_col(cols, ["종목명", "회사명", "한글 종목명", "name"])
    sector_col = _resolve_col(cols, ["업종", "업종명", "산업", "산업명", "지수업종", "KRX업종", "sector"])
    industry_col = _resolve_col(cols, ["업종", "산업", "industry"])

    if symbol_col is None or sector_col is None:
        return ProviderResult(rows={}, warnings=[f"krx-file columns not found symbol_col={symbol_col} sector_col={sector_col}"], provider_ok=False, source_detail=str(input_file))

    rows: dict[str, dict[str, str]] = {}
    for _, r in df.iterrows():
        symbol = _norm_symbol(r.get(symbol_col))
        if not symbol:
            continue
        sector = str(r.get(sector_col) or "").strip()
        industry = str(r.get(industry_col) or "").strip() if industry_col else ""
        rows[symbol] = {
            "name": str(r.get(name_col) or "").strip() if name_col else "",
            "market": "",
            "sector": sector,
            "industry": industry or sector,
            "source": "krx-file",
            "source_detail": str(input_file),
        }

    return ProviderResult(rows=rows, warnings=warnings, provider_ok=True, source_detail=str(input_file))


def _build_rows(
    universe_rows: list[dict[str, str]],
    source: str,
    fallback_sector: str,
    as_of: date,
    universe_path: Path,
    allow_partial: bool,
    input_sector_file: str | None,
    kind_url: str,
    encoding_opt: str,
) -> tuple[list[dict[str, str]], list[str], str, str]:
    warnings: list[str] = []
    used_source = source
    used_source_detail = ""

    fdr = ProviderResult(rows={}, warnings=[], provider_ok=False)
    pyk = ProviderResult(rows={}, warnings=[], provider_ok=False)
    kind = ProviderResult(rows={}, warnings=[], provider_ok=False)
    krx_file = ProviderResult(rows={}, warnings=[], provider_ok=False)

    if source in {"fdr", "auto"}:
        fdr = _fetch_fdr()
        warnings.extend(fdr.warnings)
    if source in {"pykrx", "auto"}:
        pyk = _fetch_pykrx(as_of=as_of, universe_path=universe_path)
        warnings.extend(pyk.warnings)
    if source in {"kind", "auto"}:
        kind = _fetch_kind(kind_url=kind_url)
        warnings.extend(kind.warnings)
    if source in {"krx-file", "auto"} and input_sector_file:
        krx_file = _fetch_krx_file(Path(input_sector_file), encoding_opt=encoding_opt)
        warnings.extend(krx_file.warnings)

    out: list[dict[str, str]] = []
    updated_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for u in universe_rows:
        symbol = u["symbol"]
        base_name = str(u.get("name") or "").strip()
        base_market = str(u.get("market") or "KOSPI").strip() or "KOSPI"

        meta: dict[str, str] = {}
        if source == "krx-file":
            meta = krx_file.rows.get(symbol, {})
            used_source, used_source_detail = "krx-file", krx_file.source_detail or (input_sector_file or "")
        elif source == "kind":
            meta = kind.rows.get(symbol, {})
            used_source, used_source_detail = "kind", kind.source_detail or kind_url
        elif source == "pykrx":
            meta = pyk.rows.get(symbol, {})
            used_source, used_source_detail = "pykrx", pyk.source_detail
        elif source == "fdr":
            meta = fdr.rows.get(symbol, {})
            used_source, used_source_detail = "fdr", fdr.source_detail
        elif source == "manual":
            meta = {}
            used_source, used_source_detail = "manual", "universe_file"
        else:  # auto
            if krx_file.provider_ok and symbol in krx_file.rows:
                meta = krx_file.rows[symbol]
                used_source, used_source_detail = "auto(krx-file>kind>pykrx>fdr>manual)", krx_file.source_detail
            elif kind.provider_ok and symbol in kind.rows:
                meta = kind.rows[symbol]
                used_source, used_source_detail = "auto(krx-file>kind>pykrx>fdr>manual)", kind.source_detail
            elif pyk.provider_ok and symbol in pyk.rows:
                meta = pyk.rows[symbol]
                used_source, used_source_detail = "auto(krx-file>kind>pykrx>fdr>manual)", pyk.source_detail
            elif fdr.provider_ok and symbol in fdr.rows:
                meta = fdr.rows[symbol]
                used_source, used_source_detail = "auto(krx-file>kind>pykrx>fdr>manual)", fdr.source_detail
            else:
                meta = {}
                used_source, used_source_detail = "auto(krx-file>kind>pykrx>fdr>manual)", "manual"

        row_source = str(meta.get("source") or ("manual" if source != "manual" else "manual"))
        row_source_detail = str(meta.get("source_detail") or used_source_detail or "universe_file")

        name = str(meta.get("name") or base_name).strip()
        market = str(meta.get("market") or base_market).strip() or base_market
        sector = str(meta.get("sector") or "").strip() or fallback_sector
        industry = str(meta.get("industry") or "").strip() or sector
        broad_sector = map_kind_sector_to_broad_sector(sector)
        sector_candidates = str(meta.get("sector_candidates") or sector)
        index_ticker = str(meta.get("index_ticker") or "")
        index_name = str(meta.get("index_name") or "")
        row_as_of_date = str(meta.get("as_of_date") or as_of.isoformat())

        mapping_status = "mapped" if sector != fallback_sector else "unknown"
        note = "" if mapping_status == "mapped" else "sector unavailable; fallback sector applied"

        out.append(
            {
                "symbol": symbol,
                "name": name,
                "market": market,
                "sector": sector,
                "broad_sector": broad_sector,
                "industry": industry,
                "sector_candidates": sector_candidates,
                "index_ticker": index_ticker,
                "index_name": index_name,
                "as_of_date": row_as_of_date,
                "source": row_source,
                "source_detail": row_source_detail,
                "updated_at_utc": updated_at_utc,
                "mapping_status": mapping_status,
                "note": note,
            }
        )

    mapped_rows = sum(1 for r in out if r.get("sector") != fallback_sector)
    ratio = mapped_rows / len(out) if out else 0.0
    if source == "krx-file":
        if ratio < 0.8:
            warnings.append(f"krx-file mapping ratio low: {ratio:.4f} (<0.8)")
        if ratio < 0.3 and not allow_partial:
            raise SystemExit(f"krx-file mapping ratio too low: {ratio:.4f} (<0.3) without --allow-partial")

    if source == "auto":
        if input_sector_file and not krx_file.provider_ok:
            warnings.append("auto: krx-file source unavailable, fallback to kind/pykrx/fdr/manual")
        if not kind.provider_ok:
            warnings.append("auto: KIND source unavailable, fallback to pykrx/fdr/manual")

    return out, warnings, used_source, used_source_detail


def _write_csv(path: Path, rows: list[dict[str, str]], overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise SystemExit(f"output exists; use --overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "name",
        "market",
        "sector",
        "broad_sector",
        "industry",
        "sector_candidates",
        "index_ticker",
        "index_name",
        "as_of_date",
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

    if args.source == "krx-file" and not args.input_sector_file:
        raise SystemExit("--source krx-file requires --input-sector-file")

    universe_rows, warnings = _read_universe(universe_file)
    cli_as_of = _parse_as_of_date(args.as_of_date)
    fallback_validation_date = _find_latest_validation_end_date(universe_file)
    today_utc = datetime.now(timezone.utc).date()
    as_of_date = cli_as_of or _default_recent_business_day(today_utc)
    if cli_as_of is None and fallback_validation_date and fallback_validation_date < as_of_date:
        warnings.append(
            f"--as-of-date not provided; computed recent business day={as_of_date.isoformat()}, universe validation_end_date max={fallback_validation_date.isoformat()}"
        )

    output_rows, collect_warnings, used_source, used_source_detail = _build_rows(
        universe_rows=universe_rows,
        source=args.source,
        fallback_sector=str(args.fallback_sector),
        as_of=as_of_date,
        universe_path=universe_file,
        allow_partial=bool(args.allow_partial),
        input_sector_file=args.input_sector_file,
        kind_url=str(args.kind_url),
        encoding_opt=str(args.encoding),
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

    mapped_rows = sum(1 for r in output_rows if r.get("sector") != str(args.fallback_sector))
    unknown_rows = sum(1 for r in output_rows if r.get("sector") == str(args.fallback_sector))
    unknown_ratio = (unknown_rows / actual_rows) if actual_rows else 1.0
    broad_sector_count = len({str(r.get("broad_sector") or "") for r in output_rows if str(r.get("broad_sector") or "")})
    unknown_broad_sector_rows = sum(1 for r in output_rows if str(r.get("broad_sector") or "") == "UNKNOWN")

    summary = {
        "universe_rows": expected_rows,
        "output_rows": actual_rows,
        "mapped_rows": mapped_rows,
        "unknown_rows": unknown_rows,
        "unknown_ratio": round(unknown_ratio, 6),
        "broad_sector_count": broad_sector_count,
        "unknown_broad_sector_rows": unknown_broad_sector_rows,
        "output": str(output_path),
        "source": used_source,
        "source_detail": used_source_detail,
        "warnings": warnings,
        "sample_mapped_rows": [r for r in output_rows if r.get("mapping_status") == "mapped"][:5],
    }
    if args.source in {"kind", "auto"}:
        summary["kind_url"] = str(args.kind_url)
    if args.input_sector_file:
        summary["input_sector_file"] = str(args.input_sector_file)

    print(f"[done] wrote sector map rows={actual_rows} output={output_path}")
    print("KOSPI_SECTOR_MAP_JSON=" + json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
