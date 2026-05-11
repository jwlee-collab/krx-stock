from __future__ import annotations

import csv
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pipeline.day_trading.models import IntradayBar


KIS_STOCK_MINUTE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
KIS_STOCK_MINUTE_TR_ID = "FHKST03010200"
KIS_INDEX_MINUTE_PATH = "/uapi/domestic-stock/v1/quotations/inquire-time-indexchartprice"
KIS_INDEX_MINUTE_TR_ID = "FHKUP03510200"
KIS_TOKEN_PATH = "/oauth2/tokenP"
DEFAULT_KIS_TOKEN_CACHE_PATH = Path("data/.kis_token_cache.json")

REQUIRED_KIS_ENV_VARS = ("KIS_ENV", "KIS_APP_KEY", "KIS_APP_SECRET", "KIS_BASE_URL")
FORBIDDEN_ENDPOINT_FRAGMENTS = (
    "/trading/",
    "order",
    "ord",
    "buy",
    "sell",
    "cancel",
    "modify",
    "rvsecncl",
    "cash",
    "account",
    "balance",
    "잔고",
    "주문",
    "매수",
    "매도",
    "정정",
    "취소",
    "체결",
)


class KisClientError(RuntimeError):
    """KIS client error with secret-safe diagnostics."""


class KisEndpointBlocked(KisClientError):
    """Raised when a non-quotation endpoint is requested."""


PRICE_FIELD_CANDIDATES = {
    "open": ("stck_oprc", "oprc", "open", "open_price"),
    "high": ("stck_hgpr", "hgpr", "high", "high_price"),
    "low": ("stck_lwpr", "lwpr", "low", "low_price"),
    "close": ("stck_prpr", "prpr", "close", "close_price", "last_price"),
}
TIME_FIELD_CANDIDATES = ("stck_cntg_hour", "cntg_hour", "hour", "time", "trading_time")
DATE_FIELD_CANDIDATES = ("stck_bsop_date", "bsop_date", "date", "trading_date")
PER_BAR_VOLUME_FIELDS = ("cntg_vol", "tr_vol", "volume", "trading_volume")
CUMULATIVE_VOLUME_FIELDS = ("acml_vol", "accumulated_volume")
PER_BAR_AMOUNT_FIELDS = ("tr_pbmn", "cntg_tr_pbmn", "amount", "traded_value", "trading_value")
CUMULATIVE_AMOUNT_FIELDS = ("acml_tr_pbmn", "acc_tr_pbmn", "accumulated_traded_value")


@dataclass(frozen=True)
class KisEnvConfig:
    kis_env: str
    app_key: str = field(repr=False)
    app_secret: str = field(repr=False)
    base_url: str

    @property
    def is_paper(self) -> bool:
        return self.kis_env.lower() == "paper"

    def safe_summary(self) -> dict[str, Any]:
        return {
            "kis_env": self.kis_env,
            "base_url": self.base_url,
            "app_key_present": bool(self.app_key),
            "app_secret_present": bool(self.app_secret),
        }


@dataclass(frozen=True)
class KisEnvLoadResult:
    status: str
    config: KisEnvConfig | None
    missing_env_vars: list[str]
    env_file_used: str | None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "missing_env_vars": list(self.missing_env_vars),
            "env_file_used": self.env_file_used,
            "reason": self.reason,
            "config": self.config.safe_summary() if self.config else None,
        }


@dataclass(frozen=True)
class RequiredIntradayRequest:
    date: str
    symbol: str
    timeframe: str
    source_type: str
    score_date: str | None = None
    rank: str | None = None
    score: str | None = None
    required_reason: str | None = None


@dataclass(frozen=True)
class KisMinuteParseDiagnostics:
    raw_volume_field_used: str | None
    raw_amount_field_used: str | None
    raw_price_fields_used: dict[str, str | None]
    cumulative_volume_diff_used: bool
    cumulative_amount_diff_used: bool
    estimated_traded_value_count: int
    negative_cumulative_diff_count: int
    zero_volume_positive_amount_count: int
    positive_volume_zero_amount_count: int
    invalid_mapping_sample_rows: list[dict[str, Any]]
    parsed_row_count: int
    skipped_row_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_volume_field_used": self.raw_volume_field_used,
            "raw_amount_field_used": self.raw_amount_field_used,
            "raw_price_fields_used": dict(self.raw_price_fields_used),
            "cumulative_volume_diff_used": self.cumulative_volume_diff_used,
            "cumulative_amount_diff_used": self.cumulative_amount_diff_used,
            "estimated_traded_value_count": self.estimated_traded_value_count,
            "negative_cumulative_diff_count": self.negative_cumulative_diff_count,
            "zero_volume_positive_amount_count": self.zero_volume_positive_amount_count,
            "positive_volume_zero_amount_count": self.positive_volume_zero_amount_count,
            "invalid_mapping_sample_rows": list(self.invalid_mapping_sample_rows),
            "parsed_row_count": self.parsed_row_count,
            "skipped_row_count": self.skipped_row_count,
        }


def parse_dotenv(path: str | Path) -> dict[str, str]:
    env_path = Path(path)
    if not env_path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            values[key] = value
    return values


def load_kis_env(env_file: str | Path = ".env", env_override: str | None = None) -> KisEnvLoadResult:
    file_values = parse_dotenv(env_file)
    merged = dict(file_values)
    for key in REQUIRED_KIS_ENV_VARS:
        if os.environ.get(key):
            merged[key] = os.environ[key]
    if env_override:
        merged["KIS_ENV"] = env_override

    missing = [key for key in REQUIRED_KIS_ENV_VARS if not merged.get(key)]
    if missing:
        return KisEnvLoadResult(
            status="blocked",
            config=None,
            missing_env_vars=missing,
            env_file_used=str(env_file) if Path(env_file).exists() else None,
            reason="KIS_ENV_MISSING",
        )

    kis_env = merged["KIS_ENV"].strip().lower()
    if kis_env not in {"paper", "real"}:
        return KisEnvLoadResult(
            status="blocked",
            config=None,
            missing_env_vars=[],
            env_file_used=str(env_file) if Path(env_file).exists() else None,
            reason="KIS_ENV_INVALID",
        )

    return KisEnvLoadResult(
        status="ok",
        config=KisEnvConfig(
            kis_env=kis_env,
            app_key=merged["KIS_APP_KEY"].strip(),
            app_secret=merged["KIS_APP_SECRET"].strip(),
            base_url=merged["KIS_BASE_URL"].strip().rstrip("/"),
        ),
        missing_env_vars=[],
        env_file_used=str(env_file) if Path(env_file).exists() else None,
    )


def assert_quotation_endpoint(path: str) -> None:
    lower_path = path.lower()
    if lower_path == KIS_TOKEN_PATH.lower():
        return
    if "/quotations/" not in lower_path:
        raise KisEndpointBlocked(f"KIS_QUOTATION_ONLY_HARD_BLOCK: non-quotation endpoint is forbidden ({path})")
    for fragment in FORBIDDEN_ENDPOINT_FRAGMENTS:
        if fragment in lower_path:
            raise KisEndpointBlocked(f"KIS_QUOTATION_ONLY_HARD_BLOCK: forbidden endpoint fragment ({fragment})")


def response_summary(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        summary: dict[str, Any] = {
            "rt_cd": payload.get("rt_cd"),
            "msg_cd": payload.get("msg_cd"),
            "msg1": payload.get("msg1"),
            "error_code": payload.get("error_code"),
            "error_description": payload.get("error_description"),
            "keys": sorted(str(k) for k in payload.keys()),
        }
        for key in ("output", "output1", "output2"):
            value = payload.get(key)
            if isinstance(value, list):
                summary[f"{key}_row_count"] = len(value)
            elif isinstance(value, dict):
                summary[f"{key}_keys"] = sorted(str(k) for k in value.keys())
            elif value is not None:
                summary[f"{key}_type"] = type(value).__name__
        return summary
    return {"payload_type": type(payload).__name__}


def raise_for_kis_response_error(payload: dict[str, Any]) -> None:
    rt_cd = payload.get("rt_cd")
    if rt_cd is not None and str(rt_cd) != "0":
        raise KisClientError(json.dumps({"reason": "KIS_RESPONSE_NOT_OK", "response": response_summary(payload)}, ensure_ascii=False))


def _decode_http_error(exc: urllib.error.HTTPError) -> dict[str, Any]:
    body = exc.read()
    out: dict[str, Any] = {"http_status": exc.code, "reason": exc.reason}
    try:
        payload = json.loads(body.decode("utf-8"))
        out["response"] = response_summary(payload)
    except Exception:
        out["non_json_body_length"] = len(body)
    return out


def _parse_expires_at(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


class KisQuotationClient:
    def __init__(
        self,
        config: KisEnvConfig,
        *,
        timeout_seconds: float = 15.0,
        sleep_seconds: float = 0.05,
        dry_run: bool = False,
        token_cache_path: str | Path | None = None,
    ) -> None:
        self.config = config
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds
        self.dry_run = dry_run
        self.token_cache_path = Path(token_cache_path) if token_cache_path is not None else DEFAULT_KIS_TOKEN_CACHE_PATH
        self._access_token: str | None = None
        self._token_expires_at: datetime | None = None

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return f"{self.config.base_url}{path}"

    def _read_cached_token(self) -> str | None:
        if not self.token_cache_path or not self.token_cache_path.exists():
            return None
        try:
            payload = json.loads(self.token_cache_path.read_text(encoding="utf-8"))
            token = payload.get("access_token")
            expires_at = _parse_expires_at(payload.get("expires_at"))
            cache_matches_env = (
                payload.get("kis_env") == self.config.kis_env
                and payload.get("base_url") == self.config.base_url
                and int(payload.get("app_key_length") or -1) == len(self.config.app_key)
            )
            if token and expires_at and cache_matches_env and expires_at > datetime.now(timezone.utc) + timedelta(minutes=3):
                self._access_token = token
                self._token_expires_at = expires_at
                return token
        except Exception:
            return None
        return None

    def _write_cached_token(self, token: str, expires_at: datetime) -> None:
        if not self.token_cache_path:
            return
        self.token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.token_cache_path.write_text(
            json.dumps(
                {
                    "access_token": token,
                    "expires_at": expires_at.isoformat(),
                    "kis_env": self.config.kis_env,
                    "base_url": self.config.base_url,
                    "app_key_length": len(self.config.app_key),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def token_cache_summary(self) -> dict[str, Any]:
        path = self.token_cache_path
        if not path:
            return {"path": None, "exists": False, "usable": False, "reason": "TOKEN_CACHE_DISABLED"}
        summary: dict[str, Any] = {"path": str(path), "exists": path.exists(), "usable": False}
        if not path.exists():
            summary["reason"] = "TOKEN_CACHE_MISSING"
            return summary
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            summary["reason"] = "TOKEN_CACHE_UNREADABLE"
            return summary
        expires_at = _parse_expires_at(payload.get("expires_at"))
        summary["expires_at"] = expires_at.isoformat() if expires_at else None
        summary["token_present"] = bool(payload.get("access_token"))
        summary["kis_env_matches"] = payload.get("kis_env") == self.config.kis_env
        summary["base_url_matches"] = payload.get("base_url") == self.config.base_url
        summary["app_key_length_matches"] = int(payload.get("app_key_length") or -1) == len(self.config.app_key)
        if not payload.get("access_token"):
            summary["reason"] = "TOKEN_CACHE_TOKEN_MISSING"
        elif not expires_at:
            summary["reason"] = "TOKEN_CACHE_EXPIRES_AT_INVALID"
        elif not (summary["kis_env_matches"] and summary["base_url_matches"] and summary["app_key_length_matches"]):
            summary["reason"] = "TOKEN_CACHE_ENV_MISMATCH"
        elif expires_at <= datetime.now(timezone.utc) + timedelta(minutes=3):
            summary["reason"] = "TOKEN_CACHE_EXPIRED_OR_NEAR_EXPIRY"
        else:
            summary["usable"] = True
            summary["reason"] = "TOKEN_CACHE_USABLE"
        return summary

    def request_json(
        self,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        require_quotation: bool = True,
    ) -> dict[str, Any]:
        if require_quotation:
            assert_quotation_endpoint(path)
        if self.dry_run:
            return {
                "dry_run": True,
                "method": method.upper(),
                "path": path,
                "params": dict(params or {}),
                "body_keys": sorted((body or {}).keys()),
            }

        url = self._url(path)
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        payload = None
        request_headers = {"Content-Type": "application/json; charset=utf-8"}
        request_headers.update(headers or {})
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers=request_headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            raise KisClientError(json.dumps(_decode_http_error(exc), ensure_ascii=False)) from exc
        except urllib.error.URLError as exc:
            raise KisClientError(f"KIS_NETWORK_ERROR: {exc.reason}") from exc

        if self.sleep_seconds > 0:
            time.sleep(self.sleep_seconds)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise KisClientError(f"KIS_NON_JSON_RESPONSE: bytes={len(raw)}") from exc
        if not isinstance(parsed, dict):
            raise KisClientError(f"KIS_UNEXPECTED_RESPONSE_TYPE: {type(parsed).__name__}")
        return parsed

    def get_access_token(self, *, force_refresh: bool = False) -> str:
        if self.dry_run:
            return "DRY_RUN_TOKEN"
        if self._access_token and self._token_expires_at and self._token_expires_at > datetime.now(timezone.utc) + timedelta(minutes=3):
            return self._access_token
        if not force_refresh:
            cached = self._read_cached_token()
            if cached:
                return cached
        payload = self.request_json(
            "POST",
            KIS_TOKEN_PATH,
            body={
                "grant_type": "client_credentials",
                "appkey": self.config.app_key,
                "appsecret": self.config.app_secret,
            },
            require_quotation=False,
        )
        token = payload.get("access_token")
        if not token:
            raise KisClientError(json.dumps({"reason": "KIS_TOKEN_MISSING", "response": response_summary(payload)}, ensure_ascii=False))
        expires_in = int(payload.get("expires_in") or 3600)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=max(300, expires_in))
        self._access_token = str(token)
        self._token_expires_at = expires_at
        self._write_cached_token(str(token), expires_at)
        return str(token)

    def quotation_headers(self, tr_id: str) -> dict[str, str]:
        return {
            "authorization": f"Bearer {self.get_access_token()}",
            "appkey": self.config.app_key,
            "appsecret": self.config.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    def inquire_stock_minute(self, symbol: str, *, input_hour: str = "153000") -> dict[str, Any]:
        return self.request_json(
            "GET",
            KIS_STOCK_MINUTE_PATH,
            headers=self.quotation_headers(KIS_STOCK_MINUTE_TR_ID),
            params={
                "FID_ETC_CLS_CODE": "",
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol.zfill(6),
                "FID_INPUT_HOUR_1": input_hour,
                "FID_PW_DATA_INCU_YN": "Y",
            },
        )

    def inquire_index_minute(self, index_code: str = "0001", *, input_hour: str = "153000") -> dict[str, Any]:
        return self.request_json(
            "GET",
            KIS_INDEX_MINUTE_PATH,
            headers=self.quotation_headers(KIS_INDEX_MINUTE_TR_ID),
            params={
                "FID_COND_MRKT_DIV_CODE": "U",
                "FID_INPUT_ISCD": index_code,
                "FID_INPUT_HOUR_1": input_hour,
                "FID_PW_DATA_INCU_YN": "Y",
            },
        )


def _rows_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("output2", "output", "output1"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    rows: list[dict[str, Any]] = []
    for value in payload.values():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            rows.extend(value)
    return rows


def kis_rows_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return _rows_from_payload(payload)


def _first_value(row: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _first_key_and_value(row: dict[str, Any], keys: tuple[str, ...]) -> tuple[str | None, Any]:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return key, value
    return None, None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def _safe_sample_value(value: Any) -> Any:
    if isinstance(value, (int, float)) or value is None:
        return value
    return str(value)[:80]


def _safe_row_sample(row: dict[str, Any], *, max_fields: int = 30) -> dict[str, Any]:
    allowed_keys = sorted(row.keys())[:max_fields]
    return {key: _safe_sample_value(row.get(key)) for key in allowed_keys}


def audit_kis_intraday_rows(rows: list[dict[str, Any]], *, max_rows: int = 10) -> dict[str, Any]:
    field_names = sorted({str(key) for row in rows for key in row.keys()})
    non_null_count: dict[str, int] = {}
    numeric_parseable: dict[str, bool] = {}
    for field_name in field_names:
        values = [row.get(field_name) for row in rows if row.get(field_name) not in (None, "")]
        non_null_count[field_name] = len(values)
        numeric_parseable[field_name] = bool(values) and all(_to_float(value) is not None for value in values)
    lower_names = {name.lower(): name for name in field_names}
    volume_like = [name for name in field_names if any(token in name.lower() for token in ("vol", "qty", "cntg"))]
    amount_like = [name for name in field_names if any(token in name.lower() for token in ("pbmn", "amount", "value", "tr_"))]
    price_like = [name for name in field_names if any(token in name.lower() for token in ("prpr", "oprc", "hgpr", "lwpr", "price", "open", "high", "low", "close"))]
    time_like = [name for name in field_names if any(token in name.lower() for token in ("date", "hour", "time"))]
    return {
        "row_count": len(rows),
        "field_names": field_names,
        "field_non_null_count": non_null_count,
        "field_numeric_parseable": numeric_parseable,
        "volume_candidate_fields": {
            "known": [field for field in PER_BAR_VOLUME_FIELDS + CUMULATIVE_VOLUME_FIELDS if field in lower_names or field in field_names],
            "volume_like": volume_like,
        },
        "amount_candidate_fields": {
            "known": [field for field in PER_BAR_AMOUNT_FIELDS + CUMULATIVE_AMOUNT_FIELDS if field in lower_names or field in field_names],
            "amount_like": amount_like,
        },
        "price_candidate_fields": {
            "known": [field for fields in PRICE_FIELD_CANDIDATES.values() for field in fields if field in lower_names or field in field_names],
            "price_like": price_like,
        },
        "timestamp_candidate_fields": {
            "known": [field for field in DATE_FIELD_CANDIDATES + TIME_FIELD_CANDIDATES if field in lower_names or field in field_names],
            "time_like": time_like,
        },
        "sample_rows": [_safe_row_sample(row) for row in rows[:max_rows]],
    }


def _parse_date_time(row: dict[str, Any]) -> datetime | None:
    raw_date = _first_value(row, DATE_FIELD_CANDIDATES)
    raw_time = _first_value(row, TIME_FIELD_CANDIDATES)
    if raw_date is None or raw_time is None:
        return None
    date_value = str(raw_date).replace("-", "").strip()
    time_value = str(raw_time).replace(":", "").strip().zfill(6)
    if len(date_value) != 8 or len(time_value) < 4:
        return None
    try:
        return datetime.strptime(f"{date_value}{time_value[:6]}", "%Y%m%d%H%M%S")
    except ValueError:
        return None


def parse_kis_minute_rows_with_diagnostics(
    rows: list[dict[str, Any]],
    *,
    symbol: str,
    source: str = "KIS_QUOTATION",
    raw_timeframe: str = "1m",
    estimate_missing_traded_value: bool = True,
) -> tuple[list[IntradayBar], KisMinuteParseDiagnostics]:
    parsed_rows: list[dict[str, Any]] = []
    skipped = 0
    price_fields_seen: dict[str, str | None] = {"open": None, "high": None, "low": None, "close": None}
    raw_volume_field_used: str | None = None
    raw_amount_field_used: str | None = None
    for row in rows:
        ts = _parse_date_time(row)
        if ts is None:
            skipped += 1
            continue
        price_values: dict[str, float] = {}
        price_field_names: dict[str, str | None] = {}
        for price_key, candidates in PRICE_FIELD_CANDIDATES.items():
            field_name, raw_value = _first_key_and_value(row, candidates)
            price_field_names[price_key] = field_name
            parsed = _to_float(raw_value)
            if parsed is None:
                skipped += 1
                break
            price_values[price_key] = parsed
        if len(price_values) != 4:
            continue
        open_px = price_values["open"]
        high_px = price_values["high"]
        low_px = price_values["low"]
        close_px = price_values["close"]
        if min(open_px, high_px, low_px, close_px) <= 0 or high_px < max(open_px, close_px) or low_px > min(open_px, close_px):
            skipped += 1
            continue
        for key, field_name in price_field_names.items():
            if price_fields_seen[key] is None and field_name:
                price_fields_seen[key] = field_name

        per_volume_field, per_volume_raw = _first_key_and_value(row, PER_BAR_VOLUME_FIELDS)
        cumulative_volume_field, cumulative_volume_raw = _first_key_and_value(row, CUMULATIVE_VOLUME_FIELDS)
        per_amount_field, per_amount_raw = _first_key_and_value(row, PER_BAR_AMOUNT_FIELDS)
        cumulative_amount_field, cumulative_amount_raw = _first_key_and_value(row, CUMULATIVE_AMOUNT_FIELDS)
        per_volume = _to_float(per_volume_raw)
        cumulative_volume = _to_float(cumulative_volume_raw)
        per_amount = _to_float(per_amount_raw)
        cumulative_amount = _to_float(cumulative_amount_raw)
        if raw_volume_field_used is None:
            raw_volume_field_used = per_volume_field or cumulative_volume_field
        if raw_amount_field_used is None:
            raw_amount_field_used = per_amount_field or cumulative_amount_field
        parsed_rows.append(
            {
                "timestamp": ts,
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "per_volume": per_volume,
                "per_volume_field": per_volume_field,
                "cumulative_volume": cumulative_volume,
                "cumulative_volume_field": cumulative_volume_field,
                "per_amount": per_amount,
                "per_amount_field": per_amount_field,
                "cumulative_amount": cumulative_amount,
                "cumulative_amount_field": cumulative_amount_field,
                "sample": _safe_row_sample(row),
            }
        )
    deduped = {row["timestamp"]: row for row in parsed_rows}
    ordered = sorted(deduped.values(), key=lambda row: row["timestamp"])

    bars: list[IntradayBar] = []
    previous_cumulative_volume: float | None = None
    previous_cumulative_amount: float | None = None
    cumulative_volume_diff_used = False
    cumulative_amount_diff_used = False
    estimated_traded_value_count = 0
    negative_cumulative_diff_count = 0
    zero_volume_positive_amount_count = 0
    positive_volume_zero_amount_count = 0
    invalid_mapping_samples: list[dict[str, Any]] = []

    def add_mapping_sample(row: dict[str, Any], reason: str, volume: float, amount: float | None) -> None:
        if len(invalid_mapping_samples) >= 10:
            return
        invalid_mapping_samples.append(
            {
                "timestamp": row["timestamp"].isoformat(),
                "reason": reason,
                "volume": volume,
                "amount": amount,
                "per_volume_field": row.get("per_volume_field"),
                "cumulative_volume_field": row.get("cumulative_volume_field"),
                "per_amount_field": row.get("per_amount_field"),
                "cumulative_amount_field": row.get("cumulative_amount_field"),
                "raw_sample": row.get("sample"),
            }
        )

    cumulative_volume_diff_allowed = True
    cumulative_amount_diff_allowed = True
    check_previous_cumulative_volume: float | None = None
    check_previous_cumulative_amount: float | None = None
    for row in ordered:
        cumulative_volume = row.get("cumulative_volume")
        if cumulative_volume is not None:
            if check_previous_cumulative_volume is not None and float(cumulative_volume) - float(check_previous_cumulative_volume) < 0.0:
                cumulative_volume_diff_allowed = False
                negative_cumulative_diff_count += 1
                add_mapping_sample(row, "negative_cumulative_volume_diff", float(row.get("per_volume") or 0.0), None)
            check_previous_cumulative_volume = float(cumulative_volume)
        cumulative_amount = row.get("cumulative_amount")
        if cumulative_amount is not None:
            if check_previous_cumulative_amount is not None and float(cumulative_amount) - float(check_previous_cumulative_amount) < 0.0:
                cumulative_amount_diff_allowed = False
                negative_cumulative_diff_count += 1
                add_mapping_sample(row, "negative_cumulative_amount_diff", float(row.get("per_volume") or 0.0), row.get("per_amount"))
            check_previous_cumulative_amount = float(cumulative_amount)

    for row in ordered:
        volume = row.get("per_volume")
        volume_source = row.get("per_volume_field")
        cumulative_volume = row.get("cumulative_volume")
        cumulative_volume_diff: float | None = None
        if cumulative_volume_diff_allowed and cumulative_volume is not None and previous_cumulative_volume is not None:
            cumulative_volume_diff = float(cumulative_volume) - float(previous_cumulative_volume)
            if cumulative_volume_diff < 0:
                add_mapping_sample(row, "negative_cumulative_volume_diff", float(volume or 0.0), None)
                cumulative_volume_diff = None
        if cumulative_volume_diff is not None and (volume is None or float(volume) <= 0.0):
            volume = cumulative_volume_diff
            volume_source = f"{row.get('cumulative_volume_field')}_diff"
            cumulative_volume_diff_used = True
        if volume is None:
            volume = 0.0
        if float(volume) < 0.0:
            skipped += 1
            continue

        amount = row.get("per_amount")
        amount_source = row.get("per_amount_field")
        cumulative_amount = row.get("cumulative_amount")
        cumulative_amount_diff: float | None = None
        if cumulative_amount_diff_allowed and cumulative_amount is not None and previous_cumulative_amount is not None:
            cumulative_amount_diff = float(cumulative_amount) - float(previous_cumulative_amount)
            if cumulative_amount_diff < 0:
                add_mapping_sample(row, "negative_cumulative_amount_diff", float(volume), amount)
                cumulative_amount_diff = None
        if amount is None and cumulative_amount_diff is not None:
            amount = cumulative_amount_diff
            amount_source = f"{row.get('cumulative_amount_field')}_diff"
            cumulative_amount_diff_used = True
        estimated = False
        if amount is None and estimate_missing_traded_value and float(volume) > 0.0:
            amount = float(row["close"]) * float(volume)
            amount_source = "estimated_close_x_volume"
            estimated = True
            estimated_traded_value_count += 1
        if amount is not None and float(amount) < 0.0:
            skipped += 1
            continue
        if float(volume) == 0.0 and amount is not None and float(amount) > 0.0:
            zero_volume_positive_amount_count += 1
            add_mapping_sample(row, "zero_volume_positive_amount", float(volume), float(amount))
        if float(volume) > 0.0 and (amount is None or float(amount) == 0.0):
            positive_volume_zero_amount_count += 1
            add_mapping_sample(row, "positive_volume_zero_amount", float(volume), 0.0 if amount is None else float(amount))

        source_tags = [source, f"VOL_FIELD={volume_source or 'NONE'}", f"AMOUNT_FIELD={amount_source or 'NONE'}"]
        if cumulative_volume_diff_used and volume_source and volume_source.endswith("_diff"):
            source_tags.append("CUMULATIVE_VOLUME_DIFF")
        if cumulative_amount_diff_used and amount_source and amount_source.endswith("_diff"):
            source_tags.append("CUMULATIVE_AMOUNT_DIFF")
        if estimated:
            source_tags.append("ESTIMATED_TRADE_VALUE")
        bars.append(
            IntradayBar(
                symbol=symbol,
                timestamp=row["timestamp"],
                timeframe=raw_timeframe,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(volume),
                amount=amount,
                source="|".join(source_tags),
            )
        )
        if cumulative_volume is not None:
            previous_cumulative_volume = float(cumulative_volume)
        if cumulative_amount is not None:
            previous_cumulative_amount = float(cumulative_amount)
    diagnostics = KisMinuteParseDiagnostics(
        raw_volume_field_used=raw_volume_field_used,
        raw_amount_field_used=raw_amount_field_used,
        raw_price_fields_used=price_fields_seen,
        cumulative_volume_diff_used=cumulative_volume_diff_used,
        cumulative_amount_diff_used=cumulative_amount_diff_used,
        estimated_traded_value_count=estimated_traded_value_count,
        negative_cumulative_diff_count=negative_cumulative_diff_count,
        zero_volume_positive_amount_count=zero_volume_positive_amount_count,
        positive_volume_zero_amount_count=positive_volume_zero_amount_count,
        invalid_mapping_sample_rows=invalid_mapping_samples,
        parsed_row_count=len(bars),
        skipped_row_count=skipped,
    )
    return sorted(bars, key=lambda b: (b.timestamp, b.symbol)), diagnostics


def parse_kis_minute_payload_with_diagnostics(
    payload: dict[str, Any],
    *,
    symbol: str,
    source: str = "KIS_QUOTATION",
    raw_timeframe: str = "1m",
) -> tuple[list[IntradayBar], KisMinuteParseDiagnostics]:
    return parse_kis_minute_rows_with_diagnostics(
        _rows_from_payload(payload),
        symbol=symbol,
        source=source,
        raw_timeframe=raw_timeframe,
    )


def parse_kis_minute_payload(
    payload: dict[str, Any],
    *,
    symbol: str,
    source: str = "KIS_QUOTATION",
    raw_timeframe: str = "1m",
) -> list[IntradayBar]:
    bars, _diagnostics = parse_kis_minute_payload_with_diagnostics(
        payload,
        symbol=symbol,
        source=source,
        raw_timeframe=raw_timeframe,
    )
    return sorted(bars, key=lambda b: (b.timestamp, b.symbol))


def _floor_to_minutes(ts: datetime, minutes: int) -> datetime:
    return ts.replace(minute=(ts.minute // minutes) * minutes, second=0, microsecond=0)


def aggregate_bars(bars: list[IntradayBar], timeframe: str = "5m") -> list[IntradayBar]:
    if timeframe.lower() in {"1m", "1min"}:
        return [IntradayBar(**{**bar.__dict__, "timeframe": "1m"}) for bar in sorted(bars, key=lambda b: (b.symbol, b.timestamp))]
    value = timeframe.lower().strip()
    if not value.endswith("m") or not value[:-1].isdigit():
        raise ValueError(f"unsupported timeframe: {timeframe}")
    minutes = int(value[:-1])
    if minutes <= 0:
        raise ValueError(f"unsupported timeframe: {timeframe}")

    grouped: dict[tuple[str, datetime], list[IntradayBar]] = {}
    for bar in bars:
        bucket = _floor_to_minutes(bar.timestamp, minutes)
        grouped.setdefault((bar.symbol, bucket), []).append(bar)

    out: list[IntradayBar] = []
    for (symbol, bucket), group in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        ordered = sorted(group, key=lambda b: b.timestamp)
        volume = sum(float(b.volume) for b in ordered)
        amount_values = [b.amount for b in ordered if b.amount is not None]
        amount = sum(float(v) for v in amount_values) if amount_values else ordered[-1].close * volume
        source_parts: set[str] = set()
        for bar in ordered:
            if bar.source:
                source_parts.update(part for part in str(bar.source).split("|") if part)
        if len(amount_values) != len(ordered):
            source_parts.add("ESTIMATED_TRADE_VALUE")
        out.append(
            IntradayBar(
                symbol=symbol,
                timestamp=bucket,
                timeframe=timeframe,
                open=ordered[0].open,
                high=max(b.high for b in ordered),
                low=min(b.low for b in ordered),
                close=ordered[-1].close,
                volume=volume,
                amount=amount,
                source="|".join(sorted(source_parts)) if source_parts else ordered[-1].source,
            )
        )
    return out


def parse_required_intraday_csv(
    path: str | Path,
    *,
    timeframe: str = "5m",
    start_date: str | None = None,
    end_date: str | None = None,
    max_symbols: int | None = None,
) -> list[RequiredIntradayRequest]:
    requests: list[RequiredIntradayRequest] = []
    with Path(path).open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"date", "symbol", "source_type"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"required intraday CSV missing columns: {sorted(missing)}")
        for row in reader:
            date = (row.get("date") or "").strip()
            row_timeframe = (row.get("timeframe") or timeframe).strip()
            if row_timeframe != timeframe:
                continue
            if start_date and date < start_date:
                continue
            if end_date and date > end_date:
                continue
            requests.append(
                RequiredIntradayRequest(
                    date=date,
                    symbol=(row.get("symbol") or "").strip(),
                    timeframe=row_timeframe,
                    source_type=(row.get("source_type") or "").strip().upper(),
                    score_date=(row.get("score_date") or "").strip() or None,
                    rank=(row.get("rank") or "").strip() or None,
                    score=(row.get("score") or "").strip() or None,
                    required_reason=(row.get("required_reason") or "").strip() or None,
                )
            )
    if max_symbols is None:
        return requests
    selected_candidates: set[str] = set()
    limited: list[RequiredIntradayRequest] = []
    for req in requests:
        if req.source_type == "CANDIDATE":
            if req.symbol not in selected_candidates and len(selected_candidates) >= max_symbols:
                continue
            selected_candidates.add(req.symbol)
        limited.append(req)
    return limited


def write_intraday_bars_csv(path: str | Path, bars: list[IntradayBar]) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["symbol", "timestamp", "open", "high", "low", "close", "volume", "timeframe", "traded_value", "source"],
        )
        writer.writeheader()
        for bar in sorted(bars, key=lambda b: (b.timestamp, b.symbol, b.timeframe)):
            writer.writerow(
                {
                    "symbol": bar.symbol,
                    "timestamp": bar.timestamp.replace(microsecond=0).isoformat(),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "timeframe": bar.timeframe,
                    "traded_value": bar.amount if bar.amount is not None else bar.trade_value,
                    "source": bar.source or "KIS_QUOTATION",
                }
            )
    return out
