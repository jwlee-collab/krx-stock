from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

from pipeline.day_trading.kis_client import (
    DEFAULT_KIS_TOKEN_CACHE_PATH,
    KisClientError,
    KisEnvConfig,
    KisQuotationClient,
    aggregate_bars,
    audit_kis_intraday_rows,
    assert_quotation_endpoint,
    load_kis_env,
    parse_kis_minute_payload,
    parse_kis_minute_rows_with_diagnostics,
    parse_required_intraday_csv,
    raise_for_kis_response_error,
    write_intraday_bars_csv,
)
from pipeline.day_trading.data_loader import load_intraday_prices_csv
from pipeline.day_trading.models import IntradayBar
from pipeline.db import get_connection, init_db
from scripts.check_kis_env import build_kis_env_diagnostics


class FakeTokenClient(KisQuotationClient):
    def __init__(self, config: KisEnvConfig, **kwargs) -> None:
        super().__init__(config, **kwargs)
        self.request_count = 0
        self.raise_error: Exception | None = None

    def request_json(self, *args, **kwargs):
        self.request_count += 1
        if self.raise_error:
            raise self.raise_error
        return {"access_token": "FAKE_TOKEN_SHOULD_NOT_BE_PRINTED", "expires_in": 3600}


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in ("KIS_ENV", "KIS_APP_KEY", "KIS_APP_SECRET", "KIS_BASE_URL"):
        env.pop(key, None)
    return env


def _run(args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


class KisQuoteCollectorTests(unittest.TestCase):
    def test_env_missing_is_blocked_without_secret_values(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_result = load_kis_env(Path(td) / "missing.env")
        self.assertEqual(env_result.status, "blocked")
        self.assertEqual(env_result.reason, "KIS_ENV_MISSING")
        self.assertIn("KIS_APP_KEY", env_result.missing_env_vars)

    def test_check_kis_env_detects_base_url_mismatch_and_masks_secret_values(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_path = Path(td) / ".env"
            env_path.write_text(
                "KIS_ENV=paper\n"
                "KIS_APP_KEY=FAKE_APP_KEY_SHOULD_NOT_LEAK\n"
                "KIS_APP_SECRET=FAKE_SECRET_SHOULD_NOT_LEAK\n"
                "KIS_BASE_URL=https://openapi.koreainvestment.com:9443\n",
                encoding="utf-8",
            )
            diagnostics = build_kis_env_diagnostics(env_path)
        serialized = json.dumps(diagnostics, ensure_ascii=False)
        self.assertEqual(diagnostics["status"], "blocked")
        self.assertIn("KIS_ENV_PAPER_BASE_URL_POINTS_TO_REAL", diagnostics["issues"])
        self.assertNotIn("FAKE_APP_KEY_SHOULD_NOT_LEAK", serialized)
        self.assertNotIn("FAKE_SECRET_SHOULD_NOT_LEAK", serialized)
        self.assertEqual(diagnostics["app_key_length"], len("FAKE_APP_KEY_SHOULD_NOT_LEAK"))

    def test_valid_token_cache_skips_tokenp_call(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache_path = Path(td) / ".kis_token_cache.json"
            config = KisEnvConfig(
                kis_env="paper",
                app_key="FAKE_APP_KEY_SHOULD_NOT_LEAK",
                app_secret="FAKE_SECRET_SHOULD_NOT_LEAK",
                base_url="https://openapivts.koreainvestment.com:29443",
            )
            cache_path.write_text(
                json.dumps(
                    {
                        "access_token": "CACHED_FAKE_TOKEN_SHOULD_NOT_PRINT",
                        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
                        "kis_env": "paper",
                        "base_url": config.base_url,
                        "app_key_length": len(config.app_key),
                    }
                ),
                encoding="utf-8",
            )
            client = FakeTokenClient(config, token_cache_path=cache_path)
            token = client.get_access_token()
        self.assertEqual(token, "CACHED_FAKE_TOKEN_SHOULD_NOT_PRINT")
        self.assertEqual(client.request_count, 0)

    def test_force_refresh_token_calls_tokenp_once(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache_path = Path(td) / ".kis_token_cache.json"
            config = KisEnvConfig("paper", "FAKE_APP_KEY_SHOULD_NOT_LEAK", "FAKE_SECRET_SHOULD_NOT_LEAK", "https://openapivts.koreainvestment.com:29443")
            client = FakeTokenClient(config, token_cache_path=cache_path)
            token = client.get_access_token(force_refresh=True)
            cache_exists = cache_path.exists()
        self.assertEqual(token, "FAKE_TOKEN_SHOULD_NOT_BE_PRINTED")
        self.assertEqual(client.request_count, 1)
        self.assertTrue(cache_exists)

    def test_tokenp_403_is_not_retried_and_secret_safe(self) -> None:
        config = KisEnvConfig("real", "FAKE_APP_KEY_SHOULD_NOT_LEAK", "FAKE_SECRET_SHOULD_NOT_LEAK", "https://openapi.koreainvestment.com:9443")
        client = FakeTokenClient(config, token_cache_path=Path("/private/tmp/nonexistent-token-cache.json"))
        client.raise_error = KisClientError('{"http_status": 403, "reason": "Forbidden"}')
        with self.assertRaises(KisClientError) as ctx:
            client.get_access_token(force_refresh=True)
        self.assertEqual(client.request_count, 1)
        message = str(ctx.exception)
        self.assertNotIn(config.app_key, message)
        self.assertNotIn(config.app_secret, message)

    def test_probe_dry_run_does_not_print_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_path = Path(td) / ".env"
            env_path.write_text(
                "KIS_ENV=paper\n"
                "KIS_APP_KEY=FAKE_APP_KEY_SHOULD_NOT_LEAK\n"
                "KIS_APP_SECRET=FAKE_SECRET_SHOULD_NOT_LEAK\n"
                "KIS_BASE_URL=https://openapivts.koreainvestment.com:29443\n",
                encoding="utf-8",
            )
            proc = _run(["scripts/probe_kis_intraday.py", "--env-file", str(env_path), "--dry-run"], env=_clean_env())
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        combined = proc.stdout + proc.stderr
        self.assertNotIn("FAKE_APP_KEY_SHOULD_NOT_LEAK", combined)
        self.assertNotIn("FAKE_SECRET_SHOULD_NOT_LEAK", combined)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["env"]["app_key_present"])
        self.assertTrue(payload["env"]["app_secret_present"])

    def test_order_and_account_paths_are_hard_blocked(self) -> None:
        with self.assertRaises(Exception):
            assert_quotation_endpoint("/uapi/domestic-stock/v1/trading/order-cash")
        with self.assertRaises(Exception):
            assert_quotation_endpoint("/uapi/domestic-stock/v1/trading/inquire-balance")

    def test_kis_nonzero_response_is_treated_as_blocked(self) -> None:
        with self.assertRaises(KisClientError):
            raise_for_kis_response_error({"rt_cd": "1", "msg_cd": "OPSQ0002", "msg1": "없는 서비스 코드 입니다"})

    def test_required_intraday_csv_parsing_keeps_market_proxy_with_max_symbols(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            csv_path = Path(td) / "required.csv"
            csv_path.write_text(
                "date,symbol,timeframe,source_type,score_date,rank,score,required_reason\n"
                "2026-05-08,005930,5m,CANDIDATE,2026-05-07,1,0.91,TOP_N_SWING_CANDIDATE\n"
                "2026-05-08,000660,5m,CANDIDATE,2026-05-07,2,0.88,TOP_N_SWING_CANDIDATE\n"
                "2026-05-08,999999,5m,MARKET_PROXY,,,,MARKET_CONTEXT\n",
                encoding="utf-8",
            )
            requests = parse_required_intraday_csv(csv_path, max_symbols=1)
        self.assertEqual([r.symbol for r in requests], ["005930", "999999"])
        self.assertEqual(requests[-1].source_type, "MARKET_PROXY")

    def test_mock_kis_minute_response_converts_to_loader_csv(self) -> None:
        payload = {
            "rt_cd": "0",
            "output2": [
                {
                    "stck_bsop_date": "20260508",
                    "stck_cntg_hour": "090100",
                    "stck_oprc": "100",
                    "stck_hgpr": "102",
                    "stck_lwpr": "99",
                    "stck_prpr": "101",
                    "cntg_vol": "1000",
                },
                {
                    "stck_bsop_date": "20260508",
                    "stck_cntg_hour": "090200",
                    "stck_oprc": "101",
                    "stck_hgpr": "103",
                    "stck_lwpr": "100",
                    "stck_prpr": "102",
                    "cntg_vol": "1200",
                },
                {
                    "stck_bsop_date": "20260508",
                    "stck_cntg_hour": "090500",
                    "stck_oprc": "103",
                    "stck_hgpr": "104",
                    "stck_lwpr": "102",
                    "stck_prpr": "103",
                    "cntg_vol": "900",
                },
            ],
        }
        raw = parse_kis_minute_payload(payload, symbol="005930")
        bars = aggregate_bars(raw, "5m")
        self.assertEqual(len(raw), 3)
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[0].timestamp.isoformat(), "2026-05-08T09:00:00")
        self.assertEqual(bars[0].open, 100.0)
        self.assertEqual(bars[0].high, 103.0)
        self.assertEqual(bars[0].low, 99.0)
        self.assertEqual(bars[0].close, 102.0)
        self.assertEqual(bars[0].volume, 2200.0)

        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "intraday.csv"
            write_intraday_bars_csv(out, bars)
            with out.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        self.assertEqual(rows[0]["symbol"], "005930")
        self.assertEqual(rows[0]["timeframe"], "5m")
        self.assertIn("traded_value", rows[0])

    def test_cntg_vol_positive_is_used_as_volume(self) -> None:
        rows = [
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090100",
                "stck_oprc": "100",
                "stck_hgpr": "101",
                "stck_lwpr": "99",
                "stck_prpr": "100",
                "cntg_vol": "123",
                "tr_pbmn": "12300",
            }
        ]
        bars, diagnostics = parse_kis_minute_rows_with_diagnostics(rows, symbol="005930")
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].volume, 123.0)
        self.assertEqual(bars[0].amount, 12300.0)
        self.assertEqual(diagnostics.raw_volume_field_used, "cntg_vol")
        self.assertEqual(diagnostics.raw_amount_field_used, "tr_pbmn")

    def test_acml_vol_only_uses_cumulative_diff_for_volume(self) -> None:
        rows = [
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090100",
                "stck_oprc": "100",
                "stck_hgpr": "101",
                "stck_lwpr": "99",
                "stck_prpr": "100",
                "acml_vol": "1000",
            },
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090200",
                "stck_oprc": "100",
                "stck_hgpr": "102",
                "stck_lwpr": "99",
                "stck_prpr": "101",
                "acml_vol": "1125",
            },
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090300",
                "stck_oprc": "101",
                "stck_hgpr": "103",
                "stck_lwpr": "100",
                "stck_prpr": "102",
                "acml_vol": "1250",
            },
        ]
        raw, diagnostics = parse_kis_minute_rows_with_diagnostics(rows, symbol="005930")
        self.assertTrue(diagnostics.cumulative_volume_diff_used)
        self.assertEqual([bar.volume for bar in raw], [0.0, 125.0, 125.0])
        five = aggregate_bars(raw, "5m")
        self.assertEqual(len(five), 1)
        self.assertEqual(five[0].volume, 250.0)

    def test_acml_amount_only_uses_cumulative_diff_for_traded_value(self) -> None:
        rows = [
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090100",
                "stck_oprc": "100",
                "stck_hgpr": "101",
                "stck_lwpr": "99",
                "stck_prpr": "100",
                "cntg_vol": "0",
                "acml_vol": "1000",
                "acml_tr_pbmn": "100000",
            },
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090200",
                "stck_oprc": "100",
                "stck_hgpr": "102",
                "stck_lwpr": "99",
                "stck_prpr": "101",
                "cntg_vol": "0",
                "acml_vol": "1100",
                "acml_tr_pbmn": "110500",
            },
        ]
        raw, diagnostics = parse_kis_minute_rows_with_diagnostics(rows, symbol="005930")
        self.assertTrue(diagnostics.cumulative_volume_diff_used)
        self.assertTrue(diagnostics.cumulative_amount_diff_used)
        self.assertEqual(raw[1].volume, 100.0)
        self.assertEqual(raw[1].amount, 10500.0)

    def test_zero_volume_positive_amount_is_flagged_as_invalid_mapping(self) -> None:
        rows = [
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090100",
                "stck_oprc": "100",
                "stck_hgpr": "101",
                "stck_lwpr": "99",
                "stck_prpr": "100",
                "cntg_vol": "0",
                "acml_tr_pbmn": "100000",
            },
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090200",
                "stck_oprc": "100",
                "stck_hgpr": "102",
                "stck_lwpr": "99",
                "stck_prpr": "101",
                "cntg_vol": "0",
                "acml_tr_pbmn": "110000",
            },
        ]
        _raw, diagnostics = parse_kis_minute_rows_with_diagnostics(rows, symbol="005930")
        self.assertEqual(diagnostics.zero_volume_positive_amount_count, 1)
        self.assertTrue(diagnostics.invalid_mapping_sample_rows)

    def test_positive_volume_zero_amount_is_estimated_and_counted(self) -> None:
        rows = [
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090100",
                "stck_oprc": "100",
                "stck_hgpr": "101",
                "stck_lwpr": "99",
                "stck_prpr": "100",
                "cntg_vol": "10",
            }
        ]
        raw, diagnostics = parse_kis_minute_rows_with_diagnostics(rows, symbol="005930")
        self.assertEqual(raw[0].amount, 1000.0)
        self.assertEqual(diagnostics.estimated_traded_value_count, 1)
        self.assertEqual(diagnostics.positive_volume_zero_amount_count, 0)

    def test_aggregation_does_not_create_zero_volume_bars_for_missing_minutes(self) -> None:
        rows = [
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090000",
                "stck_oprc": "100",
                "stck_hgpr": "101",
                "stck_lwpr": "99",
                "stck_prpr": "100",
                "cntg_vol": "10",
                "tr_pbmn": "1000",
            },
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "091000",
                "stck_oprc": "101",
                "stck_hgpr": "102",
                "stck_lwpr": "100",
                "stck_prpr": "101",
                "cntg_vol": "20",
                "tr_pbmn": "2020",
            },
        ]
        raw, _diagnostics = parse_kis_minute_rows_with_diagnostics(rows, symbol="005930")
        five = aggregate_bars(raw, "5m")
        self.assertEqual([bar.timestamp.isoformat() for bar in five], ["2026-05-08T09:00:00", "2026-05-08T09:10:00"])
        self.assertEqual([bar.volume for bar in five], [10.0, 20.0])

    def test_fetch_loop_filters_raw_rows_to_required_trade_date(self) -> None:
        from scripts.fetch_kis_intraday_prices import _earliest_required_page_timestamp, _filter_raw_rows_to_required_dates

        rows = [
            {"stck_bsop_date": "20260511", "stck_cntg_hour": "090000"},
            {"stck_bsop_date": "20260508", "stck_cntg_hour": "153000"},
        ]
        filtered = _filter_raw_rows_to_required_dates(rows, {"2026-05-11"})
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["stck_bsop_date"], "20260511")
        page_raw = [
            IntradayBar("005930", datetime.fromisoformat("2026-05-08T15:30:00"), "1m", 1, 1, 1, 1, 1),
            IntradayBar("005930", datetime.fromisoformat("2026-05-11T09:00:00"), "1m", 1, 1, 1, 1, 1),
        ]
        earliest = _earliest_required_page_timestamp(page_raw, {"2026-05-11"})
        self.assertEqual(earliest.isoformat(), "2026-05-11T09:00:00")

    def test_aggregation_audit_classifies_raw_normalized_and_aggregated_zero_volume(self) -> None:
        from scripts.audit_kis_intraday_aggregation import _audit_bars, _missing_minute_gaps

        raw, _diagnostics = parse_kis_minute_rows_with_diagnostics(
            [
                {
                    "stck_bsop_date": "20260511",
                    "stck_cntg_hour": "090000",
                    "stck_oprc": "100",
                    "stck_hgpr": "101",
                    "stck_lwpr": "99",
                    "stck_prpr": "100",
                    "cntg_vol": "0",
                    "tr_pbmn": "0",
                },
                {
                    "stck_bsop_date": "20260511",
                    "stck_cntg_hour": "091000",
                    "stck_oprc": "100",
                    "stck_hgpr": "101",
                    "stck_lwpr": "99",
                    "stck_prpr": "100",
                    "cntg_vol": "10",
                    "tr_pbmn": "1000",
                },
            ],
            symbol="005930",
        )
        five = aggregate_bars(raw, "5m")
        audit = _audit_bars(five, sample_limit=5)
        gaps = _missing_minute_gaps(raw, sample_limit=5)
        self.assertEqual([bar.timestamp.isoformat() for bar in five], ["2026-05-11T09:00:00", "2026-05-11T09:10:00"])
        self.assertEqual(audit["zero_volume_count"], 1)
        self.assertEqual(audit["invalid_count_by_reason"]["ZERO_VOLUME_VALID_PRICE"], 1)
        self.assertEqual(gaps["count"], 1)

    def test_aggregation_audit_dry_run_does_not_print_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_path = Path(td) / ".env"
            env_path.write_text(
                "KIS_ENV=paper\n"
                "KIS_APP_KEY=FAKE_APP_KEY_SHOULD_NOT_LEAK\n"
                "KIS_APP_SECRET=FAKE_SECRET_SHOULD_NOT_LEAK\n"
                "KIS_BASE_URL=https://openapivts.koreainvestment.com:29443\n",
                encoding="utf-8",
            )
            proc = _run(
                [
                    "scripts/audit_kis_intraday_aggregation.py",
                    "--env-file",
                    str(env_path),
                    "--trade-date",
                    "2026-05-11",
                    "--symbols",
                    "005930",
                    "--dry-run",
                ],
                env=_clean_env(),
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertNotIn("FAKE_APP_KEY_SHOULD_NOT_LEAK", proc.stdout + proc.stderr)
        self.assertNotIn("FAKE_SECRET_SHOULD_NOT_LEAK", proc.stdout + proc.stderr)

    def test_raw_kis_field_audit_summarizes_candidate_fields(self) -> None:
        rows = [
            {
                "stck_bsop_date": "20260508",
                "stck_cntg_hour": "090100",
                "stck_oprc": "100",
                "stck_hgpr": "101",
                "stck_lwpr": "99",
                "stck_prpr": "100",
                "cntg_vol": "10",
                "acml_vol": "1000",
                "acml_tr_pbmn": "100000",
            }
        ]
        audit = audit_kis_intraday_rows(rows)
        self.assertIn("cntg_vol", audit["field_names"])
        self.assertIn("cntg_vol", audit["volume_candidate_fields"]["known"])
        self.assertIn("acml_tr_pbmn", audit["amount_candidate_fields"]["known"])

    def test_field_audit_dry_run_does_not_print_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            env_path = Path(td) / ".env"
            env_path.write_text(
                "KIS_ENV=paper\n"
                "KIS_APP_KEY=FAKE_APP_KEY_SHOULD_NOT_LEAK\n"
                "KIS_APP_SECRET=FAKE_SECRET_SHOULD_NOT_LEAK\n"
                "KIS_BASE_URL=https://openapivts.koreainvestment.com:29443\n",
                encoding="utf-8",
            )
            proc = _run(
                [
                    "scripts/audit_kis_intraday_fields.py",
                    "--env-file",
                    str(env_path),
                    "--symbol",
                    "005930",
                    "--market-symbol",
                    "069500",
                    "--dry-run",
                ],
                env=_clean_env(),
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertNotIn("FAKE_APP_KEY_SHOULD_NOT_LEAK", proc.stdout + proc.stderr)
        self.assertNotIn("FAKE_SECRET_SHOULD_NOT_LEAK", proc.stdout + proc.stderr)

    def test_fetch_dry_run_parses_required_manifest_without_network_or_db_write(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            env_path = tmp / ".env"
            required = tmp / "required.csv"
            output = tmp / "out.csv"
            env_path.write_text(
                "KIS_ENV=paper\n"
                "KIS_APP_KEY=FAKE_APP_KEY_SHOULD_NOT_LEAK\n"
                "KIS_APP_SECRET=FAKE_SECRET_SHOULD_NOT_LEAK\n"
                "KIS_BASE_URL=https://openapivts.koreainvestment.com:29443\n",
                encoding="utf-8",
            )
            required.write_text(
                "date,symbol,timeframe,source_type,score_date,rank,score,required_reason\n"
                "2026-05-08,005930,5m,CANDIDATE,2026-05-07,1,0.91,TOP_N_SWING_CANDIDATE\n"
                "2026-05-08,999999,5m,MARKET_PROXY,,,,MARKET_CONTEXT\n",
                encoding="utf-8",
            )
            proc = _run(
                [
                    "scripts/fetch_kis_intraday_prices.py",
                    "--env-file",
                    str(env_path),
                    "--required-intraday-csv",
                    str(required),
                    "--output-csv",
                    str(output),
                    "--dry-run",
                ],
                env=_clean_env(),
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertNotIn("FAKE_APP_KEY_SHOULD_NOT_LEAK", proc.stdout + proc.stderr)
        self.assertNotIn("FAKE_SECRET_SHOULD_NOT_LEAK", proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["status"], "dry_run")
        self.assertEqual(payload["candidate_symbol_count"], 1)
        self.assertTrue(payload["market_proxy_required"])

    def test_fetch_dry_run_replaces_placeholder_market_proxy_with_etf_symbol(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            env_path = tmp / ".env"
            required = tmp / "required.csv"
            output = tmp / "out.csv"
            env_path.write_text(
                "KIS_ENV=paper\n"
                "KIS_APP_KEY=FAKE_APP_KEY_SHOULD_NOT_LEAK\n"
                "KIS_APP_SECRET=FAKE_SECRET_SHOULD_NOT_LEAK\n"
                "KIS_BASE_URL=https://openapivts.koreainvestment.com:29443\n",
                encoding="utf-8",
            )
            required.write_text(
                "date,symbol,timeframe,source_type,score_date,rank,score,required_reason\n"
                "2026-05-08,005930,5m,CANDIDATE,2026-05-07,1,0.91,TOP_N_SWING_CANDIDATE\n"
                "2026-05-08,MARKET_PROXY,5m,MARKET_PROXY,,,,MARKET_CONTEXT\n",
                encoding="utf-8",
            )
            proc = _run(
                [
                    "scripts/fetch_kis_intraday_prices.py",
                    "--env-file",
                    str(env_path),
                    "--required-intraday-csv",
                    str(required),
                    "--output-csv",
                    str(output),
                    "--market-proxy-source",
                    "ETF",
                    "--market-proxy-symbol",
                    "069500",
                    "--dry-run",
                ],
                env=_clean_env(),
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["market_proxy_source"], "ETF")
        self.assertEqual(payload["market_proxy_manifest_symbol"], "MARKET_PROXY")
        self.assertEqual(payload["market_proxy_fetch_symbol"], "069500")
        self.assertEqual(payload["market_proxy_output_symbol"], "069500")

    def test_fetch_dry_run_keeps_concrete_market_proxy_without_replace_flag(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            env_path = tmp / ".env"
            required = tmp / "required.csv"
            output = tmp / "out.csv"
            env_path.write_text(
                "KIS_ENV=paper\n"
                "KIS_APP_KEY=FAKE_APP_KEY_SHOULD_NOT_LEAK\n"
                "KIS_APP_SECRET=FAKE_SECRET_SHOULD_NOT_LEAK\n"
                "KIS_BASE_URL=https://openapivts.koreainvestment.com:29443\n",
                encoding="utf-8",
            )
            required.write_text(
                "date,symbol,timeframe,source_type,score_date,rank,score,required_reason\n"
                "2026-05-08,999999,5m,MARKET_PROXY,,,,MARKET_CONTEXT\n",
                encoding="utf-8",
            )
            proc = _run(
                [
                    "scripts/fetch_kis_intraday_prices.py",
                    "--env-file",
                    str(env_path),
                    "--required-intraday-csv",
                    str(required),
                    "--output-csv",
                    str(output),
                    "--market-proxy-source",
                    "ETF",
                    "--market-proxy-symbol",
                    "069500",
                    "--dry-run",
                ],
                env=_clean_env(),
            )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["market_proxy_manifest_symbol"], "999999")
        self.assertEqual(payload["market_proxy_fetch_symbol"], "069500")
        self.assertEqual(payload["market_proxy_output_symbol"], "999999")

    def test_etf_proxy_bars_can_be_written_with_proxy_symbol(self) -> None:
        from scripts.fetch_kis_intraday_prices import _remap_symbol

        payload = {
            "rt_cd": "0",
            "output2": [
                {
                    "stck_bsop_date": "20260508",
                    "stck_cntg_hour": "090100",
                    "stck_oprc": "100",
                    "stck_hgpr": "101",
                    "stck_lwpr": "99",
                    "stck_prpr": "100",
                    "cntg_vol": "1000",
                }
            ],
        }
        bars = parse_kis_minute_payload(payload, symbol="069500")
        remapped = _remap_symbol(bars, "069500", source="KIS_QUOTATION_MARKET_PROXY_ETF")
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "proxy.csv"
            write_intraday_bars_csv(out, remapped)
            with out.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        self.assertEqual(rows[0]["symbol"], "069500")
        self.assertEqual(rows[0]["source"], "KIS_QUOTATION_MARKET_PROXY_ETF")

    def test_intraday_loader_accepts_symbolic_market_proxy(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            db_path = tmp / "market.sqlite"
            csv_path = tmp / "intraday.csv"
            csv_path.write_text(
                "symbol,timestamp,open,high,low,close,volume,timeframe,traded_value\n"
                "MARKET_PROXY,2026-05-08T09:00:00,100,101,99,100,1000,5m,100000\n",
                encoding="utf-8",
            )
            conn = get_connection(db_path)
            init_db(conn)
            result = load_intraday_prices_csv(conn, csv_path)
            row = conn.execute("SELECT symbol FROM intraday_prices").fetchone()
            conn.close()
        self.assertEqual(result.valid_rows, 1)
        self.assertEqual(row["symbol"], "MARKET_PROXY")

    def test_market_proxy_collection_failure_is_explicitly_blocked_by_fetch_flow(self) -> None:
        class FakeClient:
            def inquire_index_minute(self, index_code: str, *, input_hour: str = "153000") -> dict:
                raise KisClientError("KIS_NETWORK_ERROR: blocked")

        from scripts.fetch_kis_intraday_prices import _collect_market_proxy

        with self.assertRaises(KisClientError):
            _collect_market_proxy(
                FakeClient(),
                output_symbol="999999",
                index_code="0001",
                timeframe="5m",
                confirm_timeframe="15m",
                input_hour="153000",
                session_start="090000",
                max_pages=1,
                required_dates_by_symbol={"999999": {"2026-05-08"}},
            )


if __name__ == "__main__":
    unittest.main()
