#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.kis_client import DEFAULT_KIS_TOKEN_CACHE_PATH, KisQuotationClient, load_kis_env  # noqa: E402
from scripts.check_kis_env import build_kis_env_diagnostics  # noqa: E402


def _redact(text: str, secrets: list[str]) -> str:
    out = text
    for secret in secrets:
        if secret:
            out = out.replace(secret, "***REDACTED***")
    return out


def _safe_error_summary(exc: Exception, secrets: list[str]) -> dict[str, Any]:
    raw = _redact(str(exc), secrets)
    try:
        parsed = json.loads(raw)
        return {"raw": parsed}
    except Exception:
        return {"message": raw}


def _possible_403_causes() -> list[str]:
    return [
        "KIS_ENV/KIS_BASE_URL mismatch",
        "App Key type does not match real/paper base URL",
        "KIS Open API application is not approved or enabled for token issuance",
        "tokenP issuance throttling or temporary KIS restriction",
        "KIS portal/account status requires user verification",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe KIS token issuance safely without printing token, app key, or app secret")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--env", choices=["paper", "real"], default=None)
    parser.add_argument("--token-cache", default=str(DEFAULT_KIS_TOKEN_CACHE_PATH))
    parser.add_argument("--force-refresh-token", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    env_diagnostics = build_kis_env_diagnostics(args.env_file, env_override=args.env)
    output: dict[str, Any] = {
        "status": "ok",
        "env_diagnostics": env_diagnostics,
        "token_cache": None,
        "token_status": "not_requested",
        "tokenP_called": False,
    }
    if env_diagnostics["status"] != "ok":
        output["status"] = "blocked"
        output["blocked_reason"] = "KIS_ENV_DIAGNOSTICS_FAILED"
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(2)
    env_result = load_kis_env(args.env_file, env_override=args.env)
    if env_result.status != "ok" or env_result.config is None:
        output["status"] = "blocked"
        output["blocked_reason"] = env_result.reason
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(2)
    secrets = [env_result.config.app_key, env_result.config.app_secret]
    client = KisQuotationClient(env_result.config, token_cache_path=args.token_cache, dry_run=args.dry_run)
    cache_summary = client.token_cache_summary()
    output["token_cache"] = cache_summary
    if args.dry_run:
        output["status"] = "dry_run"
        output["token_status"] = "dry_run"
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return
    if cache_summary.get("usable") and not args.force_refresh_token:
        output["token_status"] = "cache_usable"
        output["expires_at"] = cache_summary.get("expires_at")
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        return
    try:
        output["tokenP_called"] = True
        client.get_access_token(force_refresh=args.force_refresh_token)
        refreshed_cache = client.token_cache_summary()
        output["token_cache"] = refreshed_cache
        output["token_status"] = "issued"
        output["expires_at"] = refreshed_cache.get("expires_at")
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    except Exception as exc:
        output["status"] = "blocked"
        output["token_status"] = "blocked"
        output["blocked_reason"] = "KIS_TOKEN_ISSUANCE_FAILED"
        output["safe_error_summary"] = _safe_error_summary(exc, secrets)
        output["possible_causes"] = _possible_403_causes()
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
