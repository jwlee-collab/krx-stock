#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.day_trading.kis_client import REQUIRED_KIS_ENV_VARS, load_kis_env, parse_dotenv  # noqa: E402


def _raw_env_file_values(path: str | Path) -> dict[str, str]:
    env_path = Path(path)
    if not env_path.exists():
        return {}
    out: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = raw_line.split("=", 1)
        out[key.strip()] = raw_value
    return out


def _value_issues(key: str, raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []
    issues: list[str] = []
    if raw_value != raw_value.strip():
        issues.append(f"{key}_HAS_LEADING_OR_TRAILING_WHITESPACE")
    stripped = raw_value.strip()
    if (stripped.startswith('"') and stripped.endswith('"')) or (stripped.startswith("'") and stripped.endswith("'")):
        issues.append(f"{key}_HAS_OUTER_QUOTES")
    if "\\n" in raw_value or "\n" in raw_value or "\r" in raw_value:
        issues.append(f"{key}_HAS_NEWLINE_ESCAPE_OR_LINEBREAK")
    return issues


def build_kis_env_diagnostics(env_file: str | Path = ".env", env_override: str | None = None) -> dict[str, Any]:
    file_values = parse_dotenv(env_file)
    raw_file_values = _raw_env_file_values(env_file)
    merged = dict(file_values)
    sources = {key: "env_file" for key in merged}
    for key in REQUIRED_KIS_ENV_VARS:
        if os.environ.get(key):
            merged[key] = os.environ[key]
            sources[key] = "environment"
    if env_override:
        merged["KIS_ENV"] = env_override
        sources["KIS_ENV"] = "argument"
    missing = [key for key in REQUIRED_KIS_ENV_VARS if not merged.get(key)]
    issues: list[str] = []
    for key in ("KIS_APP_KEY", "KIS_APP_SECRET"):
        if sources.get(key) == "env_file":
            issues.extend(_value_issues(key, raw_file_values.get(key)))
    kis_env = str(merged.get("KIS_ENV") or "").strip().lower()
    base_url = str(merged.get("KIS_BASE_URL") or "").strip().rstrip("/")
    if kis_env == "real" and "openapivts" in base_url:
        issues.append("KIS_ENV_REAL_BASE_URL_POINTS_TO_PAPER")
    if kis_env == "paper" and "openapi.koreainvestment.com" in base_url and "openapivts" not in base_url:
        issues.append("KIS_ENV_PAPER_BASE_URL_POINTS_TO_REAL")
    if kis_env and kis_env not in {"paper", "real"}:
        issues.append("KIS_ENV_INVALID")
    status = "ok"
    if missing or any(issue.endswith("_BASE_URL_POINTS_TO_PAPER") or issue.endswith("_BASE_URL_POINTS_TO_REAL") or issue == "KIS_ENV_INVALID" for issue in issues):
        status = "blocked"
    return {
        "status": status,
        "env_file": str(env_file),
        "env_file_exists": Path(env_file).exists(),
        "missing_env_vars": missing,
        "kis_env": kis_env or None,
        "base_url": base_url or None,
        "base_url_kind": "paper" if "openapivts" in base_url else ("real" if "openapi.koreainvestment.com" in base_url else "unknown"),
        "app_key_present": bool(merged.get("KIS_APP_KEY")),
        "app_key_length": len(str(merged.get("KIS_APP_KEY") or "")),
        "app_secret_present": bool(merged.get("KIS_APP_SECRET")),
        "app_secret_length": len(str(merged.get("KIS_APP_SECRET") or "")),
        "sources": {key: sources.get(key) for key in REQUIRED_KIS_ENV_VARS},
        "issues": issues,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check KIS environment settings without printing app keys, secrets, or tokens")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--env", choices=["paper", "real"], default=None)
    args = parser.parse_args()
    diagnostics = build_kis_env_diagnostics(args.env_file, env_override=args.env)
    env_result = load_kis_env(args.env_file, env_override=args.env)
    if env_result.status != "ok":
        diagnostics["status"] = "blocked"
        diagnostics["load_reason"] = env_result.reason
    print(json.dumps(diagnostics, ensure_ascii=False, indent=2, default=str))
    raise SystemExit(0 if diagnostics["status"] == "ok" else 2)


if __name__ == "__main__":
    main()
