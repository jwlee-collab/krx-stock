#!/usr/bin/env bash
set -euo pipefail

SUMMARY="${KRX_CANDIDATE_SHADOW_SUMMARY:-$HOME/.hermes/krx/latest_candidate_shadow_summary.md}"
FALLBACK_SUMMARY="$HOME/krx-stock-persist/reports/research/exp83/exp83_candidate_shadow_daily_summary_latest.md"
ENV1="$HOME/.hermes/.env"
ENV2="$HOME/hermes-docker/config/.env"
OUT="${KRX_CANDIDATE_SHADOW_MESSAGE_OUT:-$HOME/krx-stock-persist/reports/research/exp83/latest_candidate_shadow_telegram_message.txt}"

echo ""
echo "============================================================"
echo "Send KRX Candidate Shadow Telegram Summary"
echo "============================================================"
echo ""

if [ ! -f "$SUMMARY" ] && [ -f "$FALLBACK_SUMMARY" ]; then
  SUMMARY="$FALLBACK_SUMMARY"
fi

if [ ! -f "$SUMMARY" ]; then
  echo "FAIL: candidate shadow summary file not found" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT")"

load_env_value() {
  key="$1"
  for f in "$ENV1" "$ENV2"; do
    if [ -f "$f" ]; then
      val="$(grep -E "^${key}=" "$f" | tail -1 | cut -d= -f2- | sed 's/^"//;s/"$//;s/^'\''//;s/'\''$//')" || true
      if [ -n "${val:-}" ]; then
        echo "$val"
        return 0
      fi
    fi
  done
  return 1
}

python3 - "$SUMMARY" "$OUT" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
out_path = Path(sys.argv[2])
lines = summary_path.read_text(encoding="utf-8").splitlines()


def header_value(key: str) -> str:
    prefix = f"- {key}="
    for line in lines:
        if line.startswith(prefix):
            return line.removeprefix(prefix).strip()
    return "unknown"


def label_count(label: str) -> str:
    prefix = f"| {label} |"
    for line in lines:
        if line.startswith(prefix):
            cells = [cell.strip() for cell in line.strip("|").split("|")]
            return cells[1] if len(cells) > 1 else "0"
    return "0"


def section_names(title: str, limit: int) -> list[str]:
    found = False
    names: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("## ") and found:
            break
        if stripped.startswith("## ") and stripped.endswith(title):
            found = True
            continue
        if not found or not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not cells or cells[0].lower() == "name" or set(cells[0]) <= {"-"}:
            continue
        names.append(cells[0])
        if len(names) >= limit:
            break
    return names


def bullet_lines(label: str, title: str, limit: int) -> list[str]:
    names = section_names(title, limit)
    if names:
        return [f"- {name}" for name in names]
    count = label_count(label)
    return ["- none"] if count in {"", "0"} else [f"- {count} rows"]


message = [
    "🧪 KRX Candidate Shadow",
    "",
    f"as_of: {header_value('as_of_date')}",
    "",
    "[Immediate]",
    *bullet_lines("IMMEDIATE_CANDIDATE", "Immediate Candidates", 5),
    "",
    "[Delay Review]",
    *bullet_lines("DELAY_REVIEW", "Delay Review", 5),
    "",
    "[Cooldown / Reclaim Watch]",
    *bullet_lines("COOLDOWN_RECLAIM_WATCH", "Cooldown / Reclaim Watch", 5),
    "",
    "[High Risk Overheat]",
    *bullet_lines("HIGH_RISK_OVERHEAT", "High Risk Overheat", 10),
    "",
    "[Lower Priority]",
    *bullet_lines("LOWER_PRIORITY", "Lower Priority", 5),
    "",
    "[Pending Data]",
    *bullet_lines("PENDING_DATA", "Pending Data", 5),
    "",
    "주의:",
    "- 매수 추천 아님",
    "- baseline_old 공식 paper에는 미반영",
    "- report-only shadow classification",
    "",
    "production_change=false",
    "paper_trading_change=false",
    "real_order_change=false",
]
out_path.write_text("\n".join(message) + "\n", encoding="utf-8")
PY

echo "------------------------------------------------------------"
echo "Candidate shadow Telegram message preview"
echo "------------------------------------------------------------"
cat "$OUT"

if [ "${DRY_RUN:-0}" = "1" ]; then
  echo "DRY_RUN=1; candidate shadow Telegram summary not sent."
  exit 0
fi

TOKEN="$(load_env_value TELEGRAM_BOT_TOKEN || true)"
if [ -z "${TOKEN:-}" ]; then
  TOKEN="$(load_env_value TELEGRAM_TOKEN || true)"
fi

CHAT_ID="$(load_env_value TELEGRAM_CHAT_ID || true)"
if [ -z "${CHAT_ID:-}" ]; then
  CHAT_ID="$(load_env_value TELEGRAM_HOME_CHAT_ID || true)"
fi
if [ -z "${CHAT_ID:-}" ]; then
  CHAT_ID="$(load_env_value TELEGRAM_HOME_CHANNEL || true)"
fi

if [ -z "${TOKEN:-}" ]; then
  echo "FAIL: TELEGRAM_BOT_TOKEN/TELEGRAM_TOKEN not found" >&2
  exit 2
fi

if [ -z "${CHAT_ID:-}" ]; then
  echo "FAIL: TELEGRAM_CHAT_ID/TELEGRAM_HOME_CHANNEL not found" >&2
  exit 3
fi

curl -sS --fail-with-body -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
  --data-urlencode "chat_id=${CHAT_ID}" \
  --data-urlencode "text@${OUT}" \
  --data-urlencode "disable_web_page_preview=true" >/dev/null

echo ""
echo "Candidate shadow Telegram summary sent."
