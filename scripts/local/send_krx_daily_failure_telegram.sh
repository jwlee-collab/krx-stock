#!/usr/bin/env bash
set -euo pipefail

ENV1="$HOME/.hermes/.env"
ENV2="$HOME/hermes-docker/config/.env"

TARGET_DATE="${KRX_TARGET_DATE:-${TARGET_END_DATE:-$(date +%Y-%m-%d)}}"
DB_LATEST_DATE="${KRX_DB_LATEST_DATE:-unknown}"
STAGE="${KRX_FAILURE_STAGE:-${KRX_STAGE:-unknown}}"
REASON="${KRX_FAILURE_REASON:-unknown failure}"
LOG_PATH="${KRX_FAILURE_LOG:-}"
DETAILS="${KRX_FAILURE_DETAILS:-}"
OUT="${KRX_FAILURE_MESSAGE_OUT:-$HOME/krx-stock-persist/logs/krx_daily_failure_telegram_${TARGET_DATE}_$(date +%H%M%S).txt}"

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

cat > "$OUT" <<MSG
⚠️ KRX Daily Ops Failed

- target_date: $TARGET_DATE
- db_latest_date: $DB_LATEST_DATE
- stage: $STAGE
- reason: $REASON
- official paper report not updated
- baseline_old unchanged
- Naver fallback not applied to production DB
- action required
MSG

if [ -n "$LOG_PATH" ]; then
  {
    echo "- log: $LOG_PATH"
  } >> "$OUT"
fi

if [ -n "$DETAILS" ]; then
  {
    echo ""
    echo "Details:"
    echo "$DETAILS"
  } >> "$OUT"
fi

if [ "${DRY_RUN:-0}" = "1" ]; then
  echo "DRY_RUN=1; Telegram failure alert not sent."
  echo "Failure message:"
  cat "$OUT"
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

curl -sS -X POST "https://api.telegram.org/bot${TOKEN}/sendMessage" \
  --data-urlencode "chat_id=${CHAT_ID}" \
  --data-urlencode "text@${OUT}" \
  --data-urlencode "disable_web_page_preview=true" >/dev/null

echo "Telegram failure alert sent."
