#!/usr/bin/env bash
set -euo pipefail

echo ""
echo "============================================================"
echo "KRX Scheduled Daily Runner"
echo "============================================================"
echo ""

NOW_HM="$(date +%H%M)"
RUN_DATE="${KRX_RUN_DATE:-$(date +%Y-%m-%d)}"
LOCK_DIR="${KRX_LOCK_DIR:-$HOME/krx-stock-persist/locks/daily_ops}"
DONE_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.done"
SKIPPED_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.skipped"
INPROGRESS_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.running"
STAGE_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.stage"
FAILURE_ALERT_SENT_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.failure_alert_sent"
CURRENT_LOG_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}_${NOW_HM}.log"
DB_PATH="${KRX_DB_PATH:-$HOME/krx-stock-persist/data/kospi_495_rolling_3y.db}"
MASTER_WRAPPER="${KRX_MASTER_WRAPPER:-$HOME/Projects/krx-stock/scripts/local/run_krx_daily_with_half_shadow.sh}"
FAILURE_ALERT_SCRIPT="${KRX_FAILURE_ALERT_SCRIPT:-$HOME/Projects/krx-stock/scripts/local/send_krx_daily_failure_telegram.sh}"

mkdir -p "$LOCK_DIR"

echo "Current local time: $(date)"
echo "HHMM: $NOW_HM"
echo "Run date: $RUN_DATE"
echo "Done file: $DONE_FILE"
echo "Skipped file: $SKIPPED_FILE"
echo "In-progress file: $INPROGRESS_FILE"
echo "Stage file: $STAGE_FILE"
echo "Current log file: $CURRENT_LOG_FILE"

latest_db_date() {
  python3 - "$DB_PATH" <<'PY' 2>/dev/null || true
import sqlite3
import sys

db = sys.argv[1]
try:
    con = sqlite3.connect(db)
    try:
        row = con.execute("""
            SELECT MAX(s.date)
            FROM daily_scores s
            JOIN daily_universe u
              ON s.date = u.date
             AND s.symbol = u.symbol
            WHERE u.universe_mode = 'rolling_liquidity'
              AND u.universe_size = 100
              AND u.lookback_days = 20
        """).fetchone()
        if row and row[0]:
            print(row[0])
            raise SystemExit(0)
        row = con.execute("SELECT MAX(date) FROM daily_prices").fetchone()
        if row and row[0]:
            print(row[0])
    finally:
        con.close()
except Exception:
    pass
PY
}

is_weekend_run_date() {
  python3 - "$RUN_DATE" <<'PY'
from __future__ import annotations

from datetime import date
import sys

try:
    run_date = date.fromisoformat(sys.argv[1])
except ValueError:
    raise SystemExit(1)
raise SystemExit(0 if run_date.weekday() >= 5 else 1)
PY
}

current_failure_log() {
  if [ -f "$CURRENT_LOG_FILE" ]; then
    echo "$CURRENT_LOG_FILE"
  fi
}

is_baseline_pipeline_stage() {
  case "$1" in
    baseline_old_daily_ops|update_mac_market_db|mac_market_db_update)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

detect_failure_reason() {
  local stage="$1"
  local exit_code="$2"
  local log_path
  log_path="$(current_failure_log)"

  if [ "$stage" = "stale_success_check" ]; then
    echo "stale DB/report detected before success Telegram; calendar validation uncertain"
    return 0
  fi

  if [ "$stage" = "half_shadow_addendum" ]; then
    echo "half-shadow addendum failed with exit_code=$exit_code"
    return 0
  fi

  if [ "$stage" = "integrated_summary" ]; then
    echo "integrated summary generation failed with exit_code=$exit_code"
    return 0
  fi

  if [ "$stage" = "telegram_summary" ]; then
    echo "Telegram summary send failed with exit_code=$exit_code"
    return 0
  fi

  if [ "$stage" = "html_report_send" ]; then
    echo "HTML report send failed with exit_code=$exit_code"
    return 0
  fi

  if is_baseline_pipeline_stage "$stage"; then
    if [ -n "$log_path" ] && grep -Eiq "get_market_ohlcv_by_ticker|pykrx|KeyError|None of .*시가|None of .*고가|None of .*저가|None of .*종가" "$log_path"; then
      echo "pykrx provider failure; fallback available but not applied to production DB"
      return 0
    fi

    if [ -n "$log_path" ] && grep -Eiq "timed out|TimeoutExpired|pipeline timeout" "$log_path"; then
      echo "pipeline timeout; fallback available but not applied to production DB"
      return 0
    fi

    echo "baseline pipeline failed with exit_code=$exit_code"
    return 0
  fi

  echo "master wrapper failed with exit_code=$exit_code"
}

send_failure_alert() {
  local exit_code="$1"
  local stage
  local db_latest
  local reason
  local log_path

  if [ -f "$FAILURE_ALERT_SENT_FILE" ]; then
    echo "SKIP: failure alert already sent for run date"
    cat "$FAILURE_ALERT_SENT_FILE" || true
    return 0
  fi

  stage="$(cat "$STAGE_FILE" 2>/dev/null || echo "master_wrapper")"
  db_latest="$(latest_db_date)"
  db_latest="${db_latest:-unknown}"
  reason="$(detect_failure_reason "$stage" "$exit_code")"
  log_path="$(current_failure_log)"

  echo "FAIL: scheduled run failed"
  echo "stage=$stage"
  echo "target_date=$RUN_DATE"
  echo "db_latest_date=$db_latest"
  echo "reason=$reason"

  if [ ! -f "$FAILURE_ALERT_SCRIPT" ]; then
    echo "WARN: failure alert script missing: $FAILURE_ALERT_SCRIPT"
    return 0
  fi

  if KRX_TARGET_DATE="$RUN_DATE" \
    KRX_DB_LATEST_DATE="$db_latest" \
    KRX_FAILURE_STAGE="$stage" \
    KRX_FAILURE_REASON="$reason" \
    KRX_FAILURE_LOG="${log_path:-unknown}" \
      bash "$FAILURE_ALERT_SCRIPT"; then
    {
      echo "sent_at=$(date)"
      echo "run_date=$RUN_DATE"
      echo "stage=$stage"
      echo "reason=$reason"
    } > "$FAILURE_ALERT_SENT_FILE"
  else
    echo "WARN: Telegram failure alert failed"
  fi
}

on_exit() {
  local exit_code=$?
  set +e
  rm -f "$INPROGRESS_FILE"
  if [ "$exit_code" -ne 0 ]; then
    send_failure_alert "$exit_code"
  fi
  rm -f "$STAGE_FILE"
  exit "$exit_code"
}

if [ "${FORCE_KRX_DAILY:-0}" != "1" ]; then
  if [ -f "$SKIPPED_FILE" ]; then
    echo "SKIP: weekend/non-trading calendar day"
    cat "$SKIPPED_FILE" || true
    exit 0
  fi

  if is_weekend_run_date; then
    echo "SKIP: weekend/non-trading calendar day"
    {
      echo "skipped_at=$(date)"
      echo "run_date=$RUN_DATE"
      echo "reason=weekend/non-trading calendar day"
    } > "$SKIPPED_FILE"
    exit 0
  fi

  if [ "$NOW_HM" -lt 1655 ] || [ "$NOW_HM" -gt 1810 ]; then
    echo "SKIP: outside allowed run window 16:55~18:10 KST"
    exit 0
  fi
fi

if [ -f "$DONE_FILE" ] && [ "${FORCE_KRX_DAILY:-0}" != "1" ]; then
  echo "SKIP: already completed for run date"
  cat "$DONE_FILE" || true
  exit 0
fi

if [ -f "$INPROGRESS_FILE" ] && [ "${FORCE_KRX_DAILY:-0}" != "1" ]; then
  AGE_SECONDS=$(( $(date +%s) - $(stat -f %m "$INPROGRESS_FILE") ))
  if [ "$AGE_SECONDS" -lt 7200 ]; then
    echo "SKIP: another run appears to be in progress"
    echo "age_seconds=$AGE_SECONDS"
    cat "$INPROGRESS_FILE" || true
    exit 0
  fi
  echo "WARN: stale in-progress file found; removing"
  cat "$INPROGRESS_FILE" || true
  rm -f "$INPROGRESS_FILE"
fi

echo "started_at=$(date)" > "$INPROGRESS_FILE"
echo "run_date=$RUN_DATE" >> "$INPROGRESS_FILE"
echo "started_at=$(date)" > "$CURRENT_LOG_FILE"
echo "run_date=$RUN_DATE" >> "$CURRENT_LOG_FILE"
echo "scheduled_wrapper" >> "$CURRENT_LOG_FILE"
echo "scheduled_wrapper" > "$STAGE_FILE"
trap on_exit EXIT

echo ""
echo "------------------------------------------------------------"
echo "Run master wrapper"
echo "------------------------------------------------------------"
echo ""

export TARGET_END_DATE="$RUN_DATE"
export KRX_STAGE_FILE="$STAGE_FILE"
set +e
bash "$MASTER_WRAPPER" 2>&1 | tee -a "$CURRENT_LOG_FILE"
MASTER_EXIT_CODE=${PIPESTATUS[0]}
set -e
if [ "$MASTER_EXIT_CODE" -ne 0 ]; then
  exit "$MASTER_EXIT_CODE"
fi

echo "completed_at=$(date)" > "$DONE_FILE"
echo "run_date=$RUN_DATE" >> "$DONE_FILE"
echo "status=success" >> "$DONE_FILE"

echo ""
echo "============================================================"
echo "Scheduled run completed"
echo "============================================================"
