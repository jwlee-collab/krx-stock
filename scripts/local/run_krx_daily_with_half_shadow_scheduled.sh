#!/usr/bin/env bash
set -euo pipefail

echo ""
echo "============================================================"
echo "KRX Scheduled Daily Runner"
echo "============================================================"
echo ""

NOW_HM="$(date +%H%M)"
RUN_DATE="${KRX_RUN_DATE:-$(date +%Y-%m-%d)}"
LOCK_DIR="$HOME/krx-stock-persist/locks/daily_ops"
DONE_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.done"
INPROGRESS_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.running"
STAGE_FILE="$LOCK_DIR/krx_daily_half_shadow_${RUN_DATE}.stage"
DB_PATH="${KRX_DB_PATH:-$HOME/krx-stock-persist/data/kospi_495_rolling_3y.db}"
FAILURE_ALERT_SCRIPT="$HOME/Projects/krx-stock/scripts/local/send_krx_daily_failure_telegram.sh"

mkdir -p "$LOCK_DIR"

echo "Current local time: $(date)"
echo "HHMM: $NOW_HM"
echo "Run date: $RUN_DATE"
echo "Done file: $DONE_FILE"
echo "In-progress file: $INPROGRESS_FILE"
echo "Stage file: $STAGE_FILE"

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

latest_pipeline_log() {
  ls -t "$HOME/krx-stock-persist/logs"/mac_market_db_update_*.log 2>/dev/null | head -1 || true
}

detect_failure_reason() {
  local stage="$1"
  local exit_code="$2"
  local log_path
  log_path="$(latest_pipeline_log)"

  if [ -n "$log_path" ] && grep -Eiq "get_market_ohlcv_by_ticker|pykrx|KeyError|None of .*시가|None of .*고가|None of .*저가|None of .*종가" "$log_path"; then
    echo "pykrx provider failure; fallback available but not applied to production DB"
    return 0
  fi

  if [ -n "$log_path" ] && grep -Eiq "timed out|TimeoutExpired|pipeline timeout" "$log_path"; then
    echo "pipeline timeout; fallback available but not applied to production DB"
    return 0
  fi

  if [ "$stage" = "stale_success_check" ]; then
    echo "stale DB/report detected before success Telegram; calendar validation uncertain"
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

  stage="$(cat "$STAGE_FILE" 2>/dev/null || echo "master_wrapper")"
  db_latest="$(latest_db_date)"
  db_latest="${db_latest:-unknown}"
  reason="$(detect_failure_reason "$stage" "$exit_code")"
  log_path="$(latest_pipeline_log)"

  echo "FAIL: scheduled run failed"
  echo "stage=$stage"
  echo "target_date=$RUN_DATE"
  echo "db_latest_date=$db_latest"
  echo "reason=$reason"

  if [ ! -f "$FAILURE_ALERT_SCRIPT" ]; then
    echo "WARN: failure alert script missing: $FAILURE_ALERT_SCRIPT"
    return 0
  fi

  KRX_TARGET_DATE="$RUN_DATE" \
  KRX_DB_LATEST_DATE="$db_latest" \
  KRX_FAILURE_STAGE="$stage" \
  KRX_FAILURE_REASON="$reason" \
  KRX_FAILURE_LOG="$log_path" \
    bash "$FAILURE_ALERT_SCRIPT" || echo "WARN: Telegram failure alert failed"
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
echo "scheduled_wrapper" > "$STAGE_FILE"
trap on_exit EXIT

echo ""
echo "------------------------------------------------------------"
echo "Run master wrapper"
echo "------------------------------------------------------------"
echo ""

export TARGET_END_DATE="$RUN_DATE"
export KRX_STAGE_FILE="$STAGE_FILE"
bash "$HOME/Projects/krx-stock/scripts/local/run_krx_daily_with_half_shadow.sh"

echo "completed_at=$(date)" > "$DONE_FILE"
echo "run_date=$RUN_DATE" >> "$DONE_FILE"
echo "status=success" >> "$DONE_FILE"

echo ""
echo "============================================================"
echo "Scheduled run completed"
echo "============================================================"
