#!/usr/bin/env bash
set -euo pipefail

echo ""
echo "============================================================"
echo "KRX Daily Paper Ops + Half Shadow Addendum"
echo "============================================================"
echo ""

cd "$HOME/Projects/krx-stock"
PYTHON_BIN="$HOME/Projects/krx-stock/.venv/bin/python"
PAPER_DIR="$HOME/krx-stock-persist/reports/paper_trading"

set_stage() {
  export KRX_STAGE="$1"
  echo "KRX_STAGE=$KRX_STAGE"
  if [ -n "${KRX_STAGE_FILE:-}" ]; then
    echo "$KRX_STAGE" > "$KRX_STAGE_FILE"
  fi
}

check_fresh_paper_report() {
  local target_date="${TARGET_END_DATE:-$(date +%Y-%m-%d)}"
  "$PYTHON_BIN" - "$target_date" "$PAPER_DIR" <<'PY'
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

target = sys.argv[1]
paper_dir = Path(sys.argv[2]).expanduser()
files = sorted(paper_dir.glob("*_paper_report_summary.json"), key=lambda p: p.stat().st_mtime)
if not files:
    print("FAIL: stale_success_check reason=no paper_report_summary found")
    raise SystemExit(1)

summary_path = files[-1]
summary = json.loads(summary_path.read_text(encoding="utf-8"))
latest_signal_date = str(summary.get("latest_signal_date") or "")
latest_holdings_date = str(summary.get("latest_holdings_date") or "")

try:
    target_is_weekday = date.fromisoformat(target).weekday() < 5
except ValueError:
    target_is_weekday = True

print(
    "freshness_check "
    f"target_date={target} "
    f"latest_signal_date={latest_signal_date or 'missing'} "
    f"latest_holdings_date={latest_holdings_date or 'missing'} "
    f"target_is_weekday={target_is_weekday} "
    "calendar_validation=weekday_only_uncertain"
)

if not latest_signal_date or not latest_holdings_date:
    print("FAIL: stale_success_check reason=missing latest_signal_date/latest_holdings_date")
    raise SystemExit(1)

if target_is_weekday and (latest_signal_date < target or latest_holdings_date < target):
    print(
        "FAIL: stale_success_check reason=DB/report latest date older than weekday target; "
        "success Telegram blocked; calendar validation uncertain"
    )
    raise SystemExit(1)
PY
}

echo "------------------------------------------------------------"
echo "1) Activate venv"
echo "------------------------------------------------------------"
source .venv/bin/activate

echo ""
echo "------------------------------------------------------------"
echo "2) Run baseline_old Mac daily paper ops"
echo "------------------------------------------------------------"
set_stage "baseline_old_daily_ops"
bash scripts/run_mac_daily_paper_ops.sh

echo ""
echo "------------------------------------------------------------"
echo "2-1) Prevent stale success summary"
echo "------------------------------------------------------------"
set_stage "stale_success_check"
check_fresh_paper_report

echo ""
echo "------------------------------------------------------------"
echo "3) Run half-shadow addendum wrapper"
echo "------------------------------------------------------------"
set_stage "half_shadow_addendum"
bash "$HOME/Projects/krx-stock/scripts/local/run_half_shadow_addendum_local.sh"

echo ""
echo "------------------------------------------------------------"
echo "4) Verify Hermes-readable file"
echo "------------------------------------------------------------"
ls -lh "$HOME/.hermes/krx/latest_half_shadow_addendum.md"
head -20 "$HOME/.hermes/krx/latest_half_shadow_addendum.md"

echo ""
echo "============================================================"
echo "DONE"
echo "Hermes path:"
echo "/opt/data/krx/latest_half_shadow_addendum.md"
echo "============================================================"
echo ""
echo "------------------------------------------------------------"
echo "5) Build integrated KRX daily summary for Hermes"
echo "------------------------------------------------------------"
set_stage "integrated_summary"
"$PYTHON_BIN" "$HOME/Projects/krx-stock/scripts/local/build_krx_daily_integrated_summary.py"

echo ""
echo "------------------------------------------------------------"
echo "6) Verify integrated Hermes summary file"
echo "------------------------------------------------------------"
ls -lh "$HOME/.hermes/krx/latest_krx_daily_summary.md"
head -40 "$HOME/.hermes/krx/latest_krx_daily_summary.md"
echo ""
echo "------------------------------------------------------------"
echo "7) Send integrated KRX daily summary to Telegram"
echo "------------------------------------------------------------"
set_stage "telegram_summary"
bash "$HOME/Projects/krx-stock/scripts/local/send_krx_daily_telegram_summary.sh"
