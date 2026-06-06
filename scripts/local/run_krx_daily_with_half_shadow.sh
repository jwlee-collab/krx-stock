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

latest_file() {
  "$PYTHON_BIN" - "$1" <<'PY'
from __future__ import annotations

import glob
import sys
from pathlib import Path

paths = [Path(path) for path in glob.glob(sys.argv[1])]
if not paths:
    raise SystemExit(1)
print(max(paths, key=lambda path: path.stat().st_mtime))
PY
}

candidate_expected_date() {
  "$PYTHON_BIN" - "$PAPER_DIR" "${TARGET_END_DATE:-}" <<'PY'
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

paper_dir = Path(sys.argv[1])
target = sys.argv[2] or date.today().isoformat()
paths = sorted(paper_dir.glob("*_paper_report_summary.json"), key=lambda path: path.stat().st_mtime)
if not paths:
    print(target)
    raise SystemExit(0)
data = json.loads(paths[-1].read_text(encoding="utf-8"))
print(str(data.get("latest_signal_date") or target))
PY
}

exp82_as_of_date() {
  "$PYTHON_BIN" - "$EXP82_DIR/exp82_metadata_latest.json" "$EXP82_DIR/exp82_shadow_label_latest.csv" <<'PY'
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

metadata = Path(sys.argv[1])
snapshot = Path(sys.argv[2])
if metadata.exists():
    data = json.loads(metadata.read_text(encoding="utf-8"))
    value = str(data.get("as_of_date") or "")
    if value:
        print(value)
        raise SystemExit(0)
if snapshot.exists():
    with snapshot.open("r", encoding="utf-8-sig", newline="") as handle:
        row = next(csv.DictReader(handle), None)
    if row is not None:
        value = str(row.get("as_of_date") or "")
        if value:
            print(value)
            raise SystemExit(0)
raise SystemExit(1)
PY
}

exp83_as_of_date() {
  "$PYTHON_BIN" - "$HOME/.hermes/krx/latest_candidate_shadow_summary.md" "$EXP83_DIR/exp83_candidate_shadow_daily_summary_latest.md" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

for raw in sys.argv[1:]:
    path = Path(raw)
    if not path.exists():
        continue
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("- as_of_date="):
            print(line.removeprefix("- as_of_date=").strip())
            raise SystemExit(0)
raise SystemExit(1)
PY
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

echo ""
echo "------------------------------------------------------------"
echo "8) Build candidate shadow report-only daily summary"
echo "------------------------------------------------------------"
set_stage "candidate_shadow_report_only"
CANDIDATE_DB="$HOME/krx-stock-persist/data/kospi_495_rolling_3y.db"
EXP80A_DIR="$HOME/krx-stock-persist/reports/research/exp80a_pool100"
EXP80C_DIR="$HOME/krx-stock-persist/reports/research/exp80c_pool100"
EXP80D_DIR="$HOME/krx-stock-persist/reports/research/exp80d"
EXP81_DIR="$HOME/krx-stock-persist/reports/research/exp81"
EXP82_DIR="$HOME/krx-stock-persist/reports/research/exp82"
EXP83_DIR="$HOME/krx-stock-persist/reports/research/exp83"
EXP81_DASHBOARD="$HOME/Projects/krx-stock/scripts/research/run_exp81_candidate_shadow_dashboard.py"
EXP82_MONITOR="$HOME/Projects/krx-stock/scripts/research/run_exp82_candidate_shadow_monitor.py"
EXP83_SUMMARY="$HOME/Projects/krx-stock/scripts/research/build_exp83_candidate_shadow_daily_summary.py"
CANDIDATE_TELEGRAM="$HOME/Projects/krx-stock/scripts/local/send_krx_candidate_shadow_telegram_summary.sh"
EXP80A_FORWARD="$(latest_file "$EXP80A_DIR/exp80a_candidate_forward_outcomes_*.csv" || true)"
EXP80A_SNAPSHOT="$(latest_file "$EXP80A_DIR/exp80a_candidate_snapshot_*.csv" || true)"
EXP80C_ASSIGNMENTS="$(latest_file "$EXP80C_DIR/exp80c_candidate_assignments_*.csv" || true)"
EXP80D_ENTRY_PATHS="$(latest_file "$EXP80D_DIR/exp80d_candidate_entry_paths_*.csv" || true)"
EXP80D_RULE_SUMMARY="$(latest_file "$EXP80D_DIR/exp80d_rule_timing_summary_*.csv" || true)"
CANDIDATE_EXPECTED_DATE="$(candidate_expected_date || true)"
if [ -z "$CANDIDATE_EXPECTED_DATE" ]; then
  CANDIDATE_EXPECTED_DATE="${TARGET_END_DATE:-$(date +%Y-%m-%d)}"
fi
EXP82_REFRESHED=0
EXP82_FRESH=0
EXP83_FRESH=0
echo "candidate_shadow_expected_date=$CANDIDATE_EXPECTED_DATE"
if [ -f "$EXP81_DASHBOARD" ] && [ -n "$EXP80A_FORWARD" ] && [ -n "$EXP80A_SNAPSHOT" ] && [ -n "$EXP80C_ASSIGNMENTS" ] && [ -n "$EXP80D_ENTRY_PATHS" ] && [ -n "$EXP80D_RULE_SUMMARY" ]; then
  set_stage "candidate_shadow_exp81_report_only"
  if ! "$PYTHON_BIN" "$EXP81_DASHBOARD" \
    --db "$CANDIDATE_DB" \
    --reports-dir "$HOME/krx-stock-persist/reports/paper_trading" \
    --ops-ledger "$HOME/krx-stock-persist/reports/paper_trading/krx_daily_ops_ledger.csv" \
    --exp80a-forward "$EXP80A_FORWARD" \
    --exp80a-snapshot "$EXP80A_SNAPSHOT" \
    --exp80c-assignments "$EXP80C_ASSIGNMENTS" \
    --exp80d-entry-paths "$EXP80D_ENTRY_PATHS" \
    --exp80d-rule-summary "$EXP80D_RULE_SUMMARY" \
    --out-dir "$EXP81_DIR" \
    --candidate-n 20; then
    echo "WARN: Exp81 candidate shadow dashboard failed; continuing daily ops success flow"
  fi
else
  echo "WARN: Exp81 candidate shadow inputs unavailable; skipping Exp81 refresh"
fi
if [ -f "$EXP82_MONITOR" ] && [ -n "$EXP80A_FORWARD" ] && [ -n "$EXP80A_SNAPSHOT" ] && [ -n "$EXP80D_ENTRY_PATHS" ] && [ -n "$EXP80D_RULE_SUMMARY" ]; then
  set_stage "candidate_shadow_exp82_report_only"
  if "$PYTHON_BIN" "$EXP82_MONITOR" \
    --db "$HOME/krx-stock-persist/data/kospi_495_rolling_3y.db" \
    --reports-dir "$HOME/krx-stock-persist/reports/paper_trading" \
    --ops-ledger "$HOME/krx-stock-persist/reports/paper_trading/krx_daily_ops_ledger.csv" \
    --exp81-latest "$EXP81_DIR/exp81_candidate_shadow_latest.csv" \
    --exp81-metadata "$EXP81_DIR/exp81_metadata_latest.json" \
    --exp80a-forward "$EXP80A_FORWARD" \
    --exp80a-snapshot "$EXP80A_SNAPSHOT" \
    --exp80d-entry-paths "$EXP80D_ENTRY_PATHS" \
    --exp80d-rule-summary "$EXP80D_RULE_SUMMARY" \
    --out-dir "$EXP82_DIR" \
    --append-ledger; then
    EXP82_REFRESHED=1
    EXP82_OUTPUT_DATE="$(exp82_as_of_date || true)"
    if [ "$EXP82_OUTPUT_DATE" = "$CANDIDATE_EXPECTED_DATE" ]; then
      EXP82_FRESH=1
      echo "Exp82 candidate shadow output fresh: as_of_date=$EXP82_OUTPUT_DATE"
    else
      echo "WARN: Exp82 candidate shadow output stale; expected=$CANDIDATE_EXPECTED_DATE actual=${EXP82_OUTPUT_DATE:-missing}"
    fi
  else
    echo "WARN: Exp82 candidate shadow monitor failed; continuing daily ops success flow"
  fi
else
  echo "WARN: Exp82 candidate shadow monitor or inputs unavailable; skipping Exp82 refresh"
fi
if [ "$EXP82_REFRESHED" = "1" ] && [ "$EXP82_FRESH" = "1" ] && [ -f "$EXP83_SUMMARY" ]; then
  set_stage "candidate_shadow_exp83_report_only"
  if "$PYTHON_BIN" "$EXP83_SUMMARY" \
    --exp82-latest "$EXP82_DIR/exp82_shadow_label_latest.csv" \
    --exp82-performance "$EXP82_DIR/exp82_label_performance_summary_latest.csv" \
    --exp82-metadata "$EXP82_DIR/exp82_metadata_latest.json" \
    --paper-reports-dir "$HOME/krx-stock-persist/reports/paper_trading" \
    --ops-ledger "$HOME/krx-stock-persist/reports/paper_trading/krx_daily_ops_ledger.csv" \
    --out-dir "$EXP83_DIR" \
    --hermes-dir "$HOME/.hermes/krx"; then
    EXP83_OUTPUT_DATE="$(exp83_as_of_date || true)"
    if [ "$EXP83_OUTPUT_DATE" = "$CANDIDATE_EXPECTED_DATE" ]; then
      EXP83_FRESH=1
      echo "Exp83 candidate shadow summary fresh: as_of_date=$EXP83_OUTPUT_DATE"
    else
      echo "WARN: Exp83 candidate shadow summary stale; expected=$CANDIDATE_EXPECTED_DATE actual=${EXP83_OUTPUT_DATE:-missing}"
    fi
  else
    echo "WARN: Exp83 candidate shadow daily summary failed; continuing daily ops success flow"
  fi
else
  echo "WARN: Exp83 skipped; fresh Exp82 output required before candidate shadow summary"
fi
if [ "$EXP83_FRESH" = "1" ] && [ -f "$HOME/.hermes/krx/latest_candidate_shadow_summary.md" ]; then
  ls -lh "$HOME/.hermes/krx/latest_candidate_shadow_summary.md"
fi
if [ "$EXP83_FRESH" = "1" ] && [ -f "$CANDIDATE_TELEGRAM" ]; then
  set_stage "candidate_shadow_telegram_report_only"
  if ! bash "$CANDIDATE_TELEGRAM"; then
    echo "WARN: candidate shadow Telegram summary failed; continuing daily ops success flow"
  fi
else
  echo "WARN: candidate shadow Telegram skipped; fresh Exp83 summary required"
fi
