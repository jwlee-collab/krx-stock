#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${HOME}/Projects/krx-stock"
PYTHON_BIN="${REPO_DIR}/.venv/bin/python"
REPORTS_DIR="${HOME}/krx-stock-persist/reports/paper_trading"
LOGS_DIR="${HOME}/krx-stock-persist/logs"
DB_PATH="${HOME}/krx-stock-persist/data/kospi_495_rolling_3y.db"

mkdir -p "${LOGS_DIR}"

TIMESTAMP="$(date +"%Y-%m-%d_%H%M%S")"
LOG_FILE="${LOGS_DIR}/mac_daily_paper_ops_${TIMESTAMP}.log"

on_error() {
  local exit_code=$?
  echo "FAIL: Mac daily paper report ops failed."
  echo "Last log: ${LOG_FILE}"
  exit 1
}
trap on_error ERR

exec > >(tee -a "${LOG_FILE}") 2>&1

if [[ ! -d "${REPO_DIR}" ]]; then
  echo "Missing repo dir: ${REPO_DIR}"
  exit 1
fi

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "Missing python executable: ${PYTHON_BIN}"
  exit 1
fi

if [[ ! -f "${DB_PATH}" ]]; then
  echo "Missing DB file: ${DB_PATH}"
  exit 1
fi

cd "${REPO_DIR}"

if ! git pull --ff-only; then
  echo "WARN: git pull failed. Continuing with local code."
fi

"${PYTHON_BIN}" scripts/update_mac_market_db.py \
  --db "${DB_PATH}" \
  --logs-dir "${LOGS_DIR}"

"${PYTHON_BIN}" scripts/run_mac_paper_report.py \
  --db "${DB_PATH}" \
  --reports-dir "${REPORTS_DIR}" \
  --logs-dir "${LOGS_DIR}"

"${PYTHON_BIN}" scripts/validate_mac_paper_report.py \
  --db "${DB_PATH}" \
  --reports-dir "${REPORTS_DIR}" \
  --logs-dir "${LOGS_DIR}"

LATEST_REPORT="$(find "${REPORTS_DIR}" -type f -name '*_paper_report.html' -print0 | xargs -0 ls -1t | head -n 1 || true)"

if [[ -z "${LATEST_REPORT}" || ! -f "${LATEST_REPORT}" ]]; then
  echo "No report found in ${REPORTS_DIR}"
  exit 1
fi

open "${LATEST_REPORT}"

echo "PASS: Mac daily paper report ops completed."
echo "Report: ${LATEST_REPORT}"
