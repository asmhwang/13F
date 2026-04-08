#!/bin/bash
# Refresh 13F data: ingest latest filings + resolve new CUSIPs.
# Safe to run at any frequency — already-ingested filings and already-resolved
# CUSIPs are skipped automatically.

set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON=/opt/homebrew/bin/python3
LOG="$REPO_DIR/data/refresh.log"

mkdir -p "$REPO_DIR/data"

echo "=== $(date) ===" >> "$LOG"
echo "[1/2] Ingesting latest filings..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.ingest --seed --latest-only 2>&1 | tee -a "$LOG"

echo "[2/2] Resolving new CUSIPs..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.cusip 2>&1 | tee -a "$LOG"

echo "Done." | tee -a "$LOG"
