#!/bin/bash
# Refresh 13F data: ingest filings, resolve CUSIPs, fetch prices + fundamentals,
# then recompute fund + stock rankings.
# Safe to run at any frequency — ingest/CUSIP/price steps are incremental and the
# scoring pipelines are idempotent (truncate-rebuild).

set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON=/opt/homebrew/bin/python3
LOG="$REPO_DIR/data/refresh.log"

mkdir -p "$REPO_DIR/data"

echo "=== $(date) ===" >> "$LOG"
echo "[1/6] Ingesting latest filings..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.ingest --seed --all-tracked --latest-only 2>&1 | tee -a "$LOG"

echo "[2/6] Resolving new CUSIPs..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.cusip 2>&1 | tee -a "$LOG"

echo "[2b] Resolving historical CUSIPs (offline passes)..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.cusip_local 2>&1 | tee -a "$LOG"

echo "[3/6] Fetching prices + benchmark..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.prices 2>&1 | tee -a "$LOG"

echo "[4/6] Fetching current-quarter fundamentals..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.fundamentals 2>&1 | tee -a "$LOG"

echo "[5/8] Scoring fund rankings..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.scoring.fund_pipeline 2>&1 | tee -a "$LOG"

echo "[6/8] Scoring stock rankings..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.scoring.stock_pipeline 2>&1 | tee -a "$LOG"

echo "[7/8] Scoring fund rankings v2..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.scoring.fund_pipeline_v2 2>&1 | tee -a "$LOG"

echo "[8/8] Scoring stock rankings v2..." | tee -a "$LOG"
cd "$REPO_DIR" && $PYTHON -m pipeline.scoring.stock_pipeline_v2 2>&1 | tee -a "$LOG"

echo "Done." | tee -a "$LOG"
