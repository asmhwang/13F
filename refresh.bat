@echo off
REM Refresh 13F data: ingest filings, resolve CUSIPs, fetch prices + fundamentals,
REM then recompute fund + stock rankings.
REM Safe to run at any frequency -- ingest/CUSIP/price steps are incremental and the
REM scoring pipelines are idempotent (truncate-rebuild).

set REPO_DIR=%~dp0
set LOG=%REPO_DIR%data\refresh.log

if not exist "%REPO_DIR%data" mkdir "%REPO_DIR%data"

echo === %DATE% %TIME% === >> "%LOG%"

echo [1/6] Ingesting latest filings...
echo [1/6] Ingesting latest filings... >> "%LOG%"
python -m pipeline.ingest --seed --all-tracked --latest-only 2>&1 | tee -a "%LOG%"

echo [2/6] Resolving new CUSIPs...
echo [2/6] Resolving new CUSIPs... >> "%LOG%"
python -m pipeline.cusip 2>&1 | tee -a "%LOG%"

echo [2b] Resolving historical CUSIPs (offline passes)...
echo [2b] Resolving historical CUSIPs (offline passes)... >> "%LOG%"
python -m pipeline.cusip_local 2>&1 | tee -a "%LOG%"

echo [3/6] Fetching prices + benchmark...
echo [3/6] Fetching prices + benchmark... >> "%LOG%"
python -m pipeline.prices 2>&1 | tee -a "%LOG%"

echo [4/6] Fetching current-quarter fundamentals...
echo [4/6] Fetching current-quarter fundamentals... >> "%LOG%"
python -m pipeline.fundamentals 2>&1 | tee -a "%LOG%"

echo [5/8] Scoring fund rankings...
echo [5/8] Scoring fund rankings... >> "%LOG%"
python -m pipeline.scoring.fund_pipeline 2>&1 | tee -a "%LOG%"

echo [6/8] Scoring stock rankings...
echo [6/8] Scoring stock rankings... >> "%LOG%"
python -m pipeline.scoring.stock_pipeline 2>&1 | tee -a "%LOG%"

echo [7/8] Scoring fund rankings v2...
echo [7/8] Scoring fund rankings v2... >> "%LOG%"
python -m pipeline.scoring.fund_pipeline_v2 2>&1 | tee -a "%LOG%"

echo [8/8] Scoring stock rankings v2...
echo [8/8] Scoring stock rankings v2... >> "%LOG%"
python -m pipeline.scoring.stock_pipeline_v2 2>&1 | tee -a "%LOG%"

echo Done.
echo Done. >> "%LOG%"
