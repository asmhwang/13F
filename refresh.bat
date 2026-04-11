@echo off
REM Refresh 13F data: ingest latest filings + resolve new CUSIPs.
REM Safe to run at any frequency -- already-ingested filings and already-resolved
REM CUSIPs are skipped automatically.

set REPO_DIR=%~dp0
set LOG=%REPO_DIR%data\refresh.log

if not exist "%REPO_DIR%data" mkdir "%REPO_DIR%data"

echo === %DATE% %TIME% === >> "%LOG%"

echo [1/2] Ingesting latest filings...
echo [1/2] Ingesting latest filings... >> "%LOG%"
python -m pipeline.ingest --seed --latest-only 2>&1 | tee -a "%LOG%"

echo [2/2] Resolving new CUSIPs...
echo [2/2] Resolving new CUSIPs... >> "%LOG%"
python -m pipeline.cusip 2>&1 | tee -a "%LOG%"

echo Done.
echo Done. >> "%LOG%"
