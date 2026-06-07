"""
Daily price + S&P 500 total-return benchmark ingest for the ranking pipeline.

Source: Yahoo Finance v8 chart endpoint (no API key). Adjusted close included.
Benchmark symbol: ^SP500TR (S&P 500 Total Return index).

Scope: only tickers held by tracked funds (resolved in `securities`), over the
window each ticker is actually needed: [first holding quarter, last holding
quarter + 3 years], capped at today. Incremental — already-covered tickers are
skipped via price_fetch_log.

Run directly:
    python3 -m pipeline.prices              # benchmark + held tickers
    python3 -m pipeline.prices --coverage   # print coverage report only
    python3 -m pipeline.prices --limit 5    # fetch only 5 tickers (smoke test)
"""

import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import requests

from pipeline.database import DB_PATH, get_connection

_SCHEMA_PATH = Path(__file__).parent / "scoring" / "schema.sql"
_CHART_BASE = "https://query1.finance.yahoo.com/v8/finance/chart/"
_HEADERS = {"User-Agent": "Mozilla/5.0 (13F Research)"}
_RATE_SLEEP = 0.5          # polite gap between Yahoo requests
_MAX_RETRIES = 3
_BENCHMARK_SYMBOL = "^SP500TR"


def init_schema(conn: sqlite3.Connection | None = None, db_path: Path = DB_PATH) -> None:
    """Create the price/benchmark tables if they don't exist (idempotent)."""
    c = conn or get_connection(db_path)
    c.executescript(_SCHEMA_PATH.read_text())
    c.commit()
