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


def parse_chart(payload: dict) -> list[dict]:
    """
    Turn a Yahoo v8 chart JSON payload into [{date, close, adj_close}, ...].
    Rows with a null close (non-trading gaps) are skipped. When adjclose is
    absent for a row, close is used as the adjusted value.
    """
    results = (payload.get("chart") or {}).get("result") or []
    if not results:
        return []
    res = results[0]
    timestamps = res.get("timestamp") or []
    indicators = res.get("indicators") or {}
    quote_block = (indicators.get("quote") or [{}])[0]
    adj_block = (indicators.get("adjclose") or [{}])[0]
    closes = quote_block.get("close") or []
    adjs = adj_block.get("adjclose") or []
    rows: list[dict] = []
    for i, ts in enumerate(timestamps):
        close = closes[i] if i < len(closes) else None
        if close is None:
            continue
        adj = adjs[i] if i < len(adjs) and adjs[i] is not None else close
        d = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        rows.append({"date": d, "close": close, "adj_close": adj})
    return rows


def _chart_url(symbol: str, start: str, end: str) -> str:
    p1 = int(datetime.strptime(start, "%Y-%m-%d")
             .replace(tzinfo=timezone.utc).timestamp())
    # +1 day so the end date itself is inclusive
    p2 = int((datetime.strptime(end, "%Y-%m-%d")
              .replace(tzinfo=timezone.utc) + timedelta(days=1)).timestamp())
    return (f"{_CHART_BASE}{quote(symbol)}"
            f"?period1={p1}&period2={p2}&interval=1d&events=div%2Csplit")


def _http_get(url: str) -> requests.Response:
    """GET with simple exponential backoff on 429."""
    resp = None
    for attempt in range(_MAX_RETRIES + 1):
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        if resp.status_code == 429:
            wait = 5 * (2 ** attempt)
            print(f"    [429] Yahoo rate limit — waiting {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


def fetch_prices(symbol: str, start: str, end: str) -> list[dict]:
    """Fetch + parse adjusted daily prices for one symbol over [start, end]."""
    resp = _http_get(_chart_url(symbol, start, end))
    return parse_chart(resp.json())
