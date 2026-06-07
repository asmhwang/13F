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


def _plus_three_years(d: str) -> str:
    """Add 3 years to an ISO date string, clamping Feb-29 to Feb-28."""
    y, m, day = (int(x) for x in d.split("-"))
    try:
        return date(y + 3, m, day).isoformat()
    except ValueError:                       # Feb 29 -> Feb 28
        return date(y + 3, m, day - 1).isoformat()


def held_ticker_windows(conn: sqlite3.Connection) -> list[dict]:
    """
    For each equity ticker held by a tracked fund, the date window prices are
    needed: [first holding quarter, min(last holding quarter + 3yr, today)].
    Option-only positions (put_call set) and unresolved CUSIPs are excluded.
    """
    rows = conn.execute(
        """
        SELECT s.ticker                AS ticker,
               MIN(f.period_of_report) AS first_q,
               MAX(f.period_of_report) AS last_q
        FROM holdings h
        JOIN filings f    ON f.id = h.filing_id
        JOIN securities s ON s.cusip = h.cusip
        WHERE s.ticker IS NOT NULL AND s.ticker <> ''
          AND (h.put_call IS NULL OR h.put_call = '')
          AND h.value_thousands > 0
        GROUP BY s.ticker
        """
    ).fetchall()
    today = date.today().isoformat()
    out: list[dict] = []
    for r in rows:
        end = min(_plus_three_years(r["last_q"]), today)
        out.append({"ticker": r["ticker"], "start": r["first_q"], "end": end})
    return out


def _already_covered(conn: sqlite3.Connection, ticker: str, start: str, end: str) -> bool:
    row = conn.execute(
        "SELECT first_date, last_date, status FROM price_fetch_log WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    if not row:
        return False
    if row["status"] == "no_data":
        return True                          # don't retry dead tickers
    return (row["first_date"] is not None
            and row["last_date"] is not None
            and row["first_date"] <= start
            and row["last_date"] >= end)


def _log_fetch(conn: sqlite3.Connection, ticker: str,
               first_date: str | None, last_date: str | None, status: str) -> None:
    conn.execute(
        """
        INSERT INTO price_fetch_log (ticker, first_date, last_date, status, fetched_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(ticker) DO UPDATE SET
            first_date = excluded.first_date,
            last_date  = excluded.last_date,
            status     = excluded.status,
            fetched_at = CURRENT_TIMESTAMP
        """,
        (ticker, first_date, last_date, status),
    )
    conn.commit()


def ingest_prices(db_path: Path = DB_PATH, force: bool = False,
                  limit: int | None = None) -> dict:
    """
    Fetch + store prices for every held equity ticker over its needed window.
    Incremental: tickers already covered (or marked no_data) are skipped unless
    force=True. Returns {fetched, skipped, failed, total}.
    """
    conn = get_connection(db_path)
    init_schema(conn, db_path)
    windows = held_ticker_windows(conn)
    if limit:
        windows = windows[:limit]
    fetched = skipped = failed = 0
    for w in windows:
        t, start, end = w["ticker"], w["start"], w["end"]
        if not force and _already_covered(conn, t, start, end):
            skipped += 1
            continue
        try:
            rows = fetch_prices(t, start, end)
            if rows:
                store_prices(conn, t, rows)
                _log_fetch(conn, t, rows[0]["date"], rows[-1]["date"], "ok")
                fetched += 1
            else:
                _log_fetch(conn, t, None, None, "no_data")
            time.sleep(_RATE_SLEEP)
        except Exception as exc:                # noqa: BLE001 — log and continue
            print(f"  [ERROR] {t}: {exc}")
            _log_fetch(conn, t, None, None, "error")
            failed += 1
    return {"fetched": fetched, "skipped": skipped, "failed": failed, "total": len(windows)}


def store_prices(conn: sqlite3.Connection, ticker: str, rows: list[dict]) -> int:
    """Upsert price rows for one ticker. Returns the number of rows written."""
    conn.executemany(
        """
        INSERT INTO prices (ticker, date, close, adj_close)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(ticker, date) DO UPDATE SET
            close     = excluded.close,
            adj_close = excluded.adj_close
        """,
        [(ticker, r["date"], r["close"], r["adj_close"]) for r in rows],
    )
    conn.commit()
    return len(rows)
