"""
Current-quarter fundamentals ingest from Finnhub.

For every stock held in the most recent quarter by a ranked fund, fetch sector,
market cap, shares, P/E and gross margin and upsert into the fundamentals and
sectors tables. Requires FINNHUB_API_KEY in .env.

Run directly:
    python3 -m pipeline.fundamentals            # ingest universe
    python3 -m pipeline.fundamentals --limit 5  # first 5 tickers (smoke)
"""

import os
import sqlite3
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from pipeline.database import DB_PATH, get_connection
from pipeline.scoring import adapter

_FINNHUB_BASE = "https://finnhub.io/api/v1"
_RATE_SLEEP = 1.1            # free tier: 60 req/min -> ~1s apart
_MAX_RETRIES = 3


def _api_key() -> str | None:
    return os.environ.get("FINNHUB_API_KEY") or None


def _finnhub_get(path: str, params: dict) -> dict:
    """GET {base}{path}?... with the API token, retrying on 429."""
    p = dict(params)
    p["token"] = _api_key()
    url = f"{_FINNHUB_BASE}{path}"
    resp = None
    for attempt in range(_MAX_RETRIES + 1):
        resp = requests.get(url, params=p, timeout=20)
        if resp.status_code == 429:
            if attempt < _MAX_RETRIES:
                time.sleep(5 * (2 ** attempt))
                continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return {}


def _millions(v) -> float | None:
    return v * 1_000_000 if isinstance(v, (int, float)) and v else None


def fetch_profile(ticker: str) -> dict:
    """{sector, market_cap, shares_out} from Finnhub /stock/profile2."""
    data = _finnhub_get("/stock/profile2", {"symbol": ticker})
    return {
        "sector": data.get("finnhubIndustry") or None,
        "market_cap": _millions(data.get("marketCapitalization")),
        "shares_out": _millions(data.get("shareOutstanding")),
    }


def fetch_metrics(ticker: str) -> dict:
    """
    {pe_ratio, pe_available, gross_margin_pct} from Finnhub /stock/metric.
    A non-positive or missing P/E is treated as unavailable: pe_available=0 and
    pe_ratio=0 (so it contributes nothing in the downstream regression).
    """
    metric = _finnhub_get("/stock/metric", {"symbol": ticker, "metric": "all"}).get("metric", {})
    pe = metric.get("peTTM")
    if isinstance(pe, (int, float)) and pe > 0:
        pe_ratio, pe_available = float(pe), 1
    else:
        pe_ratio, pe_available = 0.0, 0
    gm = metric.get("grossMarginTTM")
    gross_margin_pct = float(gm) if isinstance(gm, (int, float)) else None
    return {"pe_ratio": pe_ratio, "pe_available": pe_available,
            "gross_margin_pct": gross_margin_pct}


def universe_tickers(conn: sqlite3.Connection) -> list[str]:
    """
    Distinct equity tickers held in the most recent quarter by any fund present
    in fund_rankings. Options and unresolved/letterless tickers are excluded.
    """
    cq = adapter.current_quarter_date(conn)
    if cq is None:
        return []
    rows = conn.execute(
        """
        WITH ranked AS (SELECT fund_id FROM fund_rankings),
        latest AS (
            SELECT f.id, f.cik FROM filings f
            JOIN ranked r ON r.fund_id = f.cik
            WHERE f.period_of_report = :cq
              AND f.id = (SELECT f2.id FROM filings f2
                          WHERE f2.cik = f.cik AND f2.period_of_report = :cq
                          ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1)
        )
        SELECT DISTINCT s.ticker
        FROM holdings h
        JOIN latest l ON l.id = h.filing_id
        JOIN securities s ON s.cusip = h.cusip
        WHERE s.ticker IS NOT NULL AND s.ticker <> ''
          AND s.ticker NOT GLOB '*[0-9]*'
          AND (h.put_call IS NULL OR h.put_call = '')
          AND h.value_thousands > 0
        """,
        {"cq": cq},
    ).fetchall()
    return [r[0] for r in rows]
