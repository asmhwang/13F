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
