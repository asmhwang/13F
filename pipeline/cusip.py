"""
CUSIP → ticker/name/exchange resolver via the OpenFIGI API.

Free tier  (no key): 25 items/request, 10 req/min
With API key       : 250 items/request, 25 req/min

Set OPENFIGI_API_KEY in your environment to use the higher limits:
    export OPENFIGI_API_KEY=your_key_here

Run directly to resolve all unmapped CUSIPs in the database:
    python3 -m pipeline.cusip
"""

import os
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

from pipeline.database import DB_PATH, get_connection, upsert_security

_OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

# US equity exchange codes in OpenFIGI
_US_EQUITY_EXCHANGES = {"US", "UN", "UW", "UA", "UR", "UF"}

# Preferred security types (in priority order)
_EQUITY_TYPES = {"Common Stock", "ETP", "ADR", "NY Reg Shrs", "Depositary Receipt"}


def _api_key() -> str | None:
    return os.environ.get("OPENFIGI_API_KEY") or None


def _batch_size() -> int:
    return 100 if _api_key() else 10


def _rate_sleep() -> float:
    # Stay comfortably under the per-minute cap
    return 2.5 if _api_key() else 6.5


def _headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    key = _api_key()
    if key:
        h["X-OPENFIGI-APIKEY"] = key
    return h


def _best_result(data: list[dict[str, Any]]) -> dict[str, Any] | None:
    """
    Pick the most useful instrument from a list of OpenFIGI matches.
    Prefers US-listed common stock; falls back to first equity result.
    """
    if not data:
        return None

    # 1. US common stock
    for item in data:
        if (item.get("exchCode") in _US_EQUITY_EXCHANGES
                and item.get("securityType") in _EQUITY_TYPES):
            return item

    # 2. Any equity
    for item in data:
        if item.get("marketSector") == "Equity":
            return item

    # 3. Anything
    return data[0]


def resolve_cusips(cusips: list[str]) -> dict[str, dict[str, str | None]]:
    """
    Resolve a list of CUSIPs via OpenFIGI.

    Returns a dict mapping cusip → {ticker, name, exchange} for each
    CUSIP that was successfully matched. Unmatched CUSIPs are omitted.
    """
    batch = _batch_size()
    sleep = _rate_sleep()
    results: dict[str, dict[str, str | None]] = {}

    for i in range(0, len(cusips), batch):
        chunk = cusips[i : i + batch]
        payload = [{"idType": "ID_CUSIP", "idValue": c} for c in chunk]

        try:
            resp = requests.post(
                _OPENFIGI_URL,
                json=payload,
                headers=_headers(),
                timeout=30,
            )
            resp.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                print("  [429] OpenFIGI rate limit — sleeping 60s")
                time.sleep(60)
                # Retry once
                resp = requests.post(
                    _OPENFIGI_URL,
                    json=payload,
                    headers=_headers(),
                    timeout=30,
                )
                resp.raise_for_status()
            else:
                raise

        for cusip, entry in zip(chunk, resp.json()):
            data = entry.get("data")
            if not data:
                continue  # "No identifier found." or error
            best = _best_result(data)
            if best:
                results[cusip] = {
                    "ticker":   best.get("ticker"),
                    "name":     best.get("name"),
                    "exchange": best.get("exchCode"),
                }

        if i + batch < len(cusips):
            time.sleep(sleep)

    return results


_POLYGON_URL = "https://api.polygon.io/v3/reference/tickers"
_POLYGON_RATE_SLEEP = 13.0   # free tier: 5 req/min → 12s apart; 13s to be safe


def _polygon_api_key() -> str | None:
    return os.environ.get("POLYGON_API_KEY") or None


def resolve_cusips_polygon(cusips: list[str]) -> dict[str, dict[str, str | None]]:
    """
    Resolve CUSIPs via the Polygon.io reference tickers endpoint.
    One request per CUSIP — rate-limited to 5/min on the free tier.
    Returns a dict of cusip → {ticker, name, exchange} for matches.
    """
    key = _polygon_api_key()
    if not key:
        return {}

    results: dict[str, dict[str, str | None]] = {}
    for i, cusip in enumerate(cusips):
        try:
            resp = requests.get(
                _POLYGON_URL,
                params={"cusip": cusip, "apiKey": key},
                timeout=15,
            )
            if resp.status_code == 429:
                print("  [429] Polygon rate limit — sleeping 60s")
                time.sleep(60)
                resp = requests.get(
                    _POLYGON_URL,
                    params={"cusip": cusip, "apiKey": key},
                    timeout=15,
                )
            resp.raise_for_status()
            data = resp.json().get("results", [])
            if data:
                best = data[0]
                results[cusip] = {
                    "ticker":   best.get("ticker"),
                    "name":     best.get("name"),
                    "exchange": best.get("primary_exchange"),
                }
        except Exception as exc:
            print(f"  [ERROR] Polygon {cusip}: {exc}")

        if i < len(cusips) - 1:
            time.sleep(_POLYGON_RATE_SLEEP)

    return results


def update_securities(db_path: Path = DB_PATH, quiet: bool = False) -> int:
    """
    Fetch all CUSIPs from the holdings table that don't have a ticker yet,
    resolve them via OpenFIGI then Polygon.io, and store results in the
    securities table.

    Returns the number of CUSIPs newly mapped.
    """
    conn = get_connection(db_path)

    # Only fetch CUSIPs that are missing a ticker
    rows = conn.execute(
        """
        SELECT DISTINCT h.cusip
        FROM holdings h
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE s.ticker IS NULL
        ORDER BY h.cusip
        """
    ).fetchall()
    cusips = [r[0] for r in rows]

    if not cusips:
        if not quiet:
            print("All CUSIPs already mapped.")
        return 0

    key_status = "with API key" if _api_key() else "no API key — limited rate"
    print(f"Resolving {len(cusips)} CUSIPs via OpenFIGI ({key_status})")
    batch = _batch_size()
    total_batches = (len(cusips) + batch - 1) // batch
    eta_min = total_batches * _rate_sleep() / 60
    print(f"  {total_batches} batches of {batch}  —  ETA ~{eta_min:.0f} min")

    if not _api_key():
        print("  Tip: set OPENFIGI_API_KEY to go ~10x faster")

    mapped = 0
    for i in range(0, len(cusips), batch):
        chunk = cusips[i : i + batch]
        batch_num = i // batch + 1

        payload = [{"idType": "ID_CUSIP", "idValue": c} for c in chunk]
        try:
            resp = requests.post(
                _OPENFIGI_URL,
                json=payload,
                headers=_headers(),
                timeout=30,
            )
            resp.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 429:
                print("  [429] rate limit — sleeping 60s")
                time.sleep(60)
                resp = requests.post(
                    _OPENFIGI_URL,
                    json=payload,
                    headers=_headers(),
                    timeout=30,
                )
                resp.raise_for_status()
            else:
                print(f"  [ERROR] batch {batch_num}: {exc}")
                if i + batch < len(cusips):
                    time.sleep(_rate_sleep())
                continue

        batch_mapped = 0
        with get_connection(db_path) as wconn:
            for cusip, entry in zip(chunk, resp.json()):
                data = entry.get("data")
                if not data:
                    # Store a sentinel so we don't retry forever
                    upsert_security(wconn, cusip, None, None, None)
                    continue
                best = _best_result(data)
                if best:
                    upsert_security(
                        wconn,
                        cusip,
                        ticker=best.get("ticker"),
                        name=best.get("name"),
                        exchange=best.get("exchCode"),
                    )
                    batch_mapped += 1

        mapped += batch_mapped
        if not quiet:
            pct = (i + len(chunk)) / len(cusips) * 100
            print(f"  batch {batch_num}/{total_batches}  ({pct:.0f}%)  +{batch_mapped} mapped  total={mapped}")

        if i + batch < len(cusips):
            time.sleep(_rate_sleep())

    print(f"Done. {mapped}/{len(cusips)} CUSIPs resolved to tickers.")

    # ---- Polygon second pass ------------------------------------------------
    if not _polygon_api_key():
        print("Tip: set POLYGON_API_KEY for a second-pass resolver via Polygon.io")
        return mapped

    # Fetch still-unresolved CUSIPs, ordered by total holding value (most
    # impactful first) and limited to the last 2 years of filings.
    unresolved = conn.execute(
        """
        SELECT h.cusip, SUM(h.value_thousands) AS total_value
        FROM holdings h
        JOIN filings f ON f.id = h.filing_id
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE s.ticker IS NULL
          AND f.period_of_report >= date('now', '-2 years')
        GROUP BY h.cusip
        ORDER BY total_value DESC
        """
    ).fetchall()
    poly_cusips = [r[0] for r in unresolved]

    if not poly_cusips:
        print("Polygon: nothing left to resolve.")
        return mapped

    eta_min = len(poly_cusips) * _POLYGON_RATE_SLEEP / 60
    print(f"\nPolygon second pass: {len(poly_cusips)} unresolved CUSIPs  —  ETA ~{eta_min:.0f} min")

    poly_mapped = 0
    for i, cusip in enumerate(poly_cusips):
        try:
            resp = requests.get(
                _POLYGON_URL,
                params={"cusip": cusip, "apiKey": _polygon_api_key()},
                timeout=15,
            )
            if resp.status_code == 429:
                print("  [429] Polygon rate limit — sleeping 60s")
                time.sleep(60)
                resp = requests.get(
                    _POLYGON_URL,
                    params={"cusip": cusip, "apiKey": _polygon_api_key()},
                    timeout=15,
                )
            resp.raise_for_status()
            data = resp.json().get("results", [])
            with get_connection(db_path) as wconn:
                if data:
                    best = data[0]
                    upsert_security(
                        wconn,
                        cusip,
                        ticker=best.get("ticker"),
                        name=best.get("name"),
                        exchange=best.get("primary_exchange"),
                    )
                    poly_mapped += 1
                    if not quiet:
                        print(f"  [{i+1}/{len(poly_cusips)}] {cusip} → {best.get('ticker')}")
                # No result: leave existing NULL sentinel, skip re-storing
        except Exception as exc:
            print(f"  [ERROR] Polygon {cusip}: {exc}")

        if i < len(poly_cusips) - 1:
            time.sleep(_POLYGON_RATE_SLEEP)

    print(f"Polygon done. {poly_mapped}/{len(poly_cusips)} additional CUSIPs resolved.")
    return mapped + poly_mapped


if __name__ == "__main__":
    import argparse, sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    ap = argparse.ArgumentParser(description="Resolve CUSIPs to tickers via OpenFIGI + Polygon")
    ap.add_argument("--db", default=str(DB_PATH), help="Path to SQLite database")
    args = ap.parse_args()

    update_securities(Path(args.db))
