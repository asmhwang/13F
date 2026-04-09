"""
SEC EDGAR client for fetching 13F-HR filing metadata and documents.

EDGAR API references:
  - Submissions:  https://data.sec.gov/submissions/CIK{cik:010d}.json
  - Full-index:   https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx
  - EFTS search:  https://efts.sec.gov/LATEST/search-index?q=%2213F-HR%22&...
"""

import hashlib
import json
import re
import threading
import time
from pathlib import Path
from typing import Any

import requests

_HEADERS = {
    "User-Agent": "13F Research Pipeline research@example.com",
    "Accept-Encoding": "gzip, deflate",
}
_BASE      = "https://www.sec.gov"
_DATA_BASE = "https://data.sec.gov"
_EFTS_BASE = "https://efts.sec.gov"
_RATE_SLEEP  = 0.12   # SEC allows ~10 req/s; we stay safely under
_MAX_RETRIES = 4
_RETRY_BASE  = 10     # seconds; doubles each attempt: 10, 20, 40, 80

# Global rate limiter — serialises all HTTP requests across threads so the
# per-second cap is respected even during concurrent prefetching.
_rate_lock   = threading.Lock()
_last_req_at: float = 0.0


def _http_get(url: str, **kwargs) -> requests.Response:
    """
    Rate-limited GET with exponential backoff on 429.

    The lock is held for the sleep + request so only one in-flight request
    exists at a time, guaranteeing the SEC rate cap regardless of threading.
    On 429 the lock is released while waiting so other callers don't pile up.
    """
    global _last_req_at
    for attempt in range(_MAX_RETRIES + 1):
        with _rate_lock:
            gap = _RATE_SLEEP - (time.monotonic() - _last_req_at)
            if gap > 0:
                time.sleep(gap)
            _last_req_at = time.monotonic()
            resp = requests.get(url, headers=_HEADERS, timeout=30, **kwargs)

        if resp.status_code == 429:
            wait = _RETRY_BASE * (2 ** attempt)
            print(f"    [429] rate limited — waiting {wait}s (attempt {attempt + 1}/{_MAX_RETRIES})")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp

    resp.raise_for_status()  # re-raise after exhausting retries
    return resp  # unreachable

# ---------------------------------------------------------------------------
# Disk cache
# ---------------------------------------------------------------------------
# Filing documents and index files are immutable once published — cache forever.
# Submissions JSON changes when new filings arrive — cache for 1 hour.

_CACHE_DIR   = Path(__file__).parent.parent / "data" / "http_cache"
_TTL_FOREVER = 10 * 365 * 24 * 3600   # 10 years ≈ immutable
_TTL_SHORT   = 3600                    # 1 hour  for submissions JSON


def _cache_path(url: str) -> Path:
    key = hashlib.sha256(url.encode()).hexdigest()
    return _CACHE_DIR / key[:2] / (key + ".json")


def _cache_get(url: str, ttl: int) -> str | None:
    p = _cache_path(url)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        if ttl == _TTL_FOREVER or (time.time() - data["ts"] < ttl):
            return data["content"]
    except Exception:
        pass
    return None


def _cache_set(url: str, content: str) -> None:
    p = _cache_path(url)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"url": url, "ts": time.time(), "content": content}))


def _get_text(url: str, ttl: int = _TTL_FOREVER, **kwargs) -> str:
    """Fetch URL as text, reading from disk cache when available."""
    cached = _cache_get(url, ttl)
    if cached is not None:
        return cached
    resp = _http_get(url, **kwargs)
    _cache_set(url, resp.text)
    return resp.text


def _get_json(url: str, ttl: int = _TTL_FOREVER, **kwargs) -> Any:
    return json.loads(_get_text(url, ttl=ttl, **kwargs))


# ---------------------------------------------------------------------------
# Filer / submission lookup
# ---------------------------------------------------------------------------

def get_filer_submissions(cik: str) -> dict[str, Any]:
    """
    Return raw EDGAR submissions JSON for a given CIK.
    CIK is zero-padded to 10 digits automatically.
    """
    cik_padded = cik.zfill(10)
    url = f"{_DATA_BASE}/submissions/CIK{cik_padded}.json"
    return _get_json(url, ttl=_TTL_SHORT)


def get_13f_filings_for_filer(cik: str) -> list[dict[str, Any]]:
    """
    Return list of 13F-HR (and 13F-HR/A) filing metadata for a filer.

    Each item has: accession_number, period_of_report, filed_date, report_type.
    """
    data = get_filer_submissions(cik)
    recent = data.get("filings", {}).get("recent", {})

    forms        = recent.get("form", [])
    filed_dates  = recent.get("filingDate", [])
    periods      = recent.get("reportDate", [])
    accessions   = recent.get("accessionNumber", [])

    results = []
    for form, filed, period, acc in zip(forms, filed_dates, periods, accessions):
        if form in ("13F-HR", "13F-HR/A"):
            results.append({
                "accession_number": acc,
                "period_of_report": period,
                "filed_date":       filed,
                "report_type":      form,
            })

    # EDGAR only returns the most recent ~40 in `recent`; fetch older pages too
    for page_data in _fetch_older_pages(data, cik):
        for form, filed, period, acc in zip(
            page_data.get("form", []),
            page_data.get("filingDate", []),
            page_data.get("reportDate", []),
            page_data.get("accessionNumber", []),
        ):
            if form in ("13F-HR", "13F-HR/A"):
                results.append({
                    "accession_number": acc,
                    "period_of_report": period,
                    "filed_date":       filed,
                    "report_type":      form,
                })

    return results


def _fetch_older_pages(data: dict, cik: str) -> list[dict]:
    """Fetch additional filing pages referenced in `files` array (if any)."""
    pages = []
    for file_entry in data.get("filings", {}).get("files", []):
        url = f"{_DATA_BASE}/submissions/{file_entry['name']}"
        try:
            pages.append(_get_json(url, ttl=_TTL_SHORT))
        except Exception:
            pass
    return pages


# ---------------------------------------------------------------------------
# Filing document index
# ---------------------------------------------------------------------------

def get_filing_index(cik: str, accession_number: str) -> dict[str, Any]:
    """Return the filing index JSON for a specific accession number."""
    acc_clean = accession_number.replace("-", "")
    url = f"{_BASE}/Archives/edgar/data/{cik.lstrip('0')}/{acc_clean}/index.json"
    return _get_json(url)  # immutable — cache forever


def get_information_table_url(cik: str, accession_number: str) -> str | None:
    """
    Find the URL of the 13F information table document within a filing.

    Search order:
      1. Any XML whose name contains "informationtable"   (post-2013 standard)
      2. Any other .xml file that isn't the primary doc   (edge cases)
      3. The largest .txt file that isn't the index       (pre-2013 SGML)

    Returns None if nothing usable is found.
    """
    acc_clean = accession_number.replace("-", "")
    base_url  = f"{_BASE}/Archives/edgar/data/{cik.lstrip('0')}/{acc_clean}"

    try:
        index = get_filing_index(cik, accession_number)
    except Exception:
        return None

    items = index.get("directory", {}).get("item", [])

    # 1. Named information table XML
    for item in items:
        name: str = item.get("name", "")
        if "informationtable" in name.lower() and name.endswith(".xml"):
            return f"{base_url}/{name}"

    # 2. Any other non-primary XML
    for item in items:
        name = item.get("name", "")
        if name.endswith(".xml") and "primary" not in name.lower():
            return f"{base_url}/{name}"

    # 3. Legacy: largest .txt that isn't the index/header bundle
    txt_items = [
        item for item in items
        if item.get("name", "").endswith(".txt")
        and "index" not in item.get("name", "").lower()
        and item.get("name", "") != f"{acc_clean}.txt"
    ]
    if txt_items:
        # Pick by reported file size (stored as a string like "20613")
        def _size(item: dict) -> int:
            try:
                return int(item.get("size", "0"))
            except ValueError:
                return 0
        best = max(txt_items, key=_size)
        return f"{base_url}/{best['name']}"

    return None


def fetch_document(url: str) -> str:
    """Fetch raw text/XML content of an EDGAR document (cached forever)."""
    return _get_text(url)  # immutable — cache forever


def prefetch_filing_indexes(
    cik: str,
    filings: list[dict[str, Any]],
    max_workers: int = 8,
) -> None:
    """
    Concurrently warm the cache for all filing index.json files.
    Hits the network only for accession numbers not already cached;
    skips the SEC rate-limit sleep for cache hits, so this is fast
    even for large backlogs.
    """
    import threading

    cik_bare = cik.lstrip("0") or "0"
    uncached: list[str] = []
    for f in filings:
        acc_clean = f["accession_number"].replace("-", "")
        url = f"{_BASE}/Archives/edgar/data/{cik_bare}/{acc_clean}/index.json"
        if _cache_get(url, _TTL_FOREVER) is None:
            uncached.append(url)

    if not uncached:
        return

    print(f"    Prefetching {len(uncached)} filing indexes concurrently...")
    errors: list[str] = []

    def _fetch(url: str) -> None:
        try:
            _get_text(url)
        except Exception as exc:
            errors.append(f"{url}: {exc}")

    # The global _rate_lock in _http_get already serialises requests; threads
    # here just avoid blocking the main thread during the total wait time.
    threads = [threading.Thread(target=_fetch, args=(u,), daemon=True) for u in uncached]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        print(f"    {len(errors)} prefetch error(s) (will retry individually)")


# ---------------------------------------------------------------------------
# Bulk search (top filers by AUM)
# ---------------------------------------------------------------------------

def search_filers_by_name(query: str, max_results: int = 20) -> list[dict[str, Any]]:
    """
    Search EDGAR for 13F-HR filers by institution name.

    Returns list of {cik, name} dicts. Returns [] if query is under 3 chars
    or on network error.
    """
    if len(query.strip()) < 3:
        return []

    params = {
        "q": f'"{query}"',
        "forms": "13F-HR",
        "hits.hits._source": "entity_name,file_num",
        "hits.hits.total.value": "true",
    }
    try:
        resp = _http_get(
            f"{_EFTS_BASE}/LATEST/search-index",
            params=params,
        )
        resp.raise_for_status()
    except Exception:
        return []

    results = []
    for hit in resp.json().get("hits", {}).get("hits", [])[:max_results]:
        src = hit.get("_source", {})
        raw_file_num = src.get("file_num", "")
        cik = raw_file_num.replace("028-", "").lstrip("0")
        name = src.get("entity_name", "")
        if not cik or not name:
            continue
        results.append({"cik": cik, "name": name})

    return results


# ---------------------------------------------------------------------------
# Well-known large filers (seed list so users can get started immediately)
# ---------------------------------------------------------------------------

SEED_FILERS = [
    # ── Already tracked ──────────────────────────────────────────────────
    ("0001067983", "Berkshire Hathaway"),
    ("0001336528", "Pershing Square Capital Management"),
    ("0001037389", "Renaissance Technologies"),
    ("0001350694", "Bridgewater Associates"),
    ("0001166559", "Bill & Melinda Gates Foundation Trust"),
    ("0001006438", "Appaloosa Management"),
    ("0001167483", "Tiger Global Management"),
    ("0001135730", "Coatue Management"),
    ("0001103804", "Viking Global Investors"),
    ("0001061165", "Lone Pine Capital"),
    # ── Activist ─────────────────────────────────────────────────────────
    ("0001791786", "Elliott Investment Management"),
    ("0001517137", "Starboard Value"),
    ("0001351069", "ValueAct Capital"),
    ("0001040273", "Third Point"),
    # ── Long / Short Equity ──────────────────────────────────────────────
    ("0001009268", "D.E. Shaw"),
    ("0001179392", "Two Sigma Investments"),
    ("0001423053", "Citadel Advisors"),
    ("0001603466", "Point72 Asset Management"),
    ("0001061768", "Baupost Group"),
    # ── Growth ───────────────────────────────────────────────────────────
    ("0001697748", "ARK Investment Management"),
    ("0001088875", "Baillie Gifford"),
    ("0000728014", "Ruane Cunniff & Goldfarb"),
    # ── Value ────────────────────────────────────────────────────────────
    ("0000732905", "Tweedy Browne"),
    ("0001079114", "Greenlight Capital"),
    # ── Macro / Family Office ────────────────────────────────────────────
    ("0001536411", "Duquesne Family Office"),
    # ── Large Asset Managers ─────────────────────────────────────────────
    ("0001086364", "BlackRock Advisors"),
    ("0000102909", "Vanguard Group"),
    ("0000315066", "FMR LLC (Fidelity)"),
    ("0000080255", "T. Rowe Price Associates"),
    ("0000038777", "Franklin Resources"),
    ("0001422848", "Capital Research Global Investors"),
]
