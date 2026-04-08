"""
SEC EDGAR client for fetching 13F-HR filing metadata and documents.

EDGAR API references:
  - Submissions:  https://data.sec.gov/submissions/CIK{cik:010d}.json
  - Full-index:   https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/company.idx
  - EFTS search:  https://efts.sec.gov/LATEST/search-index?q=%2213F-HR%22&...
"""

import re
import time
from typing import Any

import requests

_HEADERS = {
    "User-Agent": "13F Research Pipeline research@example.com",
    "Accept-Encoding": "gzip, deflate",
}
_BASE = "https://www.sec.gov"
_DATA_BASE = "https://data.sec.gov"
_EFTS_BASE = "https://efts.sec.gov"
_RATE_SLEEP = 0.11  # SEC allows ~10 req/s; we stay under


def _get(url: str, **kwargs) -> requests.Response:
    resp = requests.get(url, headers=_HEADERS, timeout=30, **kwargs)
    resp.raise_for_status()
    time.sleep(_RATE_SLEEP)
    return resp


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
    return _get(url).json()


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
            pages.append(_get(url).json())
        except Exception:
            pass
    return pages


# ---------------------------------------------------------------------------
# Filing document index
# ---------------------------------------------------------------------------

def get_filing_index(cik: str, accession_number: str) -> dict[str, Any]:
    """
    Return the filing index JSON for a specific accession number.
    Accession number may contain dashes or not.
    """
    acc_clean = accession_number.replace("-", "")
    acc_dashed = f"{acc_clean[:10]}-{acc_clean[10:12]}-{acc_clean[12:]}"
    url = f"{_BASE}/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F-HR&dateb=&owner=include&count=1&search_text="
    # Use the index JSON endpoint instead
    url = f"{_DATA_BASE}/submissions/CIK{cik.zfill(10)}.json"
    # Directly build the filing folder URL
    folder_url = (
        f"{_BASE}/Archives/edgar/data/{cik.lstrip('0')}/{acc_clean}/"
    )
    resp = _get(folder_url + "index.json")
    return resp.json()


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
    """Fetch raw text content of an EDGAR document."""
    return _get(url).text


# ---------------------------------------------------------------------------
# Bulk search (top filers by AUM)
# ---------------------------------------------------------------------------

def search_13f_filers(query: str = "", max_results: int = 20) -> list[dict[str, Any]]:
    """
    Search EDGAR full-text search for 13F-HR filers.
    Returns list of {cik, name, latest_filing_date}.
    """
    url = (
        f"{_EFTS_BASE}/LATEST/search-index"
        f"?q=%2213F-HR%22&dateRange=custom&startdt=2024-01-01"
        f"&forms=13F-HR&hits.hits._source=period_of_report,file_date,entity_name,file_num"
        f"&hits.hits.total.value=true"
    )
    if query:
        url += f"&q=%2213F-HR%22+%22{requests.utils.quote(query)}%22"

    params = {
        "q": f'"13F-HR"',
        "forms": "13F-HR",
        "dateRange": "custom",
        "startdt": "2024-10-01",
        "enddt": "2025-03-31",
        "hits.hits.total.value": "true",
    }
    resp = _get(f"{_EFTS_BASE}/LATEST/search-index", params=params)
    data = resp.json()

    results = []
    for hit in data.get("hits", {}).get("hits", [])[:max_results]:
        src = hit.get("_source", {})
        results.append({
            "cik":          src.get("file_num", "").replace("028-", "").lstrip("0"),
            "entity_name":  src.get("entity_name", ""),
            "filed_date":   src.get("file_date", ""),
            "period":       src.get("period_of_report", ""),
        })
    return results


# ---------------------------------------------------------------------------
# Well-known large filers (seed list so users can get started immediately)
# ---------------------------------------------------------------------------

SEED_FILERS = [
    # (CIK, name)
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
]
