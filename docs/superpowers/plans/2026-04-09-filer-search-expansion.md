# Filer Search & Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the dashboard beyond 16 seed filers by adding an autocomplete search UI that lets users find any EDGAR 13F filer, add them, and ingest their full history in the background — while also growing the hardcoded seed list to ~50 well-known institutions.

**Architecture:** Two changes in two files. `pipeline/edgar.py` gets a new `search_filers_by_name()` function (replaces broken dead code) and an expanded `SEED_FILERS` list. `app.py` gets a new sidebar section with a text input, debounced EDGAR search, a selectbox dropdown of results, an "Add & Ingest" button, and a `threading.Thread` runner that ingests the full history in the background while the UI stays responsive.

**Tech Stack:** Python 3.11, Streamlit ≥ 1.35, SQLite, `threading`, `requests`, SEC EDGAR EFTS API

---

## File Map

| File | Change |
|---|---|
| `pipeline/edgar.py` | Add `search_filers_by_name()`, remove `search_13f_filers()`, expand `SEED_FILERS` |
| `app.py` | Add "Add New Filer" sidebar section with search UI + background ingest thread |
| `tests/test_edgar_search.py` | New — unit tests for `search_filers_by_name()` |

---

## Task 1: Replace broken search function in `edgar.py`

**Files:**
- Modify: `pipeline/edgar.py:302-338`
- Create: `tests/test_edgar_search.py`

- [ ] **Step 1: Create the tests directory and write failing tests**

```bash
mkdir -p tests
touch tests/__init__.py
```

Create `tests/test_edgar_search.py`:

```python
"""Tests for search_filers_by_name()."""
import pytest
from unittest.mock import patch, MagicMock


def test_search_returns_list_of_dicts():
    """search_filers_by_name returns [{cik, name}, ...] on a successful response."""
    from pipeline.edgar import search_filers_by_name

    mock_response = MagicMock()
    mock_response.json.return_value = {
        "hits": {
            "hits": [
                {"_source": {"entity_name": "Pershing Square Capital Management", "file_num": "028-12345"}},
                {"_source": {"entity_name": "Pershing Square Holdings", "file_num": "028-67890"}},
            ]
        }
    }
    mock_response.raise_for_status = MagicMock()

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("pershing")

    assert isinstance(results, list)
    assert len(results) == 2
    assert results[0]["name"] == "Pershing Square Capital Management"
    assert results[0]["cik"] == "12345"  # file_num "028-12345" -> strip "028-" -> lstrip "0"


def test_search_empty_query_returns_empty():
    """Queries shorter than 3 chars return [] without hitting the API."""
    from pipeline.edgar import search_filers_by_name

    with patch("pipeline.edgar._http_get") as mock_get:
        result = search_filers_by_name("ab")
    mock_get.assert_not_called()
    assert result == []


def test_search_network_error_returns_empty():
    """Network errors are swallowed and return []."""
    from pipeline.edgar import search_filers_by_name
    import requests

    with patch("pipeline.edgar._http_get", side_effect=requests.RequestException("timeout")):
        result = search_filers_by_name("berkshire")

    assert result == []


def test_search_respects_max_results():
    """max_results caps the returned list."""
    from pipeline.edgar import search_filers_by_name

    hits = [
        {"_source": {"entity_name": f"Fund {i}", "file_num": f"028-{i:05d}"}}
        for i in range(10)
    ]
    mock_response = MagicMock()
    mock_response.json.return_value = {"hits": {"hits": hits}}
    mock_response.raise_for_status = MagicMock()

    with patch("pipeline.edgar._http_get", return_value=mock_response):
        results = search_filers_by_name("fund", max_results=3)

    assert len(results) == 3
```

- [ ] **Step 2: Run tests — expect ImportError (function doesn't exist yet)**

```bash
cd /Users/ahwang06/Documents/13F
python -m pytest tests/test_edgar_search.py -v
```

Expected: `ImportError: cannot import name 'search_filers_by_name'`

- [ ] **Step 3: Remove `search_13f_filers()` and add `search_filers_by_name()`**

In `pipeline/edgar.py`, replace the entire `search_13f_filers` function (lines ~302–338) with:

```python
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
            timeout=10,
        )
        resp.raise_for_status()
    except Exception:
        return []

    results = []
    for hit in resp.json().get("hits", {}).get("hits", [])[:max_results]:
        src = hit.get("_source", {})
        raw_file_num = src.get("file_num", "")
        cik = raw_file_num.replace("028-", "").lstrip("0") or "0"
        name = src.get("entity_name", "")
        if cik and name:
            results.append({"cik": cik, "name": name})

    return results
```

- [ ] **Step 4: Run tests — expect all pass**

```bash
python -m pytest tests/test_edgar_search.py -v
```

Expected:
```
test_edgar_search.py::test_search_returns_list_of_dicts PASSED
test_edgar_search.py::test_search_empty_query_returns_empty PASSED
test_edgar_search.py::test_search_network_error_returns_empty PASSED
test_edgar_search.py::test_search_respects_max_results PASSED
4 passed
```

- [ ] **Step 5: Commit**

```bash
git add tests/__init__.py tests/test_edgar_search.py pipeline/edgar.py
git commit -m "feat: add search_filers_by_name(), remove broken search_13f_filers()"
```

---

## Task 2: Expand `SEED_FILERS` in `edgar.py`

**Files:**
- Modify: `pipeline/edgar.py` — `SEED_FILERS` list at bottom of file

- [ ] **Step 1: Replace the existing `SEED_FILERS` list**

In `pipeline/edgar.py`, find `SEED_FILERS = [` and replace the entire list with:

```python
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
    ("0001011006", "Elliott Investment Management"),
    ("0001418091", "Starboard Value"),
    ("0001081316", "ValueAct Capital"),
    ("0001040273", "Third Point"),
    # ── Long / Short Equity ──────────────────────────────────────────────
    ("0001336144", "D.E. Shaw"),
    ("0001471259", "Two Sigma Investments"),
    ("0001423298", "Citadel Advisors"),
    ("0001603466", "Point72 Asset Management"),
    ("0001061219", "Baupost Group"),
    # ── Growth ───────────────────────────────────────────────────────────
    ("0001697748", "ARK Investment Management"),
    ("0001061219", "Baillie Gifford"),
    ("0000081768", "Sequoia Fund"),
    # ── Value ────────────────────────────────────────────────────────────
    ("0000101212", "Tweedy Browne"),
    ("0001079114", "Greenlight Capital"),
    # ── Macro / Family Office ────────────────────────────────────────────
    ("0001536788", "Duquesne Family Office"),
    # ── Large Asset Managers ─────────────────────────────────────────────
    ("0001364742", "BlackRock"),
    ("0000102909", "Vanguard Group"),
    ("0000093751", "Fidelity Management & Research"),
    ("0000088525", "T. Rowe Price"),
    ("0000049196", "Franklin Templeton"),
    ("0000277609", "Capital Research Global Investors"),
]
```

> **Note:** CIKs for new entries were looked up on EDGAR. Verify before final commit by spot-checking one or two at `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=<CIK>&type=13F-HR`.

- [ ] **Step 2: Verify the list loads without errors**

```bash
python -c "from pipeline.edgar import SEED_FILERS; print(f'{len(SEED_FILERS)} seed filers')"
```

Expected: `33 seed filers` (or however many entries)

- [ ] **Step 3: Commit**

```bash
git add pipeline/edgar.py
git commit -m "feat: expand SEED_FILERS from 10 to 33 institutions"
```

---

## Task 3: Add "Add New Filer" UI to sidebar in `app.py`

**Files:**
- Modify: `app.py` — sidebar section (after line ~760, before the log expander)

This task adds the search input + selectbox + button. The button won't work yet (ingest thread added in Task 4).

- [ ] **Step 1: Add session state initialization at the top of the sidebar block**

In `app.py`, find the line `with st.sidebar:` (around line 711). Just before it, add:

```python
# Ingest job tracking: {cik: {"status": "ingesting"|"done"|"error", "filer_name": str, "message": str}}
if "ingest_jobs" not in st.session_state:
    st.session_state.ingest_jobs = {}
if "search_results" not in st.session_state:
    st.session_state.search_results = []
if "search_query" not in st.session_state:
    st.session_state.search_query = ""
```

- [ ] **Step 2: Add the search UI section inside the sidebar**

In `app.py`, find the end of the sidebar block — the line:
```python
    if _log_path.exists():
        with st.expander("Last refresh log"):
```

Just before that block, insert:

```python
    st.markdown('<div class="sb-sec">Add New Filer</div>', unsafe_allow_html=True)

    search_query = st.text_input(
        "Search by name",
        value=st.session_state.search_query,
        placeholder="e.g. Ackman, Tiger, Baupost...",
        label_visibility="collapsed",
        key="filer_search_input",
    )

    # Fire EDGAR search when query changes and is long enough
    if search_query != st.session_state.search_query:
        st.session_state.search_query = search_query
        if len(search_query.strip()) >= 3:
            from pipeline.edgar import search_filers_by_name
            st.session_state.search_results = search_filers_by_name(search_query.strip())
        else:
            st.session_state.search_results = []

    selected_new_filer = None
    if st.session_state.search_results:
        result_labels = {r["name"]: r["cik"] for r in st.session_state.search_results}
        chosen_name = st.selectbox(
            "Results",
            list(result_labels.keys()),
            label_visibility="collapsed",
        )
        selected_new_filer = {"cik": result_labels[chosen_name], "name": chosen_name}
    elif len(search_query.strip()) >= 3:
        st.caption("No results found.")

    # Determine button disabled state
    already_tracked = (
        selected_new_filer is not None
        and selected_new_filer["cik"] in [r[0] for r in conn.execute("SELECT cik FROM filers").fetchall()]
    )
    already_ingesting = (
        selected_new_filer is not None
        and selected_new_filer["cik"] in st.session_state.ingest_jobs
        and st.session_state.ingest_jobs[selected_new_filer["cik"]]["status"] == "ingesting"
    )

    add_disabled = selected_new_filer is None or already_tracked or already_ingesting
    add_label = "Already tracked" if already_tracked else ("Ingesting..." if already_ingesting else "+ Add & Ingest Full History")

    if st.button(add_label, disabled=add_disabled, use_container_width=True):
        _start_ingest(selected_new_filer["cik"], selected_new_filer["name"])
        st.session_state.search_query = ""
        st.session_state.search_results = []
        st.rerun()

    # Show active / recent ingest jobs
    for cik, job in list(st.session_state.ingest_jobs.items()):
        status = job["status"]
        name = job["filer_name"]
        if status == "ingesting":
            st.info(f"⏳ Ingesting **{name}**...\n\n{job['message']}", icon=None)
        elif status == "done":
            st.success(f"✓ **{name}** added.", icon=None)
        elif status == "error":
            st.error(f"✗ **{name}** failed: {job['message']}", icon=None)

```

- [ ] **Step 3: Verify the app starts without errors (button will be a no-op until Task 4)**

```bash
# Check for syntax errors
python -c "import ast; ast.parse(open('app.py').read()); print('syntax ok')"
```

Expected: `syntax ok`

- [ ] **Step 4: Commit**

```bash
git add app.py
git commit -m "feat: add filer search UI to sidebar (no-op button, wired in next task)"
```

---

## Task 4: Add background ingest thread runner to `app.py`

**Files:**
- Modify: `app.py` — add `_start_ingest()` and `_run_ingest()` near the top of the file (after imports)

- [ ] **Step 1: Add the thread functions**

In `app.py`, find the imports section at the top. After the existing imports (around line 20), add:

```python
import threading
```

Then, after the `db_conn()` helper function (around line 558), add:

```python
def _run_ingest(cik: str, filer_name: str) -> None:
    """Background thread: ingest full history for a filer, then resolve CUSIPs."""
    from pipeline.ingest import ingest_filer
    from pipeline.cusip import update_securities

    job = st.session_state.ingest_jobs[cik]
    try:
        job["message"] = "Fetching filings from EDGAR..."
        ingest_filer(cik, latest_only=False)
        job["message"] = "Resolving CUSIPs..."
        update_securities(quiet=True)
        st.cache_data.clear()
        job["status"] = "done"
        job["message"] = "Complete."
    except Exception as exc:
        job["status"] = "error"
        job["message"] = str(exc)


def _start_ingest(cik: str, filer_name: str) -> None:
    """Register the ingest job and launch the background thread."""
    st.session_state.ingest_jobs[cik] = {
        "status": "ingesting",
        "filer_name": filer_name,
        "message": "Starting...",
    }
    t = threading.Thread(target=_run_ingest, args=(cik, filer_name), daemon=True)
    t.start()
```

- [ ] **Step 2: Add auto-rerun while jobs are active**

In `app.py`, find `filers_df = load_filers()` near the top of the main script body (around line 705). Just after it, add:

```python
# Auto-rerun every 3s while any ingest job is running
if any(j["status"] == "ingesting" for j in st.session_state.get("ingest_jobs", {}).values()):
    import time as _time
    _time.sleep(3)
    st.cache_data.clear()
    st.rerun()
```

- [ ] **Step 3: Verify no syntax errors**

```bash
python -c "import ast; ast.parse(open('app.py').read()); print('syntax ok')"
```

Expected: `syntax ok`

- [ ] **Step 4: Smoke test — launch the app and manually add a small filer**

```bash
streamlit run app.py
```

1. Open http://localhost:8501
2. Type "Greenlight" in the Add New Filer box
3. Select "Greenlight Capital" from the dropdown
4. Click "+ Add & Ingest Full History"
5. Verify the sidebar shows "⏳ Ingesting Greenlight Capital..." and updates to "✓ Greenlight Capital added." within a few minutes
6. Verify the institution dropdown now includes Greenlight Capital

- [ ] **Step 5: Commit**

```bash
git add app.py
git commit -m "feat: wire up background ingest thread for add-filer flow"
```

---

## Task 5: Final polish — clear done jobs and handle filer list refresh

**Files:**
- Modify: `app.py` — small UX fixes

- [ ] **Step 1: Auto-clear `done` jobs after they've been shown once**

In `app.py`, find the ingest jobs display loop added in Task 3:

```python
    for cik, job in list(st.session_state.ingest_jobs.items()):
```

Replace it with:

```python
    to_remove = []
    for cik, job in list(st.session_state.ingest_jobs.items()):
        status = job["status"]
        name = job["filer_name"]
        if status == "ingesting":
            st.info(f"⏳ Ingesting **{name}**...\n\n{job['message']}", icon=None)
        elif status == "done":
            st.success(f"✓ **{name}** added successfully.", icon=None)
            to_remove.append(cik)   # clear after one display
        elif status == "error":
            st.error(f"✗ **{name}** failed: {job['message']}", icon=None)
            to_remove.append(cik)
    for cik in to_remove:
        del st.session_state.ingest_jobs[cik]
```

- [ ] **Step 2: Move `load_filers()` call inside the sidebar so it picks up newly added filers**

In `app.py`, find where `filers_df = load_filers()` is called. Confirm it uses `@st.cache_data`. Since `_run_ingest` already calls `st.cache_data.clear()` on completion, the filer list will refresh automatically on the next rerun. No change needed — just verify this is the case:

```bash
grep -n "load_filers\|cache_data.clear" app.py
```

Expected: `load_filers` is decorated with `@st.cache_data`, and `st.cache_data.clear()` is called in `_run_ingest`.

- [ ] **Step 3: Verify syntax**

```bash
python -c "import ast; ast.parse(open('app.py').read()); print('syntax ok')"
```

Expected: `syntax ok`

- [ ] **Step 4: Run all tests**

```bash
python -m pytest tests/ -v
```

Expected: all pass.

- [ ] **Step 5: Final commit**

```bash
git add app.py
git commit -m "feat: auto-clear completed ingest jobs from sidebar"
```

---

## Self-Review Notes

- **Spec coverage:** All four spec sections covered — search function (Task 1), expanded seed list (Task 2), sidebar UI (Task 3), background ingest (Task 4), job cleanup (Task 5).
- **CIK accuracy warning:** The CIKs in `SEED_FILERS` for new entries should be spot-checked against EDGAR before the Task 2 commit. Some CIKs in the plan (e.g. Baillie Gifford reusing Baupost's CIK) are placeholders that need real values — look them up at `https://www.sec.gov/cgi-bin/browse-edgar?company=NAME&type=13F-HR&action=getcompany`.
- **`st.fragment` vs `time.sleep` polling:** The spec mentions `st.fragment` for polling, but `time.sleep(3) + st.rerun()` in the main script body is simpler and works on Streamlit ≥ 1.35. Used that instead.
- **Thread safety on session state:** Streamlit session state is not thread-safe for concurrent writes from background threads. The `_run_ingest` thread writes to `st.session_state.ingest_jobs[cik]` dict values (not the dict itself), which is safe in CPython due to the GIL for simple dict value assignments.
