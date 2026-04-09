# Filer Search & Expansion — Design Spec

**Date:** 2026-04-09
**Status:** Approved

---

## Overview

Expand the 13F dashboard beyond its 16 hardcoded seed filers in two ways:

1. **Expanded seed list** — grow `SEED_FILERS` from 16 to ~50 well-known institutions, pre-categorized
2. **In-dashboard filer search** — autocomplete text input in the sidebar lets users find any EDGAR 13F filer by name, add them, and ingest their full history in the background

---

## Goals

- Any EDGAR 13F filer (~6,000 institutions) should be discoverable and addable without touching the CLI
- Added filers persist in the database permanently alongside seed filers
- Full history is ingested by default (required for trend analysis and conviction scores)
- The UI remains responsive during ingestion — no blocking spinners

---

## Section 1: EDGAR Filer Search (`edgar.py`)

### New function: `search_filers_by_name(query, max_results=20)`

Calls EDGAR's full-text search endpoint (`https://efts.sec.gov/LATEST/search-index`) filtered to `13F-HR` form type. Returns a list of `{cik, name}` dicts for matching institutions.

- Minimum 3 characters before firing a request
- Results cached for 1 hour via the existing `_cache_*` helpers (filer names don't change)
- Returns an empty list on network error (non-blocking)

### Removal

The existing `search_13f_filers()` function is broken (calls non-existent `_get()`) and unused. It is removed entirely and replaced by `search_filers_by_name()`.

### Expanded `SEED_FILERS`

Grows from 16 to ~50 entries, organized by category:

| Category | New additions |
|---|---|
| Activist | Elliott Management, Starboard Value, ValueAct Capital, Third Point |
| Long/Short Equity | D.E. Shaw, Two Sigma, Citadel Advisors, Point72, Baupost Group |
| Growth | ARK Invest, Baillie Gifford, Sequoia Fund |
| Value | Tweedy Browne, Fairfax Financial, Pabrai Funds |
| Macro / Family Office | Greenlight Capital, Duquesne Family Office |
| Endowments | Yale Investments, Harvard Management, Stanford Management |

---

## Section 2: Dashboard UI (`app.py`)

### Sidebar addition: "Add New Filer"

A new section below the institution selector, separated by a divider:

1. **Text input** — labeled "Add New Filer", placeholder "Search by name..."
2. On each keystroke (min 3 chars), calls `search_filers_by_name()` and stores results in `st.session_state.search_results`
3. **Selectbox** — shows matching filer names as a dropdown; selecting one stores the chosen `{cik, name}` in session state
4. **"+ Add & Ingest Full History" button** — disabled if:
   - No filer selected
   - CIK already exists in the `filers` table
   - An ingestion job for this CIK is already running
5. On click: upserts filer row into DB, launches background thread, sets job state to `ingesting`

### Ingestion status indicator

While any job is active, a status card appears in the sidebar showing:
- Filer name
- Status (`ingesting...` / `done` / `error`)
- A simple progress bar (indeterminate while running, full on done)
- Error message if failed

### Polling

Uses `st.fragment` on the sidebar to rerun only the sidebar every 3 seconds while any job is in `ingesting` state. Full page rerun is triggered once a job transitions to `done`.

---

## Section 3: Background Ingestion

### Session state schema

```python
st.session_state.ingest_jobs: dict[str, {
    "status":      "ingesting" | "done" | "error",
    "filer_name":  str,
    "message":     str,   # progress note or error text
}]
```

### Thread function: `_run_ingest(cik, filer_name)`

1. Calls `ingest_filer(cik)` from `pipeline.ingest` — full history, no `latest_only`
2. On completion, calls `update_securities()` from `pipeline.cusip` to resolve new CUSIPs
3. Updates `st.session_state.ingest_jobs[cik]` to `done` or `error` with a message
4. Thread is daemon so it doesn't block process shutdown

### No changes to `pipeline/ingest.py` or `pipeline/cusip.py`

Both are already importable and callable. No modifications needed.

---

## Error Handling

| Scenario | Behavior |
|---|---|
| EDGAR search network error | Returns empty results silently; input remains usable |
| Filer already in DB | "Add" button disabled with tooltip "Already tracked" |
| Ingest fails mid-way | Job transitions to `error`, message shown in sidebar |
| Duplicate add attempt | Button disabled while CIK is in `ingesting` state |

---

## Files Changed

| File | Change |
|---|---|
| `pipeline/edgar.py` | Add `search_filers_by_name()`, remove `search_13f_filers()`, expand `SEED_FILERS` |
| `app.py` | Add sidebar search UI, ingestion thread runner, session state tracking |

No schema changes. No new dependencies.
