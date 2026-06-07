# P5 — Fund + Stock Rankings Website (Design Spec)

**Date:** 2026-06-07
**Phase:** P5 (final phase of the 13F ranking feature)
**Status:** approved (design), pre-implementation
**Builds on:** `docs/superpowers/13F-RANKING-STATUS.md`, `docs/superpowers/specs/2026-06-07-13f-ranking-design.md`, `13F_ranking_developer_spec_v2.md`

Wire the P1–P4 result tables into the existing Streamlit app as two new pages —
**Fund Rankings** and **Stock Rankings** — with an Apple-flavored design system and
Emil-Kowalski-style CSS motion. Build now against current (thin) data with graceful
empty states; the same pages auto-fill once the operational data backfill runs.

---

## Goals / Non-Goals

**Goals**
- Two new views in the existing `st.radio` sidebar nav: Fund Rankings, Stock Rankings.
- Apple-flavored visual design (clean type, generous whitespace, card grids, 18px radius,
  soft shadow), scoped so the 3 existing views are untouched.
- Purposeful motion (Emil Kowalski tenets → CSS): staggered fade-up, hover lift,
  animated score bars, `prefers-reduced-motion` kill-switch.
- Click-through detail views (fund holdings + QPS chart; stock holders + fundamentals).
- Graceful thin-data / empty states (1 fund, 22 raw stocks, 0 filtered).
- Recalc chain wired into refresh (`refresh.sh`/`.bat` + in-app refresh).
- New data/query layer unit-tested against a fixture DB; suite stays green.

**Non-Goals**
- No new frontend stack (no React/Next). Extend Streamlit only.
- No literal apple.com scroll choreography (out of reach in Streamlit; not needed).
- No restyle of the 3 existing views (separate scope).
- No operational data backfill in this phase (deferred; pages handle thin data).

---

## Architecture

`app.py` remains the shell (already 1283 lines — do not bloat it). New isolated package:

```
webui/
  __init__.py
  theme.py          Apple design-system CSS + motion keyframes (single inject fn)
  components.py     shared renderers: hero, kpi_strip, score_bar, badge,
                    ranking_row/list, stagger wrapper, modal helpers, fmt utils
  data.py           @st.cache_data loaders + new SQL queries (pure SQL→DataFrame)
  fund_rankings.py  render_fund_rankings()
  stock_rankings.py render_stock_rankings()
```

`app.py` changes (minimal):
- Add `"Fund Rankings"`, `"Stock Rankings"` to the existing sidebar `st.radio` options.
- Call `webui.theme.inject()` once (after existing `inject_css()`).
- Dispatch: `if view == "Fund Rankings": render_fund_rankings()` etc.
- Append recalc chain to the in-app "Refresh data" action.

`fund_id == cik` (P2 adapter), so existing holdings loaders are reused for fund detail.

**Design for isolation:** `data.py` has no `st.*` calls except the `@st.cache_data`
decorator — every loader is a pure SQL→DataFrame function, independently testable.
`components.py` holds presentation-only helpers. Page modules compose them.

---

## Design System (`webui/theme.py`)

CSS injected once, all classes namespaced `.rk-` to avoid touching existing views.

- **Type:** `-apple-system, "SF Pro Display", system-ui, sans-serif`. Hero 56px/700/tight
  tracking; section 30px/600; body 16px/400; secondary `#6e6e73`; ink `#1d1d1f`.
- **Color:** page bg `#f5f5f7`; card `#ffffff`; accent Apple blue `#0071e3`;
  buy green `#34c759`; sell red `#ff3b30`; confidence High=green, Medium=amber `#ff9f0a`,
  Low=gray `#8e8e93`.
- **Spacing:** 8px base grid; section gaps 64–96px; card padding 28px; radius 18px;
  shadow `0 2px 14px rgba(0,0,0,.06)` (hover `0 8px 28px rgba(0,0,0,.10)`).

---

## Motion (Emil Kowalski tenets → CSS, in `theme.py`)

Principles: fast (200–400ms), ease-out on enter `cubic-bezier(.16,1,.3,1)`, purposeful,
origin-aware, never gratuitous, fully disabled under `prefers-reduced-motion`.

- **On-mount stagger:** rows/cards fade-up (opacity 0→1, translateY 8px→0), 40–60ms
  stagger via per-row `animation-delay`.
- **Score bars:** width 0→value via keyframe on load.
- **Hover:** card lift `translateY(-2px)` + shadow grow; row tint; 150ms ease.
- **Badges:** subtle pop-in (scale .96→1).
- **Optional enhancement:** IntersectionObserver scroll-reveal via a tiny
  `components.html` snippet — MVP ships on-mount CSS only.

Note on Streamlit reruns: animations replay on rerun; keep durations short/subtle so
replays are unobtrusive. Heavy/long animations are avoided for this reason.

---

## Page: Fund Rankings (`render_fund_rankings`)

- **Hero:** "Fund Rankings" + subtitle ("Small, concentrated funds ranked by long-term
  selection skill") + **staleness label** (quarter end · filing deadline · approx data age).
- **KPI strip:** eligible funds · top fund · median score · quarters covered.
- **Filters bar:** score range, AUM range, min positions; **sort** by any column.
- **Ranking list (custom HTML, `st.markdown`):** Rank · Fund (bold) · Score 0–100 with a
  thin animated bar · Avg AUM · Positions · Quarters · Turnover. One-hit-wonder chip when
  flagged. **Score tooltip** explains the 3 components (performance / turnover / consistency).
- **Detail (`st.dialog` modal):** opened by an "Inspect fund" selectbox (see Interaction
  Pattern). Shows current holdings table, historical **QPS chart** (Plotly line of
  `qps_excess` over `quarter_date` from `fund_quarterly_scores`), and turnover history.

Columns sourced from `fund_rankings` (+ `fund_quarterly_scores`, `fund_turnover` for detail).

---

## Page: Stock Rankings (`render_stock_rankings`)

- **Hero:** "Stock Rankings" + subtitle ("Stocks the top funds are most convicted on") +
  one-line **staleness disclaimer** ("Positions reflect holdings at quarter end. Holdings
  may have changed since filing.").
- **Tabs:** Raw / Filtered (`st.tabs`).
- **KPI strip:** universe size · # High confidence · median score · sectors covered.
- **Filters:** sector multiselect · confidence level · market-cap range; **sort**.
- **Ranking list:** Rank · Ticker · Company · Sector · Score (sector-adjusted) ·
  **Confidence badge** (High/Med/Low colored) · #Funds · **Net Change** (green=buy /
  red=sell + arrow) · Avg Tenure.
- **Detail (`st.dialog` modal):** which qualifying funds hold it + weight (position_value /
  portfolio_value) + consecutive quarters held (new `load_stock_holders` query) +
  fundamentals (market cap, P/E w/ `pe_available`, 52wk-range mini-gauge from
  `range_position`/`partial`, gross margin).
- **Filtered empty state (0 rows today):** explanatory card — "No stocks meet filtered
  criteria yet (needs ≥3 holders + $300M–$4B market cap + populated fundamentals).
  Showing Raw rankings." — not an error.

Columns sourced from `stock_rankings_raw` / `stock_rankings_filtered` (+ `holdings`/
`securities`/`fundamentals` for detail).

---

## Interaction Pattern (the one real fork)

Pure custom-HTML rows look Apple-grade but **cannot fire Streamlit click callbacks**.
Resolution: **separate display from interaction.**
- **Display:** gorgeous custom-HTML ranking list (`st.markdown(unsafe_allow_html=True)`).
- **Interaction:** a native "Inspect fund/stock" `st.selectbox` above/beside the list →
  selecting a row opens an `st.dialog` **modal** with the detail view.

`st.dialog` requires Streamlit ≥1.31 (project pins `streamlit>=1.35`, satisfied).

---

## Data layer (`webui/data.py`)

All `@st.cache_data(ttl=300)`, pure SQL→DataFrame (mirrors existing loaders):
- `load_fund_rankings()` → `fund_rankings` ordered by rank.
- `load_fund_quarterly_scores(fund_id)` → QPS chart series.
- `load_fund_turnover(fund_id)` / reuse `load_holdings(cik, period)` for detail.
- `load_stock_rankings(kind="raw"|"filtered")`.
- `load_stock_holders(ticker)` → qualifying funds holding `ticker` in latest quarter,
  with weight and consecutive-quarters-held (join `holdings`→`securities`→`fund_rankings`).
- `load_rankings_meta()` → latest quarter end + counts for staleness label.

Pure helpers (in `components.py`, testable without Streamlit): score/AUM/pct formatting,
net-change color selection, confidence→color mapping, client-side filter + sort.

---

## Refresh wiring

Append the recalc chain (in dependency order) to `refresh.sh`, `refresh.bat`, and the
in-app "Refresh data" action:
`ingest → resolve CUSIPs → prices → fundamentals → fund_pipeline → stock_pipeline`.
The price/fundamentals/pipeline steps are idempotent (truncate-rebuild), safe to re-run.

---

## Thin-data / empty-state handling

Current data (1 fund = Baupost, 22 raw stocks, 0 filtered, most fundamentals NULL) must
look intentional:
- Single ranked fund → clean hero-card, not a broken table.
- Empty Filtered tab → the explanatory card above.
- Missing fundamentals / prices → render "—", never raise.
- KPI strip degrades gracefully (e.g. "1 eligible fund").

---

## Testing

- **Unit (new):** every `data.py` loader against a fixture SQLite DB seeded with a couple
  of funds + ranking rows; assert shape, ordering, filter/join correctness. Pure helpers
  (format, color, filter, sort) tested directly. ~8–12 new tests.
- **Out of scope for unit tests:** `st.*` render functions (per existing repo convention).
- **Gate:** full suite stays green (`python3 -m pytest -q`), currently 55 passing.

---

## Risks / Mitigations

| Risk | Mitigation |
|---|---|
| Streamlit rerun replays animations | Short/subtle durations; on-mount only |
| Custom HTML can't be clicked | Native selectbox → `st.dialog` modal pattern |
| Thin data looks broken | Explicit empty/single states designed in |
| New CSS leaks into existing views | All classes namespaced `.rk-` |
| `app.py` bloat | New `webui/` package; app.py gains only dispatch + 2 radio options |

---

## Open Questions (non-blocking, defaults chosen)

1. Detail UI = `st.dialog` modal (chosen) vs inline expander — modal picked for the
   premium feel; falls back to expander only if a Streamlit version issue surfaces.
2. Scroll-reveal JS enhancement — deferred to a polish pass; MVP is on-mount CSS.
