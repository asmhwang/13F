"""
13F Holdings Dashboard — Streamlit app.

Run with:
    streamlit run app.py
"""

import subprocess
import sys
import threading
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from pipeline.database import DB_PATH, get_connection
from pipeline.edgar import search_filers_by_name

# Thread-safe job store for background ingest operations
_ingest_jobs: dict[str, dict] = {}
_ingest_lock = threading.Lock()

# Thread-safe status for background refresh
_refresh_status: dict = {"running": False, "done": False, "error": None}
_refresh_lock = threading.Lock()

# ────────────────────────────────────────────────────────────────────────────────
# Page config
# ────────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="13F Holdings",
    page_icon="◦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ────────────────────────────────────────────────────────────────────────────────
# Plotly warm-light template
# ────────────────────────────────────────────────────────────────────────────────

_CHART_BG  = "#FDFAF6"
_GRID      = "#E8E0D4"
_TEXT      = "#4A3F36"
_TEXT_DIM  = "#9C8D80"

pio.templates["calm"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor=_CHART_BG,
        plot_bgcolor=_CHART_BG,
        font=dict(family="'Nunito', sans-serif", color=_TEXT, size=12),
        colorway=["#4A7FA5", "#6B8F7A", "#C48B3F", "#B85C4A",
                  "#7A6BAF", "#4A9490", "#B87A3F", "#8A9FAF"],
        xaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            zerolinecolor=_GRID, tickcolor=_GRID,
            tickfont=dict(color=_TEXT_DIM, size=11),
        ),
        yaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            zerolinecolor=_GRID, tickcolor=_GRID,
            tickfont=dict(color=_TEXT_DIM, size=11),
        ),
        hoverlabel=dict(
            bgcolor="#FDFAF6", bordercolor=_GRID,
            font=dict(family="'Nunito', sans-serif", color=_TEXT, size=12),
        ),
        legend=dict(
            bgcolor=_CHART_BG, bordercolor=_GRID, borderwidth=1,
            font=dict(color=_TEXT_DIM, size=11),
        ),
        margin=dict(t=20, b=20, l=8, r=8),
        coloraxis=dict(colorbar=dict(
            bgcolor=_CHART_BG,
            tickfont=dict(color=_TEXT_DIM, size=10),
            outlinecolor=_GRID, outlinewidth=1,
        )),
    )
)
pio.templates.default = "calm"

CS_BLUE  = ["#FDFAF6", "#C5D9E8", "#7BAFC9", "#4A7FA5"]
CS_SAGE  = ["#FDFAF6", "#C5D9CC", "#85B89A", "#5B8A6E"]
CS_AMBER = ["#FDFAF6", "#EDD9B8", "#D4A86B", "#C48B3F"]
CS_SLATE = ["#FDFAF6", "#D0D8E0", "#8EA8BC", "#4A7FA5"]

# ────────────────────────────────────────────────────────────────────────────────
# CSS
# ────────────────────────────────────────────────────────────────────────────────

def inject_css() -> None:
    st.markdown(r"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Lora:ital,wght@0,400;0,500;0,600;1,400;1,500&family=Nunito:wght@300;400;500;600&display=swap');

/* ── Variables ─────────────────────────────────────── */
:root {
    --bg:     #F4EFE6;
    --card:   #FDFAF6;
    --surf:   #F0EBE0;
    --bdr:    #DDD4C6;
    --bdr-lo: #EDE5D8;
    --t0:     #2E2720;
    --t1:     #6E6058;
    --t2:     #A09080;
    --slate:  #4A7FA5;
    --sage:   #5B8A6E;
    --amber:  #C48B3F;
    --green:  #5B8A6E;
    --red:    #B85C4A;
    --shadow: 0 1px 4px rgba(60,40,20,0.07), 0 0 0 1px var(--bdr-lo);
}

/* ── Shell ──────────────────────────────────────────── */
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.main,
.main .block-container          { background: var(--bg) !important; }
.main .block-container          { padding-top: 1.5rem !important; max-width: 1440px !important; }

/* ── Sidebar ────────────────────────────────────────── */
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div,
[data-testid="stSidebarContent"] { background: var(--card) !important; }
[data-testid="stSidebar"]        { border-right: 1px solid var(--bdr) !important; }

/* ── Typography ─────────────────────────────────────── */
body, p, span, li               { font-family: 'Nunito', sans-serif; }
h1, h2, h3, h4                  { font-family: 'Lora', serif !important; color: var(--t0) !important; font-weight: 500 !important; }
h1                              { font-size: 2rem !important; letter-spacing: -0.01em !important; }
h2                              { font-size: 1.4rem !important; }
h3                              { font-size: 1.1rem !important; font-weight: 400 !important; }
p, .stMarkdown p                { font-family: 'Nunito', sans-serif !important; color: var(--t0) !important; font-size: 0.92rem !important; line-height: 1.6 !important; }

/* ── Selectbox ──────────────────────────────────────── */
[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child {
    background: var(--card) !important;
    border-color: var(--bdr) !important;
    color: var(--t0) !important;
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.9rem !important;
    border-radius: 6px !important;
}
[data-testid="stSelectbox"] label {
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    color: var(--t2) !important;
}
[data-baseweb="popover"],
[data-baseweb="menu"]           { background: var(--card) !important; border: 1px solid var(--bdr) !important; border-radius: 6px !important; box-shadow: var(--shadow) !important; }
[data-baseweb="option"]         { background: var(--card) !important; color: var(--t0) !important; font-family: 'Nunito', sans-serif !important; font-size: 0.9rem !important; }
[data-baseweb="option"]:hover   { background: var(--surf) !important; }

/* ── Slider ─────────────────────────────────────────── */
[data-testid="stSlider"] label  {
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    color: var(--t2) !important;
}

/* ── Radio ──────────────────────────────────────────── */
[data-testid="stRadio"] > label {
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    color: var(--t2) !important;
}
[data-testid="stRadio"] [data-testid="stMarkdownContainer"] p {
    font-size: 0.9rem !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
    font-weight: 400 !important;
    color: var(--t0) !important;
}

/* ── Buttons ────────────────────────────────────────── */
.stButton > button {
    background: var(--card) !important;
    border: 1px solid var(--bdr) !important;
    color: var(--t1) !important;
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.05em !important;
    border-radius: 6px !important;
    padding: 0.4rem 1.1rem !important;
    transition: all 0.15s ease !important;
    box-shadow: var(--shadow) !important;
}
.stButton > button:hover {
    border-color: var(--slate) !important;
    color: var(--slate) !important;
    background: #EEF4F9 !important;
}

/* ── Dividers ───────────────────────────────────────── */
hr, [data-testid="stDivider"] hr { border-color: var(--bdr-lo) !important; margin: 1.2rem 0 !important; }

/* ── Metrics ────────────────────────────────────────── */
[data-testid="metric-container"],
[data-testid="stMetric"] {
    background: var(--card) !important;
    border-radius: 8px !important;
    padding: 1rem 1.2rem !important;
    box-shadow: var(--shadow) !important;
}
[data-testid="stMetricLabel"] p {
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.72rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    color: var(--t2) !important;
    font-weight: 600 !important;
}
[data-testid="stMetricValue"] {
    font-family: 'Lora', serif !important;
    font-size: 1.5rem !important;
    color: var(--t0) !important;
}

/* ── Dataframe ──────────────────────────────────────── */
.stDataFrame > div {
    background: var(--card) !important;
    border: 1px solid var(--bdr-lo) !important;
    border-radius: 8px !important;
    overflow: hidden !important;
    box-shadow: var(--shadow) !important;
}

/* ── Expander ───────────────────────────────────────── */
[data-testid="stExpander"] {
    background: var(--card) !important;
    border: 1px solid var(--bdr-lo) !important;
    border-radius: 8px !important;
    box-shadow: var(--shadow) !important;
}
[data-testid="stExpander"] summary {
    color: var(--t1) !important;
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.85rem !important;
    font-weight: 600 !important;
}

/* ── Alerts ─────────────────────────────────────────── */
[data-testid="stAlert"] {
    background: var(--card) !important;
    border: 1px solid var(--bdr) !important;
    border-radius: 8px !important;
}
[data-testid="stAlert"] p { color: var(--t1) !important; }

/* ── Code ───────────────────────────────────────────── */
pre, .stCodeBlock { background: var(--surf) !important; border: 1px solid var(--bdr) !important; border-radius: 6px !important; }
pre code, .stCodeBlock code {
    font-size: 0.78rem !important;
    color: var(--t0) !important;
}

/* ── Caption ────────────────────────────────────────── */
[data-testid="stCaptionContainer"] p {
    font-family: 'Nunito', sans-serif !important;
    font-size: 0.78rem !important;
    color: var(--t2) !important;
}

/* ── Plotly chart wrapper ───────────────────────────── */
[data-testid="stPlotlyChart"] > div {
    border-radius: 8px !important;
    overflow: hidden !important;
}

/* ── Scrollbar ──────────────────────────────────────── */
::-webkit-scrollbar              { width: 5px; height: 5px; }
::-webkit-scrollbar-track        { background: var(--bg); }
::-webkit-scrollbar-thumb        { background: var(--bdr); border-radius: 4px; }

/* ════════════════════════════════════════════════════ */
/*  Custom HTML components                             */
/* ════════════════════════════════════════════════════ */

/* Hero */
.hero {
    display: flex;
    align-items: baseline;
    gap: 1rem;
    padding-bottom: 1.2rem;
    border-bottom: 1px solid var(--bdr);
    margin-bottom: 0;
}
.hero-title {
    font-family: 'Lora', serif;
    font-size: 1.75rem;
    font-weight: 500;
    color: var(--t0);
    letter-spacing: -0.01em;
    margin: 0;
    line-height: 1;
}
.hero-divider {
    color: var(--bdr);
    font-size: 1.2rem;
    line-height: 1;
}
.hero-context {
    font-family: 'Nunito', sans-serif;
    font-size: 0.9rem;
    color: var(--t1);
    font-weight: 400;
    font-style: italic;
}
.hero-period {
    margin-left: auto;
    font-family: 'Nunito', sans-serif;
    font-size: 0.82rem;
    color: var(--t2);
    font-weight: 500;
}
.hero-period b {
    color: var(--amber);
    font-weight: 600;
}

/* KPI strip */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem;
    margin: 1.2rem 0 1.4rem;
}
.kpi-card {
    background: var(--card);
    border-radius: 8px;
    padding: 1rem 1.1rem 0.9rem;
    box-shadow: var(--shadow);
    border-top: 3px solid transparent;
}
.kpi-label {
    font-family: 'Nunito', sans-serif;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--t2);
    font-weight: 700;
    margin-bottom: 0.45rem;
}
.kpi-val {
    font-family: 'Lora', serif;
    font-size: 1.55rem;
    font-weight: 500;
    color: var(--t0);
    line-height: 1.1;
}
.kpi-note {
    font-family: 'Nunito', sans-serif;
    font-size: 0.72rem;
    color: var(--t2);
    margin-top: 0.25rem;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

/* Section headers */
.shdr {
    display: flex;
    align-items: center;
    gap: 0.9rem;
    margin: 1.6rem 0 0.85rem;
}
.shdr-title {
    font-family: 'Lora', serif;
    font-size: 1.15rem;
    font-weight: 500;
    color: var(--t0);
    white-space: nowrap;
}
.shdr-line {
    flex: 1;
    height: 1px;
    background: var(--bdr-lo);
}
.shdr-tag {
    font-family: 'Nunito', sans-serif;
    font-size: 0.7rem;
    font-weight: 600;
    color: var(--t2);
    letter-spacing: 0.06em;
    white-space: nowrap;
}

/* Conviction formula */
.formula-card {
    background: var(--card);
    border: 1px solid var(--bdr-lo);
    border-left: 3px solid var(--amber);
    border-radius: 0 8px 8px 0;
    padding: 0.85rem 1.1rem;
    margin-bottom: 1rem;
    font-family: 'Nunito', sans-serif;
    font-size: 0.85rem;
    color: var(--t1);
    line-height: 1.7;
    box-shadow: var(--shadow);
}
.formula-card b { color: var(--amber); font-weight: 700; }

/* QoQ change badges */
.chg-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.75rem;
    margin-top: 0.9rem;
}
.chg-card {
    background: var(--card);
    border-radius: 8px;
    padding: 0.9rem 1rem;
    text-align: center;
    box-shadow: var(--shadow);
}
.chg-card-label {
    font-family: 'Nunito', sans-serif;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--t2);
    font-weight: 700;
    margin-bottom: 0.3rem;
}
.chg-card-val {
    font-family: 'Lora', serif;
    font-size: 1.8rem;
    font-weight: 500;
    line-height: 1;
}

/* Sidebar brand */
.sb-brand {
    padding: 0.85rem 0 1rem;
    border-bottom: 1px solid var(--bdr-lo);
    margin-bottom: 0.6rem;
}
.sb-title {
    font-family: 'Lora', serif;
    font-size: 1.1rem;
    font-weight: 500;
    color: var(--t0);
    letter-spacing: -0.01em;
}
.sb-sub {
    font-family: 'Nunito', sans-serif;
    font-size: 0.7rem;
    color: var(--t2);
    margin-top: 0.15rem;
    font-weight: 500;
}

.sb-sec {
    font-family: 'Nunito', sans-serif;
    font-size: 0.68rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--t2);
    font-weight: 700;
    padding: 0.9rem 0 0.2rem;
    border-top: 1px solid var(--bdr-lo);
    margin-top: 0.8rem;
}
</style>
""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# HTML component helpers
# ────────────────────────────────────────────────────────────────────────────────

_ACCENT = {
    "slate":  "#4A7FA5",
    "sage":   "#5B8A6E",
    "amber":  "#C48B3F",
    "green":  "#5B8A6E",
    "red":    "#B85C4A",
    "muted":  "#A09080",
}


def kpi_row(cards: list[tuple]) -> None:
    """cards = [(label, value, color_key, note), ...]"""
    html = '<div class="kpi-grid">'
    for label, value, color, note in cards:
        c = _ACCENT.get(color, _ACCENT["slate"])
        note_html = f'<div class="kpi-note">{note}</div>' if note else ""
        html += f"""
  <div class="kpi-card" style="border-top-color:{c}">
    <div class="kpi-label">{label}</div>
    <div class="kpi-val" style="color:{c}">{value}</div>
    {note_html}
  </div>"""
    html += "\n</div>"
    st.markdown(html, unsafe_allow_html=True)


def shdr(title: str, tag: str = "") -> None:
    tag_html = f'<span class="shdr-tag">{tag}</span>' if tag else ""
    st.markdown(f"""
<div class="shdr">
  <span class="shdr-title">{title}</span>
  <div class="shdr-line"></div>
  {tag_html}
</div>""", unsafe_allow_html=True)


def hero(period: str = "", filer: str = "") -> None:
    ctx_html  = f'<span class="hero-divider">·</span><span class="hero-context">{filer}</span>' if filer else ""
    per_html  = f'<span class="hero-period">Period&ensp;<b>{period}</b></span>' if period else ""
    st.markdown(f"""
<div class="hero">
  <span class="hero-title">13F Holdings</span>
  {ctx_html}
  {per_html}
</div>""", unsafe_allow_html=True)


def chg_badges(new: int, closed: int, increased: int, decreased: int) -> None:
    st.markdown(f"""
<div class="chg-grid">
  <div class="chg-card">
    <div class="chg-card-label">New</div>
    <div class="chg-card-val" style="color:#5B8A6E">{new}</div>
  </div>
  <div class="chg-card">
    <div class="chg-card-label">Closed</div>
    <div class="chg-card-val" style="color:#B85C4A">{closed}</div>
  </div>
  <div class="chg-card">
    <div class="chg-card-label">Increased</div>
    <div class="chg-card-val" style="color:#5B8A6E">{increased}</div>
  </div>
  <div class="chg-card">
    <div class="chg-card-label">Decreased</div>
    <div class="chg-card-val" style="color:#B85C4A">{decreased}</div>
  </div>
</div>""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# DB helpers (cached)
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def db_conn():
    return get_connection()


def _run_ingest(cik: str, filer_name: str) -> None:
    """Background thread: ingest full history for a filer, then resolve CUSIPs."""
    from pipeline.ingest import ingest_filer
    from pipeline.cusip import update_securities

    try:
        with _ingest_lock:
            _ingest_jobs[cik]["message"] = "Fetching filings from EDGAR..."
        ingest_filer(cik, latest_only=False)
        with _ingest_lock:
            _ingest_jobs[cik]["message"] = "Resolving CUSIPs..."
        update_securities(quiet=True)
        with _ingest_lock:
            _ingest_jobs[cik]["status"] = "done"
            _ingest_jobs[cik]["message"] = "Complete."
    except Exception as exc:
        with _ingest_lock:
            _ingest_jobs[cik]["status"] = "error"
            _ingest_jobs[cik]["message"] = str(exc)


def _start_ingest(cik: str, filer_name: str) -> None:
    """Register the ingest job and launch the background thread."""
    # Guard against double-launch
    with _ingest_lock:
        if cik in _ingest_jobs and _ingest_jobs[cik]["status"] == "ingesting":
            return
        _ingest_jobs[cik] = {
            "status": "ingesting",
            "filer_name": filer_name,
            "message": "Starting...",
        }
    t = threading.Thread(target=_run_ingest, args=(cik, filer_name), daemon=True)
    t.start()


def _run_refresh() -> None:
    """Background thread: run refresh.sh (or refresh.bat on Windows) and update _refresh_status."""
    global _refresh_status
    import sys
    repo = Path(__file__).parent
    if sys.platform == "win32":
        script = repo / "refresh.bat"
        cmd = ["cmd", "/c", str(script)]
    else:
        script = repo / "refresh.sh"
        cmd = ["bash", str(script)]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True
        )
        with _refresh_lock:
            if result.returncode == 0:
                _refresh_status = {"running": False, "done": True, "error": None}
            else:
                _refresh_status = {
                    "running": False,
                    "done": False,
                    "error": result.stderr or result.stdout or "Unknown error",
                }
    except Exception as exc:
        with _refresh_lock:
            _refresh_status = {"running": False, "done": False, "error": str(exc)}


@st.cache_data(ttl=300)
def load_filers():
    conn = db_conn()
    return pd.read_sql("SELECT cik, name FROM filers ORDER BY name", conn)


@st.cache_data(ttl=300)
def load_periods():
    conn = db_conn()
    rows = conn.execute(
        "SELECT DISTINCT period_of_report FROM filings ORDER BY period_of_report DESC"
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_holdings(cik: str, period: str) -> pd.DataFrame:
    conn = db_conn()
    return pd.read_sql(
        """
        SELECT h.cusip,
               COALESCE(s.ticker, MAX(h.name_of_issuer)) AS ticker,
               s.ticker                                   AS raw_ticker,
               COALESCE(s.name, MAX(h.name_of_issuer))   AS name_of_issuer,
               MAX(h.title_of_class)                      AS title_of_class,
               SUM(h.value_thousands)                     AS value_thousands,
               SUM(h.shares)                              AS shares,
               h.put_call,
               MAX(h.investment_discretion)               AS investment_discretion
        FROM holdings h
        JOIN filings f ON f.id = h.filing_id
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE f.cik = ? AND f.period_of_report = ?
          AND h.value_thousands > 0
          AND f.id = (
              SELECT f2.id FROM filings f2
              WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
              ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
          )
        GROUP BY h.cusip, h.put_call, s.ticker
        ORDER BY value_thousands DESC
        """,
        conn, params=(cik, period),
    )


@st.cache_data(ttl=300)
def load_all_holdings(period: str) -> pd.DataFrame:
    conn = db_conn()
    return pd.read_sql(
        """
        SELECT f.cik, fi.name AS filer_name,
               h.cusip,
               COALESCE(s.ticker, MAX(h.name_of_issuer)) AS ticker,
               COALESCE(s.name, MAX(h.name_of_issuer))   AS name_of_issuer,
               SUM(h.value_thousands)                     AS value_thousands,
               SUM(h.shares)                              AS shares,
               h.put_call
        FROM holdings h
        JOIN filings f  ON f.id = h.filing_id
        JOIN filers fi  ON fi.cik = f.cik
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE f.period_of_report = ?
          AND h.value_thousands > 0
          AND f.id = (
              SELECT f2.id FROM filings f2
              WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
              ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
          )
        GROUP BY f.cik, fi.name, h.cusip, h.put_call, s.ticker
        ORDER BY value_thousands DESC
        """,
        conn, params=(period,),
    )


@st.cache_data(ttl=300)
def load_filer_periods(cik: str) -> list[str]:
    conn = db_conn()
    rows = conn.execute(
        "SELECT DISTINCT period_of_report FROM filings WHERE cik = ? ORDER BY period_of_report DESC",
        (cik,),
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def load_conviction_scores(period: str, min_filers: int) -> pd.DataFrame:
    conn = db_conn()
    return pd.read_sql(
        """
        WITH latest_filings AS (
            -- One row per filer: the most recently filed filing for this period
            SELECT f.id, f.cik
            FROM filings f
            WHERE f.period_of_report = ?
              AND f.id = (
                  SELECT f2.id FROM filings f2
                  WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
                  ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
              )
        ),
        prior_period AS (
            SELECT MAX(period_of_report) AS period
            FROM filings
            WHERE period_of_report < ?
        ),
        prior_filings AS (
            SELECT f.id, f.cik FROM filings f
            JOIN prior_period pp ON f.period_of_report = pp.period
            WHERE f.id = (
                SELECT f2.id FROM filings f2
                WHERE f2.cik = f.cik AND f2.period_of_report = f.period_of_report
                ORDER BY f2.filed_date DESC, f2.id DESC LIMIT 1
            )
        ),
        filer_aum AS (
            SELECT lf.cik, SUM(h.value_thousands) AS total_aum
            FROM holdings h
            JOIN latest_filings lf ON lf.id = h.filing_id
            WHERE (h.put_call IS NULL OR h.put_call = '')
              AND h.value_thousands > 0
            GROUP BY lf.cik
        ),
        prior_holdings AS (
            SELECT h.cusip, pf.cik, SUM(h.value_thousands) AS prior_value
            FROM holdings h
            JOIN prior_filings pf ON pf.id = h.filing_id
            WHERE (h.put_call IS NULL OR h.put_call = '') AND h.value_thousands > 0
            GROUP BY h.cusip, pf.cik
        ),
        position_weights AS (
            SELECT
                h.cusip,
                COALESCE(s.name, h.name_of_issuer)           AS name_of_issuer,
                COALESCE(s.ticker, h.name_of_issuer)         AS ticker,
                lf.cik,
                h.value_thousands,
                CAST(h.value_thousands AS REAL) / NULLIF(fa.total_aum, 0) * 100
                    AS portfolio_weight_pct
            FROM holdings h
            JOIN latest_filings lf ON lf.id = h.filing_id
            JOIN filer_aum fa      ON fa.cik = lf.cik
            LEFT JOIN securities s ON s.cusip = h.cusip
            WHERE (h.put_call IS NULL OR h.put_call = '')
              AND h.value_thousands > 0
        ),
        buyer_flags AS (
            SELECT pw.cusip, pw.cik,
                CASE
                    WHEN (SELECT period FROM prior_period) IS NULL THEN NULL
                    WHEN ph.prior_value IS NULL                    THEN 1
                    WHEN pw.value_thousands > ph.prior_value       THEN 1
                    ELSE 0
                END AS is_buyer
            FROM position_weights pw
            LEFT JOIN prior_holdings ph ON ph.cusip = pw.cusip AND ph.cik = pw.cik
        )
        SELECT
            pw.cusip,
            MAX(pw.ticker)         AS ticker,
            MAX(pw.name_of_issuer) AS name_of_issuer,
            COUNT(DISTINCT pw.cik)                               AS num_filers,
            SUM(pw.value_thousands)                              AS total_value_thousands,
            ROUND(AVG(pw.portfolio_weight_pct), 2)               AS avg_weight_pct,
            ROUND(AVG(COALESCE(bf.is_buyer, 0.5)), 2)            AS net_buyer_ratio,
            ROUND(
                COUNT(DISTINCT pw.cik)
                * LOG(1 + AVG(pw.portfolio_weight_pct))
                * (1 + AVG(COALESCE(bf.is_buyer, 0.5))),
            2)                                                   AS conviction_score
        FROM position_weights pw
        LEFT JOIN buyer_flags bf ON bf.cusip = pw.cusip AND bf.cik = pw.cik
        GROUP BY pw.cusip
        HAVING num_filers >= ?
        ORDER BY conviction_score DESC
        """,
        conn,
        params=(period, period, min_filers),
    )


# ────────────────────────────────────────────────────────────────────────────────
# Boot
# ────────────────────────────────────────────────────────────────────────────────

inject_css()

if "search_results" not in st.session_state:
    st.session_state.search_results = []
if "search_query" not in st.session_state:
    st.session_state.search_query = ""

# If a filer was just added, clear the search input before the widget renders
if st.session_state.get("_clear_search"):
    st.session_state["filer_search_input"] = ""
    st.session_state.search_query = ""
    st.session_state.search_results = []
    st.session_state["_clear_search"] = False

filers_df = load_filers()
periods   = load_periods()

# Auto-rerun every 3s while any background job is running
with _ingest_lock:
    _has_active_jobs = any(j["status"] == "ingesting" for j in _ingest_jobs.values())
with _refresh_lock:
    _refresh_running = _refresh_status["running"]
if _has_active_jobs or _refresh_running:
    import time as _time
    _time.sleep(3)
    st.rerun()

if filers_df.empty:
    st.warning("No data found. Run `python -m pipeline.ingest --seed --latest-only` first.")
    st.stop()

# ────────────────────────────────────────────────────────────────────────────────
# Sidebar
# ────────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
<div class="sb-brand">
  <div class="sb-title">13F Holdings</div>
  <div class="sb-sub">Institutional Holdings Explorer</div>
</div>""", unsafe_allow_html=True)

    st.markdown('<div class="sb-sec">View</div>', unsafe_allow_html=True)
    view = st.radio(
        "view",
        ["Single Filer", "Cross-Filer Overview", "Conviction Scores"],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown('<div class="sb-sec">Filters</div>', unsafe_allow_html=True)

    if view == "Single Filer":
        filer_options = dict(zip(filers_df["name"], filers_df["cik"]))
        selected_name = st.selectbox("Institution", list(filer_options.keys()))
        selected_cik  = filer_options[selected_name]

        filer_periods = load_filer_periods(selected_cik)
        if not filer_periods:
            st.warning("No filings for this filer.")
            st.stop()

        selected_period = st.selectbox("Period", filer_periods)
        compare_period  = None
        older_periods   = [p for p in filer_periods if p < selected_period]
        if older_periods:
            compare_period = st.selectbox("Compare to (QoQ)", older_periods, index=0)
    else:
        selected_period = st.selectbox("Period", periods)

    if view == "Conviction Scores":
        min_filers_filter = st.slider("Min institutions", 1, 10, 3)

    st.markdown('<div class="sb-sec">Data</div>', unsafe_allow_html=True)

    with _refresh_lock:
        _rs = dict(_refresh_status)

    if _rs["running"]:
        st.info("Refreshing data in background…")
    else:
        if st.button("Refresh data", help="Ingest latest filings + resolve new CUSIPs"):
            with _refresh_lock:
                _refresh_status.update({"running": True, "done": False, "error": None})
            threading.Thread(target=_run_refresh, daemon=True).start()
            st.rerun()
        if _rs["done"]:
            st.success("Refresh complete.")
            st.cache_data.clear()
            with _refresh_lock:
                _refresh_status["done"] = False
        elif _rs["error"]:
            st.error(f"Refresh failed: {_rs['error']}")
            with _refresh_lock:
                _refresh_status["error"] = None

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
            st.session_state.search_results = search_filers_by_name(search_query.strip())
        else:
            st.session_state.search_results = []

    selected_new_filer = None
    if st.session_state.search_results:
        # Disambiguate duplicate names by appending CIK
        name_counts: dict[str, int] = {}
        for r in st.session_state.search_results:
            name_counts[r["name"]] = name_counts.get(r["name"], 0) + 1
        options = []
        for r in st.session_state.search_results:
            label = r["name"] if name_counts[r["name"]] == 1 else f"{r['name']} (CIK {r['cik']})"
            options.append({"label": label, "cik": r["cik"], "name": r["name"]})
        label_to_option = {o["label"]: o for o in options}
        chosen_label = st.selectbox(
            "Results",
            list(label_to_option.keys()),
            label_visibility="collapsed",
        )
        opt = label_to_option[chosen_label]
        selected_new_filer = {"cik": opt["cik"], "name": opt["name"]}
    elif len(search_query.strip()) >= 3:
        st.caption("No results found.")

    # Determine button disabled state
    tracked_ciks = set(filers_df["cik"].tolist())
    already_tracked = selected_new_filer is not None and selected_new_filer["cik"] in tracked_ciks
    already_ingesting = (
        selected_new_filer is not None
        and selected_new_filer["cik"] in _ingest_jobs
        and _ingest_jobs[selected_new_filer["cik"]]["status"] == "ingesting"
    )

    add_disabled = selected_new_filer is None or already_tracked or already_ingesting
    add_label = "Already tracked" if already_tracked else ("Ingesting..." if already_ingesting else "+ Add & Ingest Full History")

    if st.button(add_label, disabled=add_disabled, use_container_width=True):
        _start_ingest(selected_new_filer["cik"], selected_new_filer["name"])
        st.session_state["_clear_search"] = True
        st.rerun()

    # Show active / recent ingest jobs
    to_remove = []
    with _ingest_lock:
        jobs_snapshot = dict(_ingest_jobs)
    for cik, job in jobs_snapshot.items():
        status = job["status"]
        name = job["filer_name"]
        if status == "ingesting":
            st.info(f"⏳ Ingesting **{name}**...\n\n{job['message']}", icon=None)
        elif status == "done":
            st.success(f"✓ **{name}** added successfully.", icon=None)
            st.cache_data.clear()
            to_remove.append(cik)
        elif status == "error":
            st.error(f"✗ **{name}** failed: {job['message']}", icon=None)
            to_remove.append(cik)
    with _ingest_lock:
        for cik in to_remove:
            _ingest_jobs.pop(cik, None)

    _log_path = Path(__file__).parent / "data" / "refresh.log"
    if _log_path.exists():
        with st.expander("Last refresh log"):
            lines = _log_path.read_text().splitlines()
            st.code("\n".join(lines[-40:]))


# ────────────────────────────────────────────────────────────────────────────────
# Single Filer
# ────────────────────────────────────────────────────────────────────────────────

if view == "Single Filer":
    holdings = load_holdings(selected_cik, selected_period)

    if holdings.empty:
        st.info("No holdings found for this filer / period combination.")
        st.stop()

    equity  = holdings[holdings["put_call"].isna() | (holdings["put_call"] == "")]
    options = holdings[~(holdings["put_call"].isna() | (holdings["put_call"] == ""))]

    total_aum  = equity["value_thousands"].sum()
    num_pos    = equity["cusip"].nunique()
    top_issuer = equity.iloc[0]["name_of_issuer"] if not equity.empty else "—"
    top_ticker = equity.iloc[0]["ticker"]         if not equity.empty else "—"

    hero(period=selected_period, filer=selected_name)

    kpi_row([
        ("Portfolio AUM",         f"${total_aum / 1_000:,.1f}M", "amber", None),
        ("Equity Positions",      str(num_pos),                   "slate", None),
        ("Largest Position",      top_ticker,                     "sage",  top_issuer[:32] + ("…" if len(top_issuer) > 32 else "")),
        ("Options & Derivatives", str(len(options)),              "muted", "puts / calls"),
    ])

    col_left, col_right = st.columns([1, 1], gap="medium")

    with col_left:
        shdr("Portfolio Composition")
        top_n    = st.slider("Top N positions", 5, 30, 15, key="pie_n")
        pie_data = equity.head(top_n).copy()
        other_val = equity.iloc[top_n:]["value_thousands"].sum()
        if other_val > 0:
            pie_data = pd.concat([pie_data, pd.DataFrame([{
                "name_of_issuer": "Other", "ticker": "Other",
                "value_thousands": other_val,
            }])], ignore_index=True)

        fig = px.pie(
            pie_data, names="ticker", values="value_thousands", hole=0.42,
            color_discrete_sequence=[
                "#4A7FA5","#6B8F7A","#C48B3F","#7A6BAF","#4A9490",
                "#B87A3F","#8A9FAF","#B85C4A","#9FAF8A","#AF8A9F",
                "#5C8A9A","#8A5C7A","#7A9A5C","#9A7A5C","#5C7A9A",
            ],
        )
        fig.update_traces(
            textposition="inside", textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>$%{value:,.0f}K — %{percent}<extra></extra>",
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        shdr("Top Holdings by Value")
        bar_n    = st.slider("Top N", 5, 30, 15, key="bar_n")
        bar_data = equity.head(bar_n)
        fig2 = px.bar(
            bar_data, x="value_thousands", y="ticker", orientation="h",
            labels={"value_thousands": "Value ($K)", "ticker": ""},
            color="value_thousands", color_continuous_scale=CS_SLATE,
            hover_data={"name_of_issuer": True},
        )
        fig2.update_layout(yaxis={"autorange": "reversed"}, coloraxis_showscale=False)
        fig2.update_traces(hovertemplate="<b>%{y}</b><br>$%{x:,.0f}K<extra></extra>")
        st.plotly_chart(fig2, use_container_width=True)

    # QoQ changes
    if compare_period:
        st.divider()
        shdr("Quarter-over-Quarter Changes", tag=f"{compare_period} → {selected_period}")

        old_h  = load_holdings(selected_cik, compare_period)
        old_eq = old_h[old_h["put_call"].isna() | (old_h["put_call"] == "")]

        merged = equity[["cusip","ticker","name_of_issuer","value_thousands"]].merge(
            old_eq[["cusip","ticker","name_of_issuer","value_thousands"]].rename(columns={
                "value_thousands": "old_value",
                "ticker":          "old_ticker",
                "name_of_issuer":  "old_name",
            }),
            on="cusip", how="outer",
        )
        # Closed positions have no current ticker/name — fill from the prior period
        merged["ticker"]        = merged["ticker"].fillna(merged["old_ticker"]).fillna(merged["cusip"])
        merged["name_of_issuer"]= merged["name_of_issuer"].fillna(merged["old_name"]).fillna(merged["cusip"])
        merged["new_value"]  = merged["value_thousands"].fillna(0)
        merged["old_value"]  = merged["old_value"].fillna(0)
        merged["change"]     = merged["new_value"] - merged["old_value"]
        merged["pct_change"] = merged.apply(
            lambda r: (r["change"] / r["old_value"] * 100) if r["old_value"] > 0 else None,
            axis=1,
        )
        merged["status"] = merged.apply(
            lambda r: "New"       if r["old_value"] == 0 else
                      "Closed"    if r["new_value"] == 0 else
                      "Increased" if r["change"] > 0     else "Decreased",
            axis=1,
        )
        merged = merged.sort_values("change", key=abs, ascending=False)

        top_changes = merged.head(20).copy()
        bar_colors  = top_changes["status"].map({
            "New": "#5B8A6E", "Increased": "#5B8A6E",
            "Decreased": "#B85C4A", "Closed": "#B85C4A",
        })
        fig3 = go.Figure(go.Bar(
            x=top_changes["change"],
            y=top_changes["ticker"],
            orientation="h",
            marker_color=bar_colors,
            text=top_changes["status"],
            textposition="auto",
            customdata=top_changes["name_of_issuer"],
            hovertemplate="<b>%{customdata}</b><br>Change: $%{x:,.0f}K<extra></extra>",
        ))
        fig3.update_layout(
            xaxis_title="Change in Value ($K)",
            yaxis={"autorange": "reversed"},
        )
        st.plotly_chart(fig3, use_container_width=True)

        s = merged["status"].value_counts()
        chg_badges(
            new=s.get("New", 0),
            closed=s.get("Closed", 0),
            increased=s.get("Increased", 0),
            decreased=s.get("Decreased", 0),
        )

    # Full table
    st.divider()
    shdr("Full Holdings Table", tag=f"{num_pos} positions")

    display = equity.copy()
    display["value_millions"] = (display["value_thousands"] / 1_000).round(2)
    display["weight_%"]       = (display["value_thousands"] / total_aum * 100).round(2)
    display["ticker_label"]   = display["raw_ticker"].fillna("—")
    st.dataframe(
        display[["ticker_label","name_of_issuer","cusip","title_of_class",
                 "value_millions","weight_%","shares"]].rename(columns={
            "ticker_label":   "Ticker",
            "name_of_issuer": "Issuer",
            "cusip":          "CUSIP",
            "title_of_class": "Class",
            "value_millions": "Value ($M)",
            "weight_%":       "Weight %",
            "shares":         "Shares",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ────────────────────────────────────────────────────────────────────────────────
# Conviction Scores
# ────────────────────────────────────────────────────────────────────────────────

elif view == "Conviction Scores":
    hero(period=selected_period)

    st.markdown("""
<div class="formula-card">
  <b>Conviction Score</b> = num_institutions × log(1 + avg_portfolio_weight%) × (1 + net_buyer_ratio)<br>
  Rewards securities that are <em>widely held</em>, carry <em>meaningful position sizes</em>, and are being <em>bought/increased</em> vs sold.
  Net buyer ratio = fraction of holders who opened or grew their position vs prior quarter (0.5 default when no prior data).
</div>""", unsafe_allow_html=True)

    scores_df = load_conviction_scores(selected_period, min_filers_filter)

    if scores_df.empty:
        st.info("No securities meet the minimum institution threshold for this period.")
        st.stop()

    top_n_scores = st.slider("Top N", 10, 50, 25, key="scores_n")
    plot_df      = scores_df.head(top_n_scores).copy()

    shdr("Conviction Ranking", tag=f"min {min_filers_filter} institutions")
    fig_scores = px.bar(
        plot_df, x="conviction_score", y="ticker", orientation="h",
        color="conviction_score", color_continuous_scale=CS_AMBER,
        hover_data={"name_of_issuer": True, "num_filers": True, "avg_weight_pct": True},
        labels={"conviction_score": "Score", "ticker": "",
                "num_filers": "# Institutions", "avg_weight_pct": "Avg Weight %"},
    )
    fig_scores.update_layout(yaxis={"autorange": "reversed"}, coloraxis_showscale=False)
    st.plotly_chart(fig_scores, use_container_width=True)

    st.divider()
    col_s1, col_s2 = st.columns([1, 1], gap="medium")

    with col_s1:
        shdr("Score vs. Avg Weight")
        fig_scatter = px.scatter(
            scores_df.head(50),
            x="avg_weight_pct", y="conviction_score",
            size="num_filers", color="num_filers",
            hover_name="ticker", hover_data={"name_of_issuer": True},
            color_continuous_scale=CS_SLATE,
            labels={"avg_weight_pct": "Avg Portfolio Weight %",
                    "conviction_score": "Conviction Score",
                    "num_filers": "# Institutions"},
        )
        fig_scatter.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_s2:
        shdr("Breadth vs. Concentration")
        fig_bv = px.scatter(
            scores_df.head(50),
            x="num_filers", y="avg_weight_pct",
            size="total_value_thousands",
            hover_name="ticker",
            hover_data={"name_of_issuer": True, "conviction_score": True},
            color="conviction_score", color_continuous_scale=CS_AMBER,
            labels={"num_filers": "# Institutions Holding",
                    "avg_weight_pct": "Avg Portfolio Weight %",
                    "conviction_score": "Score"},
        )
        fig_bv.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_bv, use_container_width=True)

    st.divider()
    shdr("Full Conviction Table", tag=f"{len(scores_df)} securities")
    display_scores = scores_df.copy()
    display_scores["total_value_billions"] = (
        display_scores["total_value_thousands"] / 1_000_000
    ).round(3)
    st.dataframe(
        display_scores[["ticker","name_of_issuer","num_filers",
                        "avg_weight_pct","net_buyer_ratio","total_value_billions","conviction_score"]].rename(columns={
            "ticker":               "Ticker",
            "name_of_issuer":       "Issuer",
            "num_filers":           "# Institutions",
            "avg_weight_pct":       "Avg Weight %",
            "net_buyer_ratio":      "Net Buyer Ratio",
            "total_value_billions": "Total Value ($B)",
            "conviction_score":     "Conviction Score",
        }),
        use_container_width=True,
        hide_index=True,
    )


# ────────────────────────────────────────────────────────────────────────────────
# Cross-Filer Overview
# ────────────────────────────────────────────────────────────────────────────────

elif view == "Cross-Filer Overview":
    all_h = load_all_holdings(selected_period)

    if all_h.empty:
        st.info("No holdings for this period.")
        st.stop()

    equity_all = all_h[all_h["put_call"].isna() | (all_h["put_call"] == "")]

    hero(period=selected_period)

    shdr("AUM by Institution")
    aum_by_filer = (
        equity_all.groupby("filer_name")["value_thousands"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    aum_by_filer["aum_billions"] = aum_by_filer["value_thousands"] / 1_000_000

    fig_aum = px.bar(
        aum_by_filer, x="filer_name", y="aum_billions",
        labels={"filer_name": "", "aum_billions": "AUM ($B)"},
        color="aum_billions", color_continuous_scale=CS_SLATE,
    )
    fig_aum.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30)
    fig_aum.update_traces(hovertemplate="<b>%{x}</b><br>$%{y:.2f}B<extra></extra>")
    st.plotly_chart(fig_aum, use_container_width=True)

    st.divider()

    breadth = (
        equity_all.groupby(["cusip", "ticker", "name_of_issuer"])
        .agg(num_filers=("cik", "nunique"), total_value=("value_thousands", "sum"))
        .sort_values("num_filers", ascending=False)
        .head(20)
        .reset_index()
    )

    col_a, col_b = st.columns([1, 1], gap="medium")

    with col_a:
        shdr("Most Widely Held", tag="by # institutions")
        fig_b = px.bar(
            breadth, x="num_filers", y="ticker", orientation="h",
            labels={"num_filers": "# Institutions", "ticker": ""},
            color="num_filers", color_continuous_scale=CS_SAGE,
            hover_data={"name_of_issuer": True},
        )
        fig_b.update_layout(yaxis={"autorange": "reversed"}, coloraxis_showscale=False)
        st.plotly_chart(fig_b, use_container_width=True)

    with col_b:
        shdr("Highest Aggregate Value", tag="by total AUM held")
        fig_v = px.bar(
            breadth.sort_values("total_value", ascending=False).head(20),
            x="total_value", y="ticker", orientation="h",
            labels={"total_value": "Aggregate Value ($K)", "ticker": ""},
            color="total_value", color_continuous_scale=CS_AMBER,
            hover_data={"name_of_issuer": True},
        )
        fig_v.update_layout(yaxis={"autorange": "reversed"}, coloraxis_showscale=False)
        st.plotly_chart(fig_v, use_container_width=True)

    st.divider()
    shdr("Overlap Heatmap", tag="top 15 securities × institutions")

    top_cusips   = breadth.head(15)["cusip"].tolist()
    heatmap_data = (
        equity_all[equity_all["cusip"].isin(top_cusips)]
        .groupby(["filer_name", "ticker"])["value_thousands"]
        .sum()
        .reset_index()
        .pivot(index="filer_name", columns="ticker", values="value_thousands")
        .fillna(0)
    )
    fig_heat = px.imshow(
        heatmap_data,
        color_continuous_scale=CS_SAGE,
        labels={"color": "Value ($K)"},
        aspect="auto",
    )
    fig_heat.update_layout(
        xaxis_tickangle=-40,
        coloraxis_colorbar=dict(title="$K"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    st.divider()
    shdr("Aggregate Holdings Table", tag=f"{len(breadth)} securities")
    st.dataframe(
        breadth.rename(columns={
            "name_of_issuer": "Issuer",
            "cusip":          "CUSIP",
            "num_filers":     "# Institutions",
            "total_value":    "Aggregate Value ($K)",
        }),
        use_container_width=True,
        hide_index=True,
    )
