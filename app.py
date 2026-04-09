"""
13F Holdings Intelligence — Streamlit app.

Run with:
    streamlit run app.py
"""

import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from pipeline.database import DB_PATH, get_connection

# ────────────────────────────────────────────────────────────────────────────────
# Page config
# ────────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="13F Intelligence",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ────────────────────────────────────────────────────────────────────────────────
# Plotly dark template
# ────────────────────────────────────────────────────────────────────────────────

_BG   = "#0C1220"
_GRID = "#1D2B40"
_FG   = "#C8D8E8"
_DIM  = "#7A95B0"

pio.templates["13f"] = go.layout.Template(
    layout=go.Layout(
        paper_bgcolor=_BG,
        plot_bgcolor=_BG,
        font=dict(family="'JetBrains Mono', monospace", color=_FG, size=11),
        colorway=["#4B9EFF", "#2DD4BF", "#D4A843", "#34D399", "#F87171",
                  "#A78BFA", "#FB923C", "#60A5FA", "#FCD34D", "#6EE7B7"],
        xaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            tickcolor="#07090F", zerolinecolor=_GRID,
            tickfont=dict(color=_DIM, size=10),
        ),
        yaxis=dict(
            gridcolor=_GRID, linecolor=_GRID,
            tickcolor="#07090F", zerolinecolor=_GRID,
            tickfont=dict(color=_DIM, size=10),
        ),
        hoverlabel=dict(
            bgcolor="#182030", bordercolor=_GRID,
            font=dict(family="'JetBrains Mono', monospace", color=_FG, size=11),
        ),
        legend=dict(
            bgcolor=_BG, bordercolor=_GRID, borderwidth=1,
            font=dict(color=_DIM),
        ),
        margin=dict(t=24, b=24, l=8, r=8),
        coloraxis=dict(colorbar=dict(
            bgcolor=_BG, tickcolor=_DIM,
            tickfont=dict(color=_DIM, size=10),
            outlinecolor=_GRID, outlinewidth=1,
        )),
    )
)
pio.templates.default = "13f"

# Custom color scales
CS_BLUE  = ["#0C1220", "#1a3a6e", "#2563eb", "#4B9EFF"]
CS_TEAL  = ["#0C1220", "#0d4040", "#0d9488", "#2DD4BF"]
CS_GOLD  = ["#0C1220", "#4d3200", "#a06000", "#D4A843"]
CS_GREEN = ["#0C1220", "#064e3b", "#10b981", "#34D399"]

# ────────────────────────────────────────────────────────────────────────────────
# CSS injection
# ────────────────────────────────────────────────────────────────────────────────

def inject_css() -> None:
    st.markdown(r"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,500;0,600;1,300;1,400&family=Outfit:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Variables ─────────────────────────────────────────────────── */
:root {
    --bg0:    #07090F;
    --bg1:    #0C1220;
    --bg2:    #111825;
    --bg3:    #182030;
    --bdr:    #1D2B40;
    --bdr-hi: #2A3F5C;
    --t0:     #C8D8E8;
    --t1:     #7A95B0;
    --t2:     #4A6278;
    --gold:   #D4A843;
    --blue:   #4B9EFF;
    --teal:   #2DD4BF;
    --green:  #34D399;
    --red:    #F87171;
}

/* ── Shell ─────────────────────────────────────────────────────── */
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.main,
.main .block-container                 { background: var(--bg0) !important; }
.main .block-container                 { padding-top: 1.2rem !important; max-width: 1440px !important; }

/* ── Sidebar ───────────────────────────────────────────────────── */
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div,
[data-testid="stSidebarContent"]       { background: var(--bg1) !important; }
[data-testid="stSidebar"]              { border-right: 1px solid var(--bdr) !important; }

/* ── Typography ────────────────────────────────────────────────── */
body, p, li, span                      { font-family: 'Outfit', sans-serif; }
h1, h2, h3, h4                         { font-family: 'Cormorant Garamond', serif !important; color: var(--t0) !important; }
h1                                     { font-weight: 300 !important; font-size: 2.4rem !important; letter-spacing: 0.1em !important; }
h2                                     { font-weight: 400 !important; font-size: 1.55rem !important; letter-spacing: 0.04em !important; margin: 1rem 0 0.25rem !important; }
h3                                     { font-weight: 400 !important; font-size: 1.1rem !important; }
p, .stMarkdown p                       { font-family: 'Outfit', sans-serif !important; color: var(--t0) !important; font-size: 0.88rem !important; }
code                                   { font-family: 'JetBrains Mono', monospace !important; font-size: 0.8rem !important; }

/* ── Selectbox ─────────────────────────────────────────────────── */
[data-testid="stSelectbox"] div[data-baseweb="select"] > div:first-child {
    background: var(--bg2) !important;
    border-color: var(--bdr) !important;
    color: var(--t0) !important;
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.85rem !important;
}
[data-testid="stSelectbox"] svg        { fill: var(--t1) !important; }
[data-testid="stSelectbox"] label,
[data-testid="stMultiSelect"] label    {
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.7rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.14em !important;
    color: var(--t2) !important;
    font-weight: 500 !important;
}

/* Dropdown menu */
[data-baseweb="popover"],
[data-baseweb="menu"]                  { background: var(--bg2) !important; border: 1px solid var(--bdr) !important; }
[data-baseweb="option"]                { background: var(--bg2) !important; color: var(--t0) !important; font-family: 'Outfit', sans-serif !important; font-size: 0.85rem !important; }
[data-baseweb="option"]:hover          { background: var(--bg3) !important; }

/* ── Slider ────────────────────────────────────────────────────── */
[data-testid="stSlider"] label         {
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.7rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.14em !important;
    color: var(--t2) !important;
    font-weight: 500 !important;
}
[data-testid="stSlider"] [role="slider"]{ box-shadow: 0 0 0 2px var(--blue) !important; }

/* ── Radio ─────────────────────────────────────────────────────── */
[data-testid="stRadio"] > label        {
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.7rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.14em !important;
    color: var(--t2) !important;
    font-weight: 600 !important;
}
[data-testid="stRadio"] [data-testid="stMarkdownContainer"] p {
    font-size: 0.86rem !important;
    letter-spacing: 0 !important;
    text-transform: none !important;
    color: var(--t0) !important;
}

/* ── Buttons ───────────────────────────────────────────────────── */
.stButton > button {
    background: var(--bg2) !important;
    border: 1px solid var(--bdr) !important;
    color: var(--t0) !important;
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    border-radius: 2px !important;
    padding: 0.45rem 1.2rem !important;
    transition: all 0.18s ease !important;
}
.stButton > button:hover {
    border-color: var(--blue) !important;
    color: var(--blue) !important;
    background: rgba(75,158,255,0.08) !important;
}

/* ── Dividers ──────────────────────────────────────────────────── */
hr, [data-testid="stDivider"] hr       { border-color: var(--bdr) !important; margin: 1rem 0 !important; }

/* ── Default Metrics ───────────────────────────────────────────── */
[data-testid="metric-container"],
[data-testid="stMetric"]               {
    background: var(--bg1) !important;
    border: 1px solid var(--bdr) !important;
    border-radius: 3px !important;
    padding: 1rem 1.2rem !important;
}
[data-testid="stMetricLabel"] p        {
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.68rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.14em !important;
    color: var(--t2) !important;
    font-weight: 500 !important;
}
[data-testid="stMetricValue"]          { font-family: 'JetBrains Mono', monospace !important; font-size: 1.4rem !important; color: var(--t0) !important; }
[data-testid="stMetricDelta"]          { font-family: 'JetBrains Mono', monospace !important; font-size: 0.75rem !important; }

/* ── Dataframe ─────────────────────────────────────────────────── */
.stDataFrame > div                     {
    background: var(--bg1) !important;
    border: 1px solid var(--bdr) !important;
    border-radius: 3px !important;
    overflow: hidden !important;
}

/* ── Expander ──────────────────────────────────────────────────── */
[data-testid="stExpander"]             {
    background: var(--bg1) !important;
    border: 1px solid var(--bdr) !important;
    border-radius: 3px !important;
}
[data-testid="stExpander"] summary     { color: var(--t1) !important; font-family: 'Outfit', sans-serif !important; font-size: 0.8rem !important; }
[data-testid="stExpander"] summary:hover { color: var(--t0) !important; }

/* ── Alerts ────────────────────────────────────────────────────── */
[data-testid="stAlert"]                {
    background: var(--bg2) !important;
    border: 1px solid var(--bdr) !important;
    border-radius: 3px !important;
}
[data-testid="stAlert"] p              { color: var(--t1) !important; }

/* ── Code ──────────────────────────────────────────────────────── */
pre, .stCodeBlock                      { background: var(--bg2) !important; border: 1px solid var(--bdr) !important; }
pre code, .stCodeBlock code            {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.75rem !important;
    color: var(--t0) !important;
}

/* ── Caption ───────────────────────────────────────────────────── */
[data-testid="stCaptionContainer"] p   {
    font-family: 'Outfit', sans-serif !important;
    font-size: 0.75rem !important;
    color: var(--t2) !important;
}

/* ── Scrollbar ─────────────────────────────────────────────────── */
::-webkit-scrollbar                   { width: 5px; height: 5px; }
::-webkit-scrollbar-track             { background: var(--bg0); }
::-webkit-scrollbar-thumb             { background: var(--bdr-hi); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover       { background: var(--t2); }

/* ══════════════════════════════════════════════════════════════ */
/*  Custom HTML Components                                        */
/* ══════════════════════════════════════════════════════════════ */

/* Hero */
.hero {
    display: flex;
    align-items: center;
    gap: 1.2rem;
    padding: 0.6rem 0 1.4rem;
    border-bottom: 1px solid var(--bdr);
    margin-bottom: 0;
    animation: fadein 0.5s ease;
}
.hero-mark {
    font-family: 'Cormorant Garamond', serif;
    font-size: 2.8rem;
    color: var(--gold);
    line-height: 1;
    opacity: 0.85;
    user-select: none;
}
.hero-wordmark {
    font-family: 'Cormorant Garamond', serif;
    font-size: 1.85rem;
    font-weight: 300;
    color: var(--t0);
    letter-spacing: 0.18em;
    line-height: 1;
    margin: 0;
}
.hero-sub {
    font-family: 'Outfit', sans-serif;
    font-size: 0.65rem;
    color: var(--t2);
    letter-spacing: 0.24em;
    text-transform: uppercase;
    margin-top: 0.35rem;
}
.hero-context {
    margin-left: auto;
    text-align: right;
    flex-shrink: 0;
}
.hero-context-label {
    font-family: 'Outfit', sans-serif;
    font-size: 0.6rem;
    text-transform: uppercase;
    letter-spacing: 0.2em;
    color: var(--t2);
}
.hero-context-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.05rem;
    color: var(--gold);
    letter-spacing: 0.04em;
    margin-top: 0.2rem;
}

/* KPI grid */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.65rem;
    margin: 1.2rem 0 1.4rem;
}
.kpi-card {
    background: var(--bg1);
    border: 1px solid var(--bdr);
    border-radius: 3px;
    padding: 1rem 1.1rem 0.9rem 1.2rem;
    position: relative;
    overflow: hidden;
    transition: border-color 0.2s ease;
}
.kpi-card:hover { border-color: var(--bdr-hi); }
.kpi-bar {
    position: absolute;
    left: 0; top: 0;
    width: 2px; height: 100%;
    border-radius: 2px 0 0 2px;
}
.kpi-label {
    font-family: 'Outfit', sans-serif;
    font-size: 0.63rem;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: var(--t2);
    margin-bottom: 0.5rem;
    font-weight: 500;
}
.kpi-val {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.45rem;
    font-weight: 500;
    line-height: 1.1;
}
.kpi-note {
    font-family: 'Outfit', sans-serif;
    font-size: 0.68rem;
    color: var(--t2);
    margin-top: 0.3rem;
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
    font-family: 'Cormorant Garamond', serif;
    font-size: 1.22rem;
    font-weight: 400;
    color: var(--t0);
    white-space: nowrap;
    letter-spacing: 0.02em;
}
.shdr-line {
    flex: 1;
    height: 1px;
    background: var(--bdr);
}
.shdr-tag {
    font-family: 'Outfit', sans-serif;
    font-size: 0.6rem;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: var(--t2);
    background: var(--bg2);
    border: 1px solid var(--bdr);
    padding: 0.18rem 0.55rem;
    border-radius: 2px;
    white-space: nowrap;
}

/* Conviction formula callout */
.formula-card {
    background: var(--bg1);
    border: 1px solid var(--bdr);
    border-left: 2px solid var(--gold);
    border-radius: 0 3px 3px 0;
    padding: 0.8rem 1.1rem;
    margin-bottom: 1rem;
    font-family: 'JetBrains Mono', monospace;
    font-size: 0.74rem;
    color: var(--t1);
    line-height: 1.75;
}
.formula-card b { color: var(--gold); font-weight: 500; }

/* QoQ change badges */
.chg-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 0.65rem;
    margin-top: 0.9rem;
}
.chg-card {
    background: var(--bg1);
    border: 1px solid var(--bdr);
    border-radius: 3px;
    padding: 0.85rem 1rem;
    text-align: center;
}
.chg-card-label {
    font-family: 'Outfit', sans-serif;
    font-size: 0.62rem;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: var(--t2);
    margin-bottom: 0.35rem;
}
.chg-card-val {
    font-family: 'JetBrains Mono', monospace;
    font-size: 1.7rem;
    font-weight: 500;
    line-height: 1;
}

/* Sidebar brand */
.sb-brand {
    padding: 1rem 0 0.9rem;
    border-bottom: 1px solid var(--bdr);
    margin-bottom: 0.6rem;
}
.sb-mark {
    font-family: 'Cormorant Garamond', serif;
    font-size: 1.15rem;
    color: var(--gold);
    letter-spacing: 0.14em;
}
.sb-name {
    font-family: 'Outfit', sans-serif;
    font-size: 0.56rem;
    text-transform: uppercase;
    letter-spacing: 0.3em;
    color: var(--t2);
    margin-top: 0.2rem;
}

/* Sidebar section labels */
.sb-sec {
    font-family: 'Outfit', sans-serif;
    font-size: 0.58rem;
    text-transform: uppercase;
    letter-spacing: 0.24em;
    color: var(--t2);
    padding: 0.9rem 0 0.2rem;
    border-top: 1px solid var(--bdr);
    margin-top: 0.9rem;
    font-weight: 600;
}

/* Animations */
@keyframes fadein {
    from { opacity: 0; transform: translateY(-6px); }
    to   { opacity: 1; transform: translateY(0); }
}
</style>
""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# HTML component helpers
# ────────────────────────────────────────────────────────────────────────────────

_ACCENT = {
    "gold":  "#D4A843",
    "blue":  "#4B9EFF",
    "teal":  "#2DD4BF",
    "green": "#34D399",
    "red":   "#F87171",
    "dim":   "#4A6278",
}


def kpi_row(cards: list[tuple]) -> None:
    """Render a 4-column KPI strip. cards = [(label, value, color_key, note), ...]"""
    html = '<div class="kpi-grid">'
    for label, value, color, note in cards:
        c = _ACCENT.get(color, _ACCENT["blue"])
        note_html = f'<div class="kpi-note">{note}</div>' if note else ""
        html += f"""
  <div class="kpi-card">
    <div class="kpi-bar" style="background:{c}"></div>
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
    ctx_html = ""
    if period:
        lbl = filer if filer else "Period"
        ctx_html = f"""
  <div class="hero-context">
    <div class="hero-context-label">{lbl}</div>
    <div class="hero-context-value">{period}</div>
  </div>"""
    st.markdown(f"""
<div class="hero">
  <div class="hero-mark">◈</div>
  <div>
    <div class="hero-wordmark">13F INTELLIGENCE</div>
    <div class="hero-sub">SEC Institutional Holdings Analysis</div>
  </div>
  {ctx_html}
</div>""", unsafe_allow_html=True)


def chg_badges(new: int, closed: int, increased: int, decreased: int) -> None:
    st.markdown(f"""
<div class="chg-grid">
  <div class="chg-card">
    <div class="chg-card-label">New</div>
    <div class="chg-card-val" style="color:#34D399">{new}</div>
  </div>
  <div class="chg-card">
    <div class="chg-card-label">Closed</div>
    <div class="chg-card-val" style="color:#F87171">{closed}</div>
  </div>
  <div class="chg-card">
    <div class="chg-card-label">Increased</div>
    <div class="chg-card-val" style="color:#34D399">{increased}</div>
  </div>
  <div class="chg-card">
    <div class="chg-card-label">Decreased</div>
    <div class="chg-card-val" style="color:#F87171">{decreased}</div>
  </div>
</div>""", unsafe_allow_html=True)


# ────────────────────────────────────────────────────────────────────────────────
# DB helpers (cached)
# ────────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def db_conn():
    return get_connection()


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
        SELECT h.cusip, COALESCE(s.ticker, h.cusip) AS ticker,
               COALESCE(s.name, h.name_of_issuer)   AS name_of_issuer,
               h.title_of_class, h.value_thousands,
               h.shares, h.put_call, h.investment_discretion
        FROM holdings h
        JOIN filings f ON f.id = h.filing_id
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE f.cik = ? AND f.period_of_report = ?
        ORDER BY h.value_thousands DESC
        """,
        conn, params=(cik, period),
    )


@st.cache_data(ttl=300)
def load_all_holdings(period: str) -> pd.DataFrame:
    conn = db_conn()
    return pd.read_sql(
        """
        SELECT f.cik, fi.name AS filer_name,
               h.cusip, COALESCE(s.ticker, h.cusip) AS ticker,
               COALESCE(s.name, h.name_of_issuer)   AS name_of_issuer,
               h.value_thousands, h.shares, h.put_call
        FROM holdings h
        JOIN filings f  ON f.id = h.filing_id
        JOIN filers fi  ON fi.cik = f.cik
        LEFT JOIN securities s ON s.cusip = h.cusip
        WHERE f.period_of_report = ?
        ORDER BY h.value_thousands DESC
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
        WITH filer_aum AS (
            SELECT f.cik, SUM(h.value_thousands) AS total_aum
            FROM holdings h
            JOIN filings f ON f.id = h.filing_id
            WHERE f.period_of_report = ?
              AND (h.put_call IS NULL OR h.put_call = '')
            GROUP BY f.cik
        ),
        position_weights AS (
            SELECT
                h.cusip,
                COALESCE(s.name, h.name_of_issuer) AS name_of_issuer,
                COALESCE(s.ticker, h.cusip)         AS ticker,
                f.cik,
                h.value_thousands,
                CAST(h.value_thousands AS REAL) / NULLIF(fa.total_aum, 0) * 100
                    AS portfolio_weight_pct
            FROM holdings h
            JOIN filings   f  ON f.id = h.filing_id
            JOIN filer_aum fa ON fa.cik = f.cik
            LEFT JOIN securities s ON s.cusip = h.cusip
            WHERE f.period_of_report = ?
              AND (h.put_call IS NULL OR h.put_call = '')
        )
        SELECT
            cusip,
            ticker,
            name_of_issuer,
            COUNT(DISTINCT cik)                              AS num_filers,
            SUM(value_thousands)                             AS total_value_thousands,
            ROUND(AVG(portfolio_weight_pct), 2)              AS avg_weight_pct,
            ROUND(
                COUNT(DISTINCT cik)
                * LOG(1 + AVG(portfolio_weight_pct))
                * 1.0,
            2)                                               AS conviction_score
        FROM position_weights
        GROUP BY cusip, ticker, name_of_issuer
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

filers_df = load_filers()
periods   = load_periods()

if filers_df.empty:
    st.warning("No data found. Run `python -m pipeline.ingest --seed --latest-only` first.")
    st.stop()

# ────────────────────────────────────────────────────────────────────────────────
# Sidebar
# ────────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
<div class="sb-brand">
  <div class="sb-mark">◈ 13F</div>
  <div class="sb-name">Holdings Intelligence</div>
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
        filer_options  = dict(zip(filers_df["name"], filers_df["cik"]))
        selected_name  = st.selectbox("Institution", list(filer_options.keys()))
        selected_cik   = filer_options[selected_name]

        filer_periods = load_filer_periods(selected_cik)
        if not filer_periods:
            st.warning("No filings for this filer.")
            st.stop()

        selected_period = st.selectbox("Period", filer_periods)

        compare_period = None
        if len(filer_periods) > 1:
            compare_period = st.selectbox(
                "Compare to (QoQ)", filer_periods[1:], index=0
            )
    else:
        selected_period = st.selectbox("Period", periods)

    if view == "Conviction Scores":
        min_filers_filter = st.slider("Min Institutions", 1, 10, 3)

    st.markdown('<div class="sb-sec">Data</div>', unsafe_allow_html=True)

    _refresh_script = Path(__file__).parent / "refresh.sh"
    if st.button("Refresh Data", help="Ingest latest filings + resolve new CUSIPs"):
        with st.spinner("Refreshing…"):
            result = subprocess.run(
                ["bash", str(_refresh_script)],
                capture_output=True, text=True,
            )
        if result.returncode == 0:
            st.success("Refresh complete.")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Refresh failed.")
            st.code(result.stderr or result.stdout)

    _log_path = Path(__file__).parent / "data" / "refresh.log"
    if _log_path.exists():
        with st.expander("Last refresh log"):
            lines = _log_path.read_text().splitlines()
            st.code("\n".join(lines[-40:]))


# ────────────────────────────────────────────────────────────────────────────────
# Single Filer View
# ────────────────────────────────────────────────────────────────────────────────

if view == "Single Filer":
    holdings = load_holdings(selected_cik, selected_period)

    if holdings.empty:
        st.info("No holdings found for this filer / period combination.")
        st.stop()

    equity  = holdings[holdings["put_call"].isna() | (holdings["put_call"] == "")]
    options = holdings[~(holdings["put_call"].isna() | (holdings["put_call"] == ""))]

    total_aum    = equity["value_thousands"].sum()
    num_pos      = equity["cusip"].nunique()
    top_issuer   = equity.iloc[0]["name_of_issuer"] if not equity.empty else "—"
    top_ticker   = equity.iloc[0]["ticker"]         if not equity.empty else "—"

    hero(period=selected_period, filer=selected_name)

    kpi_row([
        ("Portfolio AUM",        f"${total_aum / 1_000:,.1f}M", "gold",  None),
        ("Equity Positions",     str(num_pos),                   "blue",  None),
        ("Largest Position",     top_ticker,                     "teal",  top_issuer[:32] + ("…" if len(top_issuer) > 32 else "")),
        ("Options & Derivatives",str(len(options)),              "dim",   "puts / calls"),
    ])

    col_left, col_right = st.columns([1, 1], gap="medium")

    with col_left:
        shdr("Portfolio Composition")
        top_n = st.slider("Top N positions", 5, 30, 15, key="pie_n")
        pie_data  = equity.head(top_n).copy()
        other_val = equity.iloc[top_n:]["value_thousands"].sum()
        if other_val > 0:
            pie_data = pd.concat([pie_data, pd.DataFrame([{
                "name_of_issuer": "Other", "ticker": "Other",
                "value_thousands": other_val,
            }])], ignore_index=True)

        fig = px.pie(
            pie_data, names="ticker", values="value_thousands", hole=0.44,
            color_discrete_sequence=[
                "#4B9EFF","#2DD4BF","#D4A843","#34D399","#A78BFA",
                "#FB923C","#F87171","#60A5FA","#FCD34D","#6EE7B7",
                "#93C5FD","#C4B5FD","#FCA5A5","#5EEAD4","#FDE68A",
            ],
        )
        fig.update_traces(
            textposition="inside",
            textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>$%{value:,.0f}K<br>%{percent}<extra></extra>",
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
            color="value_thousands", color_continuous_scale=CS_BLUE,
            hover_data={"name_of_issuer": True},
        )
        fig2.update_layout(yaxis={"autorange": "reversed"}, coloraxis_showscale=False)
        fig2.update_traces(hovertemplate="<b>%{y}</b><br>$%{x:,.0f}K<extra></extra>")
        st.plotly_chart(fig2, use_container_width=True)

    # ── QoQ Changes ──────────────────────────────────────────────────────────

    if compare_period:
        st.divider()
        shdr("Quarter-over-Quarter Changes", tag=f"{compare_period} → {selected_period}")

        old_h  = load_holdings(selected_cik, compare_period)
        old_eq = old_h[old_h["put_call"].isna() | (old_h["put_call"] == "")]

        merged = equity[["cusip", "ticker", "name_of_issuer", "value_thousands"]].merge(
            old_eq[["cusip", "value_thousands"]].rename(columns={"value_thousands": "old_value"}),
            on="cusip", how="outer",
        )
        merged["new_value"] = merged["value_thousands"].fillna(0)
        merged["old_value"] = merged["old_value"].fillna(0)
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
            "New": "#34D399", "Increased": "#34D399",
            "Decreased": "#F87171", "Closed": "#F87171",
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

    # ── Full Holdings Table ────────────────────────────────────────────────────

    st.divider()
    shdr("Full Holdings Table", tag=f"{num_pos} positions")

    display = equity.copy()
    display["value_millions"] = (display["value_thousands"] / 1_000).round(2)
    display["weight_%"]       = (display["value_thousands"] / total_aum * 100).round(2)
    st.dataframe(
        display[["ticker","name_of_issuer","cusip","title_of_class",
                 "value_millions","weight_%","shares"]].rename(columns={
            "ticker":         "Ticker",
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
# Conviction Scores View
# ────────────────────────────────────────────────────────────────────────────────

elif view == "Conviction Scores":
    hero(period=selected_period)

    st.markdown("""
<div class="formula-card">
  <b>Conviction Score</b> = num_institutions × log(1 + avg_portfolio_weight%)<br>
  Rewards securities that are <em>widely held</em> AND carry <em>meaningful position sizes</em> relative to each filer's AUM.
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
        color="conviction_score", color_continuous_scale=CS_GOLD,
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
            color_continuous_scale=CS_BLUE,
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
            color="conviction_score", color_continuous_scale=CS_GOLD,
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
                        "avg_weight_pct","total_value_billions","conviction_score"]].rename(columns={
            "ticker":               "Ticker",
            "name_of_issuer":       "Issuer",
            "num_filers":           "# Institutions",
            "avg_weight_pct":       "Avg Weight %",
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
        color="aum_billions", color_continuous_scale=CS_BLUE,
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
            color="num_filers", color_continuous_scale=CS_TEAL,
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
            color="total_value", color_continuous_scale=CS_GOLD,
            hover_data={"name_of_issuer": True},
        )
        fig_v.update_layout(yaxis={"autorange": "reversed"}, coloraxis_showscale=False)
        st.plotly_chart(fig_v, use_container_width=True)

    # ── Overlap Heatmap ───────────────────────────────────────────────────────

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
        color_continuous_scale=CS_TEAL,
        labels={"color": "Value ($K)"},
        aspect="auto",
    )
    fig_heat.update_layout(
        xaxis_tickangle=-40,
        coloraxis_colorbar=dict(title="$K"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # ── Aggregate Table ───────────────────────────────────────────────────────

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
