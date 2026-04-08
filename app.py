"""
13F Holdings Dashboard — Streamlit app.

Run with:
    streamlit run app.py
"""

import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from pipeline.database import DB_PATH, get_connection

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="13F Holdings Explorer",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📈 13F Holdings Explorer")

# ---------------------------------------------------------------------------
# DB helpers (cached)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

filers_df = load_filers()
periods    = load_periods()

if filers_df.empty:
    st.warning("No data found. Run `python -m pipeline.ingest --seed --latest-only` first.")
    st.stop()

with st.sidebar:
    st.header("Filters")

    view = st.radio("View", ["Single Filer", "Cross-Filer Overview", "Conviction Scores"], index=0)

    if view == "Single Filer":
        filer_options = dict(zip(filers_df["name"], filers_df["cik"]))
        selected_name = st.selectbox("Institution", list(filer_options.keys()))
        selected_cik  = filer_options[selected_name]

        filer_periods = load_filer_periods(selected_cik)
        if not filer_periods:
            st.warning("No filings for this filer.")
            st.stop()

        selected_period = st.selectbox("Period", filer_periods)

        if len(filer_periods) > 1:
            compare_period = st.selectbox(
                "Compare to (QoQ)",
                filer_periods[1:],
                index=0,
            )
        else:
            compare_period = None

    else:
        selected_period = st.selectbox("Period", periods)

    if view == "Conviction Scores":
        min_filers_filter = st.slider("Min institutions holding", 1, 10, 3)

    st.divider()
    st.header("Data")

    _refresh_script = Path(__file__).parent / "refresh.sh"
    if st.button("Refresh Data", help="Ingest latest filings + resolve new CUSIPs"):
        with st.spinner("Running refresh…"):
            result = subprocess.run(
                ["bash", str(_refresh_script)],
                capture_output=True,
                text=True,
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

# ---------------------------------------------------------------------------
# Single Filer View
# ---------------------------------------------------------------------------

if view == "Single Filer":
    holdings = load_holdings(selected_cik, selected_period)

    if holdings.empty:
        st.info("No holdings found for this filer / period combination.")
        st.stop()

    # Strip options/puts for main stats (keep separate)
    equity = holdings[holdings["put_call"].isna() | (holdings["put_call"] == "")]
    options = holdings[~(holdings["put_call"].isna() | (holdings["put_call"] == ""))]

    total_aum   = equity["value_thousands"].sum()
    num_pos     = equity["cusip"].nunique()
    top_holding = equity.iloc[0]["name_of_issuer"] if not equity.empty else "—"

    # ---- KPI row ----
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Portfolio AUM", f"${total_aum/1_000:,.0f}M")
    c2.metric("Positions", num_pos)
    c3.metric("Largest Position", top_holding)
    c4.metric("Options / Puts-Calls", len(options))

    st.divider()

    col_left, col_right = st.columns([1, 1])

    # ---- Pie chart ----
    with col_left:
        st.subheader("Portfolio Composition")
        top_n = st.slider("Show top N positions", 5, 30, 15, key="pie_n")
        pie_data = equity.head(top_n).copy()
        other_val = equity.iloc[top_n:]["value_thousands"].sum()
        if other_val > 0:
            other_row = pd.DataFrame([{
                "name_of_issuer": "Other",
                "value_thousands": other_val,
            }])
            pie_data = pd.concat([pie_data, other_row], ignore_index=True)

        fig = px.pie(
            pie_data,
            names="ticker",
            values="value_thousands",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Plotly,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig, width="stretch")

    # ---- Bar chart top holdings ----
    with col_right:
        st.subheader("Top Holdings by Value")
        bar_n = st.slider("Show top N", 5, 30, 15, key="bar_n")
        bar_data = equity.head(bar_n)
        fig2 = px.bar(
            bar_data,
            x="value_thousands",
            y="ticker",
            orientation="h",
            labels={"value_thousands": "Value ($K)", "ticker": ""},
            color="value_thousands",
            color_continuous_scale="Blues",
            hover_data={"name_of_issuer": True},
        )
        fig2.update_layout(
            yaxis={"autorange": "reversed", "tickfont": {"size": 11}},
            coloraxis_showscale=False,
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig2, width="stretch")

    # ---- QoQ Changes ----
    if compare_period:
        st.divider()
        st.subheader(f"Quarter-over-Quarter Changes  ({compare_period} → {selected_period})")

        old_h = load_holdings(selected_cik, compare_period)
        old_eq = old_h[old_h["put_call"].isna() | (old_h["put_call"] == "")]

        merged = equity[["cusip", "ticker", "name_of_issuer", "value_thousands"]].merge(
            old_eq[["cusip", "value_thousands"]].rename(columns={"value_thousands": "old_value"}),
            on="cusip", how="outer",
        )
        merged["new_value"] = merged["value_thousands"].fillna(0)
        merged["old_value"] = merged["old_value"].fillna(0)
        merged["change"]    = merged["new_value"] - merged["old_value"]
        merged["pct_change"] = merged.apply(
            lambda r: (r["change"] / r["old_value"] * 100) if r["old_value"] > 0 else None, axis=1
        )
        merged["status"] = merged.apply(
            lambda r: "New" if r["old_value"] == 0 else
                      "Closed" if r["new_value"] == 0 else
                      "Increased" if r["change"] > 0 else "Decreased",
            axis=1,
        )
        merged = merged.sort_values("change", key=abs, ascending=False)

        # Waterfall-style bar
        top_changes = merged.head(20).copy()
        colors = top_changes["status"].map({
            "New": "#2ecc71", "Increased": "#27ae60",
            "Decreased": "#e74c3c", "Closed": "#c0392b",
        })
        fig3 = go.Figure(go.Bar(
            x=top_changes["change"],
            y=top_changes["ticker"],
            orientation="h",
            marker_color=colors,
            text=top_changes["status"],
            textposition="auto",
            customdata=top_changes["name_of_issuer"],
            hovertemplate="%{customdata}<br>Change: %{x:,.0f}K<extra></extra>",
        ))
        fig3.update_layout(
            xaxis_title="Change in Value ($K)",
            yaxis={"autorange": "reversed"},
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig3, width="stretch")

        # Summary counts
        s = merged["status"].value_counts()
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("New Positions",      s.get("New", 0),       delta_color="normal")
        sc2.metric("Closed Positions",   s.get("Closed", 0),    delta_color="inverse")
        sc3.metric("Increased",          s.get("Increased", 0), delta_color="normal")
        sc4.metric("Decreased",          s.get("Decreased", 0), delta_color="inverse")

    # ---- Full holdings table ----
    st.divider()
    st.subheader("Full Holdings Table")
    display = equity.copy()
    display["value_millions"] = (display["value_thousands"] / 1000).round(2)
    display["weight_%"]       = (display["value_thousands"] / total_aum * 100).round(2)
    st.dataframe(
        display[["ticker", "name_of_issuer", "cusip", "title_of_class", "value_millions", "weight_%", "shares"]].rename(columns={
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

# ---------------------------------------------------------------------------
# Conviction Scores
# ---------------------------------------------------------------------------

elif view == "Conviction Scores":
    st.subheader(f"Conviction Scores — {selected_period}")
    st.caption(
        "Score = num_institutions × log(1 + avg_portfolio_weight%) — "
        "rewards securities that are widely held AND carry meaningful position sizes."
    )

    scores_df = load_conviction_scores(selected_period, min_filers_filter)

    if scores_df.empty:
        st.info("No securities meet the minimum institution threshold for this period.")
        st.stop()

    top_n_scores = st.slider("Show top N", 10, 50, 25, key="scores_n")
    plot_df = scores_df.head(top_n_scores).copy()

    fig_scores = px.bar(
        plot_df,
        x="conviction_score",
        y="ticker",
        orientation="h",
        color="conviction_score",
        color_continuous_scale="Teal",
        hover_data={"name_of_issuer": True, "num_filers": True, "avg_weight_pct": True},
        labels={
            "conviction_score": "Conviction Score",
            "ticker": "",
            "num_filers": "# Institutions",
            "avg_weight_pct": "Avg Weight %",
        },
    )
    fig_scores.update_layout(
        yaxis={"autorange": "reversed"},
        coloraxis_showscale=False,
        margin=dict(t=10, b=10),
    )
    st.plotly_chart(fig_scores, width="stretch")

    st.divider()

    col_s1, col_s2 = st.columns([1, 1])

    with col_s1:
        st.markdown("**Score vs. Avg Portfolio Weight**")
        fig_scatter = px.scatter(
            scores_df.head(50),
            x="avg_weight_pct",
            y="conviction_score",
            size="num_filers",
            color="num_filers",
            hover_name="ticker",
            hover_data={"name_of_issuer": True},
            color_continuous_scale="Blues",
            labels={
                "avg_weight_pct": "Avg Portfolio Weight %",
                "conviction_score": "Conviction Score",
                "num_filers": "# Institutions",
            },
        )
        fig_scatter.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig_scatter, width="stretch")

    with col_s2:
        st.markdown("**Breadth vs. Concentration**")
        fig_bv = px.scatter(
            scores_df.head(50),
            x="num_filers",
            y="avg_weight_pct",
            size="total_value_thousands",
            hover_name="ticker",
            hover_data={"name_of_issuer": True, "conviction_score": True},
            color="conviction_score",
            color_continuous_scale="Teal",
            labels={
                "num_filers": "# Institutions Holding",
                "avg_weight_pct": "Avg Portfolio Weight %",
                "conviction_score": "Conviction Score",
            },
        )
        fig_bv.update_layout(margin=dict(t=10, b=10), coloraxis_showscale=False)
        st.plotly_chart(fig_bv, width="stretch")

    st.divider()
    st.subheader("Full Conviction Table")
    display_scores = scores_df.copy()
    display_scores["total_value_billions"] = (display_scores["total_value_thousands"] / 1_000_000).round(2)
    st.dataframe(
        display_scores[["ticker", "name_of_issuer", "num_filers", "avg_weight_pct", "total_value_billions", "conviction_score"]].rename(columns={
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

# ---------------------------------------------------------------------------
# Cross-Filer Overview
# ---------------------------------------------------------------------------

elif view == "Cross-Filer Overview":
    all_h = load_all_holdings(selected_period)

    if all_h.empty:
        st.info("No holdings for this period.")
        st.stop()

    equity_all = all_h[all_h["put_call"].isna() | (all_h["put_call"] == "")]

    # ---- AUM by filer ----
    st.subheader(f"AUM by Institution — {selected_period}")
    aum_by_filer = (
        equity_all.groupby("filer_name")["value_thousands"]
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    aum_by_filer["aum_billions"] = aum_by_filer["value_thousands"] / 1_000_000

    fig_aum = px.bar(
        aum_by_filer,
        x="filer_name", y="aum_billions",
        labels={"filer_name": "", "aum_billions": "AUM ($B)"},
        color="aum_billions",
        color_continuous_scale="Blues",
    )
    fig_aum.update_layout(coloraxis_showscale=False, xaxis_tickangle=-30, margin=dict(t=10, b=80))
    st.plotly_chart(fig_aum, width="stretch")

    st.divider()

    # ---- Most widely held securities ----
    st.subheader("Most Widely Held Securities")
    col_a, col_b = st.columns([1, 1])

    with col_a:
        st.markdown("**By number of institutions holding**")
        breadth = (
            equity_all.groupby(["cusip", "ticker", "name_of_issuer"])
            .agg(num_filers=("cik", "nunique"), total_value=("value_thousands", "sum"))
            .sort_values("num_filers", ascending=False)
            .head(20)
            .reset_index()
        )
        fig_b = px.bar(
            breadth, x="num_filers", y="ticker", orientation="h",
            labels={"num_filers": "# Institutions", "ticker": ""},
            color="num_filers", color_continuous_scale="Teal",
            hover_data={"name_of_issuer": True},
        )
        fig_b.update_layout(
            yaxis={"autorange": "reversed"}, coloraxis_showscale=False, margin=dict(t=10, b=10)
        )
        st.plotly_chart(fig_b, width="stretch")

    with col_b:
        st.markdown("**By aggregate market value**")
        fig_v = px.bar(
            breadth.sort_values("total_value", ascending=False).head(20),
            x="total_value", y="ticker", orientation="h",
            labels={"total_value": "Aggregate Value ($K)", "ticker": ""},
            color="total_value", color_continuous_scale="Purples",
            hover_data={"name_of_issuer": True},
        )
        fig_v.update_layout(
            yaxis={"autorange": "reversed"}, coloraxis_showscale=False, margin=dict(t=10, b=10)
        )
        st.plotly_chart(fig_v, width="stretch")

    st.divider()

    # ---- Heatmap: filer × top security ----
    st.subheader("Overlap Heatmap — Top Securities × Institutions")
    top_cusips = breadth.head(15)["cusip"].tolist()
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
        color_continuous_scale="Blues",
        labels={"color": "Value ($K)"},
        aspect="auto",
    )
    fig_heat.update_layout(
        xaxis_tickangle=-40,
        margin=dict(t=10, b=120),
        coloraxis_colorbar=dict(title="Value ($K)"),
    )
    st.plotly_chart(fig_heat, width="stretch")

    # ---- Raw table ----
    st.divider()
    st.subheader("Aggregate Holdings Table")
    st.dataframe(
        breadth.rename(columns={
            "name_of_issuer": "Issuer", "cusip": "CUSIP",
            "num_filers": "# Institutions", "total_value": "Aggregate Value ($K)",
        }),
        use_container_width=True,
        hide_index=True,
    )
