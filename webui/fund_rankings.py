"""Fund Rankings page: ranked funds + click-through detail (holdings, QPS chart)."""
from __future__ import annotations

import html as _html

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from webui import components as c
from webui import data


def _fund_row_html(r: pd.Series) -> str:
    score = float(r.get("final_score") or 0)
    chip = '<span class="rk-chip">one-hit wonder</span>' if r.get("one_hit_wonder_flag") else ""
    return (
        '<div class="rk-row" style="grid-template-columns:44px 2.4fr 1.4fr 1fr 1fr 1fr 1fr">'
        f'<div class="rk-rank">{int(r["rank"])}</div>'
        f'<div><div class="rk-name">{_html.escape(str(r["fund_name"]))}{chip}</div>'
        f'<div class="rk-sub">{c.fmt_money((r.get("avg_aum") or 0) * 1000)} avg AUM</div></div>'
        f'<div><div class="rk-name">{score:.0f}</div>{c.score_bar_html(score)}</div>'
        f'<div><div class="rk-sub">Positions</div><div>{int(r.get("avg_position_count") or 0)}</div></div>'
        f'<div><div class="rk-sub">Quarters</div><div>{int(r.get("quarters_of_data") or 0)}</div></div>'
        f'<div><div class="rk-sub">Turnover</div><div>{c.fmt_pct(r.get("avg_turnover_rate"))}</div></div>'
        f'<div><div class="rk-sub">TWS</div><div>{c.fmt_pct(r.get("tws_raw"))}</div></div>'
        '</div>'
    )


@st.dialog("Fund detail", width="large")
def _fund_detail(fund_id: str, fund_name: str) -> None:
    st.subheader(fund_name)
    qps = data.load_fund_quarterly_scores(fund_id)
    if not qps.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=qps["quarter_date"], y=qps["qps_excess"],
                                 mode="lines+markers", line=dict(color="#0071e3", width=2),
                                 name="Excess QPS"))
        fig.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10),
                          yaxis_tickformat=".0%", paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)")
        st.caption("Historical excess QPS (3yr forward, vs S&P 500 TR)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.caption("No scoreable quarters yet.")
    to = data.load_fund_turnover(fund_id)
    if not to.empty:
        st.caption(f"Avg turnover {c.fmt_pct(to.iloc[0]['avg_turnover_rate'])} · "
                   f"multiplier {to.iloc[0]['turnover_multiplier']:.2f} · "
                   f"{int(to.iloc[0]['quarter_pairs_measured'])} quarter pairs")


def render_fund_rankings() -> None:
    df = data.load_fund_rankings()
    meta = data.load_rankings_meta()
    stale = ""
    if meta.get("latest_quarter"):
        stale = f"Most recent quarter end: {meta['latest_quarter']} · {meta['fund_count']} eligible fund(s)"
    c.hero("Fund Rankings",
           "Small, concentrated funds ranked by long-term selection skill.", stale)

    if df.empty:
        c.empty_card("No eligible funds yet. Run the fund pipeline after ingesting more small filers.")
        return

    median = df["final_score"].median()
    top = df.iloc[0]["fund_name"]
    c.kpi_strip([
        (str(len(df)), "Eligible funds"),
        (top, "Top fund"),
        (f"{median:.0f}", "Median score"),
        (str(int(df["quarters_of_data"].max() or 0)), "Max quarters"),
    ])

    with st.container():
        col1, col2 = st.columns([3, 1])
        with col2:
            sort_col = st.selectbox("Sort by",
                                    ["rank", "final_score", "avg_aum", "avg_position_count"],
                                    index=0, key="fund_sort")
        with col1:
            score_rng = st.slider("Score range", 0, 100, (0, 100), key="fund_score_rng")
    view = c.apply_filters_sort(
        df, {"final_score": (score_rng[0], score_rng[1])},
        sort_col=sort_col, ascending=(sort_col == "rank"),
    )

    c.ranking_list([_fund_row_html(r) for _, r in view.iterrows()])

    options = {f'{int(r["rank"])} · {r["fund_name"]}': r["fund_id"] for _, r in view.iterrows()}
    pick = st.selectbox("Inspect a fund", ["—"] + list(options), key="fund_inspect")
    if pick != "—":
        _fund_detail(options[pick], pick.split(" · ", 1)[1])
