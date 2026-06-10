"""Fund Rankings v2 page: clone-return shrunk-IR methodology (side-by-side
with v1). Ranked list + click-through detail (per-window excess chart)."""
from __future__ import annotations

import html as _html

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from webui import components as c
from webui import data


def _fund_row_html(r: pd.Series) -> str:
    score = float(r.get("score") or 0)
    sir = r.get("shrunk_ir_annual")
    sir_txt = "—" if sir is None or pd.isna(sir) else f"{float(sir):.2f}"
    t = r.get("t_stat")
    t_txt = "—" if t is None or pd.isna(t) else f"{float(t):.1f}"
    return (
        '<div class="rk-row" style="grid-template-columns:44px 2.4fr 1.4fr 1fr 1fr 1fr 1fr">'
        f'<div class="rk-rank">{int(r["rank"])}</div>'
        f'<div><div class="rk-name">{_html.escape(str(r["fund_name"]))}</div>'
        f'<div class="rk-sub">{c.fmt_money(r.get("median_aum"))} median AUM · '
        f'{round(r.get("median_positions") or 0)} positions</div></div>'
        f'<div><div class="rk-score">{score:.0f}</div>{c.score_bar_html(score)}</div>'
        f'<div><div class="rk-sub">Shrunk IR</div>'
        f'<div style="color:{c.net_change_color(sir)}">{sir_txt}</div></div>'
        f'<div><div class="rk-sub">t-stat</div><div>{t_txt}</div></div>'
        f'<div><div class="rk-sub">Windows</div><div>{int(r.get("n_windows") or 0)}</div></div>'
        f'<div><div class="rk-sub">Win rate</div>'
        f'<div>{c.fmt_pct(r.get("win_rate"), signed=False)}</div></div>'
        '</div>'
    )


@st.dialog("Fund detail (v2)", width="large")
def _fund_detail(fund_id: str, fund_name: str) -> None:
    st.subheader(fund_name)
    w = data.load_fund_clone_windows_v2(fund_id)
    if not w.empty:
        colors = [c.BUY_GREEN if v > 0 else c.SELL_RED for v in w["excess_return"]]
        fig = go.Figure()
        fig.add_trace(go.Bar(x=w["start_period"], y=w["excess_return"],
                             marker_color=colors, name="Excess"))
        fig.add_trace(go.Scatter(x=w["start_period"], y=w["excess_return"].cumsum(),
                                 mode="lines", line=dict(color=c.ACCENT, width=2),
                                 name="Cumulative"))
        fig.update_layout(height=280, margin=dict(l=10, r=10, t=10, b=10),
                          yaxis_tickformat=".0%", paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)",
                          legend=dict(orientation="h", y=1.1))
        st.caption("Per-window clone excess return (filed portfolio held to next "
                   "filing, vs S&P 500 TR) — non-overlapping observations")
        st.plotly_chart(fig, width="stretch")
        st.caption(f"{len(w)} valid windows · avg coverage "
                   f"{c.fmt_pct(w['coverage'].mean(), signed=False)} of book priced")
    else:
        st.caption("No valid clone windows.")


def render_fund_rankings_v2() -> None:
    df = data.load_fund_rankings_v2()
    meta = data.load_rankings_meta()
    stale = ""
    if meta.get("latest_quarter"):
        stale = f"Most recent quarter end: {meta['latest_quarter']} · {len(df)} ranked fund(s)"
    c.hero("Fund Rankings v2",
           "Skill = shrunk information ratio on non-overlapping quarterly clone returns.",
           stale)

    if df.empty:
        c.empty_card("No v2-ranked funds yet. Run pipeline/scoring/fund_pipeline_v2.py.")
        return

    pos_skill = int((df["shrunk_ir_annual"] > 0).sum())
    c.kpi_strip([
        (str(len(df)), "Ranked funds"),
        (str(pos_skill), "Positive skill"),
        (f"{df['n_windows'].max():.0f}", "Max windows"),
        (f"{df['avg_coverage'].mean() * 100:.0f}%", "Avg coverage"),
    ])

    with st.container():
        col1, col2 = st.columns([3, 1])
        with col2:
            sort_col = st.selectbox(
                "Sort by", ["rank", "median_aum", "n_windows"],
                index=0, key="fund_v2_sort",
                format_func=lambda x: {"rank": "Rank", "median_aum": "Median AUM",
                                       "n_windows": "Windows"}[x])
        with col1:
            score_rng = st.slider("Score range", 0, 100, (0, 100), key="fund_v2_score_rng")
    view = c.apply_filters_sort(
        df, {"score": (score_rng[0], score_rng[1])},
        sort_col=sort_col, ascending=(sort_col == "rank"),
    )
    if view.empty:
        c.empty_card("No funds match the current score range.")
        return

    c.ranking_list([_fund_row_html(r) for _, r in view.iterrows()])

    options = {f'{int(r["rank"])} · {r["fund_name"]}': r["fund_id"] for _, r in view.iterrows()}
    pick = c.inspect_select("Inspect a fund", list(options), key="fund_v2_inspect")
    if pick is not None:
        _fund_detail(options[pick], pick.split(" · ", 1)[1])
