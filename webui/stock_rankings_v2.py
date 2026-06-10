"""Stock Rankings v2 page: best-ideas scoring (no regression, no consensus
requirement) side-by-side with v1. Single ranked list with client-side
filters + click-through detail."""
from __future__ import annotations

import html as _html

import pandas as pd
import streamlit as st

from webui import components as c
from webui import data


def _signal_chips(r: pd.Series) -> str:
    chips = []
    if int(r.get("new_buys") or 0) > 0:
        chips.append(f'<span style="color:{c.BUY_GREEN}">▲ {int(r["new_buys"])} new</span>')
    if int(r.get("adds") or 0) > 0:
        chips.append(f'<span style="color:{c.BUY_GREEN}">+{int(r["adds"])} add</span>')
    if int(r.get("trims") or 0) > 0:
        chips.append(f'<span style="color:{c.SELL_RED}">−{int(r["trims"])} trim</span>')
    return " · ".join(chips) if chips else '<span class="rk-sub">held</span>'


def _stock_row_html(r: pd.Series) -> str:
    score = float(r.get("score") or 0)
    mw = r.get("max_weight")
    mw_txt = "—" if mw is None or pd.isna(mw) else f"{float(mw) * 100:.1f}%"
    tenure = r.get("avg_tenure")
    tenure_txt = "—" if tenure is None or pd.isna(tenure) else f"{float(tenure):.1f}q"
    top = str(r.get("top_fund_name") or "")
    if len(top) > 26:
        top = top[:25] + "…"
    return (
        '<div class="rk-row" style="grid-template-columns:44px 1.2fr 2fr 1.4fr 1fr 1fr 1.3fr 1fr">'
        f'<div class="rk-rank">{int(r["rank"])}</div>'
        f'<div class="rk-name">{_html.escape(str(r["ticker"]))}</div>'
        f'<div><div class="rk-sub">{_html.escape(str(r.get("company_name") or ""))}</div>'
        f'<div class="rk-sub">{_html.escape(str(r.get("sector") or ""))}</div></div>'
        f'<div><div class="rk-score">{score:.0f}</div>{c.score_bar_html(score)}</div>'
        f'<div><div class="rk-sub">Backers</div><div>{int(r.get("n_backers") or 0)}</div></div>'
        f'<div><div class="rk-sub">Max wt</div><div>{mw_txt}</div></div>'
        f'<div><div class="rk-sub">Activity</div><div>{_signal_chips(r)}</div></div>'
        f'<div><div class="rk-sub">Tenure</div><div>{tenure_txt}</div></div>'
        '</div>'
    )


@st.dialog("Stock detail (v2)", width="large")
def _stock_detail(ticker: str, row: pd.Series) -> None:
    st.subheader(ticker)
    cols = st.columns(4)
    cols[0].metric("Market cap", c.fmt_money(row.get("market_cap")))
    pe_av, pe = row.get("pe_available"), row.get("pe_ratio")
    pe_ok = pe_av is not None and not pd.isna(pe_av) and bool(pe_av) and not pd.isna(pe)
    cols[1].metric("P/E", f"{float(pe):.1f}" if pe_ok else "—")
    sk = row.get("top_fund_skill")
    cols[2].metric("Top backer IR", "—" if sk is None or pd.isna(sk) else f"{float(sk):.2f}")
    cols[3].metric("Price data", "fresh" if row.get("price_fresh") else "stale")
    st.caption(f"Top backer: {row.get('top_fund_name') or '—'}")
    st.markdown("**Held by v2-ranked funds**")
    holders = data.load_stock_holders_v2(ticker)
    if holders.empty:
        st.caption("No ranked-fund holdings recorded.")
    else:
        show = holders.assign(
            weight=lambda d: d["weight"].map(
                lambda w: "—" if pd.isna(w) else f"{w * 100:.1f}%"),
            shrunk_ir_annual=lambda d: d["shrunk_ir_annual"].map(
                lambda v: "—" if pd.isna(v) else f"{v:.2f}"),
        )[["rank", "fund_name", "shrunk_ir_annual", "weight", "quarters_held"]]
        st.dataframe(show, width="stretch", hide_index=True)


def render_stock_rankings_v2() -> None:
    df = data.load_stock_rankings_v2()
    meta = data.load_rankings_meta()
    stale = "Positions reflect holdings at quarter end. Holdings may have changed since filing."
    if meta.get("latest_quarter"):
        stale = f"Most recent quarter end: {meta['latest_quarter']} · " + stale
    c.hero("Stock Rankings v2",
           "Best ideas of positive-skill managers: skill × in-book conviction × recency.",
           stale)

    if df.empty:
        c.empty_card("No v2-ranked stocks yet. Run the v2 fund pipeline, then "
                     "pipeline/scoring/stock_pipeline_v2.py.")
        return

    new_buy_stocks = int((df["new_buys"] > 0).sum())
    c.kpi_strip([
        (str(len(df)), "Universe size"),
        (str(int(df["n_backers"].max() or 0)), "Max backers"),
        (str(new_buy_stocks), "With new buys"),
        (str(df["sector"].nunique()), "Sectors"),
    ])

    sectors = sorted([s for s in df.get("sector", pd.Series()).dropna().unique()])
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        pick_sectors = st.multiselect("Sector", sectors, default=sectors, key="v2_sectors")
    with col2:
        min_backers = st.selectbox("Min backers", [1, 2, 3], index=0, key="v2_min_backers")
    with col3:
        cap_band = st.selectbox(
            "Market cap", ["All", "Small/Mid ($300M–$4B)", "Large (>$4B)"],
            index=0, key="v2_cap_band")

    view = c.apply_filters_sort(
        df, {"sector": pick_sectors, "n_backers": (min_backers, 10 ** 9)},
        sort_col="rank", ascending=True,
    )
    if cap_band == "Small/Mid ($300M–$4B)":
        view = view[(view["market_cap"] >= 300e6) & (view["market_cap"] <= 4e9)]
    elif cap_band == "Large (>$4B)":
        view = view[view["market_cap"] > 4e9]
    view = view.reset_index(drop=True)
    if view.empty:
        c.empty_card("No stocks match the current filters.")
        return

    c.ranking_list([_stock_row_html(r) for _, r in view.iterrows()])

    options = {f'{int(r["rank"])} · {r["ticker"]}': i for i, (_, r) in enumerate(view.iterrows())}
    pick = c.inspect_select("Inspect a stock", list(options), key="v2_inspect")
    if pick is not None:
        row = view.iloc[options[pick]]
        _stock_detail(row["ticker"], row)
