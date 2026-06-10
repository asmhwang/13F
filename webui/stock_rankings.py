"""Stock Rankings page: Raw/Filtered tabs + click-through detail (holders, fundamentals)."""
from __future__ import annotations

import html as _html

import pandas as pd
import streamlit as st

from webui import components as c
from webui import data


def _stock_row_html(r: pd.Series) -> str:
    # The filtered table is intentionally a column subset of the raw table, so
    # net_change_pct / avg_tenure are absent there — Series.get() yields None and
    # the cells render as "—"/"·" rather than raising.
    flag = str(r.get("confidence_flag") or "")
    badge = c.badge_html(flag, c.confidence_color(flag)) if flag else ""
    nc = r.get("net_change_pct")
    nc_color = c.net_change_color(nc)
    arrow = "▲" if (nc or 0) > 0 else ("▼" if (nc or 0) < 0 else "·")
    score = r.get("sector_adjusted_score")
    score_txt = "—" if score is None or pd.isna(score) else f"{score:.2f}"
    tenure = r.get("avg_tenure")
    tenure_txt = "—" if tenure is None or pd.isna(tenure) else f"{float(tenure):.1f}q"
    return (
        '<div class="rk-row" style="grid-template-columns:44px 1.4fr 2fr 1.2fr 1fr 1fr 1.1fr 1fr">'
        f'<div class="rk-rank">{int(r["rank"])}</div>'
        f'<div class="rk-name">{_html.escape(str(r["ticker"]))}</div>'
        f'<div><div class="rk-sub">{_html.escape(str(r.get("company_name") or ""))}</div>'
        f'<div class="rk-sub">{_html.escape(str(r.get("sector") or ""))}</div></div>'
        f'<div>{badge}</div>'
        f'<div><div class="rk-sub">Score</div><div>{score_txt}</div></div>'
        f'<div><div class="rk-sub">Funds</div><div>{int(r.get("holder_count") or 0)}</div></div>'
        f'<div><div class="rk-sub">Net change</div>'
        f'<div style="color:{nc_color}">{arrow} {c.fmt_pct(nc)}</div></div>'
        f'<div><div class="rk-sub">Avg tenure</div>'
        f'<div>{tenure_txt}</div></div>'
        '</div>'
    )


@st.dialog("Stock detail", width="large")
def _stock_detail(ticker: str, row: pd.Series) -> None:
    st.subheader(ticker)
    cols = st.columns(4)
    cols[0].metric("Market cap", c.fmt_money(row.get("market_cap")))
    pe_av, pe = row.get("pe_available"), row.get("pe_ratio")
    pe_ok = pe_av is not None and not pd.isna(pe_av) and bool(pe_av) and not pd.isna(pe)
    cols[1].metric("P/E", f"{float(pe):.1f}" if pe_ok else "—")
    rp = row.get("range_position")
    cols[2].metric("52wk range", "—" if rp is None or pd.isna(rp) else f"{float(rp) * 100:.0f}%")
    # Finnhub grossMarginTTM is already in percent units (e.g. 32.4) — no ×100.
    gm = row.get("gross_margin_pct")
    cols[3].metric("Gross margin", "—" if gm is None or pd.isna(gm) else f"{float(gm):.0f}%")
    st.markdown("**Held by qualifying funds**")
    holders = data.load_stock_holders(ticker)
    if holders.empty:
        st.caption("No qualifying-fund holdings recorded.")
    else:
        show = holders.assign(
            weight=lambda d: d["weight"].map(
                lambda w: "—" if pd.isna(w) else f"{w * 100:.1f}%"),
        )[["rank", "fund_name", "weight", "quarters_held"]]
        st.dataframe(show, use_container_width=True, hide_index=True)


def _render_tab(df: pd.DataFrame, kind: str) -> None:
    if df.empty:
        if kind == "filtered":
            c.empty_card("No stocks meet the filtered criteria yet "
                         "(needs a top-fund holder + $300M–$4B market cap + populated fundamentals). "
                         "See the Raw tab.")
        else:
            c.empty_card("No ranked stocks yet. Run the stock pipeline after the fund pipeline.")
        return

    sectors = sorted([s for s in df.get("sector", pd.Series()).dropna().unique()])
    col1, col2 = st.columns(2)
    with col1:
        pick_sectors = st.multiselect("Sector", sectors, default=sectors, key=f"{kind}_sectors")
    with col2:
        confs = [x for x in ["High", "Medium", "Low"] if x in set(df.get("confidence_flag", []))]
        pick_conf = st.multiselect("Confidence", confs, default=confs, key=f"{kind}_conf")

    view = c.apply_filters_sort(
        df, {"sector": pick_sectors, "confidence_flag": pick_conf},
        sort_col="rank", ascending=True,
    )
    if view.empty:
        c.empty_card("No stocks match the current filters.")
        return

    c.ranking_list([_stock_row_html(r) for _, r in view.iterrows()])

    options = {f'{int(r["rank"])} · {r["ticker"]}': i for i, (_, r) in enumerate(view.iterrows())}
    pick = c.inspect_select("Inspect a stock", list(options), key=f"{kind}_inspect")
    if pick is not None:
        row = view.iloc[options[pick]]
        _stock_detail(row["ticker"], row)


def render_stock_rankings() -> None:
    meta = data.load_rankings_meta()
    stale = "Positions reflect holdings at quarter end. Holdings may have changed since filing."
    if meta.get("latest_quarter"):
        stale = f"Most recent quarter end: {meta['latest_quarter']} · " + stale
    c.hero("Stock Rankings",
           "Stocks the top-ranked funds are most convicted on right now.", stale)

    raw = data.load_stock_rankings("raw")
    if not raw.empty:
        high = int((raw["confidence_flag"] == "High").sum())
        med_score = raw["sector_adjusted_score"].median()
        n_sectors = raw["sector"].nunique()
        c.kpi_strip([
            (str(len(raw)), "Universe size"),
            (str(high), "High confidence"),
            ("—" if pd.isna(med_score) else f"{med_score:.2f}", "Median score"),
            (str(n_sectors), "Sectors"),
        ])

    tab_raw, tab_filt = st.tabs(["Raw Rankings", "Filtered Rankings"])
    with tab_raw:
        _render_tab(raw, "raw")
    with tab_filt:
        _render_tab(data.load_stock_rankings("filtered"), "filtered")
