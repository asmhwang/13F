"""Presentation layer: pure formatting/color/filter helpers (top half, fully
testable) and Streamlit render helpers (bottom half, added in Task 6)."""
from __future__ import annotations

import pandas as pd

# --- palette (kept in sync with theme.py dark-terminal tokens) ---
INK             = "#E8EAED"
INK_SECONDARY   = "#7E8893"
ACCENT          = "#5BAEFF"
BUY_GREEN       = "#3FD68C"
SELL_RED        = "#FF6B5E"
CONF_HIGH       = "#3FD68C"
CONF_MEDIUM     = "#FFB454"
CONF_LOW        = "#7E8893"


def fmt_money(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    v = float(v)
    if v == 0:
        return "$0"
    if abs(v) >= 1e9:
        return f"${v / 1e9:.1f}B"
    if abs(v) >= 1e6:
        return f"${v / 1e6:.1f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:.0f}"


def fmt_pct(v, signed: bool = True) -> str:
    """Format a fraction as a percent. signed=False for unsigned rates
    (e.g. turnover) where a forced "+" would imply a delta. Values that
    round to zero render as an unsigned 0.0% ("-0.0%" reads as a bug)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    pct = round(v * 100, 1)
    if pct == 0:
        return "0.0%"
    sign = "+" if signed else ""
    return f"{pct:{sign}.1f}%"


def net_change_color(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)) or v == 0:
        return INK_SECONDARY
    return BUY_GREEN if v > 0 else SELL_RED


def confidence_color(flag: str) -> str:
    return {"High": CONF_HIGH, "Medium": CONF_MEDIUM, "Low": CONF_LOW}.get(
        flag, INK_SECONDARY
    )


def apply_filters_sort(df: pd.DataFrame, filters: dict, sort_col: str | None = None,
                       ascending: bool = False) -> pd.DataFrame:
    """Client-side filter + sort for ranking tables.

    filters: {column: [allowed values]} for categorical, or
             {column: (lo, hi)} tuple for numeric ranges.
    """
    out = df.copy()
    for col, cond in filters.items():
        if col not in out.columns or cond is None:
            continue
        if isinstance(cond, tuple) and len(cond) == 2:
            lo, hi = cond
            out = out[(out[col] >= lo) & (out[col] <= hi)]
        elif isinstance(cond, (list, set)) and len(cond) > 0:
            out = out[out[col].isin(list(cond))]
    if sort_col and sort_col in out.columns:
        out = out.sort_values(sort_col, ascending=ascending)
    return out.reset_index(drop=True)


# ----------------------------- streamlit render helpers -----------------------------
import html as _html

import streamlit as st


def hero(title: str, subtitle: str, staleness: str = "") -> None:
    stale = f'<div class="rk-stale">{_html.escape(staleness)}</div>' if staleness else ""
    st.markdown(
        f'<div class="rk-wrap"><div class="rk-hero"><h1>{_html.escape(title)}</h1>'
        f'<p class="sub">{_html.escape(subtitle)}</p>{stale}</div></div>',
        unsafe_allow_html=True,
    )


def kpi_strip(cards: list[tuple[str, str]]) -> None:
    """cards = [(value, label), ...]"""
    inner = "".join(
        f'<div class="rk-kpi"><div class="v">{_html.escape(str(v))}</div>'
        f'<div class="l">{_html.escape(str(l))}</div></div>'
        for v, l in cards
    )
    st.markdown(f'<div class="rk-wrap"><div class="rk-kpis">{inner}</div></div>',
                unsafe_allow_html=True)


def score_bar_html(score: float, max_score: float = 100.0) -> str:
    pct = max(0.0, min(100.0, (score / max_score) * 100.0)) if max_score else 0.0
    return f'<div class="rk-bar"><i style="width:{pct:.0f}%"></i></div>'


def badge_html(text: str, color: str) -> str:
    # color drives currentColor: the CSS derives a tinted bg + border from it,
    # so the badge reads as a soft pill on the dark surface.
    return f'<span class="rk-badge" style="color:{color}">{_html.escape(text)}</span>'


def ranking_list(rows_html: list[str], stagger_ms: int = 50) -> None:
    """Render pre-built row HTML with staggered fade-in.

    The per-row `animation-delay` is merged into the row's existing `style="..."`
    (its grid-template-columns). Adding a *second* style attribute would be invalid
    HTML — browsers keep only the first — so we prepend into the first style instead.
    """
    parts = []
    for i, r in enumerate(rows_html):
        # Cap the cascade: past ~8 rows a per-row delay makes a long list feel
        # slow to settle (Emil), so the stagger plateaus instead of growing.
        delay = min(i, 8) * stagger_ms
        anim = f"animation-delay:{delay}ms;"
        if 'style="' in r:
            r = r.replace('style="', f'style="{anim}', 1)
        else:
            r = r.replace('<div class="rk-row"',
                          f'<div class="rk-row" style="{anim}"', 1)
        parts.append(r)
    st.markdown(f'<div class="rk-wrap">{"".join(parts)}</div>', unsafe_allow_html=True)


def empty_card(message: str) -> None:
    st.markdown(f'<div class="rk-wrap"><div class="rk-empty">{_html.escape(message)}</div></div>',
                unsafe_allow_html=True)


def inspect_select(label: str, options: list[str], key: str) -> str | None:
    """Selectbox that should open a detail dialog exactly once per selection.

    st.dialog re-opens on every rerun while its trigger condition holds, and two
    selectboxes with sticky values (one per tab) would open two dialogs in one
    run — a StreamlitAPIException. Gate on the change *event* instead: return the
    pick only on the run where this selectbox changed, and reset it to the
    placeholder on the following run so dismissal sticks and the same item can
    be inspected again.
    """
    if st.session_state.get("_rk_inspect_reset") == key:
        del st.session_state["_rk_inspect_reset"]
        st.session_state[key] = "—"

    def _mark() -> None:
        st.session_state["_rk_inspect_pending"] = key

    pick = st.selectbox(label, ["—"] + options, key=key, on_change=_mark)
    if st.session_state.get("_rk_inspect_pending") == key:
        del st.session_state["_rk_inspect_pending"]
        if pick != "—":
            st.session_state["_rk_inspect_reset"] = key
            return pick
    return None
