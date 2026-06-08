"""Presentation layer: pure formatting/color/filter helpers (top half, fully
testable) and Streamlit render helpers (bottom half, added in Task 6)."""
from __future__ import annotations

import pandas as pd

# --- palette (kept in sync with theme.py) ---
INK             = "#1d1d1f"
INK_SECONDARY   = "#6e6e73"
ACCENT          = "#0071e3"
BUY_GREEN       = "#34c759"
SELL_RED        = "#ff3b30"
CONF_HIGH       = "#34c759"
CONF_MEDIUM     = "#ff9f0a"
CONF_LOW        = "#8e8e93"


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


def fmt_pct(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{v * 100:+.1f}%"


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
    return (f'<div class="rk-bar"><i style="width:{pct:.0f}%"></i></div>')


def badge_html(text: str, color: str) -> str:
    return f'<span class="rk-badge" style="background:{color}">{_html.escape(text)}</span>'


def ranking_list(rows_html: list[str], stagger_ms: int = 50) -> None:
    """Render pre-built row HTML with staggered fade-in.

    The per-row `animation-delay` is merged into the row's existing `style="..."`
    (its grid-template-columns). Adding a *second* style attribute would be invalid
    HTML — browsers keep only the first — so we prepend into the first style instead.
    """
    parts = []
    for i, r in enumerate(rows_html):
        delay = i * stagger_ms
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
