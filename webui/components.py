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
