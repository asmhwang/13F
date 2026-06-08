"""Tests for pure presentation helpers (no Streamlit calls)."""
import pandas as pd

from webui import components as c


def test_fmt_money_scales():
    assert c.fmt_money(5_300_000_000) == "$5.3B"
    assert c.fmt_money(420_000_000) == "$420.0M"
    assert c.fmt_money(0) == "$0"
    assert c.fmt_money(None) == "—"


def test_fmt_pct_sign():
    assert c.fmt_pct(0.123) == "+12.3%"
    assert c.fmt_pct(-0.05) == "-5.0%"
    assert c.fmt_pct(None) == "—"


def test_net_change_color():
    assert c.net_change_color(0.1) == c.BUY_GREEN
    assert c.net_change_color(-0.1) == c.SELL_RED
    assert c.net_change_color(0) == c.INK_SECONDARY


def test_confidence_color():
    assert c.confidence_color("High") == c.CONF_HIGH
    assert c.confidence_color("Low") == c.CONF_LOW
    assert c.confidence_color("anything-else") == c.INK_SECONDARY


def test_apply_filters_sort():
    df = pd.DataFrame({"sector": ["Tech", "Health", "Tech"],
                       "score": [3.0, 9.0, 5.0]})
    out = c.apply_filters_sort(df, {"sector": ["Tech"]}, sort_col="score", ascending=False)
    assert list(out["score"]) == [5.0, 3.0]
    assert set(out["sector"]) == {"Tech"}
