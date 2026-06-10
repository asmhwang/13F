"""Tests for per-filing value-unit detection (parser.detect_value_divisor).

13F <value> fields are reported either in whole dollars (post-2022 SEC rule,
most filers) or still in thousands (legacy + non-compliant filers such as
Baupost, T. Rowe Price, Tieton). The divisor converts the parsed raw <value>
into thousands-of-dollars regardless of which convention a filing used.
"""

from pipeline.parser import detect_value_divisor


def _holding(value, shares):
    return {"value_thousands": value, "shares": shares, "put_call": None}


def test_dollars_convention_returns_1000():
    # value reported in whole dollars: value == price * shares.
    # e.g. three positions priced $50/$200/$30 each with realistic share counts.
    holdings = [
        _holding(50 * 100_000, 100_000),    # implied px 50
        _holding(200 * 50_000, 50_000),     # implied px 200
        _holding(30 * 200_000, 200_000),    # implied px 30
    ]
    assert detect_value_divisor(holdings) == 1000


def test_thousands_convention_returns_1():
    # value reported in thousands: value == price * shares / 1000.
    holdings = [
        _holding(50 * 100_000 // 1000, 100_000),   # implied px 0.05
        _holding(200 * 50_000 // 1000, 50_000),    # implied px 0.2
        _holding(30 * 200_000 // 1000, 200_000),   # implied px 0.03
    ]
    assert detect_value_divisor(holdings) == 1


def test_median_ignores_single_outlier():
    # One garbage row should not flip the (dollars) classification.
    holdings = [
        _holding(50 * 100_000, 100_000),    # implied px 50
        _holding(200 * 50_000, 50_000),     # implied px 200
        _holding(1, 1_000_000),             # implied px ~0 (outlier)
    ]
    assert detect_value_divisor(holdings) == 1000


def test_holdings_without_shares_are_ignored():
    # Bonds / principal-only rows (shares None or 0) must not skew detection.
    holdings = [
        _holding(50 * 100_000, 100_000),    # implied px 50 -> dollars
        _holding(1_000_000, 0),             # no shares
        _holding(5_000, None),              # no shares
    ]
    assert detect_value_divisor(holdings) == 1000


def test_no_usable_shares_falls_back_to_1():
    # Nothing to judge by -> assume already-thousands (no division), the
    # historically safe default for legacy/principal-only filings.
    holdings = [
        _holding(1_000_000, 0),
        _holding(5_000, None),
    ]
    assert detect_value_divisor([]) == 1
    assert detect_value_divisor(holdings) == 1
