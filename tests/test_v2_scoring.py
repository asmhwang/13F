"""Unit tests for the pure scoring math in the v2 pipelines (no DB)."""

import math

import pytest

from pipeline.scoring.fund_pipeline_v2 import percentile_scores, shrunk_ir
from pipeline.scoring.stock_pipeline_v2 import (
    ADD_MULT,
    NEW_BUY_MULT,
    TRIM_MULT,
    conviction_multiple,
    recency_multiplier,
    tenure_multiplier,
)


# ---------------------------------------------------------------------------
# shrunk_ir
# ---------------------------------------------------------------------------

class TestShrunkIR:
    def test_insufficient_observations(self):
        assert shrunk_ir([]) is None
        assert shrunk_ir([0.05]) is None

    def test_zero_dispersion(self):
        assert shrunk_ir([0.02, 0.02, 0.02]) is None

    def test_basic_stats(self):
        excesses = [0.02, -0.01, 0.03, 0.00]
        s = shrunk_ir(excesses, k=8)
        assert s["n"] == 4
        assert s["mean"] == pytest.approx(0.01)
        assert s["win_rate"] == pytest.approx(0.5)  # two windows > 0
        # shrinkage: mean * 4/12
        assert s["shrunk_ir_annual"] == pytest.approx(
            (0.01 * 4 / 12) / s["stdev"] * 2.0)
        assert s["ir_annual"] == pytest.approx(0.01 / s["stdev"] * 2.0)
        assert s["t_stat"] == pytest.approx(0.01 / (s["stdev"] / math.sqrt(4)))

    def test_shrinkage_grows_with_history(self):
        """Same per-window stats, longer history -> larger shrunk IR."""
        short = shrunk_ir([0.02, -0.01, 0.03, 0.00] * 2)    # n=8
        long = shrunk_ir([0.02, -0.01, 0.03, 0.00] * 10)    # n=40
        assert long["shrunk_ir_annual"] > short["shrunk_ir_annual"]
        # shrinkage factor (shrunk/unshrunk) is exactly n/(n+k)
        assert short["shrunk_ir_annual"] / short["ir_annual"] == pytest.approx(8 / 16)
        assert long["shrunk_ir_annual"] / long["ir_annual"] == pytest.approx(40 / 48)

    def test_negative_skill_stays_negative_after_shrinkage(self):
        s = shrunk_ir([-0.02, -0.01, -0.03, 0.00])
        assert s["shrunk_ir_annual"] < 0
        assert abs(s["shrunk_ir_annual"]) < abs(s["ir_annual"])


# ---------------------------------------------------------------------------
# percentile_scores
# ---------------------------------------------------------------------------

class TestPercentileScores:
    def test_empty_and_single(self):
        assert percentile_scores({}) == {}
        assert percentile_scores({"a": 5.0}) == {"a": 100.0}

    def test_outlier_does_not_compress_the_rest(self):
        """v1's min-max gave {100, ~1, ~0}; percentile keeps spacing even."""
        scores = percentile_scores({"outlier": 100.0, "mid": 1.0, "low": 0.9})
        assert scores["outlier"] == 100.0
        assert scores["mid"] == 50.0
        assert scores["low"] == 0.0

    def test_monotonic(self):
        scores = percentile_scores({"a": 3.0, "b": 1.0, "c": 2.0})
        assert scores["a"] > scores["c"] > scores["b"]


# ---------------------------------------------------------------------------
# stock v2 multipliers
# ---------------------------------------------------------------------------

class TestConviction:
    def test_typical_position_is_one(self):
        assert conviction_multiple(0.02, 0.02) == pytest.approx(1.0)

    def test_emphasized_position(self):
        assert conviction_multiple(0.10, 0.02) == pytest.approx(5.0)

    def test_capped(self):
        assert conviction_multiple(0.50, 0.01) == pytest.approx(8.0)

    def test_degenerate_median(self):
        assert conviction_multiple(0.10, 0.0) == 0.0


class TestRecency:
    def test_no_prior_filing_is_neutral(self):
        assert recency_multiplier(100.0, None, has_prior_filing=False) == 1.0

    def test_new_position(self):
        assert recency_multiplier(100.0, None, has_prior_filing=True) == NEW_BUY_MULT
        assert recency_multiplier(100.0, 0.0, has_prior_filing=True) == NEW_BUY_MULT

    def test_add_and_trim_thresholds(self):
        assert recency_multiplier(125.0, 100.0, True) == ADD_MULT     # +25%
        assert recency_multiplier(75.0, 100.0, True) == TRIM_MULT     # -25%
        assert recency_multiplier(110.0, 100.0, True) == 1.0          # +10% hold
        assert recency_multiplier(90.0, 100.0, True) == 1.0           # -10% hold


class TestTenure:
    def test_boost_and_cap(self):
        assert tenure_multiplier(0) == pytest.approx(1.0)
        assert tenure_multiplier(5) == pytest.approx(1.10)
        assert tenure_multiplier(10) == pytest.approx(1.20)
        assert tenure_multiplier(40) == pytest.approx(1.20)  # capped
