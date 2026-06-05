import unittest

from services.weight_ops import (
    apply_minimum_weight_floor,
    apply_group_caps_cash_first,
    apply_single_caps_cash_first,
    assert_invariants,
    normalize_cash_first,
    normalize_proportional,
    tighten_buy_delta,
    tighten_sell_delta,
)


class WeightOpsTest(unittest.TestCase):
    def test_normalize_cash_first_ignores_stale_cash_value(self):
        out, diag = normalize_cash_first({"SPY": 0.8, "CASH": 0.5})

        self.assertAlmostEqual(sum(out.values()), 1.0, places=12)
        self.assertAlmostEqual(out["SPY"], 0.8, places=12)
        self.assertAlmostEqual(out["CASH"], 0.2, places=12)
        self.assertFalse(diag["normalized"])

    def test_normalize_cash_first_scales_non_cash_when_over_one(self):
        out, diag = normalize_cash_first({"SPY": 0.8, "QQQ": 0.4, "CASH": 0.2})

        self.assertAlmostEqual(sum(out.values()), 1.0, places=12)
        self.assertAlmostEqual(out["SPY"], 0.8 / 1.2, places=12)
        self.assertAlmostEqual(out["QQQ"], 0.4 / 1.2, places=12)
        self.assertAlmostEqual(out["CASH"], 0.0, places=12)
        self.assertTrue(diag["normalized"])

    def test_normalize_proportional_scales_cash_together_with_risk(self):
        out, diag = normalize_proportional({"SPY": 0.8, "CASH": 0.4})

        self.assertAlmostEqual(out["SPY"], 0.8 / 1.2, places=12)
        self.assertAlmostEqual(out["CASH"], 0.4 / 1.2, places=12)
        self.assertTrue(diag["normalized"])

    def test_single_caps_release_to_cash(self):
        out, diag = apply_single_caps_cash_first(
            {"XLK": 0.18, "SPY": 0.1, "CASH": 0.72},
            {"XLK": 0.15},
        )

        self.assertAlmostEqual(out["XLK"], 0.15, places=12)
        self.assertAlmostEqual(out["SPY"], 0.1, places=12)
        self.assertAlmostEqual(out["CASH"], 0.75, places=12)
        self.assertAlmostEqual(diag["total_released"], 0.03, places=12)

    def test_group_caps_release_to_cash(self):
        out, diag = apply_group_caps_cash_first(
            {"SPY": 0.2, "QQQ": 0.2, "CASH": 0.6},
            {"core": 0.3},
            {"SPY": "core", "QQQ": "core"},
        )

        self.assertAlmostEqual(out["SPY"], 0.15, places=12)
        self.assertAlmostEqual(out["QQQ"], 0.15, places=12)
        self.assertAlmostEqual(out["CASH"], 0.7, places=12)
        self.assertAlmostEqual(diag["total_released"], 0.1, places=12)

    def test_tighten_buy_delta_is_tighten_only(self):
        out, diag = tighten_buy_delta(
            {"SPY": 0.2, "CASH": 0.8},
            {"SPY": 0.1, "CASH": 0.9},
            0.03,
        )

        self.assertAlmostEqual(out["SPY"], 0.13, places=12)
        self.assertLessEqual(out["SPY"], 0.2)
        self.assertAlmostEqual(out["CASH"], 0.87, places=12)
        self.assertEqual(diag["events"][0]["mutation_type"], "cap_single_buy_delta")

    def test_tighten_sell_delta_is_conditional(self):
        out, diag = tighten_sell_delta(
            {"SPY": 0.0, "CASH": 1.0},
            {"SPY": 0.2, "CASH": 0.8},
            0.05,
        )

        self.assertAlmostEqual(out["SPY"], 0.15, places=12)
        self.assertGreater(out["SPY"], 0.0)
        self.assertEqual(diag["events"][0]["mutation_type"], "sell_delta_throttle")

    def test_minimum_weight_floor_clears_small_positions_to_cash(self):
        out, diag = apply_minimum_weight_floor(
            {"XLU": 0.001, "XLRE": 0.0049, "XLI": 0.006, "CASH": 0.9881},
            min_weight=0.005,
        )

        self.assertAlmostEqual(out["XLU"], 0.0, places=12)
        self.assertAlmostEqual(out["XLRE"], 0.0, places=12)
        self.assertAlmostEqual(out["XLI"], 0.006, places=12)
        self.assertAlmostEqual(out["CASH"], 0.994, places=12)
        self.assertAlmostEqual(sum(out.values()), 1.0, places=12)
        self.assertAlmostEqual(diag["total_released"], 0.0059, places=12)
        self.assertEqual(
            [event["ticker"] for event in diag["cleared_positions"]],
            ["XLRE", "XLU"],
        )

    def test_assert_invariants_requires_cash_and_sum_at_most_one(self):
        assert_invariants({"SPY": 0.2, "CASH": 0.8}, label="ok")

        with self.assertRaises(AssertionError):
            assert_invariants({"SPY": 0.2}, label="missing_cash")
        with self.assertRaises(AssertionError):
            assert_invariants({"SPY": 0.8, "CASH": 0.4}, label="too_large")


if __name__ == "__main__":
    unittest.main()
