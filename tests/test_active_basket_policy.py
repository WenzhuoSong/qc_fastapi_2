import unittest

from services.active_basket_policy import evaluate_active_basket_policy


class ActiveBasketPolicyTest(unittest.TestCase):
    def test_evaluates_role_counts_and_subscale_positions(self):
        out = evaluate_active_basket_policy(
            {
                "QQQ": 0.049,
                "SPY": 0.061,
                "XLK": 0.10,
                "XLE": 0.011,
                "FTXL": 0.043,
                "PSI": 0.044,
                "CASH": 0.697,
            }
        )

        self.assertEqual(out["execution_effect"], "diagnostic_only")
        self.assertEqual(out["active_count"], 6)
        self.assertTrue(out["within_target_active_count"])
        self.assertEqual(out["roles"]["core"]["active_count"], 2)
        self.assertEqual(out["roles"]["sector"]["active_count"], 2)
        self.assertEqual(out["roles"]["thematic"]["active_count"], 2)
        self.assertEqual(out["subscale_count"], 1)
        self.assertEqual(out["subscale_positions"][0]["ticker"], "QQQ")
        self.assertAlmostEqual(out["subscale_positions"][0]["role_min_weight"], 0.05)

    def test_carries_floor_cleared_positions(self):
        out = evaluate_active_basket_policy(
            {"XLK": 0.063, "CASH": 0.937},
            minimum_weight_floor_events=[
                {"ticker": "XLRE", "original": 0.0018, "reason": "below_minimum_executable_weight"},
                {"ticker": "XLU", "original": 0.0010},
            ],
        )

        self.assertEqual(out["active_count"], 1)
        self.assertEqual(out["floor_cleared_count"], 2)
        self.assertEqual([row["ticker"] for row in out["floor_cleared_positions"]], ["XLRE", "XLU"])
        self.assertIn("global_active_count_below_target:1<4", out["warnings"])


if __name__ == "__main__":
    unittest.main()
