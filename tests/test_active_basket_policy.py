import unittest

from services.active_basket_policy import build_active_basket_calibration_report, evaluate_active_basket_policy


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

    def test_reads_strategy_breadth_report_without_enforcing_it(self):
        out = evaluate_active_basket_policy(
            {"QQQ": 0.08, "SPY": 0.08, "XLK": 0.07, "XLE": 0.05, "CASH": 0.72},
            strategy_breadth_report={
                "report_version": "strategy_breadth_calibration_v1",
                "estimated_independent_clusters": 3,
                "eligible_alpha_strategy_count": 5,
                "duplication_ratio": 0.4,
                "minimum_overlap": 60,
                "insufficient_overlap_pairs": 1,
                "execution_authority": "none",
                "target_weight_mutation": "none",
            },
        )

        self.assertEqual(out["active_count"], 4)
        self.assertTrue(out["within_target_active_count"])
        self.assertEqual(out["estimated_independent_clusters"], 3)
        self.assertEqual(out["strategy_breadth"]["estimated_independent_clusters"], 3)
        self.assertEqual(out["strategy_breadth"]["execution_authority"], "none")
        self.assertEqual(out["active_basket_calibration"]["report_version"], "active_basket_calibration_v1")
        self.assertEqual(out["active_basket_calibration"]["execution_effect"], "diagnostic_only")

    def test_calibration_report_shrinks_range_for_low_breadth_and_tail_noise(self):
        report = build_active_basket_calibration_report(
            active_basket_diagnostics={
                "active_count": 9,
                "target_active_count_min": 4,
                "target_active_count_max": 10,
                "subscale_count": 2,
                "floor_cleared_count": 1,
            },
            strategy_breadth_report={
                "estimated_independent_clusters": 3,
                "estimated_breadth_is_approximation": True,
                "execution_authority": "none",
                "target_weight_mutation": "none",
            },
            transaction_cost_summary={"transaction_cost_drag_pct": 0.003},
            realized_contribution_summary={"low_contribution_tail_count": 2},
        )

        self.assertEqual(report["execution_effect"], "diagnostic_only")
        self.assertEqual(report["operator_action"], "review_only")
        self.assertEqual(report["current_policy"], {"target_active_count_min": 4, "target_active_count_max": 10})
        self.assertEqual(report["estimated_independent_clusters"], 3)
        self.assertLess(report["suggested_range"][1], 10)
        self.assertEqual(report["suggestion"], "shrink_range_review")
        self.assertIn("estimated_breadth_3", report["suggestion_reason"])
        self.assertIn("subscale_positions_present", report["suggestion_reason"])
        self.assertIn("floor_cleared_positions_present", report["suggestion_reason"])
        self.assertIn("low_contribution_tail", report["suggestion_reason"])
        self.assertIn("transaction_cost_drag_material", report["suggestion_reason"])
        self.assertFalse(report["uses_effective_n_as_breadth"])

    def test_calibration_report_expands_only_when_breadth_is_high_and_clean(self):
        report = build_active_basket_calibration_report(
            active_basket_diagnostics={
                "active_count": 10,
                "target_active_count_min": 4,
                "target_active_count_max": 10,
                "subscale_count": 0,
                "floor_cleared_count": 0,
            },
            strategy_breadth_report={
                "estimated_independent_clusters": 11,
                "estimated_breadth_is_approximation": True,
                "execution_authority": "none",
                "target_weight_mutation": "none",
            },
            transaction_cost_summary={"transaction_cost_drag_pct": 0.0005},
            realized_contribution_summary={"low_contribution_tail_count": 0},
        )

        self.assertEqual(report["suggestion"], "expand_range_review")
        self.assertEqual(report["suggested_range"], [4, 12])
        self.assertIn("high_breadth_without_tail_or_cost_penalty", report["suggestion_reason"])


if __name__ == "__main__":
    unittest.main()
