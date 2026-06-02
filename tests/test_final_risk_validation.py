import unittest

from services.final_risk_validation import validate_final_execution_target


class FinalRiskValidationTest(unittest.TestCase):
    def test_observe_mode_hard_blocks_unknown_positive_weight(self):
        out = validate_final_execution_target(
            risk_approved_target={"CASH": 1.0},
            final_target={"COMPLETELY_UNKNOWN": 0.02, "CASH": 0.98},
            current_weights={"CASH": 1.0},
            policy_context={},
            mode="observe",
        )

        self.assertFalse(out["approved"])
        self.assertTrue(out["severe_block"])
        self.assertEqual(out["severe_violations"][0]["type"], "unknown_ticker_positive_weight")

    def test_observe_mode_records_allowed_mutation_drift(self):
        out = validate_final_execution_target(
            risk_approved_target={"PSI": 0.08, "CASH": 0.92},
            final_target={"PSI": 0.075, "CASH": 0.925},
            current_weights={"PSI": 0.05, "CASH": 0.95},
            policy_context={"post_risk_mutation_types": ["cash_raise_from_policy_cap"]},
            mode="observe",
        )

        self.assertTrue(out["approved"])
        self.assertFalse(out["severe_block"])
        self.assertEqual(out["mutation_types"], ["cash_raise_from_policy_cap"])
        self.assertEqual(out["drift"]["max_abs_drift"], 0.005)

    def test_blocking_mode_rejects_untyped_drift(self):
        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.10, "CASH": 0.90},
            final_target={"SPY": 0.12, "CASH": 0.88},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            policy_context={},
            mode="blocking",
        )

        self.assertFalse(out["approved"])
        self.assertTrue(out["unsafe_untyped_drift"])

    def test_hard_risk_new_exposure_is_severe(self):
        out = validate_final_execution_target(
            risk_approved_target={"CASH": 1.0},
            final_target={"XLE": 0.01, "CASH": 0.99},
            current_weights={"CASH": 1.0},
            policy_context={"hard_risk_tickers": ["XLE"]},
            mode="observe",
        )

        self.assertFalse(out["approved"])
        self.assertEqual(out["severe_violations"][0]["type"], "new_hard_risk_exposure")

    def test_blocking_mode_rejects_policy_violation_even_with_allowed_mutation(self):
        out = validate_final_execution_target(
            risk_approved_target={"PSI": 0.075, "CASH": 0.925},
            final_target={"PSI": 0.08, "CASH": 0.92},
            current_weights={"PSI": 0.05, "CASH": 0.95},
            policy_context={"post_risk_mutation_types": ["cash_raise_from_policy_cap"]},
            mode="blocking",
        )

        self.assertFalse(out["approved"])
        self.assertIn("execution_policy_violation", out["blocking_violations"])

    def test_blocking_mode_allows_conditional_mutation_that_only_tightens_approved_buy(self):
        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
            final_target={"SPY": 0.15, "CASH": 0.85},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            policy_context={
                "post_risk_mutation_types": ["turnover_scale_toward_current"],
                "material_drift_threshold": 0.01,
            },
            mode="blocking",
        )

        self.assertFalse(out["approved"])
        self.assertIn("conditional_mutation_material_drift_requires_human_confirmation", out["blocking_violations"])
        self.assertEqual(out["conditional_mutation_violations"], [])

    def test_blocking_mode_allows_conditional_material_drift_when_human_review_disabled(self):
        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
            final_target={"SPY": 0.15, "CASH": 0.85},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            policy_context={
                "post_risk_mutation_types": ["turnover_scale_toward_current"],
                "material_drift_threshold": 0.01,
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertEqual(out["blocking_violations"], [])

    def test_blocking_mode_rejects_conditional_mutation_that_increases_restricted_ticker(self):
        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
            final_target={"SPY": 0.15, "CASH": 0.85},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            policy_context={
                "post_risk_mutation_types": ["turnover_scale_toward_current"],
                "material_drift_threshold": 0.01,
                "scorecard_restricted_tickers": ["SPY"],
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertFalse(out["approved"])
        self.assertIn("conditional_mutation_contract_violation", out["blocking_violations"])
        self.assertEqual(
            out["conditional_mutation_violations"][0]["type"],
            "conditional_increases_restricted_ticker",
        )

    def test_blocking_mode_allows_typed_tighten_only_policy_cap_drift(self):
        out = validate_final_execution_target(
            risk_approved_target={"PSI": 0.08, "CASH": 0.92},
            final_target={"PSI": 0.075, "CASH": 0.925},
            current_weights={"PSI": 0.05, "CASH": 0.95},
            policy_context={
                "post_risk_mutation_types": ["cash_raise_from_policy_cap"],
                "material_drift_threshold": 0.001,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertEqual(out["blocking_violations"], [])

    def test_blocking_mode_allows_decay_risk_auto_reduce_mutation(self):
        out = validate_final_execution_target(
            risk_approved_target={"UVXY": 0.03, "CASH": 0.97},
            final_target={"UVXY": 0.02, "CASH": 0.98},
            current_weights={"UVXY": 0.03, "CASH": 0.97},
            policy_context={
                "post_risk_mutation_types": ["decay_risk_auto_reduce"],
                "material_drift_threshold": 0.001,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertIn("decay_risk_auto_reduce", out["allowed_mutation_types"])


if __name__ == "__main__":
    unittest.main()
