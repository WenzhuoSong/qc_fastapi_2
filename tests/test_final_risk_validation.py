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
        self.assertEqual(out["missing_mutation_ledger_tickers"], ["PSI"])

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
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": "turnover_scale_toward_current",
                                "ticker": "SPY",
                                "before": 0.20,
                                "after": 0.15,
                                "reason": "turnover scaled",
                            }
                        ]
                    }
                ],
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

    def test_blocking_mode_allows_conditional_reduction_of_restricted_ticker(self):
        out = validate_final_execution_target(
            risk_approved_target={"QQQ": 0.1500, "CASH": 0.8500},
            final_target={"QQQ": 0.1483, "CASH": 0.8517},
            current_weights={"QQQ": 0.1606, "CASH": 0.8394},
            policy_context={
                "post_risk_mutation_types": ["turnover_scale_toward_current"],
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": "turnover_scale_toward_current",
                                "ticker": "QQQ",
                                "before": 0.1500,
                                "after": 0.1483,
                                "reason": "turnover scaled but still reduced restricted ticker",
                            }
                        ]
                    }
                ],
                "scorecard_restricted_tickers": ["QQQ"],
                "material_drift_threshold": 0.001,
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertEqual(out["conditional_mutation_violations"], [])
        self.assertEqual(out["blocking_violations"], [])

    def test_blocking_mode_rejects_conditional_reversal_of_restricted_trim(self):
        out = validate_final_execution_target(
            risk_approved_target={"QQQ": 0.1400, "CASH": 0.8600},
            final_target={"QQQ": 0.1500, "CASH": 0.8500},
            current_weights={"QQQ": 0.1606, "CASH": 0.8394},
            policy_context={
                "post_risk_mutation_types": ["turnover_scale_toward_current"],
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": "turnover_scale_toward_current",
                                "ticker": "QQQ",
                                "before": 0.1400,
                                "after": 0.1500,
                                "reason": "turnover scaled restricted trim back up",
                            }
                        ]
                    }
                ],
                "scorecard_restricted_tickers": ["QQQ"],
                "material_drift_threshold": 0.001,
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertFalse(out["approved"])
        self.assertIn("conditional_mutation_contract_violation", out["blocking_violations"])
        self.assertEqual(
            out["conditional_mutation_violations"][0]["type"],
            "conditional_reverses_risk_trim",
        )

    def test_blocking_mode_rejects_hard_risk_trim_suppressed_below_minimum(self):
        out = validate_final_execution_target(
            risk_approved_target={"XLE": 0.1000, "CASH": 0.9000},
            final_target={"XLE": 0.0990, "CASH": 0.9010},
            current_weights={"XLE": 0.1000, "CASH": 0.9000},
            policy_context={
                "post_risk_mutation_types": ["turnover_scale_toward_current"],
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": "turnover_scale_toward_current",
                                "ticker": "XLE",
                                "before": 0.1000,
                                "after": 0.0990,
                                "reason": "hard-risk trim almost fully suppressed",
                            }
                        ]
                    }
                ],
                "hard_risk_tickers": ["XLE"],
                "material_drift_threshold": 0.001,
                "forced_trim_min_delta": 0.005,
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertFalse(out["approved"])
        self.assertIn("conditional_mutation_contract_violation", out["blocking_violations"])
        self.assertEqual(
            out["conditional_mutation_violations"][0]["type"],
            "hard_risk_trim_suppressed",
        )

    def test_conditional_mutation_details_prevent_unrelated_restricted_drift_false_positive(self):
        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.20, "XLE": 0.10, "CASH": 0.70},
            final_target={"SPY": 0.15, "XLE": 0.09, "CASH": 0.76},
            current_weights={"SPY": 0.20, "XLE": 0.10, "CASH": 0.70},
            policy_context={
                "post_risk_mutation_types": [
                    "defer_sell_due_to_min_hold_days",
                    "cash_raise_from_policy_cap",
                ],
                "post_risk_mutation_details": [
                    {
                        "type": "defer_sell_due_to_min_hold_days",
                        "ticker": "SPY",
                        "before": 0.10,
                        "after": 0.15,
                    },
                    {
                        "type": "cash_raise_from_policy_cap",
                        "ticker": "XLE",
                        "before": 0.10,
                        "after": 0.09,
                    }
                ],
                "hard_risk_tickers": ["XLE"],
                "material_drift_threshold": 0.001,
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertEqual(out["conditional_detail_tickers"], ["SPY"])
        self.assertEqual(out["missing_mutation_ledger_tickers"], [])
        self.assertEqual(out["conditional_mutation_violations"], [])
        self.assertEqual(out["blocking_violations"], [])

    def test_incomplete_conditional_mutation_details_fall_back_to_conservative_review(self):
        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.20, "XLE": 0.10, "CASH": 0.70},
            final_target={"SPY": 0.15, "XLE": 0.09, "CASH": 0.76},
            current_weights={"SPY": 0.20, "XLE": 0.10, "CASH": 0.70},
            policy_context={
                "post_risk_mutation_types": [
                    "defer_sell_due_to_min_hold_days",
                    "turnover_scale_toward_current",
                ],
                "post_risk_mutation_details": [
                    {
                        "type": "defer_sell_due_to_min_hold_days",
                        "ticker": "SPY",
                        "before": 0.10,
                        "after": 0.15,
                    }
                ],
                "hard_risk_tickers": ["XLE"],
                "material_drift_threshold": 0.001,
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertFalse(out["approved"])
        self.assertEqual(out["conditional_detail_tickers"], ["SPY"])
        self.assertEqual(out["missing_mutation_ledger_tickers"], ["XLE"])
        self.assertIn("incomplete_mutation_ledger", out["blocking_violations"])

    def test_governance_trim_and_min_hold_ledgers_cover_all_post_risk_drift(self):
        out = validate_final_execution_target(
            risk_approved_target={
                "IWM": 0.092153,
                "QQQ": 0.149453,
                "SPY": 0.082453,
                "XLE": 0.109653,
                "XLI": 0.034553,
                "XLK": 0.150000,
                "XLU": 0.000826,
                "CASH": 0.254148,
            },
            final_target={
                "IWM": 0.103100,
                "QQQ": 0.140400,
                "SPY": 0.093400,
                "XLE": 0.090600,
                "XLI": 0.045500,
                "XLK": 0.147200,
                "XLU": 0.000000,
                "CASH": 0.253039,
            },
            current_weights={
                "IWM": 0.103100,
                "QQQ": 0.160000,
                "SPY": 0.093400,
                "XLE": 0.121000,
                "XLI": 0.045500,
                "XLK": 0.167000,
                "XLU": 0.001000,
                "CASH": 0.181000,
            },
            policy_context={
                "post_risk_mutation_types": [
                    "loss_trim",
                    "defer_sell_due_to_min_hold_days",
                ],
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": "loss_trim",
                                "ticker": "QQQ",
                                "before": 0.149453,
                                "after": 0.140400,
                                "reason": "position governance trim",
                            },
                            {
                                "type": "loss_trim",
                                "ticker": "XLE",
                                "before": 0.109653,
                                "after": 0.090600,
                                "reason": "position governance trim",
                            },
                            {
                                "type": "loss_trim",
                                "ticker": "XLK",
                                "before": 0.150000,
                                "after": 0.147200,
                                "reason": "position governance trim",
                            },
                            {
                                "type": "loss_trim",
                                "ticker": "XLU",
                                "before": 0.000826,
                                "after": 0.000000,
                                "reason": "position governance trim",
                            },
                        ]
                    },
                    {
                        "mutations": [
                            {
                                "type": "min_hold_defer_sell",
                                "ticker": "SPY",
                                "before": 0.082453,
                                "after": 0.093400,
                                "reason": "young position sell deferred",
                            },
                            {
                                "type": "min_hold_defer_sell",
                                "ticker": "IWM",
                                "before": 0.092153,
                                "after": 0.103100,
                                "reason": "young position sell deferred",
                            },
                            {
                                "type": "min_hold_defer_sell",
                                "ticker": "XLI",
                                "before": 0.034553,
                                "after": 0.045500,
                                "reason": "young position sell deferred",
                            },
                        ]
                    },
                ],
                "hard_risk_tickers": ["XLE", "XLU"],
                "scorecard_restricted_tickers": ["QQQ", "XLE", "XLK", "XLU"],
                "material_drift_threshold": 0.001,
                "require_human_confirmation_for_conditional_material_drift": False,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertEqual(out["missing_mutation_ledger_tickers"], [])
        self.assertNotIn("incomplete_mutation_ledger", out["blocking_violations"])
        self.assertEqual(out["conditional_mutation_violations"], [])

    def test_blocking_mode_allows_typed_tighten_only_policy_cap_drift(self):
        out = validate_final_execution_target(
            risk_approved_target={"PSI": 0.08, "CASH": 0.92},
            final_target={"PSI": 0.075, "CASH": 0.925},
            current_weights={"PSI": 0.05, "CASH": 0.95},
            policy_context={
                "post_risk_mutation_types": ["cash_raise_from_policy_cap"],
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": "cash_raise_from_policy_cap",
                                "ticker": "PSI",
                                "before": 0.08,
                                "after": 0.075,
                                "reason": "policy cap",
                            }
                        ]
                    }
                ],
                "material_drift_threshold": 0.001,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertEqual(out["blocking_violations"], [])

    def test_blocking_mode_rejects_typed_drift_without_ticker_ledger(self):
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

        self.assertFalse(out["approved"])
        self.assertEqual(out["missing_mutation_ledger_tickers"], ["PSI"])
        self.assertIn("incomplete_mutation_ledger", out["blocking_violations"])

    def test_blocking_mode_allows_decay_risk_auto_reduce_mutation(self):
        out = validate_final_execution_target(
            risk_approved_target={"UVXY": 0.03, "CASH": 0.97},
            final_target={"UVXY": 0.02, "CASH": 0.98},
            current_weights={"UVXY": 0.03, "CASH": 0.97},
            policy_context={
                "post_risk_mutation_types": ["decay_risk_auto_reduce"],
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": "decay_risk_auto_reduce",
                                "ticker": "UVXY",
                                "before": 0.03,
                                "after": 0.02,
                                "reason": "decay auto reduce",
                            }
                        ]
                    }
                ],
                "material_drift_threshold": 0.001,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertIn("decay_risk_auto_reduce", out["allowed_mutation_types"])


if __name__ == "__main__":
    unittest.main()
