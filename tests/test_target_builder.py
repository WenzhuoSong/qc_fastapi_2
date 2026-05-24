import inspect
import unittest

from services.target_builder import build_target_weights, compare_target_weights


class TargetBuilderTest(unittest.TestCase):
    def test_builds_repeatable_shadow_target_from_governance(self):
        payload = dict(
            base_weights={"QQQ": 0.14, "CASH": 0.86},
            current_weights={"QQQ": 0.12, "CASH": 0.88},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={
                "position_decisions": [
                    {
                        "ticker": "QQQ",
                        "target_after": 0.11,
                        "reason_codes": ["unrealized_loss_review"],
                        "allowed_actions": ["hold", "trim"],
                    }
                ],
                "advisory_overrides": [
                    {
                        "ticker": "QQQ",
                        "validator_result": "accepted_as_trim_1.00%",
                        "target_before_override": 0.12,
                        "target_after_override": 0.11,
                    }
                ],
            },
            validated_advisory=[],
            constraints={},
            mode="target_builder_shadow",
        )

        first = build_target_weights(**payload).to_dict()
        second = build_target_weights(**payload).to_dict()

        self.assertEqual(first, second)
        self.assertEqual(first["diagnostics"]["mode"], "target_builder_shadow")
        self.assertEqual(first["diagnostics"]["execution_effect"], "none")
        self.assertFalse(first["diagnostics"]["consumes_raw_llm_adjusted_weights"])
        self.assertFalse(first["diagnostics"]["raw_llm_adjusted_weights_consumed"])
        self.assertEqual(first["target_weights"]["QQQ"], 0.11)
        self.assertEqual(first["per_ticker"]["QQQ"]["validated_llm_delta"], -0.01)
        self.assertIn("governance_adjustment", first["target_build_steps"])

    def test_gated_mode_is_explicit_in_diagnostics(self):
        out = build_target_weights(
            base_weights={"QQQ": 0.14, "CASH": 0.86},
            current_weights={"QQQ": 0.12, "CASH": 0.88},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={},
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["diagnostics"]["mode"], "target_builder_gated")
        self.assertFalse(out["diagnostics"]["raw_llm_adjusted_weights_consumed"])
        self.assertFalse(out["diagnostics"]["construction_participated"])
        self.assertIsNone(out["per_ticker"]["QQQ"]["construction_weight"])

    def test_gated_mode_can_start_from_portfolio_construction_weights(self):
        out = build_target_weights(
            base_weights={"SPY": 0.10, "CASH": 0.90},
            construction_weights={"SPY": 0.18, "CASH": 0.82},
            construction_source="portfolio_construction",
            current_weights={"SPY": 0.10, "CASH": 0.90},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={},
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["target_weights"]["SPY"], 0.18)
        self.assertEqual(out["diagnostics"]["target_construction_source"], "portfolio_construction")
        self.assertTrue(out["diagnostics"]["construction_participated"])
        self.assertEqual(out["per_ticker"]["SPY"]["base_weight"], 0.10)
        self.assertEqual(out["per_ticker"]["SPY"]["construction_weight"], 0.18)
        self.assertIn("portfolio_construction", out["per_ticker"]["SPY"]["changed_by"])

    def test_construction_weight_zero_means_explicit_clear(self):
        out = build_target_weights(
            base_weights={"QQQ": 0.12, "CASH": 0.88},
            construction_weights={"QQQ": 0.0, "CASH": 1.0},
            construction_source="portfolio_construction",
            current_weights={"QQQ": 0.12, "CASH": 0.88},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={},
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["per_ticker"]["QQQ"]["construction_weight"], 0.0)
        self.assertEqual(out["per_ticker"]["QQQ"]["final_target"], 0.0)
        self.assertIn("portfolio_construction", out["per_ticker"]["QQQ"]["changed_by"])

    def test_scorecard_no_add_clips_base_target_to_current(self):
        out = build_target_weights(
            base_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            market_scorecard={"investment_permission": "reduce_risk_only"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={},
        ).to_dict()

        self.assertEqual(out["target_weights"]["SPY"], 0.10)
        self.assertTrue(any(item.startswith("scorecard_no_add:SPY") for item in out["violations"]))

    def test_single_delta_and_turnover_caps_are_deterministic(self):
        out = build_target_weights(
            base_weights={"SPY": 0.50, "CASH": 0.50},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            market_scorecard={},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={"max_single_delta": 0.05, "max_turnover": 0.04},
        ).to_dict()

        self.assertLessEqual(out["target_weights"]["SPY"] - 0.10, 0.0401)
        self.assertTrue(any(item.startswith("single_delta_clip:SPY") for item in out["violations"]))
        self.assertTrue(any(item.startswith("turnover_clip:") for item in out["violations"]))
        self.assertTrue(out["turnover"]["within_budget"])

    def test_policy_caps_release_excess_to_cash(self):
        out = build_target_weights(
            base_weights={"PSI": 0.08, "CASH": 0.92},
            current_weights={"PSI": 0.05, "CASH": 0.95},
            market_scorecard={},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={},
        ).to_dict()

        self.assertEqual(out["target_weights"]["PSI"], 0.075)
        self.assertAlmostEqual(out["target_weights"]["CASH"], 0.925)
        self.assertGreater(out["diagnostics"]["cash_raised_by_policy_cap"], 0)
        self.assertTrue(out["diagnostics"]["policy_cap_events"])

    def test_hedge_intent_overlay_trims_before_adding_hedge(self):
        out = build_target_weights(
            base_weights={"QQQ": 0.12, "SPY": 0.60, "CASH": 0.28},
            current_weights={"QQQ": 0.12, "SPY": 0.60, "CASH": 0.28},
            market_scorecard={},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={
                "hedge_intent": {
                    "triggered": True,
                    "reasons": ["test stress"],
                    "severity": 0.8,
                    "trim_targets": ["QQQ"],
                    "cash_raise_pct": 0.05,
                    "add_hedge_etf": True,
                    "hedge_instrument": "SQQQ",
                    "hedge_weight": 0.015,
                }
            },
        ).to_dict()

        self.assertLess(out["target_weights"]["QQQ"], 0.12)
        self.assertEqual(out["target_weights"]["SQQQ"], 0.015)
        self.assertTrue(out["diagnostics"]["hedge_intent"]["applied"])
        self.assertTrue(any(item.startswith("hedge_intent_trim:QQQ") for item in out["violations"]))

    def test_compare_target_weights_marks_review_thresholds(self):
        out = compare_target_weights(
            live_target_weights={"SPY": 0.10, "CASH": 0.90},
            shadow_target_weights={"SPY": 0.13, "CASH": 0.87},
        )

        self.assertEqual(out["max_abs_diff"], 0.03)
        self.assertTrue(out["requires_review"])
        self.assertEqual(out["diffs"]["SPY"]["diff"], 0.03)

    def test_contract_does_not_accept_raw_adjusted_weights(self):
        signature = inspect.signature(build_target_weights)

        self.assertNotIn("adjusted_weights", signature.parameters)
        self.assertNotIn("raw_llm_adjusted_weights", signature.parameters)


if __name__ == "__main__":
    unittest.main()
