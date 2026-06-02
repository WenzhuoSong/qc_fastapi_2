import inspect
import unittest
from datetime import UTC, datetime

from services.target_builder import (
    ALLOWED_EVIDENCE_FIELDS,
    FORBIDDEN_EVIDENCE_FIELDS,
    build_target_weights,
    compare_target_weights,
)


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
            constraints={
                "portfolio_construction_gate": {
                    "configured_mode": "gated",
                    "effective_mode": "deterministic_target_builder",
                    "gate_status": "blocked",
                    "gate_eligible": False,
                    "blocked_reason": "promotion_gate_not_eligible",
                    "gate_blockers": ["insufficient_cycles"],
                }
            },
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["diagnostics"]["mode"], "target_builder_gated")
        self.assertFalse(out["diagnostics"]["raw_llm_adjusted_weights_consumed"])
        self.assertFalse(out["diagnostics"]["construction_participated"])
        self.assertEqual(out["diagnostics"]["target_construction_source"], "deterministic_target_builder")
        self.assertEqual(out["diagnostics"]["portfolio_construction_configured_mode"], "gated")
        self.assertEqual(out["diagnostics"]["portfolio_construction_effective_mode"], "deterministic_target_builder")
        self.assertEqual(out["diagnostics"]["portfolio_construction_gate_status"], "blocked")
        self.assertFalse(out["diagnostics"]["portfolio_construction_gate_eligible"])
        self.assertEqual(out["diagnostics"]["portfolio_construction_blocked_reason"], "promotion_gate_not_eligible")
        self.assertEqual(out["diagnostics"]["portfolio_construction_gate_blockers"], ["insufficient_cycles"])
        self.assertIsNone(out["per_ticker"]["QQQ"]["construction_weight"])
        self.assertTrue(out["diagnostics"]["diagnostic_weight_inputs_rejected"])
        self.assertEqual(
            out["diagnostics"]["weight_source_contract"]["contract_version"],
            "weight_source_contract_v1",
        )
        self.assertEqual(out["diagnostics"]["target_weight_authority"], "executable")

    def test_rejects_legacy_diagnostic_weight_source_keys(self):
        with self.assertRaisesRegex(AssertionError, "Forbidden target_builder input weight source"):
            build_target_weights(
                base_weights={"QQQ": 0.14, "CASH": 0.86},
                current_weights={"QQQ": 0.12, "CASH": 0.88},
                market_scorecard={"investment_permission": "normal_rebalance"},
                decision_style={},
                position_governance={"position_decisions": []},
                validated_advisory=[],
                constraints={"llm_adjusted_weights": {"QQQ": 0.90, "CASH": 0.10}},
                mode="target_builder_gated",
            )

    def test_rejects_nested_legacy_weight_source_keys(self):
        with self.assertRaisesRegex(AssertionError, "portfolio_construction_gate.pc_shadow_weights"):
            build_target_weights(
                base_weights={"QQQ": 0.14, "CASH": 0.86},
                current_weights={"QQQ": 0.12, "CASH": 0.88},
                market_scorecard={"investment_permission": "normal_rebalance"},
                decision_style={},
                position_governance={"position_decisions": []},
                validated_advisory=[],
                constraints={
                    "portfolio_construction_gate": {
                        "pc_shadow_weights": {"QQQ": 0.25, "CASH": 0.75}
                    }
                },
                mode="target_builder_gated",
            )

    def test_conviction_fields_are_visible_but_not_consumed(self):
        out = build_target_weights(
            base_weights={"QQQ": 0.14, "CASH": 0.86},
            current_weights={"QQQ": 0.12, "CASH": 0.88},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={
                "position_decisions": [
                    {
                        "ticker": "QQQ",
                        "target_after": 0.11,
                        "effective_confidence": 1.0,
                        "conviction": 1.0,
                        "evidence": {"conviction_status": "calibrated"},
                    }
                ]
            },
            validated_advisory=[],
            constraints={},
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["target_weights"]["QQQ"], 0.11)
        self.assertFalse(out["diagnostics"]["forbidden_evidence_fields_consumed"])
        self.assertIn("conviction", out["diagnostics"]["forbidden_evidence_fields_seen"])
        self.assertIn("effective_confidence", out["diagnostics"]["forbidden_evidence_fields_seen"])
        self.assertIn("evidence.conviction_status", out["diagnostics"]["forbidden_evidence_fields_seen"])
        self.assertNotIn("effective_confidence", ALLOWED_EVIDENCE_FIELDS)
        self.assertIn("effective_confidence", FORBIDDEN_EVIDENCE_FIELDS)

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

    def test_evidence_cap_shadow_records_would_apply_without_clipping(self):
        out = build_target_weights(
            base_weights={"DRAM": 0.03, "CASH": 0.97},
            current_weights={"DRAM": 0.01, "CASH": 0.99},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={
                "evidence_cap_diagnostics": {
                    "DRAM": {
                        "static_cap": 0.05,
                        "evidence_adjusted_cap": 0.0212,
                        "would_clip": True,
                        "coverage_ratio": 0.5,
                        "evidence_quality_multiplier": 0.424,
                        "conviction_status": "early_signal",
                        "history_days": 55,
                        "voted_count": 1,
                        "abstain_count": 1,
                        "mapping_error_count": 0,
                    }
                }
            },
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["target_weights"]["DRAM"], 0.03)
        shadow = out["diagnostics"]["evidence_cap_shadow"]
        self.assertTrue(shadow["enabled"])
        self.assertEqual(shadow["configured_mode"], "observe")
        self.assertEqual(shadow["effective_mode"], "observe")
        self.assertEqual(shadow["execution_effect"], "diagnostic_only")
        self.assertEqual(shadow["target_weight_mutation"], "none")
        self.assertEqual(shadow["would_apply_count"], 1)
        self.assertEqual(shadow["applied_count"], 0)
        self.assertEqual(shadow["rows"][0]["ticker"], "DRAM")
        self.assertTrue(shadow["rows"][0]["would_apply_cap"])
        self.assertFalse(shadow["rows"][0]["applied_cap"])
        self.assertEqual(shadow["rows"][0]["would_clip_to"], 0.0212)
        self.assertEqual(out["per_ticker"]["DRAM"]["evidence_cap_shadow"]["evidence_adjusted_cap"], 0.0212)

    def test_evidence_cap_gated_falls_back_to_observe_when_criteria_not_met(self):
        out = build_target_weights(
            base_weights={"DRAM": 0.03, "CASH": 0.97},
            current_weights={"DRAM": 0.01, "CASH": 0.99},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={
                "evidence_cap_config": {"mode": "gated"},
                "evidence_cap_diagnostics": {
                    "DRAM": {
                        "static_cap": 0.05,
                        "evidence_adjusted_cap": 0.0212,
                        "would_clip": True,
                    }
                },
            },
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["target_weights"]["DRAM"], 0.03)
        shadow = out["diagnostics"]["evidence_cap_shadow"]
        self.assertEqual(shadow["configured_mode"], "gated")
        self.assertEqual(shadow["effective_mode"], "observe")
        self.assertEqual(shadow["blocked_reason"], "enforcement_criteria_not_met")
        self.assertIn("insufficient_observe_cycles", shadow["gate_blockers"])
        self.assertEqual(shadow["applied_count"], 0)
        self.assertFalse(shadow["rows"][0]["applied_cap"])

    def test_evidence_cap_gated_clips_and_releases_to_cash_when_criteria_met(self):
        out = build_target_weights(
            base_weights={"DRAM": 0.03, "CASH": 0.97},
            current_weights={"DRAM": 0.01, "CASH": 0.99},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={
                "evidence_cap_config": {
                    "mode": "gated",
                    "observe_cycles": 12,
                    "min_observe_cycles": 10,
                    "would_clip_rate": 0.20,
                    "max_would_clip_rate": 0.30,
                    "calibration_generated_at": datetime.now(UTC).isoformat(),
                },
                "evidence_cap_diagnostics": {
                    "DRAM": {
                        "static_cap": 0.05,
                        "evidence_adjusted_cap": 0.0212,
                        "would_clip": True,
                        "coverage_ratio": 0.5,
                        "evidence_quality_multiplier": 0.424,
                        "conviction_status": "early_signal",
                        "history_days": 55,
                    }
                },
            },
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["target_weights"]["DRAM"], 0.0212)
        self.assertAlmostEqual(out["target_weights"]["CASH"], 0.9788)
        self.assertTrue(any(item.startswith("evidence_cap:") for item in out["violations"]))
        shadow = out["diagnostics"]["evidence_cap_shadow"]
        self.assertEqual(shadow["configured_mode"], "gated")
        self.assertEqual(shadow["effective_mode"], "gated")
        self.assertEqual(shadow["execution_effect"], "tighten_only")
        self.assertTrue(shadow["calibration_freshness"]["fresh"])
        self.assertEqual(shadow["target_weight_mutation"], "tighten_only")
        self.assertEqual(shadow["applied_count"], 1)
        self.assertAlmostEqual(shadow["cash_raised_by_evidence_cap"], 0.0088)
        self.assertTrue(shadow["rows"][0]["applied_cap"])
        self.assertEqual(shadow["rows"][0]["target_before_cap"], 0.03)
        self.assertEqual(shadow["rows"][0]["target_after_cap"], 0.0212)
        self.assertIn("evidence_cap", out["per_ticker"]["DRAM"]["changed_by"])
        self.assertTrue(out["per_ticker"]["DRAM"]["evidence_cap_shadow"]["applied_cap"])

    def test_evidence_cap_does_not_raise_above_static_cap(self):
        out = build_target_weights(
            base_weights={"DRAM": 0.05, "CASH": 0.95},
            current_weights={"DRAM": 0.02, "CASH": 0.98},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={
                "evidence_cap_config": {
                    "mode": "gated",
                    "observe_cycles": 10,
                    "would_clip_rate": 0.10,
                    "calibration_generated_at": datetime.now(UTC).isoformat(),
                },
                "evidence_cap_diagnostics": {
                    "DRAM": {
                        "static_cap": 0.03,
                        "evidence_adjusted_cap": 0.04,
                        "would_clip": False,
                    }
                },
            },
            mode="target_builder_gated",
        ).to_dict()

        self.assertEqual(out["target_weights"]["DRAM"], 0.03)
        shadow = out["diagnostics"]["evidence_cap_shadow"]
        self.assertEqual(shadow["rows"][0]["evidence_enforcement_cap"], 0.03)
        self.assertTrue(shadow["rows"][0]["applied_cap"])

    def test_evidence_cap_gated_does_not_clip_in_target_builder_shadow_mode(self):
        out = build_target_weights(
            base_weights={"DRAM": 0.03, "CASH": 0.97},
            current_weights={"DRAM": 0.01, "CASH": 0.99},
            market_scorecard={"investment_permission": "normal_rebalance"},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={
                "evidence_cap_config": {
                    "mode": "gated",
                    "observe_cycles": 12,
                    "min_observe_cycles": 10,
                    "would_clip_rate": 0.20,
                    "max_would_clip_rate": 0.30,
                    "calibration_generated_at": datetime.now(UTC).isoformat(),
                },
                "evidence_cap_diagnostics": {
                    "DRAM": {
                        "static_cap": 0.05,
                        "evidence_adjusted_cap": 0.0212,
                        "would_clip": True,
                    }
                },
            },
            mode="target_builder_shadow",
        ).to_dict()

        self.assertEqual(out["target_weights"]["DRAM"], 0.03)
        shadow = out["diagnostics"]["evidence_cap_shadow"]
        self.assertEqual(shadow["configured_mode"], "gated")
        self.assertEqual(shadow["effective_mode"], "observe")
        self.assertEqual(shadow["blocked_reason"], "target_builder_shadow_no_execution_authority")
        self.assertEqual(shadow["applied_count"], 0)
        self.assertEqual(shadow["target_weight_mutation"], "none")
        self.assertFalse(shadow["rows"][0]["applied_cap"])

    def test_evidence_cap_shadow_disabled_without_input(self):
        out = build_target_weights(
            base_weights={"SPY": 0.10, "CASH": 0.90},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            market_scorecard={},
            decision_style={},
            position_governance={"position_decisions": []},
            validated_advisory=[],
            constraints={},
        ).to_dict()

        shadow = out["diagnostics"]["evidence_cap_shadow"]
        self.assertFalse(shadow["enabled"])
        self.assertEqual(shadow["would_apply_count"], 0)
        self.assertEqual(shadow["rows"], [])

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

    def test_weight_arithmetic_uses_weight_ops_contract(self):
        import services.target_builder as target_builder_module

        source = inspect.getsource(target_builder_module)

        self.assertIn("from services.weight_ops import", source)
        self.assertIn("normalize_cash_first", source)
        self.assertIn("apply_single_caps_cash_first", source)
        self.assertNotIn("def _normalize_cash_first", source)


if __name__ == "__main__":
    unittest.main()
