import unittest

from services.portfolio_construction_evaluator import (
    build_portfolio_construction_promotion_gate,
    build_portfolio_construction_rollout_gate,
    criteria_from_pc_promotion_config,
    evaluate_portfolio_construction_shadow,
    is_gated_semi_auto_confirmed_risk_output,
    readiness_limits_from_pc_promotion_config,
    summarize_portfolio_construction_readiness,
)


class PortfolioConstructionEvaluatorTests(unittest.TestCase):
    def test_marks_clean_shadow_as_promotion_candidate(self):
        out = evaluate_portfolio_construction_shadow(
            shadow_weights={"SPY": 0.20, "QQQ": 0.10, "PSI": 0.05, "IWM": 0.05, "CASH": 0.60},
            actual_weights={"SPY": 0.20, "QQQ": 0.10, "PSI": 0.05, "IWM": 0.05, "CASH": 0.60},
            current_weights={"SPY": 0.18, "QQQ": 0.10, "PSI": 0.04, "IWM": 0.05, "CASH": 0.63},
        ).to_dict()

        self.assertTrue(out["promotion_ready"])
        self.assertEqual(out["status"], "promotion_candidate")
        self.assertEqual(out["execution_authority"], "none")
        self.assertTrue(out["metrics"]["basket_policy_ok"])
        self.assertTrue(out["metrics"]["candidate_policy_allowed"])

    def test_blocks_shadow_policy_violation(self):
        out = evaluate_portfolio_construction_shadow(
            shadow_weights={"PSI": 0.08, "CASH": 0.92},
            actual_weights={"PSI": 0.05, "CASH": 0.95},
            current_weights={"PSI": 0.05, "CASH": 0.95},
        ).to_dict()

        self.assertFalse(out["promotion_ready"])
        self.assertIn("shadow_policy_violation", out["blockers"])

    def test_blocks_higher_factor_violation_count(self):
        out = evaluate_portfolio_construction_shadow(
            shadow_weights={"QQQ": 0.20, "XLK": 0.15, "SOXX": 0.075, "PSI": 0.075, "CASH": 0.50},
            actual_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.20, "CASH": 0.80},
        ).to_dict()

        self.assertFalse(out["promotion_ready"])
        self.assertIn("shadow_factor_exposure_worse", out["blockers"])

    def test_blocks_turnover_delta_and_hard_risk_add(self):
        out = evaluate_portfolio_construction_shadow(
            shadow_weights={"SPY": 0.20, "XLE": 0.10, "CASH": 0.70},
            actual_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.20, "XLE": 0.00, "CASH": 0.80},
            hard_risk_tickers={"XLE"},
        ).to_dict()

        self.assertFalse(out["promotion_ready"])
        self.assertIn("shadow_turnover_too_high", out["blockers"])
        self.assertIn("shadow_adds_hard_risk_ticker", out["blockers"])

    def test_warns_when_shadow_reduces_qc_rejection_risk(self):
        out = evaluate_portfolio_construction_shadow(
            shadow_weights={"PSI": 0.075, "CASH": 0.925},
            actual_weights={"PSI": 0.08, "CASH": 0.92},
            current_weights={"PSI": 0.05, "CASH": 0.95},
        ).to_dict()

        self.assertIn("shadow_reduces_qc_rejection_risk", out["warnings"])

    def test_summarizes_rolling_readiness_candidate(self):
        evaluations = [
            {
                "promotion_ready": True,
                "blockers": [],
                "warnings": [],
                "metrics": {
                    "mean_abs_weight_deviation": 0.01,
                    "turnover_delta": 0.0,
                    "basket_policy_ok": True,
                    "candidate_policy_allowed": True,
                    "turnover_ok": True,
                    "subscale_count": 0,
                    "no_unclassified_mutations": True,
                },
            }
            for _ in range(3)
        ]

        out = summarize_portfolio_construction_readiness(evaluations, min_cycles=3, min_pass_rate=0.8)

        self.assertTrue(out["promotion_ready"])
        self.assertTrue(out["ready"])
        self.assertEqual(out["status"], "rolling_promotion_candidate")
        self.assertEqual(out["cycles"], 3)
        self.assertEqual(out["pass_rate"], 1.0)
        self.assertEqual(out["basket_policy_ok_rate"], 1.0)

    def test_summarizes_basket_readiness_low_rate_blocker(self):
        evaluations = [
            {
                "promotion_ready": True,
                "blockers": [],
                "warnings": [],
                "metrics": {
                    "mean_abs_weight_deviation": 0.01,
                    "turnover_delta": 0.0,
                    "basket_policy_ok": False,
                    "candidate_policy_allowed": True,
                    "turnover_ok": True,
                    "subscale_count": 0,
                    "no_unclassified_mutations": True,
                },
            }
            for _ in range(3)
        ]

        out = summarize_portfolio_construction_readiness(evaluations, min_cycles=3, min_pass_rate=0.8)

        self.assertFalse(out["promotion_ready"])
        self.assertEqual(out["status"], "shadow_only")
        self.assertEqual(out["basket_policy_ok_rate"], 0.0)
        self.assertIn("basket_policy_ok_rate_below_threshold", out["blockers"])

    def test_summarizes_rolling_readiness_blockers(self):
        evaluations = [
            {
                "promotion_ready": False,
                "blockers": ["shadow_policy_violation"],
                "warnings": [],
                "metrics": {"mean_abs_weight_deviation": 0.04, "turnover_delta": 0.03},
            },
            {
                "promotion_ready": True,
                "blockers": [],
                "warnings": ["shadow_reduces_turnover"],
                "metrics": {"mean_abs_weight_deviation": 0.01, "turnover_delta": -0.01},
            },
        ]

        out = summarize_portfolio_construction_readiness(evaluations, min_cycles=2, min_pass_rate=0.8)

        self.assertFalse(out["promotion_ready"])
        self.assertEqual(out["status"], "shadow_only")
        self.assertEqual(out["blocker_counts"]["shadow_policy_violation"], 1)

    def test_promotion_gate_defaults_to_shadow_only(self):
        readiness = {
            "promotion_ready": True,
            "status": "rolling_promotion_candidate",
            "cycles": 20,
            "pass_rate": 1.0,
            "blocker_counts": {},
        }

        gate = build_portfolio_construction_promotion_gate(readiness)

        self.assertFalse(gate["eligible"])
        self.assertEqual(gate["status"], "shadow_only")
        self.assertEqual(gate["portfolio_construction_mode"], "shadow")
        self.assertIn("portfolio_construction_mode_shadow", gate["blockers"])
        self.assertEqual(gate["approval_mode"], "auto")
        self.assertEqual(gate["execution_authority"], "none")

    def test_promotion_gate_eligible_only_when_enabled_and_ready(self):
        readiness = {
            "promotion_ready": True,
            "status": "rolling_promotion_candidate",
            "cycles": 20,
            "pass_rate": 0.9,
            "blocker_counts": {},
        }

        gate = build_portfolio_construction_promotion_gate(
            readiness,
            {
                "portfolio_construction_mode": "candidate",
                "enabled": True,
                "require_manual_approval": True,
            },
        )

        self.assertTrue(gate["eligible"])
        self.assertEqual(gate["status"], "eligible_for_manual_review")
        self.assertEqual(gate["would_promote_to"], "portfolio_construction_gated")

    def test_promotion_gate_can_be_disabled(self):
        readiness = {
            "promotion_ready": True,
            "status": "rolling_promotion_candidate",
            "cycles": 20,
            "pass_rate": 1.0,
            "blocker_counts": {},
        }

        gate = build_portfolio_construction_promotion_gate(
            readiness,
            {"portfolio_construction_mode": "candidate", "enabled": False},
        )

        self.assertFalse(gate["eligible"])
        self.assertEqual(gate["status"], "disabled")
        self.assertIn("promotion_gate_disabled", gate["blockers"])

    def test_promotion_gate_inherits_basket_readiness_blockers(self):
        readiness = {
            "promotion_ready": False,
            "ready": False,
            "status": "shadow_only",
            "cycles": 20,
            "pass_rate": 1.0,
            "blocker_counts": {},
            "blockers": ["basket_policy_ok_rate_below_threshold"],
        }

        gate = build_portfolio_construction_promotion_gate(
            readiness,
            {"portfolio_construction_mode": "candidate", "enabled": True},
        )

        self.assertFalse(gate["eligible"])
        self.assertEqual(gate["status"], "blocked")
        self.assertIn("basket_policy_ok_rate_below_threshold", gate["blockers"])
        self.assertIn("readiness_not_promoted", gate["blockers"])

    def test_criteria_and_readiness_limits_use_pr4_config_names(self):
        cfg = {
            "max_material_diff": 0.012,
            "max_turnover_diff": 0.018,
            "min_shadow_cycles": 5,
            "min_pass_rate": 0.95,
        }

        criteria = criteria_from_pc_promotion_config(cfg)
        limits = readiness_limits_from_pc_promotion_config(cfg)

        self.assertEqual(criteria.max_mean_weight_deviation, 0.012)
        self.assertEqual(criteria.max_turnover_delta, 0.018)
        self.assertEqual(limits["limit"], 5)
        self.assertEqual(limits["min_cycles"], 5)
        self.assertEqual(limits["min_pass_rate"], 0.95)

    def test_readiness_limits_respect_explicit_zero_for_paper_live_canary(self):
        cfg = {
            "min_shadow_cycles": 0,
            "min_cycles": 0,
            "min_pass_rate": 0.0,
            "min_basket_policy_ok_rate": 0.0,
            "min_policy_ok_rate": 0.0,
            "min_turnover_ok_rate": 0.0,
            "max_subscale_position_rate": 1.0,
            "require_no_unclassified_mutations": False,
        }

        limits = readiness_limits_from_pc_promotion_config(cfg)
        readiness = summarize_portfolio_construction_readiness(
            [],
            min_cycles=limits["min_cycles"],
            min_pass_rate=limits["min_pass_rate"],
            min_basket_policy_ok_rate=limits["min_basket_policy_ok_rate"],
            min_policy_ok_rate=limits["min_policy_ok_rate"],
            min_turnover_ok_rate=limits["min_turnover_ok_rate"],
            max_mean_weight_deviation=limits["max_mean_weight_deviation"],
            max_subscale_position_rate=limits["max_subscale_position_rate"],
            require_no_unclassified_mutations=limits["require_no_unclassified_mutations"],
        )
        gate = build_portfolio_construction_rollout_gate(
            readiness,
            {"portfolio_construction_mode": "gated", "enabled": True, **cfg},
            auth_mode="SEMI_AUTO",
        )

        self.assertEqual(limits["min_cycles"], 0)
        self.assertEqual(limits["min_pass_rate"], 0.0)
        self.assertTrue(readiness["promotion_ready"])
        self.assertTrue(gate["eligible"])
        self.assertEqual(gate["status"], "semi_auto_gated_ready")

    def test_rollout_gate_allows_gated_semi_auto_when_ready(self):
        readiness = {
            "promotion_ready": True,
            "status": "rolling_promotion_candidate",
            "cycles": 20,
            "pass_rate": 1.0,
            "blocker_counts": {},
        }

        gate = build_portfolio_construction_rollout_gate(
            readiness,
            {"portfolio_construction_mode": "gated", "enabled": True},
            auth_mode="SEMI_AUTO",
        )

        self.assertTrue(gate["eligible"])
        self.assertEqual(gate["status"], "semi_auto_gated_ready")
        self.assertEqual(gate["rollout_phase"], "semi_auto_gated")

    def test_rollout_gate_blocks_full_auto_until_confirmed_and_enabled(self):
        readiness = {
            "promotion_ready": True,
            "status": "rolling_promotion_candidate",
            "cycles": 20,
            "pass_rate": 1.0,
            "blocker_counts": {},
        }

        gate = build_portfolio_construction_rollout_gate(
            readiness,
            {"portfolio_construction_mode": "gated", "enabled": True},
            auth_mode="FULL_AUTO",
            semi_auto_confirmed_cycles=2,
        )

        self.assertFalse(gate["eligible"])
        self.assertEqual(gate["status"], "rollout_blocked")
        self.assertIn("semi_auto_gated_confirmations_insufficient", gate["blockers"])
        self.assertIn("full_auto_gated_not_enabled", gate["blockers"])

    def test_rollout_gate_allows_full_auto_after_confirmed_and_enabled(self):
        readiness = {
            "promotion_ready": True,
            "status": "rolling_promotion_candidate",
            "cycles": 20,
            "pass_rate": 1.0,
            "blocker_counts": {},
        }

        gate = build_portfolio_construction_rollout_gate(
            readiness,
            {
                "portfolio_construction_mode": "gated",
                "enabled": True,
                "allow_full_auto_gated": True,
                "min_gated_semi_auto_confirmed_cycles": 5,
            },
            auth_mode="FULL_AUTO",
            semi_auto_confirmed_cycles=5,
        )

        self.assertTrue(gate["eligible"])
        self.assertEqual(gate["status"], "full_auto_gated_ready")

    def test_identifies_gated_semi_auto_confirmed_risk_output(self):
        self.assertTrue(
            is_gated_semi_auto_confirmed_risk_output(
                {
                    "target_builder_input": {
                        "diagnostics": {
                            "construction_participated": True,
                            "target_construction_source": "portfolio_construction",
                        }
                    },
                    "portfolio_construction_promotion_gate": {
                        "portfolio_construction_mode": "gated",
                        "eligible": True,
                    },
                }
            )
        )
        self.assertFalse(
            is_gated_semi_auto_confirmed_risk_output(
                {
                    "target_builder_input": {
                        "diagnostics": {
                            "construction_participated": False,
                            "target_construction_source": "deterministic_target_builder",
                        }
                    },
                    "portfolio_construction_promotion_gate": {
                        "portfolio_construction_mode": "candidate",
                        "eligible": True,
                    },
                }
            )
        )


if __name__ == "__main__":
    unittest.main()
