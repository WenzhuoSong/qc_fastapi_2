import unittest

from services.portfolio_construction_evaluator import (
    build_portfolio_construction_promotion_gate,
    evaluate_portfolio_construction_shadow,
    summarize_portfolio_construction_readiness,
)


class PortfolioConstructionEvaluatorTests(unittest.TestCase):
    def test_marks_clean_shadow_as_promotion_candidate(self):
        out = evaluate_portfolio_construction_shadow(
            shadow_weights={"SPY": 0.20, "PSI": 0.05, "CASH": 0.75},
            actual_weights={"SPY": 0.20, "PSI": 0.05, "CASH": 0.75},
            current_weights={"SPY": 0.18, "PSI": 0.04, "CASH": 0.78},
        ).to_dict()

        self.assertTrue(out["promotion_ready"])
        self.assertEqual(out["status"], "promotion_candidate")
        self.assertEqual(out["execution_authority"], "none")

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
                "metrics": {"mean_abs_weight_deviation": 0.01, "turnover_delta": 0.0},
            }
            for _ in range(3)
        ]

        out = summarize_portfolio_construction_readiness(evaluations, min_cycles=3, min_pass_rate=0.8)

        self.assertTrue(out["promotion_ready"])
        self.assertEqual(out["status"], "rolling_promotion_candidate")
        self.assertEqual(out["cycles"], 3)
        self.assertEqual(out["pass_rate"], 1.0)

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

    def test_promotion_gate_defaults_auto_enabled(self):
        readiness = {
            "promotion_ready": True,
            "status": "rolling_promotion_candidate",
            "cycles": 20,
            "pass_rate": 1.0,
            "blocker_counts": {},
        }

        gate = build_portfolio_construction_promotion_gate(readiness)

        self.assertTrue(gate["eligible"])
        self.assertEqual(gate["status"], "auto_approved")
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
            {"enabled": True, "require_manual_approval": True},
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

        gate = build_portfolio_construction_promotion_gate(readiness, {"enabled": False})

        self.assertFalse(gate["eligible"])
        self.assertEqual(gate["status"], "disabled")
        self.assertIn("promotion_gate_disabled", gate["blockers"])


if __name__ == "__main__":
    unittest.main()
