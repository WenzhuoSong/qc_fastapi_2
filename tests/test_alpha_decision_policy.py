import unittest
from datetime import datetime, timezone

from services.alpha_decision_policy import (
    ALPHA_DECISION_POLICY_CONTRACT_VERSION,
    default_alpha_decision_policy_config,
    evaluate_alpha_decision_policy,
)


class AlphaDecisionPolicyTests(unittest.TestCase):
    def test_default_policy_is_observe_and_json_ready(self):
        cfg = default_alpha_decision_policy_config()
        decision = evaluate_alpha_decision_policy(cfg)

        self.assertEqual(cfg["mode"], "observe")
        self.assertEqual(decision["contract_version"], ALPHA_DECISION_POLICY_CONTRACT_VERSION)
        self.assertEqual(decision["effective_mode"], "observe")
        self.assertFalse(decision["recommendation_effect"])
        self.assertFalse(decision["allocation_effect"])
        self.assertEqual(decision["execution_authority"], "none")
        self.assertEqual(decision["target_weight_mutation"], "none")
        self.assertTrue(decision["never_bypasses_target_builder"])

    def test_invalid_mode_normalizes_to_observe(self):
        cfg = default_alpha_decision_policy_config({"mode": "execute"})

        self.assertEqual(cfg["mode"], "observe")
        self.assertFalse(evaluate_alpha_decision_policy(cfg)["allocation_effect"])

    def test_recommendation_mode_does_not_affect_allocation(self):
        decision = evaluate_alpha_decision_policy({"mode": "recommendation"})

        self.assertEqual(decision["effective_mode"], "recommendation")
        self.assertTrue(decision["recommendation_effect"])
        self.assertFalse(decision["allocation_effect"])

    def test_gated_mode_blocks_without_review_criteria(self):
        decision = evaluate_alpha_decision_policy({"mode": "gated"})

        self.assertEqual(decision["mode"], "gated")
        self.assertEqual(decision["effective_mode"], "recommendation")
        self.assertFalse(decision["gated_enabled"])
        self.assertIn("insufficient_observe_cycles", decision["blockers"])
        self.assertIn("operator_gated_approval_missing", decision["blockers"])
        self.assertIn("raw_adjusted_allocation_diagnostics_not_reviewed", decision["blockers"])
        self.assertIn("gated_dry_run_report_not_reviewed", decision["blockers"])

    def test_gated_mode_passes_with_explicit_review_criteria(self):
        decision = evaluate_alpha_decision_policy(
            {
                "mode": "gated",
                "observe_cycles": 25,
                "operator_gated_approved": True,
                "raw_adjusted_diagnostics_reviewed": True,
                "dry_run_report_reviewed": True,
                "evidence_cap_calibration_fresh": True,
                "dashboard_naked_conviction_blocked": True,
            }
        )

        self.assertEqual(decision["effective_mode"], "gated")
        self.assertTrue(decision["gated_enabled"])
        self.assertTrue(decision["recommendation_effect"])
        self.assertTrue(decision["allocation_effect"])
        self.assertEqual(decision["blockers"], [])

    def test_evidence_cap_report_freshness_can_satisfy_gated_criteria(self):
        now = datetime(2026, 5, 28, tzinfo=timezone.utc)
        decision = evaluate_alpha_decision_policy(
            {
                "mode": "gated",
                "observe_cycles": 25,
                "operator_gated_approved": True,
                "raw_adjusted_diagnostics_reviewed": True,
                "dry_run_report_reviewed": True,
            },
            evidence_cap_calibration={
                "recommended_config": {
                    "calibration_generated_at": "2026-05-27T00:00:00+00:00",
                    "max_calibration_age_days": 7,
                }
            },
            generated_at=now,
        )

        self.assertTrue(decision["criteria"]["evidence_cap_calibration_fresh"])
        self.assertTrue(decision["allocation_effect"])


if __name__ == "__main__":
    unittest.main()
