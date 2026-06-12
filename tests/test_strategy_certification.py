import unittest

from services.strategy_certification import certify_strategies


class StrategyCertificationTest(unittest.TestCase):
    def test_certifies_advisory_when_historical_and_live_are_ready(self):
        out = certify_strategies({
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "aligned",
            },
            "strategy_results": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 30,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "historical_hit_rate": 0.55,
                    "suggested_use": "advisory",
                    "confidence_score": 0.7,
                }
            ],
        })

        row = out["items"]["momentum_lite_v1"]
        self.assertEqual(row["status"], "advisory")
        self.assertEqual(row["approved_use"], "advisory")
        self.assertEqual(out["summary"]["actionable_count"], 1)
        audit_row = out["audit"]["rows"][0]
        self.assertEqual(audit_row["strategy_name"], "momentum_lite_v1")
        self.assertTrue(audit_row["promotion_eligible"])
        self.assertEqual(out["audit"]["summary"]["promotion_candidates"], ["momentum_lite_v1"])
        self.assertFalse(out["audit"]["summary"]["requires_operator_review"])

    def test_research_supported_when_turnover_high(self):
        out = certify_strategies({
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "insufficient",
            },
            "strategy_results": [
                {
                    "strategy_name": "mean_reversion_lite",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.75,
                    "n_forward_return_samples": 3,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.1,
                    "suggested_use": "advisory",
                    "confidence_score": 0.55,
                }
            ],
        })

        row = out["items"]["mean_reversion_lite"]
        self.assertEqual(row["status"], "research_supported")
        self.assertEqual(row["approved_use"], "research_only")
        self.assertIn("live_samples_insufficient", row["promotion_blockers"])
        self.assertIn("turnover_high", row["demotion_reasons"])
        audit_row = out["audit"]["rows"][0]
        self.assertFalse(audit_row["promotion_eligible"])
        self.assertIn("suggested_use_not_certified_for_execution", audit_row["risk_flags"])
        self.assertEqual(out["audit"]["summary"]["suggested_advisory_not_certified"], ["mean_reversion_lite"])
        self.assertTrue(out["audit"]["summary"]["requires_operator_review"])

    def test_live_samples_required_before_execution_grade(self):
        out = certify_strategies({
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "aligned",
            },
            "strategy_results": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 0,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "historical_hit_rate": 0.55,
                    "suggested_use": "advisory",
                    "confidence_score": 0.7,
                }
            ],
        })

        row = out["items"]["momentum_lite_v1"]
        self.assertEqual(row["status"], "research_supported")
        self.assertEqual(row["approved_use"], "research_only")
        self.assertEqual(row["execution_evidence_status"], "insufficient_execution_evidence")
        self.assertIn("live_samples_insufficient", row["promotion_blockers"])
        self.assertFalse(row["evidence_checks"]["checks"]["live_samples_min"]["pass"])

    def test_primary_can_be_execution_grade_when_evidence_passes(self):
        out = certify_strategies({
            "data_quality": "fresh",
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "aligned",
            },
            "strategy_results": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 8,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "historical_hit_rate": 0.55,
                    "suggested_use": "primary",
                    "confidence_score": 0.75,
                }
            ],
        })

        row = out["items"]["momentum_lite_v1"]
        self.assertEqual(row["status"], "advisory")
        self.assertEqual(row["approved_use"], "advisory")
        self.assertEqual(row["execution_evidence_status"], "execution_grade_validated")
        self.assertEqual(row["evidence_checks"]["status"], "pass")

    def test_execution_evidence_kill_switch_forces_insufficient(self):
        out = certify_strategies({
            "data_quality": "fresh",
            "strategy_execution_evidence_config": {
                "enabled": True,
                "force_advisory_only": True,
                "min_live_samples_for_execution": 5,
            },
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "aligned",
            },
            "strategy_results": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 8,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "historical_hit_rate": 0.55,
                    "suggested_use": "advisory",
                    "confidence_score": 0.75,
                }
            ],
        })

        row = out["items"]["momentum_lite_v1"]
        self.assertEqual(row["status"], "research_supported")
        self.assertEqual(row["approved_use"], "research_only")
        self.assertEqual(row["execution_evidence_status"], "insufficient_execution_evidence")
        self.assertIn("strategy_execution_evidence_disabled", row["promotion_blockers"])
        self.assertFalse(row["evidence_checks"]["checks"]["execution_evidence_enabled"]["pass"])

    def test_execution_evidence_kill_switch_round_trip_has_no_residual_state(self):
        base = {
            "data_quality": "fresh",
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "aligned",
            },
            "strategy_results": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 8,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "historical_hit_rate": 0.55,
                    "suggested_use": "advisory",
                    "confidence_score": 0.75,
                }
            ],
        }

        normal = certify_strategies({
            **base,
            "strategy_execution_evidence_config": {"force_advisory_only": False},
        })["items"]["momentum_lite_v1"]
        killed = certify_strategies({
            **base,
            "strategy_execution_evidence_config": {"force_advisory_only": True},
        })["items"]["momentum_lite_v1"]
        restored = certify_strategies({
            **base,
            "strategy_execution_evidence_config": {"force_advisory_only": False},
        })["items"]["momentum_lite_v1"]

        self.assertEqual(normal["execution_evidence_status"], "execution_grade_validated")
        self.assertEqual(killed["execution_evidence_status"], "insufficient_execution_evidence")
        self.assertIn("strategy_execution_evidence_disabled", killed["promotion_blockers"])
        self.assertEqual(restored["execution_evidence_status"], "execution_grade_validated")
        self.assertEqual(restored["evidence_checks"]["status"], "pass")

    def test_degraded_strategy_data_quality_fails_closed(self):
        out = certify_strategies({
            "data_quality": "degraded",
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "aligned",
            },
            "strategy_results": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 8,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "historical_hit_rate": 0.55,
                    "suggested_use": "advisory",
                    "confidence_score": 0.75,
                }
            ],
        })

        row = out["items"]["momentum_lite_v1"]
        self.assertEqual(row["execution_evidence_status"], "insufficient_execution_evidence")
        self.assertIn("strategy_data_quality_degraded", row["promotion_blockers"])
        self.assertFalse(row["evidence_checks"]["checks"]["strategy_data_quality_not_degraded"]["pass"])

    def test_bundle_level_conflict_does_not_leak_to_aligned_strategy(self):
        out = certify_strategies({
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "conflicted",
            },
            "strategy_results": [
                {
                    "strategy_name": "momentum_lite_v1",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 30,
                    "metric_reliability": {"level": "high"},
                    "historical_metric_reliability": {"level": "high"},
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "suggested_use": "advisory",
                    "confidence_score": 0.70,
                    "reason_codes": ["historical_strong", "live_qc_supported"],
                },
                {
                    "strategy_name": "mean_reversion_lite",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 30,
                    "metric_reliability": {"level": "high"},
                    "historical_metric_reliability": {"level": "high"},
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.0,
                    "suggested_use": "advisory",
                    "confidence_score": 0.60,
                    "reason_codes": ["historical_strong", "consensus_regime_conflict"],
                },
            ],
        })

        aligned = out["items"]["momentum_lite_v1"]
        conflicted = out["items"]["mean_reversion_lite"]
        self.assertEqual(aligned["live"]["fit"], "aligned")
        self.assertEqual(aligned["status"], "advisory")
        self.assertNotIn("live_fit_conflicted", aligned["demotion_reasons"])
        self.assertEqual(conflicted["live"]["fit"], "conflicted")
        self.assertIn("live_fit_conflicted", conflicted["demotion_reasons"])

    def test_disables_malformed_or_non_influential_strategy(self):
        out = certify_strategies({
            "strategy_results": [
                {
                    "strategy_name": "broken_strategy",
                    "data_ready": False,
                    "can_influence_allocation": False,
                    "suggested_use": "ignore",
                }
            ],
        })

        row = out["items"]["broken_strategy"]
        self.assertEqual(row["status"], "disabled")
        self.assertEqual(row["approved_use"], "none")
        self.assertEqual(out["audit"]["summary"]["disabled_or_experimental"], ["broken_strategy"])
        self.assertEqual(out["audit"]["execution_authority"], "none")

    def test_walk_forward_weak_blocks_advisory_certification(self):
        out = certify_strategies({
            "evidence_summary": {
                "historical_evidence": "strong",
                "live_fit": "aligned",
            },
            "strategy_results": [
                {
                    "strategy_name": "unstable_strategy",
                    "data_ready": True,
                    "can_influence_allocation": True,
                    "turnover": 0.20,
                    "n_forward_return_samples": 30,
                    "historical_forward_return_samples": 289,
                    "historical_sharpe": 1.2,
                    "historical_hit_rate": 0.55,
                    "walk_forward_level": "weak",
                    "walk_forward_valid_folds": 4,
                    "walk_forward_pass_rate": 0.25,
                    "walk_forward_stability_score": 0.35,
                    "suggested_use": "advisory",
                    "confidence_score": 0.7,
                }
            ],
        })

        row = out["items"]["unstable_strategy"]
        self.assertEqual(row["status"], "research_supported")
        self.assertEqual(row["approved_use"], "research_only")
        self.assertIn("walk_forward_weak", row["demotion_reasons"])
        audit_row = out["audit"]["rows"][0]
        self.assertEqual(audit_row["walk_forward_level"], "weak")
        self.assertIn("suggested_use_not_certified_for_execution", audit_row["risk_flags"])


if __name__ == "__main__":
    unittest.main()
