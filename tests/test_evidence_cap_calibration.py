import unittest

from services.evidence_cap_calibration import build_evidence_cap_calibration_report


class EvidenceCapCalibrationTest(unittest.TestCase):
    def test_recommends_gated_when_observe_and_execution_feedback_pass(self):
        cycles = []
        for idx in range(12):
            cycles.append({
                "analysis_id": idx,
                "evidence_cap_diagnostics": {
                    "DRAM": {
                        "static_cap": 0.05,
                        "evidence_adjusted_cap": 0.021,
                        "would_clip": idx < 2,
                        "evidence_quality_multiplier": 0.42,
                        "history_days": 55,
                        "mapping_error_count": 0,
                    },
                    "SPY": {
                        "static_cap": 0.20,
                        "evidence_adjusted_cap": 0.18,
                        "would_clip": False,
                        "evidence_quality_multiplier": 0.90,
                        "history_days": 1000,
                        "mapping_error_count": 0,
                    },
                },
            })
        profiles = [
            {"source_bucket": "combined", "status": "calibrated", "n": 320},
            {"source_bucket": "combined", "status": "indicative", "n": 180},
            {"source_bucket": "live_paper", "status": "early_signal", "n": 45},
        ]

        report = build_evidence_cap_calibration_report(
            cap_cycles=cycles,
            conviction_profiles=profiles,
            command_events=[
                {"event_type": "submitted", "event_status": "submitted"},
                {"event_type": "qc_accepted", "event_status": "accepted"},
            ],
            current_config={"mode": "observe", "min_observe_cycles": 10, "max_would_clip_rate": 0.30},
        )

        self.assertEqual(report["contract_version"], "evidence_cap_calibration_v1")
        self.assertTrue(report["recommendation_only"])
        self.assertEqual(report["execution_authority"], "none")
        self.assertEqual(report["target_weight_mutation"], "none")
        self.assertTrue(report["gated_readiness"]["criteria_met"])
        self.assertEqual(report["recommended_config"]["mode"], "gated")
        self.assertEqual(
            report["recommended_vote_thresholds"]["increase"]["or_single_conviction_status"],
            ["calibrated", "statistically_meaningful"],
        )
        self.assertIn("risk_off", report["recommended_vote_thresholds"]["hedge"]["requires_regime"])
        self.assertEqual(report["operator_action"], "operator_review_then_optionally_enable_gated")
        self.assertEqual(report["observe_summary"]["observe_cycles"], 12)
        self.assertAlmostEqual(report["observe_summary"]["would_clip_rate"], 2 / 24, places=6)
        self.assertEqual(report["young_etf_summary"]["cap_range_status"], "within_expected_range")
        self.assertTrue(report["recommended_config"]["young_etf_cap_within_expected_range"])

    def test_keeps_observe_when_data_is_insufficient_or_rejections_are_high(self):
        report = build_evidence_cap_calibration_report(
            cap_cycles=[
                {
                    "analysis_id": 1,
                    "evidence_cap_diagnostics": {
                        "DRAM": {
                            "static_cap": 0.05,
                            "evidence_adjusted_cap": 0.004,
                            "would_clip": True,
                            "evidence_quality_multiplier": 0.08,
                            "history_days": 40,
                        }
                    },
                }
            ],
            conviction_profiles=[
                {"source_bucket": "combined", "status": "insufficient_samples", "n": 8},
            ],
            command_events=[
                {"event_type": "submitted", "event_status": "submitted"},
                {"event_type": "qc_rejected", "event_status": "rejected", "reason": "policy_mismatch"},
            ],
            current_config={"mode": "gated", "min_multiplier": 0.10},
        )

        self.assertFalse(report["gated_readiness"]["criteria_met"])
        self.assertEqual(report["recommended_config"]["mode"], "observe")
        self.assertIn("insufficient_observe_cycles", report["gated_readiness"]["gate_blockers"])
        self.assertIn("recent_command_rejection_rate_high", report["gated_readiness"]["gate_blockers"])
        self.assertEqual(report["young_etf_summary"]["cap_range_status"], "below_expected_range")
        self.assertFalse(report["recommended_config"]["young_etf_cap_within_expected_range"])
        self.assertEqual(report["recommended_config"]["min_multiplier"], 0.15)
        self.assertIn("conviction_profiles_not_yet_meaningful", report["warnings"])

    def test_extracts_nested_evidence_bundle_diagnostics(self):
        report = build_evidence_cap_calibration_report(
            cap_cycles=[
                {
                    "analysis_id": 2,
                    "output_data": {
                        "evidence_bundle": {
                            "strategies": {
                                "evidence_cap_diagnostics": {
                                    "QQQ": {
                                        "static_cap": 0.20,
                                        "evidence_adjusted_cap": 0.18,
                                        "would_clip": False,
                                    }
                                }
                            }
                        }
                    },
                }
            ],
            conviction_profiles=[],
            command_events=[],
        )

        self.assertEqual(report["observe_summary"]["observe_cycles"], 1)
        self.assertEqual(report["observe_summary"]["cap_row_count"], 1)


if __name__ == "__main__":
    unittest.main()
