import unittest

from services.evidence_quality_cap import (
    evaluate_evidence_quality_caps,
    evidence_quality_multiplier,
    get_conviction_discount,
)


class EvidenceQualityCapTests(unittest.TestCase):
    def test_multiplier_uses_weighted_average_and_floor(self):
        self.assertAlmostEqual(
            evidence_quality_multiplier(
                coverage_ratio=0.50,
                conviction_discount=0.45,
                history_discount=55 / 252,
            ),
            0.423651,
        )
        self.assertEqual(
            evidence_quality_multiplier(
                coverage_ratio=0.0,
                conviction_discount=0.0,
                history_discount=0.0,
            ),
            0.10,
        )

    def test_unknown_conviction_status_uses_default_discount(self):
        self.assertEqual(get_conviction_discount("future_status"), 0.0)
        self.assertEqual(get_conviction_discount(None), 0.0)

    def test_conviction_discount_matches_plan_defaults(self):
        self.assertEqual(get_conviction_discount("statistically_meaningful"), 1.0)
        self.assertEqual(get_conviction_discount("calibrated"), 0.0)
        self.assertEqual(get_conviction_discount("indicative"), 0.35)
        self.assertEqual(get_conviction_discount("early_signal"), 0.10)

    def test_builds_observe_only_diagnostic_and_would_clip(self):
        out = evaluate_evidence_quality_caps(
            vote_summary={
                "DRAM": {
                    "coverage_ratio": 0.5,
                    "voted_count": 1,
                    "eligible_strategy_count": 2,
                    "abstain_count": 1,
                    "mapping_error_count": 0,
                },
            },
            evidence_cards=[
                {
                    "ticker": "DRAM",
                    "strategy": "mean_reversion_lite",
                    "vote_status": "voted",
                    "action": "increase",
                    "conviction_status": "early_signal",
                    "conviction_statistical_status": "early_signal",
                    "max_reasonable_weight": 0.04,
                    "diagnostics": {"base_cap": 0.08},
                    "vote_diagnostics": {"history_days": 55},
                }
            ],
            current_or_target_weights={"DRAM": 0.03},
        )

        dram = out["DRAM"]
        self.assertEqual(dram["execution_effect"], "diagnostic_only")
        self.assertEqual(dram["static_cap"], 0.05)
        self.assertEqual(dram["conviction_status"], "early_signal")
        self.assertEqual(dram["operational_conviction_status"], "early_signal")
        self.assertAlmostEqual(dram["evidence_quality_multiplier"], 0.283651)
        self.assertAlmostEqual(dram["evidence_adjusted_cap"], 0.014183)
        self.assertTrue(dram["would_clip"])
        self.assertEqual(dram["would_clip_to"], dram["evidence_adjusted_cap"])

    def test_missing_history_days_defaults_to_no_history_discount(self):
        out = evaluate_evidence_quality_caps(
            vote_summary={
                "SPY": {
                    "coverage_ratio": 1.0,
                    "voted_count": 1,
                    "eligible_strategy_count": 1,
                },
            },
            evidence_cards=[
                {
                    "ticker": "SPY",
                    "strategy": "momentum_lite_v1",
                    "vote_status": "voted",
                    "conviction_status": "calibrated",
                    "conviction_statistical_status": "early_signal",
                    "diagnostics": {"base_cap": 0.30},
                }
            ],
            current_or_target_weights={"SPY": 0.20},
        )

        spy = out["SPY"]
        self.assertIsNone(spy["history_days"])
        self.assertEqual(spy["history_discount"], 1.0)
        self.assertEqual(spy["static_cap"], 0.25)
        self.assertTrue(spy["would_clip"])
        self.assertEqual(spy["operational_conviction_status"], "calibrated")
        self.assertEqual(spy["conviction_status"], "early_signal")

    def test_zero_weight_mapping_error_card_does_not_erase_static_role_cap(self):
        out = evaluate_evidence_quality_caps(
            vote_summary={
                "DRAM": {
                    "coverage_ratio": 0.0,
                    "mapping_error_count": 1,
                },
            },
            evidence_cards=[
                {
                    "ticker": "DRAM",
                    "strategy": "unknown_strategy",
                    "vote_status": "mapping_error",
                    "conviction_status": "missing_profile",
                    "max_reasonable_weight": 0.0,
                }
            ],
            current_or_target_weights={"DRAM": 0.01},
        )

        self.assertEqual(out["DRAM"]["static_cap"], 0.05)
        self.assertEqual(out["DRAM"]["evidence_adjusted_cap"], 0.01)

    def test_calibrated_with_large_sample_maps_to_statistically_meaningful(self):
        out = evaluate_evidence_quality_caps(
            vote_summary={
                "SPY": {
                    "coverage_ratio": 1.0,
                    "voted_count": 1,
                    "eligible_strategy_count": 1,
                },
            },
            evidence_cards=[
                {
                    "ticker": "SPY",
                    "strategy": "momentum_lite_v1",
                    "vote_status": "voted",
                    "conviction_status": "calibrated",
                    "conviction_n": 320,
                    "diagnostics": {"base_cap": 0.30},
                }
            ],
            current_or_target_weights={"SPY": 0.20},
        )

        spy = out["SPY"]
        self.assertEqual(spy["operational_conviction_status"], "calibrated")
        self.assertEqual(spy["conviction_status"], "statistically_meaningful")
        self.assertEqual(spy["conviction_discount"], 1.0)


if __name__ == "__main__":
    unittest.main()
