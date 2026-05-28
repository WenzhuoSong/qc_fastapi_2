import unittest

from services.evidence_vote_aggregation import aggregate_etf_evidence


class EvidenceVoteAggregationTests(unittest.TestCase):
    def test_abstain_is_never_score_zero(self):
        out = aggregate_etf_evidence(
            evidence_cards=[],
            input_builder_exclusions={
                "momentum_lite_v1": {
                    "DRAM": [
                        {"type": "insufficient_history", "field": "mom_252d"},
                    ],
                },
            },
        )

        dram = out["DRAM"]
        self.assertEqual(dram["voted_count"], 0)
        self.assertEqual(dram["abstain_count"], 1)
        self.assertIsNone(dram["actionable_score"])
        self.assertEqual(dram["coverage_ratio"], 0.0)

    def test_strategy_universe_mismatch_excluded_from_denominator(self):
        out = aggregate_etf_evidence(
            evidence_cards=[
                {
                    "ticker": "DRAM",
                    "strategy": "mean_reversion_lite",
                    "vote_status": "voted",
                    "action": "increase",
                    "normalized_score": 0.72,
                    "confidence": 0.72,
                }
            ],
            input_builder_exclusions={
                "seasonality_month_end_lite": {
                    "DRAM": [
                        {"type": "strategy_universe_mismatch", "field": "ticker"},
                    ],
                },
            },
        )

        dram = out["DRAM"]
        self.assertEqual(dram["eligible_strategy_count"], 1)
        self.assertEqual(dram["voted_count"], 1)
        self.assertEqual(dram["coverage_ratio"], 1.0)
        self.assertEqual(dram["abstain_count"], 1)
        self.assertFalse(dram["abstain_reasons"][0]["counts_in_denominator"])

    def test_missing_history_abstain_counts_in_denominator(self):
        out = aggregate_etf_evidence(
            evidence_cards=[
                {
                    "ticker": "DRAM",
                    "strategy": "mean_reversion_lite",
                    "vote_status": "voted",
                    "action": "increase",
                    "normalized_score": 0.72,
                    "confidence": 0.72,
                }
            ],
            input_builder_exclusions={
                "momentum_lite_v1": {
                    "DRAM": [
                        {"type": "insufficient_history", "field": "mom_60d"},
                        {"type": "insufficient_history", "field": "mom_252d"},
                    ],
                },
            },
        )

        dram = out["DRAM"]
        self.assertEqual(dram["eligible_strategy_count"], 2)
        self.assertEqual(dram["voted_count"], 1)
        self.assertEqual(dram["coverage_ratio"], 0.5)
        self.assertEqual(dram["abstain_count"], 1)
        self.assertTrue(dram["abstain_reasons"][0]["counts_in_denominator"])
        self.assertEqual(dram["abstain_reasons"][0]["fields"], ["mom_252d", "mom_60d"])

    def test_mapping_error_count_separate_from_watch_count(self):
        out = aggregate_etf_evidence(
            evidence_cards=[
                {
                    "ticker": "SPY",
                    "strategy": "momentum_lite_v1",
                    "vote_status": "mapping_error",
                    "action": "watch",
                    "normalized_score": 0.80,
                    "confidence": 0.80,
                },
                {
                    "ticker": "SPY",
                    "strategy": "leveraged_etf_momentum_allocator",
                    "vote_status": "watch",
                    "action": "neutral",
                    "normalized_score": 0.20,
                    "confidence": 0.20,
                },
            ],
            input_builder_exclusions={},
        )

        spy = out["SPY"]
        self.assertEqual(spy["mapping_error_count"], 1)
        self.assertEqual(spy["watch_count"], 1)
        self.assertEqual(spy["voted_count"], 0)
        self.assertIsNone(spy["actionable_score"])


if __name__ == "__main__":
    unittest.main()
