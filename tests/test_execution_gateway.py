import unittest

from services.execution_gateway import build_execution_gateway


class ExecutionGatewayTest(unittest.TestCase):
    def test_strategy_conflict_requires_human_but_execution_can_be_available(self):
        gateway = build_execution_gateway({
            "evidence_summary": {
                "historical_evidence": "strong",
                "execution_permission": "human_required",
            },
            "strategy_use_summary": {"actionable_count": 1},
            "strategy_confidence": {
                "momentum_lite_v1": {
                    "suggested_use": "advisory",
                    "consensus_conflict": True,
                }
            },
            "execution_intel": {"status": "live_available"},
            "strategy_results": [{"strategy_name": "momentum_lite_v1", "turnover": 0.30}],
        })

        self.assertEqual(gateway["final_permission"], "tightened")
        self.assertEqual(gateway["source"], "strategy_layer")
        self.assertEqual(gateway["primary_reason"], "regime_consensus_mismatch")
        self.assertEqual(gateway["response_class"], "strategy_conflict")
        self.assertEqual(gateway["execution_intel_layer"]["verdict"], "acceptable")

    def test_high_turnover_requires_human_when_strategy_passes(self):
        gateway = build_execution_gateway({
            "evidence_summary": {
                "historical_evidence": "strong",
                "execution_permission": "allowed",
            },
            "strategy_use_summary": {"actionable_count": 1},
            "execution_intel": {"status": "live_available"},
            "strategy_results": [{"strategy_name": "momentum_lite_v1", "turnover": 0.74}],
        })

        self.assertEqual(gateway["final_permission"], "tightened")
        self.assertEqual(gateway["source"], "execution_intel_layer")
        self.assertEqual(gateway["primary_reason"], "high_turnover_cost")
        self.assertEqual(gateway["response_class"], "cost_turnover")

    def test_clean_strategy_and_execution_are_approved(self):
        gateway = build_execution_gateway({
            "evidence_summary": {
                "historical_evidence": "strong",
                "execution_permission": "allowed",
            },
            "strategy_use_summary": {"actionable_count": 1},
            "execution_intel": {"status": "live_available"},
            "strategy_results": [{"strategy_name": "momentum_lite_v1", "turnover": 0.30}],
        })

        self.assertEqual(gateway["final_permission"], "approved")
        self.assertEqual(gateway["source"], "gateway")

    def test_missing_qc_is_execution_intel_insufficient_not_conflicted(self):
        gateway = build_execution_gateway({
            "evidence_summary": {
                "historical_evidence": "strong",
                "execution_permission": "allowed",
                "execution_intel_status": "insufficient_data",
            },
            "strategy_use_summary": {"actionable_count": 1},
            "strategy_results": [{"strategy_name": "momentum_lite_v1", "turnover": 0.30}],
        })

        self.assertEqual(gateway["final_permission"], "tightened")
        self.assertEqual(gateway["primary_reason"], "execution_intel_insufficient_data")
        self.assertEqual(gateway["response_class"], "data_quality")
        self.assertEqual(gateway["execution_intel_layer"]["execution_intel_status"], "insufficient_data")


if __name__ == "__main__":
    unittest.main()
