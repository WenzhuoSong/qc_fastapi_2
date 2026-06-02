import unittest

from services.portfolio_construction_gate import construction_input_for_target_builder


class PipelinePortfolioConstructionGateTests(unittest.TestCase):
    def test_candidate_mode_does_not_feed_target_builder(self):
        out = construction_input_for_target_builder(
            portfolio_construction_payload={"target_weights": {"SPY": 0.20, "CASH": 0.80}},
            promotion_gate={"status": "auto_approved", "eligible": True},
            config={"portfolio_construction_mode": "candidate", "enabled": True},
        )

        self.assertFalse(out["construction_participated"])
        self.assertIsNone(out["construction_weights"])
        self.assertEqual(out["blocked_reason"], "mode_candidate_not_gated")
        self.assertEqual(out["configured_mode"], "candidate")
        self.assertEqual(out["effective_mode"], "deterministic_target_builder")
        self.assertEqual(out["construction_weight_source"], "pc_shadow_weights")
        self.assertIsNone(out["target_builder_input_key"])
        self.assertEqual(out["weight_source_contract"]["contract_version"], "weight_source_contract_v1")

    def test_gated_mode_requires_eligible_promotion_gate(self):
        out = construction_input_for_target_builder(
            portfolio_construction_payload={"target_weights": {"SPY": 0.20, "CASH": 0.80}},
            promotion_gate={"status": "blocked", "eligible": False, "blockers": ["insufficient_cycles"]},
            config={"portfolio_construction_mode": "gated", "enabled": True},
        )

        self.assertFalse(out["construction_participated"])
        self.assertIsNone(out["construction_weights"])
        self.assertEqual(out["blocked_reason"], "promotion_gate_not_eligible")
        self.assertEqual(out["configured_mode"], "gated")
        self.assertEqual(out["effective_mode"], "deterministic_target_builder")
        self.assertEqual(out["gate_status"], "blocked")
        self.assertFalse(out["gate_eligible"])
        self.assertEqual(out["gate_blockers"], ["insufficient_cycles"])

    def test_full_auto_rollout_block_prevents_construction_input(self):
        out = construction_input_for_target_builder(
            portfolio_construction_payload={"target_weights": {"SPY": 0.20, "CASH": 0.80}},
            promotion_gate={
                "status": "rollout_blocked",
                "eligible": False,
                "blockers": ["semi_auto_gated_confirmations_insufficient"],
            },
            config={"portfolio_construction_mode": "gated", "enabled": True},
        )

        self.assertFalse(out["construction_participated"])
        self.assertIsNone(out["construction_weights"])
        self.assertEqual(out["blocked_reason"], "promotion_gate_not_eligible")

    def test_gated_and_eligible_passes_construction_weights(self):
        out = construction_input_for_target_builder(
            portfolio_construction_payload={
                "target_weights": {"SPY": 0.20, "CASH": 0.80},
                "construction_source": "portfolio_construction",
            },
            promotion_gate={"status": "auto_approved", "eligible": True},
            config={"portfolio_construction_mode": "gated", "enabled": True},
        )

        self.assertTrue(out["construction_participated"])
        self.assertEqual(out["construction_weights"], {"SPY": 0.20, "CASH": 0.80})
        self.assertEqual(out["construction_source"], "portfolio_construction")
        self.assertEqual(out["execution_effect"], "target_builder_input")
        self.assertEqual(out["configured_mode"], "gated")
        self.assertEqual(out["effective_mode"], "portfolio_construction_gated")
        self.assertEqual(out["construction_weight_source"], "pc_candidate_weights")
        self.assertEqual(out["target_builder_input_key"], "pc_candidate_weights")
        self.assertEqual(out["weight_source_contract"]["contract_version"], "weight_source_contract_v1")


if __name__ == "__main__":
    unittest.main()
