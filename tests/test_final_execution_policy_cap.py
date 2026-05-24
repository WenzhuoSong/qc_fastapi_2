import unittest

from services.final_execution_policy_cap import apply_final_execution_policy_cap


class FinalExecutionPolicyCapTest(unittest.TestCase):
    def test_caps_post_governance_weights_and_recomputes_actions(self):
        out = apply_final_execution_policy_cap(
            target_weights={"XLK": 0.1722, "XLE": 0.1566, "CASH": 0.6712},
            current_weights={"XLK": 0.10, "XLE": 0.10, "CASH": 0.80},
            rebalance_threshold=0.005,
        )

        self.assertTrue(out["triggered"])
        self.assertEqual(out["policy_version"], "sprint8a")
        self.assertEqual(out["mutation_types"], ["cash_raise_from_policy_cap"])
        self.assertTrue(out["policy_evaluation"]["allowed"])
        self.assertAlmostEqual(out["target_weights"]["XLK"], 0.15, places=4)
        self.assertAlmostEqual(out["target_weights"]["XLE"], 0.15, places=4)
        self.assertGreaterEqual(out["target_weights"]["CASH"], 0.70)
        self.assertEqual([event["ticker"] for event in out["cap_events"]], ["XLK", "XLE"])
        self.assertTrue(any(action["ticker"] == "XLK" for action in out["rebalance_actions"]))

    def test_noop_when_weights_already_compliant(self):
        out = apply_final_execution_policy_cap(
            target_weights={"XLK": 0.14, "XLE": 0.12, "CASH": 0.74},
            current_weights={"XLK": 0.14, "XLE": 0.12, "CASH": 0.74},
            rebalance_threshold=0.005,
        )

        self.assertFalse(out["triggered"])
        self.assertEqual(out["cap_events"], [])
        self.assertEqual(out["mutation_types"], [])
        self.assertEqual(out["target_weights"], {"XLK": 0.14, "XLE": 0.12, "CASH": 0.74})


if __name__ == "__main__":
    unittest.main()
