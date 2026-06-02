import unittest
import inspect

import services.final_execution_policy_cap as final_cap_module
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
        self.assertEqual(out["mutation_ledger"]["mutation_types"], ["cash_raise_from_policy_cap"])
        self.assertEqual(out["mutation_ledger"]["affected_tickers"], ["XLE", "XLK"])
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
        self.assertEqual(out["mutation_ledger"]["total_mutations"], 0)
        self.assertEqual(out["target_weights"], {"XLK": 0.14, "XLE": 0.12, "CASH": 0.74})

    def test_policy_cap_does_not_renormalize_capped_weight_above_cap(self):
        out = apply_final_execution_policy_cap(
            target_weights={"XLK": 0.1672, "XLE": 0.12, "CASH": 0.60},
            current_weights={"XLK": 0.15, "XLE": 0.12, "CASH": 0.73},
            rebalance_threshold=0.005,
        )

        self.assertTrue(out["triggered"])
        self.assertTrue(out["policy_evaluation"]["allowed"])
        self.assertAlmostEqual(out["target_weights"]["XLK"], 0.15, places=6)
        self.assertAlmostEqual(out["target_weights"]["XLE"], 0.12, places=6)
        self.assertAlmostEqual(
            sum(out["target_weights"].values()),
            1.0,
            places=6,
        )
        self.assertGreater(out["target_weights"]["CASH"], 0.72)

    def test_group_cap_uses_weight_ops_and_releases_to_cash(self):
        out = apply_final_execution_policy_cap(
            target_weights={"XLK": 0.15, "XLE": 0.15, "XLI": 0.15, "XLU": 0.15, "CASH": 0.40},
            current_weights={"CASH": 1.0},
            rebalance_threshold=0.005,
        )

        self.assertTrue(out["triggered"])
        self.assertTrue(out["policy_evaluation"]["allowed"], out["policy_evaluation"])
        self.assertAlmostEqual(out["target_weights"]["XLK"], 0.1125, places=6)
        self.assertAlmostEqual(out["target_weights"]["XLE"], 0.1125, places=6)
        self.assertAlmostEqual(out["target_weights"]["XLI"], 0.1125, places=6)
        self.assertAlmostEqual(out["target_weights"]["XLU"], 0.1125, places=6)
        self.assertAlmostEqual(out["target_weights"]["CASH"], 0.55, places=6)
        self.assertEqual(out["cap_events"][0]["group_role"], "sector")
        self.assertAlmostEqual(out["cash_raised"], 0.15, places=6)
        self.assertEqual(out["cap_diagnostics"]["contract"], "weight_ops_cash_first_v1")
        self.assertEqual(
            out["mutation_ledger"]["affected_tickers"],
            ["XLE", "XLI", "XLK", "XLU"],
        )

    def test_final_cap_module_uses_weight_ops_not_private_normalizer(self):
        source = inspect.getsource(final_cap_module)

        self.assertIn("normalize_cash_first", source)
        self.assertIn("apply_single_caps_cash_first", source)
        self.assertIn("apply_group_caps_cash_first", source)
        self.assertNotIn("def _normalize_preserving_policy_caps", source)


if __name__ == "__main__":
    unittest.main()
