import unittest

from services.execution_throttle import apply_execution_throttle
from services.final_risk_validation import validate_final_execution_target


class ExecutionThrottleTests(unittest.TestCase):
    def test_stages_buy_delta_proportionally_and_preserves_sells(self):
        out = apply_execution_throttle(
            target_weights={"SPY": 0.20, "QQQ": 0.10, "XLK": 0.0, "CASH": 0.70},
            current_weights={"SPY": 0.10, "XLK": 0.05, "CASH": 0.85},
            config={"max_buy_delta": 0.05},
        )

        self.assertTrue(out["applied"])
        self.assertEqual(out["mutation_types"], ["execution_buy_delta_throttle"])
        self.assertEqual(out["metrics_before"]["buy_delta"], 0.20)
        self.assertEqual(out["metrics_after"]["buy_delta"], 0.05)
        self.assertEqual(out["metrics_after"]["sell_delta"], 0.05)
        self.assertAlmostEqual(out["staged_target_weights"]["SPY"], 0.125)
        self.assertAlmostEqual(out["staged_target_weights"]["QQQ"], 0.025)
        self.assertNotIn("XLK", out["staged_target_weights"])
        self.assertAlmostEqual(out["staged_target_weights"]["CASH"], 0.85)
        self.assertAlmostEqual(out["deferred_delta"]["SPY"], 0.075)
        self.assertAlmostEqual(out["deferred_delta"]["QQQ"], 0.075)
        self.assertEqual(out["deferred_buy_delta"], 0.15)
        self.assertEqual(out["mutation_ledger"]["mutation_types"], ["execution_buy_delta_throttle"])
        self.assertEqual(out["mutation_ledger"]["affected_tickers"], ["QQQ", "SPY"])

    def test_noop_when_within_buy_limit(self):
        out = apply_execution_throttle(
            target_weights={"SPY": 0.12, "CASH": 0.88},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            config={"max_buy_delta": 0.05},
        )

        self.assertFalse(out["applied"])
        self.assertEqual(out["reason"], "within_limits")
        self.assertEqual(out["desired_target_weights"], out["staged_target_weights"])
        self.assertEqual(out["metrics_after"]["buy_delta"], 0.02)
        self.assertEqual(out["mutation_ledger"]["total_mutations"], 0)

    def test_disabled_preserves_desired_target(self):
        out = apply_execution_throttle(
            target_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            config={"enabled": False, "max_buy_delta": 0.05},
        )

        self.assertFalse(out["applied"])
        self.assertEqual(out["reason"], "disabled")
        self.assertEqual(out["metrics_after"]["buy_delta"], 0.10)

    def test_zero_buy_limit_defers_all_new_buys(self):
        out = apply_execution_throttle(
            target_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"CASH": 1.0},
            config={"max_buy_delta": 0.0},
        )

        self.assertTrue(out["applied"])
        self.assertNotIn("SPY", out["staged_target_weights"])
        self.assertEqual(out["staged_target_weights"]["CASH"], 1.0)
        self.assertEqual(out["metrics_after"]["buy_delta"], 0.0)
        self.assertEqual(out["deferred_delta"]["SPY"], 0.2)

    def test_final_validation_allows_execution_buy_delta_throttle(self):
        throttle = apply_execution_throttle(
            target_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            config={"max_buy_delta": 0.05},
        )

        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.20, "CASH": 0.80},
            final_target={"SPY": 0.15, "CASH": 0.85},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            policy_context={
                "post_risk_mutation_types": ["execution_buy_delta_throttle"],
                "post_risk_mutation_ledgers": [throttle["mutation_ledger"]],
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertEqual(out["unknown_mutation_types"], [])
        self.assertEqual(out["missing_mutation_ledger_tickers"], [])


if __name__ == "__main__":
    unittest.main()
