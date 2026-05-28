import unittest

from services.deferred_execution_ledger import (
    build_deferred_execution_items,
    review_deferred_execution_items,
    summarize_deferred_execution_pressure,
)


class DeferredExecutionLedgerTests(unittest.TestCase):
    def test_builds_deferred_items_from_execution_throttle(self):
        items = build_deferred_execution_items(
            analysis_id=204,
            command_id="analysis_204",
            throttle={
                "contract_version": "v1",
                "reason": "buy_delta_exceeds_limit",
                "desired_target_weights": {"SPY": 0.20, "QQQ": 0.20, "CASH": 0.60},
                "staged_target_weights": {"SPY": 0.125, "QQQ": 0.125, "CASH": 0.75},
                "current_weights": {"SPY": 0.05, "QQQ": 0.05, "CASH": 0.90},
                "deferred_delta": {"SPY": 0.075, "QQQ": 0.075},
                "metrics_before": {"buy_delta": 0.30},
                "metrics_after": {"buy_delta": 0.15},
                "limits": {"max_buy_delta": 0.15},
                "buy_scale": 0.5,
            },
        )

        self.assertEqual(len(items), 2)
        spy = next(item for item in items if item["ticker"] == "SPY")
        self.assertEqual(spy["status"], "open")
        self.assertEqual(spy["side"], "buy")
        self.assertEqual(spy["original_delta"], 0.075)
        self.assertEqual(spy["remaining_delta"], 0.075)
        self.assertEqual(spy["current_weight"], 0.05)
        self.assertEqual(spy["desired_weight"], 0.20)
        self.assertEqual(spy["staged_weight"], 0.125)
        self.assertIn("analysis_204_SPY_buy_", spy["deferred_id"])

    def test_reviews_prior_deferred_item_as_still_valid_when_in_current_command(self):
        reviews = review_deferred_execution_items(
            open_items=[
                {
                    "deferred_id": "d1",
                    "ticker": "SPY",
                    "side": "buy",
                    "remaining_delta": 0.075,
                    "desired_weight": 0.20,
                }
            ],
            current_weights={"SPY": 0.125, "CASH": 0.875},
            desired_target_weights={"SPY": 0.20, "CASH": 0.80},
            staged_target_weights={"SPY": 0.175, "CASH": 0.825},
        )

        self.assertEqual(reviews[0]["status"], "still_valid")
        self.assertEqual(reviews[0]["reason"], "included_in_current_staged_command")
        self.assertAlmostEqual(reviews[0]["remaining_delta"], 0.025)

    def test_reviews_prior_deferred_item_as_cancelled_when_signal_reverses(self):
        reviews = review_deferred_execution_items(
            open_items=[
                {
                    "deferred_id": "d1",
                    "ticker": "SPY",
                    "side": "buy",
                    "remaining_delta": 0.075,
                    "desired_weight": 0.20,
                }
            ],
            current_weights={"SPY": 0.125, "CASH": 0.875},
            desired_target_weights={"SPY": 0.10, "CASH": 0.90},
            staged_target_weights={"SPY": 0.10, "CASH": 0.90},
        )

        self.assertEqual(reviews[0]["status"], "cancelled")
        self.assertEqual(reviews[0]["reason"], "current_plan_no_longer_requires_buy_delta")
        self.assertEqual(reviews[0]["remaining_delta"], 0.0)

    def test_reviews_prior_deferred_item_as_executed_when_holdings_reached_target(self):
        reviews = review_deferred_execution_items(
            open_items=[
                {
                    "deferred_id": "d1",
                    "ticker": "SPY",
                    "side": "buy",
                    "remaining_delta": 0.075,
                    "desired_weight": 0.20,
                }
            ],
            current_weights={"SPY": 0.20, "CASH": 0.80},
            desired_target_weights={"SPY": 0.20, "CASH": 0.80},
            staged_target_weights={"SPY": 0.20, "CASH": 0.80},
        )

        self.assertEqual(reviews[0]["status"], "executed")
        self.assertEqual(reviews[0]["reason"], "holdings_reached_deferred_desired_weight")
        self.assertEqual(reviews[0]["remaining_delta"], 0.0)

    def test_pressure_summary_counts_open_and_still_valid_items(self):
        summary = summarize_deferred_execution_pressure([
            {"ticker": "SPY", "status": "open", "remaining_delta": 0.03},
            {"ticker": "QQQ", "status": "still_valid", "remaining_delta": 0.02},
            {"ticker": "IWM", "status": "cancelled", "remaining_delta": 0.10},
        ])

        self.assertEqual(summary["open_count"], 2)
        self.assertEqual(summary["open_buy_delta"], 0.05)
        self.assertEqual(summary["tickers"], ["QQQ", "SPY"])


if __name__ == "__main__":
    unittest.main()
