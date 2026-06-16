import unittest

from services.broker_order_filter import (
    apply_broker_order_filter_to_snapshot,
    reconciliation_target_weights_from_command_payload,
)


class BrokerOrderFilterTests(unittest.TestCase):
    def test_suppresses_single_share_non_liquidation_trim(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"QQQ": 0.040955, "XLE": 0.037214, "CASH": 0.921831},
            current_weights={"QQQ": 0.0430, "XLE": 0.037214, "CASH": 0.919786},
            snapshot={
                "total_value": 110397.79,
                "prices": {"QQQ": 714.50, "XLE": 58.88},
            },
            config={"broker_allow_reduce_only_micro_sells": False},
        )

        self.assertTrue(result["adjusted"], result)
        self.assertEqual(result["suppressed_orders"][0]["ticker"], "QQQ")
        self.assertEqual(result["suppressed_orders"][0]["reason"], "below_min_non_liquidation_share_delta")
        self.assertAlmostEqual(result["target_weights"]["QQQ"], 0.0430, places=6)
        self.assertAlmostEqual(result["target_weights"]["CASH"], 0.919786, places=6)

    def test_allows_micro_sell_when_whole_portfolio_is_reduce_only(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"QQQ": 0.040955, "XLE": 0.037214, "CASH": 0.921831},
            current_weights={"QQQ": 0.0430, "XLE": 0.037214, "CASH": 0.919786},
            snapshot={
                "total_value": 110397.79,
                "prices": {"QQQ": 714.50, "XLE": 58.88},
            },
        )

        self.assertFalse(result["adjusted"], result)
        self.assertTrue(result["portfolio_reduce_only"])
        self.assertEqual(result["allowed_orders"][0]["ticker"], "QQQ")
        self.assertEqual(result["allowed_orders"][0]["side"], "sell")
        self.assertEqual(result["allowed_orders"][0]["micro_order_override"], "portfolio_reduce_only_sell")
        self.assertEqual(result["target_weights"]["QQQ"], 0.040955)

    def test_mixed_rebalance_does_not_use_reduce_only_micro_sell_override(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"QQQ": 0.040955, "XLE": 0.047214, "CASH": 0.911831},
            current_weights={"QQQ": 0.0430, "XLE": 0.037214, "CASH": 0.919786},
            snapshot={
                "total_value": 110397.79,
                "prices": {"QQQ": 714.50, "XLE": 58.88},
            },
        )

        self.assertTrue(result["adjusted"], result)
        self.assertFalse(result["portfolio_reduce_only"])
        self.assertEqual(result["suppressed_orders"][0]["ticker"], "QQQ")
        self.assertNotIn("micro_order_override", result["suppressed_orders"][0])

    def test_allows_liquidation_to_zero_even_when_order_is_small(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"XLU": 0.0, "CASH": 1.0},
            current_weights={"XLU": 0.0003, "CASH": 0.9997},
            snapshot={
                "total_value": 110397.79,
                "prices": {"XLU": 44.11},
            },
        )

        self.assertFalse(result["adjusted"], result)
        self.assertEqual(result["allowed_orders"][0]["ticker"], "XLU")
        self.assertTrue(result["allowed_orders"][0]["liquidation_to_zero"])
        self.assertEqual(result["target_weights"]["XLU"], 0.0)

    def test_missing_price_is_diagnostic_only(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"QQQ": 0.05, "CASH": 0.95},
            current_weights={"QQQ": 0.04, "CASH": 0.96},
            snapshot={"total_value": 100000.0, "prices": {}},
        )

        self.assertFalse(result["adjusted"], result)
        self.assertIn("price:QQQ", result["missing_inputs"])
        self.assertAlmostEqual(result["target_weights"]["QQQ"], 0.05)

    def test_rounds_up_buy_micro_order_to_min_executable_shares(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"SMH": 0.009, "CASH": 0.991},
            current_weights={"SMH": 0.0, "CASH": 1.0},
            snapshot={"total_value": 100000.0, "prices": {"SMH": 600.0}},
        )

        self.assertTrue(result["adjusted"], result)
        self.assertEqual(result["reason"], "micro_buy_orders_rounded_up")
        self.assertEqual(len(result["suppressed_orders"]), 0)
        self.assertEqual(len(result["rounded_orders"]), 1)
        rounded = result["rounded_orders"][0]
        self.assertEqual(rounded["ticker"], "SMH")
        self.assertEqual(rounded["reason"], "rounded_up_to_min_executable_buy")
        self.assertAlmostEqual(rounded["original_delta_weight"], 0.009, places=6)
        self.assertAlmostEqual(rounded["rounded_delta_weight"], 0.012, places=6)
        self.assertAlmostEqual(result["target_weights"]["SMH"], 0.012, places=6)
        self.assertAlmostEqual(result["metrics_after"]["buy_delta"], 0.012, places=6)

    def test_reconciliation_target_uses_original_target_for_rounded_buy_hint(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"SMH": 0.017302, "CASH": 0.982698},
            current_weights={"SMH": 0.0096, "CASH": 0.9904},
            snapshot={"total_value": 135_220.0, "prices": {"SMH": 647.77}},
        )

        payload = {
            "sent_weights": result["target_weights"],
            "proposed_weights": {"SMH": 0.017302, "CASH": 0.982698},
            "command_preflight": {"broker_order_filter": result},
        }

        target = reconciliation_target_weights_from_command_payload(payload)

        self.assertAlmostEqual(result["target_weights"]["SMH"], 0.019181, places=6)
        self.assertAlmostEqual(target["SMH"], 0.017302, places=6)

    def test_does_not_round_up_buy_when_multiplier_is_too_large(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"SMH": 0.002, "CASH": 0.998},
            current_weights={"SMH": 0.0, "CASH": 1.0},
            snapshot={"total_value": 100000.0, "prices": {"SMH": 600.0}},
        )

        self.assertTrue(result["adjusted"], result)
        self.assertEqual(len(result["rounded_orders"]), 0)
        self.assertEqual(result["suppressed_orders"][0]["ticker"], "SMH")
        self.assertEqual(result["suppressed_orders"][0]["reason"], "below_min_non_liquidation_share_delta")
        attempt = result["suppressed_orders"][0]["round_up_attempt"]
        self.assertFalse(attempt["allowed"])
        self.assertEqual(attempt["reason"], "round_up_multiplier_exceeds_limit")
        self.assertAlmostEqual(result["target_weights"]["SMH"], 0.0, places=6)

    def test_does_not_round_up_sell_micro_order(self):
        result = apply_broker_order_filter_to_snapshot(
            target_weights={"SMH": 0.011, "CASH": 0.989},
            current_weights={"SMH": 0.02, "CASH": 0.98},
            snapshot={"total_value": 100000.0, "prices": {"SMH": 600.0}},
            config={"broker_allow_reduce_only_micro_sells": False},
        )

        self.assertTrue(result["adjusted"], result)
        self.assertEqual(len(result["rounded_orders"]), 0)
        self.assertEqual(result["suppressed_orders"][0]["ticker"], "SMH")
        self.assertEqual(result["suppressed_orders"][0]["side"], "sell")
        self.assertNotIn("round_up_attempt", result["suppressed_orders"][0])
        self.assertAlmostEqual(result["target_weights"]["SMH"], 0.02, places=6)


if __name__ == "__main__":
    unittest.main()
