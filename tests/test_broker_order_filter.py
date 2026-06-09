import unittest

from services.broker_order_filter import apply_broker_order_filter_to_snapshot


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


if __name__ == "__main__":
    unittest.main()
