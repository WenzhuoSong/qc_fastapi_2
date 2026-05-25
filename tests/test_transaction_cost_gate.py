import unittest

from services.transaction_cost_gate import (
    default_transaction_cost_gate_config,
    evaluate_transaction_cost_gate,
)


class TransactionCostGateTests(unittest.TestCase):
    def test_default_config_uses_ibkr_observe_mode(self):
        cfg = default_transaction_cost_gate_config({})

        self.assertEqual(cfg["broker"], "IBKR")
        self.assertEqual(cfg["mode"], "observe")
        self.assertEqual(cfg["cost_rates"]["ordinary_etf"], 0.0002)
        self.assertTrue(cfg["warn_on_buys_only"])

    def test_supported_buy_has_no_warning(self):
        out = evaluate_transaction_cost_gate(
            target_weights={"SPY": 0.12, "CASH": 0.88},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            rebalance_actions=[
                {
                    "ticker": "SPY",
                    "action": "buy",
                    "weight_current": 0.10,
                    "weight_target": 0.12,
                    "weight_delta": 0.02,
                }
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "evidence_cards": [
                            {
                                "ticker": "SPY",
                                "role": "core",
                                "action": "increase",
                                "confidence": 0.9,
                                "conviction": 0.8,
                                "conviction_status": "calibrated",
                                "effective_confidence": 0.72,
                            }
                        ],
                    }
                ]
            },
        )

        self.assertEqual(out["broker"], "IBKR")
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["warnings"], [])
        self.assertEqual(out["rows"][0]["verdict"], "cost_supported")
        self.assertGreaterEqual(out["rows"][0]["edge_to_cost_ratio"], 2.0)

    def test_missing_buy_evidence_warns_but_does_not_block(self):
        out = evaluate_transaction_cost_gate(
            target_weights={"QQQ": 0.12, "CASH": 0.88},
            current_weights={"CASH": 1.0},
            rebalance_actions=[
                {
                    "ticker": "QQQ",
                    "action": "buy",
                    "weight_current": 0.0,
                    "weight_target": 0.12,
                    "weight_delta": 0.12,
                }
            ],
            strategy_evidence={"strategy_results": []},
        )

        self.assertEqual(out["mode"], "observe")
        self.assertEqual(out["execution_effect"], "diagnostic_only")
        self.assertEqual(out["status"], "observe_warning")
        self.assertEqual(out["rows"][0]["verdict"], "missing_signal_edge")
        self.assertIn("missing EvidenceCard", out["warnings"][0])

    def test_watch_signal_does_not_support_buy_edge(self):
        out = evaluate_transaction_cost_gate(
            target_weights={"QQQ": 0.12, "CASH": 0.88},
            current_weights={"CASH": 1.0},
            rebalance_actions=[
                {
                    "ticker": "QQQ",
                    "action": "buy",
                    "weight_current": 0.0,
                    "weight_target": 0.12,
                    "weight_delta": 0.12,
                }
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "evidence_cards": [
                            {
                                "ticker": "QQQ",
                                "role": "core",
                                "action": "watch",
                                "confidence": 1.0,
                                "conviction": 0.9,
                                "conviction_status": "calibrated",
                                "effective_confidence": 0.9,
                            }
                        ],
                    }
                ]
            },
        )

        self.assertEqual(out["status"], "observe_warning")
        self.assertEqual(out["rows"][0]["verdict"], "missing_signal_edge")
        self.assertIn("does not support buy", out["warnings"][0])

    def test_sell_action_is_diagnostic_only_when_buy_warning_mode_enabled(self):
        out = evaluate_transaction_cost_gate(
            target_weights={"SPY": 0.10, "CASH": 0.90},
            current_weights={"SPY": 0.20, "CASH": 0.80},
            rebalance_actions=[
                {
                    "ticker": "SPY",
                    "action": "sell",
                    "weight_current": 0.20,
                    "weight_target": 0.10,
                    "weight_delta": -0.10,
                }
            ],
            strategy_evidence={"strategy_results": []},
        )

        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["warnings"], [])
        self.assertEqual(out["rows"][0]["verdict"], "diagnostic_sell")

    def test_volatility_etp_uses_higher_ibkr_cost_bucket(self):
        out = evaluate_transaction_cost_gate(
            target_weights={"UVXY": 0.03, "CASH": 0.97},
            current_weights={"CASH": 1.0},
            rebalance_actions=[
                {
                    "ticker": "UVXY",
                    "action": "buy",
                    "weight_current": 0.0,
                    "weight_target": 0.03,
                    "weight_delta": 0.03,
                }
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "leveraged_etf_momentum_allocator",
                        "evidence_cards": [
                            {
                                "ticker": "UVXY",
                                "role": "vol_hedge",
                                "action": "hedge",
                                "confidence": 1.0,
                                "conviction": 0.7,
                                "conviction_status": "calibrated",
                                "effective_confidence": 0.7,
                            }
                        ],
                    }
                ]
            },
        )

        row = out["rows"][0]
        self.assertEqual(row["asset_cost_bucket"], "volatility_etp")
        self.assertEqual(row["estimated_cost_rate"], 0.0015)
        self.assertGreater(row["cost_drag"], 0)


if __name__ == "__main__":
    unittest.main()
