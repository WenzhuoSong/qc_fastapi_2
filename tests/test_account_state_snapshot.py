from datetime import datetime
import unittest

from services.account_state_snapshot import build_account_state_snapshot


class AccountStateSnapshotTests(unittest.TestCase):
    def test_explicit_account_state_is_preserved(self):
        snapshot = build_account_state_snapshot(
            {
                "packet_type": "heartbeat",
                "timestamp_utc": "2026-05-24T10:00:00Z",
                "account_state": {
                    "timestamp_utc": "2026-05-24T10:00:01Z",
                    "account_status": "ok",
                    "data_status": "ok",
                    "total_portfolio_value": 100000.0,
                    "cash": 25000.0,
                    "cash_pct": 0.25,
                    "buying_power": 50000.0,
                    "open_order_count": 0,
                    "is_market_open": True,
                    "policy_version": "sprint8a",
                    "last_command_id": "analysis_214",
                    "active_command_id": "analysis_214",
                    "active_execution_status": "orders_submitted",
                    "processed_command_count": 12,
                    "holdings_weights": {"spy": 0.4, "QQQ": "0.35"},
                    "holdings": [
                        {
                            "ticker": "SPY",
                            "quantity": 12,
                            "average_price": 500.25,
                            "market_price": 510.5,
                            "market_value": 6126.0,
                            "unrealized_pnl": 123.4,
                        }
                    ],
                    "target_weights": {"SPY": 0.45},
                },
            },
            qc_snapshot_id=42,
            received_at=datetime(2026, 5, 24, 10, 0, 2),
        )

        self.assertEqual(snapshot["contract_version"], "v1")
        self.assertEqual(snapshot["qc_snapshot_id"], 42)
        self.assertEqual(snapshot["source_packet_type"], "heartbeat")
        self.assertEqual(snapshot["policy_version"], "sprint8a")
        self.assertEqual(snapshot["total_value"], 100000.0)
        self.assertEqual(snapshot["cash"], 25000.0)
        self.assertEqual(snapshot["buying_power"], 50000.0)
        self.assertEqual(snapshot["open_order_count"], 0)
        self.assertFalse(snapshot["has_open_orders"])
        self.assertTrue(snapshot["is_market_open"])
        self.assertEqual(snapshot["last_command_id"], "analysis_214")
        self.assertEqual(snapshot["active_command_id"], "analysis_214")
        self.assertEqual(snapshot["active_execution_status"], "orders_submitted")
        self.assertEqual(snapshot["processed_command_count"], 12)
        self.assertEqual(snapshot["holdings_weights"], {"SPY": 0.4, "QQQ": 0.35})
        self.assertEqual(snapshot["target_weights"], {"SPY": 0.45})
        self.assertEqual(snapshot["raw_snapshot"]["last_command_id"], "analysis_214")
        self.assertEqual(snapshot["raw_snapshot"]["active_command_id"], "analysis_214")
        self.assertEqual(snapshot["raw_snapshot"]["warnings"], [])
        self.assertEqual(snapshot["raw_snapshot"]["holdings_detail_rows"][0]["ticker"], "SPY")
        self.assertEqual(snapshot["raw_snapshot"]["holdings_detail_rows"][0]["quantity"], 12.0)
        self.assertEqual(snapshot["raw_snapshot"]["holdings_detail_rows"][0]["average_price"], 500.25)

    def test_legacy_heartbeat_derives_account_state_without_blocking(self):
        snapshot = build_account_state_snapshot(
            {
                "packet_type": "heartbeat",
                "timestamp_utc": "2026-05-24T10:00:00Z",
                "portfolio": {
                    "total_value": 100000,
                    "cash": 10000,
                    "is_market_open": False,
                },
                "holdings": [
                    {"ticker": "SPY", "weight_current": 0.55, "quantity": 2, "avg_price": 700.0},
                    {"ticker": "QQQ", "weight_current": "0.35"},
                ],
                "target_weights": {"SPY": 0.6, "QQQ": 0.3},
            }
        )

        self.assertEqual(snapshot["contract_version"], "v1")
        self.assertEqual(snapshot["cash_pct"], 0.1)
        self.assertEqual(snapshot["holdings_weights"], {"SPY": 0.55, "QQQ": 0.35})
        self.assertEqual(snapshot["target_weights"], {"SPY": 0.6, "QQQ": 0.3})
        self.assertIsNone(snapshot["open_order_count"])
        self.assertIn("legacy_payload_without_explicit_account_state", snapshot["raw_snapshot"]["warnings"])
        self.assertIn("missing_buying_power", snapshot["raw_snapshot"]["warnings"])
        self.assertIn("missing_open_order_count", snapshot["raw_snapshot"]["warnings"])
        self.assertEqual(snapshot["raw_snapshot"]["holdings_detail_rows"][0]["quantity"], 2.0)
        self.assertEqual(snapshot["raw_snapshot"]["holdings_detail_rows"][0]["average_price"], 700.0)


if __name__ == "__main__":
    unittest.main()
