from datetime import datetime
import unittest

from services.account_snapshot_store import (
    build_execution_ack_account_snapshot,
    is_usable_execution_ack_account_state,
)


class AccountSnapshotStoreTests(unittest.TestCase):
    def test_ack_snapshot_overrides_holdings_target_and_last_command(self):
        snapshot = build_execution_ack_account_snapshot(
            account_state={
                "timestamp_utc": "2026-06-04T10:34:20Z",
                "account_status": "ok",
                "data_status": "ok",
                "policy_version": "sprint8a",
                "open_order_count": 0,
                "holdings_weights": {"SPY": 0.1},
                "target_weights": {"SPY": 0.1},
            },
            command_id="analysis_242",
            ack_status="filled",
            holdings_weights={"SPY": 0.071088, "QQQ": 0.102688},
            target_weights={"SPY": 0.071088, "QQQ": 0.102688},
            received_at=datetime(2026, 6, 4, 10, 34, 21),
        )

        self.assertEqual(snapshot["source_packet_type"], "execution_ack")
        self.assertEqual(snapshot["last_command_id"], "analysis_242")
        self.assertEqual(snapshot["active_execution_status"], "filled")
        self.assertEqual(snapshot["open_order_count"], 0)
        self.assertFalse(snapshot["has_open_orders"])
        self.assertEqual(snapshot["holdings_weights"], {"SPY": 0.071088, "QQQ": 0.102688})
        self.assertEqual(snapshot["target_weights"], {"SPY": 0.071088, "QQQ": 0.102688})

    def test_rejected_ack_does_not_force_last_command_id(self):
        snapshot = build_execution_ack_account_snapshot(
            account_state={
                "timestamp_utc": "2026-06-04T10:34:20Z",
                "open_order_count": 0,
                "last_command_id": "analysis_241",
            },
            command_id="analysis_242",
            ack_status="rejected",
            received_at=datetime(2026, 6, 4, 10, 34, 21),
        )

        self.assertEqual(snapshot["last_command_id"], "analysis_241")

    def test_partial_ack_uses_fallback_account_fields(self):
        snapshot = build_execution_ack_account_snapshot(
            account_state={
                "timestamp_utc": "2026-06-04T10:34:20Z",
                "open_order_count": 0,
                "holdings_weights": {"SPY": 0.071088},
            },
            fallback_account_state={
                "account_status": "ok",
                "data_status": "ok",
                "total_value": 133572.0,
                "cash": 52000.0,
                "cash_pct": 0.389,
                "buying_power": 60000.0,
                "policy_version": "sprint8a",
            },
            command_id="analysis_242",
            ack_status="reconciled",
            received_at=datetime(2026, 6, 4, 10, 34, 21),
        )

        self.assertEqual(snapshot["account_status"], "ok")
        self.assertEqual(snapshot["data_status"], "ok")
        self.assertEqual(snapshot["total_value"], 133572.0)
        self.assertEqual(snapshot["cash"], 52000.0)
        self.assertEqual(snapshot["policy_version"], "sprint8a")
        self.assertEqual(snapshot["last_command_id"], "analysis_242")

    def test_usable_ack_requires_holdings_or_open_order_count(self):
        self.assertTrue(is_usable_execution_ack_account_state({"holdings_weights": {"SPY": 0.1}}))
        self.assertTrue(is_usable_execution_ack_account_state({"open_order_count": 0}))
        self.assertFalse(is_usable_execution_ack_account_state({"policy_version": "sprint8a"}))
        self.assertFalse(is_usable_execution_ack_account_state(None))


if __name__ == "__main__":
    unittest.main()
