from datetime import UTC, datetime
import unittest

from services.command_lifecycle import build_command_lifecycle_event, build_command_reconciliation_events


class CommandLifecycleTests(unittest.TestCase):
    def test_build_command_lifecycle_event_normalizes_time_and_payload(self):
        event = build_command_lifecycle_event(
            command_id=" cmd_1 ",
            analysis_id=12,
            event_type="submitted_to_qc",
            event_status="submitted",
            source="fastapi",
            payload={"weights": {"SPY": 0.2}},
            event_time=datetime(2026, 5, 24, 10, 0, tzinfo=UTC),
        )

        self.assertEqual(event["command_id"], "cmd_1")
        self.assertEqual(event["analysis_id"], 12)
        self.assertEqual(event["event_type"], "submitted_to_qc")
        self.assertEqual(event["event_status"], "submitted")
        self.assertIsNone(event["event_time"].tzinfo)
        self.assertEqual(event["payload"]["weights"]["SPY"], 0.2)

    def test_build_command_lifecycle_event_rejects_unknown_event_type(self):
        with self.assertRaises(ValueError):
            build_command_lifecycle_event(command_id="cmd_1", event_type="surprise")

    def test_build_command_lifecycle_event_requires_command_id(self):
        with self.assertRaises(ValueError):
            build_command_lifecycle_event(command_id="", event_type="created")

    def test_reconciliation_events_mark_filled_and_reconciled_when_holdings_match(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "actual_target_weights": {"SPY": 0.2},
                "actual_holdings_weights": {"SPY": 0.201},
                "order_summary": {
                    "submitted_order_count": 1,
                    "filled_order_count": 1,
                    "open_order_count_after": 0,
                    "all_filled": True,
                },
            },
        )

        self.assertEqual([event["event_type"] for event in events], ["filled", "reconciled"])
        self.assertEqual(events[1]["event_status"], "reconciled")
        self.assertLessEqual(events[1]["payload"]["max_abs_diff"], 0.01)

    def test_reconciliation_events_mark_partial_when_qc_reports_open_orders(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "actual_target_weights": {"SPY": 0.2},
                "actual_holdings_weights": {"SPY": 0.1},
                "order_summary": {
                    "submitted_order_count": 1,
                    "open_order_count_after": 1,
                },
            },
        )

        self.assertEqual(events[0]["event_type"], "partial")
        self.assertEqual(events[0]["reason"], "qc_reports_open_orders_after_command")

    def test_reconciliation_events_mark_drift_when_holdings_do_not_match(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "actual_target_weights": {"SPY": 0.2},
                "actual_holdings_weights": {"SPY": 0.14},
                "account_state": {"open_order_count": 0, "has_open_orders": False},
            },
        )

        self.assertEqual(events[0]["event_type"], "reconciliation_drift")
        self.assertGreater(events[0]["payload"]["max_abs_diff"], 0.01)

    def test_delayed_account_snapshot_holdings_override_stale_ack_holdings(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "actual_target_weights": {"SPY": 0.2},
                "actual_holdings_weights": {"SPY": 0.05},
            },
            account_state={
                "open_order_count": 0,
                "has_open_orders": False,
                "holdings_weights": {"SPY": 0.2},
                "target_weights": {"SPY": 0.2},
            },
        )

        self.assertEqual(events[0]["event_type"], "reconciled")


if __name__ == "__main__":
    unittest.main()
