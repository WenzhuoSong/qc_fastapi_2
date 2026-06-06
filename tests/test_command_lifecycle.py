from datetime import UTC, datetime
import unittest

from services.command_lifecycle import (
    build_command_lifecycle_event,
    build_command_reconciliation_events,
    build_reconciliation_lag_report,
    lifecycle_state_from_status,
    next_lifecycle_state,
)


class CommandLifecycleTests(unittest.TestCase):
    def test_lifecycle_state_maps_submission_to_pending_ack(self):
        self.assertEqual(
            lifecycle_state_from_status(status="sent", qc_status="submitted"),
            "pending_ack",
        )

    def test_lifecycle_state_maps_qc_accepted(self):
        self.assertEqual(
            lifecycle_state_from_status(status="sent", qc_status="accepted"),
            "accepted",
        )

    def test_lifecycle_state_maps_noop_reconciled_from_qc_payload(self):
        self.assertEqual(
            lifecycle_state_from_status(
                status="sent",
                qc_status="reconciled",
                qc_response={"order_summary": {"execution_state": "noop_reconciled"}},
            ),
            "noop_reconciled",
        )

    def test_lifecycle_state_maps_reconciliation_drift_to_diverged(self):
        self.assertEqual(
            lifecycle_state_from_status(status="sent", qc_status="reconciliation_drift"),
            "diverged",
        )

    def test_lifecycle_state_maps_not_sent_to_created(self):
        self.assertEqual(
            lifecycle_state_from_status(status="deduped", qc_status="not_sent"),
            "created",
        )

    def test_next_lifecycle_state_allows_forward_progress(self):
        self.assertEqual(next_lifecycle_state("accepted", "orders_submitted"), "orders_submitted")
        self.assertEqual(next_lifecycle_state("orders_submitted", "partial"), "partial")
        self.assertEqual(next_lifecycle_state("partial", "filled"), "filled")

    def test_next_lifecycle_state_blocks_late_ack_regression(self):
        self.assertEqual(next_lifecycle_state("filled", "accepted"), "filled")
        self.assertEqual(next_lifecycle_state("filled", "rejected"), "filled")
        self.assertEqual(next_lifecycle_state("noop_reconciled", "pending_reconcile"), "noop_reconciled")
        self.assertEqual(next_lifecycle_state("diverged", "accepted"), "diverged")
        self.assertEqual(next_lifecycle_state("rejected", "accepted"), "rejected")

    def test_next_lifecycle_state_allows_filled_to_diverged_escalation(self):
        self.assertEqual(next_lifecycle_state("filled", "diverged"), "diverged")

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

        self.assertEqual([event["event_type"] for event in events], ["orders_submitted", "filled", "reconciled"])
        self.assertEqual(events[2]["event_status"], "reconciled")
        self.assertLessEqual(events[2]["payload"]["max_abs_diff"], 0.01)

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

        self.assertEqual([event["event_type"] for event in events], ["orders_submitted", "partial"])
        self.assertEqual(events[1]["reason"], "qc_reports_open_orders_after_command")

    def test_open_orders_do_not_emit_terminal_reconciliation_drift(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "actual_target_weights": {"SPY": 0.2},
                "actual_holdings_weights": {"SPY": 0.02},
                "order_summary": {
                    "submitted_order_count": 1,
                    "open_order_count_after": 1,
                },
            },
        )

        self.assertEqual([event["event_type"] for event in events], ["orders_submitted", "partial"])

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

    def test_reconciliation_events_mark_orders_submitted_from_execution_state(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "execution_state": "orders_submitted",
                "actual_target_weights": {"SPY": 0.2},
                "order_summary": {
                    "submitted_order_count": 1,
                    "filled_order_count": 0,
                    "open_order_count_after": 1,
                },
            },
        )

        self.assertEqual(events[0]["event_type"], "orders_submitted")
        self.assertEqual(events[0]["event_status"], "orders_submitted")

    def test_reconciliation_events_mark_failed_no_fill_when_qc_reports_it(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "execution_state": "failed_no_fill",
                "order_summary": {
                    "submitted_order_count": 0,
                    "filled_order_count": 0,
                    "open_order_count_after": 0,
                },
            },
        )

        self.assertEqual([event["event_type"] for event in events], ["failed_no_fill"])
        self.assertEqual(events[0]["reason"], "qc_reports_command_completed_without_fills")

    def test_noop_reconciled_does_not_emit_order_events(self):
        events = build_command_reconciliation_events(
            command_id="cmd_1",
            command_payload={"sent_weights": {"SPY": 0.2}},
            qc_response={
                "status": "accepted",
                "execution_state": "noop_reconciled",
                "actual_target_weights": {"SPY": 0.2},
                "actual_holdings_weights": {"SPY": 0.2},
                "order_summary": {
                    "action_count": 1,
                    "actual_order_count": 0,
                    "submitted_order_count": 0,
                    "filled_order_count": 0,
                    "open_order_count_after": 0,
                    "is_noop": True,
                    "noop_reason": "target_matches_current",
                },
            },
        )

        self.assertEqual([event["event_type"] for event in events], ["reconciled"])

    def test_reconciliation_lag_report_flags_accepted_without_reconciled_event(self):
        report = build_reconciliation_lag_report(
            now=datetime(2026, 5, 28, 12, 45),
            max_age_minutes=30,
            commands=[
                {
                    "command_id": "analysis_1",
                    "analysis_id": 1,
                    "command_type": "weight_adjustment",
                    "qc_status": "accepted",
                    "qc_ack_at": datetime(2026, 5, 28, 12, 0),
                },
                {
                    "command_id": "analysis_2",
                    "analysis_id": 2,
                    "command_type": "weight_adjustment",
                    "qc_status": "partial",
                    "qc_ack_at": datetime(2026, 5, 28, 12, 35),
                },
                {
                    "command_id": "analysis_3",
                    "analysis_id": 3,
                    "command_type": "policy_sync",
                    "qc_status": "accepted",
                    "qc_ack_at": datetime(2026, 5, 28, 11, 0),
                },
            ],
            events=[
                {
                    "command_id": "analysis_2",
                    "event_type": "qc_accepted",
                    "event_status": "accepted",
                    "event_time": datetime(2026, 5, 28, 12, 35),
                },
            ],
        )

        self.assertEqual(report["accepted_without_reconciled_count"], 2)
        self.assertEqual(report["overdue_count"], 1)
        self.assertEqual(report["pending_count"], 1)
        self.assertEqual(report["rows"][0]["command_id"], "analysis_1")
        self.assertEqual(report["rows"][0]["status"], "overdue")

    def test_reconciliation_lag_report_excludes_reconciled_or_drift_commands(self):
        report = build_reconciliation_lag_report(
            now=datetime(2026, 5, 28, 12, 45),
            max_age_minutes=30,
            commands=[
                {
                    "command_id": "analysis_1",
                    "analysis_id": 1,
                    "command_type": "weight_adjustment",
                    "qc_status": "accepted",
                    "qc_ack_at": datetime(2026, 5, 28, 12, 0),
                },
                {
                    "command_id": "analysis_2",
                    "analysis_id": 2,
                    "command_type": "weight_adjustment",
                    "qc_status": "accepted",
                    "qc_ack_at": datetime(2026, 5, 28, 11, 0),
                },
            ],
            events=[
                {
                    "command_id": "analysis_1",
                    "event_type": "reconciled",
                    "event_status": "reconciled",
                    "event_time": datetime(2026, 5, 28, 12, 1),
                },
                {
                    "command_id": "analysis_2",
                    "event_type": "reconciliation_drift",
                    "event_status": "drift",
                    "event_time": datetime(2026, 5, 28, 11, 1),
                },
            ],
        )

        self.assertEqual(report["accepted_without_reconciled_count"], 0)
        self.assertEqual(report["rows"], [])


if __name__ == "__main__":
    unittest.main()
