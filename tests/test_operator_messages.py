import unittest

from services.operator_messages import (
    format_circuit_state_change_message,
    format_market_closed_stale_info,
    format_qc_lifecycle_ack_message,
    format_reconciliation_guard_alert_message,
)


class OperatorMessagesTests(unittest.TestCase):
    def test_circuit_message_includes_trigger_reason_and_recommended_action(self):
        text = format_circuit_state_change_message(
            state="ALERT",
            reason="LLM failure rate=83% (5/6) in 1h",
            primary_trigger="llm_failure",
        )

        self.assertIn("Circuit ALERT", text)
        self.assertIn("Trigger: llm_failure", text)
        self.assertIn("Reason: LLM failure rate=83% (5/6) in 1h", text)
        self.assertIn("Recommended:", text)
        self.assertIn("/reset_circuit", text)

    def test_market_closed_stale_uses_info_level_no_action_copy(self):
        text = format_market_closed_stale_info(
            {
                "snapshot": {
                    "recorded_at": "2026-06-05T20:10:02",
                    "age_seconds": 27001.421,
                },
                "freshness": {
                    "classification": "expected_market_closed_stale",
                    "market_status": {"phase": "closed"},
                },
            }
        )

        self.assertIn("ℹ️ Account snapshot stale because market is closed", text)
        self.assertIn("Status: no action needed", text)
        self.assertIn("Recommended: wait for the next market heartbeat", text)

    def test_reconciliation_divergence_includes_affected_tickers(self):
        text = format_reconciliation_guard_alert_message(
            {
                "status": "diverged",
                "reason": "holdings_reconciliation_divergence",
                "max_drift": 0.012,
                "command": {"command_id": "analysis_250", "lifecycle_state": "filled"},
                "drift_tickers": [
                    {"ticker": "QQQ", "expected": 0.10, "actual": 0.12, "diff": 0.02},
                    {"ticker": "XLK", "expected": 0.08, "actual": 0.07, "diff": -0.01},
                ],
            }
        )

        self.assertIn("Reconciliation guard: diverged", text)
        self.assertIn("Status: new command blocked", text)
        self.assertIn("QQQ", text)
        self.assertIn("XLK", text)
        self.assertIn("Next action:", text)

    def test_noop_execution_copy_does_not_claim_filled(self):
        text = format_qc_lifecycle_ack_message(
            "analysis_242",
            {
                "qc_status": "accepted",
                "qc_response": {
                    "execution_state": "noop_reconciled",
                    "order_summary": {
                        "is_noop": True,
                        "action_count": 11,
                        "actual_order_count": 0,
                        "filled_order_count": 0,
                    },
                },
                "lifecycle_state": "noop_reconciled",
            },
        )

        self.assertIn("No-op reconciled", text)
        self.assertIn("Status: no orders needed", text)
        self.assertNotIn("filled", text.lower())
        self.assertNotIn("Filled", text)


if __name__ == "__main__":
    unittest.main()
