import unittest

from services.execution_audit import (
    build_execution_audit_payload,
    count_execution_actions_from_payload,
)


class ExecutionAuditTests(unittest.TestCase):
    def test_builds_proposed_audit_payload(self):
        payload = build_execution_audit_payload(
            action_status="proposed",
            proposed_weights={"AAA": 0.2, "CASH": 0.8},
            rebalance_actions=[{"ticker": "AAA", "weight_delta": 0.2}],
            estimated_cost_pct=0.001,
            reason="semi_auto_pending_confirmation",
        )

        self.assertEqual(payload["action_status"], "proposed")
        self.assertEqual(payload["proposed_weights"]["AAA"], 0.2)
        self.assertEqual(count_execution_actions_from_payload(payload), 1)

    def test_counts_non_cash_sent_weights_when_actions_missing(self):
        payload = build_execution_audit_payload(
            action_status="accepted",
            sent_weights={"AAA": 0.2, "BBB": 0.1, "CASH": 0.7},
        )

        self.assertEqual(count_execution_actions_from_payload(payload), 2)


if __name__ == "__main__":
    unittest.main()
