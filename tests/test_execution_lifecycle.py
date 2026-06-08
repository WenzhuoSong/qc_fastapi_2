from datetime import datetime, timedelta
import unittest

from services.execution_lifecycle import (
    ExecutionSkipReason,
    classify_qc_feedback_trust,
    classify_new_command_vs_active,
    default_execution_lifecycle_config,
    evaluate_active_execution_gate,
    evaluate_stale_active_execution,
    is_reduce_only_vs_actual,
    is_within_target_tolerance,
)


class ExecutionLifecycleTests(unittest.TestCase):
    def test_feedback_trust_marks_unknown_command_untrusted(self):
        result = classify_qc_feedback_trust(
            command_known=False,
            qc_response={"status": "accepted", "actual_holdings_weights": {"SPY": 0.1}},
        )

        self.assertEqual(result["status"], "unknown_command_feedback")
        self.assertFalse(result["trusted_for_reconciliation"])
        self.assertEqual(result["lifecycle_state_hint"], "pending_reconcile")

    def test_feedback_trust_requires_per_leg_status_for_hard_reconciliation(self):
        result = classify_qc_feedback_trust(
            command_known=True,
            qc_response={
                "status": "reconciled",
                "actual_target_weights": {"SPY": 0.1},
                "actual_holdings_weights": {"SPY": 0.1},
                "order_summary": {
                    "actual_order_count": 1,
                    "filled_order_count": 1,
                    "open_order_count_after": 0,
                },
            },
        )

        self.assertEqual(result["status"], "pending_reconcile")
        self.assertFalse(result["trusted_for_reconciliation"])
        self.assertIn("per_leg_fill_status", result["missing_fields"])

    def test_feedback_trust_accepts_full_holdings_with_per_leg_status(self):
        result = classify_qc_feedback_trust(
            command_known=True,
            qc_response={
                "status": "filled",
                "actual_target_weights": {"SPY": 0.1},
                "actual_holdings_weights": {"SPY": 0.1},
                "order_summary": {
                    "actual_order_count": 1,
                    "filled_order_count": 1,
                    "open_order_count_after": 0,
                    "orders": [{"ticker": "SPY", "status": "Filled", "filled_quantity": 3}],
                },
            },
        )

        self.assertEqual(result["status"], "trusted_for_reconciliation")
        self.assertTrue(result["trusted_for_reconciliation"])
        self.assertEqual(result["lifecycle_state_hint"], "filled")

    def test_feedback_trust_keeps_partial_as_in_flight(self):
        result = classify_qc_feedback_trust(
            command_known=True,
            qc_response={
                "status": "partial",
                "actual_target_weights": {"SPY": 0.1},
                "actual_holdings_weights": {"SPY": 0.05},
                "order_summary": {
                    "actual_order_count": 1,
                    "filled_order_count": 0,
                    "open_order_count_after": 1,
                },
            },
        )

        self.assertEqual(result["status"], "partial")
        self.assertFalse(result["trusted_for_reconciliation"])
        self.assertEqual(result["lifecycle_state_hint"], "partial")

    def test_feedback_trust_rejection_is_trusted_feedback_not_reconciliation_truth(self):
        result = classify_qc_feedback_trust(
            command_known=True,
            qc_response={"status": "rejected", "reason": "policy_mismatch"},
        )

        self.assertEqual(result["status"], "trusted_terminal_no_execution")
        self.assertTrue(result["trusted_feedback"])
        self.assertFalse(result["trusted_for_reconciliation"])
        self.assertEqual(result["lifecycle_state_hint"], "rejected")

    def test_feedback_trust_distinguishes_noop_from_fill(self):
        result = classify_qc_feedback_trust(
            command_known=True,
            qc_response={
                "status": "accepted",
                "execution_state": "noop_reconciled",
                "actual_target_weights": {"SPY": 0.1},
                "actual_holdings_weights": {"SPY": 0.1},
                "order_summary": {"actual_order_count": 0, "is_noop": True},
            },
        )

        self.assertEqual(result["status"], "trusted_noop_reconciled")
        self.assertTrue(result["trusted_for_reconciliation"])
        self.assertEqual(result["lifecycle_state_hint"], "noop_reconciled")

    def test_skip_reason_contract_values_are_stable(self):
        self.assertEqual(ExecutionSkipReason.THROTTLE_DEFERRED.value, "throttle_deferred")
        self.assertEqual(ExecutionSkipReason.ACTIVE_EXECUTION_WAIT.value, "active_execution_wait")
        self.assertEqual(ExecutionSkipReason.PREFLIGHT_BLOCKED.value, "preflight_blocked")
        self.assertEqual(ExecutionSkipReason.GUARD_BLOCKED.value, "guard_blocked")

    def test_same_target_classifies_as_already_in_progress(self):
        result = classify_new_command_vs_active(
            new_target={"SPY": 0.101, "QQQ": 0.2},
            active_target={"SPY": 0.1, "QQQ": 0.2},
            actual_holdings={"SPY": 0.08, "QQQ": 0.18},
            active_open_orders=3,
        )

        self.assertEqual(result, "already_in_progress")

    def test_reduce_only_classifies_as_override_candidate(self):
        result = classify_new_command_vs_active(
            new_target={"SPY": 0.05, "QQQ": 0.1},
            active_target={"SPY": 0.2, "QQQ": 0.2},
            actual_holdings={"SPY": 0.08, "QQQ": 0.12},
            active_open_orders=2,
        )

        self.assertEqual(result, "reduce_only_override_candidate")

    def test_active_open_orders_classifies_as_in_progress(self):
        result = classify_new_command_vs_active(
            new_target={"SPY": 0.2, "QQQ": 0.2},
            active_target={"SPY": 0.1, "QQQ": 0.1},
            actual_holdings={"SPY": 0.08, "QQQ": 0.08},
            active_open_orders=2,
        )

        self.assertEqual(result, "active_command_in_progress")

    def test_no_open_orders_classifies_as_pending_reconciliation(self):
        result = classify_new_command_vs_active(
            new_target={"SPY": 0.2},
            active_target={"SPY": 0.1},
            actual_holdings={"SPY": 0.1},
            active_open_orders=0,
        )

        self.assertEqual(result, "previous_command_pending_reconciliation")

    def test_reduce_only_uses_actual_holdings_not_active_target(self):
        self.assertTrue(is_reduce_only_vs_actual({"SPY": 0.09}, {"SPY": 0.1}))
        self.assertFalse(is_reduce_only_vs_actual({"SPY": 0.11}, {"SPY": 0.1}))

    def test_reduce_only_is_portfolio_level_and_blocks_embedded_adds(self):
        self.assertFalse(
            is_reduce_only_vs_actual(
                {"QQQ": 0.05, "XLK": 0.09, "CASH": 0.86},
                {"QQQ": 0.10, "XLK": 0.04, "CASH": 0.86},
            )
        )
        self.assertFalse(
            is_reduce_only_vs_actual(
                {"QQQ": 0.05, "NEW": 0.01, "CASH": 0.94},
                {"QQQ": 0.10, "CASH": 0.90},
            )
        )
        self.assertTrue(
            is_reduce_only_vs_actual(
                {"QQQ": 0.05, "XLK": 0.04, "CASH": 0.91},
                {"QQQ": 0.10, "XLK": 0.04, "CASH": 0.86},
            )
        )

    def test_same_target_requires_non_empty_target(self):
        self.assertFalse(is_within_target_tolerance({}, {}))

    def test_default_config_is_observe_mode(self):
        config = default_execution_lifecycle_config({})

        self.assertEqual(config["mode"], "observe")
        self.assertTrue(config["block_ordinary_commands_when_active_execution"])

    def test_active_execution_gate_observe_records_would_defer_but_allows(self):
        result = evaluate_active_execution_gate(
            target_weights={"SPY": 0.2},
            active_execution={
                "command_id": "analysis_1",
                "status": "partial",
                "open_order_count": 2,
                "target_weights": {"SPY": 0.1},
                "holdings_weights": {"SPY": 0.08},
            },
            config={"mode": "observe"},
        )

        self.assertTrue(result["allowed"])
        self.assertTrue(result["would_defer"])
        self.assertEqual(result["skip_reason"], "active_execution_wait")
        self.assertEqual(result["execution_effect"], "diagnostic_only")

    def test_active_execution_gate_active_blocks_ordinary_rebalance(self):
        result = evaluate_active_execution_gate(
            target_weights={"SPY": 0.2},
            active_execution={
                "command_id": "analysis_1",
                "status": "partial",
                "open_order_count": 2,
                "target_weights": {"SPY": 0.1},
                "holdings_weights": {"SPY": 0.08},
            },
            config={"mode": "active"},
        )

        self.assertFalse(result["allowed"])
        self.assertTrue(result["would_defer"])
        self.assertEqual(result["status"], "deferred_by_active_execution")
        self.assertEqual(result["execution_effect"], "active_block")

    def test_active_execution_gate_allows_reduce_only_override_candidate(self):
        result = evaluate_active_execution_gate(
            target_weights={"SPY": 0.05},
            active_execution={
                "command_id": "analysis_1",
                "status": "partial",
                "open_order_count": 2,
                "target_weights": {"SPY": 0.2},
                "holdings_weights": {"SPY": 0.08},
            },
            config={"mode": "active", "allow_reduce_only_override": True},
        )

        self.assertTrue(result["allowed"])
        self.assertFalse(result["would_defer"])
        self.assertEqual(result["status"], "reduce_only_override_allowed")

    def test_stale_active_execution_with_open_orders_alerts_operator(self):
        now = datetime(2026, 6, 2, 11, 30)
        result = evaluate_stale_active_execution(
            {
                "command_id": "analysis_1",
                "status": "partial",
                "open_order_count": 2,
                "has_open_orders": True,
                "started_at": (now - timedelta(minutes=90)).isoformat(),
            },
            {"max_active_execution_minutes": 60, "auto_cancel_stale_open_orders": False},
            now=now,
        )

        self.assertTrue(result["is_stale"])
        self.assertEqual(result["reason"], "open_orders_not_filling")
        self.assertEqual(result["auto_action"], "alert_operator")
        self.assertFalse(result["auto_cancel"])
        self.assertEqual(result["operator_action"], "check_dashboard_then_cancel_orders_if_orders_are_stuck")

    def test_stale_active_execution_without_open_orders_triggers_reconciliation(self):
        now = datetime(2026, 6, 2, 10, 45)
        result = evaluate_stale_active_execution(
            {
                "command_id": "analysis_1",
                "status": "orders_submitted",
                "open_order_count": 0,
                "has_open_orders": False,
                "started_at": (now - timedelta(minutes=45)).isoformat(),
            },
            {"max_active_execution_minutes": 30},
            now=now,
        )

        self.assertTrue(result["is_stale"])
        self.assertEqual(result["reason"], "no_open_orders_but_unreconciled")
        self.assertEqual(result["auto_action"], "trigger_reconciliation")
        self.assertEqual(result["operator_action"], "force_reconcile_if_heartbeat_does_not_close_lifecycle")

    def test_active_execution_gate_includes_stale_diagnostics(self):
        result = evaluate_active_execution_gate(
            target_weights={"SPY": 0.2},
            active_execution={
                "command_id": "analysis_1",
                "status": "partial",
                "open_order_count": 2,
                "target_weights": {"SPY": 0.1},
                "holdings_weights": {"SPY": 0.08},
                "started_at": (datetime.utcnow() - timedelta(minutes=90)).isoformat(),
            },
            config={"mode": "active", "max_active_execution_minutes": 1},
        )

        self.assertIn("stale_active_execution", result)
        self.assertEqual(result["stale_active_execution"]["command_id"], "analysis_1")

    def test_strict_mode_promotes_stale_active_execution_status(self):
        stale_started_at = (datetime.utcnow() - timedelta(minutes=90)).isoformat()
        result = evaluate_active_execution_gate(
            target_weights={"SPY": 0.2},
            active_execution={
                "command_id": "analysis_1",
                "status": "partial",
                "open_order_count": 2,
                "has_open_orders": True,
                "target_weights": {"SPY": 0.1},
                "holdings_weights": {"SPY": 0.08},
                "started_at": stale_started_at,
            },
            config={"mode": "strict", "max_active_execution_minutes": 1},
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["status"], "stale_active_execution")
        self.assertEqual(result["execution_effect"], "active_block")
        self.assertTrue(result["stale_active_execution"]["is_stale"])
        self.assertEqual(result["stale_active_execution"]["auto_action"], "alert_operator")

    def test_strict_mode_keeps_non_stale_active_execution_as_active_wait(self):
        fresh_started_at = datetime.utcnow().isoformat()
        result = evaluate_active_execution_gate(
            target_weights={"SPY": 0.2},
            active_execution={
                "command_id": "analysis_1",
                "status": "partial",
                "open_order_count": 2,
                "has_open_orders": True,
                "target_weights": {"SPY": 0.1},
                "holdings_weights": {"SPY": 0.08},
                "started_at": fresh_started_at,
            },
            config={"mode": "strict", "max_active_execution_minutes": 240},
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["status"], "deferred_by_active_execution")
        self.assertFalse(result["stale_active_execution"]["is_stale"])


if __name__ == "__main__":
    unittest.main()
