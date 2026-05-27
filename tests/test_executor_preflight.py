import unittest
from pathlib import Path

from services.execution_preflight import (
    _policy_sync_ack_status,
    command_weight_delta_metrics,
    preflight_execution_weights,
)
from services.transaction_cost_gate import format_transaction_cost_gate_summary


class ExecutorPreflightTests(unittest.TestCase):
    def test_blocks_unknown_positive_weight(self):
        result = preflight_execution_weights({"COMPLETELY_UNKNOWN": 0.01, "CASH": 0.99})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["cap_violations"][0]["ticker"], "COMPLETELY_UNKNOWN")

    def test_blocks_single_cap_violation(self):
        result = preflight_execution_weights({"PSI": 0.08, "CASH": 0.92})

        self.assertFalse(result["allowed"])
        self.assertEqual(result["cap_violations"][0]["ticker"], "PSI")

    def test_allows_policy_compliant_weights(self):
        result = preflight_execution_weights({"SPY": 0.20, "PSI": 0.075, "SQQQ": 0.03, "CASH": 0.695})

        self.assertTrue(result["allowed"], result)

    def test_preflight_block_copy_points_to_final_cap_system_bug(self):
        text = Path("agents/executor.py").read_text()

        self.assertIn("Executor preflight blocked", text)
        self.assertIn("final_policy_cap stage failed to enforce execution limits", text)
        self.assertIn("This is a system bug, not a business decision", text)

    def test_executor_syncs_policy_before_setweights(self):
        text = Path("agents/executor.py").read_text()
        sync_pos = text.index("tool_send_policy_sync")
        send_pos = text.index("result = await tool_send_weight_command")

        self.assertLess(sync_pos, send_pos)
        self.assertIn("create_or_update_policy_sync_log", text)
        self.assertIn("wait_for_qc_ack_detail(policy_sync_id", text)
        self.assertIn("policy_sync_not_accepted", text)
        self.assertIn("PolicySync failed before", text)
        self.assertIn("No command sent to QC", text)

    def test_setweights_command_carries_policy_snapshot(self):
        text = Path("tools/qc_tools.py").read_text()

        self.assertIn("policy = inp.get(\"policy\") or policy_snapshot()", text)
        self.assertIn("policy_version = policy.get(\"version\")", text)
        self.assertIn('"policy_version": policy_version', text)
        self.assertIn('"policy": policy', text)

    def test_executor_requires_final_risk_validation_before_qc_command(self):
        text = Path("agents/executor.py").read_text()
        final_pos = text.index("Final risk validation missing or failed")
        send_pos = text.index("result = await tool_send_weight_command")

        self.assertLess(final_pos, send_pos)
        self.assertIn("blocked_by_final_risk_validation", text)

    def test_telegram_confirm_requires_final_risk_validation(self):
        text = Path("services/telegram_commands.py").read_text()
        final_pos = text.index("Final risk validation missing or failed")
        send_pos = text.index("result = await tool_send_weight_command")

        self.assertLess(final_pos, send_pos)

    def test_command_delta_metrics_split_buy_sell_and_gross_turnover(self):
        result = command_weight_delta_metrics(
            {"SPY": 0.25, "QQQ": 0.10, "CASH": 0.65},
            {"SPY": 0.10, "XLK": 0.08, "CASH": 0.82},
        )

        self.assertEqual(result["buy_delta"], 0.25)
        self.assertEqual(result["sell_delta"], 0.08)
        self.assertEqual(result["gross_turnover"], 0.165)

    def test_policy_sync_success_requires_qc_ack_status(self):
        self.assertEqual(
            _policy_sync_ack_status({"success": True, "ack": {"qc_status": "accepted"}}),
            "accepted",
        )
        self.assertIsNone(_policy_sync_ack_status({"success": True}))

    def test_daily_execution_activity_excludes_preflight_and_qc_rejected_rows(self):
        text = Path("services/execution_log_store.py").read_text()

        self.assertIn('qc_status in {"not_sent", "rejected"}', text)
        self.assertIn('qc_status in {"submitted", "accepted", "timeout_no_ack"}', text)
        self.assertIn("if not _counts_toward_daily_turnover(row):", text)

    def test_executor_does_not_overwrite_duplicate_command_log(self):
        text = Path("agents/executor.py").read_text()

        self.assertIn('"command_id_idempotent" not in', text)
        self.assertIn("record_preflight_block", text)

    def test_qc_ack_model_preserves_policy_mismatch_metadata(self):
        text = Path("api/execution.py").read_text()

        self.assertIn("policy_version: str | None", text)
        self.assertIn("policy_mismatch: bool = False", text)
        self.assertIn("actual_target_weights: dict[str, float] | None", text)
        self.assertIn("actual_holdings_weights: dict[str, float] | None", text)
        self.assertIn("order_summary: dict[str, Any] | None", text)
        self.assertIn("fill_summary: dict[str, Any] | None", text)
        self.assertIn("account_state: dict[str, Any] | None", text)

    def test_executor_surfaces_transaction_cost_gate_summary(self):
        summary = format_transaction_cost_gate_summary({
            "mode": "observe",
            "broker": "IBKR",
            "summary": {
                "total_cost_drag": 0.000914,
                "min_edge_to_cost_ratio": 1.2,
                "warning_count": 2,
                "cost_model": "IBKR_return_drag_v1",
            }
        })

        self.assertIn("Cost gate: observe IBKR", summary)
        self.assertIn("drag 0.091%", summary)
        self.assertIn("min edge/cost 1.20x", summary)
        self.assertIn("warnings 2", summary)


if __name__ == "__main__":
    unittest.main()
