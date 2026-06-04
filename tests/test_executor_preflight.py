import unittest
from pathlib import Path

from services.execution_preflight import (
    _command_class_from_metrics,
    _command_config,
    _daily_command_limit,
    _daily_turnover_limit,
    _target_weights_from_execution_row,
    _policy_alignment_ok,
    _policy_sync_ack_status,
    command_weight_delta_metrics,
    format_command_preflight_blockers,
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

    def test_executor_requires_account_guard_policy_alignment_before_setweights(self):
        text = Path("agents/executor.py").read_text()
        alignment_pos = text.index("policy_alignment = policy_alignment_from_account_guard")
        send_pos = text.index("result = await tool_send_weight_command")

        self.assertLess(alignment_pos, send_pos)
        self.assertNotIn("tool_send_policy_sync", text)
        self.assertNotIn("create_or_update_policy_sync_log", text)
        self.assertNotIn("wait_for_qc_ack_detail(policy_sync_id", text)
        self.assertIn("policy_alignment_not_confirmed", text)
        self.assertIn("Deploy/sync the QC compiled policy before trading", text)
        self.assertIn("No command sent to QC", text)

    def test_executor_checks_active_execution_before_command_preflight_and_send(self):
        text = Path("agents/executor.py").read_text()
        active_pos = text.index("active_execution_gate = evaluate_active_execution_gate")
        preflight_pos = text.index("command_preflight = await preflight_execution_command")
        send_pos = text.index("result = await tool_send_weight_command")

        self.assertLess(active_pos, preflight_pos)
        self.assertLess(active_pos, send_pos)
        self.assertIn("record_active_execution_wait", text)
        self.assertIn('"execution_status": "deferred_by_active_execution"', text)
        self.assertIn("active_execution_wait", text)
        self.assertIn("Will resume after reconciliation", text)

    def test_executor_dedupes_recent_same_target_after_preflight_before_send(self):
        text = Path("agents/executor.py").read_text()
        preflight_pos = text.index("command_preflight = await preflight_execution_command")
        dedupe_pos = text.index("same_target_dedupe = await check_recent_same_target_dedupe")
        send_pos = text.index("result = await tool_send_weight_command")

        self.assertLess(preflight_pos, dedupe_pos)
        self.assertLess(dedupe_pos, send_pos)
        self.assertIn("record_recent_same_target_dedupe", text)
        self.assertIn('"execution_status": "deduped"', text)
        self.assertIn("No command sent to QC", text)

    def test_executor_telegram_distinguishes_async_qc_lifecycle_states(self):
        text = Path("agents/executor.py").read_text()

        self.assertIn("def _format_qc_lifecycle_ack_message", text)
        self.assertIn("QC_OWNERSHIP_STATUSES", text)
        self.assertIn("Accepted is not reconciled", text)
        self.assertIn("No-op reconciled", text)
        self.assertIn("Actual orders", text)
        self.assertIn("QC submitted orders", text)
        self.assertIn("Partial execution", text)
        self.assertIn("Reconciliation drift", text)
        self.assertIn("Lifecycle will reconcile from heartbeat", text)

    def test_telegram_confirm_uses_policy_alignment_not_policy_sync(self):
        text = Path("services/telegram_commands.py").read_text()
        confirm_body = text[text.index("async def _cmd_confirm") : text.index("async def _load_manual_confirm_policy_alignment")]
        alignment_pos = confirm_body.index("_load_manual_confirm_policy_alignment")
        send_pos = confirm_body.index("result = await tool_send_weight_command")

        self.assertLess(alignment_pos, send_pos)
        self.assertNotIn("tool_send_policy_sync", confirm_body)
        self.assertNotIn("create_or_update_policy_sync_log", confirm_body)
        self.assertNotIn("wait_for_qc_ack_detail(policy_sync_id", confirm_body)
        self.assertIn("No recent account state policy alignment", confirm_body)
        self.assertIn("Deploy/sync the QC compiled policy before confirming", confirm_body)

    def test_setweights_command_carries_policy_version_only(self):
        text = Path("tools/qc_tools.py").read_text()

        self.assertIn("policy_version = inp.get(\"policy_version\") or policy_snapshot().get(\"version\")", text)
        self.assertIn('"policy_version": policy_version', text)
        self.assertNotIn('"policy": policy', text)

    def test_policy_sync_command_has_json_fallback_contract(self):
        text = Path("tools/qc_tools.py").read_text()

        self.assertIn("POLICY_SYNC_PROTOCOL_VERSION", text)
        self.assertIn("build_policy_sync_command_payload", text)
        self.assertIn('"payload_json": json.dumps', text)
        self.assertIn('"roles": safe_payload.get("roles") or {}', text)
        self.assertIn('"caps": safe_payload.get("caps") or {}', text)

    def test_cancel_orders_control_command_payload_exists(self):
        text = Path("tools/qc_tools.py").read_text()

        self.assertIn("async def tool_send_cancel_orders_command", text)
        self.assertIn('"target": "CancelOrders"', text)
        self.assertIn('"target_command_id": target_command_id', text)
        self.assertIn('"reason": inp.get("reason") or "operator_cancel_orders"', text)

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

    def test_telegram_operator_commands_for_async_execution_lifecycle_exist(self):
        text = Path("services/telegram_commands.py").read_text()

        self.assertIn('cmd == "/force_reconcile"', text)
        self.assertIn('cmd == "/cancel_orders"', text)
        self.assertIn("async def _cmd_force_reconcile", text)
        self.assertIn("async def _cmd_cancel_orders", text)
        self.assertIn("force_reconcile_command", text)
        self.assertIn("tool_send_cancel_orders_command", text)
        self.assertIn("record_cancel_orders_requested", text)
        self.assertIn("Wait for QC heartbeat reconciliation", text)

    def test_rejected_pipeline_notification_cooldown_requires_successful_send(self):
        text = Path("services/pipeline.py").read_text()

        self.assertIn("notify_result = await tool_send_telegram", text)
        self.assertIn("if bool(notify_result.get(\"sent\"))", text)
        self.assertIn("await _mark_rejected_pipeline_notified", text)
        notify_pos = text.index("notify_result = await tool_send_telegram")
        mark_pos = text.index("await _mark_rejected_pipeline_notified")
        self.assertLess(notify_pos, mark_pos)

    def test_executor_active_execution_block_surfaces_stale_details(self):
        text = Path("agents/executor.py").read_text()

        self.assertIn("stale_active_execution", text)
        self.assertIn("Stale active execution:", text)
        self.assertIn("threshold=", text)
        self.assertIn("operator_action", text)
        self.assertIn("active_execution_gate.get('status')", text)

    def test_command_delta_metrics_split_buy_sell_and_gross_turnover(self):
        result = command_weight_delta_metrics(
            {"SPY": 0.25, "QQQ": 0.10, "CASH": 0.65},
            {"SPY": 0.10, "XLK": 0.08, "CASH": 0.82},
        )

        self.assertEqual(result["buy_delta"], 0.25)
        self.assertEqual(result["sell_delta"], 0.08)
        self.assertEqual(result["gross_turnover"], 0.165)

    def test_command_preflight_blockers_are_operator_readable(self):
        text = format_command_preflight_blockers({
            "blockers": ["daily_command_count_ok", "daily_gross_turnover_ok"],
            "checks": {
                "daily_command_count_ok": {"actual": 3, "threshold": 3},
                "daily_gross_turnover_ok": {"actual": 0.535, "threshold": 0.50},
            },
        })

        self.assertIn("daily command cap exceeded: actual=3, threshold=3", text)
        self.assertIn("daily turnover cap exceeded: actual=53.50%, threshold=50.00%", text)
        self.assertIn("(daily_command_count_ok)", text)

    def test_risk_reduce_command_gets_reserved_daily_budget(self):
        cfg = {
            "max_daily_commands": 2,
            "max_gross_turnover_per_day": 0.10,
            "risk_reduce_reserved_commands": 1,
            "risk_reduce_gross_turnover_per_day": 0.05,
        }

        command_class = _command_class_from_metrics({
            "buy_delta": 0.0,
            "sell_delta": 0.0806,
            "gross_turnover": 0.0403,
        })

        self.assertEqual(command_class, "risk_reduce")
        self.assertEqual(_daily_command_limit(cfg, command_class), 3)
        self.assertAlmostEqual(_daily_turnover_limit(cfg, command_class), 0.15)

    def test_ordinary_command_does_not_get_risk_reduce_reserve(self):
        cfg = {
            "max_daily_commands": 2,
            "max_gross_turnover_per_day": 0.10,
            "risk_reduce_reserved_commands": 1,
            "risk_reduce_gross_turnover_per_day": 0.05,
        }

        command_class = _command_class_from_metrics({
            "buy_delta": 0.01,
            "sell_delta": 0.0806,
            "gross_turnover": 0.0453,
        })

        self.assertEqual(command_class, "ordinary_rebalance")
        self.assertEqual(_daily_command_limit(cfg, command_class), 2)
        self.assertEqual(_daily_turnover_limit(cfg, command_class), 0.10)

    def test_risk_reduce_preflight_message_shows_reserve_applied(self):
        text = format_command_preflight_blockers({
            "blockers": ["daily_command_count_ok", "daily_gross_turnover_ok"],
            "checks": {
                "daily_command_count_ok": {
                    "actual": 3,
                    "threshold": 3,
                    "bucket": "risk_reduce",
                    "reserve_applied": 1,
                },
                "daily_gross_turnover_ok": {
                    "actual": 0.155,
                    "threshold": 0.15,
                    "bucket": "risk_reduce",
                    "reserve_applied": 0.05,
                },
            },
        })

        self.assertIn("reserve_applied=1", text)
        self.assertIn("reserve_applied=5.00%", text)

    def test_policy_sync_success_requires_qc_ack_status(self):
        self.assertEqual(
            _policy_sync_ack_status({"success": True, "ack": {"qc_status": "accepted"}}),
            "accepted",
        )
        self.assertIsNone(_policy_sync_ack_status({"success": True}))

    def test_policy_alignment_helper_accepts_account_guard_confirmation(self):
        self.assertTrue(_policy_alignment_ok({"aligned": True, "source": "account_state_guard"}))
        self.assertFalse(_policy_alignment_ok({"aligned": False}))
        self.assertFalse(_policy_alignment_ok(None))

    def test_daily_execution_activity_excludes_preflight_and_qc_rejected_rows(self):
        text = Path("services/execution_log_store.py").read_text()

        self.assertIn('qc_status in {"not_sent", "rejected", "timeout_no_execution_confirmed"}', text)
        self.assertIn('"orders_submitted"', text)
        self.assertIn('"partial"', text)
        self.assertIn('"reconciled"', text)
        self.assertIn('"timeout_no_ack"', text)
        self.assertIn("if not _counts_toward_daily_turnover(row):", text)

    def test_same_target_dedupe_config_defaults_are_stable(self):
        config = _command_config({})

        self.assertEqual(config["recent_same_target_dedupe_minutes"], 5)
        self.assertEqual(config["recent_same_target_dedupe_tolerance"], 0.005)

    def test_same_target_dedupe_extracts_sent_weights_as_fallback(self):
        row = type(
            "Row",
            (),
            {
                "qc_response": {},
                "command_payload": {
                    "sent_weights": {"SPY": 0.1},
                    "proposed_weights": {"SPY": 0.2},
                },
            },
        )()

        self.assertEqual(_target_weights_from_execution_row(row), {"SPY": 0.1})

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

    def test_qc_ack_ingests_account_snapshot_when_present(self):
        text = Path("api/execution.py").read_text()

        self.assertIn("ingest_execution_ack_snapshot", text)
        self.assertIn("ack.account_state", text)
        self.assertIn("holdings_weights=ack.actual_holdings_weights", text)
        self.assertIn("target_weights=ack.actual_target_weights", text)

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
