import unittest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch

from services.execution_preflight import (
    DEFAULT_COMMAND_PREFLIGHT_CONFIG,
    _command_class_from_metrics,
    _command_config,
    _daily_command_limit,
    _daily_turnover_limit,
    _target_fingerprint_from_execution_row,
    _target_weights_from_execution_row,
    _policy_alignment_ok,
    _policy_sync_ack_status,
    command_weight_delta_metrics,
    format_command_preflight_blockers,
    preflight_execution_command,
    preflight_execution_weights,
)
from services.execution_log_store import summarize_execution_activity_rows
from services.transaction_cost_gate import format_transaction_cost_gate_summary


class ExecutorPreflightTests(unittest.TestCase):
    def test_default_execution_command_limits_are_widened_for_live_calibration(self):
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["max_daily_commands"], 12)
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["max_gross_turnover_per_day"], 1.50)
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["risk_reduce_reserved_commands"], 4)
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["risk_reduce_gross_turnover_per_day"], 0.25)
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["max_buy_delta"], 0.15)
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["max_buy_delta_per_day"], 0.10)
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["shadow_real_money_max_buy_delta_per_day"], 0.03)
        self.assertEqual(DEFAULT_COMMAND_PREFLIGHT_CONFIG["max_sell_delta"], 0.20)

    def test_config_migration_relaxes_production_execution_command_limits(self):
        sql = Path("db/migrations/20260605_relax_execution_command_canary_limits.sql").read_text()

        self.assertIn('"max_daily_commands": 12', sql)
        self.assertIn('"max_gross_turnover_per_day": 1.50', sql)
        self.assertIn('"risk_reduce_reserved_commands": 4', sql)
        self.assertIn('"risk_reduce_gross_turnover_per_day": 0.25', sql)
        self.assertGreaterEqual(sql.count('"max_buy_delta": 0.15'), 2)
        self.assertGreaterEqual(sql.count('"max_buy_delta_per_day": 0.10'), 2)
        self.assertGreaterEqual(sql.count('"shadow_real_money_max_buy_delta_per_day": 0.03'), 2)
        self.assertGreaterEqual(sql.count('"max_sell_delta": 0.20'), 2)
        self.assertIn("ON CONFLICT (key) DO UPDATE", sql)

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
        send_pos = text.index("result = await send_setweights_command")

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
        send_pos = text.index("result = await send_setweights_command")

        self.assertLess(active_pos, preflight_pos)
        self.assertLess(active_pos, send_pos)
        self.assertIn("record_active_execution_wait", text)
        self.assertIn('"execution_status": "deferred_by_active_execution"', text)
        self.assertIn("active_execution_wait", text)
        self.assertIn("Will resume after reconciliation", text)

    def test_executor_dedupes_recent_same_target_after_preflight_before_send(self):
        text = Path("agents/executor.py").read_text()
        broker_pos = text.index("broker_order_filter = await apply_broker_order_filter")
        preflight_pos = text.index("command_preflight = await preflight_execution_command")
        dedupe_pos = text.index("same_target_dedupe = await check_recent_same_target_dedupe")
        send_pos = text.index("result = await send_setweights_command")

        self.assertLess(broker_pos, preflight_pos)
        self.assertLess(preflight_pos, dedupe_pos)
        self.assertLess(dedupe_pos, send_pos)
        self.assertIn("policy_version=policy_version", text)
        self.assertIn('command_type="SetWeights"', text)
        self.assertIn("record_recent_same_target_dedupe", text)
        self.assertIn('"execution_status": "deduped"', text)
        self.assertIn("No command sent to QC", text)
        self.assertIn("same target fingerprint", text)
        self.assertIn('command_preflight["broker_order_filter"] = broker_order_filter', text)

    def test_executor_filters_broker_micro_orders_before_active_gate(self):
        text = Path("agents/executor.py").read_text()

        broker_pos = text.index("broker_order_filter = await apply_broker_order_filter")
        active_pos = text.index("active_execution_gate = evaluate_active_execution_gate")
        preflight_pos = text.index("command_preflight = await preflight_execution_command")
        send_pos = text.index("result = await send_setweights_command")

        self.assertLess(broker_pos, active_pos)
        self.assertLess(broker_pos, preflight_pos)
        self.assertLess(broker_pos, send_pos)
        self.assertIn('"execution_status": "skipped"', text)
        self.assertIn("broker_order_filter_no_executable_delta", text)
        self.assertIn("broker order filter left no executable delta", text)

    def test_executor_budget_only_preflight_block_can_dedupe_before_blocking(self):
        text = Path("agents/executor.py").read_text()
        body = text[text.index("command_preflight = await preflight_execution_command") :]
        budget_pos = body.index("budget_only_blockers = blockers")
        dedupe_pos = body.index("same_target_dedupe = await check_recent_same_target_dedupe")
        block_record_pos = body.index("await record_preflight_block")

        self.assertLess(budget_pos, block_record_pos)
        self.assertLess(dedupe_pos, block_record_pos)
        self.assertIn('{"daily_command_count_ok", "daily_gross_turnover_ok"}', body)

    def test_executor_telegram_distinguishes_async_qc_lifecycle_states(self):
        executor_text = Path("agents/executor.py").read_text()
        message_text = Path("services/operator_messages.py").read_text()

        self.assertIn("def _format_qc_lifecycle_ack_message", executor_text)
        self.assertIn("format_qc_lifecycle_ack_message(command_id, qc_ack)", executor_text)
        self.assertIn("QC_OWNERSHIP_STATUSES", executor_text)
        self.assertIn("No-op reconciled", message_text)
        self.assertIn("Actual orders", message_text)
        self.assertIn("QC submitted orders", message_text)
        self.assertIn("Partial execution", message_text)
        self.assertIn("Reconciliation drift", message_text)
        self.assertIn("ownership not confirmed", message_text)
        self.assertIn("def _format_lifecycle_context", message_text)
        self.assertIn("Lifecycle: ", message_text)

    def test_telegram_confirm_uses_policy_alignment_not_policy_sync(self):
        text = Path("services/telegram_commands.py").read_text()
        confirm_body = text[text.index("async def _cmd_confirm") : text.index("async def _load_manual_confirm_policy_alignment")]
        alignment_pos = confirm_body.index("_load_manual_confirm_policy_alignment")
        send_pos = confirm_body.index("result = await send_setweights_command")

        self.assertLess(alignment_pos, send_pos)
        self.assertNotIn("tool_send_policy_sync", confirm_body)
        self.assertNotIn("create_or_update_policy_sync_log", confirm_body)
        self.assertNotIn("wait_for_qc_ack_detail(policy_sync_id", confirm_body)
        self.assertIn("No recent account state policy alignment", confirm_body)
        self.assertIn("Deploy/sync the QC compiled policy before confirming", confirm_body)

    def test_telegram_confirm_revalidates_proposal_and_active_execution_before_token_consumption(self):
        text = Path("services/telegram_commands.py").read_text()
        confirm_body = text[text.index("async def _cmd_confirm") : text.index("async def _load_manual_confirm_policy_alignment")]

        relevance_pos = confirm_body.index("validate_proposal_still_relevant")
        broker_pos = confirm_body.index("broker_order_filter = await apply_broker_order_filter")
        active_pos = confirm_body.index("active_execution_gate = evaluate_active_execution_gate")
        preflight_pos = confirm_body.index("command_preflight = await preflight_execution_command")
        dedupe_pos = confirm_body.index("same_target_dedupe = await check_recent_same_target_dedupe")
        token_pos = confirm_body.index("verify = await tool_verify_approval_token")
        send_pos = confirm_body.index("result = await send_setweights_command")

        self.assertLess(relevance_pos, token_pos)
        self.assertLess(broker_pos, active_pos)
        self.assertLess(active_pos, token_pos)
        self.assertLess(preflight_pos, token_pos)
        self.assertLess(dedupe_pos, token_pos)
        self.assertLess(token_pos, send_pos)
        self.assertIn("record_active_execution_wait", confirm_body)
        self.assertIn("record_recent_same_target_dedupe", confirm_body)
        self.assertIn("target_fingerprint=target_fingerprint", confirm_body)
        self.assertIn("Proposal invalidated before confirmation", confirm_body)
        self.assertIn("active execution is still pending reconciliation", confirm_body)
        self.assertIn('command_preflight["broker_order_filter"] = broker_order_filter', confirm_body)

    def test_timed_out_proposal_auto_execution_is_disabled(self):
        text = Path("services/proposal.py").read_text()
        timeout_body = text[text.index("async def check_and_handle_timeout") : text.index("async def validate_proposal_still_relevant")]

        self.assertNotIn("tool_send_weight_command", text)
        self.assertNotIn("executed_timeout_auto", timeout_body)
        self.assertIn("skipped_timeout_auto_exec_disabled", timeout_body)
        self.assertIn("auto-execution is disabled", timeout_body)
        self.assertIn("No command sent to QC", timeout_body)

    def test_setweights_command_carries_policy_version_only(self):
        text = Path("tools/qc_tools.py").read_text()

        self.assertIn("policy_version = inp.get(\"policy_version\") or policy_snapshot().get(\"version\")", text)
        self.assertIn('"policy_version": policy_version', text)
        self.assertIn('"target_fingerprint": inp.get("target_fingerprint")', text)
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

    def test_emergency_auto_liquidate_is_fail_closed_helper(self):
        text = Path("api/webhook.py").read_text()

        self.assertIn("def _emergency_auto_liquidate_enabled", text)
        self.assertIn("settings.emergency_auto_liquidate is True", text)
        self.assertNotIn("if settings.emergency_auto_liquidate:", text)

    def test_setweights_low_level_sender_is_single_controlled_service(self):
        allowed = {
            Path("services/qc_command_sender.py"),
            Path("tools/qc_tools.py"),
        }
        offenders = []
        for path in list(Path("agents").rglob("*.py")) + list(Path("services").rglob("*.py")) + list(Path("api").rglob("*.py")) + list(Path("cron").rglob("*.py")):
            text = path.read_text()
            if "tool_send_weight_command" in text and path not in allowed:
                offenders.append(str(path))

        self.assertEqual([], offenders)
        sender = Path("services/qc_command_sender.py").read_text()
        self.assertIn("async def send_setweights_command", sender)
        self.assertIn("tool_send_weight_command", sender)

        registry = Path("tools/registry.py").read_text()
        self.assertNotIn('"send_weight_command"', registry)
        self.assertNotIn('"emergency_liquidate"', registry)

    def test_executor_requires_final_risk_validation_before_qc_command(self):
        text = Path("agents/executor.py").read_text()
        final_pos = text.index("Final risk validation missing or failed")
        send_pos = text.index("result = await send_setweights_command")

        self.assertLess(final_pos, send_pos)
        self.assertIn("blocked_by_final_risk_validation", text)

    def test_telegram_confirm_requires_final_risk_validation(self):
        text = Path("services/telegram_commands.py").read_text()
        final_pos = text.index("Final risk validation missing or failed")
        send_pos = text.index("result = await send_setweights_command")

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

    def test_daily_execution_activity_tracks_realized_buy_delta(self):
        def row(status: str, qc_status: str, buy_delta: float, sell_delta: float = 0.0):
            return type(
                "Row",
                (),
                {
                    "command_type": "weight_adjustment",
                    "status": status,
                    "qc_status": qc_status,
                    "qc_response": {},
                    "command_payload": {
                        "command_preflight": {
                            "metrics": {
                                "buy_delta": buy_delta,
                                "sell_delta": sell_delta,
                                "gross_turnover": round((buy_delta + sell_delta) / 2.0, 6),
                            }
                        }
                    },
                },
            )()

        summary = summarize_execution_activity_rows([
            row("accepted", "reconciled", 0.02),
            row("accepted", "rejected", 0.50),
            row("accepted", "filled", 0.01, 0.02),
        ])

        self.assertEqual(summary["command_count"], 2)
        self.assertEqual(summary["buy_delta"], 0.03)
        self.assertEqual(summary["sell_delta"], 0.02)
        self.assertEqual(summary["gross_turnover"], 0.025)

    def test_same_target_dedupe_config_defaults_are_stable(self):
        config = _command_config({})

        self.assertEqual(config["recent_same_target_dedupe_minutes"], 5)
        self.assertEqual(config["recent_same_target_dedupe_tolerance"], 0.005)
        self.assertEqual(config["max_buy_delta_per_day"], 0.10)
        self.assertEqual(config["shadow_real_money_max_buy_delta_per_day"], 0.03)

    def test_preflight_blocks_projected_daily_buy_delta(self):
        async def run():
            with (
                patch(
                    "services.execution_log_store.command_submission_state",
                    new=AsyncMock(return_value={
                        "command_id_exists": False,
                        "analysis_id_submitted": False,
                    }),
                ),
                patch(
                    "services.execution_log_store.summarize_today_execution_activity",
                    new=AsyncMock(return_value={
                        "command_count": 1,
                        "gross_turnover": 0.045,
                        "buy_delta": 0.09,
                        "sell_delta": 0.0,
                    }),
                ),
            ):
                return await preflight_execution_command(
                    command_id="analysis_999",
                    analysis_id=999,
                    target_weights={"SPY": 0.02, "CASH": 0.98},
                    current_weights={"CASH": 1.0},
                    policy_version="sprint8a",
                    policy_sync_result={"ack_status": "accepted"},
                    policy_alignment_result={"aligned": True},
                    config={
                        "max_buy_delta_per_day": 0.10,
                        "shadow_real_money_max_buy_delta_per_day": 0.03,
                    },
                )

        result = asyncio.run(run())

        self.assertFalse(result["allowed"])
        self.assertIn("daily_buy_delta_ok", result["blockers"])
        check = result["checks"]["daily_buy_delta_ok"]
        self.assertEqual(check["actual"], 0.11)
        self.assertEqual(check["threshold"], 0.10)
        self.assertEqual(check["today_used"], 0.09)
        self.assertEqual(check["command_delta"], 0.02)
        self.assertFalse(check["shadow_real_money_would_pass"])

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

    def test_same_target_dedupe_reconstructs_fingerprint_for_legacy_rows(self):
        row = type(
            "Row",
            (),
            {
                "target_fingerprint": None,
                "command_type": "weight_adjustment",
                "policy_version": "sprint8a",
                "command_id": "analysis_242",
                "correlation_id": "analysis_242",
                "analysis_id": 242,
                "qc_response": {},
                "command_payload": {
                    "sent_weights": {"SPY": 0.1001},
                    "policy_version": "sprint8a",
                },
            },
        )()

        result = _target_fingerprint_from_execution_row(row, tolerance=0.005)

        self.assertEqual(result["command_type"], "SetWeights")
        self.assertEqual(result["policy_version"], "sprint8a")
        self.assertEqual(result["normalized_weights"]["SPY"], 0.1)
        self.assertEqual(result["source"], "reconstructed_from_execution_log")

    def test_same_target_dedupe_uses_stored_fingerprint_when_tolerance_matches(self):
        row = type(
            "Row",
            (),
            {
                "target_fingerprint": "abc123",
                "command_payload": {
                    "target_fingerprint": {"dedupe_tolerance": 0.005},
                },
            },
        )()

        result = _target_fingerprint_from_execution_row(row, tolerance=0.005)

        self.assertEqual(result["fingerprint"], "abc123")
        self.assertEqual(result["source"], "execution_log.target_fingerprint")

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
