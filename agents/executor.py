# agents/executor.py
import logging

from tools.qc_tools import tool_send_weight_command
from tools.notify_tools import tool_send_telegram
from tools.db_tools import tool_verify_approval_token
from services.execution_audit import build_execution_audit_payload
from services.execution_ack_tracker import wait_for_qc_ack_detail
from services.execution_log_store import (
    create_or_update_submitted_log,
    record_active_execution_wait,
    record_preflight_block,
    record_recent_same_target_dedupe,
)
from services.execution_lifecycle import (
    evaluate_active_execution_gate,
    load_active_execution_command,
)
from services.execution_policy import policy_snapshot
from services.execution_preflight import (
    check_recent_same_target_dedupe,
    format_command_preflight_blockers,
    preflight_execution_command,
    preflight_execution_weights,
)
from services.policy_alignment import policy_alignment_from_account_guard
from services.transaction_cost_gate import format_transaction_cost_gate_summary

logger = logging.getLogger("qc_fastapi_2.executor")


def _command_label(command_id: str) -> str:
    return str(command_id or "unknown")


QC_OWNERSHIP_STATUSES = {
    "accepted",
    "orders_submitted",
    "partial",
    "filled",
    "reconciled",
    "reconciliation_drift",
    "failed_no_fill",
    "superseded",
}


def _format_qc_lifecycle_ack_message(command_id: str, qc_ack: dict) -> str:
    status = str((qc_ack or {}).get("qc_status") or "").lower().strip()
    response = (qc_ack or {}).get("qc_response") if isinstance((qc_ack or {}).get("qc_response"), dict) else {}
    order_summary = response.get("order_summary") if isinstance(response.get("order_summary"), dict) else {}
    label = _command_label(command_id)
    if status == "accepted":
        return (
            f"✅ QC accepted ownership `{label}`\n"
            f"Execution state: {response.get('execution_state') or 'accepted'}\n"
            "Accepted is not reconciled; awaiting fill/account reconciliation."
        )
    if status == "orders_submitted":
        return (
            f"📤 QC submitted orders `{label}`\n"
            f"Orders submitted: {order_summary.get('submitted_order_count', 'n/a')}\n"
            "Awaiting fills and heartbeat reconciliation."
        )
    if status == "partial":
        return (
            f"⏳ Partial execution `{label}`\n"
            f"Filled: {order_summary.get('filled_order_count', 'n/a')}/"
            f"{order_summary.get('submitted_order_count', 'n/a')} orders\n"
            f"Open orders: {order_summary.get('open_order_count_after', order_summary.get('open_order_count', 'n/a'))}"
        )
    if status == "filled":
        return f"✅ QC reports orders filled `{label}`\nAwaiting account reconciliation."
    if status == "reconciled":
        return f"✅ Reconciled `{label}`\nActual holdings match target within tolerance."
    if status == "reconciliation_drift":
        return (
            f"⚠️ Reconciliation drift `{label}`\n"
            "Actual holdings differ from target. Next cycle will use actual holdings as baseline."
        )
    if status == "failed_no_fill":
        return f"⚠️ QC accepted `{label}` but reports no fill. Positions should be verified from account truth."
    if status == "superseded":
        return f"ℹ️ Command `{label}` was superseded by a later override."
    return (
        f"⚠️ QC ACK timeout `{label}`\n"
        "No fast QC ownership confirmation within the wait window. "
        "Lifecycle will reconcile from heartbeat; do not assume positions changed."
    )


async def run_executor_async(
    pipeline_context: dict,
    risk_out:         dict,
    analysis_id:      int,
) -> dict:
    """
    EXECUTOR does not use LLM — deterministic logic only.
    Three gates: risk approved / valid token / weight sanity.

    target_weights / rebalance_actions / estimated_cost_pct all come from
    risk_out (Stage 4 Risk Manager final execution plan).
    """
    auth_mode = pipeline_context.get("auth_mode", "SEMI_AUTO")

    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — skipping execution")
        return {
            "execution_status": "skipped",
            "execution_audit": build_execution_audit_payload(
                action_status="skipped",
                proposed_weights=risk_out.get("target_weights") or {},
                reason="manual_mode",
            ),
        }

    # Gate 1: risk must approve
    if not risk_out.get("approved"):
        reasons = risk_out.get("rejection_reasons", [])
        msg = "❌ Risk rejected execution\n" + "\n".join(str(r) for r in reasons)
        await tool_send_telegram({"text": msg})
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=risk_out.get("target_weights") or {},
                reason="rejected_by_risk",
            ),
        }

    final_validation = risk_out.get("final_validation") or {}
    if not final_validation or not final_validation.get("approved"):
        await tool_send_telegram({
            "text": "⛔ Final risk validation missing or failed — no command sent to QC."
        })
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=risk_out.get("target_weights") or {},
                reason="blocked_by_final_risk_validation",
            ),
            "final_validation": final_validation,
        }

    # Gate 2: token
    token = risk_out.get("approval_token")
    if not token:
        await tool_send_telegram({"text": "⚠️ approval_token missing — aborting execution"})
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=risk_out.get("target_weights") or {},
                reason="aborted_no_token",
            ),
        }

    verify = await tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        reason = verify.get("reason", "unknown")
        await tool_send_telegram({"text": f"⚠️ Invalid token ({reason}) — aborting execution"})
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=risk_out.get("target_weights") or {},
                reason=f"aborted_token_{reason}",
            ),
        }

    # Final target weights
    weights = risk_out.get("target_weights", {}) or {}
    if not weights:
        await tool_send_telegram({"text": "⚠️ target_weights empty — aborting execution"})
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights={},
                reason="aborted_no_weights",
            ),
        }
    execution_throttle = risk_out.get("execution_throttle") or {}
    desired_weights = execution_throttle.get("desired_target_weights") or weights

    # Gate 3: weight check (excluding CASH)
    equity_w = {k: v for k, v in weights.items() if k != "CASH"}
    w_sum = sum(float(v) for v in equity_w.values())
    if w_sum > 1.01:
        await tool_send_telegram({"text": f"⚠️ Equity weights sum {w_sum:.3f} > 1.0 — abort"})
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=desired_weights,
                reason="aborted_weight_overflow",
            ),
        }

    preflight = preflight_execution_weights(weights)
    if not preflight["allowed"]:
        cap_lines = "\n".join(
            f"  {row['ticker']}: {float(row['weight']):.2%} - {row['reason']}"
            for row in preflight["cap_violations"]
        ) or "  none"
        await tool_send_telegram(
            {
                "text": (
                    f"⛔ Executor preflight blocked `{analysis_id}`\n"
                    "final_policy_cap stage failed to enforce execution limits. "
                    "This is a system bug, not a business decision. Do not retry without investigation.\n"
                    f"Policy: {preflight['policy_version']}\n"
                    f"Cap violations:\n{cap_lines}\n"
                    f"Group violations: {preflight['group_violations']}\n"
                    "No command sent to QC."
                )
            }
        )
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=desired_weights,
                reason="blocked_by_execution_policy",
            ),
            "preflight": preflight,
        }

    # Send weights
    command_id = f"analysis_{analysis_id}"
    policy = policy_snapshot()
    policy_version = policy.get("version")
    policy_sync = None
    policy_alignment = policy_alignment_from_account_guard(
        pipeline_context.get("account_state_guard") or {},
        expected_policy_version=str(policy_version or ""),
    )
    if not policy_alignment.get("aligned"):
        await tool_send_telegram({
            "text": (
                f"⛔ Executor policy alignment assertion failed before `{_command_label(command_id)}`\n"
                f"expected={policy_alignment.get('expected_policy_version')} "
                f"actual={policy_alignment.get('actual_policy_version')} "
                f"guard_status={policy_alignment.get('guard_status')} "
                f"blockers={policy_alignment.get('guard_blockers')}\n"
                "No command sent to QC. Deploy/sync the QC compiled policy before trading."
            )
        })
        return {
            "execution_status": "failed",
            "error": "policy_alignment_not_confirmed",
            "policy_alignment": policy_alignment,
            "execution_audit": build_execution_audit_payload(
                action_status="failed",
                proposed_weights=desired_weights,
                command_id=command_id,
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason="policy_alignment_not_confirmed",
            ),
        }

    active_execution = await load_active_execution_command()
    active_execution_gate = evaluate_active_execution_gate(
        target_weights=weights,
        active_execution=active_execution,
        config=pipeline_context.get("execution_lifecycle_config") or {},
    )
    if active_execution_gate.get("would_defer"):
        logger.warning(
            "[executor] active execution gate %s | active=%s classification=%s",
            active_execution_gate.get("status"),
            active_execution_gate.get("active_command_id"),
            active_execution_gate.get("classification"),
        )
    if not active_execution_gate.get("allowed"):
        stale = active_execution_gate.get("stale_active_execution") or {}
        stale_text = ""
        if stale.get("is_stale"):
            stale_text = (
                "\nStale active execution: "
                f"{stale.get('reason')} | elapsed={float(stale.get('elapsed_minutes') or 0):.1f}m "
                f"threshold={stale.get('threshold_minutes')}m | action={stale.get('operator_action')}"
            )
        await record_active_execution_wait(
            command_id=command_id,
            analysis_id=analysis_id,
            target_weights=weights,
            active_execution_gate=active_execution_gate,
            policy_version=policy_version,
        )
        await tool_send_telegram({
            "text": (
                f"⏳ Rebalance skipped: active command "
                f"`{_command_label(active_execution_gate.get('active_command_id') or 'unknown')}` "
                "still executing\n"
                f"Open orders: {active_execution_gate.get('open_order_count')}\n"
                f"Status: {active_execution_gate.get('status')} "
                f"({active_execution_gate.get('classification')})\n"
                "Will resume after reconciliation."
                f"{stale_text}"
            )
        })
        return {
            "execution_status": "deferred_by_active_execution",
            "command_id": command_id,
            "active_execution_gate": active_execution_gate,
            "policy_version": policy_version,
            "policy_alignment": policy_alignment,
            "execution_audit": build_execution_audit_payload(
                action_status="skipped",
                proposed_weights=desired_weights,
                command_id=command_id,
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason="active_execution_wait",
            ),
        }

    command_preflight = await preflight_execution_command(
        command_id=command_id,
        analysis_id=analysis_id,
        target_weights=weights,
        current_weights=(final_validation.get("current_weights") or {}),
        policy_version=policy_version,
        policy_sync_result=policy_sync,
        policy_alignment_result=policy_alignment,
        config=pipeline_context.get("execution_command_config") or {},
    )
    if not command_preflight.get("allowed"):
        if "command_id_idempotent" not in (command_preflight.get("blockers") or []):
            await record_preflight_block(
                command_id=command_id,
                analysis_id=analysis_id,
                target_weights=weights,
                preflight_result=command_preflight,
                policy_version=policy_version,
                policy_sync_result=policy_sync,
            )
        await tool_send_telegram({
            "text": (
                f"⛔ Command preflight blocked `{_command_label(command_id)}`\n"
                f"{format_command_preflight_blockers(command_preflight)}\n"
                "No command sent to QC."
            )
        })
        return {
            "execution_status": "rejected",
            "command_id": command_id,
            "preflight": command_preflight,
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=desired_weights,
                command_id=command_id,
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason="blocked_by_command_preflight",
            ),
        }

    command_cfg = command_preflight.get("config") or {}
    same_target_dedupe = await check_recent_same_target_dedupe(
        proposed_target=weights,
        command_id=command_id,
        lookback_minutes=int(command_cfg.get("recent_same_target_dedupe_minutes") or 5),
        tolerance=float(command_cfg.get("recent_same_target_dedupe_tolerance") or 0.005),
    )
    if not same_target_dedupe.get("should_send", True):
        await record_recent_same_target_dedupe(
            command_id=command_id,
            analysis_id=analysis_id,
            target_weights=weights,
            dedupe_result=same_target_dedupe,
            policy_version=policy_version,
            preflight_result=command_preflight,
        )
        reference_id = same_target_dedupe.get("reference_command_id") or "unknown"
        tolerance = float(same_target_dedupe.get("tolerance") or 0.0)
        lookback = int(same_target_dedupe.get("lookback_minutes") or 0)
        await tool_send_telegram({
            "text": (
                f"⏭ Command deduped `{_command_label(command_id)}`\n"
                f"Recent reconciled command `{_command_label(reference_id)}` has the same target "
                f"within {tolerance:.1%} tolerance.\n"
                f"Window: {lookback}m | No command sent to QC."
            )
        })
        return {
            "execution_status": "deduped",
            "command_id": command_id,
            "same_target_dedupe": same_target_dedupe,
            "preflight": command_preflight,
            "policy_version": policy_version,
            "policy_alignment": policy_alignment,
            "execution_audit": build_execution_audit_payload(
                action_status="skipped",
                proposed_weights=desired_weights,
                command_id=command_id,
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason="recent_same_target_reconciled",
            ),
        }

    result = await tool_send_weight_command({
        "weights": weights,
        "command_id": command_id,
        "analysis_id": analysis_id,
        "policy_version": policy_version,
    })

    if result.get("success"):
        await create_or_update_submitted_log(
            command_id=command_id,
            target_weights=weights,
            analysis_id=analysis_id,
            policy_version=policy_version,
            preflight_result=command_preflight,
            policy_sync_result=policy_sync,
            qc_response=result.get("response"),
        )
        msg = (
            f"📤 Command submitted to QC `{_command_label(command_id)}`\n"
            + "\n".join(f"  {k}: {float(v):.1%}" for k, v in equity_w.items())
            + f"\nCost: {float(risk_out.get('estimated_cost_pct', 0) or 0):.2%}"
            + "\nAwaiting QC algorithm confirmation."
        )
        cost_gate_summary = format_transaction_cost_gate_summary(risk_out.get("transaction_cost_gate") or {})
        if cost_gate_summary:
            msg += f"\n{cost_gate_summary}"
        if execution_throttle.get("applied"):
            before = (execution_throttle.get("metrics_before") or {}).get("buy_delta")
            after = (execution_throttle.get("metrics_after") or {}).get("buy_delta")
            deferred = execution_throttle.get("deferred_buy_delta")
            msg += (
                "\nExecution throttle: "
                f"buy_delta {float(before or 0):.2%}->{float(after or 0):.2%}, "
                f"deferred {float(deferred or 0):.2%}"
            )
        await tool_send_telegram({"text": msg})
        qc_ack = await wait_for_qc_ack_detail(command_id)
        qc_status = qc_ack.get("qc_status")
        if qc_status in QC_OWNERSHIP_STATUSES:
            await tool_send_telegram({"text": _format_qc_lifecycle_ack_message(command_id, qc_ack)})
        elif qc_status == "rejected":
            reason = qc_ack.get("qc_rejection_reason") or "unknown"
            await tool_send_telegram({
                "text": f"❌ QC rejected `{_command_label(command_id)}`: {reason}. Positions unchanged."
            })
        else:
            await tool_send_telegram({"text": _format_qc_lifecycle_ack_message(command_id, qc_ack)})
        return {
            "execution_status": "accepted" if qc_status == "accepted" else qc_status,
            "weights_sent": weights,
            "command_id": result.get("command_id", command_id),
            "qc_response": result.get("response"),
            "qc_status": qc_status,
            "qc_rejection_reason": qc_ack.get("qc_rejection_reason"),
            "qc_ack": qc_ack,
            "policy_version": policy_version,
            "preflight": command_preflight,
            "active_execution_gate": active_execution_gate,
            "policy_sync": policy_sync,
            "policy_alignment": policy_alignment,
            "execution_audit": build_execution_audit_payload(
                action_status="accepted" if qc_status in QC_OWNERSHIP_STATUSES else "sent",
                proposed_weights=desired_weights,
                sent_weights=weights,
                command_id=result.get("command_id", command_id),
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason=None if qc_status in QC_OWNERSHIP_STATUSES else (qc_ack.get("qc_rejection_reason") or qc_status),
            ),
        }

    err = result.get("error", "unknown")
    await tool_send_telegram({"text": f"❌ QC command submission failed: {err}\nPositions unchanged."})
    return {
        "execution_status": "failed",
        "error": err,
        "command_id": command_id,
        "policy_version": policy_version,
        "preflight": command_preflight,
        "policy_sync": policy_sync,
        "policy_alignment": policy_alignment,
        "execution_audit": build_execution_audit_payload(
            action_status="failed",
            proposed_weights=desired_weights,
            command_id=command_id,
            rebalance_actions=risk_out.get("rebalance_actions") or [],
            estimated_cost_pct=risk_out.get("estimated_cost_pct"),
            reason=err,
        ),
    }
