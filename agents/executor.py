# agents/executor.py
import logging

from tools.qc_tools import tool_send_policy_sync, tool_send_weight_command
from tools.notify_tools import tool_send_telegram
from tools.db_tools import tool_verify_approval_token
from services.execution_audit import build_execution_audit_payload
from services.execution_ack_tracker import wait_for_qc_ack_detail
from services.execution_log_store import (
    create_or_update_policy_sync_log,
    create_or_update_submitted_log,
    record_preflight_block,
)
from services.execution_policy import policy_snapshot
from services.execution_preflight import preflight_execution_command, preflight_execution_weights

logger = logging.getLogger("qc_fastapi_2.executor")


def _command_label(command_id: str) -> str:
    return str(command_id or "unknown")


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
    policy_sync_id = f"{command_id}_policy"
    await create_or_update_policy_sync_log(
        command_id=policy_sync_id,
        analysis_id=analysis_id,
        policy_version=policy_version,
        policy_payload=policy,
        status="pending_send",
        qc_status="pending",
    )
    policy_sync = await tool_send_policy_sync({"command_id": policy_sync_id, "payload": policy})
    await create_or_update_policy_sync_log(
        command_id=policy_sync_id,
        analysis_id=analysis_id,
        policy_version=policy_version,
        policy_payload=policy,
        qc_response=policy_sync.get("response"),
        status="sent" if policy_sync.get("success") else "failed",
        qc_status="submitted" if policy_sync.get("success") else "not_sent",
    )
    if not policy_sync.get("success"):
        err = policy_sync.get("error", "policy sync failed")
        await tool_send_telegram({
            "text": (
                f"⛔ PolicySync failed before `{_command_label(command_id)}`\n"
                f"{err}\nNo command sent to QC."
            )
        })
        return {
            "execution_status": "failed",
            "error": err,
            "execution_audit": build_execution_audit_payload(
                action_status="failed",
                proposed_weights=desired_weights,
                command_id=command_id,
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason="policy_sync_failed",
            ),
        }
    policy_sync_ack = await wait_for_qc_ack_detail(policy_sync_id, timeout_seconds=15)
    policy_sync["ack"] = policy_sync_ack
    policy_sync["ack_status"] = policy_sync_ack.get("qc_status")
    if policy_sync_ack.get("qc_status") != "accepted":
        reason = policy_sync_ack.get("qc_rejection_reason") or policy_sync_ack.get("qc_status") or "policy sync ack missing"
        await tool_send_telegram({
            "text": (
                f"⛔ PolicySync not accepted before `{_command_label(command_id)}`\n"
                f"policy_command={policy_sync_id} status={policy_sync_ack.get('qc_status')} reason={reason}\n"
                "No command sent to QC."
            )
        })
        return {
            "execution_status": "failed",
            "error": reason,
            "policy_sync": policy_sync,
            "execution_audit": build_execution_audit_payload(
                action_status="failed",
                proposed_weights=desired_weights,
                command_id=command_id,
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason="policy_sync_not_accepted",
            ),
        }

    command_preflight = await preflight_execution_command(
        command_id=command_id,
        analysis_id=analysis_id,
        target_weights=weights,
        current_weights=(final_validation.get("current_weights") or {}),
        policy_version=policy_version,
        policy_sync_result=policy_sync,
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
                f"blockers={command_preflight.get('blockers')}\n"
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

    result = await tool_send_weight_command({
        "weights": weights,
        "command_id": command_id,
        "analysis_id": analysis_id,
        "policy": policy,
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
        if qc_status == "accepted":
            await tool_send_telegram({"text": f"✅ QC accepted `{_command_label(command_id)}`"})
        elif qc_status == "rejected":
            reason = qc_ack.get("qc_rejection_reason") or "unknown"
            await tool_send_telegram({
                "text": f"❌ QC rejected `{_command_label(command_id)}`: {reason}. Positions unchanged."
            })
        else:
            await tool_send_telegram({
                "text": (
                    f"⚠️ QC ACK timeout `{_command_label(command_id)}`\n"
                    "No QC algorithm confirmation within 30s. Verify positions manually."
                )
            })
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
            "policy_sync": policy_sync,
            "execution_audit": build_execution_audit_payload(
                action_status="accepted" if qc_status == "accepted" else "sent",
                proposed_weights=desired_weights,
                sent_weights=weights,
                command_id=result.get("command_id", command_id),
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason=None if qc_status == "accepted" else (qc_ack.get("qc_rejection_reason") or qc_status),
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
        "execution_audit": build_execution_audit_payload(
            action_status="failed",
            proposed_weights=desired_weights,
            command_id=command_id,
            rebalance_actions=risk_out.get("rebalance_actions") or [],
            estimated_cost_pct=risk_out.get("estimated_cost_pct"),
            reason=err,
        ),
    }
