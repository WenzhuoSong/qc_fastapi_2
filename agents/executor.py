# agents/executor.py
import logging

from tools.qc_tools import tool_send_weight_command
from tools.notify_tools import tool_send_telegram
from tools.db_tools import tool_verify_approval_token
from services.execution_audit import build_execution_audit_payload
from services.execution_ack_tracker import wait_for_qc_ack
from services.execution_log_store import create_or_update_submitted_log
from services.execution_preflight import preflight_execution_weights

logger = logging.getLogger("qc_fastapi_2.executor")


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

    # Gate 3: weight check (excluding CASH)
    equity_w = {k: v for k, v in weights.items() if k != "CASH"}
    w_sum = sum(float(v) for v in equity_w.values())
    if w_sum > 1.01:
        await tool_send_telegram({"text": f"⚠️ Equity weights sum {w_sum:.3f} > 1.0 — abort"})
        return {
            "execution_status": "rejected",
            "execution_audit": build_execution_audit_payload(
                action_status="rejected",
                proposed_weights=weights,
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
                proposed_weights=weights,
                reason="blocked_by_execution_policy",
            ),
            "preflight": preflight,
        }

    # Send weights
    command_id = f"analysis_{analysis_id}"
    result = await tool_send_weight_command({"weights": weights, "command_id": command_id})

    if result.get("success"):
        await create_or_update_submitted_log(
            command_id=command_id,
            target_weights=weights,
            analysis_id=analysis_id,
            qc_response=result.get("response"),
        )
        msg = (
            f"📤 Command submitted to QC `{command_id[:8]}`\n"
            + "\n".join(f"  {k}: {float(v):.1%}" for k, v in equity_w.items())
            + f"\nCost: {float(risk_out.get('estimated_cost_pct', 0) or 0):.2%}"
            + "\nAwaiting QC algorithm confirmation."
        )
        await tool_send_telegram({"text": msg})
        qc_status = await wait_for_qc_ack(command_id)
        if qc_status == "accepted":
            await tool_send_telegram({"text": f"✅ QC accepted `{command_id[:8]}`"})
        elif qc_status == "rejected":
            await tool_send_telegram({"text": f"❌ QC rejected `{command_id[:8]}`. Positions unchanged."})
        else:
            await tool_send_telegram({
                "text": (
                    f"⚠️ QC ACK timeout `{command_id[:8]}`\n"
                    "No QC algorithm confirmation within 30s. Verify positions manually."
                )
            })
        return {
            "execution_status": "accepted" if qc_status == "accepted" else qc_status,
            "weights_sent": weights,
            "command_id": result.get("command_id", command_id),
            "qc_response": result.get("response"),
            "qc_status": qc_status,
            "execution_audit": build_execution_audit_payload(
                action_status="accepted" if qc_status == "accepted" else "sent",
                proposed_weights=weights,
                sent_weights=weights,
                command_id=result.get("command_id", command_id),
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
                reason=None if qc_status == "accepted" else qc_status,
            ),
        }

    err = result.get("error", "unknown")
    await tool_send_telegram({"text": f"❌ QC command submission failed: {err}\nPositions unchanged."})
    return {
        "execution_status": "failed",
        "error": err,
        "execution_audit": build_execution_audit_payload(
            action_status="failed",
            proposed_weights=weights,
            command_id=command_id,
            rebalance_actions=risk_out.get("rebalance_actions") or [],
            estimated_cost_pct=risk_out.get("estimated_cost_pct"),
            reason=err,
        ),
    }
