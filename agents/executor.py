# agents/executor.py
import logging

from tools.qc_tools import tool_send_weight_command
from tools.notify_tools import tool_send_telegram
from tools.db_tools import tool_verify_approval_token
from services.execution_audit import build_execution_audit_payload

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

    # Send weights
    command_id = f"analysis_{analysis_id}"
    result = await tool_send_weight_command({"weights": weights, "command_id": command_id})

    if result.get("success"):
        msg = (
            f"✅ Order executed\n"
            + "\n".join(f"  {k}: {float(v):.1%}" for k, v in equity_w.items())
            + f"\nCost: {float(risk_out.get('estimated_cost_pct', 0) or 0):.2%}"
        )
        await tool_send_telegram({"text": msg})
        return {
            "execution_status": "accepted",
            "weights_sent": weights,
            "command_id": result.get("command_id", command_id),
            "qc_response": result.get("response"),
            "execution_audit": build_execution_audit_payload(
                action_status="accepted",
                proposed_weights=weights,
                sent_weights=weights,
                command_id=result.get("command_id", command_id),
                rebalance_actions=risk_out.get("rebalance_actions") or [],
                estimated_cost_pct=risk_out.get("estimated_cost_pct"),
            ),
        }

    err = result.get("error", "unknown")
    await tool_send_telegram({"text": f"❌ Order failed: {err}\nPositions unchanged."})
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
