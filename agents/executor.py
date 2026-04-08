# agents/executor.py
import logging

from tools.qc_tools import tool_send_weight_command
from tools.notify_tools import tool_send_telegram
from tools.db_tools import tool_verify_approval_token

logger = logging.getLogger("qc_fastapi_2.executor")


async def run_executor_async(
    plan:             dict,
    allocator_output: dict,
    risk_output:      dict,
    analysis_id:      int,
) -> dict:
    """
    EXECUTOR 不使用 LLM——纯确定性逻辑。
    三个局部守门：token 验证 / auth_mode / 权重校验。
    """
    auth_mode = plan.get("auth_mode", "SEMI_AUTO")

    # MANUAL 模式：不执行
    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — skipping execution")
        return {"execution_status": "skipped_manual_mode"}

    # 验证 RISK MGR 审批
    if not risk_output.get("approved"):
        reasons = risk_output.get("rejection_reasons", [])
        msg = f"❌ 风控拒绝执行\n{chr(10).join(str(r) for r in reasons)}"
        await tool_send_telegram({"text": msg})
        return {"execution_status": "rejected_by_risk"}

    # 验证 token
    token = risk_output.get("approval_token")
    if not token:
        await tool_send_telegram({"text": "⚠️ approval_token 缺失，终止执行"})
        return {"execution_status": "aborted_no_token"}

    verify = await tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        reason = verify.get("reason", "unknown")
        await tool_send_telegram({"text": f"⚠️ Token 无效（{reason}），终止执行"})
        return {"execution_status": f"aborted_token_{reason}"}

    # 取出目标权重
    recommended = allocator_output.get("recommended_plan", "A")
    plan_key    = f"plan_{recommended.lower()}"
    chosen_plan = allocator_output.get(plan_key, {})
    weights     = chosen_plan.get("target_weights", {})

    if not weights:
        await tool_send_telegram({"text": "⚠️ target_weights 为空，终止执行"})
        return {"execution_status": "aborted_no_weights"}

    # 权重校验（排除CASH）
    equity_w = {k: v for k, v in weights.items() if k != "CASH"}
    w_sum = sum(equity_w.values())
    if w_sum > 1.01:
        await tool_send_telegram({"text": f"⚠️ 权重总和 {w_sum:.3f} > 1.0，终止"})
        return {"execution_status": "aborted_weight_overflow"}

    # 下发权重
    result = await tool_send_weight_command({"weights": weights})

    if result.get("success"):
        msg = (
            f"✅ 指令已执行（方案{recommended}）\n"
            + "\n".join(f"  {k}: {v:.1%}" for k, v in equity_w.items())
            + f"\n成本: {chosen_plan.get('estimated_cost_pct', 0):.2%}"
        )
        await tool_send_telegram({"text": msg})
        return {"execution_status": "success", "weights_sent": weights}
    else:
        err = result.get("error", "unknown")
        await tool_send_telegram({"text": f"❌ 指令执行失败: {err}\n当前持仓未变。"})
        return {"execution_status": "failed", "error": err}
