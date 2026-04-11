# agents/executor.py
import logging

from tools.qc_tools import tool_send_weight_command
from tools.notify_tools import tool_send_telegram
from tools.db_tools import tool_verify_approval_token

logger = logging.getLogger("qc_fastapi_2.executor")


async def run_executor_async(
    pipeline_context: dict,
    risk_out:         dict,
    analysis_id:      int,
) -> dict:
    """
    EXECUTOR 不使用 LLM —— 纯确定性逻辑。
    三道门：风控通过 / token 有效 / 权重校验。

    新架构下 target_weights / rebalance_actions / estimated_cost_pct 全部来自
    risk_out（Stage 4 Risk Manager 产出的最终执行方案）。
    """
    auth_mode = pipeline_context.get("auth_mode", "SEMI_AUTO")

    if auth_mode == "MANUAL":
        logger.info("MANUAL mode — skipping execution")
        return {"execution_status": "skipped_manual_mode"}

    # Gate 1: 风控必须批准
    if not risk_out.get("approved"):
        reasons = risk_out.get("rejection_reasons", [])
        msg = "❌ 风控拒绝执行\n" + "\n".join(str(r) for r in reasons)
        await tool_send_telegram({"text": msg})
        return {"execution_status": "rejected_by_risk"}

    # Gate 2: token 验证
    token = risk_out.get("approval_token")
    if not token:
        await tool_send_telegram({"text": "⚠️ approval_token 缺失，终止执行"})
        return {"execution_status": "aborted_no_token"}

    verify = await tool_verify_approval_token({"token": token})
    if not verify.get("valid"):
        reason = verify.get("reason", "unknown")
        await tool_send_telegram({"text": f"⚠️ Token 无效（{reason}），终止执行"})
        return {"execution_status": f"aborted_token_{reason}"}

    # 取最终目标权重
    weights = risk_out.get("target_weights", {}) or {}
    if not weights:
        await tool_send_telegram({"text": "⚠️ target_weights 为空，终止执行"})
        return {"execution_status": "aborted_no_weights"}

    # Gate 3: 权重校验（排除 CASH）
    equity_w = {k: v for k, v in weights.items() if k != "CASH"}
    w_sum = sum(float(v) for v in equity_w.values())
    if w_sum > 1.01:
        await tool_send_telegram({"text": f"⚠️ 权重总和 {w_sum:.3f} > 1.0，终止"})
        return {"execution_status": "aborted_weight_overflow"}

    # 下发权重
    result = await tool_send_weight_command({"weights": weights})

    if result.get("success"):
        msg = (
            f"✅ 指令已执行\n"
            + "\n".join(f"  {k}: {float(v):.1%}" for k, v in equity_w.items())
            + f"\n成本: {float(risk_out.get('estimated_cost_pct', 0) or 0):.2%}"
        )
        await tool_send_telegram({"text": msg})
        return {"execution_status": "success", "weights_sent": weights}

    err = result.get("error", "unknown")
    await tool_send_telegram({"text": f"❌ 指令执行失败: {err}\n当前持仓未变。"})
    return {"execution_status": "failed", "error": err}
