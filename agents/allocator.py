# agents/allocator.py
from agents.base_agent import BaseAgent
from tools.registry import get_tool_executor

SYSTEM_PROMPT = """你是量化交易系统的权重配置器。
输入是 RESEARCHER 的市场判断 + 当前持仓快照。

你必须进行：
1. 动量评分：基于 mom_20d/60d/252d、rsi_14、atr_pct、bb_position 计算各标的合并评分
2. 权重优化：生成方案A（标准）和方案B（保守）
3. 漂移检查：过滤差値 < 2% 的调仓

灯控限制（从 system_config 读取）：
- 单仓上限： max_single_position
- 最低现金： min_cash_pct
- 漂移阈值： rebalance_threshold

严禁自行发明权重数字。所有权重必须来自评分公式计算。

回答必须为纯 JSON，包含：
- recommended_plan: A | B
- plan_a: {label, target_weights, rebalance_actions, estimated_cost_pct}
- plan_b: {label, target_weights, rebalance_actions, estimated_cost_pct}
- reasoning"""

TOOLS_DEF = [
    {
        "name": "read_latest_snapshots",
        "description": "读取最近快照（含 watchlist 评分）",
        "input_schema": {"type": "object", "properties": {"n": {"type": "integer"}}},
    },
    {
        "name": "read_system_config",
        "description": "读取风控参数和评分公式",
        "input_schema": {"type": "object", "properties": {}},
    },
]

OUTPUT_SCHEMA = {"required": ["recommended_plan", "plan_a", "plan_b"]}


def run_allocator(plan: dict, researcher_output: dict) -> dict:
    agent = BaseAgent(
        name          = "ALLOCATOR",
        system_prompt = SYSTEM_PROMPT,
        tools         = TOOLS_DEF,
        tool_executor = get_tool_executor(["read_latest_snapshots", "read_system_config"]),
        max_retries   = 1,
    )
    return agent.run(
        {
            "plan":              plan,
            "researcher_output": researcher_output,
            "task":              "生成权重方案",
        },
        OUTPUT_SCHEMA,
    )
