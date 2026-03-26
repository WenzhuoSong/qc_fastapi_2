# agents/risk_manager.py
from agents.base_agent import BaseAgent
from tools.registry import get_tool_executor

SYSTEM_PROMPT = """你是量化交易系统的首席风控官。
输入是 ALLOCATOR 的权重方案 + 当前市场状态。

你必须执行以下项目的数字检查（所有项必须有具体数字）：
1. vol_ok: hist_vol_20d < max_drawdown中的阈值
2. drawdown_ok: current_drawdown_pct < max_drawdown
3. position_ok: 单仓 <= max_single_position
4. sector_ok: 科技行业集中度 <= max_sector_concentration
5. cash_ok: 现金比 >= min_cash_pct
6. cost_ok: 预估成本 <= max_trade_cost_pct

任何一项 False 就是 REJECTED。
如果 APPROVED，调用 write_approval_token 生成一次性 token。

回答必须为纯 JSON，包含：
- approved: bool
- approval_token: str | null
- rejection_reasons: []
- quantitative_checks: {}
- reviewed_at"""

TOOLS_DEF = [
    {
        "name": "read_system_config",
        "description": "读取风控阈值",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "read_latest_portfolio",
        "description": "读取当前持仓状态",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "write_approval_token",
        "description": "APPROVED 时生成审批 token（5分钟有效）",
        "input_schema": {"type": "object", "properties": {}},
    },
]

OUTPUT_SCHEMA = {"required": ["approved", "quantitative_checks"]}


def run_risk_manager(plan: dict, allocator_output: dict) -> dict:
    agent = BaseAgent(
        name          = "RISK_MGR",
        system_prompt = SYSTEM_PROMPT,
        tools         = TOOLS_DEF,
        tool_executor = get_tool_executor([
            "read_system_config",
            "read_latest_portfolio",
            "write_approval_token",
        ]),
        max_retries   = 0,  # RISK MGR 不重试
    )
    return agent.run(
        {
            "plan":             plan,
            "allocator_output": allocator_output,
            "task":             "执行风控审查",
        },
        OUTPUT_SCHEMA,
    )
