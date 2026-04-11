# agents/researcher.py
from agents.base_agent import BaseAgent
from tools.registry import get_tool_executor

SYSTEM_PROMPT = """你是量化交易系统的首席市场研究员。
输入是最近 4 条 QC 快照 + 当前持仓状态。
你的任务：
1. 分析动量趋势方向（不要只看单点，要看趋势）
2. 识别当前市场制度
3. 输出结构化 JSON，必须包含：
   - market_judgment.regime: 必须从以下 6 个值中严格 6 选 1，不得输出其它字符串
       · bull_trend   — 强势上涨趋势（广度强、动量正、波动受控）
       · bull_weak    — 弱势上涨 / 震荡偏多
       · neutral      — 震荡无方向
       · bear_weak    — 弱势下跌 / 震荡偏空
       · bear_trend   — 强势下跌趋势
       · high_vol     — 高波动 / 风险事件驱动（不论方向）
   - market_judgment.adjusted_confidence (0−1)
   - market_judgment.uncertainty_flag (bool)
   - recommended_stance: 必须从 maintain|increase|reduce|defensive 中 4 选 1
   - reasoning: 不超过 150 字
   - consensus_points: []
   - divergence_points: []

regime 的取值直接决定下游 ALLOCATOR 的防御力度矩阵，任何非枚举值都会被视为 neutral，
所以务必严格使用上述 6 个字符串之一。

回答必须为纯 JSON，不包含任何 Markdown 或说明文字。"""

TOOLS_DEF = [
    {
        "name": "read_latest_snapshots",
        "description": "读取最近 N 条 QC 快照",
        "input_schema": {
            "type": "object",
            "properties": {"n": {"type": "integer", "default": 4}},
        },
    },
    {
        "name": "read_system_config",
        "description": "读取系统配置",
        "input_schema": {"type": "object", "properties": {}},
    },
]

OUTPUT_SCHEMA = {
    "required": [
        "market_judgment",
        "recommended_stance",
        "reasoning",
    ]
}


async def run_researcher_async(plan: dict) -> dict:
    agent = BaseAgent(
        name          = "RESEARCHER",
        system_prompt = SYSTEM_PROMPT,
        tools         = TOOLS_DEF,
        tool_executor = get_tool_executor(["read_latest_snapshots", "read_system_config"]),
        max_retries   = 2,
    )
    return await agent.run(
        {"plan": plan, "task": "分析当前市场状态"},
        OUTPUT_SCHEMA,
    )
