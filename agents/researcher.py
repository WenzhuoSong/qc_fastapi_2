# agents/researcher.py
from agents.base_agent import BaseAgent
from tools.registry import get_tool_executor

SYSTEM_PROMPT = """你是量化交易系统的首席市场研究员。
输入是最近 4 条 QC 快照 + 当前持仓状态。
你的任务：
1. 分析动量趋势方向（不要只看单点，要看趋势）
2. 识别当前市场制度
3. 输出结构化 JSON，必须包含：
   - market_judgment.regime
   - market_judgment.adjusted_confidence (0−1)
   - market_judgment.uncertainty_flag (bool)
   - recommended_stance: maintain|increase|reduce|defensive
   - reasoning: 不超过 150 字
   - consensus_points: []
   - divergence_points: []

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


def run_researcher(plan: dict) -> dict:
    agent = BaseAgent(
        name          = "RESEARCHER",
        system_prompt = SYSTEM_PROMPT,
        tools         = TOOLS_DEF,
        tool_executor = get_tool_executor(["read_latest_snapshots", "read_system_config"]),
        max_retries   = 2,
    )
    return agent.run(
        {"plan": plan, "task": "分析当前市场状态"},
        OUTPUT_SCHEMA,
    )
