# tools/registry.py
"""
ToolRegistry: 存储所有可用 Tool 的函数实现。
BaseAgent 在创建时只传入白名单内的子集。
"""
from tools.db_tools import (
    tool_read_system_config,
    tool_read_latest_snapshots,
    tool_write_decision,
    tool_write_approval_token,
    tool_verify_approval_token,
    tool_read_latest_portfolio,
)
from tools.qc_tools import (
    tool_send_weight_command,
    tool_emergency_liquidate,
)
from tools.notify_tools import tool_send_telegram

# 所有可用 Tool 的函数映射
ALL_TOOLS: dict[str, callable] = {
    "read_system_config":       tool_read_system_config,
    "read_latest_snapshots":    tool_read_latest_snapshots,
    "read_latest_portfolio":    tool_read_latest_portfolio,
    "write_decision":           tool_write_decision,
    "write_approval_token":     tool_write_approval_token,
    "verify_approval_token":    tool_verify_approval_token,
    "send_weight_command":      tool_send_weight_command,
    "emergency_liquidate":      tool_emergency_liquidate,
    "send_telegram":            tool_send_telegram,
}


def get_tool_executor(whitelist: list[str]) -> dict[str, callable]:
    """Return {name: fn} for tools in whitelist only."""
    return {
        name: fn
        for name, fn in ALL_TOOLS.items()
        if name in whitelist
    }
