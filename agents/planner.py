# agents/planner.py
"""
Phase 1 简化版 PLANNER：静态工作流，不生成动态 DAG。
只负责读取当前状态并决定 mode。
"""
from datetime import datetime
from db.session import run_async_isolated
from db.queries import get_system_config


def run_planner(trigger_type: str = "scheduled_hourly") -> dict:
    """同步入口，返回 plan 字典。"""
    config = run_async_isolated(_load_config)

    circuit     = config.get("circuit_state", {}).get("value", "CLOSED")
    auth_mode   = config.get("authorization_mode", {}).get("value", "SEMI_AUTO")
    max_dd      = config.get("max_drawdown", {}).get("value", 0.15)

    # Phase 1: STANDARD 模式（固定工作流）
    plan = {
        "plan_id":       f"P-{datetime.utcnow().strftime('%Y%m%d-%H%M')}",
        "trigger":       trigger_type,
        "mode":          "STANDARD",
        "auth_mode":     auth_mode,
        "circuit_state": circuit,
        "abort_conditions": [
            f"drawdown > {round(max_dd * 0.9, 3)}",
            "vix > 45",
        ],
        "force_constraints": _get_force_constraints(circuit),
    }
    return plan


def _get_force_constraints(circuit: str) -> dict:
    if circuit == "ALERT":
        return {"force_plan": "B", "max_position": 0.12, "allow_buy": True}
    if circuit == "DEFENSIVE":
        return {"force_plan": "hold_or_reduce", "allow_buy": False}
    return {"force_plan": None, "max_position": None, "allow_buy": True}


async def _load_config(session_factory):
    async with session_factory() as db:
        risk_params = await get_system_config(db, "risk_params")
        circuit = await get_system_config(db, "circuit_state")
        auth_mode = await get_system_config(db, "authorization_mode")

        return {
            "circuit_state": circuit.value if circuit else {"value": "CLOSED"},
            "authorization_mode": auth_mode.value if auth_mode else {"value": "SEMI_AUTO"},
            "max_drawdown": risk_params.value if risk_params else {"value": 0.15},
        }
