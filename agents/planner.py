# agents/planner.py
"""
Phase 1 简化版 PLANNER：静态工作流，不生成动态 DAG。
只负责读取当前状态并决定 mode。
"""
from datetime import datetime

from db.session import AsyncSessionLocal
from db.queries import get_system_config


async def run_planner_async(trigger_type: str = "scheduled_hourly") -> dict:
    """异步入口，返回 plan 字典。"""
    async with AsyncSessionLocal() as db:
        risk_params = await get_system_config(db, "risk_params")
        circuit_cfg = await get_system_config(db, "circuit_state")
        auth_cfg    = await get_system_config(db, "authorization_mode")

    circuit   = (circuit_cfg.value if circuit_cfg else {"value": "CLOSED"}).get("value", "CLOSED")
    auth_mode = (auth_cfg.value if auth_cfg else {"value": "SEMI_AUTO"}).get("value", "SEMI_AUTO")
    max_dd    = (risk_params.value if risk_params else {"value": 0.15}).get("value", 0.15)

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
