# tools/db_tools.py
import asyncio
import json
import uuid
from datetime import datetime, timedelta

from db.session import AsyncSessionLocal
from db.models import AgentAnalysis, SystemConfig
from db.queries import get_system_config, get_latest_snapshots, get_latest_portfolio


def _run(coro):
    """Tool 函数是同步的（被 BaseAgent 同步调用），需要这个 wrapper。
    每次创建独立事件循环，避免与 BaseAgent 的 asyncio.run() 嵌套冲突。"""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def tool_read_system_config(_input: dict) -> dict:
    async def _():
        async with AsyncSessionLocal() as db:
            config = await get_system_config(db, "risk_params")
            if not config:
                return {}
            return config.value
    return _run(_())


def tool_read_latest_snapshots(inp: dict) -> list:
    n = inp.get("n", 4)
    async def _():
        async with AsyncSessionLocal() as db:
            rows = await get_latest_snapshots(db, n)
            return [row.raw_payload for row in rows]
    return _run(_())


def tool_read_latest_portfolio(_input: dict) -> dict:
    async def _():
        async with AsyncSessionLocal() as db:
            row = await get_latest_portfolio(db)
            if not row:
                return {}
            return {
                "total_value":          float(row.total_value or 0),
                "cash_pct":             float(row.cash_pct or 0),
                "daily_pnl_pct":        float(row.daily_pnl_pct or 0),
                "current_drawdown_pct": float(row.current_drawdown_pct or 0),
                "regime_label":         row.regime_label,
                "recorded_at":          row.recorded_at.isoformat(),
            }
    return _run(_())


def tool_write_decision(inp: dict) -> dict:
    async def _():
        async with AsyncSessionLocal() as db:
            row = AgentAnalysis(
                analyzed_at       = datetime.utcnow(),
                trigger_type      = inp.get("trigger_type", "scheduled"),
                snapshot_ids      = inp.get("snapshot_ids"),
                planner_output    = inp.get("planner_output"),
                researcher_output = inp.get("researcher_output"),
                allocator_output  = inp.get("allocator_output"),
                risk_output       = inp.get("risk_output"),
                risk_approved     = inp.get("risk_approved"),
                decision          = inp.get("decision"),
                execution_status  = inp.get("execution_status", "pending"),
                notes             = inp.get("notes"),
            )
            db.add(row)
            await db.commit()
            return {"analysis_id": row.id}
    return _run(_())


def tool_write_approval_token(inp: dict) -> dict:
    """Generate + store a one-time approval token (5-min TTL)."""
    token      = str(uuid.uuid4())
    expires_at = (datetime.utcnow() + timedelta(minutes=5)).isoformat()
    async def _():
        async with AsyncSessionLocal() as db:
            from db.queries import upsert_system_config
            await upsert_system_config(
                db, "last_approval_token",
                {"token": token, "expires_at": expires_at, "used": False},
                "risk_mgr",
            )
    _run(_())
    return {"approval_token": token, "expires_at": expires_at}


def tool_verify_approval_token(inp: dict) -> dict:
    """Verify + consume the approval token."""
    provided = inp.get("token")
    async def _():
        async with AsyncSessionLocal() as db:
            config = await get_system_config(db, "last_approval_token")
            if not config:
                return {"valid": False, "reason": "no_token"}
            stored = config.value
            if stored.get("used"):
                return {"valid": False, "reason": "already_used"}
            if stored.get("token") != provided:
                return {"valid": False, "reason": "token_mismatch"}
            if datetime.utcnow().isoformat() > stored.get("expires_at", ""):
                return {"valid": False, "reason": "expired"}
            # 消耗 token
            from db.queries import upsert_system_config
            stored["used"] = True
            await upsert_system_config(db, "last_approval_token", stored, "executor")
            return {"valid": True}
    return _run(_())
