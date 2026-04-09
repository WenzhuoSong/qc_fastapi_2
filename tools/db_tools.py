# tools/db_tools.py
"""
所有 tool_* 函数均为 async。
假定调用者已在一个 event loop 中运行（来自 cron 入口 asyncio.run() 或 FastAPI 请求处理器）。
使用全局 AsyncSessionLocal —— 因为整个调用栈在同一个 loop 里，asyncpg pool 可以复用。
"""
import uuid
from datetime import datetime, timedelta

from db.session import AsyncSessionLocal
from db.models import AgentAnalysis
from db.queries import (
    get_system_config,
    get_latest_snapshots,
    get_latest_portfolio,
    upsert_system_config,
)


async def tool_read_system_config(_input: dict) -> dict:
    async with AsyncSessionLocal() as db:
        config = await get_system_config(db, "risk_params")
        if not config:
            return {}
        return config.value


async def tool_read_latest_snapshots(inp: dict) -> list:
    n = inp.get("n", 4)
    async with AsyncSessionLocal() as db:
        rows = await get_latest_snapshots(db, n)
        return [row.raw_payload for row in rows]


async def tool_read_latest_portfolio(_input: dict) -> dict:
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


async def tool_write_decision(inp: dict) -> dict:
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
        await db.refresh(row)
        return {"analysis_id": row.id}


async def tool_write_approval_token(inp: dict) -> dict:
    """Generate + store a one-time approval token (TTL = semi_auto timeout + 5 min buffer)."""
    from config import get_settings
    ttl_minutes = get_settings().semi_auto_timeout_minutes + 5
    token      = str(uuid.uuid4())
    expires_at = (datetime.utcnow() + timedelta(minutes=ttl_minutes)).isoformat()
    async with AsyncSessionLocal() as db:
        await upsert_system_config(
            db, "last_approval_token",
            {"token": token, "expires_at": expires_at, "used": False},
            "risk_mgr",
        )
    return {"approval_token": token, "expires_at": expires_at}


async def tool_verify_approval_token(inp: dict) -> dict:
    """Verify + consume the approval token."""
    provided = inp.get("token")
    async with AsyncSessionLocal() as db:
        config = await get_system_config(db, "last_approval_token")
        if not config:
            return {"valid": False, "reason": "no_token"}
        stored = config.value
        if stored.get("used"):
            return {"valid": False, "reason": "already_used"}
        if stored.get("token") != provided:
            return {"valid": False, "reason": "token_mismatch"}
        if datetime.utcnow().isoformat() > stored.get("expires_at", "1970-01-01T00:00:00"):
            return {"valid": False, "reason": "expired"}
        stored["used"] = True
        await upsert_system_config(db, "last_approval_token", stored, "executor")
        return {"valid": True}
