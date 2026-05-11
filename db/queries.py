# db/queries.py
from datetime import datetime, timedelta
from sqlalchemy import select, desc, update
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import QCSnapshot, PortfolioTimeseries, HoldingsFactor, SystemConfig, AlertLog, AgentAnalysis


async def get_latest_snapshots(db: AsyncSession, limit: int = 10):
    """获取最近 N 条快照"""
    stmt = select(QCSnapshot).order_by(desc(QCSnapshot.received_at)).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()


async def get_system_config(db: AsyncSession, key: str):
    """读取 system_config"""
    stmt = select(SystemConfig).where(SystemConfig.key == key)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def upsert_system_config(db: AsyncSession, key: str, value: dict, updated_by: str = "user"):
    """插入或更新 system_config"""
    existing = await get_system_config(db, key)
    if existing:
        existing.value = value
        existing.updated_by = updated_by
        existing.updated_at = datetime.utcnow()
    else:
        new_config = SystemConfig(key=key, value=value, updated_by=updated_by)
        db.add(new_config)
    await db.commit()


async def get_latest_portfolio(db: AsyncSession):
    """获取最新的组合时序数据"""
    stmt = select(PortfolioTimeseries).order_by(desc(PortfolioTimeseries.recorded_at)).limit(1)
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def get_holdings_latest(db: AsyncSession, snapshot_id: int = None):
    """获取最新持仓因子数据"""
    if snapshot_id:
        stmt = select(HoldingsFactor).where(HoldingsFactor.snapshot_id == snapshot_id)
    else:
        # 最新 snapshot_id
        latest_snap = await get_latest_snapshots(db, limit=1)
        if not latest_snap:
            return []
        stmt = select(HoldingsFactor).where(HoldingsFactor.snapshot_id == latest_snap[0].id)
    result = await db.execute(stmt)
    return result.scalars().all()


# ─────────────────────────── Alert helpers ───────────────────────────


async def upsert_alert(db: AsyncSession, alert_data: dict) -> AlertLog:
    """写入或更新 alert log。"""
    existing = None
    if alert_data.get("alert_id"):
        result = await db.execute(
            select(AlertLog).where(AlertLog.alert_id == alert_data["alert_id"])
        )
        existing = result.scalar_one_or_none()

    if existing:
        for k, v in alert_data.items():
            if hasattr(existing, k) and k not in ("id",):
                setattr(existing, k, v)
        await db.commit()
        await db.refresh(existing)
        return existing

    new_alert = AlertLog(**alert_data)
    db.add(new_alert)
    await db.commit()
    await db.refresh(new_alert)
    return new_alert


async def get_recent_alerts(
    db: AsyncSession,
    hours: int = 24,
    level: str = None,
) -> list[AlertLog]:
    """获取最近 N 小时的 alert 记录。"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    stmt = (
        select(AlertLog)
        .where(AlertLog.triggered_at >= cutoff)
        .order_by(desc(AlertLog.triggered_at))
    )
    if level:
        stmt = stmt.where(AlertLog.level == level)
    result = await db.execute(stmt)
    return result.scalars().all()


async def mark_alert_handled(
    db: AsyncSession,
    alert_id: int,
    handled_by: str = "system",
) -> None:
    """标记 alert 为已处理。"""
    await db.execute(
        update(AlertLog)
        .where(AlertLog.id == alert_id)
        .values(
            is_handled=True,
            handled_by=handled_by,
            handled_at=datetime.utcnow(),
        )
    )
    await db.commit()

