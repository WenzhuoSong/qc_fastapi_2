# db/queries.py
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import QCSnapshot, PortfolioTimeseries, HoldingsFactor, SystemConfig
from datetime import datetime, timedelta


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
