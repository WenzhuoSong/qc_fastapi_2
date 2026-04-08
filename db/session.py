# db/session.py
import asyncio
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from config import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

# asyncpg 驱动（postgresql+asyncpg://...）
DATABASE_URL = (
    settings.database_url
    .replace("postgresql://", "postgresql+asyncpg://")
    .replace("postgres://", "postgresql+asyncpg://")
)

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    echo=False,
)

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


class Base(DeclarativeBase):
    pass


async def init_db():
    """Create all tables on startup, and apply any missing column migrations."""
    from db import models  # noqa: F401 — ensure models are registered with Base
    logger.info("Running init_db, tables known: %s", list(Base.metadata.tables.keys()))
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # 手动补列（create_all 不会修改已存在的表）
        migrations = [
            "ALTER TABLE holdings_factors ADD COLUMN IF NOT EXISTS hist_vol_20d NUMERIC(8,6)",
        ]
        for sql in migrations:
            await conn.execute(text(sql))
    logger.info("init_db complete.")


async def get_db():
    """FastAPI dependency for DB session."""
    async with AsyncSessionLocal() as session:
        yield session


def run_async_isolated(async_fn):
    """
    在全新的事件循环中运行 async_fn(session_factory)，并使用全新创建的 engine。

    用途：sync agent 代码（被 run_in_executor 推到线程池中执行）需要做 DB 操作时调用此函数。
    每次调用都会新建一个 engine，asyncpg 连接池会绑定到这个新 loop 上，避免与主进程
    pool 跨 loop 冲突的问题。

    传入的 async_fn 必须接受一个 session_factory 参数（不能依赖全局 AsyncSessionLocal）。
    """
    async def _wrapper():
        engine = create_async_engine(DATABASE_URL, echo=False)
        try:
            session_factory = async_sessionmaker(
                engine, class_=AsyncSession, expire_on_commit=False
            )
            return await async_fn(session_factory)
        finally:
            await engine.dispose()

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(_wrapper())
    finally:
        loop.close()
