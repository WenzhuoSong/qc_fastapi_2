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
    pool_timeout=30,
    connect_args={
        "timeout": 10,           # asyncpg 连接超时 10 秒
        "command_timeout": 30,   # 单条 SQL 超时 30 秒
    },
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
    masked_url = DATABASE_URL[:DATABASE_URL.find("://") + 3] + "***" + DATABASE_URL[DATABASE_URL.rfind("@"):]
    logger.info("Connecting to: %s", masked_url)
    try:
        async with engine.begin() as conn:
            logger.info("DB connection established, running create_all...")
            await conn.run_sync(Base.metadata.create_all)
            logger.info("create_all done.")
    except Exception as e:
        logger.error("init_db FAILED: %s", e)
        raise

    # 非致命 migration：设 lock_timeout 避免阻塞启动
    migrations = [
        "ALTER TABLE holdings_factors ADD COLUMN IF NOT EXISTS hist_vol_20d NUMERIC(8,6)",
    ]
    for sql in migrations:
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SET lock_timeout = '3s'"))
                await conn.execute(text(sql))
            logger.info("Migration applied: %s", sql[:60])
        except Exception as e:
            logger.warning("Migration skipped (non-fatal): %s — %s", sql[:60], e)
    logger.info("init_db complete.")


async def get_db():
    """FastAPI dependency for DB session."""
    async with AsyncSessionLocal() as session:
        yield session
