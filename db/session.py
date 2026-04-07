# db/session.py
import logging
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
    """Create all tables on startup."""
    from db import models  # noqa: F401 — ensure models are registered with Base
    logger.info("Running init_db, tables known: %s", list(Base.metadata.tables.keys()))
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("init_db complete.")


async def get_db():
    """FastAPI dependency for DB session."""
    async with AsyncSessionLocal() as session:
        yield session
