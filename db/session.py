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
        "ALTER TABLE market_daily_features ADD COLUMN IF NOT EXISTS rsi_10 NUMERIC(6,2)",
        "ALTER TABLE market_daily_features ADD COLUMN IF NOT EXISTS rsi_14 NUMERIC(6,2)",
        "ALTER TABLE market_daily_features ADD COLUMN IF NOT EXISTS atr_pct NUMERIC(8,6)",
        "ALTER TABLE market_daily_features ADD COLUMN IF NOT EXISTS bb_position NUMERIC(6,4)",
        "ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS command_id VARCHAR(64)",
        "ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS qc_status VARCHAR(32) DEFAULT 'submitted'",
        "ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS qc_ack_at TIMESTAMP",
        "ALTER TABLE execution_log ADD COLUMN IF NOT EXISTS qc_rejection_reason TEXT",
        "CREATE INDEX IF NOT EXISTS idx_execution_log_command_id ON execution_log (command_id)",
        """
        CREATE TABLE IF NOT EXISTS strategy_frozen_signals (
            id BIGSERIAL PRIMARY KEY,
            signal_id VARCHAR(64) NOT NULL,
            signal_source VARCHAR(40) NOT NULL,
            signal_date DATE NOT NULL,
            generated_at TIMESTAMP NOT NULL,
            tradable_from_date DATE NOT NULL,
            strategy_id VARCHAR(100) NOT NULL,
            strategy_version VARCHAR(20),
            ticker VARCHAR(20) NOT NULL,
            role VARCHAR(50),
            branch VARCHAR(120),
            action VARCHAR(30) NOT NULL,
            signal_type VARCHAR(80),
            confidence DOUBLE PRECISION,
            raw_score DOUBLE PRECISION,
            normalized_score DOUBLE PRECISION,
            max_reasonable_weight DOUBLE PRECISION,
            risk_budget_cost DOUBLE PRECISION,
            feature_data_date DATE,
            data_lag_days INTEGER,
            feature_source VARCHAR(40),
            feature_authority VARCHAR(40),
            regime_at_signal VARCHAR(50),
            vix_at_signal DOUBLE PRECISION,
            evidence_contract_version VARCHAR(20),
            diagnostics JSONB,
            content_hash VARCHAR(64) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            CONSTRAINT uq_strategy_frozen_signal_id UNIQUE (signal_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_strategy_frozen_signals_date ON strategy_frozen_signals (signal_date)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_frozen_signals_strategy ON strategy_frozen_signals (strategy_id)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_frozen_signals_ticker ON strategy_frozen_signals (ticker)",
        """
        CREATE TABLE IF NOT EXISTS strategy_signal_outcomes (
            id BIGSERIAL PRIMARY KEY,
            outcome_id VARCHAR(64) NOT NULL,
            signal_id VARCHAR(64) NOT NULL,
            signal_source VARCHAR(40) NOT NULL,
            signal_date DATE NOT NULL,
            label_date DATE NOT NULL,
            strategy_id VARCHAR(100) NOT NULL,
            ticker VARCHAR(20) NOT NULL,
            branch VARCHAR(120),
            action VARCHAR(30) NOT NULL,
            horizon_days INTEGER NOT NULL,
            forward_return DOUBLE PRECISION,
            spy_forward_return DOUBLE PRECISION,
            excess_vs_spy DOUBLE PRECISION,
            drawdown_during_horizon DOUBLE PRECISION,
            spy_drawdown_during_horizon DOUBLE PRECISION,
            target_pool_drawdown DOUBLE PRECISION,
            hit BOOLEAN,
            hit_definition VARCHAR(160) NOT NULL,
            excess_calculation_method VARCHAR(30) NOT NULL,
            outcome_source VARCHAR(40) NOT NULL,
            data_quality VARCHAR(40) NOT NULL,
            content_hash VARCHAR(64) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            CONSTRAINT uq_strategy_signal_outcome_id UNIQUE (outcome_id),
            CONSTRAINT uq_strategy_signal_outcome_signal_horizon_source UNIQUE (
                signal_id,
                horizon_days,
                outcome_source
            )
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_signal_id ON strategy_signal_outcomes (signal_id)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_label_date ON strategy_signal_outcomes (label_date)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_strategy ON strategy_signal_outcomes (strategy_id)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_signal_outcomes_ticker ON strategy_signal_outcomes (ticker)",
        """
        CREATE TABLE IF NOT EXISTS strategy_conviction_profiles (
            id BIGSERIAL PRIMARY KEY,
            profile_id VARCHAR(64) NOT NULL,
            as_of_date DATE NOT NULL,
            strategy_id VARCHAR(100) NOT NULL,
            ticker VARCHAR(20) NOT NULL,
            branch VARCHAR(120),
            action VARCHAR(30) NOT NULL,
            regime_at_signal VARCHAR(50),
            horizon_days INTEGER NOT NULL,
            source_bucket VARCHAR(40) NOT NULL,
            conviction DOUBLE PRECISION,
            status VARCHAR(60) NOT NULL,
            n INTEGER NOT NULL DEFAULT 0,
            required_samples INTEGER NOT NULL DEFAULT 30,
            hit_rate DOUBLE PRECISION,
            avg_forward_return DOUBLE PRECISION,
            avg_excess_vs_spy DOUBLE PRECISION,
            ic DOUBLE PRECISION,
            max_adverse_drawdown DOUBLE PRECISION,
            data_lag_filtered INTEGER NOT NULL DEFAULT 0,
            requires_live_confirmation BOOLEAN NOT NULL DEFAULT false,
            hist_n INTEGER NOT NULL DEFAULT 0,
            live_n INTEGER NOT NULL DEFAULT 0,
            hist_weight DOUBLE PRECISION,
            live_weight DOUBLE PRECISION,
            source_counts JSONB,
            diagnostics JSONB,
            content_hash VARCHAR(64) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT now(),
            CONSTRAINT uq_strategy_conviction_profile_id UNIQUE (profile_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_as_of_date ON strategy_conviction_profiles (as_of_date)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_strategy ON strategy_conviction_profiles (strategy_id)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_ticker ON strategy_conviction_profiles (ticker)",
        "CREATE INDEX IF NOT EXISTS idx_strategy_conviction_profiles_source_bucket ON strategy_conviction_profiles (source_bucket)",
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
