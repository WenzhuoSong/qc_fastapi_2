"""Async database access for the `scout` schema.

Scout owns its own engine so it never shares pool state or settings with the
noble-flow observer. All SQL is explicit text; the schema lives in
`scout/migrations/*.sql`.
"""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from scout.config import get_scout_settings

logger = logging.getLogger("scout.db")

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _async_url(url: str) -> str:
    return url.replace("postgresql://", "postgresql+asyncpg://", 1).replace(
        "postgres://", "postgresql+asyncpg://", 1
    )


@lru_cache()
def get_engine() -> AsyncEngine:
    return create_async_engine(
        _async_url(get_scout_settings().database_url),
        pool_size=5,
        max_overflow=5,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"timeout": 10, "command_timeout": 120},
    )


@lru_cache()
def _sessionmaker() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


@asynccontextmanager
async def session() -> AsyncIterator[AsyncSession]:
    async with _sessionmaker()() as s:
        yield s


async def apply_migrations() -> list[str]:
    applied = []
    async with get_engine().begin() as conn:
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            raw = await conn.get_raw_connection()
            await raw.driver_connection.execute(path.read_text(encoding="utf-8"))
            applied.append(path.name)
    return applied


def jsonb(value: Any) -> str:
    return json.dumps(value, default=_json_default, ensure_ascii=False)


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"not JSON serialisable: {type(value).__name__}")


async def fetch_all(sql: str, params: dict | None = None) -> list[dict]:
    async with session() as s:
        result = await s.execute(text(sql), params or {})
        return [dict(r._mapping) for r in result]


async def fetch_one(sql: str, params: dict | None = None) -> dict | None:
    rows = await fetch_all(sql, params)
    return rows[0] if rows else None


async def execute(sql: str, params: dict | Sequence[dict] | None = None) -> None:
    async with session() as s:
        await s.execute(text(sql), params or {})
        await s.commit()


async def execute_many(sql: str, rows: Iterable[dict], chunk: int = 5000) -> int:
    buffer: list[dict] = []
    total = 0
    async with session() as s:
        for row in rows:
            buffer.append(row)
            if len(buffer) >= chunk:
                await s.execute(text(sql), buffer)
                total += len(buffer)
                buffer = []
        if buffer:
            await s.execute(text(sql), buffer)
            total += len(buffer)
        await s.commit()
    return total


@asynccontextmanager
async def job_run(job: str) -> AsyncIterator[dict]:
    """Record a job execution in scout.job_runs; yields a mutable summary dict."""
    summary: dict[str, Any] = {}
    async with session() as s:
        result = await s.execute(text("INSERT INTO scout.job_runs (job) VALUES (:job) RETURNING id"), {"job": job})
        run_id = result.scalar_one()
        await s.commit()
    try:
        yield summary
    except BaseException as exc:
        await execute(
            "UPDATE scout.job_runs SET status='failed', finished_at=:t, summary=CAST(:s AS JSONB), "
            "error=:e WHERE id=:id",
            {"t": datetime.now(timezone.utc), "s": jsonb(summary), "e": repr(exc)[:2000], "id": run_id},
        )
        raise
    else:
        await execute(
            "UPDATE scout.job_runs SET status='ok', finished_at=:t, summary=CAST(:s AS JSONB) WHERE id=:id",
            {"t": datetime.now(timezone.utc), "s": jsonb(summary), "id": run_id},
        )
