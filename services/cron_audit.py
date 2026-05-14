"""
Cron run audit helper.

Use `async with audit_cron_run("job_name") as audit:` around cron entrypoints.
The helper records start, finish, status, duration, rows written, summary, and
errors without changing the cron's normal exception behavior.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any, AsyncIterator

from sqlalchemy import select

from db.models import CronRunLog
from db.session import AsyncSessionLocal

logger = logging.getLogger("qc_fastapi_2.cron_audit")


class CronAuditRun:
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.run_id: int | None = None
        self.started_at = _utcnow_naive()
        self._started_monotonic = time.monotonic()
        self.rows_written = 0
        self.summary: dict[str, Any] = {}
        self.status = "success"

    def add_rows(self, count: int | None) -> None:
        try:
            self.rows_written += int(count or 0)
        except (TypeError, ValueError):
            return

    def set_summary(self, **kwargs: Any) -> None:
        self.summary.update({key: value for key, value in kwargs.items() if value is not None})

    def mark_skipped(self, reason: str | None = None) -> None:
        self.status = "skipped"
        if reason:
            self.summary["skip_reason"] = reason


@asynccontextmanager
async def audit_cron_run(job_name: str) -> AsyncIterator[CronAuditRun]:
    audit = CronAuditRun(job_name)
    await _create_run(audit)
    error_message: str | None = None
    try:
        yield audit
    except Exception as exc:
        audit.status = "failed"
        error_message = f"{type(exc).__name__}: {exc}"
        await _finish_run(audit, error_message=error_message)
        raise
    else:
        await _finish_run(audit, error_message=None)


async def read_recent_cron_runs(limit: int = 20) -> list[dict[str, Any]]:
    """Read recent cron audit rows for health reporting."""
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(CronRunLog)
            .order_by(CronRunLog.started_at.desc())
            .limit(limit)
        )
        rows = result.scalars().all()
    return [_row_to_dict(row) for row in rows]


async def _create_run(audit: CronAuditRun) -> None:
    try:
        async with AsyncSessionLocal() as db:
            row = CronRunLog(
                job_name=audit.job_name,
                started_at=audit.started_at,
                status="running",
                rows_written=0,
                summary={},
            )
            db.add(row)
            await db.commit()
            await db.refresh(row)
            audit.run_id = int(row.id)
    except Exception as exc:
        logger.warning("[cron_audit] failed to create run for %s: %s", audit.job_name, exc)


async def _finish_run(audit: CronAuditRun, error_message: str | None) -> None:
    if audit.run_id is None:
        return
    finished_at = _utcnow_naive()
    duration_ms = int((time.monotonic() - audit._started_monotonic) * 1000)
    try:
        async with AsyncSessionLocal() as db:
            row = await db.get(CronRunLog, audit.run_id)
            if row is None:
                return
            row.finished_at = finished_at
            row.status = audit.status
            row.duration_ms = duration_ms
            row.rows_written = audit.rows_written
            row.summary = audit.summary
            row.error_message = error_message
            await db.commit()
    except Exception as exc:
        logger.warning("[cron_audit] failed to finish run %s: %s", audit.run_id, exc)


def _row_to_dict(row: CronRunLog) -> dict[str, Any]:
    return {
        "id": row.id,
        "job_name": row.job_name,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
        "status": row.status,
        "duration_ms": row.duration_ms,
        "rows_written": row.rows_written,
        "summary": row.summary or {},
        "error_message": row.error_message,
    }


def _utcnow_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
