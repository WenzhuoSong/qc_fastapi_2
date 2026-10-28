"""Shared entry helper: logging, job_runs bookkeeping, failure alert."""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Awaitable, Callable

from scout import db


def run(job: str, fn: Callable[[], Awaitable[dict]], alert: bool = True) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)       # request URLs can carry tokens

    async def _main() -> None:
        await db.apply_migrations()
        try:
            from scout import learned

            await learned.seed()
        except Exception as exc:          # a seed problem must not stop the job
            logging.getLogger("scout.jobs").warning("knowledge seed failed: %s", exc)
        try:
            async with db.job_run(job) as summary:
                summary.update(await fn())
            logging.getLogger("scout.jobs").info("%s done: %s", job, json.dumps(summary, default=str)[:4000])
        except Exception as exc:
            if alert:
                from scout.telegram import esc, send

                await send(f"⚠️ Scout 任务 <b>{job}</b> 失败：{esc(repr(exc)[:500])}")
            raise
        finally:
            await db.get_engine().dispose()

    try:
        asyncio.run(_main())
    except Exception:
        logging.getLogger("scout.jobs").exception("%s failed", job)
        sys.exit(1)
