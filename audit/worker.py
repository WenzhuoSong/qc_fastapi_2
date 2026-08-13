"""Railway worker entrypoint for read-only observer audits."""
from __future__ import annotations

import asyncio
import logging
import os

from audit.service import run_observer_audit
from db.session import AsyncSessionLocal
from services.execution_authority import execution_authority_status


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("observer_audit_worker")


async def run_once() -> dict:
    fence = execution_authority_status()
    if fence.get("execution_authority") != "none" or fence.get("executable_commands_enabled"):
        raise RuntimeError("observer audit worker requires execution_authority=none")
    async with AsyncSessionLocal() as db:
        try:
            result = await run_observer_audit(db)
            await db.commit()
            return result
        except Exception:
            await db.rollback()
            raise


async def main() -> None:
    interval = max(15, int(os.getenv("QC_AUDIT_POLL_SECONDS", "60")))
    logger.info("observer audit worker started; execution authority is disabled")
    while True:
        try:
            result = await run_once()
            if result.get("status") != "unchanged":
                logger.info("observer audit result: %s", result)
        except Exception:
            logger.exception("observer audit cycle failed")
        await asyncio.sleep(interval)


if __name__ == "__main__":
    asyncio.run(main())
