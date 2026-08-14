"""Minimal pre-market QC observer health notification."""
from __future__ import annotations

import asyncio

from db.session import AsyncSessionLocal
from services.cron_audit import audit_cron_run
from services.qc_observer_health import format_current_observer_report, load_current_observer_summary
from services.qc_observer_projections import load_observer_projection_status
from tools.notify_tools import tool_send_telegram


async def main() -> None:
    async with audit_cron_run("morning_health") as audit:
        observer = await load_current_observer_summary()
        async with AsyncSessionLocal() as db:
            projection = await load_observer_projection_status(db)
        text = "\n".join(
            [
                "noble-flow morning health",
                "Execution authority: NONE",
                "",
                format_current_observer_report(observer),
                "",
                f"Projection pending: {projection.get('pending_parent_count')}",
                f"Projection status: {projection.get('checkpoint_status')}",
            ]
        )
        await tool_send_telegram({"text": text, "parse_mode": ""})
        audit.set_summary(
            observer_status=observer.get("status"),
            projection_status=projection.get("checkpoint_status"),
            projection_pending=projection.get("pending_parent_count"),
            execution_authority="none",
        )


if __name__ == "__main__":
    asyncio.run(main())
