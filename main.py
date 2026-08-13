"""Minimal read-only companion service for the QuantConnect live algorithm."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from api.observer import router as observer_router
from audit.worker import main as audit_worker_main
from services.execution_authority import execution_authority_status
from services.qc_observer_projections import backfill_observer_projections


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("noble_flow")


@asynccontextmanager
async def lifespan(_: FastAPI):
    projection_backfill = asyncio.create_task(backfill_observer_projections())
    audit_worker = asyncio.create_task(audit_worker_main())
    logger.info("noble-flow observer ready; execution authority is disabled")
    try:
        yield
    finally:
        if not projection_backfill.done():
            projection_backfill.cancel()
            with suppress(asyncio.CancelledError):
                await projection_backfill
        if not audit_worker.done():
            audit_worker.cancel()
            with suppress(asyncio.CancelledError):
                await audit_worker


app = FastAPI(
    title="noble-flow",
    description="Read-only QuantConnect observer and evidence service",
    version="2.0.0-minimal",
    lifespan=lifespan,
)
app.include_router(observer_router, prefix="/api")


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": "2.0.0-minimal",
        "role": "qc_observer_only",
        "execution_fence": execution_authority_status(),
    }
