# main.py
import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from db.session import init_db
from db.seed import seed_system_config
from api.webhook import router as webhook_router
from api.command import router as command_router
from api.status import router as status_router
from api.telegram_webhook import router as telegram_router
from api.execution import router as execution_router
from api.ops import router as ops_router
from api.observer import router as observer_router
from services.execution_authority import execution_authority_status
from services.qc_observer_projections import backfill_observer_projections

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动
    logger.info("QC FastAPI 2 starting up...")
    await init_db()
    await seed_system_config()
    projection_backfill = asyncio.create_task(backfill_observer_projections())
    logger.info("QC FastAPI 2 ready.")
    try:
        yield
    finally:
        if not projection_backfill.done():
            projection_backfill.cancel()
            with suppress(asyncio.CancelledError):
                await projection_backfill
        logger.info("QC FastAPI 2 shut down.")


app = FastAPI(
    title="QC FastAPI 2",
    description="QC Agentic Trading System",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhook_router, prefix="/api")
app.include_router(command_router, prefix="/api")
app.include_router(status_router,  prefix="/api")
app.include_router(telegram_router, prefix="/api")
app.include_router(execution_router, prefix="/api")
app.include_router(ops_router, prefix="/api")
app.include_router(observer_router, prefix="/api")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "1.0.0",
        "execution_fence": execution_authority_status(),
    }
