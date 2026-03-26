# main.py
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import get_settings
from db.session import init_db
from api.webhook import router as webhook_router
from api.command import router as command_router
from api.status import router as status_router
from api.telegram_webhook import router as telegram_router
from scheduler.runner import start_scheduler, stop_scheduler

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
    start_scheduler()
    logger.info("QC FastAPI 2 ready.")
    yield
    # 关闭
    stop_scheduler()
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


@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}
