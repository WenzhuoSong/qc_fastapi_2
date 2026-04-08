# scheduler/runner.py
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from scheduler.jobs import (
    job_hourly_analysis,
    job_post_market_report,
    job_morning_health_check,
)

logger    = logging.getLogger("qc_fastapi_2.scheduler")
_scheduler = AsyncIOScheduler(timezone="America/New_York")


def start_scheduler():
    # 每小时整点运行分析（交易时段：09:30—16:00 ET）
    _scheduler.add_job(
        job_hourly_analysis,
        CronTrigger(
            hour="10-15",
            minute="0",
            day_of_week="mon-fri",
            timezone="America/New_York",
        ),
        id="hourly_analysis",
        name="Hourly Analysis Pipeline",
        max_instances=1,
        coalesce=True,
    )

    # 盘后日报：每个交易日 16:35 ET
    _scheduler.add_job(
        job_post_market_report,
        CronTrigger(
            hour="16", minute="35",
            day_of_week="mon-fri",
            timezone="America/New_York",
        ),
        id="post_market_report",
        name="Post Market Report",
        max_instances=1,
    )

    # 早间健康检查：09:00 ET
    _scheduler.add_job(
        job_morning_health_check,
        CronTrigger(
            hour="9", minute="0",
            day_of_week="mon-fri",
            timezone="America/New_York",
        ),
        id="morning_health",
        name="Morning Health Check",
        max_instances=1,
    )

    _scheduler.start()
    logger.info("Scheduler started. Jobs: hourly_analysis, post_market_report, morning_health")


def stop_scheduler():
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")
