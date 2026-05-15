"""Daily operational health summary."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


FRESHNESS_LIMITS_HOURS = {
    "qc_heartbeat": 2,
    "daily_feature_snapshot": 36,
    "yfinance_backfill": 36,
    "news_cache": 6,
    "memory_write": 36,
}


async def build_operational_health_snapshot() -> dict[str, Any]:
    """Read operational freshness and recent job health from the database."""
    from sqlalchemy import desc, select

    from db.models import (
        AgentAnalysis,
        CronRunLog,
        MacroNewsCache,
        MarketDailyFeature,
        MemoryDaily,
        QCSnapshot,
    )
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        heartbeat = (
            await db.execute(
                select(QCSnapshot)
                .where(QCSnapshot.packet_type == "heartbeat")
                .order_by(desc(QCSnapshot.received_at))
                .limit(1)
            )
        ).scalar_one_or_none()
        feature_snapshot = (
            await db.execute(
                select(QCSnapshot)
                .where(QCSnapshot.packet_type == "daily_feature_snapshot")
                .order_by(desc(QCSnapshot.received_at))
                .limit(1)
            )
        ).scalar_one_or_none()
        yfinance = (
            await db.execute(
                select(MarketDailyFeature)
                .where(MarketDailyFeature.source == "yfinance")
                .order_by(desc(MarketDailyFeature.updated_at))
                .limit(1)
            )
        ).scalar_one_or_none()
        news = (
            await db.execute(select(MacroNewsCache).where(MacroNewsCache.id == 1))
        ).scalar_one_or_none()
        memory = (
            await db.execute(
                select(MemoryDaily).order_by(desc(MemoryDaily.trading_date)).limit(1)
            )
        ).scalar_one_or_none()
        analysis = (
            await db.execute(
                select(AgentAnalysis).order_by(desc(AgentAnalysis.analyzed_at)).limit(1)
            )
        ).scalar_one_or_none()
        failed_crons = (
            await db.execute(
                select(CronRunLog)
                .where(CronRunLog.status == "failed")
                .order_by(desc(CronRunLog.started_at))
                .limit(5)
            )
        ).scalars().all()

    now = datetime.now(UTC).replace(tzinfo=None)
    checks = {
        "qc_heartbeat": _freshness_check(
            label="QC heartbeat",
            timestamp=getattr(heartbeat, "received_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["qc_heartbeat"],
            blocker=True,
            missing_blocker=True,
        ),
        "daily_feature_snapshot": _freshness_check(
            label="Daily features",
            timestamp=getattr(feature_snapshot, "received_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["daily_feature_snapshot"],
            blocker=False,
            missing_blocker=False,
        ),
        "yfinance_backfill": _freshness_check(
            label="YFinance backfill",
            timestamp=getattr(yfinance, "updated_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["yfinance_backfill"],
            blocker=False,
            missing_blocker=False,
        ),
        "news_cache": _freshness_check(
            label="News cache",
            timestamp=getattr(news, "updated_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["news_cache"],
            blocker=False,
            missing_blocker=False,
        ),
        "memory_write": _freshness_check(
            label="Memory write",
            timestamp=getattr(memory, "updated_at", None) or getattr(memory, "created_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["memory_write"],
            blocker=False,
            missing_blocker=False,
        ),
        "pipeline_status": {
            "label": "Pipeline",
            "status": getattr(analysis, "execution_status", None) or "unknown",
            "as_of": _iso_or_none(getattr(analysis, "analyzed_at", None)),
            "blocking": False,
        },
    }
    return classify_operational_health(checks, [_cron_to_dict(row) for row in failed_crons], now=now)


def classify_operational_health(
    checks: dict[str, dict[str, Any]],
    failed_crons: list[dict[str, Any]],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify checks into execution blockers and research degradations."""
    execution_blockers: list[str] = []
    research_degradations: list[str] = []

    for check in checks.values():
        if check.get("state") == "ok":
            continue
        message = f"{check.get('label')}: {check.get('reason')}"
        if check.get("blocking"):
            execution_blockers.append(message)
        else:
            research_degradations.append(message)

    recent_failures = failed_crons[:3]
    for row in recent_failures:
        research_degradations.append(
            f"cron {row.get('job_name')}: {str(row.get('error_message') or row.get('status'))[:80]}"
        )

    overall = "execution_blocked" if execution_blockers else "research_degraded" if research_degradations else "healthy"
    return {
        "generated_at": (now or datetime.now(UTC).replace(tzinfo=None)).isoformat(),
        "overall": overall,
        "checks": checks,
        "failed_crons": recent_failures,
        "execution_blockers": execution_blockers,
        "research_degradations": research_degradations,
    }


def format_operational_health_report(snapshot: dict[str, Any]) -> str:
    """Return a concise Telegram-friendly operational report."""
    overall = snapshot.get("overall", "unknown")
    title = {
        "healthy": "Ops health: healthy",
        "research_degraded": "Ops health: research degraded",
        "execution_blocked": "Ops health: execution blocked",
    }.get(overall, f"Ops health: {overall}")
    checks = snapshot.get("checks") or {}
    lines = [title]
    for key in ("qc_heartbeat", "daily_feature_snapshot", "yfinance_backfill", "news_cache", "memory_write"):
        check = checks.get(key) or {}
        age = check.get("age_hours")
        age_text = "missing" if age is None else f"{age:.1f}h"
        lines.append(f"- {check.get('label', key)}: {check.get('state', 'unknown')} ({age_text})")

    pipeline = checks.get("pipeline_status") or {}
    lines.append(f"- Pipeline: {pipeline.get('status', 'unknown')}")

    blockers = snapshot.get("execution_blockers") or []
    degradations = snapshot.get("research_degradations") or []
    if blockers:
        lines.append("Execution blockers: " + "; ".join(blockers[:2]))
    if degradations:
        lines.append("Research degradation: " + "; ".join(degradations[:3]))
    return "\n".join(lines[:12])


def _freshness_check(
    *,
    label: str,
    timestamp: Any,
    now: datetime,
    max_age_hours: int,
    blocker: bool,
    missing_blocker: bool,
) -> dict[str, Any]:
    parsed = _to_naive_datetime(timestamp)
    if parsed is None:
        return {
            "label": label,
            "state": "missing",
            "age_hours": None,
            "as_of": None,
            "reason": "missing",
            "blocking": missing_blocker,
        }
    age_hours = max((now - parsed).total_seconds() / 3600, 0.0)
    stale = age_hours > max_age_hours
    return {
        "label": label,
        "state": "stale" if stale else "ok",
        "age_hours": round(age_hours, 2),
        "as_of": parsed.isoformat(),
        "reason": f"stale {age_hours:.1f}h > {max_age_hours}h" if stale else "fresh",
        "blocking": blocker and stale,
    }


def _cron_to_dict(row: Any) -> dict[str, Any]:
    return {
        "job_name": getattr(row, "job_name", None),
        "status": getattr(row, "status", None),
        "started_at": _iso_or_none(getattr(row, "started_at", None)),
        "error_message": getattr(row, "error_message", None),
    }


def _to_naive_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _iso_or_none(value: Any) -> str | None:
    parsed = _to_naive_datetime(value)
    return parsed.isoformat() if parsed else None
