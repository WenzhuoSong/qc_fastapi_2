"""Off-hours weekend trading review cron.

This cron turns versioned validation/execution artifacts into deterministic
weekly metrics plus an explanatory, review-only summary. It is intentionally
separate from the live trading pipeline and never submits QC commands.

Usage: python -m cron.weekend_trading_review
"""
from __future__ import annotations

import asyncio
import inspect
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Awaitable, Callable

from services.json_safety import json_safe
from services.market_calendar import us_equity_market_status
from services.weekend_review_artifacts import (
    build_weekly_review_artifacts,
    serialize_weekly_review_artifact,
)
from services.weekend_review_loader import (
    EXECUTION_AUTHORITY,
    TARGET_WEIGHT_MUTATION,
    WeekendReviewDataset,
    load_weekend_review_dataset,
)
from services.weekend_review_metrics import build_weekly_review_metrics
from services.weekend_review_summarizer import summarize_weekend_review


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("qc_fastapi_2.cron.weekend_trading_review")


DatasetLoader = Callable[..., WeekendReviewDataset | dict[str, Any] | Awaitable[WeekendReviewDataset | dict[str, Any]]]
LLMComplete = Callable[[str], str | Awaitable[str]]
ArtifactPersister = Callable[[dict[str, Any]], Any | Awaitable[Any]]
Notifier = Callable[[dict[str, Any]], Any | Awaitable[Any]]


@dataclass
class WeekendTradingReviewCronResult:
    """Result envelope for the PR4 cron boundary."""

    status: str
    reason: str | None = None
    execution_authority: str = EXECUTION_AUTHORITY
    target_weight_mutation: str = TARGET_WEIGHT_MUTATION
    market_status: dict[str, Any] = field(default_factory=dict)
    week_start: str | None = None
    week_end: str | None = None
    artifact_count: int = 0
    persisted: bool = False
    persisted_ref: Any = None
    notified: bool = False
    metrics: dict[str, Any] = field(default_factory=dict)
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    summary_report: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return json_safe(asdict(self))


async def run_weekend_trading_review(
    *,
    week_start: date | None = None,
    week_end: date | None = None,
    now: datetime | None = None,
    allow_market_open: bool = False,
    notify: bool = False,
    persist: bool = True,
    dataset_limit: int = 500,
    dataset_loader: DatasetLoader = load_weekend_review_dataset,
    llm_complete: LLMComplete | None = None,
    artifact_persister: ArtifactPersister | None = None,
    notifier: Notifier | None = None,
) -> WeekendTradingReviewCronResult:
    """Run the review loop with injectable IO for tests.

    The function reads existing data, computes metrics, builds append-only
    review artifacts, optionally persists them, and optionally sends a compact
    Telegram-style notification. It does not import or call execution modules.
    """
    review_now = _ensure_utc(now or datetime.now(UTC))
    start, end = _week_window(review_now, week_start=week_start, week_end=week_end)
    market = us_equity_market_status(review_now).to_dict()

    if market.get("is_open") and not allow_market_open:
        return WeekendTradingReviewCronResult(
            status="skipped",
            reason="market_open",
            market_status=market,
            week_start=start.isoformat(),
            week_end=end.isoformat(),
        )

    dataset = await _maybe_await(dataset_loader(week_start=start, week_end=end, limit=dataset_limit))
    metrics = build_weekly_review_metrics(dataset, review_as_of=review_now)
    artifacts = build_weekly_review_artifacts(
        metrics,
        week_start=start,
        week_end=end,
        created_at=review_now,
    )
    artifact_payloads = [serialize_weekly_review_artifact(item) for item in artifacts]

    summary = await summarize_weekend_review(
        artifacts,
        llm_complete=llm_complete or _deterministic_summary_from_prompt,
        created_at=review_now,
    )
    review_payload = build_weekend_review_payload(
        metrics=metrics,
        artifacts=artifact_payloads,
        summary_report=summary,
        review_as_of=review_now,
        week_start=start,
        week_end=end,
        market_status=market,
    )

    persisted_ref = None
    if persist:
        if artifact_persister is None:
            persisted_ref = await persist_weekend_review_payload(review_payload)
        else:
            persisted_ref = await _maybe_await(artifact_persister(review_payload))

    notified = False
    if notify and notifier is not None:
        await _maybe_await(notifier({
            "text": format_weekend_review_telegram(review_payload),
            "parse_mode": "",
        }))
        notified = True

    return WeekendTradingReviewCronResult(
        status="success",
        reason=None,
        market_status=market,
        week_start=start.isoformat(),
        week_end=end.isoformat(),
        artifact_count=len(artifact_payloads),
        persisted=bool(persist),
        persisted_ref=persisted_ref,
        notified=notified,
        metrics=metrics,
        artifacts=artifact_payloads,
        summary_report=summary,
    )


def build_weekend_review_payload(
    *,
    metrics: dict[str, Any],
    artifacts: list[dict[str, Any]],
    summary_report: dict[str, Any],
    review_as_of: datetime,
    week_start: date,
    week_end: date,
    market_status: dict[str, Any],
) -> dict[str, Any]:
    """Build the append-only payload persisted by PR4."""
    return json_safe({
        "schema_version": "weekend_trading_review_cron_v1",
        "review_as_of": _ensure_utc(review_as_of).isoformat(),
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "market_status": market_status,
        "weekend_review_metrics": metrics,
        "weekend_review_artifacts": artifacts,
        "weekend_review_artifact_count": len(artifacts),
        "weekend_review_summary": summary_report,
    })


async def persist_weekend_review_payload(payload: dict[str, Any]) -> int:
    """Append a weekend review row to AgentAnalysis.

    This intentionally inserts a new row every run rather than updating a
    previous review row.
    """
    from db.models import AgentAnalysis
    from db.session import AsyncSessionLocal

    review_as_of = _parse_datetime(payload.get("review_as_of")) or datetime.now(UTC)
    analyzed_at = _ensure_utc(review_as_of).replace(tzinfo=None)
    async with AsyncSessionLocal() as db:
        row = AgentAnalysis(
            analyzed_at=analyzed_at,
            trigger_type="weekend_review",
            snapshot_ids=[],
            planner_output={
                "review_only": True,
                "execution_authority": EXECUTION_AUTHORITY,
                "target_weight_mutation": TARGET_WEIGHT_MUTATION,
            },
            researcher_output={
                "weekend_review_summary": payload.get("weekend_review_summary") or {},
            },
            allocator_output={},
            risk_output=payload,
            risk_approved=False,
            decision={
                "review_only": True,
                "execution_authority": EXECUTION_AUTHORITY,
                "target_weight_mutation": TARGET_WEIGHT_MUTATION,
            },
            execution_status="review_only",
            notes="weekend_trading_review",
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
        return int(row.id)


def format_weekend_review_telegram(payload: dict[str, Any]) -> str:
    """Compact operator message for review-only weekend output."""
    metrics = payload.get("weekend_review_metrics") if isinstance(payload.get("weekend_review_metrics"), dict) else {}
    sections = metrics.get("sections") if isinstance(metrics.get("sections"), dict) else {}
    execution = _section_metrics(sections, "execution_truth")
    intent = _section_metrics(sections, "intent_execution")
    labels = _section_metrics(sections, "label_maturity")
    hedge = _section_metrics(sections, "hedge_review")
    summary = payload.get("weekend_review_summary") if isinstance(payload.get("weekend_review_summary"), dict) else {}
    removed = int(summary.get("removed_forbidden_line_count") or 0)
    return "\n".join([
        "Weekend trading review",
        f"Week: {payload.get('week_start')} -> {payload.get('week_end')}",
        "execution_authority=none | target_weight_mutation=none",
        f"Artifacts: {payload.get('weekend_review_artifact_count', 0)}",
        (
            "Execution: "
            f"sent={execution.get('commands_sent', 0)} "
            f"filled={execution.get('filled_count', 0)} "
            f"noop={execution.get('noop_count', 0)} "
            f"stuck={execution.get('stuck_in_flight_count', 0)}"
        ),
        (
            "Intent blockers: "
            f"risk={intent.get('risk_block_count', 0)} "
            f"final={intent.get('final_validation_block_count', 0)} "
            f"preflight={intent.get('execution_preflight_block_count', 0)} "
            f"dedupe={intent.get('dedupe_count', 0)}"
        ),
        (
            "Labels: "
            f"eligible={labels.get('eligible_label_count', 0)} "
            f"fallback={labels.get('fallback_label_count', 0)} "
            f"immature_excluded={labels.get('excluded_immature_count', 0)}"
        ),
        (
            "Hedge: "
            f"triggered={hedge.get('hedge_trigger_count', 0)} "
            f"false_negative={hedge.get('false_negative_count', 0)} "
            f"missed_protection={hedge.get('missed_protection_count', 0)}"
        ),
        f"Sanitized forbidden lines: {removed}",
    ])


def build_ops_failure_message(exc: BaseException) -> str:
    return (
        "Weekend trading review failed (ops failure, no trading action attempted): "
        f"{type(exc).__name__}: {exc}"
    )


async def main() -> None:
    from services.cron_audit import audit_cron_run
    from tools.notify_tools import tool_send_telegram

    try:
        async with audit_cron_run("weekend_trading_review") as audit:
            config = await _read_config()
            if not _bool_value(config.get("enabled", True)):
                audit.mark_skipped("disabled_by_config")
                logger.info("Weekend trading review disabled by config")
                return

            result = await run_weekend_trading_review(
                week_start=_parse_date(config.get("week_start")),
                week_end=_parse_date(config.get("week_end")),
                allow_market_open=_bool_value(config.get("allow_market_open", False)),
                notify=_bool_value(config.get("notify", False)),
                persist=_bool_value(config.get("persist", True)),
                dataset_limit=int(config.get("dataset_limit") or 500),
                notifier=tool_send_telegram,
            )
            if result.status == "skipped":
                audit.mark_skipped(result.reason)
            audit.add_rows(result.artifact_count)
            audit.set_summary(**result.to_dict())
            logger.info(
                "Weekend trading review status=%s reason=%s artifacts=%s persisted=%s",
                result.status,
                result.reason,
                result.artifact_count,
                result.persisted,
            )
    except Exception as exc:
        logger.exception("Weekend trading review FAILED")
        try:
            from tools.notify_tools import tool_send_telegram

            await tool_send_telegram({
                "text": build_ops_failure_message(exc),
                "parse_mode": "",
            })
        except Exception:
            pass
        raise


async def _read_config() -> dict[str, Any]:
    from db.queries import get_system_config
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "weekend_trading_review_config")
    return (cfg.value if cfg else {}) or {"enabled": True}


def _deterministic_summary_from_prompt(prompt: str) -> str:
    """Fallback summary used by PR4 until a real LLM transport is wired."""
    return (
        "Execution truth, intent blockers, label maturity, hedge review, debate value, "
        "basket structure, regime/risk, and self-assessment metrics were computed by "
        "deterministic Python. Operator review only: inspect insufficient-sample metrics "
        "and blocker concentrations before making future manual changes."
    )


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _week_window(
    now: datetime,
    *,
    week_start: date | None,
    week_end: date | None,
) -> tuple[date, date]:
    if week_start and week_end:
        return week_start, week_end
    day = _ensure_utc(now).date()
    start = week_start or (day - timedelta(days=day.weekday()))
    end = week_end or (start + timedelta(days=6))
    return start, end


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _section_metrics(sections: dict[str, Any], name: str) -> dict[str, Any]:
    section = sections.get(name)
    if not isinstance(section, dict):
        return {}
    metrics = section.get("metrics")
    return metrics if isinstance(metrics, dict) else {}


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value.strip()[:10])
        except ValueError:
            return None
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


if __name__ == "__main__":
    asyncio.run(main())
