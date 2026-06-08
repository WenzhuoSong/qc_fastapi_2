"""Daily operational health summary."""
from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from services.market_calendar import is_us_equity_trading_day, us_equity_holiday_name


FRESHNESS_LIMITS_HOURS = {
    "qc_heartbeat": 2,
    "daily_feature_snapshot": 36,
    "yfinance_backfill": 36,
    "news_cache": 6,
    "memory_write": 36,
}
FAILED_CRON_LOOKBACK_HOURS = 48
NEWS_CRON_SCHEDULE = "50 */2 * * * UTC"
NEWS_CRON_INTERVAL_HOURS = 2
NEWS_CRON_MINUTE_UTC = 50
NEWS_CRON_ALLOWED_MISSED_RUNS = 2
YFINANCE_CORE_HEALTH_FIELDS = (
    "close_price",
    "return_1d",
    "return_20d",
    "hist_vol_20d",
    "rsi_14",
    "atr_pct",
)
YFINANCE_LONG_HISTORY_FIELDS = ("return_60d", "return_252d", "sma_200", "beta_vs_spy")

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
HEARTBEAT_STRICT_AFTER = time(10, 0)
MARKET_CLOSE = time(16, 0)


async def build_operational_health_snapshot() -> dict[str, Any]:
    """Read operational freshness and recent job health from the database."""
    from sqlalchemy import desc, func, select

    from db.models import (
        AgentAnalysis,
        CronRunLog,
        ExecutionLog,
        MacroNewsCache,
        MarketDailyFeature,
        MemoryDaily,
        QCSnapshot,
    )
    from db.session import AsyncSessionLocal
    from services.execution_policy import TICKER_ROLES, TickerRole

    now = datetime.now(UTC).replace(tzinfo=None)
    failed_cron_cutoff = now - timedelta(hours=FAILED_CRON_LOOKBACK_HOURS)

    health_universe = sorted(
        ticker
        for ticker, role in TICKER_ROLES.items()
        if role not in {TickerRole.WATCHLIST, TickerRole.UNKNOWN}
    )

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
        yfinance_rows = (
            await db.execute(
                select(MarketDailyFeature)
                .where(MarketDailyFeature.source == "yfinance")
                .where(MarketDailyFeature.ticker.in_(health_universe))
                .order_by(
                    MarketDailyFeature.ticker,
                    desc(MarketDailyFeature.trading_date),
                    desc(MarketDailyFeature.updated_at),
                )
            )
        ).scalars().all()
        yfinance_stats_rows = (
            await db.execute(
                select(
                    MarketDailyFeature.ticker,
                    func.count(MarketDailyFeature.id),
                    func.min(MarketDailyFeature.trading_date),
                    func.max(MarketDailyFeature.trading_date),
                )
                .where(MarketDailyFeature.source == "yfinance")
                .where(MarketDailyFeature.ticker.in_(health_universe))
                .group_by(MarketDailyFeature.ticker)
            )
        ).all()
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
        execution = (
            await db.execute(
                select(ExecutionLog).order_by(desc(ExecutionLog.executed_at)).limit(1)
            )
        ).scalar_one_or_none()
        failed_crons = (
            await db.execute(
                select(CronRunLog)
                .where(CronRunLog.status == "failed")
                .where(CronRunLog.started_at >= failed_cron_cutoff)
                .order_by(desc(CronRunLog.started_at))
                .limit(5)
            )
        ).scalars().all()

    yfinance_ticker_health = _yfinance_ticker_health_check(
        universe=health_universe,
        latest_rows=_latest_yfinance_rows_by_ticker(yfinance_rows),
        stats_by_ticker={
            str(ticker): {
                "row_count": int(count or 0),
                "first_date": first_date,
                "latest_date": latest_date,
            }
            for ticker, count, first_date, latest_date in yfinance_stats_rows
        },
        now=now,
    )

    checks = {
        "qc_heartbeat": _heartbeat_freshness_check(
            label="QC heartbeat",
            timestamp=getattr(heartbeat, "received_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["qc_heartbeat"],
            blocker=True,
            missing_blocker=True,
        ),
        "daily_feature_snapshot": _trading_day_freshness_check(
            label="Daily features",
            timestamp=getattr(feature_snapshot, "received_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["daily_feature_snapshot"],
            blocker=False,
            missing_blocker=False,
        ),
        "yfinance_backfill": _trading_day_freshness_check(
            label="YFinance backfill",
            timestamp=getattr(yfinance, "updated_at", None),
            now=now,
            max_age_hours=FRESHNESS_LIMITS_HOURS["yfinance_backfill"],
            blocker=False,
            missing_blocker=False,
        ),
        "yfinance_ticker_health": yfinance_ticker_health,
        "news_cache": news_cache_freshness_check(
            timestamp=getattr(news, "updated_at", None),
            now=now,
            blocker=False,
            missing_blocker=False,
        ),
        "memory_write": _trading_day_freshness_check(
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
    snapshot = classify_operational_health(checks, [_cron_to_dict(row) for row in failed_crons], now=now)
    snapshot["execution"] = _execution_to_dict(execution)
    return snapshot


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
        state = check.get("state")
        if state is None:
            continue
        if state == "ok":
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
        age_text = "never updated" if age is None else f"{age:.1f}h"
        lines.append(f"- {check.get('label', key)}: {check.get('state', 'unknown')} ({age_text})")
    ticker_health = checks.get("yfinance_ticker_health") or {}
    if ticker_health:
        lines.append(
            "- YFinance ETF health: "
            f"{ticker_health.get('state', 'unknown')} "
            f"({ticker_health.get('ok_count', 0)}/{ticker_health.get('ticker_count', 0)} ok, "
            f"issues={ticker_health.get('issue_count', 0)})"
        )

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


def _news_cache_freshness_check(
    *,
    label: str,
    timestamp: Any,
    now: datetime,
    max_age_hours: int,
    blocker: bool,
    missing_blocker: bool,
) -> dict[str, Any]:
    """Freshness check for the 24/7 pre_fetch_news external event stream."""
    check = _freshness_check(
        label=label,
        timestamp=timestamp,
        now=now,
        max_age_hours=max_age_hours,
        blocker=blocker,
        missing_blocker=missing_blocker,
    )
    latest_expected = _latest_scheduled_utc_run(now)
    next_expected = latest_expected + timedelta(hours=NEWS_CRON_INTERVAL_HOURS)
    check.update({
        "freshness_policy": "24_7_event_stream",
        "expected_schedule": NEWS_CRON_SCHEDULE,
        "latest_expected_run_at": latest_expected.isoformat(),
        "next_expected_run_at": next_expected.isoformat(),
        "allowed_missed_runs": NEWS_CRON_ALLOWED_MISSED_RUNS,
    })

    parsed = _to_naive_datetime(timestamp)
    if parsed is None:
        check["missed_scheduled_runs"] = None
        return check

    missed_runs = _missed_scheduled_runs_since(parsed, now)
    stale_by_schedule = missed_runs > NEWS_CRON_ALLOWED_MISSED_RUNS
    check["missed_scheduled_runs"] = missed_runs
    if stale_by_schedule:
        check.update({
            "state": "stale",
            "reason": (
                f"missed {missed_runs} scheduled news runs "
                f"> allowed {NEWS_CRON_ALLOWED_MISSED_RUNS}"
            ),
            "blocking": blocker,
        })
    elif check.get("state") == "ok":
        check["reason"] = "fresh: within 24/7 news schedule"
    return check


def news_cache_freshness_check(
    *,
    timestamp: Any,
    now: datetime | None = None,
    blocker: bool = False,
    missing_blocker: bool = False,
) -> dict[str, Any]:
    """Public 24/7 news freshness contract used by health and market brief."""
    return _news_cache_freshness_check(
        label="News cache",
        timestamp=timestamp,
        now=now or datetime.now(UTC).replace(tzinfo=None),
        max_age_hours=FRESHNESS_LIMITS_HOURS["news_cache"],
        blocker=blocker,
        missing_blocker=missing_blocker,
    )


def _latest_scheduled_utc_run(now: datetime) -> datetime:
    """Return the most recent expected pre_fetch_news run for a 2h UTC schedule."""
    current = _to_naive_datetime(now) or datetime.now(UTC).replace(tzinfo=None)
    scheduled_hour = current.hour - (current.hour % NEWS_CRON_INTERVAL_HOURS)
    candidate = current.replace(
        hour=scheduled_hour,
        minute=NEWS_CRON_MINUTE_UTC,
        second=0,
        microsecond=0,
    )
    if candidate > current:
        candidate -= timedelta(hours=NEWS_CRON_INTERVAL_HOURS)
    return candidate


def _next_scheduled_utc_run_after(timestamp: datetime) -> datetime:
    """Return the first expected pre_fetch_news run strictly after timestamp."""
    current = _to_naive_datetime(timestamp) or timestamp
    latest = _latest_scheduled_utc_run(current)
    if latest <= current:
        return latest + timedelta(hours=NEWS_CRON_INTERVAL_HOURS)
    return latest


def _missed_scheduled_runs_since(timestamp: datetime, now: datetime) -> int:
    latest_expected = _latest_scheduled_utc_run(now)
    first_missed = _next_scheduled_utc_run_after(timestamp)
    if first_missed > latest_expected:
        return 0
    elapsed = latest_expected - first_missed
    return int(elapsed.total_seconds() // (NEWS_CRON_INTERVAL_HOURS * 3600)) + 1


def _heartbeat_freshness_check(
    *,
    label: str,
    timestamp: Any,
    now: datetime,
    max_age_hours: int,
    blocker: bool,
    missing_blocker: bool,
) -> dict[str, Any]:
    check = _freshness_check(
        label=label,
        timestamp=timestamp,
        now=now,
        max_age_hours=max_age_hours,
        blocker=blocker,
        missing_blocker=missing_blocker,
    )
    if check.get("state") == "ok":
        return check

    market_now = now.replace(tzinfo=UTC).astimezone(MARKET_TZ)
    market_time = market_now.time()
    in_strict_market = (
        is_us_equity_trading_day(market_now.date())
        and HEARTBEAT_STRICT_AFTER <= market_time <= MARKET_CLOSE
    )
    if in_strict_market:
        return check

    if not is_us_equity_trading_day(market_now.date()):
        holiday = us_equity_holiday_name(market_now.date())
        reason = f"market closed: {holiday}" if holiday else "market closed"
    elif market_time > MARKET_CLOSE or market_time < MARKET_OPEN:
        reason = "market closed"
    else:
        reason = "opening grace"
    check.update({
        "state": "ok",
        "reason": reason,
        "blocking": False,
        "market_status": reason,
    })
    return check


def _trading_day_freshness_check(
    *,
    label: str,
    timestamp: Any,
    now: datetime,
    max_age_hours: int,
    blocker: bool,
    missing_blocker: bool,
) -> dict[str, Any]:
    """Freshness check for daily research jobs that do not update on weekends."""
    check = _freshness_check(
        label=label,
        timestamp=timestamp,
        now=now,
        max_age_hours=max_age_hours,
        blocker=blocker,
        missing_blocker=missing_blocker,
    )
    if check.get("state") != "stale":
        return check

    parsed = _to_naive_datetime(timestamp)
    if parsed is None:
        return check

    market_now = now.replace(tzinfo=UTC).astimezone(MARKET_TZ)
    timestamp_market_date = parsed.replace(tzinfo=UTC).astimezone(MARKET_TZ).date()
    expected_date = _latest_expected_daily_research_date(market_now)
    if timestamp_market_date >= expected_date:
        check.update({
            "state": "ok",
            "reason": "trading calendar grace",
            "blocking": False,
            "market_status": "trading calendar grace",
            "expected_research_date": expected_date.isoformat(),
        })
    return check


def _latest_yfinance_rows_by_ticker(rows: list[Any]) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    for row in rows:
        ticker = str(getattr(row, "ticker", "") or "").upper().strip()
        if ticker and ticker not in latest:
            latest[ticker] = row
    return latest


def _yfinance_ticker_health_check(
    *,
    universe: list[str],
    latest_rows: dict[str, Any],
    stats_by_ticker: dict[str, dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    market_now = now.replace(tzinfo=UTC).astimezone(MARKET_TZ)
    expected_date = _latest_expected_daily_research_date(market_now)
    rows: list[dict[str, Any]] = []
    issue_count = 0
    ok_count = 0
    insufficient_history_count = 0

    for ticker in sorted({str(item).upper().strip() for item in universe if item}):
        row = latest_rows.get(ticker)
        stats = stats_by_ticker.get(ticker) or {}
        if row is None:
            rows.append({
                "ticker": ticker,
                "state": "missing",
                "reason": "missing_yfinance_row",
                "trading_date": None,
                "row_count": int(stats.get("row_count") or 0),
                "missing_core_fields": list(YFINANCE_CORE_HEALTH_FIELDS),
                "missing_long_history_fields": list(YFINANCE_LONG_HISTORY_FIELDS),
                "history_status": "missing",
            })
            issue_count += 1
            continue

        trading_date = getattr(row, "trading_date", None)
        missing_core = [
            field for field in YFINANCE_CORE_HEALTH_FIELDS
            if getattr(row, field, None) is None
        ]
        missing_long = [
            field for field in YFINANCE_LONG_HISTORY_FIELDS
            if getattr(row, field, None) is None
        ]
        quality = str(getattr(row, "data_quality_flag", "") or "ok").lower()
        row_count = int(stats.get("row_count") or 0)
        stale = bool(trading_date and trading_date < expected_date)
        if trading_date is None:
            state = "missing"
            reason = "missing_trading_date"
        elif stale:
            state = "stale"
            reason = f"latest {trading_date.isoformat()} < expected {expected_date.isoformat()}"
        elif quality not in {"ok", "none", ""}:
            state = "degraded"
            reason = f"data_quality_flag={quality}"
        elif missing_core:
            state = "degraded"
            reason = "missing_core_fields:" + ",".join(missing_core)
        else:
            state = "ok"
            reason = "ready"
            ok_count += 1

        if missing_long:
            history_status = "insufficient_history" if row_count < 260 else "missing_long_history_fields"
            insufficient_history_count += 1 if history_status == "insufficient_history" else 0
        else:
            history_status = "long_history_ready"

        if state != "ok":
            issue_count += 1

        rows.append({
            "ticker": ticker,
            "state": state,
            "reason": reason,
            "trading_date": trading_date.isoformat() if isinstance(trading_date, date) else None,
            "expected_date": expected_date.isoformat(),
            "row_count": row_count,
            "first_date": _date_iso(stats.get("first_date")),
            "missing_core_fields": missing_core,
            "missing_long_history_fields": missing_long,
            "history_status": history_status,
            "data_quality_flag": quality,
        })

    state = "ok" if issue_count == 0 else "degraded"
    sample_issues = [row for row in rows if row["state"] != "ok"][:8]
    return {
        "label": "YFinance ETF health",
        "state": state,
        "reason": (
            "all tracked ETFs have current core yfinance fields"
            if issue_count == 0
            else f"{issue_count} ETF yfinance issues"
        ),
        "blocking": False,
        "ticker_count": len(rows),
        "ok_count": ok_count,
        "issue_count": issue_count,
        "insufficient_history_count": insufficient_history_count,
        "expected_research_date": expected_date.isoformat(),
        "sample_issues": sample_issues,
        "rows": rows,
    }


def _date_iso(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    return None


def _latest_expected_daily_research_date(market_now: datetime):
    """Return the latest trading date daily research data should cover by now."""
    current_date = market_now.date()
    current_time = market_now.time()

    if is_us_equity_trading_day(current_date) and current_time >= MARKET_CLOSE:
        return current_date

    days_back = 1
    while True:
        candidate = current_date - timedelta(days=days_back)
        if is_us_equity_trading_day(candidate):
            return candidate
        days_back += 1


def _cron_to_dict(row: Any) -> dict[str, Any]:
    return {
        "job_name": getattr(row, "job_name", None),
        "status": getattr(row, "status", None),
        "started_at": _iso_or_none(getattr(row, "started_at", None)),
        "error_message": getattr(row, "error_message", None),
    }


def _execution_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {"available": False}
    return {
        "available": True,
        "analysis_id": getattr(row, "analysis_id", None),
        "executed_at": _iso_or_none(getattr(row, "executed_at", None)),
        "command_type": getattr(row, "command_type", None),
        "status": getattr(row, "status", None),
        "retry_count": getattr(row, "retry_count", None),
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
