"""Execution-evidence sample sources.

This module turns already-frozen, already-labeled signal outcomes into a
read-only paper-live evidence source for strategy certification. It does not
write labels and does not create an execution path.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from typing import Any, Iterable


EXECUTION_TRUSTED_SIGNAL_SOURCE = "fastapi_live_freeze"
DEFAULT_PAPER_LIVE_HORIZON_DAYS = 1
DEFAULT_PAPER_LIVE_ACTIONS = ("increase",)
BAD_DATA_QUALITIES = {"missing", "stale", "degraded", "bad", "error"}


def build_paper_live_outcome_metrics(
    outcomes: Iterable[Any],
    *,
    signal_source: str = EXECUTION_TRUSTED_SIGNAL_SOURCE,
    horizon_days: int = DEFAULT_PAPER_LIVE_HORIZON_DAYS,
    actions: Iterable[str] = DEFAULT_PAPER_LIVE_ACTIONS,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """Aggregate trusted paper-live outcomes by strategy.

    Only fastapi live-freeze outcomes are trusted for execution evidence. Broad
    historical replay sources remain useful for review, but are intentionally
    excluded here so historical paper performance cannot silently unlock live
    buying.
    """

    allowed_source = str(signal_source or EXECUTION_TRUSTED_SIGNAL_SOURCE)
    allowed_horizon = int(horizon_days or DEFAULT_PAPER_LIVE_HORIZON_DAYS)
    allowed_actions = {
        str(action).strip().lower()
        for action in (actions or DEFAULT_PAPER_LIVE_ACTIONS)
        if str(action).strip()
    }
    target_date = as_of_date or datetime.now(timezone.utc).date()

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    skipped: Counter[str] = Counter()
    seen = 0
    for raw in outcomes:
        seen += 1
        source = str(_record_get(raw, "signal_source") or "")
        if source != allowed_source:
            skipped["unsupported_signal_source"] += 1
            continue
        if int(_to_float(_record_get(raw, "horizon_days"), 0) or 0) != allowed_horizon:
            skipped["unsupported_horizon"] += 1
            continue
        label_date = _parse_date(_record_get(raw, "label_date"))
        if label_date is None or label_date > target_date:
            skipped["immature_or_missing_label_date"] += 1
            continue
        action = str(_record_get(raw, "action") or "").strip().lower()
        if allowed_actions and action not in allowed_actions:
            skipped["unsupported_action"] += 1
            continue
        data_quality = str(_record_get(raw, "data_quality") or "").strip().lower()
        if data_quality in BAD_DATA_QUALITIES:
            skipped["bad_data_quality"] += 1
            continue
        strategy_id = str(_record_get(raw, "strategy_id") or "").strip()
        if not strategy_id:
            skipped["missing_strategy_id"] += 1
            continue
        grouped[strategy_id].append(
            {
                "excess_vs_spy": _to_float(_record_get(raw, "excess_vs_spy"), 0.0) or 0.0,
                "hit": _to_bool(_record_get(raw, "hit")),
                "signal_date": _parse_date(_record_get(raw, "signal_date")),
                "label_date": label_date,
                "action": action,
                "data_quality": data_quality or "unknown",
            }
        )

    items: dict[str, dict[str, Any]] = {}
    for strategy_id, rows in grouped.items():
        count = len(rows)
        if count <= 0:
            continue
        excess_values = [float(row["excess_vs_spy"]) for row in rows]
        hits = [bool(row["hit"]) for row in rows]
        signal_dates = [row["signal_date"] for row in rows if row.get("signal_date") is not None]
        label_dates = [row["label_date"] for row in rows if row.get("label_date") is not None]
        action_counts = Counter(str(row.get("action") or "unknown") for row in rows)
        quality_counts = Counter(str(row.get("data_quality") or "unknown") for row in rows)
        items[strategy_id] = {
            "strategy_name": strategy_id,
            "n_forward_return_samples": count,
            "horizon_days": allowed_horizon,
            "signal_source": allowed_source,
            "trusted_for_execution_evidence": True,
            "sample_source": f"paper_live:{allowed_source}:h{allowed_horizon}",
            "metric_reliability": {"level": _reliability_level(count)},
            "avg_excess_vs_spy": round(sum(excess_values) / count, 6),
            "hit_rate": round(sum(1 for hit in hits if hit) / count, 6),
            "signal_date_min": min(signal_dates).isoformat() if signal_dates else None,
            "signal_date_max": max(signal_dates).isoformat() if signal_dates else None,
            "label_date_min": min(label_dates).isoformat() if label_dates else None,
            "label_date_max": max(label_dates).isoformat() if label_dates else None,
            "action_counts": dict(sorted(action_counts.items())),
            "data_quality_counts": dict(sorted(quality_counts.items())),
            "execution_authority": "none",
            "target_weight_mutation": "none",
        }

    return {
        "schema_version": "paper_live_strategy_execution_evidence_v1",
        "enabled": True,
        "trusted_signal_source": allowed_source,
        "horizon_days": allowed_horizon,
        "actions": sorted(allowed_actions),
        "as_of_date": target_date.isoformat(),
        "items": dict(sorted(items.items())),
        "summary": {
            "outcomes_seen": seen,
            "strategy_count": len(items),
            "sample_count": sum(item["n_forward_return_samples"] for item in items.values()),
            "skipped": dict(sorted(skipped.items())),
        },
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


async def load_paper_live_outcome_metrics(
    db: Any,
    *,
    signal_source: str = EXECUTION_TRUSTED_SIGNAL_SOURCE,
    horizon_days: int = DEFAULT_PAPER_LIVE_HORIZON_DAYS,
    actions: Iterable[str] = DEFAULT_PAPER_LIVE_ACTIONS,
    as_of_date: date | None = None,
    row_limit: int = 20000,
) -> dict[str, Any]:
    """Load trusted paper-live outcomes from DB and aggregate them."""

    from sqlalchemy import desc, select

    from db.models import StrategySignalOutcome

    target_date = as_of_date or datetime.now(timezone.utc).date()
    result = await db.execute(
        select(StrategySignalOutcome)
        .where(StrategySignalOutcome.signal_source == str(signal_source or EXECUTION_TRUSTED_SIGNAL_SOURCE))
        .where(StrategySignalOutcome.horizon_days == int(horizon_days or DEFAULT_PAPER_LIVE_HORIZON_DAYS))
        .where(StrategySignalOutcome.label_date <= target_date)
        .order_by(desc(StrategySignalOutcome.label_date), desc(StrategySignalOutcome.id))
        .limit(int(row_limit))
    )
    return build_paper_live_outcome_metrics(
        result.scalars().all(),
        signal_source=signal_source,
        horizon_days=horizon_days,
        actions=actions,
        as_of_date=target_date,
    )


def disabled_paper_live_outcome_metrics(reason: str) -> dict[str, Any]:
    return {
        "schema_version": "paper_live_strategy_execution_evidence_v1",
        "enabled": False,
        "reason": str(reason),
        "items": {},
        "summary": {"outcomes_seen": 0, "strategy_count": 0, "sample_count": 0, "skipped": {}},
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def _reliability_level(sample_count: int) -> str:
    if sample_count >= 30:
        return "high"
    if sample_count >= 5:
        return "medium"
    if sample_count > 0:
        return "insufficient"
    return "unknown"


def _record_get(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        return value.get(key)
    return getattr(value, key, None)


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y"}:
            return True
        if text in {"0", "false", "no", "n"}:
            return False
    return bool(value)
