"""Strategy health profiles built from Playground replay metrics."""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

MAX_HISTORY_POINTS = 12
MIN_HEALTH_SAMPLES = 10


def update_strategy_health_profiles(
    bundle: dict[str, Any],
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return updated strategy health profiles keyed by strategy/regime."""
    profiles = dict((existing or {}).get("profiles") or {})
    regime = str(bundle.get("regime_label") or "unknown")
    generated_at = str(bundle.get("generated_at") or datetime.now(UTC).isoformat())
    replay_metrics = bundle.get("replay_metrics") or {}

    for strategy in bundle.get("strategies") or []:
        name = strategy.get("strategy_name")
        if not name:
            continue
        metrics = replay_metrics.get(name) or {}
        key = f"{name}|{regime}"
        current = _health_point(strategy_name=name, regime=regime, metrics=metrics, generated_at=generated_at)
        old = profiles.get(key) or {}
        history = list(old.get("history") or [])
        history.append(current)
        history = history[-MAX_HISTORY_POINTS:]
        profiles[key] = {
            "strategy_name": name,
            "regime": regime,
            "latest": current,
            "history": history,
            "decay": _detect_profile_decay(history),
            "updated_at": generated_at,
        }

    decay_flags = [
        {
            "profile_key": key,
            "strategy_name": profile.get("strategy_name"),
            "regime": profile.get("regime"),
            **(profile.get("decay") or {}),
        }
        for key, profile in profiles.items()
        if (profile.get("decay") or {}).get("flagged")
    ]

    return {
        "profiles": profiles,
        "decay_flags": decay_flags,
        "updated_at": generated_at,
        "min_health_samples": MIN_HEALTH_SAMPLES,
        "max_history_points": MAX_HISTORY_POINTS,
        "parameter_adjustments": {
            "approval_required": True,
            "auto_apply": False,
            "suggestions": _build_parameter_suggestions(decay_flags),
        },
    }


async def persist_strategy_health_profiles(bundle: dict[str, Any]) -> dict[str, Any]:
    """Read, update, and persist strategy health profiles in system_config."""
    from db.queries import get_system_config, upsert_system_config
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        cfg = await get_system_config(db, "strategy_health_profiles")
        updated = update_strategy_health_profiles(bundle, cfg.value if cfg else None)
        await upsert_system_config(db, "strategy_health_profiles", updated, "strategy_health")
    return updated


def _health_point(
    *,
    strategy_name: str,
    regime: str,
    metrics: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    reliability = metrics.get("metric_reliability") or {}
    sample_size = int(metrics.get("n_forward_return_samples") or 0)
    return {
        "strategy_name": strategy_name,
        "regime": regime,
        "generated_at": generated_at,
        "rolling_ic": _float_or_none(metrics.get("ic")),
        "hit_rate": _float_or_none(metrics.get("hit_rate")),
        "avg_turnover": _float_or_none(metrics.get("avg_turnover")),
        "max_drawdown_pct": _float_or_none(metrics.get("max_drawdown_pct")),
        "sample_size": sample_size,
        "n_ic_samples": int(metrics.get("n_ic_samples") or 0),
        "metric_reliability": reliability.get("level", "unknown"),
    }


def _detect_profile_decay(history: list[dict[str, Any]]) -> dict[str, Any]:
    valid = [item for item in history if int(item.get("sample_size") or 0) >= MIN_HEALTH_SAMPLES]
    if len(valid) < 3:
        return {
            "flagged": False,
            "confidence": "low",
            "reason": f"insufficient health history ({len(valid)}/3)",
            "approval_required": True,
        }

    current = valid[-1]
    previous = valid[:-1]
    prev_ic = _avg(item.get("rolling_ic") for item in previous)
    prev_hit = _avg(item.get("hit_rate") for item in previous)
    prev_dd = _avg(item.get("max_drawdown_pct") for item in previous)
    reasons: list[str] = []

    if current.get("rolling_ic") is not None and prev_ic is not None:
        if float(current["rolling_ic"]) < prev_ic - 0.10:
            reasons.append(f"IC declined {prev_ic:.2f}->{float(current['rolling_ic']):.2f}")
    if current.get("hit_rate") is not None and prev_hit is not None:
        if float(current["hit_rate"]) < prev_hit - 0.12:
            reasons.append(f"hit_rate declined {prev_hit:.2f}->{float(current['hit_rate']):.2f}")
    if current.get("max_drawdown_pct") is not None and prev_dd is not None:
        if float(current["max_drawdown_pct"]) > prev_dd + 0.05:
            reasons.append(
                f"drawdown worsened {prev_dd:.2%}->{float(current['max_drawdown_pct']):.2%}"
            )

    flagged = len(reasons) >= 2
    return {
        "flagged": flagged,
        "confidence": "high" if len(valid) >= 6 and flagged else "medium" if flagged else "low",
        "reason": "; ".join(reasons) if reasons else "no strategy health decay detected",
        "approval_required": True,
        "suggested_action": (
            "review_strategy_parameters_approval_only" if flagged else None
        ),
        "latest_sample_size": current.get("sample_size"),
    }


def _build_parameter_suggestions(decay_flags: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "strategy_name": flag.get("strategy_name"),
            "regime": flag.get("regime"),
            "suggestion": "Review parameters or temporarily reduce advisory weight; do not auto-apply.",
            "reason": flag.get("reason"),
            "approval_required": True,
        }
        for flag in decay_flags
    ]


def _avg(values: Any) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return sum(clean) / len(clean) if clean else None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
