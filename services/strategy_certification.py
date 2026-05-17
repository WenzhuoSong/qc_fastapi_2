"""
Strategy certification MVP.

Turns Playground/evidence-bundle strategy evidence into a stable, auditable
status. This is a stateless certification snapshot, not a promotion state
machine and not an execution engine.
"""
from __future__ import annotations

from typing import Any


MIN_HISTORICAL_SAMPLES = 120
MIN_LIVE_SAMPLES = 20
MAX_ADVISORY_TURNOVER = 0.50


def certify_strategies(strategy_evidence: dict[str, Any] | None) -> dict[str, Any]:
    evidence = strategy_evidence or {}
    rows = evidence.get("strategy_results") or []
    certifications: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("strategy_name") or "")
        if not name:
            continue
        certifications[name] = _certify_one(row=row, evidence=evidence)

    summary = _summary(certifications)
    return {
        "items": certifications,
        "summary": summary,
        "policy": {
            "min_historical_samples": MIN_HISTORICAL_SAMPLES,
            "min_live_samples": MIN_LIVE_SAMPLES,
            "max_advisory_turnover": MAX_ADVISORY_TURNOVER,
            "certified_status_deferred": True,
        },
    }


def _certify_one(*, row: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    name = str(row.get("strategy_name") or "")
    historical_samples = int(_to_float(row.get("historical_forward_return_samples"), 0) or 0)
    live_samples = int(_to_float(row.get("n_forward_return_samples"), 0) or 0)
    turnover = _to_float(row.get("turnover"), 0.0) or 0.0
    sharpe = _to_float(row.get("historical_sharpe"), None)
    hit_rate = _to_float(row.get("historical_hit_rate"), None)
    data_ready = bool(row.get("data_ready"))
    can_influence = bool(row.get("can_influence_allocation"))
    suggested_use = str(row.get("suggested_use") or "watch_only")
    confidence = _to_float(row.get("confidence_score"), None)
    evidence_summary = evidence.get("evidence_summary") or {}
    reason_codes = _unique(list(row.get("reason_codes") or []))
    live_fit = _strategy_live_fit(row=row, evidence_summary=evidence_summary)
    historical_evidence = _strategy_historical_evidence(row=row, evidence_summary=evidence_summary)

    blockers: list[str] = []
    demotion_reasons: list[str] = []
    if not data_ready or not can_influence:
        blockers.append("data_not_ready")
    if historical_samples < MIN_HISTORICAL_SAMPLES:
        blockers.append("historical_samples_insufficient")
    if sharpe is not None and sharpe <= 0:
        blockers.append("historical_sharpe_nonpositive")
    if live_samples < MIN_LIVE_SAMPLES:
        blockers.append("live_samples_insufficient")
    if live_fit in {"conflicted"}:
        demotion_reasons.append("live_fit_conflicted")
    if turnover > MAX_ADVISORY_TURNOVER:
        demotion_reasons.append("turnover_high")
    if suggested_use in {"ignore"}:
        demotion_reasons.append("strategy_use_ignore")

    status = _status(
        data_ready=data_ready,
        can_influence=can_influence,
        historical_samples=historical_samples,
        historical_evidence=historical_evidence,
        live_samples=live_samples,
        live_fit=live_fit,
        turnover=turnover,
        suggested_use=suggested_use,
        sharpe=sharpe,
    )

    return {
        "strategy_name": name,
        "status": status,
        "approved_use": _approved_use(status),
        "suggested_use": suggested_use,
        "confidence_score": confidence,
        "historical": {
            "samples": historical_samples,
            "evidence": historical_evidence,
            "sharpe": sharpe,
            "hit_rate": hit_rate,
        },
        "live": {
            "samples": live_samples,
            "fit": live_fit,
        },
        "turnover": turnover,
        "promotion_blockers": _unique(blockers),
        "demotion_reasons": _unique(demotion_reasons),
        "reason_codes": reason_codes,
    }


def _strategy_live_fit(*, row: dict[str, Any], evidence_summary: dict[str, Any]) -> str:
    reason_codes = set(row.get("reason_codes") or [])
    live_samples = int(_to_float(row.get("n_forward_return_samples"), 0) or 0)
    reliability = str(((row.get("metric_reliability") or {}).get("level")) or "")
    summary_fit = str(evidence_summary.get("live_fit") or "unknown")
    if "consensus_regime_conflict" in reason_codes:
        return "conflicted"
    if "live_qc_supported" in reason_codes or reliability == "high":
        return "aligned"
    if not reason_codes and summary_fit != "unknown":
        return summary_fit
    if live_samples > 0 or "live_qc_limited" in reason_codes:
        return "insufficient"
    if "live_qc_missing" in reason_codes:
        return "insufficient"
    return summary_fit


def _strategy_historical_evidence(*, row: dict[str, Any], evidence_summary: dict[str, Any]) -> str:
    reason_codes = set(row.get("reason_codes") or [])
    historical_samples = int(_to_float(row.get("historical_forward_return_samples"), 0) or 0)
    reliability = str(((row.get("historical_metric_reliability") or {}).get("level")) or "")
    if "historical_strong" in reason_codes:
        return "strong"
    if reliability == "high" and historical_samples >= MIN_HISTORICAL_SAMPLES:
        return "strong"
    if historical_samples >= MIN_HISTORICAL_SAMPLES:
        return "medium"
    if historical_samples > 0:
        return "weak"
    return str(evidence_summary.get("historical_evidence") or "unknown")


def _status(
    *,
    data_ready: bool,
    can_influence: bool,
    historical_samples: int,
    historical_evidence: str,
    live_samples: int,
    live_fit: str,
    turnover: float,
    suggested_use: str,
    sharpe: float | None,
) -> str:
    if not data_ready or not can_influence or suggested_use == "ignore":
        return "disabled"
    if historical_samples < MIN_HISTORICAL_SAMPLES:
        return "experimental"
    if sharpe is not None and sharpe <= 0:
        return "experimental"
    historical_supported = historical_evidence in {"strong", "medium", "historical_supported", "unknown"}
    if not historical_supported:
        return "experimental"
    if (
        suggested_use == "advisory"
        and live_samples >= MIN_LIVE_SAMPLES
        and live_fit == "aligned"
        and turnover <= MAX_ADVISORY_TURNOVER
    ):
        return "advisory"
    return "research_supported"


def _approved_use(status: str) -> str:
    if status == "advisory":
        return "advisory"
    if status == "research_supported":
        return "research_only"
    return "none"


def _summary(certifications: dict[str, dict[str, Any]]) -> dict[str, Any]:
    counts = {
        "experimental": 0,
        "research_supported": 0,
        "advisory": 0,
        "disabled": 0,
    }
    for row in certifications.values():
        status = str(row.get("status") or "experimental")
        counts[status] = counts.get(status, 0) + 1
    best = None
    advisory = [row for row in certifications.values() if row.get("status") == "advisory"]
    if advisory:
        best = sorted(advisory, key=lambda row: float(row.get("confidence_score") or 0.0), reverse=True)[0]
    return {
        "counts": counts,
        "best_advisory": best,
        "actionable_count": counts.get("advisory", 0),
    }


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out
