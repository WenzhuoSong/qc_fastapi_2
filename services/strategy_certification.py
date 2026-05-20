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
        "audit": build_strategy_certification_audit(certifications),
        "policy": {
            "min_historical_samples": MIN_HISTORICAL_SAMPLES,
            "min_live_samples": MIN_LIVE_SAMPLES,
            "max_advisory_turnover": MAX_ADVISORY_TURNOVER,
            "certified_status_deferred": True,
        },
    }


def build_strategy_certification_audit(certifications: dict[str, dict[str, Any]] | None) -> dict[str, Any]:
    """Return an operator-facing audit view over certification output."""
    rows: list[dict[str, Any]] = []
    suggested_advisory_not_certified: list[str] = []
    disabled_or_experimental: list[str] = []
    promotion_candidates: list[str] = []

    for name, row in sorted((certifications or {}).items()):
        if not isinstance(row, dict):
            continue
        historical = row.get("historical") or {}
        live = row.get("live") or {}
        walk_forward = row.get("walk_forward") or {}
        status = str(row.get("status") or "experimental")
        suggested_use = str(row.get("suggested_use") or "watch_only")
        approved_use = str(row.get("approved_use") or "none")
        promotion_blockers = list(row.get("promotion_blockers") or [])
        demotion_reasons = list(row.get("demotion_reasons") or [])
        promotion_eligible = (
            status == "advisory"
            and approved_use == "advisory"
            and not promotion_blockers
            and not demotion_reasons
        )
        risk_flags: list[str] = []
        if suggested_use in {"primary", "advisory"} and approved_use != "advisory":
            risk_flags.append("suggested_use_not_certified_for_execution")
            suggested_advisory_not_certified.append(name)
        if status in {"disabled", "experimental"}:
            risk_flags.append(f"status_{status}")
            disabled_or_experimental.append(name)
        if demotion_reasons:
            risk_flags.append("has_demotion_reasons")
        if promotion_blockers:
            risk_flags.append("has_promotion_blockers")
        if promotion_eligible:
            promotion_candidates.append(name)

        rows.append({
            "strategy_name": name,
            "status": status,
            "suggested_use": suggested_use,
            "approved_use": approved_use,
            "confidence_score": row.get("confidence_score"),
            "historical_samples": historical.get("samples"),
            "historical_sharpe": historical.get("sharpe"),
            "historical_hit_rate": historical.get("hit_rate"),
            "live_samples": live.get("samples"),
            "live_fit": live.get("fit"),
            "walk_forward_level": walk_forward.get("level"),
            "walk_forward_valid_folds": walk_forward.get("valid_folds"),
            "walk_forward_pass_rate": walk_forward.get("pass_rate"),
            "turnover": row.get("turnover"),
            "promotion_eligible": promotion_eligible,
            "promotion_blockers": promotion_blockers,
            "demotion_reasons": demotion_reasons,
            "risk_flags": _unique(risk_flags),
        })

    rows.sort(
        key=lambda item: (
            _status_rank(str(item.get("status") or "")),
            -float(item.get("confidence_score") or 0.0),
            str(item.get("strategy_name") or ""),
        )
    )
    return {
        "rows": rows,
        "summary": {
            "total": len(rows),
            "promotion_candidates": promotion_candidates,
            "suggested_advisory_not_certified": suggested_advisory_not_certified,
            "disabled_or_experimental": disabled_or_experimental,
            "requires_operator_review": bool(suggested_advisory_not_certified or disabled_or_experimental),
        },
        "execution_authority": "none",
    }


def _certify_one(*, row: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    name = str(row.get("strategy_name") or "")
    historical_samples = int(_to_float(row.get("historical_forward_return_samples"), 0) or 0)
    live_samples = int(_to_float(row.get("n_forward_return_samples"), 0) or 0)
    turnover = _to_float(row.get("turnover"), 0.0) or 0.0
    sharpe = _to_float(row.get("historical_sharpe"), None)
    hit_rate = _to_float(row.get("historical_hit_rate"), None)
    walk_forward_level = str(row.get("walk_forward_level") or "missing")
    walk_forward_valid_folds = int(_to_float(row.get("walk_forward_valid_folds"), 0) or 0)
    walk_forward_pass_rate = _to_float(row.get("walk_forward_pass_rate"), None)
    walk_forward_stability_score = _to_float(row.get("walk_forward_stability_score"), None)
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
    if walk_forward_level == "weak":
        demotion_reasons.append("walk_forward_weak")
    elif walk_forward_level == "insufficient":
        blockers.append("walk_forward_insufficient")
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
        walk_forward_level=walk_forward_level,
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
        "walk_forward": {
            "level": walk_forward_level,
            "valid_folds": walk_forward_valid_folds,
            "pass_rate": walk_forward_pass_rate,
            "stability_score": walk_forward_stability_score,
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
    walk_forward_level: str,
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
    if walk_forward_level in {"weak", "insufficient"}:
        return "research_supported"
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


def _status_rank(status: str) -> int:
    return {
        "advisory": 0,
        "research_supported": 1,
        "experimental": 2,
        "disabled": 3,
    }.get(status, 9)
