"""Promotion readiness evaluator for PortfolioConstruction shadow output."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.execution_preflight import preflight_execution_weights
from services.group_contract import GROUP_DEFINITIONS, calc_factor_exposure


@dataclass(frozen=True)
class PortfolioConstructionPromotionCriteria:
    max_mean_weight_deviation: float = 0.015
    max_turnover_delta: float = 0.02


@dataclass(frozen=True)
class PortfolioConstructionEvaluation:
    promotion_ready: bool
    status: str
    blockers: list[str]
    warnings: list[str]
    metrics: dict[str, Any]
    criteria: dict[str, Any]
    execution_authority: str = "none"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def evaluate_portfolio_construction_shadow(
    *,
    shadow_weights: dict[str, Any],
    actual_weights: dict[str, Any],
    current_weights: dict[str, Any],
    hard_risk_tickers: list[str] | set[str] | None = None,
    criteria: PortfolioConstructionPromotionCriteria | None = None,
) -> PortfolioConstructionEvaluation:
    """Evaluate whether PC shadow output is mature enough to consider promotion."""
    cfg = criteria or PortfolioConstructionPromotionCriteria()
    shadow = _clean_weights(shadow_weights)
    actual = _clean_weights(actual_weights)
    current = _clean_weights(current_weights)
    hard_risk = {str(t).upper().strip() for t in (hard_risk_tickers or []) if str(t).strip()}

    mean_deviation = _mean_abs_weight_deviation(shadow, actual)
    shadow_preflight = preflight_execution_weights(shadow)
    actual_preflight = preflight_execution_weights(actual)
    shadow_factor_violations = _factor_violations(shadow)
    actual_factor_violations = _factor_violations(actual)
    shadow_turnover = _turnover(shadow, current)
    actual_turnover = _turnover(actual, current)
    turnover_delta = shadow_turnover - actual_turnover
    weight_diff = _weight_diff(shadow, actual)
    high_risk_added = sorted(
        ticker
        for ticker in hard_risk
        if shadow.get(ticker, 0.0) > current.get(ticker, 0.0) + 1e-9
    )

    blockers: list[str] = []
    warnings: list[str] = []
    if weight_diff["max_abs_diff"] > cfg.max_mean_weight_deviation + 1e-12:
        blockers.append("material_weight_deviation_too_high")
    if mean_deviation > cfg.max_mean_weight_deviation + 1e-12:
        blockers.append("mean_weight_deviation_too_high")
    if not shadow_preflight["allowed"]:
        blockers.append("shadow_policy_violation")
    if len(shadow_factor_violations) > len(actual_factor_violations):
        blockers.append("shadow_factor_exposure_worse")
    if turnover_delta > cfg.max_turnover_delta + 1e-12:
        blockers.append("shadow_turnover_too_high")
    if high_risk_added:
        blockers.append("shadow_adds_hard_risk_ticker")

    if actual_preflight["allowed"] is False and shadow_preflight["allowed"] is True:
        warnings.append("shadow_reduces_qc_rejection_risk")
    if shadow_turnover < actual_turnover - 1e-12:
        warnings.append("shadow_reduces_turnover")

    promotion_ready = not blockers
    status = "promotion_candidate" if promotion_ready else "shadow_only"
    return PortfolioConstructionEvaluation(
        promotion_ready=promotion_ready,
        status=status,
        blockers=blockers,
        warnings=warnings,
        metrics={
            "mean_abs_weight_deviation": round(mean_deviation, 6),
            "max_abs_weight_deviation": weight_diff["max_abs_diff"],
            "weight_diffs": weight_diff["diffs"],
            "shadow_turnover": round(shadow_turnover, 6),
            "actual_turnover": round(actual_turnover, 6),
            "turnover_delta": round(turnover_delta, 6),
            "shadow_policy_allowed": bool(shadow_preflight["allowed"]),
            "actual_policy_allowed": bool(actual_preflight["allowed"]),
            "shadow_policy_violations": shadow_preflight,
            "actual_policy_violations": actual_preflight,
            "shadow_factor_violations": shadow_factor_violations,
            "actual_factor_violations": actual_factor_violations,
            "shadow_high_risk_tickers_added": high_risk_added,
        },
        criteria={
            "max_mean_weight_deviation": cfg.max_mean_weight_deviation,
            "max_material_diff": cfg.max_mean_weight_deviation,
            "max_turnover_delta": cfg.max_turnover_delta,
        },
    )


def criteria_from_pc_promotion_config(config: dict[str, Any] | None) -> PortfolioConstructionPromotionCriteria:
    cfg = config or {}
    return PortfolioConstructionPromotionCriteria(
        max_mean_weight_deviation=_safe_float(cfg.get("max_material_diff"), 0.015),
        max_turnover_delta=_safe_float(cfg.get("max_turnover_diff"), 0.02),
    )


def readiness_limits_from_pc_promotion_config(config: dict[str, Any] | None) -> dict[str, Any]:
    cfg = config or {}
    min_cycles = int(cfg.get("min_shadow_cycles", cfg.get("min_cycles", 20)) or 20)
    return {
        "limit": max(min_cycles, 1),
        "min_cycles": min_cycles,
        "min_pass_rate": _safe_float(cfg.get("min_pass_rate"), 0.90),
    }


def summarize_portfolio_construction_readiness(
    evaluations: list[dict[str, Any]],
    *,
    min_cycles: int = 20,
    min_pass_rate: float = 0.90,
) -> dict[str, Any]:
    rows = [row for row in evaluations if isinstance(row, dict)]
    total = len(rows)
    ready_count = sum(1 for row in rows if bool(row.get("promotion_ready")))
    pass_rate = ready_count / total if total else 0.0
    blocker_counts: dict[str, int] = {}
    warning_counts: dict[str, int] = {}
    mean_deviations: list[float] = []
    turnover_deltas: list[float] = []

    for row in rows:
        for blocker in row.get("blockers") or []:
            key = str(blocker)
            blocker_counts[key] = blocker_counts.get(key, 0) + 1
        for warning in row.get("warnings") or []:
            key = str(warning)
            warning_counts[key] = warning_counts.get(key, 0) + 1
        metrics = row.get("metrics") or {}
        mean_deviations.append(_safe_float(metrics.get("mean_abs_weight_deviation"), 0.0))
        turnover_deltas.append(_safe_float(metrics.get("turnover_delta"), 0.0))

    promotion_ready = total >= min_cycles and pass_rate >= min_pass_rate and not blocker_counts
    status = "rolling_promotion_candidate" if promotion_ready else "collecting_evidence"
    if total >= min_cycles and pass_rate < min_pass_rate:
        status = "shadow_only"
    if blocker_counts:
        status = "shadow_only"

    return {
        "status": status,
        "promotion_ready": promotion_ready,
        "cycles": total,
        "ready_count": ready_count,
        "pass_rate": round(pass_rate, 6),
        "min_cycles": min_cycles,
        "min_pass_rate": min_pass_rate,
        "blocker_counts": dict(sorted(blocker_counts.items())),
        "warning_counts": dict(sorted(warning_counts.items())),
        "mean_abs_weight_deviation_avg": round(sum(mean_deviations) / len(mean_deviations), 6) if mean_deviations else 0.0,
        "turnover_delta_avg": round(sum(turnover_deltas) / len(turnover_deltas), 6) if turnover_deltas else 0.0,
        "execution_authority": "none",
    }


def build_portfolio_construction_promotion_gate(
    readiness: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = config or {}
    mode = _normalize_mode(cfg.get("portfolio_construction_mode"))
    enabled = bool(cfg.get("enabled", mode != "shadow"))
    require_manual_approval = bool(cfg.get("require_manual_approval", False))
    min_cycles = int(
        cfg.get(
            "min_shadow_cycles",
            cfg.get("min_cycles", readiness.get("min_cycles") or 20),
        )
        or 20
    )
    min_pass_rate = _safe_float(cfg.get("min_pass_rate", readiness.get("min_pass_rate") or 0.90), 0.90)

    blockers: list[str] = []
    if mode == "shadow":
        blockers.append("portfolio_construction_mode_shadow")
    if not enabled:
        blockers.append("promotion_gate_disabled")
    if int(readiness.get("cycles") or 0) < min_cycles:
        blockers.append("insufficient_cycles")
    if _safe_float(readiness.get("pass_rate"), 0.0) < min_pass_rate:
        blockers.append("pass_rate_below_threshold")
    if readiness.get("blocker_counts"):
        blockers.append("rolling_blockers_present")
    if not bool(readiness.get("promotion_ready")):
        blockers.append("readiness_not_promoted")

    eligible = mode != "shadow" and enabled and not blockers
    status = "eligible_for_manual_review" if eligible and require_manual_approval else "auto_approved"
    if not eligible:
        status = "shadow_only" if mode == "shadow" else ("disabled" if not enabled else "blocked")

    return {
        "status": status,
        "eligible": eligible,
        "portfolio_construction_mode": mode,
        "enabled": enabled,
        "require_manual_approval": require_manual_approval,
        "approval_mode": "manual" if require_manual_approval else "auto",
        "blockers": blockers,
        "readiness_status": readiness.get("status"),
        "cycles": readiness.get("cycles", 0),
        "pass_rate": readiness.get("pass_rate", 0.0),
        "min_cycles": min_cycles,
        "min_pass_rate": min_pass_rate,
        "execution_authority": "none",
        "would_promote_to": "portfolio_construction_gated" if eligible else None,
    }


def build_portfolio_construction_rollout_gate(
    readiness: dict[str, Any],
    config: dict[str, Any] | None = None,
    *,
    auth_mode: str = "SEMI_AUTO",
    semi_auto_confirmed_cycles: int = 0,
) -> dict[str, Any]:
    """Add PR8 rollout constraints on top of readiness/promotion eligibility."""
    cfg = config or {}
    base = build_portfolio_construction_promotion_gate(readiness, cfg)
    mode = str(base.get("portfolio_construction_mode") or "shadow")
    clean_auth = str(auth_mode or "SEMI_AUTO").strip().upper()
    min_confirmed = _safe_int(cfg.get("min_gated_semi_auto_confirmed_cycles"), 5)
    confirmed = max(_safe_int(semi_auto_confirmed_cycles, 0), 0)
    require_semi_auto_first = bool(cfg.get("require_semi_auto_gated_before_full_auto", True))
    allow_full_auto_gated = bool(cfg.get("allow_full_auto_gated", False))

    blockers = list(base.get("blockers") or [])
    rollout_phase = "not_gated"
    if mode == "gated":
        if clean_auth == "SEMI_AUTO":
            rollout_phase = "semi_auto_gated"
        elif clean_auth == "FULL_AUTO":
            rollout_phase = "full_auto_gated"
            if require_semi_auto_first and confirmed < min_confirmed:
                blockers.append("semi_auto_gated_confirmations_insufficient")
            if not allow_full_auto_gated:
                blockers.append("full_auto_gated_not_enabled")
        else:
            rollout_phase = "blocked_auth_mode"
            blockers.append("gated_rollout_requires_semi_auto_or_reviewed_full_auto")

    blockers = _unique(blockers)
    eligible = bool(base.get("eligible")) and not blockers
    status = base.get("status")
    if mode == "gated" and base.get("eligible"):
        if eligible and clean_auth == "SEMI_AUTO":
            status = "semi_auto_gated_ready"
        elif eligible and clean_auth == "FULL_AUTO":
            status = "full_auto_gated_ready"
        else:
            status = "rollout_blocked"

    return {
        **base,
        "status": status,
        "eligible": eligible,
        "blockers": blockers,
        "auth_mode": clean_auth,
        "rollout_phase": rollout_phase,
        "require_semi_auto_gated_before_full_auto": require_semi_auto_first,
        "semi_auto_confirmed_cycles": confirmed,
        "min_gated_semi_auto_confirmed_cycles": min_confirmed,
        "allow_full_auto_gated": allow_full_auto_gated,
        "would_promote_to": "portfolio_construction_gated" if eligible else None,
    }


async def load_portfolio_construction_readiness(
    *,
    limit: int = 20,
    min_cycles: int = 20,
    min_pass_rate: float = 0.90,
) -> dict[str, Any]:
    from sqlalchemy import desc, select

    from db.models import AgentAnalysis
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(AgentAnalysis.risk_output)
            .order_by(desc(AgentAnalysis.analyzed_at))
            .limit(limit)
        )
        risk_outputs = result.scalars().all()

    evaluations = []
    for risk in risk_outputs:
        if not isinstance(risk, dict):
            continue
        evaluation = risk.get("portfolio_construction_evaluation")
        if isinstance(evaluation, dict):
            evaluations.append(evaluation)
    return summarize_portfolio_construction_readiness(
        evaluations,
        min_cycles=min_cycles,
        min_pass_rate=min_pass_rate,
    )


async def load_gated_semi_auto_confirmed_cycles(*, limit: int = 50) -> dict[str, Any]:
    """Count confirmed SEMI_AUTO proposals where gated PC actually fed target_builder."""
    from sqlalchemy import desc, select

    from db.models import AgentAnalysis
    from db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(AgentAnalysis.risk_output)
            .where(AgentAnalysis.execution_status == "executed_user_confirmed")
            .order_by(desc(AgentAnalysis.analyzed_at))
            .limit(max(int(limit or 50), 1))
        )
        risk_outputs = result.scalars().all()

    confirmed = sum(
        1 for risk_output in risk_outputs
        if is_gated_semi_auto_confirmed_risk_output(risk_output)
    )
    return {
        "count": confirmed,
        "sampled": len(risk_outputs),
        "execution_status_filter": "executed_user_confirmed",
        "status": "loaded",
    }


def is_gated_semi_auto_confirmed_risk_output(risk_output: Any) -> bool:
    if not isinstance(risk_output, dict):
        return False
    target_builder = risk_output.get("target_builder_input") or {}
    diagnostics = target_builder.get("diagnostics") or {}
    gate = risk_output.get("portfolio_construction_promotion_gate") or {}
    return (
        bool(diagnostics.get("construction_participated"))
        and diagnostics.get("target_construction_source") == "portfolio_construction"
        and (
            str(gate.get("portfolio_construction_mode") or "").strip().lower() == "gated"
            or bool(gate.get("eligible"))
        )
    )


def _factor_violations(weights: dict[str, float]) -> list[dict[str, Any]]:
    exposures = calc_factor_exposure(weights)
    rows: list[dict[str, Any]] = []
    for group_name, definition in sorted(GROUP_DEFINITIONS.items()):
        exposure = float(exposures.get(group_name, 0.0) or 0.0)
        if exposure > definition.limit_pct + 1e-9:
            rows.append(
                {
                    "group": group_name,
                    "exposure": round(exposure, 6),
                    "limit": definition.limit_pct,
                }
            )
    return rows


def _mean_abs_weight_deviation(left: dict[str, float], right: dict[str, float]) -> float:
    tickers = sorted((set(left) | set(right)) - {"CASH"})
    if not tickers:
        return 0.0
    return sum(abs(left.get(ticker, 0.0) - right.get(ticker, 0.0)) for ticker in tickers) / len(tickers)


def _weight_diff(left: dict[str, float], right: dict[str, float]) -> dict[str, Any]:
    rows: dict[str, dict[str, float]] = {}
    max_abs_diff = 0.0
    for ticker in sorted((set(left) | set(right)) - {"CASH"}):
        diff = float(left.get(ticker, 0.0) or 0.0) - float(right.get(ticker, 0.0) or 0.0)
        max_abs_diff = max(max_abs_diff, abs(diff))
        if abs(diff) > 1e-9:
            rows[ticker] = {
                "shadow": round(float(left.get(ticker, 0.0) or 0.0), 6),
                "actual": round(float(right.get(ticker, 0.0) or 0.0), 6),
                "diff": round(diff, 6),
            }
    return {"max_abs_diff": round(max_abs_diff, 6), "diffs": rows}


def _turnover(target: dict[str, float], current: dict[str, float]) -> float:
    tickers = sorted((set(target) | set(current)) - {"CASH"})
    return sum(abs(target.get(ticker, 0.0) - current.get(ticker, 0.0)) for ticker in tickers)


def _clean_weights(raw: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (raw or {}).items():
        clean = str(ticker or "").upper().strip()
        if not clean:
            continue
        try:
            out[clean] = max(float(value or 0.0), 0.0)
        except (TypeError, ValueError):
            out[clean] = 0.0
    return out


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _unique(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        key = str(item)
        if key in seen:
            continue
        out.append(key)
        seen.add(key)
    return out


def _normalize_mode(value: Any) -> str:
    mode = str(value or "shadow").strip().lower()
    return mode if mode in {"shadow", "candidate", "gated"} else "shadow"
