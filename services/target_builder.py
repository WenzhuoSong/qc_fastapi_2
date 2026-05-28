"""Deterministic target weight builder.

Phase C shadow mode only: this module constructs an auditable deterministic
target lifecycle from baseline weights, current holdings, governance decisions,
and validated advisory results. It must not consume raw LLM adjusted_weights.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from services.evidence_cap_config import default_evidence_cap_config, resolve_evidence_cap_mode
from services.execution_policy import apply_policy_caps, evaluate_policy, policy_snapshot


NO_ADD_PERMISSIONS = {"hold_or_trim", "reduce_risk_only", "defensive_only", "cash_only"}

ALLOWED_EVIDENCE_FIELDS = {
    "action",
    "role",
    "signal_type",
    "max_reasonable_weight",
    "risk_budget_cost",
    "allowed_actions",
    "reason_codes",
}

FORBIDDEN_EVIDENCE_FIELDS = {
    "conviction",
    "conviction_status",
    "conviction_source_bucket",
    "conviction_n",
    "effective_confidence",
}


@dataclass
class TargetBuildResult:
    target_weights: dict[str, float]
    target_build_steps: list[str]
    per_ticker: dict[str, dict[str, Any]]
    turnover: dict[str, Any]
    violations: list[str]
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_target_weights(
    *,
    base_weights: dict[str, Any],
    construction_weights: dict[str, Any] | None = None,
    construction_source: str | None = None,
    current_weights: dict[str, Any],
    market_scorecard: dict[str, Any] | None,
    decision_style: dict[str, Any] | None,
    position_governance: dict[str, Any] | None,
    validated_advisory: list[dict[str, Any]] | None,
    constraints: dict[str, Any] | None = None,
    mode: str = "target_builder_shadow",
) -> TargetBuildResult:
    """Construct deterministic target weights from non-LLM execution contracts."""
    base = _clean_weights(base_weights)
    construction = _clean_weights(construction_weights)
    construction_participated = construction_weights is not None and bool(str(construction_source or "").strip())
    current = _clean_weights(current_weights)
    scorecard = market_scorecard or {}
    style = decision_style or {}
    governance = position_governance or {}
    cfg = constraints or {}
    pc_gate = cfg.get("portfolio_construction_gate") if isinstance(cfg.get("portfolio_construction_gate"), dict) else {}
    evidence_cap_diagnostics = (
        cfg.get("evidence_cap_diagnostics")
        if isinstance(cfg.get("evidence_cap_diagnostics"), dict)
        else {}
    )
    evidence_cap_config = default_evidence_cap_config(
        cfg.get("evidence_cap_config") if isinstance(cfg.get("evidence_cap_config"), dict) else {}
    )
    forbidden_evidence_fields_seen = _forbidden_evidence_fields_seen(governance)

    decisions = _decisions_by_ticker(governance)
    advisory = _advisory_by_ticker(validated_advisory or governance.get("advisory_overrides") or [])
    starting_weights = construction if construction_participated else base
    start_source = str(construction_source or "base_weights").strip() if construction_participated else "base_weights"
    clean_mode = _target_builder_mode(mode)
    tickers = sorted((set(base) | set(construction) | set(current) | set(decisions)) - {"CASH"})
    permission = str(scorecard.get("investment_permission") or "")
    no_add = permission in NO_ADD_PERMISSIONS or bool(scorecard.get("require_human_confirmation"))
    max_single_delta = _effective_single_delta(scorecard, style, cfg)

    work: dict[str, float] = {}
    per_ticker: dict[str, dict[str, Any]] = {}
    violations: list[str] = []
    steps = [
        "base_weight",
        "construction_weight" if construction_participated else "construction_weight_skipped",
        "scorecard_clip",
        "governance_adjustment",
        "validated_llm_delta",
        "single_delta_clip",
        "hedge_intent_overlay",
        "turnover_clip",
        "normalization",
        "evidence_cap_shadow_or_gate",
    ]

    for ticker in tickers:
        base_w = float(base.get(ticker, 0.0) or 0.0)
        construction_w = float(construction.get(ticker, 0.0) or 0.0) if construction_participated else None
        start_w = float(starting_weights.get(ticker, 0.0) or 0.0)
        current_w = float(current.get(ticker, 0.0) or 0.0)
        row = decisions.get(ticker) or {}
        advisory_row = advisory.get(ticker)
        target = start_w
        changed_by: list[str] = []
        if construction_participated and abs(start_w - base_w) > 1e-9:
            changed_by.append("portfolio_construction")

        if no_add and target > current_w:
            violations.append(f"scorecard_no_add:{ticker} {target:.2%}->{current_w:.2%}")
            target = current_w
            changed_by.append("scorecard_clip")

        governance_target = _optional_float(row.get("target_after"))
        if governance_target is not None:
            if abs(governance_target - target) > 1e-9:
                changed_by.append("position_governance")
            target = governance_target

        if no_add and target > current_w:
            violations.append(f"governance_no_add_clip:{ticker} {target:.2%}->{current_w:.2%}")
            target = current_w
            changed_by.append("scorecard_clip")

        pre_delta_clip = target
        if max_single_delta is not None:
            lower = max(current_w - max_single_delta, 0.0)
            upper = current_w + max_single_delta
            target = min(max(target, lower), upper)
            if abs(target - pre_delta_clip) > 1e-9:
                violations.append(f"single_delta_clip:{ticker} {pre_delta_clip:.2%}->{target:.2%}")
                changed_by.append("single_delta_clip")

        target = max(target, 0.0)
        if target > 1e-9:
            work[ticker] = target

        validated_delta = _validated_advisory_delta(advisory_row)
        per_ticker[ticker] = {
            "base_weight": round(base_w, 6),
            "construction_weight": round(construction_w, 6) if construction_w is not None else None,
            "target_start_source": start_source,
            "current_weight": round(current_w, 6),
            "scorecard_permission": permission,
            "governance_adjustment": (
                round(governance_target - start_w, 6)
                if governance_target is not None
                else 0.0
            ),
            "validated_llm_delta": validated_delta,
            "governance_target": round(governance_target, 6) if governance_target is not None else None,
            "pre_normalized_target": round(target, 6),
            "final_target": None,
            "reason_codes": list(row.get("reason_codes") or []),
            "allowed_actions": list(row.get("allowed_actions") or []),
            "advisory_validator_result": (advisory_row or {}).get("validator_result"),
            "changed_by": _unique(changed_by),
        }

    hedge_overlay = _apply_hedge_intent_overlay(work, current, cfg.get("hedge_intent"))
    if hedge_overlay["applied"]:
        work = hedge_overlay["weights"]
        violations.extend(hedge_overlay["violations"])
        for ticker in set(per_ticker) | set(hedge_overlay["touched_tickers"]):
            per_ticker.setdefault(
                ticker,
                {
                    "base_weight": round(float(base.get(ticker, 0.0) or 0.0), 6),
                    "construction_weight": (
                        round(float(construction.get(ticker, 0.0) or 0.0), 6)
                        if construction_participated
                        else None
                    ),
                    "target_start_source": start_source,
                    "current_weight": round(float(current.get(ticker, 0.0) or 0.0), 6),
                    "scorecard_permission": permission,
                    "governance_adjustment": 0.0,
                    "validated_llm_delta": 0.0,
                    "governance_target": None,
                    "pre_normalized_target": None,
                    "final_target": None,
                    "reason_codes": [],
                    "allowed_actions": [],
                    "advisory_validator_result": None,
                    "changed_by": [],
                },
            )
            per_ticker[ticker].setdefault("changed_by", [])
            if "hedge_intent_overlay" not in per_ticker[ticker]["changed_by"]:
                per_ticker[ticker]["changed_by"].append("hedge_intent_overlay")
            per_ticker[ticker]["hedge_intent_adjustment"] = round(
                float(work.get(ticker, 0.0) or 0.0) - float(base.get(ticker, 0.0) or 0.0),
                6,
            )

    work["CASH"] = max(1.0 - sum(work.values()), 0.0)
    normalized = _normalize_cash_first(work)
    turnover_before = _turnover(work, current)

    turnover_cap = _effective_turnover_cap(scorecard, style, cfg)
    if turnover_cap is not None and turnover_before > turnover_cap + 1e-9:
        normalized = _scale_toward_current(normalized, current, turnover_cap)
        violations.append(f"turnover_clip:{turnover_before:.2%}->{turnover_cap:.2%}")

    normalized = _normalize_cash_first(normalized)
    capped_targets, cap_events, cash_raised = apply_policy_caps(normalized)
    if cash_raised > 0:
        capped_targets["CASH"] = float(capped_targets.get("CASH", 0.0) or 0.0) + cash_raised
    normalized = _normalize_cash_first(capped_targets)
    evidence_cap_gate = _apply_evidence_cap_gate(
        evidence_cap_diagnostics=evidence_cap_diagnostics,
        target_weights=normalized,
        per_ticker=per_ticker,
        evidence_cap_config=evidence_cap_config,
        allow_enforcement=clean_mode == "target_builder_gated",
    )
    normalized = evidence_cap_gate["target_weights"]
    evidence_cap_shadow = evidence_cap_gate["diagnostics"]
    evidence_cap_applied_tickers = set(evidence_cap_gate.get("applied_tickers") or [])
    if evidence_cap_shadow.get("applied_count"):
        violations.append(f"evidence_cap:{int(evidence_cap_shadow.get('applied_count') or 0)}")
    policy_evaluation = evaluate_policy(
        weights=normalized,
        current_weights=current,
        context={
            "max_turnover_per_cycle": turnover_cap,
            "max_single_delta": max_single_delta,
        },
    )
    if cap_events:
        violations.append(f"policy_cap:{len(cap_events)}")

    cap_event_tickers = {
        str(event.get("ticker") or "").upper().strip()
        for event in cap_events
        if isinstance(event, dict) and event.get("ticker")
    }
    turnover_clipped = any(str(item).startswith("turnover_clip:") for item in violations)
    for ticker, row in per_ticker.items():
        final_target = round(float(normalized.get(ticker, 0.0) or 0.0), 6)
        pre_target = _optional_float(row.get("pre_normalized_target"))
        if turnover_clipped and pre_target is not None and abs(final_target - pre_target) > 1e-9:
            row.setdefault("changed_by", []).append("turnover_clip")
        if ticker in cap_event_tickers and pre_target is not None and abs(final_target - pre_target) > 1e-9:
            row.setdefault("changed_by", []).append("policy_cap")
        if ticker in evidence_cap_applied_tickers:
            row.setdefault("changed_by", []).append("evidence_cap")
        row["changed_by"] = _unique(row.get("changed_by") or [])
        row["final_target"] = final_target

    turnover_after = _turnover(normalized, current)
    return TargetBuildResult(
        target_weights=normalized,
        target_build_steps=steps,
        per_ticker=per_ticker,
        turnover={
            "estimated_before_clip": round(turnover_before, 6),
            "estimated": round(turnover_after, 6),
            "limit": turnover_cap,
            "within_budget": True if turnover_cap is None else turnover_after <= turnover_cap + 1e-9,
        },
        violations=violations,
        diagnostics={
            "mode": clean_mode,
            "execution_effect": "risk_manager_input" if clean_mode == "target_builder_gated" else "none",
            "consumes_raw_llm_adjusted_weights": False,
            "raw_llm_adjusted_weights_consumed": False,
            "target_construction_source": "portfolio_construction"
            if construction_participated
            else "deterministic_target_builder",
            "target_start_source": start_source,
            "construction_participated": construction_participated,
            "construction_source": str(construction_source or "").strip() or None,
            "portfolio_construction_configured_mode": pc_gate.get("configured_mode") or pc_gate.get("mode"),
            "portfolio_construction_effective_mode": pc_gate.get("effective_mode"),
            "portfolio_construction_gate_status": pc_gate.get("gate_status"),
            "portfolio_construction_gate_eligible": pc_gate.get("gate_eligible"),
            "portfolio_construction_blocked_reason": pc_gate.get("blocked_reason"),
            "portfolio_construction_gate_blockers": _string_list(pc_gate.get("gate_blockers")),
            "policy_version": policy_snapshot()["version"],
            "policy_evaluation": policy_evaluation,
            "policy_cap_events": cap_events,
            "cash_raised_by_policy_cap": cash_raised,
            "evidence_cap_shadow": evidence_cap_shadow,
            "hedge_intent": hedge_overlay["diagnostics"],
            "ticker_count": len(per_ticker),
            "allowed_evidence_fields": sorted(ALLOWED_EVIDENCE_FIELDS),
            "forbidden_evidence_fields_seen": forbidden_evidence_fields_seen,
            "forbidden_evidence_fields_consumed": False,
        },
    )


def compare_target_weights(
    *,
    live_target_weights: dict[str, Any],
    shadow_target_weights: dict[str, Any],
) -> dict[str, Any]:
    live = _clean_weights(live_target_weights)
    shadow = _clean_weights(shadow_target_weights)
    rows: dict[str, dict[str, float]] = {}
    max_abs_diff = 0.0
    for ticker in sorted((set(live) | set(shadow)) - {"CASH"}):
        diff = float(shadow.get(ticker, 0.0) or 0.0) - float(live.get(ticker, 0.0) or 0.0)
        max_abs_diff = max(max_abs_diff, abs(diff))
        if abs(diff) > 1e-9:
            rows[ticker] = {
                "live": round(float(live.get(ticker, 0.0) or 0.0), 6),
                "shadow": round(float(shadow.get(ticker, 0.0) or 0.0), 6),
                "diff": round(diff, 6),
            }
    return {
        "max_abs_diff": round(max_abs_diff, 6),
        "aggregate_turnover_diff": round(abs(_turnover(shadow, live)), 6),
        "diffs": rows,
        "requires_review": max_abs_diff > 0.015 or abs(_turnover(shadow, live)) > 0.02,
    }


def _decisions_by_ticker(governance: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in governance.get("position_decisions") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker:
            out[ticker] = row
    return out


def _forbidden_evidence_fields_seen(governance: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    for row in governance.get("position_decisions") or []:
        if not isinstance(row, dict):
            continue
        for field in FORBIDDEN_EVIDENCE_FIELDS:
            if field in row:
                seen.add(field)
        evidence = row.get("evidence")
        if isinstance(evidence, dict):
            for field in FORBIDDEN_EVIDENCE_FIELDS:
                if field in evidence:
                    seen.add(f"evidence.{field}")
    for row in governance.get("advisory_overrides") or []:
        if not isinstance(row, dict):
            continue
        for field in FORBIDDEN_EVIDENCE_FIELDS:
            if field in row:
                seen.add(f"advisory.{field}")
    return sorted(seen)


def _advisory_by_ticker(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker:
            out[ticker] = row
    return out


def _apply_hedge_intent_overlay(
    weights: dict[str, float],
    current_weights: dict[str, float],
    hedge_intent: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(hedge_intent, dict) or not hedge_intent.get("triggered"):
        return {
            "applied": False,
            "weights": weights,
            "violations": [],
            "touched_tickers": [],
            "diagnostics": {"triggered": False, "applied": False},
        }

    result = dict(weights)
    violations: list[str] = []
    touched: set[str] = set()
    cash_raise_target = max(float(hedge_intent.get("cash_raise_pct") or 0.0), 0.0)
    trim_targets = [str(t).upper().strip() for t in hedge_intent.get("trim_targets") or []]
    raised = 0.0

    for ticker in trim_targets:
        if raised >= cash_raise_target - 1e-12:
            break
        current = float(result.get(ticker, current_weights.get(ticker, 0.0)) or 0.0)
        if current <= 0.0:
            continue
        trim_amount = min(current * 0.25, cash_raise_target - raised)
        result[ticker] = max(float(result.get(ticker, current) or 0.0) - trim_amount, 0.0)
        raised += trim_amount
        touched.add(ticker)
        violations.append(f"hedge_intent_trim:{ticker} -{trim_amount:.2%}")

    hedge_instrument = str(hedge_intent.get("hedge_instrument") or "").upper().strip()
    hedge_weight = max(float(hedge_intent.get("hedge_weight") or 0.0), 0.0)
    if hedge_intent.get("add_hedge_etf") and hedge_instrument and hedge_weight > 0.0:
        result[hedge_instrument] = max(float(result.get(hedge_instrument, 0.0) or 0.0), hedge_weight)
        touched.add(hedge_instrument)
        violations.append(f"hedge_intent_add:{hedge_instrument} {hedge_weight:.2%}")

    return {
        "applied": True,
        "weights": result,
        "violations": violations,
        "touched_tickers": sorted(touched),
        "diagnostics": {
            "triggered": True,
            "applied": True,
            "reasons": list(hedge_intent.get("reasons") or hedge_intent.get("trigger_reasons") or []),
            "severity": hedge_intent.get("severity"),
            "cash_raise_target": cash_raise_target,
            "cash_raised_by_trim": round(raised, 6),
            "trim_targets": trim_targets,
            "hedge_instrument": hedge_instrument or None,
            "hedge_weight": hedge_weight,
        },
    }


def _validated_advisory_delta(row: dict[str, Any] | None) -> float:
    if not isinstance(row, dict):
        return 0.0
    if not str(row.get("validator_result") or "").startswith("accepted"):
        return 0.0
    before = _optional_float(row.get("target_before_override"))
    after = _optional_float(row.get("target_after_override"))
    if before is None or after is None:
        return 0.0
    return round(after - before, 6)


def _apply_evidence_cap_gate(
    *,
    evidence_cap_diagnostics: dict[str, Any],
    target_weights: dict[str, float],
    per_ticker: dict[str, dict[str, Any]],
    evidence_cap_config: dict[str, Any],
    allow_enforcement: bool,
) -> dict[str, Any]:
    gate = resolve_evidence_cap_mode(evidence_cap_config)
    if not allow_enforcement and gate.get("effective_mode") == "gated":
        blockers = list(gate.get("gate_blockers") or [])
        blockers.append("target_builder_shadow_no_execution_authority")
        gate = {
            **gate,
            "effective_mode": "observe",
            "blocked_reason": "target_builder_shadow_no_execution_authority",
            "gate_blockers": _unique(blockers),
            "execution_effect": "diagnostic_only",
        }
    if not evidence_cap_diagnostics:
        return {
            "target_weights": target_weights,
            "applied_tickers": [],
            "diagnostics": {
                "enabled": False,
                "configured_mode": gate.get("configured_mode"),
                "effective_mode": gate.get("effective_mode"),
                "criteria_met": gate.get("criteria_met"),
                "blocked_reason": gate.get("blocked_reason"),
                "gate_blockers": gate.get("gate_blockers") or [],
                "calibration_freshness": gate.get("calibration_freshness") or {},
                "execution_effect": gate.get("execution_effect"),
                "target_weight_mutation": "none",
                "would_apply_count": 0,
                "applied_count": 0,
                "rows": [],
            },
        }

    if gate.get("effective_mode") == "off":
        return {
            "target_weights": target_weights,
            "applied_tickers": [],
            "diagnostics": {
                "enabled": False,
                "configured_mode": gate.get("configured_mode"),
                "effective_mode": "off",
                "criteria_met": gate.get("criteria_met"),
                "blocked_reason": gate.get("blocked_reason"),
                "gate_blockers": gate.get("gate_blockers") or [],
                "calibration_freshness": gate.get("calibration_freshness") or {},
                "execution_effect": "none",
                "target_weight_mutation": "none",
                "input_schema": "pipeline_context.evidence_cap_diagnostics",
                "would_apply_count": 0,
                "applied_count": 0,
                "rows": [],
            },
        }

    out_weights = dict(target_weights)
    rows: list[dict[str, Any]] = []
    released_to_cash = 0.0
    applied_tickers: list[str] = []
    for ticker, raw in sorted(evidence_cap_diagnostics.items()):
        if not isinstance(raw, dict):
            continue
        clean_ticker = str(raw.get("ticker") or ticker or "").upper().strip()
        if not clean_ticker or clean_ticker == "CASH":
            continue
        adjusted_cap = _optional_float(raw.get("evidence_adjusted_cap"))
        static_cap = _optional_float(raw.get("static_cap"))
        target_before = round(float(out_weights.get(clean_ticker, 0.0) or 0.0), 6)
        enforcement_cap = _evidence_enforcement_cap(adjusted_cap=adjusted_cap, static_cap=static_cap)
        would_apply = (
            enforcement_cap is not None
            and target_before > max(float(enforcement_cap), 0.0) + 1e-12
        )
        applied = bool(gate.get("effective_mode") == "gated" and would_apply)
        target_after = target_before
        if applied and enforcement_cap is not None:
            target_after = round(max(float(enforcement_cap), 0.0), 6)
            out_weights[clean_ticker] = target_after
            released_to_cash += max(target_before - target_after, 0.0)
            applied_tickers.append(clean_ticker)
        row = {
            "ticker": clean_ticker,
            "target_weight": target_before,
            "target_before_cap": target_before,
            "target_after_cap": target_after,
            "static_cap": round(float(static_cap), 6) if static_cap is not None else None,
            "evidence_adjusted_cap": round(float(adjusted_cap), 6) if adjusted_cap is not None else None,
            "evidence_enforcement_cap": (
                round(float(enforcement_cap), 6) if enforcement_cap is not None else None
            ),
            "would_apply_cap": bool(would_apply),
            "would_clip_to": round(float(enforcement_cap), 6) if would_apply and enforcement_cap is not None else None,
            "applied_cap": applied,
            "input_would_clip": bool(raw.get("would_clip")),
            "coverage_ratio": _optional_round(raw.get("coverage_ratio")),
            "evidence_quality_multiplier": _optional_round(raw.get("evidence_quality_multiplier")),
            "conviction_status": raw.get("conviction_status"),
            "history_days": raw.get("history_days"),
            "voted_count": raw.get("voted_count"),
            "abstain_count": raw.get("abstain_count"),
            "mapping_error_count": raw.get("mapping_error_count"),
            "source": "pipeline_context.evidence_cap_diagnostics",
            "configured_mode": gate.get("configured_mode"),
            "effective_mode": gate.get("effective_mode"),
            "blocked_reason": gate.get("blocked_reason"),
            "execution_effect": gate.get("execution_effect"),
        }
        rows.append(row)
        if clean_ticker in per_ticker:
            per_ticker[clean_ticker]["evidence_cap_shadow"] = {
                "would_apply_cap": bool(would_apply),
                "evidence_adjusted_cap": row["evidence_adjusted_cap"],
                "would_clip_to": row["would_clip_to"],
                "applied_cap": applied,
                "target_before_cap": target_before,
                "target_after_cap": target_after,
                "effective_mode": gate.get("effective_mode"),
            }

    if released_to_cash > 0.0:
        out_weights["CASH"] = float(out_weights.get("CASH", 0.0) or 0.0) + released_to_cash
        out_weights = _normalize_cash_first(out_weights)

    rows.sort(
        key=lambda item: (
            not bool(item.get("would_apply_cap")),
            not bool(item.get("applied_cap")),
            -float(item.get("target_before_cap") or 0.0),
            str(item.get("ticker") or ""),
        )
    )
    return {
        "target_weights": out_weights,
        "applied_tickers": applied_tickers,
        "diagnostics": {
            "enabled": True,
            "configured_mode": gate.get("configured_mode"),
            "effective_mode": gate.get("effective_mode"),
            "criteria_met": gate.get("criteria_met"),
            "blocked_reason": gate.get("blocked_reason"),
            "gate_blockers": gate.get("gate_blockers") or [],
            "observe_cycles": gate.get("observe_cycles"),
            "min_observe_cycles": gate.get("min_observe_cycles"),
            "would_clip_rate": gate.get("would_clip_rate"),
            "max_would_clip_rate": gate.get("max_would_clip_rate"),
            "calibration_freshness": gate.get("calibration_freshness") or {},
            "min_multiplier": gate.get("min_multiplier"),
            "execution_effect": gate.get("execution_effect"),
            "target_weight_mutation": "tighten_only" if applied_tickers else "none",
            "input_schema": "pipeline_context.evidence_cap_diagnostics",
            "would_apply_count": sum(1 for row in rows if row.get("would_apply_cap")),
            "applied_count": len(applied_tickers),
            "cash_raised_by_evidence_cap": round(released_to_cash, 6),
            "rows": rows,
        },
    }


def _evidence_enforcement_cap(*, adjusted_cap: float | None, static_cap: float | None) -> float | None:
    if adjusted_cap is None:
        return None
    candidates = [max(float(adjusted_cap), 0.0)]
    if static_cap is not None:
        candidates.append(max(float(static_cap), 0.0))
    return min(candidates)


def _effective_single_delta(
    scorecard: dict[str, Any],
    style: dict[str, Any],
    constraints: dict[str, Any],
) -> float | None:
    values: list[float] = []
    for value in (
        constraints.get("max_single_delta"),
        scorecard.get("max_adjustment_from_base"),
        (style.get("style_limits") or {}).get("max_single_trade_pct"),
    ):
        parsed = _optional_float(value)
        if parsed is not None and parsed >= 0:
            values.append(parsed)
    return min(values) if values else None


def _effective_turnover_cap(
    scorecard: dict[str, Any],
    style: dict[str, Any],
    constraints: dict[str, Any],
) -> float | None:
    values: list[float] = []
    for value in (
        constraints.get("max_turnover"),
        scorecard.get("max_turnover_per_cycle"),
        (style.get("style_limits") or {}).get("max_turnover_per_cycle"),
    ):
        parsed = _optional_float(value)
        if parsed is not None and parsed >= 0:
            values.append(parsed)
    return min(values) if values else None


def _scale_toward_current(target: dict[str, float], current: dict[str, float], cap: float) -> dict[str, float]:
    turnover = _turnover(target, current)
    if turnover <= cap + 1e-9:
        return target
    scale = cap / turnover if turnover > 0 else 1.0
    out: dict[str, float] = {}
    for ticker in set(target) | set(current):
        out[ticker] = float(current.get(ticker, 0.0) or 0.0) + (
            float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0)
        ) * scale
    return out


def _normalize_cash_first(weights: dict[str, Any]) -> dict[str, float]:
    clean = _clean_weights(weights)
    equity = sum(value for ticker, value in clean.items() if ticker != "CASH")
    if equity >= 1.0:
        scale = 1.0 / equity if equity > 0 else 0.0
        out = {
            ticker: round(value * scale, 6)
            for ticker, value in clean.items()
            if ticker != "CASH" and value > 1e-9
        }
        out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
        return out
    out = {
        ticker: round(value, 6)
        for ticker, value in clean.items()
        if ticker != "CASH" and value > 1e-9
    }
    out["CASH"] = round(max(1.0 - sum(out.values()), 0.0), 6)
    return out


def _turnover(target: dict[str, Any], current: dict[str, Any]) -> float:
    keys = set(target) | set(current)
    return sum(
        abs(float(target.get(ticker, 0.0) or 0.0) - float(current.get(ticker, 0.0) or 0.0))
        for ticker in keys
    ) / 2.0


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    text = str(value)
    return [text] if text else []


def _clean_weights(raw: dict[str, Any] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, value in (raw or {}).items():
        clean = str(ticker or "").upper().strip()
        if not clean:
            continue
        parsed = _optional_float(value)
        out[clean] = max(parsed if parsed is not None else 0.0, 0.0)
    return out


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_round(value: Any) -> float | None:
    parsed = _optional_float(value)
    return round(parsed, 6) if parsed is not None else None


def _target_builder_mode(mode: str) -> str:
    clean = str(mode or "").strip()
    if clean in {"target_builder_gated", "target_builder_shadow"}:
        return clean
    return "target_builder_shadow"


def _unique(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out
