"""Per-ticker decision evidence ledger.

Phase 1 is intentionally narrow: aggregate current holdings, risk output, and
position governance output into an auditable ticker ledger. This module must not
recompute position governance decisions.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from services.execution_policy import get_policy, get_role, policy_snapshot


STATIC_REASON_SOURCE_EFFECTS: dict[str, tuple[tuple[str, str], ...]] = {
    "advisory_basket_loss_review": (
        ("knowledge", "satellite_basket_loss_review"),
        ("qc", "unrealized_loss_review"),
        ("strategy", "advisory_or_weaker_support"),
    ),
    "advisory_basket_loss_auto_trim": (("risk", "full_auto_governance_auto_trim"),),
    "basket_review": (("knowledge", "correlated_basket_review"),),
    "hard_risk": (("news", "hard_risk"),),
    "high_atr": (("qc", "high_atr"),),
    "hedge_only_requires_hedge_intent": (("risk", "hedge_only_guard"),),
    "human_required": (("scorecard", "human_required"),),
    "position_governance_missing": (("risk", "position_governance_missing"),),
    "replacement_candidate": (("strategy", "replacement_candidate"),),
    "risk_rejected": (("risk", "risk_rejected"),),
    "scorecard_human_required": (("scorecard", "scorecard_human_required"),),
    "scorecard_limit": (("scorecard", "scorecard_limit"),),
    "stale_evidence": (("scorecard", "stale_evidence"),),
    "strategy_support_weak": (("strategy", "strategy_support_weak"),),
    "style_limit": (("risk", "style_limit"),),
    "turnover_limit": (("risk", "turnover_limit"),),
    "unrealized_loss_review": (("qc", "unrealized_loss_review"),),
    "winner_risk_budget_review": (("qc", "winner_risk_budget_review"),),
}


DISPLAY_REASON_CODE_REPLACEMENTS: dict[str, str] = {
    "human_required": "review_flag",
    "scorecard_human_required": "scorecard_tightened",
}

DISPLAY_SOURCE_EFFECT_REPLACEMENTS: dict[str, str] = {
    "human_required": "review_flag",
    "scorecard_human_required": "scorecard_tightened",
}


def build_decision_ledger(
    *,
    evidence_bundle: dict[str, Any] | None = None,
    market_scorecard: dict[str, Any] | None = None,
    strategy_output: dict[str, Any] | None = None,
    synthesizer_output: dict[str, Any] | None = None,
    risk_output: dict[str, Any] | None = None,
    position_governance: dict[str, Any] | None = None,
    execution_audit: dict[str, Any] | None = None,
    current_holdings: dict[str, Any] | list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build a conservative audit ledger without deciding trades."""
    risk = risk_output or {}
    governance = _as_dict(position_governance) if position_governance is not None else _as_dict(risk.get("position_governance"))
    holdings = _normalize_current_holdings(current_holdings)
    current_weights = holdings["weights"]
    holding_meta = holdings["meta"]
    historical_evidence = _historical_evidence_by_ticker(evidence_bundle or {})
    base_weights = _extract_base_weights(strategy_output or {})
    strategy_targets = _extract_strategy_targets(strategy_output or {}, evidence_bundle or {})
    synthesizer_targets = _clean_weight_map((synthesizer_output or {}).get("adjusted_weights") or {})
    portfolio_construction_targets = _portfolio_construction_targets(risk)
    target_builder_targets = _target_builder_targets(risk)
    target_builder_diagnostics = _target_builder_diagnostics(risk)
    hedge_intent = _hedge_intent_payload(risk)
    target_weights = _clean_weight_map(risk.get("target_weights") or {})
    proposed_actions = _actions_by_ticker(risk.get("rebalance_actions") or [])
    approved = bool(risk.get("approved", False))
    governance_available = _governance_available(governance)
    warnings: list[str] = []
    missing_evidence: list[dict[str, Any]] = []

    if not governance_available:
        warnings.append("position_governance_missing")
        missing_evidence.append({
            "kind": "position_governance",
            "severity": "warning",
            "reason": "position_governance_missing",
        })

    decision_by_ticker = _governance_decisions_by_ticker(governance if governance_available else {})
    explanation_by_ticker = _governance_explanations_by_ticker(governance if governance_available else {})
    advisory_by_ticker = _advisory_overrides_by_ticker(governance if governance_available else {})
    tickers = _ledger_tickers(
        current_weights=current_weights,
        target_weights=target_weights,
        proposed_actions=proposed_actions,
        decision_by_ticker=decision_by_ticker,
    )

    rows = {
        ticker: _build_ticker_row(
            ticker=ticker,
            current_weight=current_weights.get(ticker, 0.0),
            holding_meta=holding_meta.get(ticker) or {},
            target_weight=target_weights.get(ticker),
            proposed_action=proposed_actions.get(ticker),
            risk=risk,
            risk_approved=approved,
            governance_available=governance_available,
            governance_decision=decision_by_ticker.get(ticker),
            governance_explanation=explanation_by_ticker.get(ticker),
            advisory_override=advisory_by_ticker.get(ticker),
            historical_evidence=historical_evidence.get(ticker),
            base_weight=base_weights.get(ticker),
            strategy_target=strategy_targets.get(ticker),
            synthesizer_target=synthesizer_targets.get(ticker),
            portfolio_construction_target=portfolio_construction_targets.get(ticker),
            target_builder_target=target_builder_targets.get(ticker),
            target_builder_diagnostics=target_builder_diagnostics,
            hedge_intent=hedge_intent,
        )
        for ticker in tickers
    }

    execution_status = _phase1_execution_status(
        risk_approved=approved,
        execution_audit=execution_audit,
    )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "phase": "phase_3_sparse_lifecycle",
        "portfolio_summary": {
            "market_permission": (market_scorecard or {}).get("investment_permission"),
            "require_human_confirmation": (market_scorecard or {}).get("require_human_confirmation"),
            "scorecard_tightened": bool((market_scorecard or {}).get("require_human_confirmation")),
            "scorecard_tightening_classes": list((market_scorecard or {}).get("confirmation_classes") or []),
            "risk_approved": approved,
            "execution_status": execution_status,
            "target_construction_mode": risk.get("target_construction_mode"),
            "raw_llm_adjusted_weights_consumed": risk.get("raw_llm_adjusted_weights_consumed"),
            "policy_version": target_builder_diagnostics.get("policy_version") or policy_snapshot()["version"],
            "cash_raised_by_policy_cap": target_builder_diagnostics.get("cash_raised_by_policy_cap"),
            "policy_cap_events": target_builder_diagnostics.get("policy_cap_events") or [],
            "final_policy_version": risk.get("final_policy_version"),
            "final_policy_cap_events": risk.get("final_policy_cap_events") or [],
            "minimum_weight_floor_events": risk.get("minimum_weight_floor_events") or [],
            "active_basket_policy": risk.get("active_basket_policy") or {},
            "final_policy_cash_raised": risk.get("final_policy_cash_raised"),
            "final_policy_cash_raised_by_minimum_weight_floor": risk.get(
                "final_policy_cash_raised_by_minimum_weight_floor"
            ),
            "final_policy_cap_triggered": bool(risk.get("final_policy_cap_triggered")),
            "hedge_intent": _compact_hedge_intent(hedge_intent),
            "portfolio_construction": _compact_portfolio_construction(risk.get("portfolio_construction_shadow")),
            "turnover": _turnover(risk.get("rebalance_actions") or []),
            "ticker_count": len(rows),
            "governance_available": governance_available,
        },
        "tickers": rows,
        "position_advisory_overrides": list(advisory_by_ticker.values())[:12],
        "missing_evidence": missing_evidence,
        "warnings": warnings,
        "placeholders": {
            "scorecard_evidence": None,
            "execution_audit": None,
            "etf_historical_evidence": "hydrated_from_empirical_profiles_when_available",
            "etf_intraday_evidence": "hydrated_from_current_holdings_when_available",
            "full_trade_lifecycle": "base_strategy_synthesizer_available_when_sources_present",
        },
    }


def apply_execution_audit_to_decision_ledger(
    ledger: dict[str, Any] | None,
    execution_audit: dict[str, Any] | None,
) -> dict[str, Any]:
    """Attach execution audit results without changing proposed/final decisions."""
    if not isinstance(ledger, dict) or not ledger:
        return ledger or {}
    audit = _compact_execution_audit(execution_audit or {})
    if not audit:
        return ledger
    out = dict(ledger)
    summary = dict(out.get("portfolio_summary") or {})
    status = audit.get("action_status") or "unknown"
    summary["execution_status"] = status
    summary["execution_audit_attached"] = True
    summary["cmd_id"] = audit.get("command_id")
    summary["qc_status"] = audit.get("qc_status") or status
    summary["qc_rejection_reason"] = audit.get("qc_rejection_reason") or audit.get("reason")
    summary["qc_timestamp"] = audit.get("qc_timestamp") or audit.get("recorded_at")
    out["portfolio_summary"] = summary
    placeholders = dict(out.get("placeholders") or {})
    placeholders["execution_audit"] = "hydrated"
    out["placeholders"] = placeholders

    affected = _execution_audit_tickers(execution_audit or {})
    rows: dict[str, Any] = {}
    for ticker, row in (out.get("tickers") or {}).items():
        if not isinstance(row, dict):
            rows[ticker] = row
            continue
        row_out = dict(row)
        key = str(row_out.get("ticker") or ticker).upper()
        if not affected or key in affected:
            row_out["execution_status"] = status
            row_out["cmd_id"] = audit.get("command_id")
            row_out["qc_status"] = audit.get("qc_status") or status
            row_out["qc_rejection_reason"] = audit.get("qc_rejection_reason") or audit.get("reason")
            row_out["qc_timestamp"] = audit.get("qc_timestamp") or audit.get("recorded_at")
            row_out["execution_audit"] = audit
            row_out["actual_execution_action"] = _actual_execution_action(row_out, status)
            row_placeholders = dict(row_out.get("placeholders") or {})
            row_placeholders["execution_audit"] = None
            row_out["placeholders"] = row_placeholders
        rows[ticker] = row_out
    out["tickers"] = rows
    return out


def _compact_execution_audit(audit: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(audit, dict) or not audit:
        return {}
    return {
        "action_status": audit.get("action_status"),
        "command_id": audit.get("command_id"),
        "qc_status": audit.get("qc_status") or audit.get("action_status"),
        "qc_rejection_reason": audit.get("qc_rejection_reason") or audit.get("reason"),
        "qc_timestamp": audit.get("qc_timestamp") or audit.get("recorded_at"),
        "reason": audit.get("reason"),
        "estimated_cost_pct": audit.get("estimated_cost_pct"),
        "recorded_at": audit.get("recorded_at"),
        "rebalance_actions": (audit.get("rebalance_actions") or [])[:10],
        "has_sent_weights": bool(audit.get("sent_weights")),
        "has_proposed_weights": bool(audit.get("proposed_weights")),
    }


def _execution_audit_tickers(audit: dict[str, Any]) -> set[str]:
    tickers: set[str] = set()
    for action in audit.get("rebalance_actions") or []:
        if isinstance(action, dict):
            ticker = str(action.get("ticker") or "").upper().strip()
            if ticker and ticker != "CASH":
                tickers.add(ticker)
    for key in ("sent_weights", "proposed_weights"):
        weights = audit.get(key) or {}
        if isinstance(weights, dict):
            tickers.update(
                str(ticker).upper().strip()
                for ticker, value in weights.items()
                if str(ticker).upper().strip() != "CASH" and _to_float(value, 0.0) > 0
            )
    return tickers


def _actual_execution_action(row: dict[str, Any], status: str) -> str:
    if status in {"accepted", "sent", "filled", "proposed"}:
        return str(row.get("final_action") or row.get("proposed_action") or "unknown")
    if status in {"rejected", "failed", "skipped"}:
        return "none"
    return "unknown"


def _build_ticker_row(
    *,
    ticker: str,
    current_weight: float,
    holding_meta: dict[str, Any],
    target_weight: float | None,
    proposed_action: dict[str, Any] | None,
    risk: dict[str, Any],
    risk_approved: bool,
    governance_available: bool,
    governance_decision: dict[str, Any] | None,
    governance_explanation: dict[str, Any] | None,
    advisory_override: dict[str, Any] | None,
    historical_evidence: dict[str, Any] | None,
    base_weight: float | None,
    strategy_target: float | None,
    synthesizer_target: float | None,
    portfolio_construction_target: float | None,
    target_builder_target: float | None,
    target_builder_diagnostics: dict[str, Any],
    hedge_intent: dict[str, Any],
) -> dict[str, Any]:
    proposed = _proposed_action(proposed_action, current_weight, target_weight)
    governance_reason_codes = list((governance_decision or {}).get("reason_codes") or [])
    risk_reason_codes = _risk_reason_codes(risk, risk_approved)
    reason_codes = _unique(governance_reason_codes + risk_reason_codes)

    if not governance_available:
        final_action = "unknown"
        reason_codes = _unique(reason_codes + ["position_governance_missing"])
    elif not risk_approved:
        final_action = "none"
    else:
        final_action = _final_action_from_governance(governance_decision, proposed)

    evidence_used = {
        "news": None,
        "historical": _historical_evidence(historical_evidence),
        "intraday": _intraday_evidence(holding_meta),
        "strategy": None,
        "scorecard": None,
        "position_governance": _governance_evidence(governance_decision, governance_available),
    }
    trade_lifecycle = _sparse_trade_lifecycle(
        current_weight=current_weight,
        base_weight=base_weight,
        strategy_target=strategy_target,
        synthesizer_target=synthesizer_target,
        portfolio_construction_target=portfolio_construction_target,
        target_builder_target=target_builder_target,
        advisory_override=advisory_override,
        risk_target=target_weight,
        governance_decision=governance_decision if governance_available else None,
        risk_approved=risk_approved,
    )
    explanation = _execution_explanation(
        ticker=ticker,
        current_weight=current_weight,
        target_weight=target_weight,
        risk_approved=risk_approved,
        governance_explanation=governance_explanation,
        governance_decision=governance_decision if governance_available else None,
        advisory_override=advisory_override,
        trade_lifecycle=trade_lifecycle,
        reason_codes=reason_codes,
        risk=risk,
    )
    execution_policy = _execution_policy_context(
        ticker=ticker,
        target_builder_diagnostics=target_builder_diagnostics,
    )
    hedge_path = _hedge_path_context(ticker, hedge_intent)
    if explanation is not None:
        explanation["entered_via_hedge_path"] = hedge_path["entered_via_hedge_path"]
        explanation["hedge_trigger_reasons"] = hedge_path["hedge_trigger_reasons"]
    source_effects = _source_effects(
        reason_codes=reason_codes,
        evidence_used=evidence_used,
        trade_lifecycle=trade_lifecycle,
    )

    return {
        "ticker": ticker,
        "current": {
            "weight": round(float(current_weight or 0.0), 6),
            "quantity": _first_present(holding_meta, "quantity", "qty"),
            "market_value": _first_present(holding_meta, "market_value", "value"),
            "average_price": _first_present(holding_meta, "average_price", "avg_price"),
            "unrealized_pnl": _first_present(holding_meta, "unrealized_pnl", "unrealized"),
            "unrealized_pnl_pct": _first_present(holding_meta, "unrealized_pnl_pct", "unrealized_percentage"),
            "holding_days": holding_meta.get("holding_days"),
        },
        "evidence_used": evidence_used,
        "source_effects": source_effects,
        "display_source_effects": _display_source_effects(source_effects),
        "trade_lifecycle": trade_lifecycle,
        "llm_advisory": _compact_advisory_override(advisory_override),
        "execution_policy": execution_policy,
        "hedge_path": hedge_path,
        "proposed_action": proposed,
        "final_action": final_action,
        "execution_status": "not_sent" if not risk_approved else "unknown",
        "risk_result": "approved" if risk_approved else "blocked",
        "governance_available": governance_available,
        "reason_codes": reason_codes,
        "display_reason_codes": _display_reason_codes(reason_codes),
        "explanation": explanation,
        "placeholders": {
            "scorecard": None,
            "execution_audit": None,
            "etf_historical": None if historical_evidence else "missing",
            "etf_intraday": None if holding_meta else "missing",
            "base_weight": None if base_weight is not None else "missing",
            "strategy_target": None if strategy_target is not None else "missing",
            "synthesizer_target": None if synthesizer_target is not None else "missing",
            "portfolio_construction_target": None if portfolio_construction_target is not None else "missing",
            "target_builder_target": None if target_builder_target is not None else "missing",
        },
    }


def _execution_explanation(
    *,
    ticker: str,
    current_weight: float,
    target_weight: float | None,
    risk_approved: bool,
    governance_explanation: dict[str, Any] | None,
    governance_decision: dict[str, Any] | None,
    advisory_override: dict[str, Any] | None,
    trade_lifecycle: dict[str, Any],
    reason_codes: list[str],
    risk: dict[str, Any],
) -> dict[str, Any] | None:
    base = dict(governance_explanation or {})
    if not base and not trade_lifecycle:
        return None
    base["strategy_intent"] = _strategy_intent_text(trade_lifecycle, governance_decision)
    base["llm_effect"] = _llm_effect_text(
        trade_lifecycle=trade_lifecycle,
        advisory_override=advisory_override,
        risk=risk,
    )
    base["construction_effect"] = _construction_effect_text(trade_lifecycle, reason_codes)
    base["risk_governance_effect"] = _risk_governance_effect_text(
        risk_approved=risk_approved,
        governance_decision=governance_decision,
        reason_codes=reason_codes,
    )
    base["final_explanation"] = _final_execution_explanation(
        ticker=ticker,
        current_weight=current_weight,
        target_weight=target_weight,
        governance_decision=governance_decision,
        trade_lifecycle=trade_lifecycle,
        risk_approved=risk_approved,
        explanation=base,
    )
    base["execution_chain"] = {
        "available_stages": trade_lifecycle.get("available_stages") or [],
        "changed_by": trade_lifecycle.get("changed_by") or [],
        "current_weight": trade_lifecycle.get("current_weight"),
        "base_weight": trade_lifecycle.get("base_weight"),
        "strategy_target": trade_lifecycle.get("strategy_target"),
        "diagnostic_llm_target": trade_lifecycle.get("diagnostic_llm_target"),
        "portfolio_construction_target": trade_lifecycle.get("portfolio_construction_target"),
        "target_builder_target": trade_lifecycle.get("target_builder_target"),
        "risk_target": trade_lifecycle.get("risk_target"),
        "governance_target": trade_lifecycle.get("governance_target"),
        "final_target": trade_lifecycle.get("final_target"),
    }
    return base


def _strategy_intent_text(
    trade_lifecycle: dict[str, Any],
    governance_decision: dict[str, Any] | None,
) -> str:
    base = trade_lifecycle.get("base_weight")
    strategy = trade_lifecycle.get("strategy_target")
    support = (governance_decision or {}).get("strategy_support")
    parts = []
    if base is not None:
        parts.append(f"quant baseline={_pct(base)}")
    if strategy is not None:
        parts.append(f"strategy consensus={_pct(strategy)}")
    if support:
        parts.append(f"support={support}")
    return "; ".join(parts) if parts else "no strategy target evidence available"


def _llm_effect_text(
    *,
    trade_lifecycle: dict[str, Any],
    advisory_override: dict[str, Any] | None,
    risk: dict[str, Any],
) -> str:
    raw_consumed = risk.get("raw_llm_adjusted_weights_consumed")
    diagnostic = trade_lifecycle.get("diagnostic_llm_target")
    validated_delta = trade_lifecycle.get("validated_advisory_delta")
    validator = (advisory_override or {}).get("validator_result")
    if raw_consumed is False:
        if validated_delta not in (None, 0, 0.0):
            return f"raw LLM weights not consumed; validated advisory delta={_pct(validated_delta)}"
        if diagnostic is not None:
            return f"raw LLM target {_pct(diagnostic)} recorded as diagnostic only"
        return "raw LLM weights not consumed; advisory is semantic evidence only"
    if validator:
        return f"LLM advisory validator={validator}"
    if diagnostic is not None:
        return f"LLM diagnostic target={_pct(diagnostic)}"
    return "no LLM weight effect recorded"


def _construction_effect_text(trade_lifecycle: dict[str, Any], reason_codes: list[str]) -> str:
    changed = set(str(item) for item in trade_lifecycle.get("changed_by") or [])
    pc = trade_lifecycle.get("portfolio_construction_target")
    tb = trade_lifecycle.get("target_builder_target")
    if "portfolio_construction_target" in changed and pc is not None:
        return f"portfolio construction target={_pct(pc)} after exposure, basket, and turnover constraints"
    if "target_builder_target" in changed and tb is not None:
        return f"target builder deterministic target={_pct(tb)}"
    if any("basket" in str(code) for code in reason_codes):
        return "basket review constrained additional exposure"
    if any(str(code).endswith("_concentration_high") for code in reason_codes):
        return "group concentration constrained additional exposure"
    return "construction left target effectively unchanged"


def _risk_governance_effect_text(
    *,
    risk_approved: bool,
    governance_decision: dict[str, Any] | None,
    reason_codes: list[str],
) -> str:
    decision = (governance_decision or {}).get("decision")
    permission = (governance_decision or {}).get("action_permission")
    if not risk_approved:
        return "risk manager blocked execution; final target reverts to current weight"
    if "scorecard_human_required" in reason_codes:
        return f"governance decision={decision}; scorecard tightened execution"
    if "hard_risk" in reason_codes:
        return f"governance decision={decision}; hard-risk review allows trim/exit only"
    if permission:
        return f"governance decision={decision}; permission={permission}"
    return f"governance decision={decision or 'unknown'}"


def _final_execution_explanation(
    *,
    ticker: str,
    current_weight: float,
    target_weight: float | None,
    governance_decision: dict[str, Any] | None,
    trade_lifecycle: dict[str, Any],
    risk_approved: bool,
    explanation: dict[str, Any],
) -> str:
    final_target = trade_lifecycle.get("final_target")
    decision = (governance_decision or {}).get("decision") or "hold"
    state = explanation.get("position_state")
    support = (governance_decision or {}).get("strategy_support")
    pnl = (governance_decision or {}).get("unrealized_pnl_pct")
    risk_budget = (governance_decision or {}).get("risk_budget_status")
    bits = [
        f"{ticker} {decision} at {_pct(final_target if final_target is not None else target_weight if target_weight is not None else current_weight)}",
    ]
    if isinstance(pnl, (int, float)):
        bits.append(f"PnL={_pct(pnl)}")
    if risk_budget:
        bits.append(f"risk_budget={risk_budget}")
    if support:
        bits.append(f"strategy_support={support}")
    if state:
        bits.append(f"state={state}")
    if not risk_approved:
        bits.append("risk blocked execution")
    changed = trade_lifecycle.get("changed_by") or []
    if changed:
        bits.append("changed_by=" + ",".join(str(item) for item in changed[:3]))
    return "; ".join(bits)


def _sparse_trade_lifecycle(
    *,
    current_weight: float,
    base_weight: float | None,
    strategy_target: float | None,
    synthesizer_target: float | None,
    portfolio_construction_target: float | None,
    target_builder_target: float | None,
    advisory_override: dict[str, Any] | None,
    risk_target: float | None,
    governance_decision: dict[str, Any] | None,
    risk_approved: bool,
) -> dict[str, Any]:
    governance_target = _to_float((governance_decision or {}).get("target_after"), None)
    final_target = governance_target if governance_target is not None else risk_target
    if not risk_approved:
        final_target = current_weight
    stages = {
        "current_weight": round(float(current_weight or 0.0), 6),
        "base_weight": round(float(base_weight), 6) if base_weight is not None else None,
        "strategy_target": round(float(strategy_target), 6) if strategy_target is not None else None,
        "synthesizer_target": round(float(synthesizer_target), 6) if synthesizer_target is not None else None,
        "diagnostic_llm_target": round(float(synthesizer_target), 6) if synthesizer_target is not None else None,
        "portfolio_construction_target": round(float(portfolio_construction_target), 6) if portfolio_construction_target is not None else None,
        "target_builder_target": round(float(target_builder_target), 6) if target_builder_target is not None else None,
        "validated_advisory_delta": _validated_advisory_delta(advisory_override),
        "risk_target": round(float(risk_target), 6) if risk_target is not None else None,
        "governance_target": round(governance_target, 6) if governance_target is not None else None,
        "final_target": round(float(final_target), 6) if final_target is not None else None,
    }
    changed_by: list[str] = []
    if base_weight is not None and abs(float(base_weight) - float(current_weight or 0.0)) > 1e-9:
        changed_by.append("base_weight")
    if strategy_target is not None and base_weight is not None and abs(float(strategy_target) - float(base_weight)) > 1e-9:
        changed_by.append("strategy_target")
    if synthesizer_target is not None and strategy_target is not None and abs(float(synthesizer_target) - float(strategy_target)) > 1e-9:
        changed_by.append("synthesizer_target")
    elif synthesizer_target is not None and base_weight is not None and abs(float(synthesizer_target) - float(base_weight)) > 1e-9:
        changed_by.append("synthesizer_target")
    if portfolio_construction_target is not None and base_weight is not None and abs(float(portfolio_construction_target) - float(base_weight)) > 1e-9:
        changed_by.append("portfolio_construction_target")
    if target_builder_target is not None and base_weight is not None and abs(float(target_builder_target) - float(base_weight)) > 1e-9:
        changed_by.append("target_builder_target")
    if stages["validated_advisory_delta"] is not None and abs(float(stages["validated_advisory_delta"])) > 1e-9:
        changed_by.append("validated_llm_advisory")
    if risk_target is not None and abs(float(risk_target) - float(current_weight or 0.0)) > 1e-9:
        changed_by.append("risk_target")
    if governance_target is not None and risk_target is not None and abs(governance_target - float(risk_target)) > 1e-9:
        changed_by.append("position_governance")
    if not risk_approved:
        changed_by.append("risk_rejected_final_target_current")
    return {
        **stages,
        "available_stages": [
            key for key in (
                "current_weight",
                "base_weight",
                "strategy_target",
                "synthesizer_target",
                "diagnostic_llm_target",
                "portfolio_construction_target",
                "target_builder_target",
                "validated_advisory_delta",
                "risk_target",
                "governance_target",
                "final_target",
            )
            if stages.get(key) is not None
        ],
        "missing_stages": [
            key for key in (
                "base_weight",
                "strategy_target",
                "synthesizer_target",
                "portfolio_construction_target",
                "target_builder_target",
            )
            if stages.get(key) is None
        ],
        "changed_by": changed_by,
        "is_sparse": True,
    }


def _extract_base_weights(strategy_output: dict[str, Any]) -> dict[str, float]:
    return _clean_weight_map(
        strategy_output.get("base_weights")
        or strategy_output.get("quant_base_weights")
        or {}
    )


def _extract_strategy_targets(
    strategy_output: dict[str, Any],
    evidence_bundle: dict[str, Any],
) -> dict[str, float]:
    strategies = evidence_bundle.get("strategies") or {}
    return _clean_weight_map(
        strategy_output.get("strategy_target_weights")
        or strategy_output.get("consensus_weights")
        or strategies.get("consensus_weights")
        or {}
    )


def _historical_evidence(profile: dict[str, Any] | None) -> dict[str, Any] | None:
    if not profile:
        return None
    return {
        "source": profile.get("source"),
        "lookback_days": profile.get("lookback_days"),
        "samples": profile.get("samples"),
        "latest_date": profile.get("latest_date"),
        "avg_return": profile.get("avg_return"),
        "volatility": profile.get("volatility"),
        "max_drawdown": profile.get("max_drawdown"),
        "benchmark_correlation": profile.get("benchmark_correlation"),
        "correlation_top": profile.get("correlation_top") or {},
        "source_state": profile.get("data_quality"),
        "freshness": {
            "source": profile.get("source"),
            "as_of": profile.get("latest_date"),
            "evaluated_at": profile.get("generated_at"),
            "is_stale": True if profile.get("data_quality") == "stale" else False if profile.get("data_quality") else None,
            "state": profile.get("data_quality"),
            "policy": "empirical_profile_provider",
            "reason": profile.get("data_quality"),
        },
    }


def _intraday_evidence(holding_meta: dict[str, Any]) -> dict[str, Any] | None:
    if not holding_meta:
        return None
    fields = {
        key: holding_meta.get(key)
        for key in (
            "price",
            "last_price",
            "market_price",
            "close_price",
            "open_price",
            "return_1d",
            "return_5d",
            "mom_20d",
            "mom_60d",
            "rsi_14",
            "atr_pct",
            "hist_vol_20d",
            "weight_current",
            "weight_target",
            "weight_drift",
        )
        if holding_meta.get(key) is not None
    }
    feature_sources = list(holding_meta.get("feature_sources") or [])
    return {
        "source": "current_holdings",
        "fields": fields,
        "feature_sources": feature_sources,
        "source_state": "available" if fields or feature_sources else "missing",
        "freshness": _freshness_from_feature_sources(feature_sources),
    }


def _freshness_from_feature_sources(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        out.append({
            "source": entry.get("source"),
            "as_of": entry.get("as_of") or entry.get("trading_date"),
            "evaluated_at": None,
            "is_stale": entry.get("is_stale"),
            "state": entry.get("state"),
            "policy": entry.get("policy") or "upstream_feature_provenance",
            "reason": entry.get("reason"),
            "filled_fields": entry.get("filled_fields") or [],
        })
    return out


def _governance_evidence(
    governance_decision: dict[str, Any] | None,
    governance_available: bool,
) -> dict[str, Any] | None:
    if not governance_available:
        return None
    decision = governance_decision or {}
    return {
        "decision": decision.get("decision"),
        "action_permission": decision.get("action_permission"),
        "strategy_support": decision.get("strategy_support"),
        "supporting_strategies": decision.get("supporting_strategies") or [],
        "risk_rank": decision.get("risk_rank"),
        "risk_budget_status": decision.get("risk_budget_status"),
        "current_weight": decision.get("current_weight"),
        "target_before": decision.get("target_before"),
        "target_after": decision.get("target_after"),
        "reason_codes": decision.get("reason_codes") or [],
    }


def _source_effects(
    *,
    reason_codes: list[str],
    evidence_used: dict[str, Any],
    trade_lifecycle: dict[str, Any],
) -> dict[str, list[str]]:
    effects: dict[str, list[str]] = {
        "qc": [],
        "yfinance": [],
        "knowledge": [],
        "news": [],
        "scorecard": [],
        "risk": [],
        "strategy": [],
    }
    for code in reason_codes:
        for source, effect in _static_reason_source_effects(str(code)):
            effects.setdefault(source, [])
            effects[source].append(effect)

    if evidence_used.get("historical"):
        effects["yfinance"].append("empirical_profile_available")
    if evidence_used.get("intraday"):
        effects["qc"].append("current_holdings_available")
    for stage in trade_lifecycle.get("changed_by") or []:
        source = _lifecycle_stage_source(str(stage))
        if source:
            effects[source].append(str(stage))

    return {
        source: _unique(values)
        for source, values in effects.items()
        if values
    }


def _display_reason_codes(reason_codes: list[str]) -> list[str]:
    return _unique([
        DISPLAY_REASON_CODE_REPLACEMENTS.get(str(code), str(code))
        for code in reason_codes
    ])


def _display_source_effects(source_effects: dict[str, list[str]]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for source, effects in (source_effects or {}).items():
        out[source] = _unique([
            DISPLAY_SOURCE_EFFECT_REPLACEMENTS.get(str(effect), str(effect))
            for effect in (effects or [])
        ])
    return {source: effects for source, effects in out.items() if effects}


def _static_reason_source_effects(reason_code: str) -> tuple[tuple[str, str], ...]:
    if reason_code in STATIC_REASON_SOURCE_EFFECTS:
        return STATIC_REASON_SOURCE_EFFECTS[reason_code]
    if reason_code.startswith("failed_check:"):
        return (("risk", reason_code),)
    if reason_code.startswith("risk_reason:"):
        return (("risk", reason_code),)
    if reason_code.endswith("_concentration_high"):
        return (("knowledge", "group_concentration_high"),)
    return ()


def _lifecycle_stage_source(stage: str) -> str | None:
    if stage in {"base_weight", "strategy_target"}:
        return "strategy"
    if stage in {"synthesizer_target", "validated_llm_advisory"}:
        return "strategy"
    if stage == "portfolio_construction_target":
        return "risk"
    if stage == "target_builder_target":
        return "risk"
    if stage in {"risk_target", "risk_rejected_final_target_current"}:
        return "risk"
    if stage == "position_governance":
        return "knowledge"
    return None


def _historical_evidence_by_ticker(evidence_bundle: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    direct_profiles = evidence_bundle.get("empirical_profiles") or {}
    if isinstance(direct_profiles, dict):
        for ticker, profile in direct_profiles.items():
            key = str(ticker or "").upper().strip()
            if key and isinstance(profile, dict):
                out[key] = profile

    resolution = ((evidence_bundle.get("knowledge") or {}).get("resolution") or {})
    for item in resolution.get("advisory_context") or []:
        if not isinstance(item, dict) or item.get("type") != "asset_profile":
            continue
        ticker = str(item.get("id") or "").upper().strip()
        profile = item.get("empirical_behavior") or {}
        if ticker and isinstance(profile, dict) and profile:
            out.setdefault(ticker, profile)
    return out


def _proposed_action(
    action: dict[str, Any] | None,
    current_weight: float,
    target_weight: float | None,
) -> str:
    if action:
        raw = str(action.get("action") or "").lower()
        if raw == "buy":
            return "add"
        if raw == "sell":
            return "trim"
        if raw:
            return raw
    if target_weight is None:
        return "hold"
    delta = float(target_weight or 0.0) - float(current_weight or 0.0)
    if delta > 1e-9:
        return "add"
    if delta < -1e-9:
        return "trim"
    return "hold"


def _final_action_from_governance(
    decision: dict[str, Any] | None,
    proposed: str,
) -> str:
    if not decision:
        return "hold" if proposed == "hold" else proposed
    governance_decision = str(decision.get("decision") or "hold")
    if governance_decision == "add":
        return "add"
    if governance_decision in {"trim", "trim_review"}:
        return "trim"
    return "hold"


def _risk_reason_codes(risk: dict[str, Any], risk_approved: bool) -> list[str]:
    out: list[str] = []
    if not risk_approved:
        out.append("risk_rejected")
    if risk.get("failed_checks"):
        out.extend(f"failed_check:{key}" for key in sorted((risk.get("failed_checks") or {}).keys()))
    for reason in risk.get("rejection_reasons") or []:
        code = _reason_to_code(reason)
        if code:
            out.append(code)
    return _unique(out)


def _reason_to_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if "human confirmation" in lowered:
        return "human_required"
    if "turnover" in lowered:
        return "turnover_limit"
    if "style" in lowered:
        return "style_limit"
    if "scorecard" in lowered:
        return "scorecard_limit"
    if "evidence" in lowered and "stale" in lowered:
        return "stale_evidence"
    return "risk_reason:" + lowered.replace(" ", "_")[:80]


def _phase1_execution_status(
    *,
    risk_approved: bool,
    execution_audit: dict[str, Any] | None,
) -> str:
    if not risk_approved:
        return "not_sent"
    if execution_audit and execution_audit.get("action_status"):
        return str(execution_audit.get("action_status"))
    return "unknown"


def _turnover(actions: list[dict[str, Any]]) -> float:
    return round(sum(abs(_to_float(item.get("weight_delta"), 0.0)) for item in actions), 6)


def _ledger_tickers(
    *,
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    proposed_actions: dict[str, dict[str, Any]],
    decision_by_ticker: dict[str, dict[str, Any]],
) -> list[str]:
    tickers = set(current_weights) | set(target_weights) | set(proposed_actions) | set(decision_by_ticker)
    tickers.discard("CASH")
    return sorted(ticker for ticker in tickers if ticker)


def _governance_available(governance: dict[str, Any] | None) -> bool:
    if not governance:
        return False
    return bool(governance.get("position_decisions"))


def _governance_decisions_by_ticker(governance: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in governance.get("position_decisions") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker:
            out[ticker] = row
    return out


def _governance_explanations_by_ticker(governance: dict[str, Any]) -> dict[str, dict[str, Any]]:
    summary = governance.get("portfolio_summary") or {}
    out: dict[str, dict[str, Any]] = {}
    for row in summary.get("position_explanations") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker:
            out[ticker] = row
    return out


def _advisory_overrides_by_ticker(governance: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in governance.get("advisory_overrides") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker:
            out[ticker] = row
    return out


def _actions_by_ticker(actions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in actions:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if ticker and ticker != "CASH":
            out[ticker] = row
    return out


def _normalize_current_holdings(
    current_holdings: dict[str, Any] | list[dict[str, Any]] | None,
) -> dict[str, Any]:
    if current_holdings is None:
        return {"weights": {}, "meta": {}}
    if isinstance(current_holdings, list):
        meta = _meta_from_holding_rows(current_holdings)
        weights = {
            ticker: _to_float(row.get("weight", row.get("current_weight")), 0.0)
            for ticker, row in meta.items()
        }
        return {"weights": _clean_weight_map(weights), "meta": meta}
    if "current_weights" in current_holdings or "holdings" in current_holdings:
        weights = _clean_weight_map(current_holdings.get("current_weights") or {})
        meta = _meta_from_holding_rows(current_holdings.get("holdings") or [])
        return {"weights": weights, "meta": meta}
    return {"weights": _clean_weight_map(current_holdings), "meta": {}}


def _meta_from_holding_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or row.get("symbol") or "").upper().strip()
        if ticker and ticker != "CASH":
            out[ticker] = row
    return out


def _clean_weight_map(raw: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key, value in (raw or {}).items():
        ticker = str(key or "").upper().strip()
        if not ticker:
            continue
        out[ticker] = _to_float(value, 0.0)
    return out


def _target_builder_targets(risk: dict[str, Any]) -> dict[str, float]:
    for key in ("target_builder_input", "target_builder_shadow"):
        payload = risk.get(key) or {}
        if isinstance(payload, dict) and isinstance(payload.get("target_weights"), dict):
            return _clean_weight_map(payload.get("target_weights") or {})
    return {}


def _target_builder_diagnostics(risk: dict[str, Any]) -> dict[str, Any]:
    for key in ("target_builder_input", "target_builder_shadow"):
        payload = risk.get(key) or {}
        if isinstance(payload, dict) and isinstance(payload.get("diagnostics"), dict):
            diagnostics = dict(payload.get("diagnostics") or {})
            break
    else:
        diagnostics = {}
    diagnostics["final_policy_cap_events"] = risk.get("final_policy_cap_events") or []
    diagnostics["minimum_weight_floor_events"] = risk.get("minimum_weight_floor_events") or []
    diagnostics["final_policy_cash_raised"] = risk.get("final_policy_cash_raised")
    diagnostics["final_policy_version"] = risk.get("final_policy_version")
    diagnostics["final_policy_cap_triggered"] = bool(risk.get("final_policy_cap_triggered"))
    return diagnostics


def _hedge_intent_payload(risk: dict[str, Any]) -> dict[str, Any]:
    diagnostics = _target_builder_diagnostics(risk)
    hedge = diagnostics.get("hedge_intent")
    if isinstance(hedge, dict) and hedge:
        return dict(hedge)
    payload = risk.get("hedge_intent") or {}
    return dict(payload) if isinstance(payload, dict) else {}


def _policy_cap_event_for_ticker(
    ticker: str,
    target_builder_diagnostics: dict[str, Any],
) -> dict[str, Any] | None:
    ticker = str(ticker or "").upper().strip()
    for event in target_builder_diagnostics.get("policy_cap_events") or []:
        if not isinstance(event, dict):
            continue
        if str(event.get("ticker") or "").upper().strip() == ticker:
            return event
    return None


def _final_policy_cap_event_for_ticker(
    ticker: str,
    target_builder_diagnostics: dict[str, Any],
) -> dict[str, Any] | None:
    ticker = str(ticker or "").upper().strip()
    role = get_role(ticker).value
    for event in target_builder_diagnostics.get("final_policy_cap_events") or []:
        if not isinstance(event, dict):
            continue
        if str(event.get("ticker") or "").upper().strip() == ticker:
            return event
        if str(event.get("group_role") or "") == role:
            return event
    return None


def _execution_policy_context(
    *,
    ticker: str,
    target_builder_diagnostics: dict[str, Any],
) -> dict[str, Any]:
    policy = get_policy(ticker)
    cap_event = _policy_cap_event_for_ticker(ticker, target_builder_diagnostics)
    final_cap_event = _final_policy_cap_event_for_ticker(ticker, target_builder_diagnostics)
    return {
        "ticker_role": get_role(ticker).value,
        "single_cap": policy.max_single_weight,
        "group_cap": policy.max_total_group_weight,
        "hedge_only": policy.hedge_only,
        "policy_version": target_builder_diagnostics.get("policy_version") or policy_snapshot()["version"],
        "policy_cap_applied": bool(cap_event),
        "policy_cap_original": (cap_event or {}).get("original"),
        "policy_cap_target": (cap_event or {}).get("capped_to"),
        "policy_group_scaled": False,
        "cash_raised_by_policy_cap": target_builder_diagnostics.get("cash_raised_by_policy_cap"),
        "final_policy_cap_applied": bool(final_cap_event),
        "final_policy_cap_original": (final_cap_event or {}).get("original"),
        "final_policy_cap_target": (final_cap_event or {}).get("capped_to"),
        "final_policy_group_scaled": bool(final_cap_event and final_cap_event.get("group_role")),
        "final_policy_cash_raised": target_builder_diagnostics.get("final_policy_cash_raised"),
    }


def _hedge_path_context(ticker: str, hedge_intent: dict[str, Any]) -> dict[str, Any]:
    ticker = str(ticker or "").upper().strip()
    hedge_instrument = str(hedge_intent.get("hedge_instrument") or "").upper().strip()
    touched = {
        str(item or "").upper().strip()
        for item in (hedge_intent.get("touched_tickers") or [])
    }
    touched.update(
        str(item or "").upper().strip()
        for item in (hedge_intent.get("trim_targets") or [])
    )
    entered = bool(
        hedge_intent.get("applied")
        and (
            ticker == hedge_instrument
            or ticker in touched
            or ticker in {
                str(item or "").upper().strip()
                for item in (
                    hedge_intent.get("hedge_tickers")
                    or hedge_intent.get("allowed_hedge_tickers")
                    or []
                )
            }
        )
    )
    return {
        "entered_via_hedge_path": entered,
        "hedge_trigger_reasons": list(hedge_intent.get("reasons") or hedge_intent.get("trigger_reasons") or []),
        "hedge_severity": hedge_intent.get("severity"),
        "hedge_instrument": hedge_instrument or None,
    }


def _compact_hedge_intent(hedge_intent: dict[str, Any]) -> dict[str, Any] | None:
    if not hedge_intent:
        return None
    triggered = bool(hedge_intent.get("triggered"))
    add_hedge = bool(hedge_intent.get("add_hedge_etf"))
    severity = _to_float(hedge_intent.get("severity"), 0.0)
    return {
        "triggered": triggered,
        "applied": hedge_intent.get("applied"),
        "severity": round(severity, 6),
        "add_hedge_etf": add_hedge,
        "selected_hedge": hedge_intent.get("hedge_instrument"),
        "hedge_instrument": hedge_intent.get("hedge_instrument"),
        "hedge_weight": hedge_intent.get("hedge_weight"),
        "why_not_add_hedge": _explain_hedge_decision(triggered=triggered, add_hedge=add_hedge, severity=severity),
        "reasons": list(hedge_intent.get("reasons") or hedge_intent.get("trigger_reasons") or []),
        "trigger_reasons": list(hedge_intent.get("reasons") or hedge_intent.get("trigger_reasons") or []),
        "trim_targets": list(hedge_intent.get("trim_targets") or []),
        "cash_raise_pct": hedge_intent.get("cash_raise_pct") or hedge_intent.get("target_cash_raise_pct") or hedge_intent.get("cash_raise_target") or 0.0,
    }


def _explain_hedge_decision(*, triggered: bool, add_hedge: bool, severity: float) -> str:
    if not triggered:
        return "hedge_intent_not_triggered"
    if add_hedge:
        return "hedge_etf_selected"
    if severity < 0.70:
        return f"severity_{severity:.2f}_below_threshold_0.70"
    return "unknown"


def _portfolio_construction_targets(risk: dict[str, Any]) -> dict[str, float]:
    payload = risk.get("portfolio_construction_shadow") or {}
    if isinstance(payload, dict) and isinstance(payload.get("target_weights"), dict):
        return _clean_weight_map(payload.get("target_weights") or {})
    return {}


def _compact_portfolio_construction(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict) or not payload:
        return None
    return {
        "mode": (payload.get("diagnostics") or {}).get("mode"),
        "target_weights": payload.get("target_weights") or {},
        "factor_exposures": payload.get("factor_exposures") or {},
        "effective_n": payload.get("effective_n"),
        "turnover": payload.get("turnover") or {},
        "violations": payload.get("violations") or [],
        "active_basket_reviews": (payload.get("diagnostics") or {}).get("active_basket_reviews") or [],
        "execution_effect": "diagnostic_only",
    }


def _compact_advisory_override(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(row, dict) or not row:
        return None
    return {
        "ticker": row.get("ticker"),
        "llm_advisory": row.get("llm_advisory"),
        "validator_result": row.get("validator_result"),
        "deterministic_decision": row.get("deterministic_decision"),
        "final_decision": row.get("final_decision"),
        "target_before_override": row.get("target_before_override"),
        "target_after_override": row.get("target_after_override"),
        "validated_delta": _validated_advisory_delta(row),
        "execution_authority": "none",
    }


def _validated_advisory_delta(row: dict[str, Any] | None) -> float | None:
    if not isinstance(row, dict) or not row:
        return None
    result = str(row.get("validator_result") or "")
    if not result.startswith("accepted"):
        return 0.0
    before = _to_float(row.get("target_before_override"), None)
    after = _to_float(row.get("target_after_override"), None)
    if before is None or after is None:
        return 0.0
    return round(float(after) - float(before), 6)


def _as_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return {
        "position_decisions": getattr(value, "position_decisions", []),
        "blocked_actions": getattr(value, "blocked_actions", []),
        "forced_trims": getattr(value, "forced_trims", []),
        "replacements": getattr(value, "replacements", []),
        "advisory_overrides": getattr(value, "advisory_overrides", []),
        "trade_summary": getattr(value, "trade_summary", {}),
        "portfolio_summary": getattr(value, "portfolio_summary", {}),
        "config": getattr(value, "config", {}),
    }


def _first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    return None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _pct(value: Any) -> str:
    try:
        return f"{float(value):.1%}"
    except (TypeError, ValueError):
        return "n/a"


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out
