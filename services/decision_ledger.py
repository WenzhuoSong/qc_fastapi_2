"""Per-ticker decision evidence ledger.

Phase 1 is intentionally narrow: aggregate current holdings, risk output, and
position governance output into an auditable ticker ledger. This module must not
recompute position governance decisions.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


STATIC_REASON_SOURCE_EFFECTS: dict[str, tuple[tuple[str, str], ...]] = {
    "advisory_basket_loss_review": (
        ("knowledge", "satellite_basket_loss_review"),
        ("qc", "unrealized_loss_review"),
        ("strategy", "advisory_or_weaker_support"),
    ),
    "basket_review": (("knowledge", "correlated_basket_review"),),
    "hard_risk": (("news", "hard_risk"),),
    "high_atr": (("qc", "high_atr"),),
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
            historical_evidence=historical_evidence.get(ticker),
            base_weight=base_weights.get(ticker),
            strategy_target=strategy_targets.get(ticker),
            synthesizer_target=synthesizer_targets.get(ticker),
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
            "risk_approved": approved,
            "execution_status": execution_status,
            "turnover": _turnover(risk.get("rebalance_actions") or []),
            "ticker_count": len(rows),
            "governance_available": governance_available,
        },
        "tickers": rows,
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
    historical_evidence: dict[str, Any] | None,
    base_weight: float | None,
    strategy_target: float | None,
    synthesizer_target: float | None,
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
        risk_target=target_weight,
        governance_decision=governance_decision if governance_available else None,
        risk_approved=risk_approved,
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
        "source_effects": _source_effects(
            reason_codes=reason_codes,
            evidence_used=evidence_used,
            trade_lifecycle=trade_lifecycle,
        ),
        "trade_lifecycle": trade_lifecycle,
        "proposed_action": proposed,
        "final_action": final_action,
        "execution_status": "not_sent" if not risk_approved else "unknown",
        "risk_result": "approved" if risk_approved else "blocked",
        "governance_available": governance_available,
        "reason_codes": reason_codes,
        "explanation": governance_explanation,
        "placeholders": {
            "scorecard": None,
            "execution_audit": None,
            "etf_historical": None if historical_evidence else "missing",
            "etf_intraday": None if holding_meta else "missing",
            "base_weight": None if base_weight is not None else "missing",
            "strategy_target": None if strategy_target is not None else "missing",
            "synthesizer_target": None if synthesizer_target is not None else "missing",
        },
    }


def _sparse_trade_lifecycle(
    *,
    current_weight: float,
    base_weight: float | None,
    strategy_target: float | None,
    synthesizer_target: float | None,
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
    if stage == "synthesizer_target":
        return "strategy"
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
