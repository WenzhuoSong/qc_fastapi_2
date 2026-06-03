"""Diagnostic-only alpha authority readiness report.

This report is the handoff between cleaned strategy/ETF mapping semantics and
future alpha attribution consumption. It does not promote strategies, mutate
weights, or change any trading gate.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any


CONTRACT_VERSION = "alpha_readiness_report_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"
MIN_CANDIDATE_LIVE_SAMPLES = 30
MIN_CANDIDATE_MAPPING_COVERAGE = 0.80
RECENT_MAPPING_WINDOW_CYCLES = 10


def build_alpha_readiness_report(
    *,
    mapping_audit: dict[str, Any],
    strategy_evidence: dict[str, Any] | None = None,
    alpha_decision_profiles: dict[str, Any] | None = None,
    recent_mapping_audits: list[dict[str, Any]] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Return deterministic diagnostic readiness rows for strategy authority.

    ``suggested_authority`` is review metadata only. This function never returns
    ``gated`` because gated alpha consumption is out of scope for this
    stabilization plan.
    """
    mapping = mapping_audit if isinstance(mapping_audit, dict) else {}
    evidence = strategy_evidence if isinstance(strategy_evidence, dict) else {}
    profiles = alpha_decision_profiles if isinstance(alpha_decision_profiles, dict) else {}
    evidence_stats = _strategy_evidence_stats(evidence)
    profile_stats = _alpha_profile_stats(profiles)
    mapping_history = _mapping_history_stats(recent_mapping_audits or [mapping])

    strategy_ids = sorted(
        set((mapping.get("strategy_coverage") or {}).keys())
        | set(evidence_stats.keys())
        | set(profile_stats.keys())
    )
    rows = [
        _readiness_row(
            strategy_id=strategy_id,
            mapping_coverage=(mapping.get("strategy_coverage") or {}).get(strategy_id) or {},
            evidence=evidence_stats.get(strategy_id) or {},
            profile=profile_stats.get(strategy_id) or {},
            mapping_history=mapping_history.get(strategy_id) or {},
        )
        for strategy_id in strategy_ids
    ]
    counts = Counter(str(row.get("suggested_authority") or "unknown") for row in rows)
    hard_mapping_error_count = sum(int(row.get("mapping_error_count") or 0) for row in rows)
    warnings: list[str] = []
    history_cycles = len(recent_mapping_audits or [mapping])
    if history_cycles < RECENT_MAPPING_WINDOW_CYCLES:
        warnings.append(
            f"mapping_error_history_short:{history_cycles}/{RECENT_MAPPING_WINDOW_CYCLES}"
        )

    return {
        "contract_version": CONTRACT_VERSION,
        "generated_at": (generated_at or datetime.now(timezone.utc)).replace(microsecond=0).isoformat(),
        "status": "available" if rows else "insufficient_data",
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "diagnostic_only": True,
        "attribution_trade_authority": "none",
        "gated_authority_out_of_scope": True,
        "criteria": {
            "candidate_min_live_sample_count": MIN_CANDIDATE_LIVE_SAMPLES,
            "candidate_min_mapping_coverage_pct": MIN_CANDIDATE_MAPPING_COVERAGE,
            "candidate_requires_non_negative_residual_alpha": True,
            "candidate_requires_no_hard_mapping_errors": True,
            "candidate_requires_no_recurring_hard_mapping_error_cycles": RECENT_MAPPING_WINDOW_CYCLES,
            "gated_out_of_scope": True,
        },
        "strategy_count": len(rows),
        "authority_counts": dict(sorted(counts.items())),
        "hard_mapping_error_count": hard_mapping_error_count,
        "candidate_count": counts.get("candidate", 0),
        "rows": rows,
        "warnings": warnings,
    }


def build_current_alpha_readiness_report(
    *,
    strategy_evidence: dict[str, Any] | None = None,
    alpha_decision_profiles: dict[str, Any] | None = None,
    recent_mapping_audits: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build readiness using the current policy universe mapping audit."""
    from services.strategy_mapping_audit import build_current_strategy_mapping_audit

    return build_alpha_readiness_report(
        mapping_audit=build_current_strategy_mapping_audit(),
        strategy_evidence=strategy_evidence or {},
        alpha_decision_profiles=alpha_decision_profiles or {},
        recent_mapping_audits=recent_mapping_audits,
    )


def _readiness_row(
    *,
    strategy_id: str,
    mapping_coverage: dict[str, Any],
    evidence: dict[str, Any],
    profile: dict[str, Any],
    mapping_history: dict[str, Any],
) -> dict[str, Any]:
    mapping_coverage_pct = _to_float(mapping_coverage.get("coverage_pct"), 0.0) or 0.0
    mapping_error_count = int(_to_float(mapping_coverage.get("mapping_error_rows"), 0) or 0)
    live_sample_count = int(_to_float(profile.get("live_sample_count"), 0) or 0)
    residual_alpha = _to_float(profile.get("residual_alpha_latest"), None)
    recurring_mapping_errors = int(_to_float(mapping_history.get("hard_mapping_error_cycles"), 0) or 0)
    explicitly_disabled = bool(evidence.get("explicitly_disabled"))

    blockers: list[str] = []
    reasons: list[str] = []
    if explicitly_disabled:
        blockers.append("strategy_explicitly_disabled")
    if mapping_error_count > 0:
        blockers.append("current_hard_mapping_errors")
    if recurring_mapping_errors > 0:
        blockers.append("recurring_hard_mapping_errors")

    if blockers:
        suggested_authority = "disabled"
    else:
        if live_sample_count < MIN_CANDIDATE_LIVE_SAMPLES:
            reasons.append("live_sample_count_below_30")
        if mapping_coverage_pct < MIN_CANDIDATE_MAPPING_COVERAGE:
            reasons.append("mapping_coverage_below_0_80")
        if residual_alpha is None:
            reasons.append("residual_alpha_missing")
        elif residual_alpha < 0.0:
            reasons.append("residual_alpha_negative")
        suggested_authority = "candidate" if not reasons else "advisory"

    return {
        "strategy_id": strategy_id,
        "mapping_coverage_pct": round(mapping_coverage_pct, 6),
        "eligible_ticker_count": int(_to_float(mapping_coverage.get("eligible_ticker_count"), 0) or 0),
        "voted_or_watch_rows": int(_to_float(mapping_coverage.get("voted_or_watch_rows"), 0) or 0),
        "mapped_rows": int(_to_float(mapping_coverage.get("mapped_rows"), 0) or 0),
        "watch_rows": int(_to_float(mapping_coverage.get("watch_rows"), 0) or 0),
        "voted_signal_count": int(_to_float(evidence.get("voted_signal_count"), 0) or 0),
        "abstain_count_by_reason": evidence.get("abstain_count_by_reason") or {},
        "mapping_error_count": mapping_error_count,
        "live_sample_count": live_sample_count,
        "residual_alpha_latest": residual_alpha,
        "residual_alpha_regime_specific": profile.get("residual_alpha_regime_specific") or {},
        "redundancy_cluster": profile.get("redundancy_cluster"),
        "max_positive_correlation": profile.get("max_positive_correlation"),
        "suggested_authority": suggested_authority,
        "readiness_reasons": reasons,
        "authority_blockers": blockers,
        "mapping_error_cycles_last_10": recurring_mapping_errors,
        "diagnostic_only": True,
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
    }


def _strategy_evidence_stats(evidence: dict[str, Any]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for card in _evidence_cards(evidence):
        strategy_id = str(card.get("strategy") or card.get("strategy_name") or "").strip()
        if not strategy_id:
            continue
        row = stats.setdefault(
            strategy_id,
            {
                "voted_signal_count": 0,
                "mapping_error_count": 0,
                "abstain_count_by_reason": Counter(),
                "explicitly_disabled": False,
            },
        )
        vote_status = str(card.get("vote_status") or "voted")
        if vote_status == "voted":
            row["voted_signal_count"] += 1
        elif vote_status == "mapping_error":
            row["mapping_error_count"] += 1
        elif vote_status == "abstain":
            row["abstain_count_by_reason"][_abstain_reason(card)] += 1

    for strategy_row in _strategy_rows(evidence):
        strategy_id = str(strategy_row.get("strategy") or strategy_row.get("strategy_name") or "").strip()
        if not strategy_id:
            continue
        row = stats.setdefault(
            strategy_id,
            {
                "voted_signal_count": 0,
                "mapping_error_count": 0,
                "abstain_count_by_reason": Counter(),
                "explicitly_disabled": False,
            },
        )
        status = str(strategy_row.get("status") or strategy_row.get("suggested_use") or "").lower()
        if status == "disabled":
            row["explicitly_disabled"] = True

    return {
        strategy_id: {
            **row,
            "abstain_count_by_reason": dict(sorted(row["abstain_count_by_reason"].items())),
        }
        for strategy_id, row in stats.items()
    }


def _alpha_profile_stats(profiles: dict[str, Any]) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = {}
    for row in profiles.get("rows") or []:
        if not isinstance(row, dict):
            continue
        strategy_id = str(row.get("strategy_id") or "").strip()
        if not strategy_id:
            continue
        current = stats.setdefault(
            strategy_id,
            {
                "live_sample_count": 0,
                "residual_alpha_latest": None,
                "residual_alpha_regime_specific": {},
                "redundancy_cluster": None,
                "max_positive_correlation": None,
            },
        )
        current["live_sample_count"] = max(
            int(current.get("live_sample_count") or 0),
            _live_sample_count(row),
        )
        residual = _to_float(row.get("residual_alpha"), None)
        sample_count = int(_to_float(row.get("sample_count"), 0) or 0)
        if residual is not None and (
            current.get("_residual_sample_count") is None
            or sample_count >= int(current.get("_residual_sample_count") or 0)
        ):
            current["residual_alpha_latest"] = residual
            current["_residual_sample_count"] = sample_count
        regime = str(row.get("regime") or "").strip()
        if regime and residual is not None:
            current["residual_alpha_regime_specific"][regime] = residual
        if current.get("redundancy_cluster") is None:
            current["redundancy_cluster"] = row.get("independence_cluster_id")
        max_corr = _to_float(row.get("max_positive_correlation"), None)
        if max_corr is not None:
            existing = _to_float(current.get("max_positive_correlation"), None)
            current["max_positive_correlation"] = max_corr if existing is None else max(existing, max_corr)

    for row in stats.values():
        row.pop("_residual_sample_count", None)
    return stats


def _mapping_history_stats(recent_audits: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    history: dict[str, dict[str, Any]] = {}
    for audit in (recent_audits or [])[-RECENT_MAPPING_WINDOW_CYCLES:]:
        if not isinstance(audit, dict):
            continue
        for strategy_id, coverage in (audit.get("strategy_coverage") or {}).items():
            row = history.setdefault(str(strategy_id), {"hard_mapping_error_cycles": 0})
            if int(_to_float((coverage or {}).get("mapping_error_rows"), 0) or 0) > 0:
                row["hard_mapping_error_cycles"] += 1
    return history


def _evidence_cards(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(evidence.get("evidence_card_rows"), list):
        return [row for row in evidence.get("evidence_card_rows") or [] if isinstance(row, dict)]
    cards: list[dict[str, Any]] = []
    for strategy in evidence.get("strategy_results") or []:
        if not isinstance(strategy, dict):
            continue
        strategy_id = strategy.get("strategy_name") or strategy.get("strategy")
        for card in strategy.get("evidence_cards") or []:
            if isinstance(card, dict):
                cards.append({"strategy": strategy_id, **card})
    return cards


def _strategy_rows(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(evidence.get("strategy_rows"), list):
        return [row for row in evidence.get("strategy_rows") or [] if isinstance(row, dict)]
    return [
        row for row in evidence.get("strategy_results") or []
        if isinstance(row, dict)
    ]


def _abstain_reason(card: dict[str, Any]) -> str:
    for key in ("abstain_reason", "vote_reason_code", "reason"):
        value = str(card.get(key) or "").strip()
        if value:
            return value
    diagnostics = card.get("vote_diagnostics") if isinstance(card.get("vote_diagnostics"), dict) else {}
    value = str(diagnostics.get("reason_code") or "").strip()
    return value or "unknown"


def _live_sample_count(row: dict[str, Any]) -> int:
    explicit = _to_float(row.get("live_sample_count"), None)
    if explicit is not None:
        return int(max(explicit, 0))
    source_buckets = {str(item) for item in row.get("source_buckets") or []}
    source_bucket = str(row.get("source_bucket") or "")
    if "live_paper" in source_buckets or source_bucket == "live_paper":
        return int(_to_float(row.get("sample_count"), 0) or 0)
    return 0


def _to_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
