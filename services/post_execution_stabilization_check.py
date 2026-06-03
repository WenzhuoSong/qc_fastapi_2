"""Global DoD checks for post-execution stabilization.

This module is read-only and diagnostic-only. It verifies the stabilization
track's completion criteria without changing config, database state, target
weights, or trading behavior.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.alpha_readiness_report import build_current_alpha_readiness_report
from services.strategy_mapping_audit import build_current_strategy_mapping_audit


CONTRACT_VERSION = "post_execution_stabilization_check_v1"
EXECUTION_AUTHORITY = "none"
TARGET_WEIGHT_MUTATION = "none"


def build_post_execution_stabilization_check(
    *,
    qc_fallback_path: str | Path | None = None,
    repo_root: str | Path | None = None,
    max_hard_mapping_errors: int = 0,
) -> dict[str, Any]:
    """Return a read-only Global Definition of Done report."""
    root = Path(repo_root or Path(__file__).resolve().parents[1])
    mapping = build_current_strategy_mapping_audit()
    alpha = build_current_alpha_readiness_report()
    source = _source_contract(root)
    qc_check = _qc_policy_check(qc_fallback_path)

    checks = [
        _check(
            "hard_mapping_errors_within_threshold",
            int(mapping.get("hard_mapping_error_count") or 0) <= int(max_hard_mapping_errors),
            value=mapping.get("hard_mapping_error_count"),
            threshold=max_hard_mapping_errors,
            details="Normal pipeline runs should not report large hard mapping_error counts.",
        ),
        _check(
            "active_advisory_strategy_profiles_present",
            not mapping.get("missing_strategy_profiles"),
            value=mapping.get("missing_strategy_profiles") or [],
            threshold="[]",
            details="Every active/advisory strategy should have a strategy profile.",
        ),
        _check(
            "execution_policy_asset_profiles_present",
            not mapping.get("missing_asset_profiles"),
            value=mapping.get("missing_asset_profiles") or [],
            threshold="[]",
            details="Every execution-policy ticker should have an asset profile or explicit exclusion.",
        ),
        _check(
            "vote_status_semantics_distinct",
            bool(source.get("evidence_matrix_display_policy"))
            and bool(source.get("mapping_audit_hard_vs_watch_reasons")),
            value={
                "evidence_matrix_display_policy": source.get("evidence_matrix_display_policy"),
                "mapping_audit_hard_vs_watch_reasons": source.get("mapping_audit_hard_vs_watch_reasons"),
            },
            threshold="voted/mapping_error visible; watch/abstain collapsed; hard/watch reasons separated",
            details="Watch/abstain/mapping_error must stay semantically distinct in reports.",
        ),
        _check(
            "qc_fallback_policy_check_available",
            bool(qc_check.get("ok")),
            value=qc_check,
            threshold="ok=True",
            details="QC fallback policy can be checked against FastAPI execution_policy.py.",
        ),
        _check(
            "operator_output_uses_scorecard_tightened",
            bool(source.get("scorecard_tightened_display")),
            value=source.get("scorecard_tightened_display"),
            threshold=True,
            details="Operator-facing output should use scorecard_tightened/review_flag for automatic tightening.",
        ),
        _check(
            "dashboard_uses_lifecycle_terminal_status",
            bool(source.get("dashboard_lifecycle_status_priority")),
            value=source.get("dashboard_lifecycle_status_priority"),
            threshold=True,
            details="Dashboard command status should use lifecycle terminal state when available.",
        ),
        _check(
            "alpha_attribution_diagnostic_until_ready",
            alpha.get("execution_authority") == "none"
            and alpha.get("target_weight_mutation") == "none"
            and alpha.get("attribution_trade_authority") == "none"
            and bool(alpha.get("gated_authority_out_of_scope")),
            value={
                "execution_authority": alpha.get("execution_authority"),
                "target_weight_mutation": alpha.get("target_weight_mutation"),
                "attribution_trade_authority": alpha.get("attribution_trade_authority"),
                "gated_authority_out_of_scope": alpha.get("gated_authority_out_of_scope"),
                "authority_counts": alpha.get("authority_counts"),
            },
            threshold="diagnostic only",
            details="Alpha attribution remains diagnostic until mapping and sample-readiness gates are satisfied.",
        ),
        _check(
            "no_new_risk_guard_added_for_semantic_cleanup",
            bool(source.get("stabilization_changes_are_diagnostic")),
            value=source.get("stabilization_changes_are_diagnostic"),
            threshold=True,
            details="This track should not add a new risk guard to solve semantic cleanup.",
        ),
    ]
    failed = [row for row in checks if not row["passed"]]
    return {
        "contract_version": CONTRACT_VERSION,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "status": "passed" if not failed else "failed",
        "execution_authority": EXECUTION_AUTHORITY,
        "target_weight_mutation": TARGET_WEIGHT_MUTATION,
        "diagnostic_only": True,
        "passed_count": len(checks) - len(failed),
        "failed_count": len(failed),
        "checks": checks,
        "mapping_summary": {
            "hard_mapping_error_count": mapping.get("hard_mapping_error_count"),
            "by_reason": mapping.get("by_reason") or {},
            "missing_strategy_profiles": mapping.get("missing_strategy_profiles") or [],
            "missing_asset_profiles": mapping.get("missing_asset_profiles") or [],
        },
        "alpha_readiness_summary": {
            "status": alpha.get("status"),
            "strategy_count": alpha.get("strategy_count"),
            "authority_counts": alpha.get("authority_counts") or {},
            "candidate_count": alpha.get("candidate_count"),
            "warnings": alpha.get("warnings") or [],
        },
        "qc_fallback_policy": qc_check,
    }


def _check(
    name: str,
    passed: bool,
    *,
    value: Any,
    threshold: Any,
    details: str,
) -> dict[str, Any]:
    return {
        "check": name,
        "passed": bool(passed),
        "value": value,
        "threshold": threshold,
        "details": details,
    }


def _source_contract(root: Path) -> dict[str, Any]:
    dashboard = _read_text(root / "dashboard" / "app.py")
    communicator = _read_text(root / "agents" / "communicator.py")
    mapping_audit = _read_text(root / "services" / "strategy_mapping_audit.py")
    alpha_readiness = _read_text(root / "services" / "alpha_readiness_report.py")
    return {
        "evidence_matrix_display_policy": (
            '"default_visible_vote_statuses": ["voted", "mapping_error"]' in dashboard
            and '"default_collapsed_vote_statuses": ["watch", "abstain"]' in dashboard
        ),
        "mapping_audit_hard_vs_watch_reasons": (
            "HARD_MAPPING_REASONS" in mapping_audit
            and "NORMAL_WATCH_REASONS" in mapping_audit
        ),
        "scorecard_tightened_display": (
            "_display_reason_code" in communicator
            and "scorecard_tightened" in communicator
            and "review_flag" in communicator
        ),
        "dashboard_lifecycle_status_priority": (
            "TERMINAL_LIFECYCLE_STATES" in dashboard
            and "_lifecycle_status_by_command" in dashboard
            and "lifecycle_display_status" in dashboard
        ),
        "stabilization_changes_are_diagnostic": (
            'EXECUTION_AUTHORITY = "none"' in alpha_readiness
            and 'TARGET_WEIGHT_MUTATION = "none"' in alpha_readiness
            and "diagnostic_only" in alpha_readiness
        ),
    }


def _qc_policy_check(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {
            "ok": False,
            "reason": "qc_fallback_path_not_provided",
        }
    file_path = Path(path)
    if not file_path.exists():
        return {
            "ok": False,
            "reason": "qc_fallback_path_not_found",
            "path": str(file_path),
        }
    try:
        from tools.generate_qc_fallback_policy import compare_qc_fallback_policy

        return {
            "path": str(file_path),
            **compare_qc_fallback_policy(file_path),
        }
    except Exception as exc:
        return {
            "ok": False,
            "reason": f"{type(exc).__name__}: {exc}",
            "path": str(file_path),
        }


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""
