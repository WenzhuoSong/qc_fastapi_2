"""Operator-facing TargetEnvelope visibility.

This module does not participate in trading decisions. It converts the
post-risk TargetEnvelope payload into a compact dashboard/API contract so the
operator can distinguish account truth, risk-approved target, executable final
target, and diagnostic-only shadow weights.
"""
from __future__ import annotations

from typing import Any


STAGE_ORDER = (
    "risk_approved",
    "position_governance",
    "position_manager",
    "final_policy_cap",
    "execution_throttle",
    "final",
)


def build_target_path_visibility(risk_out: dict[str, Any] | None) -> dict[str, Any]:
    """Build a read-only target path view for dashboard/operator review."""
    risk = risk_out if isinstance(risk_out, dict) else {}
    envelope = risk.get("target_envelope") if isinstance(risk.get("target_envelope"), dict) else {}
    actual = _clean_weights(envelope.get("current_weights") or {})
    risk_approved = _clean_weights(
        envelope.get("risk_approved_target")
        or risk.get("risk_approved_target_weights")
        or risk.get("risk_manager_input_target_weights")
        or {}
    )
    final = _clean_weights(envelope.get("final_target") or risk.get("target_weights") or {})
    legacy_final = _clean_weights(risk.get("target_weights") or {})
    llm_weights = _clean_weights(risk.get("diagnostic_llm_adjusted_weights") or {})
    pc_shadow = _clean_weights(
        ((risk.get("portfolio_construction_shadow") or {}).get("target_weights"))
        if isinstance(risk.get("portfolio_construction_shadow"), dict)
        else {}
    )

    ledger = envelope.get("ledger") if isinstance(envelope.get("ledger"), dict) else {}
    mutations = [
        row for row in (ledger.get("mutations") or [])
        if isinstance(row, dict)
    ]
    mutation_rows = _mutation_rows(
        mutations=mutations,
        actual_weights=actual,
        risk_approved_target=risk_approved,
        final_target=final,
    )
    stage_rows = _stage_rows(
        stage_snapshots=[
            row for row in (envelope.get("stage_snapshots") or [])
            if isinstance(row, dict)
        ],
        mutation_rows=mutation_rows,
    )
    warnings = []
    if envelope.get("bridge_errors"):
        warnings.append("target_envelope_bridge_errors")
    if envelope and not bool(envelope.get("accounting_ok", True)):
        warnings.append("target_envelope_accounting_not_ok")
    if not envelope:
        warnings.append("target_envelope_unavailable")

    return {
        "available": bool(envelope),
        "contract_version": "target_path_visibility_v1",
        "execution_authority": "target_envelope.final_target" if envelope else "unknown",
        "path": " -> ".join(STAGE_ORDER),
        "truth_rows": [
            _surface_row(
                "actual_holdings",
                "QC actual holdings",
                "qc_account_truth",
                actual,
                executable=True,
                visual_class="weight-executable",
                note="Latest normalized account holdings from QC heartbeat/account snapshot.",
            ),
            _surface_row(
                "risk_approved_target",
                "Risk-approved target",
                "risk_manager_approved",
                risk_approved,
                executable=True,
                visual_class="weight-executable",
                note="Frozen target after risk manager approval; start of post-risk envelope.",
            ),
            _surface_row(
                "envelope_final_target",
                "Envelope final target",
                "post_risk_execution_authority",
                final,
                executable=True,
                visual_class="weight-executable",
                note="Only executable post-risk target in active/strict TargetEnvelope mode.",
            ),
        ],
        "diagnostic_surface_rows": [
            _surface_row(
                "legacy_dict_final_target",
                "Legacy dict final target",
                "diagnostic_shadow",
                legacy_final,
                executable=False,
                visual_class="weight-reference",
                note="Compatibility mirror only; not independent execution authority.",
            ),
            _surface_row(
                "advisory_llm_weights",
                "Advisory / LLM weights",
                "advisory_only",
                llm_weights,
                executable=False,
                visual_class="weight-advisory",
                note="Research/advisory surface; must not enter target_builder as executable weights.",
            ),
            _surface_row(
                "pc_shadow_reference_weights",
                "PC shadow/reference weights",
                "reference_only",
                pc_shadow,
                executable=False,
                visual_class="weight-reference",
                note="Portfolio construction shadow/reference output, not executable unless promoted upstream.",
            ),
        ],
        "weight_rows": _combined_weight_rows(
            actual=actual,
            risk_approved=risk_approved,
            final=final,
            legacy_final=legacy_final,
            llm_weights=llm_weights,
            pc_shadow=pc_shadow,
        ),
        "stage_rows": stage_rows,
        "mutation_rows": mutation_rows,
        "accounting": {
            "ok": bool(envelope.get("accounting_ok", False)) if envelope else False,
            "violations": envelope.get("accounting_violations") or [],
            "bridge_errors": envelope.get("bridge_errors") or [],
        },
        "warnings": warnings,
    }


def _surface_row(
    key: str,
    label: str,
    authority: str,
    weights: dict[str, float],
    *,
    executable: bool,
    visual_class: str,
    note: str,
) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "authority": authority,
        "executable": executable,
        "visual_class": visual_class,
        "weight_count": _non_cash_count(weights),
        "top_weights": _top_weights(weights),
        "note": note,
    }


def _mutation_rows(
    *,
    mutations: list[dict[str, Any]],
    actual_weights: dict[str, float],
    risk_approved_target: dict[str, float],
    final_target: dict[str, float],
) -> list[dict[str, Any]]:
    rows = []
    for index, raw in enumerate(mutations):
        ticker = _ticker(raw.get("ticker"))
        if not ticker:
            continue
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        before = _to_float(raw.get("before"))
        after = _to_float(raw.get("after"))
        current = actual_weights.get(ticker, 0.0)
        rows.append(
            {
                "stage": str(metadata.get("stage") or "unknown"),
                "stage_order": _stage_index(str(metadata.get("stage") or "")),
                "index": index,
                "ticker": ticker,
                "mutation_type": raw.get("type") or raw.get("mutation_type"),
                "before": before,
                "after": after,
                "delta": _round(after - before),
                "current": _round(current),
                "risk_approved": _round(risk_approved_target.get(ticker, 0.0)),
                "final": _round(final_target.get(ticker, 0.0)),
                "stage_effect": _direction(after, before),
                "safety_effect": _direction(after, current),
                "tighten_only": bool(raw.get("tighten_only")),
                "conditional": bool(raw.get("conditional")),
                "reason": raw.get("reason"),
            }
        )
    rows.sort(key=lambda row: (int(row.get("stage_order") or 999), int(row.get("index") or 0)))
    return rows


def _stage_rows(
    *,
    stage_snapshots: list[dict[str, Any]],
    mutation_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    seen = set()
    for stage in STAGE_ORDER:
        stage_mutations = [row for row in mutation_rows if row.get("stage") == stage]
        snapshot = _snapshot_for_stage(stage_snapshots, stage)
        rows.append(
            {
                "stage": stage,
                "stage_order": _stage_index(stage),
                "changed_ticker_count": len({row.get("ticker") for row in stage_mutations}),
                "mutation_count": len(stage_mutations),
                "mutation_types": _unique(row.get("mutation_type") for row in stage_mutations),
                "safety_effects": _unique(row.get("safety_effect") for row in stage_mutations),
                "cash_actual": snapshot.get("cash_actual"),
                "cash_matches_requested": snapshot.get("cash_matches_requested"),
                "boundary_only": bool(snapshot.get("boundary_only")),
            }
        )
        seen.add(stage)
    for snapshot in stage_snapshots:
        stage = str(snapshot.get("stage") or "unknown")
        if stage in seen:
            continue
        stage_mutations = [row for row in mutation_rows if row.get("stage") == stage]
        rows.append(
            {
                "stage": stage,
                "stage_order": _stage_index(stage),
                "changed_ticker_count": len({row.get("ticker") for row in stage_mutations}),
                "mutation_count": len(stage_mutations),
                "mutation_types": _unique(row.get("mutation_type") for row in stage_mutations),
                "safety_effects": _unique(row.get("safety_effect") for row in stage_mutations),
                "cash_actual": snapshot.get("cash_actual"),
                "cash_matches_requested": snapshot.get("cash_matches_requested"),
                "boundary_only": bool(snapshot.get("boundary_only")),
            }
        )
    return rows


def _combined_weight_rows(
    *,
    actual: dict[str, float],
    risk_approved: dict[str, float],
    final: dict[str, float],
    legacy_final: dict[str, float],
    llm_weights: dict[str, float],
    pc_shadow: dict[str, float],
) -> list[dict[str, Any]]:
    rows = []
    tickers = sorted(
        (
            set(actual)
            | set(risk_approved)
            | set(final)
            | set(legacy_final)
            | set(llm_weights)
            | set(pc_shadow)
        )
        - {"CASH"}
    )
    for ticker in tickers:
        rows.append(
            {
                "ticker": ticker,
                "actual_holdings": _round(actual.get(ticker, 0.0)),
                "risk_approved_target": _round(risk_approved.get(ticker, 0.0)),
                "envelope_final_target": _round(final.get(ticker, 0.0)),
                "legacy_dict_final_target": _round(legacy_final.get(ticker, 0.0)),
                "advisory_llm_weight": _round(llm_weights.get(ticker, 0.0)),
                "pc_shadow_reference_weight": _round(pc_shadow.get(ticker, 0.0)),
                "final_vs_actual": _round(final.get(ticker, 0.0) - actual.get(ticker, 0.0)),
                "risk_reduction": bool(final.get(ticker, 0.0) < actual.get(ticker, 0.0) - 1e-9),
            }
        )
    rows.sort(key=lambda row: (-abs(float(row.get("final_vs_actual") or 0.0)), str(row.get("ticker") or "")))
    return rows


def _snapshot_for_stage(snapshots: list[dict[str, Any]], stage: str) -> dict[str, Any]:
    for row in reversed(snapshots):
        if str(row.get("stage") or "") == stage:
            return row
    return {}


def _clean_weights(raw: Any) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(raw, dict):
        return out
    for key, value in raw.items():
        ticker = _ticker(key)
        if not ticker:
            continue
        parsed = _safe_float(value)
        if parsed is None:
            continue
        out[ticker] = max(parsed, 0.0)
    return out


def _top_weights(weights: dict[str, float], limit: int = 8) -> str:
    items = [
        (ticker, weight)
        for ticker, weight in weights.items()
        if ticker != "CASH" and float(weight or 0.0) > 0
    ]
    items.sort(key=lambda item: (-float(item[1] or 0.0), item[0]))
    return ", ".join(f"{ticker} {weight:.1%}" for ticker, weight in items[:limit])


def _non_cash_count(weights: dict[str, float]) -> int:
    return sum(1 for ticker, value in weights.items() if ticker != "CASH" and float(value or 0.0) > 0)


def _direction(after: float, reference: float, tolerance: float = 1e-9) -> str:
    if after > reference + tolerance:
        return "increase"
    if after < reference - tolerance:
        return "reduce"
    return "neutral"


def _stage_index(stage: str) -> int:
    try:
        return STAGE_ORDER.index(stage)
    except ValueError:
        return 999


def _unique(values: Any) -> list[Any]:
    out = []
    for value in values:
        if value in (None, ""):
            continue
        if value not in out:
            out.append(value)
    return out


def _ticker(value: Any) -> str:
    return str(value or "").upper().strip()


def _safe_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _to_float(value: Any) -> float:
    return float(_safe_float(value) or 0.0)


def _round(value: Any) -> float:
    return round(float(_safe_float(value) or 0.0), 6)
