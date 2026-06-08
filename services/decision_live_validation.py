"""Live validation checks for decision-information utilization.

This module is read-only. It validates whether a completed pipeline run exposed
the evidence/governance/ledger fields that operators need to trust Telegram
output. It does not influence trading decisions.
"""
from __future__ import annotations

from typing import Any


async def build_latest_decision_live_validation() -> dict[str, Any]:
    """Read the latest analysis step logs and validate live decision visibility."""
    from sqlalchemy import select

    from db.models import AgentStepLog
    from db.session import AsyncSessionLocal
    from services.agent_analysis_queries import load_latest_trade_decision_analysis

    async with AsyncSessionLocal() as db:
        analysis = await load_latest_trade_decision_analysis(db)
        if not analysis:
            return {
                "overall": "fail",
                "checks": [_row("latest_analysis", "fail", "No agent analysis rows found")],
                "summary": {"pass": 0, "warn": 0, "fail": 1, "skipped": 0},
            }
        rows = (
            await db.execute(
                select(AgentStepLog)
                .where(AgentStepLog.analysis_id == analysis.id)
                .order_by(AgentStepLog.created_at.asc())
            )
        ).scalars().all()
    result = validate_decision_live_artifacts(stage_outputs=stage_outputs_from_step_logs(rows))
    result["analysis_id"] = getattr(analysis, "id", None)
    return result


def stage_outputs_from_step_logs(rows: list[Any]) -> dict[str, dict[str, Any]]:
    """Normalize AgentStepLog-like rows into {stage: output_data}."""
    out: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        stage = str(getattr(row, "stage", "") or "")
        if not stage:
            continue
        output = getattr(row, "output_data", None)
        out[stage] = output if isinstance(output, dict) else {}
    return out


def validate_decision_live_artifacts(
    *,
    stage_outputs: dict[str, dict[str, Any]] | None = None,
    communicator_text: str | None = None,
) -> dict[str, Any]:
    stages = stage_outputs or {}
    text = communicator_text
    if text is None:
        text = str((stages.get("8_communicator") or {}).get("text") or "")

    risk = stages.get("6_risk_mgr") or {}
    governance = (
        stages.get("6ba_position_governance")
        or risk.get("position_governance")
        or {}
    )
    ledger = stages.get("6d_decision_ledger") or risk.get("decision_ledger") or {}
    shaper = stages.get("5d_proposal_shaper") or _extract_proposal_shaping(stages)
    evidence = stages.get("2d_evidence_scorecard") or {}

    checks = [
        _check_data_quality_detail(text, evidence),
        _check_proposal_shaping(text, shaper),
        _check_manual_trim_review(text, governance),
        _check_advisory_weak_positive(text, governance),
        _check_hard_risk_explanation(text, governance),
        _check_decision_ledger(text, ledger),
        _check_source_effects(text, ledger),
    ]
    return {
        "overall": _overall(checks),
        "checks": checks,
        "summary": {
            "pass": sum(1 for row in checks if row["status"] == "pass"),
            "warn": sum(1 for row in checks if row["status"] == "warn"),
            "fail": sum(1 for row in checks if row["status"] == "fail"),
            "skipped": sum(1 for row in checks if row["status"] == "skipped"),
        },
    }


def format_decision_live_validation_report(result: dict[str, Any]) -> str:
    title = {
        "pass": "Decision live validation: pass",
        "warn": "Decision live validation: warning",
        "fail": "Decision live validation: fail",
    }.get(str(result.get("overall") or "unknown"), "Decision live validation: unknown")
    lines = [title]
    for row in result.get("checks") or []:
        if row.get("status") == "skipped":
            continue
        lines.append(f"- {row.get('status')}: {row.get('name')} — {row.get('message')}")
    return "\n".join(lines[:10])


def _check_data_quality_detail(text: str, evidence: dict[str, Any]) -> dict[str, Any]:
    if "Data quality detail" in text:
        return _row("data_quality_detail", "pass", "Telegram shows source-specific data quality detail")
    strategies = (evidence.get("evidence_bundle") or {}).get("strategies") or evidence.get("strategies") or {}
    summary = strategies.get("evidence_summary") or {}
    execution_status = summary.get("execution_intel_status")
    legacy_live_fit = summary.get("live_fit")
    if (
        execution_status == "insufficient_data"
        or (execution_status is None and legacy_live_fit == "insufficient")
    ) and strategies.get("snapshot_count") is not None:
        return _row(
            "data_quality_detail",
            "fail",
            "QC execution intel is insufficient but Telegram did not show Data quality detail",
        )
    return _row("data_quality_detail", "warn", "Data quality detail not found in Telegram text")


def _check_proposal_shaping(text: str, shaper: dict[str, Any]) -> dict[str, Any]:
    applied = bool(shaper.get("applied") or shaper.get("clip_log"))
    if not applied:
        return _row("proposal_shaping", "skipped", "Proposal shaper did not clip this run")
    if "Proposal shaping" in text:
        return _row("proposal_shaping", "pass", "Telegram shows proposal shaping")
    return _row("proposal_shaping", "fail", "Proposal shaping clipped but Telegram did not show it")


def _check_manual_trim_review(text: str, governance: dict[str, Any]) -> dict[str, Any]:
    hints = _manual_hints(governance)
    if not hints:
        return _row("manual_trim_review", "skipped", "No manual trim hints this run")
    if "manual trim review" in text:
        return _row("manual_trim_review", "pass", "Telegram shows manual trim review")
    return _row("manual_trim_review", "fail", "Manual trim hints exist but Telegram did not show them")


def _check_advisory_weak_positive(text: str, governance: dict[str, Any]) -> dict[str, Any]:
    hints = _manual_hints(governance)
    requires_label = any(
        "advisory_basket_loss_review" in (hint.get("reason_codes") or [])
        for hint in hints
    )
    if not requires_label:
        return _row("advisory_weak_positive", "skipped", "No advisory basket-loss hint this run")
    if "advisory=weak-positive" in text:
        return _row("advisory_weak_positive", "pass", "Telegram labels advisory support as weak-positive")
    return _row("advisory_weak_positive", "fail", "Advisory basket-loss hint lacks weak-positive label")


def _check_hard_risk_explanation(text: str, governance: dict[str, Any]) -> dict[str, Any]:
    explanations = ((governance.get("portfolio_summary") or {}).get("position_explanations") or [])
    hard_rows = [
        row for row in explanations
        if _is_hard_risk_explanation(row)
    ]
    if not hard_rows:
        return _row("hard_risk_explanation", "skipped", "No hard-risk explanation this run")
    if "no deterministic rule requires reduction" in text:
        return _row("hard_risk_explanation", "fail", "Hard-risk Telegram text contains stale safe-hold wording")
    if "hard-risk" in text or "hard_risk" in text or "hard risk" in text:
        return _row("hard_risk_explanation", "pass", "Telegram shows hard-risk review wording")
    return _row("hard_risk_explanation", "warn", "Hard-risk explanations exist but compact Telegram did not show them")


def _check_decision_ledger(text: str, ledger: dict[str, Any]) -> dict[str, Any]:
    rows = _ledger_rows(ledger)
    if not rows:
        return _row("decision_ledger", "fail", "Decision ledger is missing")
    if "Decision ledger" not in text:
        return _row("decision_ledger", "fail", "Decision ledger exists but Telegram did not show it")
    changed = any(
        str(row.get("proposed_action") or "") != str(row.get("final_action") or "")
        for row in rows
    )
    if changed and "->" not in text:
        return _row("decision_ledger", "fail", "Ledger needs proposed->final distinction but Telegram lacks arrow")
    return _row("decision_ledger", "pass", "Telegram shows proposed/final decision ledger")


def _check_source_effects(text: str, ledger: dict[str, Any]) -> dict[str, Any]:
    rows = _ledger_rows(ledger)
    if not any(row.get("source_effects") for row in rows):
        return _row("source_effects", "skipped", "Ledger source effects are empty")
    if "sources=" in text:
        return _row("source_effects", "pass", "Telegram shows compact source effects")
    return _row("source_effects", "warn", "Ledger has source effects but Telegram did not show compact sources")


def _extract_proposal_shaping(stages: dict[str, dict[str, Any]]) -> dict[str, Any]:
    synth = stages.get("5_synthesizer") or {}
    return synth.get("proposal_shaping") or {}


def _manual_hints(governance: dict[str, Any]) -> list[dict[str, Any]]:
    return (
        governance.get("manual_action_hints")
        or ((governance.get("portfolio_summary") or {}).get("manual_action_hints"))
        or []
    )


def _is_hard_risk_explanation(row: dict[str, Any]) -> bool:
    facts = row.get("explanation_facts") or {}
    if facts.get("severity") == "hard_risk":
        return True
    return "hard_risk" in str(row.get("position_state") or "")


def _ledger_rows(ledger: dict[str, Any]) -> list[dict[str, Any]]:
    tickers = ledger.get("tickers")
    if isinstance(tickers, dict):
        return [row for row in tickers.values() if isinstance(row, dict)]
    rows = ledger.get("top_decisions")
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _overall(checks: list[dict[str, Any]]) -> str:
    if any(row["status"] == "fail" for row in checks):
        return "fail"
    if any(row["status"] == "warn" for row in checks):
        return "warn"
    return "pass"


def _row(name: str, status: str, message: str) -> dict[str, str]:
    return {"name": name, "status": status, "message": message}
