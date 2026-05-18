"""Pure helpers for storing compact decision ledger summaries in memory."""
from __future__ import annotations

from typing import Any


def compact_decision_ledger_for_memory(
    ledger: dict[str, Any],
    *,
    max_rows: int = 10,
) -> dict[str, Any]:
    """Return a compact diagnostic-only ledger summary for MemoryDaily."""
    if not isinstance(ledger, dict) or not ledger:
        return {"available": False, "reason": "missing_decision_ledger"}

    tickers = ledger.get("tickers") or {}
    rows: list[dict[str, Any]] = []
    final_action_counts: dict[str, int] = {}
    proposed_action_counts: dict[str, int] = {}
    blocked_count = 0
    changed_count = 0
    for ticker, raw in tickers.items():
        if not isinstance(raw, dict):
            continue
        proposed = str(raw.get("proposed_action") or "unknown")
        final = str(raw.get("final_action") or "unknown")
        proposed_action_counts[proposed] = proposed_action_counts.get(proposed, 0) + 1
        final_action_counts[final] = final_action_counts.get(final, 0) + 1
        if final in {"none", "unknown"} and proposed not in {"hold", "none", "unknown"}:
            blocked_count += 1
        lifecycle = raw.get("trade_lifecycle") or {}
        if lifecycle.get("changed_by"):
            changed_count += 1
        governance = (raw.get("evidence_used") or {}).get("position_governance") or {}
        explanation = raw.get("explanation") or {}
        rows.append({
            "ticker": raw.get("ticker") or ticker,
            "proposed_action": proposed,
            "final_action": final,
            "execution_status": raw.get("execution_status"),
            "risk_result": raw.get("risk_result"),
            "reason_codes": (raw.get("reason_codes") or [])[:5],
            "governance_decision": governance.get("decision"),
            "position_state": explanation.get("position_state"),
            "final_target": lifecycle.get("final_target"),
            "changed_by": lifecycle.get("changed_by") or [],
            "sort_score": _memory_ledger_sort_score(raw),
        })

    rows.sort(key=lambda item: (-int(item.get("sort_score") or 0), str(item.get("ticker") or "")))
    summary = ledger.get("portfolio_summary") or {}
    return {
        "available": True,
        "phase": ledger.get("phase"),
        "portfolio_summary": {
            "risk_approved": summary.get("risk_approved"),
            "execution_status": summary.get("execution_status"),
            "governance_available": summary.get("governance_available"),
            "ticker_count": summary.get("ticker_count"),
        },
        "counts": {
            "proposed_action_counts": proposed_action_counts,
            "final_action_counts": final_action_counts,
            "blocked_count": blocked_count,
            "changed_count": changed_count,
        },
        "top_decisions": rows[:max_rows],
        "warnings": (ledger.get("warnings") or [])[:5],
        "execution_impact": "none",
    }


def build_decision_ledger_review(
    compact_ledger: dict[str, Any],
    *,
    max_examples: int = 5,
) -> dict[str, Any]:
    """Build a diagnostic proposed-vs-final summary from compact ledger rows."""
    if not isinstance(compact_ledger, dict) or not compact_ledger.get("available"):
        return {
            "available": False,
            "reason": compact_ledger.get("reason") if isinstance(compact_ledger, dict) else "missing_decision_ledger",
            "execution_impact": "none",
        }

    rows = compact_ledger.get("top_decisions") or []
    counts = compact_ledger.get("counts") or {}
    proposed_counts = counts.get("proposed_action_counts") or {}
    final_counts = counts.get("final_action_counts") or {}
    blocked = [
        row for row in rows
        if row.get("final_action") in {"none", "unknown"}
        and row.get("proposed_action") not in {"hold", "none", "unknown"}
    ]
    changed = [row for row in rows if row.get("changed_by")]
    trims = [row for row in rows if row.get("final_action") == "trim"]
    adds = [row for row in rows if row.get("final_action") == "add"]

    lines: list[str] = []
    lines.append(
        "proposed="
        + _counts_text(proposed_counts)
        + "; final="
        + _counts_text(final_counts)
    )
    if blocked:
        lines.append(
            "blocked="
            + ", ".join(
                f"{row.get('ticker')}:{row.get('proposed_action')}->{row.get('final_action')}"
                for row in blocked[:max_examples]
            )
        )
    if changed:
        lines.append(
            "changed_by="
            + ", ".join(
                f"{row.get('ticker')}:{','.join(str(x) for x in (row.get('changed_by') or [])[:2])}"
                for row in changed[:max_examples]
            )
        )

    return {
        "available": True,
        "diagnostic_only": True,
        "execution_impact": "none",
        "summary": " | ".join(lines),
        "blocked_count": int((counts.get("blocked_count") or len(blocked)) or 0),
        "changed_count": int((counts.get("changed_count") or len(changed)) or 0),
        "final_trim_count": len(trims),
        "final_add_count": len(adds),
        "examples": {
            "blocked": _review_examples(blocked, max_examples),
            "changed": _review_examples(changed, max_examples),
            "trims": _review_examples(trims, max_examples),
            "adds": _review_examples(adds, max_examples),
        },
        "warnings": (compact_ledger.get("warnings") or [])[:5],
    }


def _memory_ledger_sort_score(row: dict[str, Any]) -> int:
    proposed = str(row.get("proposed_action") or "")
    final = str(row.get("final_action") or "")
    reasons = {str(item) for item in row.get("reason_codes") or []}
    governance = (row.get("evidence_used") or {}).get("position_governance") or {}
    explanation = row.get("explanation") or {}
    score = 0
    if final in {"none", "unknown"} and proposed not in {"hold", "none", "unknown", ""}:
        score += 80
    if "hard_risk" in reasons or explanation.get("position_state") == "hard_risk":
        score += 70
    if final == "trim" or governance.get("decision") in {"trim", "trim_review"}:
        score += 50
    if governance.get("decision") == "hold_review":
        score += 35
    if final == "add":
        score += 25
    if reasons:
        score += min(20, len(reasons) * 4)
    return score


def _counts_text(counts: dict[str, Any]) -> str:
    if not counts:
        return "none"
    parts = [
        f"{key}={value}"
        for key, value in sorted(counts.items())
        if value
    ]
    return ",".join(parts) if parts else "none"


def _review_examples(rows: list[dict[str, Any]], max_examples: int) -> list[dict[str, Any]]:
    return [
        {
            "ticker": row.get("ticker"),
            "proposed_action": row.get("proposed_action"),
            "final_action": row.get("final_action"),
            "reason_codes": row.get("reason_codes") or [],
            "changed_by": row.get("changed_by") or [],
        }
        for row in rows[:max_examples]
    ]
