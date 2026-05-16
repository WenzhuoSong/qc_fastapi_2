"""
Diagnostic feedback for LLM position advisory proposals.

This module does not make execution decisions. It only summarizes validator
outcomes now and provides a pure scoring helper for later forward-return
backfills.
"""
from __future__ import annotations

from typing import Any


def build_advisory_quality_diagnostics(
    advisory_overrides: list[dict[str, Any]] | None,
    *,
    historical_records: list[dict[str, Any]] | None = None,
    min_samples: int = 5,
) -> dict[str, Any]:
    rows = [row for row in (advisory_overrides or []) if isinstance(row, dict)]
    accepted = [row for row in rows if _result_bucket(row) == "accepted"]
    rejected = [row for row in rows if _result_bucket(row) == "rejected"]
    converted = [row for row in rows if _result_bucket(row) == "converted"]
    noop = [row for row in rows if _result_bucket(row) == "noop"]

    historical = _historical_feedback(historical_records or [], min_samples=min_samples)
    return {
        "diagnostic_only": True,
        "current_run": {
            "total": len(rows),
            "accepted": len(accepted),
            "rejected": len(rejected),
            "converted": len(converted),
            "noop": len(noop),
            "by_action": _count_by(rows, "llm_advisory"),
            "by_result": _count_buckets(rows),
            "accepted_tickers": [str(row.get("ticker") or "") for row in accepted if row.get("ticker")],
            "rejected_tickers": [str(row.get("ticker") or "") for row in rejected if row.get("ticker")],
        },
        "historical_feedback": historical,
        "execution_impact": "none",
    }


def score_advisory_outcomes(
    advisory_overrides: list[dict[str, Any]] | None,
    *,
    forward_returns_by_ticker: dict[str, float],
    benchmark_return: float = 0.0,
) -> list[dict[str, Any]]:
    """
    Score accepted advisory proposals after forward returns are known.

    add is good when ticker return beats benchmark; trim/exit is good when it
    underperforms benchmark. hold/review actions are neutral diagnostics.
    """
    scored: list[dict[str, Any]] = []
    for row in advisory_overrides or []:
        if not isinstance(row, dict) or _result_bucket(row) != "accepted":
            continue
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker or ticker not in forward_returns_by_ticker:
            continue
        action = str(row.get("llm_advisory") or "").lower()
        forward_return = float(forward_returns_by_ticker[ticker])
        excess = forward_return - float(benchmark_return or 0.0)
        if action == "add":
            score = 1.0 if excess > 0 else 0.0
        elif action in {"trim", "trim_review", "exit"}:
            score = 1.0 if excess < 0 else 0.0
        else:
            score = 0.5
        scored.append({
            "ticker": ticker,
            "llm_advisory": action,
            "validator_result": row.get("validator_result"),
            "forward_return": round(forward_return, 6),
            "benchmark_return": round(float(benchmark_return or 0.0), 6),
            "excess_return": round(excess, 6),
            "outcome_score": score,
        })
    return scored


def build_advisory_outcome_backfill(
    decision: dict[str, Any],
    *,
    forward_returns_by_ticker: dict[str, float],
    benchmark_return: float = 0.0,
) -> dict[str, Any]:
    overrides = decision.get("position_advisory_overrides") or []
    scored = score_advisory_outcomes(
        overrides,
        forward_returns_by_ticker=forward_returns_by_ticker,
        benchmark_return=float(benchmark_return or 0.0),
    )
    diagnostics = build_advisory_quality_diagnostics(
        overrides,
        historical_records=scored,
    )
    return {
        "position_advisory_outcomes": scored,
        "position_advisory_quality": diagnostics,
        "position_advisory_benchmark_return": float(benchmark_return or 0.0),
    }


def _historical_feedback(records: list[dict[str, Any]], *, min_samples: int) -> dict[str, Any]:
    scores = [
        float(row.get("outcome_score"))
        for row in records
        if isinstance(row, dict) and isinstance(row.get("outcome_score"), (int, float))
    ]
    sample_size = len(scores)
    if sample_size < min_samples:
        return {
            "sample_size": sample_size,
            "verdict": "insufficient",
            "avg_outcome_score": None,
            "note": f"need {min_samples} realized advisory outcomes before feedback is reliable",
        }
    avg = sum(scores) / sample_size
    if avg >= 0.60:
        verdict = "positive"
    elif avg <= 0.40:
        verdict = "negative"
    else:
        verdict = "neutral"
    return {
        "sample_size": sample_size,
        "verdict": verdict,
        "avg_outcome_score": round(avg, 4),
        "note": "diagnostic only; does not change execution permissions",
    }


def _result_bucket(row: dict[str, Any]) -> str:
    result = str(row.get("validator_result") or "").lower()
    if result == "accepted_noop":
        return "noop"
    if result.startswith("accepted"):
        return "accepted"
    if result.startswith("rejected"):
        return "rejected"
    if result.startswith("converted"):
        return "converted"
    return "other"


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _count_buckets(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        bucket = _result_bucket(row)
        counts[bucket] = counts.get(bucket, 0) + 1
    return counts
