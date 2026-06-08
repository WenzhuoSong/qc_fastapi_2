"""Decision degradation markers for learning-safe downstream review."""

from __future__ import annotations

from typing import Any


SCHEMA_VERSION = "decision_degradation_v1"


def build_decision_degradation_report(
    *,
    pipeline_context: dict[str, Any] | None = None,
    brief: dict[str, Any] | None = None,
    base_weights: dict[str, Any] | None = None,
    news_evidence: dict[str, Any] | None = None,
    research_report: dict[str, Any] | None = None,
    bull_output: dict[str, Any] | None = None,
    bear_output: dict[str, Any] | None = None,
    rebuttal_vs_bear: dict[str, Any] | None = None,
    rebuttal_vs_bull: dict[str, Any] | None = None,
    synthesizer_out: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return explicit degradation/fallback metadata for one decision sample.

    The goal is not to block execution here. It is to prevent silent fallback
    samples from being mixed with fully observed samples during weekend review
    or future model validation.
    """
    context = pipeline_context or {}
    brief = brief or {}
    research_report = research_report or {}
    synthesizer_out = synthesizer_out or {}
    bull_output = bull_output or {}
    bear_output = bear_output or {}
    rebuttal_vs_bear = rebuttal_vs_bear or {}
    rebuttal_vs_bull = rebuttal_vs_bull or {}

    degraded_modes: list[str] = []
    fallback_paths: list[str] = []
    missing_inputs: list[str] = []

    news_degraded = _news_degraded(context)
    if news_degraded:
        degraded_modes.append("news_stale_degraded_mode")
        fallback_paths.append("news_degraded_reduce_only")

    if bool(research_report.get("used_degraded_fallback")):
        degraded_modes.append("researcher_degraded_fallback")
        fallback_paths.append("researcher_degraded_fallback")
    if bool(synthesizer_out.get("used_degraded_fallback")):
        degraded_modes.append("synthesizer_degraded_fallback")
        fallback_paths.append("synthesizer_degraded_fallback")

    for stage_name, stage_payload in (
        ("bull_researcher", bull_output),
        ("bear_researcher", bear_output),
        ("bull_cross_exam", rebuttal_vs_bear),
        ("bear_cross_exam", rebuttal_vs_bull),
    ):
        if bool(stage_payload.get("failed")):
            degraded_modes.append(f"{stage_name}_failed")
            fallback_paths.append(f"{stage_name}_failed")

    if not base_weights:
        missing_inputs.append("base_weights_missing")
    if not _has_current_weights(brief):
        missing_inputs.append("current_weights_missing")
    if not _has_news_evidence(news_evidence):
        missing_inputs.append("news_evidence_missing")
    if not _has_research_ticker_signals(research_report):
        missing_inputs.append("researcher_ticker_signals_missing")

    degraded_modes = sorted(set(degraded_modes))
    fallback_paths = sorted(set(fallback_paths))
    missing_inputs = sorted(set(missing_inputs))
    return {
        "schema_version": SCHEMA_VERSION,
        "is_degraded": bool(degraded_modes or fallback_paths or missing_inputs),
        "degraded_modes": degraded_modes,
        "fallback_paths": fallback_paths,
        "missing_inputs": missing_inputs,
        "stage_status": {
            "news": {
                "degraded": news_degraded,
                "reason": _news_degraded_reason(context),
            },
            "researcher": {
                "degraded": bool(research_report.get("used_degraded_fallback")),
                "reason": research_report.get("fallback_reason") or research_report.get("error"),
            },
            "bull_researcher": {
                "failed": bool(bull_output.get("failed")),
                "reason": bull_output.get("error") or bull_output.get("reason"),
            },
            "bear_researcher": {
                "failed": bool(bear_output.get("failed")),
                "reason": bear_output.get("error") or bear_output.get("reason"),
            },
            "bull_cross_exam": {
                "failed": bool(rebuttal_vs_bear.get("failed")),
                "reason": rebuttal_vs_bear.get("error") or rebuttal_vs_bear.get("reason"),
            },
            "bear_cross_exam": {
                "failed": bool(rebuttal_vs_bull.get("failed")),
                "reason": rebuttal_vs_bull.get("error") or rebuttal_vs_bull.get("reason"),
            },
            "synthesizer": {
                "degraded": bool(synthesizer_out.get("used_degraded_fallback")),
                "reason": synthesizer_out.get("fallback_reason") or synthesizer_out.get("error"),
            },
        },
        "evaluation_guidance": "stratify_metrics_by_degraded_mode",
        "execution_authority": "none",
        "target_weight_mutation": "none",
    }


def _news_degraded(context: dict[str, Any]) -> bool:
    payload = context.get("news_degraded_mode")
    return isinstance(payload, dict) and bool(payload.get("enabled"))


def _news_degraded_reason(context: dict[str, Any]) -> str | None:
    payload = context.get("news_degraded_mode")
    if not isinstance(payload, dict):
        return None
    reason = payload.get("reason") or payload.get("status") or payload.get("mode")
    return str(reason) if reason else None


def _has_current_weights(brief: dict[str, Any]) -> bool:
    weights = brief.get("current_weights")
    return isinstance(weights, dict) and any(_float_or_zero(v) != 0.0 for v in weights.values())


def _has_news_evidence(news_evidence: dict[str, Any] | None) -> bool:
    if not isinstance(news_evidence, dict) or not news_evidence:
        return False
    if news_evidence.get("relevant_news") or news_evidence.get("items"):
        return True
    summaries = news_evidence.get("summaries")
    if isinstance(summaries, dict) and summaries:
        return True
    return bool(news_evidence.get("status") in {"ok", "fresh", "degraded"})


def _has_research_ticker_signals(research_report: dict[str, Any]) -> bool:
    for key in ("ticker_signals", "ticker_signals_dict", "signals"):
        payload = research_report.get(key)
        if isinstance(payload, dict) and payload:
            return True
        if isinstance(payload, list) and payload:
            return True
    return False


def _float_or_zero(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
