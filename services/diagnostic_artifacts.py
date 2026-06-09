"""Versioned recommendation-loop diagnostic artifacts.

These artifacts are JSON-first, append-only observations. They are not
execution inputs and must not carry execution authority.
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


ExecutionAuthority = Literal["none"]


class DiagnosticArtifact(BaseModel):
    """Base model for immutable diagnostic observations."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str
    artifact_type: str
    artifact_id: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    source_stage: str
    execution_authority: ExecutionAuthority = "none"
    analysis_id: int

    @model_validator(mode="after")
    def _assign_artifact_id(self) -> "DiagnosticArtifact":
        if not self.artifact_id:
            payload = {
                "schema_version": self.schema_version,
                "artifact_type": self.artifact_type,
                "analysis_id": self.analysis_id,
                "created_at": self.created_at.isoformat(),
                "source_stage": self.source_stage,
            }
            digest = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()[:24]
            self.artifact_id = f"{self.schema_version}:{self.analysis_id}:{digest}"
        return self


class MarketRiskAssessment(DiagnosticArtifact):
    schema_version: Literal["market_risk_assessment_v1"] = "market_risk_assessment_v1"
    artifact_type: Literal["market_risk_assessment"] = "market_risk_assessment"
    source_stage: str = "researcher"
    market_regime: str = "unknown"
    regime_confidence: str | float | None = None
    primary_risks: list[str] = Field(default_factory=list)
    risk_direction: str = "unknown"
    conflicts: list[str] = Field(default_factory=list)
    operator_summary: str = ""


class DecisionFeatureSnapshot(DiagnosticArtifact):
    schema_version: Literal["decision_feature_snapshot_v1"] = "decision_feature_snapshot_v1"
    artifact_type: Literal["decision_feature_snapshot"] = "decision_feature_snapshot"
    source_stage: str = "decision_features"
    as_of_time: datetime
    price_source: Literal["qc_snapshot", "yfinance", "mixed", "unknown"] = "unknown"
    feature_authority: Literal["qc_live", "yfinance", "mixed", "unknown"] = "unknown"
    feature_values: dict[str, dict[str, Any]] = Field(default_factory=dict)
    raw_source_refs: list[str] = Field(default_factory=list)
    training_authority: Literal["eligible", "feature_scope_limited"] = "eligible"
    scope_limit_reasons: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _mark_mixed_authority_limited(self) -> "DecisionFeatureSnapshot":
        reasons = list(self.scope_limit_reasons or [])
        if self.feature_authority == "mixed" and "mixed_feature_authority" not in reasons:
            reasons.append("mixed_feature_authority")
        if self.price_source == "mixed" and "mixed_price_source" not in reasons:
            reasons.append("mixed_price_source")
        if reasons:
            self.training_authority = "feature_scope_limited"
            self.scope_limit_reasons = reasons
        return self


class CandidateEvent(DiagnosticArtifact):
    schema_version: Literal["candidate_event_v1"] = "candidate_event_v1"
    artifact_type: Literal["candidate_event"] = "candidate_event"
    source_stage: str = "candidate_generation"
    feature_snapshot_id: str
    strategy_id: str | None = None
    ticker: str
    action: str | None = None
    candidate_weight: float | None = None
    score: float | None = None
    reasons: list[str] = Field(default_factory=list)
    decision_ledger_ref: str | None = None


class RankingEvent(DiagnosticArtifact):
    schema_version: Literal["ranking_event_v1"] = "ranking_event_v1"
    artifact_type: Literal["ranking_event"] = "ranking_event"
    source_stage: str = "ranking"
    feature_snapshot_id: str
    ranker_id: str = "target_weight_ranking_v1"
    ranked_tickers: list[dict[str, Any]] = Field(default_factory=list)
    top_tickers: list[str] = Field(default_factory=list)


class PortfolioMixEvent(DiagnosticArtifact):
    schema_version: Literal["portfolio_mix_event_v1"] = "portfolio_mix_event_v1"
    artifact_type: Literal["portfolio_mix_event"] = "portfolio_mix_event"
    source_stage: str = "portfolio_mix"
    feature_snapshot_id: str
    target_weights: dict[str, float] = Field(default_factory=dict)
    active_count: int = 0
    cash_weight: float = 0.0
    mix_policy: dict[str, Any] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)


class DebateImpact(DiagnosticArtifact):
    schema_version: Literal["debate_impact_v1"] = "debate_impact_v1"
    artifact_type: Literal["debate_impact"] = "debate_impact"
    source_stage: str = "bull_bear_debate"
    bull_stance: str | None = None
    bear_stance: str | None = None
    bull_confidence: str | float | None = None
    bear_confidence: str | float | None = None
    bull_failed: bool = False
    bear_failed: bool = False
    cross_exam_failed: bool = False
    disagreement_count: int = 0
    disagreement_tickers: list[str] = Field(default_factory=list)
    arbitration_count: int = 0
    arbitration_tickers: list[str] = Field(default_factory=list)
    disagreement_tickers_in_target_builder: list[str] = Field(default_factory=list)
    disagreement_tickers_changed_by_target_builder: list[str] = Field(default_factory=list)
    disagreement_tickers_in_final_target: list[str] = Field(default_factory=list)
    counterfactual_available: bool = False
    execution_delta_from_debate: float | None = None
    measurement_limitations: list[str] = Field(default_factory=list)
    token_usage: dict[str, Any] = Field(default_factory=dict)


class DecisionStyleEvent(DiagnosticArtifact):
    schema_version: Literal["decision_style_event_v1"] = "decision_style_event_v1"
    artifact_type: Literal["decision_style_event"] = "decision_style_event"
    source_stage: str = "decision_style"
    analysis_style: str = "unknown"
    trade_style: str = "unknown"
    dominant_style_constraint: str | None = None
    triggered_style_rules: list[str] = Field(default_factory=list)
    causal_sources: dict[str, Any] = Field(default_factory=dict)
    news_style_influence: dict[str, Any] = Field(default_factory=dict)
    style_limits: dict[str, Any] = Field(default_factory=dict)
    style_enforcement: dict[str, Any] = Field(default_factory=dict)
    blocked_new_positions: list[str] = Field(default_factory=list)
    target_weights_pre_style_clip: dict[str, float] = Field(default_factory=dict)
    target_weights_post_style_clip: dict[str, float] = Field(default_factory=dict)
    defensive_style: bool = False
    hard_new_position_block: bool = False
    measurement_limitations: list[str] = Field(default_factory=list)


class DecisionFunnelObservability(DiagnosticArtifact):
    schema_version: Literal["decision_funnel_observability_v1"] = "decision_funnel_observability_v1"
    artifact_type: Literal["decision_funnel_observability"] = "decision_funnel_observability"
    source_stage: str = "decision_funnel_observability"
    target_weight_mutation: Literal["none"] = "none"
    min_buy_intent_delta: float = 0.0025
    min_executable_weight_reference: float | None = None
    buy_intents: list[dict[str, Any]] = Field(default_factory=list)
    stateless_independent_verdicts: dict[str, dict[str, Any]] = Field(default_factory=dict)
    stateful_incremental_blockers: dict[str, dict[str, Any]] = Field(default_factory=dict)
    stateful_pass_through_base_by_layer: dict[str, int] = Field(default_factory=dict)
    first_blocker_distribution: dict[str, int] = Field(default_factory=dict)
    stateless_all_blocker_distribution: dict[str, int] = Field(default_factory=dict)
    stateful_incremental_blocker_distribution: dict[str, int] = Field(default_factory=dict)
    single_blocker_candidate_count: int = 0
    buy_delta_metrics: dict[str, Any] = Field(default_factory=dict)
    net_position_drift: dict[str, Any] = Field(default_factory=dict)
    cash_trajectory_point: dict[str, Any] = Field(default_factory=dict)
    llm_chain_decision_influence: dict[str, Any] = Field(default_factory=dict)
    decision_style: dict[str, Any] = Field(default_factory=dict)
    decision_degradation: dict[str, Any] = Field(default_factory=dict)
    measurement_limitations: list[str] = Field(default_factory=list)


Artifact = (
    MarketRiskAssessment
    | DecisionFeatureSnapshot
    | CandidateEvent
    | RankingEvent
    | PortfolioMixEvent
    | DebateImpact
    | DecisionStyleEvent
    | DecisionFunnelObservability
)


def serialize_artifact(artifact: Artifact | dict[str, Any]) -> dict[str, Any]:
    """Return a JSONB-safe artifact dict."""
    if isinstance(artifact, BaseModel):
        return artifact.model_dump(mode="json")
    if isinstance(artifact, dict):
        return json.loads(json.dumps(artifact, default=str))
    raise TypeError(f"unsupported diagnostic artifact type: {type(artifact)!r}")


def append_diagnostic_artifacts(
    payload: dict[str, Any] | None,
    artifacts: list[Artifact | dict[str, Any]],
    *,
    key: str = "diagnostic_artifacts",
) -> dict[str, Any]:
    """Append versioned artifacts without replacing previous observations."""
    out = dict(payload or {})
    existing = out.get(key)
    records = list(existing) if isinstance(existing, list) else []
    records.extend(serialize_artifact(item) for item in artifacts)
    out[key] = records
    out["diagnostic_artifact_count"] = len(records)
    return out


def build_pipeline_diagnostic_artifacts(
    *,
    analysis_id: int,
    as_of_time: datetime,
    pipeline_context: dict[str, Any] | None,
    brief: dict[str, Any] | None,
    market_scorecard: dict[str, Any] | None,
    synthesizer_out: dict[str, Any] | None,
    risk_out: dict[str, Any] | None,
    base_weights: dict[str, float] | None,
    bull_output: dict[str, Any] | None = None,
    bear_output: dict[str, Any] | None = None,
) -> list[Artifact]:
    """Build the PR6 diagnostic artifact set from one pipeline decision."""
    context = pipeline_context or {}
    brief = brief or {}
    risk_out = risk_out or {}
    feature_snapshot = build_decision_feature_snapshot(
        analysis_id=analysis_id,
        as_of_time=as_of_time,
        pipeline_context=context,
        brief=brief,
    )
    target_weights = _float_weights(risk_out.get("target_weights") or base_weights or {})
    ranked = _ranked_tickers(target_weights)
    artifacts: list[Artifact] = [
        build_market_risk_assessment(
            analysis_id=analysis_id,
            as_of_time=as_of_time,
            market_scorecard=market_scorecard or {},
            synthesizer_out=synthesizer_out or {},
            brief=brief,
        ),
        feature_snapshot,
        RankingEvent(
            analysis_id=analysis_id,
            created_at=as_of_time,
            feature_snapshot_id=str(feature_snapshot.artifact_id),
            ranked_tickers=ranked,
            top_tickers=[row["ticker"] for row in ranked[:10]],
        ),
        PortfolioMixEvent(
            analysis_id=analysis_id,
            created_at=as_of_time,
            feature_snapshot_id=str(feature_snapshot.artifact_id),
            target_weights=target_weights,
            active_count=sum(1 for t, w in target_weights.items() if t != "CASH" and w > 0),
            cash_weight=float(target_weights.get("CASH") or 0.0),
            mix_policy=(risk_out.get("active_basket_policy") or {}),
            diagnostics={
                "approved": bool(risk_out.get("approved")),
                "final_validation": risk_out.get("final_validation") or {},
            },
        ),
        build_debate_impact(
            analysis_id=analysis_id,
            as_of_time=as_of_time,
            bull_output=bull_output or {},
            bear_output=bear_output or {},
            synthesizer_out=synthesizer_out or {},
            risk_out=risk_out,
        ),
        build_decision_style_event(
            analysis_id=analysis_id,
            as_of_time=as_of_time,
            decision_style=context.get("decision_style") or brief.get("decision_style") or {},
            risk_out=risk_out,
        ),
        build_decision_funnel_observability(
            analysis_id=analysis_id,
            as_of_time=as_of_time,
            pipeline_context=context,
            brief=brief,
            market_scorecard=market_scorecard or {},
            risk_out=risk_out,
            base_weights=base_weights or {},
        ),
    ]
    artifacts.extend(
        CandidateEvent(
            analysis_id=analysis_id,
            created_at=as_of_time,
            feature_snapshot_id=str(feature_snapshot.artifact_id),
            strategy_id="target_builder_gated",
            ticker=row["ticker"],
            action="candidate_weight",
            candidate_weight=row["weight"],
            score=row["weight"],
            reasons=["top_target_weight"],
            decision_ledger_ref=f"decision_ledger.tickers.{row['ticker']}",
        )
        for row in ranked[:20]
        if row["ticker"] != "CASH"
    )
    return artifacts


def build_decision_funnel_observability(
    *,
    analysis_id: int,
    as_of_time: datetime,
    pipeline_context: dict[str, Any],
    brief: dict[str, Any],
    market_scorecard: dict[str, Any],
    risk_out: dict[str, Any],
    base_weights: dict[str, float],
    min_buy_intent_delta: float = 0.0025,
) -> DecisionFunnelObservability:
    """Record buy-intent funnel observability without changing any target."""
    current_weights = _current_weights_from_brief(brief)
    target_weights = _float_weights(risk_out.get("target_weights") or current_weights)
    base = _float_weights(base_weights or {})
    min_exec_ref = _min_executable_weight_reference(pipeline_context, risk_out)
    buy_intents = _build_buy_intents(
        base_weights=base,
        current_weights=current_weights,
        target_weights=target_weights,
        min_buy_intent_delta=min_buy_intent_delta,
    )
    intent_tickers = [row["ticker"] for row in buy_intents]
    stateless = _decision_funnel_stateless_verdicts(
        tickers=intent_tickers,
        market_scorecard=market_scorecard or {},
        decision_style=(pipeline_context.get("decision_style") or brief.get("decision_style") or {}),
        risk_out=risk_out or {},
    )
    stateful = _decision_funnel_stateful_blockers(
        tickers=intent_tickers,
        stateless_verdicts=stateless,
        target_weights=target_weights,
        current_weights=current_weights,
        risk_out=risk_out or {},
        min_buy_intent_delta=min_buy_intent_delta,
    )
    first_blocker = _first_blocker_distribution(intent_tickers, stateless, stateful)
    stateless_all = _layer_blocker_distribution(stateless)
    stateful_all = _layer_blocker_distribution(stateful)
    single_blockers = _single_blocker_count(intent_tickers, stateless, stateful)
    buy_delta_metrics = _buy_delta_metrics(buy_intents)
    drift = _net_position_drift(current_weights=current_weights, target_weights=target_weights)
    style = pipeline_context.get("decision_style") if isinstance(pipeline_context.get("decision_style"), dict) else {}
    degradation = risk_out.get("decision_degradation") if isinstance(risk_out.get("decision_degradation"), dict) else {}
    return DecisionFunnelObservability(
        analysis_id=analysis_id,
        created_at=as_of_time,
        min_buy_intent_delta=float(min_buy_intent_delta),
        min_executable_weight_reference=min_exec_ref,
        buy_intents=buy_intents,
        stateless_independent_verdicts=stateless,
        stateful_incremental_blockers=stateful,
        stateful_pass_through_base_by_layer={
            layer: int((payload or {}).get("pass_through_base_count") or 0)
            for layer, payload in stateful.items()
        },
        first_blocker_distribution=first_blocker,
        stateless_all_blocker_distribution=stateless_all,
        stateful_incremental_blocker_distribution=stateful_all,
        single_blocker_candidate_count=single_blockers,
        buy_delta_metrics=buy_delta_metrics,
        net_position_drift=drift,
        cash_trajectory_point={
            "as_of_time": as_of_time.isoformat(),
            "current_cash_weight": round(float(current_weights.get("CASH", 0.0) or 0.0), 6),
            "target_cash_weight": round(float(target_weights.get("CASH", 0.0) or 0.0), 6),
            "cash_delta": round(
                float(target_weights.get("CASH", 0.0) or 0.0)
                - float(current_weights.get("CASH", 0.0) or 0.0),
                6,
            ),
        },
        llm_chain_decision_influence={
            "status": "shadow_unavailable",
            "reason": "no_verified_no_llm_dry_run_shadow_target",
            "requires_dry_run_no_side_effects": True,
            "no_db_write": True,
            "no_lifecycle_event": True,
            "no_execution_log": True,
            "no_qc_command": True,
            "measurement_rule": (
                "do_not_infer_llm_influence_by_subtracting_fields; compare full targets "
                "from an explicit no-LLM dry-run shadow only"
            ),
        },
        decision_style={
            "analysis_style": style.get("analysis_style") or "unknown",
            "trade_style": style.get("trade_style") or "unknown",
            "style_limits": style.get("style_limits") if isinstance(style.get("style_limits"), dict) else {},
        },
        decision_degradation={
            "is_degraded": bool(degradation.get("is_degraded")),
            "modes": [str(item) for item in (degradation.get("modes") or [])],
            "fallback_paths": [str(item) for item in (degradation.get("fallback_paths") or [])],
            "missing_inputs": [str(item) for item in (degradation.get("missing_inputs") or [])],
        },
        measurement_limitations=[
            "buy_intent_delta_is_lower_than_execution_floor_to_observe_floor_suppression",
            "stateless_layer_rates_use_all_buy_intents_as_denominator",
            "stateful_layer_rates_use_only_layer_pass_through_base_as_denominator",
            "llm_influence_rate_not_computed_without_verified_no_side_effect_dry_run_shadow",
        ],
    )


def build_decision_style_event(
    *,
    analysis_id: int,
    as_of_time: datetime,
    decision_style: dict[str, Any],
    risk_out: dict[str, Any],
) -> DecisionStyleEvent:
    """Record style causality and clipping as diagnostics, never execution authority."""
    style = decision_style or {}
    enforcement = risk_out.get("style_enforcement") if isinstance(risk_out.get("style_enforcement"), dict) else {}
    pre = _float_weights(enforcement.get("target_weights_pre_style_clip") or {})
    post = _float_weights(enforcement.get("target_weights_post_style_clip") or {})
    violations = [str(item) for item in (enforcement.get("violations") or enforcement.get("clip_log") or [])]
    blocked = _blocked_new_positions_from_style_violations(violations)
    limits = style.get("style_limits") if isinstance(style.get("style_limits"), dict) else {}
    analysis_style = str(style.get("analysis_style") or "unknown")
    trade_style = str(style.get("trade_style") or "unknown")
    hard_new_block = bool(limits.get("allow_new_positions") is False)
    return DecisionStyleEvent(
        analysis_id=analysis_id,
        created_at=as_of_time,
        analysis_style=analysis_style,
        trade_style=trade_style,
        dominant_style_constraint=style.get("dominant_style_constraint"),
        triggered_style_rules=[str(item) for item in (style.get("triggered_style_rules") or [])],
        causal_sources=style.get("causal_sources") if isinstance(style.get("causal_sources"), dict) else {},
        news_style_influence=(
            style.get("news_style_influence")
            if isinstance(style.get("news_style_influence"), dict)
            else {}
        ),
        style_limits=limits,
        style_enforcement={
            "violations": violations,
            "one_way_tightening_ok": enforcement.get("one_way_tightening_ok"),
            "post_clip_compliance": enforcement.get("post_clip_compliance") or {},
        },
        blocked_new_positions=blocked,
        target_weights_pre_style_clip=pre,
        target_weights_post_style_clip=post,
        defensive_style=(
            analysis_style == "macro_defensive"
            or trade_style in {"risk_reduce_fast", "cash_only"}
            or hard_new_block
        ),
        hard_new_position_block=hard_new_block,
        measurement_limitations=[
            "style_event_records_policy_and_clip_effects_not_causal_alpha_proof",
        ],
    )


def build_debate_impact(
    *,
    analysis_id: int,
    as_of_time: datetime,
    bull_output: dict[str, Any],
    bear_output: dict[str, Any],
    synthesizer_out: dict[str, Any],
    risk_out: dict[str, Any],
) -> DebateImpact:
    """Record debate observability without claiming causal counterfactual impact."""
    debate_summary = synthesizer_out.get("debate_summary") or {}
    disagreements = [
        row for row in (debate_summary.get("disagreement_map") or [])
        if isinstance(row, dict)
    ]
    disagreement_tickers = _unique_tickers(row.get("ticker") for row in disagreements)

    reasoning = synthesizer_out.get("reasoning_chain") or {}
    arbitration_rows = [
        row for row in (reasoning.get("step3_debate_arbitration") or [])
        if isinstance(row, dict)
    ]
    arbitration_tickers = _unique_tickers(row.get("ticker") for row in arbitration_rows)

    target_builder = risk_out.get("target_builder_input") or risk_out.get("target_builder_shadow") or {}
    tb_per_ticker = target_builder.get("per_ticker") or {}
    tb_tickers = {str(ticker).upper() for ticker in tb_per_ticker}
    tb_changed = {
        str(ticker).upper()
        for ticker, row in tb_per_ticker.items()
        if isinstance(row, dict) and row.get("changed_by")
    }
    final_tickers = {
        str(ticker).upper()
        for ticker, weight in (risk_out.get("target_weights") or {}).items()
        if str(ticker).upper() != "CASH" and _safe_float(weight) > 0.0
    }

    cross_exam_failed = bool(
        ((bull_output.get("rebuttal_vs_bear") or {}).get("failed"))
        or ((bear_output.get("rebuttal_vs_bull") or {}).get("failed"))
    )

    return DebateImpact(
        analysis_id=analysis_id,
        created_at=as_of_time,
        bull_stance=_first_present(bull_output, ("stance", "overall_stance")),
        bear_stance=_first_present(bear_output, ("stance", "overall_stance")),
        bull_confidence=_first_present(bull_output, ("confidence", "overall_confidence")),
        bear_confidence=_first_present(bear_output, ("confidence", "overall_confidence")),
        bull_failed=bool(bull_output.get("failed", False)),
        bear_failed=bool(bear_output.get("failed", False)),
        cross_exam_failed=cross_exam_failed,
        disagreement_count=len(disagreement_tickers),
        disagreement_tickers=disagreement_tickers,
        arbitration_count=len(arbitration_tickers),
        arbitration_tickers=arbitration_tickers,
        disagreement_tickers_in_target_builder=sorted(set(disagreement_tickers) & tb_tickers),
        disagreement_tickers_changed_by_target_builder=sorted(set(disagreement_tickers) & tb_changed),
        disagreement_tickers_in_final_target=sorted(set(disagreement_tickers) & final_tickers),
        counterfactual_available=False,
        execution_delta_from_debate=None,
        measurement_limitations=[
            "no_no_debate_counterfactual_shadow",
            "target_changes_are_overlap_metrics_not_causal_attribution",
        ],
        token_usage={
            "bull": (bull_output.get("_token_usage") or {}),
            "bear": (bear_output.get("_token_usage") or {}),
            "bull_cross_exam": ((bull_output.get("rebuttal_vs_bear") or {}).get("_token_usage") or {}),
            "bear_cross_exam": ((bear_output.get("rebuttal_vs_bull") or {}).get("_token_usage") or {}),
        },
    )


def _current_weights_from_brief(brief: dict[str, Any]) -> dict[str, float]:
    current = _float_weights(brief.get("current_weights") or {})
    if current:
        return current
    out: dict[str, float] = {}
    for row in brief.get("holdings") or []:
        if not isinstance(row, dict):
            continue
        ticker = str(row.get("ticker") or row.get("symbol") or "").upper().strip()
        if not ticker:
            continue
        raw = row.get("weight_current")
        if raw is None:
            raw = row.get("weight")
        value = _safe_float(raw)
        if value:
            out[ticker] = value
    return out


def _min_executable_weight_reference(
    pipeline_context: dict[str, Any],
    risk_out: dict[str, Any],
) -> float | None:
    candidates = [
        ((pipeline_context.get("risk_params") or {}).get("min_executable_weight_floor")),
        ((pipeline_context.get("risk_params") or {}).get("min_executable_weight")),
        ((risk_out.get("risk_config") or {}).get("min_executable_weight_floor")),
        ((risk_out.get("broker_order_filter") or {}).get("min_executable_weight_floor")),
    ]
    for raw in candidates:
        value = _safe_float(raw)
        if value > 0:
            return value
    return None


def _build_buy_intents(
    *,
    base_weights: dict[str, float],
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    min_buy_intent_delta: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    tickers = sorted((set(base_weights) | set(current_weights) | set(target_weights)) - {"CASH"})
    for ticker in tickers:
        base_w = float(base_weights.get(ticker, 0.0) or 0.0)
        current_w = float(current_weights.get(ticker, 0.0) or 0.0)
        final_w = float(target_weights.get(ticker, 0.0) or 0.0)
        desired_delta = base_w - current_w
        if desired_delta < float(min_buy_intent_delta):
            continue
        allowed_delta = max(final_w - current_w, 0.0)
        blocked_delta = max(desired_delta - allowed_delta, 0.0)
        out.append({
            "ticker": ticker,
            "base_weight": round(base_w, 6),
            "current_weight": round(current_w, 6),
            "final_target_weight": round(final_w, 6),
            "desired_buy_delta": round(desired_delta, 6),
            "allowed_buy_delta_after_gates": round(allowed_delta, 6),
            "blocked_buy_delta": round(blocked_delta, 6),
            "blocked": final_w <= current_w + 1e-9,
        })
    return out


def _decision_funnel_stateless_verdicts(
    *,
    tickers: list[str],
    market_scorecard: dict[str, Any],
    decision_style: dict[str, Any],
    risk_out: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    scorecard_permission = str(market_scorecard.get("investment_permission") or "").lower()
    scorecard_no_add = scorecard_permission in {
        "cash_only",
        "reduce_risk_only",
        "defensive_only",
        "hold_or_trim",
    } or bool(market_scorecard.get("require_human_confirmation"))
    style_limits = decision_style.get("style_limits") if isinstance(decision_style.get("style_limits"), dict) else {}
    style_no_add = style_limits.get("allow_new_positions") is False
    style_blocked = set()
    enforcement = risk_out.get("style_enforcement") if isinstance(risk_out.get("style_enforcement"), dict) else {}
    for item in enforcement.get("violations") or enforcement.get("clip_log") or []:
        text = str(item)
        if not text.startswith("style_new_position_blocked:"):
            continue
        ticker = text.split(":", 1)[1].split(" ", 1)[0].upper().strip()
        if ticker:
            style_blocked.add(ticker)
    broker_filter = risk_out.get("broker_order_filter") if isinstance(risk_out.get("broker_order_filter"), dict) else {}
    suppressed = {
        str(row.get("ticker") or row.get("symbol") or "").upper().strip()
        for row in (broker_filter.get("suppressed_orders") or broker_filter.get("suppressed_micro_orders") or [])
        if isinstance(row, dict)
    }
    return {
        "scorecard": {
            "layer_type": "stateless_independent",
            "denominator": len(tickers),
            "blocked_tickers": sorted(tickers if scorecard_no_add else []),
            "verdict_by_ticker": {
                ticker: {
                    "verdict": "blocked" if scorecard_no_add else "passed",
                    "reason": "scorecard_no_new_buys" if scorecard_no_add else None,
                }
                for ticker in tickers
            },
            "measurement_note": "independent policy verdict over all buy intents",
        },
        "decision_style": {
            "layer_type": "stateless_independent",
            "denominator": len(tickers),
            "blocked_tickers": sorted(
                ticker for ticker in tickers if style_no_add or ticker in style_blocked
            ),
            "verdict_by_ticker": {
                ticker: {
                    "verdict": "blocked" if (style_no_add or ticker in style_blocked) else "passed",
                    "reason": "style_new_position_blocked" if (style_no_add or ticker in style_blocked) else None,
                }
                for ticker in tickers
            },
            "measurement_note": "independent style verdict over all buy intents",
        },
        "broker_order_filter": {
            "layer_type": "stateless_independent",
            "denominator": len(tickers),
            "blocked_tickers": sorted(ticker for ticker in tickers if ticker in suppressed),
            "verdict_by_ticker": {
                ticker: {
                    "verdict": "blocked" if ticker in suppressed else "not_evaluated",
                    "reason": "micro_order_suppressed" if ticker in suppressed else "broker_filter_runs_after_execution_intent",
                }
                for ticker in tickers
            },
            "measurement_note": "order-efficiency evidence is included only when execution filter diagnostics exist",
        },
    }


def _decision_funnel_stateful_blockers(
    *,
    tickers: list[str],
    stateless_verdicts: dict[str, dict[str, Any]],
    target_weights: dict[str, float],
    current_weights: dict[str, float],
    risk_out: dict[str, Any],
    min_buy_intent_delta: float,
) -> dict[str, dict[str, Any]]:
    stateless_blocked = {
        ticker
        for layer in stateless_verdicts.values()
        for ticker in (layer.get("blocked_tickers") or [])
    }
    pass_through = [ticker for ticker in tickers if ticker not in stateless_blocked]
    governance_blocked = _blocked_tickers_from_actions(
        (risk_out.get("position_governance") or {}).get("blocked_actions") or []
    )
    final_validation = risk_out.get("final_validation") if isinstance(risk_out.get("final_validation"), dict) else {}
    final_validation_blocked_all = bool(final_validation.get("allowed") is False or final_validation.get("approved") is False)
    target_blocked = [
        ticker
        for ticker in pass_through
        if float(target_weights.get(ticker, 0.0) or 0.0)
        <= float(current_weights.get(ticker, 0.0) or 0.0) + min_buy_intent_delta / 10.0
    ]
    return {
        "position_governance": {
            "layer_type": "stateful_incremental",
            "pass_through_base_count": len(pass_through),
            "blocked_tickers": sorted(ticker for ticker in pass_through if ticker in governance_blocked),
            "measurement_note": "incremental blockers among candidates that reached position governance",
        },
        "risk_manager_final_target": {
            "layer_type": "stateful_incremental",
            "pass_through_base_count": len(pass_through),
            "blocked_tickers": sorted(target_blocked),
            "measurement_note": "final target did not preserve the buy delta among candidates reaching stateful layers",
        },
        "final_validation": {
            "layer_type": "stateful_incremental",
            "pass_through_base_count": len(pass_through),
            "blocked_tickers": sorted(pass_through if final_validation_blocked_all else []),
            "measurement_note": "portfolio-level validation blocker; denominator is candidates reaching validation",
        },
    }


def _blocked_tickers_from_actions(actions: Any) -> set[str]:
    out: set[str] = set()
    for raw in actions or []:
        text = str(raw or "")
        parts = text.split(":")
        if len(parts) >= 2 and parts[0] in {
            "buy_blocked",
            "concentration_add_blocked",
            "hedge_only_add_blocked",
            "llm_advisory_rejected",
        }:
            ticker = parts[1].upper().strip()
            if ticker:
                out.add(ticker)
    return out


def _first_blocker_distribution(
    tickers: list[str],
    stateless: dict[str, dict[str, Any]],
    stateful: dict[str, dict[str, Any]],
) -> dict[str, int]:
    order = [
        ("scorecard", stateless),
        ("decision_style", stateless),
        ("position_governance", stateful),
        ("risk_manager_final_target", stateful),
        ("final_validation", stateful),
        ("broker_order_filter", stateless),
    ]
    out: dict[str, int] = {}
    for ticker in tickers:
        for layer, source in order:
            if ticker in set((source.get(layer) or {}).get("blocked_tickers") or []):
                out[layer] = out.get(layer, 0) + 1
                break
    return dict(sorted(out.items()))


def _layer_blocker_distribution(layers: dict[str, dict[str, Any]]) -> dict[str, int]:
    return {
        layer: len((payload or {}).get("blocked_tickers") or [])
        for layer, payload in sorted(layers.items())
    }


def _single_blocker_count(
    tickers: list[str],
    stateless: dict[str, dict[str, Any]],
    stateful: dict[str, dict[str, Any]],
) -> int:
    count = 0
    all_layers = {**stateless, **stateful}
    for ticker in tickers:
        blockers = [
            layer
            for layer, payload in all_layers.items()
            if ticker in set((payload or {}).get("blocked_tickers") or [])
        ]
        if len(blockers) == 1:
            count += 1
    return count


def _buy_delta_metrics(buy_intents: list[dict[str, Any]]) -> dict[str, Any]:
    desired = sum(float(row.get("desired_buy_delta") or 0.0) for row in buy_intents)
    allowed = sum(float(row.get("allowed_buy_delta_after_gates") or 0.0) for row in buy_intents)
    blocked = sum(float(row.get("blocked_buy_delta") or 0.0) for row in buy_intents)
    if desired <= 0:
        suppression = {
            "value": None,
            "status": "not_applicable",
            "reason": "no_desired_buy_delta",
        }
    else:
        suppression = {
            "value": max(0.0, min(1.0, 1.0 - allowed / desired)),
            "status": "ok",
            "denominator": round(desired, 6),
        }
    return {
        "buy_intent_count": len(buy_intents),
        "blocked_buy_count": sum(1 for row in buy_intents if bool(row.get("blocked"))),
        "desired_buy_delta_before_gates": round(desired, 6),
        "allowed_buy_delta_after_gates": round(allowed, 6),
        "blocked_buy_delta": round(blocked, 6),
        "buy_delta_suppression_ratio": suppression,
    }


def _net_position_drift(
    *,
    current_weights: dict[str, float],
    target_weights: dict[str, float],
) -> dict[str, Any]:
    buy_delta = 0.0
    sell_delta = 0.0
    for ticker in sorted((set(current_weights) | set(target_weights)) - {"CASH"}):
        current = float(current_weights.get(ticker, 0.0) or 0.0)
        target = float(target_weights.get(ticker, 0.0) or 0.0)
        delta = target - current
        if delta > 0:
            buy_delta += delta
        elif delta < 0:
            sell_delta += abs(delta)
    return {
        "allowed_buy_delta_after_gates": round(buy_delta, 6),
        "sell_delta_after_gates": round(sell_delta, 6),
        "net_position_drift": round(buy_delta - sell_delta, 6),
        "direction": "net_add" if buy_delta > sell_delta else "net_reduce" if sell_delta > buy_delta else "flat",
    }


def build_decision_feature_snapshot(
    *,
    analysis_id: int,
    as_of_time: datetime,
    pipeline_context: dict[str, Any] | None,
    brief: dict[str, Any] | None,
) -> DecisionFeatureSnapshot:
    context = pipeline_context or {}
    brief = brief or {}
    values = _feature_values_from_holdings(brief.get("holdings") or [])
    raw_refs = _raw_source_refs(context)
    authority = _feature_authority(brief)
    return DecisionFeatureSnapshot(
        analysis_id=analysis_id,
        created_at=as_of_time,
        as_of_time=as_of_time,
        price_source=_price_source(authority),
        feature_authority=authority,
        feature_values=values,
        raw_source_refs=raw_refs,
    )


def build_market_risk_assessment(
    *,
    analysis_id: int,
    as_of_time: datetime,
    market_scorecard: dict[str, Any],
    synthesizer_out: dict[str, Any],
    brief: dict[str, Any],
) -> MarketRiskAssessment:
    judgment = synthesizer_out.get("market_judgment") or {}
    regime = judgment.get("regime") or market_scorecard.get("regime") or "unknown"
    confidence = (
        judgment.get("confidence")
        or judgment.get("adjusted_confidence")
        or market_scorecard.get("confidence")
    )
    risks = _string_list(judgment.get("primary_risks") or market_scorecard.get("dominant_reasons"))
    conflicts = _string_list(
        judgment.get("conflicts")
        or (brief.get("news_context") or {}).get("data_gaps")
        or market_scorecard.get("tightened_reasons")
    )
    return MarketRiskAssessment(
        analysis_id=analysis_id,
        created_at=as_of_time,
        market_regime=str(regime),
        regime_confidence=confidence,
        primary_risks=risks,
        risk_direction=str(
            judgment.get("risk_direction")
            or market_scorecard.get("permission")
            or market_scorecard.get("stance")
            or "unknown"
        ),
        conflicts=conflicts,
        operator_summary=str(
            judgment.get("summary")
            or market_scorecard.get("dominant_reason")
            or "advisory market risk assessment"
        ),
    )


def _feature_values_from_holdings(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    fields = [
        "price",
        "close_price",
        "weight_current",
        "weight_target",
        "daily_return_pct",
        "return_5d",
        "mom_20d",
        "mom_60d",
        "mom_252d",
        "atr_pct",
        "hist_vol_20d",
        "beta_vs_spy",
        "unrealized_pnl_pct",
        "holding_days",
    ]
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        out[ticker] = {
            field: row.get(field)
            for field in fields
            if row.get(field) is not None
        }
    return out


def _raw_source_refs(context: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    guard = context.get("account_state_guard") or {}
    snapshot = guard.get("snapshot") or {}
    if snapshot.get("id") is not None:
        refs.append(f"account_snapshot:{snapshot.get('id')}")
    if snapshot.get("qc_snapshot_id") is not None:
        refs.append(f"qc_snapshot:{snapshot.get('qc_snapshot_id')}")
    packet_type = snapshot.get("source_packet_type")
    if packet_type:
        refs.append(f"packet_type:{packet_type}")
    return refs


def _feature_authority(brief: dict[str, Any]) -> Literal["qc_live", "yfinance", "mixed", "unknown"]:
    provenance = brief.get("feature_provenance") or {}
    text = json.dumps(provenance, default=str).lower() if provenance else ""
    has_qc = "qc" in text or bool(brief.get("current_weights"))
    has_yf = "yfinance" in text
    if has_qc and has_yf:
        return "mixed"
    if has_qc:
        return "qc_live"
    if has_yf:
        return "yfinance"
    return "unknown"


def _price_source(authority: str) -> Literal["qc_snapshot", "yfinance", "mixed", "unknown"]:
    if authority == "qc_live":
        return "qc_snapshot"
    if authority == "yfinance":
        return "yfinance"
    if authority == "mixed":
        return "mixed"
    return "unknown"


def _ranked_tickers(weights: dict[str, float]) -> list[dict[str, Any]]:
    return [
        {"ticker": ticker, "rank": idx + 1, "weight": weight}
        for idx, (ticker, weight) in enumerate(
            sorted(weights.items(), key=lambda item: (-float(item[1]), item[0]))
        )
    ]


def _float_weights(value: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for ticker, raw in (value or {}).items():
        try:
            out[str(ticker).upper()] = float(raw)
        except (TypeError, ValueError):
            continue
    return out


def _string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, dict):
        return [str(k) for k, v in value.items() if v]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item)]
    return []


def _unique_tickers(values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        ticker = str(raw or "").upper().strip()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        out.append(ticker)
    return out


def _blocked_new_positions_from_style_violations(violations: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in violations:
        if not item.startswith("style_new_position_blocked:"):
            continue
        rest = item.split(":", 1)[1]
        ticker = rest.split(" ", 1)[0].upper().strip()
        if ticker and ticker not in seen:
            seen.add(ticker)
            out.append(ticker)
    return out


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None
