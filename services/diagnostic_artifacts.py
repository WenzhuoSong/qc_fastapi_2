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


Artifact = (
    MarketRiskAssessment
    | DecisionFeatureSnapshot
    | CandidateEvent
    | RankingEvent
    | PortfolioMixEvent
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
