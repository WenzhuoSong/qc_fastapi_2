"""Versioned weekend review artifacts.

These artifacts persist deterministic PR1 metrics as append-only, review-only
records. They carry no execution authority and no target mutation authority.
When pydantic is installed (as in the app requirements), the model uses
Pydantic validation. Local lightweight test environments can use the dataclass
fallback with the same invariants.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from services.json_safety import json_safe
from services.weekend_review_loader import EXECUTION_AUTHORITY, TARGET_WEIGHT_MUTATION

try:  # pragma: no cover - covered in deployed env where pydantic is present.
    from pydantic import BaseModel, ConfigDict, Field, model_validator

    HAS_PYDANTIC = True
except ModuleNotFoundError:  # pragma: no cover - exercised by local uv env.
    BaseModel = object  # type: ignore[assignment]
    ConfigDict = None  # type: ignore[assignment]
    Field = None  # type: ignore[assignment]
    HAS_PYDANTIC = False


ARTIFACT_CONTRACT_VERSION = "weekend_review_artifacts_v1"

SECTION_SCHEMA_MAP: dict[str, str] = {
    "decision_degradation": "weekly_decision_degradation_review_v1",
    "execution_truth": "weekly_execution_truth_review_v1",
    "intent_execution": "weekly_intent_execution_review_v1",
    "label_maturity": "weekly_label_maturity_review_v1",
    "hedge_review": "weekly_hedge_review_v1",
    "debate_impact": "weekly_debate_impact_review_v1",
    "regime_risk": "weekly_regime_risk_review_v1",
    "style_opportunity": "weekly_style_opportunity_review_v1",
    "decision_funnel": "weekly_decision_funnel_review_v1",
    "basket_portfolio": "weekly_strategy_basket_review_v1",
    "weekly_self_assessment": "weekly_review_self_assessment_v1",
}

ALLOWED_SCHEMA_VERSIONS = set(SECTION_SCHEMA_MAP.values())


if HAS_PYDANTIC:

    class WeeklyReviewArtifact(BaseModel):  # type: ignore[misc]
        """Pydantic weekly review artifact model."""

        model_config = ConfigDict(extra="forbid")  # type: ignore[operator]

        schema_version: str
        artifact_type: str
        artifact_id: str | None = None
        artifact_contract_version: str = ARTIFACT_CONTRACT_VERSION
        created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))  # type: ignore[misc]
        week_start: str | None = None
        week_end: str | None = None
        execution_authority: Literal["none"] = "none"
        target_weight_mutation: Literal["none"] = "none"
        metric_contract_version: str | None = None
        metrics: dict[str, Any] = Field(default_factory=dict)  # type: ignore[misc]
        rates: dict[str, Any] = Field(default_factory=dict)  # type: ignore[misc]
        evidence_refs: list[dict[str, Any]] = Field(default_factory=list)  # type: ignore[misc]
        section_payload: dict[str, Any] = Field(default_factory=dict)  # type: ignore[misc]
        source_counts: dict[str, int] = Field(default_factory=dict)  # type: ignore[misc]
        exclusion_counts: dict[str, int] = Field(default_factory=dict)  # type: ignore[misc]
        excluded_input_count: int = 0
        llm_summary: None = None
        recommendations: list[dict[str, Any]] = Field(default_factory=list)  # type: ignore[misc]

        @model_validator(mode="after")
        def _validate_artifact(self) -> "WeeklyReviewArtifact":
            _validate_schema(self.schema_version)
            if self.execution_authority != EXECUTION_AUTHORITY:
                raise ValueError("weekly review artifact execution_authority must be none")
            if self.target_weight_mutation != TARGET_WEIGHT_MUTATION:
                raise ValueError("weekly review artifact target_weight_mutation must be none")
            if self.llm_summary is not None:
                raise ValueError("PR2 artifacts must not carry an LLM summary")
            if not self.artifact_id:
                self.artifact_id = _artifact_id(serialize_weekly_review_artifact(self, assign_id=False))
            return self

else:

    @dataclass(frozen=True)
    class WeeklyReviewArtifact:
        """Dependency-light weekly review artifact model."""

        schema_version: str
        artifact_type: str
        artifact_id: str | None = None
        artifact_contract_version: str = ARTIFACT_CONTRACT_VERSION
        created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
        week_start: str | None = None
        week_end: str | None = None
        execution_authority: str = EXECUTION_AUTHORITY
        target_weight_mutation: str = TARGET_WEIGHT_MUTATION
        metric_contract_version: str | None = None
        metrics: dict[str, Any] = field(default_factory=dict)
        rates: dict[str, Any] = field(default_factory=dict)
        evidence_refs: list[dict[str, Any]] = field(default_factory=list)
        section_payload: dict[str, Any] = field(default_factory=dict)
        source_counts: dict[str, int] = field(default_factory=dict)
        exclusion_counts: dict[str, int] = field(default_factory=dict)
        excluded_input_count: int = 0
        llm_summary: None = None
        recommendations: list[dict[str, Any]] = field(default_factory=list)

        def __post_init__(self) -> None:
            _validate_schema(self.schema_version)
            if self.execution_authority != EXECUTION_AUTHORITY:
                raise ValueError("weekly review artifact execution_authority must be none")
            if self.target_weight_mutation != TARGET_WEIGHT_MUTATION:
                raise ValueError("weekly review artifact target_weight_mutation must be none")
            if self.llm_summary is not None:
                raise ValueError("PR2 artifacts must not carry an LLM summary")
            if not self.artifact_id:
                object.__setattr__(
                    self,
                    "artifact_id",
                    _artifact_id(serialize_weekly_review_artifact(self, assign_id=False)),
                )


def build_weekly_review_artifacts(
    metrics_payload: dict[str, Any],
    *,
    week_start: date | str | None = None,
    week_end: date | str | None = None,
    created_at: datetime | None = None,
) -> list[WeeklyReviewArtifact]:
    """Build one append-only artifact per deterministic metrics section."""
    created = created_at or datetime.now(timezone.utc)
    start, end = _week_window(metrics_payload, week_start=week_start, week_end=week_end)
    sections = metrics_payload.get("sections") if isinstance(metrics_payload.get("sections"), dict) else {}
    artifacts: list[WeeklyReviewArtifact] = []
    for section_name, schema_version in SECTION_SCHEMA_MAP.items():
        section = sections.get(section_name)
        if not isinstance(section, dict):
            continue
        artifacts.append(
            WeeklyReviewArtifact(
                schema_version=schema_version,
                artifact_type=section_name,
                created_at=created,
                week_start=start,
                week_end=end,
                metric_contract_version=metrics_payload.get("contract_version"),
                metrics=section.get("metrics") if isinstance(section.get("metrics"), dict) else {},
                rates=section.get("rates") if isinstance(section.get("rates"), dict) else {},
                evidence_refs=section.get("evidence_refs") if isinstance(section.get("evidence_refs"), list) else [],
                section_payload=section,
                source_counts=metrics_payload.get("source_counts") if isinstance(metrics_payload.get("source_counts"), dict) else {},
                exclusion_counts=metrics_payload.get("exclusion_counts") if isinstance(metrics_payload.get("exclusion_counts"), dict) else {},
                excluded_input_count=int(metrics_payload.get("excluded_input_count") or 0),
            )
        )
    return artifacts


def serialize_weekly_review_artifact(
    artifact: WeeklyReviewArtifact | dict[str, Any],
    *,
    assign_id: bool = True,
) -> dict[str, Any]:
    """Return a JSONB-safe artifact dict after enforcing PR2 invariants."""
    if hasattr(artifact, "model_dump"):
        payload = artifact.model_dump(mode="json")  # type: ignore[attr-defined]
    elif is_dataclass(artifact):
        payload = asdict(artifact)
    elif isinstance(artifact, dict):
        payload = dict(artifact)
    else:
        raise TypeError(f"unsupported weekly review artifact type: {type(artifact)!r}")
    _validate_artifact_payload(payload)
    if assign_id and not payload.get("artifact_id"):
        payload["artifact_id"] = _artifact_id(payload)
    return json_safe(payload)


def append_weekly_review_artifacts(
    payload: dict[str, Any] | None,
    artifacts: list[WeeklyReviewArtifact | dict[str, Any]],
    *,
    key: str = "weekend_review_artifacts",
) -> dict[str, Any]:
    """Append weekly review artifacts without replacing previous records."""
    out = dict(payload or {})
    existing = out.get(key)
    records = list(existing) if isinstance(existing, list) else []
    records.extend(serialize_weekly_review_artifact(item) for item in artifacts)
    out[key] = records
    out["weekend_review_artifact_count"] = len(records)
    return json_safe(out)


def _validate_schema(schema_version: str) -> None:
    if schema_version not in ALLOWED_SCHEMA_VERSIONS:
        raise ValueError(f"unsupported weekly review schema_version: {schema_version}")


def _validate_artifact_payload(payload: dict[str, Any]) -> None:
    _validate_schema(str(payload.get("schema_version") or ""))
    if payload.get("execution_authority") != EXECUTION_AUTHORITY:
        raise ValueError("weekly review artifact execution_authority must be none")
    if payload.get("target_weight_mutation") != TARGET_WEIGHT_MUTATION:
        raise ValueError("weekly review artifact target_weight_mutation must be none")
    if payload.get("llm_summary") is not None:
        raise ValueError("PR2 artifacts must not carry an LLM summary")


def _artifact_id(payload: dict[str, Any]) -> str:
    clean = {
        key: payload.get(key)
        for key in (
            "schema_version",
            "artifact_type",
            "created_at",
            "week_start",
            "week_end",
            "metric_contract_version",
            "metrics",
            "rates",
        )
    }
    digest = hashlib.sha256(
        json.dumps(json_safe(clean), sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{payload.get('schema_version')}:{digest}"


def _week_window(
    metrics_payload: dict[str, Any],
    *,
    week_start: date | str | None,
    week_end: date | str | None,
) -> tuple[str | None, str | None]:
    if week_start and week_end:
        return _date_str(week_start), _date_str(week_end)
    review_as_of = _parse_datetime(metrics_payload.get("review_as_of"))
    if review_as_of is None:
        return _date_str(week_start), _date_str(week_end)
    day = review_as_of.date()
    start = day - timedelta(days=day.weekday())
    end = start + timedelta(days=6)
    return start.isoformat(), end.isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _date_str(value: date | str | None) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value[:10]
    return None
