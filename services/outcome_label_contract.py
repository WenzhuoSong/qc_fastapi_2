"""Point-in-time outcome label contract.

This module defines the metadata and validation rules required before outcome
labels can become training-authority data. It intentionally does not build a
label store or run any backfill.
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from services.outcome_label_policy import (
    LABEL_PRICE_SOURCE_MAP as _LABEL_PRICE_SOURCE_MAP,
    outcome_label_contract_summary,
)


ExecutionAuthority = Literal["none"]
Horizon = Literal["1d", "5d", "20d"]
LabelSource = Literal["qc_execution", "qc_snapshot", "yfinance"]
PriceSource = Literal["fill_price", "qc_market_price", "yfinance_adjusted_close"]
TrainingAuthority = Literal["eligible", "feature_scope_limited"]


class OutcomeLabel(BaseModel):
    """Versioned outcome label metadata with point-in-time safety checks."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    label_schema_version: Literal["outcome_label_v1"] = "outcome_label_v1"
    label_id: str | None = None
    execution_authority: ExecutionAuthority = "none"

    decision_time: datetime
    as_of_time: datetime
    horizon: Horizon
    label_source: LabelSource
    price_source: PriceSource
    return_value: float = Field(alias="return")
    max_drawdown_after_decision: float

    decision_feature_snapshot_id: str | None = None
    decision_feature_snapshot_schema_version: str | None = None
    decision_feature_snapshot_as_of_time: datetime | None = None
    training_authority: TrainingAuthority = "eligible"
    scope_limit_reasons: list[str] = Field(default_factory=list)
    source_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("decision_time", "as_of_time", "decision_feature_snapshot_as_of_time", mode="before")
    @classmethod
    def _parse_dt(cls, value: Any) -> Any:
        if isinstance(value, str):
            text = value.strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            value = datetime.fromisoformat(text)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=UTC)
            return value.astimezone(UTC)
        return value

    @model_validator(mode="after")
    def _validate_point_in_time_contract(self) -> "OutcomeLabel":
        if self.as_of_time <= self.decision_time:
            raise ValueError("outcome as_of_time must be after decision_time")

        allowed_prices = _LABEL_PRICE_SOURCE_MAP[self.label_source]
        if self.price_source not in allowed_prices:
            raise ValueError(
                f"label_source={self.label_source} cannot use price_source={self.price_source}"
            )

        reasons = list(self.scope_limit_reasons or [])
        if not self.decision_feature_snapshot_id:
            reasons.append("missing_decision_feature_snapshot")
        if (
            self.decision_feature_snapshot_schema_version
            and self.decision_feature_snapshot_schema_version != "decision_feature_snapshot_v1"
        ):
            reasons.append("invalid_decision_feature_snapshot_schema")
        if (
            self.decision_feature_snapshot_as_of_time is not None
            and self.decision_feature_snapshot_as_of_time > self.decision_time
        ):
            reasons.append("feature_snapshot_after_decision_time")
        if self.source_metadata.get("feature_training_authority") == "feature_scope_limited":
            reasons.append("feature_snapshot_scope_limited")
        if self.source_metadata.get("feature_authority") == "mixed":
            reasons.append("mixed_feature_authority")
        if self.source_metadata.get("price_source_mixed") is True:
            reasons.append("mixed_price_source")

        if reasons:
            self.scope_limit_reasons = sorted(set(reasons))
            self.training_authority = "feature_scope_limited"

        if not self.label_id:
            self.label_id = self._build_label_id()
        return self

    def _build_label_id(self) -> str:
        payload = {
            "label_schema_version": self.label_schema_version,
            "decision_time": self.decision_time.isoformat(),
            "as_of_time": self.as_of_time.isoformat(),
            "horizon": self.horizon,
            "label_source": self.label_source,
            "price_source": self.price_source,
            "decision_feature_snapshot_id": self.decision_feature_snapshot_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:24]
        return f"outcome_label_v1:{digest}"


def serialize_outcome_label(label: OutcomeLabel | dict[str, Any]) -> dict[str, Any]:
    """Serialize an outcome label into JSONB-safe dict form."""
    if isinstance(label, OutcomeLabel):
        return label.model_dump(mode="json", by_alias=True)
    if isinstance(label, dict):
        return OutcomeLabel(**label).model_dump(mode="json", by_alias=True)
    raise TypeError(f"unsupported outcome label type: {type(label)!r}")


def build_outcome_label(
    *,
    decision_time: datetime,
    as_of_time: datetime,
    horizon: Horizon,
    label_source: LabelSource,
    price_source: PriceSource,
    return_value: float,
    max_drawdown_after_decision: float,
    decision_feature_snapshot: dict[str, Any] | None = None,
    source_metadata: dict[str, Any] | None = None,
) -> OutcomeLabel:
    """Construct an outcome label from a PR6 feature snapshot reference."""
    snapshot = decision_feature_snapshot or {}
    metadata = dict(source_metadata or {})
    if snapshot:
        metadata.setdefault("feature_training_authority", snapshot.get("training_authority"))
        metadata.setdefault("feature_authority", snapshot.get("feature_authority"))
        metadata.setdefault("feature_schema_version", snapshot.get("schema_version"))
    return OutcomeLabel(
        decision_time=decision_time,
        as_of_time=as_of_time,
        horizon=horizon,
        label_source=label_source,
        price_source=price_source,
        return_value=return_value,
        max_drawdown_after_decision=max_drawdown_after_decision,
        decision_feature_snapshot_id=snapshot.get("artifact_id"),
        decision_feature_snapshot_schema_version=snapshot.get("schema_version"),
        decision_feature_snapshot_as_of_time=_parse_optional_datetime(snapshot.get("as_of_time")),
        source_metadata=metadata,
    )


def label_has_training_authority(label: OutcomeLabel | dict[str, Any]) -> bool:
    """Return whether the label can be used by training datasets."""
    payload = serialize_outcome_label(label)
    return payload.get("training_authority") == "eligible"


def _parse_optional_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    return None
