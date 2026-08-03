"""Validated, idempotent ingestion for QC observer event segments."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    QCObserverEvent,
    QCObserverRejectedEvent,
    QCObserverRequestNonce,
    QCObserverStreamReceipt,
)
from services.qc_webhook_auth import ObserverRequestAuth


OBSERVER_CHANNELS = frozenset({"P0", "P1", "P2"})
MAX_EVENTS_PER_BATCH = 500
MAX_MISSING_SEQUENCE_SPAN = 10_000
REQUIRED_EVENT_FIELDS = (
    "schema_version",
    "event_id",
    "event_stream_id",
    "event_channel",
    "event_sequence",
    "event_time",
    "account_id",
    "deployment_id",
    "event_type",
    "source_fingerprint",
    "policy_fingerprint",
    "content_hash",
    "payload",
)
EVENT_CONTRACTS = {
    "decision_manifest": ("decision_manifest.v1", "P0"),
    "order_intent": ("order_intent_event.v1", "P0"),
    "broker_lifecycle_event": ("broker_lifecycle_event.v1", "P0"),
    "local_risk_event": ("local_risk_event.v1", "P0"),
    "correction_event": ("correction_event.v1", "P0"),
    "account_snapshot": ("account_snapshot.v1", "P1"),
    "heartbeat": ("heartbeat.v1", "P2"),
}
PAYLOAD_REQUIRED_FIELDS = {
    "decision_manifest": (
        "decision_id",
        "decision_kind",
        "as_of",
        "strategy_version",
        "basis_account_snapshot_id",
        "market_data_cutoff",
        "ranked_candidates",
        "raw_target_weights",
        "risk_adjusted_weights",
        "execution_style",
        "execution_authority",
    ),
    "order_intent": (
        "order_intent_id",
        "decision_id",
        "security_id",
        "requested_quantity",
        "reference_price",
        "reference_time",
        "order_type",
        "reason",
        "risk_class",
    ),
    "broker_lifecycle_event": (
        "broker_order_id",
        "status",
        "security_id",
        "event_time",
    ),
    "local_risk_event": (
        "risk_event_id",
        "security_id",
        "risk_type",
        "stage",
        "event_time",
    ),
    "correction_event": (
        "corrects_event_id",
        "correction_reason",
        "corrected_fields",
    ),
    "account_snapshot": (
        "account_snapshot_id",
        "equity",
        "cash",
        "holdings",
        "open_orders",
        "state_generation",
        "broker_reconciled",
    ),
    "heartbeat": ("observer_status", "spool_status"),
}


class ObserverEventValidationError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class ObserverEventConflict(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def canonical_event_hash(event: dict[str, Any]) -> str:
    """Hash the immutable event envelope, excluding only the hash field itself."""
    canonical = {key: value for key, value in event.items() if key != "content_hash"}
    encoded = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def normalize_observer_event(event: Any) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise ObserverEventValidationError("event_not_object", "observer event must be an object")

    missing = [field for field in REQUIRED_EVENT_FIELDS if event.get(field) is None]
    if missing:
        raise ObserverEventValidationError(
            "missing_required_fields",
            f"observer event missing required fields: {','.join(missing)}",
        )

    channel = str(event["event_channel"])
    if channel not in OBSERVER_CHANNELS:
        raise ObserverEventValidationError(
            "invalid_event_channel",
            f"event_channel must be one of {sorted(OBSERVER_CHANNELS)}",
        )

    raw_sequence = event["event_sequence"]
    if isinstance(raw_sequence, bool) or not isinstance(raw_sequence, int):
        raise ObserverEventValidationError(
            "invalid_event_sequence", "event_sequence must be a positive integer"
        )
    sequence = raw_sequence
    if sequence <= 0:
        raise ObserverEventValidationError(
            "invalid_event_sequence", "event_sequence must be a positive integer"
        )

    event_time = _parse_utc_datetime(event["event_time"])
    if not isinstance(event["payload"], dict):
        raise ObserverEventValidationError("invalid_payload", "payload must be an object")
    for field in ("event_id", "event_stream_id", "account_id", "deployment_id"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise ObserverEventValidationError(
                "invalid_required_identifier", f"{field} must be a non-empty string"
            )
    for field in ("source_fingerprint", "policy_fingerprint"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            raise ObserverEventValidationError(
                "invalid_fingerprint", f"{field} must be a non-empty string"
            )
    validate_event_payload_contract(event)

    supplied_hash = str(event["content_hash"]).lower()
    try:
        expected_hash = canonical_event_hash(event)
    except (TypeError, ValueError) as exc:
        raise ObserverEventValidationError(
            "non_canonical_event_value",
            "observer event contains a value that cannot be canonically encoded",
        ) from exc
    if supplied_hash != expected_hash:
        raise ObserverEventValidationError(
            "content_hash_mismatch",
            f"content_hash mismatch for event_id={event['event_id']}",
        )
    normalized = dict(event)
    normalized["content_hash"] = supplied_hash
    normalized["_event_time_dt"] = event_time
    return normalized


def validate_observer_batch(body: Any) -> list[dict[str, Any]]:
    events = _observer_batch_events(body)
    return [normalize_observer_event(event) for event in events]


def validate_observer_batch_partial(
    body: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate siblings independently while preserving invalid evidence."""
    events = _observer_batch_events(body)
    valid: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        try:
            valid.append(normalize_observer_event(event))
        except ObserverEventValidationError as exc:
            rejected.append(
                {
                    "event_index": index,
                    "event_id": _raw_optional(event, "event_id"),
                    "event_stream_id": _raw_optional(event, "event_stream_id"),
                    "event_channel": _raw_optional(event, "event_channel"),
                    "event_sequence": _raw_int(event, "event_sequence"),
                    "account_id": _raw_optional(event, "account_id"),
                    "deployment_id": _raw_optional(event, "deployment_id"),
                    "rejection_code": exc.code,
                    "rejection_message": str(exc),
                    "raw_content_hash": _raw_event_hash(event),
                    "raw_event": event,
                }
            )
    return valid, rejected


def enforce_observer_request_identity(
    events: list[Any],
    auth: ObserverRequestAuth,
) -> None:
    """Bind signed request identity to every accepted event envelope."""
    if auth.auth_version != "v2":
        return
    for event in events:
        if not isinstance(event, dict):
            continue
        if event.get("account_id") != auth.account_id:
            raise ObserverEventValidationError(
                "observer_request_account_mismatch",
                "event account_id does not match signed request account",
            )
        if event.get("deployment_id") != auth.deployment_id:
            raise ObserverEventValidationError(
                "observer_request_deployment_mismatch",
                "event deployment_id does not match signed request deployment",
            )


async def reserve_observer_request_nonce(
    db: AsyncSession,
    auth: ObserverRequestAuth,
    *,
    ttl_seconds: int,
) -> None:
    """Reserve a v2 nonce in the caller transaction before event ingestion."""
    if auth.auth_version != "v2":
        return
    existing = await db.scalar(
        select(QCObserverRequestNonce).where(QCObserverRequestNonce.nonce == auth.nonce)
    )
    if existing is not None:
        raise ObserverEventConflict(
            "observer_nonce_reused",
            f"observer nonce={auth.nonce} has already been accepted",
        )
    db.add(
        QCObserverRequestNonce(
            nonce=auth.nonce,
            account_id=auth.account_id,
            deployment_id=auth.deployment_id,
            request_timestamp=auth.request_timestamp,
            body_hash=auth.body_hash,
            expires_at=auth.request_timestamp + timedelta(seconds=max(ttl_seconds, 1)),
        )
    )
    try:
        await db.flush()
    except IntegrityError as exc:
        raise ObserverEventConflict(
            "observer_nonce_reused",
            f"observer nonce={auth.nonce} has already been accepted",
        ) from exc


async def quarantine_rejected_observer_events(
    db: AsyncSession,
    rejected: list[dict[str, Any]],
    *,
    request_nonce: str | None,
) -> list[dict[str, Any]]:
    affected_channels: set[tuple[str, str]] = set()
    for item in rejected:
        if item["event_stream_id"] and item["event_channel"] in OBSERVER_CHANNELS:
            affected_channels.add((item["event_stream_id"], item["event_channel"]))
        db.add(
            QCObserverRejectedEvent(
                request_nonce=request_nonce,
                event_index=item["event_index"],
                event_id=item["event_id"],
                event_stream_id=item["event_stream_id"],
                event_channel=item["event_channel"],
                event_sequence=item["event_sequence"],
                account_id=item["account_id"],
                deployment_id=item["deployment_id"],
                rejection_code=item["rejection_code"],
                rejection_message=item["rejection_message"],
                raw_content_hash=item["raw_content_hash"],
                raw_event=item["raw_event"],
            )
        )
    if rejected:
        await db.flush()
    return [
        await _refresh_channel_receipt(db, stream_id, channel)
        for stream_id, channel in sorted(affected_channels)
    ]


def validate_event_payload_contract(event: dict[str, Any]) -> None:
    event_type = str(event.get("event_type") or "")
    contract = EVENT_CONTRACTS.get(event_type)
    if contract is None:
        raise ObserverEventValidationError(
            "unsupported_event_type", f"unsupported event_type={event_type}"
        )
    expected_schema, expected_channel = contract
    if str(event.get("schema_version")) != expected_schema:
        raise ObserverEventValidationError(
            "schema_event_mismatch",
            f"event_type={event_type} requires schema_version={expected_schema}",
        )
    if str(event.get("event_channel")) != expected_channel:
        raise ObserverEventValidationError(
            "event_channel_mismatch",
            f"event_type={event_type} requires event_channel={expected_channel}",
        )

    payload = event["payload"]
    missing = [field for field in PAYLOAD_REQUIRED_FIELDS[event_type] if payload.get(field) is None]
    if missing:
        raise ObserverEventValidationError(
            "payload_missing_required_fields",
            f"event_type={event_type} payload missing: {','.join(missing)}",
        )
    if event_type == "decision_manifest":
        if payload.get("execution_authority") != "qc_only":
            raise ObserverEventValidationError(
                "invalid_decision_execution_authority",
                "decision manifest execution_authority must be qc_only",
            )
        if not isinstance(payload.get("ranked_candidates"), list):
            raise ObserverEventValidationError(
                "invalid_ranked_candidates", "ranked_candidates must be an array"
            )
        for field in ("raw_target_weights", "risk_adjusted_weights"):
            if not isinstance(payload.get(field), dict):
                raise ObserverEventValidationError(
                    "invalid_target_weights", f"{field} must be an object"
                )


async def ingest_observer_events(
    db: AsyncSession,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Insert raw facts and update channel receipts inside the caller transaction."""
    accepted_ids: list[str] = []
    duplicate_ids: list[str] = []
    affected_channels: set[tuple[str, str]] = set()
    batch_ids: dict[str, str] = {}
    batch_sequences: dict[tuple[str, str, int], str] = {}

    for event in events:
        event_id = str(event["event_id"])
        stream_id = str(event["event_stream_id"])
        channel = str(event["event_channel"])
        sequence = int(event["event_sequence"])
        affected_channels.add((stream_id, channel))

        prior_hash = batch_ids.get(event_id)
        if prior_hash is not None:
            if prior_hash != event["content_hash"]:
                raise ObserverEventConflict(
                    "event_id_hash_conflict",
                    f"event_id={event_id} appears twice with different content hashes",
                )
            duplicate_ids.append(event_id)
            continue
        sequence_key = (stream_id, channel, sequence)
        prior_event_id = batch_sequences.get(sequence_key)
        if prior_event_id is not None:
            raise ObserverEventConflict(
                "stream_sequence_conflict",
                f"stream={stream_id} channel={channel} sequence={sequence} appears twice "
                f"for event_ids={prior_event_id},{event_id}",
            )
        batch_ids[event_id] = event["content_hash"]
        batch_sequences[sequence_key] = event_id

        existing_id = await db.scalar(
            select(QCObserverEvent).where(QCObserverEvent.event_id == event_id)
        )
        identity = classify_observer_event_identity(event, existing_id=existing_id)
        if identity == "duplicate":
            duplicate_ids.append(event_id)
            continue

        existing_sequence = await db.scalar(
            select(QCObserverEvent).where(
                QCObserverEvent.event_stream_id == stream_id,
                QCObserverEvent.event_channel == channel,
                QCObserverEvent.event_sequence == sequence,
            )
        )
        classify_observer_event_identity(event, existing_sequence=existing_sequence)

        db.add(
            QCObserverEvent(
                event_id=event_id,
                event_stream_id=stream_id,
                event_channel=channel,
                event_sequence=sequence,
                schema_version=str(event["schema_version"]),
                event_type=str(event["event_type"]),
                account_id=str(event["account_id"]),
                deployment_id=str(event["deployment_id"]),
                strategy_id=_optional_string(event.get("strategy_id")),
                event_time=event["_event_time_dt"],
                source_fingerprint=_optional_string(event.get("source_fingerprint")),
                policy_fingerprint=_optional_string(event.get("policy_fingerprint")),
                content_hash=event["content_hash"],
                validation_status="valid",
                payload={key: value for key, value in event.items() if key != "_event_time_dt"},
            )
        )
        accepted_ids.append(event_id)

    await db.flush()
    channel_acks = []
    for stream_id, channel in sorted(affected_channels):
        channel_acks.append(await _refresh_channel_receipt(db, stream_id, channel))

    return {
        "status": "accepted",
        "accepted_event_ids": accepted_ids,
        "duplicate_event_ids": duplicate_ids,
        "channels": channel_acks,
    }


async def _refresh_channel_receipt(
    db: AsyncSession,
    stream_id: str,
    channel: str,
) -> dict[str, Any]:
    receipt = await db.scalar(
        select(QCObserverStreamReceipt)
        .where(
            QCObserverStreamReceipt.event_stream_id == stream_id,
            QCObserverStreamReceipt.event_channel == channel,
        )
        .with_for_update()
    )
    if receipt is None:
        receipt = QCObserverStreamReceipt(
            event_stream_id=stream_id,
            event_channel=channel,
            highest_contiguous_sequence=0,
            highest_seen_sequence=0,
            missing_sequences=[],
        )
        db.add(receipt)
        await db.flush()

    rows = (
        await db.scalars(
            select(QCObserverEvent)
            .where(
                QCObserverEvent.event_stream_id == stream_id,
                QCObserverEvent.event_channel == channel,
            )
            .order_by(QCObserverEvent.event_sequence)
        )
    ).all()
    rejected_sequences = (
        await db.scalars(
            select(QCObserverRejectedEvent.event_sequence).where(
                QCObserverRejectedEvent.event_stream_id == stream_id,
                QCObserverRejectedEvent.event_channel == channel,
                QCObserverRejectedEvent.event_sequence.is_not(None),
            )
        )
    ).all()
    sequences = [int(row.event_sequence) for row in rows]
    sequences.extend(int(value) for value in rejected_sequences)
    contiguous, highest_seen, missing = calculate_channel_receipt(sequences)

    latest = rows[-1] if rows else None
    receipt.highest_contiguous_sequence = contiguous
    receipt.highest_seen_sequence = highest_seen
    receipt.missing_sequences = missing
    receipt.last_event_id = latest.event_id if latest is not None else None
    receipt.last_event_time = latest.event_time if latest is not None else None

    return {
        "event_stream_id": stream_id,
        "event_channel": channel,
        "highest_contiguous_sequence": contiguous,
        "highest_seen_sequence": highest_seen,
        "missing_sequences": missing,
    }


def _parse_utc_datetime(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ObserverEventValidationError(
            "invalid_event_time", "event_time must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ObserverEventValidationError(
            "event_time_not_utc", "event_time must include a UTC offset"
        )
    if parsed.utcoffset().total_seconds() != 0:
        raise ObserverEventValidationError(
            "event_time_not_utc", "event_time must be normalized to UTC"
        )
    return parsed


def _optional_string(value: Any) -> str | None:
    return None if value is None or value == "" else str(value)


def _observer_batch_events(body: Any) -> list[Any]:
    if not isinstance(body, dict) or not isinstance(body.get("events"), list):
        raise ObserverEventValidationError(
            "invalid_batch", "request body must contain an events array"
        )
    events = body["events"]
    if not events:
        raise ObserverEventValidationError("empty_batch", "events array cannot be empty")
    if len(events) > MAX_EVENTS_PER_BATCH:
        raise ObserverEventValidationError(
            "batch_too_large", f"events array exceeds {MAX_EVENTS_PER_BATCH}"
        )
    return events


def _raw_optional(event: Any, field: str) -> str | None:
    if not isinstance(event, dict):
        return None
    value = event.get(field)
    return None if value is None or value == "" else str(value)


def _raw_int(event: Any, field: str) -> int | None:
    if not isinstance(event, dict):
        return None
    value = event.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _raw_event_hash(event: Any) -> str:
    try:
        encoded = json.dumps(
            event,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        encoded = repr(event).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def classify_observer_event_identity(
    event: dict[str, Any],
    *,
    existing_id: Any | None = None,
    existing_sequence: Any | None = None,
) -> str:
    """Classify an event as new/duplicate or raise a stable conflict reason."""
    event_id = str(event["event_id"])
    if existing_id is not None:
        if existing_id.content_hash != event["content_hash"]:
            raise ObserverEventConflict(
                "event_id_hash_conflict",
                f"event_id={event_id} already exists with a different content hash",
            )
        return "duplicate"
    if existing_sequence is not None:
        raise ObserverEventConflict(
            "stream_sequence_conflict",
            f"stream={event['event_stream_id']} channel={event['event_channel']} "
            f"sequence={event['event_sequence']} already belongs to "
            f"event_id={existing_sequence.event_id}",
        )
    return "new"


def calculate_channel_receipt(sequences: list[int]) -> tuple[int, int, list[int]]:
    """Return contiguous high-water mark, highest seen, and explicit gaps."""
    sequence_set = {int(value) for value in sequences if int(value) > 0}
    highest_seen = max(sequence_set, default=0)
    contiguous = 0
    while contiguous + 1 in sequence_set:
        contiguous += 1
    if highest_seen - contiguous > MAX_MISSING_SEQUENCE_SPAN:
        raise ObserverEventValidationError(
            "sequence_gap_span_too_large",
            f"sequence gap span exceeds {MAX_MISSING_SEQUENCE_SPAN}",
        )
    missing = [value for value in range(contiguous + 1, highest_seen + 1) if value not in sequence_set]
    return contiguous, highest_seen, missing
