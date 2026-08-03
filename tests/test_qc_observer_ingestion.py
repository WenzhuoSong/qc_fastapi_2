import os
import asyncio
import hashlib
import hmac
import importlib.util
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

for _name, _value in {
    "OPENAI_API_KEY": "test-only",
    "WEBHOOK_SECRET": "test-only",
    "QC_USER_ID": "1",
    "QC_API_TOKEN": "test-only",
    "QC_PROJECT_ID": "1",
    "TG_BOT_TOKEN": "test-only",
    "TG_CHAT_ID": "1",
}.items():
    os.environ.setdefault(_name, _value)

from services.qc_observer_ingestion import (
    ObserverEventConflict,
    ObserverEventValidationError,
    calculate_channel_receipt,
    canonical_event_hash,
    classify_observer_event_identity,
    ingest_observer_events,
    normalize_observer_event,
    validate_observer_batch,
    validate_observer_batch_partial,
)


def _event(**overrides):
    event = {
        "schema_version": "broker_lifecycle_event.v1",
        "event_id": "evt-1",
        "event_stream_id": "stream-deploy-1",
        "event_channel": "P0",
        "event_sequence": 1,
        "event_time": "2026-07-18T01:02:03Z",
        "account_id": "paper-account",
        "deployment_id": "deploy-1",
        "strategy_id": "stronger252",
        "source_fingerprint": "sha256:source",
        "policy_fingerprint": "sha256:policy",
        "event_type": "broker_lifecycle_event",
        "payload": {
            "broker_order_id": "123",
            "status": "filled",
            "security_id": "SPY",
            "event_time": "2026-07-18T01:02:03Z",
        },
    }
    event.update(overrides)
    event["content_hash"] = canonical_event_hash(event)
    return event


class QCObserverIngestionTests(unittest.TestCase):
    def test_canonical_hash_is_key_order_independent(self):
        first = _event()
        second = dict(reversed(list(first.items())))

        self.assertEqual(canonical_event_hash(first), canonical_event_hash(second))

    def test_normalize_accepts_valid_utc_event(self):
        normalized = normalize_observer_event(_event())

        self.assertEqual(normalized["event_channel"], "P0")
        self.assertEqual(normalized["event_sequence"], 1)
        self.assertEqual(normalized["event_time"], "2026-07-18T01:02:03Z")

    def test_tampering_after_hash_is_rejected(self):
        event = _event()
        event["payload"]["status"] = "rejected"

        with self.assertRaisesRegex(ObserverEventValidationError, "content_hash mismatch"):
            normalize_observer_event(event)

    def test_float_or_non_utc_sequence_contract_is_rejected(self):
        float_sequence = _event(event_sequence=1.0)
        with self.assertRaises(ObserverEventValidationError) as sequence_error:
            normalize_observer_event(float_sequence)
        self.assertEqual(sequence_error.exception.code, "invalid_event_sequence")

        non_utc = _event(event_time="2026-07-18T09:02:03+08:00")
        with self.assertRaises(ObserverEventValidationError) as time_error:
            normalize_observer_event(non_utc)
        self.assertEqual(time_error.exception.code, "event_time_not_utc")

    def test_empty_identifier_or_fingerprint_is_rejected(self):
        for field in ("event_id", "event_stream_id", "account_id", "deployment_id"):
            with self.subTest(field=field):
                with self.assertRaises(ObserverEventValidationError) as error:
                    normalize_observer_event(_event(**{field: ""}))
                self.assertEqual(error.exception.code, "invalid_required_identifier")
        for field in ("source_fingerprint", "policy_fingerprint"):
            with self.subTest(field=field):
                with self.assertRaises(ObserverEventValidationError) as error:
                    normalize_observer_event(_event(**{field: ""}))
                self.assertEqual(error.exception.code, "invalid_fingerprint")

    def test_batch_requires_bounded_nonempty_events_array(self):
        self.assertEqual(len(validate_observer_batch({"events": [_event()]})), 1)
        for invalid in ({}, {"events": []}, {"events": "not-a-list"}):
            with self.assertRaises(ObserverEventValidationError):
                validate_observer_batch(invalid)

    def test_partial_batch_validation_preserves_valid_siblings(self):
        invalid = _event(event_id="evt-bad", event_sequence=2)
        invalid["payload"].pop("security_id")
        invalid["content_hash"] = canonical_event_hash(invalid)

        valid, rejected = validate_observer_batch_partial(
            {"events": [_event(), invalid, _event(event_id="evt-3", event_sequence=3)]}
        )

        self.assertEqual([item["event_id"] for item in valid], ["evt-1", "evt-3"])
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["event_id"], "evt-bad")
        self.assertEqual(rejected[0]["event_sequence"], 2)
        self.assertEqual(rejected[0]["rejection_code"], "payload_missing_required_fields")

    def test_identity_classification_is_idempotent_but_detects_conflicts(self):
        event = normalize_observer_event(_event())
        same = SimpleNamespace(content_hash=event["content_hash"], event_id="evt-1")
        other_hash = SimpleNamespace(content_hash="0" * 64, event_id="evt-1")
        occupied_sequence = SimpleNamespace(event_id="evt-other")

        self.assertEqual(
            classify_observer_event_identity(event, existing_id=same),
            "duplicate",
        )
        with self.assertRaises(ObserverEventConflict) as hash_conflict:
            classify_observer_event_identity(event, existing_id=other_hash)
        self.assertEqual(hash_conflict.exception.code, "event_id_hash_conflict")
        with self.assertRaises(ObserverEventConflict) as sequence_conflict:
            classify_observer_event_identity(event, existing_sequence=occupied_sequence)
        self.assertEqual(sequence_conflict.exception.code, "stream_sequence_conflict")

    def test_channel_receipt_reports_only_real_gaps(self):
        self.assertEqual(calculate_channel_receipt([1, 2, 4]), (2, 4, [3]))
        self.assertEqual(calculate_channel_receipt([2, 3]), (0, 3, [1]))
        self.assertEqual(calculate_channel_receipt([3, 2, 1]), (3, 3, []))
        with self.assertRaises(ObserverEventValidationError) as oversized:
            calculate_channel_receipt([10_002])
        self.assertEqual(oversized.exception.code, "sequence_gap_span_too_large")

    def test_intra_batch_identity_conflicts_are_stable(self):
        class FakeDB:
            scalar = AsyncMock(return_value=None)

            def add(self, _value):
                return None

        first = normalize_observer_event(_event())
        same_id_other_content = normalize_observer_event(
            _event(payload={**_event()["payload"], "status": "canceled"})
        )
        with self.assertRaises(ObserverEventConflict) as id_conflict:
            asyncio.run(ingest_observer_events(FakeDB(), [first, same_id_other_content]))
        self.assertEqual(id_conflict.exception.code, "event_id_hash_conflict")

        same_sequence_other_id = normalize_observer_event(_event(event_id="evt-2"))
        with self.assertRaises(ObserverEventConflict) as sequence_conflict:
            asyncio.run(ingest_observer_events(FakeDB(), [first, same_sequence_other_id]))
        self.assertEqual(sequence_conflict.exception.code, "stream_sequence_conflict")

    def test_event_type_schema_and_channel_are_bound(self):
        wrong_channel = _event(event_channel="P1")
        with self.assertRaises(ObserverEventValidationError) as channel_error:
            normalize_observer_event(wrong_channel)
        self.assertEqual(channel_error.exception.code, "event_channel_mismatch")

        wrong_schema = _event(schema_version="account_snapshot.v1")
        with self.assertRaises(ObserverEventValidationError) as schema_error:
            normalize_observer_event(wrong_schema)
        self.assertEqual(schema_error.exception.code, "schema_event_mismatch")

    def test_decision_manifest_requires_qc_only_authority(self):
        payload = {
            "decision_id": "decision-1",
            "decision_kind": "weekly_rebalance",
            "as_of": "2026-07-18T01:02:03Z",
            "strategy_version": "stronger252_moo_band1_paper_v1",
            "basis_account_snapshot_id": "snapshot-1",
            "market_data_cutoff": "2026-07-17T20:00:00Z",
            "ranked_candidates": [],
            "raw_target_weights": {},
            "risk_adjusted_weights": {},
            "execution_style": "moo",
            "execution_authority": "fastapi",
        }
        event = _event(
            schema_version="decision_manifest.v1",
            event_type="decision_manifest",
            payload=payload,
        )
        with self.assertRaises(ObserverEventValidationError) as authority_error:
            normalize_observer_event(event)
        self.assertEqual(
            authority_error.exception.code,
            "invalid_decision_execution_authority",
        )

    def test_qc_and_fastapi_canonical_hash_contracts_match(self):
        module_path = Path(
            "../qc_workspace/Stronger252 MOO Band1 Paper Live/observer_telemetry.py"
        ).resolve()
        spec = importlib.util.spec_from_file_location("qc_observer_hash_contract", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        event = {
            "schema_version": "heartbeat.v1",
            "event_id": "evt-1",
            "event_stream_id": "paper-account:deploy-1",
            "event_channel": "P2",
            "event_sequence": 1,
            "event_time": "2026-07-18T03:00:00Z",
            "account_id": "paper-account",
            "deployment_id": "deploy-1",
            "strategy_id": "stronger252_moo_band1_paper_v1",
            "event_type": "heartbeat",
            "source_fingerprint": "sha256:source",
            "policy_fingerprint": "sha256:policy",
            "payload": {"observer_status": "ok", "spool_status": {}},
        }

        self.assertEqual(canonical_event_hash(event), module.canonical_event_hash(event))

    def test_qc_request_v2_signature_contract_matches_fastapi(self):
        module_path = Path(
            "../qc_workspace/Stronger252 MOO Band1 Paper Live/observer_telemetry.py"
        ).resolve()
        spec = importlib.util.spec_from_file_location("qc_observer_request_contract", module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        body = b'{"events":[{"event_id":"evt-1"}]}'
        headers = module.observer_request_headers(
            "secret",
            body,
            "paper-account",
            "deploy-1",
            timestamp="1784772000",
            nonce="0123456789abcdef",
        )
        signed = (
            b"1784772000\n0123456789abcdef\npaper-account\ndeploy-1\n" + body
        )
        expected = hmac.new(b"secret", signed, hashlib.sha256).hexdigest()

        self.assertEqual(headers["X-QC-Signature"], expected)
        self.assertEqual(headers["X-QC-Account-ID"], "paper-account")
        self.assertEqual(headers["X-QC-Deployment-ID"], "deploy-1")

    def test_qc_startup_reconciliation_uses_order_history_before_unknown(self):
        module_path = Path(
            "../qc_workspace/Stronger252 MOO Band1 Paper Live/observer_telemetry.py"
        ).resolve()
        spec = importlib.util.spec_from_file_location(
            "qc_observer_restart_contract",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        symbol = SimpleNamespace(Value="ABC", ID="sid-abc")
        historical = SimpleNamespace(
            Symbol=symbol,
            Status="Filled",
            QuantityFilled=10,
        )
        algorithm = SimpleNamespace(
            IsWarmingUp=False,
            UtcTime=datetime(2026, 7, 23, 14, 0, tzinfo=timezone.utc),
            Transactions=SimpleNamespace(
                GetOrderById=lambda order_id: historical if order_id == 42 else None
            ),
        )
        telemetry = module.ObserverTelemetry.__new__(module.ObserverTelemetry)
        telemetry.algorithm = algorithm
        telemetry.enabled = True
        telemetry.queues = {channel: [] for channel in module.CHANNELS}
        pending = SimpleNamespace(
            symbol="ABC",
            submitted_quantity=10,
            filled_quantity=3,
            remaining_quantity=7,
        )

        telemetry.record_startup_reconciliation({42: pending}, [])

        self.assertEqual(len(telemetry.queues["P0"]), 1)
        payload = telemetry.queues["P0"][0]["payload"]
        self.assertEqual(payload["status"], "Filled")
        self.assertEqual(payload["fill_quantity"], 10)
        self.assertEqual(payload["remaining_quantity"], 0)
        self.assertEqual(payload["reconstruction_source"], "qc_order_history_lookup")
        self.assertTrue(payload["reconstructed"])

    def test_qc_queue_assigns_no_sequence_before_durable_persist(self):
        module_path = Path(
            "../qc_workspace/Stronger252 MOO Band1 Paper Live/observer_telemetry.py"
        ).resolve()
        spec = importlib.util.spec_from_file_location(
            "qc_observer_sequence_contract",
            module_path,
        )
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        telemetry = module.ObserverTelemetry.__new__(module.ObserverTelemetry)
        telemetry.enabled = True
        telemetry.account_id = "paper"
        telemetry.deployment_id = "deploy"
        telemetry.event_stream_id = "paper:deploy"
        telemetry.strategy_id = "stronger252"
        telemetry.source_fingerprint = "sha256:source"
        telemetry.policy_fingerprint = "sha256:policy"
        telemetry.algorithm = SimpleNamespace(
            IsWarmingUp=False,
            UtcTime=datetime(2026, 7, 23, 14, 0, tzinfo=timezone.utc),
        )
        telemetry.queues = {channel: [] for channel in module.CHANNELS}
        telemetry.states = {
            channel: telemetry._state(channel) for channel in module.CHANNELS
        }
        telemetry.emit("heartbeat", "heartbeat.v1", {}, "P0")

        self.assertNotIn("event_sequence", telemetry.queues["P0"][0])


if __name__ == "__main__":
    unittest.main()
