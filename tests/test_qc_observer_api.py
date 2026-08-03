import hashlib
import hmac
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

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

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.observer import router
from config import get_settings
from db.session import get_db
from services.qc_observer_ingestion import (
    ObserverEventConflict,
    canonical_event_hash,
)


class _FakeDB:
    def __init__(self):
        self.commit = AsyncMock()
        self.rollback = AsyncMock()
        self.flush = AsyncMock()
        self.added = []

    def add(self, value):
        self.added.append(value)


def _body() -> bytes:
    event = {
        "schema_version": "decision_manifest.v1",
        "event_id": "evt-api-1",
        "event_stream_id": "stream-api-1",
        "event_channel": "P0",
        "event_sequence": 1,
        "event_time": "2026-07-18T02:00:00Z",
        "account_id": "paper-account",
        "deployment_id": "deploy-api-1",
        "strategy_id": "stronger252",
        "source_fingerprint": "sha256:source",
        "policy_fingerprint": "sha256:policy",
        "event_type": "decision_manifest",
        "payload": {
            "decision_id": "decision-1",
            "decision_kind": "weekly_rebalance",
            "as_of": "2026-07-18T02:00:00Z",
            "strategy_version": "stronger252_moo_band1_paper_v1",
            "basis_account_snapshot_id": "snapshot-1",
            "market_data_cutoff": "2026-07-17T20:00:00Z",
            "ranked_candidates": [],
            "raw_target_weights": {},
            "risk_adjusted_weights": {},
            "execution_style": "moo",
            "execution_authority": "qc_only",
        },
    }
    event["content_hash"] = canonical_event_hash(event)
    return json.dumps({"events": [event]}, separators=(",", ":")).encode()


def _signature(body: bytes) -> str:
    return hmac.new(b"test-only", body, hashlib.sha256).hexdigest()


class QCObserverAPITests(unittest.TestCase):
    def setUp(self):
        self._webhook_secret = os.environ.get("WEBHOOK_SECRET")
        self._qc_webhook_secret = os.environ.get("QC_WEBHOOK_SECRET")
        os.environ["WEBHOOK_SECRET"] = "test-only"
        os.environ.pop("QC_WEBHOOK_SECRET", None)
        get_settings.cache_clear()
        self.db = _FakeDB()
        app = FastAPI()
        app.include_router(router, prefix="/api")

        async def _db_override():
            yield self.db

        app.dependency_overrides[get_db] = _db_override
        self.client = TestClient(app)
        self.projection_patch = patch(
            "api.observer.run_observer_projection_batch",
            new=AsyncMock(return_value={"projected": 0}),
        )
        self.projection_patch.start()

    def tearDown(self):
        self.projection_patch.stop()
        if self._webhook_secret is None:
            os.environ.pop("WEBHOOK_SECRET", None)
        else:
            os.environ["WEBHOOK_SECRET"] = self._webhook_secret
        if self._qc_webhook_secret is None:
            os.environ.pop("QC_WEBHOOK_SECRET", None)
        else:
            os.environ["QC_WEBHOOK_SECRET"] = self._qc_webhook_secret
        get_settings.cache_clear()

    def test_unsigned_request_is_rejected_before_ingestion(self):
        with patch("api.observer.ingest_observer_events", new=AsyncMock()) as ingest:
            response = self.client.post(
                "/api/observer/events",
                content=_body(),
                headers={"Content-Type": "application/json"},
            )

        self.assertEqual(response.status_code, 401)
        ingest.assert_not_awaited()
        self.db.commit.assert_not_awaited()

    def test_projection_status_is_read_only(self):
        status = {
            "schema_version": "qc_observer_projection_status.v1",
            "read_only": True,
            "pending_parent_count": 0,
        }
        with patch(
            "api.observer.load_observer_projection_status",
            new=AsyncMock(return_value=status),
        ) as load:
            response = self.client.get("/api/observer/projections/status")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), status)
        load.assert_awaited_once_with(self.db)
        self.db.commit.assert_not_awaited()

    def test_valid_batch_commits_before_returning_channel_ack(self):
        ack = {
            "status": "accepted",
            "accepted_event_ids": ["evt-api-1"],
            "duplicate_event_ids": [],
            "channels": [
                {
                    "event_stream_id": "stream-api-1",
                    "event_channel": "P0",
                    "highest_contiguous_sequence": 1,
                    "highest_seen_sequence": 1,
                    "missing_sequences": [],
                }
            ],
        }
        body = _body()
        with patch(
            "api.observer.ingest_observer_events",
            new=AsyncMock(return_value=ack),
        ) as ingest:
            response = self.client.post(
                "/api/observer/events",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-QC-Signature": _signature(body),
                },
            )

        self.assertEqual(response.status_code, 200)
        expected = {
            **ack,
            "auth_version": "v1_legacy_transition",
            "rejected_events": [],
        }
        self.assertEqual(response.json(), expected)
        ingest.assert_awaited_once()
        self.db.commit.assert_awaited_once()
        self.db.rollback.assert_not_awaited()

    def test_conflict_rolls_back_and_returns_stable_reason(self):
        body = _body()
        conflict = ObserverEventConflict("event_id_hash_conflict", "conflicting event")
        with patch(
            "api.observer.ingest_observer_events",
            new=AsyncMock(side_effect=conflict),
        ):
            response = self.client.post(
                "/api/observer/events",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-QC-Signature": _signature(body),
                },
            )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"]["code"], "event_id_hash_conflict")
        self.db.rollback.assert_awaited_once()
        self.db.commit.assert_not_awaited()

    def test_mixed_batch_commits_valid_siblings_and_quarantines_invalid(self):
        body_payload = json.loads(_body())
        invalid = dict(body_payload["events"][0])
        invalid["event_id"] = "evt-api-invalid"
        invalid["event_sequence"] = 2
        invalid["payload"] = dict(invalid["payload"])
        invalid["payload"].pop("decision_id")
        invalid["content_hash"] = canonical_event_hash(invalid)
        body_payload["events"].append(invalid)
        body = json.dumps(body_payload, separators=(",", ":")).encode()
        ack = {
            "status": "accepted",
            "accepted_event_ids": ["evt-api-1"],
            "duplicate_event_ids": [],
            "channels": [],
        }
        quarantine_ack = [{
            "event_stream_id": "stream-api-1",
            "event_channel": "P0",
            "highest_contiguous_sequence": 2,
            "highest_seen_sequence": 2,
            "missing_sequences": [],
        }]
        with (
            patch(
                "api.observer.ingest_observer_events",
                new=AsyncMock(return_value=ack),
            ) as ingest,
            patch(
                "api.observer.quarantine_rejected_observer_events",
                new=AsyncMock(return_value=quarantine_ack),
            ) as quarantine,
        ):
            response = self.client.post(
                "/api/observer/events",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-QC-Signature": _signature(body),
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "accepted_with_rejections")
        self.assertEqual(
            response.json()["rejected_events"],
            [{"event_index": 1, "event_id": "evt-api-invalid", "code": "payload_missing_required_fields"}],
        )
        ingest.assert_awaited_once()
        quarantine.assert_awaited_once()
        self.db.commit.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
