import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from tests.env_setup import ensure_test_settings

ensure_test_settings()

from services.qc_observer_projections import (
    backfill_observer_projections,
    projection_record_for_event,
)


def _raw(event_type, payload, event_id="evt-1"):
    event_time = datetime(2026, 7, 23, 2, 0, tzinfo=timezone.utc)
    envelope = {
        "event_type": event_type,
        "payload": payload,
    }
    return SimpleNamespace(
        id=11,
        event_id=event_id,
        event_stream_id="paper:deploy-1",
        account_id="paper",
        deployment_id="deploy-1",
        strategy_id="stronger252",
        event_sequence=7,
        event_time=event_time,
        event_type=event_type,
        payload=envelope,
    )


class QCObserverProjectionTests(unittest.TestCase):
    def test_decision_projection_preserves_ranking_and_final_target(self):
        record = projection_record_for_event(
            _raw(
                "decision_manifest",
                {
                    "decision_id": "decision-1",
                    "decision_kind": "weekly_rebalance",
                    "as_of": "2026-07-23T02:00:00Z",
                    "basis_account_snapshot_id": "snapshot-1",
                    "market_data_cutoff": "2026-07-22T20:00:00Z",
                    "strategy_version": "v1",
                    "ranked_candidates": [
                        {"security_id": "sid-a", "ticker": "A", "rank": 1, "score": 0.9}
                    ],
                    "raw_target_weights": {"sid-a": 0.3},
                    "risk_adjusted_weights": {"sid-a": 0.25},
                    "execution_style": "moo",
                    "execution_authority": "qc_only",
                },
            )
        )

        self.assertEqual(record["kind"], "decision_manifest")
        self.assertEqual(record["final_target_weights"], {"sid-a": 0.25})
        self.assertEqual(record["ranked_candidates"][0]["security_id"], "sid-a")
        self.assertEqual(record["execution_authority"], "qc_only")

    def test_fill_quantity_semantics_distinguish_callback_from_reconstruction(self):
        base = {
            "broker_order_id": "42",
            "order_intent_id": "intent-1",
            "status": "partially_filled",
            "security_id": "sid-a",
            "ticker": "A",
            "event_time": "2026-07-23T02:00:00Z",
            "fill_quantity": 3,
        }
        callback = projection_record_for_event(_raw("broker_lifecycle_event", base))
        reconstructed = projection_record_for_event(
            _raw(
                "broker_lifecycle_event",
                {**base, "reconstructed": True, "reconstruction_source": "broker_open_orders"},
                event_id="evt-2",
            )
        )

        self.assertEqual(callback["fill_quantity_semantics"], "incremental_callback")
        self.assertEqual(
            reconstructed["fill_quantity_semantics"],
            "cumulative_reconstructed",
        )

    def test_snapshot_projection_keeps_children_and_counts_ledgers(self):
        record = projection_record_for_event(
            _raw(
                "account_snapshot",
                {
                    "account_snapshot_id": "snapshot-1",
                    "equity": 100000,
                    "cash": 25000,
                    "buying_power": 50000,
                    "holdings": [
                        {
                            "security_id": "sid-a",
                            "ticker": "A",
                            "quantity": 10,
                            "average_price": 100,
                            "holdings_value": 1100,
                        }
                    ],
                    "open_orders": [
                        {
                            "broker_order_id": "42",
                            "ticker": "A",
                            "quantity": 10,
                            "quantity_filled": 3,
                        }
                    ],
                    "state_generation": 9,
                    "state_active": True,
                    "broker_reconciled": True,
                    "pending_orders": {"42": {"ticker": "A"}},
                    "deferred_targets": [],
                },
            )
        )

        self.assertEqual(record["pending_order_count"], 1)
        self.assertEqual(record["deferred_target_count"], 0)
        self.assertEqual(record["holdings"][0]["quantity"], 10)
        self.assertEqual(record["open_orders"][0]["quantity_filled"], 3)

    def test_local_risk_projection_accepts_qc_cooldown_session_date(self):
        record = projection_record_for_event(
            _raw(
                "local_risk_event",
                {
                    "risk_event_id": "risk-1",
                    "security_id": "sid-a",
                    "ticker": "A",
                    "risk_type": "atr",
                    "stage": "breach",
                    "event_time": "2026-07-28T19:00:00Z",
                    "price": 90,
                    "entry": 100,
                    "anchor": 110,
                    "cooldown_until": "2026-08-18",
                },
            )
        )

        self.assertEqual(
            record["cooldown_until"],
            datetime(2026, 8, 18, tzinfo=timezone.utc),
        )

    def test_heartbeat_has_no_business_projection(self):
        self.assertIsNone(
            projection_record_for_event(
                _raw("heartbeat", {"observer_status": "ok", "spool_status": {}})
            )
        )


class QCObserverProjectionBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_backfill_drains_full_batches_until_short_batch(self):
        batches = [
            {"scanned": 2, "projected": 2, "skipped": 0, "checkpoint_raw_event_id": 2},
            {"scanned": 1, "projected": 1, "skipped": 0, "checkpoint_raw_event_id": 3},
        ]
        with patch(
            "services.qc_observer_projections.run_observer_projection_batch",
            new=AsyncMock(side_effect=batches),
        ) as run:
            result = await backfill_observer_projections(limit=2)

        self.assertEqual(result["batches"], 2)
        self.assertEqual(result["projected"], 3)
        self.assertEqual(result["checkpoint_raw_event_id"], 3)
        self.assertEqual(run.await_count, 2)


if __name__ == "__main__":
    unittest.main()
