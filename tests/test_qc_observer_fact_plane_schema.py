import os
import unittest
from pathlib import Path

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

from db.models import (
    QCAccountSnapshotHoldingProjection,
    QCAccountSnapshotOpenOrderProjection,
    QCAccountSnapshotProjection,
    QCDecisionManifestProjection,
    QCDecisionRankingProjection,
    QCLocalRiskProjection,
    QCObserverEvent,
    QCObserverEventAnnotation,
    QCObserverRejectedEvent,
    QCObserverRequestNonce,
    QCObserverStreamReceipt,
    QCOrderIntentProjection,
    QCOrderLifecycleProjection,
    QCProjectionCheckpoint,
)


class QCObserverFactPlaneSchemaTests(unittest.TestCase):
    def setUp(self):
        self.sql = Path(
            "db/migrations/20260718_create_qc_observer_fact_plane.sql"
        ).read_text()
        self.annotation_sql = Path(
            "db/migrations/20260718_create_qc_observer_event_annotations.sql"
        ).read_text()
        self.hardening_sql = Path(
            "db/migrations/20260723_harden_qc_observer_request_contract.sql"
        ).read_text()
        self.projection_sql = Path(
            "db/migrations/20260723_create_qc_observer_projections.sql"
        ).read_text()

    def test_orm_registers_minimum_observer_fact_plane(self):
        self.assertEqual(QCObserverEvent.__tablename__, "qc_observer_events")
        self.assertEqual(
            QCObserverEventAnnotation.__tablename__,
            "qc_observer_event_annotations",
        )
        self.assertEqual(
            QCObserverStreamReceipt.__tablename__, "qc_observer_stream_receipts"
        )
        self.assertEqual(QCProjectionCheckpoint.__tablename__, "qc_projection_checkpoints")

    def test_raw_event_has_both_idempotency_and_sequence_constraints(self):
        constraints = {item.name for item in QCObserverEvent.__table__.constraints}

        self.assertIn("uq_qc_observer_events_event_id", constraints)
        self.assertIn(
            "uq_qc_observer_events_stream_channel_sequence",
            constraints,
        )
        self.assertIn("event_channel", QCObserverEvent.__table__.columns)
        self.assertIn("content_hash", QCObserverEvent.__table__.columns)

    def test_migration_enforces_append_only_raw_events(self):
        self.assertIn("BEFORE UPDATE OR DELETE ON qc_observer_events", self.sql)
        self.assertIn("write a correction event instead", self.sql)

    def test_annotation_model_preserves_raw_fact_and_marks_evidence_exclusion(self):
        columns = QCObserverEventAnnotation.__table__.columns

        self.assertIn("raw_event_id", columns)
        self.assertIn("annotation_type", columns)
        self.assertIn("excludes_from_evidence", columns)

    def test_effective_event_view_excludes_annotated_artifacts(self):
        self.assertIn(
            "CREATE OR REPLACE VIEW qc_observer_effective_events",
            self.annotation_sql,
        )
        self.assertIn("annotation.excludes_from_evidence = true", self.annotation_sql)

    def test_migration_is_additive_to_existing_news_pipeline(self):
        normalized = self.sql.lower()
        for existing_table in (
            "ticker_news_library",
            "news_ticker_policies",
            "news_fetch_ledger",
        ):
            self.assertNotIn(f"alter table {existing_table}", normalized)
            self.assertNotIn(f"drop table {existing_table}", normalized)

    def test_request_replay_and_event_quarantine_are_additive(self):
        self.assertEqual(QCObserverRequestNonce.__tablename__, "qc_observer_request_nonces")
        self.assertEqual(QCObserverRejectedEvent.__tablename__, "qc_observer_rejected_events")
        self.assertIn("CREATE TABLE IF NOT EXISTS qc_observer_request_nonces", self.hardening_sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS qc_observer_rejected_events", self.hardening_sql)

    def test_business_projection_tables_are_registered_and_raw_linked(self):
        models = (
            QCDecisionManifestProjection,
            QCDecisionRankingProjection,
            QCOrderIntentProjection,
            QCOrderLifecycleProjection,
            QCLocalRiskProjection,
            QCAccountSnapshotProjection,
            QCAccountSnapshotHoldingProjection,
            QCAccountSnapshotOpenOrderProjection,
        )
        for model in models:
            with self.subTest(table=model.__tablename__):
                self.assertIn("raw_event_id", model.__table__.columns)
                self.assertIn(
                    f"CREATE TABLE IF NOT EXISTS {model.__tablename__}",
                    self.projection_sql,
                )

    def test_fill_projection_requires_explicit_quantity_semantics(self):
        columns = QCOrderLifecycleProjection.__table__.columns
        self.assertIn("fill_quantity", columns)
        self.assertIn("fill_quantity_semantics", columns)


if __name__ == "__main__":
    unittest.main()
