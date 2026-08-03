-- Additive evidence annotations for immutable QC observer facts. Raw events and
-- channel receipts remain untouched so transport sequence auditing stays exact.

CREATE TABLE IF NOT EXISTS qc_observer_event_annotations (
    id BIGSERIAL PRIMARY KEY,
    raw_event_id BIGINT NOT NULL REFERENCES qc_observer_events(id),
    annotation_type VARCHAR(40) NOT NULL,
    reason_code VARCHAR(80) NOT NULL,
    reason TEXT NOT NULL,
    excludes_from_evidence BOOLEAN NOT NULL DEFAULT false,
    annotation_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by VARCHAR(80) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_qc_observer_event_annotations_event_type
        UNIQUE (raw_event_id, annotation_type)
);

CREATE INDEX IF NOT EXISTS idx_qc_observer_event_annotations_raw
    ON qc_observer_event_annotations (raw_event_id);
CREATE INDEX IF NOT EXISTS idx_qc_observer_event_annotations_reason
    ON qc_observer_event_annotations (reason_code, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_qc_observer_event_annotations_exclusion
    ON qc_observer_event_annotations (excludes_from_evidence, created_at DESC);

CREATE OR REPLACE FUNCTION reject_qc_observer_annotation_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'qc_observer_event_annotations is append-only; add a new annotation';
END;
$$;

DROP TRIGGER IF EXISTS trg_qc_observer_event_annotations_append_only
    ON qc_observer_event_annotations;
CREATE TRIGGER trg_qc_observer_event_annotations_append_only
BEFORE UPDATE OR DELETE ON qc_observer_event_annotations
FOR EACH ROW EXECUTE FUNCTION reject_qc_observer_annotation_mutation();

CREATE OR REPLACE VIEW qc_observer_effective_events AS
SELECT event.*
FROM qc_observer_events AS event
WHERE NOT EXISTS (
    SELECT 1
    FROM qc_observer_event_annotations AS annotation
    WHERE annotation.raw_event_id = event.id
      AND annotation.excludes_from_evidence = true
);
