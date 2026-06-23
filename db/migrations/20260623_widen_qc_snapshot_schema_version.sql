-- Widen QC webhook envelope schema version for named contracts such as
-- "newbase_live_snapshot_v1". Existing numeric versions ("1.6") remain valid.
ALTER TABLE qc_snapshots
    ALTER COLUMN schema_version TYPE varchar(80);
