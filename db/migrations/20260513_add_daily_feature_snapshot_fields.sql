-- Adds structured columns for QC daily_feature_snapshot packets.
-- raw_payload in qc_snapshots remains the canonical full-fidelity archive.

ALTER TABLE holdings_factors
ADD COLUMN IF NOT EXISTS universe_role VARCHAR(20),
ADD COLUMN IF NOT EXISTS open_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS high_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS low_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS volume BIGINT,
ADD COLUMN IF NOT EXISTS dollar_volume NUMERIC(20,2),
ADD COLUMN IF NOT EXISTS return_5d NUMERIC(8,6),
ADD COLUMN IF NOT EXISTS sma_20 NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS sma_50 NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS sma_200 NUMERIC(15,4);

ALTER TABLE qc_snapshots
ALTER COLUMN packet_type TYPE VARCHAR(40);
