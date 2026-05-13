-- Add per-ticker price and one-day return fields for strategy replay metrics.
-- Execute manually before deploying QC schema_version 1.1 heartbeat payloads.

ALTER TABLE holdings_factors
ADD COLUMN IF NOT EXISTS price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS close_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS daily_return_pct NUMERIC(8,6);
