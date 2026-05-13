-- Add structured decision context storage for daily memory.
-- Execute manually before running cron.daily_analyst after deploying the ORM change.

ALTER TABLE memory_daily
ADD COLUMN IF NOT EXISTS decision JSONB;
