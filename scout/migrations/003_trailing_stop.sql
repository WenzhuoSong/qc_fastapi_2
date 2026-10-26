-- Trailing exits: the stop in force after the latest bar, updated by the tracker.
ALTER TABLE scout.opportunities ADD COLUMN IF NOT EXISTS current_stop DOUBLE PRECISION;
