-- Price maps are kept per rule version: a rule change adds rows instead of overwriting the old ones.
ALTER TABLE scout.price_levels ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'price_levels_pkey'
               AND pg_get_constraintdef(oid) = 'PRIMARY KEY (ticker, asof)') THEN
        ALTER TABLE scout.price_levels DROP CONSTRAINT price_levels_pkey;
        ALTER TABLE scout.price_levels ADD CONSTRAINT price_levels_pkey PRIMARY KEY (ticker, asof, version);
    END IF;
END $$;
