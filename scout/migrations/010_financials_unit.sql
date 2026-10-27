-- Foreign filers report in their own currency (EUR, TWD ...); keep the unit so
-- ratios stay right and valuation never mixes currencies.
ALTER TABLE scout.financials ADD COLUMN IF NOT EXISTS unit VARCHAR(16) NOT NULL DEFAULT 'USD';
