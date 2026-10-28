-- A conditional spec (`when`: market regime / volatility / sectors) keeps its rebalance cadence;
-- picks outside its contexts are stored but not counted. The pool (the control) always counts.
ALTER TABLE scout.forward_ranks ADD COLUMN IF NOT EXISTS counted BOOLEAN NOT NULL DEFAULT TRUE;
