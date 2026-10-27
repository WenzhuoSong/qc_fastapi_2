-- Price map per ticker and data date (computed after the close, never during the
-- session); every version is kept. A recorded buy locks the plan it was made from.
CREATE TABLE IF NOT EXISTS scout.price_levels (
    ticker      VARCHAR(16) NOT NULL,
    asof        DATE        NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ticker, asof)
);

ALTER TABLE scout.trades ADD COLUMN IF NOT EXISTS plan JSONB;
