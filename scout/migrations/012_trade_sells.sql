-- Sells against a recorded buy (partial or full). The trade closes when nothing remains;
-- its exit price is then the share-weighted average of these rows.
CREATE TABLE IF NOT EXISTS scout.trade_sells (
    id          BIGSERIAL PRIMARY KEY,
    trade_id    BIGINT      NOT NULL REFERENCES scout.trades(id) ON DELETE CASCADE,
    sold_on     DATE        NOT NULL,
    price       DOUBLE PRECISION NOT NULL,
    shares      DOUBLE PRECISION NOT NULL,
    note        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS trade_sells_trade_idx ON scout.trade_sells (trade_id);
