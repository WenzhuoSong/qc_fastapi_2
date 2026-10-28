-- v3 P3 for ranking strategies (gate `spec.type: rank`): on each rebalance day of the forward
-- record, the ranker's picks and every other eligible stock of that day (the selection control),
-- held from the next open to the close `hold` sessions later, as in the backtest. Returns are
-- computed once, from one price series, when the holding period is over; nothing is recomputed.
CREATE TABLE IF NOT EXISTS scout.forward_ranks (
    id          BIGSERIAL PRIMARY KEY,
    key         VARCHAR(64)  NOT NULL,          -- knowledge entry under forward test
    asof        DATE         NOT NULL,          -- rebalance date (ranked after this close)
    ticker      VARCHAR(16)  NOT NULL,
    role        VARCHAR(8)   NOT NULL,          -- pick | pool
    hold        SMALLINT     NOT NULL,
    entry_date  DATE,
    exit_date   DATE,
    ret         DOUBLE PRECISION,               -- gross, next open → close after `hold` sessions
    spy_ret     DOUBLE PRECISION,
    state       VARCHAR(8)   NOT NULL DEFAULT 'pending',   -- pending | closed | missing
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (key, asof, ticker)
);
CREATE INDEX IF NOT EXISTS forward_ranks_pending_idx ON scout.forward_ranks (key) WHERE state = 'pending';
