-- v3 P3: forward record (G3). Each signal of a gated rule, and its selection-control legs
-- (random other stocks, same date, same level formula), stored on the signal day and
-- resolved with the same simulator as the replay. Nothing is recomputed afterwards.
CREATE TABLE IF NOT EXISTS scout.forward_trades (
    id            BIGSERIAL PRIMARY KEY,
    key           VARCHAR(64)  NOT NULL,          -- knowledge entry under forward test
    detector      VARCHAR(48)  NOT NULL,
    role          VARCHAR(8)   NOT NULL,          -- signal | select
    ticker        VARCHAR(16)  NOT NULL,          -- the signal's stock (for control legs too)
    leg_ticker    VARCHAR(16)  NOT NULL,          -- the stock actually traded (= ticker for the signal)
    draw          SMALLINT     NOT NULL DEFAULT 0, -- control order: the first DRAWS legs that fill count
    asof          DATE         NOT NULL,          -- signal date
    entry_lo      DOUBLE PRECISION NOT NULL,
    entry_hi      DOUBLE PRECISION NOT NULL,
    stop          DOUBLE PRECISION NOT NULL,
    target1       DOUBLE PRECISION NOT NULL,
    target2       DOUBLE PRECISION,
    state         VARCHAR(16)  NOT NULL DEFAULT 'pending',   -- simulate states
    entry_date    DATE,
    entry_price   DOUBLE PRECISION,
    exit_date     DATE,
    exit_price    DOUBLE PRECISION,
    ret           DOUBLE PRECISION,               -- gross, as in the replay
    spy_ret       DOUBLE PRECISION,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (key, role, ticker, asof, draw)
);
CREATE INDEX IF NOT EXISTS forward_trades_open_idx ON scout.forward_trades (state) WHERE state IN ('pending', 'open');
CREATE INDEX IF NOT EXISTS forward_trades_key_idx ON scout.forward_trades (key, asof);
