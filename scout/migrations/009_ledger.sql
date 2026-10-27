-- v2 phase 2: every judgment the system makes, and how it turned out.
CREATE TABLE IF NOT EXISTS scout.judgments (
    id            BIGSERIAL PRIMARY KEY,
    kind          VARCHAR(24) NOT NULL,          -- insight | insight_lens | opportunity | sector_heat
    ticker        VARCHAR(16) NOT NULL,
    asof          DATE        NOT NULL,          -- data date the judgment was made on
    lens          VARCHAR(32) NOT NULL DEFAULT '',
    stance        SMALLINT    NOT NULL,          -- +1 bullish, 0 neutral, −1 bearish
    entry_price   DOUBLE PRECISION,
    sector_etf    VARCHAR(16),
    source_ref    TEXT,
    model         VARCHAR(48),
    payload       JSONB,
    human_rating  SMALLINT,                      -- +1 reasonable, −1 not (monthly spot check)
    human_note    TEXT,
    made_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kind, ticker, asof, lens)
);
CREATE INDEX IF NOT EXISTS judgments_ticker_idx ON scout.judgments (ticker, asof DESC);

CREATE TABLE IF NOT EXISTS scout.judgment_outcomes (
    judgment_id    BIGINT  NOT NULL REFERENCES scout.judgments(id) ON DELETE CASCADE,
    horizon        INTEGER NOT NULL,             -- trading sessions after asof
    end_date       DATE    NOT NULL,
    ret            DOUBLE PRECISION,
    spy_ret        DOUBLE PRECISION,
    sector_ret     DOUBLE PRECISION,
    excess_spy     DOUBLE PRECISION,
    excess_sector  DOUBLE PRECISION,
    hit            BOOLEAN,
    evaluated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (judgment_id, horizon)
);
