-- Conditional base rates (pre-registered in scout/knowledge/external_specs/conditional_rates_v1.yaml): each rule's
-- edge over the same-day selection control per context cell, raw and shrunk (empirical Bayes), one row per cell per run.
CREATE TABLE IF NOT EXISTS scout.conditional_rates (
    run_on        DATE        NOT NULL,
    detector      VARCHAR(48) NOT NULL,
    layer         VARCHAR(16) NOT NULL,          -- sector_regime | sector | regime | vol | all
    sector        VARCHAR(48) NOT NULL DEFAULT '',
    regime        VARCHAR(16) NOT NULL DEFAULT '',
    vol           VARCHAR(8)  NOT NULL DEFAULT '',
    n             INTEGER     NOT NULL,
    months        INTEGER     NOT NULL,
    raw_edge      DOUBLE PRECISION,
    se            DOUBLE PRECISION,
    shrunk_edge   DOUBLE PRECISION,
    lo90          DOUBLE PRECISION,
    hi90          DOUBLE PRECISION,
    first_edge    DOUBLE PRECISION,
    second_edge   DOUBLE PRECISION,
    win_rate      DOUBLE PRECISION,              -- the rule's trades net of cost above 0
    control_win   DOUBLE PRECISION,              -- the same-day controls above 0
    candidate     BOOLEAN     NOT NULL DEFAULT false,
    PRIMARY KEY (run_on, detector, layer, sector, regime, vol)
);
