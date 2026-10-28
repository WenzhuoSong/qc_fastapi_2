-- v3 P2: pre-registered specs on knowledge entries, and every gate run (trials per family).
ALTER TABLE scout.knowledge ADD COLUMN IF NOT EXISTS spec JSONB;

CREATE TABLE IF NOT EXISTS scout.gate_runs (
    id            BIGSERIAL PRIMARY KEY,
    key           VARCHAR(64)  NOT NULL,          -- knowledge entry tested
    family        VARCHAR(64)  NOT NULL,          -- variants of one idea share a family; trials are counted per family
    spec          JSONB        NOT NULL,          -- the spec as run
    backtest      JSONB,                          -- G1 report (Python)
    critique      JSONB,                          -- G2 checklist (AI, validated)
    passed        BOOLEAN,
    ran_at        TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS gate_runs_family_idx ON scout.gate_runs (family);
