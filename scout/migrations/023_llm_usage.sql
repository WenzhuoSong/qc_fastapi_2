-- One row per LLM call: which job and purpose, which model, tokens, estimated cost (list prices in
-- scout/llm_usage.py). Written by Scout and, with plain SQL, by the news pipeline.
CREATE TABLE IF NOT EXISTS scout.llm_usage (
    id             BIGSERIAL PRIMARY KEY,
    at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    job            VARCHAR(48) NOT NULL,           -- eod, premarket, weekly, web, telegram, news, a local job name …
    purpose        VARCHAR(48) NOT NULL,           -- insight, thesis, agent, event_cause, news_summary …
    model          VARCHAR(64) NOT NULL,
    input_tokens   INTEGER     NOT NULL DEFAULT 0,
    cached_tokens  INTEGER     NOT NULL DEFAULT 0,
    output_tokens  INTEGER     NOT NULL DEFAULT 0,
    est_usd        DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS llm_usage_at_idx ON scout.llm_usage (at);
