-- Weekly performance attribution records.

CREATE TABLE IF NOT EXISTS performance_attribution (
    id BIGSERIAL PRIMARY KEY,
    period_key VARCHAR(80) NOT NULL,
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    generated_at TIMESTAMP NOT NULL DEFAULT now(),
    status VARCHAR(40) NOT NULL,
    attribution_method VARCHAR(80) NOT NULL,
    portfolio_return DOUBLE PRECISION,
    arithmetic_portfolio_return DOUBLE PRECISION,
    spy_beta DOUBLE PRECISION,
    spy_beta_contribution DOUBLE PRECISION,
    qqq_beta DOUBLE PRECISION,
    qqq_beta_contribution DOUBLE PRECISION,
    momentum_beta DOUBLE PRECISION,
    momentum_factor_contribution DOUBLE PRECISION,
    intercept_contribution DOUBLE PRECISION,
    residual_alpha_candidate DOUBLE PRECISION,
    r_squared DOUBLE PRECISION,
    sample_count INTEGER NOT NULL DEFAULT 0,
    data_quality VARCHAR(40) NOT NULL,
    benchmark_source VARCHAR(40) NOT NULL,
    source_tickers JSONB,
    diagnostics JSONB,
    raw_payload JSONB,
    content_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_performance_attribution_period_key UNIQUE (period_key)
);

CREATE INDEX IF NOT EXISTS idx_performance_attribution_period_start
    ON performance_attribution (period_start);

CREATE INDEX IF NOT EXISTS idx_performance_attribution_period_end
    ON performance_attribution (period_end);
