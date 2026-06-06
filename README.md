# QC FastAPI 2 — QC Agentic Trading System

An autonomous ETF trading system that integrates with QuantConnect.
The pipeline is a **Python → LLM → Python 三段接力**: pure math computes a
baseline, a Bull/Bear structured debate layer argues for and against, a
CIO synthesizer arbitrates, and a Python risk officer applies overlays +
hard checks before execution.

## Architecture

```
qc_fastapi_2/
├── agents/          # Pipeline agents
│   ├── researcher.py       # Stage 3: LLM info synthesis → research_report
│   ├── bull_researcher.py  # Stage 4a: Bull arguments (parallel)
│   ├── bear_researcher.py  # Stage 4b: Bear arguments (parallel)
│   ├── synthesizer.py      # Stage 5: CIO arbitration → adjusted_weights
│   ├── risk_manager.py     # Stage 6: overlays + 6 checks → target weights
│   ├── communicator.py     # LLM Telegram card + Python fallback
│   ├── executor.py         # Deterministic execution (3 gates)
│   └── reporter.py         # Daily report
├── services/        # Async orchestration
│   ├── pipeline.py         # run_full_pipeline (8-stage relay)
│   ├── market_brief.py     # Stage 1: snapshot + news → brief
│   ├── quant_baseline.py   # Stage 2: pure math → base_weights
│   ├── playground.py       # Research-only multi-strategy sandbox
│   ├── transmission.py     # Macro event → sector pattern library
│   ├── finnhub_client.py   # Finnhub REST + credibility scoring
│   ├── alphavantage_client.py # Alpha Vantage News Sentiment API
│   ├── rss_fetcher.py      # RSS feed fetcher (MarketWatch/CNBC/Yahoo/Reuters)
│   ├── news_summarizer.py  # gpt-4o-mini batch news summarizer
│   ├── proposal.py         # SEMI_AUTO proposal lifecycle
│   └── telegram_commands.py# /confirm /skip /pause /status
├── strategies/      # Pluggable scoring strategies (registry)
│   ├── base.py             # Strategy ABC + ScoredTicker
│   ├── momentum_lite.py    # MomentumLiteV1 (default)
│   └── defensive_adjust.py # Defense matrix + rebalance helpers
├── cron/            # Standalone cron entry scripts
│   ├── pre_fetch_news.py   # Multi-source news → DB (Finnhub + AV + RSS)
│   ├── hourly_analysis.py  # Main agent pipeline
│   ├── pending_check.py    # SEMI_AUTO timeout handler
│   ├── playground_analysis.py # Research-only strategy comparison
│   ├── morning_health.py   # Pre-open health check
│   └── post_market_report.py # Daily summary
├── api/             # FastAPI endpoints (webhook, command, status, telegram)
├── db/              # Models, session, queries, seed
├── tools/           # DB / QC / Notify tool implementations
├── constants.py     # ETF_UNIVERSE + style buckets
├── config.py        # Pydantic Settings
└── main.py          # FastAPI app (webhook-only, no in-process scheduler)
```

The entire stack is async-only. The web service (`main.py`) only serves
webhooks; all scheduled work runs as separate Railway cron services, each
in its own Python process with its own `asyncio.run()`. This eliminates
asyncpg cross-event-loop issues by giving every job a fresh event loop.

## QC Fallback Policy Sync Check

Before merging or deploying any `services/execution_policy.py` change, verify
that the QuantConnect fallback policy file is still synchronized:

```bash
python tools/generate_qc_fallback_policy.py --check ../quantconnect_files/test1.py
```

To print the deterministic snippet derived from FastAPI:

```bash
python tools/generate_qc_fallback_policy.py --print
```

The checker is read-only. If it reports drift, update the QC fallback snippet,
keep the QC file below the QuantConnect size limit, deploy QC, then verify the
heartbeat `policy_version` matches `services.execution_policy.POLICY_VERSION`.

## Pipeline: 10-Stage Relay

```
Stage 0   guard_and_config     Python    config / pause / lock
Stage 1   market_brief         Python    snapshot + news → brief (no weights)
Stage 2   quant_baseline       Python    pure-math scoring → base_weights
Stage 2c  playground           Python    multi-strategy comparison bundle (advisory)
Stage 3   RESEARCHER           LLM       base + brief → research_report (info synthesis only)
Stage 4a  BULL RESEARCHER      LLM       research_report → bull arguments (parallel)
Stage 4b  BEAR RESEARCHER      LLM       research_report → bear arguments (parallel)
Stage 5   SYNTHESIZER          LLM       Bull/Bear arbitration → adjusted_weights
Stage 6   RISK MGR             Python    transmission → defensive → hard_risk → 6 checks
Stage 6.5 POSITION MANAGER     Python    quantity/frequency controls
Stage 7   save_analysis        Python    INSERT INTO agent_analysis (4 cols)
Stage 8   COMMUNICATOR         LLM+fb    Telegram card (5s timeout → Python fallback)
Stage 9   branch               Python    rejected / SEMI_AUTO pending / FULL_AUTO execute
```

**The baton is always weights:**

```
 base_weights       research_report      bull/bear_output     adjusted_weights      target_weights
 (Stage 2 Python) → (Stage 3 LLM)     → (Stage 4a/4b LLM) → (Stage 5 LLM)      → (Stage 6 Python) → QC
   量化研究员          信息合成              多空辩论              首席投资官仲裁          首席风控官
```

LLM calls per cycle: **4** — RESEARCHER (info synthesis) + BULL/BEAR
(parallel debate, counted as 2) + SYNTHESIZER (arbitration). All on the
correctness path with degraded fallbacks. COMMUNICATOR (5s timeout →
Python f-string fallback; not on the correctness path) adds 1 more.

### Stage-by-stage responsibilities

**Stage 1 — `market_brief`** (Python)
Reads latest `QCSnapshot` + `MacroNewsCache` (1 row) + `TickerNewsLibrary`
(48h window), computes `key_facts` (breadth, SPY mom, avg ATR,
risk_on_score, drawdown, top5/bottom5 momentum), enriches heartbeat rows with
the latest `daily_feature_snapshot`, computes `sector_rotation`, and builds a
prose summary. Rotation output includes `rotation_label`, leaders/laggards,
sector/factor ranks, and `risk_appetite_score`.
Output: `brief` dict. No weights.

**Stage 2 — `quant_baseline`** (Python)
Instantiates the active strategy from `strategies/`, calls
`strategy.score(holdings, NEUTRAL_CTX)` → `strategy.optimize(...)`. The
context is deliberately neutral — regime judgment happens downstream.
Output: `base_weights` + `scoring_breakdown` + `ranking_summary`. The
baseline is "the Python quant researcher's best guess if it only saw the
numbers."

**Stage 2c — `playground`** (Python, advisory only)
Runs traditional strategy variants (`momentum_lite_v1`,
`mean_reversion_lite`, `low_vol_factor`) and builds a comparison bundle:
strategy weights, largest divergences, consensus weights, turnover, Sharpe,
IC, and hit-rate when QC snapshots provide per-ticker `daily_return_pct`.
Replay prefers `daily_feature_snapshot` over heartbeat for richer features.
This bundle is injected into the Synthesizer prompt as
research context only; it does not bypass Risk Manager, Position Manager, or
execution gates.

**Stage 3 — `RESEARCHER`** (LLM, heavy model)
The chief market analyst. **Only analyzes, does not decide weights.**
Synthesizes quant factors + news + macro + calendar into a structured
`research_report` for the Bull/Bear debate layer. Output:

- `market_regime` — regime (6-enum) + confidence + evidence
- `macro_outlook` — summary + key_events + impact_bias
- `ticker_signals` — per-ticker quant_score + news_sentiment + combined_signal
  (strong_positive / positive / neutral / negative / strong_negative)
- `cross_signal_insights` — cross-ticker pattern observations

3 retries. Degraded fallback generates quant-only report (no news synthesis).

**Stage 4a/4b — `BULL/BEAR RESEARCHERS`** (LLM, gpt-4o, parallel)
Two adversarial analysts running via `asyncio.gather`:

- **Bull** (4a): argues maintain/increase. Finds all positive signals,
  explains why risks are manageable. Output: stance + arguments +
  ticker_views (overweight/hold) + suggested_weights + risk_acknowledgments.
- **Bear** (4b): argues reduce/defensive. Finds all risk signals, explains
  why positive signals are unreliable. Output: stance + arguments +
  ticker_views (underweight/trim/avoid) + suggested_weights + bullish_rebuttals.

Each has 2 retries. Degraded fallbacks: Bull echoes base_weights; Bear
increases CASH to 30%.

**Stage 5 — `SYNTHESIZER`** (LLM, heavy model)
The CIO / arbitrator. Weighs Bull vs Bear evidence quality, identifies
consensus and divergence points, produces final `adjusted_weights`.
**Output is interface-compatible with old researcher_out** — Risk MGR
needs no changes. Uses 5-level stance: buy / overweight / maintain /
underweight / sell. Auto-detects uncertainty when |bull_conf - bear_conf|
< 0.15 → sets `uncertainty_flag=True`. Includes `debate_summary` for
Communicator. 3 retries. Degraded fallback echoes base_weights.

**Stage 6 — `RISK MGR`** (Python)
The CRO. Not just a gatekeeper — applies deterministic corrections before
checks. Overlay chain in order:

1. **transmission_tilt** — `match_event_to_pattern(key_events)` scans 6
   canonical macro patterns (supply_shock_oil / war_geopolitical /
   rate_shock_hawkish / risk_off_credit_stress / recession_demand_collapse
   / fed_dovish_easing). On match, applies sector-level tilt vector with
   `(1 + 0.5·strength)` multipliers.
2. **defensive_adjust** — triggered when
   `regime ∈ {bear_weak, bear_trend, high_vol}` or
   `override_mode == "DEFENSIVE"`. Scales equity weights down per the
   defense matrix, `uncertainty_flag` adds +10% bonus.
3. **hard_risk_filter** — consumes `brief.hard_risks_map`. Tickers with
   earnings_soon / FDA / halt / acquisition / lawsuit flags are zeroed
   (if not currently held) and freed weight goes to CASH.

Then 6 hard checks on the final `target_weights`:

| Check | Threshold (default) |
|---|---|
| `vol_ok` | position-weighted `hist_vol_20d` < 35% |
| `drawdown_ok` | current drawdown < 15% |
| `position_ok` | max single position ≤ 20% |
| `broad_market_ok` | SPY+QQQ+IWM ≤ 40% |
| `cash_ok` | CASH ≥ 5% |
| `cost_ok` | estimated cost ≤ 0.5% |

Pass → issue one-time 5-min UUID approval token. Fail → return
`rejection_reasons` with specific per-check actuals.

**Stage 9 — `EXECUTOR`** (Python, FULL_AUTO only)
Three gates: `risk_out.approved` → `verify_approval_token` (one-shot
consume) → weight sum sanity. On pass, HMAC-SHA256 POST to
`{QC_API_URL}/projects/{PROJECT_ID}/live/commands` with target weights.

## News Layer (Cron 1, decoupled)

Two independent tables feed Stage 1:

- **`TickerNewsLibrary`** — Multi-source per-ticker news with gpt-4o-mini
  summary, sentiment, relevance, hard_risks flags, source credibility
  (Bloomberg/Reuters=100 down to 30 default). 48h rolling TTL, dedup by
  `(ticker, url)`. Each row tagged with `source_api` (finnhub/alphavantage/rss).
- **`MacroNewsCache`** — single-row cache of macro headlines + economic
  calendar + pre-stitched Chinese prose.

Both are maintained by `cron/pre_fetch_news.py` every 2h via a multi-phase
pipeline, completely independent from the main pipeline:

- **Phase A**: Finnhub — macro news + economic calendar + per-ticker news
- **Phase B**: Alpha Vantage — bulk ticker news with built-in sentiment
  (skips LLM summarization when sentiment is pre-populated)
- **Phase C**: RSS feeds — MarketWatch, CNBC, Yahoo Finance, Reuters;
  keyword-matched to ETF universe (17 tickers × keyword list)

Each phase is independently fault-tolerant. Any phase failing does not
affect the others or the main pipeline.

## Cron Jobs

**Standalone processes**, each `python -m cron.<name>` with its own
`asyncio.run()`. Configure as Railway cron services:

| Entry | Schedule (ET) | Purpose |
|---|---|---|
| `python -m cron.pre_fetch_news` | 09:50 / 11:50 / 13:50 | Multi-source news → DB (Finnhub + AV + RSS) |
| `python -m cron.hourly_analysis` | 10:00–15:00 hourly | Full 10-stage pipeline |
| `python -m cron.pending_check` | every 1 min | SEMI_AUTO timeout handler |
| `python -m cron.playground_analysis` | after close | Research-only multi-strategy comparison |
| `python -m cron.yfinance_backfill` | after close | Research/backfill OHLCV feature store, no execution authority |
| `python -m cron.daily_signal_freeze` | after playground/yfinance | Observe-only EvidenceCard signal ledger; no execution authority |
| `python -m cron.daily_signal_validation_refresh` | after signal freeze | Label mature signal outcomes and refresh conviction profiles; no execution authority |
| `python -m cron.validation_observation_refresh` | hourly or after yfinance | Backfill hedge/basket/execution validation observations and mature T+5 outcomes; no execution authority |
| `python -m cron.morning_health` | 09:00 | Pre-open health notification |
| `python -m cron.post_market_report` | 16:35 | Daily report |

## Strategy Registry

`strategies/__init__.py` holds a registry dict mapping strategy name to
a `Strategy` subclass. The active strategy is stored in
`system_config.active_strategy` and can be switched at runtime.

## Manual Migrations

Schema changes are tracked as explicit SQL files under `db/migrations/`.
When `db/models.py` changes, add a matching idempotent SQL migration and run it
manually before deploying code that depends on the new field. Current examples:

```sql
ALTER TABLE memory_daily
ADD COLUMN IF NOT EXISTS decision JSONB;

ALTER TABLE holdings_factors
ADD COLUMN IF NOT EXISTS price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS close_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS daily_return_pct NUMERIC(8,6);

ALTER TABLE holdings_factors
ADD COLUMN IF NOT EXISTS universe_role VARCHAR(20),
ADD COLUMN IF NOT EXISTS open_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS high_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS low_price NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS volume BIGINT,
ADD COLUMN IF NOT EXISTS dollar_volume NUMERIC(20,2),
ADD COLUMN IF NOT EXISTS return_5d NUMERIC(8,6),
ADD COLUMN IF NOT EXISTS sma_20 NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS sma_50 NUMERIC(15,4),
ADD COLUMN IF NOT EXISTS sma_200 NUMERIC(15,4);

ALTER TABLE qc_snapshots
ALTER COLUMN packet_type TYPE VARCHAR(40);

CREATE TABLE IF NOT EXISTS market_daily_features (
    id BIGSERIAL PRIMARY KEY,
    trading_date DATE NOT NULL,
    ticker VARCHAR(20) NOT NULL,
    source VARCHAR(30) NOT NULL DEFAULT 'yfinance',
    open_price NUMERIC(15,4),
    high_price NUMERIC(15,4),
    low_price NUMERIC(15,4),
    close_price NUMERIC(15,4),
    adj_close_price NUMERIC(15,4),
    volume BIGINT,
    dollar_volume NUMERIC(20,2),
    return_1d NUMERIC(8,6),
    return_5d NUMERIC(8,6),
    return_20d NUMERIC(8,6),
    return_60d NUMERIC(8,6),
    return_252d NUMERIC(8,6),
    sma_20 NUMERIC(15,4),
    sma_50 NUMERIC(15,4),
    sma_200 NUMERIC(15,4),
    hist_vol_20d NUMERIC(8,6),
    data_quality_flag VARCHAR(40) DEFAULT 'ok',
    raw_payload JSONB,
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    updated_at TIMESTAMP DEFAULT now() NOT NULL,
    CONSTRAINT uq_market_daily_feature_date_ticker_source UNIQUE (trading_date, ticker, source)
);
```

```
strategies/
├── base.py              # Strategy ABC + ScoredTicker dataclass
├── momentum_lite.py     # MomentumLiteV1 (default)
├── dual_momentum.py     # Relative/absolute momentum rotation
├── mean_reversion_lite.py # RSI/Bollinger mean reversion
├── low_vol_factor.py    # Low-vol + long momentum confirmation
├── risk_parity_lite.py  # Inverse-volatility allocation benchmark
├── equal_weight.py      # Equal-weight benchmark
├── defensive_adjust.py  # Regime-based defense matrix + rebalance helpers
└── __init__.py          # STRATEGY_REGISTRY + get_strategy()
```

Every strategy declares a data contract:

- `required_fields`: fields that must exist on enough holdings before scoring
- `optional_fields`: useful fields that are not hard blockers
- `min_required_coverage`: minimum universe coverage, default 70%

Playground checks this contract before running each strategy. If data is
missing, the strategy is marked `data_ready=false`, reports missing fields, and
is excluded from consensus weights.

**Current default — `MomentumLiteV1`:**
5-factor composite score:

```
0.30 · z(mom_20d) + 0.35 · z(mom_60d) + 0.20 · z(mom_252d)
+ 0.10 · z(100 - rsi_14)    # RSI reversed: overbought penalized
+ 0.05 · z(1 / atr_pct)     # low-vol bonus
```

Optimization: position count N chosen from `direction_bias + confidence`,
score-weighted (70%) blended with inverse-vol (30%), capped by
`max_single_position`, floored by `min_cash_pct`, CASH absorbs residual.

**Adding a new strategy:**

1. Create `strategies/my_strategy.py` subclassing `Strategy`.
2. Define `required_fields` / `optional_fields`.
3. Register in `STRATEGY_REGISTRY`.
4. Insert `strategy_<name>_params` via `db/seed.py` if it should be tunable.
5. Switch active strategy by updating `system_config.active_strategy`.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Database

```bash
docker run -d --name qc-fastapi-2-pg \
  -e POSTGRES_DB=qc_fastapi_2 \
  -e POSTGRES_USER=qc_fastapi_2 \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 postgres:16
```

### 3. Configuration

Copy `.env.example` to `.env`. Required keys:

- `DATABASE_URL` — PostgreSQL (asyncpg format)
- `OPENAI_API_KEY` — OpenAI API key
- `OPENAI_MODEL` — light model (default: `gpt-4o-mini`) for news summary
  + COMMUNICATOR
- `OPENAI_MODEL_HEAVY` — main reasoning model (default: `gpt-5.4-mini`) for
  RESEARCHER
- `FINNHUB_API_KEY` — news/calendar source for `pre_fetch_news`
- `ALPHAVANTAGE_API_KEY` — (optional) Alpha Vantage news sentiment
- `QC_API_URL`, `QC_USER_ID`, `QC_API_TOKEN`, `QC_PROJECT_ID` —
  QuantConnect
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- `WEBHOOK_USER`, `WEBHOOK_SECRET` — QC webhook auth
- `AUTHORIZATION_MODE` — `FULL_AUTO` | `SEMI_AUTO` | `MANUAL`
- `SEMI_AUTO_TIMEOUT_MINUTES` — default 20

### 4. Seed System Config

```bash
python -m db.seed
```

Idempotent — existing keys are left untouched.

### 5. Start the Web Service

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The web service only serves webhooks. All scheduled work runs via the
cron services above.

Optional yfinance backfill controls:

- `YFINANCE_BACKFILL_DAYS` — default `420`
- `YFINANCE_BATCH_SIZE` — default `30`
- `YFINANCE_TICKERS` — optional comma-separated override; empty means resolved ETF universe

## API Endpoints

- `GET /health` — Health check
- `GET /api/status` — System status + latest portfolio
- `POST /api/webhook/qc` — QC data packet receiver (gzip JSON)
- `POST /api/command/pause` — Pause/resume trading
- `GET /api/command/status` — Trading authorization status
- `POST /api/telegram` — Telegram webhook for user commands

## Authorization Modes

- **FULL_AUTO** — Fully autonomous execution
- **SEMI_AUTO** — Send proposal to Telegram, wait for user confirmation
  (default 20 min timeout; auto-executes if VIX < 30 and cost under
  threshold, else skips)
- **MANUAL** — No automatic execution; pipeline skipped at Stage 0

## Telegram Commands

- `/confirm` — Approve and execute the pending proposal
- `/skip` — Skip the current proposal
- `/pause` — Switch to MANUAL mode
- `/status` — Check system state

## Phase 1 Features

✅ 10-stage Python-LLM-Python relay pipeline
✅ Weights as the interstage baton (base → adjusted → target)
✅ Pluggable strategy registry (MomentumLiteV1 default)
✅ Decoupled news layer via independent `pre_fetch_news` cron
✅ Finnhub macro + per-ticker news with LLM batch summarization
✅ 3-layer risk overlays (transmission / defensive / hard_risk)
✅ Regime enum enforced end-to-end
✅ SEMI_AUTO authorization with Telegram integration
✅ Deterministic approval-token issuance with one-shot consume
✅ 6 quantitative risk checks with per-check actuals
✅ Railway cron services (no in-process scheduler)
✅ PostgreSQL with async SQLAlchemy + asyncpg
✅ QC webhook receiver with gzip decompression + HMAC-authenticated
   command posting

## Phase 2 Progress

✅ Multi-source news: Finnhub + Alpha Vantage + RSS feeds
✅ `source_api` tracking per news article (finnhub/alphavantage/rss)
✅ RSS → ETF keyword matching (17 ETF × keyword list)
✅ Intelligent LLM skip for pre-populated sentiment (Alpha Vantage)
✅ RESEARCHER refactored to info synthesis (research_report, no weights)
✅ Bull/Bear structured debate (Stage 4a/4b, parallel via asyncio.gather)
✅ Synthesizer CIO arbiter (Stage 5, interface-compatible with old researcher_out)
✅ 10-stage pipeline refactor (pipeline.py rewired)
✅ 5-level stance system (buy/overweight/maintain/underweight/sell)
✅ Communicator updated with debate_summary in Telegram card
✅ AgentStepLog table for per-stage input/output audit trail
✅ Telegram error messages now include exception details for remote debugging
