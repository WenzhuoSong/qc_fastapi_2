# QC FastAPI 2 — QC Agentic Trading System

Phase 1 implementation of an autonomous ETF trading system that integrates
with QuantConnect. The pipeline is a **Python → LLM → Python 三段接力**：
pure math computes a baseline, an LLM macro strategist adjusts it on top of
news, and a Python risk officer applies overlays + hard checks before
execution.

## Architecture

```
qc_fastapi_2/
├── agents/          # Pipeline agents
│   ├── researcher.py       # Stage 3: LLM synthesizer (base → adjusted weights)
│   ├── risk_manager.py     # Stage 4: overlays + 6 checks → target weights
│   ├── communicator.py     # LLM Telegram card + Python fallback
│   ├── executor.py         # Deterministic execution (3 gates)
│   └── reporter.py         # Daily report
├── services/        # Async orchestration
│   ├── pipeline.py         # run_full_pipeline (8-stage relay)
│   ├── market_brief.py     # Stage 1: snapshot + news → brief
│   ├── quant_baseline.py   # Stage 2: pure math → base_weights
│   ├── transmission.py     # Macro event → sector pattern library
│   ├── finnhub_client.py   # Finnhub REST + credibility scoring
│   ├── news_summarizer.py  # gpt-4o-mini batch news summarizer
│   ├── proposal.py         # SEMI_AUTO proposal lifecycle
│   └── telegram_commands.py# /confirm /skip /pause /status
├── strategies/      # Pluggable scoring strategies (registry)
│   ├── base.py             # Strategy ABC + ScoredTicker
│   ├── momentum_lite.py    # MomentumLiteV1 (default)
│   └── defensive_adjust.py # Defense matrix + rebalance helpers
├── cron/            # Standalone cron entry scripts
│   ├── pre_fetch_news.py   # Finnhub → DB (independent)
│   ├── hourly_analysis.py  # Main agent pipeline
│   ├── pending_check.py    # SEMI_AUTO timeout handler
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

## Pipeline: 8-Stage Relay

```
Stage 0  guard_and_config     Python    config / pause / lock
Stage 1  market_brief         Python    snapshot + news → brief (no weights)
Stage 2  quant_baseline       Python    pure-math scoring → base_weights
Stage 3  RESEARCHER           LLM       base_weights + brief → adjusted_weights
Stage 4  RISK MGR             Python    transmission → defensive → hard_risk → 6 checks
Stage 5  save_analysis        Python    INSERT INTO agent_analysis (4 cols)
Stage 6  COMMUNICATOR         LLM+fb    Telegram card (5s timeout → Python fallback)
Stage 7  branch               Python    rejected / SEMI_AUTO pending / FULL_AUTO execute
Stage 8  EXECUTOR             Python    3 gates → QC REST (FULL_AUTO path)
```

**The baton is always weights:**

```
 base_weights         adjusted_weights         target_weights
 (Stage 2 Python) ──► (Stage 3 LLM)    ──►     (Stage 4 Python) ──► QC
   量化研究员            宏观策略师                  首席风控官
```

LLM calls per cycle: **2** — RESEARCHER (on the correctness path, with a
degraded fallback that echoes `base_weights` if all retries fail) and
COMMUNICATOR (5s timeout → Python f-string fallback; not on the
correctness path).

### Stage-by-stage responsibilities

**Stage 1 — `market_brief`** (Python)
Reads latest `QCSnapshot` + `MacroNewsCache` (1 row) + `TickerNewsLibrary`
(48h window), computes `key_facts` (breadth, SPY mom, avg ATR,
risk_on_score, drawdown, top5/bottom5 momentum), builds a prose summary.
Output: `brief` dict. No weights.

**Stage 2 — `quant_baseline`** (Python)
Instantiates the active strategy from `strategies/`, calls
`strategy.score(holdings, NEUTRAL_CTX)` → `strategy.optimize(...)`. The
context is deliberately neutral — regime judgment happens in Stage 3.
Output: `base_weights` + `scoring_breakdown` + `ranking_summary`. The
baseline is "the Python quant researcher's best guess if it only saw the
numbers."

**Stage 3 — `RESEARCHER`** (LLM, gpt-4o)
The macro strategist. User payload includes the brief prose, key_facts,
macro news, calendar, **base_weights**, top-15 scoring breakdown, and
constraints. System prompt requires the LLM to **start from base_weights
and make qualitative micro-adjustments** (default ≤±5%, >±10% needs a
clear macro reason). Output includes:

- `market_judgment` — regime (6-enum) + confidence + uncertainty
- `recommended_stance` — 4-enum
- `adjusted_weights` — the draft proposal
- `weight_adjustments` — per-ticker delta vs base_weights + reason
- `reasoning` — 150-char Chinese rationale
- `key_events` — 3-5 phrases using terms matchable by the transmission
  pattern library

Python post-processing sanitizes the weights: unknown tickers filtered,
negative values zeroed, per-position capped at `max_single_position`,
renormalized to sum=1.0, CASH absorbs rounding residual. If all 3 LLM
retries fail, a **degraded fallback** returns base_weights as
adjusted_weights with `used_degraded_fallback=True`.

**Stage 4 — `RISK MGR`** (Python)
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

**Stage 8 — `EXECUTOR`** (Python, FULL_AUTO only)
Three gates: `risk_out.approved` → `verify_approval_token` (one-shot
consume) → weight sum sanity. On pass, HMAC-SHA256 POST to
`{QC_API_URL}/projects/{PROJECT_ID}/live/commands` with target weights.

## News Layer (Cron 1, decoupled)

Two independent tables feed Stage 1:

- **`TickerNewsLibrary`** — Finnhub per-ticker news with gpt-4o-mini
  summary, sentiment, relevance, hard_risks flags, source credibility
  (Bloomberg/Reuters=100 down to 30 default). 48h rolling TTL, dedup by
  `(ticker, url)`.
- **`MacroNewsCache`** — single-row cache of macro headlines + economic
  calendar + pre-stitched Chinese prose.

Both are maintained by `cron/pre_fetch_news.py` every 2h, completely
independent from the main pipeline. Finnhub outage → stale cache → main
pipeline still runs. Main pipeline outage → news still refreshes.

## Cron Jobs

**5 standalone processes**, each `python -m cron.<name>` with its own
`asyncio.run()`. Configure as Railway cron services:

| Entry | Schedule (ET) | Purpose |
|---|---|---|
| `python -m cron.pre_fetch_news` | 09:50 / 11:50 / 13:50 | Finnhub → DB (macro + ticker news) |
| `python -m cron.hourly_analysis` | 10:00–15:00 hourly | Full 8-stage pipeline |
| `python -m cron.pending_check` | every 1 min | SEMI_AUTO timeout handler |
| `python -m cron.morning_health` | 09:00 | Pre-open health notification |
| `python -m cron.post_market_report` | 16:35 | Daily report |

## Strategy Registry

`strategies/__init__.py` holds a registry dict mapping strategy name to
a `Strategy` subclass. The active strategy is stored in
`system_config.active_strategy` and can be switched at runtime.

```
strategies/
├── base.py              # Strategy ABC + ScoredTicker dataclass
├── momentum_lite.py     # MomentumLiteV1 (default)
├── defensive_adjust.py  # Regime-based defense matrix + rebalance helpers
└── __init__.py          # STRATEGY_REGISTRY + get_strategy()
```

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
2. Register in `STRATEGY_REGISTRY`.
3. Insert `strategy_<name>_params` via `db/seed.py`.
4. Switch active strategy by updating `system_config.active_strategy`.

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
- `OPENAI_MODEL_HEAVY` — main reasoning model (default: `gpt-4o`) for
  RESEARCHER
- `FINNHUB_API_KEY` — news/calendar source for `pre_fetch_news`
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

The web service only serves webhooks. All scheduled work runs via the 5
cron services above.

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

✅ 8-stage Python-LLM-Python relay pipeline
✅ Weights as the interstage baton (base → adjusted → target)
✅ Pluggable strategy registry (MomentumLiteV1 default)
✅ Decoupled news layer via independent `pre_fetch_news` cron
✅ Finnhub macro + per-ticker news with LLM batch summarization
✅ 3-layer risk overlays (transmission / defensive / hard_risk)
✅ Regime enum enforced end-to-end
✅ SEMI_AUTO authorization with Telegram integration
✅ Deterministic approval-token issuance with one-shot consume
✅ 6 quantitative risk checks with per-check actuals
✅ Railway cron services (5 processes, no in-process scheduler)
✅ PostgreSQL with async SQLAlchemy + asyncpg
✅ QC webhook receiver with gzip decompression + HMAC-authenticated
   command posting
