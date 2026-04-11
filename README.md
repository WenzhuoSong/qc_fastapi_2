# QC FastAPI 2 - QC Agentic Trading System 

Phase 1 implementation of the autonomous trading system that integrates with QuantConnect.

## Architecture

```
qc_fastapi_2/
├── agents/          # 6 specialized agents (Planner, Researcher, Allocator, Risk Manager, Executor, Reporter)
├── strategies/      # Pluggable scoring strategies (registry pattern)
├── api/             # FastAPI endpoints (webhook, command, status, telegram)
├── db/              # Database models, session, queries, seed
├── services/        # Async orchestration (pipeline, proposal, telegram commands)
├── cron/            # Standalone cron entry scripts (run via `python -m cron.<name>`)
├── tools/           # Tool implementations (db, qc, notify)
├── config.py        # Pydantic Settings configuration
└── main.py          # FastAPI application entry point (webhook-only)
```

The entire stack is async-only. The web service (`main.py`) only serves
webhooks; all scheduled work runs as separate Railway cron services, each
in its own Python process with its own `asyncio.run()`. This eliminates
asyncpg cross-event-loop issues by giving every job a fresh event loop.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Database Setup

Start PostgreSQL (Docker example):

```bash
docker run -d --name qc-fastapi-2-pg \
  -e POSTGRES_DB=qc_fastapi_2 \
  -e POSTGRES_USER=qc_fastapi_2 \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 postgres:16
```

### 3. Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Required environment variables:
- `DATABASE_URL` - PostgreSQL connection string
- `OPENAI_API_KEY` - OpenAI API key
- `OPENAI_MODEL` - GPT model to use (default: gpt-4o)
- `OPENAI_MODEL_MINI` - Lighter model for simple tasks (default: gpt-4o-mini)
- `QC_API_URL`, `QC_USER_ID`, `QC_API_TOKEN`, `QC_PROJECT_ID` - QuantConnect credentials
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` - Telegram notification setup
- `WEBHOOK_USER`, `WEBHOOK_SECRET` - QC webhook authentication

### 4. Start the Application

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

- `GET /health` - Health check
- `GET /api/status` - System status and latest portfolio state
- `POST /api/webhook/qc` - QC data packet receiver (gzip compressed)
- `POST /api/command/pause` - Pause/resume trading
- `GET /api/command/status` - Trading authorization status
- `POST /api/telegram` - Telegram webhook for user commands

## Authorization Modes

- **FULL_AUTO** - Fully autonomous execution
- **SEMI_AUTO** - Send proposal to Telegram, wait for user confirmation (default 20 min timeout)
- **MANUAL** - No automatic execution, manual intervention required

## Telegram Commands

- `/confirm` - Approve and execute the pending proposal
- `/skip` - Skip the current proposal
- `/pause` - Switch to MANUAL mode
- `/status` - Check system status

## Cron Jobs

Each entry is a standalone process. Configure as Railway cron services:

| Entry                               | Schedule (ET)       | Purpose                          |
|-------------------------------------|---------------------|----------------------------------|
| `python -m cron.hourly_analysis`    | 10:00–15:00 hourly  | Full 6-agent pipeline            |
| `python -m cron.morning_health`     | 09:00               | Pre-open health notification     |
| `python -m cron.post_market_report` | 16:35               | Daily report                     |
| `python -m cron.pending_check`      | every 1 min         | SEMI_AUTO proposal timeout check |

## Agent Pipeline

```
PLANNER → RESEARCHER → ALLOCATOR → RISK MGR → EXECUTOR → REPORTER
                          ↑                      ↓
                  strategies/ registry       Telegram
                  (score + optimize)         (SEMI_AUTO)
```

### RESEARCHER

Outputs `market_judgment.regime` as one of six enum values —
`bull_trend | bull_weak | neutral | bear_weak | bear_trend | high_vol` —
which directly keys into the ALLOCATOR defense matrix. Any non-enum value
is treated as `neutral`. `recommended_stance` is constrained to
`maintain | increase | reduce | defensive`.

### ALLOCATOR

All arithmetic is deterministic Python; the LLM only selects between
Plan A and Plan B and writes a short reasoning (auto-falls back to a
rule engine on any LLM failure).

1. Load latest snapshot + current holdings + `risk_params` +
   `active_strategy` from `system_config`.
2. Instantiate the active strategy from `strategies/` and call
   `strategy.score(holdings, context)` → `strategy.optimize(...)` to
   produce **Plan A** (standard target weights).
3. Run `defensive_adjust(plan_a, context)` to produce **Plan B**
   (conservative variant, regime-scaled defense).
4. Compute `rebalance_actions` and `estimated_cost_pct` for both plans
   via `compute_rebalance_actions` / `estimate_cost_pct`.
5. LLM picks A or B with reasoning. Rules decide if LLM fails:
   defensive/bear_trend/high_vol → B; drawdown ≥ 75% of max → B;
   plan A cost > `max_trade_cost_pct` → B; else A.

## Strategy Registry

`strategies/__init__.py` holds a registry dict mapping strategy name to
a `Strategy` subclass. Adding a new scoring strategy is a one-line
change plus a new class file. The active strategy is stored in
`system_config.active_strategy` and can be switched at runtime without a
code deploy.

```
strategies/
├── base.py              # Strategy ABC + ScoredTicker dataclass
├── momentum_lite.py     # MomentumLiteV1 (default)
├── defensive_adjust.py  # Regime-based defense matrix + rebalance helpers
└── __init__.py          # STRATEGY_REGISTRY + get_strategy()
```

**Current default — `MomentumLiteV1`:**
5-factor composite score on the 17-ETF universe:

```
0.30 · z(mom_20d) + 0.35 · z(mom_60d) + 0.20 · z(mom_252d)
+ 0.10 · z(100 - rsi_14)    # RSI reversed: overbought penalized
+ 0.05 · z(1 / atr_pct)     # low-vol bonus
```

Optimization: position count N chosen from
`direction_bias + confidence`, score-weighted allocation (70%) blended
with inverse-volatility weights (30%), capped by `max_single_position`,
floored by `min_cash_pct`, CASH absorbs residual.

**Adding a new strategy:**

1. Create `strategies/my_strategy.py` subclassing `Strategy` with
   `score()` and `optimize()`.
2. Register in `STRATEGY_REGISTRY` in `strategies/__init__.py`.
3. Insert default params as
   `strategy_<name>_params` in `db/seed.py` (or write directly to
   `system_config`).
4. Switch active strategy by updating `system_config.active_strategy`.

## Seeding System Config

On a fresh database, run the seeder to populate default risk params,
authorization mode, active strategy, and strategy parameters:

```bash
python -m db.seed
```

Seeding is idempotent — existing keys are left untouched.

## Phase 1 Features

✅ Complete 6-agent pipeline
✅ Tool-based architecture with BaseAgent
✅ Pluggable strategy registry (MomentumLiteV1 default)
✅ Deterministic ALLOCATOR math (LLM only picks Plan A/B)
✅ Regime enum enforced end-to-end (RESEARCHER → defense matrix)
✅ SEMI_AUTO authorization with Telegram integration
✅ Deterministic approval-token issuance from pipeline
✅ Risk management checks
✅ Railway cron services for time-based triggers
✅ PostgreSQL with async SQLAlchemy
✅ QC webhook receiver with gzip decompression
