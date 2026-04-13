# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Start the Agentix system (dev)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
# API docs at http://localhost:8000/docs

# Start PostgreSQL (Docker)
docker run -d --name qc-fastapi-2-pg \
  -e POSTGRES_DB=qc_fastapi_2 \
  -e POSTGRES_USER=qc_fastapi_2 \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 postgres:16

# Configure Telegram webhook
curl "https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://your-server/api/telegram"
```

## Architecture

This is an **agentic trading system** that integrates with QuantConnect using a 6-agent pipeline with tool-based architecture and Telegram-based authorization.

### Core Design

**Two independent crons:**

```
┌─────────────────────────────────────────────────────────┐
│ Cron 1: pre_fetch_news    every 2h @ 09:50/11:50/13:50 ET
│   Phase A: Finnhub → MacroNewsCache + TickerNewsLibrary
│   Phase B: Alpha Vantage → TickerNewsLibrary (bulk ticker)
│   Phase C: RSS feeds → keyword match → TickerNewsLibrary
│   Each phase independent. Failure → stale cache,
│   main pipeline still runs.
└─────────────────────────────────────────────────────────┘
                   ↓ writes
              MacroNewsCache / TickerNewsLibrary
                   ↑ reads
┌─────────────────────────────────────────────────────────┐
│ Cron 2: hourly_analysis   hourly @ 10:00–15:00 ET
│   guard → market_brief → QUANT_BASELINE → RESEARCHER
│   → BULL/BEAR (parallel) → SYNTHESIZER → RISK MGR
│   → save → COMMUNICATOR → branch
│   (SEMI_AUTO pending / FULL_AUTO execute)
└─────────────────────────────────────────────────────────┘
```

**Pipeline stages (10-stage Python-LLM-Python 三段接力)**:
guard_and_config → market_brief → **quant_baseline** (Python 纯数学) →
**RESEARCHER** (LLM 信息合成) → **BULL/BEAR** (LLM 并行辩论) →
**SYNTHESIZER** (LLM 仲裁) → **RISK MGR** (Python overlays + 6 项检查) →
save_analysis → COMMUNICATOR → branch

**接力棒传的是 weights**：
`base_weights` (Stage 2 Python) → `research_report` (Stage 3 LLM) →
`bull/bear_output` (Stage 4a/4b LLM parallel) → `adjusted_weights` (Stage 5 LLM) →
`target_weights` (Stage 6 Python) → execute。

LLM calls per cycle: **4+1** (RESEARCHER + BULL + BEAR + SYNTHESIZER on
correctness path; COMMUNICATOR off correctness path with 5s timeout →
Python fallback). Bull/Bear run in parallel via `asyncio.gather`. Each
agent has degraded fallback if all retries fail.

**Authorization Modes**:
- `FULL_AUTO` — Fully autonomous execution
- `SEMI_AUTO` — Send proposal to Telegram, wait for confirmation (20 min timeout)
- `MANUAL` — No automatic execution

### Project Structure

```
qc_fastapi_2/
├── agents/          # Pipeline agents
│   ├── base_agent.py       # BaseAgent with tool calling loop (legacy helper)
│   ├── researcher.py       # Stage 3: LLM info synthesis → research_report
│   ├── bull_researcher.py  # Stage 4a: Bull arguments (parallel)
│   ├── bear_researcher.py  # Stage 4b: Bear arguments (parallel)
│   ├── synthesizer.py      # Stage 5: CIO arbitration → adjusted_weights
│   ├── risk_manager.py     # Stage 6: overlays + 6 checks → target_weights + token
│   ├── communicator.py     # LLM Telegram card + Python fallback
│   ├── executor.py         # Deterministic execution logic
│   └── reporter.py         # Daily report generator
├── api/             # FastAPI endpoints
│   ├── webhook.py          # QC data packet receiver (gzip)
│   ├── status.py           # System status
│   ├── command.py          # Trading control (/pause, /status)
│   └── telegram_webhook.py # Telegram command handler
├── db/              # Database layer
│   ├── session.py          # AsyncSession + asyncpg
│   ├── models.py           # SQLAlchemy models (incl. news tables)
│   └── queries.py          # DB helper functions
├── services/        # Async orchestration layer
│   ├── pipeline.py         # run_full_pipeline
│   ├── market_brief.py     # Stage 1: snapshot + news → brief dict
│   ├── quant_baseline.py   # Stage 2: Python scoring → base_weights
│   ├── transmission.py     # Macro event → sector pattern library (used by risk_mgr)
│   ├── finnhub_client.py   # Finnhub REST client + credibility
│   ├── alphavantage_client.py # Alpha Vantage News Sentiment API client
│   ├── rss_fetcher.py      # RSS feed fetcher (MarketWatch/CNBC/Yahoo/Reuters)
│   ├── news_summarizer.py  # gpt-4o-mini batch news summarizer
│   ├── proposal.py         # SEMI_AUTO proposal + timeout handler
│   └── telegram_commands.py# /confirm /skip /pause /status
├── strategies/      # Strategy layer
│   ├── base.py             # Strategy abstract contract
│   ├── momentum_lite.py    # 5-factor momentum composite
│   └── defensive_adjust.py # Regime-based overlay helpers
├── cron/            # Cron entry scripts (standalone processes)
│   ├── pre_fetch_news.py   # Cron 1: multi-source news → DB (every 2h)
│   ├── hourly_analysis.py  # Cron 2: main pipeline (hourly)
│   ├── post_market_report.py
│   ├── morning_health.py
│   └── pending_check.py
├── tools/           # Tool implementations
│   ├── registry.py         # Tool whitelist management
│   ├── db_tools.py         # Database operations
│   ├── qc_tools.py         # QC API (weights, liquidate)
│   └── notify_tools.py     # Telegram notifications
├── constants.py     # ETF_UNIVERSE + style buckets
├── config.py        # Pydantic Settings
└── main.py          # FastAPI app entry point
```

### BaseAgent Architecture

All agents inherit from `BaseAgent` (`agents/base_agent.py`):
- **Tool Calling Loop**: OpenAI Chat Completions API with function calling until stop
- **Whitelist Enforcement**: Each agent only gets authorized tools
- **Retry Logic**: Configurable retries with error context
- **JSON Validation**: Output schema validation
- **Model Selection**: Choose between GPT-4o (default) or GPT-4o-mini for lighter tasks

Example agent instantiation:
```python
agent = BaseAgent(
    name="RESEARCHER",
    system_prompt=SYSTEM_PROMPT,
    tools=TOOLS_DEF,
    tool_executor=get_tool_executor(["read_latest_snapshots", "read_system_config"]),
    max_retries=2,
)
result = await agent.run(input_data, OUTPUT_SCHEMA)  # BaseAgent is async-only
```

All tools, agents, and services are async. Each cron entry script runs a single
`asyncio.run(main())` in its own process — no in-process scheduler.

### Database Models

| Model | Purpose |
|-------|---------|
| `SystemConfig` | Key-value config store (JSONB) |
| `QCSnapshot` | QC webhook data packets |
| `PortfolioTimeseries` | Portfolio state history |
| `HoldingsFactor` | Per-ticker factor metrics |
| `AlertLog` | QC alert tracking |
| `AgentAnalysis` | Complete agent pipeline results |
| `ExecutionLog` | QC command execution history |
| `TickerNewsLibrary` | Multi-source news (Finnhub/Alpha Vantage/RSS) + LLM summary + hard_risks (48h rolling) |
| `MacroNewsCache` | Single-row macro news + economic calendar cache |

All use async SQLAlchemy with asyncpg driver.

### Tool Registry Pattern

`tools/registry.py` defines `ALL_TOOLS` dictionary mapping tool names to functions. Agents request whitelisted subset:

```python
get_tool_executor(["read_system_config", "write_approval_token"])
```

Available tools:
- **DB Tools**: `read_system_config`, `read_latest_snapshots`, `read_latest_portfolio`, `write_decision`, `write_approval_token`, `verify_approval_token`
- **QC Tools**: `send_weight_command`, `emergency_liquidate`
- **Notify Tools**: `send_telegram`

### SEMI_AUTO Protocol

When RISK MGR approves a proposal in SEMI_AUTO mode:

1. **Proposal Generation**: Format Telegram card with plan details
2. **Store Pending State**: Write to `system_config.pending_proposal` with token
3. **Start Timer**: 20-minute countdown (configurable)
4. **User Commands**:
   - `/confirm` — Execute immediately
   - `/skip` — Abort this cycle
   - `/pause` — Switch to MANUAL mode
   - `/status` — Check system state
5. **Timeout Handler**: Auto-execute if VIX < 30 and cost < 0.3%, else skip

### Pipeline State Management

`AgentAnalysis.execution_status` tracks pipeline progress:
- `pending` — Awaiting execution
- `executed_user_confirmed` — User confirmed via /confirm
- `executed_timeout_auto` — Auto-executed after timeout
- `skipped_by_user` — User skipped via /skip
- `skipped_timeout_vix` — Timeout skipped due to VIX > 30
- `skipped_timeout_cost` — Timeout skipped due to high cost
- `rejected_by_risk` — RISK MGR rejected
- `skipped_manual_mode` — MANUAL mode active

### Cron Jobs

Each cron is a standalone Python process (run via `python -m cron.<name>`),
with its own fresh `asyncio.run()`. Configure schedules as Railway cron services:

| Entry | Schedule (ET) | Purpose |
|-------|---------------|---------|
| `python -m cron.pre_fetch_news`    | 09:50, 11:50, 13:50 | Multi-source news → DB (Finnhub + Alpha Vantage + RSS) |
| `python -m cron.hourly_analysis`   | 10:00–15:00 hourly | Full agent pipeline |
| `python -m cron.post_market_report`| 16:35 | Daily summary |
| `python -m cron.morning_health`    | 09:00 | Health check notification |
| `python -m cron.pending_check`     | every 1 min | SEMI_AUTO timeout handler |

The web service (`main.py`) only serves webhooks (`/api/webhook/qc`,
`/api/telegram`, `/api/status`, `/api/command/*`) — it no longer runs any
scheduler in-process. This eliminates asyncpg cross-event-loop issues by
giving every scheduled job a fresh Python process.

### Configuration (`config.py`)

Uses Pydantic `BaseSettings` loading from `.env`. Required keys:
- `DATABASE_URL` — PostgreSQL connection (asyncpg format)
- `OPENAI_API_KEY` — OpenAI API key
- `OPENAI_MODEL` — Default `gpt-4o`
- `OPENAI_MODEL_MINI` — Default `gpt-4o-mini` (for lighter tasks)
- `WEBHOOK_USER`, `WEBHOOK_SECRET` — QC webhook auth
- `QC_API_URL`, `QC_USER_ID`, `QC_API_TOKEN`, `QC_PROJECT_ID` — QC REST API
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — Telegram integration
- `AUTHORIZATION_MODE` — `FULL_AUTO` | `SEMI_AUTO` | `MANUAL`
- `SEMI_AUTO_TIMEOUT_MINUTES` — Default 20

Risk parameters with defaults:
- `max_drawdown` — 0.15
- `max_single_position` — 0.20
- `min_cash_pct` — 0.05
- `max_sector_concentration` — 0.60
- `rebalance_threshold` — 0.02
- `max_trade_cost_pct` — 0.005

### API Endpoints

- `GET /health` — Health check
- `GET /api/status` — System status + latest portfolio
- `POST /api/webhook/qc` — QC webhook receiver (gzip compressed JSON)
- `POST /api/command/pause` — Pause/resume trading
- `GET /api/command/status` — Check trading pause state
- `POST /api/telegram` — Telegram webhook endpoint

### QC Integration

**Webhook Flow** (QC → Agentix):
1. QC sends gzip-compressed JSON to `/api/webhook/qc`
2. Agentix verifies credentials via `x-webhook-user` and `x-webhook-secret` headers
3. Decompress and store to `qc_snapshots` table
4. Packet types: `heartbeat`, `alert`, `emergency`

**Command Flow** (Agentix → QC):
1. EXECUTOR generates target weights
2. Call `tool_send_weight_command` with weights dict
3. HMAC-SHA256 authentication
4. POST to `{QC_API_URL}/projects/{PROJECT_ID}/live/commands`
5. Emergency liquidate: `{"target": "EmergencyLiquidate"}`

### Key Design Principles

1. **Tool Isolation**: Each agent only accesses approved tools
2. **Approval Token**: One-time 5-minute token from RISK MGR to EXECUTOR
3. **Idempotency**: All DB operations safe for retries
4. **Async-First**: All DB calls use async/await with asyncpg
5. **Defensive Execution**: Three gates (token, auth_mode, weight validation)

### Deployment

Deployed on Railway with multiple services: PostgreSQL, one long-running web service (FastAPI for webhooks), and separate Railway cron services for each entry in `cron/`. See `railway.toml` and `Dockerfile`.

**Required Railway environment variables**:
```
DATABASE_URL          # auto-injected from Railway PostgreSQL service
OPENAI_API_KEY
OPENAI_MODEL=gpt-4o-mini        # light tasks (news summarization, communicator)
OPENAI_MODEL_HEAVY=gpt-4o       # main reasoning (RESEARCHER)
FINNHUB_API_KEY                 # news/calendar source for pre_fetch_news cron
ALPHAVANTAGE_API_KEY            # (optional) Alpha Vantage news sentiment API
WEBHOOK_USER=qc
WEBHOOK_SECRET
QC_API_URL=https://www.quantconnect.com/api/v2
QC_USER_ID
QC_API_TOKEN
QC_PROJECT_ID
TG_BOT_TOKEN
TG_CHAT_ID
AUTHORIZATION_MODE=SEMI_AUTO
```

**After deploy**: Set Telegram webhook:
```bash
curl "https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://{RAILWAY_DOMAIN}/api/telegram"
```

`PORT` is dynamically assigned by Railway.

### Phase 1 Scope

✅ Complete 6-agent pipeline
✅ Tool-based architecture with BaseAgent
✅ SEMI_AUTO authorization with Telegram
✅ Risk management with approval tokens
✅ Railway cron services for time-based triggers
✅ PostgreSQL with async SQLAlchemy
✅ QC webhook + REST API integration

### Phase 2 Scope

✅ Multi-source news: Finnhub + Alpha Vantage + RSS (MarketWatch/CNBC/Yahoo/Reuters)
✅ `TickerNewsLibrary.source_api` column for source tracking
✅ RSS → ETF keyword matching (17 ETF × keyword list)
✅ Alpha Vantage bulk ticker sentiment with intelligent LLM skip
✅ RESEARCHER refactored to info synthesis (research_report, no weights)
✅ Bull/Bear structured debate (Stage 4a/4b, parallel via asyncio.gather)
✅ Synthesizer CIO arbiter (Stage 5, interface-compatible with old researcher_out)
✅ 10-stage pipeline refactor (pipeline.py rewired)
✅ 5-level stance system (buy/overweight/maintain/underweight/sell)
✅ Communicator updated with debate_summary in Telegram card

### Future Enhancements

- [ ] Dynamic DAG generation (Planner upgrade)
- [ ] Decision memory + similar case retrieval
- [ ] Adaptive risk parameters
- [ ] Portfolio analytics dashboard
- [ ] Alembic database migrations
