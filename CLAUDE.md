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

**Agent Pipeline**: PLANNER → RESEARCHER → ALLOCATOR → RISK MGR → EXECUTOR → REPORTER

**Authorization Modes**:
- `FULL_AUTO` — Fully autonomous execution
- `SEMI_AUTO` — Send proposal to Telegram, wait for confirmation (20 min timeout)
- `MANUAL` — No automatic execution

### Project Structure

```
qc_fastapi_2/
├── agents/          # 6 specialized agents
│   ├── base_agent.py       # BaseAgent with tool calling loop
│   ├── planner.py          # Static workflow planner
│   ├── researcher.py       # Market regime analyst
│   ├── allocator.py        # Weight optimizer (Plan A/B)
│   ├── risk_manager.py     # Risk gatekeeper + approval token
│   ├── executor.py         # Deterministic execution logic
│   └── reporter.py         # Daily report generator
├── api/             # FastAPI endpoints
│   ├── webhook.py          # QC data packet receiver (gzip)
│   ├── status.py           # System status
│   ├── command.py          # Trading control (/pause, /status)
│   └── telegram_webhook.py # Telegram command handler
├── db/              # Database layer
│   ├── session.py          # AsyncSession + asyncpg
│   ├── models.py           # 7 SQLAlchemy models
│   └── queries.py          # DB helper functions
├── scheduler/       # APScheduler
│   ├── runner.py           # Job configuration
│   └── jobs.py             # Pipeline + SEMI_AUTO logic
├── tools/           # Tool implementations
│   ├── registry.py         # Tool whitelist management
│   ├── db_tools.py         # Database operations
│   ├── qc_tools.py         # QC API (weights, liquidate)
│   └── notify_tools.py     # Telegram notifications
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
result = agent.run(input_data, OUTPUT_SCHEMA)
```

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

### Scheduler Jobs

Defined in `scheduler/jobs.py`, configured in `scheduler/runner.py`:

| Job | Schedule | Purpose |
|-----|----------|---------|
| `job_hourly_analysis` | 10:00-15:00 ET (hourly) | Run full agent pipeline |
| `job_post_market_report` | 16:35 ET | Generate daily summary |
| `job_morning_health_check` | 09:00 ET | System status notification |

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

Deployed on Railway with 2 services: PostgreSQL and web service (FastAPI with APScheduler). See `railway.toml` and `Dockerfile`.

**Required Railway environment variables**:
```
DATABASE_URL          # auto-injected from Railway PostgreSQL service
OPENAI_API_KEY
OPENAI_MODEL=gpt-4o
OPENAI_MODEL_MINI=gpt-4o-mini
WEBHOOK_USER=qc
WEBHOOK_SECRET
QC_API_URL=https://www.quantconnect.com/api/v2
QC_USER_ID
QC_API_TOKEN
QC_PROJECT_ID
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
AUTHORIZATION_MODE=SEMI_AUTO
```

**After deploy**: Set Telegram webhook:
```bash
curl "https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://{RAILWAY_DOMAIN}/api/telegram"
```

`PORT` is dynamically assigned by Railway.

### Phase 1 Scope (Current)

✅ Complete 6-agent pipeline
✅ Tool-based architecture with BaseAgent
✅ SEMI_AUTO authorization with Telegram
✅ Risk management with approval tokens
✅ APScheduler for time-based triggers
✅ PostgreSQL with async SQLAlchemy
✅ QC webhook + REST API integration

### Future Enhancements

- [ ] Dynamic DAG generation (Planner upgrade)
- [ ] Multi-agent debate/consensus
- [ ] Adaptive risk parameters
- [ ] Portfolio analytics dashboard
- [ ] Alembic database migrations
