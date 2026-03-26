# QC FastAPI 2 - QC Agentic Trading System

Phase 1 implementation of the autonomous trading system that integrates with QuantConnect.

## Architecture

```
qc_fastapi_2/
├── agents/          # 6 specialized agents (Planner, Researcher, Allocator, Risk Manager, Executor, Reporter)
├── api/             # FastAPI endpoints (webhook, command, status, telegram)
├── db/              # Database models, session, queries
├── scheduler/       # APScheduler jobs and runner
├── tools/           # Tool implementations (db, qc, notify)
├── config.py        # Pydantic Settings configuration
└── main.py          # FastAPI application entry point
```

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

## Scheduler Jobs

- **Hourly Analysis** - 10:00-15:00 ET (market hours)
- **Morning Health Check** - 09:00 ET
- **Post Market Report** - 16:35 ET

## Agent Pipeline

```
PLANNER → RESEARCHER → ALLOCATOR → RISK MGR → EXECUTOR
                                              ↓
                                          Telegram
                                          (SEMI_AUTO)
```

## Phase 1 Features

✅ Complete 6-agent pipeline
✅ Tool-based architecture with BaseAgent
✅ SEMI_AUTO authorization with Telegram integration
✅ Risk management checks
✅ APScheduler for time-based triggers
✅ PostgreSQL with async SQLAlchemy
✅ QC webhook receiver with gzip decompression
