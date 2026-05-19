# Operator Dashboard Plan

Last updated: 2026-05-19

## Purpose

Add a small read-only operator dashboard for the trading agent.

This dashboard is for observability only:

```text
Dashboard observes system state.
Dashboard does not approve trades.
Dashboard does not write to the database.
Dashboard does not replace Telegram or Risk Manager.
```

## Deployment Shape

Use the same repository and create a separate Railway service.

Recommended layout:

```text
qc_fastapi_2/
  dashboard/
    app.py
    views/
    static/
```

Railway services:

```text
web              -> existing FastAPI webhook/API service
dashboard        -> new read-only dashboard service
cron jobs        -> existing scheduled Railway services
postgres         -> existing Railway Postgres
```

The dashboard service should have its own start command, for example:

```bash
uvicorn dashboard.app:app --host 0.0.0.0 --port $PORT
```

## Hard Boundaries

- Read-only database access in application code.
- No trade approval endpoints.
- No command submission endpoints.
- No writes to `system_config`, `agent_analysis`, execution tables, approval
  tokens, or snapshots.
- No exposure of secrets, webhook credentials, OpenAI keys, DB URLs, Telegram
  tokens, or raw approval tokens.
- No full raw LLM prompt/output dump by default.
- No public unauthenticated access.

## Authentication

Minimum viable protection:

- Basic auth or bearer token using Railway environment variables.
- Short session timeout if browser sessions are added later.

Future option:

- Cloudflare Access, Tailscale, or another identity-aware proxy in front of the
  dashboard.

## Phase A: Minimal Read-Only Health Dashboard

Status: initial implementation complete.

Goal: one page that answers whether the system is fresh, blocked, or degraded.

Panels:

- Latest QC heartbeat age.
- Latest `daily_feature_snapshot` age.
- Latest yfinance backfill date.
- Latest news cache freshness.
- Latest memory write freshness.
- Latest cron audit status.
- Latest pipeline status.
- Current circuit state.

Primary source tables/services:

- `qc_snapshots`
- `market_daily_features`
- `cron_audit`
- `system_config`
- latest `AgentAnalysis`
- existing `services/operational_health.py`

Acceptance checks:

- Dashboard can be opened from Railway public URL after auth.
- All DB access is SELECT-only.
- If QC heartbeat or daily features are stale, the dashboard shows a clear
  degraded/blocked state.
- Missing optional data does not crash the page.

## Phase B: Trading Decision Observability

Status: initial implementation complete.

Goal: show what the latest agent cycle decided and why.

Panels:

- Market scorecard:
  - investment permission
  - data quality
  - dominant constraint
  - human confirmation flag
- Strategy evidence:
  - QC snapshot count
  - QC forward samples
  - yfinance historical samples
  - live fit
  - best strategy / suggested use
- Proposal shaping:
  - whether applied
  - clipped tickers
  - turnover/single-delta caps
- Position governance:
  - lifecycle states
  - thesis status
  - basket reviews
  - manual trim review hints
- Risk result:
  - approved/rejected
  - rejection reasons
  - target weights after risk
- Execution audit:
  - sent/accepted/failed/skipped
  - actual execution action

Acceptance checks:

- A user can distinguish proposed action, risk-approved action, and actual
  execution outcome.
- Dashboard shows `human_required` and manual-review hints clearly.
- Dashboard does not imply an action is executable unless risk/execution status
  says so.

## Phase C: Data Quality And Replay Diagnostics

Status: initial implementation complete.

Goal: make sparse-data problems visible before they distort agent output.

Panels:

- QC raw heartbeat rows by day.
- QC deduped replay snapshot days.
- `daily_feature_snapshot` days.
- yfinance feature rows by date.
- Playground live samples versus historical samples.
- Return-field availability:
  - `daily_return_pct`
  - `return_1d`
  - close/price fields
- Snapshot-limit diagnostics:
  - configured lookback days
  - row limit used before dedupe
  - expected replay days after dedupe

Acceptance checks:

- The previous `QC snapshots=7` class of issue is visible as a query/limit
  diagnostic.
- The dashboard separates raw ingestion health from replay sample usability.
- yfinance-filled fields are labeled as yfinance, not QC-native.

## Phase D: Alerts And Notifications

Status: initial implementation complete.

Possible alert channels:

- Telegram. Initial implementation sends deduped operational alerts from the
  morning health cron using `services/operational_alerts.py`.
- Email.
- Grafana Cloud / Datadog later if needed.

Candidate alerts:

- QC heartbeat stale.
- Daily feature snapshot stale.
- yfinance backfill stale.
- Playground forward samples below threshold.
- Pipeline failed.
- Cron failed.
- Risk rejection repeats for same root cause.
- Proposal shaping clips unusually large turnover.
- Execution audit failed or skipped unexpectedly.

Initial implemented alerts:

- QC heartbeat stale or missing.
- Daily feature snapshot stale or missing.
- yfinance backfill stale or missing.
- Pipeline status failed/error/timeout.
- Recent cron failures.
- Latest execution failed/timeout/skipped/error.

Alert state is stored in `system_config.operational_alert_state_v1` to suppress
repeats within a cooldown window. This state is only alert metadata and does not
modify trading authorization, proposals, approvals, or execution state.

## Phase E: Grafana / Datadog Integration

Status: future.

The custom dashboard should come first because it can show trading-specific
state directly.

Grafana or Datadog can be added later for:

- infrastructure metrics
- logs
- traces
- uptime
- alert routing
- longer retention dashboards

Recommended future path:

```text
custom dashboard = trading/operator view
Grafana/Datadog  = infrastructure and alerting view
```

## Security Checklist

- Dashboard auth enabled before deploy.
- No write endpoints.
- No forms that mutate system state.
- No secrets rendered into HTML.
- Raw payload views are redacted or disabled by default.
- Logs do not print DB URL or auth token.
- Railway service variables are scoped to the dashboard service.
- Dashboard service can be paused independently from the trading webhook service.

## Non-Goals

- No trade execution.
- No `/confirm`, `/skip`, or `/pause` replacement.
- No LLM chat interface.
- No broker control panel.
- No public portfolio performance marketing page.
- No full observability stack replacement in the first version.
