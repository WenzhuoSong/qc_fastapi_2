# noble-flow

Two independent products share this repository and one Railway Postgres:

| Product | Package | Purpose |
|---|---|---|
| **Scout** | `scout/` | US-equity opportunity observer: scans ~950 stocks/ETFs after every close, finds rule-based setups, writes evidence-backed opportunity cards, tracks every card to an outcome, and answers questions via Telegram and a dashboard. |
| **QC observer** | `main.py`, `api/`, `audit/`, `dashboard/`, `services/qc_observer_*` | Read-only companion for the QuantConnect live algorithm: ingests observer events, builds projections, audits decisions, sends health/post-market notifications. `execution_authority=none` — it never sends commands to QC. |

The shared news pipeline (`cron/pre_fetch_news.py`: Finnhub + Alpha Vantage + RSS) covers the QC universe plus Scout's watchlist and open ideas.

Design: [docs/scout_opportunity_observer_design_v1_zh.md](docs/scout_opportunity_observer_design_v1_zh.md)

## Scout

```
bars (yfinance, free) ─► feature panel ─► detectors ─► ranker ─► thesis (LLM + number guard) ─► cards
                                              │                                                  │
                         base rates (3y replay, same code) ◄── tracker (same bracket simulator) ◄┘
```

- **Detectors** (`scout/detectors/rules.py`): trend breakout, pullback in uptrend, power gap, sector rotation leader, volume shock. Pure functions over a wide feature panel, so live scanning and historical replay share code.
- **Base rates** (`scout/base_rates.py`): every rule replayed over 3 years of daily bars, bucketed by market regime. Cards show these numbers. The replay uses current index members, so it has survivorship bias; the cards say so.
- **Simulator** (`scout/simulate.py`): one bracket model (trigger within 3 sessions, stop checked before target) used by both the replay and live tracking.
- **Thesis** (`scout/thesis.py`): the LLM writes prose only. Every number must trace back to the evidence or the text is retried, then replaced by a template. Prices and levels are never LLM output.
- **Delivery**: Telegram (`/ideas /idea /why /watch /stats`, free-text Q&A, 👍👎⭐🙈 feedback) and a dashboard (`/`, `/ideas`, `/ticker/{t}`, `/scoreboard`, `/watchlist`, `/chat`).

### Run locally

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://user@localhost:5432/scout
python -m scout.jobs.backfill          # universe + 3y bars + base rates (~1 min)
python -m scout.jobs.eod --no-push     # after-close scan
SCOUT_DASHBOARD_USER=u SCOUT_DASHBOARD_PASSWORD=p uvicorn scout.web.app:app --port 8765
python -m unittest discover -s tests -t .
```

### Railway services

| Service | Start command | Schedule (UTC) |
|---|---|---|
| `scout-web` | `uvicorn scout.web.app:app --host 0.0.0.0 --port $PORT` | always on |
| `scout-eod` | `python -m scout.jobs.eod` | `30 21 * * 1-5` (after close in both EDT and EST) |
| `scout-weekly` | `python -m scout.jobs.weekly` | `0 14 * * 6` |

After deploying `scout-web`, register the Telegram webhook:

```bash
curl "https://api.telegram.org/bot$SCOUT_TG_BOT_TOKEN/setWebhook?url=https://$SCOUT_DOMAIN/telegram&secret_token=$SCOUT_TG_WEBHOOK_SECRET"
```

## QC observer

| Service | Start command |
|---|---|
| `main-service` | `uvicorn main:app` (Dockerfile default) |
| `monitor` | `uvicorn dashboard.app:app` |
| `cron-morning-health` | `python -m cron.morning_health` |
| `cron-post-market-report` | `python -m cron.post_market_report` |
| `cron-pre-fetch-news` | `python -m cron.pre_fetch_news` (`50 */2 * * *`, 24/7 multi-source news) |

Contracts: [docs/newbase_fastapi_observer_contract.md](docs/newbase_fastapi_observer_contract.md), [docs/qc_newbase_live_snapshot_export_spec.md](docs/qc_newbase_live_snapshot_export_spec.md).
