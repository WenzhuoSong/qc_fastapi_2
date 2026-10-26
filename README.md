# noble-flow · Scout

**Scout** is a US-equity opportunity observer for a single user. After every close it scans ~950 stocks and ETFs, finds rule-based setups, writes evidence-backed opportunity cards (Python numbers, LLM prose), tracks every card to an outcome, and answers questions via Telegram and a dashboard. It never trades.

A shared news cron (`cron/pre_fetch_news.py`, 24/7 multi-source news: Finnhub + Alpha Vantage + RSS, every `50 */2 * * *`) feeds the news tables Scout reads.

Start here: [docs/project_overview_zh.md](docs/project_overview_zh.md) (architecture, deployment, code map, extension guide, known issues).
Design: [docs/scout_opportunity_observer_design_v1_zh.md](docs/scout_opportunity_observer_design_v1_zh.md)
Reusable legacy code: [docs/legacy_reusable_catalog_zh.md](docs/legacy_reusable_catalog_zh.md) (git tag `legacy-trading-system`)
Archived QC observer: git tag `qc-observer-final`, plus `~/Work/trading/qc_observer_archive/` locally.

```
bars (yfinance, free) ─► feature panel ─► detectors ─► ranker ─► thesis (LLM + number guard) ─► cards
                                              │                                                  │
                         base rates (3y replay, same code) ◄── tracker (same bracket simulator) ◄┘
```

## Run locally

```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql://user@localhost:5432/scout
python -m scout.jobs.backfill              # universe + 3y bars + base rates + features (~1 min)
python -m scout.jobs.eod --no-push --force # after-close scan
SCOUT_DASHBOARD_USER=u SCOUT_DASHBOARD_PASSWORD=p uvicorn scout.web.app:app --port 8765
python -m unittest discover -s tests -t .
```

## Railway services (project `noble-flow`)

| Service | Start command | Schedule (UTC) |
|---|---|---|
| `scout-web` | `uvicorn scout.web.app:app` (Dockerfile default) | always on |
| `scout-eod` | `python -m scout.jobs.eod` | `30 21 * * 1-5` |
| `scout-weekly` | `python -m scout.jobs.weekly` | `0 14 * * 6` |
| `cron-pre-fetch-news` | `python -m cron.pre_fetch_news` | `50 */2 * * *` (24/7 multi-source news) |
| `Postgres` | — | — |

Shared secrets live in the environment's Shared Variables and are referenced as `${{shared.X}}`.

Telegram webhook: `https://<scout-web domain>/telegram` with `secret_token=$SCOUT_TG_WEBHOOK_SECRET`.
