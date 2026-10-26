# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What lives here

**Scout** (`scout/`): a US-equity opportunity observer for a single user. It scans, ranks, explains and tracks opportunities, and answers questions through Telegram and a dashboard. It never trades.

`cron/pre_fetch_news.py` (+ `services/*_client.py`, `news_summarizer.py`, `db/models.py`) is the shared news pipeline: it writes `ticker_news_library` / `ticker_news_archive`, which Scout reads.

Archived, not in this tree: the QC observer (git tag `qc-observer-final`, local copy `~/Work/trading/qc_observer_archive/`) and the legacy agentic trading system (tag `legacy-trading-system`, reusable parts listed in `docs/legacy_reusable_catalog_zh.md`).

## Hard boundaries

- Scout must not import trading/execution code; the news pipeline must not import `scout`. Both enforced by `tests/test_scout_core.py::ScoutFenceTests`.
- Nothing in this repo sends orders or commands to QuantConnect or a broker.
- In Scout, numbers come from Python. The LLM writes prose around Python-computed evidence; `scout/thesis.py` rejects numbers that cannot be traced to the evidence.
- Do not tune detector parameters to the replay results. `detector_base_rates` measures the rules; it is not an optimisation target.
- A rule's edge is judged net of `simulate.ROUND_TRIP_COST` and against just holding the same stock (`avg_edge_vs_hold`), not against SPY alone. New rules start as candidates (`Detector(live=False)`: replayed and shown on the scoreboard, never scanned or pushed) until their replay shows an independent edge.
- Never log secrets: httpx INFO logging is silenced because Telegram/Finnhub URLs carry tokens.

## Scout layout

| Module | Role |
|---|---|
| `scout/config.py` | `ScoutSettings` (env vars, prefix `SCOUT_` for Scout-only knobs) |
| `scout/db.py` | Own async engine, text-SQL helpers, `apply_migrations()`, `job_run()` |
| `scout/migrations/*.sql` | Idempotent schema `scout.*`, applied at every job/web start |
| `scout/universe.py` | Seed watchlist, context ETFs, S&P 500/400 + Nasdaq-100 constituents (Wikipedia) |
| `scout/data/` | `MarketDataProvider` protocol; `yahoo.py` (free daily bars) |
| `scout/bars.py` | Bar storage: backfill, incremental refresh with split/re-adjust detection |
| `scout/features.py` | Wide (date × ticker) `FeaturePanel`; regime from SPY |
| `scout/detectors/` | `Detector` contract + `rules.py` (5 rules). `mask(panel)` → bool frame, `levels(row)` → entry/stop/targets |
| `scout/simulate.py` | Bracket simulator shared by replay and live tracking |
| `scout/base_rates.py` | Replay → `scout.detector_base_rates` (per regime) |
| `scout/opportunities.py` | Scan, score, evidence bundle, persistence, feedback weights |
| `scout/context.py` | News (shared library + Finnhub on demand) and earnings calendar |
| `scout/thesis.py` | LLM thesis + number guard + template fallback |
| `scout/tracker.py` | Lifecycle updates, outcomes, scoreboard |
| `scout/telegram.py` | Card rendering, push, commands, feedback callbacks |
| `scout/agent.py` | Read-only tool-calling Q&A agent |
| `scout/pipeline.py` | `run_eod` (skips holidays, scans/pushes a session once; `--force` reruns), `run_weekly`, `prepare`, `ensure_base_rates` |
| `scout/market_calendar.py` | US equity trading days / holidays |
| `scout/web/` | FastAPI app (webhook + dashboard) and HTML views |
| `scout/jobs/` | `eod`, `weekly`, `backfill` entrypoints (`python -m scout.jobs.<name>`) |

Reusable pieces of the removed trading system are catalogued in `docs/legacy_reusable_catalog_zh.md`; retrieve and adapt them rather than rewriting from scratch.

To add a detector, append a `Detector` to `rules.DETECTORS`, then rerun `python -m scout.jobs.backfill` (or wait for the weekly job) so its base rate exists before it ranks well.

## Commands

```bash
python -m unittest discover -s tests -t .      # full test suite (unittest, no pytest)
python -m scout.jobs.backfill                  # bootstrap Scout data
python -m scout.jobs.eod --no-push [--force]    # after-close scan without Telegram (--force: rescan/holiday)
uvicorn scout.web.app:app --port 8765          # Scout web (needs SCOUT_DASHBOARD_USER/PASSWORD)
```

## Conventions

- Async everywhere (SQLAlchemy async + asyncpg). Each cron is its own process with one `asyncio.run`.
- Scout SQL is explicit text against the `scout` schema; schema changes go in a new numbered file in `scout/migrations/`.
- User-facing text (Telegram, dashboard, LLM output) is Chinese; code and comments are English.
- Tests use `unittest`; anything that needs a database is mocked or exercised manually against a local Postgres.
