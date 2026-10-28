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
- Exits are part of a rule (`Detector.exit`: `fixed` or `trail_low`), and replay and live tracking share `scout/simulate.py`. Change an exit only when the replay improves in both halves of the sample and theory supports it.
- A rule's edge is judged net of `simulate.ROUND_TRIP_COST` and against just holding the same stock (`avg_edge_vs_hold`), not against SPY alone. New rules start as candidates (`Detector(live=False)`: replayed and shown on the scoreboard, never scanned or pushed) until their replay shows an independent edge.
- Every LLM-facing number comes with its meaning: add new features to `scout/knowledge/features.yaml` (tests enforce it). The ticker "AI 解读" is the `scout/insight.py` workflow: evidence pack → step one, one call per lens group (`GROUPS`, in parallel, each sees only its lenses and `GROUP_CONTEXT`) → step two, one synthesis call → deterministic validation per call, fallback per part. Each lens file in `scout/knowledge/lenses/NN_*.md` must belong to exactly one group (tested); edit methodology there, and rerun `python -m scout.jobs.eval_insight` after changing prompts, lenses or models.
- Web search (OpenAI Responses `web_search_preview`) is external evidence: the cause write-up must cite it, the page labels it "据外部报道（未经核实）", and numbers in prose still come from the evidence pack.
- Research data is point in time: snapshots are appended per day and financials keep every filed version (restatements add rows). Never prune history or analysis to save storage; the daily job warns at 80 % of `SCOUT_DB_VOLUME_GB` and the user expands the volume.
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
| `scout/premarket.py` | Pre-market brief: overnight quotes, opportunity positions, earnings, economic calendar (Forex Factory feed), news → LLM + checks |
| `scout/events.py` | Abnormal moves (idio ≥ 3σ ex ante, ≥ 8%, or 3× volume): detection per ticker + post-event base rates over the universe |
| `scout/event_cause.py` | Why it moved: dated news, SEC filings, insiders, earnings, web search → cause taxonomy, validated, cached in `scout.stock_events` |
| `scout/fundamentals.py` | Finnhub free fundamentals / analyst mix / EPS surprises / insiders, cached daily (`scout.fundamentals`); raw calls in `scout/data/finnhub.py` |
| `scout/research_data.py` | v2 research pool data, point in time: SEC filings + XBRL quarterly financials (`scout/data/sec.py`), Yahoo estimates / ratings / ownership / options (`scout/data/yahoo_research.py`, `scout/data/options.py`), FRED (`scout/data/fred.py`), price cross-check, storage check; `/data` page |
| `scout/research.py` | v2 phase-3 lens values (pure functions + point-in-time loader): fundamental trend, valuation vs history / sector, expectations & revisions, earnings reactions, ownership & short interest, options, macro sensitivity |
| `scout/ledger.py` | v2 judgment ledger: every AI view (overall + per lens), opportunity card and sector heat with its date and price; scored at 5/20/60 sessions vs SPY and sector; calibration (≥ 30 samples) shown on `/ledger`, in the weekly report and to the AI; per-ticker history fed back |
| `scout/levels.py` | Price map for any stock, computed after the close and stored per data date (`scout.price_levels`; pool in the research job, others on first view): supports / resistances 1–3, moving averages, pivot with preferred / alternative scenario, options ±1σ and analyst range, pullback / breakout buy points (zone min(0.5 ATR, 5 %), stop ≥ 1 ATR, targets, time stop). The live price only changes the status. A recorded buy gets its plan from the map before the buy (`scout.trades.plan`), managed daily by `portfolio.manage_trade`. Labelled as not replayed; rule plans stay fixed on the signal day, shown as a status line plus folded tracking |
| `scout/portfolio.py` | User side: account settings, position sizing, trade journal (ticker, date, fill, shares; partial sells in `scout.trade_sells`, web or Telegram `/sell`), concentration; the system sets each open trade's stop / target (`manage_trade`: plan from the pre-buy price map, chandelier stop only rising; a third at target 1, the rest trails, per the exit study, via `levels.exit_plan`) after every close |
| `scout/intraday.py` | Session monitor: price levels of trades / opportunities and direct news, each alert sent once (`scout.alerts`) |
| `scout/exit_study.py` | Trailing-stop study for the holdings plan (scale out in thirds, the rest trailing): policies replayed over every rule signal, net of cost, vs holding, per half; run `python -m scout.jobs.eval_exits` (stored as brief `exit_study`) |
| `scout/jobs/` | `premarket`, `intraday`, `options`, `eod`, `research`, `weekly`, `backfill`, `eval_insight`, `eval_exits`, `eval_levels`, `eval_rules` (fair benchmarks: SPY, equal weight, random-date and random-stock controls) entrypoints (`python -m scout.jobs.<name>`) |

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
