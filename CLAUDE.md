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
| `scout/pipeline.py` | `run_eod` (skips holidays, scans/pushes a session once; `--force` reruns; cards of rules the knowledge base rejects are saved and tracked but not pushed, `learned.held_back`), `run_weekly`, `prepare`, `ensure_base_rates` |
| `scout/market_calendar.py` | US equity trading days / holidays |
| `scout/web/` | FastAPI app (webhook + dashboard) and HTML views |
| `scout/premarket.py` | Pre-market brief: overnight quotes, opportunity positions, earnings, economic calendar (Forex Factory feed), news → LLM + checks |
| `scout/events.py` | Abnormal moves (idio ≥ 3σ ex ante, ≥ 8%, or 3× volume): detection per ticker + post-event base rates over the universe |
| `scout/event_cause.py` | Why it moved: dated news, SEC filings, insiders, earnings, web search → cause taxonomy, validated, cached in `scout.stock_events` |
| `scout/fundamentals.py` | Finnhub free fundamentals / analyst mix / EPS surprises / insiders, cached daily (`scout.fundamentals`); raw calls in `scout/data/finnhub.py` |
| `scout/research_data.py` | v2 research pool data, point in time: SEC filings + XBRL quarterly financials (`scout/data/sec.py`), Yahoo estimates / ratings / ownership / options (`scout/data/yahoo_research.py`, `scout/data/options.py`), FRED (`scout/data/fred.py`), price cross-check, storage check; `/data` page |
| `scout/research.py` | v2 phase-3 lens values (pure functions + point-in-time loader): fundamental trend, valuation vs history / sector, expectations & revisions, earnings reactions, ownership & short interest, options, macro sensitivity |
| `scout/ledger.py` | v2 judgment ledger: every AI view (overall + per lens), opportunity card and sector heat with its date and price; scored at 5/20/60 sessions vs SPY and sector; calibration (≥ 30 samples) shown on `/ledger`, in the weekly report and to the AI; per-ticker history fed back |
| `scout/learned.py` | v3 knowledge base (`scout.knowledge`, versioned): claim, mechanism, context, evidence (study briefs), status on the gate hypothesis → backtested → forward → confirmed (only the user confirms; judgments skip the backtest), negative findings kept as `rejected`, re-check dates. Seeds in `scout/knowledge/learned.yaml` (inserted once per key, never overwritten); `/knowledge` page; the AI interpretation gets the entries about its lenses / opportunity rule as `context.knowledge` (cited as kb_N) |
| `scout/gate.py` | v3 gate: G0 pre-registered spec (`scout/knowledge/specs/*.yaml`, never edited after a run; variants share a family and are counted in `scout.gate_runs`), G1 backtest (paired vs SPY, equal weight, timing and selection controls; per half, per regime; overlap with other rules; one random stream per rule), G2 AI critique checklist (numbers checked; no model = no pass). Pass: primary control positive in both halves, full-sample t ≥ 3 (Bonferroni bar if higher), monthly correlation ≤ 0.5 with passed rules, no failed check. Thresholds agreed 2026-10-28; never tune them. `python -m scout.jobs.gate register|run` |
| `scout/forward.py` | v3 forward test (G3): after each close (`run_eod`), every entry in `backtested` / `forward` with a select-control spec records its rule's signals and 20 selection-control legs per signal (fixed seed per entry/date/stock) in `scout.forward_trades`, resolved by `simulate`; control = mean of the first 3 legs that fill. Ready for the user's confirmation at ≥ 6 months, ≥ 30 complete signals, edge > 0 and t(forward − backtest) > −2; `/knowledge` refuses to confirm before |
| `scout/outlook.py` | v3 industry outlook, weekly (`run_weekly`, or `python -m scout.jobs.outlook`): market drivers in Python (sector / theme ETFs vs SPY, style spreads formed at each window's start, SPY − RSP, breadth), point-in-time slow information per sector from the research pool (revenue growth and acceleration, capex, EPS revisions, ratings; omitted below 5 companies), news and macro; at most 3 AI theses (target ETF, 60/120 sessions, a `judgment` method from the knowledge base, ≥ 1 non-price citation, falsification), validated, recorded as ledger `sector_thesis` vs SPY and RSP; a method moves to `forward` on first use. `/outlook` page |
| `scout/b0.py` | v3 interface with the user's quant system B0, offline only: `python -m scout.jobs.b0 export` writes `exports/b0/<date>/` (git-ignored): every knowledge entry incl. negatives, a strategy spec per gated rule (exact conditions, level formula source with its helpers, field definitions, exit, gate report, forward progress, status), point-in-time signals CSV, tilts only from user-confirmed methods; `import <yaml>` brings B0 results in as entries (`changed_by` = import, never confirmed). After each close `store_b0_rank` recomputes B0's own momentum ranking and market filter on Scout's data (brief `b0_rank`, 策略筛选 → 我的策略); B0's parameters stay out of the repo and come from `SCOUT_B0_SCORE` (JSON, `b0.MomentumScore`; unset = off). A ranking for information, not B0's holdings or a Scout recommendation. B0's own research files stay out of this repo (drafts live in git-ignored `exports/b0/imports/`). Scout sends nothing to QC |
| `scout/rankers.py` | Cross-sectional ranking strategies from the literature for the gate's ranking mode (`spec.type: rank`: every N sessions hold the picks for N sessions, buy next open; control = mean of every other stock that day): residual momentum, 52-week high, frog-in-the-pan, Minervini trend template. Each uses data up to day i only (tested) |
| `scout/angles.py` | v3 multi-angle features, point in time: earnings announcements (8-K item 2.02) and their 3-day return vs SPY; TTM financial quality from SEC XBRL as filed (gross profitability, accruals, asset growth, net issuance, revenue growth; `PointInTime` keeps every filed version); officer / director open-market buying from the SEC Form 4 data sets (`scout/data/sec_insiders.py`, table `scout.insider_trades`, 10b5-1 excluded). Daily snapshot `scout.angle_features` (run_eod), fed to the AI lenses (`ang_*` in features.yaml), screens (策略筛选 → 基本面与事件) and rankers; `ANGLES` is the registry shown on /data (source, history, point-in-time rule, validation path, verdict). Universe backfill: `python -m scout.jobs.sec_backfill` (weekly job keeps it current) |
| `scout/levels.py` | Price map for any stock, computed after the close and stored per data date (`scout.price_levels`; pool in the research job, others on first view): supports / resistances 1–3, moving averages, pivot with preferred / alternative scenario, options ±1σ and analyst range, pullback / breakout buy points (zone min(0.5 ATR, 5 %), stop ≥ 1 ATR, targets, time stop). The live price only changes the status. A recorded buy gets its plan from the map before the buy (`scout.trades.plan`), managed daily by `portfolio.manage_trade` (no partial sells; chandelier stop from target 1; 60-session cap). Labelled as not replayed; rule plans stay fixed on the signal day, shown as a status line plus folded tracking |
| `scout/portfolio.py` | User side: account settings, position sizing, trade journal (ticker, date, fill, shares; partial sells in `scout.trade_sells`, web or Telegram `/sell`), concentration; the system sets each open trade's stop / target (`manage_trade`: plan from the pre-buy price map, per the exit study's best policy: no partial sells, initial stop until target 1, then the chandelier stop (highest close since the buy − 3 ATR, only rising), all sold after 60 sessions; `levels.manage` / `levels.exit_plan`) after every close |
| `scout/intraday.py` | Session monitor: price levels of trades / opportunities and direct news, each alert sent once (`scout.alerts`) |
| `scout/exit_study.py` | Exit study for the holdings plan: policies (partial sells, trailing stops, when trailing starts) replayed over every rule signal, net of cost, vs holding, per half; run `python -m scout.jobs.eval_exits` (stored as brief `exit_study`) |
| `scout/jobs/` | `premarket`, `intraday`, `options`, `eod`, `research`, `weekly`, `backfill`, `eval_insight`, `eval_exits`, `eval_levels`, `eval_rules` (fair benchmarks for every rule, via `scout.gate`), `gate`, `outlook`, `b0`, `sec_backfill` entrypoints (`python -m scout.jobs.<name>`) |

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
