# Migrations for the shared news tables

Scout's own schema lives in `scout/migrations/` and is applied automatically.
This folder holds the explicit, reviewed SQL for the `public` tables still in use
(`cron_run_log`, `ticker_news_archive`; `ticker_news_library` and
`macro_news_cache` are created by `db.session.init_db`).

Rules: one dated file per change, idempotent SQL (`IF NOT EXISTS`), never
destructive (`tests/test_migration_safety.py` enforces this), and run it
before deploying code that depends on it.

History for the dropped legacy trading tables and the archived QC observer
tables is kept under git tags `legacy-trading-system` and `qc-observer-final`.
