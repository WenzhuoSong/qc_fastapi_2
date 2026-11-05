"""Read-only inspection run: `railway run -s Postgres .venv/bin/python -m scout.jobs.prod_read <script.py> [args ...]`
(user's decision 2026-11-05: the only production command Claude Code may run without asking). From a laptop the
database is reached at the Postgres service's public address (DATABASE_PUBLIC_URL); the internal one only
resolves inside Railway.

Before the script runs: every database session is opened read-only (Postgres `default_transaction_read_only`,
checked once), the Telegram and OpenAI keys are blanked so nothing is sent or spent, and migrations / job
bookkeeping are skipped. A guard against accidental writes, not a sandbox: anything that has to write (a job,
`eval_agent`, which records LLM usage) still runs separately with the user's approval.
"""
from __future__ import annotations

import asyncio
import logging
import os
import runpy
import sys

BLANKED = ("OPENAI_API_KEY", "SCOUT_TG_BOT_TOKEN", "TG_BOT_TOKEN", "SCOUT_TG_CHAT_ID", "TG_CHAT_ID")


def lock_down() -> None:
    os.environ["SCOUT_DB_READ_ONLY"] = "1"
    if os.environ.get("DATABASE_PUBLIC_URL"):
        os.environ["DATABASE_URL"] = os.environ["DATABASE_PUBLIC_URL"]
    for key in BLANKED:
        os.environ[key] = ""
    from scout.config import get_scout_settings

    get_scout_settings.cache_clear()


async def check() -> None:
    from scout import db

    row = await db.fetch_one("SHOW default_transaction_read_only")
    await db.get_engine().dispose()
    db.get_engine.cache_clear()
    db._sessionmaker.cache_clear()
    if (row or {}).get("default_transaction_read_only") != "on":
        raise SystemExit("connection is not read-only; refusing to run")


def main(argv: list[str]) -> None:
    if not argv:
        raise SystemExit("usage: python -m scout.jobs.prod_read <script.py> [args ...]")
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)       # request URLs can carry tokens
    lock_down()
    asyncio.run(check())
    sys.argv = argv
    runpy.run_path(argv[0], run_name="__main__")


if __name__ == "__main__":
    main(sys.argv[1:])
