"""Intraday monitor: `python -m scout.jobs.intraday [--no-push] [--force]`.

Railway cron `*/15 13-21 * * 1-5` (UTC) covers the session in both summer and
winter time; slots while the market is closed exit at once. --force runs
outside the session (for testing)."""
import sys

from scout.intraday import run as run_intraday
from scout.jobs._run import run

if __name__ == "__main__":
    run("intraday", lambda: run_intraday(push="--no-push" not in sys.argv, force="--force" in sys.argv))
