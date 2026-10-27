"""Options snapshot: `python -m scout.jobs.options [--force]`.

Railway cron `35,50 19,20 * * 1-5` (UTC) covers 15:35 / 15:50 ET in summer and
winter time; only a slot inside 15:30–16:00 ET while the market is open runs,
and only once per day (Yahoo option quotes are empty outside the session)."""
import sys

from scout.jobs._run import run
from scout.research_data import run_options

if __name__ == "__main__":
    run("options", lambda: run_options(force="--force" in sys.argv))
