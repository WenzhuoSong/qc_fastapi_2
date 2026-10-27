"""Daily research data for the research pool: `python -m scout.jobs.research [--limit N]`.

Railway cron `30 23 * * 1-5` (UTC), after the after-close scan: SEC filings and
financials, estimates, rating changes, ownership, FRED, price cross-check."""
import sys

from scout.jobs._run import run
from scout.research_data import run_daily

if __name__ == "__main__":
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
    run("research", lambda: run_daily(limit))
