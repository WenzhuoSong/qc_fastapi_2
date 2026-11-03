"""Weekday pre-market brief: `python -m scout.jobs.premarket [--no-push] [--force]`.

The cron fires at 13:00 and 14:00 UTC on weekdays; only the firing that lands near 9:00 New York time runs
(13:00 UTC in summer time, 14:00 UTC in winter), 30 minutes before the open. Skips holidays and sessions
already briefed; --force runs now and regenerates."""
import sys

from scout.jobs._run import run
from scout.premarket import run as run_premarket

if __name__ == "__main__":
    run("premarket", lambda: run_premarket(push="--no-push" not in sys.argv, force="--force" in sys.argv))
