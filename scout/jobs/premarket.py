"""Weekday pre-market brief: `python -m scout.jobs.premarket [--no-push] [--force]`.

Runs at 13:00 UTC (北京时间 21:00): 30 minutes before the open in US summer
time, 90 minutes in winter. Skips holidays and sessions already briefed;
--force regenerates."""
import sys

from scout.jobs._run import run
from scout.premarket import run as run_premarket

if __name__ == "__main__":
    run("premarket", lambda: run_premarket(push="--no-push" not in sys.argv, force="--force" in sys.argv))
