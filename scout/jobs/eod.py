"""Weekday after-close scan: `python -m scout.jobs.eod [--no-push] [--force]`.

--force rescans a session that was already scanned (or runs on a non-trading
day against the previous session), e.g. to regenerate cards after a fix."""
import sys

from scout.jobs._run import run
from scout.pipeline import run_eod

if __name__ == "__main__":
    run("eod", lambda: run_eod(push="--no-push" not in sys.argv, force="--force" in sys.argv))
