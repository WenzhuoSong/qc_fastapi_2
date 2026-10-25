"""Weekday after-close scan: `python -m scout.jobs.eod [--no-push]`."""
import sys

from scout.jobs._run import run
from scout.pipeline import run_eod

if __name__ == "__main__":
    run("eod", lambda: run_eod(push="--no-push" not in sys.argv))
