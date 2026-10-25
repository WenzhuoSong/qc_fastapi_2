"""Saturday: universe refresh, full bar re-download, base rates, weekly report."""
import sys

from scout.jobs._run import run
from scout.pipeline import run_weekly

if __name__ == "__main__":
    run("weekly", lambda: run_weekly(push="--no-push" not in sys.argv))
