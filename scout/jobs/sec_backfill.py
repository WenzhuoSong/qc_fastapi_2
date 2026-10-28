"""Multi-angle data for the whole universe: `python -m scout.jobs.sec_backfill [--limit N] [--no-facts] [--no-insiders]`.

SEC filings and XBRL financials for every scan stock, and open-market insider trades from the SEC
quarterly data sets. Idempotent; the weekly job runs it to stay current.
"""
import sys
from datetime import date

from scout import research_data
from scout.jobs._run import run


async def main() -> dict:
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
    out = {}
    if "--no-facts" not in sys.argv:
        out["sec"] = await research_data.universe_sec(date.today(), limit)
    if "--no-insiders" not in sys.argv:
        out["insiders"] = await research_data.insiders(date.today())
    return out


if __name__ == "__main__":
    run("sec_backfill", main, alert=False)
