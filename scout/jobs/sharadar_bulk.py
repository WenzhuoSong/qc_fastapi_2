"""Sharadar bulk download to local files: `python -m scout.jobs.sharadar_bulk [table ...] | retype <table ...> | purge`.

Local only (option A): writes `exports/sharadar/raw/*.zip`, `parquet/*.parquet` and
`manifest.json` (git-ignored); nothing goes to the database. Run with the key in the
environment, e.g. `railway run -s scout-web -- python -m scout.jobs.sharadar_bulk`.
Each table is stored with typed columns (descriptions' unit types); `retype` redoes that step.
`purge` deletes every local copy (licence: within 30 days of ending the subscription).
"""
import logging
import os
import sys

import httpx

from scout.data import sharadar

logger = logging.getLogger("scout.jobs.sharadar_bulk")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)       # request URLs can carry tokens
    args = sys.argv[1:]
    if args == ["purge"]:
        sharadar.purge()
        logger.info("deleted %s", sharadar.ROOT)
        return
    if args[:1] == ["retype"]:
        for t in args[1:]:
            sharadar.write_manifest([sharadar.retype(t)])
            logger.info("%s typed", t)
        return
    key = os.environ.get("SCOUT_SHARADAR_API_KEY", "")
    if not key:
        raise SystemExit("SCOUT_SHARADAR_API_KEY is not set")
    tables = args or list(sharadar.TABLES)
    with httpx.Client(timeout=httpx.Timeout(60.0, read=600.0)) as client:
        for t in tables:
            try:
                d = sharadar.download(key, t, client=client)
                p = sharadar.to_parquet(t)
                if t != "descriptions":
                    p = {**p, **sharadar.retype(t)}
                sharadar.write_manifest([{**d, **p}])
                logger.info("%s: %.1f MB zip, %d rows, %.1f MB parquet", t, d["bytes"] / 1e6, p["rows"], p["parquet_bytes"] / 1e6)
            except Exception as exc:          # one table must not stop the others
                logger.warning("%s failed: %s", t, exc)


if __name__ == "__main__":
    main()
