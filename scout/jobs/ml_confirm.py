"""ML confirmation test (pre-registered in scout/knowledge/external_specs/ml_confirm_v1.yaml), local only:
`python -m scout.jobs.ml_confirm run` (once; refuses when exports/ml/confirm_v1.json exists) | `report`.
"""
import json
import logging
import sys
import time

from scout.ml import confirm

logger = logging.getLogger("scout.jobs.ml_confirm")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    args = sys.argv[1:]
    if args[:1] == ["run"]:
        t0 = time.time()
        res = confirm.run()
        logger.info("confirmation done in %.0fs", time.time() - t0)
        print(confirm.report_text(res))
    elif args[:1] == ["report"]:
        print(confirm.report_text(json.loads(confirm.RESULT.read_text())))
    else:
        raise SystemExit("usage: python -m scout.jobs.ml_confirm run | report")


if __name__ == "__main__":
    main()
