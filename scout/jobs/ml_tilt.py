"""Tilt-portfolio test (pre-registered in scout/knowledge/external_specs/ml_tilt_v1.yaml), local only:
`python -m scout.jobs.ml_tilt discovery` | `confirm` (once; refuses when exports/ml/tilt_confirm_v1.json
exists) | `report`.
"""
import json
import logging
import sys
import time

from scout.ml import tilt

logger = logging.getLogger("scout.jobs.ml_tilt")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    args = sys.argv[1:]
    t0 = time.time()
    if args[:1] == ["discovery"]:
        print(tilt.report_text(tilt.run_discovery()))
    elif args[:1] == ["confirm"]:
        conf = tilt.run_confirmation()
        print(tilt.report_text(json.loads(tilt.DISCOVERY_RESULT.read_text()), conf))
    elif args[:1] == ["report"]:
        conf = json.loads(tilt.RESULT.read_text()) if tilt.RESULT.exists() else None
        print(tilt.report_text(json.loads(tilt.DISCOVERY_RESULT.read_text()), conf))
    else:
        raise SystemExit("usage: python -m scout.jobs.ml_tilt discovery | confirm | report")
    logger.info("done in %.0fs", time.time() - t0)


if __name__ == "__main__":
    main()
