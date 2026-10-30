"""Risk model (pre-registered in scout/knowledge/external_specs/ml_risk_v1.yaml), local only, discovery 2004–2021:
`python -m scout.jobs.ml_risk build` | `vol` | `crash` | `lgb` (own process: LightGBM) | `report [--cached]`.
Nothing after 2021-12-31 is read.
"""
import json
import logging
import sys
import time

from scout.ml import features, risk

logger = logging.getLogger("scout.jobs.ml_risk")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    args = sys.argv[1:]
    t0 = time.time()
    if args[:1] == ["build"]:
        out = risk.build()
        path = features.save(out, risk.STORE)
        logger.info("risk store: %d rows × %d columns, crash rate %.3f → %s", len(out), out.shape[1], out["t_crash"].mean(), path)
    elif args[:1] == ["vol"]:
        risk.run_vol()
    elif args[:1] == ["crash"]:
        risk.run_crash()
    elif args[:1] == ["lgb"]:
        risk.run_lgb()
    elif args[:1] == ["report"]:
        path = features.OUT / "results_risk_v1.json"
        res = json.loads(path.read_text()) if path.exists() and "--cached" in args else risk.report()
        print(risk.report_text(res))
    else:
        raise SystemExit(__doc__)
    logger.info("%s done in %.0fs", " ".join(args), time.time() - t0)


if __name__ == "__main__":
    main()
