"""ML stage 5 (pre-registered in scout/knowledge/external_specs/ml_v5.yaml), local only:
`python -m scout.jobs.ml_v5 build` (stage-5 extras for store v1, discovery period) |
`ridge <full|peer|own|sector|ctx_x> <21|63>` | `shrink <21|63>` | `dnn <single_21|single_63|multi>` | `report [--cached]`.
"""
import asyncio
import json
import logging
import sys
import time
from datetime import date

from scout.data import fred
from scout.ml import features, stage5

logger = logging.getLogger("scout.jobs.ml_v5")


async def _fred() -> dict:
    end = date.fromisoformat(stage5.UNTIL)
    return {sid: await fred.series(sid, date(1997, 1, 1), end) for sid in ("BAA10Y", "DGS10")}


def build() -> None:
    t0 = time.time()
    macro = asyncio.run(_fred())
    if not all(macro.values()):
        raise SystemExit("FRED series missing")
    out = stage5.build(macro)
    path = features.save(out, stage5.STORE)
    logger.info("stage-5 extras: %d rows, %d columns → %s (%.0fs)", len(out), out.shape[1], path, time.time() - t0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)       # request URLs can carry tokens
    args = sys.argv[1:]
    t0 = time.time()
    if args[:1] == ["build"]:
        build()
    elif args[:1] == ["ridge"] and len(args) == 3:
        stage5.run_ridge(args[1], int(args[2]))
    elif args[:1] == ["shrink"] and len(args) == 2:
        stage5.run_shrink(int(args[1]))
    elif args[:1] == ["dnn"] and len(args) == 2:
        stage5.run_dnn(args[1])
    elif args[:1] == ["report"]:
        res = {}
        for n in features.HORIZONS:
            path = features.OUT / f"results_v5_y{n}.json"
            res[n] = json.loads(path.read_text()) if path.exists() and "--cached" in args else stage5.report(n)
        print(stage5.report_text(res))
    else:
        raise SystemExit(__doc__)
    logger.info("%s done in %.0fs", " ".join(args), time.time() - t0)


if __name__ == "__main__":
    main()
