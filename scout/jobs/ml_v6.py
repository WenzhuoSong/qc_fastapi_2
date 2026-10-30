"""ML stage 6 (pre-registered in scout/knowledge/external_specs/ml_v6.yaml), local only, discovery 2004–2021:
`python -m scout.jobs.ml_v6 build` | `baseline <21|63>` | `stock <21|63>` | `sector <21|63>` | `two <21|63>` |
`dnn <single|multi>` | `report [--cached]`. Nothing after 2021-12-31 is read.
"""
import asyncio
import json
import logging
import sys
import time
from datetime import date

from scout.data import fred
from scout.ml import features, stage6

logger = logging.getLogger("scout.jobs.ml_v6")


async def _fred() -> dict:
    end = date.fromisoformat(stage6.UNTIL)
    return {sid: await fred.series(sid, date(1997, 1, 1), end) for sid in ("BAA10Y", "DGS10")}


def build() -> None:
    t0 = time.time()
    macro = asyncio.run(_fred())
    if not all(macro.values()):
        raise SystemExit("FRED series missing")
    stock, sector = stage6.build(macro)
    a, b = features.save(stock, stage6.STORE), features.save(sector, stage6.SECTOR_STORE)
    logger.info("stage-6 store: %d rows × %d columns → %s; sector table %d rows → %s (%.0fs)",
                len(stock), stock.shape[1], a, len(sector), b, time.time() - t0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)       # request URLs can carry tokens
    args = sys.argv[1:]
    t0 = time.time()
    if args[:1] == ["build"]:
        build()
    elif args[:1] == ["baseline"] and len(args) == 2:
        stage6.run_baseline(int(args[1]))
    elif args[:1] == ["stock"] and len(args) == 2:
        stage6.run_stock(int(args[1]))
    elif args[:1] == ["sector"] and len(args) == 2:
        stage6.run_sector(int(args[1]))
    elif args[:1] == ["two"] and len(args) == 2:
        stage6.run_two_layer(int(args[1]))
    elif args[:1] == ["dnn"] and len(args) == 2:
        stage6.run_dnn(args[1])
    elif args[:1] == ["report"]:
        res = {}
        for n in features.HORIZONS:
            path = features.OUT / f"results_v6_y{n}.json"
            res[n] = json.loads(path.read_text()) if path.exists() and "--cached" in args else stage6.report(n)
        print(stage6.report_text(res))
    else:
        raise SystemExit(__doc__)
    logger.info("%s done in %.0fs", " ".join(args), time.time() - t0)


if __name__ == "__main__":
    main()
