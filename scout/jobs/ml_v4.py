"""ML stage 4 (pre-registered in scout/knowledge/external_specs/ml_v4.yaml), local only:
`python -m scout.jobs.ml_v4 build` (feature store v2, top 3000, discovery period) |
`lgb <sec|res> <u1000|u3000> <21|63> [--threads N]` | `ridge <sec|res> <u1000|u3000> <21|63>` | `report`.
"""
import asyncio
import json
import logging
import sys
import time

import pandas as pd

from scout.jobs.ml_v1 import _fred
from scout.ml import features, stage4, wide

logger = logging.getLogger("scout.jobs.ml_v4")


def build() -> None:
    t0 = time.time()
    small = features.build(asyncio.run(_fred()), wide.UNTIL, "small", wide.PRICES_UNTIL)
    features.save(small, "raw_small_v2")
    logger.info("small part: %d rows, %.0fs", len(small), time.time() - t0)
    large = pd.read_parquet(features.OUT / "raw_v1.parquet")
    large = large[pd.to_datetime(large["date"]) <= pd.Timestamp(wide.UNTIL)]
    raw = wide.merge(large, small)
    proc = features.process(raw)
    proc["in_large"], proc["vol_63_raw"] = raw["in_large"].to_numpy(), raw["vol_63"].to_numpy()
    path = features.save(proc, "features_v2")
    logger.info("store v2: %d rows (%d top 1000), %d columns → %s (%.0fs)", len(proc), int(proc["in_large"].sum()),
                proc.shape[1], path, time.time() - t0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)       # request URLs can carry tokens
    args = sys.argv[1:]
    t0 = time.time()
    if args[:1] == ["build"]:
        build()
    elif args[:1] in (["lgb"], ["ridge"]) and len(args) >= 4:
        label, store, n = args[1], args[2], int(args[3])
        if args[0] == "lgb":
            threads = int(args[args.index("--threads") + 1]) if "--threads" in args else 5
            stage4.run_lgb(label, store, n, threads)
        else:
            stage4.run_ridge(label, store, n)
        logger.info("%s %s %s y%d done in %.0fs", args[0], label, store, n, time.time() - t0)
    elif args[:1] == ["report"]:
        res = {}
        for n in features.HORIZONS:
            path = features.OUT / f"results_v4_y{n}.json"
            res[n] = json.loads(path.read_text()) if path.exists() and "--cached" in args else stage4.report(n)
        print(stage4.report_text(res))
    else:
        raise SystemExit("usage: python -m scout.jobs.ml_v4 build | lgb|ridge <sec|res> <u1000|u3000> <21|63> | report")


if __name__ == "__main__":
    main()
