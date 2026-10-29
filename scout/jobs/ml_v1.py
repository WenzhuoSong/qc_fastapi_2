"""ML stage 1 (pre-registered in scout/knowledge/external_specs/ml_v1.yaml), local only:
`python -m scout.jobs.ml_v1 build [--until YYYY-MM-DD]` | `baselines` (discovery period only).

build: raw features and labels → exports/ml/raw_v1.parquet, typed operators → features_v1.parquet
(git-ignored, Sharadar-derived). Prints row counts and coverage only, never metrics.
"""
import asyncio
import logging
import sys
import time
from datetime import date

import json

from scout.data import fred
from scout.ml import baselines, features

logger = logging.getLogger("scout.jobs.ml_v1")


async def _fred() -> dict:
    out = {}
    for sid in ("VIXCLS", "T10Y2Y"):
        out[sid] = await fred.series(sid, date(1997, 1, 1))
    return out


def build(until: str | None) -> None:
    t0 = time.time()
    raw = features.build(asyncio.run(_fred()), until)
    suffix = "_smoke" if until else ""
    features.save(raw, f"raw_v1{suffix}")
    logger.info("raw: %d rows, %d dates, %.0fs", len(raw), raw["date"].nunique(), time.time() - t0)
    proc = features.process(raw)
    path = features.save(proc, f"features_v1{suffix}")
    cov = raw[features.PRICE + features.FUND + features.EVENTS].notna().mean().round(3).to_dict()
    logger.info("features: %d columns → %s (%.0fs); coverage %s", proc.shape[1], path, time.time() - t0, cov)


def run_baselines() -> None:
    t0 = time.time()
    res = baselines.run()
    (features.OUT / "baselines_v1.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    print(baselines.report_text(res), flush=True)
    logger.info("baselines done in %.0fs", time.time() - t0)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)       # request URLs can carry tokens
    args = sys.argv[1:]
    if args[:1] == ["build"]:
        until = args[args.index("--until") + 1] if "--until" in args else None
        build(until)
    elif args[:1] == ["baselines"]:
        run_baselines()
    else:
        raise SystemExit("usage: python -m scout.jobs.ml_v1 build [--until YYYY-MM-DD] | baselines")


if __name__ == "__main__":
    main()
