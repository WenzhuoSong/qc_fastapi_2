"""ML stage 3, sequence models (pre-registered in scout/knowledge/external_specs/ml_v3.yaml), local only:
`python -m scout.jobs.ml_v3 b4 <21|63>`, then `run <21|63>` | `report`.

b4: the stage-2b b4 scores rescored from its saved versions (own process: LightGBM before torch deadlocks on macOS).
run: one horizon's yearly walk-forward (discovery period only) → exports/ml/results_v3_y<N>.json.
report: both horizons' results as text.
"""
import json
import logging
import sys
import time

from scout.ml import features, sequence

logger = logging.getLogger("scout.jobs.ml_v3")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) >= 2:
        n = int(args[1])
        t0 = time.time()
        sequence.run(n)
        logger.info("horizon %d done in %.0fs", n, time.time() - t0)
    elif args[:1] == ["b4"] and len(args) >= 2:
        n = int(args[1])
        sequence.b4_scores(n).to_parquet(sequence.b4_path(n))
        logger.info("horizon %d: b4 scores written", n)
    elif args[:1] == ["report"]:
        res = {n: json.loads((features.OUT / f"results_v3_y{n}.json").read_text()) for n in features.HORIZONS}
        print(sequence.report_text(res))
    else:
        raise SystemExit("usage: python -m scout.jobs.ml_v3 b4 <21|63> | run <21|63> | report")


if __name__ == "__main__":
    main()
