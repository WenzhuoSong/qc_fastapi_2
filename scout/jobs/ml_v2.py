"""ML stage 2 (pre-registered in scout/knowledge/external_specs/ml_v2.yaml), local only:
`python -m scout.jobs.ml_v2 run <21|63> [--threads N]` | `report`; stage 2b (ml_v2b.yaml):
`run2b <21|63> [--threads N]` (resumes finished variants) | `report2b`.

run: one horizon's walk-forward (discovery period only) → exports/ml/results_v2_y<N>.json.
report: both horizons' results as text.
"""
import json
import logging
import sys
import time

from scout.ml import features, gbdt

logger = logging.getLogger("scout.jobs.ml_v2")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    args = sys.argv[1:]
    if args[:1] == ["run"] and len(args) >= 2:
        n = int(args[1])
        threads = int(args[args.index("--threads") + 1]) if "--threads" in args else 5
        t0 = time.time()
        res = gbdt.run(n, threads)
        (features.OUT / f"results_v2_y{n}.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
        logger.info("horizon %d done in %.0fs", n, time.time() - t0)
    elif args[:1] == ["run2b"] and len(args) >= 2:
        n = int(args[1])
        threads = int(args[args.index("--threads") + 1]) if "--threads" in args else 5
        path = features.OUT / f"results_v2b_y{n}.json"
        done = json.loads(path.read_text()) if path.exists() else {}

        def save(res):
            path.write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
            logger.info("horizon %d: %s saved", n, ", ".join(res))
        t0 = time.time()
        gbdt.run_2b(n, threads, done, save)
        logger.info("horizon %d stage 2b done in %.0fs", n, time.time() - t0)
    elif args[:1] == ["report2b"]:
        res = {n: json.loads((features.OUT / f"results_v2b_y{n}.json").read_text()) for n in features.HORIZONS}
        print(gbdt.report_2b(res))
    elif args[:1] == ["report"]:
        res = {n: json.loads((features.OUT / f"results_v2_y{n}.json").read_text()) for n in features.HORIZONS}
        print(gbdt.report_text(res))
    else:
        raise SystemExit("usage: python -m scout.jobs.ml_v2 run|run2b <21|63> [--threads N] | report | report2b")


if __name__ == "__main__":
    main()
