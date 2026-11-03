"""Low-turnover selections (pre-registered in `scout/knowledge/external_specs/slow_factors_v1.yaml`):
`python -m scout.jobs.slow_factors compute`. Local only; writes git-ignored `exports/slow_factors/result.json`
and refuses a second run.
"""
import json
import sys

from scout import slow_factors as sf

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.slow_factors compute")
    if (sf.OUT / "result.json").exists():
        raise SystemExit("already run (one run per pre-registration)")
    path = sf.save(sf.run())
    text = sf.report_text(json.loads(path.read_text()))
    (sf.OUT / "report.txt").write_text(text)
    print(text, flush=True)
