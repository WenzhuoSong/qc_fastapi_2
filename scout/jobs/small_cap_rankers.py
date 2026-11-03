"""Small-cap rankers (pre-registered in `scout/knowledge/external_specs/small_cap_rankers_v1.yaml`):
`python -m scout.jobs.small_cap_rankers compute`. Local only; writes git-ignored `exports/small_cap/result.json`
and refuses a second run.
"""
import json
import sys

from scout import small_cap_rankers as scr

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.small_cap_rankers compute")
    if (scr.OUT / "result.json").exists():
        raise SystemExit("already run (one run per pre-registration)")
    path = scr.save(scr.run())
    print(scr.report_text(json.loads(path.read_text())), flush=True)
