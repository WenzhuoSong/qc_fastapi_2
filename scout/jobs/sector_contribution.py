"""Sector contribution rotation (pre-registered in `scout/knowledge/external_specs/sector_contribution_v1.yaml`):
`python -m scout.jobs.sector_contribution compute`. Local only; writes git-ignored
`exports/sector_contribution/result.json` and refuses a second run.
"""
import json
import sys
from pathlib import Path

from scout import sector_contribution as sc

OUT = Path("exports/sector_contribution/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.sector_contribution compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = sc.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(sc.report_text(json.loads(OUT.read_text())), flush=True)
