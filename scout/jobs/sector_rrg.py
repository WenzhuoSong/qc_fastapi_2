"""Sector rotation quadrants (pre-registered in `scout/knowledge/external_specs/sector_rrg_v1.yaml`):
`python -m scout.jobs.sector_rrg compute`.

Local only (Sharadar ETF prices, French data library; no database); writes summary numbers to git-ignored
`exports/sector_rrg/result.json` and refuses a second run.
"""
import json
import sys
from pathlib import Path

from scout import sector_rrg as sr

OUT = Path("exports/sector_rrg/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.sector_rrg compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = sr.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(sr.report_text(json.loads(OUT.read_text())), flush=True)
