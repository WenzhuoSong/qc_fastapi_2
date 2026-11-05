"""Weekly direction (pre-registered in `scout/knowledge/external_specs/weekly_direction_v1.yaml`):
`python -m scout.jobs.weekly_direction compute`.

Local only (Sharadar prices, French RF, FRED fetched at run time; no database); writes summary numbers to
git-ignored `exports/weekly_direction/result.json` and refuses a second run.
"""
import asyncio
import json
import sys
from pathlib import Path

from scout import weekly_direction as wd

OUT = Path("exports/weekly_direction/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.weekly_direction compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = wd.run(asyncio.run(wd.load_macro()))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(wd.report_text(json.loads(OUT.read_text())), flush=True)
