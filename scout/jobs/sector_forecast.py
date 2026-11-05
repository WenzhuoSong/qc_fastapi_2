"""Sector forecasts (pre-registered in `scout/knowledge/external_specs/sector_forecast_v1.yaml`):
`python -m scout.jobs.sector_forecast compute`.

Local only (Sharadar prices, market caps and fundamentals, French data library; no database); writes summary
numbers to git-ignored `exports/sector_forecast/result.json` and refuses a second run.
"""
import json
import sys
from pathlib import Path

from scout import sector_forecast as sf

OUT = Path("exports/sector_forecast/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.sector_forecast compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = sf.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(sf.report_text(json.loads(OUT.read_text())), flush=True)
