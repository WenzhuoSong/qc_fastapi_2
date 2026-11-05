"""Risk warnings (pre-registered in `scout/knowledge/external_specs/risk_warning_v1.yaml`):
`python -m scout.jobs.risk_warning compute`.

Local only (Sharadar prices and market caps, FRED high-yield spread, French RF; no database); writes summary
numbers to git-ignored `exports/risk_warning/result.json` and refuses a second run.
"""
import asyncio
import json
import sys
from pathlib import Path

from scout import risk_warning as rw

OUT = Path("exports/risk_warning/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.risk_warning compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    credit = asyncio.run(rw.load_credit())
    res = rw.run(credit)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(rw.report_text(json.loads(OUT.read_text())), flush=True)
