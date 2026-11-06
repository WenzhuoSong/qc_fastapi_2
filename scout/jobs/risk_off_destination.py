"""Where the core money goes while the market filter is off (pre-registered in
`scout/knowledge/external_specs/risk_off_destination_v1.yaml`): `python -m scout.jobs.risk_off_destination compute`.

Local only (Sharadar ETF prices, French RF; no database); writes summary numbers to git-ignored
`exports/risk_off_destination/result.json` and refuses a second run.
"""
import json
import sys
from pathlib import Path

from scout import risk_off_destination as rod

OUT = Path("exports/risk_off_destination/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.risk_off_destination compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = rod.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(rod.report_text(json.loads(OUT.read_text())), flush=True)
