"""Scout's rule-based system v1 (pre-registered in `scout/knowledge/external_specs/scout_system_v1.yaml`):
`python -m scout.jobs.scout_system compute`.

Local only (Sharadar files, French T-bill rate; no database); writes summary numbers to git-ignored
`exports/scout_system/result.json` and refuses a second run. The B0 replay's stored summary
(exports/b0/sharadar_replay/result.json) is read for a descriptive comparison only.
"""
import json
import sys
from pathlib import Path

from scout import scout_system as ss

OUT = Path("exports/scout_system/result.json")
B0 = Path("exports/b0/sharadar_replay/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.scout_system compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = ss.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(ss.report_text(json.loads(OUT.read_text()), json.loads(B0.read_text()) if B0.exists() else None), flush=True)
