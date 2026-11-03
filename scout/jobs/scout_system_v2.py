"""Scout's rule-based system v2 (pre-registered in `scout/knowledge/external_specs/scout_system_v2.yaml`):
`python -m scout.jobs.scout_system_v2 compute`. Local only; writes git-ignored `exports/scout_system/result_v2.json`
and refuses a second run. The B0 replay's stored summary is read for a descriptive comparison only.
"""
import json
import sys
from pathlib import Path

from scout import scout_system_v2 as v2

OUT = Path("exports/scout_system/result_v2.json")
B0 = Path("exports/b0/sharadar_replay/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.scout_system_v2 compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    res = v2.run()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    b0 = json.loads(B0.read_text()) if B0.exists() else None
    print(v2.report_text(json.loads(OUT.read_text()), b0), flush=True)
    print("bookkeeping: largest daily move", {e: round(v["max_abs_day"], 3) for e, v in res["variants"].items()}, flush=True)
