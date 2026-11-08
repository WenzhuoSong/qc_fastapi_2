"""Pullback in calm markets (pre-registered in `scout/knowledge/external_specs/pullback_calm_market_v1.yaml`):
`python -m scout.jobs.pullback_calm compute`.

Local only (Sharadar files; no database); writes the trades and summary to git-ignored
`exports/pullback_calm/result.json` and refuses a second run.
"""
import json
import sys
from pathlib import Path

from scout import pullback_calm as pc

OUT = Path("exports/pullback_calm/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.pullback_calm compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    raw = pc.run()
    ev = pc.evaluate(raw["rows"])
    res = {"key": pc.KEY, "signals": raw["signals"], "rows": raw["rows"], "evaluation": ev, "decision": pc.decide(ev)}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(pc.report_text(json.loads(OUT.read_text())), flush=True)
