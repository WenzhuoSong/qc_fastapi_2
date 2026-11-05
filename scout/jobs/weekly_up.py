"""Weekly up calls v2 (pre-registered in `scout/knowledge/external_specs/weekly_up_v2.yaml`):
`python -m scout.jobs.weekly_up compute`.

Local only (Sharadar, French RF, FRED and the Fed's FOMC pages fetched at run time; no database); writes summary
numbers to git-ignored `exports/weekly_up/result.json` (out-of-sample predictions in `oos.parquet`) and refuses a
second run.
"""
import asyncio
import json
import sys
from pathlib import Path

from scout import weekly_up as wu

OUT = Path("exports/weekly_up/result.json")

if __name__ == "__main__":
    if sys.argv[1:] != ["compute"]:
        raise SystemExit("usage: python -m scout.jobs.weekly_up compute")
    if OUT.exists():
        raise SystemExit("already run (one run per pre-registration)")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    macro = asyncio.run(wu.load_macro())
    fomc, years = asyncio.run(wu.load_fomc())
    res = wu.run(macro, fomc, years, log=lambda s: print(s, flush=True))
    OUT.write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    print(wu.report_text(json.loads(OUT.read_text())), flush=True)
