"""Twenty years of each playbook traded by its own rules at its own pace (pre-registered in
`scout/knowledge/external_specs/playbook_history_v2.yaml`): `python -m scout.jobs.playbook_history compute`.

Local only (Sharadar files, French RF, FRED); verdicts cached in git-ignored `exports/playbook_history/verdicts.pkl`,
summary numbers in `result.json`; refuses a second run. `record` stores the report as brief `playbook_history`.
"""
import asyncio
import json
import logging
import pickle
import sys
from pathlib import Path

from scout import playbook_history as ph

OUT = Path("exports/playbook_history_v2")


async def _record() -> dict:
    from scout import db, learned
    from scout import playbooks as pb

    res = json.loads((OUT / "result.json").read_text())
    entries = {e["key"]: e["status"] for e in await learned.current()}
    status = {p.key: pb.status_of(p, entries)["text"] for p in pb.PLAYBOOKS}
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('playbook_history', CURRENT_DATE, :c, CAST(:p AS JSONB))",
                     {"c": ph.report_text(res, status), "p": db.jsonb(res)})
    return {"key": res["key"], "playbooks": len(res["playbooks"])}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("scout.playbooks").setLevel(logging.ERROR)
    cmd = sys.argv[1:]
    if cmd == ["compute"]:
        if (OUT / "result.json").exists():
            raise SystemExit("already run (one run per pre-registration)")
        OUT.mkdir(parents=True, exist_ok=True)
        H = ph.load()
        cache = OUT / "verdicts.pkl"
        ver = pickle.loads(cache.read_bytes()) if cache.exists() else ph.compute_verdicts(H)
        cache.write_bytes(pickle.dumps(ver, protocol=5))
        res = ph.evaluate(H, ver)
        (OUT / "result.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
        print(ph.report_text(res), flush=True)
    elif cmd == ["same_period"]:
        # added after the run (run note): the benchmarks over each playbook's own span; no playbook number changes
        res = json.loads((OUT / "result.json").read_text())
        H = ph.load()
        bench = ph.benchmarks(H, pickle.loads((OUT / "verdicts.pkl").read_bytes()))
        for r in res["playbooks"]:
            r["same_period"] = ph.same_period(bench, r["all"]["start"], H.dates, H.rf)
        (OUT / "result.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
        print({r["key"]: {k: v and round(v["cagr"], 4) for k, v in r["same_period"].items()} for r in res["playbooks"]})
    elif cmd == ["record"]:
        from scout.jobs._run import run as job

        job("playbook_history", _record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.playbook_history compute | same_period | record")
