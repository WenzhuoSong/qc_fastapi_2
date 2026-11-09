"""Playbook history v3 (pre-registered in `scout/knowledge/external_specs/playbook_history_v3.yaml`), local only:

  python -m scout.jobs.playbook_history_v3 verdicts         # top 1000: what v2's cache lacks (yearly playbooks monthly, Clenow on Wednesdays)
  python -m scout.jobs.playbook_history_v3 verdicts_small   # top 3000: the five small-cap versions
  python -m scout.jobs.playbook_history_v3 compute          # the one run (refuses a second)
  python -m scout.jobs.playbook_history_v3 record           # the report as brief `playbook_history_v3`

Caches and results in git-ignored exports/playbook_history_v3/; v2's verdicts are read from exports/playbook_history_v2/.
"""
import asyncio
import json
import logging
import pickle
import sys
from pathlib import Path

from scout import playbook_history as ph
from scout import playbook_history_v3 as v3

OUT = Path("exports/playbook_history_v3")
V2 = Path("exports/playbook_history_v2/verdicts.pkl")


async def _record() -> dict:
    from scout import db

    res = json.loads((OUT / "result.json").read_text())
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('playbook_history_v3', CURRENT_DATE, :c, CAST(:p AS JSONB))",
                     {"c": v3.report_text(res), "p": db.jsonb(res)})
    return {"key": res["key"], "playbooks": len(res["playbooks"]), "small": len(res.get("small", []))}


def _small_hist():
    return ph.load(top=3000, min_dv=v3.SMALL_MIN_DV)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("scout.playbooks").setLevel(logging.ERROR)
    cmd = sys.argv[1:]
    OUT.mkdir(parents=True, exist_ok=True)
    if cmd == ["verdicts"]:
        H = ph.load()
        s = v3.window(H)
        have = pickle.loads(V2.read_bytes())
        todo = v3.extra_todo(H, s, have)
        logging.info("extra verdicts on %d sessions", len(todo))
        (OUT / "verdicts_extra.pkl").write_bytes(pickle.dumps(ph.compute_verdicts(H, todo=todo), protocol=5))
    elif cmd == ["verdicts_small"]:
        H3 = _small_hist()
        s = v3.window(H3)
        (OUT / "verdicts_small.pkl").write_bytes(pickle.dumps(ph.compute_verdicts(H3, todo=v3.small_todo(H3, s)), protocol=5))
    elif cmd == ["compute"]:
        if (OUT / "result.json").exists():
            raise SystemExit("already run (one run per pre-registration)")
        from scout.small_cap_rankers import cost_table

        H = ph.load()
        s = v3.window(H)
        ver = v3.merge(pickle.loads(V2.read_bytes()), pickle.loads((OUT / "verdicts_extra.pkl").read_bytes()))
        H3 = _small_hist()
        ver3 = pickle.loads((OUT / "verdicts_small.pkl").read_bytes())
        res = v3.evaluate(H, ver, s, small=(H3, ver3, cost_table()))
        (OUT / "result.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
        print(v3.report_text(res), flush=True)
    elif cmd == ["record"]:
        from scout.jobs._run import run as job

        job("playbook_history_v3", _record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.playbook_history_v3 verdicts | verdicts_small | compute | record")
