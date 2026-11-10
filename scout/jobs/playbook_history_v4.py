"""Playbook history v4 (pre-registered in `scout/knowledge/external_specs/playbook_history_v4.yaml`), local only:

  python -m scout.jobs.playbook_history_v4 verdicts         # top 1000 on Sharadar's full fields: CAN SLIM daily, the DATA_FULL playbooks monthly
  python -m scout.jobs.playbook_history_v4 verdicts_small   # top 3000 on the full fields: the five small-cap versions
  python -m scout.jobs.playbook_history_v4 compute          # the one run (refuses a second)
  python -m scout.jobs.playbook_history_v4 record           # the report as brief `playbook_history_v4`

Caches and results in git-ignored exports/playbook_history_v4/; v2's and v3's verdicts and v3's result are read from
their own folders.
"""
import json
import logging
import pickle
import sys
from pathlib import Path

from scout import playbook_history as ph
from scout import playbook_history_v3 as v3
from scout import playbook_history_v4 as v4

OUT = Path("exports/playbook_history_v4")
V2 = Path("exports/playbook_history_v2/verdicts.pkl")
V3 = Path("exports/playbook_history_v3")


async def _record() -> dict:
    from scout import db

    res = json.loads((OUT / "result.json").read_text())
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('playbook_history_v4', CURRENT_DATE, :c, CAST(:p AS JSONB))",
                     {"c": v4.report_text(res), "p": db.jsonb(res)})
    return {"key": res["key"], "playbooks": len(res["playbooks"]), "trade_level": len(res["trade_level"])}


def _todo(H, s, monthly: tuple, daily: tuple = ("can_slim",)) -> dict[int, list]:
    books = {p.key: p for p in ph.books()}
    todo = {i: [books[k] for k in daily] for i in range(s, len(H.dates))}
    for i in v3.month_ends(H, s):
        todo[i] = todo[i] + [books[k] for k in monthly]
    return todo


def _full(top: int = 1000, min_dv: float = 20e6):
    H = ph.load(top=top, min_dv=min_dv)
    H.full = True
    return H


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("scout.playbooks").setLevel(logging.ERROR)
    cmd = sys.argv[1:]
    OUT.mkdir(parents=True, exist_ok=True)
    if cmd == ["verdicts"]:
        H = _full()
        s = v3.window(H)
        (OUT / "verdicts_full.pkl").write_bytes(pickle.dumps(ph.compute_verdicts(H, todo=_todo(H, s, tuple(v4.DATA_FULL))), protocol=5))
    elif cmd == ["verdicts_small"]:
        H3 = _full(3000, v3.SMALL_MIN_DV)
        s = v3.window(H3)
        monthly = tuple(k for k in v4.SMALL if k != "can_slim")
        (OUT / "verdicts_small.pkl").write_bytes(pickle.dumps(ph.compute_verdicts(H3, todo=_todo(H3, s, monthly)), protocol=5))
    elif cmd == ["compute"]:
        if (OUT / "result.json").exists():
            raise SystemExit("already run (one run per pre-registration)")
        from scout.small_cap_rankers import cost_table

        H = _full()
        s = v3.window(H)
        ver = v3.merge(pickle.loads(V2.read_bytes()), pickle.loads((V3 / "verdicts_extra.pkl").read_bytes()))
        full = pickle.loads((OUT / "verdicts_full.pkl").read_bytes())
        H3 = _full(3000, v3.SMALL_MIN_DV)
        small = pickle.loads((OUT / "verdicts_small.pkl").read_bytes())
        res = v4.evaluate(H, ver, full, s, json.loads((V3 / "result.json").read_text()),
                          small=(H3, small, cost_table()), inst=v4.holders())
        (OUT / "result.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str))
        print(v4.report_text(res), flush=True)
    elif cmd == ["record"]:
        from scout.jobs._run import run as job

        job("playbook_history_v4", _record, alert=False)
    else:
        raise SystemExit("usage: python -m scout.jobs.playbook_history_v4 verdicts | verdicts_small | compute | record")
