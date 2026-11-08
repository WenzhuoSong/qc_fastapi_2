"""Playbook brief: every playbook in `scout.playbooks` on the latest close (`python -m scout.jobs.playbooks
[--no-store]`). Stored as a brief (kind `playbooks`); the weekly job runs it before the weekly report."""
import sys

from scout import playbooks
from scout.jobs._run import run


async def job(store: bool = True) -> dict:
    res = await playbooks.compute()
    print(playbooks.text(res), flush=True)
    if store:
        await playbooks.store(res)
    return {"asof": res["asof"], "playbooks": len(res["playbooks"]),
            "missing": [b["key"] for b in res["playbooks"] if b.get("missing")]}


def main() -> None:
    run("playbooks", lambda: job(store="--no-store" not in sys.argv))


if __name__ == "__main__":
    main()
