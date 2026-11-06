"""Evaluation of the pre-market brief's prose: `python -m scout.jobs.eval_premarket [runs] [--show] [--session YYYY-MM-DD]`.

Builds the pack for the current session as the 9:00 New York run does (quotes as of now, so outside the pre-market
the prices are the last close's), then asks the model `runs` times (default 3). For each run: whether every part
passed validation on the first try, after the correction round, or fell back (headline / overnight / watch as one
part, each holding on its own), and the issues. `--show` prints the last run's brief. Nothing is sent and no
premarket brief is stored; writes a brief (kind `premarket_eval`). Rerun after changing `premarket.PROMPT`, its
checks or `SCOUT_AGENT_MODEL`.
"""
import sys
from datetime import date

from scout import db, premarket
from scout.jobs._run import run
from scout.market_calendar import us_equity_market_status


async def evaluate(runs: int, show: bool, session: date | None) -> dict:
    session = session or date.fromisoformat(us_equity_market_status().market_date)
    pack = await premarket.build_pack(session)
    held = len(pack["positions"])
    results, res = [], None
    for i in range(runs):
        res = await premarket.write(pack)
        tries = res["validation"]
        results.append({"run": i + 1, "source": res["source"], "first_try": bool(tries) and not tries[0],
                        "notes": sorted(res["notes"]), "issues": tries})
        print(f"run {i + 1}: {res['source']} · first try {'ok' if results[-1]['first_try'] else 'failed'} · "
              f"notes for {', '.join(sorted(res['notes'])) or 'none'}")
        for k, issues in enumerate(tries, 1):
            for x in issues:
                print(f"   try {k}: {x}")
    if show and res:
        print("\n" + premarket.render_text(pack, res["brief"], res["notes"]))
    summary = {"session": session.isoformat(), "holdings": held, "runs": runs,
               "full_pass": sum(r["source"] == "llm" for r in results), "first_try": sum(r["first_try"] for r in results),
               "notes_kept": sum(len(r["notes"]) for r in results), "results": results}
    print(f"\n{session}: {held} holdings, {runs} runs — all parts valid in {summary['full_pass']}, first try in {summary['first_try']}, "
          f"{summary['notes_kept']}/{held * runs} holding notes kept")
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('premarket_eval', :d, :c, CAST(:p AS JSONB))",
                     {"d": session, "c": f"盘前简报评测：{summary['full_pass']}/{runs} 次全部通过", "p": db.jsonb(summary)})
    return {k: v for k, v in summary.items() if k != "results"}


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    day = sys.argv[sys.argv.index("--session") + 1] if "--session" in sys.argv else None
    if day in args:
        args.remove(day)
    run("eval_premarket", lambda: evaluate(int(args[0]) if args else 3, "--show" in sys.argv,
                                           date.fromisoformat(day) if day else None), alert=False)
