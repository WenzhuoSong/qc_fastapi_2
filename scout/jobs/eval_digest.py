"""Evaluation of the digest's analyst notes: `python -m scout.jobs.eval_digest [runs] [--show]`.

Builds the latest session's holdings as the after-close job does (features, news, earnings previews, norms),
then asks the model for the notes `runs` times (default 3). For each run: whether every holding passed
validation on the first try, after the correction round, or fell back to the rule-based lines, and the issues.
Validation means numbers in the pack, no forecasts, no actions beyond the plan, and the news/reason relation.
`--show` prints the last run's rendered holdings section. Writes a brief (kind `digest_eval`). Rerun after
changing `holding_notes.PROMPT`, its checks or `SCOUT_AGENT_MODEL`.
"""
import sys

from scout import context, db, digest, holding_notes, portfolio
from scout.jobs._run import run


async def evaluate(runs: int, show: bool) -> dict:
    row = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily")
    asof = row["d"]
    g = await digest.gather(asof, await context.earnings_map(asof), [], [])
    user = await portfolio.get_settings()
    for t in g["held_rows"]:
        tk = t["ticker"]
        if tk in g["earnings"]:
            g.setdefault("previews", {})[tk] = await holding_notes.earnings_preview(
                tk, g["earnings"][tk], asof, portfolio.metrics(t).get("to_stop"), g.get("gaps", {}).get(tk))
    g["risk"] = await holding_notes.portfolio_risk(g, user.get("account_size"))
    items = digest.holding_items(g, asof)
    results = []
    for i in range(runs):
        res = await holding_notes.write(g, items)
        tries = res["validation"]
        results.append({"run": i + 1, "source": res["source"], "first_try": bool(tries) and not tries[0],
                        "passed": sorted(res["notes"]), "issues": tries})
        print(f"run {i + 1}: {res['source']} · first try {'ok' if results[-1]['first_try'] else 'failed'} · "
              f"notes for {', '.join(sorted(res['notes'])) or 'none'}")
        for k, issues in enumerate(tries, 1):
            for x in issues:
                print(f"   try {k}: {x}")
        g["notes"] = res["notes"]
    if show:
        for h in digest.holding_items(g, asof):
            print(("⚠️ " if h["attention"] else "• ") + h["head"])
            print("\n".join(f"  {x}" for x in h["lines"]))
    held = len(items)
    summary = {"asof": asof.isoformat(), "holdings": held, "runs": runs,
               "full_pass": sum(r["source"] == "llm" for r in results),
               "first_try": sum(r["first_try"] for r in results),
               "notes_kept": sum(len(r["passed"]) for r in results), "results": results}
    print(f"\n{asof}: {held} holdings, {runs} runs — all notes valid in {summary['full_pass']}, first try in {summary['first_try']}, "
          f"{summary['notes_kept']}/{held * runs} holding notes kept")
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('digest_eval', :d, :c, CAST(:p AS JSONB))",
                     {"d": asof, "c": f"持仓点评评测：{summary['full_pass']}/{runs} 次全部通过", "p": db.jsonb(summary)})
    return {k: v for k, v in summary.items() if k != "results"}


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    run("eval_digest", lambda: evaluate(int(args[0]) if args else 3, "--show" in sys.argv), alert=False)
