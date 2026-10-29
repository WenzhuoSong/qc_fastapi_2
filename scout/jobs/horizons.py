"""Holding-horizon profile of the passed strategies: `python -m scout.jobs.horizons`.

Descriptive (see `scout.horizons`): printed and stored as a brief (kind `horizon_profile`).
"""
from datetime import date

from scout import db, horizons, learned, rankers
from scout.detectors import registry
from scout.jobs._run import run
from scout.pipeline import load_panel


async def main() -> dict:
    p = await load_panel(lookback_days=None)
    out, texts = {}, []
    asset = (await learned.get("rank_low_asset_growth"))["spec"]
    days = horizons.rank_days(len(p.dates), asset)
    angle = await rankers.prepare(asset, p, rankers.arrays(p)[1], days)
    out["rank_low_asset_growth"] = horizons.profile(horizons.rank_rows(p, asset, angle))
    texts.append(horizons.report_text("资产增长最慢 10%（每 21 个交易日取一次）", out["rank_low_asset_growth"]))
    det = next(d for d in registry(include_candidates=True) if d.name == "breakout_retest")
    out["breakout_retest"] = horizons.profile(horizons.rule_rows(p, det))
    texts.append(horizons.report_text("突破回踩（按规则入场区间成交）", out["breakout_retest"]))
    text = "\n\n".join(texts)
    print(text, flush=True)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('horizon_profile', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(out)})
    return {k: len(v.get("horizons", {})) for k, v in out.items()}


if __name__ == "__main__":
    run("horizons", main, alert=False)
