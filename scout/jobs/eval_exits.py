"""Trailing-stop study for the holdings plan: `python -m scout.jobs.eval_exits`.

Every historical rule signal (live and candidate rules, full stored history) is
filled with the replay's trigger rule, then managed as the holdings plan does
(a third at target 1, a third at target 2, the rest trailing) under each policy in
`scout.exit_study.POLICIES`. Results per policy, overall and per half of the sample,
are printed and stored as a brief (kind `exit_study`) so the decision can be traced.
"""
from datetime import date

import numpy as np

from scout import base_rates, db, exit_study
from scout.detectors import registry
from scout.jobs._run import run
from scout.pipeline import load_panel


async def study() -> dict:
    p = await load_panel(lookback_days=None)
    drift = base_rates.daily_drift(p)
    cols = {k: p[k] for k in ("open", "high", "low", "close", "atr14", "sma20", "sma50")}
    cache: dict[str, dict[str, np.ndarray]] = {}
    rows: dict[str, list[dict]] = {pol.name: [] for pol in exit_study.POLICIES}
    n_dates = len(p.dates)
    for det in registry(include_candidates=True):
        mask = det.mask(p)
        for ticker, i in base_rates.hit_positions(mask):
            if i + 1 + det.trigger_window + exit_study.MAX_HOLD >= n_dates:
                continue                                    # outcome not yet observable
            d = p.dates[i]
            lv = det.levels(p.row(ticker, d))
            if not lv.valid():
                continue
            a = cache.get(ticker)
            if a is None:
                a = cache[ticker] = {k: v[ticker].to_numpy(dtype=float) for k, v in cols.items()}
            fill = next((j for j in range(i + 1, i + 1 + det.trigger_window)
                         if a["low"][j] <= lv.entry_hi and a["high"][j] >= lv.entry_lo), None)
            if fill is None:
                continue
            entry = min(max(a["open"][fill], lv.entry_lo), lv.entry_hi)
            if not np.isfinite(entry) or entry <= lv.stop:
                continue
            path = exit_study.Path(a["open"], a["high"], a["low"], a["close"], a["atr14"], a["sma20"], a["sma50"], fill)
            for pol in exit_study.POLICIES:
                ret, days = exit_study.manage(path, entry, lv.stop, lv.target1, lv.target2, pol)
                hold = float(np.exp(drift[ticker] * days) - 1) if ticker in drift else None
                rows[pol.name].append({"ret": ret, "days": days, "hold": hold, "r": ret * entry / (entry - lv.stop),
                                       "date": d, "detector": det.name})
    dates = sorted(r["date"] for r in rows["fixed"])
    if not dates:
        return {"n": 0}
    mid = dates[len(dates) // 2]
    out: dict = {"n": len(dates), "first": str(dates[0]), "split": str(mid), "last": str(dates[-1]), "policies": {}}
    for pol in exit_study.POLICIES:
        rs = rows[pol.name]
        out["policies"][pol.name] = {
            "label": pol.label,
            "all": exit_study.summarize(rs),
            "first_half": exit_study.summarize([r for r in rs if r["date"] < mid]),
            "second_half": exit_study.summarize([r for r in rs if r["date"] >= mid]),
        }
    lines = [f"出场方式对比：{out['n']} 笔成交（{out['first']} 至 {out['last']}，按 {out['split']} 分前后两半）",
             "方式 | 平均净收益 全部 / 前半 / 后半 | 跑赢持有 全部 / 前半 / 后半 | 胜率 | 平均 R | 平均持有天数"]
    for name, v in out["policies"].items():
        a, f, s = v["all"], v["first_half"], v["second_half"]
        lines.append(f"{name} {v['label']} | {a['avg_net']:+.2%} / {f['avg_net']:+.2%} / {s['avg_net']:+.2%} | "
                     f"{a['edge_vs_hold']:+.2%} / {f['edge_vs_hold']:+.2%} / {s['edge_vs_hold']:+.2%} | "
                     f"{a['win_rate']:.0%} | {a['avg_r']:+.2f} | {a['avg_days']:.0f}")
    text = "\n".join(lines)
    print(text)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('exit_study', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(out)})
    return {"n": out["n"], "split": out["split"]}


if __name__ == "__main__":
    run("eval_exits", study, alert=False)
