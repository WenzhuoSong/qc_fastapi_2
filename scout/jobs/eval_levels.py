"""Replay of the price-map buy points: `python -m scout.jobs.eval_levels [--every N] [--limit N]`.

See `scout.level_study`. Every N-th session (default 5) of every stock, the map is rebuilt
point in time; its buy points and the buy-now control are filled and managed as a holding
(a third at target 1, the rest on the chandelier stop). Results per variant overall, per
half of the sample and per market regime are printed and stored as a brief (`level_study`).
"""
import sys
from datetime import date

import numpy as np

from scout import base_rates, db, exit_study, level_study, levels, structure
from scout.jobs._run import run
from scout.pipeline import load_panel

POLICY = next(p for p in exit_study.POLICIES if p.name == "chandelier_third")
FEATURES = ("close", "atr14", "sma20", "sma50", "sma200", "dist_52w_high")
VARIANTS = {"pullback": "回调买点（进入区间成交）", "breakout": "突破买点（放量收盘站上，按收盘成交）",
            "breakout_touch": "突破买点（盘中触及即成交，对照）", "reversal": "反转确认（下降趋势中放量收盘站上）",
            "buy_now": "对照：次日开盘直接买入（同样的止损止盈方法）"}


def outcome(a: dict, plan: dict, entry: float, start: int) -> tuple[float, int, float] | None:
    """(gross return, sessions held, return in R) of a fill managed as a holding."""
    if not plan or not (plan["stop"] < entry < plan["target1"]):
        return None
    path = exit_study.Path(a["open"], a["high"], a["low"], a["close"], a["atr14"], a["sma20"], a["sma50"], start)
    t2 = max(plan["target2"], plan["target1"])
    ret, days = exit_study.manage(path, entry, plan["stop"], plan["target1"], t2, POLICY)
    return ret, days, ret * entry / (entry - plan["stop"])


async def study() -> dict:
    every = int(sys.argv[sys.argv.index("--every") + 1]) if "--every" in sys.argv else 5
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None   # smoke test: not stored
    p = await load_panel(lookback_days=None)
    drift = base_rates.daily_drift(p)
    regime = p.regime.to_numpy()
    n = len(p.dates)
    rows: dict[str, list[dict]] = {k: [] for k in VARIANTS}
    for ticker in p.stocks[:limit]:
        a = {k: p[k][ticker].to_numpy(dtype=float) for k in ("open", "high", "low", "close", "volume", "atr14", "sma20", "sma50",
                                                                "sma200", "dist_52w_high", "vol_ratio")}
        bars = [{"date": p.dates[i], "open": a["open"][i], "high": a["high"][i], "low": a["low"][i], "close": a["close"][i],
                 "volume": a["volume"][i]} for i in range(n)]
        valid = np.flatnonzero(np.isfinite(a["close"]) & np.isfinite(a["sma200"]) & np.isfinite(a["atr14"]))
        if not valid.size:
            continue
        for i in range(int(valid[0]) + 30, n - 1 - level_study.PENDING - exit_study.MAX_HOLD, every):
            if not np.isfinite(a["close"][i]):
                continue
            f = {k: float(a[k][i]) for k in FEATURES if np.isfinite(a[k][i])}
            hist = [b for b in bars[max(0, i - structure.LOOKBACK + 1): i + 1] if np.isfinite(b["close"])]
            pm = levels.price_map(f, structure.compute(hist))
            if not pm:
                continue
            d, reg = p.dates[i], str(regime[i])
            end = i + 1 + level_study.PENDING

            def add(kind: str, fill: tuple[int, float] | None, start_offset: int) -> None:
                if not fill:
                    return
                j, price = fill
                res = outcome(a, levels.locked_plan(pm, price, d), price, j + start_offset)
                if res:
                    ret, days, r = res
                    hold = float(np.exp(drift[ticker] * days) - 1) if ticker in drift else None
                    rows[kind].append({"ret": ret, "days": days, "hold": hold, "r": r, "date": d, "regime": reg})

            for e in pm["entries"]:
                lo, hi = e["zone_lo"], e["zone_hi"]
                if e["name"] == "回调买点":
                    add("pullback", level_study.fill_limit(lo, hi, a["open"], a["high"], a["low"], i + 1, end), 0)
                else:
                    kind = "breakout" if e["name"] == "突破买点" else "reversal"
                    add(kind, level_study.fill_close_confirmed(lo, hi, a["close"], a["vol_ratio"], i + 1, end), 1)
                    if kind == "breakout":
                        add("breakout_touch", level_study.fill_limit(lo, hi, a["open"], a["high"], a["low"], i + 1, end), 0)
            if np.isfinite(a["open"][i + 1]):
                add("buy_now", (i + 1, float(a["open"][i + 1])), 0)
    dates = sorted(r["date"] for r in rows["buy_now"])
    if not dates:
        return {"n": 0}
    mid = dates[len(dates) // 2]
    out: dict = {"every": every, "first": str(dates[0]), "split": str(mid), "last": str(dates[-1]), "variants": {}}
    for k, label in VARIANTS.items():
        rs = rows[k]
        out["variants"][k] = {"label": label, "all": exit_study.summarize(rs),
                              "first_half": exit_study.summarize([r for r in rs if r["date"] < mid]),
                              "second_half": exit_study.summarize([r for r in rs if r["date"] >= mid]),
                              **{f"regime_{g}": exit_study.summarize([r for r in rs if r["regime"] == g])
                                 for g in ("uptrend", "mixed", "downtrend")}}
    lines = [f"价位图买点回放：{out['first']} 至 {out['last']}，每 {every} 个交易日取样，按 {out['split']} 分前后两半；"
             "成交后按持仓方式管理（目标 1 卖 1/3，其余吊灯止损）",
             "买点 | 笔数 | 平均净收益 全部 / 前半 / 后半 | 跑赢持有 全部 / 前半 / 后半 | 上升 / 震荡 / 下降市 跑赢持有 | 胜率 | 平均持有天数"]
    for k, v in out["variants"].items():
        a_, f_, s_ = v["all"], v["first_half"], v["second_half"]
        if not a_.get("n"):
            lines.append(f"{k} {v['label']} | 0")
            continue
        reg = " / ".join(f"{v[f'regime_{g}']['edge_vs_hold']:+.2%}" if v[f"regime_{g}"].get("n") else "—"
                         for g in ("uptrend", "mixed", "downtrend"))
        lines.append(f"{k} {v['label']} | {a_['n']} | {a_['avg_net']:+.2%} / {f_.get('avg_net', 0):+.2%} / {s_.get('avg_net', 0):+.2%} | "
                     f"{a_['edge_vs_hold']:+.2%} / {f_.get('edge_vs_hold', 0):+.2%} / {s_.get('edge_vs_hold', 0):+.2%} | {reg} | "
                     f"{a_['win_rate']:.0%} | {a_['avg_days']:.0f}")
    text = "\n".join(lines)
    print(text)
    if limit:
        return {"smoke_test": limit}
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('level_study', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.today(), "c": text, "p": db.jsonb(out)})
    return {k: v["all"].get("n", 0) for k, v in out["variants"].items()}


if __name__ == "__main__":
    run("eval_levels", study, alert=False)
