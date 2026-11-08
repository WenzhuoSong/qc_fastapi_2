"""Allocation evaluation framework v1 (written down in `scout/knowledge/external_specs/allocation_eval_v1.yaml`).

A judgment on seen data, not a gate test: the core rule beside a 20 % sleeve (cash, 10-year bonds, risk parity, and
on the short sample the long-only trend sleeve) across environments — inflation × growth direction, rate direction,
stock-bond correlation sign, bear markets — with drawdown, underwater time and the worst regret. The candidates'
monthly returns come unchanged from `scout.risk_parity` and carver_trend_v1's saved run.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scout import trend_sleeve as ts

KEY = "allocation_eval_v1"
DECISION = ("core_now", "core_cash", "core_bonds", "core_rp")
TREND = "core_trend"
REFERENCE = ("sixty_forty", "equity")
DD_SLACK, WORST12_SLACK, UNDERWATER_SLACK, MIN_REGRET_GAIN = 0.05, 0.05, 12, 0.01
BEAR, CORR_WINDOW, MIN_MONTHS = 0.20, 36, 24
SHORT_START = pd.Period("2003-09", "M")
LABELS = {"core_now": "现行核心规则", "core_cash": "80% 核心 + 20% 现金", "core_bonds": "80% 核心 + 20% 国债",
          "core_rp": "80% 核心 + 20% 风险平价", "core_trend": "80% 核心 + 20% 趋势只做多",
          "sixty_forty": "60% 股 / 40% 债", "equity": "美股买入持有"}


# ── environments (each month labelled by its own data: a description, never a signal) ──

def monthly_last(daily: list[tuple]) -> pd.Series:
    s = pd.Series({pd.Timestamp(d): v for d, v in daily}).sort_index()
    return s.groupby(s.index.to_period("M")).last()


def environments(cpi: pd.Series, ip: pd.Series, y: pd.Series, eq: pd.Series, bd: pd.Series) -> pd.DataFrame:
    """One row per month; each column True / False, NaN where the data do not reach."""
    def up(s: pd.Series) -> pd.Series:
        return (s > s.shift(12)).where(s.notna() & s.shift(12).notna())

    corr = eq.rolling(CORR_WINDOW).corr(bd.reindex(eq.index))
    return pd.DataFrame({"infl_up": up(cpi / cpi.shift(12) - 1), "growth_up": up(ip / ip.shift(12) - 1), "rates_up": up(y),
                         "corr_pos": (corr > 0).where(corr.notna()), "corr": corr})


def bear_episodes(eq: pd.Series) -> list[dict]:
    """Every fall of ≥ 20 % from a previous high: the months after the high up to the trough."""
    nav = (1 + eq).cumprod()
    out, peak, peak_m, trough, low = [], -math.inf, None, None, 0.0
    for m, v in nav.items():
        if v >= peak:
            if low <= -BEAR:
                out.append({"start": peak_m + 1, "end": trough, "dd": low})
            peak, peak_m, trough, low = v, m, None, 0.0
        elif v / peak - 1 < low:
            low, trough = v / peak - 1, m
    if low <= -BEAR:
        out.append({"start": peak_m + 1, "end": trough, "dd": low})
    return out


def scenarios(env: pd.DataFrame, episodes: list[dict], idx: pd.PeriodIndex) -> dict[str, pd.PeriodIndex]:
    e = env.reindex(idx)
    out = {}
    for iu, il in ((True, "通胀上升"), (False, "通胀回落")):
        for gu, gl in ((True, "增长加速"), (False, "增长放缓")):
            out[f"{il}·{gl}"] = idx[((e["infl_up"] == iu) & (e["growth_up"] == gu)).to_numpy()]
    for col, yes, no in (("rates_up", "利率上升", "利率下降"), ("corr_pos", "股债正相关", "股债负相关")):
        out[yes] = idx[(e[col] == True).to_numpy()]            # noqa: E712 — NaN is neither
        out[no] = idx[(e[col] == False).to_numpy()]           # noqa: E712
    bear = [m for ep in episodes for m in pd.period_range(ep["start"], ep["end"], freq="M")]
    out["熊市（跌 ≥ 20%）"] = idx[idx.isin(bear)]
    return out


# ── measures ──

def ann(r: pd.Series) -> float | None:
    return float((1 + r).prod() ** (12 / len(r)) - 1) if len(r) else None


def underwater(r: pd.Series) -> int:
    nav = (1 + r).cumprod()
    longest = run = 0
    for below in (nav < nav.cummax() * (1 - 1e-12)):
        run = run + 1 if below else 0
        longest = max(longest, run)
    return longest


def window_dd(r: pd.Series) -> float:
    nav = (1 + r).cumprod()
    return float((nav / np.maximum.accumulate(np.r_[1.0, nav.to_numpy()])[1:] - 1).min())


def profile(p: dict[str, pd.Series], rf: pd.Series, idx: pd.PeriodIndex, env: pd.DataFrame, episodes: list[dict],
            decision: tuple[str, ...]) -> dict:
    p = {k: v.reindex(idx) for k, v in p.items()}
    scen = scenarios(env, episodes, idx)
    eps = [ep for ep in episodes if ep["end"] is not None and ep["start"] >= idx[0] and ep["end"] <= idx[-1]]
    out = {"from": str(idx[0]), "to": str(idx[-1]), "months": len(idx), "candidates": {},
           "scenario_months": {s: len(m) for s, m in scen.items()},
           "episodes": [{"start": str(e["start"]), "end": str(e["end"]), "market_dd": e["dd"]} for e in eps]}
    base = p["core_now"]
    for k, r in p.items():
        s = ts.monthly_stats(r, rf)
        worst = r.sort_values().iloc[: max(1, int(len(r) * 0.05))]
        roll = [(ann(r.iloc[i:i + 36]) > ann(base.iloc[i:i + 36]), window_dd(r.iloc[i:i + 36]) > window_dd(base.iloc[i:i + 36]))
                for i in range(len(r) - 35)]
        s.update({"underwater": underwater(r), "cvar5": float(worst.mean()),
                  "roll_beats": float(np.mean([a for a, _ in roll])) if roll else None,
                  "roll_smaller_dd": float(np.mean([b for _, b in roll])) if roll else None,
                  "scenarios": {n: ann(r.reindex(m)) for n, m in scen.items()},
                  "episodes": [float((1 + r.reindex(pd.period_range(e["start"], e["end"], freq="M"))).prod() - 1) for e in eps]})
        out["candidates"][k] = s
    out["regret_scenarios"] = [n for n, m in scen.items() if len(m) >= MIN_MONTHS]
    return out


def acceptable(c: dict, base: dict) -> dict:
    return {"drawdown": bool(c["max_dd"] >= base["max_dd"] - DD_SLACK),
            "worst_12m": bool(c["worst_12m"] >= base["worst_12m"] - WORST12_SLACK),
            "underwater": c["underwater"] <= base["underwater"] + UNDERWATER_SLACK}


def regrets(prof: dict, among: list[str]) -> dict[str, dict]:
    """Per scenario, the best acceptable candidate's annual return minus each candidate's; and the largest."""
    c = prof["candidates"]
    out = {}
    for k in among:
        reg = {n: max(c[o]["scenarios"][n] for o in among) - c[k]["scenarios"][n] for n in prof["regret_scenarios"]}
        out[k] = {"by_scenario": reg, "max": max(reg.values()) if reg else 0.0}
    return out


def choose(prof: dict, decision: tuple[str, ...]) -> dict:
    cands, base = prof["candidates"], prof["candidates"]["core_now"]
    checks = {k: acceptable(cands[k], base) for k in decision}
    ok = [k for k in decision if all(checks[k].values())]          # core_now always passes its own bar
    reg = regrets(prof, ok)
    best = min(ok, key=lambda k: reg[k]["max"])
    gain = reg["core_now"]["max"] - reg[best]["max"]
    pick = best if best != "core_now" and gain >= MIN_REGRET_GAIN else "core_now"
    return {"checks": checks, "acceptable": ok, "regret": reg, "best": best, "regret_gain": gain, "pick": pick}


def final(long_choice: dict, short_choice: dict) -> dict:
    pick = long_choice["pick"]
    if pick != "core_now" and pick not in short_choice["acceptable"]:
        return {"pick": "core_now", "reason": f"长样本选 {LABELS[pick]}，但它在 2003 年起的样本上不可接受"}
    note = ""
    if short_choice["pick"] == TREND:
        note = "；2003 年起趋势只做多配置胜出，但没有长历史，按预先的规定不改变结论"
    return {"pick": pick, "reason": ("长样本上最大后悔最小且两段样本都可接受" if pick != "core_now"
                                     else "没有候选在可接受的同时把最大后悔降低 1 个百分点 / 年以上") + note}


# ── report ──

def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x:+.{d}%}"


def table(prof: dict, choice: dict) -> list[str]:
    c = prof["candidates"]
    lines = [f"{prof['from']} 至 {prof['to']}（{prof['months']} 个月）",
             "组合 | 年化 | 夏普 | 最大回撤 | 最差 12 个月 | 最长水下（月）| 最差 5% 月均 | 36 个月窗口跑赢现行 / 回撤更小 | 最大后悔 | 可接受"]
    for k in LABELS:
        if k not in c:
            continue
        s = c[k]
        ok = "—" if k not in choice["checks"] else ("是" if all(choice["checks"][k].values()) else
                                                     "否（" + "、".join(n for n, v in choice["checks"][k].items() if not v) + "）")
        rb = "—" if s["roll_beats"] is None or k == "core_now" else f"{s['roll_beats']:.0%} / {s['roll_smaller_dd']:.0%}"
        mr = choice["regret"][k]["max"] if k in choice["regret"] else None
        lines.append(f"{LABELS[k]} | {_p(s['cagr'])} | {s['sharpe']:.2f} | {_p(s['max_dd'])} | {_p(s['worst_12m'])} | {s['underwater']} | "
                     f"{_p(s['cvar5'])} | {rb} | {_p(mr)} | {ok}")
    lines.append("分环境年化收益（月数）：")
    names = list(prof["scenario_months"])
    for k in LABELS:
        if k in c:
            lines.append(f"  {LABELS[k]}：" + "；".join(f"{n} {_p(c[k]['scenarios'][n])}" for n in names))
    lines.append("  月数：" + "；".join(f"{n} {m}" + ("" if n in prof["regret_scenarios"] else "（不足 24，不计后悔）")
                                     for n, m in prof["scenario_months"].items()))
    if prof["episodes"]:
        lines.append("每段熊市的累计收益：")
        for i, e in enumerate(prof["episodes"]):
            lines.append(f"  {e['start']}..{e['end']}（美股 {_p(e['market_dd'], 0)}）：" +
                         "；".join(f"{LABELS[k]} {_p(c[k]['episodes'][i], 0)}" for k in LABELS if k in c and k not in REFERENCE))
    lines.append(f"选择：可接受 {[LABELS[k] for k in choice['acceptable']]}，后悔最小 {LABELS[choice['best']]}"
                 f"（比现行少 {_p(choice['regret_gain'])} / 年）→ {LABELS[choice['pick']]}")
    return lines


def report_text(res: dict) -> str:
    m = res["monitor"]
    lines = ["核心仓配置评价（allocation_eval_v1）· 已看过的数据上的判断，不是检验", "【长样本】", *table(res["long"], res["long_choice"]),
             "【2003 年起】", *table(res["short"], res["short_choice"]),
             f"结论：{LABELS[res['final']['pick']]}（{res['final']['reason']}）",
             f"现在像哪种环境（{m['month']}，只描述，不据此换配置）：{m['labels']}；36 个月股债相关 {m['corr']:+.2f}" if m.get("corr") is not None
             else f"现在像哪种环境（{m['month']}）：{m['labels']}"]
    return "\n".join(lines)
