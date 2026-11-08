"""Technical and trend playbooks on single stocks (catalogue section 3).

Where Scout already tested a playbook's entry (`scout.rankers`), the same function decides the buys here, so the
verdict and its knowledge-base status describe the same rule. Exits follow each author; approximations of
authors who left no fixed rule (Livermore, Wyckoff) say so in their version line.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from scout.playbooks.base import BUY, FRESH_DAYS, HOLD, SELL, Ctx, Verdict, crossed_down, crossed_up, num, pct


VOL_ZH = {"up": "放大", "down": "缩小", "flat": "持平"}


# ── shared ──

def _ago(mask: pd.DataFrame, n: int = 60) -> pd.Series:
    """Sessions since the last True in the last n rows per column (0 = today), NaN when none."""
    tail = mask.iloc[-n:].to_numpy(dtype=bool)
    has = tail.any(axis=0)
    last = n - 1 - np.argmax(tail[::-1], axis=0)
    return pd.Series(np.where(has, n - 1 - last, np.nan), index=mask.columns)


def _tr(ctx: Ctx) -> pd.DataFrame:
    if "tr" not in ctx.cache:
        h, l, pc = ctx.high, ctx.low, ctx.close.shift(1)
        ctx.cache["tr"] = np.maximum(np.maximum(h - l, (h - pc).abs()), (l - pc).abs())
    return ctx.cache["tr"]


def _ranker_picks(ctx: Ctx, name: str, frac: float, day_offset: int = 0) -> list[str]:
    """Stocks a tested Scout ranker would hold from the next open, computed `day_offset` sessions ago."""
    from scout import rankers

    a, order = ctx.arrays
    i = len(a["dates"]) - 1 - day_offset
    if name in rankers.ANGLE_RANKERS:
        col = {t: j for j, t in enumerate(order)}
        a["angle"] = {i: {col[t]: v for t, v in ctx.angle.get(name, {}).items() if t in col}}
    picks = rankers.RANKERS[name](a, i, frac)
    return [order[j] for j in picks]


def ranked(ctx: Ctx, name: str, frac: float, label: str, picker=None) -> dict[str, Verdict]:
    """A tested ranking: today's picks are buys; a holding outside three times the pick share is a sell (the
    ranking's own rule at its next rebalance)."""
    picker = picker or (lambda q: _ranker_picks(ctx, name, q))
    picks = picker(frac)
    wide = set(picker(min(1.0, 3 * frac)))
    out = {t: Verdict(BUY, f"{label}排名第 {k + 1} / {len(picks)}", score=-k) for k, t in enumerate(picks)}
    for h in {x["ticker"] for x in ctx.holdings} | set(ctx.watchlist):
        if h not in out and h in ctx.stocks and h not in wide:
            out[h] = Verdict(SELL, f"已不在{label}前 {round(3 * frac * 100)}%（换仓时卖出）")
    return out


def every_day(ctx: Ctx, name: str, days: int = FRESH_DAYS) -> dict[str, int]:
    """Stocks an all-that-qualify ranker picked in the last `days` sessions → sessions ago."""
    out: dict[str, int] = {}
    for k in range(days):
        for t in _ranker_picks(ctx, name, 0.0, day_offset=k):
            out.setdefault(t, k)
    return out


# ── tested rankers ──

def high_52w(ctx: Ctx) -> dict[str, Verdict]:
    return ranked(ctx, "high_52w", 0.1, "离 52 周高点最近")


def residual_momentum(ctx: Ctx) -> dict[str, Verdict]:
    """Needs 36 months of closes: computed on `Ctx.long_close` with today's liquid stocks."""
    from scout import rankers

    lc = ctx.long_close
    if lc.empty or "SPY" not in lc:
        return {}
    order = [t for t in ctx.stocks if t in lc.columns]
    a = {"close": lc[order].to_numpy(dtype=float), "spy": lc["SPY"].to_numpy(dtype=float),
         "liquid": np.ones((len(lc), len(order)), dtype=bool)}
    return ranked(ctx, "residual_momentum", 0.1, "残差动量",
                  picker=lambda q: [order[j] for j in rankers.residual_momentum(a, len(lc) - 1, q)])


def frog_in_pan(ctx: Ctx) -> dict[str, Verdict]:
    return ranked(ctx, "frog_in_pan", 0.1, "平滑动量")


def short_term_reversal(ctx: Ctx) -> dict[str, Verdict]:
    return ranked(ctx, "short_term_reversal", 0.1, "上月跌幅")


def minervini(ctx: Ctx) -> dict[str, Verdict]:
    """Trend template (Scout's tested version) → volatility contraction → pivot breakout; sell on a close below the
    50-day average on heavy volume."""
    passing = set(_ranker_picks(ctx, "trend_template", 0.0))
    c, h, l = ctx.close, ctx.high, ctx.low
    vr, sma50 = ctx.f["vol_ratio"], ctx.f["sma50"]
    out = {}
    for t in ctx.stocks:
        if t in passing:
            hh, ll, cc = h[t].iloc[-66:-FRESH_DAYS].to_numpy(), l[t].iloc[-66:-FRESH_DAYS].to_numpy(), c[t].iloc[-FRESH_DAYS:]
            if len(hh) == 61 and np.isfinite(hh).all() and np.isfinite(ll).all():
                depth = [(hh[k:k + 20].max() - ll[k:k + 20].min()) / hh[k:k + 20].max() for k in (1, 21, 41)]
                pivot = hh[-25:].max()
                vol = vr[t].iloc[-FRESH_DAYS:]
                hit = (cc > pivot) & (cc <= 1.05 * pivot) & (vol >= 1.5)
                if depth[0] > depth[1] > depth[2] and depth[2] <= 0.10 and bool(hit.any()):
                    out[t] = Verdict(BUY, f"趋势模板通过；三段回调幅度 {pct(depth[0], 0)} → {pct(depth[1], 0)} → {pct(depth[2], 0)} 收缩；"
                                          f"放量突破枢轴 {num(pivot)}，止损参考 {num(pivot * 0.92)}（−8%）", score=float(-depth[2]))
                    continue
            out[t] = Verdict(HOLD, "趋势模板 8 条全部满足（还没有收缩后的突破买点）", score=float(ctx.f["rs_pct"][t].iloc[-1]))
        elif crossed_down(c[t], sma50[t], FRESH_DAYS) and float(vr[t].iloc[-FRESH_DAYS:].max()) >= 1.5:
            out[t] = Verdict(SELL, f"放量跌破 50 日均线 {num(sma50[t].iloc[-1])}")
    return out


def weinstein(ctx: Ctx) -> dict[str, Verdict]:
    """Stage 2 breakout (Scout's tested entry) and Weinstein's own exit: a weekly close back below the 30-week average."""
    fresh = every_day(ctx, "weinstein_stage2")
    d, s = ctx.f["wk_dist_sma30"].iloc[-1], ctx.f["wk_sma30_slope_4w"].iloc[-1]
    out = {}
    for t in ctx.stocks:
        if t in fresh:
            out[t] = Verdict(BUY, f"周线放量上穿 30 周均线（{fresh[t]} 个交易日前），均线不再下降", score=-fresh[t])
        elif d.get(t, np.nan) > 0 and s.get(t, np.nan) >= 0:
            out[t] = Verdict(HOLD, f"第二阶段：周收盘高于 30 周均线 {pct(d[t])}，均线 4 周变化 {pct(s[t])}")
        elif d.get(t, np.nan) < 0 and s.get(t, np.nan) <= 0:
            out[t] = Verdict(SELL, f"第三 / 四阶段：周收盘低于 30 周均线 {pct(d[t])}，均线在走平或下降")
    return out


def elder(ctx: Ctx) -> dict[str, Verdict]:
    fresh = every_day(ctx, "elder_triple_screen")
    chg = ctx.f["wk_macd_hist_chg"]
    new = ctx.f["wk_new"].iloc[:, 0].to_numpy(dtype=bool)
    ends = np.flatnonzero(new)
    out = {}
    if len(ends) < 2:
        return out
    now, before = chg.iloc[ends[-1]], chg.iloc[ends[-2]]
    for t in ctx.stocks:
        if t in fresh:
            out[t] = Verdict(BUY, f"周线 MACD 柱上升（潮流向上），日线 2 日力量指数转负（回调）{fresh[t]} 个交易日前", score=-fresh[t])
        elif now.get(t, np.nan) > 0:
            out[t] = Verdict(HOLD, "周线 MACD 柱仍在上升")
        elif now.get(t, np.nan) < 0 and before.get(t, np.nan) > 0:
            out[t] = Verdict(SELL, "上周周线 MACD 柱转为下降（潮流转向）")
    return out


def macd_cross(ctx: Ctx) -> dict[str, Verdict]:
    from scout.features import macd

    if "macd_hist" not in ctx.cache:
        ctx.cache["macd_hist"] = macd(ctx.close[ctx.stocks])[1]
    hist = ctx.cache["macd_hist"]
    up, down = _ago((hist > 0) & (hist.shift(1) <= 0)), _ago((hist < 0) & (hist.shift(1) >= 0))
    out = {}
    for t in ctx.stocks:
        u, dn, now = up.get(t, np.nan), down.get(t, np.nan), hist[t].iloc[-1]
        if u < FRESH_DAYS and not dn < u:
            out[t] = Verdict(BUY, f"MACD（12/26/9）金叉，{int(u)} 个交易日前", score=-u)
        elif dn < FRESH_DAYS and not u < dn:
            out[t] = Verdict(SELL, f"MACD 死叉，{int(dn)} 个交易日前")
        elif now > 0:
            out[t] = Verdict(HOLD, "MACD 柱为正")
    return out


def golden_cross(ctx: Ctx, tickers: list[str] | None = None) -> dict[str, Verdict]:
    s50, s200, out = ctx.f["sma50"], ctx.f["sma200"], {}
    for t in tickers or ctx.stocks:
        if t not in s200.columns or not np.isfinite(s200[t].iloc[-1]):
            continue
        a, b = s50[t], s200[t]
        if crossed_up(a, b, FRESH_DAYS):
            out[t] = Verdict(BUY, f"50 日均线 {num(a.iloc[-1])} 上穿 200 日均线 {num(b.iloc[-1])}（金叉）", score=float(a.iloc[-1] / b.iloc[-1]))
        elif crossed_down(a, b, FRESH_DAYS):
            out[t] = Verdict(SELL, f"50 日均线跌破 200 日均线（死叉）")
        elif a.iloc[-1] > b.iloc[-1]:
            out[t] = Verdict(HOLD, f"50 日均线在 200 日均线上方 {pct(a.iloc[-1] / b.iloc[-1] - 1)}")
        else:
            out[t] = Verdict(SELL, f"50 日均线在 200 日均线下方 {pct(a.iloc[-1] / b.iloc[-1] - 1)}")
    return out


def connors_rsi2(ctx: Ctx) -> dict[str, Verdict]:
    """Buy a close above the 200-day average with RSI(2) < 10; sell (a holding) on a close above the 5-day average."""
    c, rsi2, s200 = ctx.close, ctx.f["rsi2"], ctx.f["sma200"]
    sma5 = c.rolling(5).mean().iloc[-1]
    held = {x["ticker"] for x in ctx.holdings}
    out = {}
    for t in ctx.stocks:
        r, px, base = rsi2[t].iloc[-1], c[t].iloc[-1], s200[t].iloc[-1]
        if px > base and r < 10:
            out[t] = Verdict(BUY, f"在 200 日均线上方 {pct(px / base - 1)}，RSI(2) {num(r, 1)} < 10；收盘高于 5 日均线 {num(sma5[t])} 时卖",
                             score=float(-r))
        elif t in held and px > sma5[t]:
            out[t] = Verdict(SELL, f"收盘高于 5 日均线 {num(sma5[t])}（短线目标达到）")
    return out


# ── breakout systems ──

def turtle(ctx: Ctx, entry: int, exit_: int) -> dict[str, Verdict]:
    c, h, l = ctx.close[ctx.stocks], ctx.high[ctx.stocks], ctx.low[ctx.stocks]
    brk = _ago(c > h.rolling(entry).max().shift(1))
    ext = _ago(c < l.rolling(exit_).min().shift(1))
    n = _tr(ctx)[ctx.stocks].rolling(20).mean().iloc[-1]
    out = {}
    for t in ctx.stocks:
        b, e = brk.get(t, np.nan), ext.get(t, np.nan)
        unit = f"N（20 日 ATR）{num(n[t])}，按 1% 风险每 10 万美元约 {int(1000 / n[t])} 股，止损 2N = {num(c[t].iloc[-1] - 2 * n[t])}" \
            if np.isfinite(n[t]) and n[t] > 0 else ""
        if b < FRESH_DAYS and not e < b:
            out[t] = Verdict(BUY, f"收盘突破前 {entry} 日最高（{int(b)} 个交易日前）；{unit}", score=-b)
        elif e < FRESH_DAYS and not b < e:
            out[t] = Verdict(SELL, f"收盘跌破前 {exit_} 日最低（{int(e)} 个交易日前）")
        elif np.isfinite(b) and not e < b:
            out[t] = Verdict(HOLD, f"{int(b)} 个交易日前突破 {entry} 日高点，之后没有跌破 {exit_} 日低点")
    return out


def darvas_box(h: np.ndarray, l: np.ndarray) -> tuple[float, float] | None:
    """The latest completed Darvas box: a 52-week high followed by three lower highs; the box bottom is the lowest
    low of those three sessions. Returns (top, bottom)."""
    n = len(h)
    for k in range(n - 4, 251, -1):
        if h[k] >= np.nanmax(h[k - 251:k + 1]) and np.all(h[k + 1:k + 4] < h[k]):
            return float(h[k]), float(np.nanmin(l[k + 1:k + 4]))
    return None


def darvas(ctx: Ctx) -> dict[str, Verdict]:
    c, h, l, v = ctx.close, ctx.high, ctx.low, ctx.volume
    out = {}
    for t in ctx.stocks:
        hh, ll = h[t].to_numpy(), l[t].to_numpy()
        hh_b = hh[:-FRESH_DAYS]
        box = darvas_box(hh_b, ll[:-FRESH_DAYS]) if len(hh_b) > 260 else None
        if not box:
            continue
        top, bottom = box
        cc, vv = c[t].iloc[-FRESH_DAYS:], v[t].iloc[-FRESH_DAYS:]
        avg = float(v[t].iloc[-25:-FRESH_DAYS].mean())
        px = float(cc.iloc[-1])
        if bool(((cc > top) & (vv >= avg)).any()) and px > bottom:
            out[t] = Verdict(BUY, f"放量收盘突破箱顶 {num(top)}；止损放在箱底 {num(bottom)} 下方，新箱体形成后上移",
                             score=float(top / px))
        elif px < bottom:
            out[t] = Verdict(SELL, f"跌破最近箱底 {num(bottom)}")
        elif px > top:
            out[t] = Verdict(HOLD, f"在箱顶 {num(top)} 上方运行，止损在箱底 {num(bottom)}")
    return out


def wilcox(ctx: Ctx) -> dict[str, Verdict]:
    """New all-time high (since Scout's data start, 2014) entry, 10 × ATR42 trailing stop from the highest close."""
    c, h = ctx.close, ctx.high
    atr42 = _tr(ctx).rolling(42).mean().iloc[-1]
    out = {}
    for t in ctx.stocks:
        hist = max(ctx.prior_high.get(t, -math.inf), float(np.nanmax(h[t].iloc[:-FRESH_DAYS])) if len(h[t]) > FRESH_DAYS else -math.inf)
        cc = c[t].iloc[-FRESH_DAYS:]
        top = float(np.nanmax(c[t].iloc[-252:]))
        stop = top - 10 * float(atr42[t])
        px = float(cc.iloc[-1])
        if np.isfinite(hist) and bool((cc > hist).any()):
            out[t] = Verdict(BUY, f"创 2014 年以来新高（前高 {num(hist)}）；跟踪止损 = 最高收盘 − 10 × ATR42 = {num(stop)}",
                             score=float(px / stop - 1))
        elif px < stop:
            out[t] = Verdict(SELL, f"收盘低于跟踪止损 {num(stop)}（一年内最高收盘 {num(top)} − 10 × ATR42）")
        elif np.isfinite(hist) and top >= 0.98 * hist:
            out[t] = Verdict(HOLD, f"一年内创过新高，仍在跟踪止损 {num(stop)} 上方")
    return out


def clenow(ctx: Ctx) -> dict[str, Verdict]:
    """Stocks on the Move: S&P 500 members ranked by the annualised 90-day exponential slope × R², excluding those
    below their 100-day average or with a one-day move above 15 % in 90 days; new buys only while the S&P 500 is above
    its 200-day average; positions sized at 0.1 % of the account per ATR."""
    c = ctx.close
    members = [t for t in ctx.stocks if "sp500" in ctx.sources.get(t, [])]
    lc = np.log(c[members].iloc[-90:].to_numpy(dtype=float))
    x = np.arange(90.0)
    ok = np.isfinite(lc).all(axis=0)
    xm = x - x.mean()
    slope = (xm[:, None] * (lc - lc.mean(axis=0))).sum(axis=0) / (xm ** 2).sum()
    fit = lc.mean(axis=0) + slope * xm[:, None]
    ss_res, ss_tot = ((lc - fit) ** 2).sum(axis=0), ((lc - lc.mean(axis=0)) ** 2).sum(axis=0)
    r2 = 1 - ss_res / np.where(ss_tot > 0, ss_tot, np.nan)
    score = pd.Series((np.exp(slope * 252) - 1) * r2, index=members)[ok]
    sma100 = c[members].rolling(100).mean().iloc[-1]
    jump = c[members].pct_change().iloc[-90:].abs().max()
    eligible = score[(c[members].iloc[-1] > sma100) & (jump <= 0.15)].sort_values(ascending=False)
    top = eligible[eligible > 0].iloc[: max(1, len(members) // 5)]
    spy = c["SPY"]
    index_ok = bool(spy.iloc[-1] > spy.rolling(200).mean().iloc[-1])
    atr = _tr(ctx)[members].rolling(20).mean().iloc[-1]
    out = {}
    for k, (t, s) in enumerate(top.items()):
        size = f"，每 10 万美元约 {int(100 / atr[t])} 股（ATR {num(atr[t])}）" if atr[t] > 0 else ""
        why = f"动量分（年化斜率 × R²）{num(s)}，标普 500 中第 {k + 1}{size}"
        out[t] = Verdict(BUY, why, score=float(s)) if index_ok else Verdict(HOLD, why + "；标普 500 在 200 日均线下方，不买新股")
    bottom = set(score.sort_values().index[: len(members) // 5])
    for t in members:
        if t in out:
            continue
        if not c[t].iloc[-1] > sma100[t]:
            out[t] = Verdict(SELL, f"跌破 100 日均线 {num(sma100[t])}")
        elif jump[t] > 0.15:
            out[t] = Verdict(SELL, f"90 日内单日波动 {pct(jump[t], 0)} 超过 15%")
        elif t in bottom:
            out[t] = Verdict(SELL, f"动量分 {num(score.get(t))} 在标普 500 后 20%")
    return out


def livermore(ctx: Ctx) -> dict[str, Verdict]:
    """Scout's approximation of a pivotal point: a new 52-week closing high on at least twice the 50-day volume after
    at least 20 sessions inside a 15 % range; out on a close back below that range."""
    c, vr = ctx.close, ctx.f["vol_ratio"]
    out = {}
    for t in ctx.stocks:
        cc = c[t].to_numpy(dtype=float)
        if len(cc) < 260 or not np.isfinite(cc[-260:]).all():
            continue
        for k in range(1, FRESH_DAYS + 1):
            i = len(cc) - k
            base = cc[i - 20:i]
            if cc[i] > cc[i - 252:i].max() and base.max() / base.min() - 1 <= 0.15 and vr[t].iloc[i] >= 2:
                low = float(base.min())
                out[t] = (Verdict(BUY, f"在 {pct(base.max() / base.min() - 1, 0)} 区间整理 20 天后放量（{num(vr[t].iloc[i], 1)} 倍）"
                                       f"收出 52 周新高；跌回区间低点 {num(low)} 下方即离场", score=-k)
                          if cc[-1] > low else Verdict(SELL, f"突破后跌回整理区间低点 {num(low)} 下方"))
                break
    return out


def wyckoff_spring(ctx: Ctx) -> dict[str, Verdict]:
    """Scout's approximation: a 60-session range no wider than 25 %; a spring = a low under the range followed by a
    close back inside on at least 1.5 × volume (buy, stop under the spring low); a close 2 % under the range on that
    volume = sign of weakness (sell)."""
    c, l, h, vr = ctx.close, ctx.low, ctx.high, ctx.f["vol_ratio"]
    out = {}
    for t in ctx.stocks:
        hi, lo = h[t].iloc[-65:-FRESH_DAYS], l[t].iloc[-65:-FRESH_DAYS]
        if len(hi) < 60 or not (np.isfinite(hi).all() and np.isfinite(lo).all()):
            continue
        top, bottom = float(hi.max()), float(lo.min())
        if top / bottom - 1 > 0.25:
            continue
        ll, cc, vv = l[t].iloc[-FRESH_DAYS:], c[t].iloc[-FRESH_DAYS:], vr[t].iloc[-FRESH_DAYS:]
        if bool(((ll < bottom) & (cc > bottom) & (vv >= 1.5)).any()) and cc.iloc[-1] > bottom:
            out[t] = Verdict(BUY, f"跌破 60 日区间下沿 {num(bottom)} 后放量收回（弹簧）；止损放在本周最低 {num(ll.min())} 下方",
                             score=float(vv.max()))
        elif bool(((cc < 0.98 * bottom) & (vv >= 1.5)).any()):
            out[t] = Verdict(SELL, f"放量收在 60 日区间下沿 {num(bottom)} 下方 2% 以上（弱势信号）")
    return out


def chart_patterns(ctx: Ctx) -> dict[str, Verdict]:
    """Lo, Mamaysky & Wang's detection (Scout's tested implementation) on this week's windows; Edwards & Magee's
    direction: head-and-shoulders bottom / double bottom buy, head-and-shoulders top / double top / broadening top sell."""
    from scout import chart_patterns as cp

    out = {}
    for t in ctx.stocks:
        cc, vv = ctx.close[t].to_numpy(dtype=float)[-80:], ctx.volume[t].to_numpy(dtype=float)[-80:]
        ok = np.zeros(len(cc), dtype=bool)
        ok[-FRESH_DAYS:] = True
        for d in cp.detect(cc, vv, ok):
            side = cp.BOOK.get(d["pattern"])
            if side:
                out[t] = Verdict(BUY if side > 0 else SELL, f"{cp.ZH[d['pattern']]}（{len(cc) - 1 - d['i']} 个交易日前完成，"
                                                            f"后半段成交量{VOL_ZH[d['vol']]}）")
    return out
