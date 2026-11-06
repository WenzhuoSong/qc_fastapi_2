"""Big market days (user's request 2026-11-06, after the 2026-11-05 drop: SPY -4.4 % and a digest that read like
any other day).

When SPY moves at least 2 ex-ante daily sigmas (63-session volatility) or 2.5 %, the digest gets a block under the
market line:
- how rare the move is;
- what followed the past days with a move at least this large in the same direction (SPY since 1993 from Yahoo;
  1 week / 1 month / 3 months later: share up, median, the bad one-in-ten). Such days cluster in crises, so they
  are not independent;
- how broad it was: share of liquid stocks down / up, the sectors leading the move, VIX;
- what it did to the holdings: today's P&L and its market part;
- the reported cause: one web search through the external facts store. Verified facts are shown in full,
  unreachable ones as 有报道称 with numbers masked.
Descriptive, never a forecast. The block is stored in the eod brief payload as `market_day`.
"""
from __future__ import annotations

import logging
import math
from datetime import date, timedelta

import numpy as np
import pandas as pd

logger = logging.getLogger("scout.market_day")

SIGMAS = 2.0
MIN_MOVE = 0.025
STEP = 0.005                  # past days counted: same direction, at least today's move rounded down to 0.5 %
HISTORY_START = date(1993, 1, 29)
VOL_DAYS = 63
AFTER = {5: "1 周", 21: "1 个月", 63: "3 个月"}
EPISODE_GAP = 21              # sessions between two big days that start a new episode
BIG_STOCK_MOVE = 0.05
SECTORS_SHOWN = 3
CAUSE_MAX = 3
CAUSE_DAYS = 2                # facts published this many days before the session or later
BETA_ERR = 0.3                # a typical beta estimation error, to show how a big SPY day magnifies it


def triggered(ret: float | None, sigma: float | None) -> bool:
    if ret is None or (isinstance(ret, float) and math.isnan(ret)):
        return False
    return abs(ret) >= MIN_MOVE or (bool(sigma) and abs(ret) >= SIGMAS * sigma)


def history(close: pd.Series) -> dict | None:
    """Rarity of the last day's move and what followed past days at least as large in the same direction."""
    c = close.dropna().sort_index()
    r = c.pct_change()
    if len(r) < VOL_DAYS + 2:
        return None
    today = float(r.iloc[-1])
    sigma = float(r.iloc[-VOL_DAYS - 1:-1].std())
    sign = 1 if today > 0 else -1
    past = r.iloc[:-1]
    rank = int((sign * past > abs(today) + 1e-9).sum()) + 1
    threshold = max(STEP, math.floor(round(abs(today) / STEP, 6)) * STEP)
    hits = past[sign * past >= threshold - 1e-9]
    pos = [c.index.get_loc(d) for d in hits.index]
    episodes = sum(1 for i, p in enumerate(pos) if i == 0 or p - pos[i - 1] > EPISODE_GAP)
    years = pd.Series([d.year for d in hits.index]).value_counts()
    after = {}
    for h in AFTER:
        fwd = (c.shift(-h) / c - 1).reindex(hits.index).dropna()
        if len(fwd):
            after[str(h)] = {"n": int(len(fwd)), "up": float((fwd > 0).mean()), "median": float(fwd.median()),
                             "p10": float(fwd.quantile(0.1))}
    return {"since": str(c.index[0]), "ret": today, "sigma": sigma, "sigmas": abs(today) / sigma if sigma else None,
            "rank": rank, "threshold": threshold, "n": int(len(hits)), "episodes": episodes,
            "years": [[int(y), int(k)] for y, k in years.head(3).items()], "last": str(hits.index[-1]) if len(hits) else None,
            "after": after}


def breadth(rets: pd.Series, up: bool) -> dict | None:
    """Share of liquid stocks moving with the market today, and the share moving 5 % or more that way."""
    x = rets.dropna()
    if not len(x):
        return None
    sign = 1 if up else -1
    return {"n": int(len(x)), "with": float((sign * x > 0).mean()), "big": float((sign * x >= BIG_STOCK_MOVE).mean())}


def sector_moves(rows: list[dict], up: bool) -> dict:
    """Sectors leading the move and the ones moving least that way."""
    s = sorted((r for r in rows if r.get("kind") == "sector" and r.get("ret_1d") is not None),
               key=lambda r: r["ret_1d"], reverse=up)
    pick = lambda xs: [[r["name"], float(r["ret_1d"])] for r in xs]
    return {"lead": pick(s[:SECTORS_SHOWN]), "least": pick(s[::-1][:SECTORS_SHOWN])}


def holdings_day(items: list[dict], account: float | None) -> dict | None:
    """Today's P&L of the open trades and the part beta × SPY explains. items: value, ret_1d, market_ret_1d."""
    rows = [x for x in items if x.get("value") and x.get("ret_1d") is not None]
    if not rows:
        return None
    pnl = sum(x["value"] * x["ret_1d"] / (1 + x["ret_1d"]) for x in rows)
    market = sum(x["value"] / (1 + x["ret_1d"]) * x["market_ret_1d"] for x in rows if x.get("market_ret_1d") is not None)
    return {"pnl": pnl, "market": market, "pct": pnl / account if account else None}


def cause_lines(facts: list[dict], asof: date) -> list[dict]:
    from scout import external_facts

    out = []
    for f in facts:
        if f["status"] not in ("verified", "unreachable") or (f.get("subject") or "").upper() != "MACRO":
            continue
        if f.get("published") and f["published"] < asof - timedelta(days=CAUSE_DAYS):
            continue
        claim = f["claim"] if f["status"] == "verified" else "有报道称：" + external_facts._DIGITS.sub("…", f["claim"])
        out.append({"claim": claim, "source": external_facts.label(f), "url": f["url"], "id": f["id"]})
    return out[:CAUSE_MAX]


async def cause(asof: date, spy: float, qqq: float | None) -> list[dict] | None:
    from scout import external_facts
    from scout.config import get_scout_settings

    if not get_scout_settings().openai_api_key:        # no search ran: say nothing rather than "nothing found"
        return None
    way = "fell" if spy < 0 else "rose"
    nq = f", the Nasdaq-100 (QQQ) {qqq * 100:+.1f}%" if qqq is not None else ""
    found = await external_facts.collect(
        f"Today is {asof}. The US stock market {way} sharply today: the S&P 500 (SPY) {spy * 100:+.1f}%{nq}. "
        "What were the main reported causes of today's move in the overall US market? Use reports dated today.",
        ["MACRO"], "market_day_cause", today=asof)
    return cause_lines(found["facts"], asof)


async def build(p, asof: date, sector_rows: list[dict], held: list[dict], account: float | None,
                search: bool = True) -> dict | None:
    """The block's data, or None on an ordinary day. `held`: value, ret_1d, market_ret_1d per open trade."""
    from scout.data.yahoo import YahooProvider
    from scout.universe import BENCHMARK

    spy_r = p["ret_1d"][BENCHMARK]
    ret = float(spy_r.loc[asof]) if pd.notna(spy_r.loc[asof]) else None
    prev = spy_r.loc[:asof].iloc[-VOL_DAYS - 1:-1]
    sigma = float(prev.std()) if len(prev) >= 40 else None
    if not triggered(ret, sigma):
        return None
    up = ret > 0
    out: dict = {"date": asof.isoformat(), "ret": ret, "sigma": sigma, "up": up}
    qqq = p["ret_1d"]["QQQ"].loc[asof] if "QQQ" in p["ret_1d"].columns else None
    out["qqq"] = float(qqq) if qqq is not None and pd.notna(qqq) else None
    try:
        bars = await YahooProvider().daily_bars([BENCHMARK, "^VIX"], HISTORY_START, asof)
    except Exception as exc:              # the long history is extra; the block still has breadth and holdings
        logger.warning("market day history failed: %s", exc)
        bars = {}
    spy = bars.get(BENCHMARK)
    if spy is not None and len(spy):
        close = spy["close"].copy()
        if close.index[-1] != asof:       # Yahoo can lag at the close; Scout's own bar for the session
            close.loc[asof] = float(p["close"].at[asof, BENCHMARK])
        out["history"] = history(close)
    vix = bars.get("^VIX")
    if vix is not None and len(vix) >= 2 and vix.index[-1] == asof:
        out["vix"] = [float(vix["close"].iloc[-2]), float(vix["close"].iloc[-1])]
    liquid = p["liquid"].loc[asof].reindex(p.stocks).fillna(False).astype(bool)
    out["breadth"] = breadth(p["ret_1d"].loc[asof].reindex(p.stocks)[liquid], up)
    out["sectors"] = sector_moves(sector_rows, up)
    out["holdings"] = holdings_day(held, account)
    if search:
        try:
            out["cause"] = await cause(asof, ret, out["qqq"])
        except Exception as exc:          # a search failure must not cost the digest
            logger.warning("market day cause failed: %s", exc)
            out["cause"] = None
    return out


def _p(x: float | None, sign: bool = True, d: int = 1) -> str:
    if x is None:
        return "—"
    return f"{x * 100:+.{d}f}%" if sign else f"{x * 100:.{d}f}%"


def lines(d: dict | None) -> list[str]:
    """Plain-text lines for the digest (the caller escapes them)."""
    if not d:
        return []
    up = d["up"]
    word, move = ("大涨日", "涨") if up else ("大跌日", "跌")
    h = d.get("history")
    head = f"⚡ {word}：SPY {_p(d['ret'])}"
    if d.get("qqq") is not None:
        head += f"，QQQ {_p(d['qqq'])}"
    if d.get("sigma"):
        head += f"；约 {abs(d['ret']) / d['sigma']:.1f} 倍日常波动（过去 63 日日波动约 {_p(d['sigma'], False)}）"
    if h:
        head += f"；{h['since'][:4]} 年以来第 {h['rank']} 大的单日{move}幅"
    out = [head]
    if h and h["n"]:
        years = "、".join(f"{y}（{k} 天）" for y, k in h["years"])
        out.append(f"{h['since'][:4]} 年以来单日{move} {h['threshold'] * 100:.1f}% 以上共 {h['n']} 天，分属 {h['episodes']} 段行情，"
                   f"最多的年份：{years}；最近一次 {h['last']}。")
        out.append("这些日子之后的 SPY（扎堆出现、彼此不独立，只是历史描述，不是预测）：")
        out += [f"  {AFTER[int(k)]}：{x['up'] * 100:.0f}% 的时候上涨，中位数 {_p(x['median'])}，十次里较差的一次 {_p(x['p10'])}（{x['n']} 次）"
                for k, x in h["after"].items()]
    elif h:
        out.append(f"{h['since'][:4]} 年以来没有更大的单日{move}幅。")
    parts = []
    b = d.get("breadth")
    if b:
        parts.append(f"流动性好的 {b['n']} 只股票里 {b['with'] * 100:.0f}% {'上涨' if up else '下跌'}，"
                     f"{b['big'] * 100:.0f}% {move}超 5%")
    s = d.get("sectors") or {}
    if s.get("lead"):
        parts.append(("领涨" if up else "领跌") + "：" + "、".join(f"{n} {_p(r)}" for n, r in s["lead"]))
    if s.get("least"):
        parts.append(("最弱" if up else "最抗跌") + "：" + "、".join(f"{n} {_p(r)}" for n, r in s["least"]))
    if d.get("vix"):
        a, z = d["vix"]
        parts.append(f"VIX {a:.1f} → {z:.1f}（{_p(z / a - 1, d=0)}）")
    if parts:
        out.append("宽度：" + "；".join(parts))
    hd = d.get("holdings")
    if hd:
        share = f"（占账户 {_p(hd['pct'])}）" if hd.get("pct") is not None else ""
        out.append(f"持仓今天合计 {hd['pnl']:+,.0f}{share}，其中按 β 算的大盘部分约 {hd['market']:+,.0f}")
    out.append(f"注意：这样的日子里 β 的小误差会被放大（β 差 {BETA_ERR:g} × SPY {_p(d['ret'])} ≈ {abs(d['ret']) * BETA_ERR * 100:.1f} 个百分点），"
               "下面各股的大盘 / 行业 / 公司自身拆分只作参考。")
    if d.get("cause"):
        out.append("原因（据外部报道（未经核实））：")
        out += [f"  · {c['claim']}（{c['source']}）" for c in d["cause"]]
    elif d.get("cause") is not None:
        out.append("原因：外部报道里没有找到可用的说法，问助手或看新闻。")
    return out
