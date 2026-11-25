"""The daily market read (user's request 2026-11-25): after each close, why the market moved, written like a strategist's
note on Python-computed evidence, replacing the separate market lines of the Telegram digest.

Evidence (all computed here or taken from other modules): index, style and cross-asset moves with each one's size in
its own daily sigmas (63 sessions; ≥ UNUSUAL = beyond the ordinary); breadth; each stock's contribution to the S&P 500
(weight from Yahoo's market value, else diluted shares × close, so an estimate); sectors leading and lagging and
the day's trend changes; macro data released today against the forecast (pre-market pack, BLS actuals); prediction
market odds on macro topics with their change; large companies that reported; market headlines (titles, unverified)
and, on a big day, the verified causes (`market_day`); the holdings' β and day; the market filter; yesterday's read.

One structured `SCOUT_AGENT_MODEL` call writes: headline, what happened, 1–4 drivers (each citing evidence keys, its
strength: 强 only with an unusual move, a contribution share ≥ STRONG_SHARE or a data surprise among its keys),
what the media say against the data, the story line against yesterday's read, what it means for the holdings, what
would change the read. Validated (numbers in the pack, keys exist, the strength rule, no forecasts or actions, the
story status consistent), one correction round, else a rule-based read. Explanations after the fact: never a forecast.
Stored as brief `market_read`; the digest's short version carries its summary; `/market`; agent `get_market_read`.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, timedelta
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field

from scout import db, responses
from scout.config import get_scout_settings
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.market_read")

VOL_DAYS = 63
UNUSUAL = 2.0                      # daily sigmas: a move this large is beyond the ordinary
STRONG_SHARE = 0.3                 # a contribution carrying this share of the S&P 500's day can be a strong driver
CONTRIB_SHOWN = 5
ODDS_MOVE = 0.03                   # a prediction market outside rates / inflation is shown only after a move this large in a day
RATES = re.compile(r"美联储|降息|加息|利率|通胀|衰退|Fed|rate|inflation|recession", re.I)
ETFS = (("SPY", "标普 500", "大盘"), ("QQQ", "纳指 100", "科技成长"), ("RSP", "等权标普 500", "普通股票"), ("IWM", "罗素 2000", "小盘股"),
        ("DIA", "道琼斯", "老牌大公司"), ("SMH", "半导体", "AI 与芯片"), ("IGV", "软件", "软件"), ("TLT", "长期美债", "价格涨 = 长期利率降"),
        ("GLD", "黄金", "避险与实际利率"), ("UUP", "美元", "美元强弱"), ("USO", "原油", "能源与通胀预期"), ("IBIT", "比特币", "投机情绪"),
        ("LQD", "投资级公司债", "信用"))
LIVE = (("^TNX", "10 年期美债收益率", "长期利率；单位是百分点"), ("^VIX", "VIX", "标普期权隐含的未来 30 天波动；越高越紧张"),
        ("^VIX3M", "VIX 三个月", "高于 VIX 为常态；低于 VIX 表示短期恐慌更重"))
FORECAST = ("将上涨", "将下跌", "会上涨", "会下跌", "有望", "将延续", "会延续", "将继续", "会继续", "将持续", "会持续", "必将", "必然", "一定会",
            "预计将", "看涨后市", "看跌后市", "见底", "见顶", "反向指标", "逆向信号", "往往意味着")
ACTIONS = ("建议买入", "建议卖出", "应该买", "应该卖", "可以买入", "可以卖出", "加仓", "减仓", "抄底", "止盈")


class Driver(BaseModel):
    title: str = Field(description="推动因素，≤ 16 字，例如「大型科技股拖累」")
    explanation: str = Field(description="1–2 句：它怎样推动了今天的市场，引用证据包里的数字")
    evidence: list[str] = Field(description="引用的证据 key（证据包里每条都有 key）")
    strength: Literal["强", "中", "弱"] = Field(description="证据强度：强 = 有超出日常波动的读数、贡献占比大或数据明显偏离预期；"
                                                        "中 = 数据支持但不突出；弱 = 只有新闻标题或间接证据")


class Read(BaseModel):
    headline: str = Field(description="一句话：今天市场发生了什么、主要因为什么，≤ 45 字")
    what_happened: str = Field(description="2–3 句：指数、风格、利率 / 美元 / 原油 / 波动、宽度，哪些超出日常波动")
    drivers: list[Driver] = Field(description="1–4 个推动因素，按重要性排序")
    media_vs_data: str = Field(description="1–2 句：新闻怎么解释今天，Scout 的数据是否支持；没有相关新闻就直说")
    story_status: Literal["延续", "被推翻", "新出现", "无前一天"] = Field(description="和昨天的市场解读相比")
    story: str = Field(description="1 句：昨天的主要解释今天是延续、被推翻，还是出现了新的故事；为什么")
    for_you: str = Field(description="1–2 句：对用户的持仓意味着什么（β、所在板块、今天的盈亏），以及计划现在要求什么")
    mood: str = Field(description="1–2 句：市场情绪和仓位处在历史的什么位置（只引用 sentiment 的读数），不据此推断后市")
    would_change: str = Field(description="1 句：什么样的数据出现，会说明今天的解释不对")


PROMPT = """你是一位美股策略分析师，收盘后给一位个人投资者写今天的市场解读。证据包全部由 Python 计算：
- moves：指数、风格、利率、美元、原油、黄金、比特币、信用、VIX 的当日 / 5 日 / 20 日变化，「日常波动倍数」= 当日变化除以过去 63 日的日波动，
  abs ≥ 2 标为「超出日常波动」；key 以 global. 开头的是同一天的海外市场：亚洲在美股开盘前已收盘（可能是美股当天波动的源头），
  欧洲和美股上午重叠；还有美元兑日元、美元兑人民币、铜；
- breadth：上涨股票比例、站上 50 日线比例；contributions：按估算市值权重，个股对标普 500 当日涨跌的贡献（百分点）和占比；
- sectors：当日领先和落后的行业，近 20 日相对 SPY，趋势确认变化；macro_data：今天公布的宏观数据实际值和预期；
- odds：预测市场对宏观话题的概率和变化（市场价格，不是事实）；earnings：刚公布财报的大公司和股价反应；
- news：市场新闻标题（未经核实），causes：大涨大跌日联网核实过的原因；holdings：用户的持仓；filter：大盘过滤；
- sentiment：情绪与仓位读数（看跌 / 看涨比、AAII 散户调查、VIX 及期限结构、新高新低、站上 50 日线比例），每项有历史位置和读数
  （偏恐慌 / 偏乐观 / 正常范围）；Scout 检验过这类读数对下周涨跌没有预测力，只用来描述情绪；
- yesterday：昨天的市场解读（没有就是 null）。

工作方法：
1. 先描述今天真正异常的是什么（看日常波动倍数），普通的波动不要说成大事。
2. 找推动因素：贡献集中在哪几家公司？风格（大小盘、成长价值、等权市值）怎么分化？利率、美元、原油、波动同时怎么动？有没有宏观数据或财报？
   海外是否先动了（亚洲收盘、日元、人民币、铜）？只有海外读数异常且方向一致时，才把它写成源头。
   把能互相印证的读数放在同一个推动因素里。每个因素标证据强度，引用 key。
3. 对照新闻：新闻标题的说法有没有数据支持；只有标题、没有数据支持的，强度最多为「弱」，并写明「据报道」。
4. 和昨天的解读对照：昨天的主要解释今天是延续还是被推翻。
5. 对用户：持仓今天怎样、跟大盘的关系（β），计划要求什么（没有要做的就说照计划持有）。
6. 情绪：用 sentiment 的读数说现在大家是紧张还是乐观、极端与否；读数之间矛盾就直说。不要写「恐慌往往意味着见底」之类的推断。

硬性规则：
- 数字只能原样引用证据包里的数值，不要自己计算；涨跌数字自带正负号，不要写「跌幅为 -1.2%」。
- evidence 只能填证据包里出现的 key。
- 这是事后解释，不是预测：不写「将 / 会 / 有望」上涨下跌、延续、见底见顶；不给买卖建议。
- 解释不了就说解释不了，不要编故事；中文、专业、简洁。"""


# ── evidence ──

def sigma_move(series: pd.Series, asof, level: bool = False) -> dict | None:
    """Day / 5 / 20-session change of a close (or a level for yields) and the day in its own daily sigmas."""
    s = series.dropna()
    s = s[s.index <= asof]
    if len(s) < VOL_DAYS + 2:
        return None
    ch = s.diff() if level else s.pct_change()
    day, past = ch.iloc[-1], ch.iloc[-VOL_DAYS - 1:-1]
    sd = float(past.std())
    out = {"day": float(day), "sigmas": float(day / sd) if sd else None}
    for n in (5, 20):
        out[f"d{n}"] = float(s.iloc[-1] - s.iloc[-1 - n]) if level else float(s.iloc[-1] / s.iloc[-1 - n] - 1)
    out["last"] = float(s.iloc[-1])
    return out


def _pct(x: float | None) -> str | None:
    return None if x is None else f"{x * 100:+.1f}%"


def move_item(key: str, name: str, meaning: str, m: dict, level: bool = False) -> dict:
    f = (lambda x: f"{x:+.2f}") if level else _pct
    item = {"key": key, "名称": name, "含义": meaning, "当日": f(m["day"]), "5 日": f(m["d5"]), "20 日": f(m["d20"])}
    if level:
        item["最新"] = f"{m['last']:.2f}"
    if m.get("sigmas") is not None:
        item["日常波动倍数"] = f"{m['sigmas']:+.1f}"
        item["超出日常波动"] = abs(round(m["sigmas"], 1)) >= UNUSUAL          # as printed: "-2.0" is beyond the ordinary
    return item


def contributions(rets: pd.Series, caps: pd.Series, index_ret: float | None) -> dict | None:
    """Each stock's weight × return in percentage points of the index; the top ones each way and the largest ten's share."""
    df = pd.DataFrame({"r": rets, "cap": caps}).dropna()
    df = df[df["cap"] > 0]
    if len(df) < 100:
        return None
    df["w"] = df["cap"] / df["cap"].sum()
    df["c"] = df["w"] * df["r"]
    total = float(df["c"].sum())
    top10 = df.nlargest(10, "w")
    share = lambda x: x / total if abs(total) > 1e-4 else None  # noqa: E731
    rows = lambda d: [{"代码": t, "权重": f"{r.w * 100:.1f}%", "当日": _pct(r.r), "贡献": f"{r.c * 100:+.2f} 个百分点",  # noqa: E731
                       **({"占指数涨跌": f"{share(r.c) * 100:.0f}%"} if share(r.c) is not None and share(r.c) > 0 else {})}
                      for t, r in d.iterrows()]
    out = {"key": "contrib", "说明": "按估算市值权重（Yahoo 市值，否则稀释股数 × 收盘价），个股对标普 500 当日涨跌的贡献；是估算",
           "估算合计": f"{total * 100:+.2f} 个百分点", "标普 500 实际": _pct(index_ret),
           "最大 10 家合计贡献": f"{top10['c'].sum() * 100:+.2f} 个百分点",
           "拉动最多": rows(df.nlargest(CONTRIB_SHOWN, "c")), "拖累最多": rows(df.nsmallest(CONTRIB_SHOWN, "c"))}
    if share(float(top10["c"].sum())) is not None:
        out["最大 10 家占指数涨跌"] = f"{share(float(top10['c'].sum())) * 100:.0f}%"
    out["_strong"] = [t for t, r in df.iterrows() if share(r.c) is not None and share(r.c) >= STRONG_SHARE]
    out["_top10_share"] = share(float(top10["c"].sum()))
    return out


async def caps(asof: date, closes: pd.Series) -> pd.Series:
    """S&P 500 members' market value: Yahoo's (ownership snapshot within a week), else latest diluted shares × close."""
    from scout import valuation

    members = (await valuation.members()).get("sp500") or []
    snaps = {r["ticker"]: (r["payload"] or {}).get("marketCap") for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, payload FROM scout.research_snapshots WHERE kind = 'ownership' AND ticker = ANY(:t) "
        "AND asof BETWEEN :a AND :d ORDER BY ticker, asof DESC", {"t": members, "a": asof - timedelta(days=7), "d": asof})}
    shares = {r["ticker"]: r["value"] for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, value FROM scout.financials WHERE ticker = ANY(:t) AND metric = 'shares_diluted' "
        "AND filed <= :d ORDER BY ticker, period_end DESC, filed DESC", {"t": members, "d": asof})}
    out = {}
    for t in members:
        if snaps.get(t):
            out[t] = float(snaps[t])
        elif shares.get(t) and pd.notna(closes.get(t)):
            out[t] = float(shares[t]) * float(closes[t])
    return pd.Series(out, dtype=float)


async def build_pack(p, asof: date, sector_rows: list[dict], g: dict | None, mfilter: dict | None) -> dict:
    from scout import market_filter, prediction, sector_news, sector_trend
    from scout.data.yahoo import YahooProvider

    close = p["close"]
    moves, unusual = [], []
    for t, name, meaning in ETFS:
        if t in close.columns and (m := sigma_move(close[t], asof)):
            moves.append(move_item(f"move.{t}", f"{name}（{t}）", meaning, m))
    try:
        live = await YahooProvider().daily_bars([s for s, _, _ in LIVE], asof - timedelta(days=150), asof)
    except Exception as exc:
        logger.warning("yahoo live series failed: %s", exc)
        live = {}
    for sym, name, meaning in LIVE:
        bars = live.get(sym)
        if bars is not None and len(bars) and bars.index[-1] == asof:
            level = sym == "^TNX"
            if m := sigma_move(bars["close"], asof, level=level):
                moves.append(move_item(f"move.{sym.strip('^')}", name, meaning, m, level=level))
    from scout import global_markets as gm

    abroad = await gm.closes(asof - timedelta(days=150))
    for sym, name, region, meaning in gm.ALL:
        c = abroad.get(sym)
        if c is not None and len(c) and c.index[-1] == asof and (m := sigma_move(c, asof)):
            moves.append(move_item(f"global.{sym.strip('^').replace('=', '').replace('.', '')}", f"{name}（{region}）", meaning, m))
    for a, b, name, meaning in (("RSP", "SPY", "等权相对市值加权", "正 = 普通股票比大公司强，上涨面广；负 = 少数大公司主导"),
                                ("IWM", "SPY", "小盘相对大盘", "正 = 小盘更强，通常风险偏好高或利率预期下行"),
                                ("QQQ", "RSP", "科技成长相对普通股票", "正 = 科技成长更强")):
        if a in close.columns and b in close.columns:
            rel = (close[a] / close[b]).dropna()
            if m := sigma_move(rel, asof):
                moves.append(move_item(f"style.{a}_{b}", name, meaning, m))
    unusual = [x["key"] for x in moves if x.get("超出日常波动")]

    rets = p["ret_1d"].loc[asof].reindex(p.stocks)
    liquid = p["liquid"].loc[asof].reindex(p.stocks).fillna(False).astype(bool)
    upto = close[p.stocks].loc[:asof]
    ma50 = upto.iloc[-50:].mean()
    breadth = {"key": "breadth", "上涨股票比例": f"{(rets[liquid] > 0).mean() * 100:.0f}%",
               "站上 50 日线比例": f"{(upto.iloc[-1][liquid] > ma50[liquid]).mean() * 100:.0f}%",
               "含义": "流动性足够的股票里，今天上涨的比例和站上 50 日均线的比例"}

    spy_ret = float(p["ret_1d"].at[asof, "SPY"]) if pd.notna(p["ret_1d"].at[asof, "SPY"]) else None
    contrib = None
    try:
        contrib = contributions(rets, await caps(asof, close.loc[asof]), spy_ret)
    except Exception as exc:
        logger.warning("index contributions failed: %s", exc)

    secs = sorted((r for r in sector_rows if r.get("kind") == "sector" and r.get("ret_1d") is not None), key=lambda r: -r["ret_1d"])
    sec_item = lambda r: {"ETF": r["key"], "名称": r["name"], "当日": _pct(r["ret_1d"]), "20 日相对 SPY": _pct(r.get("rel_20"))}  # noqa: E731
    changes = [sector_trend.change_text(r["name"], ch) for r in sector_rows for ch in r.get("trend_changes") or []]
    sectors = {"key": "sectors", "领先": [sec_item(r) for r in secs[:3]], "落后": [sec_item(r) for r in secs[-3:][::-1]],
               "趋势确认变化": changes[:6]}

    pre = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'premarket' AND asof = :d ORDER BY id DESC LIMIT 1", {"d": asof})
    econ = [e for e in (((pre or {}).get("payload") or {}).get("pack") or {}).get("economic_today", []) if e.get("实际")]
    macro_data = [{"key": f"macro.{i + 1}", "事件": e["事件"], "实际": e["实际"], "预期": e.get("预期"), "对比预期": e.get("对比预期"),
                   "前值": e.get("前值")} for i, e in enumerate(econ)]
    surprises = [x["key"] for x in macro_data if x.get("对比预期") in ("高于预期", "低于预期")]

    odds = []
    try:
        def moved(e):
            return max((abs((o.get("prob") or 0) - o["before"]) for o in e["outcomes"] if o.get("before") is not None), default=0)
        board = [e for e in await prediction.board(topic="macro", week=False) if RATES.search(e["title"] + " " + (e.get("title_en") or ""))
                 or moved(e) >= ODDS_MOVE]
        for i, e in enumerate(board[:4]):
            odds.append({"key": f"odds.{i + 1}", "市场": e["title"], "来源": e["source"],
                         "各结果": [{"结果": o["label"], "现在": _pct(o["prob"])[1:] if o.get("prob") is not None else None,
                                   "约一天前": _pct(o["before"])[1:] if o.get("before") is not None else None} for o in e["outcomes"]],
                         "说明": "预测市场价格，大致看作市场认为的概率；不是事实"})
    except Exception as exc:
        logger.warning("prediction odds failed: %s", exc)

    earn = []
    if contrib is not None:
        big = {x["代码"] for x in contrib["拉动最多"] + contrib["拖累最多"]}
        rows = await db.fetch_all("SELECT DISTINCT ticker FROM scout.market_events WHERE event_type = 'earnings' AND event_date BETWEEN :a AND :d",
                                  {"a": asof - timedelta(days=1), "d": asof})
        earn = [{"key": f"earn.{r['ticker']}", "代码": r["ticker"], "当日": _pct(float(rets.get(r["ticker"])))} for r in rows
                if r["ticker"] in big and pd.notna(rets.get(r["ticker"]))]

    news = [{"key": f"news.{i + 1}", "标题": n["headline"], "来源": n.get("source"), "时间": n.get("published")}
            for i, n in enumerate((await sector_news.macro_headlines(6)) + (await sector_news.market_wraps(limit=4))) if n.get("headline")]
    md = (g or {}).get("market_day") or {}
    causes = [{"key": f"cause.{i + 1}", **c} for i, c in enumerate(md.get("cause") or [])]

    from scout import levels, portfolio
    from scout.telegram import next_step

    holdings = []
    for t in (g or {}).get("held_rows") or []:
        f = (g.get("feats") or {}).get(t["ticker"], {})
        step = next_step(t, portfolio.metrics(t))
        holdings.append({"key": f"hold.{t['ticker']}", "代码": t["ticker"], "核心仓": levels.is_core(t["ticker"]),
                         "当日": _pct(f.get("ret_1d")), "β": None if f.get("beta") is None else f"{f['beta']:.2f}",
                         "板块": t.get("sector"), "计划": step or "照计划持有"})

    from scout import sentiment

    try:
        mood = await sentiment.readings(asof)
    except Exception as exc:
        logger.warning("sentiment readings failed: %s", exc)
        mood = []

    prev = await db.fetch_one("SELECT asof, payload FROM scout.briefs WHERE kind = 'market_read' AND asof < :d ORDER BY asof DESC, id DESC LIMIT 1",
                              {"d": asof})
    yesterday = None
    if prev and (r := (prev["payload"] or {}).get("read")):
        yesterday = {"日期": str(prev["asof"]), "标题": r["headline"], "推动因素": [d["title"] for d in r["drivers"]]}

    return {"data_date": asof.isoformat(), "moves": moves, "breadth": breadth, "contributions": contrib and
            {k: v for k, v in contrib.items() if not k.startswith("_")}, "sectors": sectors, "macro_data": macro_data, "odds": odds,
            "earnings": earn, "news": news, "causes": causes, "holdings": holdings, "sentiment": mood,
            "filter": {"key": "filter", "大盘过滤": market_filter.text(mfilter)} if mfilter else None, "yesterday": yesterday,
            "_strong": sorted(set(unusual) | set(surprises) | ({"contrib"} if contrib and contrib["_strong"] else set())
                              | ({"contrib"} if contrib and (contrib["_top10_share"] or 0) >= 0.5 else set()))}


def keys(pack: dict) -> set[str]:
    out: set[str] = set()

    def walk(x):
        if isinstance(x, dict):
            if isinstance(x.get("key"), str):
                out.add(x["key"])
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
    walk({k: v for k, v in pack.items() if not k.startswith("_")})
    return out


# ── validation ──

def validate(r: Read, pack: dict) -> list[str]:
    issues = []
    texts = [r.headline, r.what_happened, r.media_vs_data, r.story, r.for_you, r.would_change, r.mood] + \
            [d.title + "。" + d.explanation for d in r.drivers]
    body = "\n".join(texts)
    allowed = allowed_numbers({k: v for k, v in pack.items() if not k.startswith("_")})
    bad = unsupported_numbers(body, allowed)
    if bad:
        issues.append(f"这些数字不在证据包里：{'、'.join(bad[:8])}")
    known = keys(pack)
    for d in r.drivers:
        unknown = [k for k in d.evidence if k not in known]
        if unknown:
            issues.append(f"「{d.title}」引用了不存在的 key：{'、'.join(unknown)}")
        if d.strength == "强" and not any(k in pack["_strong"] or k.split(".")[0] == "contrib" and "contrib" in pack["_strong"]
                                          for k in d.evidence):
            issues.append(f"「{d.title}」标为「强」，但引用的证据里没有超出日常波动的读数、集中的贡献或偏离预期的数据")
        if not d.evidence:
            issues.append(f"「{d.title}」没有引用证据")
    if not 1 <= len(r.drivers) <= 4:
        issues.append("推动因素要 1–4 个")
    for w in FORECAST + ACTIONS:
        if w in body:
            issues.append(f"出现了预测或操作用语「{w}」：这是事后解释")
    if pack.get("yesterday") is None and r.story_status != "无前一天":
        issues.append("没有昨天的解读，story_status 应为「无前一天」")
    if pack.get("yesterday") is not None and r.story_status == "无前一天":
        issues.append("有昨天的解读，story_status 不能是「无前一天」")
    if re.search(r"(?:跌幅|下跌|回落)(?:为|了|约)?\s*-\d", body):
        issues.append("双重否定：涨跌数字自带正负号")
    return issues


def fallback(pack: dict) -> Read:
    """Rule-based read: the unusual moves and the concentration of the index's day, no story."""
    spy = next((m for m in pack["moves"] if m["key"] == "move.SPY"), {})
    odd = [m for m in pack["moves"] if m.get("超出日常波动")]
    drivers = [Driver(title=m["名称"][:16], explanation=f"{m['名称']}当日 {m['当日']}，约 {m['日常波动倍数']} 倍日常波动",
                      evidence=[m["key"]], strength="强") for m in odd[:3]]
    c = pack.get("contributions")
    if c and c.get("最大 10 家占指数涨跌"):
        drivers.append(Driver(title="大公司贡献", explanation=f"最大 10 家合计贡献 {c['最大 10 家合计贡献']}", evidence=["contrib"], strength="中"))
    if not drivers:
        drivers = [Driver(title="普通波动", explanation="没有读数超出日常波动", evidence=["move.SPY"], strength="弱")]
    return Read(headline=f"标普 500 当日 {spy.get('当日', '—')}" + ("，有读数超出日常波动" if odd else "，在日常波动之内"),
                what_happened="、".join(f"{m['名称']} {m['当日']}" for m in pack["moves"][:4]),
                drivers=drivers[:4], media_vs_data="（AI 解读未通过校验，没有对照新闻）",
                story_status="无前一天" if pack.get("yesterday") is None else "新出现", story="（按规则整理，没有对照昨天）",
                for_you="持仓照计划管理", would_change="—",
                mood="；".join(f"{x['名称']} {x['数值']}，{x['读数']}" for x in pack.get("sentiment") or []) or "没有情绪读数")


async def write(pack: dict) -> tuple[Read, str, list]:
    settings = get_scout_settings()
    if not settings.openai_api_key:
        return fallback(pack), "rules", []
    from scout.viewdesk import strict_schema

    messages = [{"role": "user", "content": json.dumps({k: v for k, v in pack.items() if not k.startswith("_")},
                                                       ensure_ascii=False, default=str)}]
    tries = []
    for _ in (1, 2):
        try:
            data = await responses.create({
                "model": settings.scout_agent_model, "reasoning": {"effort": "low"}, "instructions": PROMPT, "input": messages,
                "text": {"format": {"type": "json_schema", "name": "market_read", "schema": strict_schema(Read), "strict": True}}},
                timeout=240, purpose="market_read")
            parsed = Read.model_validate_json(responses.output_text(data))
        except Exception as exc:
            logger.warning("market read LLM call failed: %s", exc)
            tries.append([f"LLM 调用失败：{type(exc).__name__}"])
            break
        issues = validate(parsed, pack)
        tries.append(issues)
        if not issues:
            return parsed, "llm", tries
        messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                     {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    return fallback(pack), "rules", tries


async def generate(p, asof: date, sector_rows: list[dict], g: dict | None, mfilter: dict | None) -> dict:
    pack = await build_pack(p, asof, sector_rows, g, mfilter)
    read, source, tries = await write(pack)
    rec = {"source": source, "read": read.model_dump(), "pack": pack, "validation": tries}
    content = render_text(read, asof, source)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('market_read', :d, :c, CAST(:p AS JSONB))",
                     {"d": asof, "c": content, "p": db.jsonb(rec)})
    return {**rec, "content": content}


async def latest() -> dict | None:
    row = await db.fetch_one("SELECT asof, content, payload FROM scout.briefs WHERE kind = 'market_read' ORDER BY id DESC LIMIT 1")
    return {**row["payload"], "asof": row["asof"], "content": row["content"]} if row else None


# ── text ──

NOTE = "事后解释，不是预测；数字由 Python 计算，新闻标题未经核实"


def render_text(r: Read, asof: date, source: str) -> str:
    lines = [f"【市场解读 · {asof}】{r.headline}", r.what_happened, "主要推动："]
    lines += [f"· {d.title}（证据{d.strength}）：{d.explanation}" for d in r.drivers]
    lines += [f"新闻与数据：{r.media_vs_data}", f"故事线（{r.story_status}）：{r.story}", f"对你：{r.for_you}",
              f"情绪：{r.mood}", f"什么会说明这个解释不对：{r.would_change}", f"（{NOTE}" + ("；AI 解读未通过校验，按规则整理" if source != "llm" else "") + "）"]
    return "\n".join(lines)


def short_lines(rec: dict | None) -> list[str]:
    """For the Telegram digest: headline, the drivers in one line each, the story line, what it means for the user."""
    from scout.telegram import esc

    if not rec:
        return []
    r = rec["read"]
    lines = ["<b>【市场解读】</b>" + esc(r["headline"])]
    lines += [f"• {esc(d['title'])}（证据{d['strength']}）：{esc(d['explanation'])}" for d in r["drivers"][:3]]
    lines.append(f"故事线（{r['story_status']}）：{esc(r['story'])}")
    if r.get("mood"):
        lines.append(f"情绪：{esc(r['mood'])}")
    lines.append(f"对你：{esc(r['for_you'])}")
    return lines
