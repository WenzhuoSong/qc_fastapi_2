"""Industry core metrics (user's request 2026-11-04, after PBF fell with the whole refining group): the numbers an
industry analyst looks at first, from free data only — FRED / EIA series (stored in `scout.macro_series` by the
research job) and, during the session, futures quotes from Yahoo. An industry without free data says so instead of
pretending (`missing`).

Each metric: latest value and date, the change over about 20 observations, where it stands in its last year
(percentile), and for those with futures a live change today. Descriptive only: a metric is never a signal.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta

from scout import db, universe

GALLON_PER_BARREL = 42


@dataclass(frozen=True)
class Metric:
    key: str
    name: str
    unit: str
    meaning: str
    series: tuple[str, ...]                   # FRED ids; one series, or the inputs of `crack`
    live: tuple[str, ...] = ()                # Yahoo futures for the session value, same order as `series`
    kind: str = "level"                       # level | crack


@dataclass(frozen=True)
class Industry:
    key: str
    name: str
    match: tuple[str, ...]                    # GICS sub-industries (instruments.industry) or sector ETFs
    metrics: tuple[str, ...]
    missing: str = ""                         # what an analyst would look at that no free source has
    proxies: tuple[str, ...] = field(default=())   # ETFs whose move stands in for the industry


METRICS = {m.key: m for m in (
    Metric("crack_321", "3-2-1 炼油差价", "美元/桶",
           "3 桶原油炼成 2 桶汽油和 1 桶柴油的毛利（纽约港汽油、超低硫柴油对 WTI 原油）；炼油公司利润的核心，差价收窄利润就下降",
           ("DGASNYH", "DDFUELNYH", "DCOILWTICO"), ("RB=F", "HO=F", "CL=F"), "crack"),
    Metric("wti", "WTI 原油", "美元/桶", "美国原油基准价；油气开采公司收入直接跟它走，炼油公司的成本", ("DCOILWTICO",), ("CL=F",)),
    Metric("brent", "布伦特原油", "美元/桶", "国际原油基准价", ("DCOILBRENTEU",), ("BZ=F",)),
    Metric("henry_hub", "亨利港天然气", "美元/百万英热", "美国天然气基准价；天然气开采公司的收入，燃气发电厂的燃料成本",
           ("DHHNGSP",), ("NG=F",)),
    Metric("gasoline", "纽约港汽油", "美元/加仑", "成品油批发价，炼油公司的主要产品", ("DGASNYH",), ("RB=F",)),
    Metric("ust10", "10 年期美债收益率", "%", "长期利率；公用事业、房地产这类高分红、借钱多的行业对它最敏感", ("DGS10",)),
    Metric("curve", "10 年 − 2 年期利差", "百分点", "长短期利率之差；越大银行借短贷长的利差越厚，倒挂（为负）时银行息差承压", ("T10Y2Y",)),
    Metric("hy_spread", "高收益债利差", "百分点", "垃圾债相对国债的额外利率；扩大说明市场担心信用风险，银行坏账预期上升", ("BAMLH0A0HYM2",)),
    Metric("mortgage30", "30 年期房贷利率", "%", "购房成本；利率越高买房需求越弱", ("MORTGAGE30US",)),
    Metric("housing_starts", "新屋开工", "千套/年（季调年率）", "新建住宅开工数量，房屋建筑商需求的直接指标", ("HOUST",)),
    Metric("retail_sales", "零售销售（不含餐饮）", "百万美元", "美国消费者在零售上的月度支出，电商和零售公司的大环境", ("RSXFS",)),
    Metric("ecommerce_share", "电商占零售比例", "%", "网上零售占全部零售的比例（季度），电商平台的长期空间", ("ECOMPCTSA",)),
    Metric("ip_semis", "半导体和电子元件工业产值指数", "指数", "美国半导体与电子元件的月度产值，只能粗略反映行业景气", ("IPG3344S",)),
)}

INDUSTRIES = (
    Industry("refining", "炼油", ("Oil & Gas Refining & Marketing",), ("crack_321", "wti", "gasoline"), proxies=("XLE",)),
    Industry("upstream", "油气开采与油服", ("Oil & Gas Exploration & Production", "Integrated Oil & Gas", "Oil & Gas Equipment & Services",
                                       "Oil & Gas Drilling"), ("wti", "brent", "henry_hub"), proxies=("XLE", "XOP")),
    Industry("midstream", "油气储运", ("Oil & Gas Storage & Transportation",), ("henry_hub", "wti"), proxies=("XLE",)),
    Industry("utilities", "公用事业", ("Electric Utilities", "Multi-Utilities", "Gas Utilities", "Water Utilities",
                                   "Independent Power Producers & Energy Traders", "Renewable Electricity", "XLU"),
             ("ust10", "henry_hub"), "电价、用电量（数据中心用电）没有免费的日度数据", ("XLU",)),
    Industry("banks", "银行", ("Regional Banks", "Diversified Banks", "XLF"), ("curve", "ust10", "hy_spread"),
             "存贷款增速、存款成本没有免费的日度数据（美联储 H.8 为周度）", ("XLF", "KRE")),
    Industry("homebuilding", "住宅建筑", ("Homebuilding",), ("mortgage30", "housing_starts"), proxies=("XHB",)),
    Industry("semis", "半导体", ("Semiconductors", "Semiconductor Materials & Equipment"), ("ip_semis",),
             "存储芯片（DRAM / NAND）合约价、晶圆厂资本开支、AI 服务器出货没有免费数据；看同行和半导体 ETF 的相对强弱", ("SMH",)),
    Industry("ecommerce", "电商与互联网服务", ("Internet Services & Infrastructure", "Broadline Retail"), ("retail_sales", "ecommerce_share"),
             "平台成交额（GMV）、商家数这类公司指标只在财报里", ("XLY",)),
    Industry("managed_care", "医疗保险", ("Managed Health Care",), (),
             "医保（Medicare Advantage）支付率、医疗成本趋势、会员数没有免费的日度数据；看同行（UNH、ELV、CVS、CI）的走势", ("XLV",)),
    Industry("gold", "黄金", ("Gold",), (), "", ("GLD", "GDX")),
)

ETF_INDUSTRY = {"XLU": "utilities", "XLF": "banks", "XLE": "upstream", "SMH": "semis"}


def industry_of(ticker: str, gics: str | None) -> Industry | None:
    key = ETF_INDUSTRY.get(ticker)
    for ind in INDUSTRIES:
        if ind.key == key or (gics and gics in ind.match) or ticker in ind.match:
            return ind
    return None


def all_series() -> list[str]:
    """Every FRED series a metric needs (the research job stores them)."""
    return sorted({s for m in METRICS.values() for s in m.series})


def crack(gasoline: float, diesel: float, crude: float) -> float:
    """3-2-1 crack spread in $/bbl: products quoted per gallon, crude per barrel."""
    return (2 * gasoline * GALLON_PER_BARREL + diesel * GALLON_PER_BARREL - 3 * crude) / 3


def history(metric: Metric, series: dict[str, list[tuple[date, float]]]) -> list[tuple[date, float]]:
    """The metric's own dated values (a crack only on days all three inputs exist)."""
    if metric.kind == "crack":
        a, b, c = (dict(series.get(s, [])) for s in metric.series)
        return [(d, crack(a[d], b[d], c[d])) for d in sorted(set(a) & set(b) & set(c))]
    return sorted(series.get(metric.series[0], []))


def reading(values: list[tuple[date, float]], lookback: int = 20, year: int = 252) -> dict | None:
    """Latest, change over `lookback` observations, percentile in the last `year` observations."""
    if not values:
        return None
    d, v = values[-1]
    prev = values[-1 - lookback][1] if len(values) > lookback else None
    last_year = [x for _, x in values[-year:]]
    pct = sum(x <= v for x in last_year) / len(last_year) if len(last_year) >= 20 else None
    return {"date": d, "value": v, "change": None if prev is None else v - prev, "pct_change": None if not prev else v / prev - 1,
            "percentile": pct, "n": len(values)}


def live_value(metric: Metric, quotes: dict[str, dict]) -> dict | None:
    """The session value from futures and its change against their previous close."""
    if not metric.live or any(not quotes.get(s) or quotes[s].get("last") is None for s in metric.live):
        return None
    last = [quotes[s]["last"] for s in metric.live]
    prev = [quotes[s]["last"] / (1 + quotes[s]["change"]) if quotes[s].get("change") is not None else None for s in metric.live]
    now = crack(*last) if metric.kind == "crack" else last[0]
    before = (crack(*prev) if metric.kind == "crack" else prev[0]) if all(p is not None for p in prev) else None
    return {"value": now, "change": None if before is None else now - before,
            "pct_change": None if not before else now / before - 1, "time_et": quotes[metric.live[0]].get("time_et")}


async def load_series(ids: list[str], since: date) -> dict[str, list[tuple[date, float]]]:
    rows = await db.fetch_all("SELECT series, date, value FROM scout.macro_series WHERE series = ANY(:s) AND date >= :d ORDER BY date",
                              {"s": ids, "d": since})
    out: dict[str, list[tuple[date, float]]] = {}
    for r in rows:
        out.setdefault(r["series"], []).append((r["date"], r["value"]))
    return out


def fmt(metric: Metric, r: dict | None, live: dict | None = None) -> str:
    """One line: value (date), 20-observation change, year percentile, live change."""
    if not r and not live:
        return f"{metric.name}：暂无数据"
    parts = []
    if r:
        v = f"{r['value']:,.2f}"
        ch = (f"，较 20 期前 {r['change']:+,.2f}" if metric.unit in ("%", "百分点") else f"，较 20 期前 {r['pct_change'] * 100:+.1f}%") \
            if r.get("change") is not None and r.get("pct_change") is not None else ""
        pc = f"，处于近一年 {r['percentile'] * 100:.0f}% 分位" if r.get("percentile") is not None else ""
        parts.append(f"{v} {metric.unit}（{r['date']:%m-%d}{ch}{pc}）")
    if live and live.get("pct_change") is not None:
        parts.append(f"期货实时 {live['value']:,.2f}，今日 {live['pct_change'] * 100:+.1f}%")
    return f"{metric.name}：" + "；".join(parts)


async def for_ticker(ticker: str, quotes: dict[str, dict] | None = None, today: date | None = None) -> dict | None:
    """The industry and its metrics for one ticker; None when no industry is registered."""
    t = universe.yahoo_symbol(ticker)
    inst = await db.fetch_one("SELECT industry FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    ind = industry_of(t, inst.get("industry"))
    if ind is None:
        return None
    today = today or date.today()
    metrics = [METRICS[k] for k in ind.metrics]
    series = await load_series(sorted({s for m in metrics for s in m.series}), today - timedelta(days=500))
    rows = []
    for m in metrics:
        r = reading(history(m, series))
        live = live_value(m, quotes or {})
        rows.append({"key": m.key, "name": m.name, "unit": m.unit, "meaning": m.meaning, "reading": r, "live": live, "text": fmt(m, r, live)})
    return {"ticker": t, "industry": ind.name, "gics": inst.get("industry"), "metrics": rows, "missing": ind.missing,
            "proxies": list(ind.proxies)}


def live_symbols(gics_by_ticker: dict[str, str | None]) -> list[str]:
    """Futures needed for the live values of these tickers' industries."""
    out = set()
    for t, g in gics_by_ticker.items():
        ind = industry_of(t, g)
        if ind:
            out |= {s for k in ind.metrics for s in METRICS[k].live}
    return sorted(out)
