"""Trading costs per stock and month from daily bars (`scout/knowledge/external_specs/small_cap_costs_v1.yaml`).

One-way cost of a Q-dollar order = half the effective spread (Abdi & Ranaldo 2017 close-high-low
estimator, monthly, floor of one cent) + square-root impact Y·σ·√(Q/ADV). A month's estimate applies
to trades in the next month. Local only: built from the git-ignored Sharadar files; the table and
its summary stay in git-ignored `exports/costs/`.
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

KEY = "small_cap_costs_v1"
OUT = Path("exports/costs")
ORDER_USD = 25_000.0
IMPACT_Y = 0.5
MIN_DAYS = 15
MIN_ADV = 1_000_000.0
MAX_PARTICIPATION = 0.05
TICK = 0.01


def abdi_ranaldo(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> float | None:
    """Effective spread (fraction) from one month of one stock's daily bars, in date order."""
    ok = np.isfinite(high) & np.isfinite(low) & np.isfinite(close) & (low > 0) & (high >= low)
    h, lo, c = np.log(high[ok]), np.log(low[ok]), np.log(close[ok])
    if len(c) < MIN_DAYS:
        return None
    eta = (h + lo) / 2
    s2 = 4 * np.mean((c[:-1] - eta[:-1]) * (c[:-1] - eta[1:]))
    return math.sqrt(max(s2, 0.0))


def corwin_schultz(high: np.ndarray, low: np.ndarray) -> float | None:
    """Corwin & Schultz (2012) high-low spread (fraction), monthly mean of two-day estimates, negatives → 0."""
    ok = np.isfinite(high) & np.isfinite(low) & (low > 0)
    h, lo = high[ok], low[ok]
    if len(h) < MIN_DAYS:
        return None
    k = 3 - 2 * math.sqrt(2)
    beta = np.log(h[:-1] / lo[:-1]) ** 2 + np.log(h[1:] / lo[1:]) ** 2
    gamma = np.log(np.maximum(h[:-1], h[1:]) / np.minimum(lo[:-1], lo[1:])) ** 2
    alpha = (np.sqrt(2 * beta) - np.sqrt(beta)) / k - np.sqrt(gamma / k)
    s = 2 * (np.exp(alpha) - 1) / (1 + np.exp(alpha))
    return float(np.mean(np.maximum(s, 0.0)))


def one_way(spread: float, sigma: float, adv: float, q: float = ORDER_USD, y: float = IMPACT_Y) -> float:
    return spread / 2 + y * sigma * math.sqrt(q / adv)


def tradeable(adv: float | None, spread: float | None, q: float = ORDER_USD) -> bool:
    return bool(spread is not None and adv and adv >= MIN_ADV and q / adv <= MAX_PARTICIPATION)


def month_table(s: pd.DataFrame) -> pd.DataFrame:
    """Per (ticker, month) from daily rows (ticker, date, high, low, close, closeunadj, volume), vectorised;
    the same estimators as `abdi_ranaldo` / `corwin_schultz`, pairs of consecutive sessions within the month."""
    s = s.sort_values(["ticker", "date"]).copy()
    s["month"] = s["date"].dt.to_period("M")
    ok = (s["low"] > 0) & (s["high"] >= s["low"]) & (s["close"] > 0)
    s.loc[~ok, ["high", "low", "close"]] = np.nan
    lh, ll, lc = np.log(s["high"]), np.log(s["low"]), np.log(s["close"])
    g = s.groupby(["ticker", "month"], sort=False)
    eta = (lh + ll) / 2
    nxt = lambda x: x.groupby([s["ticker"], s["month"]], sort=False).shift(-1)  # noqa: E731
    s["ar"] = (lc - eta) * (lc - nxt(eta))
    h1, l1 = nxt(s["high"]), nxt(s["low"])
    k = 3 - 2 * math.sqrt(2)
    beta = (lh - ll) ** 2 + (np.log(h1) - np.log(l1)) ** 2
    gamma = (np.log(np.fmax(s["high"], h1)) - np.log(np.fmin(s["low"], l1))) ** 2
    alpha = (np.sqrt(2 * beta) - np.sqrt(beta)) / k - np.sqrt(gamma / k)
    s["cs"] = np.maximum(2 * (np.exp(alpha) - 1) / (1 + np.exp(alpha)), 0.0)
    s["r"] = lc - lc.groupby([s["ticker"], s["month"]], sort=False).shift(1)
    s["dv"] = s["closeunadj"] * s["volume"]
    g = s.groupby(["ticker", "month"], sort=False)
    out = pd.DataFrame({"days": g["close"].count(), "ar": g["ar"].mean(), "spread_cs": g["cs"].mean(),
                        "sigma": g["r"].std(ddof=1), "n_r": g["r"].count(), "adv": g["dv"].median(),
                        "price": g["closeunadj"].mean()}).reset_index()
    enough = out["days"] >= MIN_DAYS
    out["spread"] = np.where(enough, np.sqrt(np.maximum(4 * out["ar"], 0.0)), np.nan)
    out["spread"] = np.where(enough & (out["price"] > 0), np.maximum(out["spread"], TICK / out["price"]), np.nan)
    out["spread_cs"] = out["spread_cs"].where(enough)
    out["sigma"] = out["sigma"].where(out["n_r"] >= MIN_DAYS - 1)
    return out.drop(columns=["ar", "n_r"])


def build(tickers: list[str]) -> pd.DataFrame:
    """Per (ticker, month): the inputs and the one-way cost of the default order, for use the next month."""
    from scout import sharadar_research as sr

    s = sr._read("stocks", ["ticker", "date", "high", "low", "close", "closeunadj", "volume"],
                 filters=[("ticker", "in", tickers), ("date", ">=", pd.Timestamp(sr.START).date())])
    s["date"] = pd.to_datetime(s["date"])
    df = month_table(s)
    ok = df["spread"].notna() & df["sigma"].notna() & (df["adv"] > 0)
    df["one_way"] = np.where(ok, df["spread"] / 2 + IMPACT_Y * df["sigma"] * np.sqrt(ORDER_USD / df["adv"].where(ok, 1.0)), np.nan)
    df["tradeable"] = [tradeable(a, sp) for a, sp in zip(df["adv"], df["spread"])]
    df["applies_to"] = df["month"] + 1
    return df


def lookup(costs: pd.DataFrame) -> dict[tuple[str, pd.Period], tuple[float, bool]]:
    """(ticker, trade month) → (one-way cost, tradeable), from the previous month's estimate."""
    return {(t, m): (c, ok) for t, m, c, ok in zip(costs["ticker"], costs["applies_to"], costs["one_way"], costs["tradeable"])}


def summarize(costs: pd.DataFrame, buckets: dict[pd.Period, dict[str, str]]) -> dict:
    """Round-trip costs by size bucket (by month-end market cap rank), overall and by year, with the
    sensitivity variants; `buckets`: month → {ticker: bucket}."""
    c = costs.copy()
    c["bucket"] = [buckets.get(m, {}).get(t) for t, m in zip(c["ticker"], c["month"])]
    c = c[c["bucket"].notna() & c["spread"].notna() & c["sigma"].notna() & (c["adv"] > 0)]
    imp = c["sigma"] * np.sqrt(ORDER_USD / c["adv"])
    variants = {"main": c["spread"] / 2 + IMPACT_Y * imp, "impact_y1": c["spread"] / 2 + imp,
                "spread_x2": c["spread"] + IMPACT_Y * imp,
                "corwin_schultz": c["spread_cs"] / 2 + IMPACT_Y * imp}
    out: dict = {"buckets": {}, "years": {}}
    order = sorted(c["bucket"].unique())
    for b in order:
        g = c["bucket"] == b
        t = g & c["tradeable"]                     # untradeable stocks (ADV near 0) would swamp means
        out["buckets"][b] = {"n": int(g.sum()), "tradeable": float(c.loc[g, "tradeable"].mean()),
                             "spread_median": float(c.loc[t, "spread"].median()),
                             "spread_floor_share": float((c.loc[t, "spread"] <= 2 * TICK / c.loc[t, "price"]).mean()),
                             "adv_median": float(c.loc[t, "adv"].median()),
                             **{f"rt_{k}": _q(2 * v[t]) for k, v in variants.items()}}
    for y in sorted(c["month"].dt.year.unique()):
        g = c["month"].dt.year == y
        out["years"][int(y)] = {b: {"rt_median": float(2 * variants["main"][g & (c["bucket"] == b) & c["tradeable"]].median()),
                                    "tradeable": float(c.loc[g & (c["bucket"] == b), "tradeable"].mean())}
                                for b in order if (g & (c["bucket"] == b)).any()}
    return out


def _q(x: pd.Series) -> dict:
    x = x.dropna()
    return {"median": float(x.median()), "mean": float(x.mean()), "p90": float(x.quantile(0.9))} if len(x) else {}


def size_buckets() -> tuple[dict[pd.Period, dict[str, str]], list[str]]:
    """Month → {ticker: bucket} from the last session's market-cap ranks among eligible stocks
    (large = 1–1000, small_1..small_5 = quintiles of ranks 1001–3000); and every ticker ever in them."""
    from scout import sharadar_research as sr

    days = sr.trading_days()
    ends = days[[i for i in range(len(days) - 1) if days[i].month != days[i + 1].month]]
    uni = sr.universe(ends, sr.eligible())
    out, names = {}, set()
    for d, u in uni.items():
        m = {t: "large" for t in u["large"]}
        for k, t in enumerate(u["small"]):
            m[t] = f"small_{k * 5 // max(len(u['small']), 1) + 1}"
        out[d.to_period("M")] = m
        names.update(m)
    return out, sorted(names)


def run() -> dict:
    buckets, names = size_buckets()
    costs = build(names)
    OUT.mkdir(parents=True, exist_ok=True)
    costs.assign(month=costs["month"].astype(str), applies_to=costs["applies_to"].astype(str)).to_parquet(OUT / f"{KEY}.parquet")
    return {"key": KEY, "order_usd": ORDER_USD, "impact_y": IMPACT_Y, "rows": int(len(costs)), **summarize(costs, buckets)}


ZH = {"large": "前 1000", **{f"small_{k}": f"1001–3000 第 {k} 档" for k in range(1, 6)}}


def report_text(res: dict) -> str:
    pc = lambda x: "—" if x is None else f"{x * 100:.2f}%"  # noqa: E731
    lines = [f"小盘成本模型（{KEY}），单笔 ${res['order_usd']:,.0f}，冲击系数 {res['impact_y']}",
             "分组 | 样本 | 可交易 | 以下只算可交易：有效价差中位 | 价差在下限附近 | 成交额中位 | 往返成本 中位 / 均值 / 90% | 冲击翻倍 | 价差翻倍 | Corwin-Schultz"]
    for b, x in res["buckets"].items():
        lines.append(f"{ZH.get(b, b)} | {x['n']:,} | {x['tradeable'] * 100:.0f}% | {pc(x['spread_median'])} | "
                     f"{x['spread_floor_share'] * 100:.0f}% | ${x['adv_median'] / 1e6:,.1f}M | "
                     f"{pc(x['rt_main'].get('median'))} / {pc(x['rt_main'].get('mean'))} / {pc(x['rt_main'].get('p90'))} | "
                     f"{pc(x['rt_impact_y1'].get('median'))} | "
                     f"{pc(x['rt_spread_x2'].get('median'))} | {pc(x['rt_corwin_schultz'].get('median'))}")
    lines.append("按年往返中位（前 1000 / 1001–3000 第 1 档 / 第 5 档）：" + "；".join(
        f"{y} {pc(v.get('large', {}).get('rt_median'))} / {pc(v.get('small_1', {}).get('rt_median'))} / {pc(v.get('small_5', {}).get('rt_median'))}"
        for y, v in res["years"].items()))
    return "\n".join(lines)
