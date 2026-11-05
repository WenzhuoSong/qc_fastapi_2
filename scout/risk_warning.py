"""Risk warnings, pre-registered in `scout/knowledge/external_specs/risk_warning_v1.yaml` (one run, local only).

Two levels: the SPDR sector ETFs and SPY. Five fixed conditions per level (trend break, volatility expansion,
overextension, breadth divergence, credit stress); two or more at a close = a warning. Measured as
(1) the share of days followed by a large drawdown within 63 sessions, warning vs not (month-clustered t,
Driscoll-Kraay as a second view), and (2) holding with cash during warnings vs holding throughout
(plus, for SPY, on top of the user's 10-month market filter). Data up to the decision close only;
trades at the next close (`cross_asset.simulate`).

Data: Sharadar closeadj for the ETFs, SEP closeadj / closeunadj and daily market caps for member breadth
(top 1000 by cap, price ≥ 5 dollars, Sharadar's current sector field), FRED Baa − 10-year spread (previous day;
the registered high-yield series has only three years on FRED, see the run note),
French daily RF.
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cross_asset as ca

KEY = "risk_warning_v1"
FAMILY = "risk_warning"
SECTORS = ("XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY", "XLRE", "XLC")
EXTRA = ("SOXX",)                       # descriptive only
MARKET = "SPY"
SECTOR_OF = {"Technology": "XLK", "Healthcare": "XLV", "Financial Services": "XLF", "Consumer Cyclical": "XLY",
             "Consumer Defensive": "XLP", "Energy": "XLE", "Industrials": "XLI", "Basic Materials": "XLB",
             "Utilities": "XLU", "Real Estate": "XLRE", "Communication Services": "XLC"}
SEMIS = ("Semiconductors", "Semiconductor Equipment & Materials")
START = pd.Timestamp("1999-01-04")
PARQUET = Path("exports/sharadar/parquet")

SMA = 200
VOL_DAYS, VOL_WINDOW, VOL_MIN, VOL_TOP = 21, 756, 504, 0.8
EXT_WINDOW, EXT_MIN, EXT_TOP = 1260, 756, 0.95
HIGH_DAYS, NEAR_HIGH, BREADTH_MAX, MIN_MEMBERS = 252, 0.95, 0.5, 10
CREDIT_DAYS, CREDIT_Z = 252, 2.0
CREDIT_SERIES = "BAA10Y"                # risk_warning_v1_run_note
WARN_MIN = 2
HORIZON = 63
BIG_DD = {"sector": -0.15, "market": -0.10}
LIFT_MIN, T_BAR = 1.5, 3.21
COST, COST_HIGH = 0.0005, 0.0020
NW_LAGS = 63
TOP_N, TOP_MARKET = 1000, 500
MIN_PRICE = 5.0
CONDITIONS = {"trend_break": "跌破 200 日均线", "vol_expansion": "21 日波动处于自身 3 年最高五分之一",
              "overextended": "偏离 200 日均线处于自身 5 年最高 5%", "breadth_divergence": "接近一年高点但成分股站上 50 日线不到一半",
              "credit_stress": "Baa 公司债利差高于一年均值 2 个标准差"}


# ── data ──

def load_members() -> pd.DataFrame:
    """Daily share of members above their 50-day average and member counts, per sector ETF, SOXX
    (semiconductor industries) and the top 500 overall ('TOP500'): columns '<key>' and '<key>_n'."""
    tick = pd.read_parquet(PARQUET / "tickers.parquet", columns=["table", "ticker", "sector", "industry"])
    tick = tick[tick["table"] == "SEP"].drop_duplicates("ticker", keep="last").set_index("ticker")
    px = pd.read_parquet(PARQUET / "stocks.parquet", columns=["ticker", "date", "closeadj", "closeunadj"],
                         filters=[("date", ">=", pd.Timestamp("1997-06-01").date())])
    px["date"] = pd.to_datetime(px["date"])
    px = px.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    g = px.groupby("ticker", sort=False)["closeadj"]
    cs = g.cumsum()
    sma50 = (cs - cs.groupby(px["ticker"], sort=False).shift(50).fillna(0.0)) / 50
    sma50[px.groupby("ticker", sort=False).cumcount() < 49] = np.nan
    px["above"] = (px["closeadj"] > sma50).where(sma50.notna())
    px = px.drop(columns=["closeadj"])
    cap = pd.read_parquet(PARQUET / "daily.parquet", columns=["ticker", "date", "marketcap"],
                          filters=[("date", ">=", pd.Timestamp("1998-01-01").date())])
    cap["date"] = pd.to_datetime(cap["date"])
    px = px.merge(cap, on=["ticker", "date"], how="inner")
    px = px[(px["closeunadj"] >= MIN_PRICE) & px["marketcap"].notna() & px["above"].notna()]
    px["rank"] = px.groupby("date")["marketcap"].rank(ascending=False, method="first")
    top = px[px["rank"] <= TOP_N].copy()
    top["key"] = top["ticker"].map(tick["sector"]).map(SECTOR_OF)
    top["semi"] = top["ticker"].map(tick["industry"]).isin(SEMIS)
    frames = []
    for name, part in (("sector", top[top["key"].notna()]),):
        agg = part.groupby(["date", "key"])["above"].agg(["mean", "size"]).unstack("key")
        frames += [agg["mean"], agg["size"].add_suffix("_n")]
    semi = top[top["semi"]].groupby("date")["above"].agg(["mean", "size"])
    frames.append(semi.rename(columns={"mean": "SOXX", "size": "SOXX_n"}))
    m = top[top["rank"] <= TOP_MARKET].groupby("date")["above"].agg(["mean", "size"])
    frames.append(m.rename(columns={"mean": "TOP500", "size": "TOP500_n"}))
    return pd.concat(frames, axis=1).sort_index()


async def load_credit() -> pd.Series:
    from datetime import date

    from scout.data import fred

    # BAMLH0A0HYM2 (the registered series) only has three years on FRED; BAA10Y per the run note
    rows = await fred.series(CREDIT_SERIES, date(1996, 1, 1), date(2026, 10, 28))
    return pd.Series({pd.Timestamp(d): v for d, v in rows}, dtype=float).sort_index()


# ── conditions (each row uses data up to that row only) ──

def trailing_share_le(x: pd.Series, window: int, min_periods: int) -> pd.Series:
    """Share of the trailing window (including today) at or below today's value."""
    return x.rolling(window, min_periods=min_periods).apply(lambda a: float(np.mean(a <= a[-1])), raw=True)


def conditions(close: pd.Series, breadth: pd.Series | None, members: pd.Series | None, credit_z: pd.Series) -> pd.DataFrame:
    """Boolean condition frame plus a 'ready' column (every condition computable)."""
    sma = close.rolling(SMA, min_periods=SMA).mean()
    ext = close / sma - 1
    vol = np.log(close).diff().rolling(VOL_DAYS, min_periods=VOL_DAYS).std()
    vol_rank = trailing_share_le(vol, VOL_WINDOW, VOL_MIN)
    ext_rank = trailing_share_le(ext, EXT_WINDOW, EXT_MIN)
    high = close.rolling(HIGH_DAYS, min_periods=HIGH_DAYS).max()
    b = breadth.reindex(close.index) if breadth is not None else pd.Series(np.nan, index=close.index)
    n = members.reindex(close.index) if members is not None else pd.Series(np.nan, index=close.index)
    cz = credit_z.reindex(close.index)
    out = pd.DataFrame({
        "trend_break": close < sma,
        "vol_expansion": vol_rank > VOL_TOP,
        "overextended": ext_rank > EXT_TOP,
        "breadth_divergence": (close >= NEAR_HIGH * high) & (b < BREADTH_MAX) & (n >= MIN_MEMBERS),
        "credit_stress": cz >= CREDIT_Z,
    }, index=close.index)
    out["ready"] = sma.notna() & vol_rank.notna() & ext_rank.notna() & high.notna() & b.notna() & cz.notna()
    out["count"] = out[list(CONDITIONS)].sum(axis=1)
    out["warning"] = out["ready"] & (out["count"] >= WARN_MIN)
    return out


def credit_z(spread: pd.Series, index: pd.DatetimeIndex) -> pd.Series:
    """z of the previous day's spread vs its trailing 252 observations."""
    s = spread.dropna()
    z = (s - s.rolling(CREDIT_DAYS, min_periods=CREDIT_DAYS).mean()) / s.rolling(CREDIT_DAYS, min_periods=CREDIT_DAYS).std()
    return z.reindex(index, method="ffill").shift(1)


# ── outcomes ──

def forward_min(close: pd.Series, h: int = HORIZON) -> pd.Series:
    """min(close[t+1 .. t+h]) / close[t] − 1; NaN without h sessions ahead."""
    a = close.to_numpy()
    out = np.full(len(a), np.nan)
    for t in range(len(a) - h):
        out[t] = a[t + 1: t + h + 1].min() / a[t] - 1
    return pd.Series(out, index=close.index)


def forward_ret(close: pd.Series, h: int) -> pd.Series:
    return close.shift(-h) / close - 1


# ── statistics ──

def dummy_regression(y: np.ndarray, w: np.ndarray, groups: np.ndarray, dates: np.ndarray) -> dict:
    """y on [1, w]: the slope is the difference in means; month-clustered and Driscoll-Kraay (NW over dates) t."""
    X = np.column_stack([np.ones(len(y)), w.astype(float)])
    xtx_inv = np.linalg.inv(X.T @ X)
    beta = xtx_inv @ X.T @ y
    u = y - X @ beta
    score = X * u[:, None]

    def sandwich(ids: np.ndarray, lags: int = 0) -> float:
        _, inv = np.unique(ids, return_inverse=True)
        s = np.zeros((inv.max() + 1, 2))
        np.add.at(s, inv, score)
        meat = s.T @ s
        for L in range(1, lags + 1):
            gamma = s[L:].T @ s[:-L]
            meat += (1 - L / (lags + 1)) * (gamma + gamma.T)
        g = len(s)
        v = xtx_inv @ meat @ xtx_inv * (g / (g - 1) if lags == 0 else 1.0)
        return float(math.sqrt(v[1, 1]))

    se_c = sandwich(groups)
    se_dk = sandwich(dates, NW_LAGS)
    return {"diff": float(beta[1]), "t_month": float(beta[1] / se_c) if se_c > 0 else None,
            "t_dk": float(beta[1] / se_dk) if se_dk > 0 else None, "months": int(len(np.unique(groups)))}


def predictive(panel: pd.DataFrame, flag: str, threshold: float) -> dict:
    """panel rows = (date, asset) with columns flag, dd (forward min), r21, r63."""
    p = panel.dropna(subset=["dd"])
    if p.empty:
        return {}
    y = (p["dd"] <= threshold).to_numpy(dtype=float)
    w = p[flag].to_numpy(dtype=bool)
    dates = p.index.get_level_values("date")
    reg = dummy_regression(y, w, dates.to_period("M").astype(str).to_numpy(), dates.to_numpy()) if 0 < w.sum() < len(w) else {}
    pw = float(y[w].mean()) if w.any() else None
    pn = float(y[~w].mean()) if (~w).any() else None
    return {"n": int(len(p)), "n_warn": int(w.sum()), "share_warn": float(w.mean()), "p_warn": pw, "p_none": pn,
            "lift": pw / pn if pw is not None and pn else None, **reg,
            "r21_warn": float(p.loc[w, "r21"].mean()) if w.any() else None, "r21_none": float(p.loc[~w, "r21"].mean()),
            "r63_warn": float(p.loc[w, "r63"].mean()) if w.any() else None, "r63_none": float(p.loc[~w, "r63"].mean())}


def halves_of(panel: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    dates = panel.index.get_level_values("date")
    mid = dates.min() + (dates.max() - dates.min()) / 2
    return panel[dates < mid], panel[dates >= mid], mid


def episodes(flags: pd.Series, dd: pd.Series, threshold: float) -> dict:
    """Warnings counted from off → on; share of episodes with a large drawdown within 63 sessions of the start."""
    f = flags.fillna(False).astype(bool)
    starts = f & ~f.shift(1, fill_value=False)
    d = dd.reindex(f.index)[starts]
    hit = (d.dropna() <= threshold)
    years = len(f) / ca.YEAR
    return {"episodes": int(starts.sum()), "per_year": float(starts.sum() / years) if years else None,
            "share_days": float(f.mean()), "episodes_with_big_dd": int(hit.sum()), "episodes_scored": int(len(hit)),
            "false_alarm_share": float(1 - hit.mean()) if len(hit) else None}


# ── exposure test ──

def month_ends(index: pd.DatetimeIndex) -> list[int]:
    return ca.month_end_rows(index)


def sector_schedule(px: pd.DataFrame, flags: pd.DataFrame, ready: pd.DataFrame, use_warning: bool) -> dict[int, dict[str, float]]:
    """Equal weight over sectors whose conditions are computable, rebalanced monthly; with warnings, a sector in
    warning is held as cash and every warning change re-targets the book (decided at row i, traded next close)."""
    ends = set(month_ends(px.index))
    f = flags.fillna(False).to_numpy(dtype=bool)
    r = ready.fillna(False).to_numpy(dtype=bool)
    cols = list(flags.columns)
    out = {}
    for i in range(len(px) - 1):
        change = use_warning and i > 0 and bool((f[i] != f[i - 1]).any())
        if not (i in ends or change or (i > 0 and bool((r[i] != r[i - 1]).any()))):
            continue
        live = [c for j, c in enumerate(cols) if r[i, j]]
        if not live:
            continue
        w = 1 / len(live)
        out[i] = {c: (0.0 if use_warning and f[i, cols.index(c)] else w) for c in live}
    return out


def market_filter_on(close: pd.Series) -> pd.Series:
    """Faber's 10-month rule as Scout uses it: month-end close above the mean of the last 10 month-end closes;
    held until the next month end."""
    ends = month_ends(close.index)
    me = close.iloc[ends]
    on = me > me.rolling(10, min_periods=10).mean()
    return on.reindex(close.index).ffill().fillna(False).astype(bool)


def market_schedule(close: pd.Series, warn: pd.Series, ready: pd.Series, mode: str) -> dict[int, dict[str, float]]:
    """mode: hold | warning | filter | filter_warning; decisions at each close where the target changes."""
    filt = market_filter_on(close)
    out, last = {}, None
    rd = ready.fillna(False).to_numpy(dtype=bool)
    wn = warn.fillna(False).to_numpy(dtype=bool)
    fl = filt.to_numpy(dtype=bool)
    for i in range(len(close) - 1):
        if not rd[i]:
            continue
        held = True
        if mode in ("warning", "filter_warning"):
            held = held and not wn[i]
        if mode in ("filter", "filter_warning"):
            held = held and fl[i]
        if held != last:
            out[i] = {MARKET: 1.0 if held else 0.0}
            last = held
    return out


def perf(ret: pd.Series, rf: pd.Series, mid: pd.Timestamp, cash: pd.Series | None = None) -> dict:
    out = {"all": ca.stats(ret, rf), "first": ca.stats(ret[ret.index < mid], rf), "second": ca.stats(ret[ret.index >= mid], rf)}
    if cash is not None:
        out["cash_share"] = float(cash.mean())
    return out


# ── run ──

def level_panel(px: pd.DataFrame, keys: list[str], conds: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for k in keys:
        c = conds[k]
        f = pd.DataFrame({"warning": c["warning"], **{n: c[n] for n in CONDITIONS}, "ready": c["ready"],
                          "dd": forward_min(px[k]), "r21": forward_ret(px[k], 21), "r63": forward_ret(px[k], HORIZON)})
        f = f[f["ready"] & (f.index >= START)].drop(columns="ready")
        f["asset"] = k
        rows.append(f)
    p = pd.concat(rows)
    p.index.name = "date"
    return p.set_index("asset", append=True)


def evaluate_level(panel: pd.DataFrame, threshold: float) -> dict:
    first, second, mid = halves_of(panel)
    out = {"mid": str(mid.date()), "warning": {"all": predictive(panel, "warning", threshold),
                                               "first": predictive(first, "warning", threshold),
                                               "second": predictive(second, "warning", threshold)}}
    out["single"] = {n: predictive(panel, n, threshold) for n in CONDITIONS}
    return out


def decide(level: dict, exposure: dict, control: str, extra: tuple[str, str] | None = None) -> dict:
    w = level["warning"]
    checks = {
        "t_month": (w["all"].get("t_month") or 0) >= T_BAR,
        "lift": (w["all"].get("lift") or 0) >= LIFT_MIN,
        "halves_positive": all((w[h].get("diff") or 0) > 0 for h in ("first", "second")),
        "sharpe_halves": all(exposure["warning"][h]["sharpe"] > exposure[control][h]["sharpe"] for h in ("first", "second")),
        "drawdown_halves": all(exposure["warning"][h]["max_dd"] > exposure[control][h]["max_dd"] for h in ("first", "second")),
    }
    out = {"checks": checks, "pass": all(checks.values())}
    if extra:
        a, b = extra
        out["adds_to_filter"] = all(exposure[a][h]["sharpe"] >= exposure[b][h]["sharpe"] for h in ("first", "second"))
    return out


def run(credit: pd.Series) -> dict:
    px, rf = ca.load(SECTORS + EXTRA + (MARKET,))
    members = load_members().reindex(px.index)
    cz = credit_z(credit, px.index)
    conds = {k: conditions(px[k].dropna().reindex(px.index), members.get(k), members.get(f"{k}_n"), cz)
             for k in SECTORS + EXTRA}
    conds[MARKET] = conditions(px[MARKET], members["TOP500"], members["TOP500_n"], cz)
    for k, c in conds.items():           # an ETF before its listing has no close: not ready
        c.loc[px[k].isna(), ["ready", "warning"]] = False

    res: dict = {"key": KEY, "data_end": str(px.index[-1].date()),
                 "starts": {k: str(c.index[c["ready"] & (c.index >= START)].min().date()) if (c["ready"] & (c.index >= START)).any() else None
                            for k, c in conds.items()}}
    sec_panel = level_panel(px, list(SECTORS), conds)
    mkt_panel = level_panel(px, [MARKET], conds)
    res["sector"] = evaluate_level(sec_panel, BIG_DD["sector"])
    res["market"] = evaluate_level(mkt_panel, BIG_DD["market"])
    res["extra"] = {k: evaluate_level(level_panel(px, [k], conds), BIG_DD["sector"]) for k in EXTRA}
    res["sector"]["episodes"] = {k: episodes(conds[k]["warning"][conds[k]["ready"] & (px.index >= START)],
                                             forward_min(px[k]), BIG_DD["sector"]) for k in SECTORS}
    res["market"]["episodes"] = episodes(conds[MARKET]["warning"][conds[MARKET]["ready"] & (px.index >= START)],
                                         forward_min(px[MARKET]), BIG_DD["market"])

    # exposure: sectors
    flags = pd.DataFrame({k: conds[k]["warning"] for k in SECTORS})
    ready = pd.DataFrame({k: conds[k]["ready"] & (px.index >= START) for k in SECTORS})
    spx = px[list(SECTORS)]
    mid = pd.Timestamp(res["sector"]["mid"])
    exp_s = {}
    for name, use in (("warning", True), ("hold", False)):
        for cost, tag in ((COST, ""), (COST_HIGH, "_cost_high")):
            sim = ca.simulate(spx, rf, sector_schedule(spx, flags, ready, use), cost)
            exp_s[name + tag] = {**perf(sim["ret"], rf, mid, sim["cash"]), "turnover_per_year": float(sum(sim["turnover"]) / (len(sim["ret"]) / ca.YEAR))}
    res["sector"]["exposure"] = exp_s
    res["sector"]["decision"] = decide(res["sector"], exp_s, "hold")

    # exposure: market
    mid = pd.Timestamp(res["market"]["mid"])
    mready = conds[MARKET]["ready"] & (px.index >= START)
    mpx = px[[MARKET]]
    exp_m = {}
    for mode in ("warning", "hold", "filter", "filter_warning"):
        for cost, tag in ((COST, ""), (COST_HIGH, "_cost_high")):
            sim = ca.simulate(mpx, rf, market_schedule(px[MARKET], conds[MARKET]["warning"], mready, mode), cost)
            exp_m[mode + tag] = {**perf(sim["ret"], rf, mid, sim["cash"]), "trades": len(sim["turnover"])}
    res["market"]["exposure"] = exp_m
    res["market"]["decision"] = decide(res["market"], exp_m, "hold", ("filter_warning", "filter"))
    res["market"]["decision"]["vs_filter"] = {h: {"warning": exp_m["warning"][h]["sharpe"], "filter": exp_m["filter"][h]["sharpe"]}
                                              for h in ("first", "second")}
    return res


# ── report ──

def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def report_text(res: dict) -> str:
    lines = [f"风险预警检验 {res['key']}（数据到 {res['data_end']}）", f"起始日：{res['starts']}"]
    for level, zh, ctl in (("sector", "板块（SPDR，合并）", "hold"), ("market", "大盘（SPY）", "hold")):
        r = res[level]
        w = r["warning"]
        lines.append(f"\n【{zh}】样本中点 {r['mid']}")
        for h in ("all", "first", "second"):
            x = w[h]
            if not x:
                continue
            lines.append(f"  {h}: 预警日占比 {x['share_warn'] * 100:.1f}%（{x['n_warn']}/{x['n']}），63 日内大回撤 预警 {_p(x['p_warn'])} vs 平时 {_p(x['p_none'])}，"
                         f"lift {x['lift'] or 0:.2f}，差 {_p(x.get('diff'))}，t(月) {x.get('t_month') or 0:.2f}，t(DK) {x.get('t_dk') or 0:.2f}；"
                         f"63 日收益 预警 {_p(x['r63_warn'])} vs 平时 {_p(x['r63_none'])}")
        lines.append("  单个条件（描述）：")
        for n, x in r["single"].items():
            if x:
                lines.append(f"    {CONDITIONS[n]}：占比 {x['share_warn'] * 100:.1f}%，大回撤 {_p(x['p_warn'])} vs {_p(x['p_none'])}，lift {x['lift'] or 0:.2f}，t(月) {x.get('t_month') or 0:.2f}")
        ex = r["exposure"]
        lines.append("  降敞口：")
        for k, v in ex.items():
            lines.append(f"    {k}: 年化 {_p(v['all']['cagr'])} 夏普 {v['all']['sharpe']:.2f}（前 {v['first']['sharpe']:.2f} / 后 {v['second']['sharpe']:.2f}）"
                         f" 最大回撤 {_p(v['all']['max_dd'])}（前 {_p(v['first']['max_dd'])} / 后 {_p(v['second']['max_dd'])}） 现金 {v.get('cash_share', 0) * 100:.0f}%")
        lines.append(f"  判定：{r['decision']}")
    lines.append(f"\n板块预警次数：{ {k: (v['episodes'], None if v['false_alarm_share'] is None else round(v['false_alarm_share'], 2)) for k, v in res['sector']['episodes'].items()} }")
    lines.append(f"大盘预警：{res['market']['episodes']}")
    for k, r in res["extra"].items():
        x = r["warning"]["all"]
        if x:
            lines.append(f"\n{k}（描述）：预警日 {x['share_warn'] * 100:.1f}%，大回撤 {_p(x['p_warn'])} vs {_p(x['p_none'])}，lift {x['lift'] or 0:.2f}，t(月) {x.get('t_month') or 0:.2f}")
    return "\n".join(lines)
