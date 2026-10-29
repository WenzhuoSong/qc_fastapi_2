"""External replication on Open Source Asset Pricing (pre-registered in
`scout/knowledge/external_specs/osap_replication.yaml`).

For each Scout ranking strategy the matching academic signal's monthly top decile minus the mean
of its ten deciles (the long side only, as Scout's pick − same-day others), 1963-07..2025-12,
judged with the gate's bar (t ≥ 3, both halves positive) and set beside Scout's own verdict.
The data is public and kept locally (`exports/research_data/osap/`, git-ignored); nothing of it
is stored in the database beyond summary numbers.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

DATA = Path(__file__).resolve().parents[1] / "exports" / "research_data" / "osap"
START, END = "1963-07-01", "2025-12-31"
OVERLAP = ("2015-11-01", "2025-12-31")
PRIMARY = "deciles_vw"
CONSTRUCTIONS = {"deciles_vw": "市值加权", "deciles_ew": "等权", "nyse": "纽交所分界点"}
MAPPING = {"rank_low_asset_growth": "AssetGrowth", "rank_net_buyback": "ShareIss1Y", "rank_gross_profitability": "GP",
           "rank_low_accruals": "Accruals", "rank_residual_momentum": "ResidualMomentum", "rank_high_52w": "High52",
           "rank_short_term_reversal": "STreversal", "rank_ear_drift": "AnnouncementReturn", "b0_core": "Mom12m"}
LABELS = {"rank_low_asset_growth": "资产增长最慢", "rank_net_buyback": "净回购", "rank_gross_profitability": "毛利率",
          "rank_low_accruals": "低应计", "rank_residual_momentum": "残差动量", "rank_high_52w": "52 周新高",
          "rank_short_term_reversal": "短期反转", "rank_ear_drift": "财报后漂移", "b0_core": "12 个月动量（B0 核心）"}
PUBLISHED = {"AssetGrowth": 2008, "ShareIss1Y": 2008, "GP": 2013, "Accruals": 1996, "ResidualMomentum": 2011,
             "High52": 2004, "STreversal": 1990, "AnnouncementReturn": 1996, "Mom12m": 1993}
BEARS = [("1968-12", "1970-05"), ("1973-01", "1974-09"), ("1980-11", "1982-08"), ("1987-08", "1987-11"),
         ("2000-03", "2002-09"), ("2007-10", "2009-02"), ("2020-02", "2020-03"), ("2022-01", "2022-09")]
MOM_CRASH = ("2009-03", "2009-05")
T_MIN = 3.0


def load(path: Path = DATA / "deciles_selected.csv") -> pd.DataFrame:
    d = pd.read_csv(path, dtype={"port": str})
    d["date"] = pd.to_datetime(d["date"])
    return d


def edge_series(d: pd.DataFrame, construction: str, signal: str) -> pd.Series:
    """Monthly top decile (10) minus the mean of the ten deciles, in percent, START..END."""
    q = d[(d.construction == construction) & (d.signalname == signal)].pivot_table(index="date", columns="port", values="ret")
    q = q[[f"{k:02d}" for k in range(1, 11)]].dropna()
    s = q["10"] - q.mean(axis=1)
    return s[(s.index >= START) & (s.index <= END)]


def stat(s: pd.Series) -> dict:
    if len(s) < 3 or s.std(ddof=1) == 0:
        return {"edge": None if s.empty else round(float(s.mean()), 4), "t": None, "months": len(s)}
    return {"edge": round(float(s.mean()), 4), "t": round(float(s.mean() / (s.std(ddof=1) / np.sqrt(len(s)))), 2), "months": len(s)}


def _months(s: pd.Series, spans) -> pd.Series:
    mask = np.zeros(len(s), dtype=bool)
    for a, b in spans:
        mask |= (s.index >= pd.Timestamp(a)) & (s.index <= pd.Timestamp(b) + pd.offsets.MonthEnd(0))
    return s[mask]


def describe(s: pd.Series, published: int) -> dict:
    mid = s.index[len(s) // 2]
    out = {"all": stat(s), "first": stat(s[s.index < mid]), "second": stat(s[s.index >= mid]), "split": str(mid.date()),
           "pre_pub": stat(s[s.index.year <= published]), "post_pub": stat(s[s.index.year > published]),
           "bear": stat(_months(s, BEARS)), "mom_crash": stat(_months(s, [MOM_CRASH])),
           "overlap": stat(s[(s.index >= OVERLAP[0]) & (s.index <= OVERLAP[1])]),
           "decades": {f"{y}s": stat(s[(s.index.year >= y) & (s.index.year < y + 10)]) for y in range(1960, 2030, 10)}}
    a = out["all"]
    out["pass"] = (a["t"] is not None and a["t"] >= T_MIN and (out["first"]["edge"] or 0) > 0 and (out["second"]["edge"] or 0) > 0)
    return out


def category(academic_pass: bool, scout_pass: bool | None, overlap_edge: float | None) -> str:
    if scout_pass is None:
        return "Scout 无结论"
    if academic_pass and scout_pass:
        return "一致有效"
    if academic_pass:
        return "学术有效、Scout 未通过：" + ("近年衰减" if (overlap_edge or 0) <= 0 else "可能是股票池或定义差异")
    return "Scout 有效、学术无效" if scout_pass else "一致无效"


def analyze(d: pd.DataFrame, scout: dict[str, dict]) -> dict:
    """scout: {key: {"passed": bool, "edge": per-holding edge, "t": t, "hold": sessions}} from the gate runs."""
    out = {}
    for key, sig in MAPPING.items():
        res = {"signal": sig, "label": LABELS[key], "published": PUBLISHED[sig]}
        for c in CONSTRUCTIONS:
            s = edge_series(d, c, sig)
            res[c] = describe(s, PUBLISHED[sig]) if len(s) else {}
        sc = scout.get(key)
        res["scout"] = sc
        prim = res[PRIMARY]
        res["category"] = category(bool(prim.get("pass")), None if sc is None else bool(sc["passed"]), prim.get("overlap", {}).get("edge"))
        ew, vw = res["deciles_ew"].get("all", {}).get("edge"), prim.get("all", {}).get("edge")
        res["size_gap"] = None if ew is None or vw is None else round(ew - vw, 4)
        out[key] = res
    return out


def report_text(res: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.2f}%"  # noqa: E731
    lines = [f"外部复现（Open Source Asset Pricing，{START[:7]} 至 {END[:7]}，第 10 组 − 十组平均，每月；主口径市值加权）",
             "策略 | 学术 全部（t） | 前半 / 后半 | 发表前 / 发表后（t） | 熊市月份 | 2015-11 起 | 等权 − 市值加权 | Scout（每 21 日，t） | 判断"]
    for key, r in res.items():
        v = r[PRIMARY]
        if not v:
            lines.append(f"{r['label']} | 无数据")
            continue
        sc = r["scout"]
        scout_txt = "—" if not sc else f"{'通过' if sc['passed'] else '未通过'} {p(sc['edge'] * 100 * 21 / sc['hold'])}（{sc['t']}）"
        lines.append(f"{r['label']}（{r['signal']}，{r['published']}） | {p(v['all']['edge'])}（{v['all']['t']}） | "
                     f"{p(v['first']['edge'])} / {p(v['second']['edge'])} | {p(v['pre_pub']['edge'])} / {p(v['post_pub']['edge'])}（{v['post_pub']['t']}） | "
                     f"{p(v['bear']['edge'])}（{v['bear']['months']} 个月） | {p(v['overlap']['edge'])}（{v['overlap']['t']}） | {p(r['size_gap'])} | "
                     f"{scout_txt} | {r['category']}")
    lines.append("逐年代（市值加权，月均超额）：")
    for key, r in res.items():
        if r[PRIMARY]:
            lines.append(f"{r['label']}：" + "，".join(f"{k} {p(x['edge'])}" for k, x in r[PRIMARY]["decades"].items() if x["months"]))
    mom = res.get("b0_core", {}).get(PRIMARY, {})
    if mom:
        lines.append(f"动量崩盘 2009-03 至 2009-05（市值加权，月均）：12 个月动量 {p(mom['mom_crash']['edge'])}")
    return "\n".join(lines)
