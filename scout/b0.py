"""Interface with B0, the user's quant system (v3 P5). Offline only, in both directions.

Export (`export`): files the B0 research session reads and backtests independently on QC:
- README.md: what each file is, what the statuses mean, the rules of use;
- knowledge.json: every current knowledge entry, positive and negative (so B0 does not
  re-test what already failed);
- strategy_<key>.md: each rule that passed Scout's gate: the exact conditions, the level
  formula (source), exit, the gate report and the forward progress, with its status;
- signals_<rule>.csv: that rule's point-in-time signals and fills over the stored history,
  plus the forward record, for B0 to test as a selection or ranking feature;
- tilts.json: sector or theme tilts, only from judgment methods the user has confirmed.
Nothing is sent anywhere: Scout never sends orders, commands or files to QuantConnect or a
broker. The user (or the B0 session) reads the directory.

Import (`import_entries`): B0's results, written as YAML by the user or the B0 session,
become knowledge entries (`changed_by` = import). An import can record a finding as a
hypothesis, backtested, forward or rejected; never as confirmed, which stays the user's call
in Scout.
"""
from __future__ import annotations

import csv
import inspect
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

from scout import db, forward, gate, learned
from scout.detectors import registry

logger = logging.getLogger("scout.b0")

EXPORT_ROOT = Path(__file__).resolve().parents[1] / "exports" / "b0"
EXPORTABLE = ("backtested", "forward", "confirmed")
IMPORT_STATUSES = ("hypothesis", "backtested", "forward", "rejected")
STATUS_NOTE = {
    "backtested": "只通过了 Scout 的回测和批判，前向验证还没开始：只能用于 B0 的离线回测，不能用于实盘。",
    "forward": "Scout 的前向验证进行中、尚未达标：只能用于 B0 的离线回测，不能用于实盘。",
    "confirmed": "用户已在 Scout 确认（回测、批判、前向都通过）。是否在 B0 里使用由用户决定。",
}


def _jsonable(x):
    if isinstance(x, (date,)):
        return x.isoformat()
    return str(x)


def entry_public(en: dict) -> dict:
    keep = ("key", "version", "kind", "polarity", "status", "title", "claim", "mechanism", "context", "applies", "evidence",
            "invalidation", "recheck_on", "spec")
    return {k: en.get(k) for k in keep if en.get(k) is not None}


def strategy_markdown(en: dict, det, run: dict | None, fwd: dict | None) -> str:
    spec = en.get("spec") or {}
    conds = "\n".join(f"| {c.label} | `{c.left}` | `{c.op}` | `{c.right}` |" for c in det.conditions)
    from scout import knowledge
    from scout.detectors import r_targets, rules as rule_defs

    # the level function and the helpers it calls, so B0 can reproduce the numbers exactly
    levels_src = "\n\n".join(inspect.getsource(f).rstrip() for f in (det.levels, rule_defs._levels, r_targets))
    levels_src = f"MIN_STOP_ATR = {rule_defs.MIN_STOP_ATR}\n\n" + levels_src
    import re as _re

    used = list(dict.fromkeys([c.left for c in det.conditions] + [c.right for c in det.conditions if isinstance(c.right, str)]
                              + _re.findall(r'r\["(\w+)"\]', inspect.getsource(det.levels))))
    fields = "\n".join(f"| `{k}` | {(knowledge.features().get(k) or {}).get('zh', '')} | {(knowledge.features().get(k) or {}).get('definition', '')} |"
                       for k in used)
    s = (run or {}).get("backtest", {}).get("summary") or {}
    k = spec.get("primary_control", "select")

    def edge(key: str) -> str:
        e = s.get(key) or {}
        return "—" if e.get("edge") is None else f"{e['edge'] * 100:+.2f}%（t {e['t']}）"

    lines = [
        f"# {en['title']}（{en['key']}，第 {en['version']} 版）", "",
        f"**状态：{learned.STATUS_ZH[en['status']]}。** {STATUS_NOTE.get(en['status'], '')}", "",
        f"**结论**：{en['claim']}", "", f"**机制**：{en.get('mechanism') or '—'}", "",
        f"**预期失效**：{spec.get('expected_failure') or '—'}", "",
        f"## 规则 `{det.name}`（{det.label}）", "", det.description, "",
        "信号日收盘后计算，次日起才能成交。同一只股票 10 个交易日内只算一次。", "",
        "| 条件 | 字段 | 比较 | 阈值 |", "|---|---|---|---|", conds, "",
        f"- 入场窗口：信号后 {det.trigger_window} 个交易日内，第一根价格区间与入场区间重叠的日线成交，成交价 = 开盘价夹到区间内；",
        (f"- 出场：止损；到目标 1 全部卖出；持有满 {det.horizon_days} 个交易日按收盘离场（`fixed`）；" if not det.exit.trailing else
         f"- 出场：止损；持有满 {det.exit.lookback} 个交易日后，止损上移到最近 {det.exit.lookback} 日最低价（只升不降），"
         f"没有固定目标，最长 {det.exit.max_hold} 个交易日（`trail_low`）；"),
        "- 每根日线先检查止损再检查目标（同一天都碰到时按止损算）；跳空越过价位时按开盘价成交；",
        f"- 成本：每笔往返 {gate.ROUND_TRIP_COST * 100:.1f}%。", "",
        "价位公式（Scout 源码）：", "", "```python", levels_src.rstrip(), "```", "",
        "用到的字段（定义来自 Scout 的 `scout/knowledge/features.yaml`）：", "",
        "| 字段 | 名称 | 定义 |", "|---|---|---|", fields, "",
        "## Scout 检验结果", "",
    ]
    if s:
        lines += [f"样本 {s.get('first')} 至 {s.get('last')}，{s.get('n')} 笔成交，按 {s.get('split')} 分前后两半；扣费后平均 {s.get('avg_net', 0) * 100:+.2f}%。", "",
                  "| 对照 | 全样本 | 前半 | 后半 |", "|---|---|---|---|"]
        lines += [f"| {gate.LABEL_ZH[x]} | {edge(f'vs_{x}')} | {edge(f'vs_{x}_first')} | {edge(f'vs_{x}_second')} |"
                  for x in (*gate.BENCHMARKS, *gate.CONTROLS)]
        lines += ["", f"主对照：{gate.LABEL_ZH[k]}。股票池是当前 S&P 500 / 400 与 Nasdaq-100 成分股（有幸存者偏差；对照来自同一池子，偏差在配对差值里基本抵消）。"]
    else:
        lines.append("（没有检验记录）")
    if fwd:
        lines += ["", f"## 前向验证", "", f"{fwd['started'] or '未开始'} 起，{fwd['signals']} 个信号，{fwd['n']} 个已完成。"]
        lines += [f"- {'✓' if c['ok'] else '…'} {c['name']}：{c['value']}（要求 {c['bar']}）" for c in fwd["criteria"]]
    lines += ["", "## 建议 B0 怎么检验", "",
              "1. 在 QC 上按上面的条件和价位公式独立实现，不调参数；",
              "2. 和 Scout 用同样的对照：同一天随机选的股票、同样的价位和出场；",
              "3. 另外检验它作为 B0 选股的过滤或排序特征，见 `signals_*.csv`；",
              "4. 结果（通过或不通过）写成 YAML 交回 Scout（格式见 README），作为独立复现记入知识库。"]
    return "\n".join(lines) + "\n"


async def export(out: Path | None = None, today: date | None = None) -> dict:
    from scout.pipeline import load_panel

    today = today or date.today()
    out = out or EXPORT_ROOT / today.isoformat()
    out.mkdir(parents=True, exist_ok=True)
    entries = await learned.current()
    (out / "knowledge.json").write_text(json.dumps([entry_public(en) for en in entries], ensure_ascii=False, indent=2,
                                                   default=_jsonable), encoding="utf-8")
    dets = {d.name: d for d in registry(include_candidates=True)}
    rules = [en for en in entries if en["kind"] == "rule" and en["status"] in EXPORTABLE and learned.rule_of(en) in dets]
    files = ["README.md", "knowledge.json"]
    p = m = None
    for en in rules:
        det = dets[learned.rule_of(en)]
        run = await db.fetch_one("SELECT backtest FROM scout.gate_runs WHERE key = :k AND passed ORDER BY id DESC LIMIT 1",
                                 {"k": en["key"]})
        fwd = await forward.report(en, today) if en["status"] in ("forward", "confirmed") else None
        name = f"strategy_{en['key']}.md"
        (out / name).write_text(strategy_markdown(en, det, run, fwd), encoding="utf-8")
        files.append(name)
        if p is None:
            p = await load_panel(lookback_days=None)
            m = gate.Market(p)
        rows = gate.fills(p, det, m)
        live = await db.fetch_all("SELECT * FROM scout.forward_trades WHERE key = :k AND role = 'signal' ORDER BY asof, ticker",
                                  {"k": en["key"]})
        name = f"signals_{det.name}.csv"
        with open(out / name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["signal_date", "ticker", "source", "entry_date", "exit_date", "gross_return", "regime"])
            for r in rows:
                w.writerow([r["date"], r["ticker"], "replay", p.dates[r["a"]], p.dates[r["b"]], f"{r['ret']:.6f}", r["regime"]])
            for r in live:
                w.writerow([r["asof"], r["ticker"], "forward", r["entry_date"] or "", r["exit_date"] or "",
                            "" if r["ret"] is None else f"{r['ret']:.6f}", ""])
        files.append(name)
    methods = {en["key"]: en for en in entries if en["kind"] == "judgment"}
    theses = await db.fetch_all("SELECT ticker, asof, stance, lens, payload FROM scout.judgments WHERE kind = 'sector_thesis' "
                                "ORDER BY asof DESC LIMIT 30")
    confirmed = [t for t in theses if (methods.get(t["lens"]) or {}).get("status") == "confirmed"]
    tilts = {"rule": "只有用户在 Scout 确认过的判断方法产生的行业判断才会出现在 tilts 里。",
             "tilts": [{"etf": t["ticker"], "date": t["asof"], "stance": t["stance"], "method": t["lens"],
                        "horizon": (t["payload"] or {}).get("horizon")} for t in confirmed],
             "unverified_for_information_only": [{"etf": t["ticker"], "date": t["asof"], "stance": t["stance"], "method": t["lens"],
                                                  "method_status": (methods.get(t["lens"]) or {}).get("status")}
                                                 for t in theses if t not in confirmed]}
    (out / "tilts.json").write_text(json.dumps(tilts, ensure_ascii=False, indent=2, default=_jsonable), encoding="utf-8")
    files.append("tilts.json")
    (out / "README.md").write_text(readme(today, rules, entries, tilts), encoding="utf-8")
    return {"dir": str(out), "files": files, "strategies": [en["key"] for en in rules], "tilts": len(tilts["tilts"])}


def readme(today: date, rules: list[dict], entries: list[dict], tilts: dict) -> str:
    counts = {s: sum(en["status"] == s for en in entries) for s in learned.STATUSES}
    return f"""# Scout → B0 导出（{today}）

Scout 是研究台，B0 是执行系统。这个目录是 Scout 交给 B0 研究会话**离线**使用的材料；Scout 不会向 QuantConnect 或券商发送任何东西。

## 文件

| 文件 | 内容 |
|---|---|
| `knowledge.json` | Scout 知识库的全部当前条目（{len(entries)} 条：已确认 {counts['confirmed']}，前向验证中 {counts['forward']}，回测通过 {counts['backtested']}，假设 {counts['hypothesis']}，否定 {counts['rejected']}，已失效 {counts['retired']}）。否定的条目也在里面，避免 B0 重复检验已经失败的想法。 |
| `strategy_*.md` | 通过 Scout 检验关的规则（{len(rules)} 条）：条件、价位公式、出场、检验结果、前向进度、状态。 |
| `signals_*.csv` | 这些规则按时点的信号和成交：`replay` 是历史回放，`forward` 是前向记录。可以检验它们作为 B0 选股过滤或排序特征的效果。 |
| `tilts.json` | 行业或主题倾向。只有用户确认过的判断方法产生的判断才会进入 `tilts`（目前 {len(tilts['tilts'])} 条）；`unverified_for_information_only` 里是未验证的判断，只供参考，不能用于配置。 |

## 状态的含义

- **回测通过**、**前向验证中**：只能在 B0 里做离线回测，不能用于实盘。
- **已确认**：用户在 Scout 里确认过（回测、AI 批判、至少 6 个月且 30 个样本的前向验证都通过）。是否用于 B0 实盘，仍由用户决定。

## B0 的结果怎么交回 Scout

写一个 YAML 文件（一个列表），每条结果一项，然后运行 `python -m scout.jobs.b0 import <文件>`：

```yaml
- key: b0_example                 # 新 key；已有的 key 会新增一个版本
  kind: rule                      # rule / feature / regularity / method
  polarity: negative              # positive（有效）/ negative（无效）
  status: rejected                # hypothesis / backtested / forward / rejected（不能直接写 confirmed）
  title: 一句话标题
  claim: 结论
  mechanism: 机制（可选）
  applies: {{topics: [b0]}}
  evidence:
    - source: b0
      date: 2026-10-28
      summary: 检验摘要（数字照抄报告）
      ref: notes/xxx_report.md    # B0 报告文件
  invalidation: 什么情况下要重新检验
  recheck_on: 2027-04-28
```
"""


# ── import ──────────────────────────────────────────────────────────────

def validate_import(items: list[dict]) -> list[str]:
    issues = []
    for i, it in enumerate(items):
        tag = it.get("key") or f"第 {i + 1} 条"
        if it.get("status") not in IMPORT_STATUSES:
            issues.append(f"{tag}：导入只能写 {' / '.join(IMPORT_STATUSES)}，不能直接写已确认")
        if it.get("status") == "rejected" and it.get("polarity") != "negative":
            issues.append(f"{tag}：否定的结论 polarity 应为 negative")
        if not any((ev.get("source") or "") == "b0" for ev in it.get("evidence") or []) and it.get("status") != "hypothesis":
            issues.append(f"{tag}：至少要有一条 source 为 b0 的证据")
        issues += [f"{tag}：{x}" for x in learned.validate(it)]
    return issues


def load_import(path: Path | str) -> list[dict]:
    items = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or []
    for it in items:
        for ev in it.get("evidence") or []:
            ev["date"] = str(ev["date"])
        if it.get("recheck_on"):
            it["recheck_on"] = str(it["recheck_on"])
        it.setdefault("context", {})
        it.setdefault("applies", {"topics": ["b0"]})
    return items


async def import_entries(items: list[dict], note: str) -> list[dict]:
    issues = validate_import(items)
    if issues:
        raise learned.KnowledgeError("；".join(issues))
    out = []
    for it in items:
        en = await learned.write(it, "import", note)
        out.append({"key": en["key"], "version": en["version"], "status": en["status"]})
    return out


# ── B0's ranking, recomputed by Scout (information only) ────────────────
# The user's B0 ranks stocks by a multi-horizon momentum score inside the largest members of
# two indexes, and holds stocks only while a market filter is on. The parameters are B0's and stay
# out of this repository: they come from the environment (SCOUT_B0_SCORE, JSON, see
# `MomentumScore.from_json`); without it the ranking is off. Scout has no ETF weights, so index
# size is approximated by 20-day dollar volume. B0's results come from its whole portfolio process,
# not from this list.

@dataclass(frozen=True)
class MomentumScore:
    weights: tuple[tuple[int, float], ...]   # (horizon in sessions, weight) on log returns
    trend_horizon: int                       # this horizon's log return must be > 0
    min_dollar_volume: float                 # 20-day average dollar volume floor
    per_index: int                           # largest members taken from each index
    indexes: tuple[str, ...]                 # instrument source tags, e.g. sp500, ndx
    gate: tuple[str, ...]                    # tickers that must close above their moving average
    gate_sma: int
    target_holdings: int
    show: int = 30

    @classmethod
    def from_json(cls, text: str) -> "MomentumScore | None":
        """{"weights": {"<sessions>": w, ...}, "trend_horizon": n, "min_dollar_volume": x, "per_index": n,
        "indexes": [...], "gate": [...], "gate_sma": n, "target_holdings": n}; None when unset."""
        if not (text or "").strip():
            return None
        d = json.loads(text)
        weights = tuple(sorted(((int(k), float(v)) for k, v in d["weights"].items()), key=lambda x: -x[0]))
        cfg = cls(weights, int(d["trend_horizon"]), float(d["min_dollar_volume"]), int(d["per_index"]),
                  tuple(d["indexes"]), tuple(d["gate"]), int(d["gate_sma"]), int(d["target_holdings"]), int(d.get("show", 30)))
        if not weights or cfg.trend_horizon not in dict(weights) or cfg.per_index <= 0 or not cfg.indexes:
            raise ValueError("SCOUT_B0_SCORE: weights must include trend_horizon; per_index and indexes are required")
        return cfg

    @property
    def longest(self) -> int:
        return max(n for n, _ in self.weights)


def score_config() -> MomentumScore | None:
    from scout.config import get_scout_settings

    return MomentumScore.from_json(get_scout_settings().scout_b0_score)


def b0_universe(close, volume, members: dict[str, list[str]], cfg: MomentumScore) -> dict[str, list[str]]:
    """ticker → the indexes whose largest `per_index` members (by 20-day dollar volume, the weight proxy) it is in."""
    dv = (close * volume).iloc[-20:].mean()
    out: dict[str, list[str]] = {}
    for index in cfg.indexes:
        names = [t for t, src in members.items() if index in src and t in dv.index and dv[t] == dv[t]]
        for t in sorted(names, key=lambda t: -dv[t])[: cfg.per_index]:
            out.setdefault(t, []).append(index)
    return out


def b0_scores(close, volume, universe, cfg: MomentumScore) -> list[dict]:
    """The weighted sum of log returns over the configured horizons, with the trend filter and the
    dollar-volume floor; highest first."""
    import numpy as np

    rows = []
    for t in universe:
        if t not in close.columns:
            continue
        c = close[t].dropna()
        if len(c) < cfg.longest + 1:
            continue
        dv = float(volume[t].dropna().tail(20).mean() * c.iloc[-1]) if t in volume.columns else 0.0
        if dv < cfg.min_dollar_volume:
            continue
        p_now = float(c.iloc[-1])
        past = {n: float(c.iloc[-1 - n]) for n, _ in cfg.weights}
        if min(p_now, *past.values()) <= 0:
            continue
        r = {n: float(np.log(p_now / past[n])) for n in past}
        if r[cfg.trend_horizon] <= 0:
            continue
        rows.append({"ticker": t, "score": sum(w * r[n] for n, w in cfg.weights), "dollar_volume": dv,
                     "returns": {str(n): r[n] for n in r}})
    rows.sort(key=lambda x: -x["score"])
    for i, x in enumerate(rows, 1):
        x["rank"] = i
    return rows


def b0_gate(close, cfg: MomentumScore) -> dict:
    """The market filter: every gate ticker above its moving average."""
    out = {"tickers": {}}
    for t in cfg.gate:
        c = close[t].dropna() if t in close.columns else None
        if c is None or len(c) < cfg.gate_sma:
            out["tickers"][t] = None
            continue
        sma = float(c.iloc[-cfg.gate_sma:].mean())
        out["tickers"][t] = {"close": float(c.iloc[-1]), "sma": sma, "above": bool(c.iloc[-1] > sma), "gap": float(c.iloc[-1] / sma - 1)}
    out["risk_on"] = bool(out["tickers"]) and all(v and v["above"] for v in out["tickers"].values())
    out["sma"] = cfg.gate_sma
    return out


async def store_b0_rank(p, asof) -> dict:
    """After the close: B0's ranking and market filter on Scout's data (brief `b0_rank`)."""
    cfg = score_config()
    if cfg is None:
        return {"skipped": "SCOUT_B0_SCORE not set"}
    inst = await db.fetch_all("SELECT ticker, name, sector, sources FROM scout.instruments WHERE kind = 'stock'")
    members = {r["ticker"]: list(r["sources"] or []) for r in inst}
    meta = {r["ticker"]: r for r in inst}
    close, volume = p["close"], p["volume"]
    uni = b0_universe(close, volume, members, cfg)
    rows = b0_scores(close, volume, uni, cfg)
    gate_ = b0_gate(close, cfg)
    top = [{**x, "indexes": uni.get(x["ticker"], []), "name": (meta.get(x["ticker"]) or {}).get("name"),
            "sector": (meta.get(x["ticker"]) or {}).get("sector")} for x in rows[: cfg.show]]
    payload = {"asof": str(asof), "gate": gate_, "universe": len(uni), "scored": len(rows), "rows": top,
               "weights": [list(w) for w in cfg.weights], "trend_horizon": cfg.trend_horizon,
               "min_dollar_volume": cfg.min_dollar_volume, "per_index": cfg.per_index, "indexes": list(cfg.indexes),
               "target_holdings": cfg.target_holdings}
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('b0_rank', :d, :c, CAST(:p AS JSONB))",
                     {"d": asof, "c": "B0 动量打分：" + "、".join(x["ticker"] for x in top[:10]), "p": db.jsonb(payload)})
    return {"universe": len(uni), "scored": len(rows), "risk_on": gate_["risk_on"], "top3": [x["ticker"] for x in top[:3]]}


async def latest_b0_rank() -> dict | None:
    if score_config() is None:
        return None
    row = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'b0_rank' ORDER BY id DESC LIMIT 1")
    return row["payload"] if row and "tickers" in (row["payload"].get("gate") or {}) else None
