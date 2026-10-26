"""Server-rendered dashboard HTML. Single-user, no build step."""
from __future__ import annotations

import json
from html import escape
from typing import Any, Iterable

from scout.telegram import CONFIDENCE_LABELS, STATUS_LABELS, base_rate_text, market_split_text, pct

CSS = """
:root{--bg:#f7f7f5;--panel:#fff;--ink:#1d1d1f;--muted:#6b6b70;--line:#e4e4e0;--accent:#2f6fde;
--up:#1a8f4c;--down:#c8393b;--warn:#b7791f;--chip:#eef2fb}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){--bg:#121214;--panel:#1b1b1e;--ink:#ececef;
--muted:#9a9aa2;--line:#2c2c31;--accent:#6c9cff;--up:#3ccf7e;--down:#ff6b6b;--warn:#f0b04f;--chip:#22283a}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:14px/1.55 -apple-system,BlinkMacSystemFont,"PingFang SC","Segoe UI",sans-serif}
header{display:flex;gap:18px;align-items:center;padding:12px 16px;border-bottom:1px solid var(--line);
background:var(--panel);position:sticky;top:0;z-index:2;flex-wrap:wrap}
header b{font-size:16px}header a{color:var(--muted);text-decoration:none}header a.on{color:var(--ink);font-weight:600}
main{max-width:1180px;margin:0 auto;padding:16px}
.grid{display:grid;gap:12px;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));align-items:start}
details.card summary{cursor:pointer;font-weight:600}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}
.card h3{margin:0 0 4px;font-size:15px}.muted{color:var(--muted)}.small{font-size:12px}
.up{color:var(--up)}.down{color:var(--down)}.warn{color:var(--warn)}
.chip{display:inline-block;background:var(--chip);border-radius:999px;padding:1px 8px;font-size:12px;margin-right:4px}
table{width:100%;border-collapse:collapse;background:var(--panel);border:1px solid var(--line);border-radius:10px;overflow:hidden}
th,td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}
th{font-size:12px;color:var(--muted);font-weight:500}tr:last-child td{border-bottom:none}
.scroll{overflow-x:auto}a{color:var(--accent)}
.levels{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;margin:8px 0}
.levels div{background:var(--bg);border-radius:6px;padding:4px 6px}.levels span{display:block;font-size:11px;color:var(--muted)}
.analysis dt{font-size:12px;color:var(--muted);margin-top:8px}.analysis dd{margin:2px 0 0}
.analysis .headline{font-weight:600;font-size:15px;margin:2px 0 6px}
.ai{border-top:1px solid var(--line);margin-top:12px;padding-top:10px}
.sectors td,.sectors th{padding:5px 8px}
ul.tight{margin:4px 0;padding-left:18px}pre{white-space:pre-wrap;font:inherit;margin:0}
input,textarea,select,button{font:inherit;color:inherit;background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:6px 8px}
button{background:var(--accent);color:#fff;border:none;cursor:pointer}
#chart{height:380px}
"""


def layout(title: str, active: str, body: str, head: str = "") -> str:
    nav = [("/", "today", "今日"), ("/ideas", "ideas", "机会看板"), ("/scoreboard", "scoreboard", "记分板"),
           ("/watchlist", "watchlist", "Watchlist"), ("/chat", "chat", "问答")]
    links = "".join(f'<a href="{h}" class="{"on" if k == active else ""}">{t}</a>' for h, k, t in nav)
    return f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(title)} · Scout</title>
<style>{CSS}</style>{head}</head><body><header><b>Scout</b>{links}
<form action="/ticker" method="get" style="margin-left:auto"><input name="t" placeholder="代码，如 NVDA" size="10"></form>
</header><main>{body}</main></body></html>"""


def e(x: Any) -> str:
    return escape("" if x is None else str(x))


def signed(x: float | None, digits: int = 1) -> str:
    if x is None:
        return "—"
    cls = "up" if x > 0 else "down" if x < 0 else ""
    return f'<span class="{cls}">{pct(x, digits, True)}</span>'


def opp_card(o: dict) -> str:
    ev, th, br = o.get("evidence") or {}, o.get("thesis") or {}, o.get("base_rate") or {}
    label = (ev.get("setup") or {}).get("label", o["primary_detector"])
    chips = "".join(f'<span class="chip">{e(d)}</span>' for d in o.get("detectors") or [])
    theme = f'<span class="chip">#{e(ev["theme"])}</span>' if ev.get("theme") else ""
    lists = ""
    if th.get("catalysts"):
        lists += "<div class='small muted'>催化</div><ul class='tight'>" + "".join(f"<li>{e(c)}</li>" for c in th["catalysts"]) + "</ul>"
    if th.get("counter_evidence"):
        lists += "<div class='small muted'>反方</div><ul class='tight'>" + "".join(f"<li>{e(c)}</li>" for c in th["counter_evidence"]) + "</ul>"
    base = (
        e(base_rate_text(br))
        if br.get("n") else "暂无历史基准"
    )
    return f"""<div class="card">
<h3><a href="/ticker/{e(o['ticker'])}">{e(o['ticker'])}</a> · {e(label)} <span class="muted small">评分 {o['score']:.2f}</span></h3>
<div class="small muted">{e(o['asof'])} · {e(STATUS_LABELS.get(o['status'], o['status']))} · {o['horizon_days']} 日 · R:R {(o.get('risk_reward') or 0):.1f}</div>
<div>{chips}{theme}</div>
<p><b>{e(th.get('headline'))}</b><br>{e(th.get('thesis'))}</p>
<div class="levels"><div><span>入场</span>{o['entry_lo']:.2f}–{o['entry_hi']:.2f}</div><div><span>止损</span>{o['stop']:.2f}</div>
<div><span>目标 1</span>{o['target1']:.2f}</div><div><span>目标 2</span>{(o.get('target2') or o['target1']):.2f}</div></div>
<div class="small">{e(market_split_text(ev.get("price") or {}) or "")}</div>
<div class="small">{base}</div>{lists}
<div class="small warn">{('⚠️ ' + e(th['key_risk'])) if th.get('key_risk') else ''}</div>
<div class="small muted">置信度 {e(CONFIDENCE_LABELS.get(th.get('confidence'), '—'))} · 文本来源 {e(o.get('thesis_source'))}</div>
</div>"""


def tlink(ticker: str, label: str | None = None) -> str:
    return f'<a href="/ticker/{e(ticker)}">{e(label or ticker)}</a>'


def scout_panel(ticker: str, headline: str, lines: list, ai: dict | None, ai_enabled: bool) -> str:
    """'Scout 分析' card: deterministic reading + entry point to the AI interpretation."""
    tone_cls = {"up": "up", "down": "down", "warn": "warn"}
    rows = "".join(
        f"<dt>{e(l.label)}</dt><dd class='{tone_cls.get(l.tone, '')}'>{e(l.text)}</dd>" for l in lines
    )
    if ai:
        ai_html = (f"<a href='#insight'>查看多视角 AI 解读 ↓</a> <span class='small muted'>生成于 {e(str(ai['created_at'])[:16])} UTC</span>"
                   f"<form method='post' action='/ticker/{e(ticker)}/ai'><button style='margin-top:6px'>重新生成</button></form>")
    elif ai_enabled:
        ai_html = ("<div class='small muted'>按 6 个分析视角（道氏趋势、支撑阻力与形态、动量与相对强度、均值回归与波动、大盘与板块、催化与事件）"
                   "对照知识库检查清单撰写，每条结论引用证据并经程序校验，约 10–20 秒。</div>"
                   f"<form method='post' action='/ticker/{e(ticker)}/ai'><button style='margin-top:6px'>生成 AI 解读</button></form>")
    else:
        ai_html = "<div class='small muted'>AI 解读需要配置 OPENAI_API_KEY。</div>"
    return (f"<div class='card analysis'><h3>Scout 分析</h3><div class='headline'>{e(headline)}</div>"
            f"<dl style='margin:0'>{rows}</dl><div class='ai'>{ai_html}</div></div>")


VERDICT_CLS = {"偏多": "up", "偏空": "down", "中性": "", "不适用": "muted"}
CONF_ZH = {"low": "低", "medium": "中", "high": "高"}


def insight_card(rec: dict) -> str:
    """Structured multi-lens AI interpretation with evidence chips and audit details."""
    from scout.insight import LENS_TITLES

    ins, pack = rec["insight"], rec["pack"]
    ev_index = {i["字段"]: i for b in pack["lenses"] for i in b["evidence"]}

    def chip(key: str) -> str:
        item = ev_index.get(key)
        tip = f"{item['名称']} {item['数值']}" + (f"｜{item['读数']}" if item.get("读数") else "") if item else key
        return f"<span class='chip' title='{e(tip)}'>{e(item['名称'] if item else key)}</span>"

    lens_html = "".join(
        f"<div class='card' style='padding:10px'><b>{e(LENS_TITLES.get(lv['lens'], lv['lens']))}</b> "
        f"<span class='{VERDICT_CLS.get(lv['verdict'], '')}'>· {e(lv['verdict'])}</span>"
        f"<ul class='tight'>{''.join(f'<li>{e(p)}</li>' for p in lv['points'])}</ul>"
        f"<div>{''.join(chip(k) for k in lv['evidence'])}</div></div>"
        for lv in ins["lenses"]
    )
    val = rec.get("validation") or {}
    hist = "；".join(f"第 {h['attempt']} 次：{'通过' if not h['issues'] else '，'.join(h['issues'])}" for h in val.get("history", []))
    source = f"{rec.get('model')} · 校验通过" if rec.get("source") == "llm" else "未通过校验，显示规则读数"
    evidence_rows = "".join(
        f"<tr><td>{e(b['title'])}</td><td>{e(i['名称'])}</td><td>{e(i['数值'])}</td><td>{e(i.get('读数') or '')}</td></tr>"
        for b in pack["lenses"] for i in b["evidence"]
    )
    import json as _json
    return f"""<div class='card' id='insight' style='grid-column:1/-1'>
<h3>AI 解读 · 多视角 <span class='small muted'>数据日期 {e(pack['data_date'])} · {e(source)}</span></h3>
<div class='headline'>{e(ins['headline'])}</div>
<p><span class='chip'>驱动力：{e(ins.get('driver', '—'))}</span> {e(ins['attribution'])}</p>
<div class='grid'>{lens_html}</div>
<p><b>关注条件</b>：{e('；'.join(ins['watch']))}<br><b>失效条件</b>：{e('；'.join(ins['invalidation']))}<br>
<b>主要风险</b>：{e(ins['key_risk'])}<br><b>置信度</b>：{e(CONF_ZH.get(ins['confidence'], ins['confidence']))} — {e(ins['confidence_reason'])}</p>
<details><summary class='small'>查看依据的数据与校验记录</summary>
<p class='small muted'>校验：{e(hist or '—')}</p>
<div class='scroll'><table><thead><tr><th>视角</th><th>指标</th><th>数值</th><th>读数</th></tr></thead><tbody>{evidence_rows}</tbody></table></div>
<pre class='small'>{e(_json.dumps(pack['context'], ensure_ascii=False, indent=1, default=str))}</pre>
</details>
<p class='small muted'>由 LLM 依据上述证据与知识库检查清单撰写，数字与引用经程序核对；非投资建议。</p></div>"""


def sector_rows(rows: list[dict], label_of: dict[str, str]) -> str:
    body = "".join(
        f"<tr><td>{tlink(r['ticker'])}</td><td>{e(label_of.get(r['ticker'], ''))}</td>"
        f"<td>{signed(r.get('ret_1d'))}</td><td>{signed(r.get('ret_5d'))}</td><td>{signed(r.get('ret_20d'))}</td></tr>"
        for r in rows
    )
    return (f"<div class='scroll'><table class='sectors'><thead><tr><th>ETF</th><th>板块</th><th>1 日</th><th>5 日</th>"
            f"<th>20 日</th></tr></thead><tbody>{body}</tbody></table></div>")


def table(headers: Iterable[str], rows: Iterable[Iterable[str]]) -> str:
    h = "".join(f"<th>{x}</th>" for x in headers)
    b = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    return f'<div class="scroll"><table><thead><tr>{h}</tr></thead><tbody>{b}</tbody></table></div>'


CHART_HEAD = '<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>'


def chart_script(bars: list[dict], levels: dict | None) -> str:
    data = [{"time": b["date"].isoformat(), "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"]}
            for b in bars]
    vols = [{"time": b["date"].isoformat(), "value": b["volume"]} for b in bars]
    return f"""<div id="chart" class="card"></div><script>
(function(){{const dark=matchMedia('(prefers-color-scheme: dark)').matches;
const el=document.getElementById('chart');const css=getComputedStyle(document.documentElement);
const c=LightweightCharts.createChart(el,{{autoSize:true,layout:{{background:{{color:'transparent'}},textColor:css.getPropertyValue('--muted')}},
grid:{{vertLines:{{color:css.getPropertyValue('--line')}},horzLines:{{color:css.getPropertyValue('--line')}}}},timeScale:{{borderVisible:false}},rightPriceScale:{{borderVisible:false}}}});
const s=c.addCandlestickSeries({{upColor:css.getPropertyValue('--up'),downColor:css.getPropertyValue('--down'),borderVisible:false,
wickUpColor:css.getPropertyValue('--up'),wickDownColor:css.getPropertyValue('--down')}});
s.setData({json.dumps(data)});
const v=c.addHistogramSeries({{priceScaleId:'vol',priceFormat:{{type:'volume'}},color:dark?'#3a3f4b':'#d5d9e2'}});
c.priceScale('vol').applyOptions({{scaleMargins:{{top:0.8,bottom:0}}}});v.setData({json.dumps(vols)});
const lv={json.dumps(levels or {})};
const add=(p,t,col)=>p&&s.createPriceLine({{price:p,title:t,color:col,lineWidth:1,lineStyle:2}});
add(lv.entry_lo,'入场下沿',css.getPropertyValue('--accent'));add(lv.entry_hi,'入场上沿',css.getPropertyValue('--accent'));
add(lv.stop,'止损',css.getPropertyValue('--down'));add(lv.target1,'目标1',css.getPropertyValue('--up'));
add(lv.target2,'目标2',css.getPropertyValue('--up'));c.timeScale().fitContent();}})();</script>"""
