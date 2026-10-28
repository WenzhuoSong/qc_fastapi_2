"""Server-rendered dashboard HTML. Single-user, no build step."""
from __future__ import annotations

import json
from datetime import date
from html import escape
from typing import Any, Iterable

from scout import knowledge, portfolio
from scout.premarket import overnight_line
from scout.sectors import QUADRANTS, rotation_read, rotation_reads
from scout.telegram import (CONFIDENCE_LABELS, STATUS_LABELS, base_rate_text, market_split_text, outcome_text, pct,
                            position_text, sector_text)

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
button.linkbtn{background:none;color:var(--accent);padding:0 2px;text-decoration:underline}
.chartrow{display:grid;grid-template-columns:minmax(0,2.2fr) minmax(260px,1fr);gap:12px;align-items:start}
.duo{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1.3fr);gap:12px;align-items:start;margin-bottom:12px}
@media (max-width:900px){.chartrow,.duo{grid-template-columns:1fr}}
.chartbox{position:relative;padding:8px}#chart{height:440px}
#legend{position:absolute;top:10px;left:14px;z-index:3;background:var(--panel);padding:2px 6px;border-radius:4px;opacity:.92}
.plan table td,.plan table th{padding:4px 6px}
.rrg .tail{opacity:0;transition:opacity .15s}.rrg.all .tail,.rrg .s:hover .tail,.rrg .s.on .tail{opacity:1}
.rrg.pick .s:not(.on){opacity:.18}.rrg .s.on text{font-weight:600}
.rrgbar{display:flex;flex-wrap:wrap;gap:4px;align-items:center;margin:6px 0}
.rrgbtn{border:1px solid var(--line);color:var(--ink);cursor:pointer;padding:1px 8px}.rrgbtn.on{background:var(--accent);color:#fff}
ul.rrgread{list-style:none;padding:0;margin:6px 0}ul.rrgread li{padding:2px 0}
"""


def layout(title: str, active: str, body: str, head: str = "") -> str:
    nav = [("/", "today", "今日"), ("/market", "market", "市场全景"), ("/ideas", "ideas", "机会看板"), ("/screens", "screens", "策略筛选"), ("/scoreboard", "scoreboard", "记分板"),
           ("/watchlist", "watchlist", "Watchlist"), ("/trades", "trades", "我的交易"), ("/outlook", "outlook", "行业前景"), ("/ledger", "ledger", "判断台账"), ("/knowledge", "knowledge", "知识库"), ("/chat", "chat", "问答"), ("/data", "data", "数据")]
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


def why_html(why: dict) -> str:
    """'为什么入选': chase-risk badge + position always visible, details collapsible."""
    if not why.get("conditions"):
        return ""
    pos = position_text(why) or ""
    badge = "<span class='chip' style='background:var(--down);color:#fff'>⚠️ 追高风险</span> " if why.get("chase_risk") else ""
    met = sum(c["ok"] for c in why["conditions"])
    rows = "".join(
        f"<tr><td>{'✓' if c['ok'] else '✗'}</td><td>{e(c['label'])}</td><td>{e(c['actual'])}</td>"
        f"<td>{e(c['op'])} {e(c['threshold'])}</td></tr>" for c in why["conditions"]
    )
    parts = "".join(
        f"<tr><td>{e(p['label'])}</td><td>{p['value']:.2f}</td><td>×{p['weight']:.2f}</td><td>{p['contribution']:.3f}</td></tr>"
        for p in why.get("score_parts") or []
    )
    adj = "；".join(a["label"] + (f" {a['value']:+.2f}" if a["key"] in ("earnings_penalty", "watchlist_bonus") else "")
                   for a in why.get("adjustments") or [])
    others = "、".join(why.get("other_rules") or [])
    return (f"<div class='small'>{badge}{e(pos.replace(' · ⚠️追高风险', ''))}</div>"
            f"<details class='small'><summary>为什么入选：{e(why['rule'])}（{met}/{len(why['conditions'])} 条件满足）"
            f"{'，同时触发 ' + e(others) if others else ''}</summary>"
            f"<table><tbody>{rows}</tbody></table>"
            f"<div style='margin-top:6px'>评分 {why['score']:.2f}（优势依据：{e(why.get('edge_basis', ''))}）</div>"
            f"<table><thead><tr><th>分项</th><th>得分</th><th>权重</th><th>贡献</th></tr></thead><tbody>{parts}</tbody></table>"
            f"{'<div>调整：' + e(adj) + '</div>' if adj else ''}</details>")


DEMOTED_NOTE = ("<div class='small warn'>该规则已降为候选（回放扣费后跑输持有），"
                "这条机会只继续跟踪到结束、计入实盘记录，不建议据此新开仓。</div>")


def is_live(detector: str) -> bool:
    from scout.detectors import registry

    return any(d.name == detector and d.live for d in registry(include_candidates=True))


def event_html(ev: dict) -> str:
    flags = ev.get("event_flags") or []
    if not flags:
        return ""
    return ("<div class='small warn'>⚡ 事件驱动：" + "；".join(f"{escape(f['type'])}（{escape(f['headline'])}）" for f in flags)
            + "。技术规则的历史统计对这类走势参考价值有限。</div>")


def opp_card(o: dict, settings: dict | None = None) -> str:
    ev, th, br = o.get("evidence") or {}, knowledge.humanize_obj(o.get("thesis") or {}), o.get("base_rate") or {}
    label = (ev.get("setup") or {}).get("label", o["primary_detector"])
    chips = "".join(f'<span class="chip">{e(d)}</span>' for d in o.get("detectors") or [])
    theme = f'<span class="chip">#{e(ev["theme"])}</span>' if ev.get("theme") else ""
    demoted = DEMOTED_NOTE if not is_live(o["primary_detector"]) and o.get("status") in ("new", "triggered") else ""
    lists = ""
    if th.get("catalysts"):
        lists += "<div class='small muted'>催化</div><ul class='tight'>" + "".join(f"<li>{e(c)}</li>" for c in th["catalysts"]) + "</ul>"
    if th.get("counter_evidence"):
        lists += "<div class='small muted'>反方</div><ul class='tight'>" + "".join(f"<li>{e(c)}</li>" for c in th["counter_evidence"]) + "</ul>"
    base = (
        e(base_rate_text(br))
        if br.get("n") else "暂无历史基准"
    )
    outcome = outcome_text(br)
    if outcome:
        base += f"<br>{e(outcome)}"
    sizing = ""
    if settings is not None and o.get("status") in ("new", "triggered"):
        sz = portfolio.size_text(portfolio.size((o["entry_lo"] + o["entry_hi"]) / 2, o.get("current_stop") or o["stop"], settings))
        sizing = (f"<div class='small'>📐 {e(sz)} · <a href='/trades?opp={e(o['id'])}'>记录入场</a></div>" if sz else "")
    return f"""<div class="card">
<h3><a href="/ticker/{e(o['ticker'])}">{e(o['ticker'])}</a> · {e(label)} <span class="muted small">评分 {o['score']:.2f}</span></h3>
<div class="small muted">{e(o['asof'])} · {e(STATUS_LABELS.get(o['status'], o['status']))} · {o['horizon_days']} 日 · R:R {(o.get('risk_reward') or 0):.1f}</div>
<div>{chips}{theme}</div>{demoted}
<p><b>{e(th.get('headline'))}</b><br>{e(th.get('thesis'))}</p>
<div class="levels"><div><span>入场</span>{o['entry_lo']:.2f}–{o['entry_hi']:.2f}</div><div><span>止损</span>{o['stop']:.2f}</div>
<div><span>目标 1</span>{o['target1']:.2f}</div><div><span>目标 2</span>{f"{o['target2']:.2f}" if (o.get('target2') or 0) > o['target1'] else "—"}</div></div>
{sizing}{event_html(ev)}
<div class="small">{e(market_split_text(ev.get("price") or {}) or "")}</div>
<div class="small">{e(sector_text(ev.get("sector_context")) or "")}</div>
{why_html(ev.get("why") or {})}
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

    ins, pack = knowledge.humanize_obj(rec["insight"]), rec["pack"]
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
{f"<p class='warn'><b>⚡ 近期异动</b>：{e(ins['recent_event'])}</p>" if ins.get('recent_event') else ''}
<p><span class='chip'>驱动力：{e(ins.get('driver', '—'))}</span> {e(ins['attribution'])}</p>
{("<p class='small'><b>视角矛盾</b>：" + "；".join(e(c) for c in ins['conflicts']) + "</p>") if ins.get('conflicts') else ''}
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


def chart_script(bars: list[dict], plan: dict | None, refs: dict | None = None) -> str:
    """Candles with a separate volume band, level lines labelled on the price axis
    only (no text over the candles), the entry zone as a shaded band, and arrow
    markers for signal / entry / exit. `refs` = support/resistance when no plan."""
    data = [{"time": b["date"].isoformat(), "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"]}
            for b in bars]
    vols = [{"time": b["date"].isoformat(), "value": b["volume"], "up": b["close"] >= b["open"]} for b in bars]
    cfg = {"plan": plan or {}, "refs": refs or {}}
    return f"""<div class="chartbox card"><div id="legend" class="small"></div><div id="chart"></div></div><script>
(function(){{const css=getComputedStyle(document.documentElement);const col=n=>css.getPropertyValue(n).trim();
const data={json.dumps(data)},vols={json.dumps(vols)},cfg={json.dumps(cfg, default=str)};
const el=document.getElementById('chart');
const c=LightweightCharts.createChart(el,{{autoSize:true,layout:{{background:{{color:'transparent'}},textColor:col('--muted')}},
grid:{{vertLines:{{visible:false}},horzLines:{{color:col('--line')}}}},crosshair:{{mode:0}},
timeScale:{{borderVisible:false}},rightPriceScale:{{borderVisible:false,scaleMargins:{{top:0.06,bottom:0.24}}}}}});
const s=c.addCandlestickSeries({{upColor:col('--up'),downColor:col('--down'),borderVisible:false,
wickUpColor:col('--up'),wickDownColor:col('--down'),priceLineVisible:false}});
s.setData(data);
const v=c.addHistogramSeries({{priceScaleId:'vol',priceFormat:{{type:'volume'}},lastValueVisible:false,priceLineVisible:false}});
c.priceScale('vol').applyOptions({{scaleMargins:{{top:0.82,bottom:0}}}});
v.setData(vols.map(x=>({{time:x.time,value:x.value,color:x.up?'rgba(46,160,90,.35)':'rgba(200,57,59,.35)'}})));
const P=cfg.plan;
// Keep every plan level on screen: extend autoscale to include stop and targets.
const keep=[P.stop,P.target1,P.target2,P.entry_lo,P.entry_hi,P.current_stop].filter(x=>x);
if(keep.length)s.applyOptions({{autoscaleInfoProvider:orig=>{{const r=orig();if(!r)return r;
  r.priceRange.minValue=Math.min(r.priceRange.minValue,...keep);r.priceRange.maxValue=Math.max(r.priceRange.maxValue,...keep);return r;}}}});
const line=(price,color,style,width)=>price&&s.createPriceLine({{price:price,color:color,lineStyle:style,lineWidth:width||1,
  axisLabelVisible:true,title:''}});
if(P.entry_lo){{
  const from=P.asof||data[0].time, band=data.filter(d=>d.time>=from).map(d=>({{time:d.time,value:P.entry_hi}}));
  const z=c.addBaselineSeries({{baseValue:{{type:'price',price:P.entry_lo}},topFillColor1:'rgba(47,111,222,.18)',
    topFillColor2:'rgba(47,111,222,.18)',topLineColor:'rgba(0,0,0,0)',bottomFillColor1:'rgba(0,0,0,0)',
    bottomFillColor2:'rgba(0,0,0,0)',bottomLineColor:'rgba(0,0,0,0)',lastValueVisible:false,priceLineVisible:false,
    crosshairMarkerVisible:false}});
  z.setData(band);
  line(P.entry_lo,col('--accent'),1);line(P.entry_hi,col('--accent'),1);
  line(P.stop,col('--down'),2,2);line(P.target1,col('--up'),2,2);
  if(P.target2&&P.target2!==P.target1)line(P.target2,col('--up'),3);
  if(P.current_stop&&P.current_stop>P.stop)line(P.current_stop,'#e08a1e',0,2);
  const m=[];
  if(P.asof)m.push({{time:P.asof,position:'belowBar',shape:'arrowUp',color:col('--accent'),text:'信号'}});
  if(P.triggered_on)m.push({{time:P.triggered_on,position:'belowBar',shape:'circle',color:col('--accent'),text:'入场'}});
  if(P.closed_on)m.push({{time:P.closed_on,position:'aboveBar',shape:'arrowDown',color:col('--muted'),text:'离场'}});
  s.setMarkers(m.sort((a,b)=>a.time<b.time?-1:1));
}}else{{
  const R=cfg.refs;line(R.resistance,col('--muted'),3);line(R.support,col('--muted'),3);
}}
const n=data.length;c.timeScale().setVisibleLogicalRange({{from:Math.max(0,n-130),to:n+3}});
const lg=document.getElementById('legend');
const fmt=(x)=>x==null?'—':x.toFixed(2);
const show=(d,vol)=>{{if(!d)return;const i=data.findIndex(x=>x.time===d.time);const prev=i>0?data[i-1].close:d.open;
  const ch=(d.close/prev-1)*100;lg.innerHTML=`<b>${{d.time}}</b> 开 ${{fmt(d.open)}} 高 ${{fmt(d.high)}} 低 ${{fmt(d.low)}} 收 ${{fmt(d.close)}}
  <span style="color:${{ch>=0?col('--up'):col('--down')}}">${{ch>=0?'+':''}}${{ch.toFixed(2)}}%</span> 量 ${{(vol/1e6).toFixed(1)}}M`;}};
show(data[n-1],vols[n-1].value);
c.subscribeCrosshairMove(p=>{{if(!p||!p.time){{show(data[n-1],vols[n-1].value);return;}}
  const d=p.seriesData.get(s),vv=p.seriesData.get(v);show(d?{{...d,time:p.time}}:null,vv?vv.value:0);}});
}})();</script>"""


def _ladder_svg(rows: list[tuple[str, float, str]], close: float, entry_lo: float, entry_hi: float, stop: float, top_t: float) -> str:
    """TradingView-style long-position ladder: red risk box, green reward box,
    blue entry band, current price marker. rows = (label, price, color var)."""
    lo = min(stop, close, *(r[1] for r in rows)) * 0.995
    hi = max(top_t, close, *(r[1] for r in rows)) * 1.005
    H, top = 300, 10

    def y(v: float) -> float:
        return top + (hi - v) / (hi - lo) * H

    mid = (entry_lo + entry_hi) / 2
    parts = [
        f"<rect x='8' y='{y(top_t):.1f}' width='70' height='{y(mid) - y(top_t):.1f}' fill='var(--up)' opacity='.16'/>",
        f"<rect x='8' y='{y(mid):.1f}' width='70' height='{y(stop) - y(mid):.1f}' fill='var(--down)' opacity='.16'/>",
        f"<rect x='8' y='{y(entry_hi):.1f}' width='70' height='{max(y(entry_lo) - y(entry_hi), 2):.1f}' fill='var(--accent)' opacity='.35'/>",
        f"<line x1='2' x2='84' y1='{y(close):.1f}' y2='{y(close):.1f}' stroke='var(--ink)' stroke-dasharray='3 2'/>",
    ]
    placed: list[float] = []
    for label, price, color in sorted(rows + [("现价", close, "--ink")], key=lambda r: -r[1]):
        yy = y(price)
        while any(abs(yy - p) < 13 for p in placed):     # keep labels from overlapping
            yy += 13
        placed.append(yy)
        parts.append(f"<line x1='78' x2='92' y1='{y(price):.1f}' y2='{yy:.1f}' stroke='var({color})' opacity='.6'/>")
        parts.append(f"<text x='96' y='{yy + 4:.1f}' font-size='11' fill='var({color})'>{escape(label)} {price:.2f}</text>")
    return f"<svg viewBox='0 0 230 {H + 2 * top}' width='100%' style='max-height:330px'>{''.join(parts)}</svg>"


def live_line(live: dict | None, close: float | None) -> str:
    """'实时 216.40（盘前 05:50 美东，较前收 +0.2%）' or the last close when no live print."""
    if live:
        return (f"<b>{live['last']:.2f}</b> <span class='small muted'>{escape(live['session'])} {escape(live['time_et'][-5:])} 美东，"
                f"较前收 {signed(live['change'])}</span>")
    return f"<b>{close:.2f}</b> <span class='small muted'>最近收盘</span>" if close else "—"


def plan_status(o: dict, price: float, today, outcome: dict | None, entry_price: float | None) -> str:
    """Where the plan is in its life: waiting for entry, in the trade, or done."""
    from scout.detectors import registry
    from scout.intraday import sessions_since
    from scout.premarket import position_label

    det = next((d for d in registry(include_candidates=True) if d.name == o["primary_detector"]), None)
    window = det.trigger_window if det else 3
    if o["status"] == "new":
        left = window - sessions_since(o["asof"], today)
        when = f"入场窗口还剩 {left} 个交易日" if left > 0 else "今天是入场窗口最后一天" if left == 0 else "入场窗口已过，收盘后会标记为未成交"
        return f"<b>规则等待入场</b> · {escape(when)} · {escape(position_label(price, o))}"
    if o["status"] == "triggered":
        held = sessions_since(o["triggered_on"], today) if o.get("triggered_on") else 0
        stop = o.get("current_stop") or o["stop"]
        text = f"<b>规则已入场</b>（{escape(str(o['triggered_on']))}"
        if entry_price:
            r = (price - entry_price) / max(entry_price - o["stop"], 1e-9)
            text += f"，假设成交 {entry_price:.2f}）· 规则浮动 {signed(price / entry_price - 1)}（{r:+.1f}R）"
        else:
            text += "）"
        return text + f" · 持有第 {held} / {o['horizon_days']} 个交易日 · 现行止损 {stop:.2f} · {escape(position_label(price, o))}"
    if outcome:
        label = STATUS_LABELS.get(outcome["outcome"], outcome["outcome"])
        return f"<b>已结束</b>：{escape(str(label))}（{escape(str(outcome['closed_on']))}），收益 {signed(outcome.get('return_pct'))}"
    return f"<b>{escape(str(STATUS_LABELS.get(o['status'], o['status'])))}</b>"


def trade_plan(o: dict, close: float | None, settings: dict | None = None, live: dict | None = None,
               today=None, outcome: dict | None = None, entry_price: float | None = None) -> str:
    """Order-ticket style panel next to the chart for the active (or latest) plan."""
    from datetime import date as _date

    ev = o.get("evidence") or {}
    ex = ev.get("exit") or {}
    trailing = ex.get("kind") == "trail_low"
    mid = (o["entry_lo"] + o["entry_hi"]) / 2
    risk = mid - o["stop"]
    price = (live or {}).get("last") or close or mid
    cur = o.get("current_stop") if (o.get("current_stop") or 0) > o["stop"] else None
    tgt = "参考目标" if trailing else "目标"
    rows = [(f"{tgt} 1", o["target1"], "--up"), ("入场上沿", o["entry_hi"], "--accent"),
            ("入场下沿", o["entry_lo"], "--accent"), ("保护性止损", o["stop"], "--down")]
    if (o.get("target2") or 0) > o["target1"]:        # equal when the swing high already lies past 2.5R
        rows.insert(0, (f"{tgt} 2", o["target2"], "--up"))
    if cur:
        rows.append(("跟进止损", cur, "--warn"))
    if entry_price:
        rows.append(("假设成交价", entry_price, "--ink"))
    table_rows = "".join(
        f"<tr><td style='color:var({c})'>{escape(label)}</td><td>{p:.2f}</td>"
        f"<td>{signed(p / price - 1)}</td><td>{(p - mid) / risk:+.1f}R</td></tr>"
        for label, p, c in sorted(rows, key=lambda r: -r[1])
    )
    label = (ev.get("setup") or {}).get("label", o["primary_detector"])
    exit_rule = ex.get("rule") or f"固定：到目标 1 止盈，{o['horizon_days']} 个交易日到期"
    extra = ""
    outcome_hist = outcome_text(o.get("base_rate") or {})
    if outcome_hist:
        extra += f"<p class='small'><b>历史上</b>：{escape(outcome_hist.removeprefix('历史结局：'))}。目标是最好情况，不是预期结果。</p>"
    if settings is not None and o.get("status") in ("new", "triggered"):
        sz = portfolio.size_text(portfolio.size(mid, o.get("current_stop") or o["stop"], settings))
        if sz:
            extra += f"<p class='small'><b>仓位</b>：{escape(sz)} · <a href='/trades?opp={escape(o['id'])}'>记录入场</a></p>"
    extra += event_html(o.get("evidence") or {})
    stop_rule = ("跟进止损每天收盘后按规则上移（只升不降）" if trailing else "止损和目标固定不变")
    return f"""<div class="card plan"><h3>交易计划 <span class="chip">{escape(label)}</span></h3>
<div class="small">现价 {live_line(live, close)}</div>
<div class="small" style="margin:4px 0">{plan_status(o, price, today or _date.today(), outcome, entry_price)}</div>
<div class="small muted">信号 {escape(str(o['asof']))} · 1R = {risk:.2f}（{risk / mid * 100:.1f}%）· R:R {(o.get('risk_reward') or 0):.1f}</div>
{_ladder_svg([(r[0], r[1], r[2]) for r in rows], price, o['entry_lo'], o['entry_hi'], o['stop'], max(o.get('target2') or 0, o['target1']))}
<table><thead><tr><th></th><th>价格</th><th>距现价</th><th>R</th></tr></thead><tbody>{table_rows}</tbody></table>
<p class="small"><b>出场规则</b>：{escape(exit_rule)}</p>
{extra}
<p class="small muted">价位在信号当天确定，是规则的一部分（与历史回放一致），不随股价调整；{stop_rule}；状态每天收盘后更新。
入场区间由信号日 ATR 决定，规定交易日内进入区间才算成交。</p></div>"""


STAGE_STATE = {"done": ("已执行", "muted"), "due": ("已到达，待卖出", "warn"), "merged": ("已越过，并入下一档一起卖", "muted"),
               "pending": ("未到", "")}


def holding_card(trades: list[dict], live: dict | None, close: float | None, today=None,
                 sells: dict[int, list[dict]] | None = None) -> str:
    """The user's open position(s) in this stock: the system stop, the take-profit plan
    (reasoning, price, share, what to sell now) and the sells recorded so far."""
    from datetime import date as _date

    from scout import levels
    from scout.intraday import sessions_since

    if not trades:
        return ""
    price = (live or {}).get("last") or close
    items = []
    for t in trades:
        pl = t.get("plan") or {}
        m = portfolio.metrics({**t, "last_close": price})
        line = (f"<div style='margin-top:6px'><b>{escape(str(t['opened_on']))} 买入 {t['shares']:g} 股 @ {t['entry_price']:.2f}</b>"
                + (f"，已卖 {m['sold']:g} 股，剩余 {m['remaining']:g} 股" if m["sold"] else "")
                + (f" · 盈亏 {signed(m['pnl_pct'])}（{m['pnl']:+,.0f}" + (f"，{m['r']:+.1f}R" if "r" in m else "") + "）" if "pnl" in m else ""))
        if t.get("stop"):
            line += (f"<div class='small'><b>止损 {t['stop']:.2f}</b>（{escape(pl.get('stop_now_basis') or pl.get('stop_basis') or '')}"
                     + (f"，距现价 {signed(t['stop'] / price - 1)}" if price else "") + "）</div>")
        if "target1" in pl:
            rows = []
            for st in levels.exit_plan(pl, t["entry_price"], t["shares"], m["remaining"], t.get("stop")):
                label, tone = (("已到达，止损已改为吊灯止损", "muted") if st["stage"] == "目标 1" and st["state"] == "done"
                               else STAGE_STATE[st["state"]])
                act = (escape(levels.due_advice(st, price, t.get("stop"))) if st["state"] == "due" else
                       "改用吊灯止损" if st["stage"] == "目标 1" and st["state"] == "pending" else
                       f"跌破时卖 {st['suggest']:g} 股" if st["state"] == "pending" and st["suggest"] else "—")
                at = (f"{st['price']:.2f}" + (f"（{st['r']:.1f}R）" if st["r"] else "")) if st["price"] else "—"
                rows.append(f"<tr><td>{escape(st['stage'])}</td><td>{at}</td><td>{escape(st['fraction'])}</td>"
                            f"<td class='{tone}'>{act}<br><span class='small'>{label}</span></td>"
                            f"<td class='small muted'>{escape(st['why'])}</td></tr>")
            line += ("<table class='small'><thead><tr><th>出场</th><th>价位</th><th>卖出</th><th>建议</th><th>逻辑</th></tr></thead>"
                     f"<tbody>{''.join(rows)}</tbody></table>")
            if not pl.get("target1_hit"):
                held = sessions_since(t["opened_on"], today or _date.today())
                line += f"<div class='small'>时间止损 {held}/{pl['time_stop_days']} 个交易日"
                if held >= pl["time_stop_days"]:
                    line += " <span class='warn'>已到时间止损仍未到目标 1，考虑卖出全部</span>"
                line += "</div>"
        sl = (sells or {}).get(t["id"]) or []
        if sl:
            line += "<div class='small'>卖出记录：" + "；".join(
                f"{escape(str(x['sold_on']))} {x['shares']:g} 股 @ {x['price']:.2f}"
                f" <form method='post' action='/trades/sells/{x['id']}/delete' style='display:inline' "
                f"onsubmit=\"return confirm('撤销这条卖出记录？')\"><button class='linkbtn small'>撤销</button></form>" for x in sl) + "</div>"
        hist = pl.get("history") or []
        if hist:
            line += "<div class='small muted'>" + "；".join(f"{escape(h['date'])} {escape(h['text'])}" for h in hist[-4:]) + "</div>"
        line += (f"<form method='post' action='/trades/{t['id']}/sell' style='display:flex;gap:4px;flex-wrap:wrap;margin-top:4px'>"
                 f"<input name='shares' value='{m['remaining']:g}' size='5' title='卖出股数' required>"
                 f"<input name='price' placeholder='卖出价' size='7' required><input name='sold_on' type='date' value='{date.today()}' required>"
                 f"<input type='hidden' name='back' value='/ticker/{escape(t['ticker'])}'><button>记录卖出</button></form></div>")
        items.append(line)
    return (f"<div class='card plan'><h3>我的持仓</h3>{''.join(items)}"
            "<p class='small muted'>止损和出场由系统管理：初始止损和目标 1 来自买入前的价位图，每天收盘后更新。"
            "不分批：到目标 1 之前用初始止损；到目标 1 不卖出，止损改为吊灯止损（买入以来最高收盘 − 3×ATR，只升不降），跌破时卖出全部；"
            f"最长持有 {levels.HOLD_SESSIONS} 个交易日。12 年出场回放里这在前后两段样本都是最好的方式（分批止盈和过早收紧止损都会砍掉大赢家）。"
            "记录卖出后（这里、「我的交易」或 Telegram <code>/sell 代码 股数 价格</code>），剩余股数、盈亏和下一步建议会立即更新。Scout 不会替你下单。</p></div>")


QUAD_COLORS = {"leading": "var(--up)", "weakening": "var(--warn)", "lagging": "var(--down)", "improving": "var(--accent)"}


def rrg_svg(rows: list[dict]) -> str:
    """Relative Rotation Graph: RS-ratio (x) vs RS-momentum (y).

    Each series shows its latest weekly step as an arrow; the full 8-week tail
    (older points fainter) appears on hover, when selected, or with 显示全部轨迹.
    """
    pts = [pt for r in rows for pt in r.get("tail") or []]
    if not pts:
        return "<p class='muted'>暂无轮动数据。</p>"
    span_x = max(abs(p[0] - 100) for p in pts) * 1.15 or 1
    span_y = max(abs(p[1] - 100) for p in pts) * 1.15 or 1
    W, H, pad = 560, 420, 30

    def xy(px: float, py: float) -> tuple[float, float]:
        return (pad + (px - 100 + span_x) / (2 * span_x) * (W - 2 * pad),
                pad + (100 + span_y - py) / (2 * span_y) * (H - 2 * pad))

    cx, cy = xy(100, 100)
    defs = "<defs>" + "".join(
        f"<marker id='rrg-{q}' viewBox='0 0 10 10' refX='9' refY='5' markerWidth='7' markerHeight='7' orient='auto'>"
        f"<path d='M0 0L10 5L0 10z' fill='{c}'/></marker>" for q, c in QUAD_COLORS.items()) + "</defs>"
    bg = (f"<rect x='{pad}' y='{pad}' width='{cx - pad}' height='{cy - pad}' fill='var(--accent)' opacity='.07'/>"
          f"<rect x='{cx}' y='{pad}' width='{W - pad - cx}' height='{cy - pad}' fill='var(--up)' opacity='.07'/>"
          f"<rect x='{cx}' y='{cy}' width='{W - pad - cx}' height='{H - pad - cy}' fill='var(--warn)' opacity='.07'/>"
          f"<rect x='{pad}' y='{cy}' width='{cx - pad}' height='{H - pad - cy}' fill='var(--down)' opacity='.07'/>"
          f"<line x1='{pad}' x2='{W - pad}' y1='{cy}' y2='{cy}' stroke='var(--line)'/>"
          f"<line x1='{cx}' x2='{cx}' y1='{pad}' y2='{H - pad}' stroke='var(--line)'/>"
          f"<text x='{W - pad - 4}' y='{pad + 14}' text-anchor='end' font-size='12' fill='var(--up)'>领先 · 强且在加强</text>"
          f"<text x='{W - pad - 4}' y='{H - pad - 6}' text-anchor='end' font-size='12' fill='var(--warn)'>转弱 · 强但在减速</text>"
          f"<text x='{pad + 4}' y='{H - pad - 6}' font-size='12' fill='var(--down)'>落后 · 弱且在走弱</text>"
          f"<text x='{pad + 4}' y='{pad + 14}' font-size='12' fill='var(--accent)'>改善 · 弱但在变强</text>"
          f"<text x='{W - pad}' y='{H - 8}' text-anchor='end' font-size='11' fill='var(--muted)'>强于大盘 →</text>"
          f"<text x='{pad}' y='{H - 8}' font-size='11' fill='var(--muted)'>← 弱于大盘</text>"
          f"<text x='{pad}' y='{pad - 10}' font-size='11' fill='var(--muted)'>↑ 动量增强</text>")
    marks, placed = [], []
    for r in rows:
        tail = r.get("tail") or []
        if not tail:
            continue
        q = r.get("quadrant")
        coords = [xy(p[0], p[1]) for p in tail]
        color = QUAD_COLORS.get(q, "var(--muted)")
        n = len(coords)
        older = "".join(
            f"<line x1='{a[0]:.1f}' y1='{a[1]:.1f}' x2='{b[0]:.1f}' y2='{b[1]:.1f}' stroke='{color}' stroke-width='1.5' "
            f"opacity='{0.15 + 0.6 * i / max(n - 2, 1):.2f}'/>" for i, (a, b) in enumerate(zip(coords[:-2], coords[1:-1])))
        older += "".join(f"<circle cx='{px:.1f}' cy='{py:.1f}' r='2' fill='{color}' opacity='{0.2 + 0.5 * i / max(n - 1, 1):.2f}'/>"
                         for i, (px, py) in enumerate(coords[:-1]))
        x, y = coords[-1]
        if n >= 2:
            head = (f"<line x1='{coords[-2][0]:.1f}' y1='{coords[-2][1]:.1f}' x2='{x:.1f}' y2='{y:.1f}' stroke='{color}' "
                    f"stroke-width='2.2' marker-end='url(#rrg-{q})'/>")
        else:
            head = f"<circle cx='{x:.1f}' cy='{y:.1f}' r='4' fill='{color}'/>"
        read = rotation_read(r)
        tip = (f"{r.get('name', r['key'])} {r['key']} · {read['text'] if read else QUADRANTS.get(q, q)}"
               f"（比 {tail[-1][0]:.1f}，动量 {tail[-1][1]:.1f}）")
        key = escape(r["key"])
        lx, ly = x + 7, y - 5
        while any(abs(lx - px) < 34 and abs(ly - py) < 12 for px, py in placed):   # crude label de-collision
            ly += 12
        placed.append((lx, ly))
        marks.append(f"<g class='s' data-k='{key}'><title>{escape(tip)}</title><g class='tail'>{older}</g>{head}"
                     f"<a href='#{key}'><text x='{lx:.1f}' y='{ly:.1f}' font-size='11' fill='var(--ink)'>{key}</text></a></g>")
    return f"<svg id='rrg' class='rrg' viewBox='0 0 {W} {H}' width='100%'>{defs}{bg}{''.join(marks)}</svg>"


RRG_GUIDE = """<details class='small' style='margin-top:8px'><summary><b>怎么读这张图</b></summary>
<ol class='tight'>
<li><b>左右看强弱</b>：横轴是相对 SPY 的强度与自身 50 日平均之比，中线右侧 = 近期跑赢大盘，左侧 = 跑输。</li>
<li><b>上下看加减速</b>：纵轴是这种相对强度比 10 个交易日前高还是低，上方 = 在变强，下方 = 在变弱。</li>
<li><b>四个象限</b>：领先（右上）强且在加强；转弱（右下）还强但在减速；落后（左下）弱且在走弱；改善（左上）弱但开始变强。</li>
<li><b>看箭头方向比看位置更重要</b>：板块通常顺时针走 改善 → 领先 → 转弱 → 落后 → 改善。箭头是最近一周的移动，指向它下一步可能进入的象限。</li>
<li><b>离中心越远趋势越明确</b>；贴近中心时象限切换意义不大。</li>
<li><b>用法</b>：规则信号出现在「领先」或「改善→领先」的板块里，有板块顺风；出现在「落后」里多半是逆流。</li>
</ol>
<p class='muted'>悬停或点选板块可看 8 周完整轨迹（越淡越早）。这是 JdK RRG 的开放近似，原版公式未公开。</p></details>"""

RRG_SCRIPT = """<script>(()=>{const svg=document.getElementById('rrg');if(!svg)return;
const sync=()=>svg.classList.toggle('pick',!!svg.querySelector('.s.on'));
document.querySelectorAll('[data-rrg]').forEach(b=>b.addEventListener('click',()=>{const k=b.dataset.rrg;
if(k==='*'){svg.classList.toggle('all');b.classList.toggle('on');return}
if(k==='-'){svg.querySelectorAll('.s.on').forEach(g=>g.classList.remove('on'));
document.querySelectorAll('[data-rrg].on').forEach(x=>{if(x.dataset.rrg!=='*')x.classList.remove('on')});sync();return}
const g=svg.querySelector(`.s[data-k="${k}"]`);if(!g)return;g.classList.toggle('on');b.classList.toggle('on');sync()}))})()</script>"""


def rrg_panel(rows: list[dict]) -> str:
    """RRG chart with selection chips, per-series readings and a reading guide."""
    reads = rotation_reads(rows)
    if not reads:
        return rrg_svg(rows)
    chips = "".join(f"<button type='button' class='chip rrgbtn' data-rrg='{escape(x['key'])}'>{escape(x['name'])}</button>"
                    for x in reads)
    controls = (f"<div class='rrgbar'><button type='button' class='chip rrgbtn' data-rrg='*'>显示全部轨迹</button>"
                f"<button type='button' class='chip rrgbtn' data-rrg='-'>清除选择</button><span class='muted small'>点选只看：</span>{chips}</div>")
    items = "".join(
        f"<li><span class='chip' style='color:{QUAD_COLORS[x['quadrant']]}'>{QUADRANTS[x['quadrant']]}</span>"
        f"<a href='#{escape(x['key'])}'>{escape(x['name'])}</a> <span class='muted'>{escape(x['key'])}</span> "
        f"{'<b>' if x['changed'] else ''}{escape(x['detail'])}{'</b>' if x['changed'] else ''}</li>" for x in reads)
    return (f"{rrg_svg(rows)}{controls}<ul class='rrgread small'>{items}</ul>{RRG_GUIDE}{RRG_SCRIPT}")


def heat_cell(value, rank: float | None, text: str) -> str:
    """Table cell shaded by its rank among sectors (green high, red low)."""
    if value is None or rank is None:
        return "<td class='muted'>—</td>"
    alpha = abs(rank - 0.5) * 0.6
    rgb = "46,160,90" if rank >= 0.5 else "200,57,59"
    return f"<td style='background:rgba({rgb},{alpha:.2f})'>{text}</td>"


def premarket_card(rec: dict | None, current: bool) -> str:
    """Latest pre-market brief: open when it is for today's session, folded otherwise."""
    if not rec:
        return ""
    b, p = rec["brief"], rec["pack"]
    source = f"{rec.get('model')} · 校验通过" if rec.get("source") == "llm" else "未通过校验，要点为规则整理"
    points = "".join(f"<li>{escape(x['text'])}</li>" for x in b["points"])
    econ = "".join(f"<tr><td>{escape(e['美东时间'])}</td><td>{escape(e['北京时间'][-5:])}</td><td>{escape(e['重要性'])}</td>"
                   f"<td>{escape(e['事件'])}</td><td>{escape(e.get('预期') or '—')}</td><td>{escape(e.get('前值') or '—')}</td></tr>"
                   for e in p["economic_today"])
    econ_html = (f"<table class='small'><thead><tr><th>美东</th><th>北京</th><th>重要性</th><th>今日数据</th><th>预期</th><th>前值</th></tr></thead>"
                 f"<tbody>{econ}</tbody></table>") if econ else "<p class='small muted'>今天没有重要经济数据。</p>"

    def tl(t: str) -> str:
        return f"<a href='/ticker/{escape(t)}'>{escape(t)}</a>"

    earn = []
    for e in p["earnings_reported"]:
        s = f"{tl(e['代码'])} {escape(e['时间'])} EPS {escape(e['EPS 实际'])} vs {escape(e['EPS 预期'])}"
        s += f"（{escape(e['EPS 超预期幅度'])}）" if e.get("EPS 超预期幅度") else ""
        s += f"，盘前 {escape(e['盘前较前收'])}" if e.get("盘前较前收") else ""
        earn.append(s)
    lines = [f"<b>已公布</b>：{'；'.join(earn)}"] if earn else []
    if p["earnings_today"]:
        lines.append("<b>今日待公布</b>：" + "、".join(f"{tl(e['代码'])}（{escape(e['时间'])}）" for e in p["earnings_today"]))
    if p["earnings_week"]:
        lines.append("<b>本周后续</b>：" + "、".join(f"{tl(e['代码'])} {escape(e['时间'])}" for e in p["earnings_week"]))
    held = "".join(f"<li>{tl(o['代码'])} {escape(o.get('盘前价', ''))} {escape(o['位置判断'])}</li>" for o in p.get("positions", []))
    opps = "".join(f"<li>{tl(o['代码'])}（{escape(o['状态'])}）{escape(o.get('盘前价', ''))} {escape(o['位置判断'])}</li>"
                   for o in p["opportunities"])
    notes = "".join(f"<li>{tl(n['ticker'])}：{escape(n['note'])}</li>" for n in b["tickers"])
    moves = "、".join(f"{tl(m['代码'])} {escape(m['盘前较前收'])}" for m in p["watchlist_moves"])
    inner = (f"<div class='headline'>{escape(b['headline'])}</div>"
             f"<p class='small'><b>隔夜</b>：{escape(overnight_line(p))}<br>{escape(b['overnight'])}</p><ol>{points}</ol>{econ_html}"
             + (f"<p class='small'>{'<br>'.join(lines)}</p>" if lines else "")
             + (f"<div class='small'><b>你的持仓</b><ul class='tight'>{held}</ul></div>" if held else "")
             + (f"<div class='small'><b>活跃机会盘前位置</b><ul class='tight'>{opps}</ul></div>" if opps else "")
             + (f"<p class='small'><b>自选股盘前异动</b>：{moves}</p>" if moves else "")
             + (f"<div class='small'><b>个股关注</b><ul class='tight'>{notes}</ul></div>" if notes else "")
             + f"<p class='small'><b>风险</b>：{escape('；'.join(b['risks']))}<br><span class='muted'>盘前价格不代表开盘价；非投资建议。</span></p>")
    title = f"盘前简报 · {escape(p['session'])} {escape(p['weekday'])} <span class='small muted'>{escape(source)}</span>"
    if current:
        return f"<div class='card analysis' style='margin-bottom:12px'><h3>{title}</h3>{inner}</div>"
    return f"<details class='card analysis' style='margin-bottom:12px'><summary>{title}（上一份）</summary>{inner}</details>"


def next_exit(t: dict, m: dict) -> str:
    """The take-profit stage an open trade is at: price and the shares to sell there."""
    from scout import levels

    pl = t.get("plan") or {}
    if "target1" not in pl:
        return f"{t['target']:.2f}" if t.get("target") else "—"
    for st in levels.exit_plan(pl, t["entry_price"], t["shares"], m["remaining"], t.get("stop")):
        if st["state"] == "due":
            return f"<span class='warn'>{escape(levels.due_advice(st, m.get('price'), t.get('stop')))}</span>"
        if st["state"] == "pending":
            return (f"{st['price']:.2f}（目标 1，到达后改用吊灯止损，不卖）" if st["stage"] == "目标 1"
                    else f"吊灯止损，{m['remaining']:g} 股")
    return "—"


def trades_table(rows: list[dict], closable: bool = True) -> str:
    """Open or closed trades: realized + open P&L, the system stop and the next take-profit step."""
    if not rows:
        return "<p class='muted small'>没有记录。</p>"
    is_open = not rows[0].get("closed_on")
    head = ["代码", "开仓", "股数", "入场", "现价" if is_open else "平均卖出", "盈亏", "R"]
    head += ["止损", "距止损", "止盈", "备注"] if is_open else ["备注"]
    body = []
    for t in rows:
        m = portfolio.metrics(t)
        pnl = (f"<span class='{'up' if m['pnl'] >= 0 else 'down'}'>{m['pnl']:+,.0f}（{signed(m['pnl_pct'])}）</span>"
               if "pnl" in m else "<span class='muted'>无行情</span>")
        if is_open and m.get("realized"):
            pnl += f"<div class='small muted'>已实现 {m['realized']:+,.0f}</div>"
        price = f"{m['price']:.2f}" if m.get("price") is not None else "—"
        if t.get("closed_on"):
            price += f"<div class='small muted'>{escape(str(t['closed_on']))}</div>"
        elif t.get("last_date"):
            price += f"<div class='small muted'>收盘 {escape(str(t['last_date']))}</div>"
        qty = f"{m['remaining']:g} / {t['shares']:g}" if is_open and m["sold"] else f"{t['shares']:g}"
        cells = [f"<a href='/ticker/{escape(t['ticker'])}'>{escape(t['ticker'])}</a>", escape(str(t["opened_on"])),
                 qty, f"{t['entry_price']:.2f}", price, pnl, f"{m['r']:+.1f}R" if "r" in m else "—"]
        if is_open:
            cells += [f"{t['stop']:.2f}" if t.get("stop") else "—", signed(m["to_stop"]) if "to_stop" in m else "—",
                      next_exit(t, m)]
        cells.append(escape(t.get("note") or ""))
        if closable and is_open:
            cells.append(
                f"<form method='post' action='/trades/{t['id']}/sell' style='display:flex;gap:4px;flex-wrap:wrap'>"
                f"<input name='shares' value='{m['remaining']:g}' size='5' title='卖出股数（默认全部剩余）' required>"
                f"<input name='price' placeholder='卖出价' size='7' required><input name='sold_on' type='date' value='{date.today()}' required>"
                f"<button>卖出</button></form>")
        elif closable:
            cells.append(f"<form method='post' action='/trades/{t['id']}/delete' onsubmit=\"return confirm('删除这条记录？')\">"
                         f"<button style='background:var(--muted)'>删除</button></form>")
        body.append(cells)
    if closable:
        head = head + ["操作"]
    return table(head, body)


CAUSE_SOURCE = {"llm": "AI 归因 · 已校验", "fallback": "规则判断"}


def events_card(ticker: str, evs: list[dict], causes: dict[str, dict], rates: dict, above_200: bool | None) -> str:
    """Abnormal days of the last 60 sessions with their researched cause and what usually followed."""
    from scout.events import base_rate_for
    from scout.insight import material

    fmt = knowledge.fmt
    if not evs:
        return ("<div class='card'><h3>异动</h3><p class='small muted'>近 60 个交易日没有统计上的异常波动"
                "（个股自身 ≥ 3σ、涨跌 ≥ 8% 或 3 倍放量）。</p></div>")
    items = []
    pending = False
    for ev in evs:
        c = causes.get(ev["date"])
        head = (f"<b>{escape(ev['date'])}</b> <span class='{'down' if ev['ret'] < 0 else 'up'}'>{fmt('event_ret', ev['ret'])}</span>"
                f" · 个股自身 {fmt('event_idio', ev['idio'])}（{fmt('event_sigma', ev.get('sigma_mult'))}）· {escape(ev['timing'])}"
                f" · 量比 {fmt('event_vol_ratio', ev.get('vol_ratio'))} · 板块当日 {fmt('event_sector_ret', ev.get('sector_ret'))}"
                f" · 之后 {fmt('event_after_ret', ev['after_ret'])}")
        body = ""
        if c:
            web = "web" in (c.get("sources") or [])
            label = "据外部报道（未经核实）" if web else CAUSE_SOURCE.get(c.get("source"), "")
            body = (f"<div><span class='chip'>{escape(c['cause_type'])}</span>"
                    + (f"<span class='chip'>关联：{escape(c['related_company'])}</span>" if c.get("related_company") else "")
                    + f"{escape(c['summary'])}</div><div class='small'>影响：{escape(c['business_impact'])}</div>"
                    f"<div class='small muted'>{escape(label)} · 置信度 {escape(CONFIDENCE_LABELS.get(c['confidence'], c['confidence']))}</div>")
            links = [(s["title"], s["url"]) for s in c.get("web_sources") or []]
            links += [(v["title"], v.get("url")) for k, v in (c.get("links") or {}).items() if k in (c.get("sources") or [])]
            if links:
                body += "<ul class='tight small'>" + "".join(
                    f"<li><a href='{escape(u)}' target='_blank' rel='noopener'>{escape(t[:110])}</a></li>" if u else f"<li>{escape(t)}</li>"
                    for t, u in links[:5]) + "</ul>"
        elif material(ev):
            pending = True
            body = "<div class='small muted'>原因未检索</div>"
        br = base_rate_for(rates, ev, above_200) if material(ev) else None
        if br:
            s = br[1]
            body += (f"<div class='small'>历史上同类异动（{escape(br[0].replace('|', ' · '))}，{s['n']} 次）之后 20 日："
                     f"相对 SPY 中位数 {fmt('event_fwd20_median', s.get('ex20_median'))}，"
                     f"跑赢 {fmt('event_fwd20_up_share', s.get('ex20_up_share'))}</div>")
        items.append(f"<li style='margin-bottom:8px'>{head}{body}</li>")
    button = (f"<form method='post' action='/ticker/{escape(ticker)}/events'><button>检索异动原因</button>"
              f"<span class='small muted'> 新闻、SEC 文件、内部人交易、网络检索，约 30 秒</span></form>") if pending else ""
    return (f"<div class='card'><h3>异动 <span class='small muted'>近 60 个交易日</span></h3><ul class='tight' style='list-style:none;padding:0'>"
            f"{''.join(items)}</ul>{button}<p class='small muted'>异动 = 个股自身涨跌 ≥ 事前 3σ，或涨跌 ≥ 8%，或 3 倍放量。"
            f"同类异动统计含幸存者偏差；跑赢比例接近 50% 说明没有方向性。</p></div>")


FUNDAMENTAL_ROWS = (
    ("估值", ("market_cap", "pe_ttm", "ps_ttm", "pb_ratio")),
    ("增长", ("rev_growth_ttm", "rev_growth_q", "eps_growth_ttm")),
    ("盈利", ("gross_margin", "op_margin", "net_margin", "roe_ttm")),
    ("资产负债", ("current_ratio", "debt_equity")),
    ("预期", ("analyst_buy_share", "analyst_buy_change", "analyst_count", "eps_beat_count", "eps_surprise_last")),
    ("内部人（90 日）", ("insider_net_value_90d", "insider_buyers_90d", "insider_sellers_90d")),
)


def fundamentals_card(f: dict) -> str:
    if not f:
        return ""
    rows = []
    for group, keys in FUNDAMENTAL_ROWS:
        cells = [f"<span title='{escape(knowledge.features()[k].get('definition', ''))}'>{escape(knowledge.zh(k))}</span> "
                 f"<b>{escape(knowledge.fmt(k, f[k]))}</b>" for k in keys if f.get(k) is not None]
        if cells:
            rows.append(f"<tr><td class='muted'>{escape(group)}</td><td>{' · '.join(cells)}</td></tr>")
    notes = [r for r in (knowledge.reading(k, f.get(k)) for k in ("pe_ttm", "rev_growth_ttm", "op_margin", "analyst_buy_change"))
             if r]
    return (f"<div class='card'><h3>基本面与预期 <span class='small muted'>Finnhub 免费数据 · 按日缓存</span></h3>"
            f"<table class='small'><tbody>{''.join(rows)}</tbody></table>"
            + (f"<p class='small'>{escape('；'.join(notes))}</p>" if notes else "")
            + "<p class='small muted'>内部人卖出多为预先计划（10b5-1），集中买入更有信息量。</p></div>")


RESEARCH_ROWS = (
    ("基本面趋势", ("fin_period", "fin_rev_yoy", "fin_rev_accel", "fin_gross_margin", "fin_gross_margin_chg", "fin_op_margin",
                "fin_op_margin_chg", "fin_fcf_margin", "fin_fcf_ttm", "fin_net_cash", "fin_share_change")),
    ("估值", ("market_cap", "val_pe_ttm", "val_fwd_pe", "val_ps_ttm", "val_pfcf", "val_ev_ebitda", "val_ps_pctile",
            "val_ev_sales_vs_sector", "val_fwd_pe_vs_sector")),
    ("预期与评级", ("est_eps_rev_30d", "est_eps_rev_90d", "est_up_30d", "est_down_30d", "est_target_upside",
               "est_target_dispersion", "est_upgrades_30d", "est_downgrades_30d", "est_pt_raises_30d", "est_pt_cuts_30d",
               "analyst_buy_share", "eps_beat_count")),
    ("财报", ("earn_days_to_next", "earn_eps_surprise", "earn_rev_surprise", "earn_last_move", "earn_avg_abs_move",
            "earn_implied_vs_hist")),
    ("持仓与资金", ("own_inst_pct", "own_top_holder_change", "own_short_float", "own_short_days", "own_short_change",
               "insider_net_value_90d", "insider_buyers_90d", "insider_sellers_90d")),
    ("期权", ("opt_iv_30", "opt_iv_rv_ratio", "opt_term_slope", "opt_move_front", "opt_put_skew", "opt_pc_volume",
            "opt_unusual_count", "opt_iv_rank", "opt_iv_history_days")),
    ("宏观敏感度", ("macro_rate_10bp", "macro_rate_t", "macro_usd_1pct", "macro_usd_t", "macro_oil_10pct", "macro_oil_t")),
)


def research_card(rs: dict, values: dict) -> str:
    """Point-in-time research data behind the new lenses, with readings and freshness."""
    spec = knowledge.features()
    rows = []
    for group, keys in RESEARCH_ROWS:
        cells = []
        for k in keys:
            if values.get(k) is None:
                continue
            reading = knowledge.reading(k, values[k])
            cells.append(f"<span title='{escape(spec[k].get('definition', ''))}'>{escape(knowledge.zh(k))}</span> "
                         f"<b>{escape(knowledge.fmt(k, values[k]))}</b>"
                         + (f" <span class='muted'>（{escape(reading)}）</span>" if reading else ""))
        if cells:
            rows.append(f"<tr><td class='muted' style='white-space:nowrap'>{escape(group)}</td><td>{'<br>'.join(cells)}</td></tr>")
    if not rows:
        return ""
    c = rs.get("context") or {}
    fresh = "；".join(f"{k} {v['日期']}" + ("（已过期）" if v["已过期"] else "") for k, v in (c.get("freshness") or {}).items())
    quarters = ""
    if c.get("quarters"):
        unit = rs.get("unit") or "USD"
        def money(v):
            return "—" if v is None else (knowledge.fmt("fin_fcf_ttm", v) if unit == "USD" else f"{v / 1e9:.1f}B {unit}")
        quarters = ("<details><summary class='small'>季度财务（SEC 申报）</summary>" + table(
            ["期末", "营收", "同比", "毛利率", "营业利润率", "自由现金流"],
            [[escape(q["period"]), money(q["revenue"]), signed(q["yoy"]),
              knowledge.fmt("fin_gross_margin", q["gross_margin"]), knowledge.fmt("fin_op_margin", q["op_margin"]),
              money(q["fcf"])] for q in reversed(c["quarters"])]) + "</details>")
    actions = ""
    if c.get("analyst_actions"):
        actions = ("<details><summary class='small'>最近评级变动</summary><ul class='tight small'>" + "".join(
            f"<li>{escape(a['date'])} {escape(a['firm'])}：{escape(a.get('from') or '—')} → {escape(a.get('to') or '—')}"
            + (f"，目标价 {a.get('prior_target') or '—'} → {a['target']}" if a.get("target") else "") + "</li>"
            for a in c["analyst_actions"]) + "</ul></details>")
    return (f"<div class='card'><h3>研究数据 <span class='small muted'>SEC · Yahoo · FRED · Finnhub</span></h3>"
            f"<table class='small'><tbody>{''.join(rows)}</tbody></table>{quarters}{actions}"
            + (f"<p class='small muted'>快照日期：{escape(fresh)}</p>" if fresh else "")
            + "<p class='small muted'>只用当时已申报 / 已发布的数据；鼠标悬停指标名看定义。</p></div>")


def rule_line(o: dict, price: float, today) -> str:
    """One line on the rule signal: what it said, and whether buying now still follows it."""
    from scout.detectors import registry
    from scout.intraday import sessions_since

    det = next((d for d in registry(include_candidates=True) if d.name == o["primary_detector"]), None)
    label = (o.get("evidence") or {}).get("setup", {}).get("label") or (det.label if det else o["primary_detector"])
    head = f"规则信号：{label}（{o['asof']}），原入场区间 {o['entry_lo']:.2f}–{o['entry_hi']:.2f}，止损 {o['stop']:.2f}，目标 {o['target1']:.2f}。"
    where = ("现价在原入场区间内，可以按规则计划执行。" if o["entry_lo"] <= price <= o["entry_hi"]
             else f"现价高于原区间上沿 {(price / o['entry_hi'] - 1) * 100:.1f}%，此时买入已不属于规则入场，按上面的价位参考评估。"
             if price > o["entry_hi"] else
             f"现价低于原区间下沿 {(price / o['entry_lo'] - 1) * 100:.1f}%" + ("，已跌破规则止损，信号的前提可能已失效。" if price <= o["stop"] else "。"))
    if o["status"] == "new" and det:
        left = det.trigger_window - sessions_since(o["asof"], today)
        where += f" 规则入场窗口还剩 {max(left, 0)} 个交易日。"
    if not (det and det.live):
        where += " 该规则已降为候选（回放跑输持有）。"
    return head + where


STATE_TONE = {"in_zone": "up", "waiting": "muted", "below": "warn", "failed": "down", "extended": "warn"}


def price_panel(pm: dict, live: dict | None, settings: dict | None, opp: dict | None = None,
                today=None, tracking_html: str = "", ticker: str = "") -> str:
    """Next to the chart: the stored price map (levels fixed at the last close) with the
    live price only moving the status; rule signals as one line, their bookkeeping folded."""
    from datetime import date as _date
    from urllib.parse import quote

    from scout import levels

    if not pm:
        return ""
    close = pm["close"]
    price = (live or {}).get("last") or close
    st = levels.status(pm, price)
    out = [f"<h3>价位参考</h3><div class='small'>现价 {live_line(live, close)}</div>"]

    if pm.get("pivot"):
        piv = pm["pivot"]
        out.append(f"<div class='small' style='margin-top:6px'><b>多空分界</b> {piv['price']:.2f}（{escape(piv['label'])}，"
                   f"距现价 {signed(piv['price'] / price - 1)}）"
                   + (f" <span class='warn'>{escape(st['pivot_text'])}</span>" if st.get("pivot_text") else "") + "</div>"
                   f"<div class='small'>首选：{escape(pm.get('preferred', ''))}</div>"
                   f"<div class='small muted'>备选：{escape(pm.get('alternative', ''))}</div>")

    lv = [(p["label"], p["price"], "--up") for p in reversed(pm.get("resistances") or [])]
    lv += [(p["label"], p["price"], "--down") for p in pm.get("supports") or []]
    if lv:
        above = [x for x in lv if x[1] > price][-4:]
        below = [x for x in lv if x[1] <= price][:4]
        row = lambda n, v, c: f"<tr><td style='color:var({c})'>{escape(n)}</td><td>{v:.2f}</td><td>{signed(v / price - 1)}</td></tr>"
        rows = ("".join(row(*x) for x in above) + f"<tr><td><b>现价</b></td><td><b>{price:.2f}</b></td><td></td></tr>"
                + "".join(row(*x) for x in below))
        out.append(f"<div style='margin-top:6px'><b>关键价位</b></div><table class='small'><tbody>{rows}</tbody></table>")

    checks = []
    if pm.get("expected_move"):
        em = pm["expected_move"]
        checks.append(f"期权隐含到 {escape(str(em.get('expiry') or ''))} 的波动 ±{em['move'] * 100:.1f}%："
                      f"{em['low']:.2f}–{em['high']:.2f}")
    if pm.get("analyst_targets"):
        at = pm["analyst_targets"]
        rng = "–".join(f"{at[k]:.2f}" for k in ("low", "high") if at.get(k))
        checks.append(f"分析师目标价 {rng}，中位 {at.get('median') or at['mean']:.2f}"
                      f"（较现价 {signed((at.get('median') or at['mean']) / price - 1)}）")
    if checks:
        out.append("<div class='small muted' style='margin-top:4px'>" + "<br>".join(checks) + "</div>")

    entries = pm.get("entries") or []
    for en, es in zip(entries, st["entries"]):
        mid = (en["zone_lo"] + en["zone_hi"]) / 2
        sz = portfolio.size_text(portfolio.size(mid, en["stop"], settings)) if settings is not None else None
        link = (f" <a class='small' href='/trades?t={quote(ticker)}&plan={quote(en['name'])}'>按此计划记录买入</a>"
                if ticker and es["state"] == "in_zone" else "")
        out.append(
            f"<div style='margin-top:10px'><b>{escape(en['name'])}</b> {en['buy_point']:.2f} "
            f"<span class='small {STATE_TONE.get(es['state'], '')}'>{escape(es['text'])}</span>{link}"
            f"<table class='small'><tbody>"
            f"<tr><td>买入区间</td><td>{en['zone_lo']:.2f}–{en['zone_hi']:.2f}</td><td class='muted'>高于上沿不追</td></tr>"
            f"<tr><td style='color:var(--down)'>止损</td><td>{en['stop']:.2f}</td><td class='muted'>{en['stop_pct'] * 100:.1f}% · {escape(en['stop_basis'])}</td></tr>"
            f"<tr><td style='color:var(--up)'>目标 1</td><td>{en['target1']:.2f}</td><td class='muted'>{escape(en['target1_basis'])} · R:R {en['rr']:.1f} · 到达后不卖，止损改用吊灯止损</td></tr>"
            f"<tr><td style='color:var(--up)'>上方参考</td><td>{en['target2']:.2f}</td><td class='muted'>{escape(en['target2_basis'])} · 只作参考，不在这里卖</td></tr>"
            f"<tr><td>时间止损</td><td>{en['time_stop_days']} 个交易日</td><td class='muted'>入场后迟迟不到目标 1 就离场</td></tr>"
            f"</tbody></table>"
            + (f"<div class='small warn'>{escape(en['warning'])}</div>" if en.get("warning") else "")
            + (f"<div class='small'><b>仓位</b>：{escape(sz)}</div>" if sz else "")
            + f"<div class='small muted'>{escape(en['note'])}</div></div>")
    if pm.get("note"):
        out.append(f"<p class='small warn'>{escape(pm['note'])}</p>")
    if opp:
        out.append(f"<p class='small' style='margin-top:8px'>{escape(rule_line(opp, price, today or _date.today()))}</p>")
        if tracking_html:
            out.append(f"<details><summary class='small muted'>系统跟踪（规则原计划与假设成交，用于给规则记分，不是你的交易）</summary>{tracking_html}</details>")
    out.append(f"<p class='small muted'>价位按 {escape(str(pm.get('asof', '')))} 收盘计算并保存，盘中不变，实时价格只更新状态。"
               f"支撑 / 阻力取近一年的主要波段高低点；买入区间为买点到其上方 min(0.5 ATR, 5%)（ATR {pm['atr']:.2f}）；"
               f"止损放在判断失效的结构位下方，至少 1 个 ATR、最多 10%。记录买入后，止损和止盈由系统按你的成交管理并每天更新。这些是价位参考，不是经过回放检验的规则信号。</p>")
    return f"<div class='card plan'>{''.join(out)}</div>"




# ── knowledge base ──────────────────────────────────────────────────────

KB_SECTIONS = (("confirmed", "已确认"), ("forward", "前向验证中"), ("backtested", "回测通过，待前向验证"), ("hypothesis", "假设（已预登记）"),
               ("rejected", "否定：经检验无效"), ("retired", "已失效"))
KB_ACTION_ZH = {"backtested": "标记回测通过", "forward": "进入前向验证", "confirmed": "确认", "rejected": "否定",
                "retired": "标记失效", "hypothesis": "重新检验"}
KB_TOPIC_ZH = {"entry": "入场", "exit": "出场", "evaluation": "评价方法"}


def _kb_scope(en: dict) -> str:
    from scout.detectors import registry
    from scout.insight import LENS_TITLES

    rules = {d.name: d.label for d in registry(include_candidates=True)}
    ap, ctx = en.get("applies") or {}, en.get("context") or {}
    parts = [rules.get(d, d) for d in ap.get("detectors") or []] + [LENS_TITLES.get(x, x) for x in ap.get("lenses") or []]
    parts += [KB_TOPIC_ZH.get(t, t) for t in ap.get("topics") or []]
    names = {"kind": "标的", "regime": "市场状态", "horizon_days": "持有期（交易日）", "stock": "个股", "etf": "ETF",
             "uptrend": "上升", "mixed": "震荡", "downtrend": "下降"}
    cond = "；".join(f"{names.get(k, k)}：{'、'.join(names.get(str(x), str(x)) for x in v) if isinstance(v, list) else v}"
                    for k, v in ctx.items())
    return "、".join(dict.fromkeys(parts)) + (f"<span class='muted'>（条件 {e(cond)}）</span>" if cond else "")


def _kb_spec(spec: dict) -> str:
    rows = [("族", spec.get("family")), ("规则", spec.get("detector")), ("主对照", {"select": "选股对照（同日随机股）", "timing": "时机对照（同股随机日）"}.get(spec.get("primary_control"), spec.get("primary_control"))),
            ("预期失效", spec.get("expected_failure")), ("登记日期", spec.get("registered_on")), ("说明", spec.get("registration_note"))]
    return ("<details class='small' style='margin-top:6px'><summary>预登记规格</summary><ul class='tight'>"
            + "".join(f"<li><b>{k}</b>：{e(v)}</li>" for k, v in rows if v) + "</ul></details>")


def _kb_gate(run: dict) -> str:
    from scout import gate

    rep, cr = run.get("backtest") or {}, run.get("critique") or {}
    crit = "".join(f"<li>{'✓' if c['ok'] else '✗'} {e(c['name'])}：{e(c['value'])}（要求 {e(c['bar'])}）</li>" for c in rep.get("criteria") or [])
    checks = ""
    if cr.get("source") == "llm":
        checks = "".join(f"<li>{ {'通过': '✓', '有疑问': '？', '不通过': '✗'}[i['verdict']]} {e(gate.CHECKS[i['check']].split('：')[0])}：{e(i['reason'])}</li>"
                         for i in cr["items"]) + f"<li><b>反方论证</b>：{e(cr.get('counter_argument'))}</li>"
    else:
        checks = f"<li>批判关未完成：{e('；'.join(cr.get('issues') or []))}</li>"
    return (f"<details class='small' style='margin-top:6px'><summary>检验关 {e(run['ran_at'].date())}：{'通过' if run['passed'] else '未通过'}</summary>"
            f"<div><b>回测</b></div><ul class='tight'>{crit}</ul><div><b>批判</b></div><ul class='tight'>{checks}</ul></details>")


def _kb_forward(rep: dict) -> str:
    def value(c: dict) -> str:
        if c["value"] is None:
            return "—"
        return signed(c["value"], 2) if c["name"].startswith("相对") else e(c["value"])

    crit = "".join(f"<li>{'✓' if c['ok'] else '…'} {e(c['name'])}：{value(c)}（要求 {e(c['bar'])}）</li>" for c in rep["criteria"])
    head = (f"前向验证：{rep['started'] or '尚未开始'} 起，{rep['signals']} 个信号（{rep['open_signals']} 个未结束），"
            f"{rep['n']} 个已完成" + ("，<b>已达标，可以确认</b>" if rep["ready"] else ""))
    return f"<div class='small' style='margin-top:8px'><b>{head}</b><ul class='tight'>{crit}</ul></div>"


def knowledge_card(en: dict, versions: list[dict], today: date, run: dict | None = None, fwd: dict | None = None) -> str:
    from scout import learned

    overdue = en.get("recheck_on") and en["status"] in (*learned.OPEN, "confirmed") and en["recheck_on"] <= today
    tone = "up" if en["polarity"] == "positive" else "down"
    evidence = "".join(f"<li><span class='chip'>{e(ev['source'])} {e(ev['date'])}</span>{e(ev['summary'])}</li>"
                       for ev in en.get("evidence") or [])
    acts = ""
    nxt = learned.allowed_next(en)
    if nxt:
        buttons = "".join(f"<button name='status' value='{s}' style='margin-right:6px"
                          f"{';background:var(--down)' if s in ('rejected', 'retired') else ''}'>{KB_ACTION_ZH[s]}</button>"
                          for s in nxt)
        acts = (f"<form method='post' action='/knowledge/{e(en['key'])}/status' class='small' style='margin-top:8px'>"
                f"<input name='note' placeholder='理由（必填）' required style='width:60%;margin-right:6px'>{buttons}</form>")
    hist = "".join(f"<tr><td>v{v['version']}</td><td>{e(v['created_at'].date() if v.get('created_at') else '')}</td>"
                   f"<td>{e(learned.STATUS_ZH.get(v['status'], v['status']))}</td><td>{e({'seed': '首批', 'system': '系统', 'user': '你', 'import': 'B0 导入'}.get(v['changed_by'], v['changed_by']))}</td>"
                   f"<td>{e(v.get('note') or '')}</td></tr>" for v in versions)
    return (f"<div class='card' id='{e(en['key'])}'><h3><span class='{tone}'>{'✓' if en['polarity'] == 'positive' else '✗'}</span> {e(en['title'])}</h3>"
            f"<div class='small muted'><span class='chip'>{e(learned.KIND_ZH.get(en['kind'], en['kind']))}</span>"
            f"{e(learned.STATUS_ZH[en['status']])} · 关于：{_kb_scope(en)}</div>"
            f"<p>{e(en['claim'])}</p>"
            + (f"<p class='small'><b>机制</b>：{e(en['mechanism'])}</p>" if en.get("mechanism") else "")
            + f"<div class='small'><b>证据</b><ul class='tight'>{evidence}</ul></div>"
            + (f"<div class='small'><b>失效条件</b>：{e(en['invalidation'])}</div>" if en.get("invalidation") else "")
            + (f"<div class='small {'warn' if overdue else 'muted'}'>复查日期 {e(en['recheck_on'])}{'（已到期）' if overdue else ''}</div>"
               if en.get("recheck_on") else "")
            + (_kb_forward(fwd) if fwd else "")
            + acts
            + (_kb_spec(en["spec"]) if en.get("spec") else "")
            + (_kb_gate(run) if run else "")
            + f"<details class='small' style='margin-top:6px'><summary>版本记录（{len(versions)}）</summary>"
              f"<table><thead><tr><th>版本</th><th>日期</th><th>状态</th><th>修改者</th><th>理由</th></tr></thead><tbody>{hist}</tbody></table></details>"
            + "</div>")


def knowledge_page(entries: list[dict], versions: dict[str, list[dict]], today: date, runs: dict[str, dict] | None = None,
                   fwd: dict[str, dict] | None = None) -> str:
    from scout import learned

    counts = {s: sum(en["status"] == s for en in entries) for s, _ in KB_SECTIONS}
    chips = " ".join(f"<a class='chip' href='#s-{s}'>{label} {counts[s]}</a>" for s, label in KB_SECTIONS if counts[s])
    due = learned.due(entries, today)
    body = ("<h2>知识库</h2><p class='small muted'>Scout 学到的结论和它们有多可靠。每条知识写明机制、成立条件、证据和复查日期；"
            "要逐关通过：预登记的假设 → 回测（公平对照、前后两半、扣成本、t ≥ 3）→ 前向验证（至少 6 个月且 30 个样本）→ 由你确认。"
            "经检验无效的也保留为否定知识，避免换个说法重复尝试。AI 解读会按股票的情境引用相关条目。</p>"
            f"<p>{chips}</p>"
            + (f"<p class='small warn'>已到复查日期：{'、'.join(e(en['title']) for en in due)}</p>" if due else ""))
    for status, label in KB_SECTIONS:
        group = [en for en in entries if en["status"] == status]
        if group:
            body += (f"<h2 id='s-{status}'>{label}</h2><div class='grid'>"
                     + "".join(knowledge_card(en, versions.get(en["key"], []), today, (runs or {}).get(en["key"]), (fwd or {}).get(en["key"]))
                             for en in group) + "</div>")
    if not entries:
        body += "<p class='muted'>知识库还是空的。</p>"
    return layout("知识库", "knowledge", body)


# ── industry outlook ────────────────────────────────────────────────────

def outlook_page(rec: dict | None, theses: list[dict]) -> str:
    from scout import outlook as ol

    if not rec:
        return layout("行业前景", "outlook", "<p class='muted'>还没有行业前景：每周六的周报任务会生成。</p>")
    drv = rec["drivers"]
    rows = []
    for etf in ol.TARGETS:
        v = drv["etfs"].get(etf)
        if v:
            rows.append([f"<a href='/ticker/{e(etf)}'>{e(etf)}</a> {e(ol.ETF_ZH.get(etf, ''))}"]
                        + [signed(v.get(f"rel_{w}")) for w in ol.WINDOWS])
    styles = [[e(label)] + [signed((drv["styles"].get(str(w)) or drv["styles"].get(w) or {}).get(k)) for w in ol.WINDOWS]
              for k, label in ol.STYLE_ZH.items()]
    b = drv["breadth"]
    o = rec.get("outlook")
    head = ("<h2>市场由谁推动</h2><p class='small muted'>Python 计算，只描述不预测。风格是股票池内的多空组合收益，组合在每个窗口开始时构建；"
            f"数据日期 {e(rec['asof'])}。</p>"
            + (f"<p>{e(o['drivers'])}</p>" if o else "")
            + f"<p class='small'>宽度：站上 50 日线 {b['above_50'] * 100:.0f}%，站上 200 日线 {b['above_200'] * 100:.0f}%，"
              f"距 52 周高点 5% 以内 {b['near_high'] * 100:.0f}%（{b['n']} 只）</p>"
            + "<div class='duo'><div>" + table(["行业 / 主题", "5 日相对 SPY", "20 日", "60 日"], rows) + "</div><div>"
            + table(["风格", "5 日", "20 日", "60 日"], styles) + "</div></div>")
    cards = []
    for t in theses:
        p = t["payload"] or {}
        outs = " · ".join(f"{h} 日 {signed(t.get(f'x{h}'))}" + (" ✓" if t.get(f"hit{h}") else " ✗" if t.get(f"hit{h}") is False else "")
                          for h in (20, 60, 120) if t.get(f"x{h}") is not None) or "未到期"
        cards.append(f"<div class='card'><h3>{e(t['ticker'])} {e(ol.ETF_ZH.get(t['ticker'], ''))} · {e(p.get('stance'))} · "
                     f"{e(p.get('horizon'))} 个交易日</h3><div class='small muted'>{e(t['asof'])} · 方法：{e(t.get('method_title') or t['lens'])}"
                     f"（{e(t.get('method_status') or '')}）· 置信度 {e(p.get('confidence'))}</div>"
                     f"<p><b>{e(p.get('title'))}</b>：{e(p.get('mechanism'))}</p>"
                     f"<div class='small'><b>证伪条件</b>：{e('；'.join(p.get('falsification') or []))}</div>"
                     f"<div class='small'><b>引用</b>：{e('、'.join(p.get('evidence') or []))}</div>"
                     f"<div class='small'>相对 SPY：{outs}</div></div>")
    body = (head + "<h2>行业判断</h2><p class='small muted'>AI 每周最多写 3 条，每条必须引用至少一条非价格证据（收入、资本开支、预期修正、"
            "评级、新闻或宏观），并写明证伪条件。判断记入判断台账，20 / 60 / 120 个交易日后对照 SPY 和等权 S&amp;P 500 打分；"
            "判断方法在知识库里前向验证，满 30 个样本前都是未验证。</p>"
            + (f"<div class='grid'>{''.join(cards)}</div>" if cards else
               f"<p class='muted'>还没有判断。{e((o or {}).get('no_thesis_reason') or '')}</p>"))
    return layout("行业前景", "outlook", body)


# ── the user's B0 ranking ───────────────────────────────────────────────

def b0_rank_panel(rk: dict | None) -> str:
    from scout.universe import SECTOR_ZH

    if not rk:
        return ("<p class='muted'>B0 动量打分未开启或还没有数据：需要在 Railway 设置 <code>SCOUT_B0_SCORE</code>"
                "（B0 的打分参数），之后每天收盘扫描时计算。</p>")
    g = rk["gate"]

    def gate_chip(t: str, v: dict | None) -> str:
        if not v:
            return f"<span class='chip'>{e(t)} 数据不足</span>"
        tone = "var(--up)" if v["above"] else "var(--down)"
        return (f"<span class='chip' style='background:{tone};color:#fff'>{e(t)} {v['close']:.2f} "
                f"{'高于' if v['above'] else '低于'} {g['sma']} 日线 {v['sma']:.2f}（{v['gap'] * 100:+.1f}%）</span>")

    names = "、".join(g["tickers"])
    state = (f"<b class='up'>大盘过滤通过</b>：{e(names)} 都在 {g['sma']} 日线上方，B0 按排名持仓。" if g["risk_on"] else
             f"<b class='down'>大盘过滤未通过</b>：{e(names)} 有在 {g['sma']} 日线下方的，B0 此时不持有股票仓位。")
    horizons = [n for n, _ in rk["weights"]]
    w = " ".join(f"{'+' if x >= 0 else '−'} {abs(x):.2f} × {n} 日" for n, x in rk["weights"]).lstrip("+ ")
    idx = {"sp500": "S&amp;P 500", "ndx": "Nasdaq-100", "sp400": "S&amp;P 400"}
    rows = []
    for r in rk["rows"]:
        top = r["rank"] <= rk["target_holdings"]
        rows.append([f"<b>{r['rank']}</b>" if top else str(r["rank"]), tlink(r["ticker"]), e((r.get("name") or "")[:22]),
                     e(SECTOR_ZH.get(r.get("sector") or "", r.get("sector") or "")), f"{r['score']:.3f}"]
                    + [signed(r["returns"].get(str(n))) for n in horizons]
                    + [f"${r['dollar_volume'] / 1e9:.1f}B" if r["dollar_volume"] >= 1e9 else f"${r['dollar_volume'] / 1e6:.0f}M",
                       " / ".join(idx.get(i, e(i)) for i in r.get("indexes") or [])])
    return (f"<div class='card' style='margin-top:12px'><h3>B0 动量打分（Scout 按 B0 的参数近似计算）</h3>"
            f"<p>{' '.join(gate_chip(t, v) for t, v in g['tickers'].items())}</p><p class='small'>{state}</p>"
            "<ul class='tight small'>"
            f"<li>分数 = {e(w)}的对数收益；{rk['trend_horizon']} 日收益须为正，20 日平均成交额 ≥ ${rk['min_dollar_volume'] / 1e6:.0f}M。</li>"
            f"<li>股票池：{'、'.join(idx.get(i, e(i)) for i in rk['indexes'])} 各取 20 日成交额最大的 {rk['per_index']} 只，近似 B0 按 ETF 权重取的成分股"
            f"（本次 {rk['universe']} 只，{rk['scored']} 只通过趋势过滤）；排名可能和 B0 实际看到的略有不同。</li>"
            f"<li>B0 设计持有前 {rk['target_holdings']} 名，另有自己的仓位、缓冲、止损和冷却规则。"
            "<b>这个列表是排名，不是 B0 的持仓，也不是 Scout 的推荐</b>：B0 的回测收益来自整套组合规则（见知识库里的 B0 条目）。</li>"
            f"</ul><p class='small muted'>数据日期 {e(rk['asof'])}</p></div>"
            + table(["排名", "代码", "名称", "行业", "分数", *[f"{n} 日" for n in horizons], "20 日成交额", "指数"], rows))


# ── rule scoreboard ─────────────────────────────────────────────────────

def _edge_txt(x: dict | None, t: bool = True) -> str:
    if not x or x.get("edge") is None:
        return "—"
    out = signed(x["edge"], 2)
    return out + (f"<span class='small muted'>（t {x['t']:.1f}）</span>" if t and x.get("t") is not None else "")


def scoreboard_page(rows: list[dict], fair_asof, legacy_computed) -> str:
    """One row per rule: the knowledge-base verdict and push state, the fair-benchmark test, by
    market regime, and what happened since (live cards, or the forward record)."""
    from scout import learned

    main, legacy = [], []
    for r in rows:
        d, en, f = r["det"], r["entry"], r["fair"] or {}
        status = learned.STATUS_ZH.get(en["status"], en["status"]) if en else "未检验"
        tone = ("up" if en and en["polarity"] == "positive" and en["status"] in ("backtested", "forward", "confirmed")
                else "down" if en and en["status"] in ("rejected", "retired") else "")
        push = ("推送中" if d.live and not r["held_back"] else "只记录，不推送" if d.live else "候选：不扫描、不推送")
        verdict = (f"<b class='{tone}'>{e(status)}</b><br><span class='small'>{push}</span>"
                   + (f"<br><a class='small' href='/knowledge#{e(en['key'])}'>知识库</a>" if en else ""))
        fair = ("—" if not f.get("n") else
                f"前半 {_edge_txt(f.get('vs_select_first'))}<br>后半 {_edge_txt(f.get('vs_select_second'))}<br>"
                f"全样本 {_edge_txt(f.get('vs_select'))}<br><span class='small muted'>{f['n']} 笔 · 扣费后平均 {pct(f.get('avg_net'), 2, True)}</span>")
        other = ("—" if not f.get("n") else
                 f"同股随机日 {_edge_txt(f.get('vs_timing'))}<br>SPY {_edge_txt(f.get('vs_spy'))}<br>等权股票池 {_edge_txt(f.get('vs_ew'))}")
        regimes = f.get("regimes") or {}
        reg = ("<br>".join(f"{lbl} {_edge_txt((regimes.get(g) or {}).get('vs_select'))} <span class='small muted'>{(regimes.get(g) or {}).get('n', 0)} 笔</span>"
                           for g, lbl in (("uptrend", "上升"), ("mixed", "震荡"), ("downtrend", "下降")))
               if regimes else "<span class='small muted'>—</span>")
        lv, fw = r["live"] or {}, r["forward"]
        if fw:
            after = (f"<b>前向验证</b> {e(fw['started'] or '未开始')} 起<br>{fw['signals']} 个信号，{fw['n']} 个已完成"
                     + (f"<br>相对选股对照 {signed(fw['edge'], 2)}" if fw.get("edge") is not None else "")
                     + f"<br><span class='small muted'>满 6 个月且 30 个样本后由你确认</span>")
        elif lv.get("total"):
            after = (f"实盘卡 {lv['total']} 张（活跃 {lv.get('active', 0)}，结束 {lv.get('closed', 0)}）<br>"
                     f"扣费胜率 {pct(lv.get('win_rate_net'), 0)} · 相对 SPY {signed(lv.get('avg_net_excess'))}<br>"
                     f"<span class='small'>👍 {lv.get('thumbs_up', 0)} 👎 {lv.get('thumbs_down', 0)}</span>")
        else:
            after = "<span class='small muted'>没有实盘记录</span>"
        main.append([f"<b>{e(d.label)}</b><br><span class='small muted'>{e(d.name)} · 持有 {d.horizon_days} 日</span>",
                     verdict, fair, other, reg, after])
        for regime, lbl in (("all", "全部"), ("uptrend", "上升"), ("mixed", "震荡"), ("downtrend", "下降")):
            s = r["legacy"].get(regime) or {}
            if s.get("n"):
                legacy.append([e(d.label), lbl, str(s["n"]), pct(s.get("win_rate_net", s.get("win_rate")), 0), signed(s.get("avg_edge_vs_hold")),
                               pct(s.get("avg_net_excess"), 1, True), pct(s.get("target_rate"), 0), pct(s.get("stop_rate"), 0)])
    body = (
        "<h2>规则记分板</h2>"
        "<p class='small muted'>每条规则的结论来自知识库；检验用 12 年日线回放，信号、成交和出场与规则完全相同。"
        "<b>主对照</b> = 同一天随机选的股票，用同样的价位公式和出场（选股是否有价值），逐个信号配对，t 值按月度均值计算；"
        "通过检验关要求前后两半都为正、全样本 t ≥ 3。股票池是当前指数成分股，有幸存者偏差；对照来自同一池子，偏差在配对差值里基本抵消。"
        f"{'检验于 ' + e(fair_asof) + '。' if fair_asof else ''}</p>"
        + table(["规则", "结论", "公平检验：相对同日随机股", "其他对照", "分市场状态（相对同日随机股）", "实盘 / 前向"], main)
        + "<details class='card' style='margin-top:12px'><summary>旧指标：相对持有（有偏差，仅作参考）</summary>"
          "<p class='small muted'>“相对持有”= 扣费收益 − 同期按该股整段历史的平均涨幅一直持有的收益。它用到了未来数据，又建立在当前成分股（过去的赢家）之上，"
          f"对规则是偏高的门槛，知识库里已标为否定。计算于 {e(legacy_computed)}。</p>"
        + table(["规则", "市场状态", "样本", "扣费胜率", "相对持有", "相对 SPY（扣费）", "达标", "止损"], legacy)
        + "</details>"
        + "<h3>规则说明</h3><ul>" + "".join(f"<li><b>{e(r['det'].label)}</b>：{e(r['det'].description)}</li>" for r in rows) + "</ul>")
    return layout("记分板", "scoreboard", body)
