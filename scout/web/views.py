"""Server-rendered dashboard HTML. Single-user, no build step."""
from __future__ import annotations

import json
from html import escape
from typing import Any, Iterable

from scout.telegram import (CONFIDENCE_LABELS, STATUS_LABELS, base_rate_text, market_split_text, pct,
                            position_text)

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
.chartrow{display:grid;grid-template-columns:minmax(0,2.2fr) minmax(260px,1fr);gap:12px;align-items:start}
@media (max-width:900px){.chartrow{grid-template-columns:1fr}}
.chartbox{position:relative;padding:8px}#chart{height:440px}
#legend{position:absolute;top:10px;left:14px;z-index:3;background:var(--panel);padding:2px 6px;border-radius:4px;opacity:.92}
.plan table td,.plan table th{padding:4px 6px}
"""


def layout(title: str, active: str, body: str, head: str = "") -> str:
    nav = [("/", "today", "今日"), ("/ideas", "ideas", "机会看板"), ("/screens", "screens", "策略筛选"), ("/scoreboard", "scoreboard", "记分板"),
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


def trade_plan(o: dict, close: float | None) -> str:
    """Order-ticket style panel next to the chart for the active (or latest) plan."""
    ev = o.get("evidence") or {}
    ex = ev.get("exit") or {}
    trailing = ex.get("kind") == "trail_low"
    mid = (o["entry_lo"] + o["entry_hi"]) / 2
    risk = mid - o["stop"]
    close = close or mid
    cur = o.get("current_stop") if (o.get("current_stop") or 0) > o["stop"] else None
    t2 = o.get("target2") or o["target1"]
    tgt = "参考目标" if trailing else "目标"
    rows = [(f"{tgt} 2", t2, "--up"), (f"{tgt} 1", o["target1"], "--up"), ("入场上沿", o["entry_hi"], "--accent"),
            ("入场下沿", o["entry_lo"], "--accent"), ("保护性止损", o["stop"], "--down")]
    if cur:
        rows.append(("跟进止损", cur, "--warn"))
    table_rows = "".join(
        f"<tr><td style='color:var({c})'>{escape(label)}</td><td>{price:.2f}</td>"
        f"<td>{signed(price / close - 1)}</td><td>{(price - mid) / risk:+.1f}R</td></tr>"
        for label, price, c in rows
    )
    status = STATUS_LABELS.get(o.get("status"), o.get("status"))
    label = (ev.get("setup") or {}).get("label", o["primary_detector"])
    exit_rule = ex.get("rule") or f"固定：到目标 1 止盈，{o['horizon_days']} 个交易日到期"
    return f"""<div class="card plan"><h3>交易计划 <span class="chip">{escape(label)}</span> <span class="chip">{escape(str(status))}</span></h3>
<div class="small muted">信号 {escape(str(o['asof']))}{' · 入场 ' + escape(str(o['triggered_on'])) if o.get('triggered_on') else ''} · 1R = {risk:.2f}（{risk / mid * 100:.1f}%）· R:R {(o.get('risk_reward') or 0):.1f}</div>
{_ladder_svg([(r[0], r[1], r[2]) for r in rows], close, o['entry_lo'], o['entry_hi'], o['stop'], t2)}
<table><thead><tr><th></th><th>价格</th><th>距现价</th><th>R</th></tr></thead><tbody>{table_rows}</tbody></table>
<p class="small"><b>出场规则</b>：{escape(exit_rule)}</p>
<p class="small muted">入场区间由信号日 ATR 决定，3 个交易日内（突破回踩类 10 日）进入区间才算成交；保护性止损放在结构位下方且至少距入场中点 1 个 ATR。</p></div>"""


def key_levels(f: dict, s: dict) -> str:
    """Panel for tickers without a plan: where the meaningful prices are."""
    close = f.get("close")
    if not close:
        return ""
    items = [("阻力位", s.get("resistance")), ("50 日线", f.get("sma50")), ("200 日线", f.get("sma200")),
             ("20 日线", f.get("sma20")), ("支撑位", s.get("support"))]
    if f.get("dist_52w_high") is not None:
        items.insert(0, ("52 周高点", close / (1 + f["dist_52w_high"])))
    rows = "".join(f"<tr><td>{escape(n)}</td><td>{v:.2f}</td><td>{signed(v / close - 1)}</td></tr>"
                   for n, v in sorted((i for i in items if i[1]), key=lambda i: -i[1]))
    return (f"<div class='card plan'><h3>关键价位</h3><div class='small muted'>当前没有规则给出的交易计划。图上灰色虚线为最近的主要支撑和阻力。</div>"
            f"<table><thead><tr><th></th><th>价格</th><th>距现价 {close:.2f}</th></tr></thead><tbody>{rows}</tbody></table></div>")
