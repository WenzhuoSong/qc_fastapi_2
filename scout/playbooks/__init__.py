"""Playbooks: every way to trade in `docs/playbook_catalog_v1_zh.md` as a function over today's data.

`run(ctx)` applies each playbook and returns, per playbook, its buys / holds / sells with reasons, plus its test
status from the knowledge base. A description of what each set of rules says now — not Scout's recommendation:
only the market filter passed the gate; most of the others were tested and rejected, or never tested.
"""
from __future__ import annotations

import logging
from datetime import date

from scout.playbooks import allocation as al
from scout.playbooks import checklist as ck
from scout.playbooks import events as ev
from scout.playbooks import exits as ex
from scout.playbooks import market as mk
from scout.playbooks import technical as te
from scout.playbooks import value as va
from scout.playbooks.base import BUY, HOLD, SELL, Ctx, Playbook, Verdict

logger = logging.getLogger("scout.playbooks")

P = Playbook
PLAYBOOKS: tuple[Playbook, ...] = (
    # ── 价值投资 ──
    P("graham", "Graham 防御型投资者", "价值投资", "Benjamin Graham", "只买大而稳、长期盈利、价格便宜的公司",
      "市值 ≥ 20 亿、近 5 年每年盈利、4 年净利润增长 ≥ 1/3、市盈率 ≤ 15、市盈率 × 市净率 ≤ 22.5、长期负债 ≤ 净资产",
      "价格远离标准（市盈率 > 40 或乘积 > 100）", "条件太严，现代市场几乎选不出股票；原书的业绩是样本内的", va.graham,
      version="Scout 近似版：没有流动比率和 20 年分红记录（数据缺），10 年盈利改为 5 年，EPS 增长改为净利润增长"),
    P("net_net", "Graham 净流动资产（net-net）", "价值投资", "Benjamin Graham", "市值低于净流动资产的 2/3 时买入",
      "市值 < 2/3 ×（流动资产 − 全部负债）", "回到净流动资产附近", "收益集中在难以买入的微型股（Alpha Architect）", None,
      missing="Scout 的财务数据没有流动资产；而且 S&P 1500 里几乎没有 net-net 股票"),
    P("magic_formula", "Greenblatt 神奇公式", "价值投资", "Joel Greenblatt", "好公司（资本回报高）+ 便宜（盈利收益率高）",
      "EBIT/EV 与资本回报两个排名相加，取前 30（不含金融、公用事业，市值 ≥ 10 亿）", "合计排名后 20%",
      "作者自报每年约 30%；独立检验结论不一，有多年跑输期", va.magic_formula, kin="rank_gross_profitability",
      version="Scout 近似版：资本回报 = EBIT /（总资产 − 现金），没有营运资本数据"),
    P("piotroski", "Piotroski F 分", "价值投资", "Joseph Piotroski", "在便宜股里挑财务正在改善的",
      "市净率最低 20% 中 F 分 ≥ 7 / 8", "便宜但 F 分 ≤ 2（价值陷阱）",
      "原文 1976–1996 每年多 7.5%；美国后期研究发现失效，海外仍有效", va.piotroski, kin="rank_low_accruals",
      version="Scout 近似版：9 项去掉流动比率，满分 8"),
    P("trending_value", "O'Shaughnessy 趋势价值", "价值投资", "James O'Shaughnessy", "最便宜的一成里买最近在涨的",
      "价值综合分前 10% 中 6 个月涨幅前 25", "价值综合分最贵的 10%", "作者 45 年回测年化约 21%（商业来源，未见独立检验）",
      va.trending_value, kin="small_value_quality_no_edge", version="Scout 近似版：EBIT 代替 EBITDA，股东收益率只算回购"),
    P("buffett_quality", "Buffett（量化版）", "价值投资", "Warren Buffett（Frazzini 等的解释）", "便宜、优质、低风险的公司长期持有",
      "质量与安全分前 20% 且 EBIT/EV 在中位数以上", "质量与安全分后 20%",
      "Berkshire 的超额大部分能用低 β + 质量 + 约 1.6 倍低成本杠杆解释（作者来自 AQR）", va.buffett_quality,
      kin="low_volatility_near_miss"),
    P("gross_profitability", "毛利能力（Novy-Marx）", "价值投资", "Robert Novy-Marx", "毛利 / 总资产最高的公司",
      "前 10%", "持仓跌出前 30%", "学术上稳健；Scout 无偏数据上未通过", va.gross_profitability, kb="rank_gross_profitability"),
    P("low_accruals", "利润质量（Sloan 应计）", "价值投资", "Richard Sloan", "利润由现金流支撑的公司",
      "应计最低 10%", "持仓跌出前 30%", "学术上经典；Scout 无偏数据上未通过", va.low_accruals, kb="rank_low_accruals"),
    P("low_asset_growth", "资产增长最慢", "价值投资", "Cooper, Gulen & Schill", "不盲目扩张的公司",
      "总资产增长最慢 10%", "持仓跌出前 30%", "Scout 无偏数据上未通过", va.low_asset_growth, kb="rank_low_asset_growth"),
    P("net_buyback", "净回购", "价值投资", "Pontiff & Woodgate", "持续回购、股本减少的公司",
      "净发行最低 10%", "持仓跌出前 30%", "Scout 无偏数据上未通过", va.net_buyback, kb="rank_net_buyback"),
    P("dogs_of_dow", "道指狗股", "价值投资", "Michael O'Higgins", "每年买道指 30 中股息率最高的 10 只",
      "股息率最高 10 只", "年底换仓", "风险调整、成本和税后基本不赢", None, missing="Scout 没有道指历史成分和股息数据"),
    # ── 成长股 ──
    P("can_slim", "O'Neil CAN SLIM", "成长股", "William O'Neil", "盈利加速、接近新高、市场确认上升时买领涨股",
      "C 当季 EPS ≥ +25%、A 3 年净利润年增 ≥ 25%、N 距 52 周高点 15% 以内、L 相对强度 ≥ 80、M 大盘确认上升",
      "放量跌破 50 日均线；持仓跌 7–8% 止损", "以作者和 IBD 的研究为主（样本内、商业）", va.can_slim, kin="rank_high_52w",
      version="Scout 近似版：没有 I（机构持股）和 S（供需），杯柄形态未定义，A 用净利润增长"),
    P("lynch", "Lynch 快速增长股 + PEG", "成长股", "Peter Lynch", "增长快、价格合理（PEG < 1）",
      "3 年净利润年增 20–50%、PEG < 1、负债 / 净资产 < 0.8", "PEG > 2", "只有 Magellan 实盘记录，无独立回测",
      va.lynch, version="Scout 近似版：增长率用过去 3 年净利润，不用预期 EPS"),
    P("minervini", "Minervini SEPA", "成长股", "Mark Minervini", "第二阶段强势股，等波动收缩后放量突破",
      "趋势模板 + 三段回调递减（最后一段 ≤ 10%）+ 放量突破枢轴（不追高 5% 以上），止损 8%", "放量跌破 50 日均线",
      "作者实盘比赛成绩；无独立检验", te.minervini, kin="rank_trend_template",
      version="趋势模板用 Scout 已检验版本；VCP 是 Scout 定义的近似版"),
    # ── 技术与趋势 ──
    P("darvas", "Darvas 箱体", "技术与趋势", "Nicolas Darvas", "新高后形成箱体，突破箱顶买，跌破箱底卖",
      "放量收盘突破箱顶（52 周高点后 3 天不再新高）", "收盘跌破箱底", "无独立检验", te.darvas, kin="trend_breakout_no_edge"),
    P("weinstein", "Weinstein 阶段分析", "技术与趋势", "Stan Weinstein", "只在第二阶段（30 周均线上方且向上）持有",
      "周线放量上穿 30 周均线", "周收盘跌回 30 周均线下方且均线走平 / 下降", "无独立检验", te.weinstein, kb="rank_weinstein_stage2",
      version="买入端是 Scout 已检验的第二阶段突破；卖出按原书"),
    P("turtle_s1", "海龟系统 1", "技术与趋势", "Richard Dennis / Curtis Faith", "突破 20 日高点买，跌破 10 日低点卖",
      "收盘突破前 20 日最高", "收盘跌破前 10 日最低", "为期货设计；个股上突破类规则 Scout 未通过", lambda c: te.turtle(c, 20, 10),
      kin="trend_breakout_no_edge"),
    P("turtle_s2", "海龟系统 2", "技术与趋势", "Richard Dennis / Curtis Faith", "突破 55 日高点买，跌破 20 日低点卖",
      "收盘突破前 55 日最高", "收盘跌破前 20 日最低", "同上", lambda c: te.turtle(c, 55, 20), kin="trend_breakout_no_edge"),
    P("wilcox", "Wilcox 新高 + 宽止损", "技术与趋势", "Cole Wilcox & Eric Crittenden", "买创历史新高的股票，用很宽的跟踪止损",
      "创 2014 年以来新高", "收盘低于最高收盘 − 10 × ATR42", "1983–2004 年扣成本后为正，收益靠少数大赢家", te.wilcox, kin="exit_vs_hold",
      version="历史新高只能算到 Scout 的数据起点（2014 年）"),
    P("clenow", "Clenow 动量系统", "技术与趋势", "Andreas Clenow", "标普 500 中走势最稳最强的股票，指数向上时才买",
      "动量分（90 日对数回归年化斜率 × R²）前 20%、在 100 日均线上方、90 日内没有 15% 单日波动、标普 500 在 200 日均线上方",
      "跌破 100 日均线、出现 15% 单日波动或动量分后 20%", "作者回测；独立复现结论不一", te.clenow, kin="rank_residual_momentum"),
    P("high_52w", "52 周新高（George & Hwang）", "技术与趋势", "George & Hwang", "离一年高点最近的股票",
      "前 10%", "持仓跌出前 30%", "学术上经典；Scout 无偏数据上未通过", te.high_52w, kb="rank_high_52w"),
    P("residual_momentum", "残差动量", "技术与趋势", "Blitz, Huij & Martens", "扣除大盘后的 12 个月动量",
      "前 10%", "持仓跌出前 30%", "Scout 无偏数据上未通过", te.residual_momentum, kb="rank_residual_momentum"),
    P("frog_in_pan", "平滑动量（温水煮青蛙）", "技术与趋势", "Da, Gurun & Warachka", "由很多小涨日累积的动量",
      "前 10%", "持仓跌出前 30%", "Scout 无偏数据上未通过", te.frog_in_pan, kb="rank_frog_in_pan"),
    P("elder", "Elder 三重滤网", "技术与趋势", "Alexander Elder", "周线定方向，日线等回调",
      "周线 MACD 柱上升且日线力量指数转负", "周线 MACD 柱转为下降", "Scout 未通过", te.elder, kb="rank_elder_triple_screen"),
    P("macd", "MACD 金叉", "技术与趋势", "Gerald Appel", "MACD 金叉买、死叉卖",
      "12/26/9 金叉", "死叉", "Scout 未通过", te.macd_cross, kb="rank_macd_cross"),
    P("golden_cross", "均线金叉（50 / 200 日）", "技术与趋势", "通用", "50 日均线在 200 日均线上方持有",
      "50 日上穿 200 日", "50 日在 200 日下方", "用在指数上不比持有好、回撤小一些，信号少", te.golden_cross),
    P("connors_rsi2", "Connors RSI(2)", "技术与趋势", "Larry Connors", "上升趋势中的短线超卖反弹",
      "在 200 日均线上方且 RSI(2) < 10", "收盘高于 5 日均线（只对持仓）", "短线均值回归；样本外证据少", te.connors_rsi2,
      kin="zscore_oversold_no_edge"),
    P("short_reversal", "短期反转", "技术与趋势", "Jegadeesh / Lehmann", "上个月跌得最多的股票反弹",
      "上月跌幅最大 10%", "持仓跌出前 30%", "Scout 未通过", te.short_term_reversal, kb="rank_short_term_reversal"),
    P("chart_patterns", "经典形态（Edwards & Magee）", "技术与趋势", "Edwards & Magee（Lo, Mamaysky & Wang 的识别）",
      "头肩底、双底买，头肩顶、双顶、扩散顶卖", "本周完成的头肩底 / 双底", "本周完成的头肩顶 / 双顶 / 扩散顶",
      "形态有增量信息，但未证明能赚超额", te.chart_patterns, kin="chart_pattern_hs"),
    P("livermore", "Livermore 关键点", "技术与趋势", "Jesse Livermore", "等价格走出整理区、放量创新高再进场",
      "20 天 15% 区间整理后放量（≥ 2 倍）创 52 周新高", "跌回整理区间低点下方", "无法检验原法",
      te.livermore, version="Scout 近似版：原书没有固定规则，这里的整理区间和放量倍数是 Scout 定的"),
    P("wyckoff", "Wyckoff 弹簧", "技术与趋势", "Richard Wyckoff", "区间下沿假跌破后收回 = 吸筹完成",
      "跌破 60 日区间下沿后放量收回", "放量收在区间下沿下方 2% 以上", "没有样本外检验，失败形态常被事后改名",
      te.wyckoff_spring, version="Scout 近似版：只取可证伪的小规则（弹簧 / 弱势信号），不判断阶段"),
    # ── 大盘择时 ──
    P("faber", "Faber 10 个月均线（你的大盘过滤）", "大盘择时", "Mebane Faber", "月末在 10 个月均线上方持有，下方拿现金",
      "月末收盘 ≥ 10 个月均值", "月末收盘 < 10 个月均值", "多国百年数据成立：少赚一点、回撤小很多", mk.faber,
      status="通过检验（SPY；1927 年起 French 数据也成立）；行业 / 主题 ETF 上没有检验"),
    P("golden_cross_etf", "均线金叉（ETF）", "大盘择时", "通用", "同上，用在指数和行业 ETF 上", "50 日上穿 200 日",
      "50 日在 200 日下方", "收益不比持有好、回撤小一些", mk.golden_cross_etf),
    P("zweig_4", "Zweig 4% 模型", "大盘择时", "Martin Zweig / Ned Davis", "周线比低点涨 4% 买、比高点跌 4% 卖",
      "周收盘比卖出后最低点 +4%", "周收盘比买入后最高点 −4%", "独立检验（到 2014）：SPY、QQQ、IWM、RSP 都跑输持有，回撤小",
      mk.zweig_four_percent, version="Scout 近似版：用 RSP（等权）代替价值线几何指数"),
    P("oneil_market", "O'Neil 市场方向", "大盘择时", "William O'Neil", "跟进日确认上升，派发日多了就减仓",
      "调整后反弹第 4 天起放量涨 ≥ 1.25%（跟进日）", "25 个交易日内 ≥ 5 个派发日或跌破反弹低点", "无独立检验；阈值随年代改过",
      mk.oneil_market, version="固定一个版本：跟进日 1.25%、派发日 −0.2%、调整 = 从 60 日高点跌 8%"),
    P("breadth_thrust", "Zweig 广度冲刺", "大盘择时", "Martin Zweig", "上涨家数在 10 天内从很少变成很多 = 新牛市开始",
      "上涨比例 10 日均值 10 天内从 < 40% 升到 > 61.5%", "—", "几十年只出现少数几次，无法统计检验", mk.breadth_thrust,
      version="Scout 近似版：用 Scout 的成分股算，不是 NYSE 全部股票"),
    P("dow_theory", "道氏理论", "大盘择时", "Charles Dow / William Hamilton", "工业和运输指数相互确认",
      "两个指数都创一年新高", "两个指数都创一年新低", "Hamilton 1902–1929 有正的风险调整收益；1929 年后扣成本无大超额",
      mk.dow_theory, version="Scout 近似版：DIA 和 IYT 的一年新高 / 新低"),
    P("liquidity", "流动性（别跟美联储作对）", "大盘择时", "Martin Zweig / Stanley Druckenmiller", "利率在降 = 顺风，利率在升 = 逆风",
      "联邦基金利率 6 个月降 ≥ 0.25 或 2 年期 3 个月降 ≥ 0.5", "反方向", "公开说法，没有成文规则", mk.liquidity,
      version="Scout 近似版：阈值是 Scout 定的"),
    P("valuation_tilt", "估值小幅调整", "大盘择时", "Cliff Asness", "估值很贵时少放一点，很便宜时多放一点，从不全进全出",
      "标普 500 市盈率在历史后 20%", "在历史前 20%", "CAPE 样本内有效、样本外择时弱；只建议小幅调整", mk.valuation_tilt,
      version="历史只从 2015 年起（Scout 的估值数据）"),
    # ── 资产配置 ──
    P("gem", "Antonacci 双动量", "资产配置", "Gary Antonacci", "美股跑赢国库券时持有美股 / 海外股中更强的，否则持有债券",
      "12 个月收益比较，每月一次", "转入债券", "样本外表现回到中位数（ReSolve 2019）", al.gem, kin="cross_asset_trend"),
    P("vaa", "Keller VAA", "资产配置", "Wouter Keller & JW Keuning", "进攻资产有一个动量转负就全部转防御",
      "4 个进攻资产动量都为正：持有最强的", "任一为负：持有最强的防御资产", "作者论文；无独立检验", al.vaa, kin="cross_asset_trend"),
    P("gtaa", "Faber GTAA 5", "资产配置", "Mebane Faber", "5 类资产各 20%，各自低于 10 个月均线时拿现金",
      "月末在 10 个月均线上方", "月末在下方", "Scout 检验未通过（2003 年起 ETF）", al.gtaa, kin="cross_asset_trend"),
    P("all_weather", "Dalio 全天候（零售版）", "资产配置", "Ray Dalio", "按经济环境分散的固定配置",
      "30% 股票、40% 长债、15% 中期国债、7.5% 黄金、7.5% 商品", "—", "2022 年约 −20%（长债拖累）", al.static("all_weather"),
      kin="risk_parity_sleeve_v1"),
    P("permanent", "Browne 永久组合", "资产配置", "Harry Browne", "股票、长债、黄金、现金各 25%",
      "固定 25% × 4", "—", "回撤小、长期收益偏低", al.static("permanent")),
    P("sixty_forty", "60 / 40", "资产配置", "通用", "60% 股票 40% 债券", "固定", "—", "基准；2022 年股债同跌",
      al.static("sixty_forty")),
    # ── 事件与投机 ──
    P("insider", "内部人集中买入", "事件与投机", "学术文献", "两位以上高管 / 董事自己掏钱买",
      "近 90 天 ≥ 2 人公开市场买入（不含 10b5-1）", "—", "学术上较多正面证据；Scout 无偏数据上未通过", ev.insider_cluster,
      kb="rank_insider_cluster"),
    P("pead", "财报后漂移", "事件与投机", "Ball & Brown / Bernard & Thomas", "财报反应强的股票继续涨",
      "近一个月报告的公司中公告 3 日反应前 10%", "持仓跌出前 30%", "大盘股 2006 年后基本消失（Martineau 2022，有争议）",
      ev.earnings_drift, kb="rank_ear_drift"),
    P("short_squeeze", "高空头 / 轧空", "事件与投机", "Asquith, Pathak & Ritter", "空头多 + 股价走强 = 可能轧空；空头多 + 走弱 = 回避",
      "空头 ≥ 15% 且在 50 日均线上方、20 日涨 ≥ 10%", "空头 ≥ 15% 且在 50 日均线下方",
      "高空头只在小盘、极高时可靠地跑输；轧空平均 +3.25%、持续约一天半", ev.short_squeeze,
      version="Scout 近似版：只有深度研究池约 150 只的当前空头数据"),
    P("spinoff", "分拆", "事件与投机", "Cusatis, Miles & Woolridge", "分拆后的子公司和母公司",
      "分拆后买入，持有 1–3 年", "—", "事前可执行版对三因子模型不显著（McConnell 等 2001）", None, missing="Scout 没有分拆事件数据"),
    # ── 出场规则（只看持仓）──
    P("exit_user", "你的计划：20% 跟踪止损", "出场规则", "你的决定（2026-10-31）", "收盘低于买入后最高收盘的 80% → 次日开盘卖",
      "—", "收盘 < 0.8 × 最高收盘", "截断大亏，强势高波动股上平均少赚", ex.user_trail, status="你的规则（Sharadar 复查后的决定）"),
    P("exit_fixed", "O'Neil / Minervini 8% 止损", "出场规则", "William O'Neil / Mark Minervini", "亏 7–8% 无条件卖",
      "—", "收盘 ≤ 买入价 × 0.92", "无独立检验", ex.fixed_stop),
    P("exit_chandelier", "LeBeau 吊灯止损", "出场规则", "Chuck LeBeau", "最高价 − 3 × ATR", "—", "收盘 < 买入后最高价 − 3 × ATR22",
      "Scout 检验：不如一直持有", ex.chandelier, kb="exit_vs_hold"),
    P("exit_wilcox", "Wilcox 10 × ATR", "出场规则", "Cole Wilcox", "很宽的波动止损", "—", "收盘 < 买入后最高收盘 − 10 × ATR42",
      "Scout 检验：不如一直持有", ex.wilcox_stop, kb="exit_vs_hold"),
    P("exit_turtle", "海龟出场", "出场规则", "Curtis Faith", "跌破 20 日低点", "—", "收盘 < 前 20 日最低", "—", ex.turtle_exit),
    P("exit_weinstein", "Weinstein 30 周均线", "出场规则", "Stan Weinstein", "周收盘跌破 30 周均线", "—", "周收盘 < 30 周均线",
      "—", ex.weinstein_exit),
    P("exit_darvas", "Darvas 箱底", "出场规则", "Nicolas Darvas", "跌破最近箱底", "—", "收盘 < 最近箱底", "—", ex.darvas_exit),
    # ── 问题清单（不给买卖）──
    P("fisher", "Fisher 15 点（问题清单）", "问题清单", "Philip Fisher", "调研式成长股投资", "—", "—", "无法检验", ck.fisher),
    P("buffett_moat", "Buffett 护城河（问题清单）", "问题清单", "Warren Buffett", "好生意、好价格、长期持有", "—", "—", "无法检验",
      ck.buffett_moat),
    P("soros", "Soros 反身性（问题清单）", "问题清单", "George Soros", "找自我强化的趋势和其中的错误", "—", "—", "无法检验", ck.soros),
)

BY_KEY = {p.key: p for p in PLAYBOOKS}
ZH_STATUS = {"confirmed": "已确认", "forward": "前向验证中", "backtested": "回测通过", "hypothesis": "假设", "rejected": "检验未通过",
             "retired": "已失效"}
TESTED_OK = ("confirmed", "forward", "backtested")
LIST_CAP = 40                       # buys / sells kept per playbook in the stored payload (counts are always complete)


def status_of(p: Playbook, entries: dict[str, str]) -> dict:
    """{text, grade}: grade 有依据 / 未通过 / 未检验 / 不适用."""
    if p.status:
        return {"text": p.status, "grade": "有依据" if p.key == "faber" else "你的规则"}
    if p.kb and p.kb in entries:
        s = entries[p.kb]
        return {"text": ZH_STATUS.get(s, s), "grade": "有依据" if s in TESTED_OK else "未通过"}
    if p.kin and p.kin in entries:
        s = entries[p.kin]
        return {"text": f"本身未检验；相近的规则 {ZH_STATUS.get(s, s)}", "grade": "未检验"}
    return {"text": "未检验" if p.school != "问题清单" else "不给买卖信号", "grade": "未检验" if p.school != "问题清单" else "不适用"}


def apply(ctx: Ctx, p: Playbook) -> dict[str, Verdict] | None:
    if p.fn is None:
        return None
    try:
        return p.fn(ctx) or {}
    except Exception as exc:                       # one playbook failing never stops the others
        logger.warning("playbook %s failed: %s", p.key, exc, exc_info=True)
        ctx.cache.setdefault("errors", {})[p.key] = type(exc).__name__
        return None


def _rows(ctx: Ctx, verdicts: dict[str, Verdict], action: str) -> list[dict]:
    rows = [(t, v) for t, v in verdicts.items() if v.action == action]
    rows.sort(key=lambda tv: (-(tv[1].score if tv[1].score is not None else -1e9),
                              -float(ctx.f["dollar_vol20"].iloc[-1].get(tv[0], 0) or 0)))
    return [{"ticker": t, "name": ctx.names.get(t, t), "why": v.why} for t, v in rows]


def run(ctx: Ctx, entries: dict[str, str] | None = None) -> dict:
    """Every playbook on the context → the stored payload."""
    entries = entries or {}
    mine = {x["ticker"] for x in ctx.holdings} | set(ctx.watchlist)
    books, by_ticker = [], {}
    for p in PLAYBOOKS:
        v = apply(ctx, p)
        rec = {"key": p.key, "name": p.name, "school": p.school, "author": p.author, "idea": p.idea,
               "buy_rule": p.buy_rule, "sell_rule": p.sell_rule, "evidence": p.evidence, "version": p.version,
               "status": status_of(p, entries)}
        if v is None:
            rec["missing"] = p.missing or (ctx.cache.get("missing", {}).get(p.key) and
                                           f"缺少数据：{'、'.join(ctx.cache['missing'][p.key])}") or \
                f"计算失败（{ctx.cache.get('errors', {}).get(p.key, '无结果')}）"
            books.append(rec)
            continue
        if not v and p.key in ctx.cache.get("missing", {}):
            rec["missing"] = f"缺少数据：{'、'.join(ctx.cache['missing'][p.key])}"
        counts = {a: sum(x.action == a for x in v.values()) for a in (BUY, HOLD, SELL)}
        rec.update({"counts": counts, "buys": _rows(ctx, v, BUY)[:LIST_CAP], "sells": _rows(ctx, v, SELL)[:LIST_CAP],
                    "mine": {t: {"action": x.action, "why": x.why} for t, x in v.items() if t in mine or t in ctx.etfs}})
        if p.school == "资产配置" or p.school == "大盘择时":
            rec["holds"] = _rows(ctx, v, HOLD)[:LIST_CAP]
        for t, x in v.items():
            if x.action in (BUY, SELL):
                by_ticker.setdefault(t, []).append([p.key, x.action])
        books.append(rec)
    return {"asof": ctx.asof.isoformat(), "playbooks": books, "by_ticker": by_ticker,
            "universe": {"stocks": len(ctx.stocks), "etfs": len(ctx.etfs)}}


def text(res: dict, top: int = 5) -> str:
    """Plain-text summary (the brief's content): per school, each playbook's status and counts, with its first buys."""
    lines = [f"打法简报 · {res['asof']}（{res['universe']['stocks']} 只股票、{res['universe']['etfs']} 只 ETF；"
             "按各打法自己的规则机械计算，不是 Scout 的推荐；只有大盘过滤通过了检验）"]
    for school in ("大盘择时", "资产配置", "价值投资", "成长股", "技术与趋势", "事件与投机", "出场规则"):
        rows = [b for b in res["playbooks"] if b["school"] == school]
        if not rows:
            continue
        lines.append(f"\n【{school}】")
        for b in rows:
            st = b["status"]["text"]
            if b.get("missing"):
                lines.append(f"· {b['name']}（{st}）：{b['missing']}")
                continue
            c = b["counts"]
            if school in ("大盘择时", "资产配置"):
                lines.append(f"· {b['name']}（{st}）：" + "；".join(f"{t} {x['action']}" for t, x in sorted(b["mine"].items())
                                                                   if t in ("SPY", "QQQ", "RSP", "DIA") or school == "资产配置"))
            elif school == "出场规则":
                lines.append(f"· {b['name']}：" + ("；".join(f"{t} {x['action']}" for t, x in b["mine"].items()) or "没有持仓"))
            else:
                first = "、".join(r["ticker"] for r in b["buys"][:top])
                lines.append(f"· {b['name']}（{st}）：买入 {c[BUY]}、持有 {c[HOLD]}、卖出 {c[SELL]}" + (f"；买入前列 {first}" if first else ""))
    return "\n".join(lines)


def digest(res: dict, top: int = 3) -> list[str]:
    """Short lines for the weekly report: market timing and allocation by name, stock playbooks as counts with their
    first buys (those with evidence first), and the exit rules that say sell on a holding."""
    books = res["playbooks"]
    lines = []
    calls = []
    for b in books:
        if b["school"] == "大盘择时" and b.get("mine"):
            spy = b["mine"].get("SPY") or b["mine"].get("RSP") or b["mine"].get("DIA")
            if spy:
                calls.append(f"{b['name'].split('（')[0]} {spy['action']}")
    if calls:
        lines.append("大盘：" + "；".join(calls))
    alloc = [f"{b['name']} → " + "、".join(f"{t}" for t, x in b["mine"].items() if x["action"] in (BUY, HOLD))
             for b in books if b["school"] == "资产配置" and b.get("mine")]
    if alloc:
        lines.append("配置：" + "；".join(alloc))
    grade = {"有依据": 0, "你的规则": 0, "未检验": 1, "未通过": 2, "不适用": 3}
    stock = sorted((b for b in books if b["school"] in ("价值投资", "成长股", "技术与趋势", "事件与投机") and b.get("counts")),
                   key=lambda b: grade.get(b["status"]["grade"], 3))
    for b in stock:
        first = "、".join(r["ticker"] for r in b["buys"][:top])
        if first:
            lines.append(f"{b['name']}（{b['status']['text']}）买入 {b['counts'][BUY]}：{first}")
    sells = {}
    for b in books:
        if b["school"] == "出场规则":
            for t, x in (b.get("mine") or {}).items():
                if x["action"] == SELL:
                    sells.setdefault(t, []).append(b["name"])
    if sells:
        lines.append("持仓：" + "；".join(f"{t} 按 {'、'.join(v)} 该卖" for t, v in sells.items()) + "（你的计划只看 20% 跟踪止损）")
    lines.append("每个打法的名单、理由和检验状态：问助手「按某打法现在买什么」或「各打法怎么看 X」。")
    return lines


async def compute(asof: date | None = None, with_splits: bool = True, with_valuation: bool = True) -> dict:
    from scout import learned
    from scout.playbooks.data import load

    ctx = await load(asof, with_splits=with_splits)
    if with_valuation:
        try:
            from scout import valuation

            ctx.cache["valuation"] = await valuation.compute(ctx.asof)
        except Exception as exc:
            logger.warning("playbooks: valuation unavailable: %s", exc)
    entries = {e["key"]: e["status"] for e in await learned.current()}
    return run(ctx, entries)


async def store(res: dict) -> None:
    from scout import db

    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('playbooks', :d, :c, CAST(:p AS JSONB))",
                     {"d": date.fromisoformat(res["asof"]), "c": text(res), "p": db.jsonb(res)})


def for_ticker(res: dict, t: str) -> list[dict]:
    """Every playbook's verdict on one ticker: {book, action, why}; a long list kept only the action (`by_ticker`)."""
    acts = {k: a for k, a in res.get("by_ticker", {}).get(t, [])}
    out = []
    for b in res["playbooks"]:
        v = (b.get("mine") or {}).get(t)
        for action, key in ((BUY, "buys"), (SELL, "sells")):
            r = None if v else next((r for r in b.get(key, []) if r["ticker"] == t), None)
            if r:
                v = {"action": action, "why": r["why"]}
        if not v and b["key"] in acts:
            v = {"action": acts[b["key"]], "why": "（名单较长，理由没有保存）"}
        if v:
            out.append({"book": b, **v})
    return out


async def latest() -> dict | None:
    from scout import db

    row = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'playbooks' ORDER BY id DESC LIMIT 1")
    return row["payload"] if row else None
