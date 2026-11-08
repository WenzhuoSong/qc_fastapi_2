# 交易打法目录 v1（2026-11-09）

用途：把常见的投资 / 交易打法按流派整理成同一格式，作为周报"打法简报"和以后预登记检验的清单。
程序：每个打法都写成了函数（`scout/playbooks/`，59 个），每周六随周报计算，结果存为简报 `playbooks`，问助手用 `get_playbooks`；
近似版（原作者没有固定规则或 Scout 缺数据）在函数的 `version` 里写明替换了什么。
每个打法写：核心想法、可以写成规则的部分、外部证据、Scout 里的状态、能否机械化、数据够不够。

阅读前的三点：

1. **书里的业绩几乎都是样本内的。** McLean & Pontiff（2016，97 个选股变量）发现论文发表后平均收益下降约三分之一；
   名人的实盘记录里有运气、杠杆和时代红利（例如 Buffett 的超额大部分能用"便宜 + 优质 + 低风险 + 约 1.6 倍低成本杠杆"解释，Frazzini, Kabiller & Pedersen）。
2. **Scout 的检验口径有两种**，要分清：
   - 选股口径：选出的股票是否比同一天、同样买卖的其他股票好（大部分打法在这一口径下失败）；
   - 绝对口径：整套打法是否比一直持有 SPY（或 SPY + 大盘过滤）好，看收益和回撤。
   你"涨时买、跌时卖"的目标对应绝对口径；一个没有选股优势的打法仍可能在绝对口径下有用（主要靠避开大跌），反之亦然。
3. **"能否机械化"决定能否检验。** 主观打法（Fisher、Soros、Livermore、Wyckoff）写不成固定规则，硬写就是我们替作者发明规则，
   这类只能作为"问题清单 / 思考框架"放进 AI 解读，不当信号。

状态标记：✅ 检验通过 · 🟡 回测通过 / 前向中 · ❌ 检验未通过 · ⬜ 未检验 · ⛔ 不能机械化

---

## 一、价值投资

| 打法 | 核心规则 | 外部证据 | Scout 状态 | 机械化 / 数据 |
|---|---|---|---|---|
| **Graham 防御型投资者** | 规模足够大、财务稳健（流动比率 ≥ 2 等）、20 年连续分红、10 年无亏损、10 年 EPS 增长 ≥ 1/3、P/E ≤ 15、P/B ≤ 1.5（或 P/E × P/B ≤ 22.5） | 条件太严，现在几乎选不出股票；Graham 晚年自己也简化了规则 | ⬜ | 可机械化；SEC XBRL 季度财务 + 分红，需 10–20 年历史（Sharadar 本地有 1999 起） |
| **Graham 净流动资产（net-net）** | 市值 < 2/3 ×（流动资产 − 全部负债 − 优先股），分散买入，持有 1–2 年 | Graham 1957 年测试、Vu（1988）、Montier（1980–2005）都很强；Alpha Architect 指出收益集中在买不进的微型股 | ⬜ | 可机械化；但美国大中盘几乎没有 net-net，Scout 的池子（S&P 1500）基本选不出 → 不适用 |
| **Greenblatt 神奇公式** | 按 EBIT/EV（盈利收益率）和 EBIT /（净固定资产 + 营运资本）（资本回报）分别排名，相加取最好的 20–30 只，持有一年 | 作者自报每年约 30%；独立研究结论不一，有多年跑输期；未找到严格的发表后样本外检验 | ⬜（近亲：毛利能力 ❌） | 可机械化；XBRL 有 EBIT、负债、现金，市值有（valuation 模块）|
| **Piotroski F-score** | 高账面市值比股票中，9 项 0/1 财务信号（盈利、现金流、ROA 改善、应计、杠杆下降、流动比率改善、无增发、毛利率改善、周转率改善），8–9 分买入 | 原文 1976–1996 每年多 7.5%；美国后期研究发现失效（Creighton 研究：高分组反而更差）；北欧等海外仍有效 | ⬜（近亲：应计 ❌、小盘价值 + 质量 ❌） | 可机械化；数据有 |
| **Buffett（量化版）** | 便宜 + 优质（盈利、稳定、增长、分红）+ 低 β，长期持有，适度低成本杠杆 | Frazzini 等：用 BAB + 质量因子后 Berkshire 的超额不再显著（作者来自 AQR，卖这类产品） | 部分：低波动 ❌（差一点：t 2.98）、毛利 ❌ | 量化版可机械化；"护城河、管理层"部分 ⛔ |
| **O'Shaughnessy 趋势价值** | 价值综合分（P/S、P/E、P/B、P/CF、EBITDA/EV、股东收益率）前 10%，再按 6 个月动量取前 25–50 只 | 作者 45 年回测年化约 21%（商业来源，未见独立检验）；价值 + 动量组合在学术上是常见结论 | ⬜（价值、动量各自 ❌） | 可机械化；数据有 |
| **Dogs of the Dow** | 每年初买道指 30 中股息率最高的 10 只，等权持有一年 | 早期回测略好；风险调整、成本和税后基本不赢（1946–1995 研究；2010–2020 跑输道指和标普） | ⬜ | 需要道指历史成分，Scout 没有 → 不做 |
| **Fisher 15 点 / 调研** | 定性清单：市场空间、研发、利润率、管理层诚信……；向客户、供应商、竞争对手打听 | 无法检验 | ⛔ | 已部分体现在公司画像（10-K 原文的客户、供应商、竞争对手）和 `research_company`；适合做"问题清单" |
| **Lynch 分类 + PEG** | 六类公司（慢速、稳定、快速增长、周期、困境反转、资产型）各有买卖标准；PEG = P/E ÷ 增长率（< 1 便宜），负债权益比 < 80% | 只有 Magellan 实盘记录，无独立回测 | ⬜ | 分类可半机械化（按增速、周期性），PEG 可算（需要增长率口径：历史 or 预期） |

## 二、成长股打法

| 打法 | 核心规则 | 外部证据 | Scout 状态 | 机械化 / 数据 |
|---|---|---|---|---|
| **O'Neil CAN SLIM** | C 当季 EPS 同比 ≥ 25%；A 近 3 年年度 EPS 增长 ≥ 25%；N 新产品 / 新高；S 供需（放量）；L 相对强度排名 ≥ 80；I 机构增持；M 大盘处于确认上升（见第四节）；在杯柄形态的枢轴点放量突破时买，跌 7–8% 止损，涨 20–25% 部分止盈 | 作者与 IBD 的研究为主（样本内、商业）；无可靠独立检验 | 部分：52 周新高 ❌、财报反应 ❌、突破 ❌ | C、A、L、N、S、M 可机械化；I 需要 13F（只在本地 Sharadar）；杯柄形态要先定死几何定义 |
| **Minervini SEPA** | 趋势模板（8 条均线 / 新高新低条件 + RS）→ 波动收缩形态（VCP：每次回调幅度递减、量缩）→ 放量突破枢轴点（不追高于枢轴 5%）→ 止损 7–8%（最多 10%）→ 盈亏比 ≥ 2:1 | 作者实盘比赛成绩；无独立检验 | 趋势模板 ❌；VCP ⬜ | VCP 需要先定死"收缩"的定义；止损 7–8% 可直接测 |
| **Lynch 快速增长股** | 年增长 20–25%（到 50%），PEG 合理，负债低 | 同上 | ⬜ | 同 Lynch |

## 三、技术分析与趋势跟踪（个股）

| 打法 | 核心规则 | 外部证据 | Scout 状态 | 机械化 / 数据 |
|---|---|---|---|---|
| **道氏理论** | 工业与运输指数相互确认的主趋势；创新高确认牛市，跌破前低确认熊市 | Brown, Goetzmann & Kumar（1998）：Hamilton 1902–1929 的社论择时有正的风险调整收益；1929 年后预测力弱，扣成本后无大超额 | ⬜ | 规则依赖解读；可做近似版（两指数同时创新高 / 新低） |
| **Livermore** | 顺着"最小阻力线"、等待关键点（pivotal point）再进场、赢了加仓（金字塔）、错了马上走 | 无法检验 | ⛔ | 原则已包含在趋势 + 止损里；"关键点"的具体定义各家说法不同 |
| **Darvas 箱体** | 新高后 3 天不再创新高 = 箱顶，其后最低点 = 箱底；收盘突破箱顶（常加放量）买入，止损放在箱底下方，新箱体形成就上移止损，永不下移 | 无独立检验；各家对"3 天还是 4 天、用盘中还是收盘价"说法不同 | ⬜（近亲：突破 ❌） | 可机械化（需先选定一个版本）；数据有 |
| **Weinstein 阶段分析** | 周线 30 周均线：第二阶段（均线走平转升、放量突破）买；跌回 30 周均线下方（第三、四阶段）卖 | 无独立检验 | 第二阶段突破 ❌（买入端；Scout 当时用固定持有期 / 计划出场，没有用"跌破 30 周均线卖"）| 可机械化；完整版（含卖出）未测 |
| **Edwards & Magee / 经典形态** | 头肩、双顶双底、三角、矩形、扩散等形态的突破 | Lo, Mamaysky & Wang（2000）：形态有增量信息但未证明能赚超额；Savin 等（2007）头肩形每年 5–7% 风险调整超额 | 10 种形态 ❌；风险章节（波动、仓位）已用在 position_risk | — |
| **Wyckoff** | 吸筹 / 派发阶段、弹簧（spring）、强势信号（SOS） | 无样本外检验；失败的形态常被事后改名，难以证伪 | ⛔（只能拆成可证伪的小规则） | — |
| **海龟（Dennis / Faith）** | 20 日（系统 1）/ 55 日（系统 2）突破进场；10 日 / 20 日新低出场；按波动（N = ATR）定仓位，每个品种最多 4 个单位 | 为期货设计；用在个股上的证据见 Wilcox | ⬜（突破 ❌；Carver 多品种趋势 ❌） | 可机械化 |
| **Wilcox & Crittenden 新高 + 宽止损** | 创历史新高买入，10 × ATR 跟踪止损，次日开盘卖；分散持有 | 1983–2004 年 18 000 多笔、扣 0.5% 往返成本后为正，收益高度右偏（少数大赢家）；2025 年 Zarattini, Pagani & Wilcox 复查 | 出场 10 × ATR42 已测（exit_vs_hold ❌ vs 一直持有）；新高买入端 ≈ 52 周新高 ❌ | 可机械化 |
| **Clenow「Stocks on the Move」** | 标普 500 成分股；按 90 日对数价格回归斜率（年化）× R² 排名；排除低于 100 日均线、90 日内单日涨跌超过 15% 的股票；指数在 200 日均线上方才买新股；按 ATR 定仓位（每只风险约 0.1% 账户）；每周三调仓，持有 10–30 只 | 作者回测；独立复现结果不一 | ⬜（近亲：残差动量 ❌、平滑动量 ❌） | 可机械化；是一个**完整系统**（选股 + 大盘过滤 + 仓位 + 出场），可以按绝对口径测 |
| **Elder 三重滤网** | 周线 MACD 定方向，日线力量指数回调时买 | — | ❌ | — |
| **MACD 金叉 / 均线金叉** | 12/26/9 金叉；50 日上穿 200 日 | 50/200 用在指数上：收益不比持有好，回撤小一些，信号少、结果依赖起点 | MACD ❌；50/200 ⬜ | — |
| **Connors RSI(2)** | 收盘在 200 日均线上方且 RSI(2) < 10（或 < 5）时买，收盘高于 5 日均线卖 | 短线均值回归；Bandy 的样本外只有 50 笔；2018 年这类回调演变成深跌时亏损 | ⬜（近亲：短期反转 ❌、z 分数超跌 ❌） | 可机械化；持有几天，成本敏感 |

## 四、大盘择时与市场环境

| 打法 | 核心规则 | 外部证据 | Scout 状态 | 机械化 / 数据 |
|---|---|---|---|---|
| **Faber 10 个月均线** | 月末收盘 ≥ 10 个月均值持有，否则现金 | 多国、百年数据都成立：少赚一点、回撤小很多 | ✅（你的大盘过滤；1927 起 French 数据也成立） | 已在用 |
| **50 / 200 日均线交叉** | 见上 | 和 Faber 类似但信号更少 | ⬜ | 与 Faber 高度重复，价值不大 |
| **Zweig 4% 模型** | 价值线指数周收盘比任一周低点涨 ≥ 4% 买，比周高点跌 ≥ 4% 卖 | Zweig 自报 1966–1996 很好；CXO 独立检验（到 2014）：SPY、QQQ、IWM、RSP 上都跑输持有，回撤小 | ⬜ | 价值线几何指数数据难得；可用 RSP（等权）近似 |
| **O'Neil 市场方向** | 跟进日（反弹第 4 天起，指数放量上涨 ≥ 约 1.25%，各版本 1%–1.7% 不等）确认上升；25 个交易日内多个派发日（放量下跌）= 压力，跌破反弹低点 = 失败 | 无独立检验；阈值随年代改过 | ⬜ | 可机械化（需先选定一个版本）；SPY / QQQ 日线成交量有 |
| **Zweig 广度冲刺** | 上涨家数比例的 10 日 EMA 在 10 个交易日内从 < 40% 升到 > 61.5% | 几十年只出现过少数几次，样本太少无法统计检验 | ⬜ | 只适合"罕见信号"式描述；Scout 可用成分股自己算（与原版 NYSE 口径不同） |
| **CAPE 估值择时** | 估值高时减仓、低时加仓 | 样本内长期预测力强，样本外择时弱（Asness 等《Market Timing: Sin a Little》；Dimson 等 23 国研究）；AQR 建议只小幅调整并配合动量 | ⬜（weekly valuation 模块只描述） | 数据有（valuation 模块、Shiller 数据） |
| **Druckenmiller / "别跟美联储作对"** | 流动性和美联储的方向比盈利更决定大盘 | 公开说法，没有成文规则 | ⛔（可做近似：利率方向 / 资产负债表变化） | FRED 有数据；要先定规则 |
| **Scout 风险预警** | 5 个条件中 ≥ 2 个 | — | 🟡（但不比大盘过滤好） | — |

## 五、资产配置与 ETF 轮动

| 打法 | 核心规则 | 外部证据 | Scout 状态 | 机械化 / 数据 |
|---|---|---|---|---|
| **60 / 40** | 60% 股票 40% 债券，定期再平衡 | 基准；2022 年股债同跌 | 对照组 | — |
| **Dalio 全天候（零售版）** | 30% 股票、40% 长期国债、15% 中期国债、7.5% 黄金、7.5% 商品 | 2022 年约 −20%（长债拖累） | 近亲：风险平价配置 ❌（1965–2003） | ETF 2004 起 |
| **Browne 永久组合** | 股票、长债、黄金、现金各 25% | 回撤小，长期收益偏低；2022 年回撤约 16–19% | ⬜ | ETF 2004 起 |
| **Antonacci 双动量（GEM）** | 每月：美股 12 个月收益 > 国库券时，持有美股、海外股中 12 个月更强的一个，否则持有综合债券 | ReSolve（2019）：样本外（1950–1973、2012–2018）表现回到中位数，原版参数有运气成分；Newfound：不同实现方式年差可达数百上千基点 | 近亲：跨资产双动量前 3 ❌（cross_asset_trend_v1） | 可机械化；SPY、EFA、AGG / BIL 2003 起；更早的海外指数 Scout 没有 |
| **Keller VAA / DAA** | 13612W 动量（1、3、6、12 个月加权）；进攻资产（SPY、EFA、EEM、AGG）中任一为负就全部转入防御资产（SHY、IEF、LQD）中最强的一个，否则持有最强的进攻资产 | 作者论文（SSRN 3002624）；无独立检验 | ⬜ | 可机械化；ETF 2003 起（EEM 等） |
| **Faber GTAA** | 5 类资产各 20%，各自低于 10 个月均线时转现金 | — | ❌（cross_asset_trend_v1） | — |
| **Carver 多品种趋势** | 多速度 EWMAC 合成 + 速度上限 + 波动目标 | 单独用不如持有，危机年赚钱 | ❌；20% 配置版 🟡（trend_sleeve_v1） | — |
| **行业轮动**（RRG、贡献度、行业动量、基本面加速、季节性） | — | — | ❌（9 次检验） | — |

## 六、投机与事件驱动

| 打法 | 核心规则 | 外部证据 | Scout 状态 | 机械化 / 数据 |
|---|---|---|---|---|
| **Soros 反身性** | 找自我强化的趋势和其中的错误，先顺势，转折时反向 | 无法检验 | ⛔ | 适合做"叙事是否在自我强化"的问题清单 |
| **财报后漂移（PEAD）** | 超预期 / 公告反应强的股票继续涨 | Martineau（2022）：大盘股 2006 年后已消失，微型股最近也消失；2025 年两篇论文反驳，分歧主要在度量方法和是否含微型股 | ❌（财报反应漂移） | — |
| **分拆（spin-off）** | 分拆后的子公司和母公司 | Cusatis 等（1993，1965–1988）三年显著超额，集中在后来被收购的公司；McConnell 等（2001，事前可执行版，1989–1995）对三因子模型不显著 | ⬜ | 需要分拆事件表（Sharadar ACTIONS 可能有，待查） |
| **高空头比例 / 轧空** | 高空头比例的股票之后跑输；轧空 | Asquith, Pathak & Ritter（2005）：只在小盘、极高空头比例中可靠；等权有、市值加权没有；轧空平均 +3.25%、持续约一天半 | ⬜ | 空头比例历史 Scout 没有（Yahoo 只有当前值）→ 暂不能测 |
| **内部人买入** | 两位以上高管 / 董事公开市场买入 | 学术上较多正面证据 | ❌（无偏数据上） | — |
| **跳空 / 放量异动 / 超跌反弹** | — | — | ❌ | — |

## 七、出场与仓位（横向）

| 打法 | 规则 | Scout 状态 |
|---|---|---|
| O'Neil / Minervini 固定止损 | 买入价下方 7–8%（最多 10%） | ⬜（Scout 测过 ATR 类止损，没测固定百分比） |
| Darvas 箱底止损 | 止损在箱底下方，只上移 | ⬜ |
| 海龟 | 2N 止损；10 / 20 日新低出场 | ⬜ |
| Weinstein | 周收盘跌破 30 周均线卖 | ⬜ |
| Wilcox | 10 × ATR42 跟踪 | ❌（vs 一直持有） |
| LeBeau 吊灯 | 3 × ATR22 | ❌（vs 一直持有） |
| **你的计划** | 收盘低于买入后最高收盘的 80% → 次日开盘全部卖出 | 你的决定（2026-10-31，Sharadar 复查后）；截断大亏，强势高波动股上平均少赚 |
| 按 ATR / 波动定仓位 | Clenow、海龟：每只承担相同风险 | Scout 有波动上限（`vol_budget_pct`） |

---

## 归类

**A. 已检验（这些结论直接放进周报）**
大盘过滤 ✅；价值 / 质量 / 动量 / 形态 / 事件类选股 ❌；ETF 轮动和行业轮动 ❌；20% 跟踪止损是你的规则。

**B. 可机械化、未检验、Scout 有数据（可预登记的候选）**

| 候选 | 为什么值得测 | 注意 |
|---|---|---|
| Antonacci 双动量 / Keller VAA | 正是"大盘 ETF 涨时买、跌时卖"的经典做法，按绝对口径评价 | 与 cross_asset_trend 同族（已 2 次）；2003 年后的 ETF 数据都看过；外部样本外证据已偏弱 |
| Clenow「Stocks on the Move」 | 完整系统（选股 + 大盘过滤 + 仓位 + 出场），按绝对口径和 SPY + 过滤比 | 与动量族、scout_system 族重叠，门槛按族计次提高 |
| O'Neil 市场方向（跟进日 / 派发日） | 大盘择时的另一种做法，反应比 Faber 快 | 阈值版本多，必须先定一个；比较对象是 Faber |
| 神奇公式 / Piotroski / 趋势价值 | 价值派最常被引用的三个，绝对口径下可能和持有相近但回撤不同 | 近亲（毛利、应计、小盘价值质量）都失败了，预期不高 |
| Weinstein 完整版、Darvas、Minervini VCP + 7–8% 止损 | 你最直接的"涨时买、跌时卖"个股打法 | 买入端近亲都失败；新的是**出场规则**，可以测"同样的买点，换这种出场 vs 20% 跟踪止损" |

**C. 可机械化但缺数据**：Graham 防御型（要 20 年分红历史）、Dogs of the Dow（道指历史成分）、Zweig 4%（价值线指数）、空头比例、分拆（待查 Sharadar）、CAN SLIM 的机构持股（只在本地 Sharadar）。

**D. 不能机械化**：Fisher、Lynch 的定性部分、Buffett 的护城河判断、Soros、Livermore、Wyckoff、Druckenmiller。
用法：变成 AI 解读里的"问题清单"（例如 Fisher：这家公司未来几年的收入空间在哪？Soros：现在的涨势是否在强化它自己的基本面？），只帮你想问题，不给买卖信号。

## 周报里怎么用（草案）

每个打法一张卡片：本周它的市场条件是否允许买 → 股票池 / ETF 中符合买入条件的 → 你的持仓里按它的规则该卖的 → 它的检验状态和一句话证据。
排序：✅ / 🟡 在前，❌ 放在"名人打法对照"栏，⛔ 只出现在问题清单里。措辞统一为"按 X 打法的规则，符合买入 / 卖出条件"，不写成 Scout 的推荐。

## 来源

- McLean & Pontiff (2016)：[Does Academic Research Destroy Stock Return Predictability?](https://Www.Gwern.net/doc/economics/2016-mclean.pdf)
- Frazzini, Kabiller & Pedersen：[Buffett's Alpha（NBER w19681）](https://www.nber.org/papers/w19681.pdf)
- Graham net-net：[Alpha Architect](https://alphaarchitect.com/ben-grahams-net-current-asset-value-net-net-strategy/)、[GuruFocus 测试](https://www.gurufocus.com/news/490147/testing-grahams-net-current-asset-value-strategy-part-2)
- 神奇公式：[Wikipedia](https://en.wikipedia.org/wiki/Magic_formula_investing)、[Erasmus 论文](https://thesis.eur.nl/pub/8041/Daggenvoorde,%20S%20%28312929%29.pdf)
- Piotroski：[Creighton 研究](https://researchworks.creighton.edu/esploro/outputs/journalArticle/Value-Stocks-and-Accounting-Screens-Has/991006117468602656)、[北欧证据](https://osuva.uwasa.fi/handle/10024/4642)
- 趋势价值：[Ritholtz](https://ritholtz.com/2011/12/trending-value-metrics-by-o%E2%80%99shaughnessy/)、[Quant Investing](https://www.quant-investing.com/strategies/oshaughnessy-trending-value-investment-strategy)
- Dogs of the Dow：[Lin (2017)](https://www.bnains.org/backtest/dogs/recherches/Investment_Performance_of_the_Dogs_of_the_Dow_Strategies_Latest_Evidence_-_Eric_C_Lin_-_2017.pdf)、[CMC Markets](https://www.cmcmarkets.com/en-au/trading-guides/dogs-of-the-dow)
- Lynch：[Validea](https://blog.validea.com/?p=7627)；Fisher：[Novel Investor](https://novelinvestor.com/philip-fishers-15-points/)
- O'Neil 跟进日 / 派发日：[LuxAlgo FTD](https://www.luxalgo.com/library/concept/follow-through-day/)、[LuxAlgo 派发日](https://www.luxalgo.com/library/concept/distribution-days/)
- Minervini：[TradingView SEPA 脚本说明](https://in.tradingview.com/script/kcihs4Hp-Minervini-SEPA-System)
- 道氏理论：[Brown, Goetzmann & Kumar (1998)](https://archive.nyu.edu/handle/2451/26858)、[CXO 摘要](https://www.cxoadvisory.com/?p=582)
- Livermore：[Reminiscences 原文](https://wyandanch-library.pages.dev/read/livermore-reminiscences)
- Darvas：[MQL5](https://www.mql5.com/en/articles/24111)、[Neostox](https://blog.neostox.com/charting/darvas-box-strategy-rules-entries-stops-worked-trade)
- 形态：[Lo, Mamaysky & Wang (2000)](https://bashtage.github.io/kevinsheppard.com/files/teaching/mfe/advanced-econometrics/Lo_Mamaysky_Wang.pdf)、[CXO](https://www.cxoadvisory.com/technical-trading/classic-papers-returns-from-pattern-based-technical-analysis/)
- Wyckoff：[Evidence Finance](https://evidencefinance.neocities.org/wyckoff)
- 海龟：[MQL5](https://www.mql5.com/en/articles/23448)、[Garner (2010)](https://tradingwithrayner.com/wp-content/uploads/2014/11/turtle-strategy-revised.pdf)
- Wilcox & Crittenden：[原文](https://www.cis.upenn.edu/~mkearns/finread/trend.pdf)、[Zarattini, Pagani & Wilcox (2025)](https://concretumgroup.com/does-trend-following-still-work-on-stocks/)
- Clenow：[TuringTrader](https://turingtrader-com.web.app/portfolios/clenow-stocks-on-the-move)、[QuantConnect 讨论](https://www.quantconnect.com/forum/discussion/16578/quot-stocks-on-the-move-quot-momentum-strategy-by-cabedovestment/)
- 50 / 200 均线：[StockCharts](https://articles.stockcharts.com/article/articles-arthurhill-2016-06-trading-strategy---crossing-to-the-dark-side---quantifying-the-golden-cross-for-the-sp-500/)、[MarketInOut](https://www.marketinout.com/blog/golden-cross-backtest-us-canada-uk)
- Connors RSI(2)：[Bandy 测试](https://vilagbagoly.blog.hu/2012/03/26/systems_howard_mean_reversion_rsi)、[Backtrex](https://backtrex.com/en/backtests/connors-rsi-2-sp-500)
- Zweig 4%：[CXO 独立检验](https://www.cxoadvisory.com/technical-trading/martin-zweigs-four-percent-model-indicator/)；广度冲刺：[LuxAlgo](https://www.luxalgo.com/library/concept/breadth-thrusts/)
- CAPE 择时：[AQR Market Timing: Sin a Little](https://www.Aqr.Com/-/media/AQR/Documents/Insights/White-Papers/Market-Timing-Sin-a-Little.pdf)
- Druckenmiller：[Benzinga](https://www.benzinga.com/analyst-ratings/analyst-color/15/04/5403650/druckenmiller-joins-fed-bashers-this-will-end-badly)
- 全天候 / 永久组合：[Validea 2022 回顾](https://blog.validea.com/a-look-at-how-a-variety-of-risk-management-approaches-have-worked-in-2022/)、[lazyportfolioetf](https://www.lazyportfolioetf.com/comparison/harry-browne-permanent-vs-ray-dalio-all-weather)
- 双动量：[ReSolve / Advisor Perspectives](https://www.advisorperspectives.com/articles/2019/05/06/global-equity-momentum-a-craftsmans-perspective)、[Newfound](https://blog.thinknewfound.com/tag/gem/)
- VAA：[Keller & Keuning 论文副本](https://thechartist.com.au/wp-content/uploads/2024/12/5369/SSRN-id4166845.pdf)、[TradingView 13612W 脚本](https://kr.tradingview.com/script/8vo7fX4M)
- PEAD：[Martineau (2022)](https://cfr.ivo-welch.info/published/papers/martineau2021rest.pdf)、[UCLA Anderson 综述](https://anderson-review.ucla.edu/is-post-earnings-announcement-drift-a-thing-again/)
- 分拆：[McConnell, Ozbilgin & Wahal (2001)](https://business.purdue.edu/faculty/mcconnell/publications/spinoffs-ex-ante-2001-vol74.pdf)
- 空头比例：[Asquith, Pathak & Ritter（NBER w10434）](https://www.nber.org/system/files/working_papers/w10434/w10434.pdf)
- BAB / QMJ：[Frazzini & Pedersen](https://research-api.cbs.dk/ws/portalfiles/portal/60082899/lasse_heje_pedersen_et_al_betting_against_beta_publishersversion.pdf)
- Soros：[GuruFocus](https://www.gurufocus.com/news/421890/george-soros-theory-of-reflexivity)

大部分规则细节来自二手资料；做预登记前要以原书为准，并在规格里写明采用的是哪个版本。
