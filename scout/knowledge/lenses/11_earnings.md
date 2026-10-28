---
id: earnings
title: 财报事件
fields: [earn_days_since, earn_days_to_next, earn_eps_surprise, earn_rev_surprise, earn_last_move, earn_avg_abs_move, earn_reactions_n, earn_implied_vs_hist, ang_ear, ang_ear_sessions_ago]
sources:
  - 财报后漂移 PEAD（Bernard & Thomas 1989）
  - 期权隐含的财报波动 vs 历史实现波动
---
**看什么**：上一次财报说明了什么、市场怎么反应；下一次财报的风险有多大。

**检查清单**
1. 上一次：EPS 和营收是否超预期，股价反应是正还是负。超预期且股价大涨，之后几周往往继续漂移（PEAD）；超预期但股价下跌，说明市场更在意指引或其他问题。
2. 下一次：距下次财报的天数。持仓跨越财报就要承担跳空风险。
3. 波动预期：历次财报平均波动有多大；期权隐含的财报波动是高于还是低于历史（期权预期 / 历史财报波动）。
4. 时间点：财报刚发布不久，新信息还在被消化；临近财报时，技术形态的可靠性下降。

**确认**：超预期 + 正面反应 + 预期上调。
**矛盾**：超预期但股价下跌（指引差或利好已被定价）。

**常见陷阱**：只看 EPS 不看营收和指引；用一次财报反应代表常态；忽略财报日期的不确定性。
- 财报公告反应（ang_ear）：公告前后 3 日相对 SPY 的涨跌，文献上之后会同向漂移；看它和异动后至今的走势是否一致。
