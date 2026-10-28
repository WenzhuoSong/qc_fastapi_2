---
id: ownership
title: 持仓与资金
fields: [own_inst_pct, own_insider_pct, own_short_float, own_short_days, own_short_change, own_top_holder_change, insider_net_value_90d, insider_buyers_90d, insider_sellers_90d, ang_insider_officer_buyers, ang_insider_data_through]
sources:
  - 内部人交易的信息含量：集中买入强于卖出（Lakonishok & Lee 2001）
  - 做空比例与轧空风险（Asquith, Pathak & Ritter 2005）
  - 13F 机构持仓（按季、滞后约 45 天）
---
**看什么**：谁在持有、谁在加减仓、空头有多拥挤。

**检查清单**
1. 内部人：90 日内公开市场的净买卖。多名内部人同时买入是少见而有信息量的信号；卖出多是计划内的，单独看不说明问题。
2. 做空：做空占流通股比例和回补天数。比例高 + 回补天数长 = 空头拥挤，利好时可能轧空，但也说明有人认真看空。空头增加要找原因。
3. 机构：机构持股比例和前十大机构的增减持。13F 数据滞后一个季度以上，只能看方向。

**确认**：内部人买入、空头减少、大机构增持。
**矛盾**：股价强势但内部人集中卖出、空头持续增加。

**常见陷阱**：把计划内的内部人卖出当成利空；忽略 13F 的滞后；做空比例高不等于一定会轧空。
- 高管 / 董事集中买入（SEC Form 4，剔除 10b5-1 计划）比单纯的卖出更有信息量；注意数据截至日期（ang_insider_data_through）。
