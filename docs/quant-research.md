# GQT 量化研究流程

这套系统的目标不是承诺每天盈利，而是回答一个可验证的问题：在扣除手续费、滑点和资金费率压力后，策略在未见过的数据上是否仍然有正期望。如果没有，系统必须保持模拟或停止，不把亏损回测包装成“推荐”。

## 为什么这样设计

- [Freqtrade 回测文档](https://www.freqtrade.io/en/stable/backtesting/)说明回测会使用历史 K 线并计入交易费用，同时提醒动态币对列表依赖当前市场状态，不能直接代表历史状态。研究结果因此固定 BTCUSDT/ETHUSDT 数据，并记录数据区间。
- [Binance USD-M Funding Rate History](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History)是资金费率的官方数据入口。研究层目前使用可配置的 8 小时资金费率压力值；接入真实历史费率后应替换该压力值，而不是忽略资金费率。
- [vn.py](https://github.com/vnpy/vnpy)提供事件驱动的 CTA 回放引擎。策略只在已收盘 K 线上计算信号，订单执行、止损优先级和成交成本由回测适配层处理。
- 时间序列不能随机打乱后训练。滚动训练、隔离测试和 embargo（训练集与测试集之间留出间隔）用于降低相邻样本和标签重叠造成的泄漏风险。参数选择只读取训练窗口，测试窗口只用于最终验收。

## 当前研究闸门

每个 BTC/ETH、每个滚动测试窗口都会记录：

- 交易数、胜率、盈亏比、profit factor 和每笔期望值；
- 费用、滑点、资金费率压力的绝对金额与成本占比；
- 年化 Sharpe、最大回撤和连续亏损次数；
- 训练窗口选择了哪组固定候选参数，以及候选参数的训练统计。

只有所有测试窗口同时满足配置中的交易数、profit factor、期望值、Sharpe、成本占比、最大回撤和连续亏损限制，组合才会被标记为 `portfolio_eligible`。这不是盈利保证，而是“允许进一步纸面运行”的最低证据门槛。

## 运行方式

```powershell
cd D:\x\gqt\vnpy_gqt
.venv\Scripts\python -m gqt_vnpy.cli import-freqtrade `
  --source "$env:LOCALAPPDATA\HillmanPick\GQT Trader\data\trading\user_data\data\binance\futures" `
  --symbol BTCUSDT --timeframe 15m

.venv\Scripts\python -m gqt_vnpy.cli backtest `
  --config config\research.json `
  --start 2022-10-03 --end 2026-07-24 `
  --output results\walk-forward
```

退出码为 `0` 只表示所有测试窗口通过闸门；非 `0` 表示至少一个窗口失败。失败结果仍会保存，便于查看具体是哪个市场阶段、成本或风险指标不达标。

## 如何走向可交易

1. 先补齐每个交易对的真实 funding、盘口深度和成交延迟数据，再重复滚动回测。
2. 保留一段完全不参与参数选择的 holdout 数据；参数、候选范围或特征改变后必须重新生成实验编号。
3. 让纸面账户运行足够长的时间，比较实时成交成本、信号延迟和回测假设的偏差。
4. 只有在样本外和纸面结果都稳定时，才考虑极小风险预算的实盘试验，并保留人工停止开关。

历史上已经出现过大量负收益订单，因此旧事件合约账本不会作为新 BTC/ETH 策略的训练或绩效证明。任何“每天固定盈利百分比”的目标都不能替代正期望、风险预算和停止条件。
