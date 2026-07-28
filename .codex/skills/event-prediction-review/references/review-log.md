# Event Prediction Review Log

Append one compact entry after each event prediction review. Keep raw ticket data in `event_predictions.sqlite`; this file stores only conclusions and next hypotheses.

## 2026-07-28 Initial Setup

- Status: event prediction virtual-order logging implemented.
- Horizons: 10m, 30m, 60m.
- Bankroll: 200 USDT.
- Stake: 5 USDT per virtual ticket.
- Data source: Binance USD-M futures public market data as a proxy settlement feed.
- Current model: deterministic factor mix of short momentum, EMA trend, breakout position, volume confirmation, funding, and long/short ratio.
- Next review threshold: wait for at least 50 settled tickets per horizon before treating win rate as actionable.
