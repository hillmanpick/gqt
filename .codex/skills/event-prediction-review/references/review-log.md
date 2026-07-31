# Event Prediction Review Log

Append one compact entry after each event prediction review. Keep raw ticket data in `event_predictions.sqlite`; this file stores only conclusions and next hypotheses.

## 2026-07-28 Initial Setup

- Status: event prediction virtual-order logging implemented.
- Horizons: 10m, 30m, 60m.
- Bankroll: unlimited virtual capital.
- Stake: 5 USDT per virtual ticket.
- Data source: Binance USD-M futures public market data as a proxy settlement feed.
- Current model: deterministic factor mix of short momentum, EMA trend, breakout position, volume confirmation, funding, and long/short ratio.
- Next review threshold: wait for at least 50 settled tickets per horizon before treating win rate as actionable.

## 2026-07-30 Live Data Review

- Sample: BTC/ETH only; settled tickets from 2026-07-28 16:52:58 to 2026-07-30 09:25:30. 10m: 3971 tickets; 30m: 3931 tickets; 60m: 3870 tickets.
- Breakeven: 10m needs 55.56% because win is +4 and loss is -5; 30m/60m need 54.05% because win is +4.25 and loss is -5.
- Result by horizon/symbol: BTC 10m 50.63% / -880, BTC 30m 50.69% / -612, BTC 60m 47.31% / -1206.25; ETH 10m 49.87% / -1015, ETH 30m 51.88% / -395, ETH 60m 48.86% / -929.5.
- Failure mode: forced every-minute betting is negative expectancy. 10m has no profitable score bucket; 60m is worst; 30m score >= 0.60 is strongly negative, likely chasing overextended moves.
- Positive pocket: 30m abs(score) 0.40-0.60 is +252.5 overall. The broader candidate after excluding BTC down and requiring 10m momentum aligned with the 30m direction is 527 tickets, 61.29% win rate, +352.75 hypothetical pnl; the more conservative UP-only subset is 385 tickets, 63.12% win rate, +322.75 hypothetical pnl.
- Next hypothesis implemented: `data_gate_30m_up_v1` records every BTC/ETH 10m/30m/60m signal but only virtually bets 30m UP signals with 0.40 <= score < 0.60 and momentum10 > 0. All other signals are stored as skip records for future review.

## 2026-07-31 15k Ticket Review

- Sample: runtime SQLite has about 15.9k total ticket rows; BTC/ETH rows are about 15.5k from 2026-07-28 16:52:58 to 2026-07-31 14:58:42. Latest settled BTC/ETH tickets checked: 15,340; open BTC/ETH tickets: 196.
- Result by horizon: 10m 5,159 tickets, 48.72% win rate, -3173.0 pnl; 30m 5,121 tickets, 49.85% win rate, -1989.75 pnl; 60m 5,060 tickets, 46.32% win rate, -3618.0 pnl. All are below payout breakeven.
- Result by symbol/horizon: BTC 10m 49.69% / -1360, BTC 30m 49.32% / -1120.75, BTC 60m 46.08% / -1863; ETH 10m 47.79% / -1801, ETH 30m 50.51% / -839, ETH 60m 46.68% / -1725.
- Failure mode: the app is still writing only `event_prediction_tickets`; no `event_prediction_signals` table exists in the runtime DB, so the running desktop build is still using forced every-cycle betting rather than the skip-gated strategy. Recent data after 2026-07-30 09:25:30 worsened sharply: 10m 43.65%, 30m 44.14%, 60m 43.82%.
- Settlement quality: some tickets settled very late, with max lateness about 69,366 seconds. Excluding tickets settled more than 120 seconds late does not change the conclusion: 10m 48.79%, 30m 50.31%, 60m 45.85%, all still negative.
- Candidate check: 30m abs(score) 0.40-0.60 is now only 54.04% and -1.0 pnl, effectively breakeven. The prior UP-only momentum gate is 470 tickets, 55.32%, +55.0 pnl overall, but after the previous review it collapsed to 81 tickets, 16.05%, -284.75 pnl; ETH was especially bad at 2 wins / 41 losses.
- Superseded hypothesis: the no-trade/skip direction below was aimed at simulated PnL protection, but the user clarified that the primary objective is full-volume data generation for agent training. Keep this result as a warning about raw score instability, not as the active implementation direction.

## 2026-07-31 Direction Dataset Correction

- Objective correction: the user wants every round to keep producing virtual direction tickets for LLM/agent training, not a no-trade filter. Strategy work should improve direction selection while preserving sample volume.
- Direction test on settled BTC/ETH tickets: current raw strategy is 48.23% on 15,403 decisive samples. Flipping all raw UP calls to DOWN improves the full sample to 52.22% and the post-review sample to 58.68%. Flipping raw UP calls plus abs(score) >= 0.60 improves the full sample to 53.04% and the post-review sample to 57.22%.
- Horizon split for raw-UP flip on post-review data: 10m 55.45% / -11.0 pnl, 30m 60.51% / +699.0 pnl, 60m 60.25% / +637.5 pnl. This is directional correction, not sample reduction.
- Implementation hypothesis: replace `data_gate_30m_up_v1` with `direction_dataset_v2`. Continue creating BTC/ETH 10m/30m/60m tickets every cycle; calibrate final direction by reversing raw UP calls and reversing strong raw DOWN calls when abs(score) >= 0.60; store `raw_score`, `raw_direction`, `final_score`, `final_direction`, `direction_flipped`, and `direction_reason` in `features_json`.
- Training data: add `scripts/export_event_training_jsonl.py` to export settled BTC/ETH tickets as structured JSONL or chat/messages JSONL for the agent training pipeline.
