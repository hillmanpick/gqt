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

## 2026-08-03 Data Sufficiency Review

- Sample: runtime SQLite has 29,057 BTC/ETH tickets, including 28,861 settled and 196 open. The active `direction_dataset_v2` subset has 13,172 settled samples from 2026-07-31 15:31:34 to 2026-08-03 11:11:01; about 4.3k-4.4k settled samples per horizon.
- All settled BTC/ETH tickets by horizon: 10m 9,666 tickets, 50.90% win rate, -4052.0 pnl; 30m 9,628 tickets, 52.12%, -1718.5 pnl; 60m 9,567 tickets, 49.65%, -3896.75 pnl.
- Active `direction_dataset_v2` by horizon: 10m 4,436 tickets, 53.11%, -974.0 pnl; 30m 4,398 tickets, 54.11%, +20.75 pnl; 60m 4,338 tickets, 53.55%, -201.5 pnl. This is materially better than legacy/raw but not a stable edge yet; 10m is still below 55.56% breakeven and 30m/60m are around the 54.05% breakeven line.
- Failure mode: the active rule's strong raw-DOWN reversal into final UP is weak. In v2, final UP tickets are only 81/123/130 decisive samples for 10m/30m/60m and win 32.10% / 46.34% / 33.85%, while final DOWN tickets are 53.50% / 54.33% / 54.16%.
- Counterfactual: keeping full sample volume but mapping every v2 prediction to DOWN would score 10m 53.79% / -705.0 pnl, 30m 54.33% / +112.5 pnl, and 60m 54.54% / +195.5 pnl. Next measurable hypothesis: `direction_dataset_v3` should remove the strong raw-DOWN-to-UP reversal, keep all BTC/ETH 10m/30m/60m tickets, and compare v2 vs v3 on a held-out rolling window before changing stake or reducing samples.

## 2026-08-03 LLM Training Pack + Factor Pipeline

- Generated local training artifacts from runtime SQLite using `scripts/build_event_llm_training_pack.py`; sample has 29,173 settled BTC/ETH rows from 2026-07-28 08:52:58 UTC to 2026-08-03 04:04:07 UTC.
- Split: train 20,421 / validation 4,376 / test 4,376, chronological to reduce leakage. Strategy mix: 15,689 `legacy_raw` and 13,484 `direction_dataset_v2`.
- Also generated a current-strategy pack with 13,260 `direction_dataset_v2` rows: train 9,282 / validation 1,989 / test 1,989.
- Factor report confirms broad realized down bias: realized up rate is 46.88% for 10m, 46.53% for 30m, and 45.70% for 60m. Strongest all-sample directional factors are mostly contrarian/trend-exhaustion: `snapshot_change_percent` high favors DOWN for all horizons; 60m `ema_long`, `ret60`, and `momentum60` high also favor DOWN.
- Current-strategy factor report strengthens the same conclusion: v2 `snapshot_change_percent` high favors DOWN by -15.45% / -26.44% / -49.34% high-minus-low up-rate edge for 10m/30m/60m, while strong raw-DOWN-to-final-UP remains a weak group.
- Training output is local-only under `data/event_llm_training` and must not be committed. Keep `event_test.*.jsonl` untouched for scoring the trained/fine-tuned model later.

## 2026-08-03 Direction Dataset v3 Implementation

- Objective: keep full-volume virtual sampling while improving direction judgment. No skip/no-trade gate was added.
- Backtest on settled `direction_dataset_v2` decisive rows before implementation: default DOWN scored 10m 54.39% / -472.0 pnl, 30m 55.16% / +456.75, 60m 55.38% / +544.0. Active v2 scored 53.84% / -697.0, 54.96% / +373.5, 54.43% / +155.5 because strong raw-DOWN-to-UP remained weak.
- Implemented `direction_dataset_v3`: default DOWN; remove strong raw-DOWN-to-UP reversal; only call UP on measured oversold rebound pockets. 10m UP condition is 24h change <= -1.0, `rsi_trend <= -0.30`, and `sentiment >= 0.30`; 30m UP condition is 24h change <= -2.0 and `sentiment >= 0.50`; 60m UP condition is 24h change <= -3.0.
- V2 counterfactual for the v3 rule: 10m 55.79% / +94.0 pnl, 30m 58.84% / +1982.25, 60m 61.78% / +3161.0; all horizons combined 58.78% / +5237.25. Treat this as a hypothesis to forward-test, not guaranteed live edge.
- UI changed so main event cards show current strategy (`direction_dataset_v3`) statistics and current-strategy realized PnL, with all-history stats shown separately as reference.

## 2026-08-03 v3 Operational Check

- UI check: event page is showing current strategy `direction_dataset_v3`; main cards use current-strategy stats while the lower row shows all-history reference stats.
- Runtime check: `direction_dataset_v3` has started writing open BTC/ETH 10m/30m/60m tickets. At check time it had 24 open tickets and 0 settled tickets, so v3 win-rate cards correctly show 0.0% until the first 10m tickets settle.
- Next review trigger: wait for at least 50 settled v3 tickets per horizon before treating the displayed v3 win rates as directional evidence.

## 2026-08-03 v3 High Win-Rate Sanity Check

- Runtime check: `direction_dataset_v3` has 1,024 settled tickets and 191 open tickets from 2026-08-03 12:18:17 to 15:43:38 local time; current-strategy virtual PnL is +1238.75.
- Horizon result: 10m 385 tickets, 53.51%, -71.0 pnl; 30m 349 tickets, 72.78%, +604.5 pnl; 60m 290 tickets, 80.34%, +705.25 pnl. Settlement latency looks normal for this run: average about 31-34s, max 216s.
- Interpretation: the high 30m/60m win rate is real in the local virtual ledger, but it is a short-window regime result. It mainly comes from v3's DOWN bias matching a down/mean-reverting market; do not treat it as stable long-term edge yet.
- Failure pocket: 10m UP rebound condition remains weak: 130 tickets, 42.31%, -155.0 pnl. Next tuning candidate, after more samples, is to tighten or remove 10m UP while leaving full-volume sampling intact.

## 2026-08-03 Event Prediction UI Operation Update

- Scope: no direction-strategy tuning and no SQLite raw-record rewrite. This was an operational UI change for easier review and manual sampling.
- Change: the event page now shows compact recent-order cards instead of embedding hard-to-scroll wide tables. Clicking a ticket opens a full detail popup; the "打开大列表" popup keeps the complete current visible table available when needed.
- Change: "立即跑一轮" now opens a 10m/30m/1h selector first. After confirmation, the app runs BTC/ETH for the selected horizons and writes the actual strategy directions back to the status line, e.g. `10m：BTC 买跌，ETH 买涨`.
- Data rule: automatic every-minute collection remains full-volume BTC/ETH 10m/30m/60m; manual selection only affects the one forced run.

## 2026-08-03 Manual Direction Popup Update

- Scope: UI/operation only; no strategy tuning and no SQLite raw-record rewrite.
- Change: after a manual "立即跑一轮" completes, the app opens a result popup titled "本轮应该买什么". It lists selected event windows with local open/close time, BTC/ETH, buy-up/buy-down direction, confidence, and whether that minute's virtual ticket was newly written or already existed.
- Data rule: the popup is manual-run only. Automatic every-minute BTC/ETH 10m/30m/60m collection continues silently and will not spam popups.

## 2026-08-03 v3 Recent Drawdown Review

- Sample: runtime `direction_dataset_v3` has 1,887 BTC/ETH tickets from 12:18 to 17:38 local time: 1,695 settled and 192 open. Overall current-strategy settled PnL is still positive at +1,379.5 USDT, but the latest hour has turned negative.
- Overall by horizon: 10m 609 settled, 46.47%, -498.0 pnl; 30m 571, 67.25%, +697.0; 60m 515, 78.83%, +1,180.5. 10m is below its 55.56% breakeven and is the persistent weak horizon.
- Since 15:43 local: 10m 226 settled, 34.96%, -419.0; 30m 224, 58.93%, +101.0; 60m 227, 77.09%, +483.75. Since 17:00 local specifically: 10m 11.84%, -299.0; 30m 12.16%, -286.75; 60m 37.18%, -121.75.
- Failure mode: 17:00 local regime flipped into an upward rebound. Latest 30m realized average moves were positive across horizons (+0.057% 10m, +0.201% 30m, +0.295% 60m), while v3 was still mostly DOWN. The worst recent bucket is `raw_up_contrarian_or_exhaustion_down_v3` on 10m: 130 tickets since 15:43, 31.54%, -281.0.
- Settlement quality: after 17:00 local average lateness was about 9-10 seconds by horizon, max 67 seconds, and no ticket exceeded 120 seconds late, so the drawdown is not explained by late settlement.
- Next tuning hypothesis: do not reduce sampling. Measure a v4 regime detector that keeps full BTC/ETH 10m/30m/60m tickets but stops forcing DOWN when short-term rebound momentum and RSI are already positive after a 24h drawdown; 10m needs separate treatment from 30m/60m.

## 2026-08-03 Active Horizon and Data Cadence Update

- Operational change: retire 10m from the active event-contract workflow because recent live-forward samples showed it dragging headline operation performance. New automatic and manual virtual tickets should now be BTC/ETH 30m and 60m only; old 10m SQLite records remain untouched and may still settle silently if already open.
- Active headline stats and training-pack exports should exclude retired 10m rows. Historical review notes keep 10m results only as legacy evidence, not as the current operating target.
- Normal futures/AI trading data cadence changed to favor more samples: `one_signal_per_candle` defaults to false and existing runtime configs/strategies are migrated away from `process_only_new_candles = True`, so signal collection is no longer blocked by the old per-candle timing gate.
