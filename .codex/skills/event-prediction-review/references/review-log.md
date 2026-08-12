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

## 2026-08-04 v3 Rebound Drawdown and v4 Runtime Switch

- Sample: runtime `direction_dataset_v3` active BTC/ETH rows after 10m retirement had 30m 1,426 settled tickets at 49.16% win rate / -640.75 pnl, and 60m 1,366 settled tickets at 51.76% / -290.25. Both are below the 54.05% breakeven for +85% payout.
- Recent failure: last 6 hours by open time fell to 30m 35.99% / -1,169.75 and 60m 34.37% / -1,170.75. The last 3 hours were still mostly rebound/up expiries while v3 continued to write DOWN, so the loss was directional regime lag rather than settlement latency or 10m drag.
- Factor finding: static v3 over-relied on default DOWN and flipped raw-UP signals into DOWN. Counterfactual raw direction improved the last 3h to positive, but the latest 1h remained noisy; pure "always UP" fit the rebound window but was not stable historically.
- Implemented `direction_dataset_v4`: keep full-volume BTC/ETH 30m/60m virtual tickets, add a rolling settled-result regime bias using the last 80 same-horizon settled rows with min 40 samples (`up_rate >= 58%` => UP, `<= 42%` => DOWN), then fall back to static v4 factor confirmation that no longer blindly flips confirmed raw-UP trends to DOWN.
- Runtime check after restart: first v4 open cycle at 2026-08-04 00:58 local wrote BTC/ETH 30m and 60m as `rolling_up_regime_v4`; 30m rolling up-rate was 0.60 and 60m was 0.825. Treat v4 as a new forward test; wait for at least 50 settled rows per horizon before judging displayed v4 win rates.

## 2026-08-04 Local Direction Model Training Baseline

- Training pack refreshed from runtime SQLite with 22,989 active BTC/ETH 30m/60m records: train 16,092, validation 3,448, test 3,449. `direction_dataset_v4` had only 436 records, so v4-only machine learning remains exploratory.
- Implemented local NumPy logistic-regression trainer `scripts/train_event_direction_model.py`. It trains separate 30m and 60m classifiers, preserves chronological splits, and adds offline walk-forward regime features using only same-horizon rows that had already closed before each sample opened.
- Result: 30m validation 61.58% / +1,199.25 pnl but test 48.03% / -976.50; 60m validation 62.01% / +1,268.25 but test 49.20% / -760.50. This is below the 54.05% breakeven on the untouched test split, so the trained model should not be deployed as the sole direction engine yet.
- Next hypothesis: keep collecting v4 samples and retrain after at least several thousand v4 settled rows, or move from linear logistic regression to a tree/boosted-table model once a stable local dependency is available.

## 2026-08-04 Hybrid Direction Trainer Optimization

- Updated `scripts/train_event_direction_model.py` from a single logistic baseline to `event_direction_hybrid_numpy_v2`: each horizon now trains logistic regression plus a bounded short-vs-long walk-forward regime-rule search. The search is fixed-size and vectorized so the full run completes locally.
- Full run result on the same refreshed pack: 30m still has no deployable candidate. Best 30m logistic reached validation 62.80% / +1,393.50 but test only 49.40% / -754.50; best 30m regime rule reached validation 61.23% / +1,143.75 but test 47.35% / -1,087.50.
- 60m improved: the selected regime rule (`horizon 20/80`, `up >= 0.50`, `down <= 0.45`, `delta = 0.05`, fallback baseline) reached validation 62.70% / +1,379.25 and test 58.17% / +645.50, both above the 54.05% breakeven. Treat this as a candidate for 60m-only forward testing, not as proof that 30m is fixed.

## 2026-08-04 direction_dataset_v5 Runtime Switch

- Implemented `direction_dataset_v5` as a forward-test strategy after the hybrid trainer completed. The only trained rule promoted to runtime is 60m: if the last 20 same-horizon settled rows have an up-rate at least 0.50 and beat the last 80 by at least 0.05, call UP; if the last 20 up-rate is at most 0.45 and trails the last 80 by at least 0.05, call DOWN. Otherwise fall back to the static direction logic.
- 30m was not promoted to a trained rule because neither logistic nor regime-rule candidates passed the untouched test split. It keeps the v4-style rolling 80-row regime logic under the v5 strategy label so new rows can be reviewed separately.
- Runtime check: after release restart at 2026-08-04 04:39 local, the first v5 cycle wrote BTC/ETH 60m as `long_horizon_20_80_down_regime_v5` with short up-rate 0.25 and long up-rate 0.525; BTC/ETH 30m wrote `rolling_up_regime_v5` with up-rate 0.6625. Wait for at least 50 settled v5 rows per horizon before judging live-forward win rate.

## 2026-08-04 v5 Forward Review and v6 Direction Gate

- Sample: 2,504 settled `direction_dataset_v5` BTC/ETH rows from 2026-08-03 20:39 UTC to 2026-08-04 07:29 UTC; 30m 1,280 and 60m 1,224. This is enough for an exploratory tuning decision, but it is still one short market regime.
- Result: v5 30m was 53.59% / -54.50 USDT; v5 60m was 48.86% / -588.50 USDT. By symbol, 30m BTC/ETH were 52.34% / 54.84%, while 60m BTC/ETH were 49.02% / 48.69%.
- Failure mode: UP calls were the drag. 30m UP was 46.46% versus DOWN 58.09%; 60m UP was 30.24% versus DOWN 58.23%. The worst group was 60m `rolling_up_regime_v5` at 15.17% (178 rows); settlement latency was normal and did not explain the losses.
- Counterfactual: keeping every ticket but mapping 30m to DOWN scored 56.33% / +269.25 USDT. For 60m, retaining UP only when `raw_score >= 0.80` scored 62.66% / +974.75 USDT; all other rows DOWN scored 62.09% / +910.00 USDT.
- Implementation: `direction_dataset_v6` keeps full-volume BTC/ETH 30m/60m collection, disables weak 30m UP paths, and requires the 60m raw-score upper tail (`>= 0.80`) before any UP regime/rebound/trend signal can pass. The training-pack `current` filter now points to v6.
- Next hypothesis: forward-test v6 for at least 500 settled rows per active horizon and re-evaluate the 60m `0.80` gate on a fresh time window before relaxing the 30m gate.
- Runtime check: after rebuilding and restarting the desktop client, the first v6 minute wrote all four BTC/ETH 30m/60m tickets with `direction_dataset_v6`; the immediately preceding minute remained v5, so the strategy boundary is unambiguous.

## 2026-08-05 v6 Forward Performance Diagnosis

- Sample: 1,845 decisive settled v6 30m tickets and 1,788 decisive settled v6 60m tickets from 2026-08-04 16:27 local through 2026-08-05 08:17/07:47 local. This exceeds the exploratory 50-ticket threshold, but the one-minute overlapping tickets are highly correlated and do not represent the same number of independent observations.
- Result by horizon: 30m 50.57% / -594.75 USDT versus completed v5 50.36% / -475.00; 60m 41.33% / -2,104.25 versus completed v5 45.25% / -1,131.75. BTC and ETH were nearly identical, so the failure is shared strategy logic rather than one symbol.
- Failure mode: v6 was tuned on a short bearish v5 window, then kept nearly all tickets DOWN while the v6 60m realized-UP rate rose to 58.17%. It wrote only 27 UP tickets out of 1,788; those extreme-score UP tickets also failed at 33.33%, showing that the `raw_score >= 0.80` tail was overfit and mean-reverted in forward data.
- Regime issue: combined-symbol rolling windows use heavily overlapping 60m outcomes. The 60m down-regime groups won only 34.17%-40.60%, and high-confidence 60m tickets (`>=80%`) won 35.71%, so regime-derived confidence is miscalibrated. Normal settlement latency rules out delayed settlement as the cause.
- Counterfactual: 30m inverse-raw direction scored 61.73% overall and 60.8% / 70.0% / 57.8% on chronological 70/15/15 splits. No single 60m constant direction was stable: always-UP scored 66.3% early but 38.1% / 40.5% later; inverse-raw improved to 82.5% / 70.3% later but only 47.3% early.
- Next hypothesis: do not tune another constant 60m direction. Backtest a symbol-specific, non-overlapping regime series and calibrate confidence out-of-sample; forward-test the robust-looking 30m inverse-raw rule separately before any runtime switch.

## 2026-08-05 v7 Reversal-Aware Design Review

- Sample refresh: current v6 pack has 1,869 decisive 30m rows and 1,810 decisive 60m rows. Chronological local training on v6 produced a 30m logistic candidate at 72.10% validation but only 54.10% test / +1.25 USDT; 60m logistic fell to 51.82% test and no 60m model was deployable.
- Static-rule rejection: contrarian raw-score thresholds performed well on v4/v6 but lost on v2/v3/v5, so replacing v6 with another fixed UP/DOWN or fixed score threshold would repeat the same regime overfit.
- 30m reversal pockets: short/medium divergence is measurable. `momentum3 >= 0.20 && momentum30 <= -0.30` predicted UP at 57.0% on v2-v5 and 72.8% on v6; `momentum3 <= -0.10 && momentum30 >= 0.20` predicted DOWN at 57.3% on v2-v5 and 69.6% on v6. These are suitable fast reversal overrides, not a complete strategy by themselves.
- 60m candidate: a per-symbol online expert selector using one non-overlapping settled observation per hour and the last 8 observations scored 54.15% on v2-v5 and 57.90% on v6. The experts are constant UP, constant DOWN, raw direction, and inverse-raw direction; current combined-symbol 20/80 hard overrides should be removed.
- v7 architecture hypothesis: keep BTC/ETH and 30m/60m state separate; let current candle divergence trigger a debounced transition state, use non-overlapping settled labels to select/confirm the active expert, and make regime output advisory rather than an unconditional direction override. Confidence must come from recent out-of-sample expert accuracy and be capped while the state is transitioning.
- Promotion rule: first write v7 candidate direction/reason/state as shadow fields on every virtual ticket. Promote only after at least 500 settled rows per horizon, with time-blocked validation and both BTC/ETH above the 54.05% payout breakeven; 30m and 60m may be promoted independently.

## 2026-08-05 direction_dataset_v7 Runtime Switch

- Implemented and restarted `direction_dataset_v7`: BTC/ETH state is separated, 30m/60m expert history uses non-overlapping symbol-specific settled samples, fast 30m reversal overrides are enabled, and the old combined 20/80 hard override is removed.
- Runtime boundary: signals through 10:13 local are v6; the 10:15 and 10:16 local cycles write all four BTC/ETH 30m/60m signals with `direction_dataset_v7`.
- Evaluation gate: v7 has no settled sample yet. Keep the virtual-only forward test running and wait for at least 500 settled rows per active horizon, then require time-blocked validation and both symbols above the 54.05% payout breakeven before promoting any narrower expert rule.

## 2026-08-06 direction_dataset_v8 Runtime Switch

- Sample refresh: v7 settled 30m 2,921 rows at 51.42% and 60m 2,863 rows at 51.90%; the full active history was 30m 50.98% and 60m 48.94%. UP predictions remained weak, especially v7 `always_up` and raw experts.
- Strict no-leakage walk-forward selection found shorter non-overlapping expert windows more responsive: 30m window 12 scored 57.16% and 60m window 5 scored 56.67% on the v7 forward rows. The 30m bottom-reversal UP override scored only 43.48% and was removed; top-reversal DOWN remains enabled.
- Implemented and restarted `direction_dataset_v8` with those settings. Runtime boundary: 11:51 local is v7; 11:52 local BTC/ETH 30m/60m signals are v8.
- Evaluation gate: keep v8 virtual-only and require at least 500 settled rows per horizon plus both BTC/ETH above the 54.05% payout breakeven before further promotion.
- Correction: the first v8 patch only removed the explicit bottom-reversal UP override; it could still select UP later through the expert fallback. The running build now hard-blocks that pattern to DOWN with reason `bottom_reversal_up_blocked_v8`. Runtime restarted at 12:22 local and is writing `direction_dataset_v8`.

## 2026-08-07 v8 Cadence and Win-Rate Diagnosis

- Cadence: the enabled loop runs at most once every 60 seconds and attempts four virtual tickets per minute: BTCUSDT/ETHUSDT x 30m/60m. The unique `(symbol, horizon_minutes, open_time)` index prevents duplicates within the same minute.
- Sample: v8 has 2,675 settled 30m tickets at 49.21% / -1,197.00 USDT and 2,625 settled 60m tickets at 39.70% / -3,486.50 USDT. Direction is the main split: 30m DOWN 53.35% versus UP 42.46%; 60m DOWN 46.44% versus UP 25.67%.
- Failure mode: confidence is not empirically calibrated. Settled 60m UP tickets averaged 71.79% displayed confidence while winning only 25.67%. The worst large reason buckets were 60m `online_expert_always_up_v7` at 11.31% and 30m `online_expert_always_up_v7` at 36.69%.
- Dependence: minute-level tickets overlap heavily and are not independent observations. Selecting one ticket per non-overlapping horizon bucket reduced the effective sample to 100 for 30m (52.00%) and 50 for 60m (48.00%), so raw ticket counts materially overstate statistical evidence.
- Settlement quality: 167 v8 30m and 226 v8 60m tickets settled more than 120 seconds late, with maximum lateness 11,273 seconds. The current recovery path uses the restart-time snapshot for overdue tickets instead of each scheduled close-time price; late rows should be re-priced from historical 1m candles or excluded from headline evaluation.
- Next measurable change: keep minute-level rows for training, but report non-overlapping evaluation cohorts; settle overdue rows from historical close-time candles; calibrate confidence from held-out expert accuracy; and require a larger non-overlapping sample plus breakeven evidence before an expert can emit high-confidence UP predictions.

## 2026-08-07 direction_dataset_v9 Commitment Rule

- User-facing problem: v8 combined contradictory raw, reversal, and online-expert outputs, so the aggregate result stayed near random instead of expressing a stable directional regime.
- Offline walk-forward check on on-time v8 rows: a per-symbol/per-horizon commitment using the last 8 non-overlapping settled samples, allowing UP only at `up_rate >= 0.65` and otherwise committing DOWN, scored BTC 30m/60m at 54.06%/56.78% and ETH 30m/60m at 51.56%/55.62%.
- Implementation: promote `direction_dataset_v9`; use 30m/60m non-overlap spacing, minimum 8 samples, a 65% UP commitment gate, and make the commitment override raw-score and fast-reversal branches. Confidence remains tied to observed regime strength rather than being artificially raised.
- Evaluation gate: v9 remains virtual-only. Wait for at least 500 settled rows per active horizon and require both BTC/ETH to clear the 54.05% payout breakeven before further direction changes.

## 2026-08-07 direction_dataset_v10 Extreme Signal Gate

- User requirement: do not publish a middle win-rate strategy; only count tickets with evidence targeting above 70% or below 30% realized accuracy.
- Backtest finding: widening the v9 commitment gate alone still produced too many medium-quality DOWN tickets. The strategy must abstain from ticket creation when the rolling evidence is not extreme.
- Implementation: promote `direction_dataset_v10`; keep all candidate signals in `event_prediction_signals`, but create a virtual ticket only when at least 8 non-overlapping settled samples exist and their up-rate is `>= 0.75` or `<= 0.25`. Middle states are recorded as `skip` and excluded from ticket win-rate stats.
- Correction: the first v10 build required 20 samples while the history query could return only about 6-8 non-overlapping 60m samples from its 400-row limit, so it skipped every ticket. The running fix aligns the startup gate and sample window at 8; treat the first 50 accepted tickets per horizon as exploratory.

- Runtime verification after correction: v10 produced 1 accepted ticket (ETHUSDT 30m, 8-sample up-rate 0.75) and skipped 63 middle candidates in the first observed cycles. The gate is active; the accepted-sample win rate remains unproven until enough tickets settle.

## 2026-08-08 v10.1 Cold-Start Gate Fix

- Incident: the v10 runtime sampler was configured with an 8-sample window while the intended extreme gate required 20 samples. It therefore reported sample size 8 and skipped every later signal after the initial accepted rows.
- Fix: v10.1 uses 20 non-overlapping settled samples for both active horizons. The one-minute evaluation loop continues running; only ticket creation is gated by `up_rate >= 0.75` or `<= 0.25`.
- Runtime check: before the fix, v10 had 3,269 skips and no new ticket after 11:24. v10.1 was rebuilt, all 34 tests passed, and the release client was restarted. Current rolling rates are BTC 30m 0.60, BTC 60m 0.50, ETH 30m 0.60, ETH 60m 0.45, so no immediate ticket is expected until an extreme regime appears.

## 2026-08-08 v10.2 Restore Full-Volume Virtual Tickets

- User correction: every minute must continue producing virtual tickets; the extreme gate must not stop the training sample stream.
- Implementation: promote `direction_dataset_v10_2`; every candidate is recorded as a `trade` and creates a ticket. The decision reason labels the state as `extreme_up`, `extreme_down`, or `balanced` using the 20-sample 75%/25% thresholds. This preserves full-volume collection while keeping extreme-state performance separable in review.
- Runtime: release v10.2 was built, tested with all 34 tests passing, and restarted. Active cadence remains BTC/ETH x 30m/60m once per minute; no real Binance order path is introduced.

## 2026-08-10 Inverse-Direction Review

- Sample: current `direction_dataset_v10_2` snapshot has 2,078 settled 30m tickets at 49.83% / -811.25 USDT and 2,018 settled 60m tickets at 46.23% / -1,459.75 USDT. With the +85% payout, breakeven is 54.05%.
- Counterfactual: reversing every settled ticket gives 30m 50.17% / -746.50 USDT and 60m 53.77% / -53.75 USDT. BTC 60m is only 54.21% / +14.75 USDT while ETH 60m is 53.32% / -68.50 USDT, so there is no cross-symbol inverse edge.
- Failure mode: negative original PnL is compatible with both directions losing because the payout house edge creates a 45.95%-54.05% band where the original and inverse strategies are both unprofitable. Overlapping minute tickets and late settlement rows further weaken the apparent sample.
- Next hypothesis: do not globally flip direction; evaluate symbol/horizon and on-time non-overlapping cohorts before any direction change, and require held-out inverse performance above 54.05% with margin.

## 2026-08-10 Extreme-Outcome Gate Feasibility

- Implementation check: v10.2 computes `extreme_up`/`extreme_down` at 75%/25% rolling regime rates, but labels `balanced` rows as `trade` too (`full_volume_*`), so the extreme condition is currently descriptive rather than a bet gate.
- Feasibility: an actual extreme-outcome strategy requires abstaining from balanced directional tickets; all-minute data collection can continue as candidate signals, while only extreme candidates count as official virtual bets.
- Risk: a 20-sample 75%/25% rolling rate cannot guarantee future realized win/loss below 30%; use a larger non-overlapping window, held-out validation, and a payout-aware 54.05% profitability gate.

## 2026-08-10 Extreme-State Data Sufficiency

- Current snapshot: v10.2 has roughly 2,150 signals per active horizon, but rolling regime history reaches only 14-19 samples for 30m and 7-11 for 60m; no signal satisfies the 20-sample extreme gate.
- Settled-state evidence: about 4,120 settled rows are labelled `balanced`; `extreme_up` and `extreme_down` have zero settled tickets. The extreme strategy therefore has no direct outcome sample yet.
- Assessment: total raw volume is enough for exploratory diagnostics, but not enough to support an extreme win/loss claim. Effective non-overlapping history remains about 72 30m and 36 60m observations, with 60m below the 50-sample exploratory threshold.
- Next hypothesis: accumulate regime history and validate extreme states separately; do not train or promote an extreme gate from the aggregate balanced-ticket PnL.

## 2026-08-10 Restore 10m Horizon

- Implementation: restored `TenMinutes` to the active horizon set, automatic and manual cycles, dashboard statistics, open/settled queries, and run-direction summaries.
- Runtime: the latest release client is running and has created BTC/ETH `10m`, `30m`, and `60m` v10.2 tickets in the shared SQLite log.
- Payout: 10m keeps its existing `+80%` virtual profit rate (`+4.00 USDT` on the 5 USDT stake); 30m/60m remain at `+85%`.

## 2026-08-11 Three-Horizon Training Package

- Package: exported 68,126 settled BTC/ETH labels with chronological 70/15/15 splits: 47,688 train, 10,219 validation, and 10,219 untouched test rows. Horizon counts are 13,913 10m, 27,078 30m, and 27,135 60m.
- Baseline: 10m 51.40% / -5,207.00 USDT, 30m 50.67% / -8,463.50 USDT, and 60m 48.87% / -13,018.50 USDT. Breakeven remains 55.56% for 10m and 54.05% for 30m/60m.
- Data quality: all raw rows remain useful for archival and direction labels, but 250 10m, 748 30m, and 965 60m settlements were more than 120 seconds late; minute-overlapping tickets must not be treated as independent evaluation samples.
- Next hypothesis: train and report each horizon separately, preserve strategy version and settlement lateness as cohort metadata, and keep the chronological test split untouched until model selection is complete.

## 2026-08-11 Held-Out Direction Model Check

- Sample: trained the local numeric-factor model on the chronological package. The current trainer covers 30m and 60m only; the restored 10m horizon has not yet been model-tested.
- Result by horizon: 30m logistic scored 56.02% / +919.00 USDT on validation but fell to 47.33% / -2,133.00 on held-out test; 60m logistic scored 55.50% / +693.25 on validation and 51.38% / -834.75 on test. Regime rules also lost on both held-out horizons (30m 51.15%, 60m 51.85%). No candidate is deployable.
- Failure mode: the apparent validation edge does not survive the later market block. Two weeks of highly overlapping minute tickets and weak derived factors capture short-lived regimes rather than a stable greater-than-70% or less-than-30% direction edge.
- Packaging cautions: record/chat inputs currently include post-settlement `baseline.correct`, so they must not be used directly for LLM fine-tuning. The chronological split also lacks a horizon-sized purge gap, and offline regime history uses scheduled `close_time` rather than actual `settled_at`; remove these leakage paths before treating future held-out metrics as promotion evidence. The NumPy logistic trainer does not consume `baseline.correct`, so that direct leak does not explain this run's weak result.
- Next hypothesis: do not tune or deploy another threshold from this pack. Add point-in-time order-book imbalance, aggressive buy/sell flow, spread/depth, liquidation and volatility-structure inputs; collect multiple market regimes; then evaluate 10m/30m/60m separately on non-overlapping walk-forward cohorts.

## 2026-08-12 Extreme-Accuracy Implementation Roadmap

- Live sample: 10m has 15,872 settled tickets at 51.57% / -5,696.00 USDT, 30m has 29,033 at 50.17% / -10,440.00, and 60m has 29,090 at 48.31% / -15,453.25. Average displayed confidence is 57.70% / 63.25% / 65.11%, so confidence is materially miscalibrated.
- Failure mode: the event engine currently relies mainly on OHLCV-derived momentum/trend plus slow snapshot levels. It ignores available kline taker-buy volume/trade count and lacks point-in-time order-book, aggressive-flow, OI-change, basis and liquidation features. Rounded open times, possibly incomplete candles, delayed current-price settlement, and leaked package fields weaken label and evaluation integrity.
- Next hypothesis: keep every-minute candidates for collection, but evaluate an official extreme subset separately. First fix decision/entry/expiry timestamps and purged walk-forward packaging; then add taker flow, book/depth imbalance, spread/microprice, OI deltas, basis and liquidation features. Promote per horizon only after 300-500 non-overlapping accepted forward signals show observed accuracy above 70%, a 95% lower bound above payout breakeven, positive PnL, and acceptable results for both BTC and ETH.

## 2026-08-12 Five-Ticket Reinvestment Cycles

- Implementation: `event_reinvest_cycle_v1` changes virtual execution to independent serial BTC/ETH chains for 10m, 30m, and 60m. Each numbered cycle starts at 5 USDT and allows at most five settled tickets; only one ticket may remain open per symbol/horizon chain.
- Payout flow: a win reinvests the full settlement return into the next ticket (10m `5.00 -> 9.00`; 30m/60m `5.00 -> 9.25`), a loss ends the cycle and resets the next cycle to 5 USDT, and a tie advances with the unchanged stake. Five completed tickets always close the cycle and reset the next one to 5 USDT.
- Visibility: ticket rows persist cycle ID, cycle number, order number, stake, and settlement return. The history card, full history list, and detail dialog show the cycle progress and compounding amounts; the full list is responsive and has explicit close/Escape handling.
- Verification: all 39 Rust tests pass, including win compounding, loss reset, one-open-ticket gating, five-ticket termination, and one-time schema migration. The release client migrated the live database to schema version 1 with all 75,467 ticket rows preserved, no duplicate cycle orders/open chains, and legacy open tickets left to settle naturally before each chain starts `C000001` at 5 USDT.
