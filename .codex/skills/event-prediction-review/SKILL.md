---
name: event-prediction-review
description: Review and improve GQT Trader event-contract virtual predictions. Use when the user asks about Binance event contract prediction, 10m/30m/1h virtual orders, prediction win rate, event prediction data collection, backtesting review, or tuning the local event prediction model.
---

# Event Prediction Review

Use this skill to review the GQT event prediction virtual-order log and propose measurable tuning changes. The product must remain virtual-only unless the user explicitly asks for real execution and the codebase supports a reviewed exchange API path.

## Workflow

1. Locate the SQLite log.
   - Desktop runtime: `%LOCALAPPDATA%\HillmanPick\GQT Trader\data\trading\user_data\event_predictions.sqlite`
   - Repo/dev fallback: `D:\x\gqt\trading\user_data\event_predictions.sqlite`
   - If neither exists, inspect the code path in `desktop/src/trading.rs` and report that no live data has been collected yet.
2. Run `scripts/export_event_review.py <sqlite-path>` to summarize results.
3. Inspect only `BTCUSDT` and `ETHUSDT`; ignore older records for unsupported symbols. Inspect horizon-level performance separately for `10m`, `30m`, and `60m`. Do not blend them into one headline number.
4. Review recent losses by reading `features_json`, `score`, `confidence`, `direction`, `stake_amount`, `virtual_pnl`, and `move_percent`.
5. Propose only measurable tuning changes, such as changing weights, adding a confidence floor, excluding low-volatility windows, or separating symbol-specific thresholds.
6. When the user asks about LLM/agent training data or factors, run `scripts/build_event_llm_training_pack.py <sqlite-path> --output-dir data/event_llm_training --strategy all --format both` from the repo root. Use the generated `manifest.json` and `factor_report.md` for the answer; do not commit generated JSONL artifacts.
7. Append a compact dated entry to `references/review-log.md` after each review. Include sample size, win rate by horizon, likely failure mode, and the next tuning hypothesis.

## Review Rules

- Treat fewer than 50 settled tickets per horizon as exploratory; do not claim stable edge.
- Binance-style event prediction in this project is scoped to `BTCUSDT` and `ETHUSDT` only.
- The current event prediction bankroll is unlimited virtual capital and each virtual ticket stakes 5 USDT. Losing tickets return 0 and realize `-stake`. Winning `10m` tickets return principal plus 80% profit (`+4.00` on 5 USDT stake). Winning `30m` and `60m` tickets return principal plus 85% profit (`+4.25` on 5 USDT stake). Open exposure is tracked for visibility but must not block new virtual tickets.
- Prefer settled tickets over open tickets. Open tickets are useful for operational checks only.
- Separate late settlements from on-time settlements when the review text shows large `settled ...s late` values.
- Do not delete or rewrite raw SQLite records during review.
- Do not optimize for one symbol if the same weights are shared across all symbols without stating that tradeoff.
- Do not use the skill file as a high-frequency data store. Raw tickets belong in SQLite; this skill stores process and compact review notes.

## Code Targets

- Prediction engine: `desktop/src/event_prediction.rs`
- App loop and dashboard: `desktop/src/app.rs`
- Workspace database path: `desktop/src/trading.rs`
- Review ledger: `references/review-log.md`
