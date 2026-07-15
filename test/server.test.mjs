import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeAutomationConfig,
  parseBacktestSummary,
  validStrategyName,
  validateRiskInput,
} from "../server.mjs";

test("strategy names stay within the strategy directory", () => {
  assert.equal(validStrategyName("MomentumV1"), true);
  assert.equal(validStrategyName("../escape"), false);
  assert.equal(validStrategyName("bad-name"), false);
});

test("risk input validates and normalizes numbers", () => {
  assert.deepEqual(
    validateRiskInput({ stakeAmount: "50", maxOpenTrades: "3", liquidationBuffer: "0.15" }),
    { stakeAmount: 50, maxOpenTrades: 3, liquidationBuffer: 0.15 },
  );
  assert.throws(() => validateRiskInput({ stakeAmount: 1, maxOpenTrades: 3, liquidationBuffer: 0.15 }));
});

test("backtest output parser extracts summary metrics", () => {
  const output = `
│ Total/Daily Avg Trades                 │ 24 / 0.43 │
│ Total profit %                         │ 2.34% │
│ Absolute profit                        │ 23.4 USDT │
│ Max % of account underwater            │ 4.20% │
│ Sharpe (closed trades)                 │ 1.18 │
│ Profit factor                          │ 1.42 │`;
  assert.deepEqual(parseBacktestSummary(output), {
    trades: 24,
    profitPercent: 2.34,
    absoluteProfit: "23.4 USDT",
    maxDrawdown: "4.20%",
    sharpe: 1.18,
    profitFactor: 1.42,
  });
});

test("automation settings use bounded healthcheck intervals", () => {
  assert.deepEqual(normalizeAutomationConfig({ enabled: 1, autoRestartDryRun: true, healthcheckSeconds: 5 }), {
    enabled: true,
    autoRestartDryRun: true,
    healthcheckSeconds: 15,
  });
  assert.equal(normalizeAutomationConfig({ healthcheckSeconds: 999 }).healthcheckSeconds, 300);
});
