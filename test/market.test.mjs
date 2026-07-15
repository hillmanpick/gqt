import test from "node:test";
import assert from "node:assert/strict";

import { calculateSentiment } from "../lib/market.mjs";

test("sentiment score is neutral for a balanced market", () => {
  assert.deepEqual(calculateSentiment({ changePercent: 0, fundingRate: 0, longShortRatio: 1 }), {
    score: 50,
    label: "中性",
    components: { trend: 0, positioning: 0, funding: 0 },
  });
});

test("sentiment score clamps extreme inputs", () => {
  const bullish = calculateSentiment({ changePercent: 100, fundingRate: 1, longShortRatio: 10 });
  const bearish = calculateSentiment({ changePercent: -100, fundingRate: -1, longShortRatio: 0 });
  assert.deepEqual({ score: bullish.score, label: bullish.label }, { score: 98, label: "极度贪婪" });
  assert.deepEqual({ score: bearish.score, label: bearish.label }, { score: 2, label: "极度恐慌" });
});
