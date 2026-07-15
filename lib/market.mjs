const BINANCE_BASE = process.env.BINANCE_FUTURES_BASE ?? "https://fapi.binance.com";
const SYMBOL_PATTERN = /^[A-Z0-9]{5,20}$/;
const INTERVALS = new Set(["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h", "12h", "1d"]);

function validateSymbol(symbol) {
  if (!SYMBOL_PATTERN.test(symbol)) throw new Error("合约代码无效");
  return symbol;
}

function validateInterval(interval) {
  if (!INTERVALS.has(interval)) throw new Error("K线周期无效");
  return interval;
}

async function getJson(pathname, params = {}) {
  const url = new URL(pathname, BINANCE_BASE);
  Object.entries(params).forEach(([key, value]) => url.searchParams.set(key, String(value)));
  const response = await fetch(url, {
    headers: { "User-Agent": "GQT/1.0" },
    signal: AbortSignal.timeout(12_000),
  });
  if (!response.ok) throw new Error(`Binance market API returned ${response.status}`);
  return response.json();
}

export async function getCandles(symbol, interval, requestedLimit = 300) {
  const limit = Math.max(50, Math.min(1000, Number(requestedLimit) || 300));
  const rows = await getJson("/fapi/v1/klines", {
    symbol: validateSymbol(symbol),
    interval: validateInterval(interval),
    limit,
  });
  return rows.map((row) => ({
    time: Math.floor(Number(row[0]) / 1000),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5]),
    closeTime: Number(row[6]),
  }));
}

function clamp(value, minimum, maximum) {
  return Math.min(maximum, Math.max(minimum, value));
}

function sentimentLabel(score) {
  if (score < 20) return "极度恐慌";
  if (score < 40) return "恐慌";
  if (score < 60) return "中性";
  if (score < 80) return "贪婪";
  return "极度贪婪";
}

export function calculateSentiment({ changePercent, fundingRate, longShortRatio }) {
  const trend = clamp(changePercent, -12, 12) * 1.8;
  const positioning = clamp((longShortRatio - 1) * 22, -14, 14);
  const funding = clamp(fundingRate * 100_000, -12, 12);
  const score = Math.round(clamp(50 + trend + positioning + funding, 0, 100));
  return {
    score,
    label: sentimentLabel(score),
    components: {
      trend: Math.round(trend * 10) / 10,
      positioning: Math.round(positioning * 10) / 10,
      funding: Math.round(funding * 10) / 10,
    },
  };
}

export async function getMarketSnapshot(symbol) {
  validateSymbol(symbol);
  const [ticker, premium, longShort, openInterest] = await Promise.all([
    getJson("/fapi/v1/ticker/24hr", { symbol }),
    getJson("/fapi/v1/premiumIndex", { symbol }),
    getJson("/futures/data/globalLongShortAccountRatio", { symbol, period: "5m", limit: 1 }),
    getJson("/fapi/v1/openInterest", { symbol }),
  ]);
  const changePercent = Number(ticker.priceChangePercent);
  const fundingRate = Number(premium.lastFundingRate);
  const longShortRatio = Number(longShort[0]?.longShortRatio ?? 1);
  return {
    symbol,
    price: Number(ticker.lastPrice),
    changePercent,
    high: Number(ticker.highPrice),
    low: Number(ticker.lowPrice),
    quoteVolume: Number(ticker.quoteVolume),
    fundingRate,
    markPrice: Number(premium.markPrice),
    nextFundingTime: Number(premium.nextFundingTime),
    longShortRatio,
    openInterest: Number(openInterest.openInterest),
    sentiment: calculateSentiment({ changePercent, fundingRate, longShortRatio }),
    timestamp: Date.now(),
  };
}

export const supportedIntervals = Object.freeze([...INTERVALS]);
