import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { existsSync } from "node:fs";
import {
  mkdir,
  readFile,
  readdir,
  rename,
  stat,
  unlink,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { analyzeWithProvider } from "./lib/ai.mjs";
import { getCandles, getMarketSnapshot } from "./lib/market.mjs";
import { SecretStore } from "./lib/secret-store.mjs";

const APP_ROOT = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_ROOT = path.join(APP_ROOT, "public");
const DATA_ROOT = path.resolve(process.env.GQT_DATA_ROOT ?? path.join(APP_ROOT, "data"));
const JOBS_FILE = path.join(DATA_ROOT, "jobs.json");
const TRADING_ROOT = path.resolve(
  process.env.QUANT_TRADING_ROOT ?? path.join(APP_ROOT, "trading"),
);
const CONFIG_PATH = path.join(TRADING_ROOT, "user_data", "config.json");
const STRATEGY_ROOT = path.join(TRADING_ROOT, "user_data", "strategies");
const PORT = Number(process.env.PORT ?? 4173);
const HOST = process.env.HOST ?? "127.0.0.1";
const MAX_LOG_SIZE = 240_000;
const activeProcesses = new Map();
const VENDOR_FILES = new Map([
  ["/vendor/lucide.min.js", path.join(APP_ROOT, "node_modules", "lucide", "dist", "umd", "lucide.min.js")],
  [
    "/vendor/lightweight-charts.js",
    path.join(
      APP_ROOT,
      "node_modules",
      "lightweight-charts",
      "dist",
      "lightweight-charts.standalone.production.js",
    ),
  ],
]);

await mkdir(DATA_ROOT, { recursive: true });
await mkdir(STRATEGY_ROOT, { recursive: true });

const secretStore = new SecretStore({ dataRoot: DATA_ROOT });
const sessions = new Map();
const loginAttempts = new Map();
let jobs = await loadJobs();

function json(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
  });
  res.end(body);
}

function text(res, statusCode, body, contentType = "text/plain; charset=utf-8") {
  res.writeHead(statusCode, {
    "Content-Type": contentType,
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
  });
  res.end(body);
}

async function readJson(filePath) {
  return JSON.parse(await readFile(filePath, "utf8"));
}

async function writeJsonAtomic(filePath, value) {
  const tempPath = `${filePath}.${randomUUID()}.tmp`;
  await writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(tempPath, filePath);
}

async function loadJobs() {
  try {
    const loaded = await readJson(JOBS_FILE);
    return Array.isArray(loaded)
      ? loaded.slice(0, 100).map((job) =>
          ["queued", "running"].includes(job.status)
            ? { ...job, status: "failed", updatedAt: new Date().toISOString(), log: `${job.log ?? ""}\n平台重启，任务状态已失效。\n` }
            : job,
        )
      : [];
  } catch {
    return [];
  }
}

async function persistJobs() {
  await writeJsonAtomic(JOBS_FILE, jobs.slice(0, 100));
}

function stripAnsi(value) {
  return value.replace(/\u001b\[[0-9;]*m/g, "");
}

export function parseBacktestSummary(rawOutput) {
  const output = stripAnsi(rawOutput);
  const readMetric = (label) => {
    const expression = new RegExp(`${label}\\s*│\\s*([^│\\r\\n]+)`);
    return output.match(expression)?.[1]?.trim() ?? null;
  };

  const tradeMatch = output.match(/Total\/Daily Avg Trades\s*│\s*(\d+)/);
  return {
    trades: tradeMatch ? Number(tradeMatch[1]) : null,
    profitPercent: numberFromMetric(readMetric("Total profit %")),
    absoluteProfit: readMetric("Absolute profit"),
    maxDrawdown: readMetric("Max % of account underwater"),
    sharpe: numberFromMetric(readMetric("Sharpe \\(closed trades\\)")),
    profitFactor: numberFromMetric(readMetric("Profit factor")),
  };
}

function numberFromMetric(value) {
  if (!value) return null;
  const match = value.match(/-?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function appendJobLog(job, chunk) {
  job.log = `${job.log ?? ""}${chunk}`.slice(-MAX_LOG_SIZE);
  job.updatedAt = new Date().toISOString();
}

function runProcess(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd ?? APP_ROOT,
      windowsHide: true,
      env: { ...process.env, ...options.env },
    });
    let stdout = "";
    let stderr = "";
    let settled = false;
    const timer = options.timeoutMs
      ? setTimeout(() => {
          child.kill();
          if (!settled) reject(new Error(`Command timed out after ${options.timeoutMs}ms`));
        }, options.timeoutMs)
      : null;

    child.stdout.on("data", (data) => {
      stdout += data.toString();
      options.onOutput?.(data.toString());
    });
    child.stderr.on("data", (data) => {
      stderr += data.toString();
      options.onOutput?.(data.toString());
    });
    child.on("error", (error) => {
      settled = true;
      if (timer) clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      settled = true;
      if (timer) clearTimeout(timer);
      resolve({ code: code ?? 1, stdout, stderr });
    });
    if (options.input) child.stdin.end(options.input);
    else child.stdin.end();
  });
}

async function dockerState() {
  const info = await runProcess("docker", ["info", "--format", "{{.ServerVersion}}"], {
    timeoutMs: 20_000,
  }).catch(() => ({ code: 1, stdout: "" }));
  if (info.code !== 0) return { available: false, version: null, bot: "unavailable" };

  const inspect = await runProcess(
    "docker",
    ["inspect", "--format", "{{.State.Status}}", "binance-futures-factor"],
    { timeoutMs: 10_000 },
  ).catch(() => ({ code: 1, stdout: "" }));

  return {
    available: true,
    version: info.stdout.trim(),
    bot: inspect.code === 0 ? inspect.stdout.trim() : "stopped",
  };
}

async function listStrategies() {
  const entries = await readdir(STRATEGY_ROOT, { withFileTypes: true });
  const strategies = [];
  for (const entry of entries) {
    if (!entry.isFile() || !entry.name.endsWith(".py")) continue;
    const filePath = path.join(STRATEGY_ROOT, entry.name);
    const details = await stat(filePath);
    strategies.push({
      name: entry.name.slice(0, -3),
      file: entry.name,
      modifiedAt: details.mtime.toISOString(),
      size: details.size,
    });
  }
  return strategies.sort((a, b) => a.name.localeCompare(b.name));
}

async function overview() {
  const [config, strategies, docker] = await Promise.all([
    readJson(CONFIG_PATH),
    listStrategies(),
    dockerState(),
  ]);
  const latestBacktest = jobs.find((job) => job.type === "backtest" && job.status === "completed");
  return {
    docker,
    mode: config.dry_run ? "dry-run" : "live",
    tradingMode: config.trading_mode,
    marginMode: config.margin_mode,
    strategy: config.strategy,
    stakeAmount: config.stake_amount,
    maxOpenTrades: config.max_open_trades,
    liquidationBuffer: config.liquidation_buffer,
    dryRunWallet: config.dry_run ? config.dry_run_wallet : null,
    tradableBalanceRatio: config.tradable_balance_ratio,
    pairs: config.exchange?.pair_whitelist ?? [],
    apiKeyConfigured: secretStore.secretStatus().binance_api_key,
    strategies,
    jobs: jobs.slice(0, 20).map(publicJob),
    latestBacktest: latestBacktest?.summary ?? null,
  };
}

function publicJob(job) {
  return {
    id: job.id,
    type: job.type,
    status: job.status,
    title: job.title,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    summary: job.summary ?? null,
    parameters: job.parameters,
    log: job.log ?? "",
    exitCode: job.exitCode ?? null,
  };
}

function validatePair(pair) {
  return /^[A-Z0-9]{2,15}\/USDT:USDT$/.test(pair);
}

function validateDate(value) {
  return /^\d{8}$/.test(value);
}

function validateTimeframe(value) {
  return ["15m", "30m", "1h", "2h", "4h", "8h", "1d"].includes(value);
}

export function validStrategyName(value) {
  return /^[A-Za-z_][A-Za-z0-9_]{2,63}$/.test(value);
}

async function startJob(type, parameters) {
  let commandArgs;
  let title;
  if (type === "backtest") {
    if (!validStrategyName(parameters.strategy)) throw new Error("策略名称无效");
    const pairs = parameters.pairs?.filter(validatePair) ?? [];
    if (!pairs.length) throw new Error("至少选择一个有效的 U 本位永续合约");
    if (!validateDate(parameters.start) || !validateDate(parameters.end)) {
      throw new Error("回测日期格式无效");
    }
    if (!validateTimeframe(parameters.timeframe)) throw new Error("时间周期无效");
    const fee = Number(parameters.fee);
    if (!Number.isFinite(fee) || fee < 0 || fee > 0.01) throw new Error("手续费参数无效");
    commandArgs = [
      "compose",
      "run",
      "--rm",
      "freqtrade",
      "backtesting",
      "--config",
      "/freqtrade/user_data/config.json",
      "--strategy",
      parameters.strategy,
      "--pairs",
      ...pairs,
      "--timerange",
      `${parameters.start}-${parameters.end}`,
      "-i",
      parameters.timeframe,
      "--fee",
      String(fee),
    ];
    title = `${parameters.strategy} · ${pairs.length} 个合约`;
  } else if (type === "download") {
    const pairs = parameters.pairs?.filter(validatePair) ?? [];
    const days = Number(parameters.days);
    if (!pairs.length) throw new Error("至少选择一个有效的 U 本位永续合约");
    if (!Number.isInteger(days) || days < 30 || days > 3650) throw new Error("下载天数应为 30-3650");
    if (!validateTimeframe(parameters.timeframe)) throw new Error("时间周期无效");
    commandArgs = [
      "compose",
      "run",
      "--rm",
      "freqtrade",
      "download-data",
      "--config",
      "/freqtrade/user_data/config.json",
      "--trading-mode",
      "futures",
      "--pairs",
      ...pairs,
      "--days",
      String(days),
      "-t",
      parameters.timeframe,
    ];
    title = `同步 ${pairs.length} 个合约 · ${days} 天`;
  } else {
    throw new Error("未知任务类型");
  }

  const job = {
    id: randomUUID(),
    type,
    status: "queued",
    title,
    parameters,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    log: "",
  };
  jobs.unshift(job);
  await persistJobs();

  const child = spawn("docker", commandArgs, {
    cwd: TRADING_ROOT,
    windowsHide: true,
    env: process.env,
  });
  activeProcesses.set(job.id, child);
  job.status = "running";
  job.updatedAt = new Date().toISOString();
  await persistJobs();

  child.stdout.on("data", (data) => appendJobLog(job, data.toString()));
  child.stderr.on("data", (data) => appendJobLog(job, data.toString()));
  child.on("error", async (error) => {
    appendJobLog(job, `\n${error.message}\n`);
    job.status = "failed";
    activeProcesses.delete(job.id);
    await persistJobs();
  });
  child.on("close", async (code) => {
    job.exitCode = code ?? 1;
    job.status = code === 0 ? "completed" : "failed";
    job.updatedAt = new Date().toISOString();
    if (type === "backtest") job.summary = parseBacktestSummary(job.log);
    activeProcesses.delete(job.id);
    await persistJobs();
  });

  return publicJob(job);
}

async function validatePython(source) {
  const result = await runProcess(
    "python",
    ["-c", "import ast,sys; ast.parse(sys.stdin.read()); print('ok')"],
    { input: source, timeoutMs: 8_000 },
  );
  if (result.code !== 0) throw new Error(result.stderr.trim() || "Python 语法校验失败");
}

async function saveStrategy(name, source) {
  if (!validStrategyName(name)) throw new Error("策略名称只能使用字母、数字和下划线");
  if (typeof source !== "string" || source.length < 80 || source.length > 500_000) {
    throw new Error("策略源码长度无效");
  }
  if (!source.includes(`class ${name}`)) throw new Error(`源码中未找到 class ${name}`);
  await validatePython(source);

  const target = path.join(STRATEGY_ROOT, `${name}.py`);
  const oldSource = existsSync(target) ? await readFile(target, "utf8") : null;
  await writeFile(target, source, "utf8");

  const validation = await runProcess(
    "docker",
    [
      "compose",
      "run",
      "--rm",
      "freqtrade",
      "list-strategies",
      "--config",
      "/freqtrade/user_data/config.json",
    ],
    { cwd: TRADING_ROOT, timeoutMs: 90_000 },
  );
  const output = `${validation.stdout}\n${validation.stderr}`;
  if (validation.code !== 0 || !output.includes(name) || !output.includes("OK")) {
    if (oldSource === null) {
      await unlink(target).catch(() => {});
    } else {
      await writeFile(target, oldSource, "utf8");
    }
    throw new Error("Freqtrade 未能加载该策略，文件已恢复");
  }
  return { name, valid: true };
}

export function validateRiskInput(input) {
  const stakeAmount = Number(input.stakeAmount);
  const maxOpenTrades = Number(input.maxOpenTrades);
  const liquidationBuffer = Number(input.liquidationBuffer);
  if (!Number.isFinite(stakeAmount) || stakeAmount < 5 || stakeAmount > 1_000_000) {
    throw new Error("单仓保证金超出允许范围");
  }
  if (!Number.isInteger(maxOpenTrades) || maxOpenTrades < 1 || maxOpenTrades > 20) {
    throw new Error("最大持仓数应为 1-20");
  }
  if (!Number.isFinite(liquidationBuffer) || liquidationBuffer < 0.05 || liquidationBuffer > 0.5) {
    throw new Error("爆仓缓冲应为 0.05-0.50");
  }
  return { stakeAmount, maxOpenTrades, liquidationBuffer };
}

async function updateRisk(input) {
  const values = validateRiskInput(input);
  const config = await readJson(CONFIG_PATH);
  config.stake_amount = values.stakeAmount;
  config.max_open_trades = values.maxOpenTrades;
  config.liquidation_buffer = values.liquidationBuffer;
  await writeJsonAtomic(CONFIG_PATH, config);
  return values;
}

async function readBody(req) {
  let body = "";
  for await (const chunk of req) {
    body += chunk.toString();
    if (body.length > 1_000_000) throw new Error("请求内容过大");
  }
  return body ? JSON.parse(body) : {};
}

function parseCookies(req) {
  return Object.fromEntries(
    (req.headers.cookie ?? "")
      .split(";")
      .map((item) => item.trim())
      .filter(Boolean)
      .map((item) => {
        const index = item.indexOf("=");
        return index === -1
          ? [item, ""]
          : [decodeURIComponent(item.slice(0, index)), decodeURIComponent(item.slice(index + 1))];
      }),
  );
}

function currentSession(req) {
  const token = parseCookies(req).gqt_session;
  if (!token) return null;
  const session = sessions.get(token);
  if (!session || session.expiresAt < Date.now()) {
    if (token) sessions.delete(token);
    return null;
  }
  session.expiresAt = Date.now() + 12 * 60 * 60 * 1000;
  return session;
}

function sessionCookie(req, token, maxAge = 43_200) {
  const forwardedHttps =
    process.env.TRUST_PROXY === "true" && req.headers["x-forwarded-proto"] === "https";
  const secure = process.env.COOKIE_SECURE === "true" || forwardedHttps ? "; Secure" : "";
  return `gqt_session=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}${secure}`;
}

function createSession(req, res) {
  const token = randomBytes(32).toString("base64url");
  sessions.set(token, { createdAt: Date.now(), expiresAt: Date.now() + 12 * 60 * 60 * 1000 });
  res.setHeader("Set-Cookie", sessionCookie(req, token));
  return token;
}

function sameOrigin(req) {
  const origin = req.headers.origin;
  if (!origin) return true;
  try {
    return new URL(origin).host === req.headers.host;
  } catch {
    return false;
  }
}

function clientAddress(req) {
  const forwarded = process.env.TRUST_PROXY === "true" ? req.headers["x-forwarded-for"] : null;
  return String(forwarded ?? req.socket.remoteAddress ?? "unknown")
    .split(",")[0]
    .trim();
}

function loginAllowed(req) {
  const address = clientAddress(req);
  const now = Date.now();
  const recent = (loginAttempts.get(address) ?? []).filter((time) => now - time < 5 * 60 * 1000);
  loginAttempts.set(address, recent);
  return recent.length < 8;
}

function recordFailedLogin(req) {
  const address = clientAddress(req);
  loginAttempts.set(address, [...(loginAttempts.get(address) ?? []), Date.now()]);
}

function validateCredentialInput(body, setup = false) {
  const values = {
    binance_api_key: body.binanceApiKey?.trim(),
    binance_api_secret: body.binanceApiSecret?.trim(),
    openai_api_key: body.openaiApiKey?.trim(),
    anthropic_api_key: body.anthropicApiKey?.trim(),
    deepseek_api_key: body.deepseekApiKey?.trim(),
  };
  if (setup && (!values.binance_api_key || !values.binance_api_secret)) {
    throw new Error("首次设置必须填写 Binance API Key 和 Secret");
  }
  if (Boolean(values.binance_api_key) !== Boolean(values.binance_api_secret)) {
    throw new Error("Binance API Key 和 Secret 必须同时填写");
  }
  for (const [name, value] of Object.entries(values)) {
    if (value && value.length < 10) throw new Error(`${name} 长度无效`);
    if (value && value.length > 512) throw new Error(`${name} 长度无效`);
  }
  return values;
}

function storeCredentials(values) {
  for (const [name, value] of Object.entries(values)) {
    if (value) secretStore.setSecret(name, value);
  }
  return secretStore.secretStatus();
}

export function normalizeAutomationConfig(value = {}) {
  return {
    enabled: Boolean(value.enabled),
    autoRestartDryRun: Boolean(value.autoRestartDryRun),
    healthcheckSeconds: Math.max(15, Math.min(300, Number(value.healthcheckSeconds) || 30)),
  };
}

function automationConfig() {
  const stored = secretStore.getSetting("automation");
  if (!stored) return normalizeAutomationConfig();
  try {
    return normalizeAutomationConfig(JSON.parse(stored));
  } catch {
    return normalizeAutomationConfig();
  }
}

function updateAutomation(body) {
  const config = normalizeAutomationConfig(body);
  secretStore.setSetting("automation", JSON.stringify(config));
  return config;
}

async function botAction(action) {
  const config = await readJson(CONFIG_PATH);
  if (action === "start" && !config.dry_run && process.env.ALLOW_LIVE_TRADING !== "true") {
    throw new Error("实盘启动未在服务器环境变量中启用");
  }
  const binanceApiKey = secretStore.getSecret("binance_api_key");
  const binanceApiSecret = secretStore.getSecret("binance_api_secret");
  if (action === "start" && !config.dry_run && (!binanceApiKey || !binanceApiSecret)) {
    throw new Error("实盘启动需要已配置的 Binance API 凭证");
  }
  config.initial_state = action === "start" ? "running" : "stopped";
  await writeJsonAtomic(CONFIG_PATH, config);
  const args = action === "start" ? ["compose", "up", "-d"] : ["compose", "stop", "freqtrade"];
  const result = await runProcess("docker", args, {
    cwd: TRADING_ROOT,
    timeoutMs: 120_000,
    env: {
      BINANCE_API_KEY: binanceApiKey ?? "",
      BINANCE_API_SECRET: binanceApiSecret ?? "",
    },
  });
  if (result.code !== 0) {
    if (action === "start") {
      config.initial_state = "stopped";
      await writeJsonAtomic(CONFIG_PATH, config);
    }
    throw new Error(result.stderr || "Docker 操作失败");
  }
  return dockerState();
}

async function botLogs() {
  const result = await runProcess(
    "docker",
    ["compose", "logs", "--no-color", "--tail", "250", "freqtrade"],
    { cwd: TRADING_ROOT, timeoutMs: 15_000 },
  );
  return stripAnsi(`${result.stdout}${result.stderr}`).slice(-MAX_LOG_SIZE);
}

async function handleApi(req, res, url) {
  if (req.method === "GET" && url.pathname === "/api/setup/status") {
    return json(res, 200, {
      needsSetup: !secretStore.isSetup(),
      authenticated: Boolean(currentSession(req)),
    });
  }
  if (req.method === "POST" && url.pathname === "/api/setup") {
    if (secretStore.isSetup()) return json(res, 409, { error: "平台已经完成首次设置" });
    if (!sameOrigin(req)) return json(res, 403, { error: "请求来源无效" });
    const body = await readBody(req);
    if (secretStore.isSetup()) return json(res, 409, { error: "平台已经完成首次设置" });
    const credentials = validateCredentialInput(body, true);
    if (
      typeof body.adminPassword !== "string" ||
      body.adminPassword.length < 12 ||
      body.adminPassword.length > 256
    ) {
      throw new Error("管理员密码需要 12-256 个字符");
    }
    secretStore.setAdminPassword(body.adminPassword);
    const status = storeCredentials(credentials);
    createSession(req, res);
    return json(res, 201, { authenticated: true, credentials: status });
  }
  if (req.method === "POST" && url.pathname === "/api/auth/login") {
    if (!sameOrigin(req)) return json(res, 403, { error: "请求来源无效" });
    if (!loginAllowed(req)) return json(res, 429, { error: "登录尝试过多，请稍后再试" });
    const body = await readBody(req);
    if (!secretStore.verifyAdminPassword(body.password)) {
      recordFailedLogin(req);
      return json(res, 401, { error: "管理员密码错误" });
    }
    loginAttempts.delete(clientAddress(req));
    createSession(req, res);
    return json(res, 200, { authenticated: true });
  }
  if (req.method === "POST" && url.pathname === "/api/auth/logout") {
    if (!sameOrigin(req)) return json(res, 403, { error: "请求来源无效" });
    const token = parseCookies(req).gqt_session;
    if (token) sessions.delete(token);
    res.setHeader("Set-Cookie", sessionCookie(req, "", 0));
    return json(res, 200, { authenticated: false });
  }

  if (!currentSession(req)) return json(res, 401, { error: "需要管理员登录" });
  if (["POST", "PUT", "PATCH", "DELETE"].includes(req.method) && !sameOrigin(req)) {
    return json(res, 403, { error: "请求来源无效" });
  }

  if (req.method === "GET" && url.pathname === "/api/credentials") {
    return json(res, 200, secretStore.secretStatus());
  }
  if (req.method === "PUT" && url.pathname === "/api/credentials") {
    const body = await readBody(req);
    const status = storeCredentials(validateCredentialInput(body));
    for (const name of body.remove ?? []) secretStore.removeSecret(name);
    return json(res, 200, { ...status, ...secretStore.secretStatus() });
  }
  if (req.method === "GET" && url.pathname === "/api/market/candles") {
    return json(
      res,
      200,
      await getCandles(
        String(url.searchParams.get("symbol") ?? "BTCUSDT").toUpperCase(),
        String(url.searchParams.get("interval") ?? "4h"),
        Number(url.searchParams.get("limit") ?? 300),
      ),
    );
  }
  if (req.method === "GET" && url.pathname === "/api/market/snapshot") {
    return json(
      res,
      200,
      await getMarketSnapshot(String(url.searchParams.get("symbol") ?? "BTCUSDT").toUpperCase()),
    );
  }
  if (req.method === "POST" && url.pathname === "/api/ai/analyze") {
    const body = await readBody(req);
    if (typeof body.prompt !== "string" || body.prompt.length < 3 || body.prompt.length > 4_000) {
      throw new Error("分析问题长度无效");
    }
    if (body.model != null && (typeof body.model !== "string" || body.model.length > 120)) {
      throw new Error("模型名称无效");
    }
    const snapshot = await getMarketSnapshot(String(body.symbol ?? "BTCUSDT").toUpperCase());
    return json(
      res,
      200,
      await analyzeWithProvider({
        provider: body.provider,
        model: body.model,
        prompt: body.prompt,
        snapshot,
        secretStore,
      }),
    );
  }
  if (req.method === "GET" && url.pathname === "/api/automation") {
    return json(res, 200, automationConfig());
  }
  if (req.method === "PUT" && url.pathname === "/api/automation") {
    return json(res, 200, updateAutomation(await readBody(req)));
  }
  if (req.method === "GET" && url.pathname === "/api/overview") {
    return json(res, 200, await overview());
  }
  if (req.method === "GET" && url.pathname === "/api/jobs") {
    return json(res, 200, jobs.slice(0, 100).map(publicJob));
  }
  if (req.method === "GET" && url.pathname.startsWith("/api/jobs/")) {
    const job = jobs.find((item) => item.id === url.pathname.split("/").at(-1));
    return job ? json(res, 200, publicJob(job)) : json(res, 404, { error: "任务不存在" });
  }
  if (req.method === "POST" && url.pathname === "/api/jobs/backtest") {
    return json(res, 202, await startJob("backtest", await readBody(req)));
  }
  if (req.method === "POST" && url.pathname === "/api/jobs/download") {
    return json(res, 202, await startJob("download", await readBody(req)));
  }
  if (req.method === "GET" && url.pathname === "/api/strategies") {
    return json(res, 200, await listStrategies());
  }
  if (url.pathname.startsWith("/api/strategies/")) {
    const name = decodeURIComponent(url.pathname.split("/").at(-1));
    if (!validStrategyName(name)) return json(res, 400, { error: "策略名称无效" });
    const filePath = path.join(STRATEGY_ROOT, `${name}.py`);
    if (req.method === "GET") {
      if (!existsSync(filePath)) return json(res, 404, { error: "策略不存在" });
      return json(res, 200, { name, source: await readFile(filePath, "utf8") });
    }
    if (req.method === "PUT") {
      const body = await readBody(req);
      return json(res, 200, await saveStrategy(name, body.source));
    }
  }
  if (req.method === "PUT" && url.pathname === "/api/config/risk") {
    return json(res, 200, await updateRisk(await readBody(req)));
  }
  if (req.method === "POST" && url.pathname === "/api/bot/start") {
    return json(res, 200, await botAction("start"));
  }
  if (req.method === "POST" && url.pathname === "/api/bot/stop") {
    return json(res, 200, await botAction("stop"));
  }
  if (req.method === "GET" && url.pathname === "/api/bot/logs") {
    return json(res, 200, { log: await botLogs() });
  }
  return json(res, 404, { error: "接口不存在" });
}

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
};

async function serveStatic(res, pathname) {
  const vendorPath = VENDOR_FILES.get(pathname);
  if (vendorPath) {
    try {
      return text(res, 200, await readFile(vendorPath), "text/javascript; charset=utf-8");
    } catch {
      return text(res, 503, "Frontend dependency is unavailable");
    }
  }
  const relative = pathname === "/" ? "index.html" : decodeURIComponent(pathname.slice(1));
  const filePath = path.resolve(PUBLIC_ROOT, relative);
  if (!filePath.startsWith(`${PUBLIC_ROOT}${path.sep}`) && filePath !== path.join(PUBLIC_ROOT, "index.html")) {
    return text(res, 403, "Forbidden");
  }
  try {
    const content = await readFile(filePath);
    return text(res, 200, content, mimeTypes[path.extname(filePath)] ?? "application/octet-stream");
  } catch {
    return text(res, 404, "Not found");
  }
}

function setSecurityHeaders(req, res) {
  res.setHeader("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self' wss://fstream.binance.com; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=()");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  if (process.env.TRUST_PROXY === "true" && req.headers["x-forwarded-proto"] === "https") {
    res.setHeader("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
  }
}

export const server = createServer(async (req, res) => {
  setSecurityHeaders(req, res);
  const url = new URL(req.url ?? "/", `http://${req.headers.host ?? `${HOST}:${PORT}`}`);
  try {
    if (url.pathname.startsWith("/api/")) return await handleApi(req, res, url);
    return await serveStatic(res, url.pathname);
  } catch (error) {
    return json(res, 400, { error: error instanceof Error ? error.message : "请求失败" });
  }
});

function startAutomationMonitor() {
  const check = async () => {
    const automation = automationConfig();
    if (automation.enabled && automation.autoRestartDryRun) {
      try {
        const config = await readJson(CONFIG_PATH);
        const state = await dockerState();
        if (config.dry_run && state.available && state.bot !== "running") await botAction("start");
      } catch (error) {
        console.error(`Automation monitor: ${error.message}`);
      }
    }
    const timer = setTimeout(check, automation.healthcheckSeconds * 1_000);
    timer.unref();
  };
  const timer = setTimeout(check, automationConfig().healthcheckSeconds * 1_000);
  timer.unref();
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  server.listen(PORT, HOST, () => {
    startAutomationMonitor();
    console.log(`Quant Platform running at http://${HOST}:${PORT}`);
    console.log(`Trading project: ${TRADING_ROOT}`);
  });
}
