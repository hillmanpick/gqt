const pageTitles = {
  overview: ["总览", "Binance U 本位永续"],
  market: ["实时行情", "K线、资金费率与市场情绪"],
  strategies: ["策略工作台", "Python · Freqtrade Interface v3"],
  backtests: ["回测", "分时段验证与成本压力测试"],
  data: ["市场数据", "K线 · 标记价格 · 资金费率"],
  execution: ["执行", "自动策略与运行日志"],
  settings: ["设置", "风控与加密密钥"],
};

const state = {
  authenticated: false,
  activePage: "overview",
  overview: null,
  jobs: [],
  strategies: [],
  selectedStrategy: null,
  credentials: null,
  automation: null,
  market: {
    symbol: "BTCUSDT",
    interval: "4h",
    timer: null,
    reconnectTimer: null,
    socket: null,
    chart: null,
    series: null,
    request: null,
  },
};

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers ?? {}) },
    credentials: "same-origin",
    ...options,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    if (response.status === 401 && !path.startsWith("/api/auth/")) showLogin();
    throw new Error(payload.error ?? "请求失败");
  }
  return payload;
}

function toast(message, isError = false) {
  const element = $("#toast");
  element.textContent = message;
  element.classList.toggle("is-error", isError);
  element.classList.add("is-visible");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => element.classList.remove("is-visible"), 3200);
}

function renderIcons() {
  globalThis.lucide?.createIcons({ attrs: { width: 16, height: 16 } });
}

function showSetup() {
  state.authenticated = false;
  $("#auth-gate").classList.remove("is-hidden");
  $("#app-shell").classList.add("is-hidden");
  $("#setup-form").classList.remove("is-hidden");
  $("#login-form").classList.add("is-hidden");
}

function showLogin() {
  state.authenticated = false;
  stopMarketPolling();
  $("#auth-gate").classList.remove("is-hidden");
  $("#app-shell").classList.add("is-hidden");
  $("#setup-form").classList.add("is-hidden");
  $("#login-form").classList.remove("is-hidden");
}

async function showApp() {
  state.authenticated = true;
  $("#auth-gate").classList.add("is-hidden");
  $("#app-shell").classList.remove("is-hidden");
  await Promise.all([loadOverview(), loadCredentials(), loadAutomation()]);
}

async function bootstrap() {
  try {
    const status = await api("/api/setup/status");
    if (status.needsSetup) showSetup();
    else if (!status.authenticated) showLogin();
    else await showApp();
  } catch (error) {
    showLogin();
    toast(error.message, true);
  }
  renderIcons();
}

function setPage(page) {
  state.activePage = page;
  $$(".nav-item").forEach((item) => item.classList.toggle("is-active", item.dataset.page === page));
  $$(".page").forEach((panel) => panel.classList.toggle("is-active", panel.dataset.pagePanel === page));
  $("#page-title").textContent = pageTitles[page][0];
  $("#page-context").textContent = pageTitles[page][1];
  if (page === "execution") loadLogs();
  if (page === "market") startMarketPolling();
  else stopMarketPolling();
}

function formatTime(value) {
  if (!value) return "--";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function formatNumber(value, maximumFractionDigits = 2) {
  if (!Number.isFinite(Number(value))) return "--";
  return new Intl.NumberFormat("zh-CN", { maximumFractionDigits }).format(Number(value));
}

function escapeHtml(value) {
  return String(value).replace(/[&<>'"]/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "'": "&#39;",
    '"': "&quot;",
  })[character]);
}

function statusLabel(status) {
  return { queued: "排队", running: "运行中", completed: "完成", failed: "失败" }[status] ?? status;
}

function jobRows(jobs, detailed = false) {
  if (!jobs.length) return `<tr><td class="empty-cell" colspan="${detailed ? 6 : 5}">暂无任务</td></tr>`;
  return jobs.map((job) => {
    const result = job.summary?.profitPercent == null ? "--" : `${job.summary.profitPercent.toFixed(2)}%`;
    if (detailed) {
      return `<tr><td>${escapeHtml(job.title)}</td><td><span class="job-status ${job.status}">${statusLabel(job.status)}</span></td><td>${job.summary?.trades ?? "--"}</td><td>${result}</td><td>${escapeHtml(job.summary?.maxDrawdown ?? "--")}</td><td>${formatTime(job.updatedAt)}</td></tr>`;
    }
    return `<tr><td>${escapeHtml(job.title)}</td><td>${job.type === "backtest" ? "回测" : "数据"}</td><td><span class="job-status ${job.status}">${statusLabel(job.status)}</span></td><td>${formatTime(job.updatedAt)}</td><td>${result}</td></tr>`;
  }).join("");
}

function updateResultChart(jobs) {
  const points = jobs
    .filter((job) => job.type === "backtest" && job.status === "completed" && job.summary?.profitPercent != null)
    .slice(0, 12)
    .reverse();
  if (!points.length) {
    $("#result-path").setAttribute("d", "M24 150 L696 150");
    $("#result-point").setAttribute("cx", "696");
    $("#result-point").setAttribute("cy", "150");
    return;
  }
  const values = points.map((job) => job.summary.profitPercent);
  const low = Math.min(0, ...values);
  const high = Math.max(0, ...values);
  const spread = Math.max(1, high - low);
  const coordinates = values.map((value, index) => ({
    x: 24 + (672 * index) / Math.max(1, values.length - 1),
    y: 160 - ((value - low) / spread) * 120,
  }));
  $("#result-path").setAttribute(
    "d",
    coordinates.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)} ${point.y.toFixed(1)}`).join(" "),
  );
  const last = coordinates.at(-1);
  $("#result-point").setAttribute("cx", last.x);
  $("#result-point").setAttribute("cy", last.y);
}

function renderOverview() {
  const data = state.overview;
  if (!data) return;
  const botRunning = data.docker.bot === "running";
  $("#sidebar-engine").textContent = botRunning ? "策略运行中" : data.docker.available ? "交易内核就绪" : "Docker 离线";
  $("#sidebar-mode").textContent = data.mode.toUpperCase();
  $("#sidebar-dot").className = `status-dot ${botRunning ? "running" : data.docker.available ? "online" : ""}`;
  $("#mode-badge").textContent = data.mode.toUpperCase();
  $("#metric-engine").textContent = botRunning ? "运行中" : "已停止";
  $("#metric-docker").textContent = data.docker.available ? `Docker ${data.docker.version}` : "Docker 不可用";
  $("#metric-mode").textContent = data.mode === "dry-run" ? "模拟盘" : "实盘";
  $("#metric-margin").textContent = `${data.marginMode} · ${data.tradingMode}`;
  const exposure = Number(data.stakeAmount) * Number(data.maxOpenTrades);
  $("#metric-exposure").textContent = `${exposure.toLocaleString()} USDT`;
  $("#metric-positions").textContent = `${data.stakeAmount} × ${data.maxOpenTrades} 仓`;
  $("#metric-strategy").textContent = data.strategy;
  $("#metric-pairs").textContent = `${data.pairs.length} 个永续合约`;
  $("#system-state").textContent = botRunning ? "运行中" : "待机";
  $("#status-contract").textContent = "USDT-M Perpetual";
  $("#status-margin").textContent = data.marginMode === "isolated" ? "逐仓" : "全仓";
  $("#status-api").textContent = data.apiKeyConfigured ? "已加密配置" : "未配置";
  $("#status-buffer").textContent = `${Math.round(data.liquidationBuffer * 100)}%`;
  const wallet = Number(data.dryRunWallet);
  const meter = data.mode === "dry-run" && wallet > 0
    ? Math.min(100, Math.round((exposure / wallet) * 100))
    : Math.min(100, Math.round(Number(data.tradableBalanceRatio ?? 1) * 100));
  $("#meter-label").textContent = data.mode === "dry-run" ? "配置仓位 / 模拟资金" : "可交易余额上限";
  $("#meter-value").textContent = `${meter}%`;
  $("#meter-fill").style.width = `${meter}%`;
  $("#overview-jobs").innerHTML = jobRows(data.jobs);
  $("#job-count").textContent = `${data.jobs.length} 条`;
  const latest = data.latestBacktest;
  $("#latest-profit").textContent = latest?.profitPercent == null ? "--" : `${latest.profitPercent.toFixed(2)}%`;
  $("#latest-trades").textContent = latest?.trades ?? "--";
  $("#latest-drawdown").textContent = latest?.maxDrawdown ?? "--";
  $("#latest-pf").textContent = latest?.profitFactor?.toFixed(2) ?? "--";
  updateResultChart(data.jobs);
  $("#execution-detail").textContent = `${botRunning ? "运行中" : "已停止"} · ${data.mode.toUpperCase()} · ${data.strategy}`;
  $("#risk-stake").value = data.stakeAmount;
  $("#risk-max-trades").value = data.maxOpenTrades;
  $("#risk-buffer").value = data.liquidationBuffer;
  $("#settings-mode").textContent = data.mode.toUpperCase();
  $("#settings-api").textContent = data.apiKeyConfigured ? "已加密配置" : "未配置";
}

function renderJobs() {
  const backtests = state.jobs.filter((job) => job.type === "backtest");
  $("#backtest-jobs").innerHTML = jobRows(backtests, true);
  const latestDownload = state.jobs.find((job) => job.type === "download");
  $("#download-status").textContent = latestDownload ? statusLabel(latestDownload.status) : "暂无任务";
  $("#download-log").textContent = latestDownload?.log || "暂无任务";
}

function renderStrategyOptions() {
  const options = state.strategies.map((strategy) => `<option value="${strategy.name}">${strategy.name}</option>`).join("");
  $("#strategy-select").innerHTML = options;
  $("#backtest-strategy").innerHTML = options;
  if (state.overview?.strategy) {
    $("#strategy-select").value = state.overview.strategy;
    $("#backtest-strategy").value = state.overview.strategy;
  }
}

function renderPairs() {
  const pairs = state.overview?.pairs ?? [];
  const markup = pairs.map((pair, index) => `<label class="pair-option"><input type="checkbox" value="${pair}" ${index < 5 ? "checked" : ""} /><span>${pair.replace("/USDT:USDT", "")}</span></label>`).join("");
  $("#backtest-pairs").innerHTML = markup;
  $("#download-pairs").innerHTML = markup;
}

async function loadOverview(showError = true) {
  try {
    state.overview = await api("/api/overview");
    state.strategies = state.overview.strategies;
    state.jobs = state.overview.jobs;
    renderOverview();
    renderJobs();
    renderStrategyOptions();
    if (!$("#backtest-pairs").children.length) renderPairs();
    if (!state.selectedStrategy && state.strategies.length) {
      await loadStrategy(state.overview.strategy || state.strategies[0].name);
    }
  } catch (error) {
    if (showError) toast(error.message, true);
  }
}

async function loadJobs() {
  try {
    state.jobs = await api("/api/jobs");
    if (state.overview) state.overview.jobs = state.jobs.slice(0, 20);
    renderJobs();
    renderOverview();
  } catch (error) {
    toast(error.message, true);
  }
}

async function loadStrategy(name) {
  if (!name) return;
  try {
    const strategy = await api(`/api/strategies/${encodeURIComponent(name)}`);
    state.selectedStrategy = name;
    $("#strategy-select").value = name;
    $("#strategy-file").textContent = `${name}.py`;
    $("#strategy-editor").value = strategy.source;
    setStrategyState("未修改", "");
  } catch (error) {
    toast(error.message, true);
  }
}

function setStrategyState(label, className) {
  const element = $("#strategy-state");
  element.textContent = label;
  element.className = `validation-state ${className}`;
}

function selectedPairs(container) {
  return $$(`#${container} input:checked`).map((input) => input.value);
}

function compactDate(value) {
  return value.replaceAll("-", "");
}

async function loadLogs() {
  try {
    const payload = await api("/api/bot/logs");
    $("#bot-log").textContent = payload.log || "暂无日志";
    $("#bot-log").scrollTop = $("#bot-log").scrollHeight;
  } catch (error) {
    $("#bot-log").textContent = error.message;
  }
}

function newStrategyTemplate(name) {
  return `from pandas import DataFrame\n\nfrom freqtrade.strategy import IStrategy\n\n\nclass ${name}(IStrategy):\n    INTERFACE_VERSION = 3\n    can_short = True\n    timeframe = "4h"\n    startup_candle_count = 100\n    stoploss = -0.08\n    minimal_roi = {"0": 0.06, "720": 0.02, "1440": 0.0}\n\n    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:\n        return dataframe\n\n    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:\n        dataframe["enter_long"] = 0\n        dataframe["enter_short"] = 0\n        return dataframe\n\n    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:\n        dataframe["exit_long"] = 0\n        dataframe["exit_short"] = 0\n        return dataframe\n`;
}

function initializeMarketChart() {
  if (state.market.chart || !globalThis.LightweightCharts) return;
  const container = $("#candlestick-chart");
  state.market.chart = globalThis.LightweightCharts.createChart(container, {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: { background: { color: "#ffffff" }, textColor: "#64706a", fontFamily: "Segoe UI, sans-serif" },
    grid: { vertLines: { color: "#edf1ee" }, horzLines: { color: "#edf1ee" } },
    rightPriceScale: { borderColor: "#d9e0dc" },
    timeScale: { borderColor: "#d9e0dc", timeVisible: true, secondsVisible: false },
    crosshair: { mode: globalThis.LightweightCharts.CrosshairMode.Normal },
  });
  state.market.series = state.market.chart.addCandlestickSeries({
    upColor: "#24825d",
    downColor: "#b84a43",
    borderUpColor: "#24825d",
    borderDownColor: "#b84a43",
    wickUpColor: "#24825d",
    wickDownColor: "#b84a43",
  });
  const observer = new ResizeObserver(() => {
    state.market.chart?.applyOptions({ width: container.clientWidth, height: container.clientHeight });
  });
  observer.observe(container);
}

async function loadMarket() {
  if (!state.authenticated || state.activePage !== "market") return;
  initializeMarketChart();
  state.market.request?.abort();
  const request = new AbortController();
  state.market.request = request;
  const symbol = state.market.symbol;
  const interval = state.market.interval;
  try {
    const query = new URLSearchParams({ symbol, interval, limit: "300" });
    const [candles, snapshot] = await Promise.all([
      api(`/api/market/candles?${query}`, { signal: request.signal }),
      api(`/api/market/snapshot?symbol=${encodeURIComponent(symbol)}`, { signal: request.signal }),
    ]);
    if (symbol !== state.market.symbol || interval !== state.market.interval) return;
    state.market.series?.setData(candles.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })));
    if (!state.market.fitted) {
      state.market.chart?.timeScale().fitContent();
      state.market.fitted = true;
    }
    renderMarketSnapshot(snapshot);
  } catch (error) {
    if (error.name !== "AbortError") toast(error.message, true);
  } finally {
    if (state.market.request === request) state.market.request = null;
  }
}

function renderMarketSnapshot(snapshot) {
  $("#market-heading").textContent = snapshot.symbol;
  $("#market-price").textContent = formatNumber(snapshot.price, snapshot.price < 1 ? 6 : 2);
  const change = $("#market-change");
  change.textContent = `${snapshot.changePercent >= 0 ? "+" : ""}${snapshot.changePercent.toFixed(2)}%`;
  change.className = `market-change ${snapshot.changePercent >= 0 ? "positive" : "negative"}`;
  $("#mark-price").textContent = formatNumber(snapshot.markPrice, snapshot.markPrice < 1 ? 6 : 2);
  $("#funding-rate").textContent = `${(snapshot.fundingRate * 100).toFixed(4)}%`;
  $("#long-short-ratio").textContent = snapshot.longShortRatio.toFixed(3);
  $("#open-interest").textContent = formatNumber(snapshot.openInterest, 0);
  $("#quote-volume").textContent = `${formatNumber(snapshot.quoteVolume / 1_000_000, 1)}M USDT`;
  $("#sentiment-score").textContent = snapshot.sentiment.score;
  $("#sentiment-label").textContent = snapshot.sentiment.label;
  $("#sentiment-fill").style.width = `${snapshot.sentiment.score}%`;
}

function setMarketConnection(label, connected = false) {
  $("#market-live-text").textContent = label;
  $("#market-live").classList.toggle("is-connected", connected);
}

function connectMarketStream() {
  if (!state.authenticated || state.activePage !== "market") return;
  if (state.market.reconnectTimer) clearTimeout(state.market.reconnectTimer);
  state.market.socket?.close();

  const symbol = state.market.symbol;
  const interval = state.market.interval;
  const socket = new WebSocket(
    `wss://fstream.binance.com/ws/${symbol.toLowerCase()}@kline_${interval}`,
  );
  state.market.socket = socket;
  setMarketConnection("连接中");

  socket.addEventListener("open", () => {
    if (state.market.socket === socket) setMarketConnection("实时", true);
  });
  socket.addEventListener("message", (event) => {
    if (state.market.socket !== socket) return;
    try {
      const payload = JSON.parse(event.data);
      const candle = payload.k;
      if (!candle || candle.s !== symbol || candle.i !== interval) return;
      const close = Number(candle.c);
      state.market.series?.update({
        time: Math.floor(Number(candle.t) / 1000),
        open: Number(candle.o),
        high: Number(candle.h),
        low: Number(candle.l),
        close,
      });
      $("#market-price").textContent = formatNumber(close, close < 1 ? 6 : 2);
    } catch {
      // Ignore malformed public market frames and wait for the next update.
    }
  });
  socket.addEventListener("close", () => {
    if (state.market.socket !== socket) return;
    state.market.socket = null;
    setMarketConnection("重连中");
    state.market.reconnectTimer = setTimeout(connectMarketStream, 2_000);
  });
  socket.addEventListener("error", () => {
    if (state.market.socket === socket) setMarketConnection("连接异常");
  });
}

function startMarketPolling() {
  stopMarketPolling();
  state.market.fitted = false;
  loadMarket();
  connectMarketStream();
  state.market.timer = setInterval(loadMarket, 15_000);
}

function stopMarketPolling() {
  if (state.market.timer) clearInterval(state.market.timer);
  state.market.timer = null;
  state.market.request?.abort();
  state.market.request = null;
  if (state.market.reconnectTimer) clearTimeout(state.market.reconnectTimer);
  state.market.reconnectTimer = null;
  const socket = state.market.socket;
  state.market.socket = null;
  socket?.close();
  setMarketConnection("已暂停");
}

function renderCredentialStatus() {
  if (!state.credentials) return;
  const setText = (selector, configured) => {
    $(selector).textContent = configured ? "已配置" : "未配置";
  };
  setText("#cred-binance", state.credentials.binance_api_key && state.credentials.binance_api_secret);
  setText("#cred-openai", state.credentials.openai_api_key);
  setText("#cred-anthropic", state.credentials.anthropic_api_key);
  setText("#cred-deepseek", state.credentials.deepseek_api_key);
  const provider = $("#ai-provider").value;
  const configured = {
    openai: state.credentials.openai_api_key,
    anthropic: state.credentials.anthropic_api_key,
    deepseek: state.credentials.deepseek_api_key,
  }[provider];
  $("#ai-provider-state").textContent = configured ? "提供商已配置" : "提供商未配置";
}

async function loadCredentials() {
  try {
    state.credentials = await api("/api/credentials");
    renderCredentialStatus();
  } catch (error) {
    toast(error.message, true);
  }
}

async function loadAutomation() {
  try {
    state.automation = await api("/api/automation");
    $("#automation-enabled").checked = state.automation.enabled;
    $("#automation-restart").checked = state.automation.autoRestartDryRun;
  } catch (error) {
    toast(error.message, true);
  }
}

$$(".nav-item").forEach((button) => button.addEventListener("click", () => setPage(button.dataset.page)));
$$('[data-go]').forEach((button) => button.addEventListener("click", () => setPage(button.dataset.go)));

$("#setup-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  try {
    await api("/api/setup", {
      method: "POST",
      body: JSON.stringify({
        adminPassword: $("#setup-password").value,
        binanceApiKey: $("#setup-binance-key").value,
        binanceApiSecret: $("#setup-binance-secret").value,
        openaiApiKey: $("#setup-openai-key").value,
        anthropicApiKey: $("#setup-anthropic-key").value,
        deepseekApiKey: $("#setup-deepseek-key").value,
      }),
    });
    await showApp();
    toast("密钥已加密保存");
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
});

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  try {
    await api("/api/auth/login", { method: "POST", body: JSON.stringify({ password: $("#login-password").value }) });
    $("#login-password").value = "";
    await showApp();
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
});

$("#logout").addEventListener("click", async () => {
  await api("/api/auth/logout", { method: "POST" });
  showLogin();
});
$("#refresh-all").addEventListener("click", () => loadOverview());
$("#refresh-jobs").addEventListener("click", loadJobs);
$("#strategy-select").addEventListener("change", (event) => loadStrategy(event.target.value));
$("#strategy-editor").addEventListener("input", () => setStrategyState("未保存", "is-dirty"));

$("#new-strategy").addEventListener("click", () => {
  const name = prompt("策略类名");
  if (!name) return;
  state.selectedStrategy = name.trim();
  $("#strategy-file").textContent = `${state.selectedStrategy}.py`;
  $("#strategy-editor").value = newStrategyTemplate(state.selectedStrategy);
  setStrategyState("未保存", "is-dirty");
});

$("#save-strategy").addEventListener("click", async () => {
  if (!state.selectedStrategy) return;
  const button = $("#save-strategy");
  button.disabled = true;
  setStrategyState("校验中", "");
  try {
    await api(`/api/strategies/${encodeURIComponent(state.selectedStrategy)}`, {
      method: "PUT",
      body: JSON.stringify({ source: $("#strategy-editor").value }),
    });
    setStrategyState("校验通过", "is-valid");
    toast("策略已保存并通过 Freqtrade 校验");
    await loadOverview(false);
  } catch (error) {
    setStrategyState("校验失败", "is-error");
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
});

$("#backtest-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  try {
    await api("/api/jobs/backtest", {
      method: "POST",
      body: JSON.stringify({
        strategy: $("#backtest-strategy").value,
        start: compactDate($("#backtest-start").value),
        end: compactDate($("#backtest-end").value),
        timeframe: $("#backtest-timeframe").value,
        fee: Number($("#backtest-fee").value),
        pairs: selectedPairs("backtest-pairs"),
      }),
    });
    toast("回测任务已启动");
    await loadJobs();
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
});

$("#download-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  try {
    await api("/api/jobs/download", {
      method: "POST",
      body: JSON.stringify({
        days: Number($("#download-days").value),
        timeframe: $("#download-timeframe").value,
        pairs: selectedPairs("download-pairs"),
      }),
    });
    toast("数据同步任务已启动");
    await loadJobs();
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
});

$("#start-bot").addEventListener("click", async () => {
  try {
    await api("/api/bot/start", { method: "POST" });
    toast("策略交易进程已启动");
    await loadOverview(false);
    await loadLogs();
  } catch (error) {
    toast(error.message, true);
  }
});
$("#stop-bot").addEventListener("click", async () => {
  try {
    await api("/api/bot/stop", { method: "POST" });
    toast("交易内核已停止");
    await loadOverview(false);
    await loadLogs();
  } catch (error) {
    toast(error.message, true);
  }
});
$("#refresh-logs").addEventListener("click", loadLogs);

$("#save-automation").addEventListener("click", async () => {
  try {
    state.automation = await api("/api/automation", {
      method: "PUT",
      body: JSON.stringify({
        enabled: $("#automation-enabled").checked,
        autoRestartDryRun: $("#automation-restart").checked,
        healthcheckSeconds: 30,
      }),
    });
    toast("自动化设置已保存");
  } catch (error) {
    toast(error.message, true);
  }
});

$("#risk-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api("/api/config/risk", {
      method: "PUT",
      body: JSON.stringify({
        stakeAmount: Number($("#risk-stake").value),
        maxOpenTrades: Number($("#risk-max-trades").value),
        liquidationBuffer: Number($("#risk-buffer").value),
      }),
    });
    toast("风控参数已保存");
    await loadOverview(false);
  } catch (error) {
    toast(error.message, true);
  }
});

$("#save-credentials").addEventListener("click", async () => {
  const button = $("#save-credentials");
  button.disabled = true;
  try {
    state.credentials = await api("/api/credentials", {
      method: "PUT",
      body: JSON.stringify({
        binanceApiKey: $("#cred-binance-key").value,
        binanceApiSecret: $("#cred-binance-secret").value,
        openaiApiKey: $("#cred-openai-key").value,
        anthropicApiKey: $("#cred-anthropic-key").value,
        deepseekApiKey: $("#cred-deepseek-key").value,
      }),
    });
    $$(".credential-fields input").forEach((input) => { input.value = ""; });
    renderCredentialStatus();
    toast("密钥已加密更新");
  } catch (error) {
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
});

$("#market-symbol").addEventListener("change", (event) => {
  state.market.symbol = event.target.value;
  startMarketPolling();
});
$$('[data-interval]').forEach((button) => button.addEventListener("click", () => {
  $$("[data-interval]").forEach((item) => item.classList.toggle("is-active", item === button));
  state.market.interval = button.dataset.interval;
  startMarketPolling();
}));
$("#ai-provider").addEventListener("change", renderCredentialStatus);
$("#run-ai").addEventListener("click", async () => {
  const button = $("#run-ai");
  button.disabled = true;
  $("#ai-output").textContent = "分析中...";
  try {
    const result = await api("/api/ai/analyze", {
      method: "POST",
      body: JSON.stringify({
        provider: $("#ai-provider").value,
        model: $("#ai-model").value.trim(),
        symbol: state.market.symbol,
        prompt: $("#ai-prompt").value,
      }),
    });
    $("#ai-output").textContent = result.text || "提供商未返回文本";
    $("#ai-provider-state").textContent = `${result.provider} · ${result.model}`;
  } catch (error) {
    $("#ai-output").textContent = error.message;
    toast(error.message, true);
  } finally {
    button.disabled = false;
  }
});

renderIcons();
await bootstrap();
setInterval(() => {
  if (state.authenticated && state.jobs.some((job) => ["queued", "running"].includes(job.status))) loadJobs();
}, 4_000);
