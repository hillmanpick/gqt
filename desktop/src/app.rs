use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use chrono::{Datelike, Local, TimeZone, Utc};
use crossbeam_channel::{Receiver, Sender, unbounded};
use directories::ProjectDirs;
use eframe::egui::{
    self, Align, Align2, Color32, FontFamily, FontId, Frame, Layout, Margin, Pos2, Rect, RichText,
    ScrollArea, Sense, Stroke, TextEdit, Ui, Vec2,
};
use zeroize::Zeroizing;

use crate::{
    ai, ai_trader,
    audit::AuditLog,
    event_prediction::{
        self, EventHorizon, EventPredictionRunDirection, EventPredictionStats,
        EventPredictionTicket,
    },
    exchange, market,
    model::{
        AiProvider, AiTradingConfig, AiTradingInput, Candle, CredentialDraft, FuturesAccount,
        FuturesPosition, Interval, MarginMode, MarketCommand, MarketEvent, MarketSnapshot, Page,
        SecretStatus, SimulationAccount, StrategyProfile,
    },
    scanner::{self, ScannerCommand, ScannerEvent, UniverseSnapshot},
    store::SecretStore,
    theme,
    trading::TradingWorkspace,
};

const AI_TIMEFRAMES: [&str; 6] = ["1m", "5m", "15m", "1h", "4h", "1d"];
const BUILD_LABEL: &str = "0.4.0-event-batch5-v2";
const RELAY_DEFAULT_MODEL: &str = "gpt-5.6-luna";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EquityRange {
    Today,
    Month,
    NinetyDays,
    Year,
}

impl EquityRange {
    const ALL: [EquityRange; 4] = [
        EquityRange::Today,
        EquityRange::Month,
        EquityRange::NinetyDays,
        EquityRange::Year,
    ];

    fn label(self) -> &'static str {
        match self {
            EquityRange::Today => "当天",
            EquityRange::Month => "当月",
            EquityRange::NinetyDays => "90天",
            EquityRange::Year => "一年",
        }
    }

    fn cutoff(self) -> i64 {
        let now = Local::now();
        match self {
            EquityRange::Today => Local
                .with_ymd_and_hms(now.year(), now.month(), now.day(), 0, 0, 0)
                .single()
                .map(|time| time.timestamp())
                .unwrap_or_else(|| now.timestamp() - 24 * 60 * 60),
            EquityRange::Month => Local
                .with_ymd_and_hms(now.year(), now.month(), 1, 0, 0, 0)
                .single()
                .map(|time| time.timestamp())
                .unwrap_or_else(|| now.timestamp() - 31 * 24 * 60 * 60),
            EquityRange::NinetyDays => now.timestamp() - 90 * 24 * 60 * 60,
            EquityRange::Year => now.timestamp() - 365 * 24 * 60 * 60,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct EquityPoint {
    time: i64,
    equity: f64,
    available: f64,
    unrealized_profit: f64,
}

pub struct GqtApp {
    store: SecretStore,
    workspace: TradingWorkspace,
    unlocked_key: Option<Zeroizing<[u8; 32]>>,
    credential_draft: CredentialDraft,
    credential_check_running: bool,
    secret_status: SecretStatus,
    account: FuturesAccount,
    account_error: String,
    account_check_running: bool,
    last_account_check: Instant,
    equity_history_path: PathBuf,
    equity_history: Vec<EquityPoint>,
    equity_range: EquityRange,
    simulation_account: SimulationAccount,
    simulation_error: String,
    simulation_check_running: bool,
    last_simulation_check: Instant,
    show_simulation_account: bool,
    page: Page,
    symbol: String,
    interval: Interval,
    candles: Vec<Candle>,
    snapshot: MarketSnapshot,
    market_connected: bool,
    market_error: String,
    market_commands: Sender<MarketCommand>,
    market_events: Receiver<MarketEvent>,
    scanner_commands: Sender<ScannerCommand>,
    scanner_events: Receiver<ScannerEvent>,
    scanner_snapshot: UniverseSnapshot,
    scanner_connected: bool,
    scanner_error: String,
    strategy_source: String,
    strategy_state: String,
    stake_amount: f64,
    max_open_trades: i64,
    risk_reward_ratio: f64,
    allow_ai_risk_sizing: bool,
    docker_available: bool,
    bot_state: String,
    bot_log: String,
    bot_action_running: bool,
    dry_run: bool,
    live_confirmation: Option<LiveAction>,
    live_acknowledged: bool,
    auto_restart: bool,
    health_check_running: bool,
    last_health_check: Instant,
    ai_config: AiTradingConfig,
    ai_symbol_whitelist: String,
    ai_provider: AiProvider,
    ai_model: String,
    relay_base_url: String,
    ai_prompt: String,
    ai_output: String,
    ai_running: bool,
    ai_decision_running: bool,
    last_ai_decision_check: Instant,
    ai_decision_status: String,
    ai_processed_candles: BTreeMap<String, i64>,
    event_prediction_enabled: bool,
    event_prediction_running: bool,
    last_event_prediction_check: Instant,
    event_prediction_status: String,
    event_prediction_open_count: i64,
    event_prediction_legacy_open_count: i64,
    event_prediction_starting_bankroll: f64,
    event_prediction_stake_amount: f64,
    event_prediction_realized_pnl: f64,
    event_prediction_open_exposure: f64,
    event_prediction_equity: f64,
    event_prediction_available_balance: f64,
    event_prediction_stats: Vec<EventPredictionStats>,
    event_prediction_all_realized_pnl: f64,
    event_prediction_all_stats: Vec<EventPredictionStats>,
    event_prediction_recent: Vec<EventPredictionTicket>,
    event_prediction_history: Vec<EventPredictionTicket>,
    event_prediction_run_dialog_open: bool,
    event_prediction_manual_10m: bool,
    event_prediction_manual_30m: bool,
    event_prediction_manual_60m: bool,
    event_prediction_order_dialog: Option<EventOrderKind>,
    event_prediction_ticket_dialog: Option<EventPredictionTicket>,
    event_prediction_cycle_dialog: Option<EventPredictionCycleDialog>,
    event_prediction_direction_dialog: Option<Vec<EventPredictionRunDirection>>,
    event_prediction_direction_dialog_pending: bool,
    toast: Option<(String, bool, Instant)>,
    task_sender: mpsc::Sender<TaskEvent>,
    task_receiver: mpsc::Receiver<TaskEvent>,
    backtest_start: String,
    backtest_end: String,
    backtest_fee: f64,
    download_days: i64,
    selected_pairs: Vec<String>,
    job_running: bool,
}

enum TaskEvent {
    Bot(Result<String, String>),
    Logs(String),
    Job(Result<String, String>),
    Ai(Result<String, String>),
    Health(bool, String),
    BinanceValidation {
        action: CredentialAction,
        api_key: String,
        api_secret: String,
        result: Result<String, String>,
    },
    Account(Result<FuturesAccount, String>),
    Simulation(Result<SimulationAccount, String>),
    AiDecision(Result<AiDecisionSummary, String>),
    EventPrediction(Result<event_prediction::EventPredictionSummary, String>),
    EventPredictionCycle {
        cycle_id: String,
        result: Result<Vec<EventPredictionTicket>, String>,
    },
}

struct AiDecisionSummary {
    message: String,
    processed: Vec<(String, i64)>,
}

#[derive(Clone, Copy)]
enum CredentialAction {
    Setup,
    Update,
}

#[derive(Clone, Copy)]
enum LiveAction {
    Enable,
    Start,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EventOrderKind {
    Open,
    Settled,
}

#[derive(Clone)]
struct EventPredictionCycleView {
    cycle_id: String,
    tickets: Vec<EventPredictionTicket>,
}

#[derive(Clone)]
enum EventPredictionCycleDialog {
    Loading { cycle_id: String },
    Ready(EventPredictionCycleView),
}

impl EventPredictionCycleDialog {
    fn cycle_id(&self) -> &str {
        match self {
            Self::Loading { cycle_id } => cycle_id,
            Self::Ready(cycle) => &cycle.cycle_id,
        }
    }
}

enum EventTicketAction {
    Ticket(EventPredictionTicket),
    Cycle(String),
}

impl GqtApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        theme::configure(&cc.egui_ctx);
        let project_dirs = ProjectDirs::from("xin", "HillmanPick", "GQT Trader")
            .expect("Windows application data directory");
        let data_root = project_dirs.data_local_dir().to_path_buf();
        std::fs::create_dir_all(&data_root).expect("create GQT data directory");
        let equity_history_path = data_root.join("equity-history.json");
        let equity_history = load_equity_history(&equity_history_path);
        let store = SecretStore::open(&data_root.join("key.db")).expect("open GQT key.db");
        let unlocked_key = if store.is_setup().unwrap_or(false) {
            store.unlock().ok()
        } else {
            None
        };
        let secret_status = store.secret_status().unwrap_or_default();
        let workspace =
            TradingWorkspace::ensure(&data_root.join("trading")).expect("create trading workspace");
        let strategy_source = workspace.strategy_source().unwrap_or_default();
        let (stake_amount, max_open_trades, risk_reward_ratio, allow_ai_risk_sizing) =
            workspace.risk().unwrap_or((120.0, 5, 1.4, false));
        let ai_config = workspace.ai_trading_config().unwrap_or_default();
        let initial_symbol = ai_config
            .symbol_whitelist
            .first()
            .cloned()
            .unwrap_or_else(|| "BTCUSDT".into());
        let ai_symbol_whitelist = format_symbol_whitelist(&ai_config.symbol_whitelist);
        let selected_pairs = if ai_config.symbol_whitelist.is_empty() {
            vec![initial_symbol.clone()]
        } else {
            ai_config.symbol_whitelist.clone()
        };
        let (market_commands, command_receiver) = unbounded();
        let (event_sender, market_events) = unbounded();
        market::start_worker(command_receiver, event_sender);
        let _ = market_commands.send(MarketCommand::Select {
            symbol: initial_symbol.clone(),
            interval: Interval::FifteenMinutes,
        });
        let (scanner_commands, scanner_events) =
            scanner::start_worker(workspace.root.join("user_data"));
        let (task_sender, task_receiver) = mpsc::channel();
        let (docker_available, bot_state) = workspace.docker_state();
        let dry_run = workspace.dry_run().unwrap_or(true);
        let auto_restart = dry_run
            && store
                .setting("auto_restart")
                .ok()
                .flatten()
                .is_some_and(|value| value == "true");
        let relay_base_url = store
            .setting("relay_base_url")
            .ok()
            .flatten()
            .unwrap_or_default();
        let ai_provider = if secret_status.relay {
            AiProvider::Relay
        } else {
            AiProvider::DeepSeek
        };
        let event_prediction_enabled = store
            .setting("event_prediction_enabled")
            .ok()
            .flatten()
            .is_none_or(|value| value != "false");
        let event_prediction_dashboard =
            event_prediction::EventPredictionLog::open(&workspace.event_predictions)
                .and_then(|log| log.dashboard())
                .unwrap_or_default();
        Self {
            store,
            workspace,
            unlocked_key,
            credential_draft: CredentialDraft::default(),
            credential_check_running: false,
            secret_status,
            account: FuturesAccount::default(),
            account_error: String::new(),
            account_check_running: false,
            last_account_check: Instant::now() - Duration::from_secs(30),
            equity_history_path,
            equity_history,
            equity_range: EquityRange::Today,
            simulation_account: SimulationAccount::default(),
            simulation_error: String::new(),
            simulation_check_running: false,
            last_simulation_check: Instant::now() - Duration::from_secs(30),
            show_simulation_account: false,
            page: Page::Overview,
            symbol: initial_symbol,
            interval: Interval::FifteenMinutes,
            candles: Vec::new(),
            snapshot: MarketSnapshot::default(),
            market_connected: false,
            market_error: String::new(),
            market_commands,
            market_events,
            scanner_commands,
            scanner_events,
            scanner_snapshot: UniverseSnapshot::default(),
            scanner_connected: false,
            scanner_error: String::new(),
            strategy_source,
            strategy_state: "未修改".into(),
            stake_amount,
            max_open_trades,
            risk_reward_ratio,
            allow_ai_risk_sizing,
            docker_available,
            bot_state,
            bot_log: String::new(),
            bot_action_running: false,
            dry_run,
            live_confirmation: None,
            live_acknowledged: false,
            auto_restart,
            health_check_running: false,
            last_health_check: Instant::now(),
            ai_config,
            ai_symbol_whitelist,
            ai_provider,
            ai_model: if ai_provider == AiProvider::Relay {
                RELAY_DEFAULT_MODEL.into()
            } else {
                String::new()
            },
            relay_base_url,
            ai_prompt: "判断当前市场状态、主要风险与需要等待的确认信号。".into(),
            ai_output: "暂无分析".into(),
            ai_running: false,
            ai_decision_running: false,
            last_ai_decision_check: Instant::now() - Duration::from_secs(60),
            ai_decision_status: "AI 闭环等待启动".into(),
            ai_processed_candles: BTreeMap::new(),
            event_prediction_enabled,
            event_prediction_running: false,
            last_event_prediction_check: Instant::now() - Duration::from_secs(90),
            event_prediction_status: event_prediction_dashboard.message,
            event_prediction_open_count: event_prediction_dashboard.open_count,
            event_prediction_legacy_open_count: event_prediction_dashboard.legacy_open_count,
            event_prediction_starting_bankroll: event_prediction_dashboard.starting_bankroll,
            event_prediction_stake_amount: event_prediction_dashboard.stake_amount,
            event_prediction_realized_pnl: event_prediction_dashboard.realized_pnl,
            event_prediction_open_exposure: event_prediction_dashboard.open_exposure,
            event_prediction_equity: event_prediction_dashboard.equity,
            event_prediction_available_balance: event_prediction_dashboard.available_balance,
            event_prediction_stats: event_prediction_dashboard.stats,
            event_prediction_all_realized_pnl: event_prediction_dashboard.all_realized_pnl,
            event_prediction_all_stats: event_prediction_dashboard.all_stats,
            event_prediction_recent: event_prediction_dashboard.open_recent,
            event_prediction_history: event_prediction_dashboard.settled_recent,
            event_prediction_run_dialog_open: false,
            event_prediction_manual_10m: true,
            event_prediction_manual_30m: true,
            event_prediction_manual_60m: true,
            event_prediction_order_dialog: None,
            event_prediction_ticket_dialog: None,
            event_prediction_cycle_dialog: None,
            event_prediction_direction_dialog: None,
            event_prediction_direction_dialog_pending: false,
            toast: None,
            task_sender,
            task_receiver,
            backtest_start: "2023-01-01".into(),
            backtest_end: "2026-01-01".into(),
            backtest_fee: 0.0005,
            download_days: 1095,
            selected_pairs,
            job_running: false,
        }
    }

    fn drain_events(&mut self) {
        while let Ok(event) = self.market_events.try_recv() {
            match event {
                MarketEvent::Candles(candles) => self.candles = candles,
                MarketEvent::Snapshot(snapshot) => {
                    if self.interval == Interval::OneSecond {
                        self.push_second_candle(snapshot.updated_at, snapshot.price);
                    } else if let Some(last) = self.candles.last_mut() {
                        last.close = snapshot.price;
                        last.high = last.high.max(snapshot.price);
                        last.low = last.low.min(snapshot.price);
                    }
                    self.snapshot = snapshot;
                }
                MarketEvent::Connection(connected) => self.market_connected = connected,
                MarketEvent::Error(error) => self.market_error = error,
            }
        }
        while let Ok(event) = self.scanner_events.try_recv() {
            match event {
                ScannerEvent::Snapshot(snapshot) => {
                    self.scanner_snapshot = snapshot;
                    self.scanner_error.clear();
                }
                ScannerEvent::Connection(connected) => self.scanner_connected = connected,
                ScannerEvent::Error(error) => self.scanner_error = error,
            }
        }
        while let Ok(event) = self.task_receiver.try_recv() {
            match event {
                TaskEvent::Bot(result) => match result {
                    Ok(message) => {
                        self.bot_action_running = false;
                        self.toast(message, false);
                        let (available, state) = self.workspace.docker_state();
                        self.docker_available = available;
                        self.bot_state = state;
                    }
                    Err(error) => {
                        self.bot_action_running = false;
                        self.toast(error, true);
                    }
                },
                TaskEvent::Logs(log) => self.bot_log = log,
                TaskEvent::Job(result) => match result {
                    Ok(log) => {
                        self.job_running = false;
                        self.bot_log = log;
                        self.toast("任务执行完成", false);
                    }
                    Err(error) => {
                        self.job_running = false;
                        self.bot_log = error.clone();
                        self.toast(error, true);
                    }
                },
                TaskEvent::Ai(result) => {
                    self.ai_running = false;
                    match result {
                        Ok(output) => self.ai_output = output,
                        Err(error) => {
                            self.ai_output = error.clone();
                            self.toast(error, true);
                        }
                    }
                }
                TaskEvent::Health(available, state) => {
                    self.health_check_running = false;
                    self.docker_available = available;
                    self.bot_state = state;
                    if available
                        && self.bot_state == "running"
                        && !self.bot_action_running
                        && !self.job_running
                        && self.workspace.runtime_uses_strategy_override()
                    {
                        self.hot_reload_bot("检测到旧策略启动命令，正在热重载交易内核");
                    }
                    if self.dry_run
                        && self.auto_restart
                        && !self.job_running
                        && !self.bot_action_running
                        && available
                        && self.bot_state != "running"
                    {
                        self.bot_action(true);
                    }
                }
                TaskEvent::BinanceValidation {
                    action,
                    api_key,
                    api_secret,
                    result,
                } => {
                    self.credential_check_running = false;
                    match result {
                        Ok(message) => {
                            if self.credential_draft.binance_key.trim() != api_key
                                || self.credential_draft.binance_secret.trim() != api_secret
                            {
                                self.toast("Binance 凭据已修改，请重新验证", true);
                            } else {
                                match action {
                                    CredentialAction::Setup => self.finish_setup(&message),
                                    CredentialAction::Update => {
                                        self.finish_credential_update(&message)
                                    }
                                }
                            }
                        }
                        Err(error) => self.toast(error, true),
                    }
                }
                TaskEvent::Account(result) => {
                    self.account_check_running = false;
                    match result {
                        Ok(account) => {
                            self.record_equity_snapshot(&account);
                            self.account = account;
                            self.account_error.clear();
                        }
                        Err(error) => self.account_error = error,
                    }
                }
                TaskEvent::Simulation(result) => {
                    self.simulation_check_running = false;
                    match result {
                        Ok(account) => {
                            self.simulation_account = account;
                            self.simulation_error.clear();
                        }
                        Err(error) => self.simulation_error = error,
                    }
                }
                TaskEvent::AiDecision(result) => {
                    self.ai_decision_running = false;
                    match result {
                        Ok(summary) => {
                            for (symbol, candle_open_time) in summary.processed {
                                self.ai_processed_candles.insert(symbol, candle_open_time);
                            }
                            self.ai_decision_status = summary.message;
                        }
                        Err(error) => {
                            self.ai_decision_status = error.clone();
                            self.toast(error, true);
                        }
                    }
                }
                TaskEvent::EventPrediction(result) => {
                    self.event_prediction_running = false;
                    let show_direction_dialog = self.event_prediction_direction_dialog_pending;
                    self.event_prediction_direction_dialog_pending = false;
                    match result {
                        Ok(summary) => {
                            let direction_text = format_event_run_directions(&summary.directions);
                            let direction_suffix = if direction_text.is_empty() {
                                String::new()
                            } else {
                                format!("；当前方向：{direction_text}")
                            };
                            self.event_prediction_status = format!(
                                "{}；本轮评估 {}，新增订单 {}，结算 {}；同链未结算时等待{}",
                                summary.message,
                                summary.evaluated,
                                summary.created,
                                summary.settled,
                                direction_suffix
                            );
                            self.event_prediction_open_count = summary.open_count;
                            self.event_prediction_legacy_open_count = summary.legacy_open_count;
                            self.event_prediction_starting_bankroll = summary.starting_bankroll;
                            self.event_prediction_stake_amount = summary.stake_amount;
                            self.event_prediction_realized_pnl = summary.realized_pnl;
                            self.event_prediction_open_exposure = summary.open_exposure;
                            self.event_prediction_equity = summary.equity;
                            self.event_prediction_available_balance = summary.available_balance;
                            self.event_prediction_stats = summary.stats;
                            self.event_prediction_all_realized_pnl = summary.all_realized_pnl;
                            self.event_prediction_all_stats = summary.all_stats;
                            self.event_prediction_recent = summary.open_recent;
                            self.event_prediction_history = summary.settled_recent;
                            if show_direction_dialog && !summary.directions.is_empty() {
                                self.event_prediction_direction_dialog =
                                    Some(summary.directions.clone());
                            }
                        }
                        Err(error) => {
                            self.event_prediction_status = error.clone();
                            self.toast(error, true);
                        }
                    }
                }
                TaskEvent::EventPredictionCycle { cycle_id, result } => {
                    let is_current = self
                        .event_prediction_cycle_dialog
                        .as_ref()
                        .is_some_and(|dialog| dialog.cycle_id() == cycle_id);
                    if !is_current {
                        continue;
                    }
                    match result {
                        Ok(tickets) if !tickets.is_empty() => {
                            self.event_prediction_cycle_dialog =
                                Some(EventPredictionCycleDialog::Ready(
                                    EventPredictionCycleView { cycle_id, tickets },
                                ));
                        }
                        Ok(_) => {
                            self.event_prediction_cycle_dialog = None;
                            self.toast(format!("周期 {cycle_id} 暂无订单"), true);
                        }
                        Err(error) => {
                            self.event_prediction_cycle_dialog = None;
                            self.toast(format!("读取周期 {cycle_id} 失败：{error}"), true);
                        }
                    }
                }
            }
        }
    }

    fn toast(&mut self, message: impl Into<String>, error: bool) {
        self.toast = Some((message.into(), error, Instant::now()));
    }

    fn render_auth(&mut self, root: &mut Ui) {
        egui::CentralPanel::default()
            .frame(Frame::NONE.fill(theme::BG).inner_margin(Margin::same(24)))
            .show(root, |ui| {
                ui.with_layout(Layout::top_down(Align::Center), |ui| {
                    ui.add_space((ui.available_height() - 520.0).max(30.0) * 0.35);
                    ui.set_max_width(460.0);
                    ui.horizontal(|ui| {
                        Frame::NONE
                            .fill(theme::YELLOW)
                            .corner_radius(6)
                            .inner_margin(Margin::same(9))
                            .show(ui, |ui| {
                                ui.label(theme::icon("candlestick-chart", 22.0, Color32::BLACK));
                            });
                        ui.vertical(|ui| {
                            ui.label(RichText::new("GQT TRADER").size(18.0).strong());
                            ui.label(
                                RichText::new("NATIVE FUTURES WORKSTATION")
                                    .size(10.0)
                                    .color(theme::MUTED),
                            );
                        });
                    });
                    ui.add_space(18.0);
                    Frame::NONE
                        .fill(theme::SURFACE)
                        .stroke(Stroke::new(1.0, theme::BORDER))
                        .corner_radius(6)
                        .inner_margin(Margin::same(24))
                        .show(ui, |ui| {
                            ui.set_min_width(410.0);
                            ui.label(RichText::new("连接 Binance Futures").size(22.0).strong());
                            ui.label(
                                RichText::new("API 密钥仅加密保存在当前 Windows 账户")
                                    .color(theme::MUTED),
                            );
                            ui.add_space(12.0);
                            field_label(ui, "Binance Futures API Key");
                            ui.add_sized(
                                [ui.available_width(), 38.0],
                                TextEdit::singleline(&mut self.credential_draft.binance_key)
                                    .password(true),
                            );
                            field_label(ui, "Binance Futures API Secret");
                            ui.add_sized(
                                [ui.available_width(), 38.0],
                                TextEdit::singleline(&mut self.credential_draft.binance_secret)
                                    .password(true),
                            );
                            egui::CollapsingHeader::new("AI 提供商（可选）")
                                .default_open(false)
                                .show(ui, |ui| {
                                    field_label(ui, "OpenAI API Key");
                                    ui.add(
                                        TextEdit::singleline(&mut self.credential_draft.openai_key)
                                            .password(true),
                                    );
                                    field_label(ui, "Claude API Key");
                                    ui.add(
                                        TextEdit::singleline(&mut self.credential_draft.claude_key)
                                            .password(true),
                                    );
                                    field_label(ui, "DeepSeek API Key");
                                    ui.add(
                                        TextEdit::singleline(
                                            &mut self.credential_draft.deepseek_key,
                                        )
                                        .password(true),
                                    );
                                    field_label(ui, "中转站 API Key");
                                    ui.add(
                                        TextEdit::singleline(&mut self.credential_draft.relay_key)
                                            .password(true),
                                    );
                                    field_label(ui, "中转站 Base URL");
                                    ui.add(
                                        TextEdit::singleline(&mut self.relay_base_url)
                                            .hint_text("https://example.com/v1"),
                                    );
                                });
                            ui.add_space(14.0);
                            if ui
                                .add_enabled_ui(!self.credential_check_running, |ui| {
                                    ui.add_sized(
                                        [ui.available_width(), 40.0],
                                        theme::primary_button(if self.credential_check_running {
                                            "正在验证 Binance..."
                                        } else {
                                            "验证并加密保存"
                                        }),
                                    )
                                })
                                .inner
                                .clicked()
                            {
                                self.connect_binance();
                            }
                        });
                });
            });
    }

    fn connect_binance(&mut self) {
        if let Err(error) = self.save_relay_endpoint() {
            self.toast(error, true);
            return;
        }
        self.validate_binance_credentials(CredentialAction::Setup);
    }

    fn finish_setup(&mut self, validation_message: &str) {
        match self.store.setup(&self.credential_draft) {
            Ok(key) => {
                self.unlocked_key = Some(key);
                self.secret_status = self.store.secret_status().unwrap_or_default();
                self.credential_draft = CredentialDraft::default();
                self.toast(validation_message, false);
            }
            Err(error) => self.toast(error.to_string(), true),
        }
    }

    fn validate_binance_credentials(&mut self, action: CredentialAction) {
        if self.credential_check_running {
            return;
        }
        let api_key = self.credential_draft.binance_key.trim().to_string();
        let api_secret = self.credential_draft.binance_secret.trim().to_string();
        self.credential_check_running = true;
        let sender = self.task_sender.clone();
        thread::spawn(move || {
            let result = exchange::validate_futures_credentials(&api_key, &api_secret)
                .map_err(|error| error.to_string());
            let _ = sender.send(TaskEvent::BinanceValidation {
                action,
                api_key,
                api_secret,
                result,
            });
        });
    }

    fn render_shell(&mut self, root: &mut Ui) {
        egui::Panel::left("navigation")
            .exact_size(192.0)
            .resizable(false)
            .frame(
                Frame::NONE
                    .fill(theme::SIDEBAR)
                    .inner_margin(Margin::symmetric(12, 16)),
            )
            .show(root, |ui| {
                ui.horizontal(|ui| {
                    Frame::NONE
                        .fill(theme::YELLOW)
                        .corner_radius(5)
                        .inner_margin(Margin::same(7))
                        .show(ui, |ui| {
                            ui.label(theme::icon("candlestick-chart", 19.0, Color32::BLACK));
                        });
                    ui.vertical(|ui| {
                        ui.label(RichText::new("GQT").size(17.0).strong());
                        ui.label(RichText::new("TRADER").size(9.0).color(theme::MUTED));
                    });
                });
                ui.add_space(26.0);
                let status_height = 66.0;
                let navigation_height = (ui.available_height() - status_height).max(0.0);
                ui.allocate_ui(Vec2::new(ui.available_width(), navigation_height), |ui| {
                    ScrollArea::vertical()
                        .id_salt("sidebar_navigation")
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            for page in Page::ALL {
                                let active = self.page == page;
                                let fill = if active {
                                    theme::SURFACE_2
                                } else {
                                    Color32::TRANSPARENT
                                };
                                let stroke = if active {
                                    Stroke::new(1.0, theme::YELLOW)
                                } else {
                                    Stroke::NONE
                                };
                                let response = Frame::NONE
                                    .fill(fill)
                                    .stroke(stroke)
                                    .corner_radius(4)
                                    .inner_margin(Margin::symmetric(10, 9))
                                    .show(ui, |ui| {
                                        ui.horizontal(|ui| {
                                            ui.label(theme::icon(
                                                page.icon(),
                                                17.0,
                                                if active { theme::YELLOW } else { theme::MUTED },
                                            ));
                                            ui.label(RichText::new(page.label()).color(
                                                if active { theme::TEXT } else { theme::MUTED },
                                            ));
                                        });
                                    })
                                    .response
                                    .interact(Sense::click());
                                if response.clicked() {
                                    self.page = page;
                                    if page == Page::Execution {
                                        self.refresh_logs();
                                    }
                                }
                                ui.add_space(3.0);
                            }
                        });
                });
                ui.separator();
                self.render_sidebar_status(ui);
            });

        egui::Panel::top("header")
            .exact_size(68.0)
            .frame(
                Frame::NONE
                    .fill(theme::BG)
                    .inner_margin(Margin::symmetric(24, 12)),
            )
            .show(root, |ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(RichText::new(self.page.label()).size(20.0).strong());
                        ui.label(
                            RichText::new(self.page.context())
                                .size(11.0)
                                .color(theme::MUTED),
                        );
                    });
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let (state_label, _, state_color, _) = self.execution_status();
                        Frame::NONE
                            .fill(if self.dry_run {
                                theme::YELLOW
                            } else {
                                theme::RED
                            })
                            .stroke(Stroke::new(
                                1.0,
                                if self.dry_run {
                                    theme::YELLOW
                                } else {
                                    theme::RED
                                },
                            ))
                            .corner_radius(5)
                            .inner_margin(Margin::symmetric(13, 7))
                            .show(ui, |ui| {
                                ui.set_min_width(118.0);
                                ui.vertical_centered(|ui| {
                                    ui.label(
                                        RichText::new(if self.dry_run {
                                            "模拟盘"
                                        } else {
                                            "实盘"
                                        })
                                        .size(14.0)
                                        .color(if self.dry_run {
                                            Color32::BLACK
                                        } else {
                                            theme::TEXT
                                        })
                                        .strong(),
                                    );
                                    ui.label(
                                        RichText::new(state_label)
                                            .size(11.0)
                                            .color(if self.dry_run {
                                                Color32::from_rgb(25, 25, 25)
                                            } else {
                                                theme::TEXT
                                            })
                                            .strong(),
                                    );
                                });
                            });
                        ui.add_space(8.0);
                        ui.colored_label(state_color, "●");
                    });
                });
            });

        egui::CentralPanel::default()
            .frame(Frame::NONE.fill(theme::BG).inner_margin(Margin::same(24)))
            .show(root, |ui| match self.page {
                Page::Overview => self.render_overview(ui),
                Page::Account => self.render_account(ui),
                Page::PositionHistory => self.render_position_history(ui),
                Page::EventPrediction => self.render_event_prediction(ui),
                Page::Market => {
                    ScrollArea::vertical()
                        .id_salt("market-page-scroll")
                        .auto_shrink([false, false])
                        .show(ui, |ui| self.render_market(ui));
                }
                Page::Strategy => self.render_strategy(ui),
                Page::Backtest => self.render_backtest(ui),
                Page::Data => self.render_data(ui),
                Page::Execution => self.render_execution(ui),
                Page::Settings => self.render_settings(ui),
            });
    }

    fn render_sidebar_status(&self, ui: &mut Ui) {
        let color = if self.docker_available {
            theme::GREEN
        } else {
            theme::MUTED
        };
        let status = if self.docker_available {
            "交易内核就绪"
        } else {
            "Docker 离线"
        };
        let mode = if self.dry_run { "DRY-RUN" } else { "LIVE" };
        ui.allocate_ui(Vec2::new(ui.available_width(), 56.0), |ui| {
            ui.horizontal(|ui| {
                ui.add_space(2.0);
                ui.colored_label(color, "●");
                ui.vertical(|ui| {
                    ui.set_max_width(132.0);
                    ui.label(RichText::new(status).size(11.0));
                    ui.label(RichText::new(mode).size(10.0).color(if self.dry_run {
                        theme::MUTED
                    } else {
                        theme::RED
                    }));
                    ui.label(
                        RichText::new(compact_build_label())
                            .size(9.0)
                            .color(theme::MUTED),
                    );
                });
            });
        });
    }

    fn render_overview(&mut self, ui: &mut Ui) {
        let exposure = self.stake_amount * self.max_open_trades as f64;
        let account_loaded = self.account.updated_at > 0;
        let columns = 4;
        ui.columns(columns, |cols| {
            metric(
                &mut cols[0],
                "合约权益",
                &if account_loaded {
                    format!("{:.2} USDT", self.account.margin_balance)
                } else {
                    "--".into()
                },
                if account_loaded {
                    "Binance Futures"
                } else {
                    "正在读取账户"
                },
                theme::YELLOW,
            );
            metric(
                &mut cols[1],
                "可用余额",
                &if account_loaded {
                    format!("{:.2} USDT", self.account.available_balance)
                } else {
                    "--".into()
                },
                "可用于新开仓",
                theme::TEXT,
            );
            metric(
                &mut cols[2],
                "未实现盈亏",
                &if account_loaded {
                    format!("{:+.2} USDT", self.account.unrealized_profit)
                } else {
                    "--".into()
                },
                &format!("{} 个持仓", self.account.positions.len()),
                if self.account.unrealized_profit >= 0.0 {
                    theme::GREEN
                } else {
                    theme::RED
                },
            );
            metric(
                &mut cols[3],
                "量化策略",
                if self.bot_state == "running" {
                    "运行中"
                } else {
                    "已停止"
                },
                if self.dry_run {
                    "FuturesFactorStrategy · Dry-run"
                } else {
                    "FuturesFactorStrategy · Live"
                },
                if self.bot_state == "running" {
                    theme::GREEN
                } else {
                    theme::MUTED
                },
            );
        });
        ui.add_space(18.0);
        ui.horizontal_top(|ui| {
            ui.allocate_ui_with_layout(
                Vec2::new((ui.available_width() - 330.0).max(420.0), 400.0),
                Layout::top_down(Align::Min),
                |ui| {
                    section_title(ui, "个人资金曲线", self.equity_range.label());
                    ui.horizontal_wrapped(|ui| {
                        for range in EquityRange::ALL {
                            let selected = self.equity_range == range;
                            let button = egui::Button::new(RichText::new(range.label()).color(
                                if selected {
                                    Color32::BLACK
                                } else {
                                    theme::MUTED
                                },
                            ))
                            .fill(if selected {
                                theme::YELLOW
                            } else {
                                theme::SURFACE_2
                            })
                            .stroke(Stroke::new(1.0, theme::BORDER))
                            .corner_radius(3);
                            if ui.add_sized([58.0, 30.0], button).clicked() {
                                self.equity_range = range;
                            }
                        }
                    });
                    ui.add_space(8.0);
                    self.draw_equity_performance(ui);
                    ui.add_space(18.0);
                    section_title(ui, "账户风险", "策略配置上限");
                    let ratio = (exposure / 1000.0).clamp(0.0, 1.0) as f32;
                    ui.add(
                        egui::ProgressBar::new(ratio)
                            .text(format!("{:.0}%", ratio * 100.0))
                            .fill(theme::YELLOW),
                    );
                },
            );
            ui.separator();
            ui.allocate_ui(Vec2::new(300.0, 400.0), |ui| {
                section_title(
                    ui,
                    "系统状态",
                    if self.market_connected {
                        "行情在线"
                    } else {
                        "行情连接中"
                    },
                );
                status_row(ui, "合约模式", "USDT-M Perpetual");
                status_row(ui, "保证金", "逐仓");
                status_row(
                    ui,
                    "API 凭证",
                    if self.secret_status.binance {
                        "已加密配置"
                    } else {
                        "未配置"
                    },
                );
                status_row(
                    ui,
                    "账户接口",
                    if account_loaded {
                        "已连接"
                    } else {
                        "连接中"
                    },
                );
                status_row(
                    ui,
                    "单仓盈亏比",
                    &format!("1:{:.1}", self.risk_reward_ratio),
                );
                status_row(ui, "本地数据库", "key.db");
            });
        });
    }

    fn render_account(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            if ui
                .selectable_label(!self.show_simulation_account, "真实账户")
                .clicked()
            {
                self.show_simulation_account = false;
            }
            if ui
                .selectable_label(self.show_simulation_account, "模拟账户")
                .clicked()
            {
                self.show_simulation_account = true;
            }
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui
                    .add_enabled(
                        if self.show_simulation_account {
                            !self.simulation_check_running
                        } else {
                            !self.account_check_running
                        },
                        theme::secondary_button(
                            if (self.show_simulation_account && self.simulation_check_running)
                                || (!self.show_simulation_account && self.account_check_running)
                            {
                                "刷新中..."
                            } else {
                                "刷新账户"
                            },
                        ),
                    )
                    .clicked()
                {
                    if self.show_simulation_account {
                        self.last_simulation_check = Instant::now() - Duration::from_secs(30);
                        self.refresh_simulation_account();
                    } else {
                        self.last_account_check = Instant::now() - Duration::from_secs(30);
                        self.refresh_account();
                    }
                }
            });
        });
        if self.show_simulation_account {
            self.render_simulation_account(ui);
            return;
        }
        if !self.account_error.is_empty() {
            ui.colored_label(theme::RED, &self.account_error);
        }
        ui.add_space(12.0);
        ui.columns(4, |cols| {
            metric(
                &mut cols[0],
                "钱包余额",
                &format!("{:.2} USDT", self.account.wallet_balance),
                "账户余额",
                theme::TEXT,
            );
            metric(
                &mut cols[1],
                "保证金余额",
                &format!("{:.2} USDT", self.account.margin_balance),
                &format!("占用 {:.2}", self.account.initial_margin),
                theme::YELLOW,
            );
            metric(
                &mut cols[2],
                "可用余额",
                &format!("{:.2} USDT", self.account.available_balance),
                "可用于开仓",
                theme::TEXT,
            );
            metric(
                &mut cols[3],
                "未实现盈亏",
                &format!("{:+.2} USDT", self.account.unrealized_profit),
                &format!("维持保证金 {:.2}", self.account.maintenance_margin),
                if self.account.unrealized_profit >= 0.0 {
                    theme::GREEN
                } else {
                    theme::RED
                },
            );
        });
        ui.add_space(18.0);
        section_title(
            ui,
            "当前持仓",
            &format!("{} 个有效仓位", self.account.positions.len()),
        );
        Frame::NONE
            .fill(theme::SURFACE)
            .stroke(Stroke::new(1.0, theme::BORDER))
            .corner_radius(5)
            .inner_margin(Margin::same(14))
            .show(ui, |ui| {
                if self.account.positions.is_empty() {
                    ui.label(RichText::new("当前没有合约持仓").color(theme::MUTED));
                    return;
                }
                ScrollArea::vertical().show(ui, |ui| {
                    egui::Grid::new("positions-grid")
                        .num_columns(9)
                        .striped(true)
                        .spacing([18.0, 12.0])
                        .show(ui, |ui| {
                            for heading in [
                                "合约",
                                "方向",
                                "数量",
                                "开仓价",
                                "标记价",
                                "杠杆",
                                "未实现盈亏",
                                "强平价",
                                "模式",
                            ] {
                                ui.label(RichText::new(heading).color(theme::MUTED).strong());
                            }
                            ui.end_row();
                            for position in &self.account.positions {
                                ui.label(RichText::new(&position.symbol).strong());
                                ui.colored_label(
                                    if position.side == "多" {
                                        theme::GREEN
                                    } else {
                                        theme::RED
                                    },
                                    &position.side,
                                );
                                ui.label(format!("{:.4}", position.quantity));
                                ui.label(format_price(position.entry_price));
                                ui.label(format_price(position.mark_price));
                                ui.label(format!("{}x", position.leverage));
                                ui.colored_label(
                                    if position.unrealized_profit >= 0.0 {
                                        theme::GREEN
                                    } else {
                                        theme::RED
                                    },
                                    format!("{:+.2}", position.unrealized_profit),
                                );
                                ui.label(format_price(position.liquidation_price));
                                ui.label(&position.margin_type);
                                ui.end_row();
                            }
                        });
                });
            });
    }

    fn render_position_history(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.label(RichText::new("仓位历史").size(16.0).strong());
                ui.label(
                    RichText::new(format!(
                        "最近 {} 笔 · 模拟交易数据库",
                        self.simulation_account.trade_history.len()
                    ))
                    .size(11.0)
                    .color(theme::MUTED),
                );
            });
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui
                    .add_enabled(
                        !self.simulation_check_running,
                        theme::secondary_button(if self.simulation_check_running {
                            "刷新中..."
                        } else {
                            "刷新历史"
                        }),
                    )
                    .clicked()
                {
                    self.last_simulation_check = Instant::now() - Duration::from_secs(30);
                    self.refresh_simulation_account();
                }
            });
        });
        ui.separator();
        if !self.simulation_error.is_empty() {
            ui.colored_label(theme::RED, &self.simulation_error);
        }
        ui.add_space(10.0);
        ui.columns(4, |cols| {
            metric(
                &mut cols[0],
                "累计已实现",
                &format!("{:+.2} USDT", self.simulation_account.realized_profit),
                "模拟交易数据库",
                if self.simulation_account.realized_profit >= 0.0 {
                    theme::GREEN
                } else {
                    theme::RED
                },
            );
            metric(
                &mut cols[1],
                "已平仓",
                &self.simulation_account.closed_trades.to_string(),
                "历史成交",
                theme::TEXT,
            );
            metric(
                &mut cols[2],
                "持仓中",
                &self.simulation_account.open_trades.len().to_string(),
                "当前模拟仓位",
                theme::YELLOW,
            );
            let win_rate = if self.simulation_account.closed_trades > 0 {
                self.simulation_account.winning_trades as f64
                    / self.simulation_account.closed_trades as f64
                    * 100.0
            } else {
                0.0
            };
            metric(
                &mut cols[3],
                "胜率",
                &format!("{win_rate:.1}%"),
                &format!("{} 笔盈利", self.simulation_account.winning_trades),
                theme::TEXT,
            );
        });
        ui.add_space(18.0);
        Frame::NONE
            .fill(theme::SURFACE)
            .stroke(Stroke::new(1.0, theme::BORDER))
            .corner_radius(5)
            .inner_margin(Margin::same(14))
            .show(ui, |ui| {
                if self.simulation_account.trade_history.is_empty() {
                    ui.label(RichText::new("暂无仓位历史").color(theme::MUTED));
                    return;
                }
                ScrollArea::both()
                    .id_salt("position-history-scroll")
                    .max_height((ui.available_height() - 12.0).max(360.0))
                    .show(ui, |ui| {
                        egui::Grid::new("position-history-grid")
                            .num_columns(14)
                            .striped(true)
                            .spacing([18.0, 12.0])
                            .show(ui, |ui| {
                                for heading in [
                                    "合约",
                                    "状态",
                                    "方向",
                                    "数量",
                                    "保证金",
                                    "开仓价",
                                    "平仓价",
                                    "杠杆",
                                    "已实现",
                                    "收益率",
                                    "开仓时间",
                                    "平仓时间",
                                    "信号",
                                    "退出",
                                ] {
                                    ui.label(RichText::new(heading).color(theme::MUTED).strong());
                                }
                                ui.end_row();
                                for trade in &self.simulation_account.trade_history {
                                    ui.label(RichText::new(&trade.pair).strong());
                                    ui.colored_label(
                                        if trade.status == "持仓中" {
                                            theme::YELLOW
                                        } else {
                                            theme::MUTED
                                        },
                                        &trade.status,
                                    );
                                    ui.colored_label(
                                        if trade.side == "多" {
                                            theme::GREEN
                                        } else {
                                            theme::RED
                                        },
                                        &trade.side,
                                    );
                                    ui.label(format!("{:.4}", trade.amount));
                                    ui.label(format!("{:.2}", trade.stake_amount));
                                    ui.label(format_price(trade.open_rate));
                                    ui.label(
                                        trade
                                            .close_rate
                                            .map(format_price)
                                            .unwrap_or_else(|| "--".into()),
                                    );
                                    ui.label(format!("{:.1}x", trade.leverage));
                                    ui.colored_label(
                                        if trade.profit_abs >= 0.0 {
                                            theme::GREEN
                                        } else {
                                            theme::RED
                                        },
                                        format!("{:+.2}", trade.profit_abs),
                                    );
                                    ui.colored_label(
                                        if trade.profit_percent >= 0.0 {
                                            theme::GREEN
                                        } else {
                                            theme::RED
                                        },
                                        format!("{:+.2}%", trade.profit_percent),
                                    );
                                    ui.label(&trade.open_date);
                                    ui.label(if trade.close_date.is_empty() {
                                        "--"
                                    } else {
                                        &trade.close_date
                                    });
                                    ui.label(&trade.tag);
                                    ui.label(if trade.exit_reason.is_empty() {
                                        "--"
                                    } else {
                                        &trade.exit_reason
                                    });
                                    ui.end_row();
                                }
                            });
                    });
            });
    }

    fn render_simulation_account(&mut self, ui: &mut Ui) {
        if !self.simulation_error.is_empty() {
            ui.colored_label(theme::RED, &self.simulation_error);
        }
        ui.add_space(12.0);
        let win_rate = if self.simulation_account.closed_trades > 0 {
            self.simulation_account.winning_trades as f64
                / self.simulation_account.closed_trades as f64
                * 100.0
        } else {
            0.0
        };
        ui.columns(4, |cols| {
            metric(
                &mut cols[0],
                "模拟权益",
                &format!("{:.2} USDT", self.simulation_account.wallet_balance),
                "独立虚拟资金",
                theme::YELLOW,
            );
            metric(
                &mut cols[1],
                "模拟可用",
                &format!("{:.2} USDT", self.simulation_account.available_balance),
                &format!("持仓占用 {:.2}", self.simulation_account.open_stake),
                theme::TEXT,
            );
            metric(
                &mut cols[2],
                "累计已实现",
                &format!("{:+.2} USDT", self.simulation_account.realized_profit),
                &format!("{} 笔已平仓", self.simulation_account.closed_trades),
                if self.simulation_account.realized_profit >= 0.0 {
                    theme::GREEN
                } else {
                    theme::RED
                },
            );
            metric(
                &mut cols[3],
                "模拟胜率",
                &format!("{win_rate:.1}%"),
                &format!("{} 笔盈利", self.simulation_account.winning_trades),
                theme::TEXT,
            );
        });
        ui.add_space(18.0);
        section_title(
            ui,
            "模拟持仓",
            &format!("{} 个虚拟仓位", self.simulation_account.open_trades.len()),
        );
        Frame::NONE
            .fill(theme::SURFACE)
            .stroke(Stroke::new(1.0, theme::BORDER))
            .corner_radius(5)
            .inner_margin(Margin::same(14))
            .show(ui, |ui| {
                if self.simulation_account.open_trades.is_empty() {
                    ui.label(RichText::new("当前没有模拟持仓").color(theme::MUTED));
                    return;
                }
                ScrollArea::vertical().show(ui, |ui| {
                    egui::Grid::new("simulation-positions-grid")
                        .num_columns(8)
                        .striped(true)
                        .spacing([18.0, 12.0])
                        .show(ui, |ui| {
                            for heading in [
                                "合约",
                                "方向",
                                "数量",
                                "保证金",
                                "开仓价",
                                "杠杆",
                                "开仓时间",
                                "信号",
                            ] {
                                ui.label(RichText::new(heading).color(theme::MUTED).strong());
                            }
                            ui.end_row();
                            for trade in &self.simulation_account.open_trades {
                                ui.label(RichText::new(&trade.pair).strong());
                                ui.colored_label(
                                    if trade.side == "多" {
                                        theme::GREEN
                                    } else {
                                        theme::RED
                                    },
                                    &trade.side,
                                );
                                ui.label(format!("{:.4}", trade.amount));
                                ui.label(format!("{:.2}", trade.stake_amount));
                                ui.label(format_price(trade.open_rate));
                                ui.label(format!("{:.1}x", trade.leverage));
                                ui.label(&trade.open_date);
                                ui.label(&trade.tag);
                                ui.end_row();
                            }
                        });
                });
            });
    }

    fn render_event_prediction(&mut self, ui: &mut Ui) {
        ScrollArea::vertical()
            .id_salt("event-prediction-page")
            .auto_shrink([false, false])
            .show(ui, |ui| self.render_event_prediction_inner(ui));
    }

    fn render_event_prediction_inner(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            ui.vertical(|ui| {
                ui.label(RichText::new("事件预测虚拟盘").size(16.0).strong());
                ui.label(
                    RichText::new(
                        format!(
                            "仅 BTC/ETH；10m / 30m / 1h 各自运行；每周期 5 条线，单线起始 5U、周期初始投入 25U；按轮次复投；当前策略 {}",
                            event_prediction::EVENT_STRATEGY_NAME
                        ),
                    )
                        .size(11.0)
                        .color(theme::MUTED),
                );
            });
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui
                    .add_enabled(
                        !self.event_prediction_running,
                        theme::primary_button(if self.event_prediction_running {
                            "运行中..."
                        } else {
                            "立即跑一轮"
                        }),
                    )
                    .clicked()
                {
                    self.event_prediction_run_dialog_open = true;
                }
                let changed = ui
                    .checkbox(&mut self.event_prediction_enabled, "启用每分钟策略评估")
                    .changed();
                if changed {
                    let value = if self.event_prediction_enabled {
                        "true"
                    } else {
                        "false"
                    };
                    if let Err(error) = self.store.set_setting("event_prediction_enabled", value) {
                        self.toast(error.to_string(), true);
                    }
                }
            });
        });
        ui.separator();
        ui.label(
            RichText::new(&self.event_prediction_status)
                .size(12.0)
                .color(theme::MUTED),
        );
        ui.add_space(10.0);

        let stat10 = event_stat(&self.event_prediction_stats, 10);
        let stat30 = event_stat(&self.event_prediction_stats, 30);
        let stat60 = event_stat(&self.event_prediction_stats, 60);
        let all_stat10 = event_stat(&self.event_prediction_all_stats, 10);
        let all_stat30 = event_stat(&self.event_prediction_all_stats, 30);
        let all_stat60 = event_stat(&self.event_prediction_all_stats, 60);
        ui.columns(4, |cols| {
            metric(
                &mut cols[0],
                "周期初始投入",
                &format!(
                    "{:.2} USDT",
                    self.event_prediction_stake_amount * event_prediction::EVENT_CYCLE_SLOTS as f64
                ),
                &format!(
                    "5 条线 × 单线 5U；虚拟本金 {}",
                    format_event_money(self.event_prediction_starting_bankroll)
                ),
                theme::YELLOW,
            );
            metric(
                &mut cols[1],
                "单线起始本金",
                &format!("{:.2} USDT", self.event_prediction_stake_amount),
                "周期初始投入 25U（5 条线 × 5U）；赢后该线回报全额复投",
                theme::TEXT,
            );
            metric(
                &mut cols[2],
                "当前策略收益",
                &format!("{:+.2} USDT", self.event_prediction_realized_pnl),
                &format!(
                    "{} 已结算 {:+.2}",
                    event_prediction::EVENT_STRATEGY_NAME,
                    self.event_prediction_realized_pnl
                ),
                if self.event_prediction_realized_pnl >= 0.0 {
                    theme::GREEN
                } else {
                    theme::RED
                },
            );
            metric(
                &mut cols[3],
                "全历史收益",
                &format!("{:+.2} USDT", self.event_prediction_all_realized_pnl),
                &format!(
                    "旧策略 + 当前策略；当前占用 {:.2}",
                    self.event_prediction_open_exposure
                ),
                if self.event_prediction_all_realized_pnl >= 0.0 {
                    theme::GREEN
                } else {
                    theme::RED
                },
            );
        });
        ui.add_space(10.0);
        ui.columns(4, |cols| {
            metric(
                &mut cols[0],
                "未结算票据",
                &self.event_prediction_open_count.to_string(),
                &format!(
                    "周期票等待到期；另有旧玩法票 {} 张正在清算",
                    self.event_prediction_legacy_open_count
                ),
                theme::YELLOW,
            );
            event_metric(&mut cols[1], "周期策略 10m", &stat10);
            event_metric(&mut cols[2], "周期策略 30m", &stat30);
            event_metric(&mut cols[3], "周期策略 1h", &stat60);
        });
        ui.add_space(10.0);
        ui.columns(4, |cols| {
            metric(
                &mut cols[0],
                "统计口径",
                "10m / 30m / 1h",
                "当前策略与全历史分别展示",
                theme::TEXT,
            );
            event_metric(&mut cols[1], "历史 10m", &all_stat10);
            event_metric(&mut cols[2], "历史 30m", &all_stat30);
            event_metric(&mut cols[3], "历史 1h", &all_stat60);
        });
        ui.add_space(18.0);
        if let Some(action) = event_ticket_list_card(
            ui,
            "未结算票据",
            "按到期时间排序，最先需要复盘的排前面",
            &self.event_prediction_recent,
            "暂无未结算票据",
            EventOrderKind::Open,
            &mut self.event_prediction_order_dialog,
        ) {
            self.handle_event_ticket_action(action);
        }
        ui.add_space(14.0);
        if let Some(action) = event_ticket_list_card(
            ui,
            "历史已结算",
            "最近 80 条已结算虚拟订单，胜/负/平都在这里看",
            &self.event_prediction_history,
            "暂无历史已结算票据",
            EventOrderKind::Settled,
            &mut self.event_prediction_order_dialog,
        ) {
            self.handle_event_ticket_action(action);
        }
    }

    fn render_market(&mut self, ui: &mut Ui) {
        let symbols = self.visible_symbols();
        ui.horizontal(|ui| {
            egui::ComboBox::from_id_salt("symbol")
                .selected_text(&self.symbol)
                .width(150.0)
                .show_ui(ui, |ui| {
                    for symbol in symbols {
                        if ui
                            .selectable_value(&mut self.symbol, symbol.clone(), &symbol)
                            .clicked()
                        {
                            self.select_market();
                        }
                    }
                });
            for interval in Interval::MARKET {
                let selected = interval == self.interval;
                let button =
                    egui::Button::new(RichText::new(interval.label()).color(if selected {
                        Color32::BLACK
                    } else {
                        theme::MUTED
                    }))
                    .fill(if selected {
                        theme::YELLOW
                    } else {
                        theme::SURFACE
                    })
                    .stroke(Stroke::new(1.0, theme::BORDER))
                    .corner_radius(3);
                if ui.add_sized([64.0, 34.0], button).clicked() {
                    self.interval = interval;
                    self.select_market();
                }
            }
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                ui.colored_label(
                    if self.market_connected {
                        theme::GREEN
                    } else {
                        theme::RED
                    },
                    if self.market_connected {
                        "● 实时"
                    } else {
                        "● 断开"
                    },
                );
            });
        });
        ui.add_space(12.0);
        Frame::NONE
            .fill(Color32::from_rgb(8, 11, 14))
            .stroke(Stroke::new(1.0, theme::BORDER))
            .corner_radius(4)
            .inner_margin(Margin::same(14))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(RichText::new(&self.symbol).size(13.0).color(theme::MUTED));
                        ui.label(
                            RichText::new(format_price(self.snapshot.price))
                                .size(25.0)
                                .strong(),
                        );
                    });
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let positive = self.snapshot.change_percent >= 0.0;
                        ui.label(
                            RichText::new(format!(
                                "{}{:.2}%",
                                if positive { "+" } else { "" },
                                self.snapshot.change_percent
                            ))
                            .color(if positive { theme::GREEN } else { theme::RED })
                            .strong(),
                        );
                    });
                });
                ui.add_space(10.0);
                draw_candles(
                    ui,
                    &self.candles,
                    self.interval,
                    &self.market_error,
                    self.market_connected,
                    460.0,
                );
            });
        ui.add_space(14.0);
        ui.columns(2, |columns| {
            columns[0].vertical(|ui| {
                section_title(ui, "市场情绪", &self.snapshot.sentiment.label);
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new(self.snapshot.sentiment.score.to_string())
                            .size(30.0)
                            .strong(),
                    );
                    ui.label(RichText::new("/ 100").color(theme::MUTED));
                });
                ui.add(
                    egui::ProgressBar::new(self.snapshot.sentiment.score as f32 / 100.0)
                        .fill(theme::YELLOW),
                );
                ui.add_space(16.0);
                status_row(ui, "标记价格", &format_price(self.snapshot.mark_price));
                status_row(
                    ui,
                    "资金费率",
                    &format!("{:.4}%", self.snapshot.funding_rate * 100.0),
                );
                status_row(
                    ui,
                    "多空比",
                    &format!("{:.3}", self.snapshot.long_short_ratio),
                );
                status_row(ui, "持仓量", &compact_number(self.snapshot.open_interest));
                status_row(
                    ui,
                    "24h 成交额",
                    &format!("{} USDT", compact_number(self.snapshot.quote_volume)),
                );
            });
            columns[1].vertical(|ui| {
                section_title(ui, "AI 研判", self.ai_provider.label());
                egui::ComboBox::from_id_salt("ai-provider")
                    .selected_text(self.ai_provider.label())
                    .show_ui(ui, |ui| {
                        for provider in AiProvider::ALL {
                            ui.selectable_value(&mut self.ai_provider, provider, provider.label());
                        }
                    });
                self.normalize_ai_model_for_provider();
                ui.add(TextEdit::singleline(&mut self.ai_model).hint_text(
                    if self.ai_provider == AiProvider::Relay {
                        RELAY_DEFAULT_MODEL
                    } else {
                        "模型（可选）"
                    },
                ));
                ui.add_sized(
                    [ui.available_width(), 74.0],
                    TextEdit::multiline(&mut self.ai_prompt),
                );
                if ui
                    .add_enabled_ui(!self.ai_running, |ui| {
                        ui.add_sized(
                            [ui.available_width(), 36.0],
                            theme::primary_button(if self.ai_running {
                                "分析中..."
                            } else {
                                "开始分析"
                            }),
                        )
                    })
                    .inner
                    .clicked()
                {
                    self.run_ai();
                }
                ScrollArea::vertical().max_height(130.0).show(ui, |ui| {
                    ui.label(RichText::new(&self.ai_output).color(theme::MUTED));
                });
            });
        });
        ui.add_space(14.0);
        self.render_scanner_panel(ui);
        if !self.market_error.is_empty() && !self.market_connected {
            ui.colored_label(theme::RED, &self.market_error);
        }
    }

    fn render_scanner_panel(&mut self, ui: &mut Ui) {
        let snapshot = &self.scanner_snapshot;
        ui.horizontal(|ui| {
            section_title(
                ui,
                "全市场扫描",
                &format!("{} 个 USDT 永续", snapshot.total_symbols),
            );
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                ui.colored_label(
                    if self.scanner_connected {
                        theme::GREEN
                    } else {
                        theme::RED
                    },
                    if self.scanner_connected {
                        "● 扫描中"
                    } else {
                        "● 扫描断开"
                    },
                );
            });
        });
        if !snapshot.recommendations.is_empty() {
            ui.add_space(6.0);
            let heading = if snapshot.sentiment_configured {
                format!(
                    "推荐候选（已接入 {} 条舆情事件）",
                    snapshot.sentiment_events
                )
            } else {
                "推荐候选（未配置新闻/X API，当前只展示行情候选）".into()
            };
            ui.label(RichText::new(heading).color(theme::MUTED));
            for recommendation in snapshot.recommendations.iter().take(5) {
                Frame::NONE
                    .fill(Color32::from_rgb(12, 16, 19))
                    .stroke(Stroke::new(1.0, theme::BORDER))
                    .corner_radius(3)
                    .inner_margin(Margin::same(9))
                    .show(ui, |ui| {
                        ui.horizontal(|ui| {
                            ui.label(RichText::new(&recommendation.symbol).strong());
                            ui.colored_label(
                                if recommendation.side == "LONG" {
                                    theme::GREEN
                                } else {
                                    theme::RED
                                },
                                &recommendation.side,
                            );
                            ui.label(format!(
                                "评分 {:.0} · 置信度 {:.0}%",
                                recommendation.score,
                                recommendation.confidence * 100.0
                            ));
                            ui.label(format!(
                                "{} 杠杆上限 {}x / 建议 {}x",
                                recommendation.category,
                                recommendation.leverage_cap,
                                recommendation.suggested_leverage
                            ));
                        });
                        ui.label(RichText::new(&recommendation.reason).color(theme::MUTED));
                        ui.label(
                            RichText::new(format!(
                                "触发：{} · 止损 {:.2}% · 止盈 {:.2}% · {}",
                                recommendation.trigger,
                                recommendation.stop_loss_percent,
                                recommendation.take_profit_percent,
                                recommendation.status
                            ))
                            .color(theme::MUTED),
                        );
                    });
                ui.add_space(4.0);
            }
        } else {
            let message = if snapshot.sentiment_configured {
                "当前没有通过行情和舆情双重门槛的候选，不强行推荐。"
            } else {
                "当前没有可执行推荐；配置合规新闻/X API 后才会启用舆情确认。"
            };
            ui.label(RichText::new(message).color(theme::MUTED));
        }
        if !snapshot.headlines.is_empty() {
            ui.add_space(8.0);
            ui.label(RichText::new("最新舆情事件").color(theme::MUTED));
            for headline in snapshot.headlines.iter().take(5) {
                ui.horizontal_wrapped(|ui| {
                    let sentiment_color = if headline.sentiment > 0.15 {
                        theme::GREEN
                    } else if headline.sentiment < -0.15 {
                        theme::RED
                    } else {
                        theme::MUTED
                    };
                    ui.colored_label(sentiment_color, &headline.symbol);
                    ui.label(format!(
                        "{} · {} · {}",
                        headline.event_type,
                        headline.source,
                        format_scanner_time(headline.published_at)
                    ));
                    if headline.url.starts_with("http://") || headline.url.starts_with("https://") {
                        ui.hyperlink_to("来源", &headline.url);
                    }
                    ui.label(&headline.title);
                });
            }
        }
        if !snapshot.candidates.is_empty() {
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.label(RichText::new("全币种行情").color(theme::MUTED));
                ui.label(
                    RichText::new("按综合评分排序，持仓量仅对前 12 个高流动性合约请求")
                        .color(theme::MUTED),
                );
            });
            ScrollArea::both()
                .id_salt("scanner-market-table")
                .max_height(390.0)
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    egui::Grid::new("scanner-market-grid")
                        .striped(true)
                        .min_col_width(72.0)
                        .show(ui, |ui| {
                            for heading in [
                                "交易对",
                                "分类",
                                "价格",
                                "24h涨跌",
                                "成交额",
                                "资金费率",
                                "舆情",
                                "评分",
                            ] {
                                ui.label(RichText::new(heading).strong().color(theme::MUTED));
                            }
                            ui.end_row();
                            for candidate in &snapshot.candidates {
                                ui.label(RichText::new(&candidate.symbol).strong());
                                ui.label(&candidate.category);
                                ui.label(format_price(candidate.price));
                                let change_color = if candidate.change_percent > 0.0 {
                                    theme::GREEN
                                } else if candidate.change_percent < 0.0 {
                                    theme::RED
                                } else {
                                    theme::MUTED
                                };
                                ui.colored_label(
                                    change_color,
                                    format!("{:+.2}%", candidate.change_percent),
                                );
                                ui.label(compact_scanner_volume(candidate.quote_volume));
                                ui.label(format!("{:+.4}%", candidate.funding_rate * 100.0));
                                let sentiment_color = if candidate.sentiment_score > 0.15 {
                                    theme::GREEN
                                } else if candidate.sentiment_score < -0.15 {
                                    theme::RED
                                } else {
                                    theme::MUTED
                                };
                                ui.colored_label(
                                    sentiment_color,
                                    if candidate.sentiment_event_count == 0 {
                                        "--".into()
                                    } else {
                                        format!(
                                            "{} {:.2}",
                                            candidate.sentiment_label, candidate.sentiment_score
                                        )
                                    },
                                );
                                ui.label(format!("{:.1}", candidate.market_score));
                                ui.end_row();
                            }
                        });
                });
        }
        self.render_scanner_rankings(ui, snapshot);
        ui.add_space(4.0);
        ui.label(
            RichText::new(format!(
                "候选池 {} 个 · 数据质量 {:.0}% · 舆情来源 {} · 内部时间 UTC，界面按 UTC+8 显示",
                snapshot.candidates.len(),
                snapshot.data_quality * 100.0,
                if snapshot.sentiment_configured {
                    "已配置"
                } else {
                    "未配置"
                }
            ))
            .color(theme::MUTED),
        );
        if !snapshot.sentiment_error.is_empty() {
            ui.colored_label(
                theme::RED,
                format!("舆情源暂不可用：{}", snapshot.sentiment_error),
            );
        }
        if !self.scanner_error.is_empty() {
            ui.colored_label(theme::RED, &self.scanner_error);
        }
    }

    fn render_scanner_rankings(&self, ui: &mut Ui, snapshot: &UniverseSnapshot) {
        ui.add_space(10.0);
        ui.label(RichText::new("市场榜单").color(theme::MUTED));
        ui.columns(3, |columns| {
            let sections = [
                ("涨幅榜", &snapshot.gainers, theme::GREEN),
                ("跌幅榜", &snapshot.losers, theme::RED),
                ("热门榜", &snapshot.hot, theme::YELLOW),
            ];
            for (column, (title, rows, accent)) in columns.iter_mut().zip(sections) {
                Frame::NONE
                    .fill(Color32::from_rgb(12, 16, 19))
                    .stroke(Stroke::new(1.0, theme::BORDER))
                    .corner_radius(3)
                    .inner_margin(Margin::same(8))
                    .show(column, |ui| {
                        ui.label(RichText::new(title).strong().color(accent));
                        for row in rows.iter().take(10) {
                            ui.horizontal(|ui| {
                                ui.label(RichText::new(&row.symbol).strong());
                                ui.label(format_price(row.price));
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    ui.colored_label(
                                        if row.change_percent >= 0.0 {
                                            theme::GREEN
                                        } else {
                                            theme::RED
                                        },
                                        format!("{:+.2}%", row.change_percent),
                                    );
                                });
                            });
                        }
                    });
            }
        });
    }

    fn render_strategy(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("FuturesFactorStrategy.py").color(theme::MUTED));
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if ui.add(theme::primary_button("保存并校验")).clicked() {
                    match self.workspace.save_strategy(&self.strategy_source) {
                        Ok(()) => {
                            self.strategy_state = "校验通过".into();
                            self.toast("策略已保存", false);
                            self.hot_reload_bot("策略已保存，正在热重载交易内核");
                        }
                        Err(error) => {
                            self.strategy_state = "校验失败".into();
                            self.toast(error.to_string(), true);
                        }
                    }
                }
                ui.label(RichText::new(&self.strategy_state).color(
                    if self.strategy_state == "校验失败" {
                        theme::RED
                    } else {
                        theme::YELLOW
                    },
                ));
            });
        });
        ui.add_space(8.0);
        let editor_view_height = ui.available_height().max(360.0);
        let mut changed = false;
        Frame::NONE
            .fill(Color32::from_rgb(8, 10, 13))
            .stroke(Stroke::new(1.0, theme::BORDER))
            .corner_radius(4)
            .inner_margin(Margin::same(8))
            .show(ui, |ui| {
                ScrollArea::both()
                    .id_salt("strategy-source-scroll")
                    .auto_shrink([false, false])
                    .max_height(editor_view_height)
                    .show(ui, |ui| {
                        let rows = self.strategy_source.lines().count().max(32);
                        let editor_height = (rows as f32 * 18.0 + 24.0).max(editor_view_height);
                        let response = ui.add_sized(
                            [ui.available_width(), editor_height],
                            TextEdit::multiline(&mut self.strategy_source)
                                .font(egui::TextStyle::Monospace)
                                .code_editor()
                                .desired_rows(rows)
                                .desired_width(f32::INFINITY),
                        );
                        changed |= response.changed();
                    });
            });
        if changed {
            self.strategy_state = "未保存".into();
        }
    }

    fn render_backtest(&mut self, ui: &mut Ui) {
        self.job_controls(ui, true);
        ui.add_space(18.0);
        section_title(ui, "回测输出", "Docker / Freqtrade");
        log_view(ui, &self.bot_log, 430.0);
    }

    fn render_data(&mut self, ui: &mut Ui) {
        self.job_controls(ui, false);
        ui.add_space(18.0);
        section_title(ui, "同步输出", "Binance Futures 历史数据");
        log_view(ui, &self.bot_log, 430.0);
    }

    fn job_controls(&mut self, ui: &mut Ui, backtest: bool) {
        let symbols = self.visible_symbols();
        Frame::NONE
            .fill(theme::SURFACE)
            .stroke(Stroke::new(1.0, theme::BORDER))
            .corner_radius(5)
            .inner_margin(Margin::same(14))
            .show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    if backtest {
                        ui.label("开始日期");
                        ui.add_sized(
                            [110.0, 34.0],
                            TextEdit::singleline(&mut self.backtest_start),
                        );
                        ui.label("结束日期");
                        ui.add_sized([110.0, 34.0], TextEdit::singleline(&mut self.backtest_end));
                        ui.label("费率");
                        ui.add(
                            egui::DragValue::new(&mut self.backtest_fee)
                                .speed(0.0001)
                                .range(0.0..=0.01),
                        );
                    } else {
                        ui.label("历史天数");
                        ui.add(egui::DragValue::new(&mut self.download_days).range(30..=3650));
                    }
                    if ui
                        .add_enabled_ui(!self.job_running, |ui| {
                            ui.add(theme::primary_button(if self.job_running {
                                "任务运行中..."
                            } else if backtest {
                                "同步数据并回测"
                            } else {
                                "同步数据"
                            }))
                        })
                        .inner
                        .clicked()
                    {
                        self.run_job(backtest);
                    }
                });
                ui.separator();
                ui.horizontal_wrapped(|ui| {
                    for symbol in symbols {
                        let mut selected = self.selected_pairs.iter().any(|value| value == &symbol);
                        if ui
                            .checkbox(&mut selected, symbol.trim_end_matches("USDT"))
                            .changed()
                        {
                            if selected {
                                self.selected_pairs.push(symbol);
                            } else {
                                self.selected_pairs.retain(|value| value != &symbol);
                            }
                        }
                    }
                });
            });
    }

    fn run_job(&mut self, backtest: bool) {
        if self.selected_pairs.is_empty() {
            self.toast("请至少选择一个交易对", true);
            return;
        }
        self.job_running = true;
        self.bot_log = if backtest {
            "正在同步所选周期的历史数据，完成后将自动运行回测...".into()
        } else {
            "正在同步 Binance Futures 历史数据...".into()
        };
        let workspace = self.workspace.clone();
        let sender = self.task_sender.clone();
        let pairs = self.selected_pairs.clone();
        let start = self.backtest_start.clone();
        let end = self.backtest_end.clone();
        let fee = self.backtest_fee;
        let days = self.download_days;
        thread::spawn(move || {
            let result = if backtest {
                workspace.run_backtest(&start, &end, fee, &pairs)
            } else {
                workspace.download_data(days, &pairs)
            }
            .map_err(|error| error.to_string());
            let _ = sender.send(TaskEvent::Job(result));
        });
    }

    fn run_ai(&mut self) {
        let Some(key) = self.unlocked_key.as_ref() else {
            return;
        };
        let secret_name = match self.ai_provider {
            AiProvider::OpenAi => "openai_api_key",
            AiProvider::Claude => "anthropic_api_key",
            AiProvider::DeepSeek => "deepseek_api_key",
            AiProvider::Relay => "relay_api_key",
        };
        let api_key = self
            .store
            .get_secret(key, secret_name)
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();
        self.ai_running = true;
        self.ai_output = "正在读取当前行情并生成风险研判...".into();
        let sender = self.task_sender.clone();
        let provider = self.ai_provider;
        let model = self.ai_model.clone();
        let relay_base_url = self.relay_base_url.clone();
        let snapshot = self.snapshot.clone();
        let prompt = self.ai_prompt.clone();
        thread::spawn(move || {
            let result = ai::analyze(
                provider,
                &model,
                &api_key,
                &relay_base_url,
                &snapshot,
                &prompt,
            )
            .map_err(|error| error.to_string());
            let _ = sender.send(TaskEvent::Ai(result));
        });
    }

    fn render_execution(&mut self, ui: &mut Ui) {
        let strategy_name = if self.ai_config.enabled {
            "AiSignalStrategy"
        } else {
            "FuturesFactorStrategy"
        };
        let (state_label, state_detail, state_color, state_icon) = self.execution_status();
        ui.horizontal_wrapped(|ui| {
            if ui.add(theme::secondary_button("编辑策略")).clicked() {
                self.page = Page::Strategy;
            }
            if ui.add(theme::secondary_button("同步历史数据")).clicked() {
                self.page = Page::Data;
            }
            if ui.add(theme::secondary_button("运行回测")).clicked() {
                self.page = Page::Backtest;
            }
            ui.separator();
            status_chip(ui, self.ai_config.strategy_profile.label(), true);
            status_chip(ui, &format!("{} 周期", self.ai_config.timeframe), true);
            status_chip(ui, "多空双向", true);
            status_chip(ui, &format!("最高 {}x", self.ai_config.leverage), true);
            ui.separator();
            if ui.selectable_label(self.dry_run, "模拟盘").clicked() && !self.dry_run {
                self.set_trading_mode(true);
            }
            if ui.selectable_label(!self.dry_run, "实盘").clicked() && self.dry_run {
                if self.bot_state == "running" {
                    self.toast("请先停止策略再切换运行模式", true);
                } else {
                    self.live_acknowledged = false;
                    self.live_confirmation = Some(LiveAction::Enable);
                }
            }
            ui.label(
                RichText::new(format!(
                    "单仓上限 {:.0} USDT · 最多 {} 仓",
                    self.stake_amount, self.max_open_trades
                ))
                .color(theme::MUTED),
            );
        });
        ui.add_space(14.0);
        Frame::NONE
            .fill(theme::SURFACE)
            .stroke(Stroke::new(1.5, state_color))
            .corner_radius(5)
            .inner_margin(Margin::same(18))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    Frame::NONE
                        .fill(Color32::from_rgba_premultiplied(
                            state_color.r(),
                            state_color.g(),
                            state_color.b(),
                            35,
                        ))
                        .corner_radius(5)
                        .inner_margin(Margin::same(12))
                        .show(ui, |ui| {
                            ui.label(theme::icon(state_icon, 30.0, state_color));
                        });
                    ui.vertical(|ui| {
                        ui.label(
                            RichText::new(state_label)
                                .size(23.0)
                                .color(state_color)
                                .strong(),
                        );
                        ui.label(
                            RichText::new(format!(
                                "{} · {} · {}",
                                state_detail,
                                if self.dry_run { "模拟盘" } else { "实盘" },
                                strategy_name
                            ))
                            .size(13.0)
                            .color(theme::TEXT),
                        );
                        ui.label(
                            RichText::new(format!(
                                "Freqtrade: {} · Docker: {}",
                                self.bot_state,
                                if self.docker_available {
                                    "已连接"
                                } else {
                                    "不可用"
                                }
                            ))
                            .size(11.0)
                            .color(theme::MUTED),
                        );
                        ui.label(
                            RichText::new(format!("AI 闭环: {}", self.ai_decision_status))
                                .size(11.0)
                                .color(if self.ai_config.enabled {
                                    theme::YELLOW
                                } else {
                                    theme::MUTED
                                }),
                        );
                    });
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        let start_label = if self.bot_action_running {
                            "处理中..."
                        } else if self.dry_run {
                            "启动模拟策略"
                        } else {
                            "启动实盘策略"
                        };
                        if ui
                            .add_enabled(
                                !self.bot_action_running,
                                theme::primary_button(start_label),
                            )
                            .clicked()
                        {
                            if self.dry_run {
                                self.bot_action(true);
                            } else {
                                self.live_acknowledged = false;
                                self.live_confirmation = Some(LiveAction::Start);
                            }
                        }
                        if ui
                            .add_enabled(!self.bot_action_running, theme::secondary_button("停止"))
                            .clicked()
                        {
                            self.bot_action(false);
                        }
                        if ui.add(theme::secondary_button("刷新日志")).clicked() {
                            self.refresh_logs();
                        }
                    });
                });
            });
        ui.add_space(18.0);
        section_title(
            ui,
            "运行日志",
            if self.docker_available {
                "Docker 已连接"
            } else {
                "Docker 不可用"
            },
        );
        log_view(ui, &self.bot_log, 500.0);
    }

    fn execution_status(&self) -> (&'static str, &'static str, Color32, &'static str) {
        if !self.docker_available {
            return (
                "Docker 不可用",
                "交易内核无法启动",
                theme::RED,
                "circle-alert",
            );
        }
        match self.bot_state.to_ascii_lowercase().as_str() {
            "running" => (
                "策略运行中",
                "正在监听信号并执行策略",
                theme::GREEN,
                "circle-play",
            ),
            "created" | "restarting" => (
                "正在启动",
                "交易内核正在准备",
                theme::YELLOW,
                "loader-circle",
            ),
            "paused" => (
                "策略已暂停",
                "容器存在但未执行",
                theme::YELLOW,
                "circle-pause",
            ),
            "exited" | "dead" | "removing" | "stopped" => {
                ("策略未启动", "不会自动下单", theme::RED, "circle-stop")
            }
            _ => (
                "状态检查中",
                "等待 Docker 返回状态",
                theme::MUTED,
                "circle-help",
            ),
        }
    }

    fn bot_action(&mut self, start: bool) {
        self.run_bot_action(start, start, None);
    }

    fn hot_reload_bot(&mut self, message: &str) {
        if self.bot_state != "running" || self.bot_action_running {
            return;
        }
        self.toast(message, false);
        self.run_bot_action(true, true, Some("交易内核已热重载".into()));
    }

    fn run_bot_action(
        &mut self,
        start: bool,
        force_recreate: bool,
        success_message: Option<String>,
    ) {
        if self.bot_action_running {
            self.toast("交易内核操作正在执行中", true);
            return;
        }
        let Some(key) = self.unlocked_key.as_ref() else {
            return;
        };
        let api_key = self
            .store
            .get_secret(key, "binance_api_key")
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();
        let api_secret = self
            .store
            .get_secret(key, "binance_api_secret")
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();
        let workspace = self.workspace.clone();
        let sender = self.task_sender.clone();
        let dry_run = self.dry_run;
        self.bot_action_running = true;
        thread::spawn(move || {
            let result = (|| {
                if start && !dry_run {
                    exchange::ensure_live_futures_trading(&api_key, &api_secret)?;
                }
                workspace.bot_action(start, force_recreate, &api_key, &api_secret)
            })()
            .map(|_| {
                if let Some(message) = success_message {
                    message
                } else if start {
                    if dry_run {
                        "Dry-run 策略已启动".into()
                    } else {
                        "实盘策略已启动".into()
                    }
                } else {
                    "交易内核已停止".into()
                }
            })
            .map_err(|error| error.to_string());
            let _ = sender.send(TaskEvent::Bot(result));
        });
    }

    fn refresh_logs(&self) {
        let workspace = self.workspace.clone();
        let sender = self.task_sender.clone();
        thread::spawn(move || {
            let _ = sender.send(TaskEvent::Logs(workspace.logs()));
        });
    }

    fn set_trading_mode(&mut self, dry_run: bool) {
        if self.bot_state == "running" {
            self.toast("请先停止策略再切换运行模式", true);
            return;
        }
        match self.workspace.update_mode(dry_run) {
            Ok(()) => {
                self.dry_run = dry_run;
                if !dry_run {
                    self.auto_restart = false;
                    let _ = self.store.set_setting("auto_restart", "false");
                }
                self.toast(
                    if dry_run {
                        "已切换到模拟盘"
                    } else {
                        "已启用实盘模式"
                    },
                    false,
                );
            }
            Err(error) => self.toast(error.to_string(), true),
        }
    }

    fn render_event_prediction_dialogs(&mut self, ctx: &egui::Context) {
        self.render_event_prediction_run_dialog(ctx);
        self.render_event_prediction_direction_dialog(ctx);
        self.render_event_prediction_order_dialog(ctx);
        self.render_event_prediction_cycle_dialog(ctx);
        self.render_event_prediction_ticket_dialog(ctx);
    }

    fn handle_event_ticket_action(&mut self, action: EventTicketAction) {
        match action {
            EventTicketAction::Ticket(ticket) => {
                self.event_prediction_cycle_dialog = None;
                self.event_prediction_ticket_dialog = Some(ticket);
            }
            EventTicketAction::Cycle(cycle_id) => self.request_event_prediction_cycle(cycle_id),
        }
    }

    fn request_event_prediction_cycle(&mut self, cycle_id: String) {
        let cycle_id = cycle_id.trim().to_string();
        if cycle_id.is_empty() {
            return;
        }
        let path = self.workspace.event_predictions.clone();
        let sender = self.task_sender.clone();
        self.event_prediction_order_dialog = None;
        self.event_prediction_ticket_dialog = None;
        self.event_prediction_cycle_dialog = Some(EventPredictionCycleDialog::Loading {
            cycle_id: cycle_id.clone(),
        });
        thread::spawn(move || {
            let result = event_prediction::EventPredictionLog::open(&path)
                .and_then(|log| log.cycle_tickets(&cycle_id))
                .map_err(|error| error.to_string());
            let _ = sender.send(TaskEvent::EventPredictionCycle { cycle_id, result });
        });
    }

    fn render_event_prediction_run_dialog(&mut self, ctx: &egui::Context) {
        if !self.event_prediction_run_dialog_open {
            return;
        }

        let mut open = true;
        let mut confirm = false;
        let mut cancel = false;
        egui::Window::new("立即跑一轮事件预测")
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .collapsible(false)
            .resizable(false)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.set_min_width(360.0);
                ui.label("选择这次要生成的虚拟预测周期：");
                ui.add_space(6.0);
                ui.checkbox(&mut self.event_prediction_manual_10m, "10 分钟");
                ui.checkbox(&mut self.event_prediction_manual_30m, "30 分钟");
                ui.checkbox(&mut self.event_prediction_manual_60m, "1 小时");
                ui.add_space(8.0);
                ui.label(
                    RichText::new("确认后会立即读取 Binance Futures 公共行情，并返回 BTC/ETH 当前买涨或买跌。")
                        .size(11.0)
                        .color(theme::MUTED),
                );
                ui.add_space(12.0);
                ui.horizontal(|ui| {
                    let has_selection = self.event_prediction_manual_10m
                        || self.event_prediction_manual_30m
                        || self.event_prediction_manual_60m;
                    if ui
                        .add_enabled(
                            has_selection && !self.event_prediction_running,
                            theme::primary_button("确认运行"),
                        )
                        .clicked()
                    {
                        confirm = true;
                    }
                    if ui.add(theme::secondary_button("取消")).clicked() {
                        cancel = true;
                    }
                });
            });

        if confirm {
            self.event_prediction_run_dialog_open = false;
            self.start_event_prediction_manual_cycle();
        } else if cancel || !open {
            self.event_prediction_run_dialog_open = false;
        }
    }

    fn render_event_prediction_direction_dialog(&mut self, ctx: &egui::Context) {
        let Some(directions) = self.event_prediction_direction_dialog.clone() else {
            return;
        };

        let mut open = true;
        let mut close = false;
        egui::Window::new("本轮应该买什么")
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .collapsible(false)
            .resizable(true)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.set_min_width(560.0);
                ui.label(
                    RichText::new("这是刚刚“立即跑一轮”生成的 BTC/ETH 虚拟预测方向：")
                        .size(13.0)
                        .color(theme::TEXT),
                );
                ui.label(
                    RichText::new(
                        "只提示当前策略方向；事件预测仍然是虚拟下单，不是真实 Binance 下单。",
                    )
                    .size(11.0)
                    .color(theme::MUTED),
                );
                ui.add_space(10.0);
                egui::Grid::new("event-prediction-direction-dialog-grid")
                    .num_columns(5)
                    .striped(true)
                    .spacing([18.0, 12.0])
                    .show(ui, |ui| {
                        for heading in ["时间段", "交易对", "建议", "置信度", "状态"] {
                            ui.label(RichText::new(heading).color(theme::MUTED).strong());
                        }
                        ui.end_row();
                        let mut sorted = directions.clone();
                        sorted.sort_by(|left, right| {
                            left.horizon_minutes
                                .cmp(&right.horizon_minutes)
                                .then_with(|| left.symbol.cmp(&right.symbol))
                        });
                        for direction in sorted {
                            ui.label(format_event_direction_window(&direction));
                            ui.label(
                                RichText::new(compact_event_symbol(&direction.symbol)).strong(),
                            );
                            ui.colored_label(
                                prediction_direction_color(&direction.direction),
                                prediction_trade_direction_label(&direction.direction),
                            );
                            ui.label(format!("{:.1}%", direction.confidence * 100.0));
                            ui.label(if direction.created {
                                "本轮已写入"
                            } else {
                                "等待上一单结算"
                            });
                            ui.end_row();
                        }
                    });
                ui.add_space(12.0);
                if ui.add(theme::primary_button("知道了")).clicked() {
                    close = true;
                }
            });

        if close || !open {
            self.event_prediction_direction_dialog = None;
        }
    }

    fn render_event_prediction_order_dialog(&mut self, ctx: &egui::Context) {
        let Some(kind) = self.event_prediction_order_dialog else {
            return;
        };
        let (title, context, tickets, scroll_id, empty_text) = match kind {
            EventOrderKind::Open => (
                "未结算票据大列表",
                "当前策略最近 80 条未结算票据；点票据编号看详情，点周期编号看整组订单",
                &self.event_prediction_recent,
                "event-prediction-open-dialog-scroll",
                "暂无未结算票据",
            ),
            EventOrderKind::Settled => (
                "历史已结算大列表",
                "当前策略最近 80 条已结算虚拟订单；点票据编号看详情，点周期编号看整组订单",
                &self.event_prediction_history,
                "event-prediction-history-dialog-scroll",
                "暂无历史已结算票据",
            ),
        };
        let mut open = true;
        let mut close = false;
        let mut selected_action = None;
        let content_rect = ctx.content_rect();
        let dialog_width = (content_rect.width() - 32.0).clamp(280.0, 1180.0);
        let dialog_height = (content_rect.height() - 32.0).clamp(280.0, 760.0);
        egui::Window::new(title)
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .collapsible(false)
            .resizable(true)
            .default_width(dialog_width)
            .max_width(dialog_width)
            .max_height(dialog_height)
            .open(&mut open)
            .show(ctx, |ui| {
                selected_action = event_ticket_table(
                    ui,
                    scroll_id,
                    title,
                    context,
                    tickets,
                    empty_text,
                    (dialog_height - 125.0).max(220.0),
                    true,
                );
                ui.add_space(8.0);
                if ui.add(theme::secondary_button("关闭")).clicked() {
                    close = true;
                }
            });

        if let Some(action) = selected_action {
            self.event_prediction_order_dialog = None;
            self.handle_event_ticket_action(action);
            return;
        }
        let escape_pressed = ctx.input(|input| input.key_pressed(egui::Key::Escape));
        if close || !open || escape_pressed {
            self.event_prediction_order_dialog = None;
        }
    }

    fn render_event_prediction_cycle_dialog(&mut self, ctx: &egui::Context) {
        let Some(dialog) = self.event_prediction_cycle_dialog.clone() else {
            return;
        };

        if let EventPredictionCycleDialog::Loading { cycle_id } = dialog {
            let mut open = true;
            let mut close = false;
            egui::Window::new(format!("正在读取周期 {cycle_id}"))
                .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
                .collapsible(false)
                .resizable(false)
                .open(&mut open)
                .show(ctx, |ui| {
                    ui.spinner();
                    ui.label("正在从完整历史中读取这个周期的全部订单…");
                    if ui.add(theme::secondary_button("取消")).clicked() {
                        close = true;
                    }
                });
            let escape_pressed = ctx.input(|input| input.key_pressed(egui::Key::Escape));
            if close || !open || escape_pressed {
                self.event_prediction_cycle_dialog = None;
            }
            return;
        }
        let EventPredictionCycleDialog::Ready(cycle) = dialog else {
            return;
        };

        let mut tickets = cycle.tickets.clone();
        tickets.sort_by(|left, right| {
            left.cycle_order
                .cmp(&right.cycle_order)
                .then_with(|| {
                    left.cycle_slot
                        .unwrap_or_default()
                        .cmp(&right.cycle_slot.unwrap_or_default())
                })
                .then_with(|| left.open_time.cmp(&right.open_time))
                .then_with(|| left.id.cmp(&right.id))
        });
        let settled = tickets
            .iter()
            .filter(|ticket| ticket.status == "settled")
            .count();
        let wins = tickets
            .iter()
            .filter(|ticket| ticket.result == "win")
            .count();
        let losses = tickets
            .iter()
            .filter(|ticket| ticket.result == "loss")
            .count();
        let ties = tickets
            .iter()
            .filter(|ticket| ticket.result == "tie")
            .count();
        let pnl = tickets
            .iter()
            .filter_map(|ticket| ticket.virtual_pnl)
            .sum::<f64>();
        let context = format!(
            "完整周期共 {} 笔，按轮次、线路排序；已结算 {}；{} 胜 / {} 负 / {} 平；累计盈亏 {:+.2} USDT",
            tickets.len(),
            settled,
            wins,
            losses,
            ties,
            pnl
        );
        let mut open = true;
        let mut close = false;
        let mut selected_action = None;
        let scroll_id = format!("event-prediction-cycle-dialog-scroll-{}", cycle.cycle_id);
        let content_rect = ctx.content_rect();
        let dialog_width = (content_rect.width() - 32.0).clamp(280.0, 1180.0);
        let dialog_height = (content_rect.height() - 32.0).clamp(280.0, 760.0);
        egui::Window::new(format!("周期订单 {}", cycle.cycle_id))
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .collapsible(false)
            .resizable(true)
            .default_width(dialog_width)
            .max_width(dialog_width)
            .max_height(dialog_height)
            .open(&mut open)
            .show(ctx, |ui| {
                selected_action = event_ticket_table(
                    ui,
                    &scroll_id,
                    &cycle.cycle_id,
                    &context,
                    &tickets,
                    "该周期暂无订单",
                    (dialog_height - 125.0).max(220.0),
                    false,
                );
                ui.add_space(8.0);
                if ui.add(theme::secondary_button("关闭")).clicked() {
                    close = true;
                }
            });

        if let Some(EventTicketAction::Ticket(ticket)) = selected_action {
            self.event_prediction_cycle_dialog = None;
            self.event_prediction_ticket_dialog = Some(ticket);
            return;
        }
        let escape_pressed = ctx.input(|input| input.key_pressed(egui::Key::Escape));
        if close || !open || escape_pressed {
            self.event_prediction_cycle_dialog = None;
        }
    }

    fn render_event_prediction_ticket_dialog(&mut self, ctx: &egui::Context) {
        let Some(ticket) = self.event_prediction_ticket_dialog.clone() else {
            return;
        };

        let mut open = true;
        let mut close = false;
        let content_rect = ctx.content_rect();
        let mut selected_cycle = None;
        let dialog_width = (content_rect.width() - 32.0).clamp(280.0, 540.0);
        let dialog_height = (content_rect.height() - 32.0).clamp(280.0, 760.0);
        egui::Window::new(format!("订单详情 {}", compact_event_id(&ticket.id)))
            .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
            .collapsible(false)
            .resizable(true)
            .min_width(dialog_width)
            .max_width(dialog_width)
            .max_height(dialog_height)
            .vscroll(true)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new(&ticket.symbol).size(18.0).strong());
                    ui.colored_label(
                        prediction_direction_color(&ticket.direction),
                        prediction_direction_label(&ticket.direction),
                    );
                    ui.colored_label(
                        prediction_result_color(&ticket),
                        prediction_result_label(&ticket),
                    );
                });
                ui.add_space(8.0);
                event_ticket_detail_row(ui, "完整票据 ID", &ticket.id);
                event_ticket_detail_row(ui, "交易对", &ticket.symbol);
                event_ticket_detail_row(ui, "周期", &format!("{}m", ticket.horizon_minutes));
                if event_ticket_cycle_detail_row(ui, &ticket) {
                    close = true;
                    selected_cycle = Some(ticket.cycle_id.clone());
                }
                event_ticket_detail_row(
                    ui,
                    "周期轮次",
                    &if ticket.cycle_order > 0 {
                        format!("第 {} 轮", ticket.cycle_order)
                    } else {
                        "--".into()
                    },
                );
                event_ticket_detail_row(
                    ui,
                    "线路槽位",
                    &if ticket.cycle_slot.is_some_and(|slot| slot > 0) {
                        format!("{} 号线", ticket.cycle_slot.unwrap_or_default())
                    } else {
                        "--".into()
                    },
                );
                event_ticket_detail_row(
                    ui,
                    "方向",
                    prediction_trade_direction_label(&ticket.direction),
                );
                event_ticket_detail_row(
                    ui,
                    "置信度",
                    &format!("{:.1}%", ticket.confidence * 100.0),
                );
                event_ticket_detail_row(ui, "分数", &format!("{:+.3}", ticket.score));
                event_ticket_detail_row(ui, "下注", &format!("{:.2} USDT", ticket.stake_amount));
                event_ticket_detail_row(
                    ui,
                    "本单结算回报",
                    &ticket
                        .cycle_balance_after
                        .map(|value| format!("{value:.2} USDT"))
                        .unwrap_or_else(|| "--".into()),
                );
                event_ticket_detail_row(ui, "开盘价", &format_price(ticket.entry_price));
                event_ticket_detail_row(
                    ui,
                    "到期价",
                    &ticket
                        .expiry_price
                        .map(format_price)
                        .unwrap_or_else(|| "--".into()),
                );
                event_ticket_detail_row(ui, "状态", &ticket.status);
                event_ticket_detail_row(ui, "结果", prediction_result_label(&ticket));
                event_ticket_detail_row(
                    ui,
                    "波动",
                    &ticket
                        .move_percent
                        .map(|value| format!("{value:+.4}%"))
                        .unwrap_or_else(|| "--".into()),
                );
                event_ticket_detail_row(
                    ui,
                    "虚拟盈亏",
                    &ticket
                        .virtual_pnl
                        .map(|value| format!("{value:+.2} USDT"))
                        .unwrap_or_else(|| "--".into()),
                );
                event_ticket_detail_row(ui, "开盘时间", &format_event_time(ticket.open_time));
                event_ticket_detail_row(ui, "到期时间", &format_event_time(ticket.close_time));
                ui.add_space(8.0);
                ui.label(RichText::new("复盘").color(theme::MUTED));
                Frame::NONE
                    .fill(theme::SURFACE_2)
                    .stroke(Stroke::new(1.0, theme::BORDER))
                    .corner_radius(4)
                    .inner_margin(Margin::same(10))
                    .show(ui, |ui| {
                        ScrollArea::vertical().max_height(130.0).show(ui, |ui| {
                            ui.label(if ticket.review.is_empty() {
                                "暂无复盘文本"
                            } else {
                                &ticket.review
                            });
                        });
                    });
                ui.add_space(10.0);
                if ui.add(theme::secondary_button("关闭")).clicked() {
                    close = true;
                }
            });

        let escape_pressed = ctx.input(|input| input.key_pressed(egui::Key::Escape));
        if let Some(cycle_id) = selected_cycle {
            self.event_prediction_ticket_dialog = None;
            self.request_event_prediction_cycle(cycle_id);
            return;
        }
        if close || !open || escape_pressed {
            self.event_prediction_ticket_dialog = None;
        }
    }

    fn render_live_confirmation(&mut self, ctx: &egui::Context) {
        let Some(action) = self.live_confirmation else {
            return;
        };
        let mut confirm = false;
        let mut cancel = false;
        egui::Window::new(match action {
            LiveAction::Enable => "启用实盘模式",
            LiveAction::Start => "启动实盘策略",
        })
        .anchor(Align2::CENTER_CENTER, Vec2::ZERO)
        .collapsible(false)
        .resizable(false)
        .show(ctx, |ui| {
            ui.set_min_width(420.0);
            ui.colored_label(theme::RED, "实盘策略会自动发送 Binance Futures 订单");
            ui.label(format!(
                "单仓保证金上限 {:.0} USDT，最多同时持有 {} 个仓位，策略最高杠杆 {}x，盈亏比 1:{:.1}。",
                self.stake_amount,
                self.max_open_trades,
                self.ai_config.leverage,
                self.risk_reward_ratio
            ));
            ui.add_space(10.0);
            ui.checkbox(
                &mut self.live_acknowledged,
                "我确认使用真实资金并承担合约交易风险",
            );
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(
                        self.live_acknowledged,
                        theme::primary_button(match action {
                            LiveAction::Enable => "确认启用实盘",
                            LiveAction::Start => "确认启动实盘策略",
                        }),
                    )
                    .clicked()
                {
                    confirm = true;
                }
                if ui.add(theme::secondary_button("取消")).clicked() {
                    cancel = true;
                }
            });
        });
        if confirm {
            self.live_confirmation = None;
            self.live_acknowledged = false;
            match action {
                LiveAction::Enable => self.set_trading_mode(false),
                LiveAction::Start => self.bot_action(true),
            }
        } else if cancel {
            self.live_confirmation = None;
            self.live_acknowledged = false;
        }
    }

    fn check_bot_health(&mut self) {
        if self.health_check_running || self.last_health_check.elapsed() < Duration::from_secs(15) {
            return;
        }
        self.health_check_running = true;
        self.last_health_check = Instant::now();
        let workspace = self.workspace.clone();
        let sender = self.task_sender.clone();
        thread::spawn(move || {
            let (available, state) = workspace.docker_state();
            let _ = sender.send(TaskEvent::Health(available, state));
        });
    }

    fn maybe_run_ai_decision_loop(&mut self) {
        if !self.ai_config.enabled {
            self.ai_decision_status = "AI 闭环未启用".into();
            return;
        }
        if self.ai_decision_running
            || self.last_ai_decision_check.elapsed() < Duration::from_secs(15)
        {
            return;
        }
        if self.bot_state != "running" {
            self.ai_decision_status = "等待策略运行后开始 AI 闭环".into();
            return;
        }
        if !self.dry_run && self.ai_config.dry_run_only {
            self.ai_decision_status = "实盘已被“仅允许模拟盘”拦截".into();
            return;
        }
        let Some(interval) = Interval::from_timeframe(&self.ai_config.timeframe) else {
            self.ai_decision_status = "AI 周期配置无效".into();
            return;
        };
        let candle_open_time = last_closed_candle_open(interval);
        let symbols = self
            .ai_config
            .symbol_whitelist
            .iter()
            .filter(|symbol| {
                !self.ai_config.one_signal_per_candle
                    || self.ai_processed_candles.get(*symbol) != Some(&candle_open_time)
            })
            .cloned()
            .collect::<Vec<_>>();
        if symbols.is_empty() {
            self.ai_decision_status =
                format!("本根 {} K 线已完成 AI 决策", self.ai_config.timeframe);
            return;
        }

        let Some(key) = self.unlocked_key.as_ref() else {
            return;
        };
        let secret_name = match self.ai_provider {
            AiProvider::OpenAi => "openai_api_key",
            AiProvider::Claude => "anthropic_api_key",
            AiProvider::DeepSeek => "deepseek_api_key",
            AiProvider::Relay => "relay_api_key",
        };
        let api_key = self
            .store
            .get_secret(key, secret_name)
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();
        if api_key.is_empty() {
            self.ai_decision_status = format!("尚未配置 {} API Key", self.ai_provider.label());
            return;
        }
        let binance_api_key = self
            .store
            .get_secret(key, "binance_api_key")
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();
        let binance_api_secret = self
            .store
            .get_secret(key, "binance_api_secret")
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();

        self.ai_decision_running = true;
        self.last_ai_decision_check = Instant::now();
        self.ai_decision_status = format!(
            "AI 正在决策 {} 根 {} K 线",
            symbols.len(),
            self.ai_config.timeframe
        );
        let sender = self.task_sender.clone();
        let provider = self.ai_provider;
        let model = self.ai_model.clone();
        let relay_base_url = self.relay_base_url.clone();
        let config = self.ai_config.clone();
        let workspace = self.workspace.clone();
        let dry_run = self.dry_run;
        let account_snapshot = if dry_run {
            simulation_to_futures_account(&self.simulation_account)
        } else {
            self.account.clone()
        };
        thread::spawn(move || {
            let result = run_ai_decision_cycle(AiDecisionCycle {
                provider,
                model,
                api_key,
                relay_base_url,
                config,
                workspace,
                dry_run,
                binance_api_key,
                binance_api_secret,
                account_snapshot,
                interval,
                candle_open_time,
                symbols,
            });
            let _ = sender.send(TaskEvent::AiDecision(result));
        });
    }

    fn maybe_run_event_prediction_loop(&mut self) {
        self.start_event_prediction_cycle(false);
    }

    fn start_event_prediction_cycle(&mut self, force: bool) {
        let _ = self.start_event_prediction_cycle_for_horizons(force, EventHorizon::ALL.to_vec());
    }

    fn start_event_prediction_manual_cycle(&mut self) {
        let mut horizons = Vec::new();
        if self.event_prediction_manual_10m {
            horizons.push(EventHorizon::TenMinutes);
        }
        if self.event_prediction_manual_30m {
            horizons.push(EventHorizon::ThirtyMinutes);
        }
        if self.event_prediction_manual_60m {
            horizons.push(EventHorizon::OneHour);
        }
        if horizons.is_empty() {
            self.toast("请至少选择一个事件预测周期", true);
            return;
        }
        if self.start_event_prediction_cycle_for_horizons(true, horizons) {
            self.event_prediction_direction_dialog_pending = true;
        }
    }

    fn start_event_prediction_cycle_for_horizons(
        &mut self,
        force: bool,
        horizons: Vec<EventHorizon>,
    ) -> bool {
        if !self.event_prediction_enabled && !force {
            self.event_prediction_status = "事件预测虚拟盘已关闭".into();
            return false;
        }
        if self.event_prediction_running {
            return false;
        }
        if !force && self.last_event_prediction_check.elapsed() < Duration::from_secs(60) {
            return false;
        }
        let symbols = event_prediction::supported_symbols();
        if symbols.is_empty() {
            self.event_prediction_status = "事件预测没有可用交易对".into();
            return false;
        }
        self.event_prediction_running = true;
        self.last_event_prediction_check = Instant::now();
        let horizon_text = format_event_horizon_selection(&horizons);
        self.event_prediction_status = format!(
            "事件预测正在运行：{} 个交易对，{} 虚拟预测样本生成中",
            symbols.len(),
            horizon_text
        );
        let path = self.workspace.event_predictions.clone();
        let sender = self.task_sender.clone();
        let run_all_horizons = horizons.len() == EventHorizon::ALL.len()
            && horizons
                .iter()
                .zip(EventHorizon::ALL.iter())
                .all(|(selected, default)| selected.minutes() == default.minutes());
        thread::spawn(move || {
            let result = if run_all_horizons {
                event_prediction::run_cycle(&path, &symbols).map_err(|error| error.to_string())
            } else {
                event_prediction::run_cycle_for_horizons(&path, &symbols, &horizons)
                    .map_err(|error| error.to_string())
            };
            let _ = sender.send(TaskEvent::EventPrediction(result));
        });
        true
    }

    fn refresh_account(&mut self) {
        if self.account_check_running || self.last_account_check.elapsed() < Duration::from_secs(8)
        {
            return;
        }
        let Some(key) = self.unlocked_key.as_ref() else {
            return;
        };
        let api_key = self
            .store
            .get_secret(key, "binance_api_key")
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();
        let api_secret = self
            .store
            .get_secret(key, "binance_api_secret")
            .ok()
            .flatten()
            .map(|value| value.to_string())
            .unwrap_or_default();
        if api_key.is_empty() || api_secret.is_empty() {
            return;
        }
        self.account_check_running = true;
        self.last_account_check = Instant::now();
        let sender = self.task_sender.clone();
        thread::spawn(move || {
            let result = exchange::fetch_futures_account(&api_key, &api_secret)
                .map_err(|error| error.to_string());
            let _ = sender.send(TaskEvent::Account(result));
        });
    }

    fn refresh_simulation_account(&mut self) {
        if self.simulation_check_running
            || self.last_simulation_check.elapsed() < Duration::from_secs(3)
        {
            return;
        }
        self.simulation_check_running = true;
        self.last_simulation_check = Instant::now();
        let workspace = self.workspace.clone();
        let sender = self.task_sender.clone();
        thread::spawn(move || {
            let result = workspace
                .simulation_account()
                .map_err(|error| error.to_string());
            let _ = sender.send(TaskEvent::Simulation(result));
        });
    }

    fn render_settings(&mut self, ui: &mut Ui) {
        ScrollArea::vertical().show(ui, |ui| {
            settings_section(
                ui,
                "运行保护",
                if self.dry_run { "Dry-run" } else { "Live" },
                |ui| {
                    let changed = ui
                        .add_enabled(
                            self.dry_run,
                            egui::Checkbox::new(
                                &mut self.auto_restart,
                                "异常停止后自动恢复模拟策略",
                            ),
                        )
                        .changed();
                    if changed {
                        let value = if self.auto_restart { "true" } else { "false" };
                        match self.store.set_setting("auto_restart", value) {
                            Ok(()) => self.toast("运行保护设置已保存", false),
                            Err(error) => self.toast(error.to_string(), true),
                        }
                    }
                    ui.horizontal(|ui| {
                        status_chip(ui, if self.dry_run { "模拟盘" } else { "实盘" }, true);
                        status_chip(ui, "Docker", self.docker_available);
                        status_row(ui, "当前状态", &self.bot_state);
                    });
                },
            );
            ui.add_space(12.0);
            settings_section(ui, "AI 自动交易", "白名单 / 执行参数", |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.checkbox(&mut self.ai_config.enabled, "启用 AI 自动交易");
                    ui.checkbox(&mut self.ai_config.dry_run_only, "仅允许模拟盘");
                    ui.checkbox(
                        &mut self.ai_config.one_signal_per_candle,
                        "限制为每根 K 线只执行一次（数据采集建议关闭）",
                    );
                });
                ui.add_space(8.0);
                ui.columns(2, |cols| {
                    field_label(&mut cols[0], "AI 提供商");
                    egui::ComboBox::from_id_salt("settings-ai-provider")
                        .selected_text(self.ai_provider.label())
                        .show_ui(&mut cols[0], |ui| {
                            for provider in AiProvider::ALL {
                                ui.selectable_value(
                                    &mut self.ai_provider,
                                    provider,
                                    provider.label(),
                                );
                            }
                        });
                    self.normalize_ai_model_for_provider();
                    field_label(&mut cols[1], "模型名（可选）");
                    cols[1].add_sized(
                        [cols[1].available_width(), 36.0],
                        TextEdit::singleline(&mut self.ai_model)
                            .hint_text(if self.ai_provider == AiProvider::Relay {
                                RELAY_DEFAULT_MODEL
                            } else {
                                "默认模型"
                            }),
                    );
                });
                ui.add_space(8.0);
                ui.columns(2, |cols| {
                    field_label(&mut cols[0], "策略档位");
                    let previous_profile = self.ai_config.strategy_profile;
                    egui::ComboBox::from_id_salt("strategy-profile")
                        .selected_text(self.ai_config.strategy_profile.label())
                        .show_ui(&mut cols[0], |ui| {
                            for profile in StrategyProfile::ALL {
                                ui.selectable_value(
                                    &mut self.ai_config.strategy_profile,
                                    profile,
                                    profile.label(),
                                );
                            }
                        });
                    if self.ai_config.strategy_profile != previous_profile {
                        let profile = self.ai_config.strategy_profile;
                        profile.apply_to(&mut self.ai_config);
                        self.risk_reward_ratio = self.ai_config.risk_reward_ratio;
                    }
                    cols[1].add_space(18.0);
                    cols[1].label(
                        RichText::new(self.ai_config.strategy_profile.hint())
                            .size(12.0)
                            .color(theme::MUTED),
                    );
                });
                ui.add_space(8.0);
                field_label(ui, "币种白名单");
                ui.add_sized(
                    [ui.available_width(), 38.0],
                    TextEdit::singleline(&mut self.ai_symbol_whitelist)
                        .hint_text("BTCUSDT, ETHUSDT"),
                );
                ui.add_space(8.0);
                ui.columns(4, |cols| {
                    field_label(&mut cols[0], "K 线周期");
                    egui::ComboBox::from_id_salt("ai-timeframe")
                        .selected_text(&self.ai_config.timeframe)
                        .show_ui(&mut cols[0], |ui| {
                            for timeframe in AI_TIMEFRAMES {
                                ui.selectable_value(
                                    &mut self.ai_config.timeframe,
                                    timeframe.to_string(),
                                    timeframe,
                                );
                            }
                        });
                    field_label(&mut cols[1], "保证金模式");
                    egui::ComboBox::from_id_salt("ai-margin-mode")
                        .selected_text(match self.ai_config.margin_mode {
                            MarginMode::Cross => "Cross",
                            MarginMode::Isolated => "Isolated",
                        })
                        .show_ui(&mut cols[1], |ui| {
                            ui.selectable_value(
                                &mut self.ai_config.margin_mode,
                                MarginMode::Cross,
                                "Cross",
                            );
                            ui.selectable_value(
                                &mut self.ai_config.margin_mode,
                                MarginMode::Isolated,
                                "Isolated",
                            );
                        });
                    field_label(&mut cols[2], "杠杆");
                    cols[2].add(egui::DragValue::new(&mut self.ai_config.leverage).range(1..=125));
                    field_label(&mut cols[3], "资金比例 %");
                    cols[3].add(
                        egui::DragValue::new(&mut self.ai_config.capital_usage_percent)
                            .speed(0.5)
                            .range(1.0..=100.0),
                    );
                });
                ui.add_space(8.0);
                let (timeframe_hint, timeframe_color) =
                    ai_timeframe_guidance(&self.ai_config.timeframe);
                ui.label(
                    RichText::new(timeframe_hint)
                        .size(12.0)
                        .color(timeframe_color),
                );
                ui.add_space(8.0);
                ui.columns(3, |cols| {
                    field_label(&mut cols[0], "多头分门槛");
                    cols[0].add(
                        egui::DragValue::new(&mut self.ai_config.minimum_long_score)
                            .speed(0.01)
                            .range(0.0..=1.0),
                    );
                    field_label(&mut cols[1], "空头分门槛");
                    cols[1].add(
                        egui::DragValue::new(&mut self.ai_config.minimum_short_score)
                            .speed(0.01)
                            .range(0.0..=1.0),
                    );
                    field_label(&mut cols[2], "方向分门槛");
                    cols[2].add(
                        egui::DragValue::new(&mut self.ai_config.minimum_factor_score)
                            .speed(0.01)
                            .range(0.0..=1.0),
                    );
                });
                ui.add_space(8.0);
                ui.columns(3, |cols| {
                    field_label(&mut cols[0], "趋势质量门槛");
                    cols[0].add(
                        egui::DragValue::new(&mut self.ai_config.minimum_trend_quality)
                            .speed(0.01)
                            .range(0.0..=1.0),
                    );
                    field_label(&mut cols[1], "ADX 门槛");
                    cols[1].add(
                        egui::DragValue::new(&mut self.ai_config.minimum_adx)
                            .speed(0.5)
                            .range(0.0..=80.0),
                    );
                    field_label(&mut cols[2], "量能门槛");
                    cols[2].add(
                        egui::DragValue::new(&mut self.ai_config.minimum_volume_ratio)
                            .speed(0.05)
                            .range(-1.0..=5.0),
                    );
                });
                ui.add_space(8.0);
                ui.columns(3, |cols| {
                    field_label(&mut cols[0], "最低置信度");
                    cols[0].add(
                        egui::DragValue::new(&mut self.ai_config.minimum_confidence)
                            .speed(0.01)
                            .range(0.0..=1.0),
                    );
                    field_label(&mut cols[1], "模型超时秒");
                    cols[1].add(
                        egui::DragValue::new(&mut self.ai_config.model_timeout_seconds)
                            .range(5..=120),
                    );
                    field_label(&mut cols[2], "行情最大延迟秒");
                    cols[2].add(
                        egui::DragValue::new(&mut self.ai_config.market_max_age_seconds)
                            .range(15..=3_600),
                    );
                });
                ui.add_space(10.0);
                if ui.add(theme::primary_button("保存 AI 参数")).clicked() {
                    self.save_ai_settings();
                }
            });
            ui.add_space(12.0);
            settings_section(ui, "仓位风控", "上限 / 盈亏比", |ui| {
                ui.columns(3, |cols| {
                    field_label(&mut cols[0], "单仓保证金上限（USDT）");
                    cols[0]
                        .add(egui::DragValue::new(&mut self.stake_amount).range(5.0..=1_000_000.0));
                    field_label(&mut cols[1], "最大同时持仓");
                    cols[1].add(egui::DragValue::new(&mut self.max_open_trades).range(1..=20));
                    field_label(&mut cols[2], "单仓盈亏比");
                    cols[2].add(
                        egui::DragValue::new(&mut self.risk_reward_ratio)
                            .speed(0.1)
                            .range(0.5..=10.0),
                    );
                });
                ui.checkbox(
                    &mut self.allow_ai_risk_sizing,
                    "AI 在上限内判断仓位 / 止盈止损",
                );
                ui.add_space(10.0);
                if ui.add(theme::primary_button("保存风控")).clicked() {
                    match self.workspace.update_risk(
                        self.stake_amount,
                        self.max_open_trades,
                        self.risk_reward_ratio,
                        self.allow_ai_risk_sizing,
                    ) {
                        Ok(()) => {
                            self.ai_config.max_stake_amount = self.stake_amount;
                            self.ai_config.risk_reward_ratio = self.risk_reward_ratio;
                            self.ai_config.allow_ai_risk_sizing = self.allow_ai_risk_sizing;
                            self.toast("风控参数已保存", false);
                            self.hot_reload_bot("风控参数已保存，正在热重载交易内核");
                        }
                        Err(error) => self.toast(error.to_string(), true),
                    }
                }
            });
            ui.add_space(12.0);
            settings_section(ui, "密钥管理", "AES-256-GCM · 本地 key.db", |ui| {
                ui.horizontal_wrapped(|ui| {
                    status_chip(ui, "Binance", self.secret_status.binance);
                    status_chip(ui, "OpenAI", self.secret_status.openai);
                    status_chip(ui, "Claude", self.secret_status.claude);
                    status_chip(ui, "DeepSeek", self.secret_status.deepseek);
                    status_chip(ui, "中转站", self.secret_status.relay);
                });
                ui.add_space(10.0);
                ui.columns(2, |cols| {
                    credential_field(
                        &mut cols[0],
                        "Binance API Key",
                        &mut self.credential_draft.binance_key,
                    );
                    credential_field(
                        &mut cols[1],
                        "Binance API Secret",
                        &mut self.credential_draft.binance_secret,
                    );
                    credential_field(
                        &mut cols[0],
                        "OpenAI API Key",
                        &mut self.credential_draft.openai_key,
                    );
                    credential_field(
                        &mut cols[1],
                        "Claude API Key",
                        &mut self.credential_draft.claude_key,
                    );
                    credential_field(
                        &mut cols[0],
                        "DeepSeek API Key",
                        &mut self.credential_draft.deepseek_key,
                    );
                    credential_field(
                        &mut cols[1],
                        "中转站 API Key",
                        &mut self.credential_draft.relay_key,
                    );
                });
                field_label(ui, "中转站 Base URL（OpenAI 兼容）");
                ui.add_sized(
                    [ui.available_width(), 38.0],
                    TextEdit::singleline(&mut self.relay_base_url)
                        .hint_text("https://example.com/v1"),
                );
                ui.label(
                    RichText::new("只填 API 根路径，不填中转站网页/控制台地址；程序会自动请求 /chat/completions。")
                        .size(11.0)
                        .color(theme::MUTED),
                );
                ui.add_space(10.0);
                if ui
                    .add_enabled(
                        !self.credential_check_running,
                        theme::primary_button(if self.credential_check_running {
                            "正在验证 Binance..."
                        } else {
                            "验证并更新密钥"
                        }),
                    )
                    .clicked()
                {
                    self.request_credential_update();
                }
            });
        });
    }

    fn save_relay_endpoint(&mut self) -> Result<(), String> {
        let key_available =
            self.secret_status.relay || !self.credential_draft.relay_key.trim().is_empty();
        let url_supplied = !self.relay_base_url.trim().is_empty();
        if !key_available && !url_supplied {
            return Ok(());
        }
        if !key_available {
            return Err("填写中转站 Base URL 时也需要填写 API Key".into());
        }
        let normalized = ai::normalize_relay_base_url(&self.relay_base_url)
            .map_err(|error| error.to_string())?;
        self.store
            .set_setting("relay_base_url", &normalized)
            .map_err(|error| error.to_string())?;
        self.relay_base_url = normalized;
        Ok(())
    }

    fn normalize_ai_model_for_provider(&mut self) {
        if self.ai_provider == AiProvider::Relay
            && (self.ai_model.trim().is_empty()
                || ["gpt-4o-mini", "gpt5.5"]
                    .iter()
                    .any(|legacy| self.ai_model.trim().eq_ignore_ascii_case(legacy)))
        {
            self.ai_model = RELAY_DEFAULT_MODEL.into();
        }
    }

    fn request_credential_update(&mut self) {
        if let Err(error) = self.save_relay_endpoint() {
            self.toast(error, true);
            return;
        }
        let binance_supplied = !self.credential_draft.binance_key.trim().is_empty()
            || !self.credential_draft.binance_secret.trim().is_empty();
        if binance_supplied {
            self.validate_binance_credentials(CredentialAction::Update);
        } else {
            self.finish_credential_update("密钥已加密更新");
        }
    }

    fn finish_credential_update(&mut self, validation_message: &str) {
        let Some(key) = self.unlocked_key.as_ref() else {
            return;
        };
        match self.store.update_credentials(key, &self.credential_draft) {
            Ok(()) => {
                self.credential_draft = CredentialDraft::default();
                self.secret_status = self.store.secret_status().unwrap_or_default();
                self.toast(validation_message, false);
                self.hot_reload_bot("密钥已更新，正在热重载交易内核");
            }
            Err(error) => self.toast(error.to_string(), true),
        }
    }

    fn visible_symbols(&self) -> Vec<String> {
        if self.ai_config.symbol_whitelist.is_empty() {
            vec!["BTCUSDT".into()]
        } else {
            self.ai_config.symbol_whitelist.clone()
        }
    }

    fn refresh_whitelist_selection(&mut self) {
        let symbols = self.visible_symbols();
        self.selected_pairs
            .retain(|selected| symbols.iter().any(|symbol| symbol == selected));
        if self.selected_pairs.is_empty() {
            self.selected_pairs = symbols.clone();
        }
        if !symbols.iter().any(|symbol| symbol == &self.symbol) {
            self.symbol = symbols.first().cloned().unwrap_or_else(|| "BTCUSDT".into());
            self.select_market();
        }
    }

    fn save_ai_settings(&mut self) {
        let symbols = match parse_symbol_whitelist(&self.ai_symbol_whitelist) {
            Ok(symbols) => symbols,
            Err(error) => {
                self.toast(error, true);
                return;
            }
        };
        self.ai_config.symbol_whitelist = symbols;
        self.ai_config.max_stake_amount = self.stake_amount;
        self.ai_config.risk_reward_ratio = self.risk_reward_ratio;
        self.ai_config.allow_ai_risk_sizing = self.allow_ai_risk_sizing;
        match self.workspace.save_ai_trading_config(&self.ai_config) {
            Ok(()) => {
                self.ai_symbol_whitelist =
                    format_symbol_whitelist(&self.ai_config.symbol_whitelist);
                self.refresh_whitelist_selection();
                self.ai_processed_candles.clear();
                self.ai_decision_status = "AI 设置已更新，等待下一轮决策".into();
                self.toast("AI 自动交易参数已保存", false);
                self.hot_reload_bot("AI 参数已保存，正在热重载交易内核");
            }
            Err(error) => self.toast(error.to_string(), true),
        }
    }

    fn select_market(&self) {
        let _ = self.market_commands.send(MarketCommand::Select {
            symbol: self.symbol.clone(),
            interval: self.interval,
        });
    }

    fn record_equity_snapshot(&mut self, account: &FuturesAccount) {
        if account.updated_at <= 0 || !account.margin_balance.is_finite() {
            return;
        }
        let time = if account.updated_at > 10_000_000_000 {
            account.updated_at / 1000
        } else {
            account.updated_at
        };
        let point = EquityPoint {
            time,
            equity: account.margin_balance,
            available: account.available_balance,
            unrealized_profit: account.unrealized_profit,
        };
        if let Some(last) = self.equity_history.last_mut()
            && point.time.saturating_sub(last.time) < 60
        {
            *last = point;
        } else {
            self.equity_history.push(point);
        }
        let cutoff = chrono::Utc::now().timestamp() - 370 * 24 * 60 * 60;
        self.equity_history
            .retain(|point| point.time >= cutoff && point.equity.is_finite());
        let _ = save_equity_history(&self.equity_history_path, &self.equity_history);
    }

    fn push_second_candle(&mut self, timestamp: i64, price: f64) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        let time = timestamp.max(chrono::Utc::now().timestamp());
        if let Some(last) = self.candles.last_mut() {
            if time <= last.time {
                last.high = last.high.max(price);
                last.low = last.low.min(price);
                last.close = price;
            } else {
                let open = last.close;
                self.candles.push(Candle {
                    time,
                    open,
                    high: open.max(price),
                    low: open.min(price),
                    close: price,
                    volume: 0.0,
                });
            }
        } else {
            self.candles.push(Candle {
                time,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 0.0,
            });
        }
        if self.candles.len() > 900 {
            let overflow = self.candles.len() - 900;
            self.candles.drain(0..overflow);
        }
    }

    fn draw_equity_performance(&self, ui: &mut Ui) {
        let (rect, _) =
            ui.allocate_exact_size(Vec2::new(ui.available_width(), 190.0), Sense::hover());
        let painter = ui.painter_at(rect);
        painter.rect_filled(rect, 4, theme::SURFACE);
        let plot = Rect::from_min_max(
            rect.min + Vec2::new(10.0, 10.0),
            rect.max - Vec2::new(78.0, 28.0),
        );
        for index in 0..5 {
            let y = egui::lerp(plot.top()..=plot.bottom(), index as f32 / 4.0);
            painter.line_segment(
                [Pos2::new(plot.left(), y), Pos2::new(plot.right(), y)],
                Stroke::new(1.0, theme::BORDER),
            );
        }
        let cutoff = self.equity_range.cutoff();
        let points = self
            .equity_history
            .iter()
            .filter(|point| point.time >= cutoff && point.equity.is_finite())
            .collect::<Vec<_>>();
        if points.is_empty() {
            painter.text(
                plot.center(),
                Align2::CENTER_CENTER,
                "等待账户刷新后生成个人资金曲线",
                FontId::new(14.0, FontFamily::Proportional),
                theme::MUTED,
            );
            return;
        }

        let min = points
            .iter()
            .map(|point| point.equity)
            .fold(f64::INFINITY, f64::min);
        let max = points
            .iter()
            .map(|point| point.equity)
            .fold(f64::NEG_INFINITY, f64::max);
        let padding = ((max - min) * 0.08).max(max.abs() * 0.0005).max(1.0);
        let low = min - padding;
        let high = max + padding;
        let spread = (high - low).max(f64::EPSILON);
        let start = cutoff.min(points.first().map(|point| point.time).unwrap_or(cutoff));
        let end = chrono::Utc::now()
            .timestamp()
            .max(points.last().map(|point| point.time).unwrap_or(start + 1));
        let duration = (end - start).max(1) as f32;
        let chart_points = points
            .iter()
            .map(|point| {
                let x_ratio = (point.time - start) as f32 / duration;
                let y_ratio = ((point.equity - low) / spread) as f32;
                Pos2::new(
                    egui::lerp(plot.left()..=plot.right(), x_ratio.clamp(0.0, 1.0)),
                    plot.bottom() - y_ratio.clamp(0.0, 1.0) * plot.height(),
                )
            })
            .collect::<Vec<_>>();
        if chart_points.len() == 1 {
            painter.circle_filled(chart_points[0], 4.0, theme::YELLOW);
        } else {
            painter.add(egui::Shape::line(
                chart_points,
                Stroke::new(2.0, theme::YELLOW),
            ));
        }
        for index in 0..=4 {
            let ratio = index as f32 / 4.0;
            let value = high - spread * ratio as f64;
            let y = egui::lerp(plot.top()..=plot.bottom(), ratio);
            painter.text(
                Pos2::new(plot.right() + 8.0, y),
                Align2::LEFT_CENTER,
                format!("{value:.2}"),
                FontId::new(11.0, FontFamily::Monospace),
                theme::MUTED,
            );
        }
        if let Some(latest) = points.last() {
            let positive = latest.unrealized_profit >= 0.0;
            painter.text(
                plot.left_top() + Vec2::new(8.0, 8.0),
                Align2::LEFT_TOP,
                format!(
                    "权益 {:.2}  可用 {:.2}  未实现 {:+.2}",
                    latest.equity, latest.available, latest.unrealized_profit
                ),
                FontId::new(12.0, FontFamily::Monospace),
                if positive { theme::GREEN } else { theme::RED },
            );
            painter.text(
                plot.left_bottom() + Vec2::new(8.0, -4.0),
                Align2::LEFT_BOTTOM,
                format!("{} 快照", format_equity_time(latest.time)),
                FontId::new(11.0, FontFamily::Proportional),
                theme::MUTED,
            );
        }
    }
}

fn load_equity_history(path: &PathBuf) -> Vec<EquityPoint> {
    let Ok(source) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(mut points) = serde_json::from_str::<Vec<EquityPoint>>(&source) else {
        return Vec::new();
    };
    points.retain(|point| point.time > 0 && point.equity.is_finite());
    points.sort_by_key(|point| point.time);
    points
}

fn save_equity_history(path: &PathBuf, points: &[EquityPoint]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let payload = serde_json::to_string_pretty(points).unwrap_or_else(|_| "[]".into());
    std::fs::write(path, format!("{payload}\n"))
}

fn format_equity_time(timestamp: i64) -> String {
    Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|time| time.format("%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "--".into())
}

fn format_event_time(timestamp: i64) -> String {
    if timestamp <= 0 {
        return "--".into();
    }
    Local
        .timestamp_opt(timestamp, 0)
        .single()
        .map(|time| time.format("%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "--".into())
}

fn format_scanner_time(timestamp: i64) -> String {
    if timestamp <= 0 {
        return "--".into();
    }
    Utc.timestamp_opt(timestamp, 0)
        .single()
        .map(|time| {
            (time + chrono::Duration::hours(8))
                .format("%m-%d %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| "--".into())
}

fn compact_scanner_volume(value: f64) -> String {
    if value >= 1_000_000_000.0 {
        format!("{:.1}B", value / 1_000_000_000.0)
    } else if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1}K", value / 1_000.0)
    } else {
        format!("{value:.0}")
    }
}

impl eframe::App for GqtApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_events();
        if self.unlocked_key.is_some() {
            self.check_bot_health();
            self.refresh_account();
            self.refresh_simulation_account();
            self.maybe_run_ai_decision_loop();
            self.maybe_run_event_prediction_loop();
        }
        ctx.request_repaint_after(Duration::from_millis(200));
    }

    fn ui(&mut self, ui: &mut Ui, _frame: &mut eframe::Frame) {
        if self.unlocked_key.is_none() {
            self.render_auth(ui);
        } else {
            self.render_shell(ui);
        }
        self.render_event_prediction_dialogs(ui.ctx());
        self.render_live_confirmation(ui.ctx());
        if let Some((message, error, created)) = self.toast.clone() {
            if created.elapsed() < Duration::from_secs(3) {
                egui::Area::new("toast".into())
                    .anchor(Align2::RIGHT_BOTTOM, Vec2::new(-22.0, -22.0))
                    .show(ui.ctx(), |ui| {
                        Frame::NONE
                            .fill(if error { theme::RED } else { theme::SURFACE_3 })
                            .stroke(Stroke::new(
                                1.0,
                                if error { theme::RED } else { theme::YELLOW },
                            ))
                            .corner_radius(4)
                            .inner_margin(Margin::symmetric(14, 10))
                            .show(ui, |ui| {
                                ui.label(RichText::new(message).color(theme::TEXT));
                            });
                    });
            } else {
                self.toast = None;
            }
        }
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        let _ = self.market_commands.send(MarketCommand::Stop);
        let _ = self.scanner_commands.send(ScannerCommand::Stop);
    }
}

struct AiDecisionCycle {
    provider: AiProvider,
    model: String,
    api_key: String,
    relay_base_url: String,
    config: AiTradingConfig,
    workspace: TradingWorkspace,
    dry_run: bool,
    binance_api_key: String,
    binance_api_secret: String,
    account_snapshot: FuturesAccount,
    interval: Interval,
    candle_open_time: i64,
    symbols: Vec<String>,
}

fn run_ai_decision_cycle(cycle: AiDecisionCycle) -> Result<AiDecisionSummary, String> {
    run_ai_decision_cycle_inner(cycle).map_err(|error| error.to_string())
}

fn run_ai_decision_cycle_inner(cycle: AiDecisionCycle) -> anyhow::Result<AiDecisionSummary> {
    let client = crate::network::client(Duration::from_secs(20))?;
    let audit = AuditLog::open(&cycle.workspace.ai_audit)?;
    let account = if !cycle.dry_run
        && !cycle.binance_api_key.is_empty()
        && !cycle.binance_api_secret.is_empty()
    {
        exchange::fetch_futures_account(&cycle.binance_api_key, &cycle.binance_api_secret)
            .unwrap_or(cycle.account_snapshot)
    } else {
        cycle.account_snapshot
    };

    let mut inputs = Vec::new();
    let mut failures = Vec::new();
    for symbol in &cycle.symbols {
        let result = (|| -> anyhow::Result<AiTradingInput> {
            let candles = market::fetch_candles(&client, symbol, cycle.interval, 160)?;
            let snapshot = market::fetch_snapshot(&client, symbol)?;
            let factor = ai_trader::calculate_factor_snapshot(&candles);
            Ok(AiTradingInput {
                symbol: symbol.clone(),
                timeframe: cycle.config.timeframe.clone(),
                candle_open_time: cycle.candle_open_time,
                candles,
                snapshot,
                factor,
                account: account.clone(),
                current_position: position_for_symbol(&account, symbol),
                configured_leverage: cycle.config.leverage,
                configured_capital_usage_percent: cycle.config.capital_usage_percent,
            })
        })();
        match result {
            Ok(input) => inputs.push(input),
            Err(error) if is_restricted_location_error(&error) => {
                let processed = cycle
                    .symbols
                    .iter()
                    .map(|symbol| (symbol.clone(), cycle.candle_open_time))
                    .collect();
                return Ok(AiDecisionSummary {
                    message: "AI 闭环已暂停：Binance Futures 拒绝了当前网络位置（HTTP 451）。实时模拟盘和实盘都需要能访问 Binance Futures 的合规网络；当前只能使用离线回测或已有本地数据测试。".into(),
                    processed,
                });
            }
            Err(error) if is_ai_output_format_error(&error) => {
                return Ok(AiDecisionSummary {
                    message: format!(
                        "AI 闭环已暂停：AI 输出不是可执行 JSON。{}。请检查中转站 Base URL 是否为 OpenAI-compatible /v1、模型名是否支持 chat/completions，或换支持 JSON 输出的模型。",
                        compact_error(&error.to_string(), 520)
                    ),
                    processed: Vec::new(),
                });
            }
            Err(error) if is_ai_provider_capacity_error(&error) => {
                return Ok(AiDecisionSummary {
                    message: format!(
                        "AI 本轮未生成信号：中转站当前不可用。{}。程序已做即时重试；下一轮会继续尝试，请确认模型/分组有可用通道。",
                        compact_error(&error.to_string(), 520)
                    ),
                    processed: Vec::new(),
                });
            }
            Err(error) => failures.push(format!("{symbol}: {error}")),
        }
    }

    let mut processed = Vec::new();
    let mut messages = Vec::new();
    if !inputs.is_empty() {
        match ai::decide_trades(
            cycle.provider,
            &cycle.model,
            &cycle.api_key,
            &cycle.relay_base_url,
            &inputs,
            &cycle.config,
        ) {
            Ok(output) => {
                for (input, signal) in inputs.iter().zip(output.signals.into_iter()) {
                    let decision = ai_trader::validate_signal(
                        &cycle.config,
                        input,
                        signal,
                        chrono::Utc::now().timestamp(),
                    );
                    audit.record_decision(
                        cycle.provider.label(),
                        &output.model,
                        input,
                        &output.raw_output,
                        &decision.signal,
                        decision.approved,
                        &decision.reason,
                    )?;
                    ai_trader::write_signal_atomically(
                        &cycle.workspace.ai_signals,
                        &decision.signal,
                    )?;
                    processed.push((input.symbol.clone(), cycle.candle_open_time));
                    messages.push(format!(
                        "{} {} {} ({:.0}%, 因子 {:.2})",
                        input.symbol,
                        action_label(&decision.signal.action),
                        if decision.approved {
                            "通过"
                        } else {
                            "拦截/观望"
                        },
                        decision.signal.confidence * 100.0,
                        input.factor.score
                    ));
                }
            }
            Err(error) if is_ai_output_format_error(&error) => {
                return Ok(AiDecisionSummary {
                    message: format!(
                        "AI 本轮未生成信号：AI 输出不是可执行 JSON。{}。请检查中转站 Base URL 是否为 OpenAI-compatible /v1、模型名是否支持 chat/completions，或换支持 JSON 输出的模型。",
                        compact_error(&error.to_string(), 520)
                    ),
                    processed: Vec::new(),
                });
            }
            Err(error) if is_ai_provider_capacity_error(&error) => {
                return Ok(AiDecisionSummary {
                    message: format!(
                        "AI 本轮未生成信号：中转站当前不可用。{}。程序已做即时重试；下一轮会继续尝试，请确认模型/分组有可用通道。",
                        compact_error(&error.to_string(), 520)
                    ),
                    processed: Vec::new(),
                });
            }
            Err(error) => failures.push(error.to_string()),
        }
    }

    if messages.is_empty() && !failures.is_empty() {
        anyhow::bail!("{}", failures.join("; "));
    }
    let mut message = if messages.is_empty() {
        "AI 本轮没有生成新信号".to_string()
    } else {
        format!("AI 决策完成：{}", messages.join("，"))
    };
    if !failures.is_empty() {
        message.push_str(&format!("；失败：{}", failures.join("；")));
    }
    Ok(AiDecisionSummary { message, processed })
}

fn simulation_to_futures_account(account: &SimulationAccount) -> FuturesAccount {
    FuturesAccount {
        wallet_balance: account.wallet_balance,
        available_balance: account.available_balance,
        margin_balance: account.wallet_balance,
        unrealized_profit: 0.0,
        initial_margin: account.open_stake,
        maintenance_margin: 0.0,
        positions: Vec::new(),
        updated_at: chrono::Utc::now().timestamp_millis(),
    }
}

fn is_restricted_location_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("restricted location") || message.contains("http 451")
}

fn is_ai_output_format_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("AI 决策中没有 JSON 对象")
        || message.contains("AI 输出没有 JSON")
        || message.contains("AI JSON 修复响应")
        || message.contains("AI 决策 JSON")
        || message.contains("返回不是有效 JSON")
}

fn is_ai_provider_capacity_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("503 service unavailable")
        || message.contains("no available channel")
        || message.contains("system cpu overloaded")
        || message.contains("temporarily unavailable")
        || message.contains("rate limit")
        || message.contains("too many requests")
}

fn compact_error(value: &str, limit: usize) -> String {
    let compact = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(limit)
        .collect::<String>();
    if compact.is_empty() {
        "没有返回可读错误内容".into()
    } else {
        compact
    }
}

fn position_for_symbol(account: &FuturesAccount, symbol: &str) -> Option<FuturesPosition> {
    account
        .positions
        .iter()
        .find(|position| position.symbol == symbol)
        .cloned()
}

fn last_closed_candle_open(interval: Interval) -> i64 {
    let seconds = interval.seconds();
    let now = chrono::Utc::now().timestamp();
    (now / seconds) * seconds - seconds
}

fn action_label(action: &crate::model::AiAction) -> &'static str {
    match action {
        crate::model::AiAction::Long => "做多",
        crate::model::AiAction::Short => "做空",
        crate::model::AiAction::Close => "平仓",
        crate::model::AiAction::Hold => "观望",
    }
}

fn draw_candles(
    ui: &mut Ui,
    candles: &[Candle],
    interval: Interval,
    market_error: &str,
    connected: bool,
    height: f32,
) {
    let desired = Vec2::new(ui.available_width(), height.max(320.0));
    let (rect, response) = ui.allocate_exact_size(desired, Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 4, Color32::from_rgb(8, 11, 14));
    painter.rect_stroke(
        rect,
        4,
        Stroke::new(1.0, theme::BORDER),
        egui::StrokeKind::Inside,
    );
    let plot = Rect::from_min_max(
        rect.min + Vec2::new(12.0, 42.0),
        rect.max - Vec2::new(76.0, 34.0),
    );
    painter.text(
        rect.min + Vec2::new(14.0, 20.0),
        Align2::LEFT_CENTER,
        format!("{} K线", interval.label()),
        FontId::new(13.0, FontFamily::Proportional),
        theme::TEXT,
    );
    let status_text = if candles.is_empty() {
        if connected {
            "等待K线数据".to_string()
        } else if market_error.is_empty() {
            "行情连接中".to_string()
        } else {
            "K线加载失败".to_string()
        }
    } else {
        format!("已加载 {} 根", candles.len())
    };
    painter.text(
        rect.right_top() + Vec2::new(-14.0, 20.0),
        Align2::RIGHT_CENTER,
        status_text,
        FontId::new(12.0, FontFamily::Proportional),
        if candles.is_empty() && !connected {
            theme::RED
        } else {
            theme::MUTED
        },
    );
    for index in 0..=5 {
        let y = egui::lerp(plot.top()..=plot.bottom(), index as f32 / 5.0);
        painter.line_segment(
            [Pos2::new(plot.left(), y), Pos2::new(plot.right(), y)],
            Stroke::new(1.0, Color32::from_rgb(30, 36, 43)),
        );
    }
    for index in 0..=6 {
        let x = egui::lerp(plot.left()..=plot.right(), index as f32 / 6.0);
        painter.line_segment(
            [Pos2::new(x, plot.top()), Pos2::new(x, plot.bottom())],
            Stroke::new(1.0, Color32::from_rgb(30, 36, 43)),
        );
    }
    if candles.is_empty() {
        let message = if market_error.is_empty() {
            "正在加载 Binance Futures K线"
        } else {
            market_error
        };
        painter.text(
            plot.center(),
            Align2::CENTER_CENTER,
            message,
            FontId::new(14.0, FontFamily::Proportional),
            if market_error.is_empty() {
                theme::MUTED
            } else {
                theme::RED
            },
        );
        return;
    }
    let max_visible = ((plot.width() / 6.0) as usize).clamp(50, 180);
    let visible = &candles[candles.len().saturating_sub(max_visible)..];
    let low = visible
        .iter()
        .map(|candle| candle.low)
        .fold(f64::INFINITY, f64::min);
    let high = visible
        .iter()
        .map(|candle| candle.high)
        .fold(f64::NEG_INFINITY, f64::max);
    if !low.is_finite() || !high.is_finite() || high <= 0.0 || low <= 0.0 {
        painter.text(
            plot.center(),
            Align2::CENTER_CENTER,
            "K线数据异常，等待下一次刷新",
            FontId::new(14.0, FontFamily::Proportional),
            theme::RED,
        );
        return;
    }
    let padding = ((high - low) * 0.06).max(high.abs() * 0.0005);
    let low = low - padding;
    let high = high + padding;
    let spread = (high - low).max(f64::EPSILON);
    let y_of = |price: f64| plot.bottom() - ((price - low) / spread) as f32 * plot.height();
    let step = plot.width() / visible.len() as f32;
    let body_width = (step * 0.66).clamp(2.0, 8.0);
    for (index, candle) in visible.iter().enumerate() {
        let x = plot.left() + step * (index as f32 + 0.5);
        let color = if candle.close >= candle.open {
            theme::GREEN
        } else {
            theme::RED
        };
        painter.line_segment(
            [
                Pos2::new(x, y_of(candle.high)),
                Pos2::new(x, y_of(candle.low)),
            ],
            Stroke::new(1.0, color),
        );
        let top = y_of(candle.open.max(candle.close));
        let bottom = y_of(candle.open.min(candle.close));
        let body = Rect::from_min_max(
            Pos2::new(x - body_width / 2.0, top),
            Pos2::new(x + body_width / 2.0, bottom.max(top + 1.0)),
        );
        painter.rect_filled(body, 0, color);
    }
    if visible.len() > 1 {
        let close_points = visible
            .iter()
            .enumerate()
            .map(|(index, candle)| {
                Pos2::new(
                    plot.left() + step * (index as f32 + 0.5),
                    y_of(candle.close),
                )
            })
            .collect::<Vec<_>>();
        painter.add(egui::Shape::line(
            close_points,
            Stroke::new(1.6, theme::YELLOW),
        ));
    }
    if let Some(last) = visible.last() {
        painter.text(
            rect.left_bottom() + Vec2::new(14.0, -15.0),
            Align2::LEFT_CENTER,
            format!(
                "最后收盘 {}  成交量 {}",
                format_price(last.close),
                compact_number(last.volume)
            ),
            FontId::new(11.0, FontFamily::Proportional),
            theme::MUTED,
        );
    }
    for index in 0..=5 {
        let ratio = index as f32 / 5.0;
        let price = high - spread * ratio as f64;
        let y = egui::lerp(plot.top()..=plot.bottom(), ratio);
        painter.text(
            Pos2::new(plot.right() + 8.0, y),
            Align2::LEFT_CENTER,
            format_price(price),
            FontId::new(11.0, FontFamily::Monospace),
            theme::MUTED,
        );
    }
    if let Some(pointer) = response
        .hover_pos()
        .filter(|pointer| plot.contains(*pointer))
    {
        let index = (((pointer.x - plot.left()) / step).floor() as usize).min(visible.len() - 1);
        let candle = &visible[index];
        let x = plot.left() + step * (index as f32 + 0.5);
        painter.line_segment(
            [Pos2::new(x, plot.top()), Pos2::new(x, plot.bottom())],
            Stroke::new(1.0, theme::MUTED),
        );
        painter.line_segment(
            [
                Pos2::new(plot.left(), pointer.y),
                Pos2::new(plot.right(), pointer.y),
            ],
            Stroke::new(1.0, theme::MUTED),
        );
        let tooltip = format!(
            "O {}  H {}  L {}  C {}",
            format_price(candle.open),
            format_price(candle.high),
            format_price(candle.low),
            format_price(candle.close)
        );
        painter.rect_filled(
            Rect::from_min_size(plot.min + Vec2::new(8.0, 8.0), Vec2::new(320.0, 26.0)),
            3,
            theme::SURFACE_2,
        );
        painter.text(
            plot.min + Vec2::new(16.0, 21.0),
            Align2::LEFT_CENTER,
            tooltip,
            FontId::new(11.0, FontFamily::Monospace),
            theme::TEXT,
        );
    }
}

fn metric(ui: &mut Ui, label: &str, value: &str, sub: &str, accent: Color32) {
    Frame::NONE
        .fill(theme::SURFACE)
        .stroke(Stroke::new(1.0, theme::BORDER))
        .corner_radius(5)
        .inner_margin(Margin::same(14))
        .show(ui, |ui| {
            ui.set_min_height(96.0);
            ui.label(RichText::new(label).size(11.0).color(theme::MUTED));
            ui.add_space(8.0);
            ui.label(RichText::new(value).size(20.0).color(accent).strong());
            ui.label(RichText::new(sub).size(11.0).color(theme::MUTED));
        });
}

fn event_stat(stats: &[EventPredictionStats], horizon_minutes: i64) -> EventPredictionStats {
    stats
        .iter()
        .find(|stat| stat.horizon_minutes == horizon_minutes)
        .cloned()
        .unwrap_or(EventPredictionStats {
            horizon_minutes,
            ..Default::default()
        })
}

fn event_metric(ui: &mut Ui, label: &str, stat: &EventPredictionStats) {
    metric(
        ui,
        label,
        &format!("{:.1}%", stat.win_rate),
        &format!(
            "{} 胜 / {} 负 / {} 平，均波 {:+.3}%，均置信 {:.1}%",
            stat.wins, stat.losses, stat.ties, stat.avg_move_percent, stat.avg_confidence
        ),
        if stat.win_rate >= 50.0 {
            theme::GREEN
        } else if stat.total > 0 {
            theme::RED
        } else {
            theme::TEXT
        },
    );
}

fn format_event_money(value: f64) -> String {
    if value.is_finite() {
        format!("{value:.2} USDT")
    } else {
        "无限".into()
    }
}

fn event_ticket_list_card(
    ui: &mut Ui,
    title: &str,
    context: &str,
    tickets: &[EventPredictionTicket],
    empty_text: &str,
    kind: EventOrderKind,
    order_dialog: &mut Option<EventOrderKind>,
) -> Option<EventTicketAction> {
    let mut selected_action = None;
    Frame::NONE
        .fill(theme::SURFACE)
        .stroke(Stroke::new(1.0, theme::BORDER))
        .corner_radius(5)
        .inner_margin(Margin::same(14))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(RichText::new(title).size(14.0).strong());
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    if ui.add(theme::secondary_button("打开大列表")).clicked() {
                        *order_dialog = Some(kind);
                    }
                    ui.label(
                        RichText::new(format!("{} 条 · {}", tickets.len(), context))
                            .size(11.0)
                            .color(theme::MUTED),
                    );
                });
            });
            ui.add_space(10.0);
            if tickets.is_empty() {
                ui.label(RichText::new(empty_text).color(theme::MUTED));
                return;
            }

            egui::Grid::new(format!("event-ticket-card-{}", event_order_kind_salt(kind)))
                .num_columns(11)
                .striped(true)
                .spacing([18.0, 10.0])
                .show(ui, |ui| {
                    for heading in [
                        "票据",
                        "交易对",
                        "周期",
                        "周期编号 / 轮次 / 线路",
                        "本单下注",
                        "本单回报",
                        "方向",
                        "结果",
                        "盈亏",
                        "开盘",
                        "到期",
                    ] {
                        ui.label(RichText::new(heading).color(theme::MUTED).strong());
                    }
                    ui.end_row();
                    for ticket in tickets.iter().take(8) {
                        if ui
                            .add(theme::secondary_button(compact_event_id(&ticket.id)))
                            .clicked()
                        {
                            selected_action = Some(EventTicketAction::Ticket(ticket.clone()));
                        }
                        ui.label(RichText::new(&ticket.symbol).strong());
                        ui.label(format!("{}m", ticket.horizon_minutes));
                        if event_cycle_button(ui, ticket, true).clicked() {
                            selected_action =
                                Some(EventTicketAction::Cycle(ticket.cycle_id.clone()));
                        }
                        ui.label(format!("{:.2}", ticket.stake_amount));
                        ui.label(
                            ticket
                                .cycle_balance_after
                                .map(|value| format!("{value:.2}"))
                                .unwrap_or_else(|| "--".into()),
                        );
                        ui.colored_label(
                            prediction_direction_color(&ticket.direction),
                            prediction_direction_label(&ticket.direction),
                        );
                        ui.colored_label(
                            prediction_result_color(ticket),
                            prediction_result_label(ticket),
                        );
                        ui.label(
                            ticket
                                .virtual_pnl
                                .map(|value| format!("{value:+.2}"))
                                .unwrap_or_else(|| "--".into()),
                        );
                        ui.label(format_event_time(ticket.open_time));
                        ui.label(format_event_time(ticket.close_time));
                        ui.end_row();
                    }
                });

            if tickets.len() > 8 {
                ui.add_space(8.0);
                ui.label(
                    RichText::new(format!(
                        "还有 {} 条，点“打开大列表”查看。",
                        tickets.len() - 8
                    ))
                    .size(11.0)
                    .color(theme::MUTED),
                );
            }
        });
    selected_action
}

fn event_ticket_table(
    ui: &mut Ui,
    scroll_id: &str,
    title: &str,
    context: &str,
    tickets: &[EventPredictionTicket],
    empty_text: &str,
    max_height: f32,
    cycle_links: bool,
) -> Option<EventTicketAction> {
    let mut selected_action = None;
    Frame::NONE
        .fill(theme::SURFACE)
        .stroke(Stroke::new(1.0, theme::BORDER))
        .corner_radius(5)
        .inner_margin(Margin::same(14))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(RichText::new(title).size(14.0).strong());
                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                    ui.label(
                        RichText::new(format!("{} 条 · {}", tickets.len(), context))
                            .size(11.0)
                            .color(theme::MUTED),
                    );
                });
            });
            ui.add_space(10.0);
            if tickets.is_empty() {
                ui.label(RichText::new(empty_text).color(theme::MUTED));
                return;
            }
            ScrollArea::both()
                .id_salt(scroll_id)
                .max_height(max_height)
                .show(ui, |ui| {
                    egui::Grid::new(format!("{scroll_id}-grid"))
                        .num_columns(18)
                        .striped(true)
                        .spacing([18.0, 12.0])
                        .show(ui, |ui| {
                            for heading in [
                                "票据",
                                "交易对",
                                "周期",
                                "周期编号",
                                "轮次 / 线路",
                                "方向",
                                "置信度",
                                "分数",
                                "下注",
                                "开盘价",
                                "到期价",
                                "结果",
                                "波动",
                                "虚拟盈亏",
                                "本单回报",
                                "开盘时间",
                                "到期时间",
                                "复盘",
                            ] {
                                ui.label(RichText::new(heading).color(theme::MUTED).strong());
                            }
                            ui.end_row();
                            for ticket in tickets {
                                if ui
                                    .add(theme::secondary_button(compact_event_id(&ticket.id)))
                                    .clicked()
                                {
                                    selected_action =
                                        Some(EventTicketAction::Ticket(ticket.clone()));
                                }
                                ui.label(RichText::new(&ticket.symbol).strong());
                                ui.label(format!("{}m", ticket.horizon_minutes));
                                if cycle_links {
                                    if event_cycle_button(ui, ticket, false).clicked() {
                                        selected_action =
                                            Some(EventTicketAction::Cycle(ticket.cycle_id.clone()));
                                    }
                                } else {
                                    ui.label(format_event_cycle_id(ticket));
                                }
                                ui.label(format_event_cycle_step(ticket));
                                ui.colored_label(
                                    prediction_direction_color(&ticket.direction),
                                    prediction_direction_label(&ticket.direction),
                                );
                                ui.label(format!("{:.1}%", ticket.confidence * 100.0));
                                ui.label(format!("{:+.3}", ticket.score));
                                ui.label(format!("{:.2}", ticket.stake_amount));
                                ui.label(format_price(ticket.entry_price));
                                ui.label(
                                    ticket
                                        .expiry_price
                                        .map(format_price)
                                        .unwrap_or_else(|| "--".into()),
                                );
                                ui.colored_label(
                                    prediction_result_color(ticket),
                                    prediction_result_label(ticket),
                                );
                                ui.label(
                                    ticket
                                        .move_percent
                                        .map(|value| format!("{value:+.4}%"))
                                        .unwrap_or_else(|| "--".into()),
                                );
                                ui.label(
                                    ticket
                                        .virtual_pnl
                                        .map(|value| format!("{value:+.1}"))
                                        .unwrap_or_else(|| "--".into()),
                                );
                                ui.label(
                                    ticket
                                        .cycle_balance_after
                                        .map(|value| format!("{value:.2}"))
                                        .unwrap_or_else(|| "--".into()),
                                );
                                ui.label(format_event_time(ticket.open_time));
                                ui.label(format_event_time(ticket.close_time));
                                ui.label(compact_error(&ticket.review, 120));
                                ui.end_row();
                            }
                        });
                });
        });
    selected_action
}

fn event_cycle_button(
    ui: &mut Ui,
    ticket: &EventPredictionTicket,
    include_step: bool,
) -> egui::Response {
    if ticket.cycle_id.is_empty() || ticket.cycle_number <= 0 {
        ui.add_enabled(false, theme::secondary_button("旧玩法"))
    } else {
        let label = if include_step {
            format!(
                "{} · {}",
                format_event_cycle_id(ticket),
                format_event_cycle_step(ticket)
            )
        } else {
            format_event_cycle_id(ticket)
        };
        ui.add(theme::secondary_button(label).sense(Sense::click()))
            .on_hover_text(format!("查看周期 {} 的全部订单", ticket.cycle_id))
    }
}

fn event_ticket_cycle_detail_row(ui: &mut Ui, ticket: &EventPredictionTicket) -> bool {
    let mut clicked = false;
    ui.horizontal(|ui| {
        ui.label(RichText::new("周期编号").color(theme::MUTED));
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            if event_cycle_button(ui, ticket, false).clicked() {
                clicked = true;
            }
        });
    });
    ui.separator();
    clicked
}

fn compact_event_id(id: &str) -> String {
    id.rsplit_once('-')
        .map(|(_, suffix)| suffix.to_string())
        .unwrap_or_else(|| id.to_string())
}

fn format_event_cycle_step(ticket: &EventPredictionTicket) -> String {
    if ticket.cycle_order > 0 {
        let slot = if ticket.cycle_slot.is_some_and(|slot| slot > 0) {
            format!("{}号线", ticket.cycle_slot.unwrap_or_default())
        } else {
            "--号线".into()
        };
        format!("第 {} 轮 / {slot}", ticket.cycle_order)
    } else {
        "旧玩法".into()
    }
}

fn format_event_cycle_id(ticket: &EventPredictionTicket) -> String {
    if ticket.cycle_number <= 0 {
        return "旧玩法".into();
    }
    format!("C{:06}", ticket.cycle_number)
}

fn event_order_kind_salt(kind: EventOrderKind) -> &'static str {
    match kind {
        EventOrderKind::Open => "open",
        EventOrderKind::Settled => "settled",
    }
}

fn event_ticket_detail_row(ui: &mut Ui, label: &str, value: &str) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(label).color(theme::MUTED));
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            ui.label(RichText::new(value).color(theme::TEXT));
        });
    });
    ui.separator();
}

fn format_event_horizon_selection(horizons: &[EventHorizon]) -> String {
    let mut labels = horizons
        .iter()
        .map(|horizon| horizon.label())
        .collect::<Vec<_>>();
    labels.dedup();
    labels.join("/")
}

fn format_event_run_directions(directions: &[EventPredictionRunDirection]) -> String {
    let mut groups = Vec::new();
    for horizon in [10, 30, 60] {
        let mut parts = directions
            .iter()
            .filter(|direction| direction.horizon_minutes == horizon)
            .map(|direction| {
                let symbol = compact_event_symbol(&direction.symbol);
                let label = prediction_trade_direction_label(&direction.direction);
                let confidence = direction.confidence * 100.0;
                let duplicate_marker = if direction.created {
                    ""
                } else {
                    "·等待结算"
                };
                format!("{symbol} {label}({confidence:.0}%{duplicate_marker})")
            })
            .collect::<Vec<_>>();
        if parts.is_empty() {
            continue;
        }
        parts.sort();
        let horizon_label = if horizon == 60 {
            "1h".into()
        } else {
            format!("{horizon}m")
        };
        groups.push(format!("{horizon_label}：{}", parts.join("，")));
    }
    groups.join("；")
}

fn format_event_direction_window(direction: &EventPredictionRunDirection) -> String {
    let horizon = if direction.horizon_minutes == 60 {
        "1小时".into()
    } else {
        format!("{}分钟", direction.horizon_minutes)
    };
    format!(
        "{}：{} → {}",
        horizon,
        format_event_time(direction.open_time),
        format_event_time(direction.close_time)
    )
}

fn compact_event_symbol(symbol: &str) -> &str {
    match symbol {
        "BTCUSDT" => "BTC",
        "ETHUSDT" => "ETH",
        _ => symbol,
    }
}

fn prediction_trade_direction_label(direction: &str) -> &'static str {
    match direction {
        "up" => "买涨",
        "down" => "买跌",
        _ => "--",
    }
}

fn prediction_direction_label(direction: &str) -> &'static str {
    match direction {
        "up" => "看涨",
        "down" => "看跌",
        _ => "--",
    }
}

fn prediction_direction_color(direction: &str) -> Color32 {
    match direction {
        "up" => theme::GREEN,
        "down" => theme::RED,
        _ => theme::MUTED,
    }
}

fn prediction_result_label(ticket: &EventPredictionTicket) -> &'static str {
    if ticket.status == "open" {
        return "待结算";
    }
    match ticket.result.as_str() {
        "win" => "胜",
        "loss" => "负",
        "tie" => "平",
        _ => "--",
    }
}

fn prediction_result_color(ticket: &EventPredictionTicket) -> Color32 {
    if ticket.status == "open" {
        return theme::YELLOW;
    }
    match ticket.result.as_str() {
        "win" => theme::GREEN,
        "loss" => theme::RED,
        _ => theme::MUTED,
    }
}

fn section_title(ui: &mut Ui, title: &str, context: &str) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(title).size(16.0).strong());
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            ui.label(RichText::new(context).size(11.0).color(theme::MUTED));
        });
    });
    ui.separator();
    ui.add_space(6.0);
}

fn status_row(ui: &mut Ui, label: &str, value: &str) {
    ui.horizontal(|ui| {
        ui.label(RichText::new(label).color(theme::MUTED));
        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
            ui.label(RichText::new(value).color(theme::TEXT));
        });
    });
    ui.separator();
}

fn field_label(ui: &mut Ui, value: &str) {
    ui.add_space(5.0);
    ui.label(RichText::new(value).size(11.0).color(theme::MUTED));
}

fn credential_field(ui: &mut Ui, label: &str, value: &mut String) {
    field_label(ui, label);
    ui.add_sized(
        [ui.available_width(), 36.0],
        TextEdit::singleline(value).password(true),
    );
}

fn settings_section(ui: &mut Ui, title: &str, context: &str, body: impl FnOnce(&mut Ui)) {
    Frame::NONE
        .fill(theme::SURFACE)
        .stroke(Stroke::new(1.0, theme::BORDER))
        .corner_radius(5)
        .inner_margin(Margin::same(16))
        .show(ui, |ui| {
            section_title(ui, title, context);
            body(ui);
        });
}

fn status_chip(ui: &mut Ui, label: &str, configured: bool) {
    Frame::NONE
        .fill(theme::SURFACE_2)
        .corner_radius(3)
        .inner_margin(Margin::symmetric(9, 6))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(label);
                ui.colored_label(
                    if configured {
                        theme::GREEN
                    } else {
                        theme::MUTED
                    },
                    if configured { "已配置" } else { "未配置" },
                );
            });
        });
}

fn format_symbol_whitelist(symbols: &[String]) -> String {
    symbols.join(", ")
}

fn compact_build_label() -> &'static str {
    BUILD_LABEL
        .split('-')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or(BUILD_LABEL)
}

fn parse_symbol_whitelist(value: &str) -> Result<Vec<String>, String> {
    let mut symbols = Vec::new();
    for raw in value.split(|character: char| {
        character == ','
            || character == ';'
            || character == '，'
            || character == '；'
            || character.is_whitespace()
    }) {
        let compact = raw
            .trim()
            .to_ascii_uppercase()
            .replace("/", "")
            .replace(":USDT", "");
        if compact.is_empty() {
            continue;
        }
        let Some(base) = compact.strip_suffix("USDT") else {
            return Err(format!("{compact} 不是 U 本位永续合约代码"));
        };
        if !(2..=12).contains(&base.len())
            || !base
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        {
            return Err(format!("{compact} 合约代码格式无效"));
        }
        if !symbols.iter().any(|symbol| symbol == &compact) {
            symbols.push(compact);
        }
    }
    if symbols.is_empty() {
        return Err("币种白名单不能为空".into());
    }
    Ok(symbols)
}

fn ai_timeframe_guidance(timeframe: &str) -> (&'static str, Color32) {
    match timeframe {
        "4h" => (
            "稳健确认：4h 信号更少，适合过滤大方向或做慢速回测。",
            theme::MUTED,
        ),
        "1m" => (
            "高风险：1m 直接跑多因子回测表现很差，不建议自动交易。",
            theme::RED,
        ),
        "5m" | "15m" => (
            "默认滚仓试验档：适合 dry-run、小资金模拟和短周期因子筛选。",
            theme::GREEN,
        ),
        "1h" => (
            "中频确认：交易频率和噪音介于 15m 与 4h 之间。",
            theme::YELLOW,
        ),
        "1d" => ("低频：信号更少，适合观察大趋势。", theme::MUTED),
        _ => ("周期未识别，请保存前检查配置。", theme::RED),
    }
}

fn log_view(ui: &mut Ui, log: &str, height: f32) {
    Frame::NONE
        .fill(Color32::from_rgb(8, 10, 13))
        .stroke(Stroke::new(1.0, theme::BORDER))
        .corner_radius(4)
        .inner_margin(Margin::same(12))
        .show(ui, |ui| {
            ScrollArea::vertical()
                .max_height(height)
                .stick_to_bottom(true)
                .show(ui, |ui| {
                    ui.set_min_height(height - 24.0);
                    ui.label(
                        RichText::new(if log.is_empty() { "暂无日志" } else { log })
                            .monospace()
                            .size(11.0)
                            .color(Color32::from_rgb(190, 200, 195)),
                    );
                });
        });
}

fn format_price(value: f64) -> String {
    if !value.is_finite() || value == 0.0 {
        return "--".into();
    }
    if value < 1.0 {
        format!("{value:.6}")
    } else {
        format!("{value:.2}")
    }
}

fn compact_number(value: f64) -> String {
    if value.abs() >= 1_000_000_000.0 {
        format!("{:.2}B", value / 1_000_000_000.0)
    } else if value.abs() >= 1_000_000.0 {
        format!("{:.2}M", value / 1_000_000.0)
    } else if value.abs() >= 1_000.0 {
        format!("{:.2}K", value / 1_000.0)
    } else {
        format!("{value:.2}")
    }
}
