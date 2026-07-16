use std::{
    collections::BTreeMap,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

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
    exchange, market,
    model::{
        AiProvider, AiTradingConfig, AiTradingInput, Candle, CredentialDraft, FuturesAccount,
        FuturesPosition, Interval, MarginMode, MarketCommand, MarketEvent, MarketSnapshot, Page,
        SecretStatus, SimulationAccount,
    },
    store::SecretStore,
    theme,
    trading::TradingWorkspace,
};

const AI_TIMEFRAMES: [&str; 6] = ["1m", "5m", "15m", "1h", "4h", "1d"];
const BUILD_LABEL: &str = "0.3.3-ai-json-repair";
const RELAY_DEFAULT_MODEL: &str = "gpt5.5";

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
    strategy_source: String,
    strategy_state: String,
    stake_amount: f64,
    max_open_trades: i64,
    risk_reward_ratio: f64,
    allow_ai_risk_sizing: bool,
    docker_available: bool,
    bot_state: String,
    bot_log: String,
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

impl GqtApp {
    pub fn new(cc: &eframe::CreationContext<'_>) -> Self {
        theme::configure(&cc.egui_ctx);
        let project_dirs = ProjectDirs::from("xin", "HillmanPick", "GQT Trader")
            .expect("Windows application data directory");
        let data_root = project_dirs.data_local_dir().to_path_buf();
        std::fs::create_dir_all(&data_root).expect("create GQT data directory");
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
            workspace.risk().unwrap_or((50.0, 3, 2.0, false));
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
            interval: Interval::FourHours,
        });
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
            simulation_account: SimulationAccount::default(),
            simulation_error: String::new(),
            simulation_check_running: false,
            last_simulation_check: Instant::now() - Duration::from_secs(30),
            show_simulation_account: false,
            page: Page::Overview,
            symbol: initial_symbol,
            interval: Interval::FourHours,
            candles: Vec::new(),
            snapshot: MarketSnapshot::default(),
            market_connected: false,
            market_error: String::new(),
            market_commands,
            market_events,
            strategy_source,
            strategy_state: "未修改".into(),
            stake_amount,
            max_open_trades,
            risk_reward_ratio,
            allow_ai_risk_sizing,
            docker_available,
            bot_state,
            bot_log: String::new(),
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
                    if let Some(last) = self.candles.last_mut() {
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
        while let Ok(event) = self.task_receiver.try_recv() {
            match event {
                TaskEvent::Bot(result) => match result {
                    Ok(message) => {
                        self.toast(message, false);
                        let (available, state) = self.workspace.docker_state();
                        self.docker_available = available;
                        self.bot_state = state;
                    }
                    Err(error) => self.toast(error, true),
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
                    if self.dry_run && self.auto_restart && available && self.bot_state != "running"
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
                                ui.label(RichText::new(page.label()).color(if active {
                                    theme::TEXT
                                } else {
                                    theme::MUTED
                                }));
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
                ui.with_layout(Layout::bottom_up(Align::LEFT), |ui| {
                    ui.separator();
                    ui.horizontal(|ui| {
                        let color = if self.docker_available {
                            theme::GREEN
                        } else {
                            theme::MUTED
                        };
                        ui.colored_label(color, "●");
                        ui.vertical(|ui| {
                            ui.label(
                                RichText::new(if self.docker_available {
                                    "交易内核就绪"
                                } else {
                                    "Docker 离线"
                                })
                                .size(12.0),
                            );
                            ui.label(
                                RichText::new(if self.dry_run { "DRY-RUN" } else { "LIVE" })
                                    .size(10.0)
                                    .color(if self.dry_run {
                                        theme::MUTED
                                    } else {
                                        theme::RED
                                    }),
                            );
                            ui.label(RichText::new(BUILD_LABEL).size(9.0).color(theme::MUTED));
                        });
                    });
                });
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
                Page::Market => self.render_market(ui),
                Page::Strategy => self.render_strategy(ui),
                Page::Backtest => self.render_backtest(ui),
                Page::Data => self.render_data(ui),
                Page::Execution => self.render_execution(ui),
                Page::Settings => self.render_settings(ui),
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
                    section_title(ui, "市场概览", "BTCUSDT / 实时状态");
                    self.draw_mini_performance(ui);
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
            for interval in Interval::ALL {
                let selected = interval == self.interval;
                let button =
                    egui::Button::new(RichText::new(interval.as_str()).color(if selected {
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
                if ui.add_sized([48.0, 34.0], button).clicked() {
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
        let sidebar_width = 292.0;
        ui.horizontal_top(|ui| {
            let chart_width = (ui.available_width() - sidebar_width - 14.0).max(480.0);
            ui.allocate_ui(Vec2::new(chart_width, 620.0), |ui| {
                ui.horizontal(|ui| {
                    ui.vertical(|ui| {
                        ui.label(RichText::new(&self.symbol).size(13.0).color(theme::MUTED));
                        ui.label(
                            RichText::new(format_price(self.snapshot.price))
                                .size(25.0)
                                .strong(),
                        );
                    });
                    ui.with_layout(Layout::right_to_left(Align::Min), |ui| {
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
                ui.add_space(6.0);
                draw_candles(ui, &self.candles);
            });
            ui.add_space(14.0);
            ui.allocate_ui(Vec2::new(sidebar_width, 620.0), |ui| {
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
                ui.add_space(18.0);
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
        if !self.market_error.is_empty() && !self.market_connected {
            ui.colored_label(theme::RED, &self.market_error);
        }
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
        let response = ui.add_sized(
            ui.available_size(),
            TextEdit::multiline(&mut self.strategy_source)
                .font(egui::TextStyle::Monospace)
                .code_editor()
                .desired_width(f32::INFINITY),
        );
        if response.changed() {
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
            "正在同步所选范围的 4h 历史数据，完成后将自动运行回测...".into()
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
                        if ui
                            .add(theme::primary_button(if self.dry_run {
                                "启动模拟策略"
                            } else {
                                "启动实盘策略"
                            }))
                            .clicked()
                        {
                            if self.dry_run {
                                self.bot_action(true);
                            } else {
                                self.live_acknowledged = false;
                                self.live_confirmation = Some(LiveAction::Start);
                            }
                        }
                        if ui.add(theme::secondary_button("停止")).clicked() {
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
        thread::spawn(move || {
            let result = workspace
                .bot_action(start, &api_key, &api_secret)
                .map(|_| {
                    if start {
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
                        "每根 K 线只执行一次",
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
                || self.ai_model.trim().eq_ignore_ascii_case("gpt-4o-mini"))
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

    fn draw_mini_performance(&self, ui: &mut Ui) {
        let (rect, _) =
            ui.allocate_exact_size(Vec2::new(ui.available_width(), 190.0), Sense::hover());
        let painter = ui.painter_at(rect);
        painter.rect_filled(rect, 4, theme::SURFACE);
        for index in 0..5 {
            let y = egui::lerp(rect.top()..=rect.bottom(), index as f32 / 4.0);
            painter.line_segment(
                [Pos2::new(rect.left(), y), Pos2::new(rect.right(), y)],
                Stroke::new(1.0, theme::BORDER),
            );
        }
        if self.candles.len() > 2 {
            let visible = &self.candles[self.candles.len().saturating_sub(80)..];
            let min = visible
                .iter()
                .map(|candle| candle.close)
                .fold(f64::INFINITY, f64::min);
            let max = visible
                .iter()
                .map(|candle| candle.close)
                .fold(f64::NEG_INFINITY, f64::max);
            let spread = (max - min).max(1.0);
            let points: Vec<Pos2> = visible
                .iter()
                .enumerate()
                .map(|(index, candle)| {
                    Pos2::new(
                        egui::lerp(
                            rect.left()..=rect.right(),
                            index as f32 / (visible.len() - 1) as f32,
                        ),
                        rect.bottom() - ((candle.close - min) / spread) as f32 * rect.height(),
                    )
                })
                .collect();
            painter.add(egui::Shape::line(points, Stroke::new(2.0, theme::YELLOW)));
        }
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
        }
        ctx.request_repaint_after(Duration::from_millis(200));
    }

    fn ui(&mut self, ui: &mut Ui, _frame: &mut eframe::Frame) {
        if self.unlocked_key.is_none() {
            self.render_auth(ui);
        } else {
            self.render_shell(ui);
        }
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
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
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

    let mut processed = Vec::new();
    let mut messages = Vec::new();
    let mut failures = Vec::new();
    for symbol in &cycle.symbols {
        let result = (|| -> anyhow::Result<String> {
            let candles = market::fetch_candles(&client, symbol, cycle.interval, 160)?;
            let snapshot = market::fetch_snapshot(&client, symbol)?;
            let input = AiTradingInput {
                symbol: symbol.clone(),
                timeframe: cycle.config.timeframe.clone(),
                candle_open_time: cycle.candle_open_time,
                candles,
                snapshot,
                account: account.clone(),
                current_position: position_for_symbol(&account, symbol),
                configured_leverage: cycle.config.leverage,
                configured_capital_usage_percent: cycle.config.capital_usage_percent,
            };
            let output = ai::decide_trade(
                cycle.provider,
                &cycle.model,
                &cycle.api_key,
                &cycle.relay_base_url,
                &input,
                &cycle.config,
            )?;
            let decision = ai_trader::validate_signal(
                &cycle.config,
                &input,
                output.signal,
                chrono::Utc::now().timestamp(),
            );
            audit.record_decision(
                cycle.provider.label(),
                &output.model,
                &input,
                &output.raw_output,
                &decision.signal,
                decision.approved,
                &decision.reason,
            )?;
            ai_trader::write_signal_atomically(&cycle.workspace.ai_signals, &decision.signal)?;
            processed.push((symbol.clone(), cycle.candle_open_time));
            Ok(format!(
                "{} {} {} ({:.0}%)",
                symbol,
                action_label(&decision.signal.action),
                if decision.approved {
                    "通过"
                } else {
                    "拦截/观望"
                },
                decision.signal.confidence * 100.0
            ))
        })();
        match result {
            Ok(message) => messages.push(message),
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
                let processed = cycle
                    .symbols
                    .iter()
                    .map(|symbol| (symbol.clone(), cycle.candle_open_time))
                    .collect();
                return Ok(AiDecisionSummary {
                    message: format!(
                        "AI 闭环已暂停：AI 输出不是可执行 JSON。{}。请检查中转站 Base URL 是否为 OpenAI-compatible /v1、模型名是否支持 chat/completions，或换支持 JSON 输出的模型。",
                        compact_error(&error.to_string(), 520)
                    ),
                    processed,
                });
            }
            Err(error) if is_ai_provider_capacity_error(&error) => {
                let processed = cycle
                    .symbols
                    .iter()
                    .map(|symbol| (symbol.clone(), cycle.candle_open_time))
                    .collect();
                return Ok(AiDecisionSummary {
                    message: format!(
                        "AI 闭环已暂停：中转站当前不可用。{}。这通常是模型在当前分组没有可用通道，或中转站服务器过载；请在中转站后台确认模型名/分组，换一个有通道的模型，或稍后重试。",
                        compact_error(&error.to_string(), 520)
                    ),
                    processed,
                });
            }
            Err(error) => failures.push(format!("{symbol}: {error}")),
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

fn draw_candles(ui: &mut Ui, candles: &[Candle]) {
    let desired = Vec2::new(
        ui.available_width(),
        (ui.available_height() - 8.0).max(380.0),
    );
    let (rect, response) = ui.allocate_exact_size(desired, Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, 2, theme::BG);
    let plot = Rect::from_min_max(
        rect.min + Vec2::new(8.0, 8.0),
        rect.max - Vec2::new(68.0, 28.0),
    );
    for index in 0..=5 {
        let y = egui::lerp(plot.top()..=plot.bottom(), index as f32 / 5.0);
        painter.line_segment(
            [Pos2::new(plot.left(), y), Pos2::new(plot.right(), y)],
            Stroke::new(1.0, theme::BORDER),
        );
    }
    for index in 0..=6 {
        let x = egui::lerp(plot.left()..=plot.right(), index as f32 / 6.0);
        painter.line_segment(
            [Pos2::new(x, plot.top()), Pos2::new(x, plot.bottom())],
            Stroke::new(1.0, theme::BORDER),
        );
    }
    if candles.is_empty() {
        painter.text(
            plot.center(),
            Align2::CENTER_CENTER,
            "正在加载 Binance Futures K线",
            FontId::new(14.0, FontFamily::Proportional),
            theme::MUTED,
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
