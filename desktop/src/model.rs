use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Overview,
    Account,
    PositionHistory,
    Market,
    Strategy,
    Backtest,
    Data,
    Execution,
    Settings,
}

impl Page {
    pub const ALL: [Page; 9] = [
        Page::Overview,
        Page::Account,
        Page::PositionHistory,
        Page::Market,
        Page::Strategy,
        Page::Backtest,
        Page::Data,
        Page::Execution,
        Page::Settings,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Page::Overview => "总览",
            Page::Account => "账户",
            Page::PositionHistory => "仓位历史",
            Page::Market => "行情",
            Page::Strategy => "策略",
            Page::Backtest => "回测",
            Page::Data => "数据",
            Page::Execution => "执行",
            Page::Settings => "设置",
        }
    }

    pub fn icon(self) -> &'static str {
        match self {
            Page::Overview => "layout-dashboard",
            Page::Account => "wallet-cards",
            Page::PositionHistory => "history",
            Page::Market => "candlestick-chart",
            Page::Strategy => "braces",
            Page::Backtest => "chart-no-axes-combined",
            Page::Data => "database",
            Page::Execution => "activity",
            Page::Settings => "sliders-horizontal",
        }
    }

    pub fn context(self) -> &'static str {
        match self {
            Page::Overview => "Binance U 本位永续",
            Page::Account => "真实账户资金与合约持仓",
            Page::PositionHistory => "模拟盘成交与仓位记录",
            Page::Market => "实时 K 线与市场情绪",
            Page::Strategy => "Rust 客户端 / Freqtrade Interface v3",
            Page::Backtest => "历史验证与成本压力测试",
            Page::Data => "合约历史数据同步",
            Page::Execution => "自动策略与运行日志",
            Page::Settings => "风控与本地加密密钥",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Interval {
    OneSecond,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
}

impl Interval {
    pub const MARKET: [Interval; 6] = [
        Interval::OneSecond,
        Interval::OneMinute,
        Interval::FifteenMinutes,
        Interval::OneHour,
        Interval::FourHours,
        Interval::OneDay,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Interval::OneSecond => "秒",
            Interval::OneMinute => "分",
            Interval::FiveMinutes => "5分",
            Interval::FifteenMinutes => "15分",
            Interval::OneHour => "1小时",
            Interval::FourHours => "4小时",
            Interval::OneDay => "一天",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Interval::OneSecond => "1s",
            Interval::OneMinute => "1m",
            Interval::FiveMinutes => "5m",
            Interval::FifteenMinutes => "15m",
            Interval::OneHour => "1h",
            Interval::FourHours => "4h",
            Interval::OneDay => "1d",
        }
    }

    pub fn from_timeframe(value: &str) -> Option<Self> {
        match value {
            "1s" => Some(Interval::OneSecond),
            "1m" => Some(Interval::OneMinute),
            "5m" => Some(Interval::FiveMinutes),
            "15m" => Some(Interval::FifteenMinutes),
            "1h" => Some(Interval::OneHour),
            "4h" => Some(Interval::FourHours),
            "1d" => Some(Interval::OneDay),
            _ => None,
        }
    }

    pub fn seconds(self) -> i64 {
        match self {
            Interval::OneSecond => 1,
            Interval::OneMinute => 60,
            Interval::FiveMinutes => 5 * 60,
            Interval::FifteenMinutes => 15 * 60,
            Interval::OneHour => 60 * 60,
            Interval::FourHours => 4 * 60 * 60,
            Interval::OneDay => 24 * 60 * 60,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Candle {
    pub time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Sentiment {
    pub score: i32,
    pub label: String,
    pub trend: f64,
    pub positioning: f64,
    pub funding: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub symbol: String,
    pub price: f64,
    pub change_percent: f64,
    pub high: f64,
    pub low: f64,
    pub quote_volume: f64,
    pub funding_rate: f64,
    pub mark_price: f64,
    pub long_short_ratio: f64,
    pub open_interest: f64,
    pub sentiment: Sentiment,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FuturesAccount {
    pub wallet_balance: f64,
    pub available_balance: f64,
    pub margin_balance: f64,
    pub unrealized_profit: f64,
    pub initial_margin: f64,
    pub maintenance_margin: f64,
    pub positions: Vec<FuturesPosition>,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FuturesPosition {
    pub symbol: String,
    pub side: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub leverage: i64,
    pub unrealized_profit: f64,
    pub liquidation_price: f64,
    pub margin_type: String,
}

#[derive(Debug, Clone, Default)]
pub struct SimulationAccount {
    pub wallet_balance: f64,
    pub available_balance: f64,
    pub realized_profit: f64,
    pub open_stake: f64,
    pub closed_trades: i64,
    pub winning_trades: i64,
    pub open_trades: Vec<SimulationTrade>,
    pub trade_history: Vec<PositionHistory>,
}

#[derive(Debug, Clone, Default)]
pub struct SimulationTrade {
    pub pair: String,
    pub side: String,
    pub amount: f64,
    pub stake_amount: f64,
    pub open_rate: f64,
    pub leverage: f64,
    pub open_date: String,
    pub tag: String,
}

#[derive(Debug, Clone, Default)]
pub struct PositionHistory {
    pub pair: String,
    pub status: String,
    pub side: String,
    pub amount: f64,
    pub stake_amount: f64,
    pub open_rate: f64,
    pub close_rate: Option<f64>,
    pub leverage: f64,
    pub profit_abs: f64,
    pub profit_percent: f64,
    pub open_date: String,
    pub close_date: String,
    pub tag: String,
    pub exit_reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AiProvider {
    OpenAi,
    Claude,
    DeepSeek,
    Relay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarginMode {
    Cross,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyProfile {
    Conservative,
    Balanced,
    Aggressive,
}

impl Default for StrategyProfile {
    fn default() -> Self {
        Self::Balanced
    }
}

impl StrategyProfile {
    pub const ALL: [StrategyProfile; 3] = [
        StrategyProfile::Conservative,
        StrategyProfile::Balanced,
        StrategyProfile::Aggressive,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            StrategyProfile::Conservative => "conservative",
            StrategyProfile::Balanced => "balanced",
            StrategyProfile::Aggressive => "aggressive",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            StrategyProfile::Conservative => "保守",
            StrategyProfile::Balanced => "均衡",
            StrategyProfile::Aggressive => "激进",
        }
    }

    pub fn hint(self) -> &'static str {
        match self {
            StrategyProfile::Conservative => "少交易、严过滤、轻滚仓，优先控制回撤。",
            StrategyProfile::Balanced => "默认档，兼顾信号质量、复利速度和模拟盘验证。",
            StrategyProfile::Aggressive => "更频繁、更快止盈、更积极滚仓，只建议先跑模拟盘和回测。",
        }
    }

    pub fn preset(self) -> StrategyProfilePreset {
        match self {
            StrategyProfile::Conservative => StrategyProfilePreset {
                leverage: 2,
                capital_usage_percent: 8.0,
                risk_reward_ratio: 1.4,
                minimum_long_score: 0.68,
                minimum_short_score: 0.68,
                minimum_factor_score: 0.18,
                minimum_trend_quality: 0.50,
                minimum_adx: 14.0,
                minimum_volume_ratio: -0.10,
                take_profit: 0.014,
                stop_loss: 0.010,
                pyramid_profit: 0.008,
                pyramid_stake_ratio: 0.30,
            },
            StrategyProfile::Balanced => StrategyProfilePreset {
                leverage: 2,
                capital_usage_percent: 12.0,
                risk_reward_ratio: 1.4,
                minimum_long_score: 0.62,
                minimum_short_score: 0.62,
                minimum_factor_score: 0.12,
                minimum_trend_quality: 0.42,
                minimum_adx: 10.0,
                minimum_volume_ratio: -0.35,
                take_profit: 0.018,
                stop_loss: 0.014,
                pyramid_profit: 0.006,
                pyramid_stake_ratio: 0.45,
            },
            StrategyProfile::Aggressive => StrategyProfilePreset {
                leverage: 3,
                capital_usage_percent: 18.0,
                risk_reward_ratio: 1.0,
                minimum_long_score: 0.58,
                minimum_short_score: 0.58,
                minimum_factor_score: 0.08,
                minimum_trend_quality: 0.35,
                minimum_adx: 8.0,
                minimum_volume_ratio: -0.50,
                take_profit: 0.012,
                stop_loss: 0.012,
                pyramid_profit: 0.004,
                pyramid_stake_ratio: 0.60,
            },
        }
    }

    pub fn apply_to(self, config: &mut AiTradingConfig) {
        let preset = self.preset();
        config.strategy_profile = self;
        config.leverage = preset.leverage;
        config.capital_usage_percent = preset.capital_usage_percent;
        config.risk_reward_ratio = preset.risk_reward_ratio;
        config.minimum_long_score = preset.minimum_long_score;
        config.minimum_short_score = preset.minimum_short_score;
        config.minimum_factor_score = preset.minimum_factor_score;
        config.minimum_trend_quality = preset.minimum_trend_quality;
        config.minimum_adx = preset.minimum_adx;
        config.minimum_volume_ratio = preset.minimum_volume_ratio;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StrategyProfilePreset {
    pub leverage: u8,
    pub capital_usage_percent: f64,
    pub risk_reward_ratio: f64,
    pub minimum_long_score: f64,
    pub minimum_short_score: f64,
    pub minimum_factor_score: f64,
    pub minimum_trend_quality: f64,
    pub minimum_adx: f64,
    pub minimum_volume_ratio: f64,
    pub take_profit: f64,
    pub stop_loss: f64,
    pub pyramid_profit: f64,
    pub pyramid_stake_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiTradingConfig {
    pub enabled: bool,
    pub dry_run_only: bool,
    pub symbol_whitelist: Vec<String>,
    pub strategy_profile: StrategyProfile,
    pub timeframe: String,
    pub margin_mode: MarginMode,
    pub leverage: u8,
    pub max_stake_amount: f64,
    pub capital_usage_percent: f64,
    pub risk_reward_ratio: f64,
    pub allow_ai_risk_sizing: bool,
    pub minimum_confidence: f64,
    pub minimum_long_score: f64,
    pub minimum_short_score: f64,
    pub minimum_factor_score: f64,
    pub minimum_trend_quality: f64,
    pub minimum_adx: f64,
    pub minimum_volume_ratio: f64,
    pub model_timeout_seconds: u64,
    pub market_max_age_seconds: i64,
    pub one_signal_per_candle: bool,
}

impl Default for AiTradingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run_only: true,
            symbol_whitelist: default_ai_symbol_whitelist(),
            strategy_profile: StrategyProfile::Balanced,
            timeframe: "15m".into(),
            margin_mode: MarginMode::Isolated,
            leverage: 2,
            max_stake_amount: 120.0,
            capital_usage_percent: 12.0,
            risk_reward_ratio: 1.4,
            allow_ai_risk_sizing: false,
            minimum_confidence: 0.75,
            minimum_long_score: 0.62,
            minimum_short_score: 0.62,
            minimum_factor_score: 0.12,
            minimum_trend_quality: 0.42,
            minimum_adx: 10.0,
            minimum_volume_ratio: -0.35,
            model_timeout_seconds: 30,
            market_max_age_seconds: 90,
            one_signal_per_candle: true,
        }
    }
}

pub fn default_ai_symbol_whitelist() -> Vec<String> {
    [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "LINKUSDT",
        "AVAXUSDT", "LTCUSDT",
    ]
    .iter()
    .map(|symbol| symbol.to_string())
    .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AiAction {
    Long,
    Short,
    Close,
    Hold,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTradeSignal {
    pub decision_id: String,
    pub symbol: String,
    pub timeframe: String,
    pub candle_open_time: i64,
    pub valid_until: i64,
    pub action: AiAction,
    pub confidence: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stake_amount: Option<f64>,
    pub stop_loss_percent: f64,
    pub take_profit_percent: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FactorSnapshot {
    pub score: f64,
    pub bias: String,
    pub long_score: f64,
    pub short_score: f64,
    pub trend_quality: f64,
    pub momentum_short: f64,
    pub momentum_medium: f64,
    pub trend: f64,
    pub adx: f64,
    pub rsi: f64,
    pub macd_histogram: f64,
    pub breakout_position: f64,
    pub realized_volatility: f64,
    pub atr_percent: f64,
    pub volume_ratio: f64,
    pub volume_confirmation: f64,
    pub close_location: f64,
    pub ema_fast: f64,
    pub ema_mid: f64,
    pub ema_slow: f64,
    pub data_points: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTradingInput {
    pub symbol: String,
    pub timeframe: String,
    pub candle_open_time: i64,
    pub candles: Vec<Candle>,
    pub snapshot: MarketSnapshot,
    pub factor: FactorSnapshot,
    pub account: FuturesAccount,
    pub current_position: Option<FuturesPosition>,
    pub configured_leverage: u8,
    pub configured_capital_usage_percent: f64,
}

impl AiProvider {
    pub const ALL: [AiProvider; 4] = [
        AiProvider::OpenAi,
        AiProvider::Claude,
        AiProvider::DeepSeek,
        AiProvider::Relay,
    ];

    pub fn label(self) -> &'static str {
        match self {
            AiProvider::OpenAi => "OpenAI",
            AiProvider::Claude => "Claude",
            AiProvider::DeepSeek => "DeepSeek",
            AiProvider::Relay => "中转站",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SecretStatus {
    pub binance: bool,
    pub openai: bool,
    pub claude: bool,
    pub deepseek: bool,
    pub relay: bool,
}

#[derive(Debug, Clone, Default)]
pub struct CredentialDraft {
    pub binance_key: String,
    pub binance_secret: String,
    pub openai_key: String,
    pub claude_key: String,
    pub deepseek_key: String,
    pub relay_key: String,
}

#[derive(Debug)]
pub enum MarketCommand {
    Select { symbol: String, interval: Interval },
    Stop,
}

#[derive(Debug)]
pub enum MarketEvent {
    Candles(Vec<Candle>),
    Snapshot(MarketSnapshot),
    Connection(bool),
    Error(String),
}
