use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Overview,
    Account,
    Market,
    Strategy,
    Backtest,
    Data,
    Execution,
    Settings,
}

impl Page {
    pub const ALL: [Page; 8] = [
        Page::Overview,
        Page::Account,
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
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
    FourHours,
    OneDay,
}

impl Interval {
    pub const ALL: [Interval; 6] = [
        Interval::OneMinute,
        Interval::FiveMinutes,
        Interval::FifteenMinutes,
        Interval::OneHour,
        Interval::FourHours,
        Interval::OneDay,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AiTradingConfig {
    pub enabled: bool,
    pub dry_run_only: bool,
    pub symbol_whitelist: Vec<String>,
    pub timeframe: String,
    pub margin_mode: MarginMode,
    pub leverage: u8,
    pub max_stake_amount: f64,
    pub capital_usage_percent: f64,
    pub risk_reward_ratio: f64,
    pub allow_ai_risk_sizing: bool,
    pub minimum_confidence: f64,
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
            timeframe: "1h".into(),
            margin_mode: MarginMode::Cross,
            leverage: 2,
            max_stake_amount: 50.0,
            capital_usage_percent: 10.0,
            risk_reward_ratio: 2.0,
            allow_ai_risk_sizing: false,
            minimum_confidence: 0.75,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTradingInput {
    pub symbol: String,
    pub timeframe: String,
    pub candle_open_time: i64,
    pub candles: Vec<Candle>,
    pub snapshot: MarketSnapshot,
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
