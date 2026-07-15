use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Page {
    Overview,
    Market,
    Strategy,
    Backtest,
    Data,
    Execution,
    Settings,
}

impl Page {
    pub const ALL: [Page; 7] = [
        Page::Overview,
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

#[derive(Debug, Clone, Default)]
pub struct Sentiment {
    pub score: i32,
    pub label: String,
    pub trend: f64,
    pub positioning: f64,
    pub funding: f64,
}

#[derive(Debug, Clone, Default)]
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AiProvider {
    OpenAi,
    Claude,
    DeepSeek,
    Relay,
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
