use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    path::PathBuf,
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, unbounded};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    network,
    sentiment::{
        SentimentAggregate, SentimentEvent, SentimentFetchConfig, SentimentStore,
        aggregate_sentiment, normalize_ticker,
    },
};

const BINANCE_BASE: &str = "https://fapi.binance.com";
const POLL_INTERVAL: Duration = Duration::from_secs(15);
const MAJOR_SYMBOLS: &[&str] = &[
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "TRXUSDT",
    "AVAXUSDT", "LINKUSDT", "DOTUSDT", "LTCUSDT", "BCHUSDT", "TONUSDT", "NEARUSDT", "UNIUSDT",
    "ATOMUSDT", "ETCUSDT", "FILUSDT", "APTUSDT", "SUIUSDT",
];

#[derive(Debug)]
pub enum ScannerCommand {
    Stop,
}

#[derive(Debug)]
pub enum ScannerEvent {
    Snapshot(UniverseSnapshot),
    Connection(bool),
    Error(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseSnapshot {
    pub updated_at: i64,
    pub total_symbols: usize,
    pub candidates: Vec<MarketCandidate>,
    pub recommendations: Vec<Recommendation>,
    pub data_quality: f64,
    pub sentiment_configured: bool,
    pub sentiment_events: usize,
    pub sentiment_updated_at: i64,
    pub sentiment_error: String,
    pub headlines: Vec<SentimentHeadline>,
    #[serde(default)]
    pub gainers: Vec<MarketCandidate>,
    #[serde(default)]
    pub losers: Vec<MarketCandidate>,
    #[serde(default)]
    pub hot: Vec<MarketCandidate>,
}

impl Default for UniverseSnapshot {
    fn default() -> Self {
        Self {
            updated_at: 0,
            total_symbols: 0,
            candidates: Vec::new(),
            recommendations: Vec::new(),
            data_quality: 0.0,
            sentiment_configured: false,
            sentiment_events: 0,
            sentiment_updated_at: 0,
            sentiment_error: String::new(),
            headlines: Vec::new(),
            gainers: Vec::new(),
            losers: Vec::new(),
            hot: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SentimentHeadline {
    pub symbol: String,
    pub title: String,
    pub source: String,
    pub event_type: String,
    pub sentiment: f64,
    pub published_at: i64,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCandidate {
    pub symbol: String,
    pub category: String,
    pub price: f64,
    pub change_percent: f64,
    pub quote_volume: f64,
    pub funding_rate: f64,
    pub open_interest: f64,
    pub volatility_percent: f64,
    pub market_score: f64,
    pub sentiment_score: f64,
    pub sentiment_quality: f64,
    pub sentiment_label: String,
    pub sentiment_event_count: usize,
    pub sentiment_source_count: usize,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    pub symbol: String,
    pub side: String,
    pub score: f64,
    pub confidence: f64,
    pub category: String,
    pub leverage_cap: u8,
    pub suggested_leverage: u8,
    pub trigger: String,
    pub stop_loss_percent: f64,
    pub take_profit_percent: f64,
    pub valid_until: i64,
    pub status: String,
    pub reason: String,
    pub sentiment_score: f64,
    pub sentiment_quality: f64,
    pub sentiment_event_count: usize,
    pub sentiment_source_count: usize,
}

#[derive(Debug, Deserialize)]
struct Ticker {
    symbol: String,
    #[serde(rename = "lastPrice")]
    last_price: String,
    #[serde(rename = "priceChangePercent")]
    change_percent: String,
    #[serde(rename = "quoteVolume")]
    quote_volume: String,
}

#[derive(Debug, Deserialize)]
struct Premium {
    symbol: String,
    #[serde(rename = "lastFundingRate")]
    funding_rate: String,
}

#[derive(Debug, Deserialize)]
struct ExchangeInfo {
    symbols: Vec<ExchangeSymbol>,
}

#[derive(Debug, Deserialize)]
struct ExchangeSymbol {
    symbol: String,
    status: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "contractType")]
    contract_type: String,
}

#[derive(Debug, Deserialize)]
struct OpenInterest {
    #[serde(rename = "openInterest")]
    open_interest: String,
}

pub fn start_worker(data_root: PathBuf) -> (Sender<ScannerCommand>, Receiver<ScannerEvent>) {
    let (commands, command_receiver) = unbounded();
    let (events, event_receiver) = unbounded();
    thread::spawn(move || run_worker(command_receiver, events, data_root));
    (commands, event_receiver)
}

fn run_worker(
    commands: Receiver<ScannerCommand>,
    events: Sender<ScannerEvent>,
    data_root: PathBuf,
) {
    let client = match network::client(Duration::from_secs(12)) {
        Ok(client) => client,
        Err(error) => {
            let _ = events.send(ScannerEvent::Error(error.to_string()));
            return;
        }
    };
    let mut known_symbols = HashSet::new();
    let mut last_universe_refresh = 0_i64;
    let sentiment_config = SentimentFetchConfig::from_env();
    let sentiment_store = SentimentStore::open(&data_root.join("sentiment.sqlite")).ok();
    let mut sentiment_events: Vec<SentimentEvent> = sentiment_store
        .as_ref()
        .and_then(|store| {
            store
                .load_events(None, Utc::now().timestamp() - 3 * 24 * 60 * 60, 10_000)
                .ok()
        })
        .unwrap_or_default();
    sentiment_events = crate::sentiment::deduplicate_events(&sentiment_events);
    let mut last_sentiment_fetch = 0_i64;
    loop {
        if let Ok(ScannerCommand::Stop) = commands.try_recv() {
            return;
        }
        let now = Utc::now().timestamp();
        match scan_once(
            &client,
            &mut known_symbols,
            &mut last_universe_refresh,
            &sentiment_config,
            sentiment_store.as_ref(),
            &mut sentiment_events,
            &mut last_sentiment_fetch,
        ) {
            Ok(snapshot) => {
                let _ = persist_snapshot(&data_root, &snapshot);
                let _ = events.send(ScannerEvent::Snapshot(snapshot));
                let _ = events.send(ScannerEvent::Connection(true));
            }
            Err(error) => {
                let _ = events.send(ScannerEvent::Connection(false));
                let _ = events.send(ScannerEvent::Error(error.to_string()));
            }
        }
        let sleep_until = now + POLL_INTERVAL.as_secs() as i64;
        while Utc::now().timestamp() < sleep_until {
            match commands.recv_timeout(Duration::from_secs(1)) {
                Ok(ScannerCommand::Stop) => return,
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
    }
}

fn persist_snapshot(data_root: &PathBuf, snapshot: &UniverseSnapshot) -> Result<()> {
    std::fs::create_dir_all(data_root).context("无法创建扫描数据目录")?;
    let target = data_root.join("realtime_scanner.json");
    let temporary = data_root.join("realtime_scanner.json.tmp");
    std::fs::write(&temporary, serde_json::to_vec_pretty(snapshot)?)?;
    if target.exists() {
        std::fs::remove_file(&target)?;
    }
    std::fs::rename(temporary, target)?;
    Ok(())
}

fn scan_once(
    client: &Client,
    known_symbols: &mut HashSet<String>,
    last_universe_refresh: &mut i64,
    sentiment_config: &SentimentFetchConfig,
    sentiment_store: Option<&SentimentStore>,
    sentiment_events: &mut Vec<SentimentEvent>,
    last_sentiment_fetch: &mut i64,
) -> Result<UniverseSnapshot> {
    let now = Utc::now().timestamp();
    let mut sentiment_error = String::new();
    if sentiment_config.enabled() && now - *last_sentiment_fetch >= 60 {
        match crate::sentiment::fetch_remote_events(client, sentiment_config) {
            Ok(events) => {
                if let Some(store) = sentiment_store {
                    let _ = store.upsert_events(&events);
                }
                sentiment_events.extend(events);
                let deduplicated = crate::sentiment::deduplicate_events(sentiment_events);
                *sentiment_events = deduplicated;
                sentiment_events.retain(|event| event.published_at >= now - 3 * 24 * 60 * 60);
                if sentiment_events.len() > 10_000 {
                    sentiment_events.sort_by_key(|event| std::cmp::Reverse(event.published_at));
                    sentiment_events.truncate(10_000);
                }
            }
            Err(error) => sentiment_error = error.to_string(),
        }
        *last_sentiment_fetch = now;
    }
    if now - *last_universe_refresh > 600 || known_symbols.is_empty() {
        *known_symbols = fetch_universe(client)?;
        *last_universe_refresh = now;
    }
    let tickers: Vec<Ticker> = get_json(client, "/fapi/v1/ticker/24hr")?;
    let premiums: Vec<Premium> = get_json(client, "/fapi/v1/premiumIndex")?;
    let funding: HashMap<String, f64> = premiums
        .into_iter()
        .filter_map(|item| {
            item.funding_rate
                .parse::<f64>()
                .ok()
                .map(|value| (item.symbol, value))
        })
        .collect();
    let mut candidates = tickers
        .into_iter()
        .filter(|item| known_symbols.contains(&item.symbol))
        .filter_map(|item| {
            let price = item.last_price.parse::<f64>().ok()?;
            let change_percent = item.change_percent.parse::<f64>().ok()?;
            let quote_volume = item.quote_volume.parse::<f64>().ok()?;
            let funding_rate = funding.get(&item.symbol).copied().unwrap_or(0.0);
            let category = category_for(&item.symbol, quote_volume);
            let market_score = market_score(change_percent, quote_volume, funding_rate);
            let aggregate = sentiment_for(&item.symbol, sentiment_events, now);
            Some(MarketCandidate {
                symbol: item.symbol,
                category,
                price,
                change_percent,
                quote_volume,
                funding_rate,
                open_interest: 0.0,
                volatility_percent: change_percent.abs().min(25.0),
                market_score,
                sentiment_score: aggregate.score,
                sentiment_quality: aggregate.quality,
                sentiment_label: aggregate.label,
                sentiment_event_count: aggregate.event_count,
                sentiment_source_count: aggregate.unique_sources,
                updated_at: now,
            })
        })
        .collect::<Vec<_>>();
    let mut gainers = candidates.clone();
    gainers.sort_by(|left, right| {
        right
            .change_percent
            .partial_cmp(&left.change_percent)
            .unwrap_or(Ordering::Equal)
    });
    gainers.truncate(10);

    let mut losers = candidates.clone();
    losers.sort_by(|left, right| {
        left.change_percent
            .partial_cmp(&right.change_percent)
            .unwrap_or(Ordering::Equal)
    });
    losers.truncate(10);

    let mut hot = candidates.clone();
    hot.sort_by(|left, right| {
        let left_hot =
            left.quote_volume.max(1.0).log10() * 0.55 + left.change_percent.abs().min(50.0) * 0.45;
        let right_hot = right.quote_volume.max(1.0).log10() * 0.55
            + right.change_percent.abs().min(50.0) * 0.45;
        right_hot.partial_cmp(&left_hot).unwrap_or(Ordering::Equal)
    });
    hot.truncate(10);

    candidates.sort_by(|left, right| {
        right
            .market_score
            .partial_cmp(&left.market_score)
            .unwrap_or(Ordering::Equal)
    });

    // Keep the full universe in the snapshot. Enrich only the most liquid rows
    // to stay within Binance request weight while still exposing every ticker.
    for candidate in candidates.iter_mut().take(12) {
        if let Ok(open_interest) = fetch_open_interest(client, &candidate.symbol) {
            candidate.open_interest = open_interest;
        }
    }

    let recommendations = candidates
        .iter()
        .filter_map(recommendation_for)
        .take(5)
        .collect();
    let mut headlines = sentiment_events
        .iter()
        .filter(|event| {
            event.published_at > 0 && (!event.title.is_empty() || !event.text.is_empty())
        })
        .map(|event| SentimentHeadline {
            symbol: event
                .symbols
                .first()
                .cloned()
                .or_else(|| {
                    if event.symbol.is_empty() {
                        None
                    } else {
                        Some(event.symbol.clone())
                    }
                })
                .unwrap_or_default(),
            title: if event.title.is_empty() {
                event.text.chars().take(140).collect()
            } else {
                event.title.chars().take(140).collect()
            },
            source: if event.provider.is_empty() {
                event.source_kind.clone()
            } else {
                event.provider.clone()
            },
            event_type: if event.event_type.is_empty() {
                "commentary".into()
            } else {
                event.event_type.clone()
            },
            sentiment: event.sentiment,
            published_at: event.published_at,
            url: event.url.clone(),
        })
        .collect::<Vec<_>>();
    headlines.sort_by_key(|item| std::cmp::Reverse(item.published_at));
    headlines.truncate(8);
    Ok(UniverseSnapshot {
        updated_at: now,
        total_symbols: known_symbols.len(),
        candidates,
        recommendations,
        data_quality: if sentiment_config.enabled() {
            1.0
        } else {
            0.75
        },
        sentiment_configured: sentiment_config.enabled(),
        sentiment_events: sentiment_events.len(),
        sentiment_updated_at: *last_sentiment_fetch,
        sentiment_error,
        headlines,
        gainers,
        losers,
        hot,
    })
}

fn sentiment_for(symbol: &str, events: &[SentimentEvent], now: i64) -> SentimentAggregate {
    let ticker = normalize_ticker(symbol);
    let aliases = match ticker.as_str() {
        "BTC" => vec!["BTC".into(), "BITCOIN".into()],
        "ETH" => vec!["ETH".into(), "ETHEREUM".into()],
        "BNB" => vec!["BNB".into(), "BINANCE COIN".into()],
        "SOL" => vec!["SOL".into(), "SOLANA".into()],
        "XRP" => vec!["XRP".into(), "RIPPLE".into()],
        "DOGE" => vec!["DOGE".into(), "DOGECOIN".into()],
        "ADA" => vec!["ADA".into(), "CARDANO".into()],
        "AVAX" => vec!["AVAX".into(), "AVALANCHE".into()],
        "LINK" => vec!["LINK".into(), "CHAINLINK".into()],
        _ => vec![ticker.clone()],
    };
    aggregate_sentiment(
        symbol,
        events,
        &aliases,
        now,
        &crate::sentiment::SentimentConfig::default(),
    )
}

fn fetch_universe(client: &Client) -> Result<HashSet<String>> {
    let info: ExchangeInfo = get_json(client, "/fapi/v1/exchangeInfo")?;
    let symbols = info
        .symbols
        .into_iter()
        .filter(|item| {
            item.status == "TRADING"
                && item.quote_asset == "USDT"
                && item.contract_type == "PERPETUAL"
                && item.symbol.ends_with("USDT")
        })
        .map(|item| item.symbol)
        .collect();
    Ok(symbols)
}

fn fetch_open_interest(client: &Client, symbol: &str) -> Result<f64> {
    let item: OpenInterest =
        get_json_with_query(client, "/fapi/v1/openInterest", [("symbol", symbol)])?;
    item.open_interest.parse().context("持仓量字段无效")
}

fn get_json<T: serde::de::DeserializeOwned>(client: &Client, path: &str) -> Result<T> {
    read_response(client.get(format!("{BINANCE_BASE}{path}")).send()?, path)
}

fn get_json_with_query<T: serde::de::DeserializeOwned, const N: usize>(
    client: &Client,
    path: &str,
    query: [(&str, &str); N],
) -> Result<T> {
    read_response(
        client
            .get(format!("{BINANCE_BASE}{path}"))
            .query(&query[..])
            .send()?,
        path,
    )
}

fn read_response<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
    path: &str,
) -> Result<T> {
    let status = response.status();
    let body = response.text().context("无法读取 Binance 扫描响应")?;
    if !status.is_success() {
        let detail = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| value.get("msg").and_then(Value::as_str).map(str::to_string))
            .unwrap_or(body);
        bail!("Binance 扫描 {path} 失败: HTTP {status} {detail}");
    }
    serde_json::from_str(&body).context("Binance 扫描响应不是有效 JSON")
}

fn category_for(symbol: &str, quote_volume: f64) -> String {
    if MAJOR_SYMBOLS.contains(&symbol) || quote_volume >= 1_000_000_000.0 {
        "主流".into()
    } else {
        "山寨".into()
    }
}

fn market_score(change_percent: f64, quote_volume: f64, funding_rate: f64) -> f64 {
    let momentum = (change_percent.abs() / 8.0).clamp(0.0, 1.0) * 45.0;
    let liquidity = (quote_volume.max(1.0).log10() / 10.0).clamp(0.0, 1.0) * 35.0;
    let funding_penalty = (funding_rate.abs() * 100_000.0).clamp(0.0, 1.0) * 20.0;
    (momentum + liquidity - funding_penalty).clamp(0.0, 100.0)
}

fn recommendation_for(candidate: &MarketCandidate) -> Option<Recommendation> {
    if candidate.quote_volume < 20_000_000.0 || candidate.change_percent.abs() < 0.35 {
        return None;
    }
    if candidate.sentiment_event_count == 0 || candidate.sentiment_quality < 0.2 {
        return None;
    }
    let side = if candidate.change_percent > 0.0 {
        "LONG"
    } else {
        "SHORT"
    };
    let leverage_cap = if candidate.category == "主流" {
        50
    } else {
        5
    };
    let volatility = candidate.volatility_percent.max(0.6);
    let suggested_leverage = if candidate.category == "主流" {
        (30.0 / volatility).round().clamp(5.0, 50.0) as u8
    } else {
        (3.0 / volatility).round().clamp(1.0, 5.0) as u8
    };
    let aligned_sentiment = if side == "LONG" {
        candidate.sentiment_score
    } else {
        -candidate.sentiment_score
    };
    if aligned_sentiment < -0.20 && candidate.sentiment_quality >= 0.35 {
        return None;
    }
    let sentiment_component = ((aligned_sentiment + 1.0) * 50.0).clamp(0.0, 100.0);
    let score = (candidate.market_score * 0.70 + sentiment_component * 0.30).clamp(0.0, 100.0);
    let confidence = ((candidate.market_score / 100.0) * 0.70
        + candidate.sentiment_quality * aligned_sentiment.abs() * 0.30)
        .clamp(0.0, 0.95);
    let status = if confidence >= 0.62 {
        "观察候选"
    } else {
        "信号不足"
    };
    Some(Recommendation {
        symbol: candidate.symbol.clone(),
        side: side.into(),
        score,
        confidence,
        category: candidate.category.clone(),
        leverage_cap,
        suggested_leverage,
        trigger: format!("突破当前价 {:.4}% 后确认", volatility.min(2.0)),
        stop_loss_percent: (volatility * 0.8).clamp(0.4, 4.0),
        take_profit_percent: (volatility * 1.4).clamp(0.8, 8.0),
        valid_until: Utc::now().timestamp() + 15 * 60,
        status: status.into(),
        reason: format!(
            "{}动量 {:.2}%，舆情 {} ({:.2}, {:.0}%质量, {}个来源)，24h 成交额 {}，资金费率 {:.4}%；等待价格和成交量确认",
            if candidate.change_percent > 0.0 {
                "上涨"
            } else {
                "下跌"
            },
            candidate.change_percent,
            candidate.sentiment_label,
            candidate.sentiment_score,
            candidate.sentiment_quality * 100.0,
            candidate.sentiment_source_count,
            compact_volume(candidate.quote_volume),
            candidate.funding_rate * 100.0
        ),
        sentiment_score: candidate.sentiment_score,
        sentiment_quality: candidate.sentiment_quality,
        sentiment_event_count: candidate.sentiment_event_count,
        sentiment_source_count: candidate.sentiment_source_count,
    })
}

fn compact_volume(value: f64) -> String {
    if value >= 1_000_000_000.0 {
        format!("{:.1}B", value / 1_000_000_000.0)
    } else if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else {
        format!("{:.0}", value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn major_coins_have_higher_leverage_cap() {
        assert_eq!(category_for("BTCUSDT", 1.0), "主流");
        assert_eq!(category_for("ABCUSDT", 1.0), "山寨");
        assert_eq!(
            recommendation_for(&MarketCandidate {
                symbol: "ABCUSDT".into(),
                category: "山寨".into(),
                price: 1.0,
                change_percent: 3.0,
                quote_volume: 50_000_000.0,
                funding_rate: 0.0,
                open_interest: 0.0,
                volatility_percent: 1.0,
                market_score: 80.0,
                sentiment_score: 0.6,
                sentiment_quality: 0.8,
                sentiment_label: "bullish".into(),
                sentiment_event_count: 2,
                sentiment_source_count: 2,
                updated_at: 1,
            })
            .unwrap()
            .leverage_cap,
            5
        );
    }

    #[test]
    fn quiet_markets_are_not_recommended() {
        assert!(
            recommendation_for(&MarketCandidate {
                symbol: "BTCUSDT".into(),
                category: "主流".into(),
                price: 1.0,
                change_percent: 0.1,
                quote_volume: 2_000_000_000.0,
                funding_rate: 0.0,
                open_interest: 0.0,
                volatility_percent: 0.1,
                market_score: 20.0,
                sentiment_score: 0.0,
                sentiment_quality: 0.0,
                sentiment_label: "unconfirmed".into(),
                sentiment_event_count: 0,
                sentiment_source_count: 0,
                updated_at: 1,
            })
            .is_none()
        );
    }
}
