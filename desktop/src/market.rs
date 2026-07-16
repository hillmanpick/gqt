use std::{thread, time::Duration};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, Sender};
use reqwest::blocking::{Client, Response};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    model::{Candle, Interval, MarketCommand, MarketEvent, MarketSnapshot, Sentiment},
    network,
};

const BINANCE_BASE: &str = "https://fapi.binance.com";
const RESTRICTED_LOCATION_HINT: &str = "Binance Futures 拒绝了当前网络位置（HTTP 451 restricted location）。实时模拟盘和实盘都需要能访问 Binance Futures 的合规网络；当前只能使用离线回测或已有本地数据测试。";

pub fn start_worker(commands: Receiver<MarketCommand>, events: Sender<MarketEvent>) {
    thread::spawn(move || {
        let client = match network::client_builder(Duration::from_secs(12))
            .user_agent("GQT-Trader/0.2")
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                let _ = events.send(MarketEvent::Error(error.to_string()));
                return;
            }
        };
        let mut symbol = "BTCUSDT".to_string();
        let mut interval = Interval::FourHours;
        let mut refresh_candles = true;
        let mut ticks = 0_u32;

        loop {
            while let Ok(command) = commands.try_recv() {
                match command {
                    MarketCommand::Select {
                        symbol: next,
                        interval: next_interval,
                    } => {
                        symbol = next;
                        interval = next_interval;
                        refresh_candles = true;
                        ticks = 0;
                    }
                    MarketCommand::Stop => return,
                }
            }

            if refresh_candles || ticks.is_multiple_of(8) {
                match fetch_candles(&client, &symbol, interval, 300) {
                    Ok(candles) => {
                        let _ = events.send(MarketEvent::Candles(candles));
                        let _ = events.send(MarketEvent::Connection(true));
                    }
                    Err(error) => {
                        let _ = events.send(MarketEvent::Connection(false));
                        let _ = events.send(MarketEvent::Error(error.to_string()));
                    }
                }
                refresh_candles = false;
            }
            match fetch_snapshot(&client, &symbol) {
                Ok(snapshot) => {
                    let _ = events.send(MarketEvent::Snapshot(snapshot));
                    let _ = events.send(MarketEvent::Connection(true));
                }
                Err(error) => {
                    let _ = events.send(MarketEvent::Connection(false));
                    let _ = events.send(MarketEvent::Error(error.to_string()));
                }
            }
            ticks = ticks.wrapping_add(1);

            match commands.recv_timeout(Duration::from_secs(2)) {
                Ok(MarketCommand::Select {
                    symbol: next,
                    interval: next_interval,
                }) => {
                    symbol = next;
                    interval = next_interval;
                    refresh_candles = true;
                    ticks = 0;
                }
                Ok(MarketCommand::Stop) => return,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            }
        }
    });
}

pub fn fetch_candles(
    client: &Client,
    symbol: &str,
    interval: Interval,
    limit: usize,
) -> Result<Vec<Candle>> {
    validate_symbol(symbol)?;
    let rows: Vec<Vec<Value>> = read_binance_json(
        client
            .get(format!("{BINANCE_BASE}/fapi/v1/klines"))
            .query(&[
                ("symbol", symbol),
                ("interval", interval.as_str()),
                ("limit", &limit.clamp(50, 1000).to_string()),
            ])
            .send()
            .context("无法连接 Binance Futures K线接口")?,
        "Binance K线请求失败",
    )?;
    rows.into_iter()
        .map(|row| {
            if row.len() < 6 {
                bail!("Binance K线字段不完整");
            }
            Ok(Candle {
                time: row[0].as_i64().unwrap_or_default() / 1000,
                open: parse_number(&row[1])?,
                high: parse_number(&row[2])?,
                low: parse_number(&row[3])?,
                close: parse_number(&row[4])?,
                volume: parse_number(&row[5])?,
            })
        })
        .collect()
}

pub fn fetch_snapshot(client: &Client, symbol: &str) -> Result<MarketSnapshot> {
    validate_symbol(symbol)?;
    let ticker: Value = get_json(client, "/fapi/v1/ticker/24hr", symbol)?;
    let premium: Value = get_json(client, "/fapi/v1/premiumIndex", symbol)?;
    let open_interest: Value = get_json(client, "/fapi/v1/openInterest", symbol)?;
    let long_short: Vec<Value> = read_binance_json(
        client
            .get(format!(
                "{BINANCE_BASE}/futures/data/globalLongShortAccountRatio"
            ))
            .query(&[("symbol", symbol), ("period", "5m"), ("limit", "1")])
            .send()
            .context("无法连接 Binance Futures 多空比接口")?,
        "Binance 多空比请求失败",
    )?;
    let change_percent = field_number(&ticker, "priceChangePercent")?;
    let funding_rate = field_number(&premium, "lastFundingRate")?;
    let long_short_ratio = long_short
        .first()
        .and_then(|item| item.get("longShortRatio"))
        .map(parse_number)
        .transpose()?
        .unwrap_or(1.0);

    Ok(MarketSnapshot {
        symbol: symbol.to_string(),
        price: field_number(&ticker, "lastPrice")?,
        change_percent,
        high: field_number(&ticker, "highPrice")?,
        low: field_number(&ticker, "lowPrice")?,
        quote_volume: field_number(&ticker, "quoteVolume")?,
        funding_rate,
        mark_price: field_number(&premium, "markPrice")?,
        long_short_ratio,
        open_interest: field_number(&open_interest, "openInterest")?,
        sentiment: calculate_sentiment(change_percent, funding_rate, long_short_ratio),
        updated_at: chrono::Utc::now().timestamp(),
    })
}

fn get_json(client: &Client, path: &str, symbol: &str) -> Result<Value> {
    read_binance_json(
        client
            .get(format!("{BINANCE_BASE}{path}"))
            .query(&[("symbol", symbol)])
            .send()
            .context("无法连接 Binance Futures 行情接口")?,
        "Binance 行情请求失败",
    )
}

fn read_binance_json<T: DeserializeOwned>(response: Response, context: &str) -> Result<T> {
    let status = response.status();
    let body = response.text().context("无法读取 Binance 返回")?;
    if status.is_success() {
        return serde_json::from_str(&body).context("Binance 返回不是有效 JSON");
    }

    let message = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("msg")
                .and_then(|message| message.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| body.trim().to_string());
    if status.as_u16() == 451 || message.to_ascii_lowercase().contains("restricted location") {
        bail!("{context}: {RESTRICTED_LOCATION_HINT}");
    }
    bail!("{context}: HTTP {status} {message}");
}

fn parse_number(value: &Value) -> Result<f64> {
    value
        .as_str()
        .and_then(|value| value.parse().ok())
        .or_else(|| value.as_f64())
        .ok_or_else(|| anyhow::anyhow!("Binance 数值字段无效"))
}

fn field_number(value: &Value, name: &str) -> Result<f64> {
    value
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("Binance 缺少字段 {name}"))
        .and_then(parse_number)
}

fn validate_symbol(symbol: &str) -> Result<()> {
    if !(5..=20).contains(&symbol.len())
        || !symbol
            .chars()
            .all(|character| character.is_ascii_uppercase() || character.is_ascii_digit())
    {
        bail!("合约代码无效");
    }
    Ok(())
}

pub fn calculate_sentiment(
    change_percent: f64,
    funding_rate: f64,
    long_short_ratio: f64,
) -> Sentiment {
    let trend = change_percent.clamp(-12.0, 12.0) * 1.8;
    let positioning = ((long_short_ratio - 1.0) * 22.0).clamp(-14.0, 14.0);
    let funding = (funding_rate * 100_000.0).clamp(-12.0, 12.0);
    let score = (50.0 + trend + positioning + funding)
        .clamp(0.0, 100.0)
        .round() as i32;
    let label = match score {
        0..=19 => "极度恐慌",
        20..=39 => "恐慌",
        40..=59 => "中性",
        60..=79 => "贪婪",
        _ => "极度贪婪",
    };
    Sentiment {
        score,
        label: label.to_string(),
        trend: (trend * 10.0).round() / 10.0,
        positioning: (positioning * 10.0).round() / 10.0,
        funding: (funding * 10.0).round() / 10.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn neutral_sentiment_is_fifty() {
        let sentiment = calculate_sentiment(0.0, 0.0, 1.0);
        assert_eq!(sentiment.score, 50);
        assert_eq!(sentiment.label, "中性");
    }
}
