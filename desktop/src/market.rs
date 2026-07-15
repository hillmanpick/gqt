use std::{thread, time::Duration};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, Sender};
use reqwest::blocking::Client;
use serde_json::Value;

use crate::model::{Candle, Interval, MarketCommand, MarketEvent, MarketSnapshot, Sentiment};

const BINANCE_BASE: &str = "https://fapi.binance.com";

pub fn start_worker(commands: Receiver<MarketCommand>, events: Sender<MarketEvent>) {
    thread::spawn(move || {
        let client = match Client::builder()
            .timeout(Duration::from_secs(12))
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

fn fetch_candles(
    client: &Client,
    symbol: &str,
    interval: Interval,
    limit: usize,
) -> Result<Vec<Candle>> {
    validate_symbol(symbol)?;
    let rows: Vec<Vec<Value>> = client
        .get(format!("{BINANCE_BASE}/fapi/v1/klines"))
        .query(&[
            ("symbol", symbol),
            ("interval", interval.as_str()),
            ("limit", &limit.clamp(50, 1000).to_string()),
        ])
        .send()?
        .error_for_status()?
        .json()
        .context("Binance K线响应格式无效")?;
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

fn fetch_snapshot(client: &Client, symbol: &str) -> Result<MarketSnapshot> {
    validate_symbol(symbol)?;
    let ticker: Value = get_json(client, "/fapi/v1/ticker/24hr", symbol)?;
    let premium: Value = get_json(client, "/fapi/v1/premiumIndex", symbol)?;
    let open_interest: Value = get_json(client, "/fapi/v1/openInterest", symbol)?;
    let long_short: Vec<Value> = client
        .get(format!(
            "{BINANCE_BASE}/futures/data/globalLongShortAccountRatio"
        ))
        .query(&[("symbol", symbol), ("period", "5m"), ("limit", "1")])
        .send()?
        .error_for_status()?
        .json()?;
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
    Ok(client
        .get(format!("{BINANCE_BASE}{path}"))
        .query(&[("symbol", symbol)])
        .send()?
        .error_for_status()?
        .json()?)
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
