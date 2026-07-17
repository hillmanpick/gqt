use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::model::{
    AiAction, AiTradeSignal, AiTradingConfig, AiTradingInput, Candle, FactorSnapshot,
};

#[derive(Debug, Clone)]
pub struct RiskDecision {
    pub approved: bool,
    pub reason: String,
    pub signal: AiTradeSignal,
}

pub fn calculate_factor_snapshot(candles: &[Candle]) -> FactorSnapshot {
    if candles.len() < 120 {
        return FactorSnapshot {
            score: 0.0,
            bias: "insufficient_data".into(),
            data_points: candles.len(),
            ..Default::default()
        };
    }

    let closes = candles
        .iter()
        .map(|candle| candle.close)
        .collect::<Vec<_>>();
    let volumes = candles
        .iter()
        .map(|candle| candle.volume)
        .collect::<Vec<_>>();
    let ema_fast_series = ema_series(&closes, 20);
    let ema_slow_series = ema_series(&closes, 50);
    let ema_major_series = ema_series(&closes, 100);
    let momentum_short = pct_change_series(&closes, 6);
    let momentum_medium = pct_change_series(&closes, 42);
    let realized_volatility = realized_volatility_series(&closes, 20);
    let atr_percent = atr_percent_series(candles, 14);
    let adx = adx_series(candles, 14);
    let rsi = rsi_series(&closes, 14);
    let macd_histogram = macd_histogram_series(&closes, 12, 26, 9);
    let breakout_position = donchian_position_series(candles, 55);
    let close_location = close_location_series(candles);
    let volume_ratio = volume_ratio_series(&volumes, 42);
    let trend = ema_fast_series
        .iter()
        .zip(ema_slow_series.iter())
        .map(|(fast, slow)| {
            if fast.is_finite() && slow.is_finite() && slow.abs() > f64::EPSILON {
                fast / slow - 1.0
            } else {
                f64::NAN
            }
        })
        .collect::<Vec<_>>();

    let latest_close = last_finite(&closes).unwrap_or_default();
    let latest_ema_fast = last_finite(&ema_fast_series).unwrap_or_default();
    let latest_ema_mid = last_finite(&ema_slow_series).unwrap_or_default();
    let latest_ema_slow = last_finite(&ema_major_series).unwrap_or_default();
    let latest_trend = last_finite(&trend).unwrap_or_default();
    let latest_adx = last_finite(&adx).unwrap_or_default();
    let latest_rsi = last_finite(&rsi).unwrap_or(50.0);
    let latest_macd = last_finite(&macd_histogram).unwrap_or_default();
    let macd_zscore = latest_zscore(&macd_histogram, 126);
    let latest_breakout = last_finite(&breakout_position).unwrap_or_default();
    let latest_atr = last_finite(&atr_percent).unwrap_or_default();
    let latest_volume_ratio = last_finite(&volume_ratio).unwrap_or_default();
    let latest_close_location = last_finite(&close_location).unwrap_or_default();

    let trend_long = weighted_score(&[
        (flag(latest_close > latest_ema_fast), 0.18),
        (flag(latest_ema_fast > latest_ema_mid), 0.24),
        (flag(latest_ema_mid > latest_ema_slow), 0.18),
        (bounded(latest_adx, 16.0, 35.0), 0.25),
        (
            bounded(latest_trend, 0.0, latest_atr.max(0.003) * 4.0),
            0.15,
        ),
    ]);
    let trend_short = weighted_score(&[
        (flag(latest_close < latest_ema_fast), 0.18),
        (flag(latest_ema_fast < latest_ema_mid), 0.24),
        (flag(latest_ema_mid < latest_ema_slow), 0.18),
        (bounded(latest_adx, 16.0, 35.0), 0.25),
        (
            bounded(-latest_trend, 0.0, latest_atr.max(0.003) * 4.0),
            0.15,
        ),
    ]);
    let momentum_long = weighted_score(&[
        (
            bounded(last_finite(&momentum_short).unwrap_or_default(), 0.0, 0.018),
            0.30,
        ),
        (
            bounded(
                last_finite(&momentum_medium).unwrap_or_default(),
                0.0,
                0.060,
            ),
            0.30,
        ),
        (bounded(macd_zscore, 0.0, 2.0), 0.25),
        (flag(latest_macd > 0.0), 0.15),
    ]);
    let momentum_short = weighted_score(&[
        (
            bounded(
                -last_finite(&momentum_short).unwrap_or_default(),
                0.0,
                0.018,
            ),
            0.30,
        ),
        (
            bounded(
                -last_finite(&momentum_medium).unwrap_or_default(),
                0.0,
                0.060,
            ),
            0.30,
        ),
        (bounded(-macd_zscore, 0.0, 2.0), 0.25),
        (flag(latest_macd < 0.0), 0.15),
    ]);
    let rsi_long = center_score(latest_rsi, 60.0, 18.0) * bounded(latest_rsi, 46.0, 53.0);
    let rsi_short = center_score(latest_rsi, 40.0, 18.0) * bounded(54.0 - latest_rsi, 0.0, 8.0);
    let breakout_long = bounded(latest_breakout, 0.20, 0.85);
    let breakout_short = bounded(-latest_breakout, 0.20, 0.85);
    let volume_confirmation = bounded(latest_volume_ratio, -0.10, 0.85);
    let volatility_quality = volatility_environment_score(latest_atr);
    let candle_long = bounded(latest_close_location, 0.10, 0.85);
    let candle_short = bounded(-latest_close_location, 0.10, 0.85);

    let long_score = round4(weighted_score(&[
        (trend_long, 0.25),
        (momentum_long, 0.20),
        (rsi_long, 0.14),
        (breakout_long, 0.16),
        (volume_confirmation, 0.12),
        (volatility_quality, 0.08),
        (candle_long, 0.05),
    ]));
    let short_score = round4(weighted_score(&[
        (trend_short, 0.25),
        (momentum_short, 0.20),
        (rsi_short, 0.14),
        (breakout_short, 0.16),
        (volume_confirmation, 0.12),
        (volatility_quality, 0.08),
        (candle_short, 0.05),
    ]));

    let raw_score = (long_score - short_score).clamp(-1.0, 1.0);
    let score = round4(raw_score);
    let trend_quality = round4(trend_long.max(trend_short));
    let bias = if score >= 0.55 {
        "strong_bullish"
    } else if score >= 0.20 {
        "bullish"
    } else if score <= -0.55 {
        "strong_bearish"
    } else if score <= -0.20 {
        "bearish"
    } else {
        "neutral"
    };

    FactorSnapshot {
        score,
        bias: bias.into(),
        long_score,
        short_score,
        trend_quality,
        momentum_short: round4(last_finite(&pct_change_series(&closes, 6)).unwrap_or_default()),
        momentum_medium: round4(last_finite(&momentum_medium).unwrap_or_default()),
        trend: round4(latest_trend),
        adx: round4(latest_adx),
        rsi: round4(latest_rsi),
        macd_histogram: round4(latest_macd),
        breakout_position: round4(latest_breakout),
        realized_volatility: round4(last_finite(&realized_volatility).unwrap_or_default()),
        atr_percent: round4(latest_atr),
        volume_ratio: round4(latest_volume_ratio),
        volume_confirmation: round4(volume_confirmation),
        close_location: round4(latest_close_location),
        ema_fast: round4(latest_ema_fast),
        ema_mid: round4(latest_ema_mid),
        ema_slow: round4(latest_ema_slow),
        data_points: candles.len(),
    }
}

pub fn validate_config(config: &AiTradingConfig) -> Result<()> {
    if config.symbol_whitelist.is_empty() {
        bail!("AI 交易币种白名单不能为空");
    }
    let mut seen = BTreeSet::new();
    for symbol in &config.symbol_whitelist {
        validate_symbol(symbol)?;
        if !seen.insert(symbol) {
            bail!("AI 交易币种白名单不能重复");
        }
    }
    if !matches!(
        config.timeframe.as_str(),
        "1m" | "5m" | "15m" | "1h" | "4h" | "1d"
    ) {
        bail!("AI 交易周期不支持");
    }
    if !(1..=125).contains(&config.leverage) {
        bail!("杠杆必须在 1 到 125 倍之间");
    }
    if !config.max_stake_amount.is_finite()
        || !(5.0..=1_000_000.0).contains(&config.max_stake_amount)
    {
        bail!("单仓保证金上限必须在 5 到 1,000,000 USDT 之间");
    }
    if !config.capital_usage_percent.is_finite()
        || !(1.0..=100.0).contains(&config.capital_usage_percent)
    {
        bail!("资金使用比例必须在 1% 到 100% 之间");
    }
    if !config.risk_reward_ratio.is_finite() || !(0.5..=10.0).contains(&config.risk_reward_ratio) {
        bail!("单仓盈亏比必须在 0.5 到 10 之间");
    }
    if !config.minimum_confidence.is_finite() || !(0.0..=1.0).contains(&config.minimum_confidence) {
        bail!("最低置信度必须在 0 到 1 之间");
    }
    if !config.minimum_long_score.is_finite() || !(0.0..=1.0).contains(&config.minimum_long_score) {
        bail!("多头因子门槛必须在 0 到 1 之间");
    }
    if !config.minimum_short_score.is_finite() || !(0.0..=1.0).contains(&config.minimum_short_score)
    {
        bail!("空头因子门槛必须在 0 到 1 之间");
    }
    if !config.minimum_factor_score.is_finite()
        || !(0.0..=1.0).contains(&config.minimum_factor_score)
    {
        bail!("方向因子门槛必须在 0 到 1 之间");
    }
    if !config.minimum_trend_quality.is_finite()
        || !(0.0..=1.0).contains(&config.minimum_trend_quality)
    {
        bail!("趋势质量门槛必须在 0 到 1 之间");
    }
    if !config.minimum_adx.is_finite() || !(0.0..=80.0).contains(&config.minimum_adx) {
        bail!("ADX 门槛必须在 0 到 80 之间");
    }
    if !config.minimum_volume_ratio.is_finite()
        || !(-1.0..=5.0).contains(&config.minimum_volume_ratio)
    {
        bail!("成交量确认门槛必须在 -1 到 5 之间");
    }
    if !(5..=120).contains(&config.model_timeout_seconds) {
        bail!("模型超时必须在 5 到 120 秒之间");
    }
    if !(15..=3_600).contains(&config.market_max_age_seconds) {
        bail!("行情最大延迟必须在 15 到 3600 秒之间");
    }
    Ok(())
}

fn validate_symbol(symbol: &str) -> Result<()> {
    let Some(base) = symbol.strip_suffix("USDT") else {
        bail!("AI 白名单只支持 U 本位永续合约");
    };
    if !(2..=12).contains(&base.len())
        || !base
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
    {
        bail!("AI 白名单合约代码格式无效");
    }
    Ok(())
}

pub fn validate_signal(
    config: &AiTradingConfig,
    input: &AiTradingInput,
    mut signal: AiTradeSignal,
    now: i64,
) -> RiskDecision {
    let rejection = if !config.enabled {
        Some("AI 自动交易未启用".to_string())
    } else if !config
        .symbol_whitelist
        .iter()
        .any(|item| item == &input.symbol)
    {
        Some("交易对不在白名单".to_string())
    } else if signal.symbol != input.symbol || signal.timeframe != input.timeframe {
        Some("AI 返回的交易对或周期与输入不一致".to_string())
    } else if signal.candle_open_time != input.candle_open_time {
        Some("AI 返回的 K 线标识与输入不一致".to_string())
    } else if signal.valid_until <= now {
        Some("AI 信号已经过期".to_string())
    } else if now.saturating_sub(input.snapshot.updated_at) > config.market_max_age_seconds {
        Some("行情数据已经过期".to_string())
    } else if !signal.confidence.is_finite()
        || !(0.0..=1.0).contains(&signal.confidence)
        || signal.confidence < config.minimum_confidence
    {
        Some("AI 信号置信度不足或无效".to_string())
    } else if signal
        .stake_amount
        .is_some_and(|stake| !stake.is_finite() || stake < 5.0 || stake > config.max_stake_amount)
    {
        Some("AI 给出的仓位金额超出单仓上限".to_string())
    } else if signal.action == AiAction::Long && !factor_confirms_long(&input.factor, config) {
        Some(format!(
            "多因子未确认做多：long_score {:.2}，score {:.2}，ADX {:.1}，RSI {:.1}",
            input.factor.long_score, input.factor.score, input.factor.adx, input.factor.rsi
        ))
    } else if signal.action == AiAction::Short && !factor_confirms_short(&input.factor, config) {
        Some(format!(
            "多因子未确认做空：short_score {:.2}，score {:.2}，ADX {:.1}，RSI {:.1}",
            input.factor.short_score, input.factor.score, input.factor.adx, input.factor.rsi
        ))
    } else if signal.action == AiAction::Long
        && input.snapshot.funding_rate > 0.0008
        && input.snapshot.long_short_ratio > 1.7
    {
        Some("资金费率和多空比过热，放弃追多".to_string())
    } else if signal.action == AiAction::Short
        && input.snapshot.funding_rate < -0.0008
        && input.snapshot.long_short_ratio > 0.0
        && input.snapshot.long_short_ratio < 0.65
    {
        Some("资金费率和多空比过度偏空，放弃追空".to_string())
    } else if signal.action == AiAction::Hold {
        Some("AI 决策为 hold".to_string())
    } else {
        None
    };

    if let Some(reason) = rejection {
        signal.action = AiAction::Hold;
        signal.reason = reason.clone();
        return RiskDecision {
            approved: false,
            reason,
            signal,
        };
    }

    RiskDecision {
        approved: true,
        reason: "多因子验证通过".into(),
        signal,
    }
}

#[allow(dead_code)]
fn validate_signal_legacy(
    config: &AiTradingConfig,
    input: &AiTradingInput,
    mut signal: AiTradeSignal,
    now: i64,
) -> RiskDecision {
    let rejection = if !config.enabled {
        Some("AI 自动交易未启用")
    } else if !config
        .symbol_whitelist
        .iter()
        .any(|item| item == &input.symbol)
    {
        Some("交易对不在白名单")
    } else if signal.symbol != input.symbol || signal.timeframe != input.timeframe {
        Some("AI 返回的交易对或周期与输入不一致")
    } else if signal.candle_open_time != input.candle_open_time {
        Some("AI 返回的 K 线标识与输入不一致")
    } else if signal.valid_until <= now {
        Some("AI 信号已经过期")
    } else if now.saturating_sub(input.snapshot.updated_at) > config.market_max_age_seconds {
        Some("行情数据已经过期")
    } else if !signal.confidence.is_finite()
        || !(0.0..=1.0).contains(&signal.confidence)
        || signal.confidence < config.minimum_confidence
    {
        Some("AI 信号置信度不足或无效")
    } else if signal
        .stake_amount
        .is_some_and(|stake| !stake.is_finite() || stake < 5.0 || stake > config.max_stake_amount)
    {
        Some("AI 给出的仓位金额超出单仓上限")
    } else if signal.action == AiAction::Long && input.factor.score <= -0.35 {
        Some("本地因子偏空，拦截 AI 做多信号")
    } else if signal.action == AiAction::Short && input.factor.score >= 0.35 {
        Some("本地因子偏多，拦截 AI 做空信号")
    } else if signal.action == AiAction::Hold {
        Some("AI 决策为 hold")
    } else {
        None
    };

    if let Some(reason) = rejection {
        signal.action = AiAction::Hold;
        signal.reason = reason.into();
        return RiskDecision {
            approved: false,
            reason: reason.into(),
            signal,
        };
    }

    RiskDecision {
        approved: true,
        reason: "风控验证通过".into(),
        signal,
    }
}

fn factor_confirms_long(factor: &FactorSnapshot, config: &AiTradingConfig) -> bool {
    factor.data_points >= 120
        && factor.long_score >= config.minimum_long_score
        && factor.score >= config.minimum_factor_score
        && factor.trend_quality >= config.minimum_trend_quality
        && factor.adx >= config.minimum_adx
        && (46.0..=76.0).contains(&factor.rsi)
        && factor.atr_percent >= 0.0004
        && factor.atr_percent <= 0.0900
        && factor.volume_ratio >= config.minimum_volume_ratio
}

fn factor_confirms_short(factor: &FactorSnapshot, config: &AiTradingConfig) -> bool {
    factor.data_points >= 120
        && factor.short_score >= config.minimum_short_score
        && factor.score <= -config.minimum_factor_score
        && factor.trend_quality >= config.minimum_trend_quality
        && factor.adx >= config.minimum_adx
        && (24.0..=54.0).contains(&factor.rsi)
        && factor.atr_percent >= 0.0004
        && factor.atr_percent <= 0.0900
        && factor.volume_ratio >= config.minimum_volume_ratio
}

pub fn write_signal_atomically(path: &Path, signal: &AiTradeSignal) -> Result<()> {
    let mut signals: BTreeMap<String, AiTradeSignal> = if path.exists() {
        serde_json::from_str(&fs::read_to_string(path)?).context("现有 AI 信号文件格式无效")?
    } else {
        BTreeMap::new()
    };
    signals.insert(freqtrade_pair(&signal.symbol), signal.clone());
    let temporary = temporary_path(path);
    fs::write(
        &temporary,
        format!("{}\n", serde_json::to_string_pretty(&signals)?),
    )?;
    replace_file(&temporary, path).context("无法原子更新 AI 信号文件")?;
    Ok(())
}

fn freqtrade_pair(symbol: &str) -> String {
    symbol
        .strip_suffix("USDT")
        .map(|base| format!("{base}/USDT:USDT"))
        .unwrap_or_else(|| symbol.to_string())
}

fn temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("ai_signals.json");
    path.with_file_name(format!("{file_name}.{}.tmp", rand::random::<u64>()))
}

fn replace_file(temporary: &Path, target: &Path) -> Result<()> {
    #[cfg(windows)]
    if target.exists() {
        fs::remove_file(target)?;
    }
    fs::rename(temporary, target)?;
    Ok(())
}

fn ema_series(values: &[f64], period: usize) -> Vec<f64> {
    if values.is_empty() || period == 0 {
        return Vec::new();
    }
    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut ema = Vec::with_capacity(values.len());
    let mut current = values[0];
    for value in values {
        if value.is_finite() {
            current = value * multiplier + current * (1.0 - multiplier);
        }
        ema.push(current);
    }
    ema
}

fn pct_change_series(values: &[f64], period: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if index < period {
                return f64::NAN;
            }
            let previous = values[index - period];
            if previous.is_finite() && previous.abs() > f64::EPSILON && value.is_finite() {
                value / previous - 1.0
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn realized_volatility_series(values: &[f64], window: usize) -> Vec<f64> {
    let returns = values
        .windows(2)
        .map(|pair| {
            if pair[0] > 0.0 && pair[1] > 0.0 {
                (pair[1] / pair[0]).ln()
            } else {
                f64::NAN
            }
        })
        .collect::<Vec<_>>();
    let mut output = vec![f64::NAN; values.len()];
    for index in 1..values.len() {
        let end = index;
        let start = end.saturating_sub(window);
        output[index] = stddev(&returns[start..end]).unwrap_or(f64::NAN);
    }
    output
}

fn volume_ratio_series(values: &[f64], window: usize) -> Vec<f64> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if index + 1 < window || !value.is_finite() {
                return f64::NAN;
            }
            let start = index + 1 - window;
            let mean = mean(&values[start..=index]).unwrap_or(f64::NAN);
            if mean.is_finite() && mean.abs() > f64::EPSILON {
                value / mean - 1.0
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn atr_percent_series(candles: &[Candle], window: usize) -> Vec<f64> {
    let true_ranges = true_range_series(candles);
    let mut output = vec![f64::NAN; candles.len()];
    for index in 0..candles.len() {
        if index + 1 < window {
            continue;
        }
        let start = index + 1 - window;
        let atr = mean(&true_ranges[start..=index]).unwrap_or(f64::NAN);
        let close = candles[index].close;
        output[index] = if close.is_finite() && close.abs() > f64::EPSILON {
            atr / close
        } else {
            f64::NAN
        };
    }
    output
}

fn true_range_series(candles: &[Candle]) -> Vec<f64> {
    candles
        .iter()
        .enumerate()
        .map(|(index, candle)| {
            if index == 0 {
                candle.high - candle.low
            } else {
                let previous_close = candles[index - 1].close;
                (candle.high - candle.low)
                    .max((candle.high - previous_close).abs())
                    .max((candle.low - previous_close).abs())
            }
        })
        .collect()
}

fn adx_series(candles: &[Candle], window: usize) -> Vec<f64> {
    if candles.is_empty() || window == 0 {
        return Vec::new();
    }
    let true_ranges = true_range_series(candles);
    let mut plus_dm = vec![0.0; candles.len()];
    let mut minus_dm = vec![0.0; candles.len()];
    for index in 1..candles.len() {
        let up_move = candles[index].high - candles[index - 1].high;
        let down_move = candles[index - 1].low - candles[index].low;
        if up_move > down_move && up_move > 0.0 {
            plus_dm[index] = up_move;
        }
        if down_move > up_move && down_move > 0.0 {
            minus_dm[index] = down_move;
        }
    }

    let mut dx = vec![f64::NAN; candles.len()];
    for index in 0..candles.len() {
        if index + 1 < window {
            continue;
        }
        let start = index + 1 - window;
        let tr_sum = sum_finite(&true_ranges[start..=index]);
        if tr_sum <= f64::EPSILON {
            continue;
        }
        let plus_di = 100.0 * sum_finite(&plus_dm[start..=index]) / tr_sum;
        let minus_di = 100.0 * sum_finite(&minus_dm[start..=index]) / tr_sum;
        let total = plus_di + minus_di;
        if total > f64::EPSILON {
            dx[index] = 100.0 * (plus_di - minus_di).abs() / total;
        }
    }

    let mut adx = vec![f64::NAN; candles.len()];
    for index in 0..candles.len() {
        if index + 1 < window * 2 {
            continue;
        }
        let start = index + 1 - window;
        adx[index] = mean(&dx[start..=index]).unwrap_or(f64::NAN);
    }
    adx
}

fn rsi_series(values: &[f64], window: usize) -> Vec<f64> {
    if values.is_empty() || window == 0 {
        return Vec::new();
    }
    let mut output = vec![f64::NAN; values.len()];
    for index in window..values.len() {
        let start = index + 1 - window;
        let mut gain = 0.0;
        let mut loss = 0.0;
        for pair in values[start..=index].windows(2) {
            let change = pair[1] - pair[0];
            if change > 0.0 {
                gain += change;
            } else {
                loss += change.abs();
            }
        }
        output[index] = if loss <= f64::EPSILON {
            100.0
        } else {
            let rs = gain / loss;
            100.0 - 100.0 / (1.0 + rs)
        };
    }
    output
}

fn macd_histogram_series(
    values: &[f64],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> Vec<f64> {
    let fast = ema_series(values, fast_period);
    let slow = ema_series(values, slow_period);
    let macd = fast
        .iter()
        .zip(slow.iter())
        .map(|(fast, slow)| fast - slow)
        .collect::<Vec<_>>();
    let signal = ema_series(&macd, signal_period);
    macd.iter()
        .zip(signal.iter())
        .map(|(macd, signal)| macd - signal)
        .collect()
}

fn donchian_position_series(candles: &[Candle], window: usize) -> Vec<f64> {
    let mut output = vec![f64::NAN; candles.len()];
    for index in 0..candles.len() {
        if index < window {
            continue;
        }
        let start = index - window;
        let high = candles[start..index]
            .iter()
            .map(|candle| candle.high)
            .filter(|value| value.is_finite())
            .fold(f64::NEG_INFINITY, f64::max);
        let low = candles[start..index]
            .iter()
            .map(|candle| candle.low)
            .filter(|value| value.is_finite())
            .fold(f64::INFINITY, f64::min);
        let range = high - low;
        if range.is_finite() && range > f64::EPSILON {
            let midpoint = (high + low) * 0.5;
            output[index] = ((candles[index].close - midpoint) / range * 2.0).clamp(-1.5, 1.5);
        }
    }
    output
}

fn close_location_series(candles: &[Candle]) -> Vec<f64> {
    candles
        .iter()
        .map(|candle| {
            let range = candle.high - candle.low;
            if range.is_finite() && range > f64::EPSILON {
                ((candle.close - candle.low) / range * 2.0 - 1.0).clamp(-1.0, 1.0)
            } else {
                f64::NAN
            }
        })
        .collect()
}

fn latest_zscore(values: &[f64], window: usize) -> f64 {
    let latest = last_finite(values).unwrap_or_default();
    let recent = values
        .iter()
        .rev()
        .filter(|value| value.is_finite())
        .take(window)
        .copied()
        .collect::<Vec<_>>();
    if recent.len() < 20 {
        return 0.0;
    }
    let average = mean(&recent).unwrap_or(0.0);
    let deviation = stddev(&recent).unwrap_or(0.0);
    if deviation.abs() <= f64::EPSILON {
        0.0
    } else {
        ((latest - average) / deviation).clamp(-4.0, 4.0)
    }
}

fn sum_finite(values: &[f64]) -> f64 {
    values.iter().filter(|value| value.is_finite()).sum()
}

fn mean(values: &[f64]) -> Option<f64> {
    let mut sum = 0.0;
    let mut count = 0.0;
    for value in values.iter().filter(|value| value.is_finite()) {
        sum += value;
        count += 1.0;
    }
    (count > 0.0).then_some(sum / count)
}

fn stddev(values: &[f64]) -> Option<f64> {
    let average = mean(values)?;
    let mut sum = 0.0;
    let mut count = 0.0;
    for value in values.iter().filter(|value| value.is_finite()) {
        sum += (value - average).powi(2);
        count += 1.0;
    }
    (count > 1.0).then_some((sum / count).sqrt())
}

fn last_finite(values: &[f64]) -> Option<f64> {
    values.iter().rev().copied().find(|value| value.is_finite())
}

fn round4(value: f64) -> f64 {
    if value.is_finite() {
        (value * 10_000.0).round() / 10_000.0
    } else {
        0.0
    }
}

fn weighted_score(items: &[(f64, f64)]) -> f64 {
    let mut weighted = 0.0;
    let mut total = 0.0;
    for (value, weight) in items {
        if value.is_finite() && weight.is_finite() && *weight > 0.0 {
            weighted += value.clamp(0.0, 1.0) * weight;
            total += weight;
        }
    }
    if total > f64::EPSILON {
        (weighted / total).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

fn bounded(value: f64, low: f64, high: f64) -> f64 {
    if !value.is_finite() || high <= low {
        return 0.0;
    }
    ((value - low) / (high - low)).clamp(0.0, 1.0)
}

fn center_score(value: f64, center: f64, half_width: f64) -> f64 {
    if !value.is_finite() || half_width <= f64::EPSILON {
        return 0.0;
    }
    (1.0 - (value - center).abs() / half_width).clamp(0.0, 1.0)
}

fn volatility_environment_score(atr_percent: f64) -> f64 {
    if !atr_percent.is_finite() || !(0.0004..=0.0900).contains(&atr_percent) {
        return 0.0;
    }
    if atr_percent < 0.0010 {
        return bounded(atr_percent, 0.0004, 0.0010);
    }
    if atr_percent <= 0.0350 {
        return 1.0;
    }
    (1.0 - bounded(atr_percent, 0.0350, 0.0900) * 0.85).clamp(0.0, 1.0)
}

fn flag(value: bool) -> f64 {
    if value { 1.0 } else { 0.0 }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AiTradingInput, FactorSnapshot, MarketSnapshot};

    #[test]
    fn rejects_non_whitelisted_symbol_as_hold() {
        let config = AiTradingConfig {
            enabled: true,
            ..Default::default()
        };
        let input = AiTradingInput {
            symbol: "UNIUSDT".into(),
            timeframe: "1h".into(),
            candle_open_time: 10,
            candles: vec![],
            snapshot: MarketSnapshot {
                price: 100.0,
                updated_at: 20,
                ..Default::default()
            },
            factor: FactorSnapshot::default(),
            account: Default::default(),
            current_position: None,
            configured_leverage: 2,
            configured_capital_usage_percent: 10.0,
        };
        let signal = AiTradeSignal {
            decision_id: "one".into(),
            symbol: "UNIUSDT".into(),
            timeframe: "1h".into(),
            candle_open_time: 10,
            valid_until: 100,
            action: AiAction::Long,
            confidence: 0.9,
            stake_amount: None,
            stop_loss_percent: 1.0,
            take_profit_percent: 2.0,
            reason: "test".into(),
        };
        let decision = validate_signal(&config, &input, signal, 20);
        assert!(!decision.approved);
        assert_eq!(decision.signal.action, AiAction::Hold);
    }

    #[test]
    fn rejects_invalid_ai_config_symbols() {
        let config = AiTradingConfig {
            symbol_whitelist: vec!["BTC/USDT:USDT".into()],
            ..Default::default()
        };
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn updates_signal_file_repeatedly() {
        let path =
            std::env::temp_dir().join(format!("gqt-ai-signals-{}.json", rand::random::<u64>()));
        let first = AiTradeSignal {
            decision_id: "one".into(),
            symbol: "BTCUSDT".into(),
            timeframe: "1h".into(),
            candle_open_time: 10,
            valid_until: 100,
            action: AiAction::Long,
            confidence: 0.9,
            stake_amount: None,
            stop_loss_percent: 1.0,
            take_profit_percent: 2.0,
            reason: "first".into(),
        };
        let second = AiTradeSignal {
            decision_id: "two".into(),
            reason: "second".into(),
            ..first.clone()
        };
        write_signal_atomically(&path, &first).unwrap();
        write_signal_atomically(&path, &second).unwrap();
        let stored: BTreeMap<String, AiTradeSignal> =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(stored["BTC/USDT:USDT"].decision_id, "two");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn calculates_factor_from_rising_market() {
        let candles = (0..180)
            .map(|index| Candle {
                time: index,
                open: 100.0 + index as f64 * 0.4,
                high: 101.0 + index as f64 * 0.4,
                low: 99.0 + index as f64 * 0.4,
                close: 100.0 + index as f64 * 0.5,
                volume: 1000.0 + index as f64 * 5.0,
            })
            .collect::<Vec<_>>();
        let factor = calculate_factor_snapshot(&candles);
        assert!(factor.score.is_finite());
        assert_eq!(factor.data_points, 180);
        assert!(factor.momentum_short > 0.0);
        assert!(factor.ema_fast > factor.ema_slow);
    }

    #[test]
    fn rejects_long_when_local_factor_is_bearish() {
        let config = AiTradingConfig {
            enabled: true,
            ..Default::default()
        };
        let input = AiTradingInput {
            symbol: "BTCUSDT".into(),
            timeframe: "1h".into(),
            candle_open_time: 10,
            candles: vec![],
            snapshot: MarketSnapshot {
                price: 100.0,
                updated_at: 20,
                ..Default::default()
            },
            factor: FactorSnapshot {
                score: -0.60,
                bias: "bearish".into(),
                ..Default::default()
            },
            account: Default::default(),
            current_position: None,
            configured_leverage: 2,
            configured_capital_usage_percent: 10.0,
        };
        let signal = AiTradeSignal {
            decision_id: "bearish-block".into(),
            symbol: "BTCUSDT".into(),
            timeframe: "1h".into(),
            candle_open_time: 10,
            valid_until: 100,
            action: AiAction::Long,
            confidence: 0.9,
            stake_amount: None,
            stop_loss_percent: 1.0,
            take_profit_percent: 2.0,
            reason: "test".into(),
        };
        let decision = validate_signal(&config, &input, signal, 20);
        assert!(!decision.approved);
        assert_eq!(decision.signal.action, AiAction::Hold);
        assert!(decision.reason.contains("多因子未确认做多"));
    }
}
