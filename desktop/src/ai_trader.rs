use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::model::{AiAction, AiTradeSignal, AiTradingConfig, AiTradingInput};

#[derive(Debug, Clone)]
pub struct RiskDecision {
    pub approved: bool,
    pub reason: String,
    pub signal: AiTradeSignal,
}

pub fn validate_config(config: &AiTradingConfig) -> Result<()> {
    if config.symbol_whitelist.is_empty() {
        bail!("AI 交易币种白名单不能为空");
    }
    if !(1..=125).contains(&config.leverage) {
        bail!("杠杆必须在 1 到 125 倍之间");
    }
    if !(1.0..=100.0).contains(&config.capital_usage_percent) {
        bail!("资金使用比例必须在 1% 到 100% 之间");
    }
    if !(0.0..=1.0).contains(&config.minimum_confidence) {
        bail!("最低置信度必须在 0 到 1 之间");
    }
    if !(5..=120).contains(&config.model_timeout_seconds) {
        bail!("模型超时必须在 5 到 120 秒之间");
    }
    if !(15..=3_600).contains(&config.market_max_age_seconds) {
        bail!("行情最大延迟必须在 15 到 3600 秒之间");
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
    fs::rename(&temporary, path).context("无法原子更新 AI 信号文件")?;
    Ok(())
}

fn freqtrade_pair(symbol: &str) -> String {
    symbol
        .strip_suffix("USDT")
        .map(|base| format!("{base}/USDT:USDT"))
        .unwrap_or_else(|| symbol.to_string())
}

fn temporary_path(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AiTradingInput, MarketSnapshot};

    #[test]
    fn rejects_non_whitelisted_symbol_as_hold() {
        let config = AiTradingConfig {
            enabled: true,
            ..Default::default()
        };
        let input = AiTradingInput {
            symbol: "SOLUSDT".into(),
            timeframe: "1h".into(),
            candle_open_time: 10,
            candles: vec![],
            snapshot: MarketSnapshot {
                price: 100.0,
                updated_at: 20,
                ..Default::default()
            },
            account: Default::default(),
            current_position: None,
            configured_leverage: 2,
            configured_capital_usage_percent: 10.0,
        };
        let signal = AiTradeSignal {
            decision_id: "one".into(),
            symbol: "SOLUSDT".into(),
            timeframe: "1h".into(),
            candle_open_time: 10,
            valid_until: 100,
            action: AiAction::Long,
            confidence: 0.9,
            stop_loss_percent: 1.0,
            take_profit_percent: 2.0,
            reason: "test".into(),
        };
        let decision = validate_signal(&config, &input, signal, 20);
        assert!(!decision.approved);
        assert_eq!(decision.signal.action, AiAction::Hold);
    }
}
