use std::{
    collections::{BTreeMap, BTreeSet},
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
            symbol: "UNIUSDT".into(),
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
}
