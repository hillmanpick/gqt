use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};

use crate::model::{
    AiAction, AiProvider, AiTradeSignal, AiTradingConfig, AiTradingInput, MarketSnapshot,
};
use crate::network;

const ANALYST_RULES: &str = "你是加密货币合约市场的风险分析助手。只分析给定市场数据，输出简洁中文；必须列出市场状态、支持因素、反对因素、风险和需要等待的确认信号。不得声称已经下单，不得要求或使用交易权限，不得把分析写成确定收益承诺。";
const TRADE_DECISION_RULES: &str = "你是加密货币合约交易信号研究助手。你只能基于输入数据输出严格 JSON，不能输出 Markdown、解释段落、代码块或额外文字。回复必须以 { 开头并以 } 结尾。允许的 action 只有 long、short、close、hold。你不能承诺收益，遇到不确定、震荡、数据不足或风险过高必须输出 hold。stop_loss_percent 和 take_profit_percent 使用价格波动百分比，不使用杠杆后收益百分比。";

pub struct AiDecisionOutput {
    pub model: String,
    pub raw_output: String,
    pub signal: AiTradeSignal,
}

#[derive(Debug, Deserialize)]
struct RawTradeDecision {
    #[serde(deserialize_with = "deserialize_ai_action")]
    action: AiAction,
    confidence: f64,
    #[serde(default)]
    stake_amount: Option<f64>,
    #[serde(default)]
    stop_loss_percent: Option<f64>,
    #[serde(default)]
    take_profit_percent: Option<f64>,
    #[serde(default)]
    reason: Option<String>,
}

pub fn analyze(
    provider: AiProvider,
    requested_model: &str,
    api_key: &str,
    relay_base_url: &str,
    snapshot: &MarketSnapshot,
    prompt: &str,
) -> Result<String> {
    if api_key.trim().is_empty() {
        bail!("尚未配置 {} API Key", provider.label());
    }

    let model = if requested_model.trim().is_empty() {
        default_model(provider)
    } else {
        requested_model.trim()
    };
    let market_context = format!(
        "交易对: {}\n最新价: {:.8}\n24h涨跌: {:.3}%\n24h最高/最低: {:.8} / {:.8}\n标记价格: {:.8}\n资金费率: {:.6}%\n多空比: {:.4}\n持仓量: {:.2}\n24h成交额: {:.2} USDT\n量化情绪: {} ({}/100)\n趋势分: {:.2}\n仓位分: {:.2}\n资金费率分: {:.2}\n\n用户关注: {}",
        snapshot.symbol,
        snapshot.price,
        snapshot.change_percent,
        snapshot.high,
        snapshot.low,
        snapshot.mark_price,
        snapshot.funding_rate * 100.0,
        snapshot.long_short_ratio,
        snapshot.open_interest,
        snapshot.quote_volume,
        snapshot.sentiment.label,
        snapshot.sentiment.score,
        snapshot.sentiment.trend,
        snapshot.sentiment.positioning,
        snapshot.sentiment.funding,
        prompt.trim(),
    );
    let client =
        network::client(std::time::Duration::from_secs(45)).context("无法创建 AI 请求客户端")?;

    match provider {
        AiProvider::OpenAi => openai(&client, model, api_key, &market_context),
        AiProvider::Claude => claude(&client, model, api_key, &market_context),
        AiProvider::DeepSeek => deepseek(&client, model, api_key, &market_context),
        AiProvider::Relay => relay(&client, model, api_key, relay_base_url, &market_context),
    }
}

pub fn decide_trade(
    provider: AiProvider,
    requested_model: &str,
    api_key: &str,
    relay_base_url: &str,
    input: &AiTradingInput,
    config: &AiTradingConfig,
) -> Result<AiDecisionOutput> {
    if api_key.trim().is_empty() {
        bail!("尚未配置 {} API Key", provider.label());
    }

    let model = selected_model(provider, requested_model).to_string();
    let prompt = trade_decision_prompt(input, config)?;
    let client = network::client(std::time::Duration::from_secs(config.model_timeout_seconds))
        .context("无法创建 AI 决策请求客户端")?;
    let raw_output = request_text(
        provider,
        &client,
        &model,
        api_key,
        relay_base_url,
        TRADE_DECISION_RULES,
        &prompt,
        700,
        0.0,
    )?;
    let mut raw_output = raw_output;
    let decision_json = match extract_json_object(&raw_output) {
        Ok(value) => value,
        Err(first_error) => {
            let repaired = repair_trade_decision_json(
                provider,
                &client,
                &model,
                api_key,
                relay_base_url,
                &raw_output,
                input,
                config,
            )
            .with_context(|| {
                format!(
                    "AI 输出没有 JSON，自动修复失败。原始返回: {}",
                    preview_text(&raw_output)
                )
            })?;
            raw_output = format!("{raw_output}\n\n[json_repair]\n{repaired}");
            if repaired.trim().is_empty() {
                bail!("{first_error}: {}", preview_text(&raw_output));
            }
            repaired
        }
    };
    let raw_decision: RawTradeDecision =
        serde_json::from_str(&decision_json).with_context(|| {
            format!(
                "AI 决策 JSON 字段格式无效: {}",
                preview_text(&decision_json)
            )
        })?;
    let now = chrono::Utc::now().timestamp();
    let timeframe_seconds = crate::model::Interval::from_timeframe(&input.timeframe)
        .map(crate::model::Interval::seconds)
        .unwrap_or(60);
    let stop_loss = raw_decision
        .stop_loss_percent
        .unwrap_or(1.5)
        .clamp(0.1, 20.0);
    let take_profit = raw_decision
        .take_profit_percent
        .unwrap_or(stop_loss * config.risk_reward_ratio)
        .clamp(0.1, 100.0)
        .max(stop_loss * 0.5);
    Ok(AiDecisionOutput {
        model,
        raw_output,
        signal: AiTradeSignal {
            decision_id: format!(
                "ai-{}-{}-{}",
                input.symbol, input.timeframe, input.candle_open_time
            ),
            symbol: input.symbol.clone(),
            timeframe: input.timeframe.clone(),
            candle_open_time: input.candle_open_time,
            valid_until: now.max(input.candle_open_time + timeframe_seconds) + timeframe_seconds,
            action: raw_decision.action,
            confidence: raw_decision.confidence,
            stake_amount: raw_decision.stake_amount,
            stop_loss_percent: stop_loss,
            take_profit_percent: take_profit,
            reason: raw_decision
                .reason
                .unwrap_or_else(|| "AI 未提供理由".into())
                .chars()
                .take(300)
                .collect(),
        },
    })
}

fn repair_trade_decision_json(
    provider: AiProvider,
    client: &Client,
    model: &str,
    api_key: &str,
    relay_base_url: &str,
    raw_output: &str,
    input: &AiTradingInput,
    config: &AiTradingConfig,
) -> Result<String> {
    let repair_prompt = serde_json::to_string_pretty(&json!({
        "task": "The previous response was not valid JSON. Convert it into one valid JSON object only.",
        "hard_rule": "If the previous response is not a clear actionable signal, return hold with confidence 0.",
        "previous_response": raw_output.chars().take(2000).collect::<String>(),
        "required_output_schema": {
            "action": "long | short | close | hold",
            "confidence": "number from 0 to 1",
            "stake_amount": "optional number or null",
            "stop_loss_percent": "number from 0.1 to 20",
            "take_profit_percent": "number from 0.1 to 100",
            "reason": "short Chinese reason"
        },
        "must_respect": {
            "symbol": input.symbol,
            "timeframe": input.timeframe,
            "max_stake_amount": config.max_stake_amount,
            "risk_reward_ratio": config.risk_reward_ratio,
            "minimum_confidence": config.minimum_confidence
        },
        "fallback_output": {
            "action": "hold",
            "confidence": 0.0,
            "stake_amount": null,
            "stop_loss_percent": 1.5,
            "take_profit_percent": 3.0,
            "reason": "AI 原始输出不是有效 JSON，安全观望"
        }
    }))?;
    let repaired = request_text(
        provider,
        client,
        model,
        api_key,
        relay_base_url,
        TRADE_DECISION_RULES,
        &repair_prompt,
        350,
        0.0,
    )?;
    extract_json_object(&repaired).with_context(|| {
        format!(
            "AI JSON 修复响应仍没有 JSON。修复返回: {}",
            preview_text(&repaired)
        )
    })
}

fn default_model(provider: AiProvider) -> &'static str {
    match provider {
        AiProvider::OpenAi => "gpt-4.1-mini",
        AiProvider::Claude => "claude-sonnet-4-20250514",
        AiProvider::DeepSeek => "deepseek-chat",
        AiProvider::Relay => "gpt-5.6-luna",
    }
}

fn selected_model<'a>(provider: AiProvider, requested_model: &'a str) -> &'a str {
    let requested_model = requested_model.trim();
    if requested_model.is_empty()
        || (provider == AiProvider::Relay
            && ["gpt-4o-mini", "gpt5.5"]
                .iter()
                .any(|legacy| requested_model.eq_ignore_ascii_case(legacy)))
    {
        default_model(provider)
    } else {
        requested_model
    }
}

fn trade_decision_prompt(input: &AiTradingInput, config: &AiTradingConfig) -> Result<String> {
    let recent: Vec<Value> = input
        .candles
        .iter()
        .rev()
        .take(80)
        .rev()
        .map(|candle| {
            json!({
                "time": candle.time,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
            })
        })
        .collect();
    Ok(serde_json::to_string_pretty(&json!({
        "task": "Return one trading decision as JSON only.",
        "strict_output": "Return exactly one JSON object. Do not wrap it in Markdown. Do not add comments.",
        "output_schema": {
            "action": "long | short | close | hold",
            "confidence": "number from 0 to 1",
            "stake_amount": "optional USDT margin, must be <= max_stake_amount",
            "stop_loss_percent": "price move percent, 0.1 to 20",
            "take_profit_percent": "price move percent, should respect risk_reward_ratio",
            "reason": "short Chinese reason"
        },
        "example_output": {
            "action": "hold",
            "confidence": 0.0,
            "stake_amount": null,
            "stop_loss_percent": 1.5,
            "take_profit_percent": 3.0,
            "reason": "数据不足，等待确认"
        },
        "constraints": {
            "symbol_whitelist": config.symbol_whitelist,
            "timeframe": config.timeframe,
            "leverage": config.leverage,
            "max_stake_amount": config.max_stake_amount,
            "capital_usage_percent": config.capital_usage_percent,
            "risk_reward_ratio": config.risk_reward_ratio,
            "minimum_confidence": config.minimum_confidence,
            "allow_ai_risk_sizing": config.allow_ai_risk_sizing,
            "dry_run_only": config.dry_run_only,
            "one_signal_per_candle": config.one_signal_per_candle
        },
        "input": {
            "symbol": input.symbol,
            "timeframe": input.timeframe,
            "candle_open_time": input.candle_open_time,
            "configured_leverage": input.configured_leverage,
            "configured_capital_usage_percent": input.configured_capital_usage_percent,
            "account": {
                "available_balance": input.account.available_balance,
                "margin_balance": input.account.margin_balance,
                "unrealized_profit": input.account.unrealized_profit,
                "open_positions": input.account.positions.len()
            },
            "current_position": input.current_position,
            "snapshot": input.snapshot,
            "recent_candles": recent
        }
    }))?)
}

fn request_text(
    provider: AiProvider,
    client: &Client,
    model: &str,
    api_key: &str,
    relay_base_url: &str,
    instructions: &str,
    input: &str,
    max_tokens: u32,
    temperature: f32,
) -> Result<String> {
    match provider {
        AiProvider::OpenAi => openai_with_rules(client, model, api_key, instructions, input),
        AiProvider::Claude => {
            claude_with_rules(client, model, api_key, instructions, input, max_tokens)
        }
        AiProvider::DeepSeek => chat_completions(
            client,
            "https://api.deepseek.com/chat/completions",
            "DeepSeek",
            model,
            api_key,
            instructions,
            input,
            max_tokens,
            temperature,
            true,
        ),
        AiProvider::Relay => {
            let base_url = normalize_relay_base_url(relay_base_url)?;
            let endpoint = if base_url.ends_with("/chat/completions") {
                base_url
            } else {
                format!("{base_url}/chat/completions")
            };
            chat_completions(
                client,
                &endpoint,
                "中转站",
                model,
                api_key,
                instructions,
                input,
                max_tokens,
                temperature,
                true,
            )
        }
    }
}

fn openai(client: &Client, model: &str, api_key: &str, input: &str) -> Result<String> {
    openai_with_rules(client, model, api_key, ANALYST_RULES, input)
}

fn openai_with_rules(
    client: &Client,
    model: &str,
    api_key: &str,
    instructions: &str,
    input: &str,
) -> Result<String> {
    let response = client
        .post("https://api.openai.com/v1/responses")
        .bearer_auth(api_key.trim())
        .json(&json!({
            "model": model,
            "instructions": instructions,
            "input": input
        }))
        .send()
        .context("OpenAI 请求失败")?;
    let body = read_json(response, "OpenAI")?;
    body["output"]
        .as_array()
        .and_then(|items| {
            items.iter().find_map(|item| {
                item["content"].as_array().and_then(|content| {
                    content.iter().find_map(|part| {
                        (part["type"] == "output_text")
                            .then(|| part["text"].as_str())
                            .flatten()
                    })
                })
            })
        })
        .map(str::to_owned)
        .context("OpenAI 返回中没有文本")
}

fn claude(client: &Client, model: &str, api_key: &str, input: &str) -> Result<String> {
    claude_with_rules(client, model, api_key, ANALYST_RULES, input, 1200)
}

fn claude_with_rules(
    client: &Client,
    model: &str,
    api_key: &str,
    system: &str,
    input: &str,
    max_tokens: u32,
) -> Result<String> {
    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key.trim())
        .header("anthropic-version", "2023-06-01")
        .json(&json!({
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": [{"role": "user", "content": input}]
        }))
        .send()
        .context("Claude 请求失败")?;
    let body = read_json(response, "Claude")?;
    body["content"]
        .as_array()
        .and_then(|items| {
            items.iter().find_map(|item| {
                (item["type"] == "text")
                    .then(|| item["text"].as_str())
                    .flatten()
            })
        })
        .map(str::to_owned)
        .context("Claude 返回中没有文本")
}

fn deepseek(client: &Client, model: &str, api_key: &str, input: &str) -> Result<String> {
    chat_completions(
        client,
        "https://api.deepseek.com/chat/completions",
        "DeepSeek",
        model,
        api_key,
        ANALYST_RULES,
        input,
        1200,
        0.2,
        false,
    )
}

fn relay(
    client: &Client,
    model: &str,
    api_key: &str,
    base_url: &str,
    input: &str,
) -> Result<String> {
    let base_url = normalize_relay_base_url(base_url)?;
    let endpoint = if base_url.ends_with("/chat/completions") {
        base_url
    } else {
        format!("{base_url}/chat/completions")
    };
    chat_completions(
        client,
        &endpoint,
        "中转站",
        model,
        api_key,
        ANALYST_RULES,
        input,
        1200,
        0.2,
        false,
    )
}

fn chat_completions(
    client: &Client,
    endpoint: &str,
    provider: &str,
    model: &str,
    api_key: &str,
    system: &str,
    input: &str,
    max_tokens: u32,
    temperature: f32,
    json_output: bool,
) -> Result<String> {
    match chat_completions_attempt(
        client,
        endpoint,
        provider,
        model,
        api_key,
        system,
        input,
        max_tokens,
        temperature,
        json_output,
    ) {
        Ok(text) => Ok(text),
        Err(error) if provider == "中转站" && is_no_available_channel_error(&error) => {
            let candidates = discover_relay_model_candidates(client, endpoint, api_key, model)
                .with_context(|| format!("中转站模型 {model} 不可用，且自动读取模型列表失败"))?;
            let mut failures = vec![format!("{model}: {error}")];
            for candidate in candidates.iter().take(8) {
                match chat_completions_attempt(
                    client,
                    endpoint,
                    provider,
                    candidate,
                    api_key,
                    system,
                    input,
                    max_tokens,
                    temperature,
                    json_output,
                ) {
                    Ok(text) => return Ok(text),
                    Err(error) if is_transient_provider_error(&error) => {
                        return Err(error).with_context(|| {
                            format!(
                                "中转站模型 {model} 不可用，自动切换到 {candidate} 时服务端仍不可用"
                            )
                        });
                    }
                    Err(error) => failures.push(format!("{candidate}: {error}")),
                }
            }
            bail!(
                "中转站模型 {model} 不可用，自动尝试 {} 个模型仍失败：{}",
                candidates.len().min(8),
                failures.join("；").chars().take(900).collect::<String>()
            )
        }
        Err(error) => Err(error),
    }
}

fn chat_completions_attempt(
    client: &Client,
    endpoint: &str,
    provider: &str,
    model: &str,
    api_key: &str,
    system: &str,
    input: &str,
    max_tokens: u32,
    temperature: f32,
    json_output: bool,
) -> Result<String> {
    match chat_completions_once(
        client,
        endpoint,
        provider,
        model,
        api_key,
        system,
        input,
        max_tokens,
        temperature,
        json_output,
    ) {
        Err(error) if json_output && is_response_format_error(&error) => chat_completions_once(
            client,
            endpoint,
            provider,
            model,
            api_key,
            system,
            input,
            max_tokens,
            temperature,
            false,
        )
        .with_context(|| format!("{provider} 不支持 JSON mode，降级重试仍失败")),
        result => result,
    }
}

fn discover_relay_model_candidates(
    client: &Client,
    chat_endpoint: &str,
    api_key: &str,
    current_model: &str,
) -> Result<Vec<String>> {
    let endpoint = model_list_endpoint(chat_endpoint)?;
    let response = client
        .get(endpoint)
        .bearer_auth(api_key.trim())
        .send()
        .context("中转站模型列表请求失败")?;
    let body = read_response_text(response, "中转站模型列表")?;
    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("中转站模型列表不是有效 JSON: {}", preview_text(&body)))?;
    let candidates = rank_chat_model_ids(parse_model_ids(&value), current_model);
    if candidates.is_empty() {
        bail!("中转站模型列表里没有可用聊天模型");
    }
    Ok(candidates)
}

fn model_list_endpoint(chat_endpoint: &str) -> Result<String> {
    chat_endpoint
        .trim_end_matches('/')
        .strip_suffix("/chat/completions")
        .map(|base| format!("{base}/models"))
        .context("中转站 Chat Completions 地址无效，无法推导 /models")
}

fn parse_model_ids(value: &Value) -> Vec<String> {
    let Some(items) = value
        .get("data")
        .and_then(|data| data.as_array())
        .or_else(|| value.as_array())
    else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for item in items {
        let id = item
            .get("id")
            .and_then(|value| value.as_str())
            .or_else(|| item.get("model").and_then(|value| value.as_str()))
            .or_else(|| item.as_str());
        if let Some(id) = id {
            let id = id.trim();
            if !id.is_empty() && !ids.iter().any(|existing| existing == id) {
                ids.push(id.to_string());
            }
        }
    }
    ids
}

fn rank_chat_model_ids(ids: Vec<String>, current_model: &str) -> Vec<String> {
    let preferred = [
        "gpt-5.6-luna",
        "gpt-5",
        "gpt-4.1",
        "gpt-4o",
        "deepseek-chat",
        "claude-sonnet-4-20250514",
        "gemini-2.5-pro",
        "qwen-max",
        "gpt-3.5-turbo",
    ];
    let mut ranked = Vec::new();
    for wanted in preferred {
        if let Some(id) = ids
            .iter()
            .find(|id| id.eq_ignore_ascii_case(wanted) && !id.eq_ignore_ascii_case(current_model))
        {
            ranked.push(id.clone());
        }
    }
    for id in ids {
        if id.eq_ignore_ascii_case(current_model)
            || !is_chat_model_candidate(&id)
            || ranked
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(&id))
        {
            continue;
        }
        ranked.push(id);
    }
    ranked
}

fn is_chat_model_candidate(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    let excluded = [
        "embedding",
        "rerank",
        "moderation",
        "whisper",
        "tts",
        "dall",
        "image",
        "vision-only",
        "audio",
        "sd-",
        "stable-diffusion",
    ];
    !excluded.iter().any(|item| lower.contains(item))
}

fn chat_completions_once(
    client: &Client,
    endpoint: &str,
    provider: &str,
    model: &str,
    api_key: &str,
    system: &str,
    input: &str,
    max_tokens: u32,
    temperature: f32,
    json_output: bool,
) -> Result<String> {
    let mut payload = json!({
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": input}
        ],
        "temperature": temperature,
        "max_tokens": max_tokens
    });
    if json_output {
        payload["response_format"] = json!({"type": "json_object"});
    }
    let response = client
        .post(endpoint)
        .bearer_auth(api_key.trim())
        .json(&payload)
        .send()
        .with_context(|| format!("{provider} 请求失败"))?;
    let body_text = read_response_text(response, provider)?;
    match serde_json::from_str::<Value>(&body_text) {
        Ok(body) => chat_completion_text_from_value(&body, provider),
        Err(_) => {
            let text = body_text.trim();
            if text.is_empty() {
                bail!("{provider} 返回空内容");
            }
            Ok(text.to_string())
        }
    }
}

fn is_response_format_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("response_format")
        || message.contains("json_object")
        || message.contains("json mode")
}

fn is_no_available_channel_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("no available channel")
        || message.contains("model") && message.contains("under group")
}

fn is_transient_provider_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("system cpu overloaded")
        || message.contains("service temporarily unavailable")
        || message.contains("too many requests")
        || message.contains("rate limit")
}

fn extract_json_object(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        return Ok(trimmed.to_string());
    }
    let start = trimmed
        .find('{')
        .with_context(|| format!("AI 决策中没有 JSON 对象: {}", preview_text(trimmed)))?;
    let end = trimmed
        .rfind('}')
        .with_context(|| format!("AI 决策 JSON 不完整: {}", preview_text(trimmed)))?;
    if end <= start {
        bail!("AI 决策 JSON 不完整");
    }
    Ok(trimmed[start..=end].to_string())
}

fn deserialize_ai_action<'de, D>(deserializer: D) -> std::result::Result<AiAction, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.trim().to_ascii_lowercase().as_str() {
        "long" | "buy" => Ok(AiAction::Long),
        "short" | "sell" => Ok(AiAction::Short),
        "close" | "exit" => Ok(AiAction::Close),
        "hold" | "wait" | "none" => Ok(AiAction::Hold),
        other => Err(serde::de::Error::custom(format!(
            "action 必须是 long/short/close/hold，实际为 {other}"
        ))),
    }
}

fn chat_completion_text_from_value(body: &Value, provider: &str) -> Result<String> {
    if let Some(text) = body["choices"][0]["message"]["content"].as_str() {
        return Ok(text.to_string());
    }
    if let Some(parts) = body["choices"][0]["message"]["content"].as_array() {
        let text = parts
            .iter()
            .filter_map(|part| {
                part["text"]
                    .as_str()
                    .or_else(|| part["content"].as_str())
                    .or_else(|| part["value"].as_str())
            })
            .collect::<Vec<_>>()
            .join("");
        if !text.trim().is_empty() {
            return Ok(text);
        }
    }
    if let Some(text) = body["choices"][0]["text"].as_str() {
        return Ok(text.to_string());
    }
    if let Some(text) = body["content"].as_str() {
        return Ok(text.to_string());
    }
    if let Some(text) = body["message"].as_str() {
        return Ok(text.to_string());
    }
    bail!(
        "{provider} 返回中没有文本: {}",
        preview_text(&body.to_string())
    )
}

pub fn normalize_relay_base_url(value: &str) -> Result<String> {
    let value = value.trim().trim_end_matches('/');
    if value.is_empty() {
        bail!("请填写中转站 Base URL");
    }
    let url = reqwest::Url::parse(value).context("中转站 Base URL 格式无效")?;
    let host = url.host_str().context("中转站 Base URL 缺少主机名")?;
    let localhost = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if url.scheme() != "https" && !(url.scheme() == "http" && localhost) {
        bail!("中转站必须使用 HTTPS，本机调试地址除外");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("中转站 Base URL 不能包含用户名或密码");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("中转站 Base URL 不能包含查询参数或片段");
    }
    Ok(value.to_string())
}

fn read_json(response: reqwest::blocking::Response, provider: &str) -> Result<Value> {
    let body = read_response_text(response, provider)?;
    if looks_like_html(&body) {
        bail!(
            "{provider} 返回的是网页 HTML，不是 API JSON。中转站 Base URL 可能填成了官网/控制台地址，请改成 OpenAI-compatible API 地址，例如 https://example.com/v1"
        );
    }
    serde_json::from_str(&body)
        .with_context(|| format!("{provider} 返回不是有效 JSON: {}", preview_text(&body)))
}

fn read_response_text(response: reqwest::blocking::Response, provider: &str) -> Result<String> {
    let status = response.status();
    let body = response.text().context("无法读取 AI 返回")?;
    if !status.is_success() {
        if looks_like_html(&body) {
            bail!(
                "{provider} 接口返回 {status}: 收到网页 HTML，不是 API JSON。中转站 Base URL 可能填成了官网/控制台地址，请改成 OpenAI-compatible API 地址，例如 https://example.com/v1"
            );
        }
        let detail = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| {
                value["error"]["message"]
                    .as_str()
                    .or_else(|| value["message"].as_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| body.chars().take(300).collect());
        bail!("{} 接口返回 {}: {}", provider, status, detail);
    }
    if looks_like_html(&body) {
        bail!(
            "{provider} 返回的是网页 HTML，不是 API JSON。中转站 Base URL 可能填成了官网/控制台地址，请改成 OpenAI-compatible API 地址，例如 https://example.com/v1"
        );
    }
    Ok(body)
}

fn looks_like_html(value: &str) -> bool {
    let trimmed = value.trim_start().to_ascii_lowercase();
    trimmed.starts_with("<!doctype html")
        || trimmed.starts_with("<html")
        || trimmed.contains("<head>")
        || trimmed.contains("<body")
}

fn preview_text(value: &str) -> String {
    let preview = value
        .trim()
        .chars()
        .flat_map(|character| {
            if character.is_control() {
                ' '.to_lowercase()
            } else {
                character.to_lowercase()
            }
        })
        .take(240)
        .collect::<String>();
    if preview.is_empty() {
        "<空内容>".into()
    } else {
        preview
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_relay_base_urls() {
        assert_eq!(
            normalize_relay_base_url("https://relay.example.com/v1/").unwrap(),
            "https://relay.example.com/v1"
        );
        assert!(normalize_relay_base_url("http://relay.example.com/v1").is_err());
        assert!(normalize_relay_base_url("http://127.0.0.1:8080/v1").is_ok());
        assert!(normalize_relay_base_url("https://user:pass@example.com/v1").is_err());
    }

    #[test]
    fn extracts_json_from_markdown_response() {
        let value = "```json\n{\"action\":\"HOLD\",\"confidence\":0.1}\n```";
        let extracted = extract_json_object(value).unwrap();
        assert_eq!(extracted, r#"{"action":"HOLD","confidence":0.1}"#);
    }

    #[test]
    fn parses_flexible_trade_actions() {
        let decision: RawTradeDecision =
            serde_json::from_str(r#"{"action":"LONG","confidence":0.9,"reason":"test"}"#).unwrap();
        assert_eq!(decision.action, AiAction::Long);
    }

    #[test]
    fn reads_openai_compatible_content_text() {
        let body = json!({
            "choices": [{
                "message": {"content": "{\"action\":\"hold\",\"confidence\":0.2}"}
            }]
        });
        assert_eq!(
            chat_completion_text_from_value(&body, "test").unwrap(),
            r#"{"action":"hold","confidence":0.2}"#
        );
    }

    #[test]
    fn parses_and_ranks_relay_models() {
        let body = json!({
            "data": [
                {"id": "text-embedding-3-small"},
                {"id": "deepseek-chat"},
                {"id": "gpt-5.6-luna"},
                {"id": "whisper-1"}
            ]
        });
        let ranked = rank_chat_model_ids(parse_model_ids(&body), "gpt-5.6-luna");
        assert_eq!(ranked, vec!["deepseek-chat".to_string()]);
    }

    #[test]
    fn derives_models_endpoint_from_chat_endpoint() {
        assert_eq!(
            model_list_endpoint("https://api.example.com/v1/chat/completions").unwrap(),
            "https://api.example.com/v1/models"
        );
    }

    #[test]
    fn migrates_old_relay_default_model() {
        assert_eq!(
            selected_model(AiProvider::Relay, "gpt-4o-mini"),
            "gpt-5.6-luna"
        );
        assert_eq!(selected_model(AiProvider::Relay, "gpt5.5"), "gpt-5.6-luna");
        assert_eq!(selected_model(AiProvider::Relay, ""), "gpt-5.6-luna");
    }
}
