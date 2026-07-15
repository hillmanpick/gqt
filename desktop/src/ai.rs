use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::model::{AiProvider, MarketSnapshot};

const ANALYST_RULES: &str = "你是加密货币合约市场的风险分析助手。只分析给定市场数据，输出简洁中文；必须列出市场状态、支持因素、反对因素、风险和需要等待的确认信号。不得声称已经下单，不得要求或使用交易权限，不得把分析写成确定收益承诺。";

pub fn analyze(
    provider: AiProvider,
    requested_model: &str,
    api_key: &str,
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
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(45))
        .build()
        .context("无法创建 AI 请求客户端")?;

    match provider {
        AiProvider::OpenAi => openai(&client, model, api_key, &market_context),
        AiProvider::Claude => claude(&client, model, api_key, &market_context),
        AiProvider::DeepSeek => deepseek(&client, model, api_key, &market_context),
    }
}

fn default_model(provider: AiProvider) -> &'static str {
    match provider {
        AiProvider::OpenAi => "gpt-4.1-mini",
        AiProvider::Claude => "claude-sonnet-4-20250514",
        AiProvider::DeepSeek => "deepseek-chat",
    }
}

fn openai(client: &Client, model: &str, api_key: &str, input: &str) -> Result<String> {
    let response = client
        .post("https://api.openai.com/v1/responses")
        .bearer_auth(api_key.trim())
        .json(&json!({
            "model": model,
            "instructions": ANALYST_RULES,
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
    let response = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key.trim())
        .header("anthropic-version", "2023-06-01")
        .json(&json!({
            "model": model,
            "max_tokens": 1200,
            "system": ANALYST_RULES,
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
    let response = client
        .post("https://api.deepseek.com/chat/completions")
        .bearer_auth(api_key.trim())
        .json(&json!({
            "model": model,
            "messages": [
                {"role": "system", "content": ANALYST_RULES},
                {"role": "user", "content": input}
            ],
            "temperature": 0.2,
            "max_tokens": 1200
        }))
        .send()
        .context("DeepSeek 请求失败")?;
    let body = read_json(response, "DeepSeek")?;
    body["choices"][0]["message"]["content"]
        .as_str()
        .map(str::to_owned)
        .context("DeepSeek 返回中没有文本")
}

fn read_json(response: reqwest::blocking::Response, provider: &str) -> Result<Value> {
    let status = response.status();
    let body = response.text().context("无法读取 AI 返回")?;
    if !status.is_success() {
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
    serde_json::from_str(&body).context("AI 返回不是有效 JSON")
}
