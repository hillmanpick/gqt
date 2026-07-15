use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::model::{AiProvider, MarketSnapshot};

const ANALYST_RULES: &str = "你是加密货币合约市场的风险分析助手。只分析给定市场数据，输出简洁中文；必须列出市场状态、支持因素、反对因素、风险和需要等待的确认信号。不得声称已经下单，不得要求或使用交易权限，不得把分析写成确定收益承诺。";

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
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(45))
        .build()
        .context("无法创建 AI 请求客户端")?;

    match provider {
        AiProvider::OpenAi => openai(&client, model, api_key, &market_context),
        AiProvider::Claude => claude(&client, model, api_key, &market_context),
        AiProvider::DeepSeek => deepseek(&client, model, api_key, &market_context),
        AiProvider::Relay => relay(&client, model, api_key, relay_base_url, &market_context),
    }
}

fn default_model(provider: AiProvider) -> &'static str {
    match provider {
        AiProvider::OpenAi => "gpt-4.1-mini",
        AiProvider::Claude => "claude-sonnet-4-20250514",
        AiProvider::DeepSeek => "deepseek-chat",
        AiProvider::Relay => "gpt-4o-mini",
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
    chat_completions(
        client,
        "https://api.deepseek.com/chat/completions",
        "DeepSeek",
        model,
        api_key,
        input,
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
    chat_completions(client, &endpoint, "中转站", model, api_key, input)
}

fn chat_completions(
    client: &Client,
    endpoint: &str,
    provider: &str,
    model: &str,
    api_key: &str,
    input: &str,
) -> Result<String> {
    let response = client
        .post(endpoint)
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
        .with_context(|| format!("{provider} 请求失败"))?;
    let body = read_json(response, provider)?;
    body["choices"][0]["message"]["content"]
        .as_str()
        .map(str::to_owned)
        .with_context(|| format!("{provider} 返回中没有文本"))
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
}
