use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use hmac::{Hmac, Mac};
use reqwest::blocking::{Client, Response};
use serde_json::Value;
use sha2::Sha256;

const BINANCE_FUTURES_BASE: &str = "https://fapi.binance.com";

pub fn validate_futures_credentials(api_key: &str, api_secret: &str) -> Result<String> {
    let api_key = api_key.trim();
    let api_secret = api_secret.trim();
    if !(10..=512).contains(&api_key.len()) || !(10..=512).contains(&api_secret.len()) {
        bail!("Binance API Key 或 Secret 长度无效");
    }

    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("无法创建 Binance 验证客户端")?;
    let time = read_json(
        client
            .get(format!("{BINANCE_FUTURES_BASE}/fapi/v1/time"))
            .send()
            .context("无法连接 Binance Futures")?,
        "同步 Binance 服务器时间失败",
    )?;
    let server_time = time["serverTime"]
        .as_i64()
        .context("Binance 服务器时间响应无效")?;
    let query = format!("timestamp={server_time}&recvWindow=5000");
    let signature = sign_query(api_secret, &query)?;
    let account = read_json(
        client
            .get(format!(
                "{BINANCE_FUTURES_BASE}/fapi/v3/account?{query}&signature={signature}"
            ))
            .header("X-MBX-APIKEY", api_key)
            .send()
            .context("Binance Futures 凭据验证请求失败")?,
        "Binance Futures 凭据验证失败",
    )?;
    if !account["canTrade"].as_bool().unwrap_or(false) {
        bail!("API Key 有效，但未开通 Binance Futures 交易权限");
    }
    Ok("Binance Futures API Key、Secret 和交易权限验证通过".into())
}

fn sign_query(secret: &str, query: &str) -> Result<String> {
    let mut signer = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| anyhow!("Binance API Secret 无效"))?;
    signer.update(query.as_bytes());
    Ok(hex::encode(signer.finalize().into_bytes()))
}

fn read_json(response: Response, context: &str) -> Result<Value> {
    let status = response.status();
    let body = response.text().context("无法读取 Binance 返回")?;
    let value = serde_json::from_str::<Value>(&body).context("Binance 返回不是有效 JSON")?;
    if status.is_success() {
        return Ok(value);
    }

    let code = value["code"].as_i64().unwrap_or_default();
    let message = match code {
        -1022 => "Secret 不正确，签名验证失败".to_string(),
        -2014 => "API Key 格式无效".to_string(),
        -2015 => "API Key 无效，或当前 IP/合约权限不在允许范围".to_string(),
        _ => value["msg"]
            .as_str()
            .unwrap_or("Binance 拒绝了验证请求")
            .to_string(),
    };
    bail!("{context}: {message}（错误码 {code}）")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_query_with_hmac_sha256() {
        assert_eq!(
            sign_query("key", "The quick brown fox jumps over the lazy dog").unwrap(),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }
}
