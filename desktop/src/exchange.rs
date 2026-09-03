use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use hmac::{Hmac, Mac};
use reqwest::blocking::{Client, Response};
use serde_json::Value;
use sha2::Sha256;

use crate::model::{FuturesAccount, FuturesPosition};
use crate::network;

const BINANCE_FUTURES_BASE: &str = "https://fapi.binance.com";

pub fn validate_futures_credentials(api_key: &str, api_secret: &str) -> Result<String> {
    let account = validate_and_fetch_account(api_key, api_secret)?;
    if account["canTrade"].as_bool().unwrap_or(false) {
        Ok("Binance Futures API Key、Secret 和交易权限验证通过".into())
    } else {
        Ok(
            "Binance API Key 和 Secret 验证通过；当前账户返回 canTrade=false，已按只读/模拟盘模式保存，实盘启动前仍会被阻止"
                .into(),
        )
    }
}

pub fn ensure_live_futures_trading(api_key: &str, api_secret: &str) -> Result<()> {
    let account = validate_and_fetch_account(api_key, api_secret)?;
    if !account["canTrade"].as_bool().unwrap_or(false) {
        bail!(
            "Binance 账户当前返回 canTrade=false，不能启动实盘。请先在 Binance 官方账户中开通 USDⓈ-M Futures，并确认 API Key 的合约交易权限已经保存生效"
        );
    }
    Ok(())
}

fn validate_and_fetch_account(api_key: &str, api_secret: &str) -> Result<Value> {
    let api_key = api_key.trim();
    let api_secret = api_secret.trim();
    if !(10..=512).contains(&api_key.len()) || !(10..=512).contains(&api_secret.len()) {
        bail!("Binance API Key 或 Secret 长度无效");
    }

    let client = futures_client()?;
    let server_time = server_time(&client)?;
    signed_get(
        &client,
        "/fapi/v3/account",
        api_key,
        api_secret,
        server_time,
    )
}

pub fn fetch_futures_account(api_key: &str, api_secret: &str) -> Result<FuturesAccount> {
    let client = futures_client()?;
    let server_time = server_time(&client)?;
    let account = signed_get(
        &client,
        "/fapi/v3/account",
        api_key.trim(),
        api_secret.trim(),
        server_time,
    )?;
    let position_data = signed_get(
        &client,
        "/fapi/v3/positionRisk",
        api_key.trim(),
        api_secret.trim(),
        server_time,
    )?;
    parse_futures_account(&account, &position_data)
}

fn parse_futures_account(account: &Value, position_data: &Value) -> Result<FuturesAccount> {
    let mut positions = position_data
        .as_array()
        .context("Binance 持仓响应无效")?
        .iter()
        .filter_map(|position| {
            let amount = number(position, "positionAmt");
            (amount.abs() > f64::EPSILON).then(|| FuturesPosition {
                symbol: position["symbol"].as_str().unwrap_or_default().to_string(),
                side: if amount >= 0.0 { "多" } else { "空" }.into(),
                quantity: amount.abs(),
                entry_price: number(position, "entryPrice"),
                mark_price: number(position, "markPrice"),
                leverage: position["leverage"]
                    .as_str()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(1),
                unrealized_profit: number(position, "unRealizedProfit"),
                liquidation_price: number(position, "liquidationPrice"),
                margin_type: position["marginType"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            })
        })
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| left.symbol.cmp(&right.symbol));

    Ok(FuturesAccount {
        wallet_balance: number(account, "totalWalletBalance"),
        available_balance: number(account, "availableBalance"),
        margin_balance: number(account, "totalMarginBalance"),
        unrealized_profit: number(account, "totalUnrealizedProfit"),
        initial_margin: number(account, "totalInitialMargin"),
        maintenance_margin: number(account, "totalMaintMargin"),
        positions,
        updated_at: chrono::Utc::now().timestamp_millis(),
    })
}

fn futures_client() -> Result<Client> {
    network::binance_client(Duration::from_secs(15)).context("无法创建 Binance 请求客户端")
}

fn server_time(client: &Client) -> Result<i64> {
    let time = read_json(
        client
            .get(format!("{BINANCE_FUTURES_BASE}/fapi/v1/time"))
            .send()
            .context("无法连接 Binance Futures")?,
        "同步 Binance 服务器时间失败",
    )?;
    time["serverTime"]
        .as_i64()
        .context("Binance 服务器时间响应无效")
}

fn signed_get(
    client: &Client,
    path: &str,
    api_key: &str,
    api_secret: &str,
    server_time: i64,
) -> Result<Value> {
    let query = format!("timestamp={server_time}&recvWindow=5000");
    let signature = sign_query(api_secret, &query)?;
    read_json(
        client
            .get(format!(
                "{BINANCE_FUTURES_BASE}{path}?{query}&signature={signature}"
            ))
            .header("X-MBX-APIKEY", api_key)
            .send()
            .context("Binance Futures 签名请求失败")?,
        "Binance Futures 签名请求被拒绝",
    )
}

fn number(value: &Value, key: &str) -> f64 {
    value[key]
        .as_str()
        .and_then(|value| value.parse().ok())
        .or_else(|| value[key].as_f64())
        .unwrap_or_default()
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
    if status.as_u16() == 451
        || value["msg"]
            .as_str()
            .is_some_and(|message| message.to_ascii_lowercase().contains("restricted location"))
    {
        bail!(
            "{context}: Binance Futures 拒绝了当前网络位置（HTTP 451 restricted location）。实时模拟盘和实盘都需要能访问 Binance Futures 的合规网络；当前只能使用离线回测或已有本地数据测试。（错误码 {code}）"
        )
    }
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

    #[test]
    fn parses_account_balances_and_active_positions() {
        let account = serde_json::json!({
            "totalWalletBalance": "1250.50",
            "availableBalance": "900.25",
            "totalMarginBalance": "1275.75",
            "totalUnrealizedProfit": "25.25",
            "totalInitialMargin": "100.0",
            "totalMaintMargin": "5.0"
        });
        let positions = serde_json::json!([
            {
                "symbol": "BTCUSDT", "positionAmt": "0.01", "entryPrice": "60000",
                "markPrice": "61000", "leverage": "2", "unRealizedProfit": "10",
                "liquidationPrice": "30000", "marginType": "isolated"
            },
            {"symbol": "ETHUSDT", "positionAmt": "0"}
        ]);
        let parsed = parse_futures_account(&account, &positions).unwrap();
        assert_eq!(parsed.wallet_balance, 1250.5);
        assert_eq!(parsed.available_balance, 900.25);
        assert_eq!(parsed.positions.len(), 1);
        assert_eq!(parsed.positions[0].side, "多");
        assert_eq!(parsed.positions[0].leverage, 2);
    }
}
