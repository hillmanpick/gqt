use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;
use rand::{RngCore, rngs::OsRng};
use serde_json::{Value, json};

const DEFAULT_CONFIG: &str = include_str!("../../trading/user_data/config.json");
const DEFAULT_STRATEGY: &str =
    include_str!("../../trading/user_data/strategies/FuturesFactorStrategy.py");
const DEFAULT_COMPOSE: &str = include_str!("../../trading/docker-compose.yml");

#[derive(Debug, Clone)]
pub struct TradingWorkspace {
    pub root: PathBuf,
    pub config: PathBuf,
    pub strategy: PathBuf,
}

impl TradingWorkspace {
    pub fn ensure(root: &Path) -> Result<Self> {
        let strategy_dir = root.join("user_data").join("strategies");
        fs::create_dir_all(&strategy_dir).context("无法创建策略目录")?;
        let config = root.join("user_data").join("config.json");
        let strategy = strategy_dir.join("FuturesFactorStrategy.py");
        let compose = root.join("docker-compose.yml");
        write_if_missing(&config, DEFAULT_CONFIG)?;
        migrate_config(&config)?;
        write_if_missing(&strategy, DEFAULT_STRATEGY)?;
        write_if_missing(&compose, DEFAULT_COMPOSE)?;
        Ok(Self {
            root: root.to_path_buf(),
            config,
            strategy,
        })
    }

    pub fn strategy_source(&self) -> Result<String> {
        Ok(fs::read_to_string(&self.strategy)?)
    }

    pub fn save_strategy(&self, source: &str) -> Result<()> {
        if source.len() < 80 || source.len() > 500_000 {
            bail!("策略源码长度无效");
        }
        let mut child = Command::new(python_command())
            .args([
                "-c",
                "import ast,sys; ast.parse(sys.stdin.read()); print('ok')",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("无法启动 Python 语法校验")?;
        use std::io::Write;
        child
            .stdin
            .take()
            .context("无法写入 Python 校验进程")?
            .write_all(source.as_bytes())?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            bail!("{}", String::from_utf8_lossy(&output.stderr).trim());
        }
        fs::write(&self.strategy, source)?;
        Ok(())
    }

    pub fn risk(&self) -> Result<(f64, i64, f64)> {
        let config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        Ok((
            config["stake_amount"].as_f64().unwrap_or(50.0),
            config["max_open_trades"].as_i64().unwrap_or(3),
            config["liquidation_buffer"].as_f64().unwrap_or(0.15),
        ))
    }

    pub fn dry_run(&self) -> Result<bool> {
        let config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        Ok(config["dry_run"].as_bool().unwrap_or(true))
    }

    pub fn update_mode(&self, dry_run: bool) -> Result<()> {
        let mut config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        config["dry_run"] = Value::from(dry_run);
        fs::write(
            &self.config,
            format!("{}\n", serde_json::to_string_pretty(&config)?),
        )?;
        Ok(())
    }

    pub fn update_risk(&self, stake: f64, max_trades: i64, buffer: f64) -> Result<()> {
        if !(5.0..=1_000_000.0).contains(&stake)
            || !(1..=20).contains(&max_trades)
            || !(0.05..=0.5).contains(&buffer)
        {
            bail!("风控参数超出允许范围");
        }
        let mut config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        config["stake_amount"] = Value::from(stake);
        config["max_open_trades"] = Value::from(max_trades);
        config["liquidation_buffer"] = Value::from(buffer);
        fs::write(
            &self.config,
            format!("{}\n", serde_json::to_string_pretty(&config)?),
        )?;
        Ok(())
    }

    pub fn docker_state(&self) -> (bool, String) {
        let available = Command::new("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        if !available {
            return (false, "Docker 不可用".into());
        }
        let status = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{.State.Status}}",
                "binance-futures-factor",
            ])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
            .unwrap_or_else(|| "stopped".into());
        (true, status)
    }

    pub fn bot_action(&self, start: bool, api_key: &str, api_secret: &str) -> Result<String> {
        let args: &[&str] = if start {
            &["compose", "up", "-d"]
        } else {
            &["compose", "stop", "freqtrade"]
        };
        let output = Command::new("docker")
            .args(args)
            .current_dir(&self.root)
            .env("BINANCE_API_KEY", api_key)
            .env("BINANCE_API_SECRET", api_secret)
            .output()
            .context("无法启动 Docker")?;
        if !output.status.success() {
            bail!("{}", String::from_utf8_lossy(&output.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    pub fn run_backtest(
        &self,
        start: &str,
        end: &str,
        fee: f64,
        symbols: &[String],
    ) -> Result<String> {
        let start = NaiveDate::parse_from_str(start.trim(), "%Y-%m-%d")
            .context("开始日期格式应为 YYYY-MM-DD")?;
        let end = NaiveDate::parse_from_str(end.trim(), "%Y-%m-%d")
            .context("结束日期格式应为 YYYY-MM-DD")?;
        if end <= start {
            bail!("结束日期必须晚于开始日期");
        }
        if !(0.0..=0.01).contains(&fee) {
            bail!("回测费率超出允许范围");
        }
        let pairs = normalized_pairs(symbols)?;
        let mut command = vec![
            "backtesting".into(),
            "--config".into(),
            "/freqtrade/user_data/config.json".into(),
            "--strategy".into(),
            "FuturesFactorStrategy".into(),
            "--timerange".into(),
            format!("{}-{}", start.format("%Y%m%d"), end.format("%Y%m%d")),
            "--fee".into(),
            format!("{fee:.8}"),
            "--pairs".into(),
        ];
        command.extend(pairs);
        self.run_compose(&command)
    }

    pub fn download_data(&self, days: i64, symbols: &[String]) -> Result<String> {
        if !(30..=3650).contains(&days) {
            bail!("历史数据天数超出允许范围");
        }
        let pairs = normalized_pairs(symbols)?;
        let mut command = vec![
            "download-data".into(),
            "--config".into(),
            "/freqtrade/user_data/config.json".into(),
            "--exchange".into(),
            "binance".into(),
            "--trading-mode".into(),
            "futures".into(),
            "--timeframes".into(),
            "4h".into(),
            "--days".into(),
            days.to_string(),
            "--pairs".into(),
        ];
        command.extend(pairs);
        self.run_compose(&command)
    }

    pub fn logs(&self) -> String {
        Command::new("docker")
            .args([
                "compose",
                "logs",
                "--no-color",
                "--tail",
                "250",
                "freqtrade",
            ])
            .current_dir(&self.root)
            .output()
            .map(|output| {
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
            })
            .unwrap_or_else(|error| error.to_string())
    }

    fn run_compose(&self, command: &[String]) -> Result<String> {
        let mut args = vec![
            "compose".to_string(),
            "run".into(),
            "--rm".into(),
            "freqtrade".into(),
        ];
        args.extend_from_slice(command);
        let output = Command::new("docker")
            .args(&args)
            .current_dir(&self.root)
            .output()
            .context("无法启动 Docker，请确认 Docker Desktop 已运行")?;
        let log = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        if !output.status.success() {
            bail!("{}", log.trim());
        }
        Ok(log.trim().to_string())
    }
}

fn normalized_pairs(symbols: &[String]) -> Result<Vec<String>> {
    if symbols.is_empty() {
        bail!("请至少选择一个交易对");
    }
    symbols
        .iter()
        .map(|symbol| {
            let symbol = symbol.trim().to_ascii_uppercase();
            let base = symbol
                .strip_suffix("USDT")
                .filter(|base| {
                    (2..=12).contains(&base.len())
                        && base
                            .chars()
                            .all(|character| character.is_ascii_alphanumeric())
                })
                .context("交易对格式无效")?;
            Ok(format!("{base}/USDT:USDT"))
        })
        .collect()
}

fn write_if_missing(path: &Path, value: &str) -> Result<()> {
    if !path.exists() {
        fs::write(path, value)?;
    }
    Ok(())
}

fn migrate_config(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("无法读取 Freqtrade 配置")?;
    let mut config: Value = serde_json::from_str(&source).context("Freqtrade 配置 JSON 无效")?;
    let api = config
        .as_object_mut()
        .context("Freqtrade 配置根节点无效")?
        .entry("api_server")
        .or_insert_with(|| json!({}));
    let api = api.as_object_mut().context("api_server 配置无效")?;
    api.entry("enabled").or_insert(json!(false));
    api.entry("listen_ip_address").or_insert(json!("127.0.0.1"));
    api.entry("listen_port").or_insert(json!(8080));
    api.entry("username").or_insert(json!("gqt"));
    api.entry("password")
        .or_insert_with(|| json!(random_config_secret()));
    api.entry("jwt_secret_key")
        .or_insert_with(|| json!(random_config_secret()));
    api.entry("ws_token")
        .or_insert_with(|| json!(random_config_secret()));
    api.entry("CORS_origins").or_insert(json!([]));
    api.entry("verbosity").or_insert(json!("error"));

    config["initial_state"] = json!("running");
    let updated = format!("{}\n", serde_json::to_string_pretty(&config)?);
    if updated != source {
        fs::write(path, updated).context("无法更新 Freqtrade 配置")?;
    }
    Ok(())
}

fn random_config_secret() -> String {
    let mut value = [0_u8; 32];
    OsRng.fill_bytes(&mut value);
    hex::encode(value)
}

fn python_command() -> &'static str {
    if cfg!(windows) { "python" } else { "python3" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_binance_symbols_to_freqtrade_pairs() {
        let pairs = normalized_pairs(&["BTCUSDT".into(), "ethusdt".into()]).unwrap();
        assert_eq!(pairs, ["BTC/USDT:USDT", "ETH/USDT:USDT"]);
    }

    #[test]
    fn rejects_empty_or_invalid_pairs() {
        assert!(normalized_pairs(&[]).is_err());
        assert!(normalized_pairs(&["BTC/USDT".into()]).is_err());
    }

    #[test]
    fn migrates_legacy_api_server_config() {
        let path = std::env::temp_dir().join(format!(
            "gqt-freqtrade-config-{}.json",
            rand::random::<u64>()
        ));
        fs::write(
            &path,
            r#"{"dry_run":true,"api_server":{"enabled":false},"initial_state":"stopped"}"#,
        )
        .unwrap();
        migrate_config(&path).unwrap();
        let config: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(config["api_server"]["listen_ip_address"], "127.0.0.1");
        assert!(
            config["api_server"]["jwt_secret_key"]
                .as_str()
                .unwrap()
                .len()
                >= 32
        );
        assert_eq!(config["initial_state"], "running");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn updates_dry_run_mode() {
        let root = std::env::temp_dir().join(format!("gqt-mode-{}", rand::random::<u64>()));
        let workspace = TradingWorkspace::ensure(&root).unwrap();
        assert!(workspace.dry_run().unwrap());
        workspace.update_mode(false).unwrap();
        assert!(!workspace.dry_run().unwrap());
        let _ = fs::remove_dir_all(root);
    }
}
