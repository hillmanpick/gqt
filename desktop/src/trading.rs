use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use chrono::{Duration as ChronoDuration, NaiveDate};
use rand::{RngCore, rngs::OsRng};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use serde_json::{Value, json};

use crate::model::{
    AiTradingConfig, MarginMode, PositionHistory, SimulationAccount, SimulationTrade,
    default_ai_symbol_whitelist,
};

const DEFAULT_CONFIG: &str = include_str!("../../trading/user_data/config.json");
const DEFAULT_STRATEGY: &str =
    include_str!("../../trading/user_data/strategies/FuturesFactorStrategy.py");
const DEFAULT_AI_STRATEGY: &str =
    include_str!("../../trading/user_data/strategies/AiSignalStrategy.py");
const DEFAULT_AI_CONFIG: &str = include_str!("../../trading/user_data/ai_config.json");
const DEFAULT_COMPOSE: &str = include_str!("../../trading/docker-compose.yml");

#[derive(Debug, Clone)]
pub struct TradingWorkspace {
    pub root: PathBuf,
    pub config: PathBuf,
    pub strategy: PathBuf,
    pub ai_strategy: PathBuf,
    pub ai_config: PathBuf,
    pub ai_signals: PathBuf,
    pub ai_audit: PathBuf,
}

impl TradingWorkspace {
    pub fn ensure(root: &Path) -> Result<Self> {
        let strategy_dir = root.join("user_data").join("strategies");
        fs::create_dir_all(&strategy_dir).context("无法创建策略目录")?;
        let config = root.join("user_data").join("config.json");
        let strategy = strategy_dir.join("FuturesFactorStrategy.py");
        let ai_strategy = strategy_dir.join("AiSignalStrategy.py");
        let ai_config = root.join("user_data").join("ai_config.json");
        let compose = root.join("docker-compose.yml");
        write_if_missing(&config, DEFAULT_CONFIG)?;
        migrate_config(&config)?;
        write_if_missing(&strategy, DEFAULT_STRATEGY)?;
        write_if_missing(&ai_strategy, DEFAULT_AI_STRATEGY)?;
        write_if_missing(&ai_config, DEFAULT_AI_CONFIG)?;
        migrate_ai_config(&ai_config)?;
        write_if_missing(&compose, DEFAULT_COMPOSE)?;
        migrate_compose(&compose)?;
        let workspace = Self {
            root: root.to_path_buf(),
            config,
            strategy,
            ai_strategy,
            ai_config,
            ai_signals: root.join("user_data").join("ai_signals.json"),
            ai_audit: root.join("user_data").join("ai_audit.sqlite"),
        };
        let ai_config = workspace.ai_trading_config().unwrap_or_default();
        workspace.sync_ai_runtime_config(&ai_config)?;
        Ok(workspace)
    }

    pub fn ai_trading_config(&self) -> Result<AiTradingConfig> {
        let config = serde_json::from_str(&fs::read_to_string(&self.ai_config)?)?;
        crate::ai_trader::validate_config(&config)?;
        Ok(config)
    }

    pub fn save_ai_trading_config(&self, config: &AiTradingConfig) -> Result<()> {
        crate::ai_trader::validate_config(config)?;
        write_json_atomic(&self.ai_config, config).context("无法保存 AI 自动交易配置")?;
        self.sync_ai_runtime_config(config)?;
        Ok(())
    }

    fn sync_ai_runtime_config(&self, ai_config: &AiTradingConfig) -> Result<()> {
        let pairs = normalized_pairs(&ai_config.symbol_whitelist)?;
        let mut config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        let exchange = config
            .as_object_mut()
            .context("Freqtrade 配置根节点无效")?
            .entry("exchange")
            .or_insert_with(|| json!({}));
        let exchange = exchange.as_object_mut().context("exchange 配置无效")?;
        exchange.insert("pair_whitelist".into(), json!(pairs));
        ensure_order_book_pricing(&mut config)?;
        config["stake_amount"] = json!(ai_config.max_stake_amount);
        config["gqt_max_stake_amount"] = json!(ai_config.max_stake_amount);
        config["gqt_risk_reward_ratio"] = json!(ai_config.risk_reward_ratio);
        config["gqt_ai_risk_sizing"] = json!(ai_config.allow_ai_risk_sizing);
        if ai_config.enabled {
            config["strategy"] = json!("AiSignalStrategy");
            config["timeframe"] = json!(ai_config.timeframe);
            config["margin_mode"] = json!(match ai_config.margin_mode {
                MarginMode::Cross => "cross",
                MarginMode::Isolated => "isolated",
            });
        } else if config["strategy"].as_str() == Some("AiSignalStrategy") {
            config["strategy"] = json!("FuturesFactorStrategy");
            config["timeframe"] = json!("4h");
            config["margin_mode"] = json!("isolated");
        }
        write_json_atomic(&self.config, &config).context("无法同步 Freqtrade 交易白名单")?;
        Ok(())
    }

    pub fn strategy_source(&self) -> Result<String> {
        Ok(fs::read_to_string(&self.strategy)?)
    }

    pub fn save_strategy(&self, source: &str) -> Result<()> {
        if source.len() < 80 || source.len() > 500_000 {
            bail!("策略源码长度无效");
        }
        let mut child = background_command(python_command())
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

    pub fn risk(&self) -> Result<(f64, i64, f64, bool)> {
        let config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        let ai_config = self.ai_trading_config().unwrap_or_default();
        Ok((
            config["gqt_max_stake_amount"]
                .as_f64()
                .or_else(|| config["stake_amount"].as_f64())
                .unwrap_or(ai_config.max_stake_amount),
            config["max_open_trades"].as_i64().unwrap_or(3),
            config["gqt_risk_reward_ratio"]
                .as_f64()
                .unwrap_or(ai_config.risk_reward_ratio),
            config["gqt_ai_risk_sizing"]
                .as_bool()
                .unwrap_or(ai_config.allow_ai_risk_sizing),
        ))
    }

    pub fn dry_run(&self) -> Result<bool> {
        let config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        Ok(config["dry_run"].as_bool().unwrap_or(true))
    }

    pub fn update_mode(&self, dry_run: bool) -> Result<()> {
        let mut config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        config["dry_run"] = Value::from(dry_run);
        write_json_atomic(&self.config, &config)?;
        Ok(())
    }

    pub fn update_risk(
        &self,
        max_stake: f64,
        max_trades: i64,
        risk_reward_ratio: f64,
        allow_ai_risk_sizing: bool,
    ) -> Result<()> {
        if !max_stake.is_finite()
            || !(5.0..=1_000_000.0).contains(&max_stake)
            || !(1..=20).contains(&max_trades)
            || !risk_reward_ratio.is_finite()
            || !(0.5..=10.0).contains(&risk_reward_ratio)
        {
            bail!("风控参数超出允许范围");
        }
        let mut config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        config["stake_amount"] = Value::from(max_stake);
        config["gqt_max_stake_amount"] = Value::from(max_stake);
        config["max_open_trades"] = Value::from(max_trades);
        config["gqt_risk_reward_ratio"] = Value::from(risk_reward_ratio);
        config["gqt_ai_risk_sizing"] = Value::from(allow_ai_risk_sizing);
        if config["liquidation_buffer"].as_f64().is_none() {
            config["liquidation_buffer"] = Value::from(0.15);
        }
        write_json_atomic(&self.config, &config)?;

        let mut ai_config = self.ai_trading_config().unwrap_or_default();
        ai_config.max_stake_amount = max_stake;
        ai_config.risk_reward_ratio = risk_reward_ratio;
        ai_config.allow_ai_risk_sizing = allow_ai_risk_sizing;
        crate::ai_trader::validate_config(&ai_config)?;
        write_json_atomic(&self.ai_config, &ai_config)?;
        Ok(())
    }

    pub fn docker_state(&self) -> (bool, String) {
        let available = background_command("docker")
            .args(["info", "--format", "{{.ServerVersion}}"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        if !available {
            return (false, "Docker 不可用".into());
        }
        let status = background_command("docker")
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

    pub fn runtime_uses_strategy_override(&self) -> bool {
        background_command("docker")
            .args([
                "inspect",
                "--format",
                "{{json .Config.Cmd}}",
                "binance-futures-factor",
            ])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).contains("\"--strategy\""))
            .unwrap_or(false)
    }

    pub fn bot_action(
        &self,
        start: bool,
        force_recreate: bool,
        api_key: &str,
        api_secret: &str,
    ) -> Result<String> {
        let args: Vec<&str> = if start {
            let mut args = vec!["compose", "up", "-d"];
            if force_recreate {
                args.push("--force-recreate");
            }
            args
        } else {
            vec!["compose", "stop", "--timeout", "30", "freqtrade"]
        };
        let database_url = if self.dry_run()? {
            "sqlite:////freqtrade/user_data/tradesv3.dryrun.sqlite"
        } else {
            "sqlite:////freqtrade/user_data/tradesv3.live.sqlite"
        };
        let output = background_command("docker")
            .args(&args)
            .current_dir(&self.root)
            .env("BINANCE_API_KEY", api_key)
            .env("BINANCE_API_SECRET", api_secret)
            .env("FREQTRADE_DB_URL", database_url)
            .output()
            .context("无法启动 Docker")?;
        if !output.status.success() {
            bail!("{}", String::from_utf8_lossy(&output.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    pub fn simulation_account(&self) -> Result<SimulationAccount> {
        let config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        let initial_wallet = config["dry_run_wallet"].as_f64().unwrap_or(1000.0);
        let database = self.root.join("user_data").join("tradesv3.dryrun.sqlite");
        if !database.exists() {
            return Ok(SimulationAccount {
                wallet_balance: initial_wallet,
                available_balance: initial_wallet,
                ..Default::default()
            });
        }

        let connection = Connection::open_with_flags(
            database,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .context("无法读取模拟交易数据库")?;
        let (realized_profit, closed_trades, winning_trades): (f64, i64, i64) = connection
            .query_row(
                "SELECT
                    COALESCE(SUM(COALESCE(close_profit_abs, realized_profit, 0)), 0),
                    COUNT(*),
                    COALESCE(SUM(CASE WHEN COALESCE(close_profit_abs, realized_profit, 0) > 0 THEN 1 ELSE 0 END), 0)
                 FROM trades WHERE is_open = 0",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("无法统计模拟交易盈亏")?;
        let mut statement = connection.prepare(
            "SELECT pair, is_short, amount, stake_amount, open_rate, leverage,
                    open_date, COALESCE(enter_tag, '')
             FROM trades WHERE is_open = 1 ORDER BY open_date DESC",
        )?;
        let open_trades = statement
            .query_map([], |row| {
                Ok(SimulationTrade {
                    pair: row.get(0)?,
                    side: if row.get::<_, bool>(1)? { "空" } else { "多" }.into(),
                    amount: row.get::<_, Option<f64>>(2)?.unwrap_or_default(),
                    stake_amount: row.get::<_, Option<f64>>(3)?.unwrap_or_default(),
                    open_rate: row.get::<_, Option<f64>>(4)?.unwrap_or_default(),
                    leverage: row.get::<_, Option<f64>>(5)?.unwrap_or(1.0),
                    open_date: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
                    tag: row.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let open_stake = open_trades
            .iter()
            .map(|trade| trade.stake_amount)
            .sum::<f64>();
        let trade_history = recent_position_history(&connection, 80)?;
        let wallet_balance = initial_wallet + realized_profit;
        Ok(SimulationAccount {
            wallet_balance,
            available_balance: (wallet_balance - open_stake).max(0.0),
            realized_profit,
            open_stake,
            closed_trades,
            winning_trades,
            open_trades,
            trade_history,
        })
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
        let data_start = start - ChronoDuration::days(90);
        let mut download_command = vec![
            "download-data".into(),
            "--config".into(),
            "/freqtrade/user_data/config.json".into(),
            "--exchange".into(),
            "binance".into(),
            "--trading-mode".into(),
            "futures".into(),
            "--timeframes".into(),
            "4h".into(),
            "--prepend".into(),
            "--timerange".into(),
            format!("{}-{}", data_start.format("%Y%m%d"), end.format("%Y%m%d")),
            "--pairs".into(),
        ];
        download_command.extend(pairs.clone());
        let download_log = self
            .run_compose(&download_command)
            .context("回测前同步历史数据失败")?;

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
        let backtest_log = self.run_compose(&command)?;
        Ok(format!(
            "===== 历史数据同步 =====\n{}\n\n===== 回测结果 =====\n{}",
            download_log, backtest_log
        ))
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
            "--prepend".into(),
            "--days".into(),
            days.to_string(),
            "--pairs".into(),
        ];
        command.extend(pairs);
        self.run_compose(&command)
    }

    pub fn logs(&self) -> String {
        background_command("docker")
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
        let output = background_command("docker")
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

fn recent_position_history(connection: &Connection, limit: usize) -> Result<Vec<PositionHistory>> {
    let mut statement = connection.prepare(
        "SELECT pair, is_open, is_short, amount, stake_amount, open_rate,
                close_rate, leverage, open_date, COALESCE(close_date, ''),
                COALESCE(enter_tag, ''), COALESCE(exit_reason, ''),
                COALESCE(close_profit_abs, realized_profit, 0),
                COALESCE(close_profit, 0)
         FROM trades
         ORDER BY COALESCE(close_date, open_date) DESC
         LIMIT ?1",
    )?;
    let rows = statement.query_map([limit as i64], |row| {
        let is_open = row.get::<_, bool>(1)?;
        let is_short = row.get::<_, bool>(2)?;
        Ok(PositionHistory {
            pair: row.get(0)?,
            status: if is_open { "持仓中" } else { "已平仓" }.into(),
            side: if is_short { "空" } else { "多" }.into(),
            amount: row.get::<_, Option<f64>>(3)?.unwrap_or_default(),
            stake_amount: row.get::<_, Option<f64>>(4)?.unwrap_or_default(),
            open_rate: row.get::<_, Option<f64>>(5)?.unwrap_or_default(),
            close_rate: row.get(6)?,
            leverage: row.get::<_, Option<f64>>(7)?.unwrap_or(1.0),
            open_date: row.get::<_, Option<String>>(8)?.unwrap_or_default(),
            close_date: row.get::<_, Option<String>>(9)?.unwrap_or_default(),
            tag: row.get(10)?,
            exit_reason: row.get(11)?,
            profit_abs: row.get::<_, Option<f64>>(12)?.unwrap_or_default(),
            profit_percent: row.get::<_, Option<f64>>(13)?.unwrap_or_default() * 100.0,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("无法读取仓位历史")
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

fn migrate_ai_config(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("无法读取 AI 自动交易配置")?;
    let source_value: Value = serde_json::from_str(&source).context("AI 自动交易配置 JSON 无效")?;
    let mut config: AiTradingConfig =
        serde_json::from_str(&source).context("AI 自动交易配置 JSON 无效")?;
    let mut should_write = [
        "minimum_long_score",
        "minimum_short_score",
        "minimum_factor_score",
        "minimum_trend_quality",
        "minimum_adx",
        "minimum_volume_ratio",
    ]
    .iter()
    .any(|key| source_value.get(*key).is_none());
    if config.symbol_whitelist == ["BTCUSDT".to_string(), "ETHUSDT".to_string()] {
        config.symbol_whitelist = default_ai_symbol_whitelist();
        should_write = true;
    }
    if should_write {
        write_json_atomic(path, &config).context("无法迁移 AI 默认配置")?;
    }
    Ok(())
}

fn write_json_atomic<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    write_text_atomic(path, &format!("{}\n", serde_json::to_string_pretty(value)?))
}

fn write_text_atomic(path: &Path, value: &str) -> Result<()> {
    let temporary = unique_temporary_path(path);
    fs::write(&temporary, value)?;
    replace_file(&temporary, path)?;
    Ok(())
}

fn replace_file(temporary: &Path, target: &Path) -> Result<()> {
    #[cfg(windows)]
    if target.exists() {
        fs::remove_file(target)
            .with_context(|| format!("无法替换现有文件 {}", target.to_string_lossy()))?;
    }
    fs::rename(temporary, target).with_context(|| {
        format!(
            "无法把 {} 替换为 {}",
            temporary.to_string_lossy(),
            target.to_string_lossy()
        )
    })?;
    Ok(())
}

fn unique_temporary_path(path: &Path) -> PathBuf {
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("config.json");
    path.with_file_name(format!("{file_name}.{}.tmp", rand::random::<u64>()))
}

fn migrate_config(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("无法读取 Freqtrade 配置")?;
    let mut config: Value = serde_json::from_str(&source).context("Freqtrade 配置 JSON 无效")?;
    ensure_order_book_pricing(&mut config)?;
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

fn ensure_order_book_pricing(config: &mut Value) -> Result<()> {
    let root = config.as_object_mut().context("Freqtrade 配置根节点无效")?;
    for key in ["entry_pricing", "exit_pricing"] {
        let pricing = root.entry(key).or_insert_with(|| json!({}));
        let pricing = pricing
            .as_object_mut()
            .with_context(|| format!("{key} 配置无效"))?;
        pricing.insert("use_order_book".into(), json!(true));
        pricing.entry("order_book_top").or_insert(json!(1));
    }
    Ok(())
}

fn migrate_compose(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("无法读取 Docker Compose 配置")?;
    let updated = source
        .replace(
            "sqlite:////freqtrade/user_data/tradesv3.sqlite",
            "${FREQTRADE_DB_URL:-sqlite:////freqtrade/user_data/tradesv3.dryrun.sqlite}",
        )
        .replace("\n      --strategy FuturesFactorStrategy", "");
    if updated != source {
        fs::write(path, updated).context("无法迁移 Docker Compose 配置")?;
    }
    Ok(())
}

fn random_config_secret() -> String {
    let mut value = [0_u8; 32];
    OsRng.fill_bytes(&mut value);
    hex::encode(value)
}

fn background_command(program: &str) -> Command {
    let mut command = Command::new(program);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
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
        assert_eq!(config["entry_pricing"]["use_order_book"], true);
        assert_eq!(config["entry_pricing"]["order_book_top"], 1);
        assert_eq!(config["exit_pricing"]["use_order_book"], true);
        assert_eq!(config["exit_pricing"]["order_book_top"], 1);
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
    fn migrates_compose_strategy_override() {
        let path = std::env::temp_dir().join(format!("gqt-compose-{}.yml", rand::random::<u64>()));
        fs::write(
            &path,
            r#"services:
  freqtrade:
    command: >
      trade
      --db-url sqlite:////freqtrade/user_data/tradesv3.sqlite
      --config /freqtrade/user_data/config.json
      --strategy FuturesFactorStrategy
"#,
        )
        .unwrap();
        migrate_compose(&path).unwrap();
        let migrated = fs::read_to_string(&path).unwrap();
        assert!(!migrated.contains("--strategy FuturesFactorStrategy"));
        assert!(migrated.contains("FREQTRADE_DB_URL"));
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

    #[test]
    fn updates_risk_limits_and_ai_sizing_config() {
        let root = std::env::temp_dir().join(format!("gqt-risk-{}", rand::random::<u64>()));
        let workspace = TradingWorkspace::ensure(&root).unwrap();
        workspace.update_risk(500.0, 10, 3.0, true).unwrap();
        assert_eq!(workspace.risk().unwrap(), (500.0, 10, 3.0, true));
        let ai_config = workspace.ai_trading_config().unwrap();
        assert_eq!(ai_config.max_stake_amount, 500.0);
        assert_eq!(ai_config.risk_reward_ratio, 3.0);
        assert!(ai_config.allow_ai_risk_sizing);
        let config: Value =
            serde_json::from_str(&fs::read_to_string(&workspace.config).unwrap()).unwrap();
        assert_eq!(config["stake_amount"], 500.0);
        assert_eq!(config["gqt_risk_reward_ratio"], 3.0);
        assert_eq!(config["gqt_ai_risk_sizing"], true);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn saves_ai_config_and_syncs_runtime_whitelist() {
        let root = std::env::temp_dir().join(format!("gqt-ai-runtime-{}", rand::random::<u64>()));
        let workspace = TradingWorkspace::ensure(&root).unwrap();
        let ai_config = AiTradingConfig {
            enabled: true,
            symbol_whitelist: vec!["SOLUSDT".into(), "DOGEUSDT".into()],
            timeframe: "1h".into(),
            margin_mode: MarginMode::Cross,
            leverage: 3,
            ..Default::default()
        };
        workspace.save_ai_trading_config(&ai_config).unwrap();
        workspace.save_ai_trading_config(&ai_config).unwrap();
        let stored_ai = workspace.ai_trading_config().unwrap();
        assert_eq!(
            stored_ai.symbol_whitelist,
            vec!["SOLUSDT".to_string(), "DOGEUSDT".to_string()]
        );
        let config: Value =
            serde_json::from_str(&fs::read_to_string(&workspace.config).unwrap()).unwrap();
        assert_eq!(config["strategy"], "AiSignalStrategy");
        assert_eq!(config["timeframe"], "1h");
        assert_eq!(config["margin_mode"], "cross");
        assert_eq!(
            config["exchange"]["pair_whitelist"],
            json!(["SOL/USDT:USDT", "DOGE/USDT:USDT"])
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn migrates_legacy_two_symbol_ai_whitelist() {
        let root = std::env::temp_dir().join(format!("gqt-ai-migrate-{}", rand::random::<u64>()));
        let user_data = root.join("user_data");
        fs::create_dir_all(&user_data).unwrap();
        fs::write(
            user_data.join("ai_config.json"),
            r#"{
              "enabled": false,
              "dry_run_only": true,
              "symbol_whitelist": ["BTCUSDT", "ETHUSDT"],
              "timeframe": "1h",
              "margin_mode": "cross",
              "leverage": 2,
              "capital_usage_percent": 10.0,
              "minimum_confidence": 0.75,
              "model_timeout_seconds": 30,
              "market_max_age_seconds": 90,
              "one_signal_per_candle": true
            }"#,
        )
        .unwrap();
        let workspace = TradingWorkspace::ensure(&root).unwrap();
        let stored_ai = workspace.ai_trading_config().unwrap();
        assert_eq!(stored_ai.symbol_whitelist, default_ai_symbol_whitelist());
        let config: Value =
            serde_json::from_str(&fs::read_to_string(&workspace.config).unwrap()).unwrap();
        assert_eq!(
            config["exchange"]["pair_whitelist"]
                .as_array()
                .unwrap()
                .len(),
            10
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reads_simulation_wallet_and_trades() {
        let root = std::env::temp_dir().join(format!("gqt-sim-{}", rand::random::<u64>()));
        let workspace = TradingWorkspace::ensure(&root).unwrap();
        let database = root.join("user_data").join("tradesv3.dryrun.sqlite");
        let connection = Connection::open(database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE trades (
                    pair TEXT, is_open INTEGER, is_short INTEGER, amount REAL,
                    stake_amount REAL, open_rate REAL, leverage REAL, open_date TEXT,
                    enter_tag TEXT, close_profit_abs REAL, realized_profit REAL,
                    close_rate REAL, close_profit REAL, close_date TEXT, exit_reason TEXT
                 );
                 INSERT INTO trades VALUES
                    ('BTC/USDT:USDT', 0, 0, 0.01, 100, 60000, 2, '2026-01-01', 'factor_long', 25, 25, 62500, 0.25, '2026-01-02', 'roi'),
                    ('ETH/USDT:USDT', 1, 1, 0.1, 50, 3000, 2, '2026-02-01', 'factor_short', NULL, 0, NULL, NULL, NULL, NULL);",
            )
            .unwrap();
        drop(connection);
        let account = workspace.simulation_account().unwrap();
        assert_eq!(account.wallet_balance, 1025.0);
        assert_eq!(account.available_balance, 975.0);
        assert_eq!(account.closed_trades, 1);
        assert_eq!(account.open_trades[0].side, "空");
        assert_eq!(account.trade_history.len(), 2);
        assert_eq!(account.trade_history[0].status, "持仓中");
        assert_eq!(account.trade_history[1].exit_reason, "roi");
        let _ = fs::remove_dir_all(root);
    }
}
