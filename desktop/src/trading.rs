use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration as ChronoDuration, FixedOffset, NaiveDate, NaiveDateTime, Utc};
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
const DEFAULT_BINANCE_PROXY: &str = include_str!("../../trading/binance_proxy.py");

#[derive(Debug, Clone)]
pub struct TradingWorkspace {
    pub root: PathBuf,
    pub config: PathBuf,
    pub strategy: PathBuf,
    pub ai_strategy: PathBuf,
    pub ai_config: PathBuf,
    pub ai_signals: PathBuf,
    pub ai_audit: PathBuf,
    pub event_predictions: PathBuf,
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
        let binance_proxy = root.join("binance_proxy.py");
        write_if_missing(&config, DEFAULT_CONFIG)?;
        migrate_config(&config)?;
        write_if_missing(&strategy, DEFAULT_STRATEGY)?;
        migrate_default_strategy(&strategy)?;
        write_if_missing(&ai_strategy, DEFAULT_AI_STRATEGY)?;
        migrate_ai_strategy(&ai_strategy)?;
        write_if_missing(&ai_config, DEFAULT_AI_CONFIG)?;
        migrate_ai_config(&ai_config)?;
        fs::write(&binance_proxy, DEFAULT_BINANCE_PROXY)
            .context("无法更新 Binance Docker 出口桥")?;
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
            event_predictions: root.join("user_data").join("event_predictions.sqlite"),
        };
        let ai_config = workspace.ai_trading_config().unwrap_or_default();
        workspace.sync_ai_runtime_config(&ai_config)?;
        Ok(workspace)
    }

    pub fn ensure_binance_egress(&self) -> Result<()> {
        let output = background_command("docker")
            .args(["compose", "up", "-d", "binance-egress"])
            .current_dir(&self.root)
            .env("GQT_DOCKER_PROXY", "")
            .output()
            .context("无法启动 Binance Docker 出口桥")?;
        if !output.status.success() {
            bail!("{}", String::from_utf8_lossy(&output.stderr).trim());
        }

        let address = "127.0.0.1:18080"
            .parse()
            .expect("valid Binance proxy address");
        for _ in 0..30 {
            if std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_millis(100))
                .is_ok()
            {
                return Ok(());
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        bail!("Binance Docker 出口桥启动后未监听 127.0.0.1:18080")
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
        let all_futures_symbols = config["gqt_all_futures_symbols"].as_bool().unwrap_or(false);
        let exchange = config
            .as_object_mut()
            .context("Freqtrade 配置根节点无效")?
            .entry("exchange")
            .or_insert_with(|| json!({}));
        let exchange = exchange.as_object_mut().context("exchange 配置无效")?;
        if all_futures_symbols {
            exchange.remove("pair_whitelist");
            config["pairlists"] = json!([
                {
                    "method": "VolumePairList",
                    "number_assets": 1000,
                    "sort_key": "quoteVolume",
                    "min_value": 0,
                    "refresh_period": 900
                }
            ]);
        } else {
            exchange.insert("pair_whitelist".into(), json!(pairs));
            config["pairlists"] = json!([{ "method": "StaticPairList" }]);
        }
        ensure_order_book_pricing(&mut config)?;
        ensure_compound_defaults(&mut config)?;
        ensure_ccxt_proxy(&mut config, None)?;
        let profile_preset = ai_config.strategy_profile.preset();
        let effective_leverage = ai_config.leverage.max(1);
        config["gqt_strategy_profile"] = json!(ai_config.strategy_profile.as_str());
        config["gqt_compound_capital_usage_percent"] = json!(ai_config.capital_usage_percent);
        config["gqt_compound_take_profit"] = json!(profile_preset.take_profit);
        config["gqt_compound_stop_loss"] = json!(profile_preset.stop_loss);
        config["gqt_compound_pyramid_profit"] = json!(profile_preset.pyramid_profit);
        config["gqt_compound_pyramid_stake_ratio"] = json!(profile_preset.pyramid_stake_ratio);
        config["gqt_compound_leverage"] = json!(effective_leverage);
        config["gqt_fee_rate"] = json!(profile_preset.fee_rate);
        config["gqt_slippage_rate"] = json!(profile_preset.slippage_rate);
        config["gqt_min_net_profit"] = json!(profile_preset.min_net_profit);
        config["gqt_min_pyramid_net_profit"] = json!(profile_preset.min_pyramid_net_profit);
        config["gqt_time_roll_net_profit"] = json!(profile_preset.time_roll_net_profit);
        config["gqt_daily_profit_lock_enabled"] = json!(true);
        config["gqt_daily_profit_force_exit"] = json!(true);
        config["gqt_daily_profit_target"] = json!(profile_preset.daily_profit_target);
        config["gqt_daily_profit_timezone_offset_hours"] =
            json!(profile_preset.daily_profit_timezone_offset_hours);
        config["minimal_roi"] = json!({"0": 0.99});
        config["stoploss"] = json!(-0.045);
        config["trailing_stop"] = json!(false);
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
            config["timeframe"] = json!("5m");
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
        // A container with the fixed name can have been started from another
        // checkout (for example the repository's trading/ directory).  That
        // silently splits the simulated account from the desktop workspace.
        // Remove only that container when its bind mount is not this workspace;
        // the SQLite files remain untouched on disk.
        if start && !self.container_uses_workspace()? {
            let output = background_command("docker")
                .args(["rm", "-f", "binance-futures-factor"])
                .output()
                .context("无法移除指向其他工作区的交易容器")?;
            if !output.status.success() {
                bail!(
                    "无法移除指向其他工作区的交易容器: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
        }
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
        if start {
            let ai_config = self.ai_trading_config().unwrap_or_default();
            self.sync_ai_runtime_config(&ai_config)?;
        }
        let mut command = background_command("docker");
        command
            .args(&args)
            .current_dir(&self.root)
            .env("BINANCE_API_KEY", api_key)
            .env("BINANCE_API_SECRET", api_secret)
            .env("FREQTRADE_DB_URL", database_url);
        let output = command.output().context("无法启动 Docker")?;
        if !output.status.success() {
            bail!("{}", String::from_utf8_lossy(&output.stderr).trim());
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    fn container_uses_workspace(&self) -> Result<bool> {
        let output = background_command("docker")
            .args([
                "inspect",
                "--format",
                "{{json .Mounts}}",
                "binance-futures-factor",
            ])
            .output()
            .context("无法检查交易容器挂载")?;
        if !output.status.success() {
            // No container yet: compose up will create the correctly mounted one.
            return Ok(true);
        }
        let mounts: Value =
            serde_json::from_slice(&output.stdout).context("交易容器挂载信息无效")?;
        let expected = fs::canonicalize(&self.root)
            .unwrap_or_else(|_| self.root.clone())
            .to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase();
        Ok(mounts.as_array().is_some_and(|items| {
            items.iter().any(|mount| {
                mount["Destination"].as_str() == Some("/freqtrade/user_data")
                    && mount["Source"]
                        .as_str()
                        .map(|source| {
                            source.replace('\\', "/").to_ascii_lowercase()
                                == format!("{expected}/user_data")
                        })
                        .unwrap_or(false)
            })
        }))
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
                    open_date: format_trade_time(
                        &row.get::<_, Option<String>>(6)?.unwrap_or_default(),
                    ),
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
        let timeframe = self.configured_timeframe()?;
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
            timeframe,
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
        let timeframe = self.configured_timeframe()?;
        let mut command = vec![
            "download-data".into(),
            "--config".into(),
            "/freqtrade/user_data/config.json".into(),
            "--exchange".into(),
            "binance".into(),
            "--trading-mode".into(),
            "futures".into(),
            "--timeframes".into(),
            timeframe,
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

    fn configured_timeframe(&self) -> Result<String> {
        let config: Value = serde_json::from_str(&fs::read_to_string(&self.config)?)?;
        let timeframe = config["timeframe"].as_str().unwrap_or("15m");
        if !matches!(timeframe, "1m" | "5m" | "15m" | "1h" | "4h" | "1d") {
            bail!("Freqtrade timeframe is invalid: {timeframe}");
        }
        Ok(timeframe.to_string())
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
            open_date: format_trade_time(&row.get::<_, Option<String>>(8)?.unwrap_or_default()),
            close_date: format_trade_time(&row.get::<_, Option<String>>(9)?.unwrap_or_default()),
            tag: row.get(10)?,
            exit_reason: row.get(11)?,
            profit_abs: row.get::<_, Option<f64>>(12)?.unwrap_or_default(),
            profit_percent: row.get::<_, Option<f64>>(13)?.unwrap_or_default() * 100.0,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("无法读取仓位历史")
}

fn format_trade_time(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return String::new();
    }
    let zone = FixedOffset::east_opt(8 * 60 * 60).expect("UTC+8 is a valid fixed offset");
    if let Ok(time) = DateTime::parse_from_rfc3339(value) {
        return time
            .with_timezone(&zone)
            .format("%m-%d %H:%M:%S")
            .to_string();
    }
    for format in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
        if let Ok(time) = NaiveDateTime::parse_from_str(value, format) {
            return DateTime::<Utc>::from_naive_utc_and_offset(time, Utc)
                .with_timezone(&zone)
                .format("%m-%d %H:%M:%S")
                .to_string();
        }
    }
    value.to_string()
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

fn migrate_default_strategy(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("无法读取 Freqtrade 策略")?;
    let legacy_default = source
        .contains("Multi-factor Binance futures baseline for research and dry-run.")
        || (source.contains("timeframe = \"4h\"")
            && !source.contains("alpha_compound_long")
            && source.contains("FuturesFactorStrategy"))
        || (source.contains("class FuturesFactorStrategy")
            && source.contains("alpha_compound_long")
            && !source.contains("PROFILE_DEFAULTS"))
        || (source.contains("class FuturesFactorStrategy")
            && source.contains("PROFILE_DEFAULTS")
            && (!source.contains("gqt_fee_rate")
                || !source.contains("gqt_daily_profit_target")
                || !source.contains("gqt_daily_profit_timezone_offset_hours")));
    if legacy_default {
        fs::write(path, DEFAULT_STRATEGY).context("无法迁移默认 Freqtrade 策略")?;
    } else {
        let updated = source.replace(
            "process_only_new_candles = True",
            "process_only_new_candles = False",
        );
        if updated != source {
            fs::write(path, updated).context("无法更新 Freqtrade 策略处理频率")?;
        }
    }
    Ok(())
}

fn migrate_ai_strategy(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("Failed to read AI Freqtrade strategy")?;
    let legacy_default = source.contains("class AiSignalStrategy")
        && (!source.contains("gqt_fee_rate")
            || !source.contains("gqt_daily_profit_target")
            || !source.contains("gqt_daily_profit_timezone_offset_hours"));
    if legacy_default {
        fs::write(path, DEFAULT_AI_STRATEGY)
            .context("Failed to migrate default AI Freqtrade strategy")?;
    } else {
        let updated = source.replace(
            "process_only_new_candles = True",
            "process_only_new_candles = False",
        );
        if updated != source {
            fs::write(path, updated).context("Failed to update AI strategy processing cadence")?;
        }
    }
    Ok(())
}

fn migrate_ai_config(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("无法读取 AI 自动交易配置")?;
    let source_value: Value = serde_json::from_str(&source).context("AI 自动交易配置 JSON 无效")?;
    let mut config: AiTradingConfig =
        serde_json::from_str(&source).context("AI 自动交易配置 JSON 无效")?;
    let mut should_write = [
        "strategy_profile",
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
    if is_legacy_ai_default(&config) {
        config.timeframe = "15m".into();
        config.margin_mode = MarginMode::Isolated;
        config.max_stake_amount = 120.0;
        config.capital_usage_percent = 12.0;
        config.risk_reward_ratio = 1.4;
        config.minimum_long_score = 0.62;
        config.minimum_short_score = 0.62;
        config.minimum_factor_score = 0.12;
        config.minimum_trend_quality = 0.42;
        config.minimum_adx = 10.0;
        config.minimum_volume_ratio = -0.35;
        should_write = true;
    }
    if config.one_signal_per_candle {
        config.one_signal_per_candle = false;
        should_write = true;
    }
    if should_write {
        write_json_atomic(path, &config).context("无法迁移 AI 默认配置")?;
    }
    Ok(())
}

fn is_legacy_ai_default(config: &AiTradingConfig) -> bool {
    !config.enabled
        && config.timeframe == "4h"
        && config.margin_mode == MarginMode::Cross
        && nearly(config.max_stake_amount, 50.0)
        && nearly(config.capital_usage_percent, 10.0)
        && nearly(config.risk_reward_ratio, 2.0)
}

fn nearly(left: f64, right: f64) -> bool {
    (left - right).abs() < 1e-9
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
    let legacy_factor_default = config["strategy"].as_str() == Some("FuturesFactorStrategy")
        && config["timeframe"].as_str() == Some("4h")
        && nearly(config["stake_amount"].as_f64().unwrap_or(50.0), 50.0)
        && nearly(
            config["gqt_max_stake_amount"].as_f64().unwrap_or(50.0),
            50.0,
        )
        && nearly(config["gqt_risk_reward_ratio"].as_f64().unwrap_or(2.0), 2.0)
        && config["max_open_trades"].as_i64().unwrap_or(3) == 3;
    ensure_order_book_pricing(&mut config)?;
    ensure_compound_defaults(&mut config)?;
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
    if legacy_factor_default {
        config["max_open_trades"] = json!(5);
        config["stake_amount"] = json!(120.0);
        config["gqt_max_stake_amount"] = json!(120.0);
        config["gqt_risk_reward_ratio"] = json!(1.4);
        config["liquidation_buffer"] = json!(0.20);
        if let Some(timeout) = config["unfilledtimeout"].as_object_mut() {
            timeout.insert("entry".into(), json!(4));
            timeout.insert("exit".into(), json!(4));
        }
    }
    if config["strategy"].as_str() == Some("FuturesFactorStrategy")
        && config["timeframe"].as_str() == Some("4h")
    {
        config["timeframe"] = json!("15m");
    }
    let updated = format!("{}\n", serde_json::to_string_pretty(&config)?);
    if updated != source {
        fs::write(path, updated).context("无法更新 Freqtrade 配置")?;
    }
    Ok(())
}

fn ensure_compound_defaults(config: &mut Value) -> Result<()> {
    let root = config.as_object_mut().context("Freqtrade 配置根节点无效")?;
    let profile = root
        .entry("gqt_strategy_profile")
        .or_insert(json!("balanced"));
    if !matches!(
        profile.as_str(),
        Some("conservative" | "balanced" | "aggressive")
    ) {
        *profile = json!("balanced");
    }
    root.entry("gqt_compound_enabled").or_insert(json!(true));
    root.entry("gqt_compound_capital_usage_percent")
        .or_insert(json!(12.0));
    root.entry("gqt_compound_take_profit")
        .or_insert(json!(0.018));
    root.entry("gqt_compound_stop_loss").or_insert(json!(0.014));
    root.entry("gqt_compound_pyramid_profit")
        .or_insert(json!(0.006));
    root.entry("gqt_compound_pyramid_stake_ratio")
        .or_insert(json!(0.45));
    root.entry("gqt_compound_leverage").or_insert(json!(2));
    root.entry("gqt_execution_mode").or_insert(json!("paper"));
    root.entry("gqt_major_leverage_cap").or_insert(json!(50));
    root.entry("gqt_alt_leverage_cap").or_insert(json!(5));
    root.entry("gqt_sentiment_required").or_insert(json!(true));
    root.entry("gqt_sentiment_enabled").or_insert(json!(true));
    root.entry("gqt_all_futures_symbols").or_insert(json!(true));
    root.entry("gqt_paper_data_collection")
        .or_insert(json!(true));
    root.entry("gqt_paper_collection_hold_minutes")
        .or_insert(json!(3));
    root.entry("gqt_fee_rate").or_insert(json!(0.0005));
    root.entry("gqt_slippage_rate").or_insert(json!(0.0002));
    root.entry("gqt_min_net_profit").or_insert(json!(0.006));
    root.entry("gqt_min_pyramid_net_profit")
        .or_insert(json!(0.0025));
    root.entry("gqt_time_roll_net_profit")
        .or_insert(json!(0.0025));
    root.entry("gqt_daily_profit_lock_enabled")
        .or_insert(json!(true));
    root.entry("gqt_daily_profit_force_exit")
        .or_insert(json!(true));
    root.entry("gqt_daily_profit_target").or_insert(json!(0.10));
    root.entry("gqt_daily_profit_timezone_offset_hours")
        .or_insert(json!(8.0));
    root.insert("minimal_roi".into(), json!({"0": 0.99}));
    root.insert("stoploss".into(), json!(-0.045));
    root.insert("trailing_stop".into(), json!(false));
    root.entry("liquidation_buffer").or_insert(json!(0.20));
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

fn ensure_ccxt_proxy(config: &mut Value, proxy: Option<&str>) -> Result<()> {
    let root = config
        .as_object_mut()
        .context("Freqtrade config root is invalid")?;

    for key in ["ccxt_config", "ccxt_async_config"] {
        if root.get(key).is_some_and(|value| !value.is_object()) {
            bail!("legacy {key} config is invalid");
        }
    }
    let legacy_ccxt = ["ccxt_config", "ccxt_async_config"].map(|key| (key, root.remove(key)));
    let exchange = root
        .entry("exchange")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .context("exchange config is invalid")?;

    for (key, legacy) in legacy_ccxt {
        let ccxt = exchange.entry(key).or_insert_with(|| json!({}));
        let ccxt = ccxt
            .as_object_mut()
            .with_context(|| format!("{key} config is invalid"))?;
        if let Some(Value::Object(legacy)) = legacy {
            for (legacy_key, legacy_value) in legacy {
                if !ccxt.contains_key(&legacy_key) {
                    ccxt.insert(legacy_key, legacy_value);
                }
            }
        }
        ccxt.remove("httpProxy");
        ccxt.remove("httpsProxy");
        ccxt.remove("aiohttpProxy");
        if let Some(proxy) = proxy {
            ccxt.insert("httpsProxy".into(), json!(proxy));
        }
    }
    Ok(())
}

fn migrate_compose(path: &Path) -> Result<()> {
    let source = fs::read_to_string(path).context("无法读取 Docker Compose 配置")?;
    let mut updated = source
        .replace(
            "sqlite:////freqtrade/user_data/tradesv3.sqlite",
            "${FREQTRADE_DB_URL:-sqlite:////freqtrade/user_data/tradesv3.dryrun.sqlite}",
        )
        .replace("\n      --strategy FuturesFactorStrategy", "");
    let trailing_newline = updated.ends_with('\n');
    updated = updated
        .lines()
        .filter(|line| {
            ![
                "GQT_DOCKER_PROXY:",
                "HTTP_PROXY:",
                "HTTPS_PROXY:",
                "ALL_PROXY:",
                "http_proxy:",
                "https_proxy:",
                "all_proxy:",
            ]
            .iter()
            .any(|name| line.trim_start().starts_with(name))
        })
        .collect::<Vec<_>>()
        .join("\n");
    if trailing_newline {
        updated.push('\n');
    }
    if !updated.contains("\n  binance-egress:") {
        let service = "  binance-egress:\n    image: freqtradeorg/freqtrade:2026.6\n    container_name: gqt-binance-egress\n    restart: unless-stopped\n    entrypoint: [\"python\", \"/opt/binance_proxy.py\"]\n    volumes:\n      - ./binance_proxy.py:/opt/binance_proxy.py:ro\n    ports:\n      - \"127.0.0.1:18080:18080\"\n\n";
        let services_end = updated
            .find("services:")
            .and_then(|index| updated[index..].find('\n').map(|offset| index + offset + 1))
            .context("Docker Compose 缺少 services 节")?;
        updated.insert_str(services_end, service);
    }
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
        assert!(migrated.contains("binance-egress:"));
        assert!(!migrated.contains("GQT_DOCKER_PROXY"));
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
            leverage: 50,
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
        assert_eq!(config["gqt_compound_leverage"], 50);
        assert!(config["exchange"]["pair_whitelist"].is_null());
        assert_eq!(config["pairlists"][0]["method"], "VolumePairList");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stores_ccxt_proxy_under_exchange_and_migrates_legacy_values() {
        let mut config = json!({
            "exchange": {
                "name": "binance",
                "ccxt_config": {"enableRateLimit": true},
                "ccxt_async_config": {"enableRateLimit": true}
            },
            "ccxt_config": {
                "legacyOption": "keep-me",
                "httpProxy": "http://old-proxy:7890"
            },
            "ccxt_async_config": {
                "legacyAsyncOption": "keep-me-too",
                "aiohttpProxy": "http://old-proxy:7890"
            }
        });

        ensure_ccxt_proxy(&mut config, Some("http://host.docker.internal:7890")).unwrap();

        assert!(config.get("ccxt_config").is_none());
        assert!(config.get("ccxt_async_config").is_none());
        assert_eq!(config["exchange"]["ccxt_config"]["enableRateLimit"], true);
        assert_eq!(config["exchange"]["ccxt_config"]["legacyOption"], "keep-me");
        assert_eq!(
            config["exchange"]["ccxt_config"]["httpsProxy"],
            "http://host.docker.internal:7890"
        );
        assert!(config["exchange"]["ccxt_config"].get("httpProxy").is_none());
        assert!(
            config["exchange"]["ccxt_config"]
                .get("aiohttpProxy")
                .is_none()
        );
        assert_eq!(
            config["exchange"]["ccxt_async_config"]["legacyAsyncOption"],
            "keep-me-too"
        );
        assert_eq!(
            config["exchange"]["ccxt_async_config"]["httpsProxy"],
            "http://host.docker.internal:7890"
        );
        assert!(
            config["exchange"]["ccxt_async_config"]
                .get("httpProxy")
                .is_none()
        );
        assert!(
            config["exchange"]["ccxt_async_config"]
                .get("aiohttpProxy")
                .is_none()
        );
    }

    #[test]
    fn removes_only_ccxt_proxy_values_when_proxy_is_unavailable() {
        let mut config = json!({
            "exchange": {
                "name": "binance",
                "ccxt_config": {
                    "enableRateLimit": true,
                    "customOption": 7,
                    "httpProxy": "http://old-proxy:7890",
                    "httpsProxy": "http://old-proxy:7890",
                    "aiohttpProxy": "http://old-proxy:7890"
                },
                "ccxt_async_config": {
                    "enableRateLimit": true,
                    "customAsyncOption": 9,
                    "httpProxy": "http://old-proxy:7890",
                    "httpsProxy": "http://old-proxy:7890",
                    "aiohttpProxy": "http://old-proxy:7890"
                }
            }
        });

        ensure_ccxt_proxy(&mut config, None).unwrap();

        assert_eq!(config["exchange"]["name"], "binance");
        for key in ["ccxt_config", "ccxt_async_config"] {
            assert_eq!(config["exchange"][key]["enableRateLimit"], true);
            assert!(config["exchange"][key].get("httpProxy").is_none());
            assert!(config["exchange"][key].get("httpsProxy").is_none());
            assert!(config["exchange"][key].get("aiohttpProxy").is_none());
        }
        assert_eq!(config["exchange"]["ccxt_config"]["customOption"], 7);
        assert_eq!(
            config["exchange"]["ccxt_async_config"]["customAsyncOption"],
            9
        );
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
        assert!(!stored_ai.one_signal_per_candle);
        let config: Value =
            serde_json::from_str(&fs::read_to_string(&workspace.config).unwrap()).unwrap();
        assert!(config["exchange"]["pair_whitelist"].is_null());
        assert_eq!(config["pairlists"][0]["method"], "VolumePairList");
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
        assert_eq!(account.wallet_balance, 10025.0);
        assert_eq!(account.available_balance, 9975.0);
        assert_eq!(account.closed_trades, 1);
        assert_eq!(account.open_trades[0].side, "空");
        assert_eq!(account.trade_history.len(), 2);
        assert_eq!(account.trade_history[0].status, "持仓中");
        assert_eq!(account.trade_history[1].exit_reason, "roi");
        let _ = fs::remove_dir_all(root);
    }
}
