use std::{collections::BTreeMap, path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use rusqlite::{Connection, Row, params};
use serde_json::{Value, json};

use crate::{
    market,
    model::{Candle, Interval, MarketSnapshot},
    network,
};

pub const EVENT_STARTING_BANKROLL_USDT: f64 = f64::INFINITY;
pub const EVENT_STAKE_USDT: f64 = 5.0;
pub const EVENT_TEN_MINUTE_PROFIT_RATE: f64 = 0.80;
pub const EVENT_DEFAULT_PROFIT_RATE: f64 = 0.85;
pub const EVENT_SUPPORTED_SYMBOLS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];
pub const EVENT_STRATEGY_NAME: &str = "direction_dataset_v3";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventHorizon {
    TenMinutes,
    ThirtyMinutes,
    OneHour,
}

impl EventHorizon {
    pub const ALL: [EventHorizon; 3] = [
        EventHorizon::TenMinutes,
        EventHorizon::ThirtyMinutes,
        EventHorizon::OneHour,
    ];

    pub fn minutes(self) -> i64 {
        match self {
            EventHorizon::TenMinutes => 10,
            EventHorizon::ThirtyMinutes => 30,
            EventHorizon::OneHour => 60,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            EventHorizon::TenMinutes => "10m",
            EventHorizon::ThirtyMinutes => "30m",
            EventHorizon::OneHour => "1h",
        }
    }

    fn seconds(self) -> i64 {
        self.minutes() * 60
    }
}

pub fn supported_symbols() -> Vec<String> {
    EVENT_SUPPORTED_SYMBOLS
        .iter()
        .map(|symbol| symbol.to_string())
        .collect()
}

fn is_supported_symbol(symbol: &str) -> bool {
    EVENT_SUPPORTED_SYMBOLS
        .iter()
        .any(|supported| supported.eq_ignore_ascii_case(symbol.trim()))
}

fn supported_cycle_symbols(symbols: &[String]) -> Vec<String> {
    let mut filtered = symbols
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .filter(|symbol| is_supported_symbol(symbol))
        .collect::<Vec<_>>();
    filtered.sort();
    filtered.dedup();
    if filtered.is_empty() {
        supported_symbols()
    } else {
        filtered
    }
}

fn strategy_marker(strategy: &str) -> String {
    format!("\"strategy_version\":\"{strategy}\"")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventDirection {
    Up,
    Down,
}

impl EventDirection {
    fn as_str(self) -> &'static str {
        match self {
            EventDirection::Up => "up",
            EventDirection::Down => "down",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventSignalAction {
    Trade,
}

impl EventSignalAction {
    fn as_str(self) -> &'static str {
        match self {
            EventSignalAction::Trade => "trade",
        }
    }
}

#[derive(Debug, Clone)]
struct EventStrategyDecision {
    action: EventSignalAction,
    reason: String,
}

impl EventStrategyDecision {
    fn trade(reason: impl Into<String>) -> Self {
        Self {
            action: EventSignalAction::Trade,
            reason: reason.into(),
        }
    }

    fn should_trade(&self) -> bool {
        self.action == EventSignalAction::Trade
    }
}

#[derive(Debug, Clone, Default)]
pub struct EventPredictionTicket {
    pub id: String,
    pub symbol: String,
    pub horizon_minutes: i64,
    pub open_time: i64,
    pub close_time: i64,
    pub direction: String,
    pub confidence: f64,
    pub score: f64,
    pub stake_amount: f64,
    pub entry_price: f64,
    pub expiry_price: Option<f64>,
    pub status: String,
    pub result: String,
    pub move_percent: Option<f64>,
    pub virtual_pnl: Option<f64>,
    pub review: String,
}

#[derive(Debug, Clone, Default)]
pub struct EventPredictionStats {
    pub horizon_minutes: i64,
    pub total: i64,
    pub wins: i64,
    pub losses: i64,
    pub ties: i64,
    pub win_rate: f64,
    pub avg_confidence: f64,
    pub avg_move_percent: f64,
}

#[derive(Debug, Clone, Default)]
pub struct EventPredictionRunDirection {
    pub symbol: String,
    pub horizon_minutes: i64,
    pub open_time: i64,
    pub close_time: i64,
    pub direction: String,
    pub confidence: f64,
    pub created: bool,
}

#[derive(Debug, Clone, Default)]
pub struct EventPredictionSummary {
    pub created: usize,
    pub evaluated: usize,
    pub settled: usize,
    pub open_count: i64,
    pub starting_bankroll: f64,
    pub stake_amount: f64,
    pub realized_pnl: f64,
    pub open_exposure: f64,
    pub equity: f64,
    pub available_balance: f64,
    pub stats: Vec<EventPredictionStats>,
    pub all_realized_pnl: f64,
    pub all_stats: Vec<EventPredictionStats>,
    pub open_recent: Vec<EventPredictionTicket>,
    pub settled_recent: Vec<EventPredictionTicket>,
    pub directions: Vec<EventPredictionRunDirection>,
    pub message: String,
}

#[derive(Debug, Clone)]
struct NewEventPrediction {
    id: String,
    symbol: String,
    horizon: EventHorizon,
    open_time: i64,
    close_time: i64,
    direction: EventDirection,
    confidence: f64,
    score: f64,
    stake_amount: f64,
    entry_price: f64,
    features: Value,
}

#[derive(Debug, Clone)]
struct DueTicket {
    id: String,
    symbol: String,
    horizon_minutes: i64,
    close_time: i64,
    direction: String,
    entry_price: f64,
    stake_amount: f64,
    confidence: f64,
    score: f64,
}

#[derive(Debug, Clone)]
struct DueSignal {
    id: String,
    symbol: String,
    horizon_minutes: i64,
    close_time: i64,
    direction: String,
    entry_price: f64,
    stake_amount: f64,
    confidence: f64,
    score: f64,
    action: String,
    strategy: String,
    reason: String,
}

#[derive(Debug, Clone, Default)]
struct EventCreationSummary {
    created: usize,
    evaluated: usize,
    directions: Vec<EventPredictionRunDirection>,
}

struct EventBankroll {
    realized_pnl: f64,
    open_exposure: f64,
    equity: f64,
    available_balance: f64,
}

pub struct EventPredictionLog {
    connection: Connection,
}

impl EventPredictionLog {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .context("Failed to create event prediction directory")?;
        }
        let connection = Connection::open(path).context("Failed to open event prediction log")?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS event_prediction_tickets (
                id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL,
                open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                score REAL NOT NULL,
                stake_amount REAL NOT NULL DEFAULT 5.0,
                entry_price REAL NOT NULL,
                expiry_price REAL,
                status TEXT NOT NULL,
                result TEXT,
                move_percent REAL,
                virtual_pnl REAL,
                features_json TEXT NOT NULL,
                review TEXT,
                settled_at INTEGER
             );
             CREATE UNIQUE INDEX IF NOT EXISTS unique_event_prediction_round
             ON event_prediction_tickets(symbol, horizon_minutes, open_time);
             CREATE INDEX IF NOT EXISTS event_prediction_status_close
             ON event_prediction_tickets(status, close_time);
             CREATE INDEX IF NOT EXISTS event_prediction_symbol_horizon
             ON event_prediction_tickets(symbol, horizon_minutes);
             CREATE INDEX IF NOT EXISTS event_prediction_settled_recent
             ON event_prediction_tickets(status, settled_at);
             CREATE TABLE IF NOT EXISTS event_prediction_signals (
                id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL,
                open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL,
                direction TEXT NOT NULL,
                confidence REAL NOT NULL,
                score REAL NOT NULL,
                stake_amount REAL NOT NULL DEFAULT 5.0,
                entry_price REAL NOT NULL,
                action TEXT NOT NULL,
                strategy TEXT NOT NULL,
                skip_reason TEXT,
                status TEXT NOT NULL,
                result TEXT,
                expiry_price REAL,
                move_percent REAL,
                virtual_pnl REAL,
                features_json TEXT NOT NULL,
                review TEXT,
                settled_at INTEGER
             );
             CREATE UNIQUE INDEX IF NOT EXISTS unique_event_prediction_signal_round
             ON event_prediction_signals(symbol, horizon_minutes, open_time);
             CREATE INDEX IF NOT EXISTS event_prediction_signal_status_close
             ON event_prediction_signals(status, close_time);
             CREATE INDEX IF NOT EXISTS event_prediction_signal_symbol_horizon
             ON event_prediction_signals(symbol, horizon_minutes);",
        )?;
        migrate_schema(&connection)?;
        Ok(Self { connection })
    }

    pub fn dashboard(&self) -> Result<EventPredictionSummary> {
        let bankroll = self.bankroll()?;
        let all_bankroll = self.all_bankroll()?;
        Ok(EventPredictionSummary {
            created: 0,
            evaluated: 0,
            settled: 0,
            open_count: self.open_count()?,
            starting_bankroll: EVENT_STARTING_BANKROLL_USDT,
            stake_amount: EVENT_STAKE_USDT,
            realized_pnl: bankroll.realized_pnl,
            open_exposure: bankroll.open_exposure,
            equity: bankroll.equity,
            available_balance: bankroll.available_balance,
            stats: self.stats()?,
            all_realized_pnl: all_bankroll.realized_pnl,
            all_stats: self.all_stats()?,
            open_recent: self.open_recent(80)?,
            settled_recent: self.settled_recent(80)?,
            directions: Vec::new(),
            message: format!("事件预测虚拟盘就绪：当前策略 {EVENT_STRATEGY_NAME}"),
        })
    }

    fn record_prediction(&self, prediction: &NewEventPrediction, now: i64) -> Result<bool> {
        if !is_supported_symbol(&prediction.symbol) {
            return Ok(false);
        }
        let features_json =
            serde_json::to_string(&prediction.features).context("Failed to encode features")?;
        let changed = self.connection.execute(
            "INSERT OR IGNORE INTO event_prediction_tickets
             (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
              confidence, score, stake_amount, entry_price, status, features_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'open', ?12)",
            params![
                prediction.id,
                now,
                prediction.symbol,
                prediction.horizon.minutes(),
                prediction.open_time,
                prediction.close_time,
                prediction.direction.as_str(),
                prediction.confidence,
                prediction.score,
                prediction.stake_amount,
                prediction.entry_price,
                features_json,
            ],
        )?;
        Ok(changed == 1)
    }

    fn record_signal(
        &self,
        prediction: &NewEventPrediction,
        decision: &EventStrategyDecision,
        now: i64,
    ) -> Result<bool> {
        if !is_supported_symbol(&prediction.symbol) {
            return Ok(false);
        }
        let features_json =
            serde_json::to_string(&prediction.features).context("Failed to encode features")?;
        let changed = self.connection.execute(
            "INSERT OR IGNORE INTO event_prediction_signals
             (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
              confidence, score, stake_amount, entry_price, action, strategy, skip_reason,
              status, features_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                     'open', ?15)",
            params![
                prediction.id,
                now,
                prediction.symbol,
                prediction.horizon.minutes(),
                prediction.open_time,
                prediction.close_time,
                prediction.direction.as_str(),
                prediction.confidence,
                prediction.score,
                prediction.stake_amount,
                prediction.entry_price,
                decision.action.as_str(),
                EVENT_STRATEGY_NAME,
                decision.reason.as_str(),
                features_json,
            ],
        )?;
        Ok(changed == 1)
    }

    fn due_tickets(&self, now: i64) -> Result<Vec<DueTicket>> {
        let mut statement = self.connection.prepare(
            "SELECT id, symbol, horizon_minutes, close_time, direction, entry_price, stake_amount,
                    confidence, score
             FROM event_prediction_tickets
             WHERE status = 'open'
               AND close_time <= ?1
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
             ORDER BY close_time ASC",
        )?;
        let rows = statement.query_map([now], |row| {
            Ok(DueTicket {
                id: row.get(0)?,
                symbol: row.get(1)?,
                horizon_minutes: row.get(2)?,
                close_time: row.get(3)?,
                direction: row.get(4)?,
                entry_price: row.get(5)?,
                stake_amount: row.get(6)?,
                confidence: row.get(7)?,
                score: row.get(8)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to read due event prediction tickets")
    }

    fn due_signals(&self, now: i64) -> Result<Vec<DueSignal>> {
        let mut statement = self.connection.prepare(
            "SELECT id, symbol, horizon_minutes, close_time, direction, entry_price, stake_amount,
                    confidence, score, action, strategy, COALESCE(skip_reason, '')
             FROM event_prediction_signals
             WHERE status = 'open'
               AND close_time <= ?1
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
             ORDER BY close_time ASC",
        )?;
        let rows = statement.query_map([now], |row| {
            Ok(DueSignal {
                id: row.get(0)?,
                symbol: row.get(1)?,
                horizon_minutes: row.get(2)?,
                close_time: row.get(3)?,
                direction: row.get(4)?,
                entry_price: row.get(5)?,
                stake_amount: row.get(6)?,
                confidence: row.get(7)?,
                score: row.get(8)?,
                action: row.get(9)?,
                strategy: row.get(10)?,
                reason: row.get(11)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to read due event prediction signals")
    }

    fn settle_due_with_prices(&self, now: i64, prices: &BTreeMap<String, f64>) -> Result<usize> {
        let due = self.due_tickets(now)?;
        let mut settled = 0;
        for ticket in due {
            let Some(price) = prices.get(&ticket.symbol).copied() else {
                continue;
            };
            if !price.is_finite() || price <= 0.0 || ticket.entry_price <= 0.0 {
                continue;
            }
            let (result, move_percent) =
                event_settlement_result(&ticket.direction, ticket.entry_price, price);
            let settlement_return =
                event_settlement_return(result, ticket.horizon_minutes, ticket.stake_amount);
            let virtual_pnl = settlement_return - ticket.stake_amount;
            let review = format!(
                "{}: {} {}m stake {:.2} USDT, return {:.2} USDT, pnl {:+.2} USDT, entry {:.8}, expiry {:.8}, move {:+.4}%, confidence {:.1}%, score {:+.3}, settled {}s late",
                result,
                ticket.direction,
                ticket.horizon_minutes,
                ticket.stake_amount,
                settlement_return,
                virtual_pnl,
                ticket.entry_price,
                price,
                move_percent,
                ticket.confidence * 100.0,
                ticket.score,
                now.saturating_sub(ticket.close_time)
            );
            self.connection.execute(
                "UPDATE event_prediction_tickets
                 SET status = 'settled', result = ?1, expiry_price = ?2, move_percent = ?3,
                     virtual_pnl = ?4, review = ?5, settled_at = ?6
                 WHERE id = ?7 AND status = 'open'",
                params![
                    result,
                    price,
                    move_percent,
                    virtual_pnl,
                    review,
                    now,
                    ticket.id,
                ],
            )?;
            settled += 1;
        }
        Ok(settled)
    }

    fn settle_due_signals_with_prices(
        &self,
        now: i64,
        prices: &BTreeMap<String, f64>,
    ) -> Result<usize> {
        let due = self.due_signals(now)?;
        let mut settled = 0;
        for signal in due {
            let Some(price) = prices.get(&signal.symbol).copied() else {
                continue;
            };
            if !price.is_finite() || price <= 0.0 || signal.entry_price <= 0.0 {
                continue;
            }
            let (result, move_percent) =
                event_settlement_result(&signal.direction, signal.entry_price, price);
            let settlement_return =
                event_settlement_return(result, signal.horizon_minutes, signal.stake_amount);
            let virtual_pnl = settlement_return - signal.stake_amount;
            let review = format!(
                "{} signal: action {}, {} {}m stake {:.2} USDT, hypothetical pnl {:+.2} USDT, entry {:.8}, expiry {:.8}, move {:+.4}%, confidence {:.1}%, score {:+.3}, strategy {}, reason {}, settled {}s late",
                result,
                signal.action,
                signal.direction,
                signal.horizon_minutes,
                signal.stake_amount,
                virtual_pnl,
                signal.entry_price,
                price,
                move_percent,
                signal.confidence * 100.0,
                signal.score,
                signal.strategy,
                signal.reason,
                now.saturating_sub(signal.close_time)
            );
            self.connection.execute(
                "UPDATE event_prediction_signals
                 SET status = 'settled', result = ?1, expiry_price = ?2, move_percent = ?3,
                     virtual_pnl = ?4, review = ?5, settled_at = ?6
                 WHERE id = ?7 AND status = 'open'",
                params![
                    result,
                    price,
                    move_percent,
                    virtual_pnl,
                    review,
                    now,
                    signal.id,
                ],
            )?;
            settled += 1;
        }
        Ok(settled)
    }

    fn open_count(&self) -> Result<i64> {
        let marker = strategy_marker(EVENT_STRATEGY_NAME);
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM event_prediction_tickets
                 WHERE status = 'open'
                   AND symbol IN ('BTCUSDT', 'ETHUSDT')
                   AND instr(features_json, ?1) > 0",
                [marker],
                |row| row.get(0),
            )
            .context("Failed to count open event predictions")
    }

    fn bankroll(&self) -> Result<EventBankroll> {
        self.bankroll_for_strategy(Some(EVENT_STRATEGY_NAME))
    }

    fn all_bankroll(&self) -> Result<EventBankroll> {
        self.bankroll_for_strategy(None)
    }

    fn bankroll_for_strategy(&self, strategy: Option<&str>) -> Result<EventBankroll> {
        let strategy_filter = strategy.map(strategy_marker);
        let realized_sql = if strategy_filter.is_some() {
            "SELECT COALESCE(SUM(virtual_pnl), 0.0)
                     FROM event_prediction_tickets
                     WHERE status = 'settled'
                       AND symbol IN ('BTCUSDT', 'ETHUSDT')
                       AND instr(features_json, ?1) > 0"
        } else {
            "SELECT COALESCE(SUM(virtual_pnl), 0.0)
                     FROM event_prediction_tickets
                     WHERE status = 'settled'
                       AND symbol IN ('BTCUSDT', 'ETHUSDT')"
        };
        let realized_params = strategy_filter.iter().map(String::as_str);
        let realized_pnl = self
            .connection
            .query_row(
                realized_sql,
                rusqlite::params_from_iter(realized_params),
                |row| row.get::<_, f64>(0),
            )
            .context("Failed to read event prediction realized pnl")?;
        let open_sql = if strategy_filter.is_some() {
            "SELECT COALESCE(SUM(stake_amount), 0.0)
                     FROM event_prediction_tickets
                     WHERE status = 'open'
                       AND symbol IN ('BTCUSDT', 'ETHUSDT')
                       AND instr(features_json, ?1) > 0"
        } else {
            "SELECT COALESCE(SUM(stake_amount), 0.0)
                     FROM event_prediction_tickets
                     WHERE status = 'open'
                       AND symbol IN ('BTCUSDT', 'ETHUSDT')"
        };
        let open_params = strategy_filter.iter().map(String::as_str);
        let open_exposure = self
            .connection
            .query_row(open_sql, rusqlite::params_from_iter(open_params), |row| {
                row.get::<_, f64>(0)
            })
            .context("Failed to read event prediction open exposure")?;
        let equity = if EVENT_STARTING_BANKROLL_USDT.is_finite() {
            EVENT_STARTING_BANKROLL_USDT + realized_pnl
        } else {
            f64::INFINITY
        };
        let available_balance = if equity.is_finite() {
            equity - open_exposure
        } else {
            f64::INFINITY
        };
        Ok(EventBankroll {
            realized_pnl,
            open_exposure,
            equity,
            available_balance,
        })
    }

    fn stats(&self) -> Result<Vec<EventPredictionStats>> {
        self.stats_for_strategy(Some(EVENT_STRATEGY_NAME))
    }

    fn all_stats(&self) -> Result<Vec<EventPredictionStats>> {
        self.stats_for_strategy(None)
    }

    fn stats_for_strategy(&self, strategy: Option<&str>) -> Result<Vec<EventPredictionStats>> {
        let mut statement = self.connection.prepare(if strategy.is_some() {
            "SELECT horizon_minutes,
                        COUNT(*),
                        SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN result = 'tie' THEN 1 ELSE 0 END),
                        AVG(confidence),
                        AVG(move_percent)
                 FROM event_prediction_tickets
                 WHERE status = 'settled'
                   AND symbol IN ('BTCUSDT', 'ETHUSDT')
                   AND instr(features_json, ?1) > 0
                 GROUP BY horizon_minutes
                 ORDER BY horizon_minutes"
        } else {
            "SELECT horizon_minutes,
                        COUNT(*),
                        SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END),
                        SUM(CASE WHEN result = 'tie' THEN 1 ELSE 0 END),
                        AVG(confidence),
                        AVG(move_percent)
                 FROM event_prediction_tickets
                 WHERE status = 'settled'
                   AND symbol IN ('BTCUSDT', 'ETHUSDT')
                 GROUP BY horizon_minutes
                 ORDER BY horizon_minutes"
        })?;
        let marker_params = strategy
            .map(strategy_marker)
            .map(|marker| vec![marker])
            .unwrap_or_default();
        let rows = statement.query_map(rusqlite::params_from_iter(marker_params), |row| {
            let total: i64 = row.get(1)?;
            let wins: i64 = row.get::<_, Option<i64>>(2)?.unwrap_or(0);
            let losses: i64 = row.get::<_, Option<i64>>(3)?.unwrap_or(0);
            let ties: i64 = row.get::<_, Option<i64>>(4)?.unwrap_or(0);
            let decisive = wins + losses;
            Ok(EventPredictionStats {
                horizon_minutes: row.get(0)?,
                total,
                wins,
                losses,
                ties,
                win_rate: if decisive > 0 {
                    wins as f64 / decisive as f64 * 100.0
                } else {
                    0.0
                },
                avg_confidence: row.get::<_, Option<f64>>(5)?.unwrap_or(0.0) * 100.0,
                avg_move_percent: row.get::<_, Option<f64>>(6)?.unwrap_or(0.0),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to read event prediction stats")
    }

    fn open_recent(&self, limit: usize) -> Result<Vec<EventPredictionTicket>> {
        let marker = strategy_marker(EVENT_STRATEGY_NAME);
        let mut statement = self.connection.prepare(
            "SELECT id, symbol, horizon_minutes, open_time, close_time, direction,
                    confidence, score, stake_amount, entry_price, expiry_price, status,
                    COALESCE(result, ''), move_percent, virtual_pnl, COALESCE(review, '')
             FROM event_prediction_tickets
             WHERE status = 'open'
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
               AND instr(features_json, ?2) > 0
             ORDER BY close_time ASC, open_time DESC, horizon_minutes ASC
             LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64, marker], ticket_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to read open event predictions")
    }

    fn settled_recent(&self, limit: usize) -> Result<Vec<EventPredictionTicket>> {
        let marker = strategy_marker(EVENT_STRATEGY_NAME);
        let mut statement = self.connection.prepare(
            "SELECT id, symbol, horizon_minutes, open_time, close_time, direction,
                    confidence, score, stake_amount, entry_price, expiry_price, status,
                    COALESCE(result, ''), move_percent, virtual_pnl, COALESCE(review, '')
             FROM event_prediction_tickets
             WHERE status = 'settled'
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
               AND instr(features_json, ?2) > 0
             ORDER BY COALESCE(settled_at, close_time) DESC, close_time DESC, open_time DESC
             LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64, marker], ticket_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to read settled event predictions")
    }
}

fn ticket_from_row(row: &Row<'_>) -> rusqlite::Result<EventPredictionTicket> {
    Ok(EventPredictionTicket {
        id: row.get(0)?,
        symbol: row.get(1)?,
        horizon_minutes: row.get(2)?,
        open_time: row.get(3)?,
        close_time: row.get(4)?,
        direction: row.get(5)?,
        confidence: row.get(6)?,
        score: row.get(7)?,
        stake_amount: row.get(8)?,
        entry_price: row.get(9)?,
        expiry_price: row.get(10)?,
        status: row.get(11)?,
        result: row.get(12)?,
        move_percent: row.get(13)?,
        virtual_pnl: row.get(14)?,
        review: row.get(15)?,
    })
}

pub fn run_cycle(path: &Path, symbols: &[String]) -> Result<EventPredictionSummary> {
    run_cycle_for_horizons(path, symbols, &EventHorizon::ALL)
}

pub fn run_cycle_for_horizons(
    path: &Path,
    symbols: &[String],
    horizons: &[EventHorizon],
) -> Result<EventPredictionSummary> {
    let log = EventPredictionLog::open(path)?;
    let symbols = supported_cycle_symbols(symbols);
    let horizons = normalized_horizons(horizons);
    let client = network::client(Duration::from_secs(20))?;
    let now = chrono::Utc::now().timestamp();
    let mut failures = Vec::new();
    let mut settlement_prices = BTreeMap::new();
    for symbol in due_symbols(&log, now)? {
        match market::fetch_snapshot(&client, &symbol) {
            Ok(snapshot) => {
                settlement_prices.insert(symbol, usable_price(&snapshot));
            }
            Err(error) => failures.push(format!("{symbol}: {error}")),
        }
    }
    let settled = log.settle_due_with_prices(now, &settlement_prices)?;
    let settled_signals = log.settle_due_signals_with_prices(now, &settlement_prices)?;

    let open_time = (now / 60) * 60;
    let mut creation = EventCreationSummary::default();
    for symbol in &symbols {
        let result = create_symbol_predictions(&log, &client, symbol, &horizons, open_time, now);
        match result {
            Ok(summary) => {
                creation.created += summary.created;
                creation.evaluated += summary.evaluated;
                creation.directions.extend(summary.directions);
            }
            Err(error) => failures.push(format!("{symbol}: {error}")),
        }
    }

    let open_count = log.open_count()?;
    let bankroll = log.bankroll()?;
    let all_bankroll = log.all_bankroll()?;
    let stats = log.stats()?;
    let all_stats = log.all_stats()?;
    let open_recent = log.open_recent(80)?;
    let settled_recent = log.settled_recent(80)?;
    if creation.created == 0 && settled == 0 && settled_signals == 0 && !failures.is_empty() {
        bail!("{}", failures.join("; "));
    }
    let mut message = format!(
        "事件预测：当前策略 {}，当前未结算 {}，当前占用 {:.2}，当前策略盈亏 {:+.2}，全历史盈亏 {:+.2}，信号结算 {}",
        EVENT_STRATEGY_NAME,
        open_count,
        bankroll.open_exposure,
        bankroll.realized_pnl,
        all_bankroll.realized_pnl,
        settled_signals
    );
    if !failures.is_empty() {
        message.push_str(&format!("; failures: {}", failures.join("; ")));
    }
    Ok(EventPredictionSummary {
        created: creation.created,
        evaluated: creation.evaluated,
        settled,
        open_count,
        starting_bankroll: EVENT_STARTING_BANKROLL_USDT,
        stake_amount: EVENT_STAKE_USDT,
        realized_pnl: bankroll.realized_pnl,
        open_exposure: bankroll.open_exposure,
        equity: bankroll.equity,
        available_balance: bankroll.available_balance,
        stats,
        all_realized_pnl: all_bankroll.realized_pnl,
        all_stats,
        open_recent,
        settled_recent,
        directions: creation.directions,
        message,
    })
}

fn normalized_horizons(horizons: &[EventHorizon]) -> Vec<EventHorizon> {
    let mut normalized = Vec::new();
    for horizon in horizons {
        if !normalized
            .iter()
            .any(|existing: &EventHorizon| existing.minutes() == horizon.minutes())
        {
            normalized.push(*horizon);
        }
    }
    if normalized.is_empty() {
        EventHorizon::ALL.to_vec()
    } else {
        normalized.sort_by_key(|horizon| horizon.minutes());
        normalized
    }
}

fn due_symbols(log: &EventPredictionLog, now: i64) -> Result<Vec<String>> {
    let mut statement = log.connection.prepare(
        "SELECT DISTINCT symbol FROM (
            SELECT symbol FROM event_prediction_tickets
             WHERE status = 'open'
               AND close_time <= ?1
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
            UNION
            SELECT symbol FROM event_prediction_signals
             WHERE status = 'open'
               AND close_time <= ?1
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
         )
         ORDER BY symbol",
    )?;
    let rows = statement.query_map([now], |row| row.get(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .context("Failed to read due event prediction symbols")
}

fn migrate_schema(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(event_prediction_tickets)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !columns.iter().any(|column| column == "stake_amount") {
        connection.execute(
            "ALTER TABLE event_prediction_tickets
             ADD COLUMN stake_amount REAL NOT NULL DEFAULT 5.0",
            [],
        )?;
    }
    migrate_payout_model(connection)?;
    Ok(())
}

fn migrate_payout_model(connection: &Connection) -> Result<()> {
    connection
        .execute(
            "UPDATE event_prediction_tickets
             SET virtual_pnl = CASE
                WHEN result = 'win' AND horizon_minutes = 10 THEN stake_amount * ?1
                WHEN result = 'win' THEN stake_amount * ?2
                WHEN result = 'loss' THEN -stake_amount
                WHEN result = 'tie' THEN 0.0
                ELSE virtual_pnl
             END
             WHERE status = 'settled'",
            params![EVENT_TEN_MINUTE_PROFIT_RATE, EVENT_DEFAULT_PROFIT_RATE],
        )
        .context("Failed to migrate event prediction payout model")?;
    connection
        .execute(
            "UPDATE event_prediction_signals
             SET virtual_pnl = CASE
                WHEN result = 'win' AND horizon_minutes = 10 THEN stake_amount * ?1
                WHEN result = 'win' THEN stake_amount * ?2
                WHEN result = 'loss' THEN -stake_amount
                WHEN result = 'tie' THEN 0.0
                ELSE virtual_pnl
             END
             WHERE status = 'settled'",
            params![EVENT_TEN_MINUTE_PROFIT_RATE, EVENT_DEFAULT_PROFIT_RATE],
        )
        .context("Failed to migrate event prediction signal payout model")?;
    Ok(())
}

fn event_profit_rate(horizon_minutes: i64) -> f64 {
    if horizon_minutes == 10 {
        EVENT_TEN_MINUTE_PROFIT_RATE
    } else {
        EVENT_DEFAULT_PROFIT_RATE
    }
}

fn event_settlement_result(
    direction: &str,
    entry_price: f64,
    expiry_price: f64,
) -> (&'static str, f64) {
    let move_percent = (expiry_price / entry_price - 1.0) * 100.0;
    let result = if move_percent.abs() <= 0.000001 {
        "tie"
    } else if (move_percent > 0.0 && direction == "up")
        || (move_percent < 0.0 && direction == "down")
    {
        "win"
    } else {
        "loss"
    };
    (result, move_percent)
}

fn event_settlement_return(result: &str, horizon_minutes: i64, stake_amount: f64) -> f64 {
    match result {
        "win" => stake_amount * (1.0 + event_profit_rate(horizon_minutes)),
        "loss" => 0.0,
        _ => stake_amount,
    }
}

fn create_symbol_predictions(
    log: &EventPredictionLog,
    client: &Client,
    symbol: &str,
    horizons: &[EventHorizon],
    open_time: i64,
    now: i64,
) -> Result<EventCreationSummary> {
    let candles = market::fetch_candles(client, symbol, Interval::OneMinute, 240)?;
    if candles.len() < 80 {
        bail!(
            "not enough 1m candles for event prediction: {}",
            candles.len()
        );
    }
    let snapshot = market::fetch_snapshot(client, symbol)?;
    let mut summary = EventCreationSummary::default();
    for horizon in horizons {
        let prediction = make_prediction(symbol, *horizon, &candles, &snapshot, open_time)?;
        let decision =
            EventStrategyDecision::trade("每轮全量虚拟预测样本：用结算结果训练方向 agent");
        summary.evaluated += 1;
        let _ = log.record_signal(&prediction, &decision, now)?;
        let created = decision.should_trade() && log.record_prediction(&prediction, now)?;
        if created {
            summary.created += 1;
        }
        summary.directions.push(EventPredictionRunDirection {
            symbol: prediction.symbol.clone(),
            horizon_minutes: prediction.horizon.minutes(),
            open_time: prediction.open_time,
            close_time: prediction.close_time,
            direction: prediction.direction.as_str().into(),
            confidence: prediction.confidence,
            created,
        });
    }
    Ok(summary)
}

fn make_prediction(
    symbol: &str,
    horizon: EventHorizon,
    candles: &[Candle],
    snapshot: &MarketSnapshot,
    open_time: i64,
) -> Result<NewEventPrediction> {
    let entry_price = usable_price(snapshot);
    if !entry_price.is_finite() || entry_price <= 0.0 {
        bail!("invalid entry price");
    }
    let mut features = prediction_features(candles, snapshot);
    let raw_score = horizon_score(horizon, &features).clamp(-1.0, 1.0);
    let raw_direction = if raw_score > 0.0 {
        EventDirection::Up
    } else if raw_score < 0.0 {
        EventDirection::Down
    } else if snapshot.change_percent >= 0.0 {
        EventDirection::Up
    } else {
        EventDirection::Down
    };
    let direction_decision = calibrated_direction(horizon, raw_score, raw_direction, &features);
    let direction = direction_decision.direction;
    let score = signed_score_for_direction(direction_decision.score_strength, direction);
    add_direction_training_fields(
        &mut features,
        horizon,
        raw_score,
        raw_direction,
        &direction_decision,
        score,
    );
    let confidence = (0.50 + direction_decision.score_strength * 0.38).clamp(0.51, 0.88);
    let close_time = open_time + horizon.seconds();
    let id = format!(
        "{}-{}m-{}",
        symbol.to_ascii_uppercase(),
        horizon.minutes(),
        open_time
    );
    Ok(NewEventPrediction {
        id,
        symbol: symbol.to_ascii_uppercase(),
        horizon,
        open_time,
        close_time,
        direction,
        confidence,
        score,
        stake_amount: EVENT_STAKE_USDT,
        entry_price,
        features,
    })
}

#[derive(Debug, Clone)]
struct DirectionDecision {
    direction: EventDirection,
    reason: &'static str,
    flipped: bool,
    score_strength: f64,
    factor_score: f64,
}

fn calibrated_direction(
    horizon: EventHorizon,
    raw_score: f64,
    raw_direction: EventDirection,
    features: &Value,
) -> DirectionDecision {
    let abs_score = raw_score.abs();
    if let Some((reason, factor_score)) = rebound_up_signal(horizon, features) {
        return DirectionDecision {
            direction: EventDirection::Up,
            reason,
            flipped: raw_direction != EventDirection::Up,
            score_strength: abs_score.max(factor_score).clamp(0.05, 1.0),
            factor_score,
        };
    }

    if raw_direction == EventDirection::Up {
        let factor_score = -exhaustion_down_score(horizon, features);
        let score_strength = abs_score.max(factor_score.abs());
        return DirectionDecision {
            direction: EventDirection::Down,
            reason: "raw_up_contrarian_or_exhaustion_down_v3",
            flipped: true,
            score_strength,
            factor_score,
        };
    }

    let factor_score = -exhaustion_down_score(horizon, features);
    let score_strength = abs_score.max(factor_score.abs());
    DirectionDecision {
        direction: EventDirection::Down,
        reason: "raw_down_continuation_no_strong_reversal_v3",
        flipped: false,
        score_strength,
        factor_score,
    }
}

fn signed_score_for_direction(abs_score: f64, direction: EventDirection) -> f64 {
    match direction {
        EventDirection::Up => abs_score.abs(),
        EventDirection::Down => -abs_score.abs(),
    }
    .clamp(-1.0, 1.0)
}

fn add_direction_training_fields(
    features: &mut Value,
    horizon: EventHorizon,
    raw_score: f64,
    raw_direction: EventDirection,
    decision: &DirectionDecision,
    final_score: f64,
) {
    if let Some(object) = features.as_object_mut() {
        object.insert("strategy_version".into(), json!(EVENT_STRATEGY_NAME));
        object.insert("horizon_minutes".into(), json!(horizon.minutes()));
        object.insert("raw_score".into(), json!(raw_score));
        object.insert("raw_direction".into(), json!(raw_direction.as_str()));
        object.insert("final_score".into(), json!(final_score));
        object.insert("final_direction".into(), json!(decision.direction.as_str()));
        object.insert("direction_flipped".into(), json!(decision.flipped));
        object.insert("direction_reason".into(), json!(decision.reason));
        object.insert("factor_score".into(), json!(decision.factor_score));
    }
}

fn rebound_up_signal(horizon: EventHorizon, features: &Value) -> Option<(&'static str, f64)> {
    let snapshot_change = feature_value(features, "snapshot_change_percent");
    let sentiment = feature_value(features, "sentiment");
    let rsi_trend = feature_value(features, "rsi_trend");
    match horizon {
        EventHorizon::TenMinutes => {
            if snapshot_change <= -1.0 && rsi_trend <= -0.30 && sentiment >= 0.30 {
                let oversold = (-snapshot_change / 4.0).clamp(0.0, 1.0);
                let rsi = (-rsi_trend).clamp(0.0, 1.0);
                let sentiment_strength = sentiment.clamp(0.0, 1.0);
                Some((
                    "short_oversold_rsi_sentiment_rebound_v3",
                    (0.40 * oversold + 0.35 * rsi + 0.25 * sentiment_strength).clamp(0.05, 1.0),
                ))
            } else {
                None
            }
        }
        EventHorizon::ThirtyMinutes => {
            if snapshot_change <= -2.0 && sentiment >= 0.50 {
                let oversold = (-snapshot_change / 5.0).clamp(0.0, 1.0);
                let sentiment_strength = sentiment.clamp(0.0, 1.0);
                Some((
                    "mid_24h_oversold_sentiment_rebound_v3",
                    (0.65 * oversold + 0.35 * sentiment_strength).clamp(0.05, 1.0),
                ))
            } else {
                None
            }
        }
        EventHorizon::OneHour => {
            if snapshot_change <= -3.0 {
                Some((
                    "long_extreme_24h_oversold_rebound_v3",
                    (-snapshot_change / 6.0).clamp(0.05, 1.0),
                ))
            } else {
                None
            }
        }
    }
}

fn exhaustion_down_score(horizon: EventHorizon, features: &Value) -> f64 {
    let snapshot_change =
        (feature_value(features, "snapshot_change_percent") / 5.0).clamp(0.0, 1.0);
    let trend = match horizon {
        EventHorizon::TenMinutes => feature_value(features, "momentum3"),
        EventHorizon::ThirtyMinutes => feature_value(features, "momentum30"),
        EventHorizon::OneHour => {
            0.50 * feature_value(features, "momentum60")
                + 0.50 * feature_value(features, "ema_long")
        }
    }
    .clamp(0.0, 1.0);
    (0.60 * snapshot_change + 0.40 * trend).clamp(0.05, 1.0)
}

fn usable_price(snapshot: &MarketSnapshot) -> f64 {
    if snapshot.mark_price.is_finite() && snapshot.mark_price > 0.0 {
        snapshot.mark_price
    } else {
        snapshot.price
    }
}

fn prediction_features(candles: &[Candle], snapshot: &MarketSnapshot) -> Value {
    let closes = candles
        .iter()
        .map(|candle| candle.close)
        .collect::<Vec<_>>();
    let ret3 = pct_change(candles, 3);
    let ret10 = pct_change(candles, 10);
    let ret30 = pct_change(candles, 30);
    let ret60 = pct_change(candles, 60);
    let volatility = mean_abs_return(&closes, 80).max(0.0005);
    let momentum3 = normalized_return(candles, 3, volatility);
    let momentum10 = normalized_return(candles, 10, volatility);
    let momentum30 = normalized_return(candles, 30, volatility);
    let momentum60 = normalized_return(candles, 60, volatility);
    let ema_short = ema_bias(&closes, 8, 21, volatility);
    let ema_mid = ema_bias(&closes, 13, 34, volatility);
    let ema_long = ema_bias(&closes, 21, 55, volatility);
    let rsi = rsi(&closes, 14);
    let rsi_trend = ((rsi - 50.0) / 25.0).clamp(-1.0, 1.0);
    let volume_ratio = volume_ratio(candles, 45);
    let volume_bias = momentum3.signum() * (volume_ratio / 1.5).clamp(0.0, 1.0);
    let breakout = breakout_bias(candles, 60);
    let long_short_bias = ((snapshot.long_short_ratio - 1.0) / 0.35).clamp(-1.0, 1.0);
    let funding_bias = (-snapshot.funding_rate / 0.0008).clamp(-1.0, 1.0);
    let sentiment = (long_short_bias * 0.75 + funding_bias * 0.25).clamp(-1.0, 1.0);
    json!({
        "ret3": ret3,
        "ret10": ret10,
        "ret30": ret30,
        "ret60": ret60,
        "volatility": volatility,
        "momentum3": momentum3,
        "momentum10": momentum10,
        "momentum30": momentum30,
        "momentum60": momentum60,
        "ema_short": ema_short,
        "ema_mid": ema_mid,
        "ema_long": ema_long,
        "rsi": rsi,
        "rsi_trend": rsi_trend,
        "volume_ratio": volume_ratio,
        "volume_bias": volume_bias,
        "breakout": breakout,
        "long_short_bias": long_short_bias,
        "funding_bias": funding_bias,
        "sentiment": sentiment,
        "snapshot_change_percent": snapshot.change_percent,
    })
}

fn feature_value(features: &Value, name: &str) -> f64 {
    features
        .get(name)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .unwrap_or_default()
}

fn horizon_score(horizon: EventHorizon, features: &Value) -> f64 {
    match horizon {
        EventHorizon::TenMinutes => {
            0.42 * feature_value(features, "momentum3")
                + 0.22 * feature_value(features, "momentum10")
                + 0.18 * feature_value(features, "ema_short")
                + 0.10 * feature_value(features, "volume_bias")
                + 0.08 * feature_value(features, "sentiment")
        }
        EventHorizon::ThirtyMinutes => {
            0.20 * feature_value(features, "momentum10")
                + 0.30 * feature_value(features, "momentum30")
                + 0.22 * feature_value(features, "ema_mid")
                + 0.10 * feature_value(features, "breakout")
                + 0.10 * feature_value(features, "sentiment")
                + 0.08 * feature_value(features, "rsi_trend")
        }
        EventHorizon::OneHour => {
            0.16 * feature_value(features, "momentum10")
                + 0.26 * feature_value(features, "momentum60")
                + 0.28 * feature_value(features, "ema_long")
                + 0.14 * feature_value(features, "breakout")
                + 0.10 * feature_value(features, "sentiment")
                + 0.06 * feature_value(features, "rsi_trend")
        }
    }
}

fn pct_change(candles: &[Candle], periods: usize) -> f64 {
    if candles.len() <= periods {
        return 0.0;
    }
    let latest = candles
        .last()
        .map(|candle| candle.close)
        .unwrap_or_default();
    let previous = candles[candles.len() - 1 - periods].close;
    if latest.is_finite() && previous.is_finite() && previous > 0.0 {
        latest / previous - 1.0
    } else {
        0.0
    }
}

fn normalized_return(candles: &[Candle], periods: usize, volatility: f64) -> f64 {
    let scale = volatility * (periods as f64).sqrt().max(1.0);
    (pct_change(candles, periods) / scale.max(0.0005)).clamp(-2.0, 2.0) / 2.0
}

fn mean_abs_return(closes: &[f64], window: usize) -> f64 {
    let returns = closes
        .windows(2)
        .rev()
        .take(window)
        .filter_map(|pair| {
            if pair[0].is_finite() && pair[1].is_finite() && pair[0] > 0.0 {
                Some((pair[1] / pair[0] - 1.0).abs())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if returns.is_empty() {
        return 0.0;
    }
    returns.iter().sum::<f64>() / returns.len() as f64
}

fn ema_bias(closes: &[f64], fast_period: usize, slow_period: usize, volatility: f64) -> f64 {
    let fast = ema(closes, fast_period);
    let slow = ema(closes, slow_period);
    if fast.is_finite() && slow.is_finite() && slow > 0.0 {
        ((fast / slow - 1.0) / volatility.max(0.0005)).clamp(-2.0, 2.0) / 2.0
    } else {
        0.0
    }
}

fn ema(values: &[f64], period: usize) -> f64 {
    let Some(first) = values.first().copied() else {
        return 0.0;
    };
    let alpha = 2.0 / (period as f64 + 1.0);
    values.iter().skip(1).fold(first, |average, value| {
        if value.is_finite() {
            alpha * *value + (1.0 - alpha) * average
        } else {
            average
        }
    })
}

fn rsi(closes: &[f64], period: usize) -> f64 {
    if closes.len() <= period {
        return 50.0;
    }
    let slice = &closes[closes.len() - period - 1..];
    let mut gain = 0.0;
    let mut loss = 0.0;
    for pair in slice.windows(2) {
        let change = pair[1] - pair[0];
        if change >= 0.0 {
            gain += change;
        } else {
            loss += change.abs();
        }
    }
    if loss <= f64::EPSILON {
        100.0
    } else {
        let rs = gain / loss;
        100.0 - 100.0 / (1.0 + rs)
    }
}

fn volume_ratio(candles: &[Candle], window: usize) -> f64 {
    if candles.len() <= window {
        return 0.0;
    }
    let latest = candles
        .last()
        .map(|candle| candle.volume)
        .unwrap_or_default();
    let start = candles.len() - 1 - window;
    let average = candles[start..candles.len() - 1]
        .iter()
        .map(|candle| candle.volume)
        .filter(|volume| volume.is_finite())
        .sum::<f64>()
        / window as f64;
    if average > f64::EPSILON && latest.is_finite() {
        latest / average - 1.0
    } else {
        0.0
    }
}

fn breakout_bias(candles: &[Candle], window: usize) -> f64 {
    if candles.len() <= window {
        return 0.0;
    }
    let latest = candles
        .last()
        .map(|candle| candle.close)
        .unwrap_or_default();
    let start = candles.len() - window;
    let high = candles[start..]
        .iter()
        .map(|candle| candle.high)
        .filter(|value| value.is_finite())
        .fold(f64::NEG_INFINITY, f64::max);
    let low = candles[start..]
        .iter()
        .map(|candle| candle.low)
        .filter(|value| value.is_finite())
        .fold(f64::INFINITY, f64::min);
    let spread = high - low;
    if spread.is_finite() && spread > f64::EPSILON {
        ((latest - (high + low) * 0.5) / (spread * 0.5)).clamp(-1.0, 1.0)
    } else {
        0.0
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn predicts_all_event_horizons_and_settles() {
        let root = std::env::temp_dir().join(format!("gqt-event-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        let candles = sample_candles();
        let snapshot = MarketSnapshot {
            symbol: "BTCUSDT".into(),
            price: 103.0,
            mark_price: 103.0,
            long_short_ratio: 1.12,
            funding_rate: 0.0001,
            change_percent: 2.0,
            ..Default::default()
        };

        let open_time = 1_700_000_000;
        let mut created = 0;
        for horizon in EventHorizon::ALL {
            let prediction =
                make_prediction("BTCUSDT", horizon, &candles, &snapshot, open_time).unwrap();
            assert!(matches!(
                prediction.direction,
                EventDirection::Up | EventDirection::Down
            ));
            if log.record_prediction(&prediction, open_time).unwrap() {
                created += 1;
            }
        }
        assert_eq!(created, 3);
        assert_eq!(log.open_count().unwrap(), 3);
        let open_dashboard = log.dashboard().unwrap();
        assert_eq!(open_dashboard.stake_amount, 5.0);
        assert!(open_dashboard.starting_bankroll.is_infinite());
        assert_eq!(open_dashboard.open_exposure, 15.0);
        assert!(open_dashboard.available_balance.is_infinite());

        let mut prices = BTreeMap::new();
        prices.insert("BTCUSDT".into(), 101.0);
        assert_eq!(
            log.settle_due_with_prices(open_time + 60 * 60 + 1, &prices)
                .unwrap(),
            3
        );
        let dashboard = log.dashboard().unwrap();
        assert_eq!(dashboard.open_count, 0);
        assert_eq!(dashboard.open_exposure, 0.0);
        assert_close(dashboard.realized_pnl, 12.5);
        assert!(dashboard.equity.is_infinite());
        assert!(dashboard.available_balance.is_infinite());
        assert_eq!(dashboard.open_recent.len(), 0);
        assert_eq!(dashboard.settled_recent.len(), 3);
        let pnl_by_horizon = dashboard
            .settled_recent
            .iter()
            .map(|ticket| (ticket.horizon_minutes, ticket.virtual_pnl.unwrap()))
            .collect::<BTreeMap<_, _>>();
        assert_close(*pnl_by_horizon.get(&10).unwrap(), 4.0);
        assert_close(*pnl_by_horizon.get(&30).unwrap(), 4.25);
        assert_close(*pnl_by_horizon.get(&60).unwrap(), 4.25);
        assert_eq!(
            dashboard.stats.iter().map(|stat| stat.total).sum::<i64>(),
            3
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn direction_dataset_calibrates_direction_without_dropping_samples() {
        let neutral_features = json!({
            "snapshot_change_percent": 1.2,
            "sentiment": 0.1,
            "rsi_trend": 0.1,
            "momentum3": 0.2,
            "momentum30": 0.2,
            "momentum60": 0.2,
            "ema_long": 0.2,
        });
        let raw_up = calibrated_direction(
            EventHorizon::ThirtyMinutes,
            0.42,
            EventDirection::Up,
            &neutral_features,
        );
        assert_eq!(raw_up.direction, EventDirection::Down);
        assert!(raw_up.flipped);

        let strong_raw_down = calibrated_direction(
            EventHorizon::OneHour,
            -0.72,
            EventDirection::Down,
            &neutral_features,
        );
        assert_eq!(strong_raw_down.direction, EventDirection::Down);
        assert!(!strong_raw_down.flipped);

        let moderate_raw_down = calibrated_direction(
            EventHorizon::TenMinutes,
            -0.32,
            EventDirection::Down,
            &neutral_features,
        );
        assert_eq!(moderate_raw_down.direction, EventDirection::Down);
        assert!(!moderate_raw_down.flipped);

        let short_rebound_features = json!({
            "snapshot_change_percent": -1.2,
            "sentiment": 0.45,
            "rsi_trend": -0.35,
        });
        let short_rebound = calibrated_direction(
            EventHorizon::TenMinutes,
            -0.18,
            EventDirection::Down,
            &short_rebound_features,
        );
        assert_eq!(short_rebound.direction, EventDirection::Up);
        assert!(short_rebound.flipped);

        let mid_rebound_features = json!({
            "snapshot_change_percent": -2.1,
            "sentiment": 0.55,
        });
        let mid_rebound = calibrated_direction(
            EventHorizon::ThirtyMinutes,
            -0.22,
            EventDirection::Down,
            &mid_rebound_features,
        );
        assert_eq!(mid_rebound.direction, EventDirection::Up);

        let long_rebound_features = json!({
            "snapshot_change_percent": -3.1,
        });
        let long_rebound = calibrated_direction(
            EventHorizon::OneHour,
            -0.22,
            EventDirection::Down,
            &long_rebound_features,
        );
        assert_eq!(long_rebound.direction, EventDirection::Up);

        let mut features = json!({"momentum10": 0.25});
        add_direction_training_fields(
            &mut features,
            EventHorizon::ThirtyMinutes,
            0.42,
            EventDirection::Up,
            &raw_up,
            -0.42,
        );
        assert_eq!(features["strategy_version"], EVENT_STRATEGY_NAME);
        assert_eq!(features["raw_direction"], "up");
        assert_eq!(features["final_direction"], "down");
        assert_eq!(features["direction_flipped"], true);
        assert!(features["factor_score"].as_f64().is_some());
    }

    #[test]
    fn ignores_unsupported_event_symbols() {
        let root = std::env::temp_dir().join(format!("gqt-event-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        let candles = sample_candles();
        let snapshot = MarketSnapshot {
            symbol: "XRPUSDT".into(),
            price: 1.0,
            mark_price: 1.0,
            long_short_ratio: 1.0,
            funding_rate: 0.0,
            change_percent: 0.0,
            ..Default::default()
        };
        let prediction = make_prediction(
            "XRPUSDT",
            EventHorizon::TenMinutes,
            &candles,
            &snapshot,
            1_700_000_000,
        )
        .unwrap();

        assert!(!log.record_prediction(&prediction, 1_700_000_000).unwrap());
        assert_eq!(log.dashboard().unwrap().open_count, 0);
        assert_eq!(
            supported_cycle_symbols(&["SOLUSDT".into(), "DOGEUSDT".into()]),
            supported_symbols()
        );

        let _ = fs::remove_dir_all(root);
    }

    fn sample_candles() -> Vec<Candle> {
        (0..160)
            .map(|index| {
                let close = 100.0 + index as f64 * 0.02;
                Candle {
                    time: 1_700_000_000 + index * 60,
                    open: close - 0.03,
                    high: close + 0.06,
                    low: close - 0.08,
                    close,
                    volume: 1000.0 + index as f64,
                }
            })
            .collect()
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 0.000001,
            "expected {expected}, got {actual}"
        );
    }
}
