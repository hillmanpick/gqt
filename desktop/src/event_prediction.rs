use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;
use rusqlite::{Connection, Row, TransactionBehavior, params};
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
pub const EVENT_STRATEGY_NAME: &str = "event_reinvest_batch5_v2";
pub const EVENT_CYCLE_SLOTS: i64 = 5;
const EVENT_SCHEMA_VERSION: i64 = 3;
const EVENT_EXPERT_HISTORY_LIMIT: i64 = 400;
const EVENT_EXPERT_MIN_SAMPLES: usize = 8;
const EVENT_UP_COMMITMENT_RATE: f64 = 0.65;
const EVENT_EXTREME_MIN_SAMPLES: usize = 20;
const EVENT_EXTREME_UP_RATE: f64 = 0.75;
const EVENT_EXTREME_DOWN_RATE: f64 = 0.25;
const EVENT_30M_EXPERT_SPACING_SECONDS: i64 = 30 * 60;
const EVENT_30M_EXPERT_SAMPLE_LIMIT: usize = EVENT_EXTREME_MIN_SAMPLES;
const EVENT_60M_EXPERT_SPACING_SECONDS: i64 = 60 * 60;
const EVENT_60M_EXPERT_SAMPLE_LIMIT: usize = EVENT_EXTREME_MIN_SAMPLES;

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

    fn is_active(self) -> bool {
        matches!(
            self,
            EventHorizon::TenMinutes | EventHorizon::ThirtyMinutes | EventHorizon::OneHour
        )
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
    Observe,
}

impl EventSignalAction {
    fn as_str(self) -> &'static str {
        match self {
            EventSignalAction::Trade => "trade",
            EventSignalAction::Observe => "observe",
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

    fn observe(reason: impl Into<String>) -> Self {
        Self {
            action: EventSignalAction::Observe,
            reason: reason.into(),
        }
    }

    #[cfg(test)]
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
    pub cycle_id: String,
    pub cycle_number: i64,
    pub cycle_order: i64,
    pub cycle_slot: Option<i64>,
    pub cycle_balance_after: Option<f64>,
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
    pub legacy_open_count: i64,
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
    cycle_id: String,
    cycle_number: i64,
    cycle_order: i64,
    cycle_slot: i64,
    features: Value,
}

#[derive(Debug, Clone)]
struct CyclePlan {
    cycle_id: String,
    cycle_number: i64,
    cycle_order: i64,
    cycle_slot: i64,
    stake_amount: f64,
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
    cycle_id: String,
    cycle_number: i64,
    cycle_order: i64,
    cycle_slot: i64,
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

fn event_cycle_id(symbol: &str, horizon: EventHorizon, cycle_number: i64) -> String {
    format!(
        "{}-{}m-C{:06}",
        symbol.to_ascii_uppercase(),
        horizon.minutes(),
        cycle_number
    )
}

fn initial_cycle_plans(symbol: &str, horizon: EventHorizon, cycle_number: i64) -> Vec<CyclePlan> {
    let cycle_id = event_cycle_id(symbol, horizon, cycle_number);
    (1..=EVENT_CYCLE_SLOTS)
        .map(|cycle_slot| CyclePlan {
            cycle_id: cycle_id.clone(),
            cycle_number,
            cycle_order: 1,
            cycle_slot,
            stake_amount: EVENT_STAKE_USDT,
        })
        .collect()
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
        let mut connection =
            Connection::open(path).context("Failed to open event prediction log")?;
        connection
            .busy_timeout(Duration::from_secs(10))
            .context("Failed to configure event prediction database timeout")?;
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
                settled_at INTEGER,
                cycle_id TEXT,
                cycle_number INTEGER,
                cycle_order INTEGER,
                cycle_slot INTEGER,
                cycle_balance_after REAL
             );
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
        migrate_schema(&mut connection)?;
        Ok(Self { connection })
    }

    pub fn dashboard(&self) -> Result<EventPredictionSummary> {
        let bankroll = self.bankroll()?;
        let all_bankroll = self.all_bankroll()?;
        let legacy_open_count = self.legacy_open_count()?;
        Ok(EventPredictionSummary {
            created: 0,
            evaluated: 0,
            settled: 0,
            open_count: self.open_count()?,
            legacy_open_count,
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
            message: format!(
                "事件预测虚拟盘就绪：当前策略 {EVENT_STRATEGY_NAME}，活跃周期 10m/30m/1h，等待旧票结算 {legacy_open_count}"
            ),
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
              confidence, score, stake_amount, entry_price, status, features_json,
              cycle_id, cycle_number, cycle_order, cycle_slot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'open', ?12,
                     ?13, ?14, ?15, ?16)",
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
                prediction.cycle_id,
                prediction.cycle_number,
                prediction.cycle_order,
                prediction.cycle_slot,
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

    fn cycle_plans(&self, symbol: &str, horizon: EventHorizon) -> Result<Vec<CyclePlan>> {
        let legacy_open_count = self.connection.query_row(
            "SELECT COUNT(*)
               FROM event_prediction_tickets
              WHERE status = 'open'
                AND symbol = ?1
                AND horizon_minutes = ?2
                AND cycle_slot IS NULL",
            params![symbol, horizon.minutes()],
            |row| row.get::<_, i64>(0),
        )?;
        if legacy_open_count > 0 {
            return Ok(Vec::new());
        }

        let active_cycle_number = self.connection.query_row(
            "SELECT COALESCE(MAX(cycle_number), 0)
               FROM event_prediction_tickets
              WHERE symbol = ?1
                AND horizon_minutes = ?2
                AND cycle_slot IS NOT NULL",
            params![symbol, horizon.minutes()],
            |row| row.get::<_, i64>(0),
        )?;
        if active_cycle_number == 0 {
            let historical_max = self.connection.query_row(
                "SELECT COALESCE(MAX(cycle_number), 0)
                   FROM event_prediction_tickets
                  WHERE symbol = ?1
                    AND horizon_minutes = ?2",
                params![symbol, horizon.minutes()],
                |row| row.get::<_, i64>(0),
            )?;
            return Ok(initial_cycle_plans(symbol, horizon, historical_max + 1));
        }
        let cycle_number = active_cycle_number;

        let mut statement = self.connection.prepare(
            "SELECT cycle_slot, cycle_order, status, COALESCE(result, ''), stake_amount,
                    COALESCE(cycle_balance_after, 0.0)
               FROM event_prediction_tickets
              WHERE symbol = ?1
                AND horizon_minutes = ?2
                AND cycle_number = ?3
                AND cycle_slot IS NOT NULL
              ORDER BY cycle_slot ASC, cycle_order DESC, created_at DESC",
        )?;
        let mut latest = BTreeMap::new();
        for row in statement.query_map(params![symbol, horizon.minutes(), cycle_number], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
            ))
        })? {
            let row = row?;
            latest.entry(row.0).or_insert(row);
        }

        let all_lost = (1..=EVENT_CYCLE_SLOTS).all(|slot| {
            latest
                .get(&slot)
                .is_some_and(|row| row.2 == "settled" && row.3 == "loss")
        });
        if all_lost {
            return Ok(initial_cycle_plans(symbol, horizon, cycle_number + 1));
        }

        let cycle_id = event_cycle_id(symbol, horizon, cycle_number);
        let mut plans = Vec::new();
        for slot in 1..=EVENT_CYCLE_SLOTS {
            let Some(last) = latest.get(&slot) else {
                plans.push(CyclePlan {
                    cycle_id: cycle_id.clone(),
                    cycle_number,
                    cycle_order: 1,
                    cycle_slot: slot,
                    stake_amount: EVENT_STAKE_USDT,
                });
                continue;
            };
            if last.2 == "open" || last.3 == "loss" {
                continue;
            }
            let stake_amount = if last.3 == "win" {
                if last.5 > 0.0 {
                    last.5
                } else {
                    event_settlement_return("win", horizon.minutes(), last.4)
                }
            } else {
                last.4
            };
            if !stake_amount.is_finite() || stake_amount <= 0.0 {
                bail!("invalid event cycle stake: {stake_amount}");
            }
            plans.push(CyclePlan {
                cycle_id: cycle_id.clone(),
                cycle_number,
                cycle_order: last.1 + 1,
                cycle_slot: slot,
                stake_amount,
            });
        }
        Ok(plans)
    }

    fn due_tickets(&self, now: i64) -> Result<Vec<DueTicket>> {
        let mut statement = self.connection.prepare(
            "SELECT id, symbol, horizon_minutes, close_time, direction, entry_price, stake_amount,
                    confidence, score, COALESCE(cycle_id, ''), COALESCE(cycle_number, 0),
                    COALESCE(cycle_order, 0), COALESCE(cycle_slot, 0)
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
                cycle_id: row.get(9)?,
                cycle_number: row.get(10)?,
                cycle_order: row.get(11)?,
                cycle_slot: row.get(12)?,
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
            let cycle_balance_after = if ticket.cycle_number > 0 {
                Some(settlement_return)
            } else {
                None
            };
            let review = format!(
                "{}: {} {}m stake {:.2} USDT, return {:.2} USDT, pnl {:+.2} USDT, cycle {} slot {} order {}, entry {:.8}, expiry {:.8}, move {:+.4}%, confidence {:.1}%, score {:+.3}, settled {}s late",
                result,
                ticket.direction,
                ticket.horizon_minutes,
                ticket.stake_amount,
                settlement_return,
                virtual_pnl,
                if ticket.cycle_id.is_empty() {
                    "legacy"
                } else {
                    &ticket.cycle_id
                },
                ticket.cycle_slot,
                ticket.cycle_order,
                ticket.entry_price,
                price,
                move_percent,
                ticket.confidence * 100.0,
                ticket.score,
                now.saturating_sub(ticket.close_time)
            );
            let changed = self.connection.execute(
                "UPDATE event_prediction_tickets
                 SET status = 'settled', result = ?1, expiry_price = ?2, move_percent = ?3,
                     virtual_pnl = ?4, review = ?5, settled_at = ?6, cycle_balance_after = ?7
                 WHERE id = ?8 AND status = 'open'",
                params![
                    result,
                    price,
                    move_percent,
                    virtual_pnl,
                    review,
                    now,
                    cycle_balance_after,
                    ticket.id,
                ],
            )?;
            settled += changed;
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
            let changed = self.connection.execute(
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
            settled += changed;
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
                   AND horizon_minutes IN (10, 30, 60)
                   AND instr(features_json, ?1) > 0",
                [marker],
                |row| row.get(0),
            )
            .context("Failed to count open event predictions")
    }

    fn legacy_open_count(&self) -> Result<i64> {
        let marker = strategy_marker(EVENT_STRATEGY_NAME);
        self.connection
            .query_row(
                "SELECT COUNT(*) FROM event_prediction_tickets
                 WHERE status = 'open'
                   AND symbol IN ('BTCUSDT', 'ETHUSDT')
                   AND horizon_minutes IN (10, 30, 60)
                   AND instr(features_json, ?1) = 0",
                [marker],
                |row| row.get(0),
            )
            .context("Failed to count legacy open event predictions")
    }

    fn recent_expert_bias(
        &self,
        symbol: &str,
        horizon: EventHorizon,
        now: i64,
    ) -> Result<Option<DirectionRegimeBias>> {
        let (spacing_seconds, sample_limit) = match horizon {
            EventHorizon::ThirtyMinutes => (
                EVENT_30M_EXPERT_SPACING_SECONDS,
                EVENT_30M_EXPERT_SAMPLE_LIMIT,
            ),
            EventHorizon::OneHour => (
                EVENT_60M_EXPERT_SPACING_SECONDS,
                EVENT_60M_EXPERT_SAMPLE_LIMIT,
            ),
            EventHorizon::TenMinutes => return Ok(None),
        };
        let mut statement = self.connection.prepare(
            "SELECT open_time, move_percent
               FROM event_prediction_tickets
              WHERE status = 'settled'
                AND result IN ('win', 'loss', 'tie')
                AND symbol = ?1
                AND horizon_minutes = ?2
                AND close_time <= ?3
                AND move_percent IS NOT NULL
                AND instr(features_json, '\"strategy_version\"') > 0
              ORDER BY close_time DESC, COALESCE(settled_at, close_time) DESC
              LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![symbol, horizon.minutes(), now, EVENT_EXPERT_HISTORY_LIMIT],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, f64>(1)?)),
        )?;
        let mut seen_buckets = BTreeSet::new();
        let mut samples = Vec::new();
        for row in rows {
            let (open_time, move_percent) = row?;
            let bucket = open_time.div_euclid(spacing_seconds);
            if !seen_buckets.insert(bucket) {
                continue;
            }
            samples.push(ExpertSample { move_percent });
            if samples.len() >= sample_limit {
                break;
            }
        }
        if samples.is_empty() {
            return Ok(None);
        }
        let up = samples
            .iter()
            .filter(|sample| sample.move_percent > 0.000001)
            .count();
        let up_rate = up as f64 / samples.len() as f64;
        let direction =
            if samples.len() >= EVENT_EXPERT_MIN_SAMPLES && up_rate >= EVENT_UP_COMMITMENT_RATE {
                EventDirection::Up
            } else {
                EventDirection::Down
            };
        let reason = if direction == EventDirection::Up {
            "rolling_up_commitment_v9"
        } else {
            "rolling_down_commitment_v9"
        };
        let score_strength = if samples.len() < EVENT_EXPERT_MIN_SAMPLES {
            0.20
        } else {
            ((up_rate - 0.5).abs() * 2.0).clamp(0.20, 0.65)
        };
        Ok(Some(DirectionRegimeBias {
            direction,
            reason,
            sample_size: samples.len() as i64,
            up_rate,
            long_sample_size: None,
            long_up_rate: None,
            score_strength,
        }))
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
                       AND horizon_minutes IN (10, 30, 60)
                       AND instr(features_json, ?1) > 0"
        } else {
            "SELECT COALESCE(SUM(virtual_pnl), 0.0)
                     FROM event_prediction_tickets
                     WHERE status = 'settled'
                       AND symbol IN ('BTCUSDT', 'ETHUSDT')
                       AND horizon_minutes IN (10, 30, 60)"
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
                       AND horizon_minutes IN (10, 30, 60)
                       AND instr(features_json, ?1) > 0"
        } else {
            "SELECT COALESCE(SUM(stake_amount), 0.0)
                     FROM event_prediction_tickets
                     WHERE status = 'open'
                       AND symbol IN ('BTCUSDT', 'ETHUSDT')
                       AND horizon_minutes IN (10, 30, 60)"
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
                   AND horizon_minutes IN (10, 30, 60)
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
                   AND horizon_minutes IN (10, 30, 60)
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
                    COALESCE(result, ''), move_percent, virtual_pnl,
                    COALESCE(cycle_id, ''), COALESCE(cycle_number, 0),
                    COALESCE(cycle_order, 0), cycle_slot, cycle_balance_after, COALESCE(review, '')
             FROM event_prediction_tickets
             WHERE status = 'open'
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
               AND horizon_minutes IN (10, 30, 60)
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
                    COALESCE(result, ''), move_percent, virtual_pnl,
                    COALESCE(cycle_id, ''), COALESCE(cycle_number, 0),
                    COALESCE(cycle_order, 0), cycle_slot, cycle_balance_after, COALESCE(review, '')
             FROM event_prediction_tickets
             WHERE status = 'settled'
               AND symbol IN ('BTCUSDT', 'ETHUSDT')
               AND horizon_minutes IN (10, 30, 60)
               AND instr(features_json, ?2) > 0
             ORDER BY COALESCE(settled_at, close_time) DESC, close_time DESC, open_time DESC
             LIMIT ?1",
        )?;
        let rows = statement.query_map(params![limit as i64, marker], ticket_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to read settled event predictions")
    }

    pub fn cycle_tickets(&self, cycle_id: &str) -> Result<Vec<EventPredictionTicket>> {
        let cycle_id = cycle_id.trim();
        if cycle_id.is_empty() {
            bail!("event prediction cycle id is empty");
        }
        let mut statement = self.connection.prepare(
            "SELECT id, symbol, horizon_minutes, open_time, close_time, direction,
                    confidence, score, stake_amount, entry_price, expiry_price, status,
                    COALESCE(result, ''), move_percent, virtual_pnl,
                    COALESCE(cycle_id, ''), COALESCE(cycle_number, 0),
                    COALESCE(cycle_order, 0), cycle_slot, cycle_balance_after, COALESCE(review, '')
             FROM event_prediction_tickets
             WHERE cycle_id = ?1
             ORDER BY cycle_order ASC, cycle_slot ASC, created_at ASC, open_time ASC",
        )?;
        let rows = statement.query_map([cycle_id], ticket_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .context("Failed to read event prediction cycle tickets")
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
        cycle_id: row.get(15)?,
        cycle_number: row.get(16)?,
        cycle_order: row.get(17)?,
        cycle_slot: row.get(18)?,
        cycle_balance_after: row.get(19)?,
        review: row.get(20)?,
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
    let client = network::binance_client(Duration::from_secs(20))?;
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
    let legacy_open_count = log.legacy_open_count()?;
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
        "事件预测：当前策略 {}，周期票未结算 {}，等待旧票结算 {}，当前占用 {:.2}，当前策略盈亏 {:+.2}，全历史盈亏 {:+.2}，信号结算 {}",
        EVENT_STRATEGY_NAME,
        open_count,
        legacy_open_count,
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
        legacy_open_count,
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
        if horizon.is_active()
            && !normalized
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

fn migrate_schema(connection: &mut Connection) -> Result<()> {
    let schema_version =
        connection.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))?;
    if schema_version == EVENT_SCHEMA_VERSION {
        return Ok(());
    }
    if schema_version > EVENT_SCHEMA_VERSION {
        bail!(
            "event prediction database schema version {schema_version} is newer than supported version {EVENT_SCHEMA_VERSION}"
        );
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .context("Failed to start event prediction schema migration")?;
    let columns = {
        let mut statement = transaction.prepare("PRAGMA table_info(event_prediction_tickets)")?;
        statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    if schema_version < 1 {
        if !columns.iter().any(|column| column == "stake_amount") {
            transaction.execute(
                "ALTER TABLE event_prediction_tickets
                 ADD COLUMN stake_amount REAL NOT NULL DEFAULT 5.0",
                [],
            )?;
        }
        for (column, definition) in [
            ("cycle_id", "TEXT"),
            ("cycle_number", "INTEGER"),
            ("cycle_order", "INTEGER"),
            ("cycle_balance_after", "REAL"),
        ] {
            if !columns.iter().any(|existing| existing == column) {
                transaction.execute(
                    &format!(
                        "ALTER TABLE event_prediction_tickets ADD COLUMN {column} {definition}"
                    ),
                    [],
                )?;
            }
        }
        migrate_payout_model(&transaction)?;
    }
    if !columns.iter().any(|column| column == "cycle_slot") {
        transaction.execute(
            "ALTER TABLE event_prediction_tickets ADD COLUMN cycle_slot INTEGER",
            [],
        )?;
    }
    ensure_cycle_indexes_can_be_created(&transaction)?;
    transaction.execute_batch(
        "DROP INDEX IF EXISTS unique_event_prediction_round;
         DROP INDEX IF EXISTS event_prediction_cycle;
         DROP INDEX IF EXISTS unique_event_prediction_cycle_order;
         DROP INDEX IF EXISTS unique_event_prediction_open_cycle_chain;
         DROP INDEX IF EXISTS event_prediction_cycle_id;
         CREATE UNIQUE INDEX IF NOT EXISTS unique_event_prediction_round_slot
         ON event_prediction_tickets(symbol, horizon_minutes, open_time, cycle_slot)
         WHERE cycle_slot IS NOT NULL;
         CREATE INDEX IF NOT EXISTS event_prediction_cycle
         ON event_prediction_tickets(symbol, horizon_minutes, cycle_number, cycle_order, cycle_slot);
         CREATE UNIQUE INDEX IF NOT EXISTS unique_event_prediction_cycle_order
         ON event_prediction_tickets(symbol, horizon_minutes, cycle_number, cycle_order, cycle_slot)
         WHERE cycle_number IS NOT NULL AND cycle_order IS NOT NULL AND cycle_slot IS NOT NULL;
         CREATE UNIQUE INDEX IF NOT EXISTS unique_event_prediction_open_cycle_slot
         ON event_prediction_tickets(symbol, horizon_minutes, cycle_slot)
         WHERE status = 'open' AND cycle_number IS NOT NULL AND cycle_slot IS NOT NULL;
         CREATE INDEX IF NOT EXISTS event_prediction_cycle_id
         ON event_prediction_tickets(cycle_id, cycle_order, cycle_slot);",
    )?;
    transaction.pragma_update(None, "user_version", EVENT_SCHEMA_VERSION)?;
    transaction
        .commit()
        .context("Failed to commit event prediction schema migration")?;
    Ok(())
}

fn ensure_cycle_indexes_can_be_created(connection: &Connection) -> Result<()> {
    let duplicate_order = connection.query_row(
        "SELECT EXISTS(
            SELECT 1
              FROM event_prediction_tickets
             WHERE cycle_number IS NOT NULL AND cycle_order IS NOT NULL AND cycle_slot IS NOT NULL
             GROUP BY symbol, horizon_minutes, cycle_number, cycle_order, cycle_slot
            HAVING COUNT(*) > 1
         )",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if duplicate_order {
        bail!("duplicate event prediction cycle orders must be resolved before migration");
    }
    let duplicate_open_chain = connection.query_row(
        "SELECT EXISTS(
            SELECT 1
              FROM event_prediction_tickets
             WHERE status = 'open' AND cycle_number IS NOT NULL AND cycle_slot IS NOT NULL
             GROUP BY symbol, horizon_minutes, cycle_slot
            HAVING COUNT(*) > 1
         )",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if duplicate_open_chain {
        bail!("multiple open event prediction tickets exist in one cycle slot");
    }
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
             WHERE status = 'settled'
               AND result IN ('win', 'loss', 'tie')
               AND (virtual_pnl IS NULL OR ABS(virtual_pnl - CASE
                    WHEN result = 'win' AND horizon_minutes = 10 THEN stake_amount * ?1
                    WHEN result = 'win' THEN stake_amount * ?2
                    WHEN result = 'loss' THEN -stake_amount
                    WHEN result = 'tie' THEN 0.0
                    ELSE virtual_pnl
               END) > 0.000000001)",
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
             WHERE status = 'settled'
               AND result IN ('win', 'loss', 'tie')
               AND (virtual_pnl IS NULL OR ABS(virtual_pnl - CASE
                    WHEN result = 'win' AND horizon_minutes = 10 THEN stake_amount * ?1
                    WHEN result = 'win' THEN stake_amount * ?2
                    WHEN result = 'loss' THEN -stake_amount
                    WHEN result = 'tie' THEN 0.0
                    ELSE virtual_pnl
               END) > 0.000000001)",
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
        let cycle_plans = log.cycle_plans(symbol, *horizon)?;
        let regime_bias = log.recent_expert_bias(symbol, *horizon, now)?;
        let base_prediction = make_prediction(
            symbol,
            *horizon,
            &candles,
            &snapshot,
            open_time,
            regime_bias.as_ref(),
        )?;
        let state_decision = extreme_commitment_decision(&base_prediction.features);
        summary.evaluated += 1;
        if cycle_plans.is_empty() {
            let decision = EventStrategyDecision::observe(format!(
                "batch_waiting_for_settlement_{}",
                state_decision.reason
            ));
            let _ = log.record_signal(&base_prediction, &decision, now)?;
            summary.directions.push(EventPredictionRunDirection {
                symbol: base_prediction.symbol.clone(),
                horizon_minutes: base_prediction.horizon.minutes(),
                open_time: base_prediction.open_time,
                close_time: base_prediction.close_time,
                direction: base_prediction.direction.as_str().into(),
                confidence: base_prediction.confidence,
                created: false,
            });
            continue;
        }
        let signal_decision =
            EventStrategyDecision::trade(format!("{}_batch5_available", state_decision.reason));
        let _ = log.record_signal(&base_prediction, &signal_decision, now)?;
        let mut any_created = false;
        for plan in cycle_plans {
            let mut prediction = base_prediction.clone();
            prediction.id = format!(
                "{}-S{}-O{}",
                prediction.id, plan.cycle_slot, plan.cycle_order
            );
            prediction.stake_amount = plan.stake_amount;
            prediction.cycle_id = plan.cycle_id.clone();
            prediction.cycle_number = plan.cycle_number;
            prediction.cycle_order = plan.cycle_order;
            prediction.cycle_slot = plan.cycle_slot;
            add_cycle_fields(&mut prediction.features, &plan);
            let created = log.record_prediction(&prediction, now)?;
            summary.created += usize::from(created);
            any_created |= created;
        }
        summary.directions.push(EventPredictionRunDirection {
            symbol: base_prediction.symbol.clone(),
            horizon_minutes: base_prediction.horizon.minutes(),
            open_time: base_prediction.open_time,
            close_time: base_prediction.close_time,
            direction: base_prediction.direction.as_str().into(),
            confidence: base_prediction.confidence,
            created: any_created,
        });
    }
    Ok(summary)
}

fn extreme_commitment_decision(features: &Value) -> EventStrategyDecision {
    let sample_size = feature_value(features, "regime_sample_size")
        .max(0.0)
        .round() as usize;
    let up_rate = feature_value(features, "regime_up_rate");
    let strong_up = sample_size >= EVENT_EXTREME_MIN_SAMPLES && up_rate >= EVENT_EXTREME_UP_RATE;
    let strong_down =
        sample_size >= EVENT_EXTREME_MIN_SAMPLES && up_rate <= EVENT_EXTREME_DOWN_RATE;
    let state = if strong_up {
        "extreme_up"
    } else if strong_down {
        "extreme_down"
    } else {
        "balanced"
    };
    EventStrategyDecision::trade(format!(
        "full_volume_{state}_v10_2_sample_{sample_size}_up_rate_{up_rate:.3}"
    ))
}

fn add_cycle_fields(features: &mut Value, plan: &CyclePlan) {
    if let Some(object) = features.as_object_mut() {
        object.insert("cycle_id".into(), json!(plan.cycle_id));
        object.insert("cycle_number".into(), json!(plan.cycle_number));
        object.insert("cycle_order".into(), json!(plan.cycle_order));
        object.insert("cycle_slot".into(), json!(plan.cycle_slot));
        object.insert("cycle_slots".into(), json!(EVENT_CYCLE_SLOTS));
        object.insert("cycle_stake_amount".into(), json!(plan.stake_amount));
    }
}

fn make_prediction(
    symbol: &str,
    horizon: EventHorizon,
    candles: &[Candle],
    snapshot: &MarketSnapshot,
    open_time: i64,
    regime_bias: Option<&DirectionRegimeBias>,
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
    let direction_decision =
        calibrated_direction(horizon, raw_score, raw_direction, &features, regime_bias);
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
        cycle_id: String::new(),
        cycle_number: 0,
        cycle_order: 0,
        cycle_slot: 0,
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
    regime_sample_size: Option<i64>,
    regime_up_rate: Option<f64>,
    regime_long_sample_size: Option<i64>,
    regime_long_up_rate: Option<f64>,
}

#[derive(Debug, Clone)]
struct DirectionRegimeBias {
    direction: EventDirection,
    reason: &'static str,
    sample_size: i64,
    up_rate: f64,
    long_sample_size: Option<i64>,
    long_up_rate: Option<f64>,
    score_strength: f64,
}

#[derive(Debug, Clone, Copy)]
struct ExpertSample {
    move_percent: f64,
}

fn fast_reversal_signal(
    horizon: EventHorizon,
    features: &Value,
) -> Option<(EventDirection, &'static str, f64)> {
    if !matches!(horizon, EventHorizon::ThirtyMinutes) {
        return None;
    }
    let momentum3 = feature_value(features, "momentum3");
    let momentum30 = feature_value(features, "momentum30");
    if momentum3 >= 0.20 && momentum30 <= -0.30 {
        return Some((
            EventDirection::Down,
            "bottom_reversal_up_blocked_v8",
            (0.5 * momentum3 + 0.5 * (-momentum30)).clamp(0.08, 0.80),
        ));
    }
    if momentum3 <= -0.10 && momentum30 >= 0.20 {
        return Some((
            EventDirection::Down,
            "fast_top_reversal_down_v8",
            (0.5 * (-momentum3) + 0.5 * momentum30).clamp(0.08, 0.80),
        ));
    }
    None
}

fn calibrated_direction(
    horizon: EventHorizon,
    raw_score: f64,
    raw_direction: EventDirection,
    features: &Value,
    regime_bias: Option<&DirectionRegimeBias>,
) -> DirectionDecision {
    let abs_score = raw_score.abs();
    if let Some(bias) = regime_bias {
        return DirectionDecision {
            direction: bias.direction,
            reason: bias.reason,
            flipped: raw_direction != bias.direction,
            score_strength: abs_score.max(bias.score_strength).clamp(0.05, 1.0),
            factor_score: signed_factor_score(bias.score_strength, bias.direction),
            regime_sample_size: Some(bias.sample_size),
            regime_up_rate: Some(bias.up_rate),
            regime_long_sample_size: bias.long_sample_size,
            regime_long_up_rate: bias.long_up_rate,
        };
    }

    if let Some((direction, reason, factor_score)) = fast_reversal_signal(horizon, features) {
        return DirectionDecision {
            direction,
            reason,
            flipped: raw_direction != direction,
            score_strength: abs_score.max(factor_score).clamp(0.05, 1.0),
            factor_score: signed_factor_score(factor_score, direction),
            regime_sample_size: None,
            regime_up_rate: None,
            regime_long_sample_size: None,
            regime_long_up_rate: None,
        };
    }

    let direction = if raw_score <= 0.0 {
        EventDirection::Up
    } else {
        EventDirection::Down
    };
    let factor_score = signed_factor_score(abs_score.max(0.08), direction);
    DirectionDecision {
        direction,
        reason: "raw_score_contrarian_fallback_v7",
        flipped: raw_direction != direction,
        score_strength: abs_score.max(0.08),
        factor_score,
        regime_sample_size: None,
        regime_up_rate: None,
        regime_long_sample_size: None,
        regime_long_up_rate: None,
    }
}

fn signed_factor_score(abs_score: f64, direction: EventDirection) -> f64 {
    match direction {
        EventDirection::Up => abs_score.abs(),
        EventDirection::Down => -abs_score.abs(),
    }
    .clamp(-1.0, 1.0)
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
        if let Some(sample_size) = decision.regime_sample_size {
            object.insert("regime_sample_size".into(), json!(sample_size));
        }
        if let Some(up_rate) = decision.regime_up_rate {
            object.insert("regime_up_rate".into(), json!(up_rate));
        }
        if let Some(sample_size) = decision.regime_long_sample_size {
            object.insert("regime_long_sample_size".into(), json!(sample_size));
        }
        if let Some(up_rate) = decision.regime_long_up_rate {
            object.insert("regime_long_up_rate".into(), json!(up_rate));
        }
    }
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
    fn predicts_active_event_horizons_and_settles() {
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
        let mut expected_pnl_by_horizon = BTreeMap::new();
        for horizon in EventHorizon::ALL {
            let prediction =
                make_prediction("BTCUSDT", horizon, &candles, &snapshot, open_time, None).unwrap();
            assert!(matches!(
                prediction.direction,
                EventDirection::Up | EventDirection::Down
            ));
            let expected_pnl = if prediction.direction == EventDirection::Up {
                5.0 * event_profit_rate(horizon.minutes())
            } else {
                -5.0
            };
            expected_pnl_by_horizon.insert(horizon.minutes(), expected_pnl);
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
        prices.insert("BTCUSDT".into(), 105.0);
        assert_eq!(
            log.settle_due_with_prices(open_time + 60 * 60 + 1, &prices)
                .unwrap(),
            3
        );
        let dashboard = log.dashboard().unwrap();
        assert_eq!(dashboard.open_count, 0);
        assert_eq!(dashboard.open_exposure, 0.0);
        assert_close(
            dashboard.realized_pnl,
            expected_pnl_by_horizon.values().sum(),
        );
        assert!(dashboard.equity.is_infinite());
        assert!(dashboard.available_balance.is_infinite());
        assert_eq!(dashboard.open_recent.len(), 0);
        assert_eq!(dashboard.settled_recent.len(), 3);
        let pnl_by_horizon = dashboard
            .settled_recent
            .iter()
            .map(|ticket| (ticket.horizon_minutes, ticket.virtual_pnl.unwrap()))
            .collect::<BTreeMap<_, _>>();
        for (horizon, expected_pnl) in expected_pnl_by_horizon {
            assert_close(*pnl_by_horizon.get(&horizon).unwrap(), expected_pnl);
        }
        assert_eq!(
            dashboard.stats.iter().map(|stat| stat.total).sum::<i64>(),
            3
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn active_ten_minute_requests_create_new_samples() {
        let horizons = normalized_horizons(&[EventHorizon::TenMinutes]);
        assert_eq!(
            horizons
                .iter()
                .map(|horizon| horizon.minutes())
                .collect::<Vec<_>>(),
            vec![10]
        );
    }

    #[test]
    fn batch5_first_cycle_opens_five_slots_at_five_usdt() {
        let root = std::env::temp_dir().join(format!("gqt-event-cycle-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        let candles = sample_candles();
        let snapshot = sample_snapshot();
        let horizon = EventHorizon::TenMinutes;
        let plans = log.cycle_plans("BTCUSDT", horizon).unwrap();
        assert_eq!(plans.len(), 5);
        for (index, plan) in plans.iter().enumerate() {
            assert_eq!(plan.cycle_number, 1);
            assert_eq!(plan.cycle_order, 1);
            assert_eq!(plan.cycle_slot, index as i64 + 1);
            assert_close(plan.stake_amount, 5.0);
            let prediction = cycle_prediction(horizon, &candles, &snapshot, 1_700_100_000, plan);
            assert!(
                log.record_prediction(&prediction, prediction.open_time)
                    .unwrap()
            );
        }
        assert_eq!(log.open_count().unwrap(), 5);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn batch5_slots_advance_or_stop_independently_and_have_no_order_limit() {
        let root = std::env::temp_dir().join(format!("gqt-event-cycle-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        let candles = sample_candles();
        let snapshot = sample_snapshot();
        let horizon = EventHorizon::ThirtyMinutes;
        let mut plans = log.cycle_plans("ETHUSDT", horizon).unwrap();
        for plan in &plans {
            let prediction = cycle_prediction(horizon, &candles, &snapshot, 1_700_200_000, plan);
            assert!(
                log.record_prediction(&prediction, prediction.open_time)
                    .unwrap()
            );
        }
        settle_slot(&log, 1, "win", 9.25);
        settle_slot(&log, 2, "tie", 5.0);
        settle_slot(&log, 3, "loss", 0.0);
        plans = log.cycle_plans("ETHUSDT", horizon).unwrap();
        assert_eq!(
            plans.iter().map(|p| p.cycle_slot).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_close(plans[0].stake_amount, 9.25);
        assert_close(plans[1].stake_amount, 5.0);
        for order in 2..=7 {
            let plan = log
                .cycle_plans("ETHUSDT", horizon)
                .unwrap()
                .into_iter()
                .find(|p| p.cycle_slot == 1)
                .unwrap();
            assert_eq!(plan.cycle_order, order);
            let prediction =
                cycle_prediction(horizon, &candles, &snapshot, 1_700_200_000 + order, &plan);
            assert!(
                log.record_prediction(&prediction, prediction.open_time)
                    .unwrap()
            );
            settle_slot(&log, 1, "tie", plan.stake_amount);
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn batch5_starts_next_cycle_only_after_all_five_slots_lose() {
        let root =
            std::env::temp_dir().join(format!("gqt-event-all-loss-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        let candles = sample_candles();
        let snapshot = sample_snapshot();
        let horizon = EventHorizon::TenMinutes;
        for plan in log.cycle_plans("BTCUSDT", horizon).unwrap() {
            let prediction = cycle_prediction(horizon, &candles, &snapshot, 1_700_250_000, &plan);
            assert!(
                log.record_prediction(&prediction, prediction.open_time)
                    .unwrap()
            );
        }
        for slot in 1..EVENT_CYCLE_SLOTS {
            settle_slot(&log, slot, "loss", 0.0);
        }
        assert!(log.cycle_plans("BTCUSDT", horizon).unwrap().is_empty());
        settle_slot(&log, EVENT_CYCLE_SLOTS, "loss", 0.0);
        let next = log.cycle_plans("BTCUSDT", horizon).unwrap();
        assert_eq!(next.len(), 5);
        assert!(next.iter().all(|plan| plan.cycle_number == 2));
        assert!(next.iter().all(|plan| plan.cycle_order == 1));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_open_ticket_blocks_new_cycle_until_settlement() {
        let root = std::env::temp_dir().join(format!("gqt-event-legacy-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        log.connection
            .execute(
                "INSERT INTO event_prediction_tickets
                 (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
                  confidence, score, stake_amount, entry_price, status, features_json)
                 VALUES ('legacy-open', 1, 'BTCUSDT', 10, 60, 660, 'up', 0.5, 0.1,
                         5.0, 100.0, 'open', '{}')",
                [],
            )
            .unwrap();

        assert!(
            log.cycle_plans("BTCUSDT", EventHorizon::TenMinutes)
                .unwrap()
                .is_empty()
        );
        let mut prices = BTreeMap::new();
        prices.insert("BTCUSDT".into(), 101.0);
        assert_eq!(log.settle_due_with_prices(661, &prices).unwrap(), 1);
        assert_eq!(
            log.cycle_plans("BTCUSDT", EventHorizon::TenMinutes)
                .unwrap()
                .len(),
            5
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn database_prevents_two_open_tickets_in_one_cycle_slot() {
        let root = std::env::temp_dir().join(format!("gqt-event-lock-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        let candles = sample_candles();
        let snapshot = sample_snapshot();
        let horizon = EventHorizon::OneHour;
        let plan = log.cycle_plans("BTCUSDT", horizon).unwrap().remove(0);
        let first = cycle_prediction(horizon, &candles, &snapshot, 1_700_300_000, &plan);
        let second = cycle_prediction(horizon, &candles, &snapshot, 1_700_300_060, &plan);

        assert!(log.record_prediction(&first, first.open_time).unwrap());
        assert!(!log.record_prediction(&second, second.open_time).unwrap());
        assert_eq!(log.open_count().unwrap(), 1);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cycle_ticket_query_returns_every_order_in_cycle_order() {
        let root =
            std::env::temp_dir().join(format!("gqt-event-cycle-view-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let log = EventPredictionLog::open(&root.join("events.sqlite")).unwrap();
        for (id, cycle_id, cycle_number, cycle_order, cycle_slot, open_time, status, result) in [
            ("cycle-2", "BTCUSDT-10m-C000001", 1, 2, 2, 200, "open", None),
            (
                "other-cycle",
                "BTCUSDT-10m-C000002",
                2,
                1,
                1,
                300,
                "settled",
                Some("win"),
            ),
            (
                "cycle-1",
                "BTCUSDT-10m-C000001",
                1,
                1,
                2,
                100,
                "settled",
                Some("win"),
            ),
            (
                "cycle-1-slot-1",
                "BTCUSDT-10m-C000001",
                1,
                1,
                1,
                101,
                "settled",
                Some("tie"),
            ),
        ] {
            log.connection
                .execute(
                    "INSERT INTO event_prediction_tickets
                     (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
                      confidence, score, stake_amount, entry_price, status, result, virtual_pnl,
                      features_json, cycle_id, cycle_number, cycle_order, cycle_slot,
                      cycle_balance_after)
                     VALUES (?1, ?2, 'BTCUSDT', 10, ?2, ?2 + 600, 'up', 0.6, 0.2,
                             5.0, 100.0, ?7, ?8, 4.0, '{}', ?3, ?4, ?5, ?6, 9.0)",
                    params![
                        id,
                        open_time,
                        cycle_id,
                        cycle_number,
                        cycle_order,
                        cycle_slot,
                        status,
                        result
                    ],
                )
                .unwrap();
        }

        let tickets = log.cycle_tickets("BTCUSDT-10m-C000001").unwrap();
        assert_eq!(tickets.len(), 3);
        assert_eq!(tickets[0].id, "cycle-1-slot-1");
        assert_eq!(tickets[0].cycle_order, 1);
        assert_eq!(tickets[0].cycle_slot, Some(1));
        assert_eq!(tickets[0].status, "settled");
        assert_eq!(tickets[1].id, "cycle-1");
        assert_eq!(tickets[1].cycle_slot, Some(2));
        assert_eq!(tickets[2].id, "cycle-2");
        assert_eq!(tickets[2].cycle_order, 2);
        assert_eq!(tickets[2].cycle_slot, Some(2));
        assert_eq!(tickets[2].status, "open");
        assert!(log.cycle_tickets("   ").is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn schema_migration_runs_once_and_preserves_legacy_open_tickets() {
        let root =
            std::env::temp_dir().join(format!("gqt-event-migration-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("events.sqlite");
        {
            let connection = Connection::open(&path).unwrap();
            connection
                .execute_batch(
                    "CREATE TABLE event_prediction_tickets (
                        id TEXT PRIMARY KEY,
                        created_at INTEGER NOT NULL,
                        symbol TEXT NOT NULL,
                        horizon_minutes INTEGER NOT NULL,
                        open_time INTEGER NOT NULL,
                        close_time INTEGER NOT NULL,
                        direction TEXT NOT NULL,
                        confidence REAL NOT NULL,
                        score REAL NOT NULL,
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
                     CREATE TABLE event_prediction_signals (
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
                     INSERT INTO event_prediction_tickets
                     (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
                      confidence, score, entry_price, status, features_json)
                     VALUES ('legacy-open', 1, 'BTCUSDT', 10, 60, 660, 'up', 0.5, 0.1,
                             100.0, 'open', '{}');
                     INSERT INTO event_prediction_tickets
                     (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
                      confidence, score, entry_price, status, result, virtual_pnl, features_json)
                     VALUES ('legacy-win', 1, 'ETHUSDT', 30, 60, 1860, 'up', 0.5, 0.1,
                             100.0, 'settled', 'win', 999.0, '{}');",
                )
                .unwrap();
        }

        let log = EventPredictionLog::open(&path).unwrap();
        assert_eq!(log.legacy_open_count().unwrap(), 1);
        assert!(
            log.cycle_plans("BTCUSDT", EventHorizon::TenMinutes)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            log.connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            EVENT_SCHEMA_VERSION
        );
        assert_close(
            log.connection
                .query_row(
                    "SELECT virtual_pnl FROM event_prediction_tickets WHERE id = 'legacy-win'",
                    [],
                    |row| row.get::<_, f64>(0),
                )
                .unwrap(),
            4.25,
        );
        drop(log);

        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE migration_update_counter (updates INTEGER NOT NULL);
                 INSERT INTO migration_update_counter VALUES (0);
                 CREATE TRIGGER count_ticket_migration_updates
                 AFTER UPDATE ON event_prediction_tickets
                 BEGIN
                    UPDATE migration_update_counter SET updates = updates + 1;
                 END;",
            )
            .unwrap();
        drop(connection);

        let log = EventPredictionLog::open(&path).unwrap();
        assert_eq!(
            log.connection
                .query_row("SELECT updates FROM migration_update_counter", [], |row| {
                    row.get::<_, i64>(0)
                },)
                .unwrap(),
            0
        );
        let indexes = {
            let mut statement = log
                .connection
                .prepare("PRAGMA index_list(event_prediction_tickets)")
                .unwrap();
            statement
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert!(
            indexes
                .iter()
                .any(|name| name == "unique_event_prediction_cycle_order")
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "unique_event_prediction_open_cycle_slot")
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "event_prediction_cycle_id")
        );
        drop(log);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn schema_version_one_adds_cycle_lookup_index_without_rewriting_tickets() {
        let root =
            std::env::temp_dir().join(format!("gqt-event-migration-v1-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("events.sqlite");
        let log = EventPredictionLog::open(&path).unwrap();
        log.connection
            .execute(
                "INSERT INTO event_prediction_tickets
                 (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
                  confidence, score, stake_amount, entry_price, status, result, virtual_pnl,
                  features_json, cycle_id, cycle_number, cycle_order, cycle_balance_after)
                 VALUES ('v1-cycle', 1, 'BTCUSDT', 10, 60, 660, 'up', 0.5, 0.1,
                         5.0, 100.0, 'settled', 'win', 4.0, '{}',
                         'BTCUSDT-10m-C000001', 1, 1, 9.0)",
                [],
            )
            .unwrap();
        log.connection
            .execute_batch(
                "DROP INDEX event_prediction_cycle_id;
                 PRAGMA user_version = 1;
                 CREATE TABLE migration_v1_update_counter (updates INTEGER NOT NULL);
                 INSERT INTO migration_v1_update_counter VALUES (0);
                 CREATE TRIGGER count_v1_ticket_updates
                 AFTER UPDATE ON event_prediction_tickets
                 BEGIN
                    UPDATE migration_v1_update_counter SET updates = updates + 1;
                 END;",
            )
            .unwrap();
        drop(log);

        let log = EventPredictionLog::open(&path).unwrap();
        assert_eq!(
            log.connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            EVENT_SCHEMA_VERSION
        );
        assert_eq!(
            log.connection
                .query_row(
                    "SELECT updates FROM migration_v1_update_counter",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0
        );
        let has_lookup_index = log
            .connection
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM pragma_index_list('event_prediction_tickets')
                     WHERE name = 'event_prediction_cycle_id'
                 )",
                [],
                |row| row.get::<_, bool>(0),
            )
            .unwrap();
        assert!(has_lookup_index);
        drop(log);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn schema_v2_to_v3_keeps_history_null_and_new_batch_follows_historical_cycle() {
        let root =
            std::env::temp_dir().join(format!("gqt-event-migration-v2-{}", rand::random::<u64>()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("events.sqlite");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE event_prediction_tickets (
                id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, symbol TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL, open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL, direction TEXT NOT NULL, confidence REAL NOT NULL,
                score REAL NOT NULL, stake_amount REAL NOT NULL DEFAULT 5.0,
                entry_price REAL NOT NULL, expiry_price REAL, status TEXT NOT NULL, result TEXT,
                move_percent REAL, virtual_pnl REAL, features_json TEXT NOT NULL, review TEXT,
                settled_at INTEGER, cycle_id TEXT, cycle_number INTEGER, cycle_order INTEGER,
                cycle_balance_after REAL
             );
             CREATE TABLE event_prediction_signals (
                id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, symbol TEXT NOT NULL,
                horizon_minutes INTEGER NOT NULL, open_time INTEGER NOT NULL,
                close_time INTEGER NOT NULL, direction TEXT NOT NULL, confidence REAL NOT NULL,
                score REAL NOT NULL, stake_amount REAL NOT NULL DEFAULT 5.0,
                entry_price REAL NOT NULL, action TEXT NOT NULL, strategy TEXT NOT NULL,
                skip_reason TEXT, status TEXT NOT NULL, result TEXT, expiry_price REAL,
                move_percent REAL, virtual_pnl REAL, features_json TEXT NOT NULL, review TEXT,
                settled_at INTEGER
             );
             INSERT INTO event_prediction_tickets
             (id, created_at, symbol, horizon_minutes, open_time, close_time, direction,
              confidence, score, stake_amount, entry_price, status, result, features_json,
              cycle_id, cycle_number, cycle_order, cycle_balance_after)
             VALUES ('v1-history', 1, 'BTCUSDT', 10, 60, 660, 'up', 0.5, 0.1, 5.0,
                     100.0, 'settled', 'loss', '{}', 'BTCUSDT-10m-C000007', 7, 1, 0.0);
             CREATE TABLE migration_v2_update_counter (updates INTEGER NOT NULL);
             INSERT INTO migration_v2_update_counter VALUES (0);
             CREATE TRIGGER count_v2_ticket_updates AFTER UPDATE ON event_prediction_tickets
             BEGIN UPDATE migration_v2_update_counter SET updates = updates + 1; END;
             PRAGMA user_version = 2;",
            )
            .unwrap();
        drop(connection);

        let log = EventPredictionLog::open(&path).unwrap();
        assert_eq!(
            log.connection
                .query_row(
                    "SELECT updates FROM migration_v2_update_counter",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
        assert!(log.connection.query_row(
            "SELECT cycle_slot IS NULL FROM event_prediction_tickets WHERE id = 'v1-history'",
            [], |row| row.get::<_, bool>(0)
        ).unwrap());
        let plans = log
            .cycle_plans("BTCUSDT", EventHorizon::TenMinutes)
            .unwrap();
        assert_eq!(plans.len(), 5);
        assert!(plans.iter().all(|plan| plan.cycle_number == 8));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn direction_dataset_v9_commits_to_one_direction() {
        let neutral_features = json!({
            "momentum3": 0.0,
            "momentum30": 0.0,
        });
        let raw_up = calibrated_direction(
            EventHorizon::ThirtyMinutes,
            0.42,
            EventDirection::Up,
            &neutral_features,
            None,
        );
        assert_eq!(raw_up.direction, EventDirection::Down);
        assert_eq!(raw_up.reason, "raw_score_contrarian_fallback_v7");
        assert!(raw_up.flipped);

        let raw_down = calibrated_direction(
            EventHorizon::OneHour,
            -0.42,
            EventDirection::Down,
            &neutral_features,
            None,
        );
        assert_eq!(raw_down.direction, EventDirection::Up);
        assert!(raw_down.flipped);

        let bottom_features = json!({
            "momentum3": 0.25,
            "momentum30": -0.35,
        });
        let bottom = calibrated_direction(
            EventHorizon::ThirtyMinutes,
            -0.40,
            EventDirection::Down,
            &bottom_features,
            None,
        );
        assert_eq!(bottom.direction, EventDirection::Down);
        assert_eq!(bottom.reason, "bottom_reversal_up_blocked_v8");
        assert!(!bottom.flipped);

        let top_features = json!({
            "momentum3": -0.12,
            "momentum30": 0.22,
        });
        let top = calibrated_direction(
            EventHorizon::ThirtyMinutes,
            0.40,
            EventDirection::Up,
            &top_features,
            None,
        );
        assert_eq!(top.direction, EventDirection::Down);
        assert_eq!(top.reason, "fast_top_reversal_down_v8");
        assert!(top.flipped);

        let commitment_bias = DirectionRegimeBias {
            direction: EventDirection::Down,
            reason: "rolling_down_commitment_v9",
            sample_size: 8,
            up_rate: 0.625,
            long_sample_size: None,
            long_up_rate: None,
            score_strength: 0.25,
        };
        let committed = calibrated_direction(
            EventHorizon::OneHour,
            -0.10,
            EventDirection::Up,
            &neutral_features,
            Some(&commitment_bias),
        );
        assert_eq!(committed.direction, EventDirection::Down);
        assert_eq!(committed.reason, "rolling_down_commitment_v9");

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
    fn direction_dataset_v10_2_keeps_full_volume() {
        let strong_up = extreme_commitment_decision(&json!({
            "regime_sample_size": 20,
            "regime_up_rate": 0.80,
        }));
        assert!(strong_up.should_trade());
        assert_eq!(strong_up.action.as_str(), "trade");

        let strong_down = extreme_commitment_decision(&json!({
            "regime_sample_size": 20,
            "regime_up_rate": 0.20,
        }));
        assert!(strong_down.should_trade());

        let middle = extreme_commitment_decision(&json!({
            "regime_sample_size": 20,
            "regime_up_rate": 0.55,
        }));
        assert!(middle.should_trade());
        assert_eq!(middle.action.as_str(), "trade");
        assert!(middle.reason.contains("balanced"));
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
            None,
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

    fn sample_snapshot() -> MarketSnapshot {
        MarketSnapshot {
            symbol: "BTCUSDT".into(),
            price: 103.0,
            mark_price: 103.0,
            long_short_ratio: 1.12,
            funding_rate: 0.0001,
            change_percent: 2.0,
            ..Default::default()
        }
    }

    fn cycle_prediction(
        horizon: EventHorizon,
        candles: &[Candle],
        snapshot: &MarketSnapshot,
        open_time: i64,
        plan: &CyclePlan,
    ) -> NewEventPrediction {
        let symbol = if horizon == EventHorizon::ThirtyMinutes {
            "ETHUSDT"
        } else {
            "BTCUSDT"
        };
        let mut prediction =
            make_prediction(symbol, horizon, candles, snapshot, open_time, None).unwrap();
        prediction.stake_amount = plan.stake_amount;
        prediction.cycle_id = plan.cycle_id.clone();
        prediction.cycle_number = plan.cycle_number;
        prediction.cycle_order = plan.cycle_order;
        prediction.cycle_slot = plan.cycle_slot;
        prediction.id = format!(
            "{}-S{}-O{}",
            prediction.id, plan.cycle_slot, plan.cycle_order
        );
        add_cycle_fields(&mut prediction.features, plan);
        prediction
    }

    fn settle_slot(log: &EventPredictionLog, cycle_slot: i64, result: &str, balance_after: f64) {
        let stake = log
            .connection
            .query_row(
                "SELECT stake_amount FROM event_prediction_tickets
              WHERE status = 'open' AND cycle_slot = ?1",
                [cycle_slot],
                |row| row.get::<_, f64>(0),
            )
            .unwrap();
        let pnl = balance_after - stake;
        assert_eq!(
            log.connection
                .execute(
                    "UPDATE event_prediction_tickets
                SET status = 'settled', result = ?1, virtual_pnl = ?2,
                    cycle_balance_after = ?3, settled_at = close_time
              WHERE status = 'open' AND cycle_slot = ?4",
                    params![result, pnl, balance_after, cycle_slot],
                )
                .unwrap(),
            1
        );
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 0.000001,
            "expected {expected}, got {actual}"
        );
    }
}
