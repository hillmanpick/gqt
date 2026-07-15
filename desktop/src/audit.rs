use std::path::Path;

use anyhow::Result;
use rusqlite::{Connection, params};

use crate::model::{AiTradeSignal, AiTradingInput};

pub struct AuditLog {
    connection: Connection,
}

impl AuditLog {
    pub fn open(path: &Path) -> Result<Self> {
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS ai_decisions (
                decision_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                candle_open_time INTEGER NOT NULL,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                input_json TEXT NOT NULL,
                raw_output TEXT NOT NULL,
                parsed_signal_json TEXT NOT NULL,
                risk_result TEXT NOT NULL,
                risk_reason TEXT NOT NULL,
                order_result_json TEXT
             );
             CREATE UNIQUE INDEX IF NOT EXISTS unique_ai_candle
             ON ai_decisions(symbol, timeframe, candle_open_time);",
        )?;
        Ok(Self { connection })
    }

    pub fn record_decision(
        &self,
        provider: &str,
        model: &str,
        input: &AiTradingInput,
        raw_output: &str,
        signal: &AiTradeSignal,
        approved: bool,
        risk_reason: &str,
    ) -> Result<bool> {
        let changed = self.connection.execute(
            "INSERT OR IGNORE INTO ai_decisions
             (decision_id, created_at, symbol, timeframe, candle_open_time, provider, model,
              input_json, raw_output, parsed_signal_json, risk_result, risk_reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                signal.decision_id,
                chrono::Utc::now().timestamp(),
                signal.symbol,
                signal.timeframe,
                signal.candle_open_time,
                provider,
                model,
                serde_json::to_string(input)?,
                raw_output,
                serde_json::to_string(signal)?,
                if approved { "approved" } else { "hold" },
                risk_reason,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn record_order_result(&self, decision_id: &str, result_json: &str) -> Result<()> {
        self.connection.execute(
            "UPDATE ai_decisions SET order_result_json = ?1 WHERE decision_id = ?2",
            params![result_json, decision_id],
        )?;
        Ok(())
    }
}
