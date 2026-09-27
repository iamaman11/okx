use std::collections::BTreeSet;

use okx_api::PublicCandle;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::ReferenceRegistry;

pub const MARKET_HISTORY_SCHEMA_V1: &str = "okx.market-history/v1";
pub const MARKET_HISTORY_SOURCE_V1: &str = "okx_public_rest_history";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCandle {
    pub open_time_ms: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub volume_currency: String,
    pub volume_quote: Option<String>,
    pub confirmed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketHistorySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub bar: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub history_generation: String,
    pub all_confirmed: bool,
    pub oldest_open_time_ms: String,
    pub newest_open_time_ms: String,
    pub candles: Vec<HistoryCandle>,
}

#[derive(Debug, Error)]
pub enum MarketHistoryError {
    #[error("history source receive timestamp is empty")]
    EmptySourceTimestamp,

    #[error("history limit must be between 1 and 100")]
    InvalidLimit,

    #[error("instrument '{0}' is not present in the reference registry")]
    InstrumentNotFound(String),

    #[error("instrument '{0}' is not live")]
    InstrumentNotLive(String),

    #[error("OKX returned no history candles")]
    EmptyHistory,

    #[error("OKX returned more history candles than requested")]
    TooManyRows,

    #[error("history candle timestamp '{0}' is invalid")]
    InvalidTimestamp(String),

    #[error("history contains duplicate candle timestamp '{0}'")]
    DuplicateTimestamp(String),

    #[error("history candle is missing required field '{0}'")]
    MissingField(&'static str),

    #[error("history candle confirm value '{0}' is invalid")]
    InvalidConfirm(String),

    #[error("failed to serialize normalized market history: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl MarketHistorySnapshot {
    pub fn from_public(
        reference: &ReferenceRegistry,
        instrument_id: &str,
        bar: &str,
        requested_limit: u16,
        source_received_at: impl Into<String>,
        rows: Vec<PublicCandle>,
    ) -> Result<Self, MarketHistoryError> {
        if !(1..=100).contains(&requested_limit) {
            return Err(MarketHistoryError::InvalidLimit);
        }

        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(MarketHistoryError::EmptySourceTimestamp);
        }

        let instrument = reference
            .get(instrument_id)
            .ok_or_else(|| MarketHistoryError::InstrumentNotFound(instrument_id.to_owned()))?;
        if instrument.state != "live" {
            return Err(MarketHistoryError::InstrumentNotLive(
                instrument_id.to_owned(),
            ));
        }

        if rows.is_empty() {
            return Err(MarketHistoryError::EmptyHistory);
        }
        if rows.len() > requested_limit as usize {
            return Err(MarketHistoryError::TooManyRows);
        }

        let mut normalized = Vec::with_capacity(rows.len());
        let mut timestamps = BTreeSet::new();

        for row in rows {
            let timestamp = row
                .timestamp_ms
                .parse::<u64>()
                .map_err(|_| MarketHistoryError::InvalidTimestamp(row.timestamp_ms.clone()))?;
            if !timestamps.insert(timestamp) {
                return Err(MarketHistoryError::DuplicateTimestamp(row.timestamp_ms));
            }

            let confirmed = match row.confirm.as_str() {
                "0" => false,
                "1" => true,
                _ => return Err(MarketHistoryError::InvalidConfirm(row.confirm)),
            };

            normalized.push((
                timestamp,
                HistoryCandle {
                    open_time_ms: required("ts", row.timestamp_ms)?,
                    open: required("o", row.open)?,
                    high: required("h", row.high)?,
                    low: required("l", row.low)?,
                    close: required("c", row.close)?,
                    volume: required("vol", row.volume)?,
                    volume_currency: required("volCcy", row.volume_currency)?,
                    volume_quote: optional(row.volume_quote),
                    confirmed,
                },
            ));
        }

        normalized.sort_by_key(|(timestamp, _)| *timestamp);
        let candles = normalized
            .into_iter()
            .map(|(_, candle)| candle)
            .collect::<Vec<_>>();
        let oldest_open_time_ms = candles
            .first()
            .expect("history is non-empty")
            .open_time_ms
            .clone();
        let newest_open_time_ms = candles
            .last()
            .expect("history is non-empty")
            .open_time_ms
            .clone();
        let all_confirmed = candles.iter().all(|candle| candle.confirmed);

        let mut snapshot = Self {
            schema: MARKET_HISTORY_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.to_owned(),
            bar: bar.to_owned(),
            requested_limit,
            reference_generation: reference.generation().as_str().to_owned(),
            source: MARKET_HISTORY_SOURCE_V1.to_owned(),
            source_received_at,
            history_generation: String::new(),
            all_confirmed,
            oldest_open_time_ms,
            newest_open_time_ms,
            candles,
        };
        snapshot.history_generation = generation_for(&snapshot)?;
        Ok(snapshot)
    }
}

fn required(field: &'static str, value: String) -> Result<String, MarketHistoryError> {
    if value.trim().is_empty() {
        Err(MarketHistoryError::MissingField(field))
    } else {
        Ok(value)
    }
}

fn optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| (!value.trim().is_empty()).then_some(value))
}

fn generation_for(snapshot: &MarketHistorySnapshot) -> Result<String, MarketHistoryError> {
    #[derive(Serialize)]
    struct GenerationInput<'a> {
        schema: &'static str,
        instrument_id: &'a str,
        bar: &'a str,
        requested_limit: u16,
        reference_generation: &'a str,
        source: &'static str,
        candles: &'a [HistoryCandle],
    }

    let encoded = serde_json::to_vec(&GenerationInput {
        schema: MARKET_HISTORY_SCHEMA_V1,
        instrument_id: &snapshot.instrument_id,
        bar: &snapshot.bar,
        requested_limit: snapshot.requested_limit,
        reference_generation: &snapshot.reference_generation,
        source: MARKET_HISTORY_SOURCE_V1,
        candles: &snapshot.candles,
    })?;
    let digest = Sha256::digest(encoded);
    Ok(format!("sha256:{digest:x}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::PublicInstrument;

    fn reference() -> ReferenceRegistry {
        ReferenceRegistry::from_public(
            "2026-09-27T00:00:00.000Z",
            vec![PublicInstrument {
                instrument_type: "SWAP".to_owned(),
                instrument_id: "DOGE-USDT-SWAP".to_owned(),
                instrument_family: "DOGE-USDT".to_owned(),
                underlying: "DOGE-USDT".to_owned(),
                state: "live".to_owned(),
                rule_type: "normal".to_owned(),
                base_currency: String::new(),
                quote_currency: String::new(),
                settle_currency: "USDT".to_owned(),
                tick_size: "0.00001".to_owned(),
                lot_size: "0.01".to_owned(),
                min_size: "0.01".to_owned(),
                max_limit_size: "1000000".to_owned(),
                max_market_size: "100000".to_owned(),
                max_limit_amount: String::new(),
                max_market_amount: String::new(),
                contract_type: "linear".to_owned(),
                contract_value: "1000".to_owned(),
                contract_value_currency: "DOGE".to_owned(),
                fee_group_id: "4".to_owned(),
                lever: "50".to_owned(),
                list_time: "1700000000000".to_owned(),
                expiry_time: String::new(),
            }],
        )
        .expect("reference")
    }

    fn row(timestamp_ms: &str, close: &str, confirm: &str) -> PublicCandle {
        PublicCandle {
            timestamp_ms: timestamp_ms.to_owned(),
            open: "0.12".to_owned(),
            high: "0.13".to_owned(),
            low: "0.11".to_owned(),
            close: close.to_owned(),
            volume: "100".to_owned(),
            volume_currency: "12.5".to_owned(),
            volume_quote: Some("12.5".to_owned()),
            confirm: confirm.to_owned(),
        }
    }

    #[test]
    fn history_is_chronological_and_content_addressed() {
        let first = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:00:00.000Z",
            vec![
                row("1790470800000", "0.126", "1"),
                row("1790467200000", "0.125", "1"),
            ],
        )
        .expect("history");

        assert_eq!(first.candles[0].open_time_ms, "1790467200000");
        assert_eq!(first.candles[1].open_time_ms, "1790470800000");
        assert!(first.all_confirmed);

        let second = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:01:00.000Z",
            vec![
                row("1790470800000", "0.126", "1"),
                row("1790467200000", "0.125", "1"),
            ],
        )
        .expect("history");

        assert_eq!(first.history_generation, second.history_generation);
        assert_ne!(first.source_received_at, second.source_received_at);
    }

    #[test]
    fn history_rejects_duplicate_timestamp() {
        let error = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            2,
            "2026-09-27T14:00:00.000Z",
            vec![
                row("1790467200000", "0.125", "1"),
                row("1790467200000", "0.126", "1"),
            ],
        )
        .expect_err("duplicate");

        assert!(matches!(error, MarketHistoryError::DuplicateTimestamp(_)));
    }

    #[test]
    fn history_preserves_unconfirmed_state_explicitly() {
        let history = MarketHistorySnapshot::from_public(
            &reference(),
            "DOGE-USDT-SWAP",
            "1H",
            1,
            "2026-09-27T14:00:00.000Z",
            vec![row("1790467200000", "0.125", "0")],
        )
        .expect("history");

        assert!(!history.all_confirmed);
        assert!(!history.candles[0].confirmed);
    }
}
