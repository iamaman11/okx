use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const FEE_SCHEDULE_SCHEMA_V1: &str = "okx.fee-schedule/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeScheduleSnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub reference_generation: String,
    pub source_received_at: String,
    pub exchange_timestamp_ms: String,
    pub level: String,
    pub maker_rate: String,
    pub taker_rate: String,
    pub exact_for_instrument: bool,
    pub fee_generation: String,
}

#[derive(Debug, Error)]
pub enum FeeScheduleError {
    #[error("fee schedule field '{0}' is empty")]
    Missing(&'static str),
    #[error("failed to serialize fee schedule: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl FeeScheduleSnapshot {
    pub fn new(
        instrument_id: impl Into<String>,
        reference_generation: impl Into<String>,
        source_received_at: impl Into<String>,
        exchange_timestamp_ms: impl Into<String>,
        level: impl Into<String>,
        maker_rate: impl Into<String>,
        taker_rate: impl Into<String>,
        exact_for_instrument: bool,
    ) -> Result<Self, FeeScheduleError> {
        let mut value = Self {
            schema: FEE_SCHEDULE_SCHEMA_V1.to_owned(),
            instrument_id: instrument_id.into(),
            reference_generation: reference_generation.into(),
            source_received_at: source_received_at.into(),
            exchange_timestamp_ms: exchange_timestamp_ms.into(),
            level: level.into(),
            maker_rate: maker_rate.into(),
            taker_rate: taker_rate.into(),
            exact_for_instrument,
            fee_generation: String::new(),
        };
        for (name, field) in [
            ("instrument_id", value.instrument_id.as_str()),
            ("reference_generation", value.reference_generation.as_str()),
            ("source_received_at", value.source_received_at.as_str()),
            ("exchange_timestamp_ms", value.exchange_timestamp_ms.as_str()),
            ("maker_rate", value.maker_rate.as_str()),
            ("taker_rate", value.taker_rate.as_str()),
        ] {
            if field.trim().is_empty() {
                return Err(FeeScheduleError::Missing(name));
            }
        }
        let encoded = serde_json::to_vec(&(
            &value.instrument_id,
            &value.reference_generation,
            &value.exchange_timestamp_ms,
            &value.level,
            &value.maker_rate,
            &value.taker_rate,
            value.exact_for_instrument,
        ))?;
        value.fee_generation = format!("sha256:{:x}", Sha256::digest(encoded));
        Ok(value)
    }
}
