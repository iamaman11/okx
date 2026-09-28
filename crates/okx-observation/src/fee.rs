use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const FEE_SCHEDULE_SCHEMA_V1: &str = "okx.fee-schedule/v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeeScheduleInput {
    pub instrument_id: String,
    pub reference_generation: String,
    pub source_received_at: String,
    pub exchange_timestamp_ms: String,
    pub level: String,
    pub maker_rate: String,
    pub taker_rate: String,
    pub exact_for_instrument: bool,
}

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
    pub fn from_input(input: FeeScheduleInput) -> Result<Self, FeeScheduleError> {
        let mut value = Self {
            schema: FEE_SCHEDULE_SCHEMA_V1.to_owned(),
            instrument_id: input.instrument_id,
            reference_generation: input.reference_generation,
            source_received_at: input.source_received_at,
            exchange_timestamp_ms: input.exchange_timestamp_ms,
            level: input.level,
            maker_rate: input.maker_rate,
            taker_rate: input.taker_rate,
            exact_for_instrument: input.exact_for_instrument,
            fee_generation: String::new(),
        };
        for (name, field) in [
            ("instrument_id", value.instrument_id.as_str()),
            ("reference_generation", value.reference_generation.as_str()),
            ("source_received_at", value.source_received_at.as_str()),
            (
                "exchange_timestamp_ms",
                value.exchange_timestamp_ms.as_str(),
            ),
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
            &value.level,
            &value.maker_rate,
            &value.taker_rate,
            value.exact_for_instrument,
        ))?;
        value.fee_generation = format!("sha256:{:x}", Sha256::digest(encoded));
        Ok(value)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn input(exchange_timestamp_ms: &str, maker_rate: &str, taker_rate: &str) -> FeeScheduleInput {
        FeeScheduleInput {
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            reference_generation: "sha256:reference".to_owned(),
            source_received_at: "2026-09-29T00:00:00Z".to_owned(),
            exchange_timestamp_ms: exchange_timestamp_ms.to_owned(),
            level: "Lv1".to_owned(),
            maker_rate: maker_rate.to_owned(),
            taker_rate: taker_rate.to_owned(),
            exact_for_instrument: true,
        }
    }

    #[test]
    fn exchange_timestamp_does_not_change_fee_generation() {
        let first = FeeScheduleSnapshot::from_input(input(
            "1790630000000",
            "-0.0002",
            "-0.0005",
        ))
        .expect("first");
        let second = FeeScheduleSnapshot::from_input(input(
            "1790639999999",
            "-0.0002",
            "-0.0005",
        ))
        .expect("second");

        assert_eq!(first.fee_generation, second.fee_generation);
        assert_ne!(first.exchange_timestamp_ms, second.exchange_timestamp_ms);
    }

    #[test]
    fn actual_fee_change_changes_fee_generation() {
        let first = FeeScheduleSnapshot::from_input(input(
            "1790630000000",
            "-0.0002",
            "-0.0005",
        ))
        .expect("first");
        let second = FeeScheduleSnapshot::from_input(input(
            "1790630000001",
            "-0.0001",
            "-0.0005",
        ))
        .expect("second");

        assert_ne!(first.fee_generation, second.fee_generation);
    }
}
