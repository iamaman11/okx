use serde::{Deserialize, Serialize};

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
