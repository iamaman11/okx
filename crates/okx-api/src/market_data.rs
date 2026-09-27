use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublicTicker {
    #[serde(rename = "instType", default)]
    pub instrument_type: String,
    #[serde(rename = "instId", default)]
    pub instrument_id: String,
    #[serde(default)]
    pub last: String,
    #[serde(rename = "askPx", default)]
    pub ask_price: String,
    #[serde(rename = "bidPx", default)]
    pub bid_price: String,
    #[serde(default)]
    pub ts: String,
}
