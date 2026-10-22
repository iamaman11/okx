//! Action class is confirmed against native state before admission.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionClass {
    Read,
    IncreaseRisk,
    ReduceRisk,
    Maintenance,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandIdentity {
    pub account: String,
    pub idempotency_key: String,
    pub expires_in_ms: u64,
}

impl CommandIdentity {
    pub fn valid(&self) -> bool {
        !self.account.is_empty()
            && self.account.len() <= 128
            && (8..=128).contains(&self.idempotency_key.len())
            && self
                .idempotency_key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            && (1..=30_000).contains(&self.expires_in_ms)
    }
}
