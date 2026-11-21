//! Engine-owned persisted graph activation and confirmed idle cursor.
//!
//! These are financial-SQL domain facts, not computation or Research API DTOs.
//! A cursor may only advance in the same transaction that proves no financial
//! action is required. Opening/closing requires a separate atomic intent claim.
use crate::position_target::TargetPosition;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineGraphBinding {
    pub account_key: String,
    pub instance_id: String,
    pub definition_version: String,
    pub policy_revision: i64,
    pub session_id: String,
    pub graph_sha256: String,
    pub instrument_id: String,
    pub timeframe_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineGraphCursor {
    pub sequence: u64,
    pub closed_ms: u64,
    pub intent: TargetPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineGraphProgress {
    pub binding: EngineGraphBinding,
    pub cursor: EngineGraphCursor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphIdleAck {
    Advanced,
    Replayed,
}

impl EngineGraphBinding {
    pub fn valid(&self) -> bool {
        !self.account_key.is_empty()
            && self.account_key.len() <= 128
            && !self.instance_id.is_empty()
            && self.instance_id.len() <= 128
            && !self.definition_version.is_empty()
            && self.definition_version.len() <= 128
            && self.policy_revision > 0
            && (1..=128).contains(&self.session_id.len())
            && self.session_id.bytes().all(|b| {
                b.is_ascii_alphanumeric() || b"_-:.".contains(&b)
            })
            && self.graph_sha256.len() == 64
            && self
                .graph_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && (3..=40).contains(&self.instrument_id.len())
            && self.instrument_id.bytes().all(|b| {
                b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-'
            })
            && matches!(
                self.timeframe_ms,
                60_000 | 180_000 | 300_000 | 900_000 | 1_800_000 | 3_600_000
                    | 14_400_000 | 86_400_000
            )
    }
}

impl EngineGraphCursor {
    pub fn valid_for(&self, binding: &EngineGraphBinding) -> bool {
        (1..=100_000).contains(&self.sequence)
            && self.closed_ms > 0
            && self.closed_ms <= i64::MAX as u64
            && self.closed_ms % binding.timeframe_ms == 0
    }
}
