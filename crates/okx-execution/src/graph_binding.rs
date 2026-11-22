//! Engine-owned persisted graph activation and confirmed idle cursor.
//!
//! These are financial-SQL domain facts, not computation or Research API DTOs.
//! A cursor may only advance in the same transaction that proves no financial
//! action is required. Opening/closing requires a separate atomic intent claim.
use crate::position_target::TargetPosition;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

/// Bounded discovery from original finance SQL. `scanned` includes stale
/// historical revisions so the caller advances its page even when no active
/// graph happens to be present in the current slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineGraphBindingPage {
    pub scanned: usize,
    pub active: Vec<EngineGraphBinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphIdleAck {
    Advanced,
    Replayed,
}

/// The original finance writer has durably linked one graph candle to a
/// PREPARED opening intent. This is not permission to reserve risk or POST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphOpenClaim {
    Prepared { intent_id: String },
    Replayed { intent_id: String },
}

/// Shared exact graph-derived identity for TradeService and its finance SQL
/// owner. Neither layer independently invents a second idempotency rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphOpenIdentity {
    pub decision_key: String,
    pub decision_id: String,
    pub input_digest: String,
}

/// Stable reduction identity for one graph publication and ONE original
/// opening reference. It is only an immutable candidate: a live Close must
/// still be claimed with the original finance writer in a later stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphCloseIdentity {
    pub decision_key: String,
    pub decision_id: String,
    pub input_digest: String,
}

/// A real original PREPARED intent fetched in the SAME finance transaction as
/// the graph cursor. No additional read RPC or RiskBook/POST grant.
#[derive(Debug, Clone)]
pub struct GraphOpenAdmission {
    pub claim: GraphOpenClaim,
    pub intent: crate::StoredIntent,
}

impl EngineGraphBinding {
    pub fn open_identity(&self, publication: &EngineGraphCursor) -> Option<GraphOpenIdentity> {
        if !self.valid() || !publication.valid_for(self) {
            return None;
        }
        let body = serde_json::to_string(&(self, publication)).ok()?;
        let input_digest = format!("{:x}", Sha256::digest(body.as_bytes()));
        let decision_key = format!("graph_{}", &input_digest[..32]);
        let decision_id = format!(
            "strategy:{}:{}:{}",
            self.instance_id, self.definition_version, decision_key
        );
        Some(GraphOpenIdentity {
            decision_key,
            decision_id,
            input_digest,
        })
    }

    pub fn close_identity(
        &self,
        publication: &EngineGraphCursor,
        original_reference: &str,
    ) -> Option<GraphCloseIdentity> {
        if !self.valid()
            || !publication.valid_for(self)
            || publication.intent == TargetPosition::Hold
            || !(1..=128).contains(&original_reference.len())
            || !original_reference
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-:.".contains(&b))
        {
            return None;
        }
        // Include every original scope dimension and the exact opening
        // reference. Different positions from one candle MUST NOT share an
        // idempotency key; retries of the same candidate MUST share it.
        let body =
            serde_json::to_vec(&("okx.graph-close/v1", self, publication, original_reference))
                .ok()?;
        let digest = format!("{:x}", Sha256::digest(body));
        let decision_key = format!("graph_close_{}", &digest[..32]);
        let decision_id = format!(
            "strategy:{}:{}:{}",
            self.instance_id, self.definition_version, decision_key
        );
        Some(GraphCloseIdentity {
            decision_key,
            decision_id,
            input_digest: digest,
        })
    }

    pub fn valid(&self) -> bool {
        !self.account_key.is_empty()
            && self.account_key.len() <= 128
            && !self.instance_id.is_empty()
            && self.instance_id.len() <= 128
            && !self.definition_version.is_empty()
            && self.definition_version.len() <= 128
            && self.policy_revision > 0
            && (1..=128).contains(&self.session_id.len())
            && self
                .session_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-:.".contains(&b))
            && self.graph_sha256.len() == 64
            && self
                .graph_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && (3..=40).contains(&self.instrument_id.len())
            && self
                .instrument_id
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
            && matches!(
                self.timeframe_ms,
                60_000
                    | 180_000
                    | 300_000
                    | 900_000
                    | 1_800_000
                    | 3_600_000
                    | 14_400_000
                    | 86_400_000
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_identity_is_bound_to_original_reference_account_revision_and_bar() {
        let binding = EngineGraphBinding {
            account_key: "account-a".into(),
            instance_id: "strategy-a".into(),
            definition_version: "version-1".into(),
            policy_revision: 7,
            session_id: "engine:strategy-a:btc".into(),
            graph_sha256: "a".repeat(64),
            instrument_id: "BTC-USDT-SWAP".into(),
            timeframe_ms: 60_000,
        };
        let candle = EngineGraphCursor {
            sequence: 3,
            closed_ms: 180_000,
            intent: TargetPosition::Flat,
        };
        let stable = binding.close_identity(&candle, "entry-a").unwrap();
        assert_eq!(stable, binding.close_identity(&candle, "entry-a").unwrap());
        assert_ne!(
            stable.decision_key,
            binding
                .close_identity(&candle, "entry-b")
                .unwrap()
                .decision_key
        );
        let mut foreign = binding.clone();
        foreign.account_key = "account-b".into();
        assert_ne!(
            stable.decision_key,
            foreign
                .close_identity(&candle, "entry-a")
                .unwrap()
                .decision_key
        );
        foreign = binding.clone();
        foreign.policy_revision += 1;
        assert_ne!(
            stable.decision_key,
            foreign
                .close_identity(&candle, "entry-a")
                .unwrap()
                .decision_key
        );
        let mut next = candle.clone();
        next.sequence += 1;
        next.closed_ms += 60_000;
        assert_ne!(
            stable.decision_key,
            binding
                .close_identity(&next, "entry-a")
                .unwrap()
                .decision_key
        );
        next.intent = TargetPosition::Hold;
        assert!(binding.close_identity(&next, "entry-a").is_none());
        assert!(binding.close_identity(&candle, "../foreign").is_none());
        assert!(binding.close_identity(&candle, "").is_none());
    }
}
