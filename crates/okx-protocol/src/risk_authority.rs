//! Risk authority messages over the existing authenticated direct WebSocket.
//! A risk receipt is not exchange permission until a separate POST fence.
use serde::{Deserialize, Serialize};
pub const RISK_COMMAND_SCHEMA_V1: &str = "okx.risk-command/v1";
pub const RISK_RESULT_SCHEMA_V1: &str = "okx.risk-result/v1";

fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn token(value: &str) -> bool {
    (8..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn positive(value: &str) -> bool {
    let (whole, frac) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || whole.len() > 15
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || whole.len() > 1 && whole.starts_with('0')
        || value.ends_with('.')
        || frac.len() > 9
        || !frac.bytes().all(|b| b.is_ascii_digit())
    {
        return false;
    }
    let Ok(integer) = whole.parse::<u128>() else {
        return false;
    };
    integer > 0 || frac.bytes().any(|b| b != b'0')
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskAccountOccupancy {
    pub account_key: String,
    pub occupied_usdt: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRiskCensus {
    pub account_keys: Vec<String>,
    pub occupied: Vec<RiskAccountOccupancy>,
    pub generated_at_ms: u64,
    pub source_digest: String,
    pub complete: bool,
    pub native_account_evidence_complete: bool,
    pub native_order_evidence_complete: bool,
    pub native_ledger_evidence_complete: bool,
}
impl NativeRiskCensus {
    pub fn valid(&self) -> bool {
        let mut observed = self
            .occupied
            .iter()
            .map(|v| v.account_key.as_str())
            .collect::<Vec<_>>();
        observed.sort_unstable();
        self.complete
            && self.native_account_evidence_complete
            && self.native_order_evidence_complete
            && self.native_ledger_evidence_complete
            && self.generated_at_ms > 0
            && hash(&self.source_digest)
            && !self.account_keys.is_empty()
            && self.account_keys.len() <= 32
            && self.account_keys.windows(2).all(|v| v[0] < v[1])
            && self.account_keys.iter().all(|s| {
                let parts = s.split(':').collect::<Vec<_>>();
                parts.len() == 3
                    && (2..=32).contains(&parts[0].len())
                    && parts[0]
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_' || b == b'-')
                    && parts[1] == "real"
                    && hash(parts[2])
            })
            && observed
                == self
                    .account_keys
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
            && self.occupied.iter().all(|v| {
                let x = &v.occupied_usdt;
                x == "0" || positive(x)
            })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskResolution {
    Unknown,
    Open,
    Released,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "action",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum RiskCommandAction {
    Reserve {
        venue: String,
        account_uid: String,
        run_id: String,
        intent_id: String,
        strategy_id: String,
        plan_sha256: String,
        requested_loss_usdt: String,
        policy_revision: u64,
        census: NativeRiskCensus,
    },
    BeginPost {
        reservation_key: String,
        plan_sha256: String,
        policy_revision: u64,
        expected_risk_book_sequence: u64,
        source_proof_sha256: String,
    },
    Resolve {
        reservation_key: String,
        plan_sha256: String,
        expected_risk_book_sequence: u64,
        next_state: RiskResolution,
        source_proof_sha256: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskCommand {
    pub schema: String,
    pub command_id: String,
    pub action: RiskCommandAction,
}
impl RiskCommand {
    pub fn valid(&self) -> bool {
        if self.schema != RISK_COMMAND_SCHEMA_V1 || !token(&self.command_id) {
            return false;
        }
        match &self.action {
            RiskCommandAction::Reserve {
                venue,
                account_uid,
                run_id,
                intent_id,
                strategy_id,
                plan_sha256,
                requested_loss_usdt,
                policy_revision,
                census,
            } => {
                (2..=32).contains(&venue.len())
                    && venue
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_' || b == b'-')
                    && hash(account_uid)
                    && token(run_id)
                    && token(intent_id)
                    && (2..=64).contains(&strategy_id.len())
                    && strategy_id.as_bytes()[0].is_ascii_alphabetic()
                    && strategy_id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
                    && hash(plan_sha256)
                    && positive(requested_loss_usdt)
                    && *policy_revision > 0
                    && census.valid()
            }
            RiskCommandAction::BeginPost {
                reservation_key,
                plan_sha256,
                policy_revision,
                expected_risk_book_sequence,
                source_proof_sha256,
            } => {
                !reservation_key.is_empty()
                    && reservation_key.len() <= 512
                    && hash(plan_sha256)
                    && *policy_revision > 0
                    && *expected_risk_book_sequence > 0
                    && hash(source_proof_sha256)
            }
            RiskCommandAction::Resolve {
                reservation_key,
                plan_sha256,
                expected_risk_book_sequence,
                next_state: _,
                source_proof_sha256,
            } => {
                !reservation_key.is_empty()
                    && reservation_key.len() <= 512
                    && hash(plan_sha256)
                    && *expected_risk_book_sequence > 0
                    && hash(source_proof_sha256)
            }
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskDecisionStatus {
    Denied,
    Held,
    PostAuthorized,
    Updated,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskResult {
    pub schema: String,
    pub command_id: String,
    pub status: RiskDecisionStatus,
    pub reason: Option<String>,
    pub reservation_key: Option<String>,
    pub risk_book_sequence: Option<u64>,
    pub policy_revision: Option<u64>,
}
impl RiskResult {
    pub fn valid(&self) -> bool {
        self.schema == RISK_RESULT_SCHEMA_V1
            && token(&self.command_id)
            && (self.status == RiskDecisionStatus::Denied
                || self.reservation_key.is_some()
                    && self.risk_book_sequence.is_some_and(|s| s > 0)
                    && self.policy_revision.is_some_and(|r| r > 0))
            && self
                .reason
                .as_ref()
                .is_none_or(|s| (1..=128).contains(&s.len()))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_contract_denies_partial_native_risk_proof() {
        let uid = "a".repeat(64);
        let key = format!("okx:real:{uid}");
        let command = RiskCommand {
            schema: RISK_COMMAND_SCHEMA_V1.to_owned(),
            command_id: "risk_request_01".to_owned(),
            action: RiskCommandAction::Reserve {
                venue: "okx".to_owned(),
                account_uid: uid,
                run_id: "run_12345678".to_owned(),
                intent_id: "intent_12345678".to_owned(),
                strategy_id: "momentum_A".to_owned(),
                plan_sha256: "c".repeat(64),
                requested_loss_usdt: "1".to_owned(),
                policy_revision: 1,
                census: NativeRiskCensus {
                    account_keys: vec![key.clone()],
                    occupied: vec![RiskAccountOccupancy {
                        account_key: key,
                        occupied_usdt: "0".to_owned(),
                    }],
                    generated_at_ms: 1,
                    source_digest: "b".repeat(64),
                    complete: false,
                    native_account_evidence_complete: true,
                    native_order_evidence_complete: true,
                    native_ledger_evidence_complete: true,
                },
            },
        };
        assert!(!command.valid());
        let mut ready = command;
        if let RiskCommandAction::Reserve { census, .. } = &mut ready.action {
            census.complete = true;
        }
        assert!(ready.valid());
        let json = serde_json::to_string(&ready).unwrap();
        assert_eq!(serde_json::from_str::<RiskCommand>(&json).unwrap(), ready);
        if let RiskCommandAction::Reserve {
            requested_loss_usdt,
            ..
        } = &mut ready.action
        {
            *requested_loss_usdt = "1e3".to_owned();
        }
        assert!(!ready.valid());
    }
    #[test]
    fn denied_decision_cannot_count_as_post_authorization() {
        let mut result = RiskResult {
            schema: RISK_RESULT_SCHEMA_V1.to_owned(),
            command_id: "risk_request_01".to_owned(),
            status: RiskDecisionStatus::Denied,
            reason: Some("UNVERIFIED".to_owned()),
            reservation_key: None,
            risk_book_sequence: None,
            policy_revision: None,
        };
        assert!(result.valid());
        result.status = RiskDecisionStatus::PostAuthorized;
        assert!(!result.valid());
    }
}
