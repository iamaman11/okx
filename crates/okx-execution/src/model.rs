use okx_analysis::{HardRiskPolicy, TcaReferencePriceBasis, TradingMandate};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const EXECUTION_PLAN_SCHEMA_V1: &str = "okx.execution-plan/v1";
pub const EXECUTION_LINEAGE_SCHEMA_V1: &str = "okx.execution-lineage/v1";
const CLIENT_ORDER_ID_PREFIX: &str = "okx";
const CLIENT_ORDER_ID_HASH_CHARS: usize = 29;
const AMEND_REQUEST_ID_PREFIX: &str = "amx";
const AMEND_REQUEST_ID_HASH_CHARS: usize = 29;
const REVERSE_OPEN_INTENT_PREFIX: &str = "rvx";
const REVERSE_OPEN_INTENT_HASH_CHARS: usize = 29;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeMode {
    Cross,
    Isolated,
}

impl TradeMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cross => "cross",
            Self::Isolated => "isolated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
}

impl PositionSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionAction {
    Open,
    Add,
    Hedge,
    Reduce,
    Close,
}

impl ExecutionAction {
    pub const fn is_risk_increasing(self) -> bool {
        matches!(self, Self::Open | Self::Add | Self::Hedge)
    }

    pub const fn is_risk_reducing(self) -> bool {
        matches!(self, Self::Reduce | Self::Close)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Limit,
    PostOnly,
    Fok,
    Ioc,
}

impl OrderType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::PostOnly => "post_only",
            Self::Fok => "fok",
            Self::Ioc => "ioc",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionDecisionReference {
    pub decision_time_ms: u64,
    pub price: String,
    pub price_basis: TcaReferencePriceBasis,
    pub price_policy_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLineageBinding {
    pub schema: String,
    pub origin_evidence_id: String,
    pub origin_schema: String,
    pub origin_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority_evidence_id: Option<String>,
    pub decision_reference: ExecutionDecisionReference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionIntent {
    pub intent_id: String,
    pub expected_reference_generation: String,
    pub expected_account_generation: String,
    pub instrument_id: String,
    pub trade_mode: TradeMode,
    pub position_side: PositionSide,
    pub action: ExecutionAction,
    pub order_type: OrderType,
    pub size: String,
    pub price: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenRiskEvidence {
    pub fee_generation: String,
    pub requested_max_settle_notional: String,
    pub requested_max_loss_settle: String,
    pub requested_target_rr: String,
    pub stop_price: String,
    pub target_price: String,
    pub entry_settle_notional: String,
    pub stop_loss_settle: String,
    pub actual_target_rr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRiskBinding {
    pub mandate: TradingMandate,
    pub policy: HardRiskPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlan {
    pub schema: String,
    pub intent_id: String,
    pub client_order_id: String,
    pub reference_generation: String,
    pub account_generation: String,
    pub account_uid_fingerprint: String,
    pub instrument_id: String,
    pub trade_mode: TradeMode,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub action: ExecutionAction,
    pub order_type: OrderType,
    pub size: String,
    pub price: String,
    pub open_risk: Option<OpenRiskEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub risk_binding: Option<ExecutionRiskBinding>,
}

pub(crate) fn valid_intent_id(value: &str) -> bool {
    (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub(crate) fn valid_mutation_id(value: &str) -> bool {
    (8..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub fn derive_client_order_id(intent_id: &str) -> String {
    let digest = Sha256::digest(format!("okx-execution-v1:{intent_id}").as_bytes());
    let hex = format!("{digest:x}");
    format!(
        "{CLIENT_ORDER_ID_PREFIX}{}",
        &hex[..CLIENT_ORDER_ID_HASH_CHARS]
    )
}

pub fn derive_reverse_open_intent_id(root_intent_id: &str) -> String {
    let digest =
        Sha256::digest(format!("okx-execution-reverse-open-v1:{root_intent_id}").as_bytes());
    let hex = format!("{digest:x}");
    format!(
        "{REVERSE_OPEN_INTENT_PREFIX}{}",
        &hex[..REVERSE_OPEN_INTENT_HASH_CHARS]
    )
}

pub fn derive_amend_request_id(intent_id: &str, mutation_id: &str) -> String {
    let digest =
        Sha256::digest(format!("okx-execution-amend-v1:{intent_id}:{mutation_id}").as_bytes());
    let hex = format!("{digest:x}");
    format!(
        "{AMEND_REQUEST_ID_PREFIX}{}",
        &hex[..AMEND_REQUEST_ID_HASH_CHARS]
    )
}

pub(crate) const fn order_side(action: ExecutionAction, position_side: PositionSide) -> OrderSide {
    match (action.is_risk_increasing(), position_side) {
        (true, PositionSide::Long) | (false, PositionSide::Short) => OrderSide::Buy,
        (true, PositionSide::Short) | (false, PositionSide::Long) => OrderSide::Sell,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_order_id_is_stable_alphanumeric_and_bounded() {
        let first = derive_client_order_id("intent_0123456789abcdef");
        let again = derive_client_order_id("intent_0123456789abcdef");
        let other = derive_client_order_id("intent_fedcba9876543210");

        assert_eq!(first, again);
        assert_ne!(first, other);
        assert_eq!(first.len(), 32);
        assert!(first.bytes().all(|byte| byte.is_ascii_alphanumeric()));
    }

    #[test]
    fn reverse_open_intent_id_is_stable_alphanumeric_and_bounded() {
        let first = derive_reverse_open_intent_id("intent_reverse_01234567");
        let again = derive_reverse_open_intent_id("intent_reverse_01234567");
        let other = derive_reverse_open_intent_id("intent_reverse_76543210");

        assert_eq!(first, again);
        assert_ne!(first, other);
        assert_eq!(first.len(), 32);
        assert!(valid_intent_id(&first));
    }

    #[test]
    fn amend_request_id_is_stable_alphanumeric_and_bounded() {
        let first = derive_amend_request_id("intent_0123456789abcdef", "mutation_01234567");
        let again = derive_amend_request_id("intent_0123456789abcdef", "mutation_01234567");
        let other = derive_amend_request_id("intent_0123456789abcdef", "mutation_76543210");

        assert_eq!(first, again);
        assert_ne!(first, other);
        assert_eq!(first.len(), 32);
        assert!(first.bytes().all(|byte| byte.is_ascii_alphanumeric()));
    }

    #[test]
    fn long_short_side_mapping_is_explicit() {
        assert_eq!(
            order_side(ExecutionAction::Open, PositionSide::Long),
            OrderSide::Buy
        );
        assert_eq!(
            order_side(ExecutionAction::Open, PositionSide::Short),
            OrderSide::Sell
        );
        assert_eq!(
            order_side(ExecutionAction::Close, PositionSide::Long),
            OrderSide::Sell
        );
        assert_eq!(
            order_side(ExecutionAction::Close, PositionSide::Short),
            OrderSide::Buy
        );
        assert_eq!(
            order_side(ExecutionAction::Add, PositionSide::Long),
            OrderSide::Buy
        );
        assert_eq!(
            order_side(ExecutionAction::Hedge, PositionSide::Short),
            OrderSide::Sell
        );
        assert_eq!(
            order_side(ExecutionAction::Reduce, PositionSide::Long),
            OrderSide::Sell
        );
        assert!(ExecutionAction::Add.is_risk_increasing());
        assert!(ExecutionAction::Hedge.is_risk_increasing());
        assert!(ExecutionAction::Reduce.is_risk_reducing());
    }
}
