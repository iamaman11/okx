use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const EXECUTION_PLAN_SCHEMA_V1: &str = "okx.execution-plan/v1";
const CLIENT_ORDER_ID_PREFIX: &str = "okx";
const CLIENT_ORDER_ID_HASH_CHARS: usize = 29;

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
    Close,
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
}

pub fn derive_client_order_id(intent_id: &str) -> String {
    let digest = Sha256::digest(format!("okx-execution-v1:{intent_id}").as_bytes());
    let hex = format!("{digest:x}");
    format!(
        "{CLIENT_ORDER_ID_PREFIX}{}",
        &hex[..CLIENT_ORDER_ID_HASH_CHARS]
    )
}

pub(crate) const fn order_side(action: ExecutionAction, position_side: PositionSide) -> OrderSide {
    match (action, position_side) {
        (ExecutionAction::Open, PositionSide::Long)
        | (ExecutionAction::Close, PositionSide::Short) => OrderSide::Buy,
        (ExecutionAction::Open, PositionSide::Short)
        | (ExecutionAction::Close, PositionSide::Long) => OrderSide::Sell,
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
    }
}
