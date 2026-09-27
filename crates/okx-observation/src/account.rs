use std::collections::BTreeSet;

use okx_api::{AccountConfig, BalanceSnapshot, PendingOrder, Position};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const ACCOUNT_SNAPSHOT_SCHEMA_V1: &str = "okx.account-snapshot/v1";
pub const ACCOUNT_REST_SOURCE_V1: &str = "okx_private_rest_bootstrap";
pub const M4_REST_BOOTSTRAP_REASON: &str = "M4_PRIVATE_REST_BOOTSTRAP_ONLY";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBalanceDetail {
    pub currency: String,
    pub equity: String,
    pub cash_balance: Option<String>,
    pub available_equity: Option<String>,
    pub available_balance: Option<String>,
    pub frozen_balance: Option<String>,
    pub equity_usd: Option<String>,
    pub unrealized_pnl: Option<String>,
    pub update_time_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBalanceState {
    pub total_equity_usd: String,
    pub adjusted_equity_usd: Option<String>,
    pub isolated_equity_usd: Option<String>,
    pub initial_margin_requirement_usd: Option<String>,
    pub maintenance_margin_requirement_usd: Option<String>,
    pub margin_ratio: Option<String>,
    pub notional_usd: Option<String>,
    pub update_time_ms: Option<String>,
    pub details: Vec<AccountBalanceDetail>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountPositionState {
    pub instrument_type: String,
    pub instrument_id: String,
    pub position: String,
    pub position_side: String,
    pub margin_mode: String,
    pub average_price: Option<String>,
    pub mark_price: Option<String>,
    pub liquidation_price: Option<String>,
    pub unrealized_pnl: Option<String>,
    pub unrealized_pnl_ratio: Option<String>,
    pub leverage: Option<String>,
    pub margin: Option<String>,
    pub initial_margin_requirement: Option<String>,
    pub maintenance_margin_requirement: Option<String>,
    pub margin_ratio: Option<String>,
    pub notional_usd: Option<String>,
    pub margin_currency: Option<String>,
    pub creation_time_ms: Option<String>,
    pub update_time_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingOrderState {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub instrument_type: String,
    pub instrument_id: String,
    pub side: String,
    pub position_side: Option<String>,
    pub trade_mode: String,
    pub order_type: String,
    pub price: Option<String>,
    pub size: String,
    pub accumulated_fill_size: String,
    pub average_fill_price: Option<String>,
    pub state: String,
    pub reduce_only: Option<bool>,
    pub creation_time_ms: String,
    pub update_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountSnapshot {
    pub schema: String,
    pub source: String,
    pub source_received_at: String,
    pub account_generation: String,
    pub quality_reason: String,
    pub private_ws_connected: bool,
    pub account_level: String,
    pub position_mode: String,
    pub account_type: String,
    pub account_uid_fingerprint: String,
    pub api_key_permissions: Vec<String>,
    pub balance: AccountBalanceState,
    pub positions: Vec<AccountPositionState>,
    pub pending_orders: Vec<PendingOrderState>,
}

#[derive(Debug, Error)]
pub enum AccountError {
    #[error("account source receive timestamp is empty")]
    EmptySourceTimestamp,

    #[error("account config is missing required field '{0}'")]
    MissingConfigField(&'static str),

    #[error("account balance is missing required field '{0}'")]
    MissingBalanceField(&'static str),

    #[error("balance detail is missing required field '{0}'")]
    MissingBalanceDetailField(&'static str),

    #[error("position is missing required field '{0}'")]
    MissingPositionField(&'static str),

    #[error("pending order is missing required field '{0}'")]
    MissingOrderField(&'static str),

    #[error("duplicate balance currency '{0}'")]
    DuplicateBalanceCurrency(String),

    #[error("duplicate position identity '{0}'")]
    DuplicatePosition(String),

    #[error("duplicate pending order id '{0}'")]
    DuplicateOrder(String),

    #[error("pending order reduceOnly value '{0}' is invalid")]
    InvalidReduceOnly(String),

    #[error("failed to serialize normalized account snapshot: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl AccountSnapshot {
    pub fn from_rest_bootstrap(
        source_received_at: impl Into<String>,
        config: AccountConfig,
        balance: BalanceSnapshot,
        positions: Vec<Position>,
        pending_orders: Vec<PendingOrder>,
        api_key_permissions: Vec<String>,
    ) -> Result<Self, AccountError> {
        let source_received_at = source_received_at.into();
        if source_received_at.trim().is_empty() {
            return Err(AccountError::EmptySourceTimestamp);
        }

        let account_level = required_config("acctLv", config.account_level)?;
        let position_mode = required_config("posMode", config.position_mode)?;
        let account_type = required_config("type", config.account_type)?;
        let uid = required_config("uid", config.uid)?;
        let account_uid_fingerprint = format!("{:x}", Sha256::digest(uid.as_bytes()));

        let mut balance_currencies = BTreeSet::new();
        let mut details = Vec::with_capacity(balance.details.len());
        for detail in balance.details {
            let currency = required_balance_detail("ccy", detail.ccy)?;
            if !balance_currencies.insert(currency.clone()) {
                return Err(AccountError::DuplicateBalanceCurrency(currency));
            }
            details.push(AccountBalanceDetail {
                currency,
                equity: required_balance_detail("eq", detail.equity)?,
                cash_balance: optional(detail.cash_balance),
                available_equity: optional(detail.available_equity),
                available_balance: optional(detail.available_balance),
                frozen_balance: optional(detail.frozen_balance),
                equity_usd: optional(detail.equity_usd),
                unrealized_pnl: optional(detail.unrealized_pnl),
                update_time_ms: optional(detail.update_time),
            });
        }
        details.sort_by(|a, b| a.currency.cmp(&b.currency));

        let balance = AccountBalanceState {
            total_equity_usd: required_balance("totalEq", balance.total_equity)?,
            adjusted_equity_usd: optional(balance.adjusted_equity),
            isolated_equity_usd: optional(balance.isolated_equity),
            initial_margin_requirement_usd: optional(balance.initial_margin_requirement),
            maintenance_margin_requirement_usd: optional(balance.maintenance_margin_requirement),
            margin_ratio: optional(balance.margin_ratio),
            notional_usd: optional(balance.notional_usd),
            update_time_ms: optional(balance.update_time),
            details,
        };

        let mut position_ids = BTreeSet::new();
        let mut normalized_positions = Vec::with_capacity(positions.len());
        for position in positions {
            let instrument_id = required_position("instId", position.instrument_id)?;
            let position_side = required_position("posSide", position.position_side)?;
            let margin_mode = required_position("mgnMode", position.margin_mode)?;
            let identity = format!("{instrument_id}|{position_side}|{margin_mode}");
            if !position_ids.insert(identity.clone()) {
                return Err(AccountError::DuplicatePosition(identity));
            }
            normalized_positions.push(AccountPositionState {
                instrument_type: required_position("instType", position.instrument_type)?,
                instrument_id,
                position: required_position("pos", position.pos)?,
                position_side,
                margin_mode,
                average_price: optional(position.average_price),
                mark_price: optional(position.mark_price),
                liquidation_price: optional(position.liquidation_price),
                unrealized_pnl: optional(position.unrealized_pnl),
                unrealized_pnl_ratio: optional(position.unrealized_pnl_ratio),
                leverage: optional(position.lever),
                margin: optional(position.margin),
                initial_margin_requirement: optional(position.initial_margin_requirement),
                maintenance_margin_requirement: optional(position.maintenance_margin_requirement),
                margin_ratio: optional(position.margin_ratio),
                notional_usd: optional(position.notional_usd),
                margin_currency: optional(position.ccy),
                creation_time_ms: optional(position.creation_time),
                update_time_ms: optional(position.update_time),
            });
        }
        normalized_positions.sort_by(|a, b| {
            (&a.instrument_id, &a.position_side, &a.margin_mode)
                .cmp(&(&b.instrument_id, &b.position_side, &b.margin_mode))
        });

        let mut order_ids = BTreeSet::new();
        let mut normalized_orders = Vec::with_capacity(pending_orders.len());
        for order in pending_orders {
            let order_id = required_order("ordId", order.order_id)?;
            if !order_ids.insert(order_id.clone()) {
                return Err(AccountError::DuplicateOrder(order_id));
            }
            normalized_orders.push(PendingOrderState {
                order_id,
                client_order_id: optional(order.client_order_id),
                instrument_type: required_order("instType", order.instrument_type)?,
                instrument_id: required_order("instId", order.instrument_id)?,
                side: required_order("side", order.side)?,
                position_side: optional(order.position_side),
                trade_mode: required_order("tdMode", order.trade_mode)?,
                order_type: required_order("ordType", order.order_type)?,
                price: optional(order.px),
                size: required_order("sz", order.sz)?,
                accumulated_fill_size: required_order(
                    "accFillSz",
                    order.accumulated_fill_size,
                )?,
                average_fill_price: optional(order.average_fill_price),
                state: required_order("state", order.state)?,
                reduce_only: parse_optional_bool(order.reduce_only)?,
                creation_time_ms: required_order("cTime", order.creation_time)?,
                update_time_ms: required_order("uTime", order.update_time)?,
            });
        }
        normalized_orders.sort_by(|a, b| a.order_id.cmp(&b.order_id));

        let mut permissions = api_key_permissions;
        permissions.sort();
        permissions.dedup();

        let mut snapshot = Self {
            schema: ACCOUNT_SNAPSHOT_SCHEMA_V1.to_owned(),
            source: ACCOUNT_REST_SOURCE_V1.to_owned(),
            source_received_at,
            account_generation: String::new(),
            quality_reason: M4_REST_BOOTSTRAP_REASON.to_owned(),
            private_ws_connected: false,
            account_level,
            position_mode,
            account_type,
            account_uid_fingerprint,
            api_key_permissions: permissions,
            balance,
            positions: normalized_positions,
            pending_orders: normalized_orders,
        };
        snapshot.account_generation = generation_for(&snapshot)?;
        Ok(snapshot)
    }
}

fn required_config(field: &'static str, value: String) -> Result<String, AccountError> {
    if value.trim().is_empty() {
        Err(AccountError::MissingConfigField(field))
    } else {
        Ok(value)
    }
}

fn required_balance(field: &'static str, value: String) -> Result<String, AccountError> {
    if value.trim().is_empty() {
        Err(AccountError::MissingBalanceField(field))
    } else {
        Ok(value)
    }
}

fn required_balance_detail(field: &'static str, value: String) -> Result<String, AccountError> {
    if value.trim().is_empty() {
        Err(AccountError::MissingBalanceDetailField(field))
    } else {
        Ok(value)
    }
}

fn required_position(field: &'static str, value: String) -> Result<String, AccountError> {
    if value.trim().is_empty() {
        Err(AccountError::MissingPositionField(field))
    } else {
        Ok(value)
    }
}

fn required_order(field: &'static str, value: String) -> Result<String, AccountError> {
    if value.trim().is_empty() {
        Err(AccountError::MissingOrderField(field))
    } else {
        Ok(value)
    }
}

fn optional(value: String) -> Option<String> {
    (!value.trim().is_empty()).then_some(value)
}

fn parse_optional_bool(value: String) -> Result<Option<bool>, AccountError> {
    match value.trim() {
        "" => Ok(None),
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        other => Err(AccountError::InvalidReduceOnly(other.to_owned())),
    }
}

fn generation_for(snapshot: &AccountSnapshot) -> Result<String, AccountError> {
    #[derive(Serialize)]
    struct GenerationInput<'a> {
        schema: &'static str,
        source: &'static str,
        quality_reason: &'static str,
        private_ws_connected: bool,
        account_level: &'a str,
        position_mode: &'a str,
        account_type: &'a str,
        account_uid_fingerprint: &'a str,
        api_key_permissions: &'a [String],
        balance: &'a AccountBalanceState,
        positions: &'a [AccountPositionState],
        pending_orders: &'a [PendingOrderState],
    }

    let encoded = serde_json::to_vec(&GenerationInput {
        schema: ACCOUNT_SNAPSHOT_SCHEMA_V1,
        source: ACCOUNT_REST_SOURCE_V1,
        quality_reason: M4_REST_BOOTSTRAP_REASON,
        private_ws_connected: false,
        account_level: &snapshot.account_level,
        position_mode: &snapshot.position_mode,
        account_type: &snapshot.account_type,
        account_uid_fingerprint: &snapshot.account_uid_fingerprint,
        api_key_permissions: &snapshot.api_key_permissions,
        balance: &snapshot.balance,
        positions: &snapshot.positions,
        pending_orders: &snapshot.pending_orders,
    })?;
    Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use okx_api::{BalanceDetail, BalanceSnapshot, PendingOrder};

    fn config() -> AccountConfig {
        AccountConfig {
            account_level: "2".to_owned(),
            position_mode: "long_short_mode".to_owned(),
            uid: "raw-user-id-never-output".to_owned(),
            main_uid: "raw-user-id-never-output".to_owned(),
            account_type: "0".to_owned(),
            account_stp_mode: "cancel_maker".to_owned(),
            auto_loan: false,
            greeks_type: "PA".to_owned(),
            fee_type: "0".to_owned(),
            label: "observer".to_owned(),
            ip: String::new(),
            perm: "read_only".to_owned(),
        }
    }

    fn balance() -> BalanceSnapshot {
        BalanceSnapshot {
            total_equity: "1000.125".to_owned(),
            adjusted_equity: "999.5".to_owned(),
            isolated_equity: String::new(),
            initial_margin_requirement: "100".to_owned(),
            maintenance_margin_requirement: "50".to_owned(),
            margin_ratio: "20".to_owned(),
            notional_usd: "500".to_owned(),
            update_time: "1790520000000".to_owned(),
            details: vec![BalanceDetail {
                ccy: "USDT".to_owned(),
                equity: "1000.125".to_owned(),
                cash_balance: "900".to_owned(),
                available_equity: "800".to_owned(),
                available_balance: "800".to_owned(),
                frozen_balance: "100".to_owned(),
                equity_usd: "1000.125".to_owned(),
                unrealized_pnl: "0.125".to_owned(),
                update_time: "1790520000000".to_owned(),
            }],
        }
    }

    #[test]
    fn rest_bootstrap_is_deterministic_and_never_emits_raw_uid() {
        let first = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            Vec::new(),
            vec!["read_only".to_owned()],
        )
        .expect("snapshot");
        let second = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:01.000Z",
            config(),
            balance(),
            Vec::new(),
            Vec::new(),
            vec!["read_only".to_owned()],
        )
        .expect("snapshot");

        assert_eq!(first.account_generation, second.account_generation);
        assert_ne!(first.source_received_at, second.source_received_at);
        let json = serde_json::to_string(&first).expect("json");
        assert!(!json.contains("raw-user-id-never-output"));
        assert_eq!(first.quality_reason, M4_REST_BOOTSTRAP_REASON);
        assert!(!first.private_ws_connected);
    }

    #[test]
    fn duplicate_pending_order_id_fails_closed() {
        let order = PendingOrder {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: "123".to_owned(),
            client_order_id: String::new(),
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            trade_mode: "cross".to_owned(),
            order_type: "limit".to_owned(),
            px: "0.1".to_owned(),
            sz: "100".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            average_fill_price: String::new(),
            state: "live".to_owned(),
            reduce_only: "false".to_owned(),
            creation_time: "1790520000000".to_owned(),
            update_time: "1790520000000".to_owned(),
        };
        let error = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            vec![order.clone(), order],
            vec!["read_only".to_owned()],
        )
        .expect_err("duplicate");

        assert!(matches!(error, AccountError::DuplicateOrder(_)));
    }
}
