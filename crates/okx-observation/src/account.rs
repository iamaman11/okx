use std::collections::BTreeSet;

use okx_api::{AccountConfig, BalanceSnapshot, PendingOrder, Position};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const ACCOUNT_SNAPSHOT_SCHEMA_V1: &str = "okx.account-snapshot/v1";
pub const ACCOUNT_SNAPSHOT_SCHEMA_V2: &str = "okx.account-snapshot/v2";
pub const ACCOUNT_REST_SOURCE_V1: &str = "okx_private_rest_bootstrap";
pub const ACCOUNT_CONVERGED_SOURCE_V2: &str = "okx_private_rest_plus_ws";
pub const M4_REST_BOOTSTRAP_REASON: &str = "M4_PRIVATE_REST_BOOTSTRAP_ONLY";
pub const M4_REST_WS_CONVERGED_REASON: &str = "M4_PRIVATE_REST_WS_CONVERGED";

#[derive(Debug, Clone)]
pub enum AccountWsEvent {
    Account(Vec<BalanceSnapshot>),
    Positions(Vec<Position>),
    Orders(Vec<PendingOrder>),
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_ws_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_ws_connection_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_ws_last_inbound_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_ws_events_applied: Option<u64>,
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

    #[error("private account timestamp '{field}' is invalid: '{value}'")]
    InvalidTimestamp { field: &'static str, value: String },

    #[error("private websocket connection fingerprint is missing")]
    MissingPrivateConnectionFingerprint,

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

        let balance = normalize_balance(balance)?;

        let normalized_positions = normalize_positions(positions)?;

        let normalized_orders = normalize_orders(pending_orders)?;

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
            private_ws_generation: None,
            private_ws_connection_fingerprint: None,
            private_ws_last_inbound_ms: None,
            private_ws_events_applied: None,
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

    pub fn converge_private_ws(
        mut self,
        private_ws_generation: u64,
        private_ws_connection_fingerprint: impl Into<String>,
        private_ws_last_inbound_ms: u64,
        events: &[AccountWsEvent],
    ) -> Result<Self, AccountError> {
        for event in events {
            match event {
                AccountWsEvent::Account(updates) => {
                    for update in updates {
                        self.apply_balance_update(update.clone())?;
                    }
                }
                AccountWsEvent::Positions(updates) => {
                    for update in updates {
                        self.apply_position_update(update.clone())?;
                    }
                }
                AccountWsEvent::Orders(updates) => {
                    for update in updates {
                        self.apply_order_update(update.clone())?;
                    }
                }
            }
        }

        self.schema = ACCOUNT_SNAPSHOT_SCHEMA_V2.to_owned();
        self.source = ACCOUNT_CONVERGED_SOURCE_V2.to_owned();
        self.quality_reason = M4_REST_WS_CONVERGED_REASON.to_owned();
        let private_ws_connection_fingerprint = private_ws_connection_fingerprint.into();
        if private_ws_connection_fingerprint.trim().is_empty() {
            return Err(AccountError::MissingPrivateConnectionFingerprint);
        }

        self.private_ws_connected = true;
        self.private_ws_generation = Some(private_ws_generation);
        self.private_ws_connection_fingerprint = Some(private_ws_connection_fingerprint);
        self.private_ws_last_inbound_ms = Some(private_ws_last_inbound_ms);
        self.private_ws_events_applied = Some(events.len() as u64);
        self.positions.sort_by(position_sort);
        self.pending_orders
            .sort_by(|a, b| a.order_id.cmp(&b.order_id));
        self.balance
            .details
            .sort_by(|a, b| a.currency.cmp(&b.currency));
        self.account_generation = generation_for(&self)?;
        Ok(self)
    }

    fn apply_balance_update(&mut self, update: BalanceSnapshot) -> Result<(), AccountError> {
        let incoming = normalize_balance(update)?;
        if timestamp_is_newer_or_equal(
            incoming.update_time_ms.as_deref(),
            self.balance.update_time_ms.as_deref(),
            "balance.uTime",
        )? {
            let existing_details = std::mem::take(&mut self.balance.details);
            self.balance = AccountBalanceState {
                details: existing_details,
                ..incoming.clone()
            };
        }

        for detail in incoming.details {
            match self
                .balance
                .details
                .iter()
                .position(|current| current.currency == detail.currency)
            {
                Some(index)
                    if timestamp_is_newer_or_equal(
                        detail.update_time_ms.as_deref(),
                        self.balance.details[index].update_time_ms.as_deref(),
                        "balance.details.uTime",
                    )? =>
                {
                    self.balance.details[index] = detail;
                }
                Some(_) => {}
                None => self.balance.details.push(detail),
            }
        }
        Ok(())
    }

    fn apply_position_update(&mut self, update: Position) -> Result<(), AccountError> {
        let incoming = normalize_position(update)?;
        required_event_timestamp(incoming.update_time_ms.as_deref(), "position.uTime")?;
        let identity = position_identity(&incoming);

        let existing = self
            .positions
            .iter()
            .position(|current| position_identity(current) == identity);

        if let Some(index) = existing {
            if !timestamp_is_newer_or_equal(
                incoming.update_time_ms.as_deref(),
                self.positions[index].update_time_ms.as_deref(),
                "position.uTime",
            )? {
                return Ok(());
            }
            if is_zero_decimal_text(&incoming.position) {
                self.positions.remove(index);
            } else {
                self.positions[index] = incoming;
            }
        } else if !is_zero_decimal_text(&incoming.position) {
            self.positions.push(incoming);
        }
        Ok(())
    }

    fn apply_order_update(&mut self, update: PendingOrder) -> Result<(), AccountError> {
        let incoming = normalize_order(update)?;
        required_event_timestamp(Some(incoming.update_time_ms.as_str()), "order.uTime")?;

        let existing = self
            .pending_orders
            .iter()
            .position(|current| current.order_id == incoming.order_id);

        if let Some(index) = existing {
            if !timestamp_is_newer_or_equal(
                Some(incoming.update_time_ms.as_str()),
                Some(self.pending_orders[index].update_time_ms.as_str()),
                "order.uTime",
            )? {
                return Ok(());
            }
            if is_terminal_order_state(&incoming.state) {
                self.pending_orders.remove(index);
            } else {
                self.pending_orders[index] = incoming;
            }
        } else if !is_terminal_order_state(&incoming.state) {
            self.pending_orders.push(incoming);
        }
        Ok(())
    }
}

fn normalize_balance(balance: BalanceSnapshot) -> Result<AccountBalanceState, AccountError> {
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

    Ok(AccountBalanceState {
        total_equity_usd: required_balance("totalEq", balance.total_equity)?,
        adjusted_equity_usd: optional(balance.adjusted_equity),
        isolated_equity_usd: optional(balance.isolated_equity),
        initial_margin_requirement_usd: optional(balance.initial_margin_requirement),
        maintenance_margin_requirement_usd: optional(balance.maintenance_margin_requirement),
        margin_ratio: optional(balance.margin_ratio),
        notional_usd: optional(balance.notional_usd),
        update_time_ms: optional(balance.update_time),
        details,
    })
}

fn normalize_positions(
    positions: Vec<Position>,
) -> Result<Vec<AccountPositionState>, AccountError> {
    let mut position_ids = BTreeSet::new();
    let mut normalized = Vec::with_capacity(positions.len());
    for position in positions {
        let position = normalize_position(position)?;
        let identity = position_identity(&position);
        if !position_ids.insert(identity.clone()) {
            return Err(AccountError::DuplicatePosition(identity));
        }
        normalized.push(position);
    }
    normalized.sort_by(position_sort);
    Ok(normalized)
}

fn normalize_position(position: Position) -> Result<AccountPositionState, AccountError> {
    Ok(AccountPositionState {
        instrument_type: required_position("instType", position.instrument_type)?,
        instrument_id: required_position("instId", position.instrument_id)?,
        position: required_position("pos", position.pos)?,
        position_side: required_position("posSide", position.position_side)?,
        margin_mode: required_position("mgnMode", position.margin_mode)?,
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
    })
}

fn normalize_orders(orders: Vec<PendingOrder>) -> Result<Vec<PendingOrderState>, AccountError> {
    let mut order_ids = BTreeSet::new();
    let mut normalized = Vec::with_capacity(orders.len());
    for order in orders {
        let order = normalize_order(order)?;
        if !order_ids.insert(order.order_id.clone()) {
            return Err(AccountError::DuplicateOrder(order.order_id));
        }
        normalized.push(order);
    }
    normalized.sort_by(|a, b| a.order_id.cmp(&b.order_id));
    Ok(normalized)
}

fn normalize_order(order: PendingOrder) -> Result<PendingOrderState, AccountError> {
    Ok(PendingOrderState {
        order_id: required_order("ordId", order.order_id)?,
        client_order_id: optional(order.client_order_id),
        instrument_type: required_order("instType", order.instrument_type)?,
        instrument_id: required_order("instId", order.instrument_id)?,
        side: required_order("side", order.side)?,
        position_side: optional(order.position_side),
        trade_mode: required_order("tdMode", order.trade_mode)?,
        order_type: required_order("ordType", order.order_type)?,
        price: optional(order.px),
        size: required_order("sz", order.sz)?,
        accumulated_fill_size: required_order("accFillSz", order.accumulated_fill_size)?,
        average_fill_price: optional(order.average_fill_price),
        state: required_order("state", order.state)?,
        reduce_only: parse_optional_bool(order.reduce_only)?,
        creation_time_ms: required_order("cTime", order.creation_time)?,
        update_time_ms: required_order("uTime", order.update_time)?,
    })
}

fn position_identity(position: &AccountPositionState) -> String {
    format!(
        "{}|{}|{}",
        position.instrument_id, position.position_side, position.margin_mode
    )
}

fn position_sort(a: &AccountPositionState, b: &AccountPositionState) -> std::cmp::Ordering {
    (&a.instrument_id, &a.position_side, &a.margin_mode).cmp(&(
        &b.instrument_id,
        &b.position_side,
        &b.margin_mode,
    ))
}

fn required_event_timestamp(value: Option<&str>, field: &'static str) -> Result<u64, AccountError> {
    let value = value.unwrap_or_default();
    value
        .parse::<u64>()
        .map_err(|_| AccountError::InvalidTimestamp {
            field,
            value: value.to_owned(),
        })
}

fn timestamp_is_newer_or_equal(
    incoming: Option<&str>,
    current: Option<&str>,
    field: &'static str,
) -> Result<bool, AccountError> {
    let incoming = required_event_timestamp(incoming, field)?;
    match current {
        Some(current) if !current.is_empty() => {
            let current = required_event_timestamp(Some(current), field)?;
            Ok(incoming >= current)
        }
        _ => Ok(true),
    }
}

fn is_zero_decimal_text(value: &str) -> bool {
    let value = value.trim().trim_start_matches(['+', '-']);
    let mut saw_digit = false;
    for byte in value.bytes() {
        match byte {
            b'0' => saw_digit = true,
            b'.' => {}
            b'1'..=b'9' => return false,
            _ => return false,
        }
    }
    saw_digit
}

fn is_terminal_order_state(state: &str) -> bool {
    matches!(state, "filled" | "canceled" | "mmp_canceled")
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
        schema: &'a str,
        source: &'a str,
        quality_reason: &'a str,
        private_ws_connected: bool,
        private_ws_generation: Option<u64>,
        private_ws_connection_fingerprint: Option<&'a str>,
        private_ws_last_inbound_ms: Option<u64>,
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
        schema: &snapshot.schema,
        source: &snapshot.source,
        quality_reason: &snapshot.quality_reason,
        private_ws_connected: snapshot.private_ws_connected,
        private_ws_generation: snapshot.private_ws_generation,
        private_ws_connection_fingerprint: snapshot.private_ws_connection_fingerprint.as_deref(),
        private_ws_last_inbound_ms: snapshot.private_ws_last_inbound_ms,
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

    fn pending_order(order_id: &str, state: &str, update_time: &str) -> PendingOrder {
        PendingOrder {
            instrument_type: "SWAP".to_owned(),
            instrument_id: "DOGE-USDT-SWAP".to_owned(),
            order_id: order_id.to_owned(),
            client_order_id: String::new(),
            side: "buy".to_owned(),
            position_side: "long".to_owned(),
            trade_mode: "cross".to_owned(),
            order_type: "limit".to_owned(),
            px: "0.1".to_owned(),
            sz: "100".to_owned(),
            accumulated_fill_size: "0".to_owned(),
            average_fill_price: String::new(),
            state: state.to_owned(),
            reduce_only: "false".to_owned(),
            creation_time: "1790519999000".to_owned(),
            update_time: update_time.to_owned(),
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
    fn converged_snapshot_ignores_older_balance_event() {
        let rest = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            Vec::new(),
            vec!["read_only".to_owned()],
        )
        .expect("rest");

        let mut stale = balance();
        stale.total_equity = "1".to_owned();
        stale.update_time = "1790519999999".to_owned();
        stale.details[0].equity = "1".to_owned();
        stale.details[0].update_time = "1790519999999".to_owned();

        let converged = rest
            .converge_private_ws(
                7,
                "conn-fingerprint-a",
                1790520000100,
                &[AccountWsEvent::Account(vec![stale])],
            )
            .expect("converged");

        assert_eq!(converged.schema, ACCOUNT_SNAPSHOT_SCHEMA_V2);
        assert_eq!(converged.balance.total_equity_usd, "1000.125");
        assert_eq!(converged.private_ws_generation, Some(7));
        assert!(converged.private_ws_connected);
        assert_eq!(converged.quality_reason, M4_REST_WS_CONVERGED_REASON);
    }

    #[test]
    fn terminal_order_delta_removes_rest_pending_order() {
        let live = pending_order("123", "live", "1790520000000");
        let rest = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            vec![live],
            vec!["read_only".to_owned()],
        )
        .expect("rest");

        let canceled = pending_order("123", "canceled", "1790520000100");
        let converged = rest
            .converge_private_ws(
                8,
                "conn-fingerprint-b",
                1790520000200,
                &[AccountWsEvent::Orders(vec![canceled])],
            )
            .expect("converged");

        assert!(converged.pending_orders.is_empty());
    }

    #[test]
    fn private_connection_fingerprint_changes_content_generation() {
        let first = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            Vec::new(),
            vec!["read_only".to_owned()],
        )
        .expect("rest")
        .converge_private_ws(1, "conn-fingerprint-a", 1790520000100, &[])
        .expect("first");

        let second = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            Vec::new(),
            vec!["read_only".to_owned()],
        )
        .expect("rest")
        .converge_private_ws(1, "conn-fingerprint-b", 1790520000100, &[])
        .expect("second");

        assert_ne!(first.account_generation, second.account_generation);
        assert_ne!(
            first.private_ws_connection_fingerprint,
            second.private_ws_connection_fingerprint
        );
    }

    #[test]
    fn private_generation_changes_content_generation() {
        let first = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            Vec::new(),
            vec!["read_only".to_owned()],
        )
        .expect("rest")
        .converge_private_ws(1, "conn-fingerprint-a", 1790520000100, &[])
        .expect("first");

        let second = AccountSnapshot::from_rest_bootstrap(
            "2026-09-27T15:00:00.000Z",
            config(),
            balance(),
            Vec::new(),
            Vec::new(),
            vec!["read_only".to_owned()],
        )
        .expect("rest")
        .converge_private_ws(2, "conn-fingerprint-b", 1790520000100, &[])
        .expect("second");

        assert_ne!(first.account_generation, second.account_generation);
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
