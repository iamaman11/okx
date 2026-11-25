//! Caller-supplied observations for analytical risk calculations.
//! These inputs are not account authority and do not provide account access.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountBalanceDetail {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub liabilities: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountAuthorityEvidence {
    pub scope: &'static str,
    pub account_type: String,
    pub is_subaccount: bool,
    pub account_uid_fingerprint: String,
    pub main_account_uid_fingerprint: Option<String>,
    pub api_key_permissions: Vec<String>,
    pub multi_account_inventory_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountHistoryCoverage {
    pub resource: String,
    pub documented_window: &'static str,
    pub instrument_count: usize,
    pub sample_instruments: Vec<String>,
    pub rows: usize,
    pub pages: usize,
    pub complete_within_bound: bool,
    pub newest_event_time_ms: Option<String>,
    pub oldest_event_time_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingBalanceEvidence {
    pub currency: String,
    pub balance: String,
    pub available_balance: String,
    pub frozen_balance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CurrencyAggregate {
    pub currency: String,
    pub amount: String,
    pub events: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AccountLedgerSummary {
    pub schema: &'static str,
    pub source_received_at: String,
    pub current_account_as_of_ms: Option<String>,
    pub account_generation: String,
    pub authority: AccountAuthorityEvidence,
    pub total_equity_usd: String,
    pub trading_equity_detail_usd_sum: String,
    pub trading_equity_residual_usd: String,
    pub funding_balances: Vec<FundingBalanceEvidence>,
    pub open_positions: usize,
    pub pending_orders: usize,
    pub current_unrealized_pnl: Vec<CurrencyAggregate>,
    pub history_coverage: Vec<AccountHistoryCoverage>,
    pub realized_pnl_basis: &'static str,
    pub realized_pnl: Vec<CurrencyAggregate>,
    pub daily_realized_pnl_utc_basis: &'static str,
    pub daily_realized_pnl_utc_day_start_ms: Option<String>,
    pub daily_realized_pnl_utc_day_end_ms: Option<String>,
    pub daily_realized_pnl_utc: Vec<CurrencyAggregate>,
    pub trade_fee_basis: &'static str,
    pub trade_fees: Vec<CurrencyAggregate>,
    pub funding_basis: &'static str,
    pub funding: Vec<CurrencyAggregate>,
    pub position_pnl_identity_rows_checked: usize,
    pub fill_order_links_checked: usize,
    pub fill_order_links_unresolved_due_to_truncation: usize,
}
