//! Normalized, transport-independent facts consumed by deterministic analysis.
//! Venue parsers and freshness policy remain in `okx-observation`.
//! Canonical normalized inputs shared by venue adapters and analysis.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FundingRequirement {
    Required,
    NotApplicable,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum InstrumentType {
    Swap,
    Futures,
}

impl std::fmt::Display for InstrumentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Swap => f.write_str("SWAP"),
            Self::Futures => f.write_str("FUTURES"),
        }
    }
}

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

impl AccountBalanceState {
    /// Compare the full balance evidence while ignoring provider receipt timestamps.
    /// Currency identities, row order, and every financial field remain significant.
    pub fn same_values_ignoring_update_times(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.update_time_ms = None;
        right.update_time_ms = None;
        for detail in &mut left.details {
            detail.update_time_ms = None;
        }
        for detail in &mut right.details {
            detail.update_time_ms = None;
        }
        left == right
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeFillIdentity {
    pub instrument_type: String,
    pub instrument_id: String,
    pub order_id: Option<String>,
    pub client_order_id: String,
    pub trade_id: String,
    /// Venue-native exact link to the corresponding account bill.
    pub bill_id: Option<String>,
    /// Venue-reported realized PnL of this fill, not the account's net PnL.
    pub fill_pnl: Option<String>,
    pub side: String,
    pub position_side: String,
    pub fill_price: String,
    pub fill_size: String,
    pub fee: Option<String>,
    pub fee_currency: Option<String>,
    pub execution_type: Option<String>,
    pub fill_time_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCandle {
    pub open_time_ms: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub volume_currency: String,
    pub volume_quote: Option<String>,
    pub confirmed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketHistorySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub bar: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub history_generation: String,
    pub all_confirmed: bool,
    pub oldest_open_time_ms: String,
    pub newest_open_time_ms: String,
    pub candles: Vec<HistoryCandle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketTradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTrade {
    pub trade_id: String,
    pub price: String,
    pub size_contracts: String,
    pub side: MarketTradeSide,
    pub source: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketTradesSnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub trades_generation: String,
    pub oldest_exchange_timestamp_ms: Option<String>,
    pub newest_exchange_timestamp_ms: Option<String>,
    pub trades: Vec<MarketTrade>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingHistoryEvent {
    pub funding_time_ms: String,
    pub funding_rate: String,
    pub realized_rate: Option<String>,
    pub formula_type: Option<String>,
    pub method: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingHistorySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub funding_generation: String,
    pub oldest_funding_time_ms: Option<String>,
    pub newest_funding_time_ms: Option<String>,
    pub events: Vec<FundingHistoryEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenInterestHistoryPoint {
    pub timestamp_ms: String,
    pub open_interest_contracts: String,
    pub open_interest_currency: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenInterestHistorySnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub period: String,
    pub requested_limit: u16,
    pub reference_generation: String,
    pub source: String,
    pub source_received_at: String,
    pub open_interest_generation: String,
    pub oldest_timestamp_ms: Option<String>,
    pub newest_timestamp_ms: Option<String>,
    pub points: Vec<OpenInterestHistoryPoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TickerState {
    pub last: String,
    pub last_size: String,
    pub best_ask: String,
    pub best_ask_size: String,
    pub best_bid: String,
    pub best_bid_size: String,
    pub open_24h: Option<String>,
    pub high_24h: Option<String>,
    pub low_24h: Option<String>,
    pub volume_currency_24h: Option<String>,
    pub volume_24h: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarkPriceState {
    pub price: String,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexPriceState {
    pub index_id: String,
    pub price: String,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingState {
    pub rate: String,
    pub funding_time_ms: String,
    pub next_funding_time_ms: String,
    pub premium: Option<String>,
    pub min_rate: Option<String>,
    pub max_rate: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OpenInterestState {
    pub contracts: String,
    pub currency: Option<String>,
    pub usd: Option<String>,
    pub exchange_timestamp_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketSnapshot {
    pub schema: String,
    pub instrument_id: String,
    pub instrument_type: InstrumentType,
    pub underlying: String,
    pub reference_generation: String,
    pub market_generation: String,
    pub source_received_at: String,
    pub ticker: TickerState,
    pub mark_price: MarkPriceState,
    pub index_price: IndexPriceState,
    pub funding: Option<FundingState>,
    pub open_interest: OpenInterestState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BookLevel {
    pub price: String,
    pub size: String,
    pub order_count: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderBookStatus {
    WaitingSnapshot,
    Contiguous,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OrderBookSnapshot {
    pub generation: u64,
    pub status: OrderBookStatus,
    pub seq_id: Option<i64>,
    pub exchange_timestamp_ms: Option<String>,
    pub asks: Vec<BookLevel>,
    pub bids: Vec<BookLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UpcomingRuleChange {
    pub param: String,
    pub new_value: String,
    pub effective_time_ms: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstrumentSpec {
    pub instrument_id: String,
    pub instrument_type: InstrumentType,
    pub instrument_family: Option<String>,
    pub underlying: Option<String>,
    pub state: String,
    pub rule_type: Option<String>,
    pub funding_requirement: FundingRequirement,
    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
    pub settle_currency: Option<String>,
    pub tick_size: String,
    pub lot_size: String,
    pub min_size: String,
    pub max_limit_size: Option<String>,
    pub max_market_size: Option<String>,
    pub max_limit_amount: Option<String>,
    pub max_market_amount: Option<String>,
    pub contract_type: Option<String>,
    pub contract_value: Option<String>,
    pub contract_value_currency: Option<String>,
    pub fee_group_id: Option<String>,
    pub max_leverage: Option<String>,
    pub list_time_ms: Option<String>,
    pub expiry_time_ms: Option<String>,
    pub initial_price_limit_pct: Option<String>,
    pub floating_price_limit_pct: Option<String>,
    pub maximum_price_limit_pct: Option<String>,
    pub upcoming_rule_changes: Vec<UpcomingRuleChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InstrumentRulesSnapshot {
    pub reference_generation: String,
    pub source_received_at: String,
    pub instrument: InstrumentSpec,
}

pub use crate::fee_model::{
    FEE_SCHEDULE_SCHEMA_V1, FeeScheduleError, FeeScheduleInput, FeeScheduleSnapshot,
};
