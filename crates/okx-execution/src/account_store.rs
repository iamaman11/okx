//! Sole durable owner of operations created after T0. No import or implicit initialization.
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
#[cfg(test)]
#[path = "account_store_tests.rs"]
mod tests;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("ACCOUNT_STORAGE_UNAVAILABLE: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("ACCOUNT_STORAGE_UNAVAILABLE: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Invalid(&'static str),
}
impl StoreError {
    /// Attach an operation phase only to a SQLite progress-handler interruption.
    /// Other storage errors retain their original error owner and diagnostics.
    pub fn with_interruption_stage(self, stage: &'static str) -> Self {
        match self {
            Self::Sql(rusqlite::Error::SqliteFailure(code, _))
                if code.code == rusqlite::ErrorCode::OperationInterrupted =>
            {
                Self::Invalid(stage)
            }
            other => other,
        }
    }
}
pub type StoreResult<T> = Result<T, StoreError>;
/// Exact economic-event partition by strategy owner (None = unattributed), currency and component.
pub type FinanceAttribution = std::collections::BTreeMap<(Option<String>, String, String), Decimal>;
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountMeta {
    pub initialize_operation_id: String,
    pub release_id: String,
    pub account_key: String,
    pub installation_id: String,
    pub initialized_at: i64,
    pub opening_source: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Decision {
    pub decision_id: String,
    pub instance_id: Option<String>,
    pub decision_at: i64,
    pub input_digest: String,
    pub policy_revision: i64,
    pub code_version: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NewIntent {
    pub plan: IntentPlan,
    pub intent_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub client_order_id: String,
    pub deadline: i64,
    pub plan_hash: String,
    pub policy_revision: i64,
    pub risk_increase: bool,
    pub instrument_id: String,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IntentState {
    Prepared,
    Reserved,
    Posting,
    Unknown,
    Acknowledged,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}
impl IntentState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "PREPARED",
            Self::Reserved => "RESERVED",
            Self::Posting => "POSTING",
            Self::Unknown => "UNKNOWN",
            Self::Acknowledged => "ACKNOWLEDGED",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
        }
    }
    pub fn terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Cancelled | Self::Rejected)
    }
    fn parse(s: &str) -> StoreResult<Self> {
        Ok(match s {
            "PREPARED" => Self::Prepared,
            "RESERVED" => Self::Reserved,
            "POSTING" => Self::Posting,
            "UNKNOWN" => Self::Unknown,
            "ACKNOWLEDGED" => Self::Acknowledged,
            "PARTIALLY_FILLED" => Self::PartiallyFilled,
            "FILLED" => Self::Filled,
            "CANCELLED" => Self::Cancelled,
            "REJECTED" => Self::Rejected,
            _ => return Err(StoreError::Invalid("CORRUPT_INTENT_STATE")),
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredIntent {
    pub plan: IntentPlan,
    pub intent_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub client_order_id: String,
    pub state: IntentState,
    pub revision: i64,
    pub deadline: i64,
    pub plan_hash: String,
    pub policy_revision: i64,
    pub risk_increase: bool,
    pub instrument_id: String,
    pub exchange_order_id: Option<String>,
    pub incident: Option<String>,
}
/// Exact durable SQL witness that no local POST claim was ever committed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnsentSqlReceipt {
    pub reservation_key: String,
    pub account_key: String,
    pub intent_id: String,
    pub plan_hash: String,
    pub request_hash: String,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CentralReceipt {
    pub reservation_key: String,
    pub account_key: String,
    pub installation_id: String,
    pub instance_id: Option<String>,
    pub intent_id: String,
    pub plan_hash: String,
    pub policy_revision: i64,
    pub receipt_hash: String,
    pub requested_loss_usd: Decimal,
    pub conversion_digest: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IntentPlan {
    pub strategy_definition_id: String,
    pub strategy_definition_version: String,
    pub strategy_parameters_digest: String,
    pub contract_value: Decimal,
    pub inverse: bool,
    pub settlement_currency: String,
    pub position_side: String,
    pub private_source_generation: i64,
    pub operation: String,
    pub side: String,
    pub quantity: Decimal,
    pub limit_price: Decimal,
    pub stop_price: Option<Decimal>,
    pub strategy_instance: String,
    pub planned_loss_usd: Decimal,
    pub source_generation: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtectionReceipt {
    pub intent_id: String,
    pub algo_id: String,
    pub side: String,
    pub quantity: Decimal,
    pub trigger_price: Decimal,
    pub trigger_basis: String,
    pub verified_at: i64,
    pub source_digest: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationReceipt {
    pub operation_id: String,
    pub kind: String,
    pub request_hash: String,
    pub result_ref: String,
    pub committed_at: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FillEffect {
    Open { side: String },
    Reduce,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsdConversionEvidence {
    pub observed_at: i64,
    pub source_digest: String,
    pub usd_per_currency: std::collections::BTreeMap<String, Decimal>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInstance {
    pub instance_id: String,
    pub definition_id: String,
    pub definition_version: String,
    pub parameters: String,
    pub enabled: bool,
    pub policy_revision: i64,
    pub budget: Decimal,
    pub last_decision_key: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyDefinitionVersion {
    pub definition_id: String,
    pub version: String,
    pub parameters: String,
    pub parameters_digest: String,
    pub created_at: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinanceEntry {
    pub entry_id: String,
    pub economic_event_key: String,
    pub component: String,
    pub allocation_leg: String,
    pub amount: Decimal,
    pub currency: String,
    pub instance_id: Option<String>,
    pub source_ref: String,
    pub time: i64,
    pub reverses_entry_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub side: String,
    pub fill_id: String,
    pub instrument_id: String,
    pub trade_id: String,
    pub exchange_order_id: String,
    pub intent_id: Option<String>,
    pub time: i64,
    pub qty: Decimal,
    pub price: Decimal,
    pub fee: Decimal,
    pub currency: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bill {
    pub bill_id: String,
    pub kind: String,
    pub time: i64,
    pub amount: Decimal,
    pub currency: String,
    pub economic_event_key: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCursor {
    pub source: String,
    pub stream_partition: String,
    pub cursor: String,
    pub watermark: i64,
    pub generation: i64,
    pub coverage_gaps: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestPage {
    pub funding_events: Vec<FundingEvent>,
    pub attributed_funding_ids: Vec<String>,
    pub fills: Vec<Fill>,
    pub bills: Vec<Bill>,
    pub finance: Vec<FinanceEntry>,
    pub cash_flows: Vec<CashFlow>,
    pub allocations: Vec<Allocation>,
    pub lots: Vec<StoredLot>,
    pub nav: Vec<NavPoint>,
    pub projected_fill_ids: Vec<String>,
    pub cursor: Option<SourceCursor>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingEvent {
    pub event_id: String,
    pub instrument_id: String,
    pub time: i64,
    pub amount: Decimal,
    pub currency: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueMark {
    pub instrument_id: String,
    pub price: Decimal,
    pub source_ref: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CashFlow {
    pub event_id: String,
    pub kind: String,
    pub currency: String,
    pub amount: Decimal,
    pub time: i64,
    pub source_ref: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allocation {
    pub event_id: String,
    pub instance_id: String,
    pub delta: Decimal,
    pub currency: String,
    pub time: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredLot {
    pub lot_id: String,
    pub instance_id: Option<String>,
    pub intent_id: Option<String>,
    pub fill_id: Option<String>,
    pub instrument_id: String,
    pub side: String,
    pub remaining_qty: Decimal,
    pub cost: Decimal,
    pub opened_at: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NavPoint {
    pub scope_id: String,
    pub time: i64,
    pub source_version: String,
    pub equity: Decimal,
    pub pnl: Decimal,
    pub flows: Decimal,
    pub currency: String,
    pub coverage: String,
    pub mark_source: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpeningSnapshot {
    pub opening_equity_usd: Decimal,
    pub source_ref: String,
    pub observed_at: i64,
    pub positions: usize,
    pub orders: usize,
    pub conditional_orders: usize,
    pub liabilities: Decimal,
    pub balances: Vec<(String, Decimal)>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinancialRisk {
    pub daily_loss_usd: Decimal,
    pub drawdown_ratio: Decimal,
    pub nav_complete: bool,
    pub source_as_of: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverySnapshot {
    pub required: bool,
    pub backup_at: Option<i64>,
    pub coverage_complete: bool,
    pub active_receipts: Vec<CentralReceipt>,
    pub unresolved_intents: bool,
    pub cumulative_loss_usd: Decimal,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NavWindow {
    pub scope_id: String,
    pub currency: String,
    pub opening_equity: Option<Decimal>,
    pub current_equity: Option<Decimal>,
    pub flows: Option<Decimal>,
    pub pnl: Option<Decimal>,
    pub opening_as_of: Option<i64>,
    pub current_as_of: Option<i64>,
    pub coverage: String,
}

/// A book allocation of existing account equity; not an OKX transfer and not a risk budget.
/// The application authority boundary must authenticate this request before calling SQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorizedCapitalAssignment {
    pub account_key: String,
    pub installation_id: String,
    pub nav_source_ref: String,
    pub operation_id: String,
    pub request_hash: String,
    pub instance_id: String,
    pub delta_usd: Decimal,
    pub authority_revision: i64,
    pub authority_receipt: String,
    pub time: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountCapitalState {
    pub account_nav_usd: Decimal,
    pub assigned_usd: Decimal,
    pub unattributed_residual_usd: Decimal,
    pub strategy_principal_usd: std::collections::BTreeMap<String, Decimal>,
    pub nav_source_as_of: i64,
    pub nav_source_ref: String,
    pub coverage: String,
    pub reason_code: Option<String>,
}

/// Venue net position and mark observed with the same authenticated account
/// snapshot as account NAV. Signed contracts reflect OKX net/long/short mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValuationPosition {
    pub instrument_id: String,
    pub signed_contracts: Decimal,
    pub mark_price: Decimal,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyValuationEvidence {
    pub account_key: String,
    pub account_nav_source: String,
    pub as_of: i64,
    pub conversion: UsdConversionEvidence,
    pub venue_positions: Vec<ValuationPosition>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyValuation {
    pub instance_id: String,
    pub capital_usd: Decimal,
    pub realized_usd: Decimal,
    pub unrealized_usd: Decimal,
    pub equity_usd: Decimal,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StrategyValuationReconciliation {
    pub account_nav_usd: Decimal,
    pub attributed_usd: Decimal,
    pub unattributed_usd: Decimal,
    pub as_of: i64,
    pub source_ref: String,
    pub strategies: Vec<StrategyValuation>,
    pub coverage: String,
}

/// Connection stays on its owning blocking worker. It is never a network transaction.
#[derive(Debug)]
pub struct AccountStore {
    connection: Connection,
    path: PathBuf,
    meta: AccountMeta,
    _lease: std::fs::File,
    maintenance_error: Option<String>,
    // Derived memory-only views. SQLite remains the sole durable source;
    // both are rebuilt after reopen and invalidated only after COMMIT.
    cash_flow_totals_cache:
        std::cell::RefCell<Option<std::collections::BTreeMap<(String, String), Decimal>>>,
    latest_capital_assignment_time: std::cell::RefCell<Option<Option<i64>>>,
}
impl AccountStore {
    pub(crate) fn run_before_deadline<T>(
        &mut self,
        deadline: std::time::Instant,
        operation: impl FnOnce(&mut Self) -> StoreResult<T>,
    ) -> StoreResult<T> {
        self.connection
            .progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
        let result = operation(self);
        self.connection.progress_handler(0, None::<fn() -> bool>)?;
        match result {
            Err(StoreError::Sql(rusqlite::Error::SqliteFailure(e, _)))
                if e.code == rusqlite::ErrorCode::OperationInterrupted
                    && std::time::Instant::now() >= deadline =>
            {
                Err(StoreError::Invalid("ACCOUNT_STORAGE_DEADLINE"))
            }
            other => other,
        }
    }
}
pub fn exact(value: Decimal) -> String {
    value.normalize().to_string()
}
fn decimal(value: String) -> StoreResult<Decimal> {
    Decimal::from_str(&value).map_err(|_| StoreError::Invalid("INVALID_EXACT_DECIMAL"))
}
#[path = "ledger/sql/mod.rs"]
mod sql;

fn wall_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|v| v.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
