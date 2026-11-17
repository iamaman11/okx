//! One installation financial owner.
use crate::{AccountStorageSnapshot, WalMaintenanceObservation};
use std::sync::atomic::AtomicU64;

#[derive(Default)]
struct SqlTimingCounters {
    jobs: AtomicU64,
    last_queue_wait_ms: AtomicU64,
    last_sql_owner_ms: AtomicU64,
    max_queue_wait_ms: AtomicU64,
    max_sql_owner_ms: AtomicU64,
}

/// Diagnostic-only timings for jobs that reached this account's SQL owner.
#[derive(Clone, Debug, serde::Serialize)]
pub struct AccountSqlTimingV1 {
    pub schema: &'static str,
    pub observed_jobs: u64,
    pub last_queue_wait_ms: u64,
    pub last_sql_owner_ms: u64,
    pub max_queue_wait_ms: u64,
    pub max_sql_owner_ms: u64,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct MaintenanceEvent {
    pub id: u64,
    pub kind: &'static str,
    pub started_ms: i64,
    pub finished_ms: Option<i64>,
    pub elapsed_us: Option<u64>,
    pub outcome: &'static str,
    pub wal: Option<WalMaintenanceObservation>,
}
#[derive(serde::Serialize)]
pub struct AccountMaintenanceTimingV1 {
    pub schema: &'static str,
    pub state: &'static str,
    pub events: Vec<MaintenanceEvent>,
    pub evicted: u64,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct AccountStorageReadback {
    pub schema: &'static str,
    pub state: &'static str,
    pub snapshot: Option<AccountStorageSnapshot>,
    pub last_attempt_ms: Option<i64>,
    pub reason_code: Option<String>,
    pub refresh_pending: bool,
    pub refresh_attempts: u64,
    pub publication_skipped: u64,
    pub owner_terminated: bool,
}

mod unified;
pub use crate::risk::{NativeOpeningAccount, NativeOpeningSource};
pub use unified::{
    NativeAccountDiagnosticSnapshot, NativeCampaignAccountPolicy, NativeCampaignPolicy,
    NativeCampaignPolicyReceipt, NativeCampaignStrategyPolicy, NativeIntentDiagnosticSnapshot,
    NativePostDecision, NativePostRequest, NativeRecoveryProof, NativeReserveDecision,
    NativeReserveRequest, NativeRiskStopDiagnostic, ReportedOpeningCensus, ReportedOpeningReceipt,
    ReportedRiskCensus, StrategyDecisionClaimCommand, StrategyMutationCommand,
    UnifiedFinanceAccountPolicyReadback, UnifiedFinanceAccountReadback,
    UnifiedFinanceBoundSettlement, UnifiedFinanceBoundSettlementReceipt, UnifiedFinanceBoundUnsent,
    UnifiedFinanceBoundUnsentReceipt, UnifiedFinanceCampaignSummary, UnifiedFinanceHandle,
    UnifiedFinanceInventory, UnifiedFinancePolicy, UnifiedFinancePolicyAccount,
    UnifiedFinanceReservationReadback,
};
#[cfg(test)]
pub(crate) use unified::{
    UnifiedFinanceWrite, UnifiedFinanceWriteReceipt, apply_original_unified_finance_transaction,
    install_reported_opening_sql, native_reservation_receipt_sql, original_bound_entry_source_sql,
    original_bound_unsent_source_sql,
};
#[cfg(test)]
pub(crate) use unified::{test_bind_native_census, test_reserve_reported_sql};
