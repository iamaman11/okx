//! Installation-wide finance command and readback types.
//! The private owner owns the queue; sources validate original receipts; transactions commit changes.
#[cfg(test)]
use crate::{StoreError, StoreResult};
use std::sync::Arc;
mod opening_ingress;
mod owner;
#[cfg(test)]
mod owner_account_source_scope_tests;
#[cfg(test)]
mod owner_admission_crash_tests;
#[cfg(test)]
mod owner_diagnostic_busy_tests;
#[cfg(test)]
mod owner_entry_diagnostic_tests;
#[cfg(test)]
mod owner_finance_snapshot_tests;
#[cfg(test)]
mod owner_financial_crash_tests;
#[cfg(test)]
mod owner_financial_reply_tests;
#[cfg(test)]
mod owner_queue_tests;
#[cfg(test)]
mod owner_reserve_reply_tests;
mod owner_startup;
#[cfg(test)]
mod owner_startup_tests;
#[cfg(test)]
mod owner_terminal_rollback_tests;
#[cfg(test)]
mod owner_workload_tests;
#[cfg(test)]
pub(crate) use opening_ingress::install_reported_opening_sql;
pub use opening_ingress::{ReportedOpeningCensus, ReportedOpeningReceipt};
mod account_initialization;
mod admission_identity;
mod admission_lifecycle;
pub use admission_identity::{AdmissionIntentId, AdmissionReservationKey, AdmissionRunId};
mod authority_reads;
mod campaign_initialization;
mod campaign_readiness;
pub use campaign_initialization::{
    NativeCampaignAccountPolicy, NativeCampaignPolicy, NativeCampaignPolicyReceipt,
    NativeCampaignStrategyPolicy,
};
mod census_reads;
mod census_time;
pub use census_time::{CensusObservedAtMs, CensusReadFloorMs};
mod finance_authority;
mod financial_admission;
mod native_admission;
mod operation_diagnostics;
mod order_observation;
#[cfg(test)]
pub(crate) use native_admission::{
    bind_census as test_bind_native_census, reserve_reported_sql as test_reserve_reported_sql,
};
pub use operation_diagnostics::{
    NativeAccountDiagnosticSnapshot, NativeBudgetDenial, NativeIntentDiagnosticSnapshot,
    NativeRiskStopDiagnostic,
};
mod entry_settlement;
mod posting_recovery;
mod storage_recovery;
mod strategy_operations;
pub use storage_recovery::NativeRecoveryProof;
pub use strategy_operations::{StrategyDecisionClaimCommand, StrategyMutationCommand};
mod protection_cleanup;
mod protection_resize;
mod protection_status;
mod readback;
mod reduction_submission;
mod reservation;
mod source_permission;
mod source_stops;
mod terminal_status;
mod unsent_status;
pub use native_admission::{
    NativePostDecision, NativePostRequest, NativeReserveDecision, NativeReserveRequest,
    ReportedRiskCensus,
};
mod reserve_source;
pub(crate) use reservation::{native_reservation_receipt_sql, reserve_native_sql};
mod sources;
mod trade_reads;
mod transaction;
use finance_authority::require_verified_live_finance_authority;
use sources::{original_bound_entry_settlement_sql, original_bound_unsent_release_sql};
#[cfg(test)]
pub(crate) use sources::{original_bound_entry_source_sql, original_bound_unsent_source_sql};
pub(crate) use transaction::apply_original_unified_finance_transaction;
/// Test-only assertion against the immutable, independently verified
/// stopped-import marker. A trial cannot create a live finance grant.
#[cfg(test)]
fn require_offline_finance_trial(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
) -> StoreResult<()> {
    let marker: String = tx.query_row(
        "SELECT state FROM finance_import_manifest
         WHERE singleton=1 AND installation_id=?1",
        [installation],
        |row| row.get(0),
    )?;
    if marker != "OFFLINE_NOT_AUTHORIZED" {
        return Err(StoreError::Invalid("UNIFIED_TRIAL_REQUIRES_OFFLINE_IMPORT"));
    }
    Ok(())
}

#[derive(Clone)]
pub struct UnifiedFinanceHandle {
    owner: Arc<owner::FinanceWorkerOwner>,
    installation: String,
}
impl UnifiedFinanceHandle {
    /// Immutable identity of the SQL owner; no grant or mutable state.
    pub fn installation_id(&self) -> &str {
        &self.installation
    }
}

/// Exactly one Rust SQL writer for the account and RiskBook parts of
/// native financial commands. Typed inputs are produced by the existing
/// domain/source owners, never by arbitrary JSON or Cloudflare budget code.
/// The *same* dispatch is used for rollback-only offline contract tests.
/// #510: one authenticated campaign policy is applied to every ORIGINAL
/// AccountStore primary row and the ORIGINAL RiskBook policy revision under
/// one installation-owned SQL transaction. This DTO is NOT an authority
/// receipt: the live caller and RB09 activation gate remain independent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedFinancePolicyAccount {
    pub account_key: String,
    pub paused: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedFinancePolicy {
    pub campaign_id: String,
    pub policy_revision: u64,
    pub campaign_loss_limit_usd: String,
    pub accounts: Vec<UnifiedFinancePolicyAccount>,
}

/// Authenticated ReleaseUnsent is an address to the ORIGINAL AccountStore
/// no-send receipt, never a source-proof constructor or a POST capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedFinanceBoundUnsent {
    pub campaign: String,
    pub reservation_key: String,
    pub plan_sha256: String,
    pub expected_sequence: u64,
    pub account_key: String,
    pub intent_id: String,
    pub request_hash: String,
    pub reason: String,
    pub record_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedFinanceBoundUnsentReceipt {
    pub reservation_key: String,
    pub book_sequence: u64,
    pub policy_revision: u64,
}

/// Source-only settlement address from the authenticated native RiskCommand.
/// Numeric fields are comparisons against original AccountStore immutable
/// SQL receipts, NEVER instructions to debit a caller-selected amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedFinanceBoundSettlement {
    pub campaign: String,
    pub reservation_key: String,
    pub plan_sha256: String,
    pub expected_sequence: u64,
    pub cumulative_entry_usd: String,
    pub settlement_revision: u64,
    pub source_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedFinanceBoundSettlementReceipt {
    pub reservation_key: String,
    pub book_sequence: u64,
    pub policy_revision: u64,
    pub cumulative_entry_usd: String,
}

#[derive(Debug, Clone)]
pub(crate) enum UnifiedFinanceWrite {
    ReserveNative {
        campaign: String,
        input: crate::risk::ReserveInput,
        census: crate::risk::VerifiedCensus,
    },
    InstallReportedOpening {
        source: crate::risk::NativeOpeningSource,
        census: ReportedOpeningCensus,
    },
    AssignBoundCapital {
        account: String,
        command: crate::CapitalAssignmentCommand,
    },
    AssignCapital {
        account: String,
        request: crate::AuthorizedCapitalAssignment,
    },
    CommitStrategyNav {
        account: String,
        evidence: crate::StrategyValuationEvidence,
        cursor: crate::SourceCursor,
    },
    ProjectAccountNav {
        account: String,
        snapshot: crate::AccountNavSnapshot,
    },
    ProjectPendingFills {
        account: String,
        limit: usize,
    },
    AttributePendingFunding {
        account: String,
        conversion: crate::UsdConversionEvidence,
        limit: usize,
    },
    IngestCapturedBills {
        account: String,
        input: crate::CapturedBillPage,
        conversion: crate::UsdConversionEvidence,
    },
    IngestCapturedFills {
        account: String,
        input: crate::CapturedFillPage,
        conversion: crate::UsdConversionEvidence,
    },
    IngestSourcePageUsd {
        account: String,
        page: crate::IngestPage,
        conversion: crate::UsdConversionEvidence,
    },
    IngestSourcePage {
        account: String,
        page: crate::IngestPage,
    },
    RecordSourceBills {
        account: String,
        bills: Vec<crate::Bill>,
        order_sources: Vec<crate::BillOrderSource>,
    },
    RecordSourceFills {
        account: String,
        fills: Vec<crate::Fill>,
    },
    /// The original AccountStore decision, immutable plan and intent are
    /// persisted under the same installation writer as subsequent RiskBook
    /// commands. An idempotent replay returns the FIRST stored identity.
    Prepare {
        account: String,
        decision: crate::Decision,
        intent: crate::NewIntent,
    },
    /// An installation-wide policy update, including all original account
    /// policy rows and the campaign RiskBook CAS revision, in ONE COMMIT.
    ApplyPolicy {
        policy: UnifiedFinancePolicy,
    },
    #[cfg(test)]
    InstallEntryOpening {
        source: crate::risk::NativeOpeningSource,
        census: crate::risk::VerifiedCensus,
        now_ms: i64,
    },
    #[cfg(test)]
    Reserve {
        campaign: String,
        input: crate::risk::ReserveInput,
        census: crate::risk::VerifiedCensus,
        receipt: crate::CentralReceipt,
    },
    BeginPost {
        campaign: String,
        input: crate::risk::PostFenceInput,
    },
    #[cfg(test)]
    SettleEntry {
        campaign: String,
        account: String,
        intent: String,
        expected_sequence: u64,
        generation: u64,
        source: crate::risk::EntrySettlementSource,
    },
    /// Wire fields are an address+CAS only. All economic and account identity
    /// proofs are independently fetched from ORIGINAL AccountStore in this tx.
    SettleBoundEntry {
        request: UnifiedFinanceBoundSettlement,
    },
    /// Original immutable REJECTED_UNSENT source, not caller proof flags.
    ReleaseBoundUnsent {
        request: UnifiedFinanceBoundUnsent,
    },
    ResolveTerminal {
        campaign: String,
        input: crate::risk::TerminalResolveInput,
    },
    #[cfg(test)]
    ReleaseUnsent {
        campaign: String,
        account: String,
        intent: String,
        expected_sequence: u64,
        generation: u64,
    },
    RecoverPosting {
        account: String,
        now: i64,
    },
    RecoverInstallation {
        now: i64,
    },
    RecordOutcome {
        account: String,
        intent: String,
        state: crate::IntentState,
        exchange_order_id: Option<String>,
        evidence: String,
        now: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnifiedFinanceWriteReceipt {
    CapitalAssignmentReported(crate::CapitalAssignmentResult),
    CapitalAssigned(crate::OperationReceipt),
    StrategyNavCommitted(crate::StrategyValuationReconciliation),
    Prepared { inserted: bool, intent_id: String },
    PolicyApplied,
    Risk(crate::risk::UnifiedRiskDecision),
    Recovered { changed: usize },
    OutcomeRecorded,
    SourceFillsRecorded,
    SourceBillsRecorded,
    SourcePageIngested,
    FundingAttributed { count: usize },
    FillsProjected { count: usize },
    SourcePageRejected { reason: &'static str },
    BoundEntrySettled(UnifiedFinanceBoundSettlementReceipt),
    BoundUnsentReleased(UnifiedFinanceBoundUnsentReceipt),
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct UnifiedFinanceInventory {
    pub accounts: i64,
    pub campaigns: i64,
    pub state: String,
}

/// Exact original RiskBook projection, not a second financial calculation.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct UnifiedFinanceCampaignSummary {
    /// Durable campaign inception, independent of its later money opening.
    pub campaign_started_at_ms: Option<u64>,
    pub opening: Option<UnifiedFinanceOpeningReadback>,
    pub campaign: String,
    pub book_sequence: u64,
    pub policy_revision: u64,
    pub global_reserved_usdt: String,
    pub loss_limit_usd: String,
    pub consumed_loss_usd: String,
    pub reserved_loss_usd: String,
    pub remaining_loss_usd: String,
    pub entry_limit_usd: String,
    pub consumed_entry_usd: String,
    pub reserved_entry_usd: String,
    pub entry_bound_breach_usd: String,
    pub remaining_entry_usd: String,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct UnifiedFinanceOpeningReadback {
    pub source_digest: String,
    pub as_of_ms: u64,
    pub campaign_started_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnifiedFinanceAccountReadback {
    pub account_key: String,
    pub policy_revision: u64,
    pub book_sequence: u64,
    pub active_reservations: u64,
    pub reserved_loss_usdt: String,
    pub reserved_loss_usd: String,
    pub cumulative_loss_usd: String,
    pub latest_activity_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnifiedFinanceReservationReadback {
    pub reservation_key: String,
    pub plan_sha256: String,
    pub state: &'static str,
    pub reservation_sequence: u64,
    pub current_book_sequence: u64,
    pub policy_revision: u64,
}

/// Exact original stopped AccountStore policy, not an incoming proposal.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct UnifiedFinanceAccountPolicyReadback {
    pub account_key: String,
    pub revision: u64,
    pub paused: bool,
    pub campaign_limit_usd: String,
}
