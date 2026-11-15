//! Installation-wide finance command and readback types.
//! The private owner owns the queue; sources validate original receipts; transactions commit changes.
#[cfg(test)]
use crate::{StoreError, StoreResult};
use std::sync::Arc;
mod owner;
mod readback;
mod sources;
mod transaction;
use sources::{original_bound_entry_settlement_sql, original_bound_unsent_release_sql};
pub(crate) use sources::{original_bound_entry_source_sql, original_bound_unsent_source_sql};
pub(crate) use transaction::apply_original_unified_finance_transaction;
use transaction::require_verified_live_finance_authority;
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
    InstallEntryOpening {
        source: crate::risk::NativeOpeningSource,
        census: crate::risk::VerifiedCensus,
        now_ms: i64,
    },
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
    Prepared { inserted: bool, intent_id: String },
    PolicyApplied,
    Risk(crate::risk::UnifiedRiskDecision),
    Recovered { changed: usize },
    OutcomeRecorded,
    SourceFillsRecorded,
    SourceBillsRecorded,
    SourcePageIngested,
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
