//! Native SQLite campaign owner, not a second policy authority.
//! The module is private until authenticated initialization, legacy fencing,
//! full terminal rules and production callers are proved by #510.
//! No public opening or POST permission exists in this intermediate draft.
mod owner;
mod store;
pub(crate) use store::{
    AccountReadback, ReservationReadback, read_unified_campaign_account_summary,
    read_unified_campaign_reservation, read_unified_campaign_summary, verify_legacy_import_source,
    verify_unified_import_campaign,
};

// Original RiskBook operations for the shared finance SQL writer. These are
// DOMAIN calls on the caller's existing transaction, not a second owner,
// separate campaign database or bypass around AccountStore source proofs.
pub(crate) use store::PostFenceInput;
pub(crate) use store::{ReserveInput, VerifiedCensus};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnifiedRiskDecision {
    Reserved {
        reservation_key: String,
        book_sequence: u64,
        replayed: bool,
    },
    Denied {
        reason: String,
    },
    PostAuthorized {
        reservation_key: String,
        book_sequence: u64,
        existing_modelled_loss_usdt: String,
    },
    EntrySettled {
        reservation_key: String,
        book_sequence: u64,
        cumulative_entry_usd: String,
        replayed: bool,
    },
    UnsentReleased {
        reservation_key: String,
        book_sequence: u64,
        replayed: bool,
    },
}

/// Preserve exact public error identity for scope, source, CAS and policy
/// refusals without leaking raw SQLite or transport errors into a grant.
fn original_finance_error(error: &store::RiskStoreError) -> &'static str {
    use store::RiskStoreError as E;
    match error {
        E::Invalid(reason) | E::Corrupt(reason) => reason,
        E::StaleSequence => "STALE_SESSION_OR_BOOK_SEQUENCE",
        E::StaleGeneration => "STALE_RUNTIME_GENERATION",
        E::PolicyConflict => "POLICY_SNAPSHOT_NOT_AUTHORITATIVE",
        E::CensusUnproven => "COMPLETE_ACCOUNT_CENSUS_UNPROVEN",
        E::EntrySqlUnproven => "DURABLE_ENTRY_SQL_PROOF_MISSING",
        E::EntrySqlUnavailable => "ENTRY_SQL_PROOF_UNAVAILABLE",
        E::UnsentSqlUnproven => "DURABLE_UNSENT_SQL_PROOF_MISSING",
        E::UnsentSqlUnavailable => "UNSENT_SQL_PROOF_UNAVAILABLE",
        E::LossRegression => "CUMULATIVE_LOSS_SOURCE_REGRESSION",
        E::IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
        E::AlreadyResolved => "INTENT_ALREADY_POSTED_OR_RESOLVED",
        E::WriterBusy => "RISK_WRITER_ALREADY_RUNNING",
        E::QueueFull => "RISK_OWNER_QUEUE_FULL",
        E::WorkerStopped => "RISK_OWNER_STOPPED",
        E::OutcomeUnknown => "RISK_OUTCOME_UNKNOWN_DO_NOT_REPLAY",
        E::IdentityMismatch => "RISK_BOOK_IDENTITY_MISMATCH",
        E::Uninitialized => "RISK_BOOK_UNINITIALIZED",
        E::Denied(_) => "RISK_RULE_DENIED",
        E::Money(_) => "RISK_MONEY_INVALID",
        E::Sql(_) | E::Io(_) | E::Encode(_) => "RISK_STORAGE_UNAVAILABLE",
    }
}

pub(crate) fn reserve_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    input: &ReserveInput,
    census: &VerifiedCensus,
    receipt: &crate::CentralReceipt,
) -> Result<UnifiedRiskDecision, &'static str> {
    use store::{ReserveDecision, RiskBookStore};
    match RiskBookStore::reserve_unified_account_transaction(
        tx,
        installation,
        campaign,
        input,
        census,
        receipt,
    )
    .map_err(|e| original_finance_error(&e))?
    {
        ReserveDecision::Held(receipt) => Ok(UnifiedRiskDecision::Reserved {
            reservation_key: receipt.reservation_key,
            book_sequence: receipt.book_sequence,
            replayed: receipt.replayed,
        }),
        ReserveDecision::Rejected(reason) => Ok(UnifiedRiskDecision::Denied {
            reason: reason.to_string(),
        }),
    }
}

pub(crate) fn begin_post_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    input: &PostFenceInput,
) -> Result<UnifiedRiskDecision, &'static str> {
    use store::{PostDecision, begin_unified_post_and_account_transaction};
    match begin_unified_post_and_account_transaction(tx, installation, campaign, input)
        .map_err(|e| original_finance_error(&e))?
    {
        PostDecision::Authorized(receipt) => Ok(UnifiedRiskDecision::PostAuthorized {
            reservation_key: receipt.reservation_key,
            book_sequence: receipt.book_sequence,
            existing_modelled_loss_usdt: receipt.existing_modelled_loss_usdt,
        }),
        PostDecision::Denied(reason) => Ok(UnifiedRiskDecision::Denied {
            reason: reason.to_string(),
        }),
    }
}

pub(crate) fn settle_partial_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    account: &str,
    intent: &str,
    expected_sequence: u64,
    generation: u64,
) -> Result<UnifiedRiskDecision, &'static str> {
    let receipt = store::RiskBookStore::settle_unified_partial_from_account_sql_transaction(
        tx,
        installation,
        campaign,
        account,
        intent,
        expected_sequence,
        generation,
    )
    .map_err(|e| original_finance_error(&e))?;
    Ok(UnifiedRiskDecision::EntrySettled {
        reservation_key: receipt.reservation_key,
        book_sequence: receipt.book_sequence,
        cumulative_entry_usd: receipt.cumulative_entry_usd,
        replayed: receipt.replayed,
    })
}

pub(crate) fn release_unsent_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    account: &str,
    intent: &str,
    expected_sequence: u64,
    generation: u64,
) -> Result<UnifiedRiskDecision, &'static str> {
    let receipt = store::RiskBookStore::release_unified_unsent_from_account_sql_transaction(
        tx,
        installation,
        campaign,
        account,
        intent,
        expected_sequence,
        generation,
    )
    .map_err(|e| original_finance_error(&e))?;
    Ok(UnifiedRiskDecision::UnsentReleased {
        reservation_key: receipt.reservation_key,
        book_sequence: receipt.book_sequence,
        replayed: receipt.replayed,
    })
}

#[cfg(test)]
mod tests;

// Offline contractual trials and the eventual native SQL FIFO now invoke
// the SAME typed original Rust operations above, never parallel test-only
// Reserve/Settle/Unsent algorithms with a different transaction boundary.

/// Test-only native PostCausalProof constructor for the real v6 SQL
/// transaction integration witness. Synthetic proof is NEVER compiled into
/// production and is not an authorization/venue sender.
#[cfg(test)]
pub(crate) fn test_stage_scoped_begin_post(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    reservation: &str,
    plan: &str,
    cohort: &[&str],
    expected_sequence: u64,
) -> Result<u64, String> {
    use okx_kernel::risk::{
        model::{PostAccountRead, PostCausalProof},
        money::RiskAmount,
    };
    use std::collections::BTreeMap;
    use store::{PostFenceInput, VerifiedCensus};
    let zero = RiskAmount::zero();
    let reads = cohort
        .iter()
        .enumerate()
        .map(|(i, account)| PostAccountRead {
            account_key: (*account).into(),
            private_generation: 7,
            source_read_id: format!("{:064x}", i + 1),
            content_digest: "c".repeat(64),
            started_after_held_ms: 0,
            completed_after_held_ms: 1,
        })
        .collect();
    let input = PostFenceInput {
        reservation_key: reservation.to_owned(),
        plan_sha256: plan.to_owned(),
        policy_revision: 1,
        expected_sequence,
        now_ms: 10_002,
        source_proof_digest: "b".repeat(64),
        census: VerifiedCensus {
            complete: true,
            generation: 7,
            source_digest: "d".repeat(64),
            generated_at_ms: 10_001,
            read_floor_ms: 9_996,
            occupied: cohort
                .iter()
                .map(|id| ((*id).to_owned(), zero.clone()))
                .collect::<BTreeMap<_, _>>(),
            cumulative_loss_usd: cohort
                .iter()
                .map(|id| ((*id).to_owned(), zero.clone()))
                .collect::<BTreeMap<_, _>>(),
        },
        causal: PostCausalProof {
            reserve_anchor: reservation.to_owned(),
            native_incarnation: "f".repeat(32),
            elapsed_to_proof_ms: 2,
            account_reads: reads,
        },
    };
    match begin_post_unified_native(tx, installation, campaign, &input).map_err(str::to_owned)? {
        UnifiedRiskDecision::PostAuthorized { book_sequence, .. } => Ok(book_sequence),
        UnifiedRiskDecision::Denied { reason } => Err(reason),
        _ => Err("UNIFIED_POST_RECEIPT_UNEXPECTED".into()),
    }
}
