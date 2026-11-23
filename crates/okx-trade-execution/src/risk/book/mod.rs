//! Campaign risk rules on the installation owner's existing transaction.
//! Production has no campaign database or separate worker. Component tests
//! exercise SQL rules directly; concurrency belongs to UnifiedFinanceHandle.
mod store;
pub(crate) fn read_unified_campaign_opening(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
) -> Result<Option<NativeOpeningSource>, &'static str> {
    store::read_unified_campaign_opening(tx, installation, campaign)
        .map_err(|e| original_finance_error(&e))
}
#[cfg(test)]
pub(crate) use store::verify_legacy_import_source;
pub(crate) use store::{
    AccountReadback, ReservationReadback, read_unified_campaign_account_summary,
    read_unified_campaign_reservation, read_unified_campaign_summary,
    verify_unified_import_campaign,
};

// Original RiskBook operations for the shared finance SQL writer. These are
// DOMAIN calls on the caller's existing transaction, not a second owner,
// separate campaign database or bypass around AccountStore source proofs.
pub use store::{NativeOpeningAccount, NativeOpeningSource};
pub(crate) use store::{PostFenceInput, TerminalResolveInput};
pub(crate) use store::{ReserveInput, VerifiedCensus};

/// The ORIGINAL AccountStore receipt type to debit; never caller-supplied
/// monetary values. All three sources share the same RiskBook arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntrySettlementSource {
    Partial,
    Terminal,
    Late,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnifiedRiskDecision {
    Reserved {
        reservation_key: String,
        book_sequence: u64,
        replayed: bool,
    },
    EntryOpeningInstalled {
        source_digest: String,
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
    TerminalReleased {
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
        E::UnsentSqlUnproven => "DURABLE_UNSENT_SQL_PROOF_MISSING",
        E::LossRegression => "CUMULATIVE_LOSS_SOURCE_REGRESSION",
        E::IdempotencyConflict => "IDEMPOTENCY_CONFLICT",
        E::AlreadyResolved => "INTENT_ALREADY_POSTED_OR_RESOLVED",
        #[cfg(test)]
        E::WriterBusy => "RISK_WRITER_ALREADY_RUNNING",
        #[cfg(test)]
        E::IdentityMismatch => "RISK_BOOK_IDENTITY_MISMATCH",
        #[cfg(test)]
        E::Uninitialized => "RISK_BOOK_UNINITIALIZED",
        E::Denied(reason) => reason.code(),
        E::Money(reason) => reason.code(),
        E::Sql(_) | E::Io(_) | E::Encode(_) => "RISK_STORAGE_UNAVAILABLE",
    }
}

/// One original AccountStore baseline for ALL accounts, one original
/// RiskBook budget, one native installation-owned SQL COMMIT.
pub(crate) fn install_entry_opening_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    source: &NativeOpeningSource,
    census: &VerifiedCensus,
    now_ms: i64,
) -> Result<UnifiedRiskDecision, &'static str> {
    let installed =
        store::RiskBookStore::install_unified_entry_opening_from_account_sql_transaction(
            tx,
            installation,
            source,
            census,
            now_ms,
        )
        .map_err(|error| original_finance_error(&error))?;
    Ok(UnifiedRiskDecision::EntryOpeningInstalled {
        source_digest: installed.source_digest,
        book_sequence: installed.book_sequence,
        replayed: installed.replayed,
    })
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

#[expect(
    clippy::too_many_arguments,
    reason = "Keep SQL scope, original proof and CAS fields explicit at this bounded boundary."
)]
pub(crate) fn settle_entry_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    account: &str,
    intent: &str,
    expected_sequence: u64,
    generation: u64,
    source: EntrySettlementSource,
) -> Result<UnifiedRiskDecision, &'static str> {
    let receipt = store::RiskBookStore::settle_unified_entry_from_account_sql_transaction(
        tx,
        installation,
        campaign,
        account,
        intent,
        expected_sequence,
        generation,
        source,
    )
    .map_err(|e| original_finance_error(&e))?;
    Ok(UnifiedRiskDecision::EntrySettled {
        reservation_key: receipt.reservation_key,
        book_sequence: receipt.book_sequence,
        cumulative_entry_usd: receipt.cumulative_entry_usd,
        replayed: receipt.replayed,
    })
}

/// Only the immutable original terminal debit + full native flat census
/// can unlock this terminal refund. No second finance writer or network owner.
pub(crate) fn resolve_terminal_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    campaign: &str,
    input: &TerminalResolveInput,
) -> Result<UnifiedRiskDecision, &'static str> {
    let accepted = store::RiskBookStore::resolve_unified_terminal_from_account_sql_transaction(
        tx,
        installation,
        campaign,
        input,
    )
    .map_err(|error| original_finance_error(&error))?;
    Ok(UnifiedRiskDecision::TerminalReleased {
        reservation_key: accepted.reservation_key,
        book_sequence: accepted.book_sequence,
        replayed: accepted.replayed,
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

/// Project the same original AccountStore outcome and RiskBook state under
/// one installation-owned SQLite transaction. No new grant, post or release.
pub(crate) fn reconcile_outcome_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: &str,
    intent: &str,
) -> Result<(), &'static str> {
    store::RiskBookStore::reconcile_unified_outcome_from_account_sql_transaction(
        tx,
        installation,
        account,
        intent,
    )
    .map_err(|error| original_finance_error(&error))
}

/// Original POST-fence recovery: the local intent and RiskBook transition
/// happen under one caller-owned installation transaction and retain all
/// charged budget until independent venue readback resolves UNKNOWN.
pub(crate) fn recover_posting_unified_native(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: &str,
) -> Result<usize, &'static str> {
    store::RiskBookStore::recover_unified_posting_from_account_sql_transaction(
        tx,
        installation,
        account,
    )
    .map_err(|error| original_finance_error(&error))
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
    use okx_domain_core::risk::{
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
    // Exercise the exact command/savepoint boundary used by the finance FIFO,
    // including the real child-process kill and original WAL reopen witnesses.
    // Only input construction above is test-specific, never the algorithm.
    use crate::account_worker::{
        UnifiedFinanceWrite, UnifiedFinanceWriteReceipt, apply_original_unified_finance_transaction,
    };
    let receipt = apply_original_unified_finance_transaction(
        tx,
        installation,
        UnifiedFinanceWrite::BeginPost {
            campaign: campaign.to_owned(),
            input,
        },
    )
    .map_err(|error| error.to_string())?;
    match receipt {
        UnifiedFinanceWriteReceipt::Risk(UnifiedRiskDecision::PostAuthorized {
            book_sequence,
            ..
        }) => Ok(book_sequence),
        UnifiedFinanceWriteReceipt::Risk(UnifiedRiskDecision::Denied { reason }) => Err(reason),
        _ => Err("UNIFIED_POST_RECEIPT_UNEXPECTED".into()),
    }
}

#[cfg(test)]
mod diagnostic_contract_tests {
    use super::{original_finance_error, store::RiskStoreError};
    use okx_domain_core::risk::{money::MoneyError, rules::RiskRuleError};

    #[test]
    fn risk_book_contract_domain_reason_survives_finance_boundary() {
        for (error, expected) in [
            (
                RiskStoreError::Denied(RiskRuleError::EntryBudgetExhausted),
                "CAMPAIGN_ENTRY_BUDGET_EXHAUSTED",
            ),
            (
                RiskStoreError::Denied(RiskRuleError::AccountAlreadyOverLimit),
                "ACCOUNT_ALREADY_OVER_LIMIT",
            ),
            (
                RiskStoreError::Denied(RiskRuleError::PostCausalProofUnproven),
                "POST_CAUSAL_PROOF_UNPROVEN",
            ),
            (
                RiskStoreError::Money(MoneyError::NumericRange),
                "NUMERIC_RANGE",
            ),
            (
                RiskStoreError::Money(MoneyError::Negative),
                "NEGATIVE_RISK_AMOUNT",
            ),
        ] {
            assert_eq!(original_finance_error(&error), expected);
        }
    }
}
