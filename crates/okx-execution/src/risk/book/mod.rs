//! Native SQLite campaign owner, not a second policy authority.
//! The module is private until authenticated initialization, legacy fencing,
//! full terminal rules and production callers are proved by #510.
//! No public opening or POST permission exists in this intermediate draft.
mod owner;
mod store;
pub(crate) use store::{
    read_unified_campaign_summary, verify_legacy_import_source, verify_unified_import_campaign,
};
#[cfg(test)]
mod tests;

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
    use store::{
        PostDecision, PostFenceInput, VerifiedCensus, begin_unified_post_and_account_transaction,
    };
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
    match begin_unified_post_and_account_transaction(tx, installation, campaign, &input)
        .map_err(|e| e.to_string())?
    {
        PostDecision::Authorized(staged) => Ok(staged.book_sequence),
        PostDecision::Denied(reason) => Err(reason.to_string()),
    }
}
