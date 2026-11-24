//! Resolve untrusted settlement/no-send addresses against original immutable SQL receipts.
use super::{
    UnifiedFinanceBoundSettlement, UnifiedFinanceBoundSettlementReceipt, UnifiedFinanceBoundUnsent,
    UnifiedFinanceBoundUnsentReceipt,
};
use crate::{AccountStore, StoreError, StoreResult};
use rusqlite::OptionalExtension;
pub(crate) fn original_bound_unsent_source_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    request: &UnifiedFinanceBoundUnsent,
) -> StoreResult<()> {
    use sha2::Digest;
    if installation.is_empty()
        || request.campaign.is_empty()
        || request.account_key.is_empty()
        || request.intent_id.is_empty()
        || request.reservation_key.is_empty()
        || request.plan_sha256.is_empty()
    {
        return Err(StoreError::Invalid("UNIFIED_UNSENT_WIRE_SCOPE_INVALID"));
    }
    let assigned: Option<String> = tx
        .query_row(
            "SELECT campaign_id FROM account_policy
         WHERE installation_id=?1 AND account_key=?2 AND singleton=1",
            rusqlite::params![installation, &request.account_key],
            |row| row.get(0),
        )
        .optional()?;
    if assigned.as_deref() != Some(request.campaign.as_str()) {
        return Err(StoreError::Invalid(
            "UNIFIED_UNSENT_ACCOUNT_CAMPAIGN_MISMATCH",
        ));
    }
    let original = AccountStore::unsent_release_receipt_sql(
        tx,
        Some(installation),
        &request.account_key,
        &request.intent_id,
    )?
    .ok_or(StoreError::Invalid("UNIFIED_UNSENT_ORIGINAL_PROOF_MISSING"))?;
    let digest = format!(
        "{:x}",
        sha2::Sha256::digest(
            serde_json::to_vec(&original)
                .map_err(|_| StoreError::Invalid("UNIFIED_UNSENT_ORIGINAL_DIGEST_INVALID"))?
        )
    );
    if original.reservation_key != request.reservation_key
        || original.account_key != request.account_key
        || original.intent_id != request.intent_id
        || original.plan_hash != request.plan_sha256
        || original.request_hash != request.request_hash
        || original.reason != request.reason
        || digest != request.record_digest
    {
        return Err(StoreError::Invalid("UNIFIED_UNSENT_WIRE_SOURCE_CONFLICT"));
    }
    Ok(())
}

pub(super) fn original_bound_unsent_release_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    request: &UnifiedFinanceBoundUnsent,
) -> StoreResult<UnifiedFinanceBoundUnsentReceipt> {
    original_bound_unsent_source_sql(tx, installation, request)?;
    let (generation, revision): (i64, i64) = tx.query_row(
        "SELECT generation,policy_revision FROM risk_meta
         WHERE installation_id=?1 AND campaign_id=?2 AND singleton=1",
        rusqlite::params![installation, &request.campaign],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let generation = u64::try_from(generation)
        .map_err(|_| StoreError::Invalid("UNIFIED_UNSENT_GENERATION_INVALID"))?;
    let policy_revision = u64::try_from(revision)
        .map_err(|_| StoreError::Invalid("UNIFIED_UNSENT_POLICY_REVISION_INVALID"))?;
    if generation == 0 || policy_revision == 0 {
        return Err(StoreError::Invalid(
            "UNIFIED_UNSENT_AUTHORITY_REVISION_INVALID",
        ));
    }
    // The original v6 domain re-reads the very same scoped AccountStore
    // receipt and commits a RiskBook release/tombstone under this tx.
    let outcome = crate::risk::release_unsent_unified_native(
        tx,
        installation,
        &request.campaign,
        &request.account_key,
        &request.intent_id,
        request.expected_sequence,
        generation,
    )
    .map_err(StoreError::Invalid)?;
    match outcome {
        crate::risk::UnifiedRiskDecision::UnsentReleased {
            reservation_key,
            book_sequence,
            ..
        } if reservation_key == request.reservation_key => {
            // The immutable original no-send receipt, RiskBook tombstone and
            // account acknowledgement have ONE transaction boundary. A lost
            // caller reply cannot leave a release waiting for a second writer.
            let evidence = format!("native-unsent-sql:{}", request.record_digest);
            AccountStore::acknowledge_central_release_sql(
                tx,
                Some((installation, &request.account_key)),
                &reservation_key,
                &evidence,
                super::opening_ingress::owner_now_ms()?,
            )?;
            Ok(UnifiedFinanceBoundUnsentReceipt {
                reservation_key,
                book_sequence,
                policy_revision,
            })
        }
        _ => Err(StoreError::Invalid("UNIFIED_UNSENT_RECEIPT_UNEXPECTED")),
    }
}

/// Resolve authenticated wire SettleEntry against the ORIGINAL immutable
/// AccountStore source. The caller may name an exact reservation/plan/digest
/// but cannot choose the account, intent, settlement kind, generation,
/// revision or money without matching a producer-sealed durable SQL receipt.
/// No SQL writes and no second financial owner here.
pub(crate) fn original_bound_entry_source_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: &str,
    intent: &str,
    request: &UnifiedFinanceBoundSettlement,
) -> StoreResult<crate::risk::EntrySettlementSource> {
    use crate::risk::EntrySettlementSource;
    use okx_domain_core::risk::money::RiskAmount;
    if installation.is_empty()
        || account.is_empty()
        || intent.is_empty()
        || request.campaign.is_empty()
        || request.reservation_key.is_empty()
        || request.reservation_key.len() > 512
        || request.plan_sha256.len() != 64
        || request.source_digest.len() != 64
        || ![&request.plan_sha256, &request.source_digest]
            .iter()
            .all(|s| {
                s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        || request.expected_sequence == 0
        || request.settlement_revision == 0
    {
        return Err(StoreError::Invalid("UNIFIED_SETTLEMENT_WIRE_SCOPE_INVALID"));
    }
    let claimed = RiskAmount::parse(&request.cumulative_entry_usd)
        .map_err(|_| StoreError::Invalid("UNIFIED_SETTLEMENT_AMOUNT_INVALID"))?;
    // All three ORIGINAL read functions verify immutable event receipts,
    // plan, account, venue order and exact revision in their original code.
    // Corrupt one source -> fail closed; never try the next as a fallback.
    let candidates = [
        (
            EntrySettlementSource::Partial,
            AccountStore::partial_entry_settlement_receipt_sql(
                tx,
                Some(installation),
                account,
                intent,
            )?,
        ),
        (
            EntrySettlementSource::Terminal,
            AccountStore::final_entry_settlement_receipt_sql(
                tx,
                installation,
                account,
                intent,
                false,
            )?,
        ),
        (
            EntrySettlementSource::Late,
            AccountStore::final_entry_settlement_receipt_sql(
                tx,
                installation,
                account,
                intent,
                true,
            )?,
        ),
    ];
    let mut selected = None;
    for (source, row) in candidates {
        let Some(proof) = row else { continue };
        let actual = RiskAmount::parse(&proof.cumulative_entry_usd.normalize().to_string())
            .map_err(|_| StoreError::Invalid("UNIFIED_SETTLEMENT_SOURCE_MONEY_INVALID"))?;
        if proof.account_key == account
            && proof.intent_id == intent
            && proof.reservation_key == request.reservation_key
            && proof.plan_hash == request.plan_sha256
            && proof.source_digest == request.source_digest
            && proof.settlement_revision == request.settlement_revision
            && claimed == actual
        {
            if selected.is_some() {
                return Err(StoreError::Invalid("UNIFIED_SETTLEMENT_SOURCE_AMBIGUOUS"));
            }
            selected = Some(source);
        }
    }
    selected.ok_or(StoreError::Invalid("UNIFIED_SETTLEMENT_SOURCE_MISMATCH"))
}

/// One original RiskBook/AccountStore source lookup and one original financial
/// mutation in the SAME already-held BEGIN IMMEDIATE transaction.
pub(super) fn original_bound_entry_settlement_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    request: &UnifiedFinanceBoundSettlement,
) -> StoreResult<UnifiedFinanceBoundSettlementReceipt> {
    let row: Option<(String, String, String)> = tx
        .query_row(
            "SELECT account_key,intent_id,plan_sha256
         FROM risk_entries WHERE installation_id=?1 AND campaign_id=?2
           AND reservation_key=?3",
            rusqlite::params![installation, &request.campaign, &request.reservation_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (account, intent, plan) = row.ok_or(StoreError::Invalid(
        "UNIFIED_SETTLEMENT_RESERVATION_NOT_FOUND",
    ))?;
    if plan != request.plan_sha256 {
        return Err(StoreError::Invalid("UNIFIED_SETTLEMENT_PLAN_MISMATCH"));
    }
    let (generation, revision): (i64, i64) = tx.query_row(
        "SELECT generation,policy_revision FROM risk_meta
         WHERE installation_id=?1 AND campaign_id=?2 AND singleton=1",
        rusqlite::params![installation, &request.campaign],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let generation = u64::try_from(generation)
        .map_err(|_| StoreError::Invalid("UNIFIED_SETTLEMENT_GENERATION_INVALID"))?;
    let policy_revision = u64::try_from(revision)
        .map_err(|_| StoreError::Invalid("UNIFIED_SETTLEMENT_POLICY_REVISION_INVALID"))?;
    if generation == 0 || policy_revision == 0 {
        return Err(StoreError::Invalid(
            "UNIFIED_SETTLEMENT_AUTHORITY_REVISION_INVALID",
        ));
    }
    let source = original_bound_entry_source_sql(tx, installation, &account, &intent, request)?;
    let outcome = crate::risk::settle_entry_unified_native(
        tx,
        installation,
        &request.campaign,
        &account,
        &intent,
        request.expected_sequence,
        generation,
        source,
    )
    .map_err(StoreError::Invalid)?;
    match outcome {
        crate::risk::UnifiedRiskDecision::EntrySettled {
            reservation_key,
            book_sequence,
            cumulative_entry_usd,
            ..
        } => Ok(UnifiedFinanceBoundSettlementReceipt {
            reservation_key,
            book_sequence,
            policy_revision,
            cumulative_entry_usd,
        }),
        _ => Err(StoreError::Invalid("UNIFIED_SETTLEMENT_RECEIPT_UNEXPECTED")),
    }
}
