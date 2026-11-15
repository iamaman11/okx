//! Native Status no-send settlement from original SQL under one writer/COMMIT.
use super::{
    UnifiedFinanceBoundUnsent, UnifiedFinanceHandle, UnifiedFinanceWrite,
    UnifiedFinanceWriteReceipt, apply_original_unified_finance_transaction,
    require_verified_live_finance_authority,
};
use crate::{AccountStore, StoreError, StoreResult};
use sha2::{Digest, Sha256};
use std::time::Instant;

impl UnifiedFinanceHandle {
    /// `true` means a local reduce-only reservation; `false` means the
    /// original RiskBook also atomically acknowledged this no-send release.
    /// This operation never calls the venue or manufactures a source proof.
    pub async fn release_unsent_status(
        &self,
        deadline: Instant,
        account: String,
        intent: String,
        plan: String,
    ) -> StoreResult<bool> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_verified_live_finance_authority(&tx, &installation)?;
            let outcome =
                release_original_unsent_status_sql(&tx, &installation, &account, &intent, &plan)?;
            tx.commit().map_err(|_| {
                StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY")
            })?;
            Ok(outcome)
        })
        .await
    }
}

fn release_original_unsent_status_sql(
    tx: &rusqlite::Transaction<'_>,
    installation: &str,
    account: &str,
    intent: &str,
    plan: &str,
) -> StoreResult<bool> {
    AccountStore::verify_source_account_sql(tx, installation, account)?;
    let original =
        AccountStore::unsent_release_receipt_sql(tx, Some(installation), account, intent)?
            .ok_or(StoreError::Invalid("DURABLE_UNSENT_SQL_PROOF_MISSING"))?;
    if original.plan_hash != plan {
        return Err(StoreError::Invalid("UNIFIED_UNSENT_STATUS_PLAN_MISMATCH"));
    }
    if original.reservation_key.starts_with("local:") {
        AccountStore::acknowledge_local_unsent_release_sql(
            tx,
            Some((installation, account)),
            intent,
            account,
            super::opening_ingress::owner_now_ms()?,
        )?;
        return Ok(true);
    }
    let campaign = AccountStore::account_policy_sql(tx, Some((installation, account)))?.campaign_id;
    let sequence: i64 = tx.query_row(
        "SELECT book_sequence FROM risk_meta WHERE installation_id=?1 AND campaign_id=?2 AND singleton=1",
        rusqlite::params![installation,campaign], |r|r.get(0),
    )?;
    let expected_sequence = u64::try_from(sequence)
        .map_err(|_| StoreError::Invalid("UNIFIED_UNSENT_STATUS_SEQUENCE_INVALID"))?;
    let record_digest = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&original)
                .map_err(|_| StoreError::Invalid("UNIFIED_UNSENT_ORIGINAL_DIGEST_INVALID"))?
        )
    );
    let request = UnifiedFinanceBoundUnsent {
        campaign,
        reservation_key: original.reservation_key,
        plan_sha256: original.plan_hash,
        expected_sequence,
        account_key: original.account_key,
        intent_id: original.intent_id,
        request_hash: original.request_hash,
        reason: original.reason,
        record_digest,
    };
    match apply_original_unified_finance_transaction(
        tx,
        installation,
        UnifiedFinanceWrite::ReleaseBoundUnsent { request },
    )? {
        UnifiedFinanceWriteReceipt::BoundUnsentReleased(_) => Ok(false),
        _ => Err(StoreError::Invalid(
            "UNIFIED_UNSENT_STATUS_RECEIPT_UNEXPECTED",
        )),
    }
}
