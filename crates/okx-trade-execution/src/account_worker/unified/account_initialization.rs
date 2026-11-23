//! Verified flat account sources seed the SAME installation SQL writer.
//! Identity authentication belongs to the Agent producer; this operation grants no POST.
use super::UnifiedFinanceHandle;
use crate::{AccountMeta, AccountStore, OpeningSnapshot, StoreError, StoreResult};
use rusqlite::{TransactionBehavior, params};
use std::time::Instant;

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn account_key(value: &str) -> bool {
    let parts = value.split(':').collect::<Vec<_>>();
    parts.len() == 3 && parts[0] == "okx" && matches!(parts[1], "real" | "demo") && digest(parts[2])
}

impl UnifiedFinanceHandle {
    /// Original immutable metadata for an authenticated reconnect/retry. The
    /// caller also compares ACCOUNT_CONNECTED's pinned configuration digest.
    pub async fn account_metadata(
        &self,
        deadline: Instant,
        account: String,
    ) -> StoreResult<Option<AccountMeta>> {
        let installation = self.installation.clone();
        if !account_key(&account) {
            return Err(StoreError::Invalid("ACCOUNT_IDENTITY_MISMATCH"));
        }
        self.submit_before(Some(deadline), move |db| {
            AccountStore::account_metadata_sql(db, &installation, &account)
        })
        .await
    }

    /// Exact replay means the same original source and connect configuration.
    /// A new observation after lost reply must inspect original metadata rather
    /// than overwrite the account opening or reset its current financial rows.
    pub async fn initialize_account(
        &self,
        deadline: Instant,
        meta: AccountMeta,
        opening: OpeningSnapshot,
        configuration_digest: String,
    ) -> StoreResult<AccountMeta> {
        if meta.installation_id != self.installation || !account_key(&meta.account_key) {
            return Err(StoreError::Invalid("ACCOUNT_IDENTITY_MISMATCH"));
        }
        if !digest(&configuration_digest)
            || !digest(&opening.source_ref)
            || opening.opening_equity_usd < rust_decimal::Decimal::ZERO
        {
            return Err(StoreError::Invalid(
                "NATIVE_ACCOUNT_INITIALIZATION_SOURCE_INVALID",
            ));
        }
        AccountStore::validate_account_opening(&meta, &opening)?;
        let installation = self.installation.clone();
        self.submit_before(Some(deadline),move |db|{
            // This is installation validity, not risk authority or a substitute
            // for source authentication. No campaign/balance/grant is inferred.
            AccountStore::inspect_finance_installation(db,&installation)?;
            let tx=db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let scope=Some((installation.as_str(),meta.account_key.as_str()));
            let connection_id=format!("account-connect:{}",meta.initialize_operation_id);
            let expected_source=AccountStore::initialization_request_hash(scope,&meta,&opening)?;
            if let Some(existing)=AccountStore::account_metadata_sql(&tx,&installation,&meta.account_key)? {
                let source=AccountStore::operation_sql(&tx,scope,&meta.initialize_operation_id)?;
                let connection=AccountStore::operation_sql(&tx,scope,&connection_id)?;
                if existing!=meta || !matches!(source,Some(ref r) if r.kind=="INITIALIZE"
                    && r.request_hash==expected_source && r.result_ref==meta.opening_source && r.committed_at==meta.initialized_at)
                    || !matches!(connection,Some(ref r) if r.kind=="ACCOUNT_CONNECTED" && r.request_hash==configuration_digest
                        && r.result_ref==meta.account_key && r.committed_at==meta.initialized_at) {
                    return Err(StoreError::Invalid("INITIALIZE_OPERATION_CONFLICT"));
                }
                tx.commit()?;
                return Ok(existing);
            }
            let count:i64=tx.query_row("SELECT COUNT(*) FROM account_meta",[],|r|r.get(0))?;
            if count>=32 {return Err(StoreError::Invalid("ACCOUNT_COHORT_LIMIT"));}
            AccountStore::initialize_account_rows_sql(&tx,scope,&meta,&opening)?;
            AccountStore::ensure_manual_instance_sql(&tx,scope,meta.initialized_at)?;
            tx.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                VALUES(?1,?2,?3,'ACCOUNT_CONNECTED',?4,?2,?5)",
                params![installation,meta.account_key,connection_id,configuration_digest,meta.initialized_at])?;
            tx.commit().map_err(|_|StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?;
            Ok(meta)
        }).await
    }
}

#[cfg(test)]
mod tests;
