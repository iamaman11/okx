//! Original no-send release acknowledgement in the existing caller transaction.
use super::super::*;

impl AccountStore {
    pub(crate) fn acknowledge_central_release_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        key: &str,
        evidence: &str,
        now: i64,
    ) -> StoreResult<()> {
        if evidence.is_empty() {
            return Err(StoreError::Invalid("CENTRAL_RELEASE_EVIDENCE_REQUIRED"));
        }
        let hash: String = match scope {
            None => tx.query_row(
                "SELECT receipt_hash FROM risk_receipts WHERE reservation_key=?1
                 UNION ALL SELECT request_hash FROM operation_receipts
                 WHERE kind='RISK_RESERVE_ATTEMPT' AND result_ref=?1 LIMIT 1",
                [key],
                |r| r.get(0),
            )?,
            Some((i, a)) => {
                Self::verify_source_account_sql(tx, i, a)?;
                tx.query_row(
                    "SELECT receipt_hash FROM risk_receipts
                     WHERE reservation_key=?1 AND installation_id=?2 AND account_key=?3
                     UNION ALL SELECT request_hash FROM operation_receipts
                     WHERE kind='RISK_RESERVE_ATTEMPT' AND result_ref=?1
                       AND installation_id=?2 AND account_key=?3 LIMIT 1",
                    params![key, i, a],
                    |r| r.get(0),
                )?
            }
        };
        match scope {
            None => {
                tx.execute(
                    "UPDATE risk_receipts SET state='RELEASED' WHERE reservation_key=?1",
                    [key],
                )?;
                tx.execute("INSERT INTO operation_receipts VALUES(?1,'CENTRAL_RELEASE',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING", params![format!("central-release:{key}"),hash,evidence,now])?;
            }
            Some((i, a)) => {
                tx.execute("UPDATE risk_receipts SET state='RELEASED' WHERE reservation_key=?1 AND installation_id=?2 AND account_key=?3", params![key,i,a])?;
                tx.execute("INSERT INTO operation_receipts VALUES(?1,?2,?3,'CENTRAL_RELEASE',?4,?5,?6) ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING", params![i,a,format!("central-release:{key}"),hash,evidence,now])?;
                let saved = Self::operation_sql(tx, scope, &format!("central-release:{key}"))?
                    .ok_or(StoreError::Invalid("CENTRAL_RELEASE_ACK_MISSING"))?;
                if saved.kind != "CENTRAL_RELEASE" || saved.request_hash != hash {
                    return Err(StoreError::Invalid("CENTRAL_RELEASE_ACK_CONFLICT"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn acknowledge_local_unsent_release_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        account: &str,
        now: i64,
    ) -> StoreResult<()> {
        use sha2::{Digest, Sha256};
        let proof = Self::unsent_release_receipt_sql(tx, scope.map(|s| s.0), account, id)?
            .ok_or(StoreError::Invalid("LOCAL_UNSENT_SQL_PROOF_REQUIRED"))?;
        let intent = Self::find_intent_sql(tx, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("LOCAL_UNSENT_INTENT_MISSING"))?;
        let local_key = format!("local:{id}");
        let receipt = Self::reservation_receipt_sql(tx, scope, id, CentralReceiptView::Retained)?
            .ok_or(StoreError::Invalid("LOCAL_UNSENT_RECEIPT_MISSING"))?;
        if intent.risk_increase
            || intent.state != IntentState::Rejected
            || intent.exchange_order_id.is_some()
            || proof.reservation_key != local_key
            || proof.intent_id != id
            || proof.account_key != account
            || proof.plan_hash != intent.plan_hash
            || proof.request_hash != intent.request_hash
            || receipt.reservation_key != local_key
            || receipt.account_key != account
            || receipt.plan_hash != intent.plan_hash
            || Self::operation_sql(tx, scope, &format!("risk-reserve:{id}"))?.is_some()
        {
            return Err(StoreError::Invalid("LOCAL_UNSENT_RELEASE_PROOF_MISMATCH"));
        }
        let bytes = serde_json::to_vec(&proof)
            .map_err(|_| StoreError::Invalid("LOCAL_UNSENT_PROOF_SERIALIZATION_FAILED"))?;
        let evidence = format!("local-unsent-sql:{:x}", Sha256::digest(bytes));
        Self::acknowledge_central_release_sql(tx, scope, &local_key, &evidence, now)
    }
}
