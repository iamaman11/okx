//! Original admission writes in either legacy or installation-scoped SQL.
use super::super::*;
impl AccountStore {
    fn admission_begin_post_at_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<Option<i64>> {
        Ok(match scope {
            Some((i,a))=>db.query_row("SELECT begin_post_at FROM intents WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3",params![id,i,a],|r|r.get(0))?,
            None=>db.query_row("SELECT begin_post_at FROM intents WHERE intent_id=?1",[id],|r|r.get(0))?,
        })
    }
    pub(crate) fn bind_entry_quote_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        quote: &EntryQuoteEvidence,
    ) -> StoreResult<()> {
        let intent = Self::verified_stored_entry_quote_sql(db, scope, quote)?;
        if intent.state != IntentState::Prepared {
            return Err(StoreError::Invalid("ENTRY_QUOTE_INTENT_STATE_INVALID"));
        }
        let name = format!("entry-quote:{}", intent.intent_id);
        let encoded = serde_json::to_string(quote)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_SERIALIZATION_FAILED"))?;
        if let Some(prior) = Self::operation_sql(db, scope, &name)? {
            if prior.kind != "ENTRY_QUOTE_UNPROVEN"
                || prior.request_hash != intent.plan_hash
                || prior.result_ref != encoded
            {
                return Err(StoreError::Invalid("ENTRY_QUOTE_IDEMPOTENCY_CONFLICT"));
            }
            return Ok(());
        }
        match scope {
            Some((i, a)) => {
                db.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,'ENTRY_QUOTE_UNPROVEN',?4,?5,?6)",params![i,a,name,intent.plan_hash,encoded,quote.observed_at])?;
            }
            None => {
                db.execute("INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,'ENTRY_QUOTE_UNPROVEN',?2,?3,?4)",params![name,intent.plan_hash,encoded,quote.observed_at])?;
            }
        }
        Ok(())
    }
    pub(crate) fn bind_entry_admission_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        proof: &EntryAdmissionEvidence,
    ) -> StoreResult<()> {
        Self::verified_entry_admission_sql(db, scope, proof)?;
        let key = format!("entry-admission:{}", proof.intent_id);
        let body = serde_json::to_string(proof)
            .map_err(|_| StoreError::Invalid("ENTRY_ADMISSION_SERIALIZATION_FAILED"))?;
        if let Some(prior) = Self::operation_sql(db, scope, &key)? {
            if prior.kind != "ENTRY_ADMISSION_PROVEN"
                || prior.request_hash != proof.plan_hash
                || prior.result_ref != body
                || prior.committed_at != proof.observed_at
            {
                return Err(StoreError::Invalid("ENTRY_ADMISSION_IDEMPOTENCY_CONFLICT"));
            }
            return Ok(());
        }
        let intent = Self::find_intent_sql(db, scope, "intent_id", &proof.intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_INTENT_MISSING"))?;
        if intent.state != IntentState::Prepared {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_INTENT_STATE_INVALID"));
        }
        match scope {
            Some((i, a)) => {
                db.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,'ENTRY_ADMISSION_PROVEN',?4,?5,?6)",params![i,a,key,proof.plan_hash,body,proof.observed_at])?;
            }
            None => {
                db.execute("INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,'ENTRY_ADMISSION_PROVEN',?2,?3,?4)",params![key,proof.plan_hash,body,proof.observed_at])?;
            }
        }
        Ok(())
    }
    pub(crate) fn mark_reserve_attempt_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        key: &str,
        now: i64,
    ) -> StoreResult<()> {
        if key.is_empty() || key.len() > 512 {
            return Err(StoreError::Invalid("INVALID_RESERVE_IDENTITY"));
        }
        let intent = Self::find_intent_sql(tx, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        if intent.state != IntentState::Prepared
            || Self::admission_begin_post_at_sql(tx, scope, id)?.is_some()
        {
            return Err(StoreError::Invalid("INVALID_RESERVE_ATTEMPT_STATE"));
        }
        let name = format!("risk-reserve:{id}");
        if let Some(previous) = Self::operation_sql(tx, scope, &name)? {
            if previous.kind != "RISK_RESERVE_ATTEMPT"
                || previous.request_hash != intent.request_hash
                || previous.result_ref != key
            {
                return Err(StoreError::Invalid("RESERVE_ATTEMPT_IDEMPOTENCY_CONFLICT"));
            }
        } else {
            match scope {
                Some((i, a)) => {
                    tx.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,'RISK_RESERVE_ATTEMPT',?4,?5,?6)",params![i,a,name,intent.request_hash,key,now])?;
                }
                None => {
                    tx.execute(
                        "INSERT INTO operation_receipts VALUES(?1,'RISK_RESERVE_ATTEMPT',?2,?3,?4)",
                        params![name, intent.request_hash, key, now],
                    )?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn reject_unsent_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        reason: &str,
        now: i64,
        proven_not_reserved: bool,
    ) -> StoreResult<bool> {
        if reason.is_empty() {
            return Err(StoreError::Invalid("UNSENT_REASON_REQUIRED"));
        }
        let intent = Self::find_intent_sql(tx, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        if Self::admission_begin_post_at_sql(tx, scope, id)?.is_some() {
            return Err(StoreError::Invalid("POST_OUTCOME_REQUIRES_RECONCILIATION"));
        }
        let first = intent.state != IntentState::Rejected;
        if first {
            if !matches!(intent.state, IntentState::Prepared | IntentState::Reserved) {
                return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
            }
            match scope {
                Some((i, a)) => {
                    tx.execute("UPDATE intents SET state='REJECTED',incident=?2,updated_at=?3,revision=revision+1 WHERE intent_id=?1 AND installation_id=?4 AND account_key=?5",params![id,reason,now,i,a])?;
                    tx.execute("UPDATE risk_receipts SET state='UNSENT_PENDING_CENTRAL' WHERE intent_id=?1 AND installation_id=?2 AND account_key=?3",params![id,i,a])?;
                    tx.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,'REJECTED_UNSENT',?4,?5,?6)",params![i,a,format!("terminal:{id}"),intent.request_hash,reason,now])?;
                }
                None => {
                    tx.execute("UPDATE intents SET state='REJECTED',incident=?2,updated_at=?3,revision=revision+1 WHERE intent_id=?1",params![id,reason,now])?;
                    tx.execute("UPDATE risk_receipts SET state='UNSENT_PENDING_CENTRAL' WHERE intent_id=?1",[id])?;
                    tx.execute(
                        "INSERT INTO operation_receipts VALUES(?1,'REJECTED_UNSENT',?2,?3,?4)",
                        params![format!("terminal:{id}"), intent.request_hash, reason, now],
                    )?;
                }
            }
        }
        if proven_not_reserved {
            let original = Self::operation_sql(tx, scope, &format!("terminal:{id}"))?;
            if original.as_ref().is_none_or(|v| {
                v.kind != "REJECTED_UNSENT" || v.request_hash != intent.request_hash
            }) {
                return Err(StoreError::Invalid("UNSENT_TERMINAL_RECEIPT_MISSING"));
            }
            let operation = format!("risk-reserve:{id}");
            let changed=match scope {
                Some((i,a))=>tx.execute("UPDATE operation_receipts SET kind='RISK_RESERVE_DENIED' WHERE operation_id=?1 AND kind IN('RISK_RESERVE_ATTEMPT','RISK_RESERVE_DENIED') AND installation_id=?2 AND account_key=?3",params![operation,i,a])?,
                None=>tx.execute("UPDATE operation_receipts SET kind='RISK_RESERVE_DENIED' WHERE operation_id=?1 AND kind IN('RISK_RESERVE_ATTEMPT','RISK_RESERVE_DENIED')",[operation])?,
            };
            if changed != 1 {
                return Err(StoreError::Invalid("RESERVE_ATTEMPT_MARKER_MISSING"));
            }
        }
        Ok(first)
    }
    pub(crate) fn unsent_before_reserve_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<bool> {
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        let legacy = "SELECT EXISTS(SELECT 1 FROM intents i JOIN operation_receipts t ON t.operation_id='terminal:'||i.intent_id WHERE i.intent_id=?1 AND i.state='REJECTED' AND i.begin_post_at IS NULL AND i.exchange_order_id IS NULL AND t.kind='REJECTED_UNSENT' AND t.request_hash=i.request_hash AND t.result_ref=i.incident AND NOT EXISTS(SELECT 1 FROM risk_receipts r WHERE r.intent_id=i.intent_id) AND NOT EXISTS(SELECT 1 FROM operation_receipts a WHERE a.operation_id='risk-reserve:'||i.intent_id))";
        let scoped = "SELECT EXISTS(SELECT 1 FROM intents i JOIN operation_receipts t ON t.operation_id='terminal:'||i.intent_id AND t.installation_id=i.installation_id AND t.account_key=i.account_key WHERE i.intent_id=?1 AND i.installation_id=?2 AND i.account_key=?3 AND i.state='REJECTED' AND i.begin_post_at IS NULL AND i.exchange_order_id IS NULL AND t.kind='REJECTED_UNSENT' AND t.request_hash=i.request_hash AND t.result_ref=i.incident AND NOT EXISTS(SELECT 1 FROM risk_receipts r WHERE r.intent_id=i.intent_id AND r.installation_id=i.installation_id AND r.account_key=i.account_key) AND NOT EXISTS(SELECT 1 FROM operation_receipts a WHERE a.operation_id='risk-reserve:'||i.intent_id AND a.installation_id=i.installation_id AND a.account_key=i.account_key))";
        Ok(match scope {
            Some((i, a)) => db.query_row(scoped, params![id, i, a], |r| r.get(0))?,
            None => db.query_row(legacy, [id], |r| r.get(0))?,
        })
    }
}
