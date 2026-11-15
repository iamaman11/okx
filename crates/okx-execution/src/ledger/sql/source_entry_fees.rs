//! Original entry fee ownership proof and immutable receipts in one transaction.
use super::super::*;
impl AccountStore {
    pub(crate) fn record_entry_fee_links_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        links: &[EntryOrderFeeLink],
    ) -> StoreResult<()> {
        use rusqlite::{params_from_iter, types::Value};
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(tx, installation, account)?;
        }
        let values = |mut original: Vec<Value>| {
            if let Some((installation, account)) = scope {
                original.push(installation.to_owned().into());
                original.push(account.to_owned().into());
            }
            original
        };
        // The authenticated bill/order link belongs to THIS ingestion
        // transaction. A source cursor cannot advance without identical
        // immutable bill, finance event, venue order and account owner.
        if !links.is_empty() {
            tx.execute_batch(
                "CREATE TRIGGER IF NOT EXISTS immutable_entry_fee_owner_update
                 BEFORE UPDATE ON operation_receipts
                 WHEN OLD.kind IN ('ENTRY_ORDER_FEE_PROVEN','ENTRY_ORDER_FEE_INDEX')
                 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_FEE_OWNER'); END;
                 CREATE TRIGGER IF NOT EXISTS immutable_entry_fee_owner_delete
                 BEFORE DELETE ON operation_receipts
                 WHEN OLD.kind IN ('ENTRY_ORDER_FEE_PROVEN','ENTRY_ORDER_FEE_INDEX')
                 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_ENTRY_FEE_OWNER'); END;",
            )?;
        }
        for link in links {
            if link.bill_id.is_empty()
                || link.intent_id.is_empty()
                || link.exchange_order_id.is_empty()
                || !link.instrument_id.ends_with("-USDT-SWAP")
                || link.fee_usdt >= Decimal::ZERO
                || link.bill_at <= 0
            {
                return Err(StoreError::Invalid("ENTRY_BILL_LINK_INVALID"));
            }
            let owner: Option<(String, String, i64)> = tx
                .query_row(
                    match scope {
                        None => {
                            "SELECT i.instrument_id,i.plan_hash,i.begin_post_at FROM intents i
                 WHERE i.intent_id=?1 AND i.exchange_order_id=?2
                   AND i.risk_increase=1"
                        }
                        Some(_) => {
                            "SELECT i.instrument_id,i.plan_hash,i.begin_post_at FROM intents i
                 WHERE i.intent_id=?1 AND i.exchange_order_id=?2
                   AND i.risk_increase=1 AND i.installation_id=?3 AND i.account_key=?4"
                        }
                    },
                    params_from_iter(values(vec![
                        link.intent_id.clone().into(),
                        link.exchange_order_id.clone().into(),
                    ])),
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let (instrument, plan_hash, posted_at) =
                owner.ok_or(StoreError::Invalid("ENTRY_BILL_ORDER_UNVERIFIED"))?;
            if instrument != link.instrument_id || posted_at <= 0 || posted_at > link.bill_at {
                return Err(StoreError::Invalid("ENTRY_BILL_ORDER_UNVERIFIED"));
            }
            let key = format!("bill:{}", link.bill_id);
            let canonical: Option<(i64, String, String, String)> = tx
                .query_row(
                    match scope {None=>"SELECT b.time,b.currency,e.amount,e.entry_id FROM bills b
                 JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                  AND e.component='FEE' AND e.allocation_leg='account'
                 JOIN finance_source_links l ON l.entry_id=e.entry_id
                  AND l.source_ref=?2
                 WHERE b.bill_id=?1 AND b.economic_event_key=?2",Some(_)=>"SELECT b.time,b.currency,e.amount,e.entry_id FROM bills b
                 JOIN finance_entries e ON e.economic_event_key=b.economic_event_key AND e.installation_id=b.installation_id AND e.account_key=b.account_key
                  AND e.component='FEE' AND e.allocation_leg='account'
                 JOIN finance_source_links l ON l.entry_id=e.entry_id AND l.installation_id=e.installation_id AND l.account_key=e.account_key
                  AND l.source_ref=?2
                 WHERE b.bill_id=?1 AND b.economic_event_key=?2 AND b.installation_id=?3 AND b.account_key=?4"}, params_from_iter(values(vec![link.bill_id.clone().into(),key.clone().into()])),
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()?;
            let (bill_at, ccy, amount, entry_id) =
                canonical.ok_or(StoreError::Invalid("ENTRY_BILL_FINANCE_PROVENANCE_MISSING"))?;
            if bill_at != link.bill_at || ccy != "USDT" || decimal(amount)? != link.fee_usdt {
                return Err(StoreError::Invalid(
                    "ENTRY_BILL_AMOUNT_OR_CURRENCY_MISMATCH",
                ));
            }
            let operation_id = format!("entry-bill:{}:{}", link.intent_id, link.bill_id);
            let encoded = serde_json::to_string(link)
                .map_err(|_| StoreError::Invalid("ENTRY_BILL_LINK_SERIALIZATION_FAILED"))?;
            let first_seen = tx.execute(
                match scope {None=>"INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,'ENTRY_ORDER_FEE_PROVEN',?2,?3,?4)
                 ON CONFLICT(operation_id) DO NOTHING",Some(_)=>"INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at,installation_id,account_key)
                 VALUES(?1,'ENTRY_ORDER_FEE_PROVEN',?2,?3,?4,?5,?6)
                 ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING"}, params_from_iter(values(vec![operation_id.clone().into(),plan_hash.clone().into(),encoded.clone().into(),link.bill_at.clone().into()])),
            )?;
            let old: (String, String, String, i64) = tx.query_row(
                match scope {None=>"SELECT kind,request_hash,result_ref,committed_at
                 FROM operation_receipts WHERE operation_id=?1",Some(_)=>"SELECT kind,request_hash,result_ref,committed_at
                 FROM operation_receipts WHERE operation_id=?1 AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![operation_id.clone().into()])),
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            if old
                != (
                    "ENTRY_ORDER_FEE_PROVEN".into(),
                    plan_hash.clone(),
                    encoded.clone(),
                    link.bill_at,
                )
            {
                return Err(StoreError::Invalid("ENTRY_BILL_LINK_IMMUTABLE_CONFLICT"));
            }
            // One immutable bill-id read index for historical continuity.
            // It points to the same source bytes, not another financial
            // debit or owner, and avoids a million-intent correlated scan.
            let bill_index = format!("entry-bill-owner:{}", link.bill_id);
            tx.execute(
                match scope {None=>"INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,'ENTRY_ORDER_FEE_INDEX',?2,?3,?4)
                 ON CONFLICT(operation_id) DO NOTHING",Some(_)=>"INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at,installation_id,account_key)
                 VALUES(?1,'ENTRY_ORDER_FEE_INDEX',?2,?3,?4,?5,?6)
                 ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING"}, params_from_iter(values(vec![bill_index.clone().into(),plan_hash.clone().into(),encoded.clone().into(),link.bill_at.clone().into()])),
            )?;
            let indexed: (String, String, String, i64) = tx.query_row(
                match scope {None=>"SELECT kind,request_hash,result_ref,committed_at
                 FROM operation_receipts WHERE operation_id=?1",Some(_)=>"SELECT kind,request_hash,result_ref,committed_at
                 FROM operation_receipts WHERE operation_id=?1 AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![bill_index.clone().into()])),
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            if indexed
                != (
                    "ENTRY_ORDER_FEE_INDEX".into(),
                    plan_hash,
                    encoded,
                    link.bill_at,
                )
            {
                return Err(StoreError::Invalid("ENTRY_BILL_INDEX_CONFLICT"));
            }
            // A newly observed independent fee after terminal RELEASE
            // rejoins the SAME bounded maintenance scan for central debit.
            // Never quietly mark the account settled at an older amount.
            if first_seen == 1 {
                let original_fee: bool = tx.query_row(
                    match scope {None=>"SELECT EXISTS(
                        SELECT 1 FROM finance_source_links src
                        JOIN fills f ON src.source_ref='fill:'||f.fill_id
                        WHERE src.entry_id=?1 AND f.intent_id=?2)",Some(_)=>"SELECT EXISTS(
                        SELECT 1 FROM finance_source_links src
                        JOIN fills f ON src.source_ref='fill:'||f.fill_id AND f.installation_id=src.installation_id AND f.account_key=src.account_key
                        WHERE src.entry_id=?1 AND f.intent_id=?2 AND src.installation_id=?3 AND src.account_key=?4)"}, params_from_iter(values(vec![entry_id.clone().into(),link.intent_id.clone().into()])),
                    |r| r.get(0),
                )?;
                if !original_fee {
                    tx.execute(
                        match scope {None=>"UPDATE risk_receipts SET state='LATE_SETTLEMENT_PENDING'
                         WHERE intent_id=?1 AND state='RELEASED'",Some(_)=>"UPDATE risk_receipts SET state='LATE_SETTLEMENT_PENDING'
                         WHERE intent_id=?1 AND state='RELEASED' AND installation_id=?2 AND account_key=?3"}, params_from_iter(values(vec![link.intent_id.clone().into()])),
                    )?;
                }
            }
        }
        Ok(())
    }
}
