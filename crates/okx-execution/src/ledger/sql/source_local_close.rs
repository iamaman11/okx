//! Original local Close fill and separate billed-fee proof, namespace scoped.
use super::super::*;
use super::source_entry_costs::{entry_read, entry_source_values};
use sha2::{Digest, Sha256};
impl AccountStore {
    pub(crate) fn acknowledge_local_terminal_close_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        order_id: &str,
        filled: Decimal,
        venue_at: i64,
        now: i64,
    ) -> StoreResult<()> {
        let (_, _, _, _, proof) =
            Self::local_terminal_close_evidence_sql(tx, scope, id, order_id, filled, venue_at)?;
        let intent = Self::find_intent_sql(tx, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("LOCAL_CLOSE_INTENT_MISSING"))?;
        let local_key = format!("local:{id}");
        let key = format!("local-terminal-release:{id}");
        let prior = Self::operation_sql(tx, scope, &key)?;
        if prior
            .as_ref()
            .is_some_and(|v| v.request_hash != intent.request_hash || v.result_ref != proof)
        {
            return Err(StoreError::Invalid("LOCAL_CLOSE_PROOF_CONFLICT"));
        }
        let changed=match scope {
            None=>tx.execute("UPDATE risk_receipts SET state='RELEASED' WHERE intent_id=?1 AND reservation_key=?2 AND state IN('TERMINAL_PENDING_CENTRAL','RELEASED')",params![id,local_key])?,
            Some((i,a))=>tx.execute("UPDATE risk_receipts SET state='RELEASED' WHERE intent_id=?1 AND reservation_key=?2 AND state IN('TERMINAL_PENDING_CENTRAL','RELEASED') AND installation_id=?3 AND account_key=?4",params![id,local_key,i,a])?,
        };
        if changed != 1 {
            return Err(StoreError::Invalid("LOCAL_CLOSE_STATE_NOT_TERMINAL"));
        }
        match scope {
            None => {
                tx.execute("INSERT INTO operation_receipts VALUES(?1,'LOCAL_TERMINAL_RELEASE',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING",params![key,intent.request_hash,proof,now])?;
            }
            Some((i, a)) => {
                tx.execute("INSERT INTO operation_receipts VALUES(?1,?2,?3,'LOCAL_TERMINAL_RELEASE',?4,?5,?6) ON CONFLICT(installation_id,account_key,operation_id) DO NOTHING",params![i,a,key,intent.request_hash,proof,now])?;
            }
        }
        Ok(())
    }
    pub(crate) fn local_terminal_close_evidence_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        order_id: &str,
        venue_filled: Decimal,
        venue_updated_at: i64,
    ) -> StoreResult<(Decimal, u64, u64, Decimal, String)> {
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("LOCAL_CLOSE_INTENT_MISSING"))?;
        if intent.risk_increase
            || !intent.state.terminal()
            || intent.exchange_order_id.as_deref() != Some(order_id)
            || intent.plan.quantity != venue_filled
            || venue_filled <= Decimal::ZERO
            || venue_updated_at <= 0
            || !intent.instrument_id.ends_with("-USDT-SWAP")
        {
            return Err(StoreError::Invalid("LOCAL_CLOSE_TERMINAL_PROOF_INVALID"));
        }
        let receipt = Self::reservation_receipt_sql(db, scope, id, CentralReceiptView::Retained)?
            .ok_or(StoreError::Invalid("LOCAL_CLOSE_RECEIPT_MISSING"))?;
        let local_key = format!("local:{id}");
        if receipt.reservation_key != local_key
            || receipt.account_key != meta.account_key
            || receipt.plan_hash != intent.plan_hash
        {
            return Err(StoreError::Invalid("LOCAL_CLOSE_RECEIPT_MISMATCH"));
        }
        for (source, partition) in [("fills", "SWAP"), ("bills", "account")] {
            let cursor = Self::entry_settlement_cursor_sql(db, scope, source, partition)?
                .ok_or(StoreError::Invalid("LOCAL_CLOSE_SOURCE_MISSING"))?;
            if cursor.generation <= 0
                || !cursor.cursor.is_empty()
                || !cursor.coverage_gaps.is_empty()
                || cursor.watermark < venue_updated_at
            {
                return Err(StoreError::Invalid("LOCAL_CLOSE_SOURCE_INCOMPLETE"));
            }
        }
        // Read each attributed close fill (including unmatched records) so
        // an INNER JOIN cannot silently conceal an unpaid or unlinked fee.
        let mut total = Decimal::ZERO;
        let mut proof_rows = Vec::new();
        {
            let mut q = db.prepare(if scope.is_some() {
                entry_read!(
                    "SELECT f.fill_id,f.qty,f.fee,f.currency,f.trade_id,
                        s.bill_id,s.direction
                 FROM fills f LEFT JOIN swap_fill_sources s ON s.fill_id=f.fill_id
                 WHERE f.intent_id=?3 AND f.exchange_order_id=?4
                 ORDER BY f.fill_id LIMIT 1001"
                )
            } else {
                "SELECT f.fill_id,f.qty,f.fee,f.currency,f.trade_id,
                        s.bill_id,s.direction
                 FROM fills f LEFT JOIN swap_fill_sources s ON s.fill_id=f.fill_id
                 WHERE f.intent_id=?1 AND f.exchange_order_id=?2
                 ORDER BY f.fill_id LIMIT 1001"
            })?;
            let mut rows = q.query(rusqlite::params_from_iter(entry_source_values(
                scope,
                vec![id.to_owned().into(), order_id.to_owned().into()],
            )))?;
            while let Some(row) = rows.next()? {
                if proof_rows.len() >= 1000 {
                    return Err(StoreError::Invalid("LOCAL_CLOSE_FILLS_UNBOUNDED"));
                }
                let fill_id: String = row.get(0)?;
                let qty = decimal(row.get::<_, String>(1)?)?;
                let fee = decimal(row.get::<_, String>(2)?)?;
                let currency: String = row.get(3)?;
                let trade: String = row.get(4)?;
                let bill: Option<String> = row.get(5)?;
                let direction: Option<String> = row.get(6)?;
                if qty <= Decimal::ZERO
                    || direction.as_deref() != Some("CLOSE")
                    || bill.as_deref().is_none_or(str::is_empty)
                    || trade.is_empty()
                {
                    return Err(StoreError::Invalid("LOCAL_CLOSE_FILL_OR_BILL_UNVERIFIED"));
                }
                let bill = bill.unwrap();
                // Verify the separate bill's exact trade/order provenance and
                // canonical fee entry; two candidate bills never count as one.
                let mut evidence = db.prepare(if scope.is_some() {
                    entry_read!(
                        "SELECT e.amount,e.currency FROM bill_order_sources o
                     JOIN bills b ON b.bill_id=o.bill_id
                     JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                     WHERE o.bill_id=?3 AND o.instrument_id=?4
                       AND o.exchange_order_id=?5 AND o.trade_id=?6
                     LIMIT 2"
                    )
                } else {
                    "SELECT e.amount,e.currency FROM bill_order_sources o
                     JOIN bills b ON b.bill_id=o.bill_id
                     JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                       AND e.component='FEE' AND e.allocation_leg='account'
                     WHERE o.bill_id=?1 AND o.instrument_id=?2
                       AND o.exchange_order_id=?3 AND o.trade_id=?4
                     LIMIT 2"
                })?;
                let matched = evidence
                    .query_map(
                        rusqlite::params_from_iter(entry_source_values(
                            scope,
                            vec![
                                bill.clone().into(),
                                intent.instrument_id.clone().into(),
                                order_id.to_owned().into(),
                                trade.clone().into(),
                            ],
                        )),
                        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                    )?
                    .collect::<Result<Vec<_>, _>>()?;
                if matched.len() != 1
                    || decimal(matched[0].0.clone())? != fee
                    || matched[0].1 != currency
                {
                    return Err(StoreError::Invalid("LOCAL_CLOSE_FEE_BILL_UNVERIFIED"));
                }
                total = total
                    .checked_add(qty)
                    .ok_or(StoreError::Invalid("LOCAL_CLOSE_QUANTITY_OVERFLOW"))?;
                proof_rows.push((
                    fill_id,
                    trade,
                    bill,
                    qty.to_string(),
                    fee.to_string(),
                    currency,
                ));
            }
        }
        if proof_rows.is_empty() || total != venue_filled {
            return Err(StoreError::Invalid("LOCAL_CLOSE_FILL_SUM_MISMATCH"));
        }
        let fill_count = proof_rows.len() as u64;
        let mut fee_usdt = Decimal::ZERO;
        let mut linked_bills = std::collections::BTreeSet::new();
        for row in &proof_rows {
            fee_usdt = fee_usdt
                .checked_add(decimal(row.4.clone())?)
                .ok_or(StoreError::Invalid("LOCAL_CLOSE_FEE_OVERFLOW"))?;
            if !linked_bills.insert(row.2.clone()) {
                return Err(StoreError::Invalid("LOCAL_CLOSE_DUPLICATE_BILL"));
            }
        }
        let bill_count = linked_bills.len() as u64;
        let proof = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "okx.local-terminal-close/v1",
                    meta.account_key.as_str(),
                    meta.installation_id.as_str(),
                    id,
                    order_id,
                    intent.plan_hash.as_str(),
                    venue_filled.to_string(),
                    proof_rows
                ))
                .map_err(|_| StoreError::Invalid("LOCAL_CLOSE_PROOF_SERIALIZATION"))?
            )
        );
        Ok((total, fill_count, bill_count, fee_usdt, proof))
    }
}
