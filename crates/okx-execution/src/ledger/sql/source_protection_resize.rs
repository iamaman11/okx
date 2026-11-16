//! Original billed partial reduction and stop-resize ownership in one SQL snapshot.
use super::super::*;
use super::source_entry_costs::{entry_read, entry_source_values};
type ProtectionResizeSource = (
    StoredIntent,
    ProtectionReceipt,
    String,
    Option<OperationReceipt>,
    Option<OperationReceipt>,
);
impl AccountStore {
    pub fn protection_resize_source(
        &self,
        id: &str,
        order: &str,
        filled: Decimal,
        venue_at: i64,
        position: Decimal,
    ) -> StoreResult<ProtectionResizeSource> {
        Self::protection_resize_source_sql(
            &self.connection,
            None,
            id,
            order,
            filled,
            venue_at,
            position,
        )
    }
    pub(crate) fn protection_resize_source_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        order: &str,
        filled: Decimal,
        venue_at: i64,
        position_qty: Decimal,
    ) -> StoreResult<ProtectionResizeSource> {
        let reduction = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("LOCAL_REDUCE_INTENT_MISSING"))?;
        if reduction.risk_increase
            || reduction.plan.operation != "reduce"
            || reduction.state != IntentState::Filled
            || position_qty <= Decimal::ZERO
        {
            return Err(StoreError::Invalid(
                "PROTECTION_SHRINK_REDUCTION_UNVERIFIED",
            ));
        }
        let (fee, count, _) =
            Self::prove_local_partial_reduce_sql(db, scope, id, order, filled, venue_at)?;
        if count == 0 || fee > Decimal::ZERO {
            return Err(StoreError::Invalid(
                "PROTECTION_SHRINK_FINANCIAL_PROOF_INCOMPLETE",
            ));
        }
        let lots = Self::active_lots_sql(db, scope, &reduction.instrument_id)?;
        if lots.len() != 1
            || lots[0].remaining_qty != position_qty
            || lots[0].side != reduction.plan.position_side
            || lots[0].instance_id.as_deref() != Some(reduction.plan.strategy_instance.as_str())
        {
            return Err(StoreError::Invalid(
                "PROTECTION_SHRINK_OWNERSHIP_UNVERIFIED",
            ));
        }
        let origin = lots[0].intent_id.as_ref().ok_or(StoreError::Invalid(
            "PROTECTION_SHRINK_ORIGINAL_INTENT_UNATTRIBUTED",
        ))?;
        if origin == id {
            return Err(StoreError::Invalid(
                "PROTECTION_SHRINK_OWNERSHIP_UNVERIFIED",
            ));
        }
        let opening = Self::find_intent_sql(db, scope, "intent_id", origin)?.ok_or(
            StoreError::Invalid("PROTECTION_SHRINK_ORIGINAL_INTENT_MISSING"),
        )?;
        if !opening.risk_increase
            || opening.instrument_id != reduction.instrument_id
            || opening.plan.strategy_instance != reduction.plan.strategy_instance
            || opening.plan.position_side != reduction.plan.position_side
            || opening.incident.is_some()
            || Self::reservation_receipt_sql(db, scope, origin, CentralReceiptView::ActiveExposure)?
                .is_none()
        {
            return Err(StoreError::Invalid(
                "PROTECTION_SHRINK_ORIGINAL_RISK_UNVERIFIED",
            ));
        }
        let old = Self::protection_sql(db, scope, origin)?.ok_or(StoreError::Invalid(
            "PROTECTION_SHRINK_ORIGINAL_STOP_MISSING",
        ))?;
        let covered = Self::entry_settlement_cursor_sql(db, scope, "fills", "SWAP")?.ok_or(
            StoreError::Invalid("PROTECTION_SHRINK_ORIGINAL_FILL_CURSOR_MISSING"),
        )?;
        let original_fill =
            Self::intent_fill_cost_evidence_sql(db, scope, origin, covered.watermark)?;
        let original_bills =
            Self::intent_fee_bill_evidence_sql(db, scope, origin, covered.watermark)?;
        if original_bills.linked_bill_count != original_fill.fill_count
            || original_fill.filled_quantity
                != position_qty
                    .checked_add(filled)
                    .ok_or(StoreError::Invalid("PROTECTION_SHRINK_QUANTITY_OVERFLOW"))?
        {
            return Err(StoreError::Invalid(
                "PROTECTION_SHRINK_GROSS_NET_QUANTITY_UNVERIFIED",
            ));
        }
        let key = format!("protection-shrink:{origin}:{}", exact(position_qty));
        let operation = Self::operation_sql(db, scope, &key)?;
        let verified = Self::operation_sql(db, scope, &format!("{key}:verified"))?;
        if old.quantity != original_fill.filled_quantity
            && (old.quantity != position_qty || verified.is_none())
        {
            return Err(StoreError::Invalid("PROTECTION_SHRINK_STOP_VOLUME_CHANGED"));
        }
        Ok((opening, old, key, operation, verified))
    }
    pub(crate) fn prove_local_partial_reduce_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        order_id: &str,
        venue_qty: Decimal,
        venue_at: i64,
    ) -> StoreResult<(Decimal, u64, String)> {
        use sha2::{Digest, Sha256};
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("LOCAL_REDUCE_INTENT_MISSING"))?;
        if intent.risk_increase
            || intent.plan.operation != "reduce"
            || intent.state != IntentState::Filled
            || intent.exchange_order_id.as_deref() != Some(order_id)
            || venue_qty <= Decimal::ZERO
            || venue_qty != intent.plan.quantity
            || venue_at <= 0
            || !intent.instrument_id.ends_with("-USDT-SWAP")
        {
            return Err(StoreError::Invalid("LOCAL_REDUCE_TERMINAL_PROOF_INVALID"));
        }
        let local = Self::reservation_receipt_sql(db, scope, id, CentralReceiptView::Retained)?
            .ok_or(StoreError::Invalid("LOCAL_REDUCE_RECEIPT_MISSING"))?;
        if local.reservation_key != format!("local:{id}")
            || local.account_key != meta.account_key
            || local.plan_hash != intent.plan_hash
        {
            return Err(StoreError::Invalid("LOCAL_REDUCE_RECEIPT_MISMATCH"));
        }
        for (source, partition) in [("fills", "SWAP"), ("bills", "account")] {
            let cursor = Self::entry_settlement_cursor_sql(db, scope, source, partition)?
                .ok_or(StoreError::Invalid("LOCAL_REDUCE_SOURCE_MISSING"))?;
            if cursor.generation <= 0
                || !cursor.cursor.is_empty()
                || !cursor.coverage_gaps.is_empty()
                || cursor.watermark < venue_at
            {
                return Err(StoreError::Invalid("LOCAL_REDUCE_SOURCE_INCOMPLETE"));
            }
        }
        let mut rows = db.prepare(if scope.is_some() {
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
        let mut page = rows.query(rusqlite::params_from_iter(entry_source_values(
            scope,
            vec![id.to_owned().into(), order_id.to_owned().into()],
        )))?;
        let mut qty = Decimal::ZERO;
        let mut fee = Decimal::ZERO;
        let mut proof_rows = Vec::new();
        while let Some(row) = page.next()? {
            if proof_rows.len() >= 1000 {
                return Err(StoreError::Invalid("LOCAL_REDUCE_FILLS_UNBOUNDED"));
            }
            let fill: String = row.get(0)?;
            let amount = decimal(row.get::<_, String>(1)?)?;
            let charge = decimal(row.get::<_, String>(2)?)?;
            let currency: String = row.get(3)?;
            let trade: String = row.get(4)?;
            let bill: Option<String> = row.get(5)?;
            let direction: Option<String> = row.get(6)?;
            if amount <= Decimal::ZERO
                || direction.as_deref() != Some("CLOSE")
                || trade.is_empty()
                || bill.as_deref().is_none_or(str::is_empty)
            {
                return Err(StoreError::Invalid("LOCAL_REDUCE_FILL_OR_BILL_UNVERIFIED"));
            }
            let bill = bill.unwrap();
            let mut check = db.prepare(if scope.is_some() {
                entry_read!(
                    "SELECT e.amount,e.currency FROM bill_order_sources o
                 JOIN bills b ON b.bill_id=o.bill_id
                 JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                   AND e.component='FEE' AND e.allocation_leg='account'
                 WHERE o.bill_id=?3 AND o.instrument_id=?4
                   AND o.exchange_order_id=?5 AND o.trade_id=?6 LIMIT 2"
                )
            } else {
                "SELECT e.amount,e.currency FROM bill_order_sources o
                 JOIN bills b ON b.bill_id=o.bill_id
                 JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                   AND e.component='FEE' AND e.allocation_leg='account'
                 WHERE o.bill_id=?1 AND o.instrument_id=?2
                   AND o.exchange_order_id=?3 AND o.trade_id=?4 LIMIT 2"
            })?;
            let linked = check
                .query_map(
                    rusqlite::params_from_iter(entry_source_values(
                        scope,
                        vec![
                            bill.to_owned().into(),
                            intent.instrument_id.clone().into(),
                            order_id.to_owned().into(),
                            trade.clone().into(),
                        ],
                    )),
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )?
                .collect::<Result<Vec<_>, _>>()?;
            if linked.len() != 1
                || decimal(linked[0].0.clone())? != charge
                || linked[0].1 != currency
            {
                return Err(StoreError::Invalid("LOCAL_REDUCE_FEE_BILL_UNVERIFIED"));
            }
            qty = qty
                .checked_add(amount)
                .ok_or(StoreError::Invalid("LOCAL_REDUCE_QUANTITY_OVERFLOW"))?;
            fee = fee
                .checked_add(charge)
                .ok_or(StoreError::Invalid("LOCAL_REDUCE_FEE_OVERFLOW"))?;
            proof_rows.push((fill, trade, bill, amount.to_string(), charge.to_string()));
        }
        if proof_rows.is_empty() || qty != venue_qty {
            return Err(StoreError::Invalid("LOCAL_REDUCE_FILL_SUM_MISMATCH"));
        }
        let attributed: i64 = db.query_row(
            if scope.is_some() {
                entry_read!("SELECT COUNT(*) FROM fills WHERE intent_id=?3")
            } else {
                "SELECT COUNT(*) FROM fills WHERE intent_id=?1"
            },
            rusqlite::params_from_iter(entry_source_values(scope, vec![id.to_owned().into()])),
            |r| r.get(0),
        )?;
        if attributed != proof_rows.len() as i64 {
            return Err(StoreError::Invalid(
                "LOCAL_REDUCE_OTHER_ORDER_FILLS_UNVERIFIED",
            ));
        }
        let digest = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "okx.local-partial-reduce/v1",
                    meta.account_key.as_str(),
                    meta.installation_id.as_str(),
                    id,
                    order_id,
                    intent.plan_hash.as_str(),
                    venue_qty.to_string(),
                    &proof_rows,
                ))
                .map_err(|_| StoreError::Invalid("LOCAL_REDUCE_PROOF_SERIALIZATION"))?
            )
        );
        Ok((fee, proof_rows.len() as u64, digest))
    }
}
