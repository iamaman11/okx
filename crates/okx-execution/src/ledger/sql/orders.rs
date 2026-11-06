use super::super::*;
impl AccountStore {
    pub fn verify_protection(&mut self, p: &ProtectionReceipt) -> StoreResult<()> {
        let intent = self
            .intent(&p.intent_id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        let required_side = if intent.plan.side == "buy" {
            "sell"
        } else {
            "buy"
        };
        if p.algo_id.is_empty()
            || p.side != required_side
            || p.quantity <= Decimal::ZERO
            || p.quantity > intent.plan.quantity
            || Some(p.trigger_price) != intent.plan.stop_price
            || p.trigger_basis != "mark"
            || p.source_digest.is_empty()
        {
            return Err(StoreError::Invalid("PROTECTION_VERIFICATION_FAILED"));
        }
        self.connection.execute("INSERT INTO protection_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(intent_id) DO UPDATE SET algo_id=excluded.algo_id,side=excluded.side,quantity=excluded.quantity,trigger_price=excluded.trigger_price,trigger_basis=excluded.trigger_basis,verified_at=excluded.verified_at,source_digest=excluded.source_digest",params![p.intent_id,p.algo_id,p.side,exact(p.quantity),exact(p.trigger_price),p.trigger_basis,p.verified_at,p.source_digest])?;
        Ok(())
    }

    /// Read-only proof of a signed, completely billed partial reducing IOC.
    /// No account-wide position assumption, reservation release, or test
    /// money mutation. The terminal venue quantity is compared to EVERY
    /// attributed finance row, with a separate exact signed bill per fill.
    /// A partial reduction must stay locally outstanding until final flat.
    pub fn prove_local_partial_reduce(
        &self,
        id: &str,
        order_id: &str,
        venue_qty: Decimal,
        venue_at: i64,
    ) -> StoreResult<(Decimal, u64, String)> {
        use sha2::{Digest, Sha256};
        let intent = self
            .intent(id)?
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
        let local = self
            .central_receipt(id)?
            .ok_or(StoreError::Invalid("LOCAL_REDUCE_RECEIPT_MISSING"))?;
        if local.reservation_key != format!("local:{id}")
            || local.account_key != self.meta.account_key
            || local.plan_hash != intent.plan_hash
        {
            return Err(StoreError::Invalid("LOCAL_REDUCE_RECEIPT_MISMATCH"));
        }
        for (source, partition) in [("fills", "SWAP"), ("bills", "account")] {
            let cursor = self
                .cursor(source, partition)?
                .ok_or(StoreError::Invalid("LOCAL_REDUCE_SOURCE_MISSING"))?;
            if cursor.generation <= 0
                || !cursor.cursor.is_empty()
                || !cursor.coverage_gaps.is_empty()
                || cursor.watermark < venue_at
            {
                return Err(StoreError::Invalid("LOCAL_REDUCE_SOURCE_INCOMPLETE"));
            }
        }
        let mut rows = self.connection.prepare(
            "SELECT f.fill_id,f.qty,f.fee,f.currency,f.trade_id,
                    s.bill_id,s.direction
             FROM fills f LEFT JOIN swap_fill_sources s ON s.fill_id=f.fill_id
             WHERE f.intent_id=?1 AND f.exchange_order_id=?2
             ORDER BY f.fill_id LIMIT 1001",
        )?;
        let mut page = rows.query(params![id, order_id])?;
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
            let mut check = self.connection.prepare(
                "SELECT e.amount,e.currency FROM bill_order_sources o
                 JOIN bills b ON b.bill_id=o.bill_id
                 JOIN finance_entries e ON e.economic_event_key=b.economic_event_key
                   AND e.component='FEE' AND e.allocation_leg='account'
                 WHERE o.bill_id=?1 AND o.instrument_id=?2
                   AND o.exchange_order_id=?3 AND o.trade_id=?4 LIMIT 2",
            )?;
            let linked = check
                .query_map(
                    params![&bill, &intent.instrument_id, order_id, &trade],
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
        let attributed: i64 = self.connection.query_row(
            "SELECT COUNT(*) FROM fills WHERE intent_id=?1",
            [id],
            |r| r.get(0),
        )?;
        if attributed != proof_rows.len() as i64 {
            return Err(StoreError::Invalid("LOCAL_REDUCE_OTHER_ORDER_FILLS_UNVERIFIED"));
        }
        let digest = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "okx.local-partial-reduce/v1",
                    self.meta.account_key.as_str(),
                    self.meta.installation_id.as_str(),
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
    pub fn protection(&self, id: &str) -> StoreResult<Option<ProtectionReceipt>> {
        let raw=self.connection.query_row("SELECT algo_id,side,quantity,trigger_price,trigger_basis,verified_at,source_digest FROM protection_receipts WHERE intent_id=?1",[id],|r|Ok((ProtectionReceipt{intent_id:id.into(),algo_id:r.get(0)?,side:r.get(1)?,quantity:Decimal::ZERO,trigger_price:Decimal::ZERO,trigger_basis:r.get(4)?,verified_at:r.get(5)?,source_digest:r.get(6)?},r.get::<_,String>(2)?,r.get::<_,String>(3)?))).optional()?;
        raw.map(|(mut p, q, t)| {
            p.quantity = decimal(q)?;
            p.trigger_price = decimal(t)?;
            Ok(p)
        })
        .transpose()
    }
    pub fn record_order_readback(
        &mut self,
        id: &str,
        order_id: &str,
        state: IntentState,
        cumulative_fill: Decimal,
        now: i64,
    ) -> StoreResult<()> {
        let i = self
            .intent(id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        if cumulative_fill < Decimal::ZERO
            || cumulative_fill > i.plan.quantity
            || order_id.is_empty()
        {
            return Err(StoreError::Invalid("ORDER_READBACK_INVALID"));
        }
        self.connection.execute("INSERT INTO orders VALUES(?1,?1,?2,?3,NULL,?4,?5) ON CONFLICT(order_id) DO UPDATE SET filled_qty=excluded.filled_qty,state=excluded.state WHERE intent_id=excluded.intent_id AND client_order_id=excluded.client_order_id",params![order_id,id,i.client_order_id,exact(cumulative_fill),state.as_str()])?;
        self.record_outcome(
            id,
            state,
            Some(order_id),
            "authenticated-order-readback",
            now,
        )
    }
    /// Resolve a durable protection stop only after the caller has independently
    /// verified fresh flat venue state, complete finance/central settlement and
    /// zero pending algos on this instrument. Require the SAME original central
    /// release receipt and zero SQL lots in the transaction that clears stop,
    /// intent incident and seals immutable recovery evidence.
    pub fn resolve_protection_incident_after_flat(
        &mut self,
        id: &str,
        source_digest: &str,
        now: i64,
    ) -> StoreResult<bool> {
        if source_digest.len() != 64 || !source_digest.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(StoreError::Invalid("PROTECTION_FLAT_SOURCE_DIGEST_INVALID"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (instrument, incident): (String, Option<String>) = tx.query_row(
            "SELECT instrument_id, incident FROM intents WHERE intent_id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if !matches!(
            incident.as_deref(),
            Some("PROTECTION_PENDING_OR_FAILED" | "MANAGED_AGGREGATE_PROTECTION_UNVERIFIED")
        ) {
            return Ok(false);
        }
        let active: i64 = tx.query_row(
            "SELECT COUNT(*) FROM position_lots WHERE instrument_id=?1 AND remaining_qty<>'0'",
            [&instrument],
            |row| row.get(0),
        )?;
        if active != 0 {
            return Err(StoreError::Invalid("PROTECTION_FLAT_SQL_EXPOSURE_REMAINS"));
        }
        // A venue-flat GET and an arbitrary 64-char digest cannot release a
        // still-held Worker grant. Verify the ORIGINAL risk receipt and its
        // persisted central-release acknowledgement inside this transaction,
        // binding account, intent, reservation and immutable receipt hash.
        let released: Option<String> = tx
            .query_row(
                "SELECT rr.receipt_hash FROM risk_receipts rr
                 WHERE rr.intent_id=?1 AND rr.account_key=?2
                   AND rr.state='RELEASED' AND rr.reservation_key NOT LIKE 'local:%'
                   AND EXISTS (
                       SELECT 1 FROM operation_receipts op
                       WHERE op.operation_id='central-release:'||rr.reservation_key
                         AND op.kind='CENTRAL_RELEASE'
                         AND op.request_hash=rr.receipt_hash
                         AND LENGTH(op.result_ref)>0
                   )",
                params![id, &self.meta.account_key],
                |row| row.get(0),
            )
            .optional()?;
        let release_receipt = released
            .filter(|receipt_hash| !receipt_hash.is_empty())
            .ok_or(StoreError::Invalid(
                "PROTECTION_FLAT_CENTRAL_RELEASE_NOT_PROVEN",
            ))?;
        let changed = tx.execute(
            "UPDATE risk_stops SET resolution=?2 WHERE stop_id=?1 AND resolution IS NULL",
            params![format!("protection:{id}"), source_digest],
        )?;
        if changed != 1 {
            return Err(StoreError::Invalid("PROTECTION_RISK_STOP_NOT_ACTIVE"));
        }
        let cleared = tx.execute(
            "UPDATE intents SET incident=NULL,revision=revision+1,updated_at=?2
             WHERE intent_id=?1 AND incident IN
             ('PROTECTION_PENDING_OR_FAILED','MANAGED_AGGREGATE_PROTECTION_UNVERIFIED')",
            params![id, now],
        )?;
        if cleared != 1 {
            return Err(StoreError::Invalid(
                "PROTECTION_INCIDENT_CHANGED_DURING_RESOLUTION",
            ));
        }
        // The original worker receipt and verified source digest are
        // durable together with the incident/stop transition. Replaying
        // status cannot generate a second resolution.
        tx.execute(
            "INSERT INTO operation_receipts VALUES
             (?1,'PROTECTION_INCIDENT_FLAT_RESOLVED',?2,?3,?4)",
            params![
                format!("protection-recovered:{id}"),
                release_receipt,
                source_digest,
                now
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }
    pub fn protection_incident(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<()> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE intents SET incident=?2,revision=revision+1,updated_at=?3 WHERE intent_id=?1",
            params![id, reason, now],
        )?;
        tx.execute("INSERT INTO risk_stops VALUES(?1,'account',?2,?3,NULL) ON CONFLICT(stop_id) DO NOTHING",params![format!("protection:{id}"),reason,now])?;
        tx.commit()?;
        Ok(())
    }
}
