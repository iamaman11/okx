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
    /// zero pending algos on this instrument. Reject remaining SQL lots in the
    /// SAME SQLite transaction that clears both stop and intent incident.
    pub fn resolve_protection_incident_after_flat(
        &mut self,
        id: &str,
        source_digest: &str,
        now: i64,
    ) -> StoreResult<bool> {
        if source_digest.len() != 64
            || !source_digest.bytes().all(|c| c.is_ascii_hexdigit())
        {
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
        let changed = tx.execute(
            "UPDATE risk_stops SET resolution=?2 WHERE stop_id=?1 AND resolution IS NULL",
            params![format!("protection:{id}"), source_digest],
        )?;
        if changed != 1 {
            return Err(StoreError::Invalid("PROTECTION_RISK_STOP_NOT_ACTIVE"));
        }
        tx.execute(
            "UPDATE intents SET incident=NULL,revision=revision+1,updated_at=?2
             WHERE intent_id=?1 AND incident IN
             ('PROTECTION_PENDING_OR_FAILED','MANAGED_AGGREGATE_PROTECTION_UNVERIFIED')",
            params![id, now],
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
