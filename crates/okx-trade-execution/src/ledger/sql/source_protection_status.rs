//! Original protective receipt/incident rules on the caller's scoped SQL transaction.
use super::super::*;

impl AccountStore {
    fn protection_release_ack_bound_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        receipt: &CentralReceipt,
        ack: Option<&OperationReceipt>,
    ) -> StoreResult<bool> {
        let Some(ack) = ack.filter(|a| a.kind == "CENTRAL_RELEASE" && !a.result_ref.is_empty())
        else {
            return Ok(false);
        };
        if let Some((i, a)) = scope {
            let native:Option<(String,String,String,String)>=db.query_row(
                "SELECT state,plan_sha256,release_request_hash,release_proof_digest FROM risk_entries
                 WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3 AND reservation_key=?4",
                params![i,a,receipt.intent_id,receipt.reservation_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))
            ).optional()?;
            if let Some((state, plan, request, proof)) = native {
                return Ok(state == "RELEASED"
                    && plan == receipt.plan_hash
                    && request == ack.request_hash
                    && proof == ack.result_ref
                    && proof.len() == 64);
            }
        }
        // Imported historical legacy acknowledgements retain their own
        // original hash domain. A present Native row must match above.
        Ok(ack.request_hash == receipt.receipt_hash)
    }

    pub fn effective_protected_quantity(
        &self,
        id: &str,
        filled: Decimal,
    ) -> StoreResult<Option<Decimal>> {
        Self::effective_protected_quantity_sql(&self.connection, None, id, filled)
    }
    pub(crate) fn verify_protection_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        p: &ProtectionReceipt,
    ) -> StoreResult<()> {
        let intent = Self::find_intent_sql(db, scope, "intent_id", &p.intent_id)?
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
        match scope {
            None => {
                db.execute("INSERT INTO protection_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(intent_id) DO UPDATE SET algo_id=excluded.algo_id,side=excluded.side,quantity=excluded.quantity,trigger_price=excluded.trigger_price,trigger_basis=excluded.trigger_basis,verified_at=excluded.verified_at,source_digest=excluded.source_digest",params![p.intent_id,p.algo_id,p.side,exact(p.quantity),exact(p.trigger_price),p.trigger_basis,p.verified_at,p.source_digest])?;
            }
            Some((i, a)) => {
                Self::verify_source_account_sql(db, i, a)?;
                db.execute("INSERT INTO protection_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(installation_id,account_key,intent_id) DO UPDATE SET algo_id=excluded.algo_id,side=excluded.side,quantity=excluded.quantity,trigger_price=excluded.trigger_price,trigger_basis=excluded.trigger_basis,verified_at=excluded.verified_at,source_digest=excluded.source_digest",params![i,a,p.intent_id,p.algo_id,p.side,exact(p.quantity),exact(p.trigger_price),p.trigger_basis,p.verified_at,p.source_digest])?;
            }
        }
        Ok(())
    }
    pub(crate) fn protection_incident_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        reason: &str,
        now: i64,
    ) -> StoreResult<()> {
        match scope {
            None => {
                db.execute("UPDATE intents SET incident=?2,revision=revision+1,updated_at=?3 WHERE intent_id=?1",params![id,reason,now])?;
                db.execute("INSERT INTO risk_stops VALUES(?1,'account',?2,?3,NULL) ON CONFLICT(stop_id) DO NOTHING",params![format!("protection:{id}"),reason,now])?;
            }
            Some((i, a)) => {
                Self::verify_source_account_sql(db, i, a)?;
                Self::find_intent_sql(db, scope, "intent_id", id)?
                    .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
                db.execute("UPDATE intents SET incident=?2,revision=revision+1,updated_at=?3 WHERE intent_id=?1 AND installation_id=?4 AND account_key=?5",params![id,reason,now,i,a])?;
                db.execute("INSERT INTO risk_stops VALUES(?1,?2,?3,'account',?4,?5,NULL) ON CONFLICT(installation_id,account_key,stop_id) DO NOTHING",params![i,a,format!("protection:{id}"),reason,now])?;
            }
        }
        Ok(())
    }
    pub(crate) fn resolve_protection_incident_after_flat_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        digest: &str,
        now: i64,
    ) -> StoreResult<bool> {
        if digest.len() != 64 || !digest.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(StoreError::Invalid("PROTECTION_FLAT_SOURCE_DIGEST_INVALID"));
        }
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        if !matches!(
            intent.incident.as_deref(),
            Some("PROTECTION_PENDING_OR_FAILED" | "MANAGED_AGGREGATE_PROTECTION_UNVERIFIED")
        ) {
            return Ok(false);
        }
        let active:i64=match scope {
            None=>db.query_row("SELECT COUNT(*) FROM position_lots WHERE instrument_id=?1 AND remaining_qty<>'0'",[&intent.instrument_id],|r|r.get(0))?,
            Some((i,a))=>db.query_row("SELECT COUNT(*) FROM position_lots WHERE instrument_id=?1 AND remaining_qty<>'0' AND installation_id=?2 AND account_key=?3",params![intent.instrument_id,i,a],|r|r.get(0))?,
        };
        if active != 0 {
            return Err(StoreError::Invalid("PROTECTION_FLAT_SQL_EXPOSURE_REMAINS"));
        }
        let released:Option<String>=match scope {
            None=>db.query_row("SELECT rr.receipt_hash FROM risk_receipts rr WHERE rr.intent_id=?1 AND rr.account_key=(SELECT account_key FROM account_meta WHERE singleton=1) AND rr.state='RELEASED' AND rr.reservation_key NOT LIKE 'local:%' AND EXISTS(SELECT 1 FROM operation_receipts op WHERE op.operation_id='central-release:'||rr.reservation_key AND op.kind='CENTRAL_RELEASE' AND op.request_hash=rr.receipt_hash AND LENGTH(op.result_ref)>0)",[id],|r|r.get(0)).optional()?,
            Some((i,a))=>db.query_row("SELECT rr.receipt_hash FROM risk_receipts rr WHERE rr.intent_id=?1 AND rr.installation_id=?2 AND rr.account_key=?3 AND rr.state='RELEASED' AND rr.reservation_key NOT LIKE 'local:%' AND EXISTS(SELECT 1 FROM operation_receipts op WHERE op.installation_id=rr.installation_id AND op.account_key=rr.account_key AND op.operation_id='central-release:'||rr.reservation_key AND op.kind='CENTRAL_RELEASE' AND LENGTH(op.result_ref)>0 AND (
                        EXISTS(SELECT 1 FROM risk_entries e WHERE e.installation_id=rr.installation_id
                            AND e.account_key=rr.account_key AND e.intent_id=rr.intent_id
                            AND e.reservation_key=rr.reservation_key AND e.plan_sha256=rr.plan_hash
                            AND e.state='RELEASED' AND e.release_request_hash=op.request_hash
                            AND e.release_proof_digest=op.result_ref AND length(e.release_proof_digest)=64)
                        OR (NOT EXISTS(SELECT 1 FROM risk_entries e WHERE e.installation_id=rr.installation_id
                            AND e.account_key=rr.account_key AND e.intent_id=rr.intent_id)
                            AND op.request_hash=rr.receipt_hash)))",params![id,i,a],|r|r.get(0)).optional()?,
        };
        let receipt = released
            .filter(|v| !v.is_empty())
            .ok_or(StoreError::Invalid(
                "PROTECTION_FLAT_CENTRAL_RELEASE_NOT_PROVEN",
            ))?;
        let changed=match scope {
            None=>db.execute("UPDATE risk_stops SET resolution=?2 WHERE stop_id=?1 AND resolution IS NULL",params![format!("protection:{id}"),digest])?,
            Some((i,a))=>db.execute("UPDATE risk_stops SET resolution=?2 WHERE stop_id=?1 AND resolution IS NULL AND installation_id=?3 AND account_key=?4",params![format!("protection:{id}"),digest,i,a])?,
        };
        if changed != 1 {
            return Err(StoreError::Invalid("PROTECTION_RISK_STOP_NOT_ACTIVE"));
        }
        let cleared=match scope {
            None=>db.execute("UPDATE intents SET incident=NULL,revision=revision+1,updated_at=?2 WHERE intent_id=?1 AND incident IN ('PROTECTION_PENDING_OR_FAILED','MANAGED_AGGREGATE_PROTECTION_UNVERIFIED')",params![id,now])?,
            Some((i,a))=>db.execute("UPDATE intents SET incident=NULL,revision=revision+1,updated_at=?2 WHERE intent_id=?1 AND incident IN ('PROTECTION_PENDING_OR_FAILED','MANAGED_AGGREGATE_PROTECTION_UNVERIFIED') AND installation_id=?3 AND account_key=?4",params![id,now,i,a])?,
        };
        if cleared != 1 {
            return Err(StoreError::Invalid(
                "PROTECTION_INCIDENT_CHANGED_DURING_RESOLUTION",
            ));
        }
        match scope {
            None => {
                db.execute("INSERT INTO operation_receipts VALUES(?1,'PROTECTION_INCIDENT_FLAT_RESOLVED',?2,?3,?4)",params![format!("protection-recovered:{id}"),receipt,digest,now])?;
            }
            Some((i, a)) => {
                db.execute("INSERT INTO operation_receipts VALUES(?1,?2,?3,'PROTECTION_INCIDENT_FLAT_RESOLVED',?4,?5,?6)",params![i,a,format!("protection-recovered:{id}"),receipt,digest,now])?;
            }
        }
        Ok(true)
    }
    pub(crate) fn effective_protected_quantity_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        filled: Decimal,
    ) -> StoreResult<Option<Decimal>> {
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        let lots = Self::active_lots_sql(db, scope, &intent.instrument_id)?;
        let own = lots
            .iter()
            .filter(|l| l.intent_id.as_deref() == Some(id))
            .fold(Decimal::ZERO, |sum, lot| sum + lot.remaining_qty);
        if own == Decimal::ZERO
            && intent.state.terminal()
            && Self::reservation_receipt_sql(db, scope, id, CentralReceiptView::ActiveExposure)?
                .is_none()
        {
            let receipt =
                Self::reservation_receipt_sql(db, scope, id, CentralReceiptView::Retained)?;
            let terminal = Self::operation_sql(db, scope, &format!("entry-settlement:{id}"))?;
            if let (Some(receipt), Some(terminal)) = (receipt, terminal) {
                let proof: EntrySettlementEvidence = serde_json::from_str(&terminal.result_ref)
                    .map_err(|_| StoreError::Invalid("TERMINAL_ENTRY_PROOF_CORRUPT"))?;
                let release = Self::operation_sql(
                    db,
                    scope,
                    &format!("central-release:{}", receipt.reservation_key),
                )?;
                if terminal.kind == "ENTRY_SETTLEMENT_PROVEN"
                    && proof.schema == "okx.entry-terminal-settlement/v1"
                    && proof.intent_id == id
                    && receipt.intent_id == id
                    && intent.exchange_order_id.as_deref() == Some(proof.venue_order_id.as_str())
                    && proof.plan_hash == intent.plan_hash
                    && receipt.plan_hash == intent.plan_hash
                    && proof.account_key == receipt.account_key
                    && proof.reservation_key == receipt.reservation_key
                    && proof.venue_filled_quantity == filled
                    && Self::protection_release_ack_bound_sql(
                        db,
                        scope,
                        &receipt,
                        release.as_ref(),
                    )?
                {
                    return Ok(None);
                }
            }
        }
        if own <= Decimal::ZERO || own > filled {
            return Err(StoreError::Invalid("PROTECTION_ORIGINAL_SQL_LOT_MISMATCH"));
        }
        if own == filled {
            return Ok(Some(own));
        }
        let receipt = Self::protection_sql(db, scope, id)?.ok_or(StoreError::Invalid(
            "PROTECTION_REDUCED_LOT_RECEIPT_MISSING",
        ))?;
        let operation = Self::operation_sql(
            db,
            scope,
            &format!("protection-shrink:{id}:{}:verified", exact(own)),
        )?
        .ok_or(StoreError::Invalid("PROTECTION_REDUCE_SOURCE_ACK_MISSING"))?;
        if operation.kind != "protection_shrink_verified"
            || operation.result_ref != "VERIFIED"
            || receipt.quantity != own
        {
            return Err(StoreError::Invalid("PROTECTION_REDUCE_SOURCE_MISMATCH"));
        }
        Ok(Some(own))
    }
}
