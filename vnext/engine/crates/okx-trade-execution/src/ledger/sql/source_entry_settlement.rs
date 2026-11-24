//! Original source-backed settlement in one caller-owned SQL snapshot.
use super::super::*;
use super::source_entry_costs::{entry_read, entry_source_values};
use sha2::{Digest, Sha256};
impl AccountStore {
    pub(crate) fn entry_late_pending_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<bool> {
        if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
        }
        Ok(db.query_row(match scope {
            None=>"SELECT state='LATE_SETTLEMENT_PENDING' FROM risk_receipts WHERE intent_id=?1",
            Some(_)=>entry_read!("SELECT state='LATE_SETTLEMENT_PENDING' FROM risk_receipts WHERE intent_id=?3"),
        },rusqlite::params_from_iter(entry_source_values(scope,vec![id.to_owned().into()])),|r|r.get(0)).optional()?.unwrap_or(false))
    }
    pub(crate) fn partial_entry_ack_pending_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
    ) -> StoreResult<bool> {
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        let Some(head) = Self::operation_sql(db, scope, &format!("entry-partial-head:{id}"))?
        else {
            return Ok(false);
        };
        let proof: EntrySettlementEvidence = serde_json::from_str(&head.result_ref)
            .map_err(|_| StoreError::Invalid("PARTIAL_ENTRY_HEAD_CORRUPT"))?;
        if head.kind != "ENTRY_PARTIAL_HEAD"
            || head.request_hash != proof.plan_hash
            || proof.schema != "okx.entry-partial-settlement/v1"
            || proof.account_key != meta.account_key
            || proof.intent_id != id
            || proof.settlement_revision == 0
            || proof.source_digest.len() != 64
            || !proof.source_digest.bytes().all(|c| c.is_ascii_hexdigit())
        {
            return Err(StoreError::Invalid("PARTIAL_ENTRY_HEAD_INVALID"));
        }
        let immutable = Self::operation_sql(
            db,
            scope,
            &format!("entry-partial:{id}:r{}", proof.settlement_revision),
        )?
        .ok_or(StoreError::Invalid("PARTIAL_ENTRY_REVISION_MISSING"))?;
        if immutable.kind != "ENTRY_PARTIAL_SETTLEMENT_PROVEN"
            || immutable.result_ref != head.result_ref
            || immutable.request_hash != proof.plan_hash
        {
            return Err(StoreError::Invalid("PARTIAL_ENTRY_REVISION_MISMATCH"));
        }
        match Self::operation_sql(
            db,
            scope,
            &format!("entry-partial-settled:{id}:r{}", proof.settlement_revision),
        )? {
            None => Ok(true),
            Some(receipt)
                if receipt.kind == "ENTRY_PARTIAL_SETTLED"
                    && receipt.request_hash == proof.plan_hash
                    && receipt.result_ref == proof.source_digest =>
            {
                Ok(false)
            }
            _ => Err(StoreError::Invalid("PARTIAL_ENTRY_ACK_CONFLICT")),
        }
    }
    pub(crate) fn verify_native_entry_ack_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        accepted: &EntrySettlementEvidence,
        late: bool,
    ) -> StoreResult<()> {
        let Some((installation, account)) = scope else {
            return Err(StoreError::Invalid("UNIFIED_SETTLEMENT_ACK_SCOPE_REQUIRED"));
        };
        Self::verify_source_account_sql(db, installation, account)?;
        if accepted.account_key != account {
            return Err(StoreError::Invalid(
                "UNIFIED_SETTLEMENT_ACK_IDENTITY_INVALID",
            ));
        }
        let stored = if late {
            Self::final_entry_settlement_receipt_sql(
                db,
                installation,
                account,
                &accepted.intent_id,
                true,
            )?
        } else {
            Self::partial_entry_settlement_receipt_sql(
                db,
                Some(installation),
                account,
                &accepted.intent_id,
            )?
        }
        .ok_or(StoreError::Invalid("UNIFIED_SETTLEMENT_ACK_SOURCE_MISSING"))?;
        if stored != *accepted {
            return Err(StoreError::Invalid("UNIFIED_SETTLEMENT_ACK_SOURCE_CHANGED"));
        }
        if late {
            let current = Self::entry_settlement_cursor_sql(db, scope, "bills", "account")?
                .ok_or(StoreError::Invalid("LATE_ENTRY_BILLS_UNCOVERED"))?;
            let (accepted_fee, _) = Self::intent_additional_entry_fee_evidence_sql(
                db,
                scope,
                &accepted.intent_id,
                accepted.source_covered_through_ms,
            )?;
            let (latest_fee, _) = Self::intent_additional_entry_fee_evidence_sql(
                db,
                scope,
                &accepted.intent_id,
                current.watermark,
            )?;
            if latest_fee > accepted_fee {
                return Err(StoreError::Invalid("LATE_ENTRY_NEW_BILL_PENDING"));
            }
        }
        let (prefix, kind) = if late {
            ("entry-late-settled", "ENTRY_LATE_SETTLED")
        } else {
            ("entry-partial-settled", "ENTRY_PARTIAL_SETTLED")
        };
        let ack = Self::operation_sql(
            db,
            scope,
            &format!(
                "{prefix}:{}:r{}",
                accepted.intent_id, accepted.settlement_revision
            ),
        )?
        .ok_or(StoreError::Invalid("UNIFIED_SETTLEMENT_ACK_NOT_COMMITTED"))?;
        if ack.kind != kind
            || ack.request_hash != accepted.plan_hash
            || ack.result_ref != accepted.source_digest
        {
            return Err(StoreError::Invalid("UNIFIED_SETTLEMENT_ACK_CONFLICT"));
        }
        Ok(())
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Keep SQL scope, original proof and CAS fields explicit at this bounded boundary."
    )]
    fn persist_entry_settlement_record_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        operation: &str,
        kind: &str,
        hash: &str,
        body: &str,
        at: i64,
        head: bool,
    ) -> StoreResult<()> {
        match (scope, head) {
            (Some((i, a)), false) => {
                tx.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![i,a,operation,kind,hash,body,at])?;
            }
            (Some((i, a)), true) => {
                tx.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(installation_id,account_key,operation_id) DO UPDATE SET result_ref=excluded.result_ref,committed_at=excluded.committed_at",params![i,a,operation,kind,hash,body,at])?;
            }
            (None, false) => {
                tx.execute("INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,?4,?5)",params![operation,kind,hash,body,at])?;
            }
            (None, true) => {
                tx.execute("INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(operation_id) DO UPDATE SET result_ref=excluded.result_ref,committed_at=excluded.committed_at",params![operation,kind,hash,body,at])?;
            }
        }
        Ok(())
    }

    pub(crate) fn entry_settlement_identity_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
    ) -> StoreResult<AccountMeta> {
        let mut values = vec![];
        let sql = if let Some((i, a)) = scope {
            Self::verify_source_account_sql(db, i, a)?;
            values.push(rusqlite::types::Value::from(i.to_owned()));
            values.push(a.to_owned().into());
            "SELECT account_key,installation_id,initialized_at,opening_source,initialize_operation_id,release_id FROM account_meta WHERE singleton=1 AND installation_id=?1 AND account_key=?2"
        } else {
            "SELECT account_key,installation_id,initialized_at,opening_source,initialize_operation_id,release_id FROM account_meta WHERE singleton=1"
        };
        Ok(db.query_row(sql, rusqlite::params_from_iter(values), |r| {
            Ok(AccountMeta {
                account_key: r.get(0)?,
                installation_id: r.get(1)?,
                initialized_at: r.get(2)?,
                opening_source: r.get(3)?,
                initialize_operation_id: r.get(4)?,
                release_id: r.get(5)?,
            })
        })?)
    }
    pub(crate) fn entry_settlement_cursor_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        source: &str,
        partition: &str,
    ) -> StoreResult<Option<SourceCursor>> {
        if let Some((i, a)) = scope {
            return Self::scoped_source_cursor_sql(db, i, a, source, partition);
        }
        Ok(db.query_row("SELECT cursor,watermark,generation,coverage_gaps FROM source_cursors WHERE source=?1 AND stream_partition=?2",params![source,partition],|r|Ok(SourceCursor{source:source.into(),stream_partition:partition.into(),cursor:r.get(0)?,watermark:r.get(1)?,generation:r.get(2)?,coverage_gaps:r.get(3)?})).optional()?)
    }
    pub(crate) fn bind_zero_fill_terminal_entry_settlement_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        venue_updated_at: i64,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let db: &Connection = tx;
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("ZERO_ENTRY_INTENT_MISSING"))?;
        if intent.state != IntentState::Cancelled
            || !intent.risk_increase
            || !intent.instrument_id.ends_with("-USDT-SWAP")
            || venue_updated_at <= 0
            || covered_through < venue_updated_at
        {
            return Err(StoreError::Invalid("ZERO_ENTRY_VENUE_TERMINAL_UNVERIFIED"));
        }
        let venue_id = intent
            .exchange_order_id
            .as_deref()
            .ok_or(StoreError::Invalid("ZERO_ENTRY_ORDER_ID_MISSING"))?;
        let post: Option<i64> = db.query_row(
            match scope {
                None => "SELECT begin_post_at FROM intents WHERE intent_id=?1",
                Some(_) => entry_read!("SELECT begin_post_at FROM intents WHERE intent_id=?3"),
            },
            rusqlite::params_from_iter(entry_source_values(scope, vec![id.to_owned().into()])),
            |r| r.get(0),
        )?;
        let admission = Self::entry_admission_sql(db, scope, id)?
            .ok_or(StoreError::Invalid("ZERO_ENTRY_ADMISSION_MISSING"))?;
        let central = Self::reservation_receipt_sql(db, scope, id, CentralReceiptView::Retained)?
            .ok_or(StoreError::Invalid("ZERO_ENTRY_RESERVATION_MISSING"))?;
        if post.is_none_or(|t| t <= 0 || t < admission.observed_at || venue_updated_at < t)
            || admission.other_entry_costs_upper_usd != Decimal::ZERO
            || admission.plan_hash != intent.plan_hash
            || admission.account_key != meta.account_key
            || central.plan_hash != intent.plan_hash
            || central.account_key != meta.account_key
        {
            return Err(StoreError::Invalid("ZERO_ENTRY_CAUSAL_PROOF_INVALID"));
        }
        // Replays preserve the ORIGINAL covered source frontier even if
        // ongoing capture has advanced for other orders or later bills.
        let key = format!("entry-settlement:{id}");
        let prior = Self::operation_sql(db, scope, &key)?;
        let proof_through = if let Some(prior) = prior.as_ref() {
            let original: EntrySettlementEvidence = serde_json::from_str(&prior.result_ref)
                .map_err(|_| StoreError::Invalid("ZERO_ENTRY_REPLAY_CORRUPT"))?;
            if original.source_covered_through_ms > covered_through {
                return Err(StoreError::Invalid("ZERO_ENTRY_COVERAGE_REGRESSION"));
            }
            original.source_covered_through_ms
        } else {
            covered_through
        };
        // Independently completed stream frontiers, NOT a missing page.
        for (source, partition) in [("fills", "SWAP"), ("bills", "account")] {
            let cursor = Self::entry_settlement_cursor_sql(db, scope, source, partition)?
                .ok_or(StoreError::Invalid("ZERO_ENTRY_COVERAGE_UNPROVEN"))?;
            if cursor.generation <= 0
                || !cursor.cursor.is_empty()
                || !cursor.coverage_gaps.is_empty()
                || cursor.watermark < proof_through
            {
                return Err(StoreError::Invalid("ZERO_ENTRY_COVERAGE_UNPROVEN"));
            }
        }
        let unexplained: i64 = db.query_row(
            match scope {
                None => {
                    "SELECT EXISTS(SELECT 1 FROM fills
                 WHERE (intent_id=?1 OR exchange_order_id=?2)
                   AND time<=?4)
               OR EXISTS(SELECT 1 FROM bill_order_sources o
                 JOIN bills b ON b.bill_id=o.bill_id
                 WHERE o.exchange_order_id=?2
                   AND b.time>=?3 AND b.time<=?4)
               OR EXISTS(SELECT 1 FROM bills b
                 LEFT JOIN bill_order_sources o ON o.bill_id=b.bill_id
                 WHERE o.bill_id IS NULL
                   AND b.time>=?3 AND b.time<=?4
                   AND b.amount!='0'
                   AND b.kind NOT IN ('1:11','1:12','8:173','8:174')
                   AND b.kind NOT LIKE '7:%')"
                }
                Some(_) => entry_read!(
                    "SELECT EXISTS(SELECT 1 FROM fills
                 WHERE (intent_id=?3 OR exchange_order_id=?4)
                   AND time<=?6)
               OR EXISTS(SELECT 1 FROM bill_order_sources o
                 JOIN bills b ON b.bill_id=o.bill_id
                 WHERE o.exchange_order_id=?4
                   AND b.time>=?5 AND b.time<=?6)
               OR EXISTS(SELECT 1 FROM bills b
                 LEFT JOIN bill_order_sources o ON o.bill_id=b.bill_id
                 WHERE o.bill_id IS NULL
                   AND b.time>=?5 AND b.time<=?6
                   AND b.amount!='0'
                   AND b.kind NOT IN ('1:11','1:12','8:173','8:174')
                   AND b.kind NOT LIKE '7:%')"
                ),
            },
            rusqlite::params_from_iter(entry_source_values(
                scope,
                vec![
                    id.to_owned().into(),
                    venue_id.to_owned().into(),
                    post.unwrap().into(),
                    proof_through.into(),
                ],
            )),
            |r| r.get(0),
        )?;
        if unexplained != 0 {
            return Err(StoreError::Invalid("ZERO_ENTRY_MONEY_UNRECONCILED"));
        }
        let source = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "okx.entry-zero-fill-terminal/v1",
                    meta.account_key.as_str(),
                    meta.installation_id.as_str(),
                    id,
                    intent.plan_hash.as_str(),
                    admission.entry_proof_digest.as_str(),
                    central.reservation_key.as_str(),
                    venue_id,
                    venue_updated_at,
                    proof_through,
                ))
                .map_err(|_| StoreError::Invalid("ZERO_ENTRY_DIGEST_FAILED"))?
            )
        );
        let evidence = EntrySettlementEvidence {
            schema: "okx.entry-terminal-settlement/v1".into(),
            account_key: meta.account_key.clone(),
            intent_id: id.into(),
            plan_hash: intent.plan_hash,
            reservation_key: central.reservation_key,
            venue_order_id: venue_id.into(),
            venue_update_ms: venue_updated_at,
            source_covered_through_ms: proof_through,
            venue_filled_quantity: Decimal::ZERO,
            booked_notional_and_fee_usdt: Decimal::ZERO,
            cumulative_entry_usd: Decimal::ZERO,
            settlement_revision: 1,
            source_digest: source,
        };
        let encoded = serde_json::to_string(&evidence)
            .map_err(|_| StoreError::Invalid("ZERO_ENTRY_SERIALIZE_FAILED"))?;
        if let Some(saved) = prior {
            if saved.kind != "ENTRY_SETTLEMENT_PROVEN" || saved.request_hash != evidence.plan_hash {
                return Err(StoreError::Invalid("ZERO_ENTRY_REPLAY_CONFLICT"));
            }
            let original: EntrySettlementEvidence = serde_json::from_str(&saved.result_ref)
                .map_err(|_| StoreError::Invalid("ZERO_ENTRY_REPLAY_CORRUPT"))?;
            // Advancing source frontiers does not rewrite this proof. Caller
            // replays the SAME frozen coverage snapshot after a lost ACK.
            if original != evidence || saved.committed_at != venue_updated_at {
                return Err(StoreError::Invalid("ZERO_ENTRY_REPLAY_CONFLICT"));
            }
            return Ok(original);
        }
        if Self::operation_sql(db, scope, &format!("entry-partial-head:{id}"))?.is_some() {
            return Err(StoreError::Invalid("ZERO_ENTRY_PRIOR_PARTIAL_CONFLICT"));
        }
        Self::persist_entry_settlement_record_sql(
            tx,
            scope,
            &(key),
            "ENTRY_SETTLEMENT_PROVEN",
            &evidence.plan_hash,
            &encoded,
            venue_updated_at,
            false,
        )?;
        Ok(evidence)
    }
    pub(crate) fn bind_terminal_entry_settlement_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let db: &Connection = tx;
        Self::entry_settlement_identity_sql(db, scope)?;
        if venue_filled_quantity == Decimal::ZERO {
            return Self::bind_zero_fill_terminal_entry_settlement_sql(
                tx,
                scope,
                id,
                venue_updated_at,
                covered_through,
            );
        }
        Self::bind_cumulative_entry_settlement_sql(
            tx,
            scope,
            id,
            venue_updated_at,
            venue_filled_quantity,
            covered_through,
            true,
        )
    }
    pub(crate) fn bind_partial_entry_settlement_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let db: &Connection = tx;
        Self::entry_settlement_identity_sql(db, scope)?;
        Self::bind_cumulative_entry_settlement_sql(
            tx,
            scope,
            id,
            venue_updated_at,
            venue_filled_quantity,
            covered_through,
            false,
        )
    }
    pub(crate) fn bind_cumulative_entry_settlement_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
        terminal: bool,
    ) -> StoreResult<EntrySettlementEvidence> {
        let db: &Connection = tx;
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        let intent = Self::find_intent_sql(db, scope, "intent_id", id)?
            .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_INTENT_MISSING"))?;
        if (terminal && !matches!(intent.state, IntentState::Filled | IntentState::Cancelled))
            || (!terminal
                && !matches!(
                    intent.state,
                    IntentState::PartiallyFilled | IntentState::Filled | IntentState::Cancelled
                ))
            || venue_updated_at <= 0
            || covered_through < venue_updated_at
            || venue_filled_quantity <= Decimal::ZERO
            || venue_filled_quantity > intent.plan.quantity
        {
            return Err(StoreError::Invalid(
                "ENTRY_SETTLEMENT_TERMINAL_SOURCE_UNVERIFIED",
            ));
        }
        let admission = Self::entry_admission_sql(db, scope, id)?
            .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_ADMISSION_MISSING"))?;
        let began_post: Option<i64> = db.query_row(
            match scope {
                None => "SELECT begin_post_at FROM intents WHERE intent_id=?1",
                Some(_) => entry_read!("SELECT begin_post_at FROM intents WHERE intent_id=?3"),
            },
            rusqlite::params_from_iter(entry_source_values(scope, vec![id.to_owned().into()])),
            |row| row.get(0),
        )?;
        if began_post
            .is_none_or(|at| at <= 0 || at < admission.observed_at || venue_updated_at < at)
        {
            return Err(StoreError::Invalid(
                "ENTRY_SETTLEMENT_POST_CAUSALITY_UNVERIFIED",
            ));
        }
        // Re-read the ORIGINAL immutable SQL cut-off on lost-ACK replay,
        // even if the ongoing source-capture cursor has advanced. Verify
        // the original source digest again; no new amount or different bill
        // witness may silently inherit the previous Worker authority.
        let key = format!("entry-settlement:{id}");
        let terminal_receipt = Self::operation_sql(db, scope, &key)?;
        let partial_head_key = format!("entry-partial-head:{id}");
        let partial_head = Self::operation_sql(db, scope, &partial_head_key)?;
        let previous = terminal_receipt.as_ref().or(partial_head.as_ref());
        let saved = previous
            .map(|receipt| {
                serde_json::from_str::<EntrySettlementEvidence>(&receipt.result_ref)
                    .map_err(|_| StoreError::Invalid("ENTRY_SETTLEMENT_RECEIPT_CORRUPT"))
            })
            .transpose()?;
        if let Some(old) = saved.as_ref() {
            if old.intent_id != id
                || old.plan_hash != intent.plan_hash
                || old.account_key != meta.account_key
                || old.settlement_revision == 0
                || (terminal_receipt.is_some() && old.schema != "okx.entry-terminal-settlement/v1")
                || (terminal_receipt.is_none() && old.schema != "okx.entry-partial-settlement/v1")
                || previous.is_some_and(|receipt| receipt.request_hash != intent.plan_hash)
            {
                return Err(StoreError::Invalid("ENTRY_SETTLEMENT_PREVIOUS_INVALID"));
            }
            // A mutable pointer is not evidence. Every revision must also
            // have its original immutable receipt with the identical bytes.
            if terminal_receipt.is_none() {
                let witness = Self::operation_sql(
                    db,
                    scope,
                    &format!("entry-partial:{id}:r{}", old.settlement_revision),
                )?
                .ok_or(StoreError::Invalid("ENTRY_PARTIAL_RECEIPT_MISSING"))?;
                if witness.kind != "ENTRY_PARTIAL_SETTLEMENT_PROVEN"
                    || witness.result_ref != previous.unwrap().result_ref
                    || witness.request_hash != old.plan_hash
                {
                    return Err(StoreError::Invalid("ENTRY_PARTIAL_RECEIPT_MISMATCH"));
                }
            }
        }
        // Recheck old immutable source facts at their original frontier.
        // A later bill/fill cannot rewrite the first accepted revision.
        if let Some(old) = saved.as_ref() {
            let historical =
                Self::intent_fee_bill_evidence_sql(db, scope, id, old.source_covered_through_ms)?;
            let (historical_extra, historical_extra_digest) =
                Self::intent_additional_entry_fee_evidence_sql(
                    db,
                    scope,
                    id,
                    old.source_covered_through_ms,
                )?;
            let original_total = historical
                .fill
                .gross_notional_usdt
                .checked_add(historical.conservative_verified_fee_usdt)
                .and_then(|v| v.checked_add(historical_extra))
                .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_MONEY_OVERFLOW"))?;
            let original_usd = original_total
                .checked_mul(admission.admission_usd_per_usdt)
                .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_MONEY_OVERFLOW"))?
                .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity)
                .checked_add(admission.other_entry_costs_upper_usd)
                .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_MONEY_OVERFLOW"))?;
            // Amount equality alone cannot prove immutable economic events:
            // a replacement fill or bill could have the same total. Rebuild
            // the exact original digest at its frozen source frontier.
            let original_bytes = if historical_extra > Decimal::ZERO {
                serde_json::to_vec(&(
                    old.schema.as_str(),
                    &meta.account_key,
                    &meta.installation_id,
                    id,
                    &intent.plan_hash,
                    &admission.entry_proof_digest,
                    &historical.fill.source_digest,
                    &historical.source_digest,
                    &historical.fill.reservation_key,
                    &historical.fill.exchange_order_id,
                    old.venue_update_ms,
                    old.venue_filled_quantity,
                    original_total,
                    original_usd,
                    historical_extra_digest,
                ))
            } else {
                serde_json::to_vec(&(
                    old.schema.as_str(),
                    &meta.account_key,
                    &meta.installation_id,
                    id,
                    &intent.plan_hash,
                    &admission.entry_proof_digest,
                    &historical.fill.source_digest,
                    &historical.source_digest,
                    &historical.fill.reservation_key,
                    &historical.fill.exchange_order_id,
                    old.venue_update_ms,
                    old.venue_filled_quantity,
                    original_total,
                    original_usd,
                ))
            }
            .map_err(|_| StoreError::Invalid("ENTRY_SETTLEMENT_SOURCE_DIGEST_INVALID"))?;
            let original_digest = format!("{:x}", Sha256::digest(original_bytes));
            if historical.fill.filled_quantity != old.venue_filled_quantity
                || original_total != old.booked_notional_and_fee_usdt
                || original_usd != old.cumulative_entry_usd
                || historical.fill.exchange_order_id != old.venue_order_id
                || historical.fill.reservation_key != old.reservation_key
                || original_digest != old.source_digest
            {
                return Err(StoreError::Invalid("ENTRY_SETTLEMENT_PRIOR_SOURCE_CHANGED"));
            }
        }
        let witness_through = if terminal_receipt.is_some() {
            saved.as_ref().unwrap().source_covered_through_ms
        } else {
            covered_through
        };
        if witness_through < venue_updated_at || witness_through > covered_through {
            return Err(StoreError::Invalid("ENTRY_SETTLEMENT_COVERAGE_REGRESSION"));
        }
        let bill = Self::intent_fee_bill_evidence_sql(db, scope, id, witness_through)?;
        let fill = &bill.fill;
        if fill.intent_id != id
            || fill.plan_hash != intent.plan_hash
            || fill.filled_quantity != venue_filled_quantity
            || fill.exchange_order_id.is_empty()
            || fill.fill_count == 0
            || bill.linked_bill_count != fill.fill_count
            || bill.conservative_verified_fee_usdt != fill.conservative_fee_usdt
            || bill.bills_covered_through < venue_updated_at
        {
            return Err(StoreError::Invalid("ENTRY_SETTLEMENT_FILL_BILL_INCOMPLETE"));
        }
        let central = Self::reservation_receipt_sql(db, scope, id, CentralReceiptView::Retained)?
            .ok_or(StoreError::Invalid(
            "ENTRY_SETTLEMENT_CENTRAL_RECEIPT_MISSING",
        ))?;
        if central.reservation_key != fill.reservation_key
            || central.account_key != admission.account_key
            || central.plan_hash != admission.plan_hash
        {
            return Err(StoreError::Invalid("ENTRY_SETTLEMENT_RESERVATION_MISMATCH"));
        }
        let (extra_fee, extra_digest) =
            Self::intent_additional_entry_fee_evidence_sql(db, scope, id, witness_through)?;
        let full_usdt = fill
            .gross_notional_usdt
            .checked_add(bill.conservative_verified_fee_usdt)
            .and_then(|v| v.checked_add(extra_fee))
            .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_MONEY_OVERFLOW"))?;
        let usd = full_usdt
            .checked_mul(admission.admission_usd_per_usdt)
            .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_MONEY_OVERFLOW"))?
            .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity)
            .checked_add(admission.other_entry_costs_upper_usd)
            .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_MONEY_OVERFLOW"))?;
        if full_usdt <= Decimal::ZERO || usd <= Decimal::ZERO {
            return Err(StoreError::Invalid("ENTRY_SETTLEMENT_MONEY_INVALID"));
        }
        // A genuine exchange debit above the approved quote is not permission
        // to hide it. The cumulative proof retains the full native amount;
        // RiskBook records the breach atomically and bans new entry grants.
        let next_revision = if let Some(old) = saved.as_ref() {
            if terminal_receipt.is_some() {
                old.settlement_revision
            } else if !terminal
                && old.cumulative_entry_usd == usd
                && old.venue_filled_quantity == venue_filled_quantity
            {
                // A later cursor with no new economic amount is a read-only
                // acknowledgement, not an additional debit or book revision.
                return Ok(old.clone());
            } else {
                if usd < old.cumulative_entry_usd
                    || venue_filled_quantity < old.venue_filled_quantity
                {
                    return Err(StoreError::Invalid("ENTRY_SETTLEMENT_REGRESSION"));
                }
                old.settlement_revision
                    .checked_add(1)
                    .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_REVISION_OVERFLOW"))?
            }
        } else {
            1
        };
        let schema = if terminal {
            "okx.entry-terminal-settlement/v1"
        } else {
            "okx.entry-partial-settlement/v1"
        };
        let bytes = if extra_fee > Decimal::ZERO {
            serde_json::to_vec(&(
                schema,
                &meta.account_key,
                &meta.installation_id,
                id,
                &intent.plan_hash,
                &admission.entry_proof_digest,
                &fill.source_digest,
                &bill.source_digest,
                &fill.reservation_key,
                &fill.exchange_order_id,
                venue_updated_at,
                venue_filled_quantity,
                full_usdt,
                usd,
                extra_digest,
            ))
        } else {
            serde_json::to_vec(&(
                schema,
                &meta.account_key,
                &meta.installation_id,
                id,
                &intent.plan_hash,
                &admission.entry_proof_digest,
                &fill.source_digest,
                &bill.source_digest,
                &fill.reservation_key,
                &fill.exchange_order_id,
                venue_updated_at,
                venue_filled_quantity,
                full_usdt,
                usd,
            ))
        }
        .map_err(|_| StoreError::Invalid("ENTRY_SETTLEMENT_SOURCE_DIGEST_INVALID"))?;
        let settlement = EntrySettlementEvidence {
            schema: schema.into(),
            account_key: meta.account_key.clone(),
            intent_id: id.into(),
            plan_hash: intent.plan_hash,
            reservation_key: fill.reservation_key.clone(),
            venue_order_id: fill.exchange_order_id.clone(),
            venue_update_ms: venue_updated_at,
            source_covered_through_ms: witness_through,
            venue_filled_quantity,
            booked_notional_and_fee_usdt: full_usdt,
            cumulative_entry_usd: usd,
            settlement_revision: next_revision,
            source_digest: format!("{:x}", Sha256::digest(bytes)),
        };
        if let Some(prior) = terminal_receipt {
            let saved = saved.ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_RECEIPT_CORRUPT"))?;
            if prior.kind != "ENTRY_SETTLEMENT_PROVEN" || prior.request_hash != settlement.plan_hash
            {
                return Err(StoreError::Invalid("ENTRY_SETTLEMENT_IDEMPOTENCY_CONFLICT"));
            }
            // Cursor watermark can advance even when all immutable economic
            // facts stay unchanged. An ACK replay must use the original
            // durable digest/revision, but cannot accept a changed venue
            // filled quantity, cost, account, reservation or order.
            if saved.schema != settlement.schema
                || saved.account_key != settlement.account_key
                || saved.intent_id != settlement.intent_id
                || saved.plan_hash != settlement.plan_hash
                || saved.reservation_key != settlement.reservation_key
                || saved.venue_order_id != settlement.venue_order_id
                || saved.venue_update_ms != settlement.venue_update_ms
                || saved.source_covered_through_ms != settlement.source_covered_through_ms
                || saved.venue_filled_quantity != settlement.venue_filled_quantity
                || saved.booked_notional_and_fee_usdt != settlement.booked_notional_and_fee_usdt
                || saved.cumulative_entry_usd != settlement.cumulative_entry_usd
                || saved.settlement_revision != settlement.settlement_revision
                || saved.source_digest != settlement.source_digest
                || prior.committed_at != saved.venue_update_ms
            {
                return Err(StoreError::Invalid("ENTRY_SETTLEMENT_IDEMPOTENCY_CONFLICT"));
            }
            return Ok(saved);
        }
        let encoded = serde_json::to_string(&settlement)
            .map_err(|_| StoreError::Invalid("ENTRY_SETTLEMENT_SERIALIZATION_FAILED"))?;
        if terminal {
            // The canonical terminal receipt is the ONLY amount counted by
            // historical campaign reconstruction; earlier partial receipts
            // never appear as extra historical entry purchases.
            Self::persist_entry_settlement_record_sql(
                tx,
                scope,
                &(key),
                "ENTRY_SETTLEMENT_PROVEN",
                &settlement.plan_hash,
                &encoded,
                settlement.venue_update_ms,
                false,
            )?;
        } else {
            let revision_key = format!("entry-partial:{id}:r{}", settlement.settlement_revision);
            Self::persist_entry_settlement_record_sql(
                tx,
                scope,
                &(revision_key),
                "ENTRY_PARTIAL_SETTLEMENT_PROVEN",
                &settlement.plan_hash,
                &encoded,
                settlement.venue_update_ms,
                false,
            )?;
            Self::persist_entry_settlement_record_sql(
                tx,
                scope,
                &(partial_head_key),
                "ENTRY_PARTIAL_HEAD",
                &settlement.plan_hash,
                &encoded,
                settlement.venue_update_ms,
                true,
            )?;
        }
        Ok(settlement)
    }
    pub(crate) fn bind_late_entry_settlement_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: Option<(&str, &str)>,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        let db: &Connection = tx;
        let meta = Self::entry_settlement_identity_sql(db, scope)?;
        let terminal_key = format!("entry-settlement:{id}");
        let receipt = Self::operation_sql(db, scope, &terminal_key)?
            .ok_or(StoreError::Invalid("LATE_ENTRY_TERMINAL_RECEIPT_MISSING"))?;
        if receipt.kind != "ENTRY_SETTLEMENT_PROVEN" {
            return Err(StoreError::Invalid("LATE_ENTRY_TERMINAL_RECEIPT_INVALID"));
        }
        let terminal: EntrySettlementEvidence = serde_json::from_str(&receipt.result_ref)
            .map_err(|_| StoreError::Invalid("LATE_ENTRY_TERMINAL_RECEIPT_INVALID"))?;
        if terminal.intent_id != id
            || terminal.venue_update_ms != venue_updated_at
            || terminal.venue_filled_quantity != venue_filled_quantity
            || terminal.source_covered_through_ms > covered_through
        {
            return Err(StoreError::Invalid("LATE_ENTRY_VENUE_IDENTITY_CHANGED"));
        }
        // Re-attest the immutable terminal source and admission before
        // creating any new money proof, including retries after a lost ACK.
        Self::bind_terminal_entry_settlement_sql(
            tx,
            scope,
            id,
            venue_updated_at,
            venue_filled_quantity,
            covered_through,
        )?;
        let admission = Self::entry_admission_sql(db, scope, id)?
            .ok_or(StoreError::Invalid("LATE_ENTRY_ADMISSION_MISSING"))?;
        let (base_extra, _) = Self::intent_additional_entry_fee_evidence_sql(
            db,
            scope,
            id,
            terminal.source_covered_through_ms,
        )?;
        let (all_extra, extra_digest) =
            Self::intent_additional_entry_fee_evidence_sql(db, scope, id, covered_through)?;
        if all_extra < base_extra {
            return Err(StoreError::Invalid("LATE_ENTRY_FEE_REGRESSION"));
        }
        let added = all_extra
            .checked_sub(base_extra)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        let total = terminal
            .booked_notional_and_fee_usdt
            .checked_add(added)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        let usd = total
            .checked_mul(admission.admission_usd_per_usdt)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
            .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity)
            .checked_add(admission.other_entry_costs_upper_usd)
            .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
        let head_key = format!("entry-late-head:{id}");
        let previous = Self::operation_sql(db, scope, &head_key)?;
        let old = previous
            .as_ref()
            .map(|r| {
                serde_json::from_str::<EntrySettlementEvidence>(&r.result_ref)
                    .map_err(|_| StoreError::Invalid("LATE_ENTRY_HEAD_CORRUPT"))
            })
            .transpose()?;
        if let Some(prior) = old.as_ref() {
            let witness = Self::operation_sql(
                db,
                scope,
                &format!("entry-late:{id}:r{}", prior.settlement_revision),
            )?
            .ok_or(StoreError::Invalid("LATE_ENTRY_REVISION_MISSING"))?;
            if prior.schema != "okx.entry-late-settlement/v1"
                || prior.intent_id != id
                || prior.plan_hash != terminal.plan_hash
                || prior.reservation_key != terminal.reservation_key
                || prior.venue_order_id != terminal.venue_order_id
                || prior.venue_filled_quantity != terminal.venue_filled_quantity
                || witness.kind != "ENTRY_LATE_SETTLEMENT_PROVEN"
                || witness.result_ref != previous.as_ref().unwrap().result_ref
                || witness.request_hash != terminal.plan_hash
            {
                return Err(StoreError::Invalid("LATE_ENTRY_RECEIPT_IDENTITY_CHANGED"));
            }
            let (at_extra, prior_digest) = Self::intent_additional_entry_fee_evidence_sql(
                db,
                scope,
                id,
                prior.source_covered_through_ms,
            )?;
            let prior_total = terminal
                .booked_notional_and_fee_usdt
                .checked_add(
                    at_extra
                        .checked_sub(base_extra)
                        .ok_or(StoreError::Invalid("LATE_ENTRY_FEE_REGRESSION"))?,
                )
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            let prior_usd = prior_total
                .checked_mul(admission.admission_usd_per_usdt)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?
                .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity)
                .checked_add(admission.other_entry_costs_upper_usd)
                .ok_or(StoreError::Invalid("MONEY_OVERFLOW"))?;
            let prior_bytes = serde_json::to_vec(&(
                "okx.entry-late-settlement/v1",
                terminal.source_digest.as_str(),
                meta.account_key.as_str(),
                id,
                terminal.plan_hash.as_str(),
                terminal.venue_order_id.as_str(),
                prior.source_covered_through_ms,
                prior_total,
                prior_usd,
                prior_digest,
            ))
            .map_err(|_| StoreError::Invalid("LATE_ENTRY_SOURCE_SERIALIZATION_FAILED"))?;
            if prior_total != prior.booked_notional_and_fee_usdt
                || prior_usd != prior.cumulative_entry_usd
                || format!("{:x}", Sha256::digest(prior_bytes)) != prior.source_digest
            {
                return Err(StoreError::Invalid("LATE_ENTRY_PRIOR_SOURCE_CHANGED"));
            }
        }
        let previous_amount = old
            .as_ref()
            .map_or(terminal.cumulative_entry_usd, |v| v.cumulative_entry_usd);
        if usd < previous_amount {
            return Err(StoreError::Invalid("LATE_ENTRY_FEE_REGRESSION"));
        }
        if usd == previous_amount {
            return Ok(old.unwrap_or(terminal));
        }
        if covered_through
            <= old
                .as_ref()
                .map_or(terminal.source_covered_through_ms, |v| {
                    v.source_covered_through_ms
                })
        {
            return Err(StoreError::Invalid("LATE_ENTRY_FRONTIER_NOT_ADVANCED"));
        }
        let revision = old
            .as_ref()
            .map_or(terminal.settlement_revision, |v| v.settlement_revision)
            .checked_add(1)
            .ok_or(StoreError::Invalid("LATE_ENTRY_REVISION_OVERFLOW"))?;
        let digest_bytes = serde_json::to_vec(&(
            "okx.entry-late-settlement/v1",
            terminal.source_digest.as_str(),
            meta.account_key.as_str(),
            id,
            terminal.plan_hash.as_str(),
            terminal.venue_order_id.as_str(),
            covered_through,
            total,
            usd,
            extra_digest,
        ))
        .map_err(|_| StoreError::Invalid("LATE_ENTRY_SOURCE_SERIALIZATION_FAILED"))?;
        let new = EntrySettlementEvidence {
            schema: "okx.entry-late-settlement/v1".into(),
            account_key: terminal.account_key,
            intent_id: id.to_owned(),
            plan_hash: terminal.plan_hash,
            reservation_key: terminal.reservation_key,
            venue_order_id: terminal.venue_order_id,
            venue_update_ms: venue_updated_at,
            source_covered_through_ms: covered_through,
            venue_filled_quantity,
            booked_notional_and_fee_usdt: total,
            cumulative_entry_usd: usd,
            settlement_revision: revision,
            source_digest: format!("{:x}", Sha256::digest(digest_bytes)),
        };
        let encoded = serde_json::to_string(&new)
            .map_err(|_| StoreError::Invalid("LATE_ENTRY_SERIALIZATION_FAILED"))?;
        // Current Native installations already own their immutable guards;
        // keep financial settlement DML-only. Historical fixtures retain
        // their existing initialization behaviour.
        let native: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='finance_installation')",
            [], |r| r.get(0),
        )?;
        if !native {
            db.execute_batch(
                "CREATE TRIGGER IF NOT EXISTS immutable_entry_late_update
             BEFORE UPDATE ON operation_receipts
             WHEN OLD.kind IN ('ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED')
             BEGIN SELECT RAISE(ABORT,'IMMUTABLE_LATE_ENTRY'); END;
             CREATE TRIGGER IF NOT EXISTS immutable_entry_late_delete
             BEFORE DELETE ON operation_receipts
             WHEN OLD.kind IN ('ENTRY_LATE_SETTLEMENT_PROVEN','ENTRY_LATE_SETTLED')
             BEGIN SELECT RAISE(ABORT,'IMMUTABLE_LATE_ENTRY'); END;",
            )?;
        }
        Self::persist_entry_settlement_record_sql(
            tx,
            scope,
            &(format!("entry-late:{id}:r{revision}")),
            "ENTRY_LATE_SETTLEMENT_PROVEN",
            &new.plan_hash,
            &encoded,
            covered_through,
            false,
        )?;
        Self::persist_entry_settlement_record_sql(
            tx,
            scope,
            &(head_key),
            "ENTRY_LATE_HEAD",
            &new.plan_hash,
            &encoded,
            covered_through,
            true,
        )?;
        Ok(new)
    }
}
