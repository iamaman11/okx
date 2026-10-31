use super::super::*;
use sha2::{Digest, Sha256};
impl AccountStore {
    /// No second ledger/table: one immutable, idempotent record in the existing
    /// operation_receipts journal, bound to the original saved intent/plan.
    /// Its kind explicitly denies any claim of ALL-IN cost authorization.
    fn verified_stored_entry_quote(&self, quote: &EntryQuoteEvidence) -> StoreResult<StoredIntent> {
        let intent = self
            .intent(&quote.intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_INTENT_MISSING"))?;
        let maker = Decimal::from_str(&quote.maker_rate)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_FEE_RATE_INVALID"))?;
        let taker = Decimal::from_str(&quote.taker_rate)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_FEE_RATE_INVALID"))?;
        if quote.schema != "okx.entry-known-quote/v1"
            || quote.account_key != self.meta.account_key
            || quote.installation_id != self.meta.installation_id
            || quote.plan_hash != intent.plan_hash
            || quote.policy_revision != intent.policy_revision
            || quote.instrument_id != intent.instrument_id
            || !intent.risk_increase
            || quote.quantity != intent.plan.quantity
            || quote.limit_price != intent.plan.limit_price
            || quote.contract_value != intent.plan.contract_value
            || quote.quantity <= Decimal::ZERO
            || quote.limit_price <= Decimal::ZERO
            || quote.contract_value <= Decimal::ZERO
            || quote.fee_rate < Decimal::ZERO
            || quote.fee_rate != maker.abs().max(taker.abs())
            || quote.fee_level.is_empty()
            || quote.fee_exchange_at <= 0
            || quote.usd_per_usdt <= Decimal::ZERO
            || quote.fee_received_at <= 0
            || quote.conversion_received_at <= 0
            || quote.observed_at <= 0
            || quote.fee_received_at > quote.observed_at
            || quote.fee_exchange_at > quote.observed_at
            || quote.conversion_received_at > quote.observed_at
            || quote.observed_at - quote.fee_received_at > 5_000
            || quote.observed_at - quote.fee_exchange_at > 5_000
            || quote.observed_at - quote.conversion_received_at > 5_000
            || quote.fee_generation.len() != 71
            || !quote.fee_generation.starts_with("sha256:")
            || !quote.fee_generation[7..]
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
            || quote.reference_generation.is_empty()
            || quote.conversion_digest.len() != 64
            || !quote
                .conversion_digest
                .bytes()
                .all(|c| c.is_ascii_hexdigit())
        {
            return Err(StoreError::Invalid("ENTRY_QUOTE_SCOPE_UNVERIFIED"));
        }
        let tariff_bytes = serde_json::to_vec(&(
            &quote.instrument_id,
            &quote.reference_generation,
            &quote.fee_level,
            &quote.maker_rate,
            &quote.taker_rate,
            true,
        ))
        .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_FEE_HASH_INVALID"))?;
        let tariff_digest = format!("sha256:{:x}", Sha256::digest(tariff_bytes));
        if tariff_digest != quote.fee_generation {
            return Err(StoreError::Invalid("ENTRY_QUOTE_FEE_HASH_INVALID"));
        }
        let notional = quote
            .quantity
            .checked_mul(quote.contract_value)
            .and_then(|v| v.checked_mul(quote.limit_price))
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?;
        let fee = notional
            .checked_mul(quote.fee_rate)
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToPositiveInfinity);
        let total_usdt = notional
            .checked_add(fee)
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?;
        let usd = total_usdt
            .checked_mul(quote.usd_per_usdt)
            .ok_or(StoreError::Invalid("ENTRY_QUOTE_OVERFLOW"))?
            .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity);
        if (notional, fee, total_usdt, usd)
            != (
                quote.notional_usdt,
                quote.fee_usdt,
                quote.total_usdt,
                quote.total_usd,
            )
        {
            return Err(StoreError::Invalid("ENTRY_QUOTE_ARITHMETIC_MISMATCH"));
        }
        Ok(intent)
    }

    pub fn bind_entry_quote(&mut self, quote: &EntryQuoteEvidence) -> StoreResult<()> {
        let intent = self.verified_stored_entry_quote(quote)?;
        if intent.state != IntentState::Prepared {
            return Err(StoreError::Invalid("ENTRY_QUOTE_INTENT_STATE_INVALID"));
        }
        let name = format!("entry-quote:{}", intent.intent_id);
        let encoded = serde_json::to_string(quote)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_SERIALIZATION_FAILED"))?;
        if let Some(prior) = self.operation(&name)? {
            if prior.kind != "ENTRY_QUOTE_UNPROVEN"
                || prior.request_hash != intent.plan_hash
                || prior.result_ref != encoded
            {
                return Err(StoreError::Invalid("ENTRY_QUOTE_IDEMPOTENCY_CONFLICT"));
            }
            return Ok(());
        }
        self.connection.execute(
            "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'ENTRY_QUOTE_UNPROVEN',?2,?3,?4)",
            params![name, intent.plan_hash, encoded, quote.observed_at],
        )?;
        Ok(())
    }

    pub fn entry_quote(&self, intent_id: &str) -> StoreResult<Option<EntryQuoteEvidence>> {
        let name = format!("entry-quote:{intent_id}");
        let Some(receipt) = self.operation(&name)? else {
            return Ok(None);
        };
        if receipt.kind != "ENTRY_QUOTE_UNPROVEN" {
            return Err(StoreError::Invalid("ENTRY_QUOTE_INVALID_RECEIPT"));
        }
        let quote: EntryQuoteEvidence = serde_json::from_str(&receipt.result_ref)
            .map_err(|_| StoreError::Invalid("ENTRY_QUOTE_CORRUPT"))?;
        if quote.intent_id != intent_id
            || quote.plan_hash != receipt.request_hash
            || quote.observed_at != receipt.committed_at
        {
            return Err(StoreError::Invalid("ENTRY_QUOTE_CORRUPT"));
        }
        // Revalidate against the actual durable plan and exact arithmetic on
        // EVERY read, including after Windows restart and v2 DB reopen.
        self.verified_stored_entry_quote(&quote)?;
        Ok(Some(quote))
    }
    /// Validate the independently produced native cost proof against the
    /// immutable quote on EVERY read, including after a Windows restart.
    /// The digest here is a binding to the native source witness, not a
    /// cryptographic claim that arbitrary user-supplied evidence is trusted.
    fn verified_entry_admission(&self, proof: &EntryAdmissionEvidence) -> StoreResult<()> {
        let quote = self
            .entry_quote(&proof.intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_QUOTE_MISSING"))?;
        let bytes = serde_json::to_vec(&quote)
            .map_err(|_| StoreError::Invalid("ENTRY_ADMISSION_QUOTE_DIGEST_INVALID"))?;
        let expected_quote_digest = format!("{:x}", Sha256::digest(bytes));
        let computed = quote
            .total_usdt
            .checked_mul(proof.admission_usd_per_usdt)
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_UPPER_OVERFLOW"))?
            .round_dp_with_strategy(9, rust_decimal::RoundingStrategy::ToPositiveInfinity)
            .checked_add(proof.other_entry_costs_upper_usd)
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_UPPER_OVERFLOW"))?;
        let digest_valid = |v: &str| {
            v.len() == 64
                && v.bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        };
        if proof.schema != "okx.entry-admission/v2"
            || proof.account_key != self.meta.account_key
            || proof.installation_id != self.meta.installation_id
            || proof.intent_id != quote.intent_id
            || proof.plan_hash != quote.plan_hash
            || !digest_valid(&proof.quote_digest)
            || proof.quote_digest != expected_quote_digest
            || !digest_valid(&proof.entry_proof_digest)
            || proof.admission_usd_per_usdt != quote.usd_per_usdt
            || proof.admission_usd_per_usdt <= Decimal::ZERO
            || proof.other_entry_costs_upper_usd < Decimal::ZERO
            || proof.entry_upper_usd != computed
            || proof.entry_upper_usd < quote.total_usd
            || proof.entry_upper_usd <= Decimal::ZERO
            || proof.observed_at < quote.observed_at
            || proof.observed_at - quote.observed_at > 5_000
        {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_SOURCE_UNVERIFIED"));
        }
        Ok(())
    }

    /// A fully admitted entry is a native, source-bound quote + USD upper +
    /// proof digest. Replays are exact; source tampering is never an update.
    pub fn bind_entry_admission(&mut self, proof: &EntryAdmissionEvidence) -> StoreResult<()> {
        self.verified_entry_admission(proof)?;
        let key = format!("entry-admission:{}", proof.intent_id);
        let body = serde_json::to_string(proof)
            .map_err(|_| StoreError::Invalid("ENTRY_ADMISSION_SERIALIZATION_FAILED"))?;
        if let Some(prior) = self.operation(&key)? {
            if prior.kind != "ENTRY_ADMISSION_PROVEN"
                || prior.request_hash != proof.plan_hash
                || prior.result_ref != body
                || prior.committed_at != proof.observed_at
            {
                return Err(StoreError::Invalid("ENTRY_ADMISSION_IDEMPOTENCY_CONFLICT"));
            }
            return Ok(());
        }
        let intent = self
            .intent(&proof.intent_id)?
            .ok_or(StoreError::Invalid("ENTRY_ADMISSION_INTENT_MISSING"))?;
        if intent.state != IntentState::Prepared {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_INTENT_STATE_INVALID"));
        }
        self.connection.execute(
            "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
             VALUES(?1,'ENTRY_ADMISSION_PROVEN',?2,?3,?4)",
            params![key, proof.plan_hash, body, proof.observed_at],
        )?;
        Ok(())
    }

    pub fn entry_admission(&self, id: &str) -> StoreResult<Option<EntryAdmissionEvidence>> {
        let key = format!("entry-admission:{id}");
        let Some(receipt) = self.operation(&key)? else {
            return Ok(None);
        };
        if receipt.kind != "ENTRY_ADMISSION_PROVEN" {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_INVALID_RECEIPT"));
        }
        let proof: EntryAdmissionEvidence = serde_json::from_str(&receipt.result_ref)
            .map_err(|_| StoreError::Invalid("ENTRY_ADMISSION_CORRUPT"))?;
        if proof.intent_id != id
            || proof.plan_hash != receipt.request_hash
            || proof.observed_at != receipt.committed_at
        {
            return Err(StoreError::Invalid("ENTRY_ADMISSION_CORRUPT"));
        }
        self.verified_entry_admission(&proof)?;
        Ok(Some(proof))
    }

    /// E1.4 final source: only a terminal, independently read-back exchange
    /// order whose complete fills and fee bills exist in the sole SQL owner
    /// can produce a cumulative USD charge. Partial/zero-fill or stale pages
    /// do NOT create a terminal accounting claim. The up-front source-bound
    /// Frozen admission FX and separately verified entry fees come from the SAME durable admission
    /// used for Worker RESERVE; no observed spot FX is promoted here.
    pub fn bind_terminal_entry_settlement(
        &mut self,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        self.bind_cumulative_entry_settlement(id, venue_updated_at, venue_filled_quantity, covered_through, true)
    }

    /// Incremental fills and their independently linked bills are settled
    /// BEFORE terminal release. No new journal, scheduler or funding formula:
    /// this is the same admission FX and source ledger as terminal settlement.
    pub fn bind_partial_entry_settlement(
        &mut self,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
    ) -> StoreResult<EntrySettlementEvidence> {
        self.bind_cumulative_entry_settlement(id, venue_updated_at, venue_filled_quantity, covered_through, false)
    }

    fn bind_cumulative_entry_settlement(
        &mut self,
        id: &str,
        venue_updated_at: i64,
        venue_filled_quantity: Decimal,
        covered_through: i64,
        terminal: bool,
    ) -> StoreResult<EntrySettlementEvidence> {
        let intent = self
            .intent(id)?
            .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_INTENT_MISSING"))?;
        if (terminal && !matches!(intent.state, IntentState::Filled | IntentState::Cancelled))
            || (!terminal && !matches!(intent.state, IntentState::PartiallyFilled | IntentState::Filled | IntentState::Cancelled))
            || venue_updated_at <= 0
            || covered_through < venue_updated_at
            || venue_filled_quantity <= Decimal::ZERO
            || venue_filled_quantity > intent.plan.quantity
        {
            return Err(StoreError::Invalid(
                "ENTRY_SETTLEMENT_TERMINAL_SOURCE_UNVERIFIED",
            ));
        }
        let admission = self
            .entry_admission(id)?
            .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_ADMISSION_MISSING"))?;
        let began_post: Option<i64> = self.connection.query_row(
            "SELECT begin_post_at FROM intents WHERE intent_id=?1",
            [id],
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
        let terminal_receipt = self.operation(&key)?;
        let partial_head_key = format!("entry-partial-head:{id}");
        let partial_head = self.operation(&partial_head_key)?;
        let previous = terminal_receipt.as_ref().or(partial_head.as_ref());
        let saved = previous
            .map(|receipt| {
                serde_json::from_str::<EntrySettlementEvidence>(&receipt.result_ref)
                    .map_err(|_| StoreError::Invalid("ENTRY_SETTLEMENT_RECEIPT_CORRUPT"))
            })
            .transpose()?;
        if let Some(old) = saved.as_ref() {
            if old.intent_id != id || old.plan_hash != intent.plan_hash
                || old.account_key != self.meta.account_key
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
                let witness = self.operation(&format!("entry-partial:{id}:r{}", old.settlement_revision))?
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
            let historical = self.intent_fee_bill_evidence(id, old.source_covered_through_ms)?;
            if historical.fill.filled_quantity != old.venue_filled_quantity
                || historical.fill.gross_notional_usdt
                    .checked_add(historical.conservative_verified_fee_usdt)
                    != Some(old.booked_notional_and_fee_usdt)
                || historical.fill.exchange_order_id != old.venue_order_id
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
        let bill = self.intent_fee_bill_evidence(id, witness_through)?;
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
        let central = self.central_receipt(id)?.ok_or(StoreError::Invalid(
            "ENTRY_SETTLEMENT_CENTRAL_RECEIPT_MISSING",
        ))?;
        if central.reservation_key != fill.reservation_key
            || central.account_key != admission.account_key
            || central.plan_hash != admission.plan_hash
        {
            return Err(StoreError::Invalid("ENTRY_SETTLEMENT_RESERVATION_MISMATCH"));
        }
        let full_usdt = fill
            .gross_notional_usdt
            .checked_add(bill.conservative_verified_fee_usdt)
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
            if terminal_receipt.is_some() { old.settlement_revision }
            else if !terminal && old.cumulative_entry_usd == usd
                && old.venue_filled_quantity == venue_filled_quantity {
                // A later cursor with no new economic amount is a read-only
                // acknowledgement, not an additional debit or book revision.
                return Ok(old.clone());
            } else {
                if usd < old.cumulative_entry_usd
                    || venue_filled_quantity < old.venue_filled_quantity {
                    return Err(StoreError::Invalid("ENTRY_SETTLEMENT_REGRESSION"));
                }
                old.settlement_revision.checked_add(1)
                    .ok_or(StoreError::Invalid("ENTRY_SETTLEMENT_REVISION_OVERFLOW"))?
            }
        } else { 1 };
        let schema = if terminal { "okx.entry-terminal-settlement/v1" }
            else { "okx.entry-partial-settlement/v1" };
        let bytes = serde_json::to_vec(&(
            schema,
            &self.meta.account_key,
            &self.meta.installation_id,
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
        .map_err(|_| StoreError::Invalid("ENTRY_SETTLEMENT_SOURCE_DIGEST_INVALID"))?;
        let settlement = EntrySettlementEvidence {
            schema: schema.into(),
            account_key: self.meta.account_key.clone(),
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
            self.connection.execute(
                "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,'ENTRY_SETTLEMENT_PROVEN',?2,?3,?4)",
                params![key, settlement.plan_hash, encoded, settlement.venue_update_ms],
            )?;
        } else {
            let revision_key = format!("entry-partial:{id}:r{}", settlement.settlement_revision);
            let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,'ENTRY_PARTIAL_SETTLEMENT_PROVEN',?2,?3,?4)",
                params![revision_key, settlement.plan_hash, encoded, settlement.venue_update_ms],
            )?;
            tx.execute(
                "INSERT INTO operation_receipts(operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,'ENTRY_PARTIAL_HEAD',?2,?3,?4)
                 ON CONFLICT(operation_id) DO UPDATE SET result_ref=excluded.result_ref,
                    committed_at=excluded.committed_at",
                params![partial_head_key, settlement.plan_hash, encoded, settlement.venue_update_ms],
            )?;
            tx.commit()?;
        }
        Ok(settlement)
    }

    /// Durable address of a central reservation attempt, committed before
    /// any WSS RESERVE. A crash or lost ACK cannot orphan the Worker grant.
    pub fn mark_reserve_attempt(&mut self, id: &str, key: &str, now: i64) -> StoreResult<()> {
        if key.is_empty() || key.len() > 512 {
            return Err(StoreError::Invalid("INVALID_RESERVE_IDENTITY"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, hash, begin): (String, String, Option<i64>) = tx.query_row(
            "SELECT state,request_hash,begin_post_at FROM intents WHERE intent_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if state != "PREPARED" || begin.is_some() {
            return Err(StoreError::Invalid("INVALID_RESERVE_ATTEMPT_STATE"));
        }
        let name = format!("risk-reserve:{id}");
        let previous: Option<(String, String, String)> = tx
            .query_row(
                "SELECT kind,request_hash,result_ref FROM operation_receipts WHERE operation_id=?1",
                [&name],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        if let Some((kind, prior_hash, prior_key)) = previous {
            if kind != "RISK_RESERVE_ATTEMPT" || prior_hash != hash || prior_key != key {
                return Err(StoreError::Invalid("RESERVE_ATTEMPT_IDEMPOTENCY_CONFLICT"));
            }
        } else {
            tx.execute(
                "INSERT INTO operation_receipts VALUES(?1,'RISK_RESERVE_ATTEMPT',?2,?3,?4)",
                params![name, hash, key, now],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// An explicit central DENIED plus an authenticated address-specific
    /// NOT_FOUND readback proves this attempt minted no reservation. Both
    /// the original reason and its no-risk marker commit atomically.
    pub fn reject_denied_reserve(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<()> {
        self.reject_unsent_inner(id, reason, now, true).map(|_| ())
    }

    pub fn declined_reserve(&self, id: &str) -> StoreResult<bool> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(
               SELECT 1 FROM intents i
               JOIN operation_receipts o ON o.operation_id='risk-reserve:'||i.intent_id
               JOIN operation_receipts t ON t.operation_id='terminal:'||i.intent_id
               WHERE i.intent_id=?1 AND i.state='REJECTED' AND i.begin_post_at IS NULL
                 AND o.kind='RISK_RESERVE_DENIED'
                 AND o.request_hash=i.request_hash
                 AND t.kind='REJECTED_UNSENT' AND t.request_hash=i.request_hash)",
            [id],
            |r| r.get(0),
        )?)
    }

    /// F1: the prepare transaction committed, but no central RESERVE was
    /// attempted. A matching terminal SQL receipt proves no native POST.
    pub fn unsent_before_reserve(&self, id: &str) -> StoreResult<bool> {
        self.connection
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM intents i
                   JOIN operation_receipts t ON t.operation_id='terminal:'||i.intent_id
                   WHERE i.intent_id=?1 AND i.state='REJECTED'
                     AND i.begin_post_at IS NULL AND i.exchange_order_id IS NULL
                     AND t.kind='REJECTED_UNSENT'
                     AND t.request_hash=i.request_hash AND t.result_ref=i.incident
                     AND NOT EXISTS(SELECT 1 FROM risk_receipts r
                       WHERE r.intent_id=i.intent_id)
                     AND NOT EXISTS(SELECT 1 FROM operation_receipts a
                       WHERE a.operation_id='risk-reserve:'||i.intent_id))",
                [id],
                |r| r.get(0),
            )
            .map_err(StoreError::from)
    }

    pub fn reconcilable_intents(&self, limit: usize) -> StoreResult<Vec<StoredIntent>> {
        self.reconcilable_intents_page(limit, 0)
    }

    /// Bounded, addressable paging prevents a failed early intent from starving
    /// later account intents. Offset is a scheduling cursor, not a second owner.
    pub fn reconcilable_intents_page(
        &self,
        limit: usize,
        offset: usize,
    ) -> StoreResult<Vec<StoredIntent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let ids = {
            let mut q=self.connection.prepare(
              "SELECT intent_id FROM (
                SELECT intent_id,CASE WHEN state='UNSENT_PENDING_CENTRAL' THEN 0 ELSE 3 END AS priority
                FROM risk_receipts
                WHERE state IN('HELD','POSTED','UNKNOWN','OPEN','TERMINAL_PENDING_CENTRAL','UNSENT_PENDING_CENTRAL')
                UNION ALL
                SELECT i.intent_id,1 AS priority FROM intents i
                JOIN operation_receipts o ON o.operation_id='risk-reserve:'||i.intent_id
                WHERE o.kind='RISK_RESERVE_ATTEMPT' AND o.request_hash=i.request_hash
                  AND i.state IN('PREPARED','REJECTED')
                  AND NOT EXISTS(SELECT 1 FROM risk_receipts r WHERE r.intent_id=i.intent_id)
                  AND NOT EXISTS(SELECT 1 FROM operation_receipts ack
                    WHERE ack.operation_id='central-release:'||o.result_ref
                      AND ack.kind='CENTRAL_RELEASE')
                UNION ALL
                SELECT i.intent_id,2 AS priority FROM intents i
                WHERE i.state='PREPARED' AND i.deadline<=?2
                  AND NOT EXISTS(SELECT 1 FROM risk_receipts r
                    WHERE r.intent_id=i.intent_id)
                  AND NOT EXISTS(SELECT 1 FROM operation_receipts a
                    WHERE a.operation_id='risk-reserve:'||i.intent_id)
               ) GROUP BY intent_id ORDER BY MIN(priority),intent_id LIMIT ?1 OFFSET ?3"
            )?;
            q.query_map(
                params![
                    limit.min(100) as i64,
                    wall_time_ms(),
                    offset.min(100_000) as i64
                ],
                |r| r.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?
        };
        ids.iter()
            .map(|id| {
                self.intent(id)?
                    .ok_or(StoreError::Invalid("INTENT_MISSING"))
            })
            .collect()
    }
    pub fn acknowledge_central_release(&mut self, key: &str, evidence: &str) -> StoreResult<()> {
        if evidence.is_empty() {
            return Err(StoreError::Invalid("CENTRAL_RELEASE_EVIDENCE_REQUIRED"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let hash: String = tx.query_row(
            "SELECT receipt_hash FROM risk_receipts WHERE reservation_key=?1
             UNION ALL SELECT request_hash FROM operation_receipts
               WHERE kind='RISK_RESERVE_ATTEMPT' AND result_ref=?1
             LIMIT 1",
            [key],
            |r| r.get(0),
        )?;
        tx.execute(
            "UPDATE risk_receipts SET state='RELEASED' WHERE reservation_key=?1",
            [key],
        )?;
        tx.execute("INSERT INTO operation_receipts VALUES(?1,'CENTRAL_RELEASE',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING",params![format!("central-release:{key}"),hash,evidence,wall_time_ms()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn reject_unsent(&mut self, id: &str, reason: &str, now: i64) -> StoreResult<bool> {
        self.reject_unsent_inner(id, reason, now, false)
    }

    fn reject_unsent_inner(
        &mut self,
        id: &str,
        reason: &str,
        now: i64,
        proven_not_reserved: bool,
    ) -> StoreResult<bool> {
        if reason.is_empty() {
            return Err(StoreError::Invalid("UNSENT_REASON_REQUIRED"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, begin, hash): (String, Option<i64>, String) = tx.query_row(
            "SELECT state,begin_post_at,request_hash FROM intents WHERE intent_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if begin.is_some() {
            return Err(StoreError::Invalid("POST_OUTCOME_REQUIRES_RECONCILIATION"));
        }
        let first = state != "REJECTED";
        if first {
            if !matches!(state.as_str(), "PREPARED" | "RESERVED") {
                return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
            }
            tx.execute("UPDATE intents SET state='REJECTED',incident=?2,updated_at=?3,revision=revision+1 WHERE intent_id=?1",params![id,reason,now])?;
            tx.execute(
                "UPDATE risk_receipts SET state='UNSENT_PENDING_CENTRAL' WHERE intent_id=?1",
                [id],
            )?;
            tx.execute(
                "INSERT INTO operation_receipts VALUES(?1,'REJECTED_UNSENT',?2,?3,?4)",
                params![format!("terminal:{id}"), hash, reason, now],
            )?;
        }
        if proven_not_reserved {
            let original: Option<(String, String)> = tx
                .query_row(
                    "SELECT kind,request_hash FROM operation_receipts WHERE operation_id=?1",
                    [format!("terminal:{id}")],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if original
                .as_ref()
                .is_none_or(|(kind, request)| kind != "REJECTED_UNSENT" || request != &hash)
            {
                return Err(StoreError::Invalid("UNSENT_TERMINAL_RECEIPT_MISSING"));
            }
            let operation = format!("risk-reserve:{id}");
            let changed = tx.execute(
                "UPDATE operation_receipts SET kind='RISK_RESERVE_DENIED'
                 WHERE operation_id=?1
                   AND kind IN('RISK_RESERVE_ATTEMPT','RISK_RESERVE_DENIED')",
                [&operation],
            )?;
            if changed != 1 {
                return Err(StoreError::Invalid("RESERVE_ATTEMPT_MARKER_MISSING"));
            }
        }
        tx.commit()?;
        Ok(first)
    }
    /// A no-send release requires the existing durable rejection receipt,
    /// matching the original request, with no committed post claim.
    /// A gateway timeout or venue rejection can never produce this witness.
    pub fn unsent_release_receipt(&self, id: &str) -> StoreResult<Option<UnsentSqlReceipt>> {
        self.connection
            .query_row(
                "SELECT COALESCE(r.reservation_key,a.result_ref),
                        COALESCE(r.account_key,?2),
                        i.intent_id,i.plan_hash,i.request_hash,i.incident
                 FROM intents i
                 LEFT JOIN risk_receipts r ON r.intent_id=i.intent_id
                 LEFT JOIN operation_receipts a ON a.operation_id='risk-reserve:'||i.intent_id
                    AND a.kind='RISK_RESERVE_ATTEMPT' AND a.request_hash=i.request_hash
                 JOIN operation_receipts o ON o.operation_id='terminal:'||i.intent_id
                 WHERE i.intent_id=?1 AND i.state='REJECTED'
                   AND i.begin_post_at IS NULL AND i.exchange_order_id IS NULL
                   AND (r.state IN('UNSENT_PENDING_CENTRAL','RELEASED') OR
                        (r.reservation_key IS NULL AND a.result_ref IS NOT NULL))
                   AND o.kind='REJECTED_UNSENT'
                   AND o.request_hash=i.request_hash AND o.result_ref=i.incident",
                params![id, self.meta.account_key],
                |r| {
                    Ok(UnsentSqlReceipt {
                        reservation_key: r.get(0)?,
                        account_key: r.get(1)?,
                        intent_id: r.get(2)?,
                        plan_hash: r.get(3)?,
                        request_hash: r.get(4)?,
                        reason: r.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(StoreError::from)
    }
    pub fn central_receipt(&self, id: &str) -> StoreResult<Option<CentralReceipt>> {
        let row=self.connection.query_row("SELECT reservation_key,account_key,installation_id,instance_id,plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest FROM risk_receipts WHERE intent_id=?1",[id],|r|Ok((CentralReceipt{reservation_key:r.get(0)?,account_key:r.get(1)?,installation_id:r.get(2)?,instance_id:r.get(3)?,intent_id:id.into(),plan_hash:r.get(4)?,policy_revision:r.get(5)?,receipt_hash:r.get(6)?,requested_loss_usd:Decimal::ZERO,conversion_digest:r.get(8)?},r.get::<_,String>(7)?))).optional()?;
        row.map(|(mut receipt, amount)| {
            receipt.requested_loss_usd = decimal(amount)?;
            Ok(receipt)
        })
        .transpose()
    }
    /// Only a central reservation still charged to the risk book can cover
    /// exchange exposure. A historically retained RELEASED SQL receipt is
    /// not permission to consider its position/order centrally reserved.
    pub fn active_central_receipt(&self, id: &str) -> StoreResult<Option<CentralReceipt>> {
        let state: Option<String> = self
            .connection
            .query_row(
                "SELECT state FROM risk_receipts WHERE intent_id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if state.as_deref().is_some_and(risk_receipt_covers_exposure) {
            self.central_receipt(id)
        } else {
            Ok(None)
        }
    }
    pub fn by_exchange_order_id(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("exchange_order_id", key)
    }
    pub fn prepare(
        &mut self,
        decision: &Decision,
        intent: &NewIntent,
    ) -> StoreResult<StoredIntent> {
        if intent.plan.contract_value <= Decimal::ZERO
            || intent.plan.settlement_currency.is_empty()
            || !matches!(intent.plan.position_side.as_str(), "long" | "short")
        {
            return Err(StoreError::Invalid("INVALID_CONTRACT_MECHANICS"));
        }
        if let Some(existing) = self.by_idempotency(&intent.idempotency_key)? {
            if existing.request_hash != intent.request_hash {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(existing);
        }
        if intent.deadline <= decision.decision_at
            || intent.policy_revision != decision.policy_revision
        {
            return Err(StoreError::Invalid("INVALID_INTENT_DEADLINE_OR_POLICY"));
        }
        if intent.plan.quantity <= Decimal::ZERO
            || intent.plan.limit_price <= Decimal::ZERO
            || intent.plan.planned_loss_usd < Decimal::ZERO
            || !matches!(intent.plan.side.as_str(), "buy" | "sell")
            || intent.plan.stop_price.is_some_and(|p| p <= Decimal::ZERO)
        {
            return Err(StoreError::Invalid("INVALID_INTENT_PLAN"));
        }
        let definition = self.strategy_definition(&intent.plan.strategy_instance)?;
        if definition.definition_id != intent.plan.strategy_definition_id
            || definition.version != intent.plan.strategy_definition_version
            || definition.parameters_digest != intent.plan.strategy_parameters_digest
        {
            return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
        }
        if decision
            .instance_id
            .as_ref()
            .is_some_and(|id| id != &intent.plan.strategy_instance)
        {
            return Err(StoreError::Invalid("STRATEGY_OWNER_MISMATCH"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Manual commands insert their decision with the intent. Autonomous
        // strategies instead reuse the exact pending SQL decision already
        // durably claimed before planning; no second decision is permitted.
        let claimed_strategy = decision.decision_id.starts_with("strategy:");
        if claimed_strategy {
            let prior: Option<(Option<String>, String, i64)> = tx
                .query_row(
                    "SELECT instance_id,input_digest,policy_revision FROM decisions WHERE decision_id=?1",
                    [&decision.decision_id],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            let (owner, digest, revision) =
                prior.ok_or(StoreError::Invalid("STRATEGY_CLAIM_MISSING"))?;
            if owner != decision.instance_id
                || digest != decision.input_digest
                || revision != decision.policy_revision
            {
                return Err(StoreError::Invalid("STRATEGY_CLAIM_MISMATCH"));
            }
            let already_bound: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM intents WHERE decision_id=?1)",
                [&decision.decision_id],
                |r| r.get(0),
            )?;
            if already_bound {
                return Err(StoreError::Invalid("STRATEGY_DECISION_ALREADY_BOUND"));
            }
        } else {
            tx.execute(
                "INSERT INTO decisions VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    decision.decision_id,
                    decision.instance_id,
                    decision.decision_at,
                    decision.input_digest,
                    decision.policy_revision,
                    decision.code_version
                ],
            )?;
        }
        tx.execute("INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at) VALUES(?1,?2,?3,?4,'PREPARED',0,?5,?6,?7,?8,?9,?10,?11,?11)",params![intent.intent_id,intent.idempotency_key,intent.request_hash,decision.decision_id,intent.client_order_id,intent.deadline,intent.plan_hash,intent.policy_revision,intent.risk_increase,intent.instrument_id,decision.decision_at])?;
        tx.execute(
            "INSERT INTO intent_plans VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)",
            params![
                intent.intent_id,
                intent.plan.operation,
                intent.plan.side,
                exact(intent.plan.quantity),
                exact(intent.plan.limit_price),
                intent.plan.stop_price.map(exact),
                intent.plan.strategy_instance,
                exact(intent.plan.planned_loss_usd),
                intent.plan.source_generation,
                intent.plan.position_side,
                intent.plan.private_source_generation,
                exact(intent.plan.contract_value),
                intent.plan.inverse,
                intent.plan.settlement_currency,
                intent.plan.strategy_definition_id, intent.plan.strategy_definition_version,intent.plan.strategy_parameters_digest
            ],
        )?;
        if claimed_strategy {
            // Binding and bar completion are one transaction. A crash before
            // commit leaves a retriable claim; after commit the intent itself
            // is the unique persisted decision outcome.
            tx.execute(
                "UPDATE strategy_instances SET last_decision_key=?2 WHERE instance_id=?1",
                params![intent.plan.strategy_instance, intent.idempotency_key],
            )?;
        }
        tx.commit()?;
        self.intent(&intent.intent_id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))
    }
    pub fn intent(&self, id: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("intent_id", id)
    }
    pub fn by_idempotency(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("idempotency_key", key)
    }
    pub fn by_client_order_id(&self, key: &str) -> StoreResult<Option<StoredIntent>> {
        self.find_intent("client_order_id", key)
    }
    pub fn operation(&self, id: &str) -> StoreResult<Option<OperationReceipt>> {
        Ok(self.connection.query_row("SELECT kind,request_hash,result_ref,committed_at FROM operation_receipts WHERE operation_id=?1",[id],|r|Ok(OperationReceipt{operation_id:id.into(),kind:r.get(0)?,request_hash:r.get(1)?,result_ref:r.get(2)?,committed_at:r.get(3)?})).optional()?)
    }
    // Durable marker is committed before any cancel/amend exchange mutation.
    pub fn begin_operation(
        &mut self,
        id: &str,
        hash: &str,
        kind: &str,
        now: i64,
    ) -> StoreResult<bool> {
        if let Some(old) = self.operation(id)? {
            if old.request_hash != hash || old.kind != kind {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(false);
        }
        self.connection.execute(
            "INSERT INTO operation_receipts VALUES(?1,?2,?3,'POSTING',?4)",
            params![id, kind, hash, now],
        )?;
        Ok(true)
    }
    pub fn operation_receipt(
        &mut self,
        id: &str,
        hash: &str,
        kind: &str,
        result: &str,
        now: i64,
    ) -> StoreResult<OperationReceipt> {
        if let Some(old) = self.operation(id)? {
            if old.request_hash != hash || old.kind != kind {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            if old.result_ref != "POSTING" && old.result_ref != result {
                return Err(StoreError::Invalid("OPERATION_RESULT_CONFLICT"));
            }
        }
        self.connection.execute("INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5) ON CONFLICT(operation_id) DO UPDATE SET result_ref=excluded.result_ref,committed_at=excluded.committed_at",params![id,kind,hash,result,now])?;
        self.operation(id)?
            .ok_or(StoreError::Invalid("OPERATION_MISSING"))
    }
    pub(super) fn find_intent(&self, column: &str, key: &str) -> StoreResult<Option<StoredIntent>> {
        let sql = format!(
            "SELECT intent_id,idempotency_key,request_hash,client_order_id,state,revision,deadline,plan_hash,policy_revision,risk_increase,instrument_id,exchange_order_id,incident FROM intents WHERE {column}=?1"
        );
        let raw = self
            .connection
            .query_row(&sql, [key], |r| {
                Ok((
                    StoredIntent {
                        plan: IntentPlan::default(),
                        intent_id: r.get(0)?,
                        idempotency_key: r.get(1)?,
                        request_hash: r.get(2)?,
                        client_order_id: r.get(3)?,
                        state: IntentState::Prepared,
                        revision: r.get(5)?,
                        deadline: r.get(6)?,
                        plan_hash: r.get(7)?,
                        policy_revision: r.get(8)?,
                        risk_increase: r.get(9)?,
                        instrument_id: r.get(10)?,
                        exchange_order_id: r.get(11)?,
                        incident: r.get(12)?,
                    },
                    r.get::<_, String>(4)?,
                ))
            })
            .optional()?;
        raw.map(|(mut i, s)| {
            i.state = IntentState::parse(&s)?;
            i.plan = self.plan(&i.intent_id)?;
            Ok(i)
        })
        .transpose()
    }
    pub(super) fn plan(&self, id: &str) -> StoreResult<IntentPlan> {
        let (mut p,q,price,stop,loss,cv)=self.connection.query_row("SELECT operation,side,quantity,limit_price,stop_price,strategy_instance,planned_loss_usd,source_generation,position_side,private_source_generation,contract_value,inverse,settlement_currency,strategy_definition_id,strategy_definition_version,strategy_parameters_digest FROM intent_plans WHERE intent_id=?1",[id],|r|Ok((IntentPlan{operation:r.get(0)?,side:r.get(1)?,strategy_instance:r.get(5)?,source_generation:r.get(7)?,position_side:r.get(8)?,private_source_generation:r.get(9)?,inverse:r.get(11)?,settlement_currency:r.get(12)?,strategy_definition_id:r.get(13)?,strategy_definition_version:r.get(14)?,strategy_parameters_digest:r.get(15)?,..Default::default()},r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,Option<String>>(4)?,r.get::<_,String>(6)?,r.get::<_,String>(10)?)))?;
        p.contract_value = decimal(cv)?;
        p.quantity = decimal(q)?;
        p.limit_price = decimal(price)?;
        p.stop_price = stop.map(decimal).transpose()?;
        p.planned_loss_usd = decimal(loss)?;
        Ok(p)
    }
    pub fn reserve(&mut self, receipt: &CentralReceipt, now: i64) -> StoreResult<()> {
        let i = self
            .intent(&receipt.intent_id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        if receipt.account_key != self.meta.account_key
            || receipt.installation_id != self.meta.installation_id
            || receipt.plan_hash != i.plan_hash
            || receipt.policy_revision != i.policy_revision
            || receipt.receipt_hash.is_empty()
            || receipt.requested_loss_usd < Decimal::ZERO
            || receipt.conversion_digest.is_empty()
        {
            return Err(StoreError::Invalid("CENTRAL_RECEIPT_MISMATCH"));
        }
        if i.state == IntentState::Reserved {
            let saved: String = self.connection.query_row(
                "SELECT receipt_hash FROM risk_receipts WHERE reservation_key=?1",
                [&receipt.reservation_key],
                |r| r.get(0),
            )?;
            return if saved == receipt.receipt_hash {
                Ok(())
            } else {
                Err(StoreError::Invalid("CENTRAL_RECEIPT_CONFLICT"))
            };
        }
        if i.state != IntentState::Prepared {
            return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
        }
        let owner: Option<String> = self.connection.query_row(
            "SELECT instance_id FROM decisions JOIN intents USING(decision_id) WHERE intent_id=?1",
            [&i.intent_id],
            |r| r.get(0),
        )?;
        if owner != receipt.instance_id {
            return Err(StoreError::Invalid("CENTRAL_INSTANCE_MISMATCH"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO risk_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'HELD')",
            params![
                receipt.reservation_key,
                receipt.intent_id,
                receipt.account_key,
                receipt.installation_id,
                receipt.instance_id,
                receipt.plan_hash,
                receipt.policy_revision,
                receipt.receipt_hash,
                exact(receipt.requested_loss_usd),
                receipt.conversion_digest
            ],
        )?;
        tx.execute("UPDATE intents SET state='RESERVED',revision=revision+1,updated_at=?2 WHERE intent_id=?1 AND state='PREPARED'",params![i.intent_id,now])?;
        tx.commit()?;
        Ok(())
    }
    // Returns true exactly once. The caller must never POST when this returns false/error.
    pub fn begin_post(&mut self, id: &str, policy_revision: i64, now: i64) -> StoreResult<bool> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, deadline, revision, increase): (String, i64, i64, bool) = tx.query_row(
            "SELECT state,deadline,policy_revision,risk_increase FROM intents WHERE intent_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        if state != "RESERVED" {
            return Ok(false);
        }
        if now >= deadline {
            return Err(StoreError::Invalid("COMMAND_EXPIRED"));
        }
        let (current, paused): (i64, bool) = tx.query_row(
            "SELECT revision,paused FROM account_policy WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if revision != policy_revision || current != revision {
            return Err(StoreError::Invalid("POLICY_CHANGED"));
        }
        if increase {
            let same:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM intent_plans p JOIN strategy_instances i ON i.instance_id=p.strategy_instance WHERE p.intent_id=?1 AND i.definition_id=p.strategy_definition_id AND i.definition_version=p.strategy_definition_version AND i.parameters_digest=p.strategy_parameters_digest AND (i.enabled=1 OR i.instance_id='manual'))",[id],|r|r.get(0))?;
            if !same {
                return Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"));
            }
            let stops: i64 = tx.query_row(
                "SELECT count(*) FROM risk_stops WHERE resolution IS NULL",
                [],
                |r| r.get(0),
            )?;
            if paused || stops > 0 {
                return Err(StoreError::Invalid("RISK_INCREASE_STOPPED"));
            }
        }
        if increase {
            let recovery: bool =
                tx.query_row("SELECT requires_reconcile FROM storage_recovery", [], |r| {
                    r.get(0)
                })?;
            if recovery {
                return Err(StoreError::Invalid("STORAGE_RECOVERY_INCOMPLETE"));
            }
        }
        if increase {
            let unknown: i64 = tx.query_row(
                "SELECT count(*) FROM intents WHERE state IN('POSTING','UNKNOWN')",
                [],
                |r| r.get(0),
            )?;
            if unknown > 0 {
                return Err(StoreError::Invalid("ACCOUNT_UNKNOWN_EXPOSURE"));
            }
        }
        tx.execute("UPDATE intents SET state='POSTING',begin_post_at=?2,updated_at=?2,revision=revision+1 WHERE intent_id=?1 AND state='RESERVED' AND begin_post_at IS NULL",params![id,now])?;
        tx.execute(
            "UPDATE risk_receipts SET state='POSTED' WHERE intent_id=?1",
            [id],
        )?;
        tx.commit()?;
        Ok(true)
    }
    // Exchange readback is authoritative; a timeout or negative lookup is UNKNOWN.
    pub fn record_outcome(
        &mut self,
        id: &str,
        state: IntentState,
        exchange_order_id: Option<&str>,
        evidence: &str,
        now: i64,
    ) -> StoreResult<()> {
        let i = self
            .intent(id)?
            .ok_or(StoreError::Invalid("INTENT_MISSING"))?;
        let allowed = match i.state {
            IntentState::Posting | IntentState::Unknown => matches!(
                state,
                IntentState::Unknown
                    | IntentState::Acknowledged
                    | IntentState::PartiallyFilled
                    | IntentState::Filled
                    | IntentState::Cancelled
                    | IntentState::Rejected
            ),
            IntentState::Acknowledged => matches!(
                state,
                IntentState::Acknowledged
                    | IntentState::PartiallyFilled
                    | IntentState::Filled
                    | IntentState::Cancelled
            ),
            IntentState::PartiallyFilled => matches!(
                state,
                IntentState::PartiallyFilled | IntentState::Filled | IntentState::Cancelled
            ),
            _ => i.state == state,
        };
        if !allowed || evidence.is_empty() {
            return Err(StoreError::Invalid("INVALID_INTENT_TRANSITION"));
        }
        if i.exchange_order_id
            .as_deref()
            .is_some_and(|old| Some(old) != exchange_order_id)
        {
            return Err(StoreError::Invalid("EXCHANGE_ID_CONFLICT"));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("UPDATE intents SET state=?2,exchange_order_id=COALESCE(exchange_order_id,?3),updated_at=?4,revision=revision+1 WHERE intent_id=?1",params![id,state.as_str(),exchange_order_id,now])?;
        tx.execute(
            "UPDATE risk_receipts SET state=?2 WHERE intent_id=?1",
            params![
                id,
                if state == IntentState::Unknown {
                    "UNKNOWN"
                } else if i.risk_increase
                    && matches!(state, IntentState::Filled | IntentState::PartiallyFilled)
                {
                    "OPEN"
                } else if state.terminal() {
                    "TERMINAL_PENDING_CENTRAL"
                } else {
                    "OPEN"
                }
            ],
        )?;
        if state.terminal() {
            tx.execute("INSERT INTO operation_receipts VALUES(?1,'TERMINAL',?2,?3,?4) ON CONFLICT(operation_id) DO NOTHING",params![format!("terminal:{id}"),i.request_hash,evidence,now])?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn recover_posting(&mut self, now: i64) -> StoreResult<usize> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let n=tx.execute("UPDATE intents SET state='UNKNOWN',updated_at=?1,revision=revision+1 WHERE state='POSTING'",[now])?;
        tx.execute("UPDATE risk_receipts SET state='UNKNOWN' WHERE intent_id IN(SELECT intent_id FROM intents WHERE state='UNKNOWN')",[])?;
        tx.commit()?;
        Ok(n)
    }
    /// Bounded combined local account obligations, including terminal
    /// intents whose RiskBook reservations have NOT yet been released.
    pub fn risk_census_intents(
        &self,
        limit: usize,
    ) -> StoreResult<(Vec<StoredIntent>, Vec<String>)> {
        if limit == 0 || limit > 100 {
            return Err(StoreError::Invalid("COHORT_CENSUS_LIMIT_INVALID"));
        }
        let live = self.active_intents(limit)?;
        let pending = self.reconcilable_intents_page(limit, 0)?;
        let mut all = std::collections::BTreeMap::new();
        for intent in &live {
            all.insert(intent.intent_id.clone(), intent.clone());
        }
        let mut unresolved = std::collections::BTreeSet::new();
        for intent in pending {
            unresolved.insert(intent.intent_id.clone());
            all.insert(intent.intent_id.clone(), intent);
        }
        if live.len() >= limit || unresolved.len() >= limit || all.len() >= limit {
            return Err(StoreError::Invalid("COHORT_LOCAL_INTENTS_TRUNCATED"));
        }
        Ok((
            all.into_values().collect(),
            unresolved.into_iter().collect(),
        ))
    }

    pub fn active_intents(&self, limit: usize) -> StoreResult<Vec<StoredIntent>> {
        let mut s=self.connection.prepare("SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT ?1")?;
        let ids = s
            .query_map([limit.min(1000) as i64], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ids.iter()
            .map(|id| {
                self.intent(id)?
                    .ok_or(StoreError::Invalid("INTENT_MISSING"))
            })
            .collect()
    }
    // Cache an already-authorized revision. This grants no new financial authority.
}

/// SQL risk receipt state is read under the existing single AccountStore owner.
fn risk_receipt_covers_exposure(state: &str) -> bool {
    matches!(
        state,
        "HELD" | "POSTED" | "UNKNOWN" | "OPEN" | "TERMINAL_PENDING_CENTRAL"
    )
}

#[cfg(test)]
mod active_receipt_tests {
    use super::*;

    #[test]
    fn released_or_no_send_receipt_never_covers_open_exposure() {
        for state in [
            "HELD",
            "POSTED",
            "UNKNOWN",
            "OPEN",
            "TERMINAL_PENDING_CENTRAL",
        ] {
            assert!(risk_receipt_covers_exposure(state), "{state}");
        }
        for state in [
            "RELEASED",
            "UNSENT_PENDING_CENTRAL",
            "",
            "CORRUPT",
            "PREPARED",
        ] {
            assert!(!risk_receipt_covers_exposure(state), "{state}");
        }
    }
}
