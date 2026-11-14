//! One campaign-side entry debit per original native SQL revision.
//! The original AccountStore proves fill/bill/FX economics, never the
//! target reservation's transitional POSTED/UNKNOWN/OPEN label.
//! This module does NOT calculate fill, bill, fee, FX or venue facts; those
//! economic facts are produced and sealed by the existing AccountStore owner.
//! No arbitrary network settlement, late fee or terminal release is accepted.
use super::{
    LEGACY_MAX_SAFE_INTEGER, Result, RiskBookStore, RiskStoreError, amount, integrity,
    read_meta_for_scope, reserve_scope_values, valid_digest,
};
use crate::account_store::EntrySettlementEvidence;
use okx_kernel::risk::money::RiskAmount;
use rusqlite::{
    OptionalExtension, Transaction, TransactionBehavior, params_from_iter, types::Value,
};

#[derive(Debug, Clone)]
pub(in crate::risk::book) struct PartialEntrySqlProof {
    account_key: String,
    reservation_key: String,
    plan_sha256: String,
    revision: u64,
    cumulative_usd: RiskAmount,
    source_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::risk::book) struct EntrySettlementReceipt {
    pub reservation_key: String,
    pub book_sequence: u64,
    pub cumulative_entry_usd: String,
    pub entry_bound_breach_usd: String,
    pub replayed: bool,
}
fn overage(value: &RiskAmount, limit: &RiskAmount) -> Result<RiskAmount> {
    if value > limit {
        Ok(value.checked_sub(limit)?)
    } else {
        Ok(RiskAmount::zero())
    }
}
fn aggregate_overage(
    consumed: &RiskAmount,
    reserved: &RiskAmount,
    limit: &RiskAmount,
) -> Result<RiskAmount> {
    if RiskAmount::sum_exceeds(&[consumed, reserved], limit) {
        Ok(consumed.checked_add(reserved)?.checked_sub(limit)?)
    } else {
        Ok(RiskAmount::zero())
    }
}
impl PartialEntrySqlProof {
    pub(in crate::risk::book) fn from_account_receipt(
        evidence: EntrySettlementEvidence,
    ) -> Result<Self> {
        if evidence.schema != "okx.entry-partial-settlement/v1"
            || !evidence.account_key.starts_with("okx:real:")
            || !valid_digest(evidence.account_key.strip_prefix("okx:real:").unwrap_or(""))
            || evidence.reservation_key.is_empty()
            || evidence.reservation_key.len() > 512
            || !valid_digest(&evidence.plan_hash)
            || !valid_digest(&evidence.source_digest)
            || evidence.settlement_revision == 0
            || evidence.settlement_revision > LEGACY_MAX_SAFE_INTEGER
        {
            return Err(RiskStoreError::Invalid("PARTIAL_SQL_PROOF_INVALID"));
        }
        let cumulative =
            RiskAmount::parse_positive(&evidence.cumulative_entry_usd.normalize().to_string())?;
        Ok(Self {
            account_key: evidence.account_key,
            reservation_key: evidence.reservation_key,
            plan_sha256: evidence.plan_hash,
            revision: evidence.settlement_revision,
            cumulative_usd: cumulative,
            source_digest: evidence.source_digest,
        })
    }
}
impl RiskBookStore {
    /// Idempotent partial entry settlement, only from current native AccountStore.
    /// A lost campaign reply does not debit again; a newer revision has one CAS.
    pub(in crate::risk::book) fn settle_partial_entry(
        &mut self,
        proof: &PartialEntrySqlProof,
        expected_sequence: u64,
        generation: u64,
    ) -> Result<EntrySettlementReceipt> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = Self::settle_partial_entry_sql_transaction(
            &tx,
            None,
            proof,
            expected_sequence,
            generation,
        )?;
        tx.commit()?;
        Ok(result)
    }

    /// Original AccountStore SQL financial-source proof and original RiskBook
    /// entry-budget debit inside ONE installation-owned SQLite transaction.
    /// The source is queried from the very SAME snapshot, not reconstructed
    /// from caller-provided money, flags or network JSON. No COMMIT here.
    pub(in crate::risk::book) fn settle_unified_partial_from_account_sql_transaction(
        tx: &Transaction<'_>,
        installation: &str,
        campaign: &str,
        account_key: &str,
        intent_id: &str,
        expected_sequence: u64,
        generation: u64,
    ) -> Result<EntrySettlementReceipt> {
        if installation.is_empty()
            || campaign.is_empty()
            || account_key.is_empty()
            || intent_id.is_empty()
        {
            return Err(RiskStoreError::Invalid(
                "UNIFIED_ENTRY_SOURCE_SCOPE_INVALID",
            ));
        }
        let assigned: Option<String> = tx
            .query_row(
                "SELECT campaign_id FROM account_policy
             WHERE installation_id=?1 AND account_key=?2 AND singleton=1",
                rusqlite::params![installation, account_key],
                |row| row.get(0),
            )
            .optional()?;
        if assigned.as_deref() != Some(campaign) {
            return Err(RiskStoreError::EntrySqlUnproven);
        }
        let original = crate::account_store::AccountStore::partial_entry_settlement_receipt_sql(
            tx,
            Some(installation),
            account_key,
            intent_id,
        )
        .map_err(|error| match error {
            crate::StoreError::Invalid(reason) => RiskStoreError::Invalid(reason),
            crate::StoreError::Sql(error) => RiskStoreError::Sql(error),
            crate::StoreError::Io(error) => RiskStoreError::Io(error),
        })?
        .ok_or(RiskStoreError::EntrySqlUnproven)?;
        let proof = PartialEntrySqlProof::from_account_receipt(original.clone())?;
        if proof.account_key != account_key {
            return Err(RiskStoreError::EntrySqlUnproven);
        }
        let accepted = Self::settle_partial_entry_sql_transaction(
            tx,
            Some((installation, campaign)),
            &proof,
            expected_sequence,
            generation,
        )?;
        crate::account_store::AccountStore::acknowledge_unified_partial_entry_sql(
            tx,
            installation,
            account_key,
            &original,
        )
        .map_err(|error| match error {
            crate::StoreError::Invalid(reason) => RiskStoreError::Invalid(reason),
            crate::StoreError::Sql(error) => RiskStoreError::Sql(error),
            crate::StoreError::Io(error) => RiskStoreError::Io(error),
        })?;
        Ok(accepted)
    }

    /// The ORIGINAL partial-entry arithmetic shared between v5 and scoped v6.
    /// The caller owns one transaction/COMMIT, never a parallel SQL writer.
    pub(in crate::risk::book) fn settle_partial_entry_sql_transaction(
        tx: &Transaction<'_>,
        scope: Option<(&str, &str)>,
        proof: &PartialEntrySqlProof,
        expected_sequence: u64,
        generation: u64,
    ) -> Result<EntrySettlementReceipt> {
        if expected_sequence == 0 || expected_sequence > LEGACY_MAX_SAFE_INTEGER {
            return Err(RiskStoreError::StaleSequence);
        }
        let meta = read_meta_for_scope(tx, scope)?;
        integrity::verify_reservation_aggregates_for_scope(tx, &meta, scope)?;
        if generation != meta.generation {
            return Err(RiskStoreError::StaleGeneration);
        }
        let read_sql = if scope.is_some() {
            "SELECT account_key,plan_sha256,state,entry_upper_usd,
                    reserved_entry_usd,entry_settled_usd,entry_settlement_revision,
                    entry_settlement_digest,entry_bound_breach_usd
             FROM risk_entries WHERE reservation_key=?1
               AND installation_id=?2 AND campaign_id=?3"
        } else {
            "SELECT account_key,plan_sha256,state,entry_upper_usd,
                    reserved_entry_usd,entry_settled_usd,entry_settlement_revision,
                    entry_settlement_digest,entry_bound_breach_usd
             FROM risk_entries WHERE reservation_key=?1"
        };
        let read_args =
            reserve_scope_values(vec![Value::Text(proof.reservation_key.clone())], scope);
        let entry: Option<(
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            String,
            String,
        )> = tx
            .query_row(read_sql, params_from_iter(read_args.iter()), |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ))
            })
            .optional()?;
        let Some((
            account,
            plan,
            state,
            upper,
            remaining,
            previous,
            prior_revision,
            digest,
            prior_breach,
        )) = entry
        else {
            return Err(RiskStoreError::Invalid("PARTIAL_RESERVATION_MISSING"));
        };
        if account != proof.account_key || plan != proof.plan_sha256 {
            return Err(RiskStoreError::IdempotencyConflict);
        }
        // The original AccountStore can prove partial fills while a venue
        // ACK is lost (UNKNOWN) or after the same SQL owner has acknowledged
        // the order (OPEN). These states retain the SAME held money. A
        // RELEASED/HELD reservation may never use this partial debit path.
        if !matches!(state.as_str(), "POSTED" | "UNKNOWN" | "OPEN") {
            return Err(RiskStoreError::AlreadyResolved);
        }
        let prior_revision = u64::try_from(prior_revision)
            .map_err(|_| RiskStoreError::Corrupt("ENTRY_SETTLEMENT_REVISION"))?;
        let before = amount(previous)?;
        let upper = amount(upper)?;
        let previous_reserved = amount(remaining)?;
        let previous_over = overage(&before, &upper)?;
        if amount(prior_breach)? != previous_over {
            return Err(RiskStoreError::Corrupt("ENTRY_SETTLEMENT_BOUND"));
        }
        if proof.revision == prior_revision {
            if prior_revision == 0
                || digest != proof.source_digest
                || before != proof.cumulative_usd
                || expected_sequence > meta.sequence
            {
                return Err(RiskStoreError::IdempotencyConflict);
            }
            return Ok(EntrySettlementReceipt {
                reservation_key: proof.reservation_key.clone(),
                book_sequence: meta.sequence,
                cumulative_entry_usd: before.canonical()?,
                entry_bound_breach_usd: previous_over.canonical()?,
                replayed: true,
            });
        }
        if expected_sequence != meta.sequence {
            return Err(RiskStoreError::StaleSequence);
        }
        if proof.revision
            != prior_revision
                .checked_add(1)
                .ok_or(RiskStoreError::Corrupt(
                    "ENTRY_SETTLEMENT_REVISION_OVERFLOW",
                ))?
            || proof.cumulative_usd < before
            || digest == proof.source_digest
        {
            return Err(RiskStoreError::Invalid(
                "ENTRY_SETTLEMENT_REVISION_CONFLICT",
            ));
        }
        let expected_remaining = if upper > before {
            upper.checked_sub(&before)?
        } else {
            RiskAmount::zero()
        };
        if expected_remaining != previous_reserved {
            return Err(RiskStoreError::Corrupt("ENTRY_RESERVED_PRIMARY_MISMATCH"));
        }
        let delta = proof.cumulative_usd.checked_sub(&before)?;
        let remaining_after = if upper > proof.cumulative_usd {
            upper.checked_sub(&proof.cumulative_usd)?
        } else {
            RiskAmount::zero()
        };
        let reserve_debit = previous_reserved.checked_sub(&remaining_after)?;
        let consumed_after = meta.entry_consumed.checked_add(&delta)?;
        let reserved_after = meta.entry_reserved.checked_sub(&reserve_debit)?;
        let current_over = overage(&proof.cumulative_usd, &upper)?;
        let individual_delta = current_over.checked_sub(&previous_over)?;
        let prior_aggregate = aggregate_overage(
            &meta.entry_consumed,
            &meta.entry_reserved,
            &meta.entry_limit,
        )?;
        let current_aggregate =
            aggregate_overage(&consumed_after, &reserved_after, &meta.entry_limit)?;
        let aggregate_delta = if current_aggregate > prior_aggregate {
            current_aggregate.checked_sub(&prior_aggregate)?
        } else {
            RiskAmount::zero()
        };
        let breach_delta = if individual_delta > aggregate_delta {
            individual_delta
        } else {
            aggregate_delta
        };
        let breach_after = meta.entry_breach.checked_add(&breach_delta)?;
        let next = meta
            .sequence
            .checked_add(1)
            .filter(|v| *v <= LEGACY_MAX_SAFE_INTEGER)
            .ok_or(RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let next_i64 =
            i64::try_from(next).map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let entry_update = if scope.is_some() {
            "UPDATE risk_entries SET reserved_entry_usd=?1,entry_settled_usd=?2,
                    entry_settlement_revision=?3,entry_settlement_digest=?4,entry_bound_breach_usd=?5
             WHERE reservation_key=?6 AND state IN ('POSTED','UNKNOWN','OPEN') AND entry_settlement_revision=?7
               AND installation_id=?8 AND campaign_id=?9"
        } else {
            "UPDATE risk_entries SET reserved_entry_usd=?1,entry_settled_usd=?2,
                    entry_settlement_revision=?3,entry_settlement_digest=?4,entry_bound_breach_usd=?5
             WHERE reservation_key=?6 AND state IN ('POSTED','UNKNOWN','OPEN') AND entry_settlement_revision=?7"
        };
        let entry_args = reserve_scope_values(
            vec![
                Value::Text(remaining_after.canonical()?),
                Value::Text(proof.cumulative_usd.canonical()?),
                Value::Integer(
                    i64::try_from(proof.revision)
                        .map_err(|_| RiskStoreError::Invalid("ENTRY_REVISION_RANGE"))?,
                ),
                Value::Text(proof.source_digest.clone()),
                Value::Text(current_over.canonical()?),
                Value::Text(proof.reservation_key.clone()),
                Value::Integer(
                    i64::try_from(prior_revision)
                        .map_err(|_| RiskStoreError::Corrupt("ENTRY_REVISION_RANGE"))?,
                ),
            ],
            scope,
        );
        let changed = tx.execute(entry_update, params_from_iter(entry_args.iter()))?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        let meta_update = if scope.is_some() {
            "UPDATE risk_meta SET book_sequence=?1,entry_consumed_usd=?2,
                    entry_reserved_usd=?3,entry_bound_breach_usd=?4
             WHERE singleton=1 AND book_sequence=?5 AND generation=?6
               AND installation_id=?7 AND campaign_id=?8"
        } else {
            "UPDATE risk_meta SET book_sequence=?1,entry_consumed_usd=?2,
                    entry_reserved_usd=?3,entry_bound_breach_usd=?4
             WHERE singleton=1 AND book_sequence=?5 AND generation=?6"
        };
        let meta_args = reserve_scope_values(
            vec![
                Value::Integer(next_i64),
                Value::Text(consumed_after.canonical()?),
                Value::Text(reserved_after.canonical()?),
                Value::Text(breach_after.canonical()?),
                Value::Integer(
                    i64::try_from(meta.sequence)
                        .map_err(|_| RiskStoreError::Corrupt("SEQUENCE_RANGE"))?,
                ),
                Value::Integer(
                    i64::try_from(meta.generation).map_err(|_| RiskStoreError::StaleGeneration)?,
                ),
            ],
            scope,
        );
        let changed = tx.execute(meta_update, params_from_iter(meta_args.iter()))?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        Ok(EntrySettlementReceipt {
            reservation_key: proof.reservation_key.clone(),
            book_sequence: next,
            cumulative_entry_usd: proof.cumulative_usd.canonical()?,
            entry_bound_breach_usd: current_over.canonical()?,
            replayed: false,
        })
    }
}
