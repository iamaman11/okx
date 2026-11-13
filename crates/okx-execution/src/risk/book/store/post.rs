//! Single, durable HELD -> POSTED fence inside the campaign SQLite owner.
//! POSTED means at most one potential venue submission. A missing/lost ACK
//! MUST be inspected; repeating BEGIN_POST cannot grant another send.
//! Production ingress remains private until authenticated source construction,
//! policy epoch and legacy Worker fence are proven.
use super::{
    LEGACY_MAX_SAFE_INTEGER, Result, RiskBookStore, RiskStoreError, VerifiedCensus,
    amount, apply_census_loss, read_meta, safe_integer, valid_digest,
    verify_reservation_aggregates,
};
use okx_kernel::risk::{
    model::PostCausalProof,
    money::RiskAmount,
    rules::{RiskRuleError, validate_account_occupancy, validate_post_causality},
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

#[derive(Debug, Clone)]
pub(in crate::risk::book) struct PostFenceInput {
    pub reservation_key: String,
    pub plan_sha256: String,
    pub policy_revision: u64,
    pub expected_sequence: u64,
    pub now_ms: i64,
    pub source_proof_digest: String,
    pub census: VerifiedCensus,
    pub causal: PostCausalProof,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::risk::book) struct PostAuthorized {
    pub reservation_key: String,
    pub book_sequence: u64,
    /// Total modelled loss across the cohort, excluding this candidate only.
    pub existing_modelled_loss_usdt: String,
}

impl RiskBookStore {
    /// Private until Native authenticated evidence and durable policy epoch
    /// are wired. This operation never submits a venue order.
    pub(in crate::risk::book) fn begin_post(
        &mut self,
        request: &PostFenceInput,
    ) -> Result<PostAuthorized> {
        if request.reservation_key.is_empty()
            || request.reservation_key.len() > 512
            || !valid_digest(&request.plan_sha256)
            || !valid_digest(&request.source_proof_digest)
            || request.policy_revision == 0
            || request.expected_sequence == 0
            || request.policy_revision > LEGACY_MAX_SAFE_INTEGER
            || request.expected_sequence > LEGACY_MAX_SAFE_INTEGER
            || request.now_ms <= 0
            || !safe_integer(request.now_ms)
        {
            return Err(RiskStoreError::Invalid("POST_FENCE_IDENTITY"));
        }
        let census = &request.census;
        if !census.complete
            || census.generation == 0
            || census.generation > LEGACY_MAX_SAFE_INTEGER
            || !valid_digest(&census.source_digest)
            || census.generated_at_ms <= 0
            || census.read_floor_ms <= 0
            || !safe_integer(census.generated_at_ms)
            || !safe_integer(census.read_floor_ms)
            || census.read_floor_ms > census.generated_at_ms
            || census.generated_at_ms > request.now_ms
            || request.now_ms - census.generated_at_ms > 5_000
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        let tx = self.connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut meta = read_meta(&tx)?;
        verify_reservation_aggregates(&tx, &meta)?;
        if meta.policy_revision != request.policy_revision {
            return Err(RiskStoreError::PolicyConflict);
        }
        if meta.generation != census.generation {
            return Err(RiskStoreError::StaleGeneration);
        }
        if meta.sequence != request.expected_sequence {
            return Err(RiskStoreError::StaleSequence);
        }

        let entry: Option<(String, String, String, i64, i64, String, String, String, i64)> = tx
            .query_row(
                "SELECT plan_sha256,state,account_key,held_at_ms,reserve_read_floor_ms,
                        native_incarnation,reserved_usdt,entry_upper_usd,book_sequence
                 FROM risk_entries WHERE reservation_key=?1",
                params![&request.reservation_key],
                |r| Ok((
                    r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?,
                    r.get(5)?, r.get(6)?, r.get(7)?, r.get(8)?
                )),
            )
            .optional()?;
        let Some((plan, state, account, held_at_ms, held_read_floor, incarnation,
                  reserved_usdt, entry_upper, reservation_seq)) = entry else {
            return Err(RiskStoreError::Invalid("POST_RESERVATION_NOT_FOUND"));
        };
        if plan != request.plan_sha256 {
            return Err(RiskStoreError::IdempotencyConflict);
        }
        if state != "HELD" {
            return Err(RiskStoreError::AlreadyResolved);
        }
        if reservation_seq <= 0 || u64::try_from(reservation_seq).ok().is_none_or(|s| s > meta.sequence) {
            return Err(RiskStoreError::Corrupt("POST_RESERVATION_SEQUENCE"));
        }
        if held_at_ms <= 0
            || held_read_floor <= 0
            || request.now_ms < held_at_ms
            || request.now_ms - held_at_ms > 5_000
            || census.generated_at_ms <= held_at_ms
            || census.read_floor_ms < held_read_floor
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        let mut policies = Vec::new();
        {
            let mut query = tx.prepare(
                "SELECT p.account_key,p.limit_usdt,p.entry_limit_usd,o.reserved_usdt
                 FROM risk_policy_account p
                 JOIN risk_account_occupied o USING(account_key)
                 ORDER BY p.account_key",
            )?;
            let rows = query.query_map([], |r| Ok((
                r.get::<_, String>(0)?, r.get::<_, String>(1)?,
                r.get::<_, String>(2)?, r.get::<_, String>(3)?
            )))?;
            for row in rows {
                policies.push(row?);
            }
        }
        let keys = policies.iter().map(|row| row.0.clone()).collect::<Vec<_>>();
        if keys.is_empty()
            || keys.iter().ne(census.occupied.keys())
            || keys.iter().ne(census.cumulative_loss_usd.keys())
        {
            return Err(RiskStoreError::CensusUnproven);
        }
        validate_post_causality(
            &request.causal,
            &request.reservation_key,
            &incarnation,
            &keys,
        )?;

        // Both fresh source facts and the exact current cached commitments
        // participate in the same database transaction. Other accounts
        // cannot hide existing external occupancy while this one posts.
        let mut external = RiskAmount::zero();
        let mut entry_cap = None;
        for (member, limit, entry_limit, centrally_reserved) in &policies {
            let attested = &census.occupied[member];
            let total = attested.checked_add(&amount(centrally_reserved.clone())?)?;
            if let Err(error) = validate_account_occupancy(&amount(limit.clone())?, &total) {
                return Err(RiskStoreError::Denied(error));
            }
            external = external.checked_add(attested)?;
            if member == &account {
                entry_cap = Some(amount(entry_limit.clone())?);
            }
        }
        let entry_cap = entry_cap.ok_or(RiskStoreError::Corrupt("POST_ACCOUNT_POLICY_MISSING"))?;
        if amount(entry_upper)? > entry_cap {
            return Err(RiskStoreError::Denied(RiskRuleError::AccountEntryLimitExceeded));
        }
        let total = external.checked_add(&meta.global_reserved)?;
        if total > meta.global_limit {
            return Err(RiskStoreError::Denied(RiskRuleError::InsufficientAggregateOrAccountRisk));
        }
        if !meta.entry_breach.is_zero() {
            return Err(RiskStoreError::Denied(RiskRuleError::BoundBreach));
        }
        // A complete native financial observation may advance the cumulative
        // loss high-water. It is persisted even when the refreshed budget
        // subsequently blocks POST; the SQL transaction owns this mutation.
        apply_census_loss(&tx, &mut meta, census)?;
        if RiskAmount::sum_exceeds(&[&meta.loss_consumed, &meta.loss_reserved], &meta.loss_limit) {
            tx.commit()?;
            return Err(RiskStoreError::Denied(RiskRuleError::LossBudgetExhausted));
        }
        if RiskAmount::sum_exceeds(&[&meta.entry_consumed, &meta.entry_reserved], &meta.entry_limit) {
            tx.commit()?;
            return Err(RiskStoreError::Denied(RiskRuleError::EntryBudgetExhausted));
        }

        let next_seq = meta.sequence.checked_add(1)
            .filter(|v| *v <= LEGACY_MAX_SAFE_INTEGER)
            .ok_or(RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let next_seq_sql = i64::try_from(next_seq)
            .map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
        let changed = tx.execute(
            "UPDATE risk_entries SET state='POSTED',
                    post_authorized_at_ms=?1,post_sequence=?2,
                    post_proof_digest=?3,post_source_digest=?4,
                    post_existing_occupied_usdt=?5
             WHERE reservation_key=?6 AND state='HELD' AND plan_sha256=?7
               AND policy_revision=?8 AND book_sequence=?9",
            params![
                request.now_ms,
                next_seq_sql,
                &request.source_proof_digest,
                &census.source_digest,
                total.checked_sub(&amount(reserved_usdt)?)?.canonical()?,
                &request.reservation_key,
                &request.plan_sha256,
                i64::try_from(request.policy_revision).map_err(|_| RiskStoreError::PolicyConflict)?,
                reservation_seq,
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        let changed = tx.execute(
            "UPDATE risk_meta SET book_sequence=?1
             WHERE singleton=1 AND book_sequence=?2 AND policy_revision=?3 AND generation=?4",
            params![
                next_seq_sql,
                i64::try_from(meta.sequence).map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?,
                i64::try_from(meta.policy_revision).map_err(|_| RiskStoreError::PolicyConflict)?,
                i64::try_from(meta.generation).map_err(|_| RiskStoreError::StaleGeneration)?,
            ],
        )?;
        if changed != 1 {
            return Err(RiskStoreError::StaleSequence);
        }
        let existing = total.checked_sub(&amount(reserved_usdt)?)?.canonical()?;
        tx.commit()?;
        Ok(PostAuthorized {
            reservation_key: request.reservation_key.clone(),
            book_sequence: next_seq,
            existing_modelled_loss_usdt: existing,
        })
    }
}
