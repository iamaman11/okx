//! Single, durable HELD -> POSTED fence inside the campaign SQLite owner.
//! POSTED means at most one potential venue submission. A missing/lost ACK
//! MUST be inspected; repeating BEGIN_POST cannot grant another send.
//! Production ingress remains private until authenticated source construction,
//! policy epoch and legacy Worker fence are proven.
use super::{
    LEGACY_MAX_SAFE_INTEGER, Result, RiskBookStore, RiskStoreError, VerifiedCensus, amount,
    apply_census_loss_for_scope, read_meta_for_scope, safe_integer, valid_digest,
};
use okx_kernel::risk::{
    model::PostCausalProof,
    money::RiskAmount,
    rules::{RiskRuleError, validate_account_occupancy, validate_post_causality},
};
use rusqlite::{
    OptionalExtension, Transaction, TransactionBehavior, params_from_iter, types::Value,
};

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

/// Before the caller's COMMIT, Authorized is an SQL transition proposal,
/// NOT permission to send an exchange order.
#[derive(Debug)]
pub(in crate::risk::book) enum PostDecision {
    Authorized(PostAuthorized),
    Denied(RiskRuleError),
}

impl RiskBookStore {
    /// Legacy v5 and scoped v6 run ONE Rust policy/monotonic-loss algorithm.
    /// Only the legacy owner commits here; v6 callers own their transaction.
    pub(in crate::risk::book) fn begin_post(
        &mut self,
        request: &PostFenceInput,
    ) -> Result<PostAuthorized> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = begin_post_transaction(&tx, request, None)?;
        tx.commit()?;
        match result {
            PostDecision::Authorized(grant) => Ok(grant),
            PostDecision::Denied(reason) => Err(RiskStoreError::Denied(reason)),
        }
    }
}

/// PRIVATE, no TradeService entrypoint, no post permission.
pub(in crate::risk::book) fn begin_unified_post_transaction(
    tx: &Transaction<'_>,
    installation: &str,
    campaign: &str,
    request: &PostFenceInput,
) -> Result<PostDecision> {
    if installation.is_empty() || campaign.is_empty() {
        return Err(RiskStoreError::Invalid("UNIFIED_POST_SCOPE_INVALID"));
    }
    begin_post_transaction(tx, request, Some((installation, campaign)))
}

/// Full native account + RiskBook BEGIN_POST under ONE caller-owned v6 SQL
/// transaction. No independent writer and no venue POST. Do not interpret
/// Authorized as venue capability until the caller has committed this tx and
/// authenticated live source/owner epoch outside this private draft.
pub(in crate::risk::book) fn begin_unified_post_and_account_transaction(
    tx: &Transaction<'_>,
    installation: &str,
    campaign: &str,
    request: &PostFenceInput,
) -> Result<PostDecision> {
    let decision = begin_unified_post_transaction(tx, installation, campaign, request)?;
    let PostDecision::Authorized(_) = &decision else {
        // Preserve the original high-water observation on ordinary denial.
        return Ok(decision);
    };
    let (account, intent): (String, String) = tx.query_row(
        "SELECT account_key,intent_id FROM risk_entries
         WHERE installation_id=?1 AND campaign_id=?2 AND reservation_key=?3
           AND state='POSTED' AND plan_sha256=?4",
        rusqlite::params![
            installation, campaign, request.reservation_key, request.plan_sha256
        ],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let updated = crate::account_store::AccountStore::begin_post_sql_transaction(
        tx,
        Some((installation, &account)),
        &intent,
        i64::try_from(request.policy_revision).map_err(|_| RiskStoreError::PolicyConflict)?,
        request.now_ms,
        Some(&request.reservation_key),
    )
    .map_err(|error| match error {
        crate::account_store::StoreError::Invalid(reason) => RiskStoreError::Invalid(reason),
        crate::account_store::StoreError::Sql(error) => RiskStoreError::Sql(error),
        crate::account_store::StoreError::Io(error) => RiskStoreError::Io(error),
    })?;
    if !updated {
        return Err(RiskStoreError::AlreadyResolved);
    }
    Ok(decision)
}

fn scope_values(scope: Option<(&str, &str)>) -> Vec<Value> {
    if let Some((installation, campaign)) = scope {
        vec![
            Value::Text(installation.to_owned()),
            Value::Text(campaign.to_owned()),
        ]
    } else {
        Vec::new()
    }
}

fn begin_post_transaction(
    tx: &Transaction<'_>,
    request: &PostFenceInput,
    scope: Option<(&str, &str)>,
) -> Result<PostDecision> {
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
    let mut meta = read_meta_for_scope(tx, scope)?;
    super::integrity::verify_reservation_aggregates_for_scope(tx, &meta, scope)?;
    if meta.policy_revision != request.policy_revision {
        return Err(RiskStoreError::PolicyConflict);
    }
    if meta.generation != census.generation {
        return Err(RiskStoreError::StaleGeneration);
    }
    if meta.sequence != request.expected_sequence {
        return Err(RiskStoreError::StaleSequence);
    }

    let entry: Option<(
        String,
        String,
        String,
        String,
        i64,
        i64,
        String,
        String,
        String,
        i64,
    )> = tx
        .query_row(
            if scope.is_some() {
                "SELECT plan_sha256,state,account_key,intent_id,held_at_ms,reserve_read_floor_ms,
                            native_incarnation,reserved_usdt,entry_upper_usd,book_sequence
                     FROM risk_entries WHERE reservation_key=?1
                     AND installation_id=?2 AND campaign_id=?3"
            } else {
                "SELECT plan_sha256,state,account_key,intent_id,held_at_ms,reserve_read_floor_ms,
                            native_incarnation,reserved_usdt,entry_upper_usd,book_sequence
                     FROM risk_entries WHERE reservation_key=?1"
            },
            params_from_iter(
                std::iter::once(Value::Text(request.reservation_key.clone()))
                    .chain(scope_values(scope))
                    .collect::<Vec<_>>()
                    .iter(),
            ),
            |r| {
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
                    r.get(9)?,
                ))
            },
        )
        .optional()?;
    let Some((
        plan,
        state,
        account,
        intent_id,
        held_at_ms,
        held_read_floor,
        incarnation,
        reserved_usdt,
        entry_upper,
        reservation_seq,
    )) = entry
    else {
        return Err(RiskStoreError::Invalid("POST_RESERVATION_NOT_FOUND"));
    };
    if plan != request.plan_sha256 {
        return Err(RiskStoreError::IdempotencyConflict);
    }
    if state != "HELD" {
        return Err(RiskStoreError::AlreadyResolved);
    }
    if let Some((installation, campaign)) = scope {
        // Same-file original AccountStore state is mandatory. A standalone
        // RiskBook HELD row cannot authorize a venue POST before the original
        // intent and its campaign assignment are durably RESERVED.
        let source: Option<(String, String, Option<String>, i64)> = tx
            .query_row(
                "SELECT i.plan_hash,i.state,p.campaign_id,i.risk_increase
                 FROM intents i JOIN account_policy p
                   ON p.installation_id=i.installation_id
                  AND p.account_key=i.account_key AND p.singleton=1
                 WHERE i.installation_id=?1 AND i.account_key=?2 AND i.intent_id=?3",
                rusqlite::params![installation, &account, &intent_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()?;
        if !matches!(source,
            Some((ref plan, ref state, Some(ref assigned), 1))
                if plan == &request.plan_sha256
                    && state == "RESERVED"
                    && assigned == campaign
        ) {
            return Err(RiskStoreError::EntrySqlUnproven);
        }
    }
    if reservation_seq <= 0
        || u64::try_from(reservation_seq)
            .ok()
            .is_none_or(|s| s > meta.sequence)
    {
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
        let query = if scope.is_some() {
            "SELECT p.account_key,p.limit_usdt,p.entry_limit_usd,o.reserved_usdt
                 FROM risk_policy_account p
                 JOIN risk_account_occupied o
                   ON p.installation_id=o.installation_id
                  AND p.campaign_id=o.campaign_id AND p.account_key=o.account_key
                 WHERE p.installation_id=?1 AND p.campaign_id=?2
                 ORDER BY p.account_key"
        } else {
            "SELECT p.account_key,p.limit_usdt,p.entry_limit_usd,o.reserved_usdt
                 FROM risk_policy_account p
                 JOIN risk_account_occupied o USING(account_key)
                 ORDER BY p.account_key"
        };
        let mut query = tx.prepare(query)?;
        let args = scope_values(scope);
        let rows = query.query_map(params_from_iter(args.iter()), |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })?;
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
        return Err(RiskStoreError::Denied(
            RiskRuleError::AccountEntryLimitExceeded,
        ));
    }
    let total = external.checked_add(&meta.global_reserved)?;
    if total > meta.global_limit {
        return Err(RiskStoreError::Denied(
            RiskRuleError::InsufficientAggregateOrAccountRisk,
        ));
    }
    if !meta.entry_breach.is_zero() {
        return Err(RiskStoreError::Denied(RiskRuleError::BoundBreach));
    }
    // A complete native financial observation may advance the cumulative
    // loss high-water. It is persisted even when the refreshed budget
    // subsequently blocks POST; the SQL transaction owns this mutation.
    apply_census_loss_for_scope(tx, &mut meta, census, scope)?;
    if RiskAmount::sum_exceeds(
        &[&meta.loss_consumed, &meta.loss_reserved],
        &meta.loss_limit,
    ) {
        return Ok(PostDecision::Denied(RiskRuleError::LossBudgetExhausted));
    }
    if RiskAmount::sum_exceeds(
        &[&meta.entry_consumed, &meta.entry_reserved],
        &meta.entry_limit,
    ) {
        return Ok(PostDecision::Denied(RiskRuleError::EntryBudgetExhausted));
    }

    let next_seq = meta
        .sequence
        .checked_add(1)
        .filter(|v| *v <= LEGACY_MAX_SAFE_INTEGER)
        .ok_or(RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
    let next_seq_sql =
        i64::try_from(next_seq).map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?;
    let existing = total.checked_sub(&amount(reserved_usdt)?)?.canonical()?;
    let sql = if scope.is_some() {
        "UPDATE risk_entries SET state='POSTED',
                    post_authorized_at_ms=?1,post_sequence=?2,
                    post_proof_digest=?3,post_source_digest=?4,
                    post_existing_occupied_usdt=?5
             WHERE reservation_key=?6 AND state='HELD' AND plan_sha256=?7
               AND policy_revision=?8 AND book_sequence=?9
               AND installation_id=?10 AND campaign_id=?11"
    } else {
        "UPDATE risk_entries SET state='POSTED',
                    post_authorized_at_ms=?1,post_sequence=?2,
                    post_proof_digest=?3,post_source_digest=?4,
                    post_existing_occupied_usdt=?5
             WHERE reservation_key=?6 AND state='HELD' AND plan_sha256=?7
               AND policy_revision=?8 AND book_sequence=?9"
    };
    let mut args = vec![
        Value::Integer(request.now_ms),
        Value::Integer(next_seq_sql),
        Value::Text(request.source_proof_digest.clone()),
        Value::Text(census.source_digest.clone()),
        Value::Text(existing.clone()),
        Value::Text(request.reservation_key.clone()),
        Value::Text(request.plan_sha256.clone()),
        Value::Integer(
            i64::try_from(request.policy_revision).map_err(|_| RiskStoreError::PolicyConflict)?,
        ),
        Value::Integer(reservation_seq),
    ];
    args.extend(scope_values(scope));
    let changed = tx.execute(sql, params_from_iter(args.iter()))?;
    if changed != 1 {
        return Err(RiskStoreError::StaleSequence);
    }
    let sql = if scope.is_some() {
        "UPDATE risk_meta SET book_sequence=?1
             WHERE singleton=1 AND book_sequence=?2 AND policy_revision=?3 AND generation=?4
               AND installation_id=?5 AND campaign_id=?6"
    } else {
        "UPDATE risk_meta SET book_sequence=?1
             WHERE singleton=1 AND book_sequence=?2 AND policy_revision=?3 AND generation=?4"
    };
    let mut args = vec![
        Value::Integer(next_seq_sql),
        Value::Integer(
            i64::try_from(meta.sequence)
                .map_err(|_| RiskStoreError::Corrupt("SEQUENCE_OVERFLOW"))?,
        ),
        Value::Integer(
            i64::try_from(meta.policy_revision).map_err(|_| RiskStoreError::PolicyConflict)?,
        ),
        Value::Integer(
            i64::try_from(meta.generation).map_err(|_| RiskStoreError::StaleGeneration)?,
        ),
    ];
    args.extend(scope_values(scope));
    let changed = tx.execute(sql, params_from_iter(args.iter()))?;
    if changed != 1 {
        return Err(RiskStoreError::StaleSequence);
    }
    Ok(PostDecision::Authorized(PostAuthorized {
        reservation_key: request.reservation_key.clone(),
        book_sequence: next_seq,
        existing_modelled_loss_usdt: existing,
    }))
}
