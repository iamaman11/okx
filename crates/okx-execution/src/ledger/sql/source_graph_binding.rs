//! Durable graph identity and *idle-only* publication checkpoints.
//!
//! Reuses ORIGINAL immutable operation receipts in the one finance database:
//! no side database, schema drift, or unsupervised background writer.
use super::super::*;
use crate::graph_binding::{
    EngineGraphBinding, EngineGraphBindingPage, EngineGraphCursor, EngineGraphProgress,
    GraphCloseClaim, GraphCloseRecovery, GraphCloseRecoveryStage, GraphIdleAck, GraphOpenClaim,
};
use crate::position_target::{ExposureSide, TargetPlan, plan_strategy_target};
use sha2::{Digest, Sha256};

const BINDING_KIND: &str = "graph_strategy_binding_v1";
const CURSOR_KIND: &str = "graph_strategy_idle_cursor_v1";
const OPEN_KIND: &str = "graph_strategy_prepared_open_v1";
const CLOSE_KIND: &str = "graph_strategy_prepared_close_v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedGraphOpen {
    input_fingerprint: String,
    intent_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedGraphClose {
    input_fingerprint: String,
    original_reference: String,
    intent_id: String,
}

fn binding_key(binding: &EngineGraphBinding) -> String {
    format!(
        "graph-binding:{:x}:{}:{:x}",
        Sha256::digest(binding.instance_id.as_bytes()),
        binding.policy_revision,
        Sha256::digest(binding.instrument_id.as_bytes())
    )
}

fn cursor_prefix(binding: &EngineGraphBinding) -> String {
    format!(
        "graph-cursor:{:x}:{}:{:x}:",
        Sha256::digest(binding.instance_id.as_bytes()),
        binding.policy_revision,
        Sha256::digest(binding.instrument_id.as_bytes())
    )
}

fn cursor_key(binding: &EngineGraphBinding, sequence: u64) -> String {
    format!("{}{:020}", cursor_prefix(binding), sequence)
}

fn open_key(binding: &EngineGraphBinding, sequence: u64) -> String {
    format!(
        "graph-open:{:x}:{}:{:x}:{sequence:020}",
        Sha256::digest(binding.instance_id.as_bytes()),
        binding.policy_revision,
        Sha256::digest(binding.instrument_id.as_bytes())
    )
}

fn close_prefix(binding: &EngineGraphBinding, sequence: u64) -> String {
    format!(
        "graph-close:{:x}:{}:{:x}:{sequence:020}:",
        Sha256::digest(binding.instance_id.as_bytes()),
        binding.policy_revision,
        Sha256::digest(binding.instrument_id.as_bytes()),
    )
}

fn close_key(
    binding: &EngineGraphBinding,
    published: &EngineGraphCursor,
    reference: &str,
) -> StoreResult<String> {
    Ok(format!(
        "{}{}",
        close_prefix(binding, published.sequence),
        binding
            .close_identity(published, reference)
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_CLOSE_IDENTITY_INVALID"))?
            .decision_key
    ))
}

fn encode<T: serde::Serialize>(value: &T) -> StoreResult<(String, String)> {
    let body = serde_json::to_string(value)
        .map_err(|_| StoreError::Invalid("GRAPH_FINANCE_RECEIPT_ENCODING_FAILED"))?;
    let digest = format!("{:x}", Sha256::digest(body.as_bytes()));
    Ok((body, digest))
}

fn decode<T: serde::de::DeserializeOwned>(
    receipt: &OperationReceipt,
    kind: &str,
) -> StoreResult<T> {
    let digest = format!("{:x}", Sha256::digest(receipt.result_ref.as_bytes()));
    if receipt.kind != kind || receipt.request_hash != digest {
        return Err(StoreError::Invalid("GRAPH_FINANCE_RECEIPT_CORRUPT"));
    }
    serde_json::from_str(&receipt.result_ref)
        .map_err(|_| StoreError::Invalid("GRAPH_FINANCE_RECEIPT_CORRUPT"))
}

fn insert_receipt(
    tx: &rusqlite::Transaction<'_>,
    scope: (&str, &str),
    id: &str,
    kind: &str,
    value: &impl serde::Serialize,
    now: i64,
) -> StoreResult<()> {
    let (body, digest) = encode(value)?;
    tx.execute(
        "INSERT INTO operation_receipts
         (installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![scope.0, scope.1, id, kind, digest, body, now],
    )?;
    Ok(())
}

impl AccountStore {
    /// Reconstruct all reductions for ONE candle from immutable original
    /// finance receipts after restart. A terminal venue label without the
    /// independently reconciled LOCAL_TERMINAL_RELEASE is still pending.
    pub(crate) fn graph_close_recoveries_sql(
        db: &Connection,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        published: &EngineGraphCursor,
    ) -> StoreResult<Vec<GraphCloseRecovery>> {
        if !published.valid_for(binding) || published.intent == crate::position_target::TargetPosition::Hold {
            return Err(StoreError::Invalid("GRAPH_CLOSE_RECOVERY_PUBLICATION_INVALID"));
        }
        Self::graph_progress_sql(db, scope, binding)?
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_BINDING_MISSING"))?;
        let prefix = close_prefix(binding, published.sequence);
        let mut statement = db.prepare(
            "SELECT operation_id,kind,request_hash,result_ref,committed_at
             FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2
               AND operation_id>=?3 AND operation_id<?4
             ORDER BY operation_id LIMIT 257",
        )?;
        let receipts = statement.query_map(
            params![scope.0, scope.1, prefix, format!("{prefix}~")],
            |row| Ok(OperationReceipt {
                operation_id: row.get(0)?,
                kind: row.get(1)?,
                request_hash: row.get(2)?,
                result_ref: row.get(3)?,
                committed_at: row.get(4)?,
            }),
        )?.collect::<rusqlite::Result<Vec<_>>>()?;
        if receipts.len() > 256 {
            return Err(StoreError::Invalid("GRAPH_CLOSE_RECOVERY_LIMIT"));
        }
        let snapshot = Self::strategy_position_snapshot_sql(
            db, Some(scope), &binding.instance_id, &binding.instrument_id,
        )?;
        let mut outcomes = Vec::with_capacity(receipts.len());
        for receipt in receipts {
            let recorded: PreparedGraphClose = decode(&receipt, CLOSE_KIND)?;
            if receipt.operation_id != close_key(binding, published, &recorded.original_reference)?
                || recorded.input_fingerprint.len() != 64
                || !recorded.input_fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(StoreError::Invalid("GRAPH_CLOSE_RECOVERY_RECEIPT_INVALID"));
            }
            let intent = Self::find_intent_sql(db, Some(scope), "intent_id", &recorded.intent_id)?
                .ok_or(StoreError::Invalid("GRAPH_CLOSE_RECOVERY_INTENT_MISSING"))?;
            let expected_key = binding.close_identity(published, &recorded.original_reference)
                .ok_or(StoreError::Invalid("GRAPH_CLOSE_RECOVERY_IDENTITY_INVALID"))?;
            if intent.idempotency_key != expected_key.decision_key
                || intent.plan.operation != "close"
                || intent.plan.strategy_instance != binding.instance_id
                || intent.instrument_id != binding.instrument_id
                || intent.risk_increase
            {
                return Err(StoreError::Invalid("GRAPH_CLOSE_RECOVERY_INTENT_MISMATCH"));
            }
            let residual = snapshot.lots.iter().any(|lot| {
                lot.intent_id.as_deref() == Some(recorded.original_reference.as_str())
            });
            let stage = if !intent.state.terminal() {
                GraphCloseRecoveryStage::AwaitingOrderOutcome
            } else if residual {
                GraphCloseRecoveryStage::TerminalResidualExposure
            } else {
                let local = Self::operation_sql(
                    db, Some(scope), &format!("local-terminal-release:{}", intent.intent_id),
                )?;
                match local {
                    Some(proof) if proof.kind == "LOCAL_TERMINAL_RELEASE"
                        && proof.request_hash == intent.request_hash
                        && proof.result_ref.len() == 64
                        && proof.result_ref.bytes().all(|b| b.is_ascii_hexdigit()) => {
                        let released: bool = db.query_row(
                            "SELECT EXISTS(SELECT 1 FROM risk_receipts
                             WHERE installation_id=?1 AND account_key=?2
                               AND intent_id=?3 AND reservation_key=?4
                               AND plan_hash=?5 AND state='RELEASED')",
                            params![scope.0, scope.1, intent.intent_id,
                                format!("local:{}", intent.intent_id), intent.plan_hash],
                            |row| row.get(0),
                        )?;
                        if released {
                            GraphCloseRecoveryStage::Settled
                        } else {
                            GraphCloseRecoveryStage::AwaitingSourceSettlement
                        }
                    }
                    None => GraphCloseRecoveryStage::AwaitingSourceSettlement,
                    Some(_) => return Err(StoreError::Invalid("GRAPH_CLOSE_RECOVERY_PROOF_CORRUPT")),
                }
            };
            outcomes.push(GraphCloseRecovery {
                original_reference: recorded.original_reference,
                intent_id: intent.intent_id,
                intent_state: intent.state,
                stage,
            });
        }
        Ok(outcomes)
    }

    /// Find active graph bindings from original immutable receipts. Page
    /// by commit time and stored key using the existing receipt index, then
    /// filter stopped/revised strategies. A
    /// process restart needs no volatile subscription registry or side DB.
    pub(crate) fn graph_bindings_page_sql(
        db: &Connection,
        scope: (&str, &str),
        limit: usize,
        offset: usize,
    ) -> StoreResult<EngineGraphBindingPage> {
        if !(1..=4).contains(&limit) || offset > 100_000 {
            return Err(StoreError::Invalid("GRAPH_FINANCE_PAGE_INVALID"));
        }
        let mut query = db.prepare(
            "SELECT operation_id,kind,request_hash,result_ref,committed_at
             FROM operation_receipts
             WHERE installation_id=?1 AND account_key=?2 AND kind=?3
             ORDER BY committed_at,operation_id LIMIT ?4 OFFSET ?5",
        )?;
        let receipts = query
            .query_map(
                params![scope.0, scope.1, BINDING_KIND, limit as i64, offset as i64],
                |row| {
                    Ok(OperationReceipt {
                        operation_id: row.get(0)?,
                        kind: row.get(1)?,
                        request_hash: row.get(2)?,
                        result_ref: row.get(3)?,
                        committed_at: row.get(4)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let scanned = receipts.len();
        let mut active = Vec::new();
        for receipt in receipts {
            let binding: EngineGraphBinding = decode(&receipt, BINDING_KIND)?;
            if !binding.valid()
                || binding.account_key != scope.1
                || receipt.operation_id != binding_key(&binding)
            {
                return Err(StoreError::Invalid("GRAPH_FINANCE_BINDING_RECEIPT_INVALID"));
            }
            let Some(instance) = Self::instance_sql(db, Some(scope), &binding.instance_id)? else {
                continue;
            };
            if instance.enabled
                && instance.definition_id == "graph_signal_v1"
                && instance.policy_revision == binding.policy_revision
                && instance.definition_version == binding.definition_version
            {
                active.push(binding);
            }
        }
        Ok(EngineGraphBindingPage { scanned, active })
    }

    fn graph_binding_current_sql(
        db: &Connection,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
    ) -> StoreResult<()> {
        if !binding.valid() || binding.account_key != scope.1 {
            return Err(StoreError::Invalid("GRAPH_FINANCE_BINDING_INVALID"));
        }
        let instance = Self::instance_sql(db, Some(scope), &binding.instance_id)?
            .ok_or(StoreError::Invalid("STRATEGY_INSTANCE_NOT_FOUND"))?;
        // Graph activation is a separate strategy mode. Never allow the
        // existing per-bar EMA dispatcher and graph outbox to trade the
        // same instance under one original finance owner.
        if instance.definition_id != "graph_signal_v1" {
            return Err(StoreError::Invalid("GRAPH_FINANCE_STRATEGY_MODE_MISMATCH"));
        }
        if !instance.enabled
            || instance.policy_revision != binding.policy_revision
            || instance.definition_version != binding.definition_version
        {
            return Err(StoreError::Invalid("GRAPH_FINANCE_STRATEGY_CHANGED"));
        }
        Ok(())
    }

    fn last_graph_cursor_sql(
        db: &Connection,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
    ) -> StoreResult<EngineGraphCursor> {
        let prefix = cursor_prefix(binding);
        // Fixed 20-digit sequence makes PRIMARY KEY range order equivalent
        // to monotone sequence order without a new index or table.
        let next_prefix = format!("{prefix}~");
        let row: Option<(String, String, String, String, i64)> = db
            .query_row(
                "SELECT operation_id,kind,request_hash,result_ref,committed_at
                 FROM operation_receipts WHERE installation_id=?1 AND account_key=?2
                 AND operation_id>=?3 AND operation_id<?4
                 ORDER BY operation_id DESC LIMIT 1",
                params![scope.0, scope.1, prefix, next_prefix],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()?;
        let (operation_id, kind, request_hash, result_ref, committed_at) =
            row.ok_or(StoreError::Invalid("GRAPH_FINANCE_CURSOR_MISSING"))?;
        let cursor: EngineGraphCursor = decode(
            &OperationReceipt {
                operation_id: operation_id.clone(),
                kind,
                request_hash,
                result_ref,
                committed_at,
            },
            CURSOR_KIND,
        )?;
        if !cursor.valid_for(binding) || operation_id != cursor_key(binding, cursor.sequence) {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CURSOR_CORRUPT"));
        }
        Ok(cursor)
    }

    /// Read the binding for exactly one instrument of the current strategy
    /// revision. Stopped or reconfigured strategies cannot revive an old one.
    pub(crate) fn graph_progress_sql(
        db: &Connection,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
    ) -> StoreResult<Option<EngineGraphProgress>> {
        Self::graph_binding_current_sql(db, scope, binding)?;
        let Some(receipt) = Self::operation_sql(db, Some(scope), &binding_key(binding))? else {
            return Ok(None);
        };
        let stored: EngineGraphBinding = decode(&receipt, BINDING_KIND)?;
        if stored != *binding {
            return Err(StoreError::Invalid("GRAPH_FINANCE_BINDING_CONFLICT"));
        }
        let cursor = Self::last_graph_cursor_sql(db, scope, &stored)?;
        Ok(Some(EngineGraphProgress {
            binding: stored,
            cursor,
        }))
    }

    /// Register an existing, explicitly activated strategy and its verified
    /// Research restore cursor. Replays return the current durable progress;
    /// they never rewind it. One graph per instrument and policy revision.
    pub(crate) fn bind_graph_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        restored: &EngineGraphCursor,
        now: i64,
    ) -> StoreResult<EngineGraphProgress> {
        if now <= 0 || !binding.valid() || !restored.valid_for(binding) {
            return Err(StoreError::Invalid("GRAPH_FINANCE_RESTORE_INVALID"));
        }
        Self::graph_binding_current_sql(tx, scope, binding)?;
        if let Some(existing) = Self::graph_progress_sql(tx, scope, binding)? {
            return Ok(existing);
        }
        insert_receipt(tx, scope, &binding_key(binding), BINDING_KIND, binding, now)?;
        insert_receipt(
            tx,
            scope,
            &cursor_key(binding, restored.sequence),
            CURSOR_KIND,
            restored,
            now,
        )?;
        Ok(EngineGraphProgress {
            binding: binding.clone(),
            cursor: restored.clone(),
        })
    }

    /// ACK only when this exact original-finance transaction sees no action
    /// and every staged Close for the candle has original terminal settlement.
    pub(crate) fn acknowledge_graph_idle_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        published: &EngineGraphCursor,
        now: i64,
    ) -> StoreResult<GraphIdleAck> {
        if now <= 0 || !published.valid_for(binding) || published.closed_ms > now as u64 {
            return Err(StoreError::Invalid("GRAPH_FINANCE_PUBLICATION_INVALID"));
        }
        let state = Self::graph_progress_sql(tx, scope, binding)?
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_BINDING_MISSING"))?;
        let prior = state.cursor;
        if published.sequence == prior.sequence {
            return if *published == prior {
                Ok(GraphIdleAck::Replayed)
            } else {
                Err(StoreError::Invalid("GRAPH_FINANCE_CONFLICTING_REPLAY"))
            };
        }
        if prior.sequence.checked_add(1) != Some(published.sequence) {
            return Err(StoreError::Invalid("GRAPH_FINANCE_SEQUENCE_GAP"));
        }
        if published.closed_ms <= prior.closed_ms {
            return Err(StoreError::Invalid("GRAPH_FINANCE_NOT_MONOTONIC"));
        }
        let snapshot = Self::strategy_position_snapshot_sql(
            tx,
            Some(scope),
            &binding.instance_id,
            &binding.instrument_id,
        )?;
        let plan = plan_strategy_target(
            published.intent,
            &binding.instance_id,
            &binding.instrument_id,
            &snapshot.lots,
            snapshot.in_flight,
        )
        .map_err(|_| StoreError::Invalid("GRAPH_FINANCE_POSITION_INVALID"))?;
        if plan != TargetPlan::NoChange {
            return Err(StoreError::Invalid("GRAPH_FINANCE_ATOMIC_INTENT_REQUIRED"));
        }
        if !matches!(published.intent, crate::position_target::TargetPosition::Hold)
            && Self::graph_close_recoveries_sql(tx, scope, binding, published)?
                .iter()
                .any(|progress| progress.stage != GraphCloseRecoveryStage::Settled)
        {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_SETTLEMENT_PENDING"));
        }
        insert_receipt(
            tx,
            scope,
            &cursor_key(binding, published.sequence),
            CURSOR_KIND,
            published,
            now,
        )?;
        Ok(GraphIdleAck::Advanced)
    }

    /// The first actionable path: one new opening, original strategy decision,
    /// PREPARED intent and graph cursor are committed in a SINGLE finance
    /// writer transaction. No new financial admission, exchange send or
    /// second SQL owner is introduced. Close/flip/settlement remain denied.
    #[expect(
        clippy::too_many_arguments,
        reason = "One original SQL owner requires explicit scoped evidence and CAS data."
    )]
    pub(crate) fn prepare_graph_open_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        published: &EngineGraphCursor,
        decision: &Decision,
        intent: &NewIntent,
        now: i64,
    ) -> StoreResult<GraphOpenClaim> {
        // Preserve the original finance command's failure-atomicity even if
        // a future caller mistakenly commits its outer transaction on Err.
        tx.execute_batch("SAVEPOINT okx_graph_open_command")?;
        match Self::prepare_graph_open_inner_sql(
            tx, scope, binding, published, decision, intent, now,
        ) {
            Ok(claim) => {
                tx.execute_batch("RELEASE SAVEPOINT okx_graph_open_command")?;
                Ok(claim)
            }
            Err(error) => {
                tx.execute_batch(
                    "ROLLBACK TO SAVEPOINT okx_graph_open_command;
                     RELEASE SAVEPOINT okx_graph_open_command",
                )?;
                Err(error)
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Keep scoped original financial claim data together."
    )]
    fn prepare_graph_open_inner_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        published: &EngineGraphCursor,
        decision: &Decision,
        intent: &NewIntent,
        now: i64,
    ) -> StoreResult<GraphOpenClaim> {
        if now <= 0 || !published.valid_for(binding) || published.closed_ms > now as u64 {
            return Err(StoreError::Invalid("GRAPH_FINANCE_PUBLICATION_INVALID"));
        }
        let state = Self::graph_progress_sql(tx, scope, binding)?
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_BINDING_MISSING"))?;
        let identity = binding
            .open_identity(published)
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_OPEN_IDENTITY_INVALID"))?;
        let key = identity.decision_key;
        let expected_input_digest = identity.input_digest;
        let (_, fingerprint) = encode(&(binding, published, decision, intent))?;
        if intent.idempotency_key != key
            || intent.plan.strategy_instance != binding.instance_id
            || intent.instrument_id != binding.instrument_id
            || decision.instance_id.as_deref() != Some(binding.instance_id.as_str())
            || decision.decision_id != identity.decision_id
            || decision.input_digest != expected_input_digest
            || decision.policy_revision < 1
            || decision.policy_revision != intent.policy_revision
        {
            return Err(StoreError::Invalid("GRAPH_FINANCE_OPEN_IDENTITY_INVALID"));
        }
        let prior = state.cursor;
        if published.sequence == prior.sequence {
            if *published != prior {
                return Err(StoreError::Invalid("GRAPH_FINANCE_CONFLICTING_REPLAY"));
            }
            let receipt =
                Self::operation_sql(tx, Some(scope), &open_key(binding, published.sequence))?
                    .ok_or(StoreError::Invalid("GRAPH_FINANCE_OPEN_RECEIPT_MISSING"))?;
            let recorded: PreparedGraphOpen = decode(&receipt, OPEN_KIND)?;
            if recorded.input_fingerprint != fingerprint || recorded.intent_id != intent.intent_id {
                return Err(StoreError::Invalid("GRAPH_FINANCE_CONFLICTING_REPLAY"));
            }
            let existing = Self::find_intent_sql(tx, Some(scope), "intent_id", &intent.intent_id)?
                .ok_or(StoreError::Invalid("GRAPH_FINANCE_PREPARED_INTENT_MISSING"))?;
            if existing.idempotency_key != key || existing.request_hash != intent.request_hash {
                return Err(StoreError::Invalid("GRAPH_FINANCE_OPEN_RECEIPT_CORRUPT"));
            }
            return Ok(GraphOpenClaim::Replayed {
                intent_id: intent.intent_id.clone(),
            });
        }
        if prior.sequence.checked_add(1) != Some(published.sequence) {
            return Err(StoreError::Invalid("GRAPH_FINANCE_SEQUENCE_GAP"));
        }
        if published.closed_ms <= prior.closed_ms {
            return Err(StoreError::Invalid("GRAPH_FINANCE_NOT_MONOTONIC"));
        }
        if decision.decision_at != now {
            return Err(StoreError::Invalid(
                "GRAPH_FINANCE_OPEN_DECISION_TIME_INVALID",
            ));
        }
        let snapshot = Self::strategy_position_snapshot_sql(
            tx,
            Some(scope),
            &binding.instance_id,
            &binding.instrument_id,
        )?;
        let plan = plan_strategy_target(
            published.intent,
            &binding.instance_id,
            &binding.instrument_id,
            &snapshot.lots,
            snapshot.in_flight,
        )
        .map_err(|_| StoreError::Invalid("GRAPH_FINANCE_POSITION_INVALID"))?;
        let side_matches = match plan {
            TargetPlan::Open(ExposureSide::Long) => {
                intent.plan.position_side == "long" && intent.plan.side == "buy"
            }
            TargetPlan::Open(ExposureSide::Short) => {
                intent.plan.position_side == "short" && intent.plan.side == "sell"
            }
            _ => false,
        };
        if !side_matches || intent.plan.operation != "open" || !intent.risk_increase {
            return Err(StoreError::Invalid(
                "GRAPH_FINANCE_OPEN_REQUIRES_EMPTY_EXPOSURE",
            ));
        }
        let claimed = Self::claim_strategy_decision_sql(
            tx,
            Some(scope),
            &binding.instance_id,
            &key,
            &expected_input_digest,
            decision.policy_revision,
            Some(&binding.definition_version),
            now,
        )?;
        if claimed.as_deref() != Some(decision.decision_id.as_str()) {
            return Err(StoreError::Invalid("GRAPH_FINANCE_OPEN_CLAIM_CONFLICT"));
        }
        if !Self::prepare_sql_transaction(tx, Some(scope), decision, intent)? {
            return Err(StoreError::Invalid("GRAPH_FINANCE_OPEN_ALREADY_PREPARED"));
        }
        insert_receipt(
            tx,
            scope,
            &cursor_key(binding, published.sequence),
            CURSOR_KIND,
            published,
            now,
        )?;
        insert_receipt(
            tx,
            scope,
            &open_key(binding, published.sequence),
            OPEN_KIND,
            &PreparedGraphOpen {
                input_fingerprint: fingerprint,
                intent_id: intent.intent_id.clone(),
            },
            now,
        )?;
        Ok(GraphOpenClaim::Prepared {
            intent_id: intent.intent_id.clone(),
        })
    }

    /// Stage exactly ONE lot-reference reduction in the ORIGINAL finance
    /// transaction: immutable decision, PREPARED intent and claim receipt.
    /// Crucially there is NO graph cursor ACK. An in-flight/partial close
    /// blocks the same candle until authoritative settlement is re-read.
    #[expect(
        clippy::too_many_arguments,
        reason = "One scoped original SQL claim with explicit evidence."
    )]
    pub(crate) fn prepare_graph_close_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        published: &EngineGraphCursor,
        original_reference: &str,
        decision: &Decision,
        intent: &NewIntent,
        now: i64,
    ) -> StoreResult<GraphCloseClaim> {
        tx.execute_batch("SAVEPOINT okx_graph_close_command")?;
        match Self::prepare_graph_close_inner_sql(
            tx,
            scope,
            binding,
            published,
            original_reference,
            decision,
            intent,
            now,
        ) {
            Ok(claim) => {
                tx.execute_batch("RELEASE SAVEPOINT okx_graph_close_command")?;
                Ok(claim)
            }
            Err(error) => {
                tx.execute_batch(
                    "ROLLBACK TO SAVEPOINT okx_graph_close_command;
                     RELEASE SAVEPOINT okx_graph_close_command",
                )?;
                Err(error)
            }
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Keep original reference and financial plan in one claim."
    )]
    fn prepare_graph_close_inner_sql(
        tx: &rusqlite::Transaction<'_>,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        published: &EngineGraphCursor,
        original_reference: &str,
        decision: &Decision,
        intent: &NewIntent,
        now: i64,
    ) -> StoreResult<GraphCloseClaim> {
        if now <= 0 || !published.valid_for(binding) || published.closed_ms > now as u64 {
            return Err(StoreError::Invalid("GRAPH_FINANCE_PUBLICATION_INVALID"));
        }
        let state = Self::graph_progress_sql(tx, scope, binding)?
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_BINDING_MISSING"))?;
        let identity = binding
            .close_identity(published, original_reference)
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_CLOSE_IDENTITY_INVALID"))?;
        let (_, fingerprint) = encode(&(binding, published, original_reference, decision, intent))?;
        if intent.idempotency_key != identity.decision_key
            || intent.plan.strategy_instance != binding.instance_id
            || intent.instrument_id != binding.instrument_id
            || intent.plan.operation != "close"
            || intent.risk_increase
            || decision.instance_id.as_deref() != Some(binding.instance_id.as_str())
            || decision.decision_id != identity.decision_id
            || decision.input_digest != identity.input_digest
            || decision.policy_revision < 1
            || decision.policy_revision != intent.policy_revision
        {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_IDENTITY_INVALID"));
        }
        if state.cursor.sequence.checked_add(1) != Some(published.sequence)
            || published.closed_ms <= state.cursor.closed_ms
        {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_CURSOR_NOT_NEXT"));
        }
        let receipt_id = close_key(binding, published, original_reference)?;
        if let Some(receipt) = Self::operation_sql(tx, Some(scope), &receipt_id)? {
            let recorded: PreparedGraphClose = decode(&receipt, CLOSE_KIND)?;
            if recorded.input_fingerprint != fingerprint
                || recorded.original_reference != original_reference
                || recorded.intent_id != intent.intent_id
            {
                return Err(StoreError::Invalid("GRAPH_FINANCE_CONFLICTING_REPLAY"));
            }
            let existing = Self::find_intent_sql(tx, Some(scope), "intent_id", &intent.intent_id)?
                .ok_or(StoreError::Invalid("GRAPH_FINANCE_PREPARED_INTENT_MISSING"))?;
            if existing.idempotency_key != identity.decision_key
                || existing.request_hash != intent.request_hash
            {
                return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_RECEIPT_CORRUPT"));
            }
            return Ok(GraphCloseClaim::Replayed {
                intent_id: intent.intent_id.clone(),
            });
        }
        if decision.decision_at != now {
            return Err(StoreError::Invalid(
                "GRAPH_FINANCE_CLOSE_DECISION_TIME_INVALID",
            ));
        }
        let original = Self::find_intent_sql(tx, Some(scope), "intent_id", original_reference)?
            .ok_or(StoreError::Invalid("GRAPH_FINANCE_CLOSE_ORIGINAL_MISSING"))?;
        if original.intent_id != original_reference
            || original.plan.operation != "open"
            || original.plan.strategy_instance != binding.instance_id
            || original.instrument_id != binding.instrument_id
            || original.plan.position_side != intent.plan.position_side
        {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_ORIGINAL_MISMATCH"));
        }
        let snapshot = Self::strategy_position_snapshot_sql(
            tx,
            Some(scope),
            &binding.instance_id,
            &binding.instrument_id,
        )?;
        let plan = plan_strategy_target(
            published.intent,
            &binding.instance_id,
            &binding.instrument_id,
            &snapshot.lots,
            snapshot.in_flight,
        )
        .map_err(|_| StoreError::Invalid("GRAPH_FINANCE_POSITION_INVALID"))?;
        let TargetPlan::CloseExisting(references) = plan else {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_EXPOSURE_REQUIRED"));
        };
        if !references
            .iter()
            .any(|reference| reference == original_reference)
        {
            return Err(StoreError::Invalid(
                "GRAPH_FINANCE_CLOSE_REFERENCE_NOT_TARGETED",
            ));
        }
        let mut quantity = Decimal::ZERO;
        for lot in snapshot
            .lots
            .iter()
            .filter(|lot| lot.intent_id.as_deref() == Some(original_reference))
        {
            if lot.side != original.plan.position_side || lot.remaining_qty <= Decimal::ZERO {
                return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_LOT_INVALID"));
            }
            quantity = quantity
                .checked_add(lot.remaining_qty)
                .ok_or(StoreError::Invalid("GRAPH_FINANCE_CLOSE_QUANTITY_OVERFLOW"))?;
        }
        let side = match original.plan.position_side.as_str() {
            "long" => "sell",
            "short" => "buy",
            _ => {
                return Err(StoreError::Invalid(
                    "GRAPH_FINANCE_CLOSE_ORIGINAL_SIDE_INVALID",
                ));
            }
        };
        if quantity <= Decimal::ZERO || intent.plan.quantity != quantity || intent.plan.side != side
        {
            return Err(StoreError::Invalid(
                "GRAPH_FINANCE_CLOSE_EXACT_QUANTITY_REQUIRED",
            ));
        }
        let claimed = Self::claim_strategy_decision_sql(
            tx,
            Some(scope),
            &binding.instance_id,
            &identity.decision_key,
            &identity.input_digest,
            decision.policy_revision,
            Some(&binding.definition_version),
            now,
        )?;
        if claimed.as_deref() != Some(decision.decision_id.as_str()) {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_CLAIM_CONFLICT"));
        }
        if !Self::prepare_sql_transaction(tx, Some(scope), decision, intent)? {
            return Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_ALREADY_PREPARED"));
        }
        insert_receipt(
            tx,
            scope,
            &receipt_id,
            CLOSE_KIND,
            &PreparedGraphClose {
                input_fingerprint: fingerprint,
                original_reference: original_reference.to_owned(),
                intent_id: intent.intent_id.clone(),
            },
            now,
        )?;
        Ok(GraphCloseClaim::Prepared {
            intent_id: intent.intent_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::position_target::TargetPosition;

    fn initialized(db: Connection) -> Connection {
        db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        db.pragma_update(None, "foreign_keys", "ON").unwrap();
        for account in ["account-a", "account-b"] {
            db.execute(
                "INSERT INTO account_meta VALUES(1,?1,'installation',2,1,'test','init','test')",
                [account],
            )
            .unwrap();
            db.execute(
                "INSERT INTO account_policy VALUES('installation',?1,1,1,0,'campaign','20','0')",
                [account],
            )
            .unwrap();
        }
        db
    }

    fn database() -> Connection {
        initialized(Connection::open_in_memory().unwrap())
    }

    fn binding(db: &Connection, account: &str) -> EngineGraphBinding {
        let instance =
            AccountStore::instance_sql(db, Some(("installation", account)), "strategy-a")
                .unwrap()
                .unwrap();
        EngineGraphBinding {
            account_key: account.into(),
            instance_id: "strategy-a".into(),
            definition_version: instance.definition_version,
            policy_revision: instance.policy_revision,
            session_id: "research:strategy-a:btc".into(),
            graph_sha256: "a".repeat(64),
            instrument_id: "BTC-USDT-SWAP".into(),
            timeframe_ms: 60_000,
        }
    }

    fn create(tx: &rusqlite::Transaction<'_>, account: &str) {
        let instance = StrategyInstance {
            instance_id: "strategy-a".into(),
            definition_id: "graph_signal_v1".into(),
            definition_version: "1".into(),
            parameters: "{}".into(),
            enabled: true,
            policy_revision: 1,
            budget: Decimal::ONE,
            last_decision_key: None,
        };
        AccountStore::mutate_instance_sql(
            tx,
            Some(("installation", account)),
            "test-strategy-create",
            "test-create-hash",
            "create",
            &instance,
            None,
            1,
        )
        .unwrap();
    }

    fn cursor(sequence: u64, intent: TargetPosition) -> EngineGraphCursor {
        EngineGraphCursor {
            sequence,
            closed_ms: sequence * 60_000,
            intent,
        }
    }

    fn opening(
        db: &Connection,
        scope: (&str, &str),
        binding: &EngineGraphBinding,
        publication: &EngineGraphCursor,
    ) -> (Decision, NewIntent) {
        let definition =
            AccountStore::strategy_definition_sql(db, Some(scope), &binding.instance_id).unwrap();
        let identity = binding.open_identity(publication).unwrap();
        let side = match publication.intent {
            TargetPosition::Long => ("long", "buy"),
            TargetPosition::Short => ("short", "sell"),
            _ => ("long", "buy"),
        };
        (
            Decision {
                decision_id: identity.decision_id,
                instance_id: Some(binding.instance_id.clone()),
                decision_at: 181_000,
                input_digest: identity.input_digest,
                policy_revision: 1,
                code_version: "graph-open/v1".into(),
            },
            NewIntent {
                plan: IntentPlan {
                    strategy_definition_id: definition.definition_id,
                    strategy_definition_version: definition.version,
                    strategy_parameters_digest: definition.parameters_digest,
                    contract_value: Decimal::ONE,
                    inverse: false,
                    settlement_currency: "USDT".into(),
                    position_side: side.0.into(),
                    private_source_generation: 1,
                    operation: "open".into(),
                    side: side.1.into(),
                    quantity: Decimal::ONE,
                    limit_price: Decimal::from(100),
                    stop_price: Some(Decimal::from(90)),
                    strategy_instance: binding.instance_id.clone(),
                    planned_loss_usd: Decimal::ONE,
                    source_generation: 1,
                },
                intent_id: "graph-test-intent".into(),
                idempotency_key: identity.decision_key,
                request_hash: "graph-test-request-hash".into(),
                client_order_id: "graph-test-client".into(),
                deadline: 300_000,
                plan_hash: "graph-test-plan-hash".into(),
                policy_revision: 1,
                risk_increase: true,
                instrument_id: binding.instrument_id.clone(),
            },
        )
    }

    fn close_fixture(
        db: &mut Connection,
    ) -> (EngineGraphBinding, EngineGraphCursor, Decision, NewIntent) {
        let scope = ("installation", "account-a");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
        let binding = binding(&tx, scope.1);
        AccountStore::bind_graph_sql(
            &tx,
            scope,
            &binding,
            &cursor(2, TargetPosition::Flat),
            121_000,
        )
        .unwrap();
        let publication = cursor(3, TargetPosition::Long);
        let (opening_decision, opening_intent) = opening(&tx, scope, &binding, &publication);
        assert!(matches!(
            AccountStore::prepare_graph_open_sql(
                &tx,
                scope,
                &binding,
                &publication,
                &opening_decision,
                &opening_intent,
                181_000
            ),
            Ok(GraphOpenClaim::Prepared { .. })
        ));
        tx.execute(
            "UPDATE intents SET state='FILLED' WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
            params![scope.0, scope.1, opening_intent.intent_id],
        ).unwrap();
        tx.execute(
            "INSERT INTO position_lots (installation_id,account_key,lot_id,instance_id,intent_id,fill_id,instrument_id,side,remaining_qty,cost,opened_at)
             VALUES (?1,?2,'graph-original-lot',?3,?4,NULL,?5,'long','1','100',182000)",
            params![scope.0, scope.1, binding.instance_id, opening_intent.intent_id, binding.instrument_id],
        ).unwrap();
        tx.commit().unwrap();

        let closing = cursor(4, TargetPosition::Flat);
        let identity = binding
            .close_identity(&closing, &opening_intent.intent_id)
            .unwrap();
        let mut plan = opening_intent.plan;
        plan.operation = "close".into();
        plan.side = "sell".into();
        plan.stop_price = None;
        (
            binding,
            closing,
            Decision {
                decision_id: identity.decision_id,
                instance_id: Some("strategy-a".into()),
                decision_at: 241_000,
                input_digest: identity.input_digest,
                policy_revision: 1,
                code_version: "graph-close/v1".into(),
            },
            NewIntent {
                plan,
                intent_id: "graph-test-close-intent".into(),
                idempotency_key: identity.decision_key,
                request_hash: "graph-test-close-request-hash".into(),
                client_order_id: "graph-test-close-client".into(),
                deadline: 300_000,
                plan_hash: "graph-test-close-plan-hash".into(),
                policy_revision: 1,
                risk_increase: false,
                instrument_id: "BTC-USDT-SWAP".into(),
            },
        )
    }

    #[test]
    fn graph_close_claim_stages_original_reduction_without_ack_and_replays() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let (binding, closing, decision, intent) = close_fixture(&mut db);
        {
            let tx = db.transaction().unwrap();
            assert_eq!(
                AccountStore::prepare_graph_close_sql(
                    &tx,
                    scope,
                    &binding,
                    &closing,
                    "graph-test-intent",
                    &decision,
                    &intent,
                    241_000,
                )
                .unwrap(),
                GraphCloseClaim::Prepared {
                    intent_id: intent.intent_id.clone()
                }
            );
            tx.commit().unwrap();
        }
        assert_eq!(
            AccountStore::graph_progress_sql(&db, scope, &binding)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            3
        );
        let saved = AccountStore::find_intent_sql(&db, Some(scope), "intent_id", &intent.intent_id)
            .unwrap()
            .unwrap();
        assert_eq!(saved.state, IntentState::Prepared);
        assert_eq!(saved.plan.operation, "close");
        assert_eq!(saved.plan.quantity, Decimal::ONE);
        let tx = db.transaction().unwrap();
        assert_eq!(
            AccountStore::prepare_graph_close_sql(
                &tx,
                scope,
                &binding,
                &closing,
                "graph-test-intent",
                &decision,
                &intent,
                242_000,
            )
            .unwrap(),
            GraphCloseClaim::Replayed {
                intent_id: intent.intent_id.clone()
            }
        );
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(&tx, scope, &binding, &closing, 242_000),
            Err(StoreError::Invalid("GRAPH_FINANCE_ATOMIC_INTENT_REQUIRED"))
        ));
        assert!(
            AccountStore::strategy_position_snapshot_sql(
                &tx,
                Some(scope),
                &binding.instance_id,
                &binding.instrument_id
            )
            .unwrap()
            .in_flight
        );
    }

    #[test]
    fn graph_close_claim_rejects_quantity_foreign_reference_conflicts_and_rolls_back() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let (binding, closing, decision, intent) = close_fixture(&mut db);
        {
            let tx = db.transaction().unwrap();
            let mut wrong = intent.clone();
            wrong.plan.quantity = Decimal::from(2);
            assert!(matches!(
                AccountStore::prepare_graph_close_sql(
                    &tx,
                    scope,
                    &binding,
                    &closing,
                    "graph-test-intent",
                    &decision,
                    &wrong,
                    241_000
                ),
                Err(StoreError::Invalid(
                    "GRAPH_FINANCE_CLOSE_EXACT_QUANTITY_REQUIRED"
                ))
            ));
            assert!(matches!(
                AccountStore::prepare_graph_close_sql(
                    &tx,
                    scope,
                    &binding,
                    &closing,
                    "foreign-entry",
                    &decision,
                    &intent,
                    241_000
                ),
                Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_IDENTITY_INVALID"))
            ));
            // Caller committing a failed command must not leak the decision.
            tx.commit().unwrap();
        }
        assert!(
            AccountStore::find_intent_sql(&db, Some(scope), "intent_id", &intent.intent_id)
                .unwrap()
                .is_none()
        );
        {
            let tx = db.transaction().unwrap();
            assert!(matches!(
                AccountStore::prepare_graph_close_sql(
                    &tx,
                    scope,
                    &binding,
                    &closing,
                    "graph-test-intent",
                    &decision,
                    &intent,
                    241_000
                ),
                Ok(GraphCloseClaim::Prepared { .. })
            ));
            // Simulate a crash before commit.
        }
        assert!(
            AccountStore::find_intent_sql(&db, Some(scope), "intent_id", &intent.intent_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            AccountStore::graph_progress_sql(&db, scope, &binding)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            3
        );
        let tx = db.transaction().unwrap();
        assert!(matches!(
            AccountStore::prepare_graph_close_sql(
                &tx,
                scope,
                &binding,
                &cursor(5, TargetPosition::Flat),
                "graph-test-intent",
                &decision,
                &intent,
                301_000
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_IDENTITY_INVALID"))
        ));
        assert_eq!(
            AccountStore::prepare_graph_close_sql(
                &tx,
                scope,
                &binding,
                &closing,
                "graph-test-intent",
                &decision,
                &intent,
                241_000
            )
            .unwrap(),
            GraphCloseClaim::Prepared {
                intent_id: intent.intent_id.clone()
            }
        );
        let mut conflicting = intent.clone();
        conflicting.request_hash = "other-close-request".into();
        assert!(matches!(
            AccountStore::prepare_graph_close_sql(
                &tx,
                scope,
                &binding,
                &closing,
                "graph-test-intent",
                &decision,
                &conflicting,
                241_000
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_CONFLICTING_REPLAY"))
        ));
        tx.commit().unwrap();
    }

    #[test]
    fn graph_close_restart_requires_original_finance_release_before_cursor_ack() {
        let path = std::env::temp_dir().join(format!(
            "okx-graph-close-recovery-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let mut db = initialized(Connection::open(&path).unwrap());
        let scope = ("installation", "account-a");
        let (binding, publication, decision, intent) = close_fixture(&mut db);
        {
            let tx = db.transaction().unwrap();
            assert!(matches!(
                AccountStore::prepare_graph_close_sql(
                    &tx, scope, &binding, &publication, "graph-test-intent",
                    &decision, &intent, 241_000,
                ),
                Ok(GraphCloseClaim::Prepared { .. })
            ));
            tx.commit().unwrap();
        }
        drop(db);
        let mut reopened = Connection::open(&path).unwrap();
        reopened.pragma_update(None, "foreign_keys", "ON").unwrap();
        let inspect = |db: &Connection| {
            AccountStore::graph_close_recoveries_sql(db, scope, &binding, &publication).unwrap()
        };
        assert_eq!(inspect(&reopened), vec![GraphCloseRecovery {
            original_reference: "graph-test-intent".into(),
            intent_id: intent.intent_id.clone(),
            intent_state: IntentState::Prepared,
            stage: GraphCloseRecoveryStage::AwaitingOrderOutcome,
        }]);
        // A terminal venue order and zero remaining lots are NOT a final
        // finance proof. Independent fills/bills and local risk release follow.
        reopened.execute(
            "UPDATE intents SET state='FILLED' WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
            params![scope.0, scope.1, intent.intent_id],
        ).unwrap();
        reopened.execute(
            "UPDATE position_lots SET remaining_qty='0' WHERE installation_id=?1 AND account_key=?2 AND lot_id='graph-original-lot'",
            params![scope.0, scope.1],
        ).unwrap();
        assert_eq!(inspect(&reopened)[0].stage, GraphCloseRecoveryStage::AwaitingSourceSettlement);
        {
            let tx = reopened.transaction().unwrap();
            assert!(matches!(
                AccountStore::acknowledge_graph_idle_sql(&tx, scope, &binding, &publication, 242_000),
                Err(StoreError::Invalid("GRAPH_FINANCE_CLOSE_SETTLEMENT_PENDING"))
            ));
        }
        // Mimic only the immutable receipt emitted by ORIGINAL
        // acknowledge_local_terminal_close_sql. That routine's actual
        // authenticated fill+bill proof is covered by its own tests.
        reopened.execute(
            "INSERT INTO operation_receipts
             (installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
             VALUES (?1,?2,?3,'LOCAL_TERMINAL_RELEASE',?4,?5,243000)",
            params![scope.0, scope.1, format!("local-terminal-release:{}", intent.intent_id),
                intent.request_hash, "f".repeat(64)],
        ).unwrap();
        assert_eq!(inspect(&reopened)[0].stage, GraphCloseRecoveryStage::AwaitingSourceSettlement);
        reopened.execute(
            "INSERT INTO risk_receipts
             (reservation_key,intent_id,account_key,installation_id,instance_id,
              plan_hash,policy_revision,receipt_hash,requested_loss_usd,conversion_digest,state)
             VALUES (?1,?2,?3,?4,'strategy-a',?5,1,?6,'0',?7,'RELEASED')",
            params![format!("local:{}", intent.intent_id), intent.intent_id,
                scope.1, scope.0, intent.plan_hash, "b".repeat(64), "c".repeat(64)],
        ).unwrap();
        assert_eq!(inspect(&reopened)[0].stage, GraphCloseRecoveryStage::Settled);
        {
            let tx = reopened.transaction().unwrap();
            assert_eq!(
                AccountStore::acknowledge_graph_idle_sql(&tx, scope, &binding, &publication, 244_000).unwrap(),
                GraphIdleAck::Advanced
            );
            tx.commit().unwrap();
        }
        assert_eq!(AccountStore::graph_progress_sql(&reopened, scope, &binding)
            .unwrap().unwrap().cursor.sequence, 4);
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn graph_close_terminal_residual_fails_closed_without_new_intent() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let (binding, publication, decision, intent) = close_fixture(&mut db);
        {
            let tx = db.transaction().unwrap();
            assert!(matches!(AccountStore::prepare_graph_close_sql(
                &tx, scope, &binding, &publication, "graph-test-intent",
                &decision, &intent, 241_000,
            ), Ok(GraphCloseClaim::Prepared { .. })));
            tx.commit().unwrap();
        }
        // A terminal cancellation can still leave the original position.
        db.execute(
            "UPDATE intents SET state='CANCELLED' WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
            params![scope.0, scope.1, intent.intent_id],
        ).unwrap();
        assert_eq!(
            AccountStore::graph_close_recoveries_sql(&db, scope, &binding, &publication).unwrap(),
            vec![GraphCloseRecovery {
                original_reference: "graph-test-intent".into(),
                intent_id: intent.intent_id.clone(),
                intent_state: IntentState::Cancelled,
                stage: GraphCloseRecoveryStage::TerminalResidualExposure,
            }]
        );
        let tx = db.transaction().unwrap();
        assert_eq!(AccountStore::prepare_graph_close_sql(
            &tx, scope, &binding, &publication, "graph-test-intent",
            &decision, &intent, 242_000,
        ).unwrap(), GraphCloseClaim::Replayed { intent_id: intent.intent_id.clone() });
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(&tx, scope, &binding, &publication, 242_000),
            Err(StoreError::Invalid("GRAPH_FINANCE_ATOMIC_INTENT_REQUIRED"))
        ));
        let count: i64 = tx.query_row(
            "SELECT COUNT(*) FROM intents WHERE installation_id=?1 AND account_key=?2 AND idempotency_key=?3",
            params![scope.0, scope.1, intent.idempotency_key], |r| r.get(0)
        ).unwrap();
        assert_eq!(count, 1, "a terminal partial/zero-fill may not invent a new POST");
    }

    #[test]
    fn graph_open_claim_commits_original_decision_prepared_intent_and_cursor_once() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let (binding, publication, decision, intent) = {
            let tx = db.transaction().unwrap();
            create(&tx, scope.1);
            let b = binding(&tx, scope.1);
            AccountStore::bind_graph_sql(&tx, scope, &b, &cursor(2, TargetPosition::Flat), 121_000)
                .unwrap();
            let publication = cursor(3, TargetPosition::Long);
            let (decision, intent) = opening(&tx, scope, &b, &publication);
            assert_eq!(
                AccountStore::prepare_graph_open_sql(
                    &tx,
                    scope,
                    &b,
                    &publication,
                    &decision,
                    &intent,
                    181_000
                )
                .unwrap(),
                GraphOpenClaim::Prepared {
                    intent_id: intent.intent_id.clone()
                }
            );
            tx.commit().unwrap();
            (b, publication, decision, intent)
        };
        assert_eq!(
            AccountStore::graph_progress_sql(&db, scope, &binding)
                .unwrap()
                .unwrap()
                .cursor,
            publication
        );
        let stored =
            AccountStore::find_intent_sql(&db, Some(scope), "intent_id", &intent.intent_id)
                .unwrap()
                .unwrap();
        assert_eq!(stored.state, IntentState::Prepared);
        assert_eq!(stored.idempotency_key, intent.idempotency_key);
        assert_eq!(stored.plan, intent.plan);
        let risk_rows: i64 = db.query_row(
            "SELECT COUNT(*) FROM risk_entries WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
            params![scope.0, scope.1, intent.intent_id], |r| r.get(0)
        ).unwrap();
        assert_eq!(
            risk_rows, 0,
            "PREPARED graph intent is not a risk reservation"
        );
        let tx = db.transaction().unwrap();
        assert_eq!(
            AccountStore::prepare_graph_open_sql(
                &tx,
                scope,
                &binding,
                &publication,
                &decision,
                &intent,
                241_000
            )
            .unwrap(),
            GraphOpenClaim::Replayed {
                intent_id: intent.intent_id.clone()
            }
        );
        tx.commit().unwrap();
        let decisions: i64 = db.query_row(
            "SELECT COUNT(*) FROM decisions WHERE installation_id=?1 AND account_key=?2 AND decision_id=?3",
            params![scope.0, scope.1, decision.decision_id], |r| r.get(0)
        ).unwrap();
        assert_eq!(decisions, 1);
        let intents: i64 = db.query_row(
            "SELECT COUNT(*) FROM intents WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
            params![scope.0, scope.1, intent.intent_id], |r| r.get(0)
        ).unwrap();
        assert_eq!(intents, 1);
    }

    #[test]
    fn graph_open_claim_rejects_wrong_side_idle_gap_and_conflicting_replay() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
        let b = binding(&tx, scope.1);
        AccountStore::bind_graph_sql(&tx, scope, &b, &cursor(2, TargetPosition::Flat), 121_000)
            .unwrap();
        let open = cursor(3, TargetPosition::Short);
        let (decision, intent) = opening(&tx, scope, &b, &open);
        let mut wrong_side = intent.clone();
        wrong_side.plan.side = "buy".into();
        assert!(matches!(
            AccountStore::prepare_graph_open_sql(
                &tx,
                scope,
                &b,
                &open,
                &decision,
                &wrong_side,
                181_000
            ),
            Err(StoreError::Invalid(
                "GRAPH_FINANCE_OPEN_REQUIRES_EMPTY_EXPOSURE"
            ))
        ));
        let flat = cursor(3, TargetPosition::Flat);
        let (flat_decision, flat_intent) = opening(&tx, scope, &b, &flat);
        assert!(matches!(
            AccountStore::prepare_graph_open_sql(
                &tx,
                scope,
                &b,
                &flat,
                &flat_decision,
                &flat_intent,
                181_000
            ),
            Err(StoreError::Invalid(
                "GRAPH_FINANCE_OPEN_REQUIRES_EMPTY_EXPOSURE"
            ))
        ));
        let gap = cursor(4, TargetPosition::Short);
        let (gap_decision, gap_intent) = opening(&tx, scope, &b, &gap);
        assert!(matches!(
            AccountStore::prepare_graph_open_sql(
                &tx,
                scope,
                &b,
                &gap,
                &gap_decision,
                &gap_intent,
                241_000
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_SEQUENCE_GAP"))
        ));
        AccountStore::prepare_graph_open_sql(&tx, scope, &b, &open, &decision, &intent, 181_000)
            .unwrap();
        let mut conflicting = intent.clone();
        conflicting.plan_hash = "changed-plan-hash".into();
        assert!(matches!(
            AccountStore::prepare_graph_open_sql(
                &tx,
                scope,
                &b,
                &open,
                &decision,
                &conflicting,
                241_000
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_CONFLICTING_REPLAY"))
        ));
        let next = cursor(4, TargetPosition::Long);
        let (mut next_decision, next_intent) = opening(&tx, scope, &b, &next);
        next_decision.decision_at = 241_000;
        assert!(
            matches!(
                AccountStore::prepare_graph_open_sql(
                    &tx,
                    scope,
                    &b,
                    &next,
                    &next_decision,
                    &next_intent,
                    241_000
                ),
                Err(StoreError::Invalid(
                    "GRAPH_FINANCE_OPEN_REQUIRES_EMPTY_EXPOSURE"
                ))
            ),
            "unsettled PREPARED opening must block the next graph open"
        );
        tx.commit().unwrap();
    }

    #[test]
    fn graph_open_claim_crash_before_commit_rolls_back_intent_decision_and_cursor() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let b = {
            let tx = db.transaction().unwrap();
            create(&tx, scope.1);
            let b = binding(&tx, scope.1);
            AccountStore::bind_graph_sql(&tx, scope, &b, &cursor(2, TargetPosition::Flat), 121_000)
                .unwrap();
            tx.commit().unwrap();
            b
        };
        let publication = cursor(3, TargetPosition::Long);
        let (decision, intent) = {
            let tx = db.transaction().unwrap();
            let (decision, intent) = opening(&tx, scope, &b, &publication);
            AccountStore::prepare_graph_open_sql(
                &tx,
                scope,
                &b,
                &publication,
                &decision,
                &intent,
                181_000,
            )
            .unwrap();
            (decision, intent)
            // Dropping the transaction emulates a crash before COMMIT.
        };
        assert_eq!(
            AccountStore::graph_progress_sql(&db, scope, &b)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            2
        );
        assert!(
            AccountStore::find_intent_sql(&db, Some(scope), "intent_id", &intent.intent_id)
                .unwrap()
                .is_none()
        );
        let claimed: i64 = db.query_row(
            "SELECT COUNT(*) FROM decisions WHERE installation_id=?1 AND account_key=?2 AND decision_id=?3",
            params![scope.0, scope.1, decision.decision_id], |r| r.get(0)
        ).unwrap();
        assert_eq!(claimed, 0);
    }

    #[test]
    fn graph_open_failed_prepare_undoes_claim_even_when_caller_commits_error() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let (b, decision, intent) = {
            let tx = db.transaction().unwrap();
            create(&tx, scope.1);
            let b = binding(&tx, scope.1);
            AccountStore::bind_graph_sql(&tx, scope, &b, &cursor(2, TargetPosition::Flat), 121_000)
                .unwrap();
            let (decision, mut intent) = opening(&tx, scope, &b, &cursor(3, TargetPosition::Long));
            // Fails the original finance intent deadline check *after* the
            // strategy decision claim has been inserted into this savepoint.
            intent.deadline = decision.decision_at;
            assert!(matches!(
                AccountStore::prepare_graph_open_sql(
                    &tx,
                    scope,
                    &b,
                    &cursor(3, TargetPosition::Long),
                    &decision,
                    &intent,
                    181_000
                ),
                Err(StoreError::Invalid("INVALID_INTENT_DEADLINE_OR_POLICY"))
            ));
            tx.commit().unwrap(); // Deliberate mis-use of an Err result.
            (b, decision, intent)
        };
        assert_eq!(
            AccountStore::graph_progress_sql(&db, scope, &b)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            2
        );
        assert!(
            AccountStore::find_intent_sql(&db, Some(scope), "intent_id", &intent.intent_id)
                .unwrap()
                .is_none()
        );
        let claims: i64 = db.query_row(
            "SELECT COUNT(*) FROM decisions WHERE installation_id=?1 AND account_key=?2 AND decision_id=?3",
            params![scope.0, scope.1, decision.decision_id], |r| r.get(0)
        ).unwrap();
        assert_eq!(claims, 0);
    }

    #[test]
    fn owner_replay_and_sql_reopen_preserve_exact_cursor_without_rewind() {
        let path = std::env::temp_dir().join(format!(
            "okx-graph-cursor-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let mut db = initialized(Connection::open(&path).unwrap());
        let scope = ("installation", "account-a");
        let b = {
            let tx = db.transaction().unwrap();
            create(&tx, scope.1);
            let b = binding(&tx, scope.1);
            let stored = AccountStore::bind_graph_sql(
                &tx,
                scope,
                &b,
                &cursor(2, TargetPosition::Hold),
                121_000,
            )
            .unwrap();
            assert_eq!(stored.cursor.sequence, 2);
            tx.commit().unwrap();
            b
        };
        {
            let tx = db.transaction().unwrap();
            assert_eq!(
                AccountStore::acknowledge_graph_idle_sql(
                    &tx,
                    scope,
                    &b,
                    &cursor(3, TargetPosition::Hold),
                    181_000,
                )
                .unwrap(),
                GraphIdleAck::Advanced
            );
            tx.commit().unwrap();
        }
        drop(db);
        let mut reopened = Connection::open(&path).unwrap();
        reopened.pragma_update(None, "foreign_keys", "ON").unwrap();
        {
            let tx = reopened.transaction().unwrap();
            let restored = AccountStore::bind_graph_sql(
                &tx,
                scope,
                &b,
                &cursor(2, TargetPosition::Hold),
                241_000,
            )
            .unwrap();
            assert_eq!(
                restored.cursor.sequence, 3,
                "idempotent restore must not rewind"
            );
            assert_eq!(
                AccountStore::acknowledge_graph_idle_sql(
                    &tx,
                    scope,
                    &b,
                    &cursor(3, TargetPosition::Hold),
                    241_000,
                )
                .unwrap(),
                GraphIdleAck::Replayed
            );
            tx.commit().unwrap();
        }
        let graph_receipts: i64 = reopened
            .query_row(
                "SELECT COUNT(*) FROM operation_receipts WHERE kind LIKE 'graph_%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(graph_receipts, 3, "one binding, one baseline, one ACK");
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn never_ack_actionable_gap_conflict_stale_or_foreign_events() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let other_scope = ("installation", "account-b");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
        create(&tx, other_scope.1);
        let b = binding(&tx, scope.1);
        let foreign = binding(&tx, other_scope.1);
        AccountStore::bind_graph_sql(&tx, scope, &b, &cursor(2, TargetPosition::Hold), 121_000)
            .unwrap();
        AccountStore::bind_graph_sql(
            &tx,
            other_scope,
            &foreign,
            &cursor(2, TargetPosition::Hold),
            121_000,
        )
        .unwrap();
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(
                &tx,
                scope,
                &b,
                &cursor(3, TargetPosition::Long),
                181_000,
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_ATOMIC_INTENT_REQUIRED"))
        ));
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(
                &tx,
                scope,
                &b,
                &cursor(4, TargetPosition::Hold),
                241_000,
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_SEQUENCE_GAP"))
        ));
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(
                &tx,
                scope,
                &b,
                &cursor(2, TargetPosition::Flat),
                181_000,
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_CONFLICTING_REPLAY"))
        ));
        let mut old = cursor(3, TargetPosition::Hold);
        old.closed_ms = 60_000;
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(&tx, scope, &b, &old, 181_000,),
            Err(StoreError::Invalid("GRAPH_FINANCE_NOT_MONOTONIC"))
        ));
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(
                &tx,
                scope,
                &foreign,
                &cursor(3, TargetPosition::Hold),
                181_000,
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_BINDING_INVALID"))
        ));
        assert_eq!(
            AccountStore::graph_progress_sql(&tx, scope, &b)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            2
        );
        assert_eq!(
            AccountStore::graph_progress_sql(&tx, other_scope, &foreign)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            2
        );
        tx.commit().unwrap();
    }

    #[test]
    fn stopped_or_revised_strategy_invalidates_binding_without_deleting_money() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
        let b = binding(&tx, scope.1);
        AccountStore::bind_graph_sql(&tx, scope, &b, &cursor(2, TargetPosition::Hold), 121_000)
            .unwrap();
        let mut instance = AccountStore::instance_sql(&tx, Some(scope), &b.instance_id)
            .unwrap()
            .unwrap();
        instance.enabled = false;
        instance.policy_revision = 2;
        AccountStore::mutate_instance_sql(
            &tx,
            Some(scope),
            "test-stop",
            "test-stop-hash",
            "stop",
            &instance,
            Some(1),
            181_000,
        )
        .unwrap();
        assert!(matches!(
            AccountStore::graph_progress_sql(&tx, scope, &b),
            Err(StoreError::Invalid("GRAPH_FINANCE_STRATEGY_CHANGED"))
        ));
        assert!(matches!(
            AccountStore::acknowledge_graph_idle_sql(
                &tx,
                scope,
                &b,
                &cursor(3, TargetPosition::Hold),
                241_000,
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_STRATEGY_CHANGED"))
        ));
        tx.commit().unwrap();
    }

    #[test]
    fn graph_binding_discovery_pages_from_original_finance_and_skips_stopped() {
        let mut db = database();
        let account = ("installation", "account-a");
        let foreign = ("installation", "account-b");
        assert_eq!(
            AccountStore::graph_bindings_page_sql(&db, account, 1, 0).unwrap(),
            EngineGraphBindingPage {
                scanned: 0,
                active: vec![]
            }
        );
        let bound = {
            let tx = db.transaction().unwrap();
            create(&tx, account.1);
            create(&tx, foreign.1);
            let bound = binding(&tx, account.1);
            let other = binding(&tx, foreign.1);
            AccountStore::bind_graph_sql(
                &tx,
                account,
                &bound,
                &cursor(2, TargetPosition::Hold),
                121_000,
            )
            .unwrap();
            AccountStore::bind_graph_sql(
                &tx,
                foreign,
                &other,
                &cursor(2, TargetPosition::Hold),
                121_000,
            )
            .unwrap();
            tx.commit().unwrap();
            bound
        };
        assert_eq!(
            AccountStore::graph_bindings_page_sql(&db, account, 1, 0).unwrap(),
            EngineGraphBindingPage {
                scanned: 1,
                active: vec![bound.clone()]
            }
        );
        assert_eq!(
            AccountStore::graph_bindings_page_sql(&db, account, 1, 1).unwrap(),
            EngineGraphBindingPage {
                scanned: 0,
                active: vec![]
            }
        );
        assert!(matches!(
            AccountStore::graph_bindings_page_sql(&db, account, 5, 0),
            Err(StoreError::Invalid("GRAPH_FINANCE_PAGE_INVALID"))
        ));
        let tx = db.transaction().unwrap();
        let mut stopped = AccountStore::instance_sql(&tx, Some(account), &bound.instance_id)
            .unwrap()
            .unwrap();
        stopped.enabled = false;
        stopped.policy_revision += 1;
        AccountStore::mutate_instance_sql(
            &tx,
            Some(account),
            "discovery-stop",
            "discovery-stop-hash",
            "stop",
            &stopped,
            Some(1),
            181_000,
        )
        .unwrap();
        tx.commit().unwrap();
        assert_eq!(
            AccountStore::graph_bindings_page_sql(&db, account, 1, 0).unwrap(),
            EngineGraphBindingPage {
                scanned: 1,
                active: vec![]
            },
            "stale receipts advance the scan but cannot resurrect a stopped graph"
        );
    }

    #[test]
    fn one_revision_can_bind_two_instruments_with_independent_cursors() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let (btc, eth) = {
            let tx = db.transaction().unwrap();
            create(&tx, scope.1);
            let btc = binding(&tx, scope.1);
            let eth = EngineGraphBinding {
                session_id: "research:strategy-a:eth".into(),
                graph_sha256: "b".repeat(64),
                instrument_id: "ETH-USDT-SWAP".into(),
                ..btc.clone()
            };
            AccountStore::bind_graph_sql(
                &tx,
                scope,
                &btc,
                &cursor(2, TargetPosition::Hold),
                121_000,
            )
            .unwrap();
            AccountStore::bind_graph_sql(
                &tx,
                scope,
                &eth,
                &cursor(2, TargetPosition::Flat),
                121_000,
            )
            .unwrap();
            tx.commit().unwrap();
            (btc, eth)
        };
        let page = AccountStore::graph_bindings_page_sql(&db, scope, 4, 0).unwrap();
        assert_eq!(page.scanned, 2);
        assert_eq!(page.active.len(), 2);
        assert!(page.active.contains(&btc));
        assert!(page.active.contains(&eth));
        {
            let tx = db.transaction().unwrap();
            assert_eq!(
                AccountStore::acknowledge_graph_idle_sql(
                    &tx,
                    scope,
                    &btc,
                    &cursor(3, TargetPosition::Hold),
                    181_000,
                )
                .unwrap(),
                GraphIdleAck::Advanced
            );
            tx.commit().unwrap();
        }
        assert_eq!(
            AccountStore::graph_progress_sql(&db, scope, &btc)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            3
        );
        assert_eq!(
            AccountStore::graph_progress_sql(&db, scope, &eth)
                .unwrap()
                .unwrap()
                .cursor
                .sequence,
            2
        );
        let tx = db.transaction().unwrap();
        let conflicting = EngineGraphBinding {
            session_id: "research:forged".into(),
            ..eth
        };
        assert!(matches!(
            AccountStore::bind_graph_sql(
                &tx,
                scope,
                &conflicting,
                &cursor(2, TargetPosition::Hold),
                241_000,
            ),
            Err(StoreError::Invalid("GRAPH_FINANCE_BINDING_CONFLICT"))
        ));
    }

    #[test]
    fn failed_transaction_never_persists_a_phantom_ack() {
        let mut db = database();
        let scope = ("installation", "account-a");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
        let b = binding(&tx, scope.1);
        AccountStore::bind_graph_sql(&tx, scope, &b, &cursor(2, TargetPosition::Flat), 121_000)
            .unwrap();
        tx.commit().unwrap();
        {
            let tx = db.transaction().unwrap();
            assert_eq!(
                AccountStore::acknowledge_graph_idle_sql(
                    &tx,
                    scope,
                    &b,
                    &cursor(3, TargetPosition::Flat),
                    181_000,
                )
                .unwrap(),
                GraphIdleAck::Advanced
            );
            // Simulate the crash between the SQL write and its commit.
        }
        let last = AccountStore::graph_progress_sql(&db, scope, &b)
            .unwrap()
            .unwrap();
        assert_eq!(last.cursor.sequence, 2);
    }
}
