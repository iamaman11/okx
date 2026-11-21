//! Durable graph identity and *idle-only* publication checkpoints.
//!
//! Reuses ORIGINAL immutable operation receipts in the one finance database:
//! no side database, schema drift, or unsupervised background writer.
use super::super::*;
use crate::graph_binding::{
    EngineGraphBinding, EngineGraphCursor, EngineGraphProgress, GraphIdleAck,
};
use crate::position_target::{TargetPlan, plan_strategy_target};
use sha2::{Digest, Sha256};

const BINDING_KIND: &str = "graph_strategy_binding_v1";
const CURSOR_KIND: &str = "graph_strategy_idle_cursor_v1";

fn binding_key(binding: &EngineGraphBinding) -> String {
    format!(
        "graph-binding:{:x}:{}",
        Sha256::digest(binding.instance_id.as_bytes()),
        binding.policy_revision
    )
}

fn cursor_prefix(binding: &EngineGraphBinding) -> String {
    format!(
        "graph-cursor:{:x}:{}:",
        Sha256::digest(binding.instance_id.as_bytes()),
        binding.policy_revision
    )
}

fn cursor_key(binding: &EngineGraphBinding, sequence: u64) -> String {
    format!("{}{:020}", cursor_prefix(binding), sequence)
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

    /// Read the only installed graph binding for the **current** strategy
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
    /// they never rewind it. One graph per strategy policy revision.
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

    /// ACK only when an *original finance snapshot inside this same writer
    /// transaction* shows no financial action required. Opening and closing
    /// must be bound to the original decision/intent claim in a later slice.
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

    fn binding(account: &str) -> EngineGraphBinding {
        EngineGraphBinding {
            account_key: account.into(),
            instance_id: "strategy-a".into(),
            definition_version: "1".into(),
            policy_revision: 1,
            session_id: "research:strategy-a:btc".into(),
            graph_sha256: "a".repeat(64),
            instrument_id: "BTC-USDT-SWAP".into(),
            timeframe_ms: 60_000,
        }
    }

    fn create(tx: &rusqlite::Transaction<'_>, account: &str) {
        let instance = StrategyInstance {
            instance_id: "strategy-a".into(),
            definition_id: "no_trade".into(),
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
        let b = binding("account-a");
        let scope = ("installation", "account-a");
        {
            let tx = db.transaction().unwrap();
            create(&tx, scope.1);
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
        }
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
        let b = binding("account-a");
        let foreign = binding("account-b");
        let scope = ("installation", "account-a");
        let other_scope = ("installation", "account-b");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
        create(&tx, other_scope.1);
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
        let b = binding("account-a");
        let scope = ("installation", "account-a");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
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
    fn failed_transaction_never_persists_a_phantom_ack() {
        let mut db = database();
        let b = binding("account-a");
        let scope = ("installation", "account-a");
        let tx = db.transaction().unwrap();
        create(&tx, scope.1);
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
