//! Original durable external-operation markers, shared by both SQL namespaces.
use super::super::*;
impl AccountStore {
    pub(crate) fn recent_cleanup_diagnostics_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        limit: usize,
    ) -> StoreResult<Vec<ProtectionCleanupOperationReceipt>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = if scope.is_some() {
            "WITH operation_receipts AS (SELECT * FROM main.operation_receipts WHERE installation_id=?2 AND account_key=?3), intents AS (SELECT * FROM main.intents WHERE installation_id=?2 AND account_key=?3) "
        } else {
            ""
        };
        let mut args = vec![rusqlite::types::Value::from(limit.min(128) as i64)];
        if let Some((installation, account)) = scope {
            Self::verify_source_account_sql(db, installation, account)?;
            args.push(installation.to_owned().into());
            args.push(account.to_owned().into());
        }
        let mut statement=db.prepare(&format!("{prefix}SELECT o.operation_id,i.intent_id,
                    CASE WHEN substr(o.operation_id,1,8)='cleanup_' THEN substr(o.operation_id,9) END,
                    o.kind,o.request_hash,o.result_ref,o.committed_at
             FROM operation_receipts AS o LEFT JOIN intents AS i ON i.client_order_id=
               CASE WHEN substr(o.operation_id,1,8)='cleanup_' THEN substr(o.operation_id,9) END
             WHERE o.kind='protection_cleanup'
             ORDER BY o.committed_at DESC,o.operation_id DESC LIMIT ?1"))?;
        Ok(statement
            .query_map(rusqlite::params_from_iter(args), |row| {
                Ok(ProtectionCleanupOperationReceipt {
                    operation_id: row.get(0)?,
                    intent_id: row.get(1)?,
                    client_order_id: row.get(2)?,
                    kind: row.get(3)?,
                    request_hash: row.get(4)?,
                    result_ref: row.get(5)?,
                    committed_at: row.get(6)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }
    pub(crate) fn begin_operation_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        hash: &str,
        kind: &str,
        now: i64,
    ) -> StoreResult<bool> {
        if let Some(old) = Self::operation_sql(db, scope, id)? {
            if old.request_hash != hash || old.kind != kind {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            return Ok(false);
        }
        match scope {
            None => {
                db.execute(
                    "INSERT INTO operation_receipts VALUES(?1,?2,?3,'POSTING',?4)",
                    params![id, kind, hash, now],
                )?;
            }
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,?4,?5,'POSTING',?6)",params![installation,account,id,kind,hash,now])?;
            }
        }
        Ok(true)
    }

    pub(crate) fn operation_receipt_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        hash: &str,
        kind: &str,
        result: &str,
        now: i64,
    ) -> StoreResult<OperationReceipt> {
        if let Some(old) = Self::operation_sql(db, scope, id)? {
            if old.request_hash != hash || old.kind != kind {
                return Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"));
            }
            if old.result_ref != "POSTING" && old.result_ref != result {
                return Err(StoreError::Invalid("OPERATION_RESULT_CONFLICT"));
            }
        }
        match scope {
            None => {
                db.execute("INSERT INTO operation_receipts VALUES(?1,?2,?3,?4,?5) ON CONFLICT(operation_id) DO UPDATE SET result_ref=excluded.result_ref,committed_at=excluded.committed_at",params![id,kind,hash,result,now])?;
            }
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.execute("INSERT INTO operation_receipts(installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(installation_id,account_key,operation_id) DO UPDATE SET result_ref=excluded.result_ref,committed_at=excluded.committed_at",params![installation,account,id,kind,hash,result,now])?;
            }
        }
        Self::operation_sql(db, scope, id)?.ok_or(StoreError::Invalid("OPERATION_MISSING"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn risk_book_contract_cleanup_marker_scope_once_conflict_and_reopen() {
        let path = std::env::temp_dir().join(format!(
            "okx-cleanup-scoped-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut db = Connection::open(&path).unwrap();
        db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        db.pragma_update(None, "foreign_keys", "ON").unwrap();
        for (installation, account) in [("I", "A"), ("I", "B"), ("OTHER", "A")] {
            db.execute(
                "INSERT INTO account_meta VALUES(1,?1,?2,2,1,'source','initialize','release')",
                params![account, installation],
            )
            .unwrap();
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            assert!(
                AccountStore::begin_operation_sql(
                    &tx,
                    Some((installation, account)),
                    "cleanup_client",
                    "hash",
                    "protection_cleanup",
                    2
                )
                .unwrap()
            );
            assert!(
                !AccountStore::begin_operation_sql(
                    &tx,
                    Some((installation, account)),
                    "cleanup_client",
                    "hash",
                    "protection_cleanup",
                    3
                )
                .unwrap()
            );
            assert!(
                AccountStore::begin_operation_sql(
                    &tx,
                    Some((installation, account)),
                    "cleanup_client",
                    "other",
                    "protection_cleanup",
                    3
                )
                .is_err()
            );
            tx.commit().unwrap();
        }
        drop(db);
        let mut db = Connection::open(&path).unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(
            !AccountStore::begin_operation_sql(
                &tx,
                Some(("I", "A")),
                "cleanup_client",
                "hash",
                "protection_cleanup",
                4
            )
            .unwrap()
        );
        AccountStore::operation_receipt_sql(
            &tx,
            Some(("I", "A")),
            "cleanup_client",
            "hash",
            "protection_cleanup",
            "VERIFIED_CANCELED",
            4,
        )
        .unwrap();
        assert!(
            AccountStore::operation_receipt_sql(
                &tx,
                Some(("I", "A")),
                "cleanup_client",
                "hash",
                "protection_cleanup",
                "other-result",
                5
            )
            .is_err()
        );
        for (installation, account, expected) in [
            ("I", "A", "VERIFIED_CANCELED"),
            ("I", "B", "POSTING"),
            ("OTHER", "A", "POSTING"),
        ] {
            assert_eq!(
                AccountStore::operation_sql(&tx, Some((installation, account)), "cleanup_client")
                    .unwrap()
                    .unwrap()
                    .result_ref,
                expected
            );
        }
        tx.rollback().unwrap();
        assert_eq!(
            AccountStore::operation_sql(&db, Some(("I", "A")), "cleanup_client")
                .unwrap()
                .unwrap()
                .result_ref,
            "POSTING"
        );
        drop(db);
        std::fs::remove_file(path).unwrap();
    }
}
