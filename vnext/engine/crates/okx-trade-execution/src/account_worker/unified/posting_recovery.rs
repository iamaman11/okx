//! Cold-start recovery uses the original joint financial command, never a send grant.
use super::{
    UnifiedFinanceHandle, UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction,
};
use crate::{AccountStore, StoreError, StoreResult};
use std::time::Instant;

impl UnifiedFinanceHandle {
    /// A confirmed original POST fence becomes UNKNOWN. Its budget remains
    /// charged until independent venue observations resolve the outcome.
    /// The return count is deliberately not a CommittedVenueSend capability.
    pub async fn recover_posting(&self, account: String, deadline: Instant) -> StoreResult<usize> {
        self.recover_original_posting(Some(account), deadline).await
    }

    /// The installed account roster comes from this writer's original SQL
    /// identities. No caller-supplied subset may silently hide an obligation.
    pub async fn recover_installation_posting(&self, deadline: Instant) -> StoreResult<usize> {
        self.recover_original_posting(None, deadline).await
    }

    async fn recover_original_posting(
        &self,
        account: Option<String>,
        deadline: Instant,
    ) -> StoreResult<usize> {
        let installation = self.installation.clone();
        self.submit_before(Some(deadline), move |db| {
            commit_original_posting_recovery(db, &installation, account)
        })
        .await
    }
}

/// The production transaction boundary is shared by the two typed owner APIs.
/// Clock and activation are checked on the sole writer after queue admission.
fn commit_original_posting_recovery(
    db: &mut rusqlite::Connection,
    installation: &str,
    account: Option<String>,
) -> StoreResult<usize> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    super::source_permission::require_source_account(&tx, installation, account.as_deref())?;
    let now = super::opening_ingress::owner_now_ms()?;
    let command = match account {
        Some(account) => {
            AccountStore::verify_source_account_sql(&tx, installation, &account)?;
            UnifiedFinanceWrite::RecoverPosting { account, now }
        }
        None => UnifiedFinanceWrite::RecoverInstallation { now },
    };
    let receipt = apply_original_unified_finance_transaction(&tx, installation, command)?;
    let UnifiedFinanceWriteReceipt::Recovered { changed } = receipt else {
        return Err(StoreError::Invalid("UNIFIED_RECOVERY_RECEIPT_UNEXPECTED"));
    };
    tx.commit()
        .map_err(|_| StoreError::Invalid("UNIFIED_FINANCE_OUTCOME_UNKNOWN_DO_NOT_REPLAY"))?;
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::{Connection, OptionalExtension, params};

    const INSTALLATION: &str = "posting-recovery-installation";
    const CAMPAIGN: &str = "posting-recovery-campaign";

    fn original_posting_fixture(db: &Connection) {
        db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
            .unwrap();
        db.pragma_update(None, "foreign_keys", "ON").unwrap();
        db.execute(
            "INSERT INTO risk_meta VALUES(1,?1,?2,1,7,3,1,'10','2','10','0','2','100','0','0','4','0')",
            params![CAMPAIGN, INSTALLATION],
        ).unwrap();
        // Explicitly synthetic original POSTING fixtures, not source
        // authentication, a live activation marker or a venue POST witness.
        for account in ["account-A", "account-B"] {
            let reservation = format!("reservation-{account}");
            let hash = "a".repeat(64);
            db.execute(
                "INSERT INTO account_meta VALUES(1,?1,?2,2,1,'test','init','test')",
                params![account, INSTALLATION],
            )
            .unwrap();
            db.execute(
                "INSERT INTO account_policy VALUES(?1,?2,1,1,0,?3,'10','0')",
                params![INSTALLATION, account, CAMPAIGN],
            )
            .unwrap();
            db.execute(
                "INSERT INTO decisions VALUES(?1,?2,'same-decision',NULL,1,?3,1,'test')",
                params![INSTALLATION, account, hash],
            )
            .unwrap();
            db.execute(
                "INSERT INTO intents VALUES(?1,?2,'same-intent','same-key',?3,'same-decision',
                 'POSTING',1,'same-client',20000,?3,1,1,'BTC-USDT-SWAP',1,10,10,NULL,NULL)",
                params![INSTALLATION, account, hash],
            )
            .unwrap();
            db.execute(
                "INSERT INTO risk_receipts VALUES(?1,'same-intent',?2,?3,NULL,?4,1,?4,'1',?4,'POSTED')",
                params![reservation, account, INSTALLATION, hash],
            ).unwrap();
            db.execute(
                "INSERT INTO risk_account_occupied VALUES(?1,?2,?3,'1')",
                params![INSTALLATION, CAMPAIGN, account],
            )
            .unwrap();
            db.execute(
                "INSERT INTO risk_policy_account VALUES(?1,?2,?3,'10','10','10')",
                params![INSTALLATION, CAMPAIGN, account],
            )
            .unwrap();
            db.execute(
                "INSERT INTO risk_loss_source VALUES(?1,?2,?3,'0',?4)",
                params![INSTALLATION, CAMPAIGN, account, hash],
            )
            .unwrap();
            db.execute(
                "INSERT INTO risk_policy_strategy VALUES(?1,?2,?3,'manual','10')",
                params![INSTALLATION, CAMPAIGN, account],
            )
            .unwrap();
            db.execute(
                "INSERT INTO risk_strategy_occupied VALUES(?1,?2,?3,'manual','1')",
                params![INSTALLATION, CAMPAIGN, account],
            )
            .unwrap();
            db.execute(
                "INSERT INTO risk_entries(
                 installation_id,campaign_id,reservation_key,account_key,strategy_id,intent_id,
                 plan_sha256,request_digest,policy_revision,state,loss_usdt,loss_usd,entry_upper_usd,
                 reserved_usdt,reserved_loss_usd,reserved_entry_usd,entry_proof_digest,source_digest,
                 book_sequence,held_at_ms,reserve_read_floor_ms,native_incarnation,
                 post_authorized_at_ms,post_sequence,post_proof_digest,post_source_digest)
                 VALUES(?1,?2,?3,?4,'manual','same-intent',?5,?5,1,'POSTED','1','1','2',
                        '1','1','2',?5,?5,1,5,4,?6,10,2,?5,?5)",
                params![INSTALLATION, CAMPAIGN, reservation, account, hash, "b".repeat(32)],
            ).unwrap();
        }
        let fk: Option<String> = db
            .query_row("PRAGMA foreign_key_check", [], |r| r.get(0))
            .optional()
            .unwrap();
        assert!(fk.is_none());
    }

    // The original joint command is intentionally called OFFLINE, with no
    // fabricated production grant. The public APIs must still reject it.
    fn recover_offline(db: &mut Connection, account: Option<&str>) -> StoreResult<usize> {
        let tx = db.transaction().unwrap();
        let command = match account {
            Some(account) => UnifiedFinanceWrite::RecoverPosting {
                account: account.into(),
                now: 20,
            },
            None => UnifiedFinanceWrite::RecoverInstallation { now: 20 },
        };
        let result = apply_original_unified_finance_transaction(&tx, INSTALLATION, command);
        // Commit even after Err: the original command must own its rollback.
        tx.commit().unwrap();
        match result? {
            UnifiedFinanceWriteReceipt::Recovered { changed } => Ok(changed),
            _ => panic!("unexpected recovery receipt"),
        }
    }

    fn states(db: &Connection) -> Vec<(String, String, String)> {
        db.prepare(
            "SELECT i.state,r.state,e.state FROM intents i
             JOIN risk_receipts r USING(installation_id,account_key,intent_id)
             JOIN risk_entries e USING(installation_id,account_key,intent_id)
             ORDER BY i.account_key",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
    }

    fn capacities(db: &Connection) -> (String, String, String) {
        db.query_row(
            "SELECT global_reserved_usdt,loss_reserved_usd,entry_reserved_usd FROM risk_meta",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap()
    }

    #[test]
    fn stopped_authority_rejects_production_recovery_before_any_mutation() {
        let mut db = Connection::open_in_memory().unwrap();
        original_posting_fixture(&db);
        db.execute(
            "INSERT INTO finance_import_manifest VALUES(1,?1,?2,?2,2,1,0,0,'OFFLINE_NOT_AUTHORIZED')",
            params![INSTALLATION, "c".repeat(64)],
        ).unwrap();
        let before = db.total_changes();
        for account in [Some("account-A".into()), None] {
            assert!(matches!(
                commit_original_posting_recovery(&mut db, INSTALLATION, account),
                Err(StoreError::Invalid("FINANCE_CUTOVER_NOT_ACCEPTED"))
            ));
        }
        assert_eq!(db.total_changes(), before);
        assert!(
            states(&db)
                .iter()
                .all(|s| s == &("POSTING".into(), "POSTED".into(), "POSTED".into()))
        );
    }

    #[test]
    fn original_joint_recovery_retains_budget_and_does_not_touch_other_account() {
        let mut db = Connection::open_in_memory().unwrap();
        original_posting_fixture(&db);
        let before = capacities(&db);
        assert_eq!(recover_offline(&mut db, Some("account-A")).unwrap(), 1);
        assert_eq!(
            states(&db),
            vec![
                ("UNKNOWN".into(), "UNKNOWN".into(), "UNKNOWN".into()),
                ("POSTING".into(), "POSTED".into(), "POSTED".into()),
            ]
        );
        assert_eq!(recover_offline(&mut db, Some("account-A")).unwrap(), 0);
        assert_eq!(recover_offline(&mut db, None).unwrap(), 1);
        assert_eq!(recover_offline(&mut db, None).unwrap(), 0);
        assert_eq!(capacities(&db), before);
        let sequence: i64 = db
            .query_row("SELECT book_sequence FROM risk_meta", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            sequence, 5,
            "recovery replay must not generate another transition"
        );
    }

    #[test]
    fn broken_second_account_rolls_back_all_original_joint_recovery() {
        let mut db = Connection::open_in_memory().unwrap();
        original_posting_fixture(&db);
        db.execute(
            "UPDATE risk_receipts SET plan_hash='foreign-plan' WHERE account_key='account-B'",
            [],
        )
        .unwrap();
        let before = states(&db);
        let reserved = capacities(&db);
        assert!(recover_offline(&mut db, None).is_err());
        assert_eq!(states(&db), before);
        assert_eq!(capacities(&db), reserved);
        let sequence: i64 = db
            .query_row("SELECT book_sequence FROM risk_meta", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            sequence, 3,
            "even an outer COMMIT after Err cannot preserve half recovery"
        );
    }

    #[test]
    fn original_joint_recovery_survives_sqlite_close_and_reopen_without_new_grant() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "okx-posting-recovery-{}-{nonce}.sqlite",
            std::process::id()
        ));
        // Reserve a new test file; never open or overwrite an existing path.
        drop(
            std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        let mut db = Connection::open(&path).unwrap();
        db.pragma_update(None, "journal_mode", "WAL").unwrap();
        db.pragma_update(None, "synchronous", "FULL").unwrap();
        original_posting_fixture(&db);
        let reserved = capacities(&db);
        assert_eq!(recover_offline(&mut db, None).unwrap(), 2);
        drop(db);

        let mut reopened = Connection::open(&path).unwrap();
        reopened.pragma_update(None, "foreign_keys", "ON").unwrap();
        assert!(
            states(&reopened)
                .iter()
                .all(|s| s == &("UNKNOWN".into(), "UNKNOWN".into(), "UNKNOWN".into()))
        );
        assert_eq!(capacities(&reopened), reserved);
        assert_eq!(recover_offline(&mut reopened, None).unwrap(), 0);
        let grant_count: i64 = reopened
            .query_row(
                "SELECT COUNT(*) FROM intents WHERE state='POSTING' OR begin_post_at!=10",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            grant_count, 0,
            "reopen cannot create a new durable POST fence"
        );
        assert_eq!(
            reopened
                .query_row::<String, _, _>("PRAGMA integrity_check", [], |r| r.get(0))
                .unwrap(),
            "ok"
        );
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }
}
