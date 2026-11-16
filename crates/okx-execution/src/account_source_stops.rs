//! Original source stop resolution on the existing ledger transaction.
use super::*;
impl AccountStore {
    pub(crate) fn resolve_source_stop_sql(
        db: &Connection,
        scope: Option<(&str, &str)>,
        id: &str,
        evidence: &str,
    ) -> StoreResult<()> {
        if evidence.is_empty() {
            return Err(StoreError::Invalid("STOP_RESOLUTION_EVIDENCE_REQUIRED"));
        }
        match scope {
            None => {
                db.execute(
                    "UPDATE risk_stops SET resolution=?2 WHERE stop_id=?1 AND resolution IS NULL",
                    params![id, evidence],
                )?;
            }
            Some((installation, account)) => {
                Self::verify_source_account_sql(db, installation, account)?;
                db.execute("UPDATE risk_stops SET resolution=?2 WHERE stop_id=?1 AND resolution IS NULL AND installation_id=?3 AND account_key=?4",params![id,evidence,installation,account])?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn risk_book_contract_source_stop_resolution_is_scoped_and_requires_evidence() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(include_str!("../schema/finance-unified-v1.sql"))
            .unwrap();
        for (installation, account) in [("I", "A"), ("I", "B"), ("OTHER", "A")] {
            db.execute(
                "INSERT INTO account_meta VALUES(1,?1,?2,2,1,'source','initialize','release')",
                params![account, installation],
            )
            .unwrap();
            db.execute("INSERT INTO risk_stops VALUES(?1,?2,'usd-conversion-unavailable','account','USD_CONVERSION_SOURCE_UNAVAILABLE',1,NULL)",params![installation,account]).unwrap();
        }
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(
            AccountStore::resolve_source_stop_sql(
                &tx,
                Some(("I", "A")),
                "usd-conversion-unavailable",
                ""
            )
            .is_err()
        );
        assert!(
            AccountStore::resolve_source_stop_sql(
                &tx,
                Some(("I", "missing")),
                "usd-conversion-unavailable",
                "source-digest"
            )
            .is_err()
        );
        AccountStore::resolve_source_stop_sql(
            &tx,
            Some(("I", "A")),
            "usd-conversion-unavailable",
            "source-digest",
        )
        .unwrap();
        AccountStore::resolve_source_stop_sql(
            &tx,
            Some(("I", "A")),
            "usd-conversion-unavailable",
            "different-evidence",
        )
        .unwrap();
        for (installation, account, expected) in [
            ("I", "A", Some("source-digest")),
            ("I", "B", None),
            ("OTHER", "A", None),
        ] {
            let actual: Option<String> = tx
                .query_row(
                    "SELECT resolution FROM risk_stops WHERE installation_id=?1 AND account_key=?2",
                    params![installation, account],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(actual.as_deref(), expected);
        }
        tx.rollback().unwrap();
        let resolved: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM risk_stops WHERE resolution IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(resolved, 0);
    }
}
