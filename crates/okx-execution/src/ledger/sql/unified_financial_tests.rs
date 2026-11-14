//! Direct tests of the future one-file transaction: not a production cutover.
use super::*;
use rusqlite::{Connection, TransactionBehavior, params};
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    path: PathBuf,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "okx-finance-r4-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("finance.sqlite");
        Self { root, path }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn connection(path: &Path) -> Connection {
    let db = Connection::open(path).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    db.busy_timeout(std::time::Duration::ZERO).unwrap();
    db
}
fn fixture_original_receipt(
    account: &str,
    intent: &str,
    reservation: &str,
    order: &str,
    source_digest: &str,
    value: &str,
    revision: u64,
) -> String {
    // Structurally valid original *format* from isolated test inputs, NOT an
    // authenticated venue producer. Real-source migration is separately
    // proven by the native AccountStore producer test.
    let usd: rust_decimal::Decimal = value.parse().unwrap();
    serde_json::to_string(&crate::EntrySettlementEvidence {
        schema: "okx.entry-partial-settlement/v1".into(),
        account_key: account.into(),
        intent_id: intent.into(),
        plan_hash: "e".repeat(64),
        reservation_key: reservation.into(),
        venue_order_id: order.into(),
        venue_update_ms: 7,
        source_covered_through_ms: 7,
        venue_filled_quantity: rust_decimal::Decimal::ONE,
        booked_notional_and_fee_usdt: usd,
        cumulative_entry_usd: usd,
        settlement_revision: revision,
        source_digest: source_digest.into(),
    })
    .unwrap()
}
fn seed(db: &Connection) {
    db.execute_batch(CORE_SCHEMA).unwrap();
    db.execute(
        "INSERT INTO finance_installations VALUES('install-one',1)",
        [],
    )
    .unwrap();
    for account in ["acct-A", "acct-B"] {
        db.execute(
            "INSERT INTO finance_accounts VALUES('install-one',?1)",
            [account],
        )
        .unwrap();
        db.execute(
            "INSERT INTO finance_intents VALUES('install-one',?1,'intent-1',?2,'order-same')",
            params![account, "e".repeat(64)],
        )
        .unwrap();
        db.execute(
            "INSERT INTO finance_fills VALUES(
                'install-one',?1,'fill-same','intent-1','order-same',
                'trade-same','BTC-USDT-SWAP')",
            [account],
        )
        .unwrap();
        let source_digest = if account == "acct-A" {
            "a".repeat(64)
        } else {
            "b".repeat(64)
        };
        let amount = if account == "acct-A" { "6" } else { "4" };
        db.execute(
            "INSERT INTO finance_bills VALUES(
                'install-one',?1,'bill-same','fill-same',?2)",
            params![account, &source_digest],
        )
        .unwrap();
        let original = fixture_original_receipt(
            account,
            "intent-1",
            &format!("reserve-{account}"),
            "order-same",
            &source_digest,
            amount,
            1,
        );
        db.execute(
            "INSERT INTO finance_entry_sources(
                 installation_id,account_key,intent_id,settlement_revision,
                 fill_id,bill_id,source_digest,cumulative_entry_usd,original_receipt_json) VALUES(
                'install-one',?1,'intent-1',1,'fill-same','bill-same',?2,?3,?4)",
            params![account, &source_digest, amount, &original],
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO finance_campaigns VALUES(
          'install-one','campaign-X',1,7,0,'30','0','0','20','0')",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT INTO finance_campaigns VALUES(
          'install-one','campaign-Y',1,7,0,'30','0','0','3','0')",
        [],
    )
    .unwrap();
    for account in ["acct-A", "acct-B"] {
        db.execute(
            "INSERT INTO finance_campaign_accounts VALUES('install-one','campaign-X',?1)",
            [account],
        )
        .unwrap();
        db.execute(
            "INSERT INTO finance_risk_entries
             (installation_id,campaign_id,reservation_key,account_key,intent_id,
              plan_sha256,state,entry_upper_usd,entry_settled_usd,reserved_entry_usd)
             VALUES('install-one','campaign-X',?1,?2,'intent-1',?3,'POSTED','10','0','10')",
            params![format!("reserve-{account}"), account, "e".repeat(64)],
        )
        .unwrap();
    }
    db.execute(
        "INSERT INTO finance_campaign_accounts VALUES('install-one','campaign-Y','acct-A')",
        [],
    )
    .unwrap();
    db.execute(
        "INSERT INTO finance_intents VALUES('install-one','acct-A','intent-Y',?1,'order-y')",
        ["e".repeat(64)],
    )
    .unwrap();
    db.execute(
        "INSERT INTO finance_risk_entries
         (installation_id,campaign_id,reservation_key,account_key,intent_id,
          plan_sha256,state,entry_upper_usd,entry_settled_usd,reserved_entry_usd)
         VALUES('install-one','campaign-Y','reserve-Y','acct-A','intent-Y',?1,'POSTED','3','0','3')",
        ["e".repeat(64)],
    ).unwrap();
}
fn entry_a() -> EntryRef<'static> {
    EntryRef {
        installation: "install-one",
        campaign: "campaign-X",
        reservation: "reserve-acct-A",
        account: "acct-A",
        intent: "intent-1",
        source_revision: 1,
        expected_sequence: 0,
        generation: 7,
    }
}
fn entry_b() -> EntryRef<'static> {
    EntryRef {
        installation: "install-one",
        campaign: "campaign-X",
        reservation: "reserve-acct-B",
        account: "acct-B",
        intent: "intent-1",
        source_revision: 1,
        expected_sequence: 1,
        generation: 7,
    }
}
fn campaign(db: &Connection, name: &str) -> (i64, String, String, String) {
    db.query_row(
        "SELECT book_sequence,entry_consumed_usd,entry_reserved_usd,entry_breach_usd
         FROM finance_campaigns WHERE installation_id='install-one' AND campaign_id=?1",
        [name],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )
    .unwrap()
}
fn receipt_count(db: &Connection) -> i64 {
    db.query_row("SELECT count(*) FROM finance_entry_receipts", [], |r| {
        r.get(0)
    })
    .unwrap()
}

#[test]
fn risk_book_contract_single_finance_sqlite_scopes_identical_venue_ids() {
    let fixture = Fixture::new("scoped-ids");
    let mut db = connection(&fixture.path);
    seed(&db);
    for table in [
        "finance_intents",
        "finance_fills",
        "finance_bills",
        "finance_entry_sources",
    ] {
        let n: i64 = db
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE account_key IN('acct-A','acct-B')"),
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            n >= 2,
            "{table}: original identical account-local IDs must coexist"
        );
    }
    // Foreign account provenance cannot be used even if fill and bill IDs
    // are byte-for-byte the same on both accounts.
    let bad = EntryRef {
        account: "acct-B",
        ..entry_a()
    };
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        settle_entry(&tx, &bad),
        Err(StoreError::Invalid("UNIFIED_ACCOUNT_OR_INTENT_MISMATCH"))
    ));
    tx.rollback().unwrap();
    assert_eq!(
        campaign(&db, "campaign-X"),
        (0, "0".into(), "20".into(), "0".into())
    );

    // Same single physical SQLite: account A and B consume one shared budget,
    // while another campaign in that database remains unchanged.
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let first = settle_entry(&tx, &entry_a()).unwrap();
    assert_eq!(first.cumulative_entry_usd, "6");
    assert_eq!(first.book_sequence, 1);
    tx.commit().unwrap();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let second = settle_entry(&tx, &entry_b()).unwrap();
    assert_eq!(second.cumulative_entry_usd, "4");
    assert_eq!(second.book_sequence, 2);
    tx.commit().unwrap();
    assert_eq!(
        campaign(&db, "campaign-X"),
        (2, "10".into(), "10".into(), "0".into())
    );
    assert_eq!(
        campaign(&db, "campaign-Y"),
        (0, "0".into(), "3".into(), "0".into())
    );
    assert_eq!(receipt_count(&db), 2);
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let replay = settle_entry(&tx, &entry_a()).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.book_sequence, 2);
    tx.commit().unwrap();
    assert_eq!(receipt_count(&db), 2);

    // Same venue bill ID on B is not a permitted A-source when it has no
    // matching A fill. This is enforced by composite SQL FOREIGN KEY, not UI.
    let original = fixture_original_receipt(
        "acct-A",
        "intent-1",
        "reserve-acct-A",
        "order-same",
        &"d".repeat(64),
        "7",
        2,
    );
    let foreign = db.execute(
        "INSERT INTO finance_entry_sources(
                 installation_id,account_key,intent_id,settlement_revision,
                 fill_id,bill_id,source_digest,cumulative_entry_usd,original_receipt_json) VALUES(
            'install-one','acct-A','intent-1',2,'fill-b-only','bill-same',?1,'7',?2)",
        params!["d".repeat(64), original],
    );
    assert!(
        matches!(foreign, Err(rusqlite::Error::SqliteFailure(_, _))),
        "invalid account/intent/source FK must fail, not the evidence requirement"
    );
}

#[test]
fn risk_book_contract_single_finance_sqlite_atomically_rolls_back_both_balances_and_receipt() {
    let fixture = Fixture::new("rollback");
    let mut db = connection(&fixture.path);
    seed(&db);
    {
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        settle_entry(&tx, &entry_a()).unwrap();
        let internal: i64 = tx
            .query_row("SELECT count(*) FROM finance_entry_receipts", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(internal, 1);
        // Failure before COMMIT: even the original source-linked receipt and
        // risk projection are one SQLite transaction and must both roll back.
        tx.rollback().unwrap();
    }
    assert_eq!(receipt_count(&db), 0);
    assert_eq!(
        campaign(&db, "campaign-X"),
        (0, "0".into(), "20".into(), "0".into())
    );
    let unsettled: String = db
        .query_row(
            "SELECT entry_settled_usd FROM finance_risk_entries
         WHERE reservation_key='reserve-acct-A'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(unsettled, "0");

    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    settle_entry(&tx, &entry_a()).unwrap();
    tx.commit().unwrap();
    let after = campaign(&db, "campaign-X");
    assert_eq!(after, (1, "6".into(), "14".into(), "0".into()));
    assert_eq!(receipt_count(&db), 1);
    // The source is original immutable SQL, not a caller-supplied amount.
    assert!(
        db.execute(
            "UPDATE finance_entry_sources SET cumulative_entry_usd='600'
         WHERE account_key='acct-A'",
            [],
        )
        .is_err()
    );
    assert!(
        db.execute("DELETE FROM finance_bills WHERE account_key='acct-A'", [])
            .is_err()
    );
    assert_eq!(campaign(&db, "campaign-X"), after);
}

#[test]
fn risk_book_contract_single_finance_sqlite_rejects_stale_revision_corrupt_cache_and_busy_writer() {
    let fixture = Fixture::new("fail-closed");
    let mut db = connection(&fixture.path);
    seed(&db);
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let mut competing = connection(&fixture.path);
    assert!(
        competing
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .is_err()
    );
    tx.rollback().unwrap();
    assert!(
        competing
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .is_ok()
    );
    drop(competing);

    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    settle_entry(&tx, &entry_a()).unwrap();
    tx.commit().unwrap();
    let read = campaign(&db, "campaign-X");
    assert!(matches!(
        {
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            settle_entry(
                &tx,
                &EntryRef {
                    expected_sequence: 5,
                    ..entry_a()
                },
            )
        },
        Err(StoreError::Invalid("UNIFIED_RECEIPT_REPLAY_CONFLICT"))
    ));
    let second_digest = "d".repeat(64);
    db.execute(
        "INSERT INTO finance_bills VALUES(
             'install-one','acct-A','bill-next','fill-same',?1)",
        [&second_digest],
    )
    .unwrap();
    let later = fixture_original_receipt(
        "acct-A",
        "intent-1",
        "reserve-acct-A",
        "order-same",
        &second_digest,
        "5",
        2,
    );
    db.execute(
        "INSERT INTO finance_entry_sources(
                 installation_id,account_key,intent_id,settlement_revision,
                 fill_id,bill_id,source_digest,cumulative_entry_usd,original_receipt_json) VALUES(
         'install-one','acct-A','intent-1',2,'fill-same','bill-next',?1,'5',?2)",
        params![second_digest, later],
    )
    .unwrap();
    {
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert!(matches!(
            settle_entry(
                &tx,
                &EntryRef {
                    source_revision: 2,
                    expected_sequence: 1,
                    ..entry_a()
                }
            ),
            Err(StoreError::Invalid("UNIFIED_SETTLEMENT_REGRESSION"))
        ));
        tx.rollback().unwrap();
    }
    assert_eq!(campaign(&db, "campaign-X"), read);
    // Deliberate disk corruption must not create apparent new budget.
    db.execute(
        "UPDATE finance_campaigns SET entry_consumed_usd='0'
         WHERE campaign_id='campaign-X'",
        [],
    )
    .unwrap();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        settle_entry(
            &tx,
            &EntryRef {
                source_revision: 2,
                expected_sequence: 1,
                ..entry_a()
            }
        ),
        Err(StoreError::Invalid("UNIFIED_CAMPAIGN_AGGREGATES_CORRUPT"))
    ));
}

#[test]
fn risk_book_contract_child_unified_sqlite_commit_boundary() {
    let Ok(path) = std::env::var("OKX_510_UNIFIED_CHILD_DB") else {
        return;
    };
    let mode = std::env::var("OKX_510_UNIFIED_CHILD_POINT").unwrap();
    let mut db = connection(Path::new(&path));
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    settle_entry(&tx, &entry_a()).unwrap();
    if mode == "after" {
        tx.commit().unwrap();
    } else {
        assert_eq!(mode, "before");
        // Keep a real uncommitted SQLite transaction and WAL live.
        std::mem::forget(tx);
    }
    use std::io::Write;
    println!("UNIFIED_COMMIT_BOUNDARY_{mode}");
    std::io::stdout().flush().unwrap();
    std::thread::park();
    unreachable!("parent must OS-kill this SQLite writer at the witness");
}

fn kill_child_at(path: &Path, phase: &str) {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "risk_book_contract_child_unified_sqlite_commit_boundary",
            "--nocapture",
        ])
        .env("OKX_510_UNIFIED_CHILD_DB", path)
        .env("OKX_510_UNIFIED_CHILD_POINT", phase)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let marker = format!("UNIFIED_COMMIT_BOUNDARY_{phase}");
    let (tx, rx) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if line.unwrap_or_default().contains(&marker) {
                let _ = tx.send(());
                return;
            }
        }
    });
    let ready = rx.recv_timeout(Duration::from_secs(15));
    child.kill().unwrap();
    assert!(
        !child.wait().unwrap().success(),
        "child must really be killed by OS"
    );
    reader.join().unwrap();
    assert!(
        ready.is_ok(),
        "SQLite process did not reach the requested COMMIT barrier"
    );
}

#[test]
fn risk_book_contract_single_finance_sqlite_real_os_kill_before_and_after_one_commit() {
    let fixture = Fixture::new("kill-one-file");
    {
        let db = connection(&fixture.path);
        seed(&db);
    }
    kill_child_at(&fixture.path, "before");
    {
        let db = connection(&fixture.path);
        assert_eq!(
            campaign(&db, "campaign-X"),
            (0, "0".into(), "20".into(), "0".into())
        );
        assert_eq!(receipt_count(&db), 0);
    }
    kill_child_at(&fixture.path, "after");
    let mut reopened = connection(&fixture.path);
    assert_eq!(
        campaign(&reopened, "campaign-X"),
        (1, "6".into(), "14".into(), "0".into())
    );
    assert_eq!(receipt_count(&reopened), 1);
    let tx = reopened
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let replay = settle_entry(&tx, &entry_a()).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.book_sequence, 1);
    tx.commit().unwrap();
    assert_eq!(receipt_count(&reopened), 1);
}

#[test]
fn risk_book_contract_single_finance_sqlite_rejects_forged_primary_receipt_projection() {
    let fixture = Fixture::new("immutable-primary");
    let mut db = connection(&fixture.path);
    seed(&db);
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    settle_entry(&tx, &entry_a()).unwrap();
    tx.commit().unwrap();
    assert_eq!(
        campaign(&db, "campaign-X"),
        (1, "6".into(), "14".into(), "0".into())
    );

    // The original SQL receipt remains immutable, but an unauthorized
    // modification of a cached risk entry must not mint future capacity.
    db.execute(
        "UPDATE finance_risk_entries SET source_digest=?1
         WHERE reservation_key='reserve-acct-A'",
        ["d".repeat(64)],
    )
    .unwrap();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        settle_entry(
            &tx,
            &EntryRef {
                source_revision: 1,
                ..entry_a()
            }
        ),
        Err(StoreError::Invalid("UNIFIED_PRIMARY_RECEIPT_CORRUPT"))
    ));
    tx.rollback().unwrap();

    // The single-file STRICT schema forbids removing an obligation just by
    // changing the label to RELEASED while reserved money remains.
    assert!(
        db.execute(
            "UPDATE finance_risk_entries SET state='RELEASED'
         WHERE reservation_key='reserve-acct-B'",
            [],
        )
        .is_err()
    );
}

#[test]
fn risk_book_contract_unified_receipt_rejects_wrong_account_source_and_digest() {
    let fixture = Fixture::new("receipt-fk-isolation");
    let mut db = connection(&fixture.path);
    seed(&db);
    // Before the fix, these two independent FKs could both be satisfied:
    // a reservation owned by account A, but a source owned by account B.
    // The true one-to-one FK must include reservation, account AND intent.
    let wrong_account = db.execute(
        "INSERT INTO finance_entry_receipts(
           installation_id,campaign_id,reservation_key,account_key,intent_id,
           settlement_revision,source_digest,cumulative_entry_usd,book_sequence)
         VALUES('install-one','campaign-X','reserve-acct-A',
                'acct-B','intent-1',1,?1,'4',1)",
        ["b".repeat(64)],
    );
    assert!(
        matches!(
            wrong_account,
            Err(rusqlite::Error::SqliteFailure(ref code, _))
                if code.code == rusqlite::ErrorCode::ConstraintViolation
        ),
        "receipt may NEVER borrow another account's original source"
    );

    let wrong_digest = db.execute(
        "INSERT INTO finance_entry_receipts(
           installation_id,campaign_id,reservation_key,account_key,intent_id,
           settlement_revision,source_digest,cumulative_entry_usd,book_sequence)
         VALUES('install-one','campaign-X','reserve-acct-A',
                'acct-A','intent-1',1,?1,'6',1)",
        ["b".repeat(64)],
    );
    assert!(
        matches!(
            wrong_digest,
            Err(rusqlite::Error::SqliteFailure(ref code, _))
                if code.code == rusqlite::ErrorCode::ConstraintViolation
        ),
        "receipt digest must identify the SAME immutable source revision"
    );
    assert_eq!(receipt_count(&db), 0);
    assert_eq!(
        campaign(&db, "campaign-X"),
        (0, "0".into(), "20".into(), "0".into())
    );
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert_eq!(
        settle_entry(&tx, &entry_a()).unwrap().cumulative_entry_usd,
        "6"
    );
    tx.commit().unwrap();
    assert_eq!(receipt_count(&db), 1);
}

#[test]
fn risk_book_contract_unified_rejects_missing_or_fake_native_receipt() {
    let fixture = Fixture::new("native-evidence-mandatory");
    let mut db = connection(&fixture.path);
    seed(&db);
    // No source row can be created with a NULL/empty missing producer
    // receipt and then used to increase consumed risk.
    let missing = db.execute(
        "INSERT INTO finance_entry_sources(
           installation_id,account_key,intent_id,settlement_revision,
           fill_id,bill_id,source_digest,cumulative_entry_usd)
         VALUES('install-one','acct-A','intent-1',2,
                'fill-same','bill-same',?1,'7')",
        ["c".repeat(64)],
    );
    assert!(matches!(
        missing,
        Err(rusqlite::Error::SqliteFailure(ref code, _))
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    ));

    // A nonempty but non-EntrySettlementEvidence JSON value is not a
    // substitute for the original account financial producer.
    let digest = "c".repeat(64);
    db.execute(
        "INSERT INTO finance_bills VALUES(
           'install-one','acct-A','bill-invalid','fill-same',?1)",
        [&digest],
    )
    .unwrap();
    db.execute(
        "INSERT INTO finance_entry_sources(
           installation_id,account_key,intent_id,settlement_revision,
           fill_id,bill_id,source_digest,cumulative_entry_usd,original_receipt_json)
         VALUES('install-one','acct-A','intent-1',2,
                'fill-same','bill-invalid',?1,'7',?2)",
        params![digest, r#""not-an-original-receipt""#],
    )
    .unwrap();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        settle_entry(
            &tx,
            &EntryRef {
                source_revision: 2,
                ..entry_a()
            }
        ),
        Err(StoreError::Invalid("UNIFIED_ORIGINAL_JSON_CORRUPT"))
    ));
    tx.rollback().unwrap();
    assert_eq!(receipt_count(&db), 0);
    assert_eq!(
        campaign(&db, "campaign-X"),
        (0, "0".into(), "20".into(), "0".into())
    );
}

#[test]
fn risk_book_contract_unified_one_original_intent_one_campaign_and_original_plan() {
    let fixture = Fixture::new("single-intent-owner");
    let db = connection(&fixture.path);
    seed(&db);
    // Both campaigns include account A; an existing source intent from
    // campaign X cannot be booked against campaign Y as a second risk entry.
    let duplicate = db.execute(
        "INSERT INTO finance_risk_entries(
          installation_id,campaign_id,reservation_key,account_key,intent_id,
          plan_sha256,state,entry_upper_usd,reserved_entry_usd)
         VALUES('install-one','campaign-Y','reserve-duplicate',
                'acct-A','intent-1',?1,'POSTED','1','1')",
        ["e".repeat(64)],
    );
    assert!(matches!(
        duplicate,
        Err(rusqlite::Error::SqliteFailure(ref code, _))
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    ), "one original account+intent must have one budget owner across all campaigns");

    // A different account+intent can be allocated independently, but the
    // plan hash MUST match the original immutable native intent.
    let wrong_plan = db.execute(
        "INSERT INTO finance_risk_entries(
          installation_id,campaign_id,reservation_key,account_key,intent_id,
          plan_sha256,state,entry_upper_usd,reserved_entry_usd)
         VALUES('install-one','campaign-X','reserve-bad-plan',
                'acct-A','intent-Y',?1,'POSTED','1','1')",
        ["d".repeat(64)],
    );
    assert!(matches!(
        wrong_plan,
        Err(rusqlite::Error::SqliteFailure(ref code, _))
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    ), "the native source plan and the budget reservation must have identical identity");
    assert_eq!(receipt_count(&db), 0);
    assert_eq!(campaign(&db, "campaign-X"), (0, "0".into(), "20".into(), "0".into()));
    assert_eq!(campaign(&db, "campaign-Y"), (0, "0".into(), "3".into(), "0".into()));
}
