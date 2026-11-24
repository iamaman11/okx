//! OFFLINE original SQL contracts, never activation, runtime or 26/26 acceptance.
//! No send claim is manufactured here. Real owner activation remains mandatory.
use super::*;
use crate::account_worker::{
    CensusObservedAtMs, CensusReadFloorMs, NativeReserveDecision, NativeReserveRequest,
    ReportedRiskCensus, UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
    apply_original_unified_finance_transaction, test_bind_native_census, test_reserve_reported_sql,
};
use std::collections::BTreeMap;
const NOW: i64 = 10;
fn account() -> String {
    format!("okx:real:{}", "a".repeat(64))
}
const CAMPAIGN: &str = "campaign-20";
fn amount(v: &str) -> okx_engine_risk::risk::money::RiskAmount {
    okx_engine_risk::risk::money::RiskAmount::parse(v).unwrap()
}
fn census(at: i64) -> ReportedRiskCensus {
    ReportedRiskCensus {
        source_digest: "d".repeat(64),
        generated_at_ms: CensusObservedAtMs::new(at).unwrap(),
        read_floor_ms: CensusReadFloorMs::new(at - 1).unwrap(),
        occupied: BTreeMap::from([(account(), "0".into())]),
        cumulative_loss_usd: BTreeMap::from([(account(), "0".into())]),
    }
}
fn fixture() -> (Connection, NativeReserveRequest) {
    let mut source = store_with_account(&account());
    let stored = intent_with_plan_hash_and_id(&mut source, &"e".repeat(64), "intent_0001");
    let quote = e1_source_quote(&source, &stored);
    source.bind_entry_quote(&quote).unwrap();
    source
        .bind_entry_admission(&EntryAdmissionEvidence {
            schema: "okx.entry-admission/v2".into(),
            account_key: account(),
            installation_id: "installation".into(),
            intent_id: stored.intent_id,
            plan_hash: stored.plan_hash,
            quote_digest: format!(
                "{:x}",
                sha2::Sha256::digest(serde_json::to_vec(&quote).unwrap())
            ),
            admission_usd_per_usdt: Decimal::ONE,
            other_entry_costs_upper_usd: Decimal::ZERO,
            entry_upper_usd: quote.total_usd,
            entry_proof_digest: "f".repeat(64),
            observed_at: quote.observed_at,
        })
        .unwrap();
    let original = source.path.clone();
    drop(source);
    let original =
        Connection::open_with_flags(original, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut db = Connection::open(path()).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut db,
        "installation",
        &[&original],
    )
    .unwrap();
    // Same FK-backed offline policy fixtures used by original scoped RiskBook
    // contracts. These rows neither activate finance nor open a production gate.
    db.execute("INSERT INTO risk_meta VALUES(1,?1,'installation',1,7,0,1,'20','0','20','0','0','200','0','0','0','0')",[CAMPAIGN]).unwrap();
    db.execute(
        "INSERT INTO risk_account_occupied VALUES('installation',?1,?2,'0')",
        params![CAMPAIGN, account()],
    )
    .unwrap();
    db.execute(
        "INSERT INTO risk_policy_account VALUES('installation',?1,?2,'20','20','200')",
        params![CAMPAIGN, account()],
    )
    .unwrap();
    db.execute(
        "INSERT INTO risk_loss_source VALUES('installation',?1,?2,'0',?3)",
        params![CAMPAIGN, account(), "d".repeat(64)],
    )
    .unwrap();
    db.execute(
        "INSERT INTO risk_policy_strategy VALUES('installation',?1,?2,'manual','20')",
        params![CAMPAIGN, account()],
    )
    .unwrap();
    db.execute(
        "INSERT INTO risk_strategy_occupied VALUES('installation',?1,?2,'manual','0')",
        params![CAMPAIGN, account()],
    )
    .unwrap();
    db.execute("INSERT INTO nav_watermarks VALUES('installation',?1,'account','100','100',?2,'COMPLETE') ON CONFLICT(installation_id,account_key,scope_id) DO UPDATE SET time=excluded.time,coverage=excluded.coverage",params![account(),NOW]).unwrap();
    let request = NativeReserveRequest {
        venue: "okx".into(),
        uid: "a".repeat(64),
        run_id: crate::account_worker::AdmissionRunId::new("run_000001".into()).unwrap(),
        intent_id: crate::account_worker::AdmissionIntentId::new("intent_0001".into()).unwrap(),
        strategy_id: "manual".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_sha256: "e".repeat(64),
        entry_proof_digest: "f".repeat(64),
        conversion_digest: "b".repeat(64),
        conversion_rate: "1".into(),
        loss_usdt: crate::risk::UsdtLoss::parse("5").unwrap(),
        loss_usd: crate::risk::UsdLoss::parse("5").unwrap(),
        entry_upper_usd: crate::risk::UsdEntryUpper::parse("100.05").unwrap(),
        native_incarnation: "f".repeat(32),
        conversion_observed_at_ms: NOW - 1,
        policy_revision: 1,
    };
    (db, request)
}

#[test]
fn risk_book_contract_native_reported_reserve_rejects_understatement_before_any_write() {
    let (mut db, mut request) = fixture();
    request.loss_usdt = crate::risk::UsdtLoss::parse("1").unwrap();
    request.loss_usd = crate::risk::UsdLoss::parse("1").unwrap();
    let tx = db.transaction().unwrap();
    let changes = tx.total_changes();
    assert!(matches!(
        test_reserve_reported_sql(&tx, "installation", CAMPAIGN, request, census(NOW), NOW),
        Err(StoreError::Invalid("NATIVE_RESERVE_ORIGINAL_LOSS_MISMATCH"))
    ));
    assert_eq!(changes, tx.total_changes());
    tx.commit().unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))
            .unwrap(),
        0
    );
}

#[test]
fn risk_book_contract_native_reserve_post_unknown_reopen_no_second_authorization() {
    let (mut db, request) = fixture();
    // The NAV source cannot be newer than owner time. Publish the exact
    // reserve witness, then advance it for post-held observation.
    db.execute("UPDATE nav_watermarks SET time=?1", [NOW])
        .unwrap();
    let tx = db.transaction().unwrap();
    let NativeReserveDecision::Held {
        receipt,
        book_sequence,
        ..
    } = test_reserve_reported_sql(
        &tx,
        "installation",
        CAMPAIGN,
        request.clone(),
        census(NOW),
        NOW,
    )
    .unwrap()
    else {
        panic!("source-complete reserve denied");
    };
    let reservation = receipt.reservation_key.clone();
    assert_eq!(receipt.requested_loss_usd, Decimal::from(5));
    assert_eq!(
        AccountStore::find_intent_sql(
            &tx,
            Some(("installation", &account())),
            "intent_id",
            "intent_0001"
        )
        .unwrap()
        .unwrap()
        .state,
        IntentState::Reserved
    );
    let (risk, loss, entry): (String, String, String) = tx
        .query_row(
            "SELECT global_reserved_usdt,loss_reserved_usd,entry_reserved_usd FROM risk_meta",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (amount(&risk), amount(&loss), amount(&entry)),
        (amount("5"), amount("5"), amount("100.05"))
    );
    let NativeReserveDecision::Held {
        receipt: repeat, ..
    } = test_reserve_reported_sql(
        &tx,
        "installation",
        CAMPAIGN,
        request.clone(),
        census(NOW),
        NOW,
    )
    .unwrap()
    else {
        panic!("exact replay denied");
    };
    assert_eq!(receipt.receipt_hash, repeat.receipt_hash);
    tx.commit().unwrap();
    db.execute("UPDATE nav_watermarks SET time=?1", [NOW + 1])
        .unwrap();
    let tx = db.transaction().unwrap();
    let post = crate::risk::PostFenceInput {
        reservation_key: reservation.clone(),
        plan_sha256: request.plan_sha256.clone(),
        policy_revision: 1,
        expected_sequence: book_sequence,
        now_ms: NOW + 2,
        source_proof_digest: "b".repeat(64),
        census: test_bind_native_census(&tx, "installation", CAMPAIGN, &census(NOW + 1), NOW + 2)
            .unwrap(),
        causal: crate::risk::PostCausalProof {
            reserve_anchor: reservation.clone(),
            native_incarnation: "f".repeat(32),
            elapsed_to_proof_ms: 2,
            account_reads: vec![crate::risk::PostAccountRead {
                account_key: account(),
                private_generation: 7,
                source_read_id: "1".repeat(64),
                content_digest: "c".repeat(64),
                started_after_held_ms: 0,
                completed_after_held_ms: 1,
            }],
        },
    };
    let authorized = apply_original_unified_finance_transaction(
        &tx,
        "installation",
        UnifiedFinanceWrite::BeginPost {
            campaign: CAMPAIGN.into(),
            input: post.clone(),
        },
    )
    .unwrap();
    assert!(matches!(
        authorized,
        UnifiedFinanceWriteReceipt::Risk(crate::risk::UnifiedRiskDecision::PostAuthorized { .. })
    ));
    tx.commit().unwrap();
    // This is an offline committed SQL witness only. We do NOT construct
    // CommittedVenueSend; only the real gated owner can issue that capability.
    let file = db.path().unwrap().to_owned();
    drop(db);
    let mut db = Connection::open(file).unwrap();
    let tx = db.transaction().unwrap();
    assert!(
        apply_original_unified_finance_transaction(
            &tx,
            "installation",
            UnifiedFinanceWrite::BeginPost {
                campaign: CAMPAIGN.into(),
                input: post.clone()
            }
        )
        .is_err()
    );
    AccountStore::record_order_readback_sql(
        &tx,
        Some(("installation", &account())),
        "intent_0001",
        "venue-order",
        IntentState::Unknown,
        Decimal::ZERO,
        NOW + 3,
    )
    .unwrap();
    crate::risk::reconcile_outcome_unified_native(&tx, "installation", &account(), "intent_0001")
        .unwrap();
    tx.commit().unwrap();
    let file = db.path().unwrap().to_owned();
    drop(db);
    let mut db = Connection::open(file).unwrap();
    let tx = db.transaction().unwrap();
    assert!(
        apply_original_unified_finance_transaction(
            &tx,
            "installation",
            UnifiedFinanceWrite::BeginPost {
                campaign: CAMPAIGN.into(),
                input: post
            }
        )
        .is_err()
    );
    assert_eq!(
        AccountStore::find_intent_sql(
            &tx,
            Some(("installation", &account())),
            "intent_id",
            "intent_0001"
        )
        .unwrap()
        .unwrap()
        .state,
        IntentState::Unknown
    );
    assert_eq!(
        tx.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))
            .unwrap(),
        1
    );
    // Account-only fixture import creates no full migration manifest and
    // therefore cannot meet the real owner's activation preconditions.
    assert_eq!(
        tx.query_row::<i64, _, _>("SELECT COUNT(*) FROM finance_import_manifest", [], |r| r
            .get(0))
            .unwrap(),
        0
    );
    AccountStore::record_order_readback_sql(
        &tx,
        Some(("installation", &account())),
        "intent_0001",
        "venue-order",
        IntentState::Acknowledged,
        Decimal::ZERO,
        NOW + 4,
    )
    .unwrap();
    crate::risk::reconcile_outcome_unified_native(&tx, "installation", &account(), "intent_0001")
        .unwrap();
    let state: String = tx
        .query_row(
            "SELECT state FROM risk_entries WHERE reservation_key=?1",
            [reservation],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(state, "OPEN");
    tx.commit().unwrap();
}
