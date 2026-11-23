//! Original protective rules after importing two accounts with colliding venue IDs.
//! These offline storage contracts never create a live authority or POST grant.
use super::*;

const A: &str = "protection-A";
const B: &str = "protection-B";

fn imported(release_a: bool) -> Connection {
    let mut a = store_with_account(A);
    let mut b = store_with_account(B);
    for store in [&mut a, &mut b] {
        intent(store);
        reserve(store);
        store
            .protection_incident("intent", "PROTECTION_PENDING_OR_FAILED", 4)
            .unwrap();
    }
    if release_a {
        // The original acknowledgement writes both RELEASED and the bound
        // CENTRAL_RELEASE operation before import. No synthetic release flag.
        a.acknowledge_central_release("central-key", "original-release-evidence")
            .unwrap();
    }
    let ap = a.path.clone();
    let bp = b.path.clone();
    drop(a);
    drop(b);
    let sa = Connection::open_with_flags(ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sb = Connection::open_with_flags(bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut target = Connection::open(path()).unwrap();
    target.pragma_update(None, "journal_mode", "WAL").unwrap();
    target.pragma_update(None, "synchronous", "FULL").unwrap();
    target.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut target,
        "installation",
        &[&sa, &sb],
    )
    .unwrap();
    target
}

fn protection() -> ProtectionReceipt {
    ProtectionReceipt {
        intent_id: "intent".into(),
        algo_id: "same-venue-algo".into(),
        side: "sell".into(),
        quantity: Decimal::ONE,
        trigger_price: Decimal::from(95),
        trigger_basis: "mark".into(),
        verified_at: 5,
        source_digest: "a".repeat(64),
    }
}

fn lot(db: &Connection, account: &str, qty: &str) {
    // Explicit primary lot fixture; this is not a signed fill or risk grant.
    // Identical IDs and instrument deliberately exercise namespace isolation.
    db.execute("INSERT INTO position_lots VALUES('installation',?1,'same-lot',NULL,'intent',NULL,'BTC-USDT-SWAP','long',?2,'100',4)",params![account,qty]).unwrap();
}

#[test]
fn risk_book_contract_protection_receipts_use_original_rules_and_namespace() {
    let mut db = imported(false);
    let tx = db.transaction().unwrap();
    let receipt = protection();
    AccountStore::verify_protection_sql(&tx, Some(("installation", A)), &receipt).unwrap();
    assert!(
        AccountStore::protection_sql(&tx, Some(("installation", B)), "intent")
            .unwrap()
            .is_none()
    );
    AccountStore::verify_protection_sql(&tx, Some(("installation", B)), &receipt).unwrap();
    let count: i64 = tx
        .query_row("SELECT COUNT(*) FROM protection_receipts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 2,
        "same venue algo ID belongs to independent account namespaces"
    );
    for invalid in [
        ProtectionReceipt {
            side: "buy".into(),
            ..receipt.clone()
        },
        ProtectionReceipt {
            quantity: Decimal::from(2),
            ..receipt.clone()
        },
        ProtectionReceipt {
            trigger_price: Decimal::from(94),
            ..receipt.clone()
        },
        ProtectionReceipt {
            source_digest: String::new(),
            ..receipt.clone()
        },
    ] {
        assert!(matches!(
            AccountStore::verify_protection_sql(&tx, Some(("installation", A)), &invalid),
            Err(StoreError::Invalid("PROTECTION_VERIFICATION_FAILED"))
        ));
    }
    assert!(
        AccountStore::verify_protection_sql(&tx, Some(("other-installation", A)), &receipt)
            .is_err()
    );
    tx.rollback().unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM protection_receipts", [], |r| r.get(0))
            .unwrap(),
        0
    );
}

#[test]
fn risk_book_contract_protection_resolution_requires_original_local_release_and_is_replayable() {
    let mut db = imported(true);
    // A foreign account's non-flat position cannot block A's own flat proof.
    lot(&db, B, "1");
    let digest = "b".repeat(64);
    let a = Some(("installation", A));
    let b = Some(("installation", B));
    let tx = db.transaction().unwrap();
    assert!(matches!(
        AccountStore::resolve_protection_incident_after_flat_sql(&tx, a, "intent", "invalid", 6),
        Err(StoreError::Invalid("PROTECTION_FLAT_SOURCE_DIGEST_INVALID"))
    ));
    assert!(matches!(
        AccountStore::resolve_protection_incident_after_flat_sql(&tx, b, "intent", &digest, 6),
        Err(StoreError::Invalid("PROTECTION_FLAT_SQL_EXPOSURE_REMAINS"))
    ));
    assert!(
        AccountStore::resolve_protection_incident_after_flat_sql(&tx, a, "intent", &digest, 6)
            .unwrap()
    );
    tx.rollback().unwrap();
    assert!(
        AccountStore::find_intent_sql(&db, a, "intent_id", "intent")
            .unwrap()
            .unwrap()
            .incident
            .is_some()
    );
    assert!(
        AccountStore::operation_sql(&db, a, "protection-recovered:intent")
            .unwrap()
            .is_none()
    );

    let tx = db.transaction().unwrap();
    assert!(
        AccountStore::resolve_protection_incident_after_flat_sql(&tx, a, "intent", &digest, 6)
            .unwrap()
    );
    let before = tx.total_changes();
    assert!(
        !AccountStore::resolve_protection_incident_after_flat_sql(&tx, a, "intent", &digest, 7)
            .unwrap()
    );
    assert_eq!(
        before,
        tx.total_changes(),
        "repeat must not create another resolution"
    );
    let saved = AccountStore::operation_sql(&tx, a, "protection-recovered:intent")
        .unwrap()
        .unwrap();
    assert_eq!(saved.kind, "PROTECTION_INCIDENT_FLAT_RESOLVED");
    assert_eq!(saved.request_hash, "central-hash");
    assert_eq!(saved.result_ref, digest);
    assert!(
        AccountStore::find_intent_sql(&tx, b, "intent_id", "intent")
            .unwrap()
            .unwrap()
            .incident
            .is_some()
    );
    assert!(
        AccountStore::operation_sql(&tx, b, "protection-recovered:intent")
            .unwrap()
            .is_none()
    );
    tx.commit().unwrap();
    let tx = db.transaction().unwrap();
    tx.execute(
        "DELETE FROM position_lots WHERE installation_id='installation' AND account_key=?1",
        [B],
    )
    .unwrap();
    assert!(
        matches!(
            AccountStore::resolve_protection_incident_after_flat_sql(&tx, b, "intent", &digest, 8),
            Err(StoreError::Invalid(
                "PROTECTION_FLAT_CENTRAL_RELEASE_NOT_PROVEN"
            ))
        ),
        "A's same-key release acknowledgement cannot release B"
    );
    tx.rollback().unwrap();
}

#[test]
fn risk_book_contract_protection_quantity_and_incident_use_one_account_snapshot() {
    let mut db = imported(false);
    lot(&db, A, "1");
    lot(&db, B, "0.5");
    let a = Some(("installation", A));
    let b = Some(("installation", B));
    let tx = db.transaction().unwrap();
    assert_eq!(
        AccountStore::effective_protected_quantity_sql(&tx, a, "intent", Decimal::ONE).unwrap(),
        Some(Decimal::ONE)
    );
    assert!(matches!(
        AccountStore::effective_protected_quantity_sql(&tx, b, "intent", Decimal::ONE),
        Err(StoreError::Invalid(
            "PROTECTION_REDUCED_LOT_RECEIPT_MISSING"
        ))
    ));
    assert!(matches!(
        AccountStore::effective_protected_quantity_sql(&tx, a, "intent", Decimal::new(5, 1)),
        Err(StoreError::Invalid("PROTECTION_ORIGINAL_SQL_LOT_MISMATCH"))
    ));
    AccountStore::protection_incident_sql(
        &tx,
        a,
        "intent",
        "MANAGED_AGGREGATE_PROTECTION_UNVERIFIED",
        9,
    )
    .unwrap();
    assert_eq!(
        AccountStore::find_intent_sql(&tx, a, "intent_id", "intent")
            .unwrap()
            .unwrap()
            .incident
            .as_deref(),
        Some("MANAGED_AGGREGATE_PROTECTION_UNVERIFIED")
    );
    assert_eq!(
        AccountStore::find_intent_sql(&tx, b, "intent_id", "intent")
            .unwrap()
            .unwrap()
            .incident
            .as_deref(),
        Some("PROTECTION_PENDING_OR_FAILED")
    );
    assert!(
        AccountStore::protection_incident_sql(
            &tx,
            Some(("foreign", A)),
            "intent",
            "PROTECTION_PENDING_OR_FAILED",
            10
        )
        .is_err()
    );
    tx.rollback().unwrap();
    assert_eq!(
        AccountStore::find_intent_sql(&db, a, "intent_id", "intent")
            .unwrap()
            .unwrap()
            .incident
            .as_deref(),
        Some("PROTECTION_PENDING_OR_FAILED")
    );
}

#[test]
fn risk_book_contract_partial_reduce_scoped_sources_match_original_and_cannot_borrow_sibling_bill()
{
    fn reduction(account: &str, with_bill: bool) -> AccountStore {
        let mut s = store_with_account(account);
        let definition = s.ensure_manual_instance(1).unwrap();
        let decision = Decision {
            decision_id: "decision".into(),
            instance_id: None,
            decision_at: 2,
            input_digest: "source".into(),
            policy_revision: 1,
            code_version: "test".into(),
        };
        let pending = NewIntent {
            plan: IntentPlan {
                strategy_definition_id: definition.definition_id,
                strategy_definition_version: definition.version,
                strategy_parameters_digest: definition.parameters_digest,
                operation: "reduce".into(),
                side: "sell".into(),
                quantity: Decimal::ONE,
                limit_price: Decimal::from(100),
                stop_price: None,
                strategy_instance: "manual".into(),
                planned_loss_usd: Decimal::ZERO,
                source_generation: 1,
                position_side: "long".into(),
                private_source_generation: 1,
                contract_value: Decimal::ONE,
                inverse: false,
                settlement_currency: "USDT".into(),
            },
            intent_id: "intent".into(),
            idempotency_key: "request".into(),
            request_hash: "body".into(),
            client_order_id: "stableclient".into(),
            deadline: 100,
            plan_hash: "plan".into(),
            policy_revision: 1,
            risk_increase: false,
            instrument_id: "BTC-USDT-SWAP".into(),
        };
        s.prepare(&decision, &pending).unwrap();
        s.reserve(
            &CentralReceipt {
                reservation_key: "local:intent".into(),
                account_key: account.into(),
                installation_id: "installation".into(),
                instance_id: None,
                intent_id: "intent".into(),
                plan_hash: "plan".into(),
                policy_revision: 1,
                receipt_hash: "local-receipt".into(),
                requested_loss_usd: Decimal::ZERO,
                conversion_digest: "fx".into(),
            },
            3,
        )
        .unwrap();
        assert!(s.begin_post("intent", 1, 4).unwrap());
        s.record_order_readback(
            "intent",
            "venue-order",
            IntentState::Filled,
            Decimal::ONE,
            7,
        )
        .unwrap();
        let fill = Fill {
            fill_id: "BTC-USDT-SWAP:trade:venue-order".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            trade_id: "trade".into(),
            exchange_order_id: "venue-order".into(),
            intent_id: Some("intent".into()),
            time: 7,
            qty: Decimal::ONE,
            price: Decimal::from(100),
            fee: Decimal::new(-1, 2),
            currency: "USDT".into(),
            side: "sell".into(),
        };
        let event = "fill:BTC-USDT-SWAP:trade:venue-order";
        s.ingest_page(&IngestPage {
            fills: vec![fill],
            swap_fill_sources: vec![SwapFillSource {
                fill_id: "BTC-USDT-SWAP:trade:venue-order".into(),
                bill_id: "same-bill".into(),
                sub_type: "5".into(),
                position_side: "long".into(),
                direction: "CLOSE".into(),
            }],
            bills: if with_bill {
                vec![Bill {
                    bill_id: "same-bill".into(),
                    kind: "2:5".into(),
                    time: 7,
                    amount: Decimal::new(-1, 2),
                    currency: "USDT".into(),
                    economic_event_key: event.into(),
                }]
            } else {
                vec![]
            },
            bill_order_sources: if with_bill {
                vec![BillOrderSource {
                    bill_id: "same-bill".into(),
                    instrument_id: "BTC-USDT-SWAP".into(),
                    exchange_order_id: "venue-order".into(),
                    trade_id: "trade".into(),
                }]
            } else {
                vec![]
            },
            finance: if with_bill {
                vec![FinanceEntry {
                    entry_id: "same-fee".into(),
                    economic_event_key: event.into(),
                    component: "FEE".into(),
                    allocation_leg: "account".into(),
                    amount: Decimal::new(-1, 2),
                    currency: "USDT".into(),
                    instance_id: None,
                    source_ref: "bill:same-bill".into(),
                    time: 7,
                    reverses_entry_id: None,
                }]
            } else {
                vec![]
            },
            cursor: Some(SourceCursor {
                source: "fills".into(),
                stream_partition: "SWAP".into(),
                cursor: String::new(),
                watermark: 10,
                generation: 1,
                coverage_gaps: String::new(),
            }),
            ..Default::default()
        })
        .unwrap();
        s.ingest_page(&IngestPage {
            cursor: Some(SourceCursor {
                source: "bills".into(),
                stream_partition: "account".into(),
                cursor: String::new(),
                watermark: 10,
                generation: 1,
                coverage_gaps: String::new(),
            }),
            ..Default::default()
        })
        .unwrap();
        s
    }
    let a = reduction(A, true);
    let b = reduction(B, false);
    let expected = a
        .prove_local_partial_reduce("intent", "venue-order", Decimal::ONE, 7)
        .unwrap();
    assert_eq!(expected.0, Decimal::new(-1, 2));
    assert_eq!(expected.1, 1);
    let (ap, bp) = (a.path.clone(), b.path.clone());
    drop(a);
    drop(b);
    let sa = Connection::open_with_flags(ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sb = Connection::open_with_flags(bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut db = Connection::open(path()).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut db,
        "installation",
        &[&sa, &sb],
    )
    .unwrap();
    let tx = db.transaction().unwrap();
    let before = tx.total_changes();
    assert_eq!(
        AccountStore::prove_local_partial_reduce_sql(
            &tx,
            Some(("installation", A)),
            "intent",
            "venue-order",
            Decimal::ONE,
            7
        )
        .unwrap(),
        expected
    );
    assert!(matches!(
        AccountStore::prove_local_partial_reduce_sql(
            &tx,
            Some(("installation", B)),
            "intent",
            "venue-order",
            Decimal::ONE,
            7
        ),
        Err(StoreError::Invalid("LOCAL_REDUCE_FEE_BILL_UNVERIFIED"))
    ));
    assert!(
        AccountStore::prove_local_partial_reduce_sql(
            &tx,
            Some(("foreign", A)),
            "intent",
            "venue-order",
            Decimal::ONE,
            7
        )
        .is_err()
    );
    assert!(
        AccountStore::protection_resize_source_sql(
            &tx,
            Some(("installation", A)),
            "intent",
            "venue-order",
            Decimal::ONE,
            7,
            Decimal::ONE
        )
        .is_err(),
        "billed reduce does not imply independently proven remaining opening lots"
    );
    assert_eq!(
        tx.total_changes(),
        before,
        "proof reads cannot mutate positions, receipts or budget"
    );
    tx.rollback().unwrap();
}
