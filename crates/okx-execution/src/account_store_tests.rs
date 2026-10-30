use super::*;
use sha2::Digest;
use std::sync::atomic::{AtomicU64, Ordering};
static COUNTER: AtomicU64 = AtomicU64::new(0);
#[test]
fn e1_sql_known_quote_is_immutable_scoped_and_never_all_in_permission() {
    let mut s = store();
    let i = intent(&mut s);
    let mut proof = EntryQuoteEvidence {
        schema: "okx.entry-known-quote/v1".into(),
        account_key: s.meta.account_key.clone(),
        installation_id: s.meta.installation_id.clone(),
        intent_id: i.intent_id.clone(),
        plan_hash: i.plan_hash.clone(),
        policy_revision: i.policy_revision,
        instrument_id: i.instrument_id.clone(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(100),
        contract_value: Decimal::ONE,
        fee_rate: Decimal::new(5, 4),
        maker_rate: "-0.0002".into(),
        taker_rate: "0.0005".into(),
        fee_level: "Lv1".into(),
        fee_exchange_at: 10,
        usd_per_usdt: Decimal::ONE,
        notional_usdt: Decimal::from(100),
        fee_usdt: Decimal::new(5, 2),
        total_usdt: Decimal::new(10005, 2),
        total_usd: Decimal::new(10005, 2),
        fee_generation: format!(
            "sha256:{:x}",
            sha2::Sha256::digest(
                serde_json::to_vec(&(
                    "BTC-USDT-SWAP",
                    "test_reference",
                    "Lv1",
                    "-0.0002",
                    "0.0005",
                    true
                ))
                .unwrap()
            )
        ),
        reference_generation: "test_reference".into(),
        fee_received_at: 10,
        conversion_digest: "b".repeat(64),
        conversion_received_at: 10,
        observed_at: 11,
    };
    s.bind_entry_quote(&proof).unwrap();
    s.bind_entry_quote(&proof).unwrap();
    assert_eq!(s.entry_quote(&i.intent_id).unwrap(), Some(proof.clone()));
    assert!(s.connection.execute(
        "UPDATE operation_receipts SET result_ref='tampered' WHERE operation_id='entry-quote:intent'",
        []
    ).is_err());
    assert!(
        s.connection
            .execute(
                "DELETE FROM operation_receipts WHERE operation_id='entry-quote:intent'",
                []
            )
            .is_err()
    );
    assert_eq!(s.entry_quote(&i.intent_id).unwrap(), Some(proof.clone()));
    let mut changed = proof.clone();
    changed.total_usd += Decimal::ONE;
    assert!(matches!(
        s.bind_entry_quote(&changed),
        Err(StoreError::Invalid("ENTRY_QUOTE_ARITHMETIC_MISMATCH"))
    ));
    changed = proof.clone();
    changed.fee_generation = format!("sha256:{}", "c".repeat(64));
    assert!(matches!(
        s.bind_entry_quote(&changed),
        Err(StoreError::Invalid("ENTRY_QUOTE_FEE_HASH_INVALID"))
    ));
    changed = proof.clone();
    changed.maker_rate = "-0.0003".into();
    assert!(matches!(
        s.bind_entry_quote(&changed),
        Err(StoreError::Invalid("ENTRY_QUOTE_FEE_HASH_INVALID"))
    ));
    changed = proof.clone();
    changed.taker_rate = "0.000500".into();
    assert!(matches!(
        s.bind_entry_quote(&changed),
        Err(StoreError::Invalid("ENTRY_QUOTE_FEE_HASH_INVALID"))
    ));
    changed = proof.clone();
    changed.fee_exchange_at = 0;
    assert!(matches!(
        s.bind_entry_quote(&changed),
        Err(StoreError::Invalid("ENTRY_QUOTE_SCOPE_UNVERIFIED"))
    ));
    changed = proof.clone();
    changed.account_key = "other".into();
    assert!(matches!(
        s.bind_entry_quote(&changed),
        Err(StoreError::Invalid("ENTRY_QUOTE_SCOPE_UNVERIFIED"))
    ));
    changed = proof.clone();
    changed.fee_rate = Decimal::ZERO;
    // Native owner checks fee vs authenticated maker/taker before math.
    assert!(matches!(
        s.bind_entry_quote(&changed),
        Err(StoreError::Invalid("ENTRY_QUOTE_SCOPE_UNVERIFIED"))
    ));
    assert_eq!(s.entry_quote(&i.intent_id).unwrap(), Some(proof.clone()));
    reserve(&mut s);
    assert!(s.entry_quote(&i.intent_id).unwrap().is_some());
    // No proof flag is emitted and no SQL risk grant is minted by the quote.
    assert!(s.central_receipt(&i.intent_id).unwrap().is_some());
    proof.observed_at = 9;
    assert!(s.bind_entry_quote(&proof).is_err());
    let file = s.path.clone();
    let owner = s.meta.clone();
    drop(s);
    let reopened =
        AccountStore::open_existing(&file, &owner.account_key, &owner.installation_id).unwrap();
    assert_eq!(
        reopened
            .entry_quote(&i.intent_id)
            .unwrap()
            .unwrap()
            .total_usd,
        Decimal::new(10005, 2)
    );
    assert!(
        reopened
            .connection
            .execute(
                "DELETE FROM operation_receipts WHERE operation_id='entry-quote:intent'",
                []
            )
            .is_err()
    );
}
#[test]
fn e2_census_retains_terminal_intents_until_central_release() {
    let mut s = store();
    let i = intent(&mut s);
    assert!(matches!(
        s.risk_census_intents(0),
        Err(StoreError::Invalid("COHORT_CENSUS_LIMIT_INVALID"))
    ));
    let (live, pending) = s.risk_census_intents(100).unwrap();
    assert!(live.iter().any(|x| x.intent_id == i.intent_id));
    assert!(pending.contains(&i.intent_id));
    reserve(&mut s);
    assert!(s.begin_post(&i.intent_id, 1, 4).unwrap());
    s.record_outcome(
        &i.intent_id,
        IntentState::Filled,
        Some("order_1"),
        "venue",
        5,
    )
    .unwrap();
    let (all, pending) = s.risk_census_intents(100).unwrap();
    assert!(
        all.iter()
            .any(|x| x.intent_id == i.intent_id && x.state.terminal())
    );
    assert!(pending.contains(&i.intent_id));
    s.acknowledge_central_release("central-key", "verified")
        .unwrap();
    let (after, pending) = s.risk_census_intents(100).unwrap();
    assert!(!after.iter().any(|x| x.intent_id == i.intent_id));
    assert!(!pending.contains(&i.intent_id));
}

#[test]
fn k5_recovery_pages_over_100_intents_are_disjoint_and_bounded() {
    let mut s = store();
    let first = intent(&mut s);
    for n in 1..=129 {
        let d = Decision {
            decision_id: format!("decision_{n:04}"),
            instance_id: None,
            decision_at: 2,
            input_digest: "inputs".into(),
            policy_revision: 1,
            code_version: "test".into(),
        };
        let i = NewIntent {
            plan: first.plan.clone(),
            intent_id: format!("intent_{n:04}"),
            idempotency_key: format!("request_{n:04}"),
            request_hash: format!("body_{n:04}"),
            client_order_id: format!("client_{n:04}"),
            deadline: 100,
            plan_hash: first.plan_hash.clone(),
            policy_revision: 1,
            risk_increase: true,
            instrument_id: "BTC-USDT-SWAP".into(),
        };
        s.prepare(&d, &i).unwrap();
    }
    let first_page = s.reconcilable_intents_page(100, 0).unwrap();
    let next_page = s.reconcilable_intents_page(100, 100).unwrap();
    assert_eq!(first_page.len(), 100);
    assert_eq!(next_page.len(), 30);
    let ids = first_page
        .iter()
        .chain(next_page.iter())
        .map(|i| i.intent_id.clone())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), 130);
    assert!(s.reconcilable_intents_page(100, 200).unwrap().is_empty());
    assert!(s.reconcilable_intents_page(0, 0).unwrap().is_empty());
}
#[test]
fn e1_intent_fill_cost_uses_only_covered_native_sources_and_survives_restart() {
    let mut s = store();
    intent(&mut s);
    acknowledged_entry_intent(&mut s);
    let fill = |n: u32, qty: Decimal, price: i64, fee: Decimal| Fill {
        fill_id: format!("fill-{n}"),
        instrument_id: "BTC-USDT-SWAP".into(),
        trade_id: format!("trade-{n}"),
        exchange_order_id: "venue-order".into(),
        intent_id: Some("intent".into()),
        time: 4 + i64::from(n),
        qty,
        price: Decimal::from(price),
        fee,
        currency: "USDT".into(),
        side: "buy".into(),
    };
    let first = fill(1, Decimal::new(4, 1), 100, Decimal::new(-1, 2));
    let second = fill(2, Decimal::new(6, 1), 90, Decimal::new(-2, 2));
    let page = IngestPage {
        fills: vec![first, second],
        ..Default::default()
    };
    s.ingest_page(&page).unwrap();
    assert!(matches!(
        s.intent_fill_cost_evidence("intent", 6),
        Err(StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"))
    ));
    let pending = IngestPage {
        cursor: Some(SourceCursor {
            source: "fills".into(),
            stream_partition: "SWAP".into(),
            cursor: "1|6|continuation".into(),
            watermark: 1,
            generation: 1,
            coverage_gaps: "BACKFILL_PENDING".into(),
        }),
        ..Default::default()
    };
    s.ingest_page(&pending).unwrap();
    assert!(matches!(
        s.intent_fill_cost_evidence("intent", 6),
        Err(StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"))
    ));
    let complete = IngestPage {
        cursor: Some(SourceCursor {
            source: "fills".into(),
            stream_partition: "SWAP".into(),
            cursor: String::new(),
            watermark: 6,
            generation: 1,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    s.ingest_page(&complete).unwrap();
    let verified = s.intent_fill_cost_evidence("intent", 6).unwrap();
    assert_eq!(verified.fill_count, 2);
    assert_eq!(verified.reservation_key, "central-key");
    assert_eq!(verified.exchange_order_id, "venue-order");
    assert_eq!(verified.gross_notional_usdt, Decimal::from(94));
    assert_eq!(verified.conservative_fee_usdt, Decimal::new(3, 2));
    assert_eq!(verified.source_digest.len(), 64);
    s.ingest_page(&page).unwrap();
    assert_eq!(s.intent_fill_cost_evidence("intent", 6).unwrap(), verified);
    assert!(matches!(
        s.intent_fill_cost_evidence("intent", 7),
        Err(StoreError::Invalid("ENTRY_FILL_COVERAGE_UNPROVEN"))
    ));
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let reopened =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(
        reopened.intent_fill_cost_evidence("intent", 6).unwrap(),
        verified
    );
}

#[test]
fn e1_intent_fill_cost_requires_real_post_and_matching_venue_order() {
    let mut s = store();
    intent(&mut s);
    s.ingest_page(&IngestPage {
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
    assert!(matches!(
        s.intent_fill_cost_evidence("intent", 10),
        Err(StoreError::Invalid("ENTRY_FILL_ORDER_UNVERIFIED"))
    ));
    acknowledged_entry_intent(&mut s);
    s.ingest_page(&IngestPage {
        fills: vec![Fill {
            fill_id: "foreign-order-fill".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            trade_id: "foreign-order-trade".into(),
            exchange_order_id: "some-other-order".into(),
            intent_id: Some("intent".into()),
            time: 6,
            qty: Decimal::ONE,
            price: Decimal::from(100),
            fee: Decimal::new(-1, 2),
            currency: "USDT".into(),
            side: "buy".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        s.intent_fill_cost_evidence("intent", 10),
        Err(StoreError::Invalid("ENTRY_FILL_EXCHANGE_ORDER_MISMATCH"))
    ));
}

#[test]
fn e1_intent_fill_cost_rejects_wrong_side_price_and_overfills() {
    let cases = [
        ("sell", "1", "100", "ENTRY_FILL_SOURCE_INVALID"),
        ("buy", "1", "101", "ENTRY_FILL_PRICE_BOUND_VIOLATED"),
        ("buy", "1.01", "100", "ENTRY_FILL_QUANTITY_EXCEEDED"),
    ];
    for (side, qty, price, expected) in cases {
        let mut s = store();
        intent(&mut s);
        acknowledged_entry_intent(&mut s);
        s.ingest_page(&IngestPage {
            fills: vec![Fill {
                fill_id: "unbound-fill".into(),
                instrument_id: "BTC-USDT-SWAP".into(),
                trade_id: "unbound-trade".into(),
                exchange_order_id: "venue-order".into(),
                intent_id: Some("intent".into()),
                time: 6,
                qty: Decimal::from_str_exact(qty).unwrap(),
                price: Decimal::from_str_exact(price).unwrap(),
                fee: Decimal::new(-1, 2),
                currency: "USDT".into(),
                side: side.into(),
            }],
            ..Default::default()
        })
        .unwrap();
        s.ingest_page(&IngestPage {
            cursor: Some(SourceCursor {
                source: "fills".into(),
                stream_partition: "SWAP".into(),
                cursor: String::new(),
                watermark: 6,
                generation: 1,
                coverage_gaps: String::new(),
            }),
            ..Default::default()
        })
        .unwrap();
        match s.intent_fill_cost_evidence("intent", 6) {
            Err(StoreError::Invalid(reason)) => assert_eq!(reason, expected),
            other => panic!("misbound fill should never become proof: {other:?}"),
        }
    }
}

#[test]
fn e1_intent_fill_cost_rejects_unverified_currency_and_missing_fills() {
    let mut s = store();
    intent(&mut s);
    acknowledged_entry_intent(&mut s);
    s.ingest_page(&IngestPage {
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
    assert!(matches!(
        s.intent_fill_cost_evidence("intent", 10),
        Err(StoreError::Invalid("ENTRY_FILL_EVIDENCE_EMPTY"))
    ));
    s.ingest_page(&IngestPage {
        fills: vec![Fill {
            fill_id: "fee-in-other-currency".into(),
            instrument_id: "BTC-USDT-SWAP".into(),
            trade_id: "foreign-fee".into(),
            exchange_order_id: "venue-order".into(),
            intent_id: Some("intent".into()),
            time: 8,
            qty: Decimal::ONE,
            price: Decimal::from(100),
            fee: Decimal::new(-1, 2),
            currency: "BTC".into(),
            side: "buy".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        s.intent_fill_cost_evidence("intent", 10),
        Err(StoreError::Invalid("ENTRY_FILL_SOURCE_INVALID"))
    ));
}

#[test]
fn f0_crash_before_prepare_commit_is_retriable_once() {
    let s = store();
    assert!(s.reconcilable_intents(16).unwrap().is_empty());
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert!(recovered.reconcilable_intents(16).unwrap().is_empty());
    let first = intent(&mut recovered);
    assert_eq!(first.state, IntentState::Prepared);
    assert_eq!(recovered.active_intents(16).unwrap().len(), 1);
}

#[test]
fn f1_prepared_before_reserve_is_recovered_once_without_central_release() {
    let mut s = store();
    intent(&mut s);
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    let pending = recovered.reconcilable_intents(16).unwrap();
    assert_eq!(pending.len(), 1, "expired pre-reserve must not be orphaned");
    assert_eq!(pending[0].state, IntentState::Prepared);
    recovered
        .reject_unsent("intent", "COMMAND_EXPIRED", 5)
        .unwrap();
    assert!(recovered.unsent_before_reserve("intent").unwrap());
    assert!(
        recovered
            .unsent_release_receipt("intent")
            .unwrap()
            .is_none()
    );
    assert!(recovered.reconcilable_intents(16).unwrap().is_empty());
    assert!(!recovered.begin_post("intent", 1, 6).unwrap());
    assert_eq!(
        recovered
            .intent("intent")
            .unwrap()
            .unwrap()
            .incident
            .as_deref(),
        Some("COMMAND_EXPIRED")
    );
}

#[test]
fn f3_crash_after_native_reserve_before_post_claim_releases_no_send() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut restored =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(restored.reconcilable_intents(16).unwrap().len(), 1);
    assert_eq!(
        restored.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
    restored
        .reject_unsent("intent", "COMMAND_EXPIRED", 5)
        .unwrap();
    let proof = restored.unsent_release_receipt("intent").unwrap().unwrap();
    assert_eq!(proof.reservation_key, "central-key");
    assert_eq!(proof.reason, "COMMAND_EXPIRED");
    assert!(!restored.begin_post("intent", 1, 6).unwrap());
    restored
        .acknowledge_central_release("central-key", "worker-release-proof")
        .unwrap();
    assert!(restored.reconcilable_intents(16).unwrap().is_empty());
}

#[test]
fn f8_central_release_local_ack_replay_keeps_first_receipt() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.reject_unsent("intent", "CENTRAL_POST_DENIED", 5).unwrap();
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(recovered.reconcilable_intents(16).unwrap().len(), 1);
    recovered
        .acknowledge_central_release("central-key", "central-released")
        .unwrap();
    recovered
        .acknowledge_central_release("central-key", "central-released")
        .unwrap();
    let receipt = recovered
        .operation("central-release:central-key")
        .unwrap()
        .unwrap();
    assert_eq!(receipt.result_ref, "central-released");
    assert_eq!(recovered.reconcilable_intents(16).unwrap().len(), 0);
    assert!(!recovered.begin_post("intent", 1, 6).unwrap());
}

#[test]
fn f2_lost_reserve_receipt_recovers_after_restart_without_second_reserve() {
    let mut s = store();
    intent(&mut s);
    let key = "3:okx4:real64:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa14:continuous_engine6:intent";
    s.mark_reserve_attempt("intent", key, 3).unwrap();
    // Before a remote reply there is no local CentralReceipt to scan.
    assert!(s.central_receipt("intent").unwrap().is_none());
    assert_eq!(s.reconcilable_intents(16).unwrap().len(), 1);
    let p = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(s.reconcilable_intents(16).unwrap()[0].intent_id, "intent");
    s.reject_unsent("intent", "RESERVE_REPLY_LOST", 5).unwrap();
    let proof = s.unsent_release_receipt("intent").unwrap().unwrap();
    assert_eq!(proof.reservation_key, key);
    assert_eq!(proof.reason, "RESERVE_REPLY_LOST");
    assert_eq!(s.reconcilable_intents(16).unwrap().len(), 1);
    s.acknowledge_central_release(key, "worker-released")
        .unwrap();
    assert_eq!(s.reconcilable_intents(16).unwrap().len(), 0);
    // Both proof and first cause survive a lost local release ACK.
    let proof = s.unsent_release_receipt("intent").unwrap().unwrap();
    assert_eq!(proof.reason, "RESERVE_REPLY_LOST");
    s.acknowledge_central_release(key, "worker-released")
        .unwrap();
    assert!(!s.begin_post("intent", 1, 6).unwrap());
}
#[test]
fn f2_explicit_worker_reserve_denial_has_no_orphan_risk_and_no_post() {
    let mut s = store();
    intent(&mut s);
    s.mark_reserve_attempt("intent", "risk_reserve_not_granted", 3)
        .unwrap();
    s.reject_denied_reserve("intent", "ORDER_OR_STRATEGY_RISK_LIMIT", 5)
        .unwrap();
    assert!(s.declined_reserve("intent").unwrap());
    assert!(s.unsent_release_receipt("intent").unwrap().is_none());
    assert!(s.reconcilable_intents(16).unwrap().is_empty());
    assert_eq!(
        s.intent("intent").unwrap().unwrap().incident.as_deref(),
        Some("ORDER_OR_STRATEGY_RISK_LIMIT")
    );
}
#[test]
fn f2_durable_marker_conflicts_and_never_becomes_post_claim() {
    let mut s = store();
    intent(&mut s);
    s.mark_reserve_attempt("intent", "original-reserve-key", 3)
        .unwrap();
    s.mark_reserve_attempt("intent", "original-reserve-key", 4)
        .unwrap();
    assert!(
        s.mark_reserve_attempt("intent", "different-key", 5)
            .is_err()
    );
    s.reject_unsent("intent", "RESERVE_LOCAL_COMMIT_UNCONFIRMED", 6)
        .unwrap();
    let evidence = s.unsent_release_receipt("intent").unwrap().unwrap();
    assert_eq!(evidence.reservation_key, "original-reserve-key");
    assert_eq!(evidence.reason, "RESERVE_LOCAL_COMMIT_UNCONFIRMED");
    assert!(!s.begin_post("intent", 1, 7).unwrap());
}
#[test]
fn sql_deadline_handler_is_removed_after_operation_error() {
    let mut s = store();
    let result: StoreResult<()> =
        s.run_before_deadline(std::time::Instant::now() + Duration::from_millis(1), |_| {
            std::thread::sleep(Duration::from_millis(3));
            Err(StoreError::Invalid("fixture-operation-failure"))
        });
    assert!(matches!(
        result,
        Err(StoreError::Invalid("fixture-operation-failure"))
    ));
    let total:i64=s.connection.query_row("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT sum(x) FROM n",[],|r|r.get(0)).unwrap();
    assert_eq!(total, 50005000);
}
#[tokio::test]
async fn long_reader_is_interrupted_at_deadline_and_worker_recovers() {
    let s = store();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let h = crate::AccountStoreHandle::open_existing(p, m.account_key, m.installation_id)
        .await
        .unwrap();
    let result=h.call(std::time::Instant::now()+Duration::from_millis(20),|s|{let _:i64=s.connection.query_row("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n",[],|r|r.get(0))?;Ok(())}).await;
    assert!(matches!(
        result,
        Err(StoreError::Invalid("ACCOUNT_STORAGE_DEADLINE"))
    ));
    assert_eq!(
        h.call(std::time::Instant::now() + Duration::from_secs(2), |s| Ok(
            s.meta.initialized_at
        ))
        .await
        .unwrap(),
        1
    );
}
#[test]
fn persisted_source_generation_survives_restart_without_losing_cursors() {
    let mut s = store();
    assert_eq!(s.source_generation_base().unwrap(), 0);
    let cursor = |generation| IngestPage {
        cursor: Some(SourceCursor {
            source: "fills".into(),
            stream_partition: "SWAP".into(),
            cursor: String::new(),
            watermark: 20,
            generation,
            coverage_gaps: String::new(),
        }),
        ..Default::default()
    };
    s.ingest_page(&cursor(5)).unwrap();
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(recovered.source_generation_base().unwrap(), 5);
    assert!(matches!(
        recovered.ingest_page(&cursor(1)),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    recovered.ingest_page(&cursor(6)).unwrap();
    assert_eq!(
        recovered
            .cursor("fills", "SWAP")
            .unwrap()
            .unwrap()
            .generation,
        6
    );
    assert_eq!(recovered.source_generation_base().unwrap(), 6);
}
#[test]
fn missing_fx_stops_risk_and_does_not_commit_zero_loss_or_cursor() {
    let mut s = store();
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "source".into(),
        usd_per_currency: Default::default(),
    };
    let page = IngestPage {
        finance: vec![fee("fee", "source")],
        cursor: Some(SourceCursor {
            source: "bills".into(),
            stream_partition: "account".into(),
            cursor: "done".into(),
            watermark: 5,
            generation: 1,
            coverage_gaps: "".into(),
        }),
        ..Default::default()
    };
    assert!(s.ingest_page_with_usd(&page, &fx).is_err());
    assert_eq!(s.stops().unwrap()[0].2, "USD_CONVERSION_SOURCE_UNAVAILABLE");
    assert!(s.cursor("bills", "account").unwrap().is_none());
    assert!(s.finance_history(1, 10, 0, "", 100).unwrap().is_empty());
}
#[test]
fn external_flow_rejects_non_positive_fx_without_committing_or_advancing_cursor() {
    for invalid_rate in [Decimal::ZERO, -Decimal::ONE] {
        let mut s = store();
        let page = IngestPage {
            cash_flows: vec![CashFlow {
                event_id: "fx-flow".into(),
                kind: "DEPOSIT".into(),
                currency: "USDT".into(),
                amount: Decimal::from(6),
                time: 5,
                source_ref: "exchange:deposit".into(),
            }],
            cursor: Some(SourceCursor {
                source: "bills".into(),
                stream_partition: "account".into(),
                cursor: "done".into(),
                watermark: 5,
                generation: 1,
                coverage_gaps: String::new(),
            }),
            ..Default::default()
        };
        let fx = UsdConversionEvidence {
            observed_at: 5,
            source_digest: "verified-fx".into(),
            usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), invalid_rate)]),
        };
        assert!(matches!(
            s.ingest_page_with_usd(&page, &fx),
            Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"))
        ));
        assert_eq!(s.stops().unwrap()[0].2, "USD_CONVERSION_SOURCE_UNAVAILABLE");
        assert!(s.cursor("bills", "account").unwrap().is_none());
        assert!(s.finance_history(1, 10, 0, "", 100).unwrap().is_empty());
        assert_eq!(
            s.usd_component_total("EXTERNAL_FLOW_USD").unwrap(),
            Decimal::ZERO
        );

        // The failed batch must be safely replayable with verified FX,
        // without duplicating the original economic deposit.
        let valid_fx = UsdConversionEvidence {
            observed_at: 5,
            source_digest: "verified-positive-fx".into(),
            usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::from(2))]),
        };
        s.ingest_page_with_usd(&page, &valid_fx).unwrap();
        s.ingest_page_with_usd(&page, &valid_fx).unwrap();
        assert_eq!(
            s.usd_component_total("EXTERNAL_FLOW_USD").unwrap(),
            Decimal::from(12)
        );
        assert_eq!(
            s.finance_history(1, 10, 0, "", 100)
                .unwrap()
                .into_iter()
                .filter(|e| e.component == "EXTERNAL_FLOW_USD")
                .count(),
            1
        );
        assert_eq!(s.cursor("bills", "account").unwrap().unwrap().watermark, 5);
    }
}

#[test]
fn allocation_and_bar_claim_survive_retry_and_restart() {
    let mut s = store();
    owned_fill(&mut s, "a", "a-order", 100, "long", 3);
    assert!(
        s.claim_strategy_decision("a", "bar-100", "inputs", 1, 5)
            .unwrap()
    );
    assert!(
        s.claim_strategy_decision("a", "bar-100", "inputs", 1, 5)
            .unwrap()
    );
    s.finish_strategy_no_trade("a", "bar-100", 5).unwrap();
    assert!(
        !s.claim_strategy_decision("a", "bar-100", "inputs", 1, 5)
            .unwrap()
    );
    let mut a = s.instances().unwrap().remove(0);
    a.budget = Decimal::from(15);
    a.policy_revision = 2;
    s.mutate_instance("allocation-update", "newbody", "update", &a, Some(1), 6)
        .unwrap();
    s.mutate_instance("allocation-update", "newbody", "update", &a, Some(1), 6)
        .unwrap();
    let amounts = s
        .connection
        .prepare("SELECT delta FROM allocations WHERE instance_id='a' ORDER BY time,event_id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(amounts, vec!["10", "5"]);
    // Updating or replaying the risk budget never creates capital facts.
    let count: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM nav_points WHERE scope_id='strategy:a'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &m.account_key, &m.installation_id).unwrap();
    assert!(
        !s.financial_risk(7, Some("a")).unwrap().nav_complete,
        "restart must not restore fictitious strategy NAV",
    );
    // A budget-only revision retains the same immutable strategy
    // definition; the terminal bar must not run a second time.
    assert!(
        !s.claim_strategy_decision("a", "bar-100", "inputs", 2, 7)
            .unwrap()
    );
    assert_eq!(
        s.instances().unwrap()[0].last_decision_key.as_deref(),
        Some("bar-100")
    );
}
#[test]
fn nav_window_excludes_deposit_and_reports_pre_t0_unavailable() {
    let mut s = store();
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page_with_usd(
        &IngestPage {
            cash_flows: vec![CashFlow {
                event_id: "deposit".into(),
                kind: "DEPOSIT".into(),
                currency: "USDT".into(),
                amount: Decimal::from(50),
                time: 5,
                source_ref: "exchange".into(),
            }],
            ..Default::default()
        },
        &fx,
    )
    .unwrap();
    assert_eq!(
        s.usd_component_total("EXTERNAL_FLOW_USD").unwrap(),
        Decimal::from(50)
    );
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 6,
            source_version: "source".into(),
            equity: Decimal::from(155),
            pnl: Decimal::from(5),
            flows: Decimal::from(50),
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "source".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let v = s.nav_summary(1, 6, &[], &["USD".into()]).unwrap();
    assert_eq!(v[0].flows, Some(Decimal::from(50)));
    assert_eq!(v[0].pnl, Some(Decimal::from(5)));
    assert_eq!(
        s.financial_risk(6, None).unwrap().drawdown_ratio,
        Decimal::ZERO
    );
    let before = s.nav_summary(0, 6, &[], &["USD".into()]).unwrap();
    assert!(before[0].opening_equity.is_none());
    assert!(before[0].pnl.is_none());
    assert_eq!(before[0].coverage, "UNAVAILABLE_BEFORE_T0");
}
#[test]
fn funding_opposite_instances_conserves_account_and_keeps_residual_explicit() {
    let mut s = store();
    owned_fill(&mut s, "a", "a1", 100, "long", 3);
    owned_fill(&mut s, "a", "a2", 100, "long", 4);
    owned_fill(&mut s, "b", "b1", 100, "short", 5);
    let fx = UsdConversionEvidence {
        observed_at: 10,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page(&IngestPage {
        bills: vec![Bill {
            bill_id: "f".into(),
            kind: "FUNDING".into(),
            time: 9,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
            economic_event_key: "bill:f".into(),
        }],
        funding_events: vec![FundingEvent {
            event_id: "f".into(),
            instrument_id: "BTC".into(),
            time: 9,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    s.attribute_pending_funding(&fx, 10).unwrap();
    let f = s
        .finance_history(1, 10, 0, "", 100)
        .unwrap()
        .into_iter()
        .filter(|e| e.component == "FUNDING")
        .collect::<Vec<_>>();
    assert_eq!(f.iter().map(|e| e.amount).sum::<Decimal>(), -Decimal::ONE);
    assert_eq!(
        f.iter()
            .find(|e| e.instance_id.as_deref() == Some("a"))
            .unwrap()
            .amount,
        Decimal::from(-2)
    );
    assert_eq!(
        f.iter()
            .find(|e| e.instance_id.as_deref() == Some("b"))
            .unwrap()
            .amount,
        Decimal::ONE
    );
}
fn owned_fill(s: &mut AccountStore, instance: &str, id: &str, price: i64, side: &str, time: i64) {
    let i = StrategyInstance {
        instance_id: instance.into(),
        definition_id: "fixture".into(),
        definition_version: "1".into(),
        parameters: "{}".into(),
        enabled: true,
        policy_revision: 1,
        budget: Decimal::from(10),
        last_decision_key: None,
    };
    s.mutate_instance(&format!("create:{instance}"), "hash", "create", &i, None, 2)
        .unwrap();
    let definition = s.strategy_definition(instance).unwrap();
    let plan = IntentPlan {
        strategy_definition_id: definition.definition_id,
        strategy_definition_version: definition.version,
        strategy_parameters_digest: definition.parameters_digest,
        operation: "open".into(),
        side: if side == "long" { "buy" } else { "sell" }.into(),
        position_side: side.into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(price),
        stop_price: None,
        strategy_instance: instance.into(),
        planned_loss_usd: Decimal::ONE,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::new(1, 2),
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: format!("d:{id}"),
        instance_id: Some(instance.into()),
        decision_at: 2,
        input_digest: "input".into(),
        policy_revision: 1,
        code_version: "fixture".into(),
    };
    let intent = NewIntent {
        plan,
        intent_id: id.into(),
        idempotency_key: id.into(),
        request_hash: "body".into(),
        client_order_id: id.into(),
        deadline: 1000,
        plan_hash: "plan".into(),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: "BTC".into(),
    };
    s.prepare(&decision, &intent).unwrap();
    s.ingest_attributed_fill(
        Fill {
            side: if side == "long" { "buy" } else { "sell" }.into(),
            fill_id: format!("f:{id}"),
            instrument_id: "BTC".into(),
            trade_id: id.into(),
            exchange_order_id: format!("o:{id}"),
            intent_id: Some(id.into()),
            time,
            qty: Decimal::ONE,
            price: Decimal::from(price),
            fee: Decimal::ZERO,
            currency: "USDT".into(),
        },
        FillEffect::Open { side: side.into() },
        None,
    )
    .unwrap();
}
#[test]
fn unverified_strategy_budget_is_never_strategy_nav() {
    let mut s = store();
    s.ensure_manual_instance(2).unwrap();
    owned_fill(&mut s, "a", "a-order", 100, "long", 3);
    owned_fill(&mut s, "b", "b-order", 200, "long", 4);
    // Policy allocations are recorded, but no risk budget can create an
    // opening equity, flow, or drawdown high-watermark for a strategy.
    let scope_count: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM nav_scopes WHERE scope_id LIKE 'strategy:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let synthetic_nav: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM nav_points WHERE scope_id LIKE 'strategy:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let synthetic_peak: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM nav_watermarks WHERE scope_id LIKE 'strategy:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(scope_count, 3);
    assert_eq!(synthetic_nav, 0);
    assert_eq!(synthetic_peak, 0);
    // Even a legacy projection incorrectly labelled COMPLETE cannot establish
    // strategy capital or become an equity/profit figure in portfolio views.
    s.ingest_page(&IngestPage {
        nav: vec![
            NavPoint {
                scope_id: "strategy:a".into(),
                time: 5,
                source_version: "legacy-budget-is-not-capital".into(),
                equity: Decimal::from(12),
                pnl: Decimal::from(2),
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: "legacy".into(),
            },
            NavPoint {
                scope_id: "strategy:manual".into(),
                time: 5,
                source_version: "legacy-manual-budget".into(),
                equity: Decimal::from(10),
                pnl: Decimal::ZERO,
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: "unverified-manual-budget".into(),
            },
        ],
        ..Default::default()
    })
    .unwrap();

    let windows = s.nav_summary(2, 6, &[], &["USD".into()]).unwrap();
    for id in ["manual", "a", "b"] {
        let scope = format!("strategy:{id}");
        let nav = windows.iter().find(|n| n.scope_id == scope).unwrap();
        assert_eq!(
            nav.coverage,
            if id == "manual" {
                "NOT_APPLICABLE"
            } else {
                "PARTIAL"
            }
        );
        assert_eq!(nav.opening_equity, None);
        assert_eq!(nav.current_equity, None);
        assert_eq!(nav.flows, None);
        assert_eq!(nav.pnl, None);
        assert!(!s.financial_risk(5, Some(id)).unwrap().nav_complete);
    }
    assert!(s.financial_risk(5, None).unwrap().nav_complete);
    let manual_status = s.strategy_status("manual", 5).unwrap();
    assert_eq!(manual_status["capital_scope"], "account");
    assert_eq!(manual_status["risk"]["nav_complete"], true);
    assert_eq!(manual_status["nav"][0]["coverage"], "NOT_APPLICABLE");
    let funded_without_proof = s.strategy_status("a", 5).unwrap();
    assert_eq!(funded_without_proof["capital_scope"], "strategy");
    assert_eq!(funded_without_proof["risk"]["nav_complete"], false);
    assert_eq!(funded_without_proof["nav"][0]["coverage"], "PARTIAL");
}
#[test]
fn funding_at_event_respects_opposite_instances_and_later_fills() {
    let mut s = store();
    owned_fill(&mut s, "a", "a-order", 100, "long", 3);
    owned_fill(&mut s, "b", "b-order", 100, "short", 20);
    let fx = UsdConversionEvidence {
        observed_at: 30,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page(&IngestPage {
        bills: vec![Bill {
            bill_id: "funding".into(),
            kind: "FUNDING".into(),
            time: 10,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
            economic_event_key: "bill:funding".into(),
        }],
        funding_events: vec![FundingEvent {
            event_id: "funding".into(),
            instrument_id: "BTC".into(),
            time: 10,
            amount: -Decimal::ONE,
            currency: "USDT".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.attribute_pending_funding(&fx, 100).unwrap(), 1);
    assert_eq!(s.attribute_pending_funding(&fx, 100).unwrap(), 0);
    let rows = s.finance_history(1, 30, 0, "", 100).unwrap();
    let funding = rows
        .iter()
        .filter(|e| e.component == "FUNDING")
        .collect::<Vec<_>>();
    assert_eq!(
        funding.iter().filter(|e| e.amount != Decimal::ZERO).count(),
        1
    );
    assert_eq!(funding[0].instance_id.as_deref(), Some("a"));
    assert_eq!(s.policy().unwrap().4, Decimal::ONE);
}
#[test]
fn utc_daily_risk_rolls_without_replenishing_campaign_or_clearing_stop() {
    let mut s = store();
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "fx".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::ONE)]),
    };
    s.ingest_page_with_usd(
        &IngestPage {
            finance: vec![fee("fee", "source")],
            ..Default::default()
        },
        &fx,
    )
    .unwrap();
    assert_eq!(
        s.financial_risk(5, None).unwrap().daily_loss_usd,
        Decimal::new(25, 2)
    );
    s.latch_stop("drawdown", "account", "DRAWDOWN", 5).unwrap();
    assert_eq!(
        s.financial_risk(86_400_005, None).unwrap().daily_loss_usd,
        Decimal::ZERO
    );
    assert_eq!(s.policy().unwrap().4, Decimal::new(25, 2));
    assert_eq!(s.stops().unwrap().len(), 1);
}
#[test]
fn p02_m2_interest_fee_funding_charge_once_with_fx_and_never_refill_on_profit() {
    let mut s = store();
    let mut interest = fee("bill-interest", "bill:interest");
    interest.economic_event_key = "bill:interest".into();
    interest.component = "INTEREST".into();
    interest.amount = Decimal::new(-4, 1);

    let mut funding = fee("bill-funding", "bill:funding");
    funding.economic_event_key = "bill:funding".into();
    funding.component = "FUNDING".into();
    funding.amount = Decimal::new(-1, 1);

    let mut profit = fee("bill-profit", "bill:profit");
    profit.economic_event_key = "bill:profit".into();
    profit.component = "REALIZED_PNL".into();
    profit.amount = Decimal::from(4);

    let fee_fact = fee("trade-fee", "fill:fee");
    let page = IngestPage {
        finance: vec![interest.clone(), funding, fee_fact, profit],
        ..Default::default()
    };
    let fx = UsdConversionEvidence {
        observed_at: 4,
        source_digest: "attested-usdt-usd".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::from(2))]),
    };
    s.ingest_page_with_usd(&page, &fx).unwrap();
    s.ingest_page_with_usd(&page, &fx).unwrap();
    // (0.40 interest + 0.10 funding + 0.25 fee) * 2 USD/USDT.
    // A 4 USDT realized profit is recorded separately and does not refill risk.
    assert_eq!(s.policy().unwrap().4, Decimal::new(15, 1));
    assert_eq!(
        s.usd_component_total("CAMPAIGN_LOSS_USD").unwrap(),
        Decimal::new(15, 1)
    );
    assert_eq!(
        s.finance_totals(1, 10, None)
            .unwrap()
            .get(&("USDT".into(), "INTEREST".into()))
            .copied(),
        Some(Decimal::new(-4, 1))
    );
    let events = s.finance_history(1, 10, 0, "", 50).unwrap();
    assert_eq!(
        events.iter().filter(|e| e.component == "INTEREST").count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.component == "CAMPAIGN_LOSS_USD")
            .count(),
        3
    );

    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    recovered.ingest_page_with_usd(&page, &fx).unwrap();
    assert_eq!(recovered.policy().unwrap().4, Decimal::new(15, 1));

    let mut rebate = interest;
    rebate.entry_id = "bill-interest-rebate".into();
    rebate.economic_event_key = "bill:interest-rebate".into();
    rebate.amount = Decimal::new(4, 1);
    rebate.source_ref = "bill:interest-rebate".into();
    recovered
        .ingest_page_with_usd(
            &IngestPage {
                finance: vec![rebate],
                ..Default::default()
            },
            &fx,
        )
        .unwrap();
    assert_eq!(recovered.policy().unwrap().4, Decimal::new(15, 1));
}

#[test]
fn p02_m2_interest_without_verified_fx_rolls_back_and_preserves_cursor() {
    let mut s = store();
    let mut interest = fee("bill-interest", "bill:interest");
    interest.economic_event_key = "bill:interest".into();
    interest.component = "INTEREST".into();
    interest.amount = Decimal::new(-4, 1);
    let cursor = SourceCursor {
        source: "bills".into(),
        stream_partition: "account".into(),
        cursor: "bill-interest".into(),
        watermark: 5,
        generation: 1,
        coverage_gaps: String::new(),
    };
    let page = IngestPage {
        finance: vec![interest],
        cursor: Some(cursor.clone()),
        ..Default::default()
    };
    let missing_fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "missing-interest-rate".into(),
        usd_per_currency: Default::default(),
    };
    assert!(matches!(
        s.ingest_page_with_usd(&page, &missing_fx),
        Err(StoreError::Invalid("USD_CONVERSION_SOURCE_UNAVAILABLE"))
    ));
    assert!(s.cursor("bills", "account").unwrap().is_none());
    assert!(s.finance_history(1, 10, 0, "", 20).unwrap().is_empty());
    assert_eq!(s.policy().unwrap().4, Decimal::ZERO);

    let verified_fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "verified-interest-rate".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::from(2))]),
    };
    s.ingest_page_with_usd(&page, &verified_fx).unwrap();
    s.ingest_page_with_usd(&page, &verified_fx).unwrap();
    assert_eq!(
        s.cursor("bills", "account").unwrap().unwrap().watermark,
        cursor.watermark
    );
    assert_eq!(s.policy().unwrap().4, Decimal::new(8, 1));
}

#[test]
fn campaign_conversion_is_attested_deduplicated_and_profit_never_refills() {
    let mut s = store();
    let e = fee("fee", "fill-source");
    let fx = UsdConversionEvidence {
        observed_at: 4,
        source_digest: "authenticated-usdt-usd".into(),
        usd_per_currency: std::collections::BTreeMap::from([(
            "USDT".into(),
            Decimal::new(10001, 4),
        )]),
    };
    let page = IngestPage {
        finance: vec![e],
        ..Default::default()
    };
    s.ingest_page_with_usd(&page, &fx).unwrap();
    let loss = s.policy().unwrap().4;
    assert_eq!(loss, Decimal::new(250025, 6));
    let later = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "different-rate".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::from(2))]),
    };
    s.ingest_page_with_usd(&page, &later).unwrap();
    assert_eq!(s.policy().unwrap().4, loss);
    let profit = FinanceEntry {
        entry_id: "profit".into(),
        economic_event_key: "profit".into(),
        component: "REALIZED_PNL".into(),
        amount: Decimal::from(20),
        ..fee("unused", "bill-source")
    };
    s.ingest_page_with_usd(
        &IngestPage {
            finance: vec![profit],
            ..Default::default()
        },
        &fx,
    )
    .unwrap();
    assert_eq!(s.policy().unwrap().4, loss);
    let rows = s.finance_history(1, 10, 0, "", 100).unwrap();
    assert!(
        rows.iter()
            .any(|e| e.component == "CAMPAIGN_LOSS_USD" && e.source_ref.contains("rate:1.0001"))
    );
}
#[test]
fn finance_filters_are_parameterized_and_exact() {
    let mut s = store();
    let mut e = fee("fee", "source");
    e.currency = "USD".into();
    s.ingest_page(&IngestPage {
        finance: vec![e],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        s.finance_history_filtered(1, 10, 0, "", 10, &[], &["USD".into()])
            .unwrap()
            .len(),
        1
    );
    assert!(
        s.finance_history_filtered(1, 10, 0, "", 10, &[], &["USD' OR 1=1 --".into()])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        s.finance_totals_filtered(1, 10, &[], &["USD".into()])
            .unwrap()[&("USD".into(), "FEE".into())],
        Decimal::new(-25, 2)
    );
    assert!(
        s.finance_history_filtered(1, 10, 0, "", 10, &[], &vec!["USD".into(); 33])
            .is_err()
    );
}
#[test]
fn pending_fill_projection_is_exactly_once() {
    let mut s = store();
    let f = Fill {
        side: "buy".into(),
        fill_id: "pending".into(),
        instrument_id: "BTC".into(),
        trade_id: "t".into(),
        exchange_order_id: "o".into(),
        intent_id: None,
        time: 3,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::new(-1, 2),
        currency: "USDT".into(),
    };
    // The bill and fill are two provenance links for ONE economic fee;
    // unlike unrelated fees, an identical canonical economic event must
    // never be charged twice on replay after a pending projection.
    let mut source_fee = fee("source-fee", "bill-source");
    source_fee.economic_event_key = "fill:BTC:t:o".into();
    source_fee.amount = Decimal::new(-1, 2);
    s.ingest_page(&IngestPage {
        fills: vec![f],
        finance: vec![source_fee],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.project_pending_fills(100).unwrap(), 1);
    assert_eq!(s.project_pending_fills(100).unwrap(), 0);
    assert_eq!(s.active_lots("BTC").unwrap().len(), 1);
    assert_eq!(s.finance_history(1, 10, 0, "", 10).unwrap().len(), 1);
    assert_eq!(
        s.finance_totals(1, 10, None)
            .unwrap()
            .get(&("USDT".into(), "FEE".into()))
            .copied(),
        Some(Decimal::new(-1, 2))
    );
    let linked: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM finance_source_links WHERE entry_id='source-fee'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(linked, 2, "bill + fill proof, one economic fee");
}
#[test]
fn backup_blocks_risk_until_exchange_and_central_reconcile() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    let backup = path();
    s.backup(&backup).unwrap();
    let mut s =
        AccountStore::open_existing(backup, &s.meta.account_key, &s.meta.installation_id).unwrap();
    assert!(s.begin_post("intent", 1, 4).is_err());
    assert!(s.mark_storage_reconciled(i64::MAX, "exchange", "").is_err());
    s.mark_storage_reconciled(i64::MAX, "exchange", "central")
        .unwrap();
    assert!(s.begin_post("intent", 1, 4).unwrap());
}
#[test]
fn periodic_backup_uses_readonly_snapshot_and_requires_restoration_reconciliation() {
    // An isolated directory prevents cross-test retention collisions.
    let original = store();
    let original_path = original.path.clone();
    let meta = original.meta.clone();
    drop(original);
    let root = std::env::temp_dir().join(format!(
        "okx-backup-test-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let source_path = root.join("account.db");
    std::fs::rename(&original_path, &source_path).unwrap();
    let mut active =
        AccountStore::open_existing(&source_path, &meta.account_key, &meta.installation_id)
            .unwrap();
    // The backup's source connection is separate and read-only. Risk writes
    // still have precisely one authorized AccountStore writer.
    active.ensure_manual_instance(1).unwrap();
    let snapshot_path = source_path.clone();
    let snapshot_meta = meta.clone();
    let backup = std::thread::spawn(move || {
        AccountStore::periodic_backup_from_reader(snapshot_path, snapshot_meta)
    });
    // No source writer lease is transferred to the backup thread.
    assert!(active.policy().is_ok());
    backup.join().unwrap().unwrap();
    assert!(!active.storage_recovery_required().unwrap());
    let backups = root.join("backups");
    let target = std::fs::read_dir(&backups)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "db"))
        .unwrap();
    assert!(target.with_extension("manifest.json").is_file());
    assert_eq!(
        std::fs::read_dir(&backups).unwrap().count(),
        2,
        "only a complete DB and checksum manifest should remain"
    );
    let restored =
        AccountStore::open_existing(&target, &meta.account_key, &meta.installation_id).unwrap();
    assert!(restored.storage_recovery_required().unwrap());
    assert_eq!(restored.meta().account_key, meta.account_key);
    drop(restored);
    drop(active);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn periodic_backup_keeps_consistent_wal_snapshot_with_concurrent_writes_and_rowids() {
    // The same production backup path, through a separate SQLite connection:
    // rowids in financial history must not be renumbered by compaction.
    let original = store();
    let original_path = original.path.clone();
    let meta = original.meta.clone();
    drop(original);
    let root = std::env::temp_dir().join(format!(
        "okx-backup-live-wal-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&root).unwrap();
    let source_path = root.join("account.db");
    std::fs::rename(&original_path, &source_path).unwrap();
    let active =
        AccountStore::open_existing(&source_path, &meta.account_key, &meta.installation_id)
            .unwrap();
    active
        .connection
        .execute_batch(
            "CREATE TABLE backup_wal_fixture(payload TEXT NOT NULL);
             INSERT INTO backup_wal_fixture VALUES('first'),('second'),('third');
             DELETE FROM backup_wal_fixture WHERE payload='second';
             WITH RECURSIVE n(x) AS (
                VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<512
             ) INSERT INTO backup_wal_fixture SELECT hex(randomblob(4096)) FROM n;",
        )
        .unwrap();

    let start = std::sync::Arc::new(std::sync::Barrier::new(2));
    let backup_start = start.clone();
    let backup_path = source_path.clone();
    let backup_meta = meta.clone();
    let backup = std::thread::spawn(move || {
        backup_start.wait();
        AccountStore::periodic_backup_from_reader(backup_path, backup_meta)
    });
    start.wait();
    // A live single-owner writer keeps committing while the backup reads
    // through its own connection. The backup must not acquire writer ownership.
    for n in 0..256 {
        active
            .connection
            .execute(
                "INSERT INTO backup_wal_fixture(payload) VALUES(?1)",
                [format!("live-{n:04}")],
            )
            .unwrap();
    }
    backup.join().unwrap().unwrap();

    assert!(!active.storage_recovery_required().unwrap());
    let snapshots = root.join("backups");
    let path = std::fs::read_dir(&snapshots)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "db"))
        .unwrap();
    assert!(path.with_extension("manifest.json").is_file());
    let restored =
        AccountStore::open_existing(&path, &meta.account_key, &meta.installation_id).unwrap();
    assert!(restored.storage_recovery_required().unwrap());
    let preserved_rowid: i64 = restored
        .connection
        .query_row(
            "SELECT rowid FROM backup_wal_fixture WHERE payload='third'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        preserved_rowid, 3,
        "backup must retain immutable SQLite rowids"
    );
    let copied_rows: i64 = restored
        .connection
        .query_row("SELECT COUNT(*) FROM backup_wal_fixture", [], |r| r.get(0))
        .unwrap();
    let live_rows: i64 = active
        .connection
        .query_row("SELECT COUNT(*) FROM backup_wal_fixture", [], |r| r.get(0))
        .unwrap();
    assert!(copied_rows >= 514 && copied_rows <= live_rows);
    let quick_check: String = restored
        .connection
        .query_row("PRAGMA quick_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(quick_check, "ok");
    drop(restored);
    drop(active);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(target_os = "linux")]
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}
#[cfg(target_os = "linux")]
#[test]
#[ignore = "explicit million-history performance workload"]
fn sqlite_million_history_bounded_rss() {
    let mut s = store();
    intent(&mut s);
    let tx = s.connection.transaction().unwrap();
    tx.execute_batch("WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000) INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at) SELECT 'terminal-'||x,'key-'||x,'request-hash','decision','FILLED',4,'client-'||x,100,'plan',1,1,'BTC-USDT-SWAP',2,4 FROM n; INSERT INTO intent_plans SELECT intent_id,'open','buy','1','100','95','manual','5',1,'long',1,'1',0,'USDT',i.definition_id,i.definition_version,i.parameters_digest FROM intents CROSS JOIN strategy_instances i WHERE state='FILLED' AND i.instance_id='manual';").unwrap();
    tx.commit().unwrap();
    s.checkpoint().unwrap();
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let before = rss_kib();
    let start = std::time::Instant::now();
    let s = AccountStore::open_existing(&path, &meta.account_key, &meta.installation_id).unwrap();
    let active = s.active_intents(100).unwrap();
    let delta = rss_kib().saturating_sub(before);
    assert_eq!(active.len(), 1);
    assert!(delta <= 256 * 1024, "RSS increase {delta} KiB");
    let mut q=s.connection.prepare("EXPLAIN QUERY PLAN SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT 100").unwrap();
    let plans = q
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(plans.iter().any(|s| s.contains("idx_intents_active")));
    let cold_us = start.elapsed().as_micros();
    for _ in 0..5 {
        std::hint::black_box(s.account_summary().unwrap());
    }
    let mut measurements = Vec::new();
    let mut max_bytes = 0;
    for _ in 0..30 {
        let tick = std::time::Instant::now();
        let bytes = serde_json::to_vec(&s.account_summary().unwrap()).unwrap();
        measurements.push(tick.elapsed().as_micros() as u64);
        max_bytes = max_bytes.max(bytes.len());
    }
    measurements.sort();
    assert!(measurements[28] <= 200000);
    assert!(max_bytes <= 4096);
    use sha2::Digest;
    let fixture_hash = format!(
        "{:x}",
        sha2::Sha256::digest(
            concat!(
                include_str!("../schema/account-v2.sql"),
                "1000000 terminal intents+plans;1active"
            )
            .as_bytes()
        )
    );
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("model name"))
        .unwrap_or("unknown")
        .to_owned();
    let evidence = serde_json::json!({"test":"sqlite_million_history_bounded_rss","result":"PASS","os":std::env::consts::OS,"cpu":cpu,"sqlite_version":rusqlite::version(),"sqlite_source_id":s.sqlite_runtime().unwrap().1,"terminal_intents":1000000,"active_intents":active.len(),"warmup_runs":5,"measured_runs":30,"p50_us":measurements[14],"p95_us":measurements[28],"max_us":measurements[29],"response_bytes_max":max_bytes,"rss_delta_kib":delta,"cold_active_us":cold_us,"query_plan":plans,"fixture_sha256":fixture_hash});
    let output = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/verification/398-sql-million.json");
    std::fs::create_dir_all(output.parent().unwrap()).unwrap();
    std::fs::write(output, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    println!("{}", evidence);
    println!(
        "million_history terminal_count=1000000 active_count={} rss_delta_kib={delta} cold_active_elapsed_us={} sqlite={} index={plans:?}",
        active.len(),
        start.elapsed().as_micros(),
        rusqlite::version()
    );
    drop(q);
    drop(s);
    std::fs::remove_file(path).unwrap();
}
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "okx-t0-{}-{}.db",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}
#[test]
fn account_store_wal_has_one_checkpoint_owner_after_initialize_and_reopen() {
    let store = store();
    let path = store.path.clone();
    let meta = store.meta.clone();
    let auto: i64 = store
        .connection
        .pragma_query_value(None, "wal_autocheckpoint", |r| r.get(0))
        .unwrap();
    let synchronous: i64 = store
        .connection
        .pragma_query_value(None, "synchronous", |r| r.get(0))
        .unwrap();
    let mode: String = store
        .connection
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .unwrap();
    assert_eq!(auto, 0, "only the account worker may checkpoint");
    assert_eq!(synchronous, 2, "FULL durability must not be weakened");
    assert_eq!(mode, "wal");
    let (busy, frames, checked) = store.checkpoint().unwrap();
    assert_eq!(busy, 0);
    assert_eq!(frames, checked, "manual PASSIVE checkpoint must work");
    drop(store);
    let reopened =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    let auto: i64 = reopened
        .connection
        .pragma_query_value(None, "wal_autocheckpoint", |r| r.get(0))
        .unwrap();
    let synchronous: i64 = reopened
        .connection
        .pragma_query_value(None, "synchronous", |r| r.get(0))
        .unwrap();
    assert_eq!(auto, 0, "reopen must not revive competing auto-checkpoints");
    assert_eq!(synchronous, 2);
    reopened.integrity_check().unwrap();
}

#[test]
fn d1_statement_vm_counters_have_fixed_ids_and_valid_nonnegative_counts() {
    let mut store = store();
    *store.cash_flow_totals_cache.borrow_mut() = None;
    let mut cash = None;
    let flows = store
        .cash_flow_totals_observed(|stat| cash = Some(stat))
        .unwrap();
    assert!(!flows.is_empty());
    let cash = cash.unwrap();
    assert_eq!(cash.statement_id, "cash_flow_totals_select");
    assert!(cash.succeeded);
    assert!(cash.rows > 0);
    assert!(cash.vm_steps >= cash.rows);

    let mut projected = None;
    let count = store
        .project_pending_fills_observed(100, |stat| projected = Some(stat))
        .unwrap();
    let stat = projected.unwrap();
    assert_eq!(stat.statement_id, "pending_fills_select");
    assert!(stat.succeeded);
    assert_eq!(stat.rows, count as u64);
    assert!(stat.vm_steps > 0);
}

#[test]
fn d1_maintenance_observation_distinguishes_skipped_and_real_passive_checkpoint() {
    let mut s = store();
    let mut first = None;
    s.maintenance_observed(|observed| first = Some(observed))
        .unwrap();
    let first = first.unwrap();
    assert_eq!(first.checkpoint_mode, "PASSIVE");
    assert!(first.wal_before_bytes.is_some());
    assert!(first.wal_after_bytes.is_some());
    assert!(!first.checkpoint_attempted);
    assert!(first.checkpoint_busy.is_none());

    s.connection
        .execute_batch("CREATE TABLE d1_wal_observation(id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    s.connection
        .execute(
            "INSERT INTO d1_wal_observation(payload) VALUES(zeroblob(8000000))",
            [],
        )
        .unwrap();
    let mut observed = None;
    s.maintenance_observed(|checkpoint| observed = Some(checkpoint))
        .unwrap();
    let result = observed.unwrap();
    assert!(result.checkpoint_attempted);
    assert_eq!(result.checkpoint_mode, "PASSIVE");
    assert_eq!(result.checkpoint_busy, Some(0));
    assert!(result.log_frames.unwrap() >= 1000);
    assert!(result.checkpointed_frames.unwrap() > 0);
    assert!(result.checkpoint_elapsed_us.is_some());
    assert!(result.wal_before_bytes.unwrap() >= 4_000_000);
    assert!(result.wal_after_bytes.is_some());
}

#[test]
fn d1_ingest_reports_all_transaction_outcomes_including_pre_commit_errors() {
    let mut s = store();
    let mut success = None;
    s.ingest_page_observed(&IngestPage::default(), |timing| success = Some(timing))
        .unwrap();
    let success = success.unwrap();
    assert!(success.commit_succeeded);

    let mut bad_fill = k5_f7_fixture_fill();
    bad_fill.qty = Decimal::ZERO;
    let mut invalid = None;
    let result = s.ingest_page_observed(
        &IngestPage {
            fills: vec![bad_fill],
            ..Default::default()
        },
        |timing| invalid = Some(timing),
    );
    assert!(matches!(result, Err(StoreError::Invalid("INVALID_FILL"))));
    let invalid = invalid.unwrap();
    assert!(!invalid.commit_succeeded);
    assert_eq!(invalid.commit_us, 0);

    s.connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let mut begin_failure = None;
    let failed = s.ingest_page_observed(&IngestPage::default(), |timing| {
        begin_failure = Some(timing)
    });
    assert!(failed.is_err());
    s.connection.execute_batch("ROLLBACK").unwrap();
    let begin_failure = begin_failure.unwrap();
    assert_eq!(begin_failure.body_us, 0);
    assert_eq!(begin_failure.commit_us, 0);
    assert!(!begin_failure.commit_succeeded);
}

fn store() -> AccountStore {
    let p = path();
    let m = AccountMeta {
        initialize_operation_id: "connect-op".into(),
        release_id: "release".into(),
        account_key: "okx:real:verified".into(),
        installation_id: "installation".into(),
        initialized_at: 1,
        opening_source: "authenticated-flat".into(),
    };
    let s = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: m.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    let mut a = AccountStore::initialize_flat(p, m, &s).unwrap();
    a.apply_authority_policy(1, false, "campaign-20", Decimal::from(20))
        .unwrap();
    a.ensure_manual_instance(1).unwrap();
    a
}
fn intent(s: &mut AccountStore) -> StoredIntent {
    let definition = s.ensure_manual_instance(1).unwrap();
    let d = Decision {
        decision_id: "decision".into(),
        instance_id: None,
        decision_at: 2,
        input_digest: "inputs".into(),
        policy_revision: 1,
        code_version: "test".into(),
    };
    let i = NewIntent {
        plan: IntentPlan {
            strategy_definition_id: definition.definition_id,
            strategy_definition_version: definition.version,
            strategy_parameters_digest: definition.parameters_digest,
            contract_value: Decimal::ONE,
            inverse: false,
            settlement_currency: "USDT".into(),
            position_side: "long".into(),
            private_source_generation: 1,
            operation: "open".into(),
            side: "buy".into(),
            quantity: Decimal::ONE,
            limit_price: Decimal::from(100),
            stop_price: Some(Decimal::from(95)),
            strategy_instance: "manual".into(),
            planned_loss_usd: Decimal::from(5),
            source_generation: 1,
        },
        intent_id: "intent".into(),
        idempotency_key: "request".into(),
        request_hash: "body".into(),
        client_order_id: "stableclient".into(),
        deadline: 100,
        plan_hash: "plan".into(),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: "BTC-USDT-SWAP".into(),
    };
    s.prepare(&d, &i).unwrap()
}
fn reserve(s: &mut AccountStore) {
    s.reserve(
        &CentralReceipt {
            reservation_key: "central-key".into(),
            account_key: s.meta.account_key.clone(),
            installation_id: s.meta.installation_id.clone(),
            instance_id: None,
            intent_id: "intent".into(),
            plan_hash: "plan".into(),
            policy_revision: 1,
            receipt_hash: "central-hash".into(),
            requested_loss_usd: Decimal::from(5),
            conversion_digest: "usd-conversion".into(),
        },
        3,
    )
    .unwrap();
}
fn acknowledged_entry_intent(s: &mut AccountStore) {
    reserve(s);
    assert!(s.begin_post("intent", 1, 4).unwrap());
    s.record_outcome(
        "intent",
        IntentState::Acknowledged,
        Some("venue-order"),
        "venue-order-ack",
        4,
    )
    .unwrap();
}
fn fee(id: &str, source: &str) -> FinanceEntry {
    FinanceEntry {
        entry_id: id.into(),
        economic_event_key: "fill:BTC:trade:order".into(),
        component: "FEE".into(),
        allocation_leg: "account".into(),
        amount: Decimal::new(-25, 2),
        currency: "USDT".into(),
        instance_id: None,
        source_ref: source.into(),
        time: 4,
        reverses_entry_id: None,
    }
}
#[test]
fn sqlite_runtime_version_supported() {
    let s = store();
    let (v, source) = s.sqlite_runtime().unwrap();
    assert!(rusqlite::version_number() >= 3_051_003, "{v}");
    assert!(!source.is_empty());
    s.integrity_check().unwrap();
}
#[test]
fn sql_ingest_retry_exactly_once() {
    let mut s = store();
    let p = IngestPage {
        finance: vec![fee("fee", "fill")],
        ..Default::default()
    };
    s.ingest_page(&p).unwrap();
    s.ingest_page(&p).unwrap();
    let v = s.finance_history(1, 10, 0, "", 100).unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].amount, Decimal::new(-25, 2));
}
#[test]
fn p02_m2_finance_reversal_is_exact_one_owner_and_idempotent_after_restart() {
    let mut s = store();
    let mut original = fee("original-fee", "fill:original");
    original.economic_event_key = "economic:original-fee".into();
    original.currency = "USD".into();
    s.ingest_page(&IngestPage {
        finance: vec![original.clone()],
        ..Default::default()
    })
    .unwrap();

    let mut reversal = original.clone();
    reversal.entry_id = "reversal-fee".into();
    reversal.economic_event_key = "economic:reversal-fee".into();
    reversal.amount = -original.amount;
    reversal.source_ref = "bill:corrected".into();
    reversal.time = 8;
    reversal.reverses_entry_id = Some(original.entry_id.clone());
    let page = IngestPage {
        finance: vec![reversal.clone()],
        ..Default::default()
    };
    s.ingest_page(&page).unwrap();
    s.ingest_page(&page).unwrap();
    assert_eq!(
        s.finance_totals(1, 10, None)
            .unwrap()
            .get(&("USD".into(), "FEE".into()))
            .copied(),
        Some(Decimal::ZERO)
    );
    assert_eq!(s.finance_history(1, 10, 0, "", 20).unwrap().len(), 2);

    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut restored =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    restored.ingest_page(&page).unwrap();
    assert_eq!(restored.finance_history(1, 10, 0, "", 20).unwrap().len(), 2);
    assert_eq!(
        restored
            .finance_totals(1, 10, None)
            .unwrap()
            .get(&("USD".into(), "FEE".into()))
            .copied(),
        Some(Decimal::ZERO)
    );

    let mut second = reversal;
    second.entry_id = "duplicate-reversal".into();
    second.economic_event_key = "economic:duplicate-reversal".into();
    assert!(matches!(
        restored.ingest_page(&IngestPage {
            finance: vec![second],
            ..Default::default()
        }),
        Err(StoreError::Invalid("FINANCE_REVERSAL_ALREADY_APPLIED"))
    ));
    assert_eq!(restored.finance_history(1, 10, 0, "", 20).unwrap().len(), 2);
}

#[test]
fn p02_m2_unverified_or_wrong_reversal_rolls_back_finance_and_source_cursor() {
    let mut s = store();
    let mut original = fee("original-fee", "fill:original");
    original.economic_event_key = "economic:original-fee".into();
    original.currency = "USD".into();
    s.ingest_page(&IngestPage {
        finance: vec![original.clone()],
        ..Default::default()
    })
    .unwrap();

    let cursor = SourceCursor {
        source: "bills".into(),
        stream_partition: "account".into(),
        cursor: "corrected".into(),
        watermark: 8,
        generation: 1,
        coverage_gaps: String::new(),
    };
    let mut correction = original.clone();
    correction.entry_id = "corrected-fee".into();
    correction.economic_event_key = "economic:corrected-fee".into();
    correction.amount = -original.amount;
    correction.time = 8;
    correction.source_ref = "bill:correction".into();
    correction.reverses_entry_id = Some("missing-original".into());
    assert!(matches!(
        s.ingest_page(&IngestPage {
            finance: vec![correction.clone()],
            cursor: Some(cursor.clone()),
            ..Default::default()
        }),
        Err(StoreError::Invalid("FINANCE_REVERSAL_ORIGINAL_MISSING"))
    ));
    for changed in 0..5 {
        let mut wrong = correction.clone();
        wrong.reverses_entry_id = Some(original.entry_id.clone());
        match changed {
            0 => wrong.amount = Decimal::ONE,
            1 => wrong.currency = "USDT".into(),
            2 => wrong.component = "FUNDING".into(),
            3 => wrong.allocation_leg = "strategy:manual".into(),
            _ => wrong.instance_id = Some("manual".into()),
        }
        assert!(matches!(
            s.ingest_page(&IngestPage {
                finance: vec![wrong],
                cursor: Some(cursor.clone()),
                ..Default::default()
            }),
            Err(StoreError::Invalid("FINANCE_REVERSAL_MISMATCH"))
        ));
    }
    assert!(s.cursor("bills", "account").unwrap().is_none());
    assert_eq!(s.finance_history(1, 10, 0, "", 20).unwrap().len(), 1);

    // A late original can be ingested first, then its reversal retried.
    correction.reverses_entry_id = Some(original.entry_id);
    s.ingest_page(&IngestPage {
        finance: vec![correction.clone()],
        cursor: Some(cursor),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.cursor("bills", "account").unwrap().unwrap().watermark, 8);

    let mut chain = correction;
    chain.entry_id = "chain".into();
    chain.economic_event_key = "economic:chain".into();
    chain.amount = -chain.amount;
    chain.reverses_entry_id = Some("corrected-fee".into());
    assert!(matches!(
        s.ingest_page(&IngestPage {
            finance: vec![chain],
            ..Default::default()
        }),
        Err(StoreError::Invalid("FINANCE_REVERSAL_MISMATCH"))
    ));
}

#[test]
fn p02_immutable_economic_event_rejects_changed_time_or_reversal_without_advancing_cursor() {
    let mut s = store();
    let original = fee("canonical-fee", "fill:proof");
    s.ingest_page(&IngestPage {
        finance: vec![original.clone()],
        ..Default::default()
    })
    .unwrap();

    // The same economic event can acquire an additional real source link.
    let mut linked = original.clone();
    linked.entry_id = "second-alias".into();
    linked.source_ref = "bill:proof".into();
    linked.time = 5; // Exchange bill posts later than the originating fill.
    s.ingest_page(&IngestPage {
        finance: vec![linked.clone()],
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![linked.clone()],
        ..Default::default()
    })
    .unwrap();
    let source_links: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM finance_source_links WHERE entry_id='canonical-fee'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(source_links, 2);

    let cursor = SourceCursor {
        source: "bills".into(),
        stream_partition: "account".into(),
        cursor: "later".into(),
        watermark: 8,
        generation: 1,
        coverage_gaps: String::new(),
    };
    for change_reversal in [false, true] {
        let mut changed = if change_reversal {
            linked.clone()
        } else {
            original.clone()
        };
        if change_reversal {
            changed.reverses_entry_id = Some("another-event".into());
        } else {
            changed.time = 8;
        }
        assert!(matches!(
            s.ingest_page(&IngestPage {
                finance: vec![changed],
                cursor: Some(cursor.clone()),
                ..Default::default()
            }),
            Err(StoreError::Invalid("CANONICAL_ECONOMIC_EVENT_CONFLICT"))
        ));
        assert!(s.cursor("bills", "account").unwrap().is_none());
    }
    let rows = s.finance_history(1, 10, 0, "", 20).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].time, original.time);
    assert_eq!(rows[0].reverses_entry_id, None);
    assert_eq!(
        s.finance_totals(1, 10, None)
            .unwrap()
            .get(&("USDT".into(), "FEE".into()))
            .copied(),
        Some(original.amount)
    );
}

#[test]
fn p02_immutable_source_rows_reject_time_proof_and_event_link_conflicts() {
    let mut s = store();
    let bill = Bill {
        bill_id: "source-bill".into(),
        kind: "FUNDING".into(),
        time: 4,
        amount: -Decimal::ONE,
        currency: "USDT".into(),
        economic_event_key: "bill:source-bill".into(),
    };
    let flow = CashFlow {
        event_id: "source-deposit".into(),
        kind: "DEPOSIT".into(),
        currency: "USDT".into(),
        amount: Decimal::from(10),
        time: 5,
        source_ref: "exchange:source-deposit".into(),
    };
    let allocation = Allocation {
        event_id: "risk-budget".into(),
        instance_id: "manual".into(),
        delta: Decimal::from(3),
        currency: "USD".into(),
        time: 6,
    };
    let original = IngestPage {
        bills: vec![bill.clone()],
        cash_flows: vec![flow.clone()],
        allocations: vec![allocation.clone()],
        ..Default::default()
    };
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "attested-usdt-rate".into(),
        usd_per_currency: std::collections::BTreeMap::from([("USDT".into(), Decimal::new(125, 2))]),
    };
    s.ingest_page_with_usd(&original, &fx).unwrap();
    s.ingest_page_with_usd(&original, &fx).unwrap();
    assert_eq!(
        s.usd_component_total("EXTERNAL_FLOW_USD").unwrap(),
        Decimal::new(125, 1)
    );

    let mut bill_time = original.clone();
    bill_time.bills[0].time = 9;
    let mut bill_event = original.clone();
    bill_event.bills[0].economic_event_key = "bill:other".into();
    let mut flow_time = original.clone();
    flow_time.cash_flows[0].time = 9;
    let mut flow_proof = original.clone();
    flow_proof.cash_flows[0].source_ref = "exchange:other-deposit".into();
    let mut allocation_time = original.clone();
    allocation_time.allocations[0].time = 9;

    let cursor = SourceCursor {
        source: "bills".into(),
        stream_partition: "account".into(),
        cursor: "should-not-advance".into(),
        watermark: 9,
        generation: 1,
        coverage_gaps: String::new(),
    };
    for (mut invalid, reason) in [
        (bill_time, "IMMUTABLE_BILL_CONFLICT"),
        (bill_event, "IMMUTABLE_BILL_CONFLICT"),
        (flow_time, "IMMUTABLE_CASH_FLOW_CONFLICT"),
        (flow_proof, "IMMUTABLE_CASH_FLOW_CONFLICT"),
        (allocation_time, "IMMUTABLE_ALLOCATION_CONFLICT"),
    ] {
        // Valid rows earlier in this transaction must also roll back if a
        // later source in the same page contradicts an immutable row.
        invalid.bills.push(Bill {
            bill_id: "must-rollback".into(),
            ..bill.clone()
        });
        invalid
            .finance
            .push(fee("must-rollback-fee", "bill:rollback"));
        invalid.cursor = Some(cursor.clone());
        assert!(matches!(
            s.ingest_page_with_usd(&invalid, &fx),
            Err(StoreError::Invalid(code)) if code == reason
        ));
        assert!(s.cursor("bills", "account").unwrap().is_none());
        let bill_count: i64 = s
            .connection
            .query_row("SELECT COUNT(*) FROM bills", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            bill_count, 1,
            "source conflict must roll back the whole page"
        );
        assert!(
            s.finance_history(1, 10, 0, "", 100)
                .unwrap()
                .iter()
                .all(|row| row.entry_id != "must-rollback-fee")
        );
        assert_eq!(
            s.usd_component_total("EXTERNAL_FLOW_USD").unwrap(),
            Decimal::new(125, 1)
        );
    }
    // A correct page remains idempotent and can advance its source cursor.
    let mut replay = original;
    replay.cursor = Some(cursor);
    s.ingest_page_with_usd(&replay, &fx).unwrap();
    assert_eq!(s.cursor("bills", "account").unwrap().unwrap().watermark, 9);
    let (time, proof): (i64, String) = s
        .connection
        .query_row(
            "SELECT time,source_ref FROM cash_flows WHERE event_id='source-deposit'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((time, proof), (5, "exchange:source-deposit".to_string()));
}

#[test]
fn p02_fill_replay_rejects_changed_provenance_and_preserves_cursor() {
    let mut s = store();
    let original = Fill {
        side: "buy".into(),
        fill_id: "source-fill".into(),
        instrument_id: "BTC".into(),
        trade_id: "source-trade".into(),
        exchange_order_id: "source-order".into(),
        intent_id: None,
        time: 4,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::ZERO,
        currency: "USDT".into(),
    };
    let page = IngestPage {
        fills: vec![original.clone()],
        ..Default::default()
    };
    s.ingest_page(&page).unwrap();
    s.ingest_page(&page).unwrap();
    let cursor = SourceCursor {
        source: "fills".into(),
        stream_partition: "SWAP".into(),
        cursor: "new-page".into(),
        watermark: 10,
        generation: 1,
        coverage_gaps: String::new(),
    };

    for field in 0..4 {
        let mut changed = original.clone();
        match field {
            0 => changed.fill_id = "other-fill-id".into(),
            1 => changed.intent_id = Some("late-owner".into()),
            2 => changed.time = 9,
            _ => changed.side = "sell".into(),
        }
        let mut invalid = page.clone();
        let mut earlier = original.clone();
        earlier.fill_id = "transient-fill".into();
        earlier.trade_id = "transient-trade".into();
        invalid.fills = vec![earlier, changed];
        invalid.cursor = Some(cursor.clone());
        assert!(matches!(
            s.ingest_page(&invalid),
            Err(StoreError::Invalid("IMMUTABLE_FILL_CONFLICT"))
        ));
        assert!(s.cursor("fills", "SWAP").unwrap().is_none());
        let saved: (i64, i64) = s
            .connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM fills), (SELECT COUNT(*) FROM fill_projection_pending)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(saved, (1, 1), "the whole page must roll back");
    }
    assert_eq!(s.project_pending_fills(10).unwrap(), 1);
    assert_eq!(s.project_pending_fills(10).unwrap(), 0);
    assert_eq!(s.active_lots("BTC").unwrap().len(), 1);
    let mut replay = page;
    replay.cursor = Some(cursor);
    s.ingest_page(&replay).unwrap();
    assert_eq!(s.cursor("fills", "SWAP").unwrap().unwrap().watermark, 10);
}

#[test]
fn p02_m2_two_account_money_oracle_distinguishes_transfers_from_group_income() {
    let book = |suffix: &str, opening: i64| {
        let meta = AccountMeta {
            initialize_operation_id: format!("oracle-init-{suffix}"),
            release_id: "oracle-release".into(),
            account_key: format!("okx:demo:oracle-{suffix}"),
            installation_id: format!("oracle-install-{suffix}"),
            initialized_at: 1,
            opening_source: format!("oracle-opening-{suffix}"),
        };
        let snapshot = OpeningSnapshot {
            opening_equity_usd: Decimal::from(opening),
            source_ref: meta.opening_source.clone(),
            observed_at: 1,
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USD".into(), Decimal::from(opening))],
        };
        AccountStore::initialize_flat(path(), meta, &snapshot).unwrap()
    };
    let mut a = book("a", 1000);
    let mut b = book("b", 500);
    let flow = |id: &str, kind: &str, amount: i64| CashFlow {
        event_id: format!("oracle:{id}"),
        kind: kind.into(),
        currency: "USD".into(),
        amount: Decimal::from(amount),
        time: 3,
        source_ref: format!("venue:{id}"),
    };
    let economic = |id: &str, component: &str, amount: i64| FinanceEntry {
        entry_id: format!("entry:{id}"),
        economic_event_key: format!("bill:{id}"),
        component: component.into(),
        allocation_leg: "account".into(),
        amount: Decimal::from(amount),
        currency: "USD".into(),
        instance_id: None,
        source_ref: format!("bill:{id}"),
        time: 4,
        reverses_entry_id: None,
    };
    let entries = vec![
        economic("a-realized", "REALIZED_PNL", 20),
        economic("a-fee", "FEE", -2),
        economic("a-funding", "FUNDING", -1),
    ];
    let fx = UsdConversionEvidence {
        observed_at: 5,
        source_digest: "oracle-usd-identity".into(),
        usd_per_currency: Default::default(),
    };
    let a_page = IngestPage {
        cash_flows: vec![
            flow("external-deposit-a", "DEPOSIT", 100),
            flow("internal-a-to-b-out", "TRANSFER_OUT", -50),
        ],
        bills: entries
            .iter()
            .map(|e| Bill {
                bill_id: e.economic_event_key.clone(),
                kind: e.component.clone(),
                time: e.time,
                amount: e.amount,
                currency: e.currency.clone(),
                economic_event_key: e.economic_event_key.clone(),
            })
            .collect(),
        finance: entries,
        ..Default::default()
    };
    let b_page = IngestPage {
        cash_flows: vec![flow("internal-a-to-b-in", "TRANSFER_IN", 50)],
        ..Default::default()
    };
    for (s, page) in [(&mut a, &a_page), (&mut b, &b_page)] {
        s.ingest_page_with_usd(page, &fx).unwrap();
        s.ingest_page_with_usd(page, &fx).unwrap();
    }
    for (s, equity, flows, pnl) in [(&mut a, 1067, 50, 17), (&mut b, 550, 50, 0)] {
        s.ingest_page(&IngestPage {
            nav: vec![NavPoint {
                scope_id: "account".into(),
                time: 6,
                source_version: "oracle-venue-snapshot".into(),
                equity: Decimal::from(equity),
                pnl: Decimal::from(pnl),
                flows: Decimal::from(flows),
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: "oracle-venue".into(),
            }],
            ..Default::default()
        })
        .unwrap();
        let n = s.nav_summary(1, 6, &[], &["USD".into()]).unwrap();
        let account = n.iter().find(|n| n.scope_id == "account").unwrap();
        assert_eq!(account.coverage, "COMPLETE");
        assert_eq!(account.current_equity, Some(Decimal::from(equity)));
        assert_eq!(account.flows, Some(Decimal::from(flows)));
        assert_eq!(account.pnl, Some(Decimal::from(pnl)));
        assert_eq!(
            s.usd_component_total("INTERNAL_CAPITAL_USD").unwrap(),
            Decimal::ZERO
        );
    }
    let a_flows = a.usd_component_total("EXTERNAL_FLOW_USD").unwrap();
    let b_flows = b.usd_component_total("EXTERNAL_FLOW_USD").unwrap();
    assert_eq!((a_flows, b_flows), (Decimal::from(50), Decimal::from(50)));
    // The A→B transfer is -50+50 at group scope, not group income.
    assert_eq!(a_flows + b_flows, Decimal::from(100));
    let net = a.usd_component_total("REALIZED_PNL").unwrap()
        + a.usd_component_total("FEE").unwrap()
        + a.usd_component_total("FUNDING").unwrap();
    assert_eq!(net, Decimal::from(17));
    assert_eq!(
        a.opening_equity_usd().unwrap() + b.opening_equity_usd().unwrap(),
        Decimal::from(1500)
    );
    assert_eq!(
        Decimal::from(1500) + a_flows + b_flows + net,
        Decimal::from(1617)
    );
    assert_eq!(
        Decimal::from(1067) + Decimal::from(550),
        Decimal::from(1617)
    );
    assert_eq!(
        a.usd_component_total("CAMPAIGN_LOSS_USD").unwrap(),
        Decimal::from(3)
    );
}
#[test]
fn p02_immutable_nav_replay_rejects_conflict_before_highwater_and_cursor() {
    let mut s = store();
    let opening = NavPoint {
        scope_id: "account".into(),
        time: 4,
        source_version: "verified-mark-v1".into(),
        equity: Decimal::from(110),
        pnl: Decimal::from(10),
        flows: Decimal::ZERO,
        currency: "USD".into(),
        coverage: "COMPLETE".into(),
        mark_source: "exchange-snapshot".into(),
    };
    s.ingest_page(&IngestPage {
        nav: vec![opening.clone()],
        ..Default::default()
    })
    .unwrap();
    // Exact replays remain idempotent and must not create a second NAV row.
    s.ingest_page(&IngestPage {
        nav: vec![opening.clone()],
        ..Default::default()
    })
    .unwrap();
    let cursor = SourceCursor {
        source: "account_snapshot".into(),
        stream_partition: "account".into(),
        cursor: "new-page".into(),
        watermark: 5,
        generation: 1,
        coverage_gaps: String::new(),
    };
    for field in 0..3 {
        let mut conflict = opening.clone();
        match field {
            0 => conflict.equity = Decimal::from(9999),
            1 => conflict.coverage = "PARTIAL".into(),
            _ => conflict.mark_source = "different-exchange-snapshot".into(),
        }
        assert!(matches!(
            s.ingest_page(&IngestPage {
                nav: vec![conflict],
                finance: vec![fee("rolled-back-fee", "bill:later")],
                cursor: Some(cursor.clone()),
                ..Default::default()
            }),
            Err(StoreError::Invalid("IMMUTABLE_NAV_SOURCE_CONFLICT"))
        ));
        assert!(s.cursor("account_snapshot", "account").unwrap().is_none());
    }
    let count: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM nav_points WHERE scope_id='account' AND time=4",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let (highwater, adjusted): (String, String) = s
        .connection
        .query_row(
            "SELECT highwater,adjusted_equity FROM nav_watermarks WHERE scope_id='account'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(highwater, "110");
    assert_eq!(adjusted, "110");
    assert!(s.finance_history(1, 10, 0, "", 20).unwrap().is_empty());

    // A genuinely new snapshot version remains a legitimate next fact.
    let mut revised = opening;
    revised.source_version = "verified-mark-v2".into();
    revised.equity = Decimal::from(115);
    revised.pnl = Decimal::from(15);
    s.ingest_page(&IngestPage {
        nav: vec![revised],
        cursor: Some(cursor),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        s.cursor("account_snapshot", "account")
            .unwrap()
            .unwrap()
            .watermark,
        5
    );
}

#[test]
fn fee_fill_bill_counted_once() {
    let mut s = store();
    s.ingest_page(&IngestPage {
        finance: vec![fee("fill-fee", "fill:trade")],
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![fee("bill-fee", "bill:bill1")],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.finance_history(1, 10, 0, "", 100).unwrap().len(), 1);
    let count: i64 = s
        .connection
        .query_row("SELECT count(*) FROM finance_source_links", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 2);
}
#[test]
fn source_page_and_cursor_atomic() {
    let mut s = store();
    let cursor = SourceCursor {
        source: "fills".into(),
        stream_partition: "BTC".into(),
        cursor: "next".into(),
        watermark: 10,
        generation: 1,
        coverage_gaps: "".into(),
    };
    let bad = FinanceEntry {
        currency: "".into(),
        ..fee("bad", "source")
    };
    assert!(
        s.ingest_page(&IngestPage {
            finance: vec![fee("good", "source"), bad],
            cursor: Some(cursor.clone()),
            ..Default::default()
        })
        .is_err()
    );
    assert!(s.cursor("fills", "BTC").unwrap().is_none());
    assert!(s.finance_history(1, 20, 0, "", 10).unwrap().is_empty());
    s.ingest_page(&IngestPage {
        finance: vec![fee("good", "source")],
        cursor: Some(cursor),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.cursor("fills", "BTC").unwrap().unwrap().watermark, 10);
}
/// Real child-process termination, not an in-process Error or orderly
/// SQLite close. The Rust test binary is reused as the child; no new binary,
/// runner, service, or venue dependency.
fn k5_f7_fixture_fill() -> Fill {
    Fill {
        side: "buy".into(),
        fill_id: "fill-f7".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        trade_id: "trade-f7".into(),
        exchange_order_id: "order-f7".into(),
        intent_id: Some("intent".into()),
        time: 4,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::new(-10, 2),
        currency: "USDT".into(),
    }
}

#[test]
fn k5_child_hard_exit_at_native_claim() {
    let Ok(path) = std::env::var("OKX_K5_CHILD_SQL_PATH") else {
        return;
    };
    let phase = std::env::var("OKX_K5_CHILD_PHASE").expect("child phase");
    let account = std::env::var("OKX_K5_CHILD_ACCOUNT").expect("account identity");
    let installation = std::env::var("OKX_K5_CHILD_INSTALL").expect("installation identity");
    let mut store = AccountStore::open_existing(path, &account, &installation).unwrap();
    match phase.as_str() {
        "F0" | "F1" | "F2" | "F3" | "F4" | "F8" => {}
        "F7" => {
            let fill = k5_f7_fixture_fill();
            let tx = store
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            tx.execute(
                "INSERT INTO fills VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    fill.fill_id,
                    fill.instrument_id,
                    fill.trade_id,
                    fill.exchange_order_id,
                    fill.intent_id,
                    fill.time,
                    exact(fill.qty),
                    exact(fill.price),
                    exact(fill.fee),
                    fill.currency,
                    fill.side
                ],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO fill_projection_pending VALUES(?1,?2)",
                params!["fill-f7", "buy"],
            )
            .unwrap();
            tx.commit().unwrap();
        }
        "F5" | "F6" => {
            assert!(store.begin_post("intent", 1, 4).unwrap());
            if phase == "F6" {
                // This durable marker stands in for exactly one venue
                // place_order with its stable client order identity.
                // The process is then killed before any venue ACK arrives.
                let venue_path = std::env::var("OKX_K5_CHILD_VENUE_LOG").unwrap();
                let mut f = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(venue_path)
                    .unwrap();
                use std::io::Write;
                writeln!(f, "stableclient").unwrap();
                f.sync_all().unwrap();
            }
        }
        _ => panic!("unknown native kill phase"),
    }
    // Publish marker after the SQLite FULL WAL commit with the connection
    // still live. Parent must kill this process at the exact boundary.
    let ready = std::env::var("OKX_K5_CHILD_READY").unwrap();
    std::fs::write(ready, phase).unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn k5_real_os_kill_f0_through_f8_never_reposts_or_duplicates_finance() {
    // Same Rust binary and exact mounted test DB across the process boundary.
    // The child is deliberately not allowed to connect to OKX or RiskBook.
    for phase in ["F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"] {
        let mut store = store();
        let original = if phase == "F0" {
            None
        } else {
            Some(intent(&mut store))
        };
        if matches!(phase, "F3" | "F4" | "F5" | "F6" | "F8") {
            reserve(&mut store);
        }
        if phase == "F2" {
            store
                .mark_reserve_attempt("intent", "f2-original-central-reserve-key", 3)
                .unwrap();
        }
        if phase == "F8" {
            store
                .reject_unsent("intent", "CENTRAL_POST_DENIED", 5)
                .unwrap();
        }
        let path = store.path.clone();
        let meta = store.meta.clone();
        let venue_log = path.with_extension(format!("{phase}.venue-marker"));
        let ready = path.with_extension(format!("{phase}.ready"));
        drop(store);

        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--nocapture")
            .arg("k5_child_hard_exit_at_native_claim")
            .env("OKX_K5_CHILD_SQL_PATH", &path)
            .env("OKX_K5_CHILD_PHASE", phase)
            .env("OKX_K5_CHILD_ACCOUNT", &meta.account_key)
            .env("OKX_K5_CHILD_INSTALL", &meta.installation_id)
            .env("OKX_K5_CHILD_VENUE_LOG", &venue_log)
            .env("OKX_K5_CHILD_READY", &ready)
            .spawn()
            .expect("spawn child test executable");
        let mut witnessed = false;
        for _ in 0..200 {
            if ready.exists() {
                witnessed = true;
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("{phase}: helper stopped before the SQL claim boundary: {status}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            witnessed,
            "{phase}: child never reported durable SQL boundary"
        );
        assert_eq!(std::fs::read_to_string(&ready).unwrap(), phase);
        child.kill().expect("OS kill at durable SQL boundary");
        let status = child.wait().expect("reap killed native process");
        assert!(
            !status.success(),
            "{phase}: child must be killed, not finish normally"
        );
        let mut recovered =
            AccountStore::open_existing(&path, &meta.account_key, &meta.installation_id).unwrap();
        recovered.integrity_check().unwrap();
        if phase == "F0" {
            assert!(recovered.intent("intent").unwrap().is_none());
            let first = intent(&mut recovered);
            assert_eq!(intent(&mut recovered).intent_id, first.intent_id);
            assert_eq!(recovered.active_intents(16).unwrap().len(), 1);
            assert!(!venue_log.exists());
            continue;
        }
        let saved = recovered.intent("intent").unwrap().unwrap();
        assert_eq!(
            saved.client_order_id,
            original.as_ref().unwrap().client_order_id
        );
        if phase == "F7" {
            let pending_before: i64 = recovered
                .connection
                .query_row(
                    "SELECT count(*) FROM fill_projection_pending WHERE fill_id='fill-f7'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                pending_before, 1,
                "raw fact+pending persisted across OS kill"
            );
            assert!(recovered.active_lots("BTC-USDT-SWAP").unwrap().is_empty());
            for _ in 0..2 {
                recovered
                    .ingest_attributed_fill(
                        k5_f7_fixture_fill(),
                        FillEffect::Open {
                            side: "long".into(),
                        },
                        None,
                    )
                    .unwrap();
            }
            let pending_after: i64 = recovered
                .connection
                .query_row(
                    "SELECT count(*) FROM fill_projection_pending WHERE fill_id='fill-f7'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(pending_after, 0, "native projection complete");
            let lots = recovered.active_lots("BTC-USDT-SWAP").unwrap();
            assert_eq!(lots.len(), 1, "replay never doubles the position");
            assert_eq!(lots[0].remaining_qty, Decimal::ONE);
            assert_eq!(
                recovered
                    .finance_totals(1, 10, None)
                    .unwrap()
                    .get(&("USDT".into(), "FEE".into()))
                    .copied(),
                Some(Decimal::new(-10, 2)),
                "fee recovered once even when raw fill was committed before crash"
            );
            assert!(!venue_log.exists(), "reconciliation must never resend F7");
            continue;
        }
        if matches!(phase, "F5" | "F6") {
            assert_eq!(saved.state, IntentState::Posting);
            assert_eq!(recovered.recover_posting(5).unwrap(), 1);
            assert_eq!(
                recovered.intent("intent").unwrap().unwrap().state,
                IntentState::Unknown
            );
            assert!(!recovered.begin_post("intent", 1, 6).unwrap());
            assert_eq!(recovered.recover_posting(7).unwrap(), 0);
        } else if phase == "F8" {
            assert_eq!(saved.state, IntentState::Rejected);
            assert_eq!(recovered.reconcilable_intents(16).unwrap().len(), 1);
            recovered
                .acknowledge_central_release("central-key", "worker-proof")
                .unwrap();
            recovered
                .acknowledge_central_release("central-key", "worker-proof")
                .unwrap();
            assert!(recovered.reconcilable_intents(16).unwrap().is_empty());
            assert!(!recovered.begin_post("intent", 1, 6).unwrap());
        } else {
            assert_eq!(
                saved.state,
                if matches!(phase, "F1" | "F2") {
                    IntentState::Prepared
                } else {
                    IntentState::Reserved
                }
            );
            if phase == "F2" {
                assert!(
                    recovered
                        .operation("risk-reserve:intent")
                        .unwrap()
                        .is_some()
                );
                assert!(recovered.central_receipt("intent").unwrap().is_none());
            }
            assert!(
                recovered
                    .reject_unsent("intent", "COMMAND_EXPIRED", 5)
                    .unwrap()
            );
            if phase == "F2" {
                let proof = recovered.unsent_release_receipt("intent").unwrap().unwrap();
                assert_eq!(proof.reservation_key, "f2-original-central-reserve-key");
                recovered
                    .acknowledge_central_release(&proof.reservation_key, "worker-fenced")
                    .unwrap();
            } else if phase != "F1" {
                // F4: Worker may have accepted BEGIN_POST, but no local
                // SQL POST claim was ever committed. The same durable SQL
                // rejection witness can release central POSTED, proven in
                // the separate Worker CAS tests.
                let proof = recovered.unsent_release_receipt("intent").unwrap().unwrap();
                assert_eq!(proof.reservation_key, "central-key");
                recovered
                    .acknowledge_central_release("central-key", "worker-released")
                    .unwrap();
            }
            assert_eq!(
                recovered.intent("intent").unwrap().unwrap().state,
                IntentState::Rejected
            );
            assert!(!recovered.begin_post("intent", 1, 6).unwrap());
        }
        if phase == "F6" {
            assert_eq!(
                std::fs::read_to_string(&venue_log).unwrap().trim(),
                saved.client_order_id
            );
            // Recovery never calls the mock POST closure or adds a second
            // log record; the unacknowledged send remains MAY_HAVE_SENT.
            assert_eq!(
                std::fs::read_to_string(&venue_log).unwrap().lines().count(),
                1
            );
        } else {
            assert!(!venue_log.exists(), "{phase}: 0 venue POST expected");
        }
    }
}

#[test]
fn crash_before_after_begin_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(&p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
    assert!(s.begin_post("intent", 1, 4).unwrap());
    drop(s);
    let mut s = AccountStore::open_existing(&p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(s.recover_posting(5).unwrap(), 1);
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Unknown
    );
    assert!(!s.begin_post("intent", 1, 6).unwrap());
}
#[test]
fn duplicate_execute_one_post() {
    let mut s = store();
    let first = intent(&mut s);
    let second = intent(&mut s);
    assert_eq!(first.intent_id, second.intent_id);
    reserve(&mut s);
    reserve(&mut s);
    let mut posts = 0;
    for _ in 0..3 {
        if s.begin_post("intent", 1, 4).unwrap() {
            posts += 1;
        }
    }
    assert_eq!(posts, 1);
}
#[test]
fn stop_after_reserve_before_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.latch_stop("stop", "account", "USER_PAUSE", 4).unwrap();
    assert!(s.begin_post("intent", 1, 5).is_err());
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
}
#[test]
fn lost_response_after_post_returns_unknown() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    assert!(s.begin_post("intent", 1, 4).unwrap());
    s.record_outcome("intent", IntentState::Unknown, None, "timeout", 5)
        .unwrap();
    assert!(!s.begin_post("intent", 1, 6).unwrap());
    let state: String = s
        .connection
        .query_row("SELECT state FROM risk_receipts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(state, "UNKNOWN");
    s.record_outcome(
        "intent",
        IntentState::Filled,
        Some("order"),
        "private-readback",
        7,
    )
    .unwrap();
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Filled
    );
}
#[test]
fn expired_command_never_executes_later() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    assert!(s.begin_post("intent", 1, 100).is_err());
    assert!(s.begin_post("intent", 1, 101).is_err());
}
#[test]
fn corrupt_or_missing_db_never_reinitialized() {
    let missing = path();
    assert!(AccountStore::open_existing(&missing, "account", "installation").is_err());
    assert!(!missing.exists());
    let corrupt = path();
    std::fs::write(&corrupt, b"broken sqlite").unwrap();
    assert!(AccountStore::open_existing(&corrupt, "account", "installation").is_err());
    assert_eq!(std::fs::read(corrupt).unwrap(), b"broken sqlite");
}
#[test]
fn sql_money_overflow_rejected() {
    let mut s = store();
    let f = Fill {
        side: "buy".into(),
        fill_id: "fill".into(),
        instrument_id: "BTC".into(),
        trade_id: "trade".into(),
        exchange_order_id: "order".into(),
        intent_id: None,
        time: 3,
        qty: Decimal::MAX,
        price: Decimal::from(2),
        fee: Decimal::ZERO,
        currency: "USDT".into(),
    };
    assert!(
        s.ingest_page(&IngestPage {
            fills: vec![f],
            ..Default::default()
        })
        .is_err()
    );
}
#[test]
fn history_growth_does_not_expand_hot_read() {
    let mut s = store();
    intent(&mut s);
    let query = "EXPLAIN QUERY PLAN SELECT intent_id FROM intents WHERE state IN('PREPARED','RESERVED','POSTING','UNKNOWN','ACKNOWLEDGED','PARTIALLY_FILLED') ORDER BY state,updated_at,intent_id LIMIT 100";
    let mut q = s.connection.prepare(query).unwrap();
    let plans = q
        .query_map([], |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        plans.iter().any(|v| v.contains("idx_intents_active")),
        "{plans:?}"
    );
    assert_eq!(s.active_intents(100).unwrap().len(), 1);
}
#[test]
fn campaign_budget_survives_day_restart_and_profit() {
    let mut s = store();
    let e = FinanceEntry {
        entry_id: "loss".into(),
        economic_event_key: "closed-order-1".into(),
        component: "CAMPAIGN_LOSS_USD".into(),
        allocation_leg: "account".into(),
        amount: Decimal::from(3),
        currency: "USD".into(),
        instance_id: None,
        source_ref: "attested:conversion".into(),
        time: 5,
        reverses_entry_id: None,
    };
    s.ingest_page(&IngestPage {
        finance: vec![e.clone()],
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![e],
        ..Default::default()
    })
    .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![FinanceEntry {
            entry_id: "profit".into(),
            economic_event_key: "profit".into(),
            component: "REALIZED_PNL".into(),
            allocation_leg: "account".into(),
            amount: Decimal::from(50),
            currency: "USD".into(),
            instance_id: None,
            source_ref: "trade2".into(),
            time: 86_400_000,
            reverses_entry_id: None,
        }],
        ..Default::default()
    })
    .unwrap();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let s = AccountStore::open_existing(p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(s.policy().unwrap().4, Decimal::from(3));
}
#[test]
fn new_format_backup_restore_reconciles() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.begin_post("intent", 1, 4).unwrap();
    let backup = path();
    s.backup(&backup).unwrap();
    let mut restored =
        AccountStore::open_existing(&backup, &s.meta.account_key, &s.meta.installation_id).unwrap();
    assert_eq!(restored.recover_posting(5).unwrap(), 1);
    assert!(!restored.begin_post("intent", 1, 6).unwrap());
    restored
        .record_outcome(
            "intent",
            IntentState::Cancelled,
            Some("order"),
            "exchange+central-readback",
            7,
        )
        .unwrap();
    assert_eq!(
        restored.intent("intent").unwrap().unwrap().state,
        IntentState::Cancelled
    );
}
#[test]
fn disk_full_before_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.connection.execute_batch("CREATE TABLE fault_padding(bytes BLOB) STRICT; CREATE TEMP TRIGGER inject_full BEFORE UPDATE OF state ON intents WHEN NEW.state='POSTING' BEGIN INSERT INTO fault_padding VALUES(zeroblob(1000000)); END;").unwrap();
    let pages: i64 = s
        .connection
        .pragma_query_value(None, "page_count", |r| r.get(0))
        .unwrap();
    s.connection
        .pragma_update(None, "max_page_count", pages)
        .unwrap();
    let error = s.begin_post("intent", 1, 4).unwrap_err();
    assert!(
        matches!(error,StoreError::Sql(rusqlite::Error::SqliteFailure(e,_))if e.code==rusqlite::ErrorCode::DiskFull)
    );
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
}
#[test]
fn storage_failure_after_post() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.begin_post("intent", 1, 4).unwrap();
    s.connection
        .pragma_update(None, "query_only", "ON")
        .unwrap();
    assert!(
        s.record_outcome("intent", IntentState::Filled, Some("order"), "ack", 5)
            .is_err()
    );
    s.connection
        .pragma_update(None, "query_only", "OFF")
        .unwrap();
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Posting
    );
    assert!(!s.begin_post("intent", 1, 6).unwrap());
}
#[test]
fn daily_roll_does_not_clear_stop() {
    let mut s = store();
    s.latch_stop("drawdown", "account", "LATCHED_DRAWDOWN", 1)
        .unwrap();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &m.account_key, &m.installation_id).unwrap();
    assert_eq!(s.recover_posting(86_400_001).unwrap(), 0);
    assert_eq!(s.stops().unwrap()[0].2, "LATCHED_DRAWDOWN");
}
#[test]
fn external_fill_unattributed_not_zero_risk() {
    let mut s = store();
    let fill = Fill {
        side: "buy".into(),
        fill_id: "external".into(),
        instrument_id: "BTC".into(),
        trade_id: "t".into(),
        exchange_order_id: "o".into(),
        intent_id: None,
        time: 3,
        qty: Decimal::ONE,
        price: Decimal::from(100),
        fee: Decimal::new(-1, 2),
        currency: "USDT".into(),
    };
    s.ingest_attributed_fill(
        fill.clone(),
        FillEffect::Open {
            side: "long".into(),
        },
        None,
    )
    .unwrap();
    s.ingest_attributed_fill(
        fill,
        FillEffect::Open {
            side: "long".into(),
        },
        None,
    )
    .unwrap();
    let lots = s.active_lots("BTC").unwrap();
    assert_eq!(lots.len(), 1);
    assert!(lots[0].instance_id.is_none());
    assert_eq!(lots[0].remaining_qty, Decimal::ONE);
    assert_eq!(
        s.finance_totals(1, 10, None).unwrap()[&("USDT".into(), "FEE".into())],
        Decimal::new(-1, 2)
    );
}
#[test]
fn partial_fill_protection_tracks_size() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.begin_post("intent", 1, 4).unwrap();
    let p = ProtectionReceipt {
        intent_id: "intent".into(),
        algo_id: "algo".into(),
        side: "sell".into(),
        quantity: Decimal::new(4, 1),
        trigger_price: Decimal::from(95),
        trigger_basis: "mark".into(),
        verified_at: 5,
        source_digest: "authenticated".into(),
    };
    s.verify_protection(&p).unwrap();
    assert_eq!(
        s.protection("intent").unwrap().unwrap().quantity,
        Decimal::new(4, 1)
    );
    let wrong = ProtectionReceipt {
        side: "buy".into(),
        ..p.clone()
    };
    assert!(s.verify_protection(&wrong).is_err());
    s.verify_protection(&ProtectionReceipt {
        quantity: Decimal::ONE,
        ..p
    })
    .unwrap();
    assert_eq!(
        s.protection("intent").unwrap().unwrap().quantity,
        Decimal::ONE
    );
}
#[test]
fn policy_change_invalidates_unsent_plan() {
    let mut s = store();
    intent(&mut s);
    reserve(&mut s);
    s.apply_authority_policy(2, false, "campaign-20", Decimal::from(20))
        .unwrap();
    assert!(s.begin_post("intent", 1, 4).is_err());
    assert_eq!(
        s.intent("intent").unwrap().unwrap().state,
        IntentState::Reserved
    );
}
#[test]
fn initialize_retry_is_exact_and_never_reinitializes() {
    let s = store();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let fresh = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: "changed".into(),
        observed_at: 100,
        positions: 1,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(999))],
    };
    let s = AccountStore::initialize_or_inspect_exact(&p, m.clone(), &fresh).unwrap();
    assert_eq!(s.meta.initialized_at, 1);
    assert_eq!(s.policy().unwrap().2, "campaign-20");
    drop(s);
    let bad = AccountMeta {
        initialize_operation_id: "different-op".into(),
        ..m
    };
    assert!(AccountStore::initialize_or_inspect_exact(&p, bad, &fresh).is_err());
}
#[test]
fn duplicate_cancel_never_posts_twice() {
    let mut s = store();
    assert!(s.begin_operation("cancel", "request", "CANCEL", 1).unwrap());
    assert!(!s.begin_operation("cancel", "request", "CANCEL", 2).unwrap());
    assert!(
        s.begin_operation("cancel", "different", "CANCEL", 2)
            .is_err()
    );
    s.operation_receipt("cancel", "request", "CANCEL", "CANCELLED", 3)
        .unwrap();
    assert!(!s.begin_operation("cancel", "request", "CANCEL", 4).unwrap());
}
#[tokio::test]
async fn writer_queue_backpressure_and_account_isolation() {
    let s = store();
    let p = s.path.clone();
    let m = s.meta.clone();
    drop(s);
    let h = crate::AccountStoreHandle::open_existing(
        p.clone(),
        m.account_key.clone(),
        m.installation_id.clone(),
    )
    .await
    .unwrap();
    assert!(AccountStore::open_existing(p, &m.account_key, &m.installation_id).is_err());
    let (block_tx, block_rx) = std::sync::mpsc::channel();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let first = h.clone();
    let task = tokio::spawn(async move {
        first
            .call(
                std::time::Instant::now() + Duration::from_secs(5),
                move |_| {
                    let _ = ready_tx.send(());
                    block_rx.recv().unwrap();
                    Ok(())
                },
            )
            .await
    });
    ready_rx.await.unwrap();
    let mut jobs = Vec::new();
    for _ in 0..129 {
        let h = h.clone();
        jobs.push(tokio::spawn(async move {
            h.call(std::time::Instant::now() + Duration::from_secs(5), |_| {
                Ok(())
            })
            .await
        }));
    }
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    let mut other = store();
    intent(&mut other);
    assert_eq!(other.active_intents(10).unwrap().len(), 1);
    block_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    let mut rejected = 0;
    for job in jobs {
        if matches!(
            job.await.unwrap(),
            Err(StoreError::Invalid("ACCOUNT_STORAGE_BACKPRESSURE"))
        ) {
            rejected += 1;
        }
    }
    assert!(rejected > 0);
}

#[tokio::test]
async fn two_account_100_parallel_sql_readonly_isolation() {
    // Two distinct account keys and actor-owned databases. No exchange transport.
    let first = store();
    let second_meta = AccountMeta {
        account_key: "okx:demo:fixture-account-b".into(),
        installation_id: "fixture-installation-b".into(),
        initialize_operation_id: "fixture-connect-b".into(),
        ..first.meta.clone()
    };
    let opening = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: second_meta.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    let second = AccountStore::initialize_flat(path(), second_meta, &opening).unwrap();
    let a_key = first.meta.account_key.clone();
    let b_key = second.meta.account_key.clone();
    assert_ne!(a_key, b_key);
    let (a_path, a_meta) = (first.path.clone(), first.meta.clone());
    let (b_path, b_meta) = (second.path.clone(), second.meta.clone());
    drop(first);
    drop(second);
    let a = crate::AccountStoreHandle::open_existing(
        a_path,
        a_meta.account_key,
        a_meta.installation_id,
    )
    .await
    .unwrap();
    let b = crate::AccountStoreHandle::open_existing(
        b_path,
        b_meta.account_key,
        b_meta.installation_id,
    )
    .await
    .unwrap();
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();
    for index in 0..100 {
        let handle = if index % 2 == 0 { a.clone() } else { b.clone() };
        tasks.push(tokio::spawn(async move {
            let issued = std::time::Instant::now();
            let (key, size) = handle
                .call(std::time::Instant::now() + Duration::from_secs(15), |s| {
                    let summary = s.account_summary()?;
                    let bytes = serde_json::to_vec(&summary).unwrap().len();
                    Ok((s.meta.account_key.clone(), bytes))
                })
                .await
                .unwrap();
            (index, key, size, issued.elapsed().as_micros() as u64)
        }));
    }
    let mut latencies = Vec::new();
    let mut max_bytes = 0;
    for task in tasks {
        let (index, key, bytes, latency) = task.await.unwrap();
        assert_eq!(
            key.as_str(),
            if index % 2 == 0 {
                a_key.as_str()
            } else {
                b_key.as_str()
            }
        );
        assert!(bytes <= 12_288);
        max_bytes = max_bytes.max(bytes);
        latencies.push(latency);
    }
    latencies.sort_unstable();
    println!(
        "NATIVE_SQL_100_PARALLEL_JSON:{}",
        serde_json::json!({
            "type": "native_sql_actors_not_network",
            "os": std::env::consts::OS,
            "accounts": 2,
            "distinct_account_keys": true,
            "operations": 100,
            "real_exchange_posts": 0,
            "p50_us": latencies[49],
            "p95_us": latencies[94],
            "max_us": latencies[99],
            "elapsed_ms": start.elapsed().as_millis(),
            "response_bytes_max": max_bytes,
            "network_p95_measured": false
        })
    );
}

#[test]
fn strategy_create_sql_persists_receipt_replay_and_rejects_conflicts() {
    // Real native SQL business transition, not a fake Durable Object response.
    let mut s = store();
    let proposed = StrategyInstance {
        instance_id: "ema_398".into(),
        definition_id: "ema_cross_v1".into(),
        definition_version: "1".into(),
        parameters: serde_json::json!({
            "parameters": {"fast": 10, "slow": 30},
            "instruments": ["BTC-USDT-SWAP"],
            "timeframe_ms": 60000
        })
        .to_string(),
        enabled: false,
        policy_revision: 1,
        budget: Decimal::from(3),
        last_decision_key: None,
    };
    assert!(s.instance("ema_398").unwrap().is_none());

    let accepted = s
        .mutate_instance(
            "strategy-create-398",
            "body-create-398",
            "create",
            &proposed,
            None,
            100,
        )
        .unwrap();
    let persisted = s.instance("ema_398").unwrap().unwrap();
    assert_eq!(persisted.definition_id, "ema_cross_v1");
    assert!(persisted.definition_version.starts_with("1-"));
    assert!(!persisted.enabled);
    assert_eq!(persisted.policy_revision, 1);
    assert_eq!(persisted.budget, Decimal::from(3));
    let result: StrategyInstance = serde_json::from_str(&accepted.result_ref).unwrap();
    assert_eq!(result.instance_id, persisted.instance_id);
    assert_eq!(result.definition_version, persisted.definition_version);
    assert_eq!(result.parameters, persisted.parameters);
    let definition = s.strategy_definition("ema_398").unwrap();
    assert_eq!(definition.parameters, persisted.parameters);
    assert_eq!(definition.version, persisted.definition_version);
    assert_eq!(
        s.strategy_status("ema_398", 101).unwrap()["instance"]["enabled"],
        false
    );
    let allocation_count: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM allocations WHERE instance_id='ema_398'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(allocation_count, 1);

    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    let replay = recovered
        .mutate_instance(
            "strategy-create-398",
            "body-create-398",
            "create",
            &proposed,
            None,
            102,
        )
        .unwrap();
    assert_eq!(replay.result_ref, accepted.result_ref);
    assert!(matches!(
        recovered.mutate_instance(
            "strategy-create-398",
            "changed-body-398",
            "create",
            &proposed,
            None,
            103
        ),
        Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"))
    ));
    assert!(matches!(
        recovered.mutate_instance(
            "strategy-create-other-key-398",
            "body-create-398",
            "create",
            &proposed,
            None,
            104
        ),
        Err(StoreError::Invalid("INSTANCE_ALREADY_EXISTS"))
    ));
    let saved = recovered.instance("ema_398").unwrap().unwrap();
    assert_eq!(saved.policy_revision, 1);
    assert_eq!(saved.budget, Decimal::from(3));
    let count: i64 = recovered
        .connection
        .query_row(
            "SELECT COUNT(*) FROM operation_receipts WHERE kind='STRATEGY_create' AND result_ref LIKE '%ema_398%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let allocations: i64 = recovered
        .connection
        .query_row(
            "SELECT COUNT(*) FROM allocations WHERE instance_id='ema_398'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(allocations, 1);
}

#[test]
fn central_policy_revisions_persist_in_sql_and_isolate_accounts() {
    // Synthetic central-owner CAS outcomes are applied to two independent
    // account-local SQLite writers. No live governance is changed.
    let mut first = store();
    let second_meta = AccountMeta {
        account_key: "okx:demo:fixture-account-b".into(),
        installation_id: "fixture-installation-b".into(),
        initialize_operation_id: "fixture-connect-b".into(),
        ..first.meta.clone()
    };
    let opening = OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: second_meta.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    let mut second = AccountStore::initialize_flat(path(), second_meta, &opening).unwrap();
    second
        .apply_authority_policy(1, false, "campaign-20", Decimal::from(20))
        .unwrap();
    assert_ne!(first.meta.account_key, second.meta.account_key);
    assert_eq!(first.policy().unwrap().0, 1);
    assert_eq!(second.policy().unwrap().0, 1);

    first
        .apply_authority_policy(2, true, "campaign-20", Decimal::from(20))
        .unwrap();
    assert_eq!(first.policy().unwrap().0, 2);
    assert!(first.policy().unwrap().1);
    assert_eq!(first.account_summary().unwrap()["paused"], true);
    assert!(matches!(
        first.apply_authority_policy(2, false, "campaign-20", Decimal::from(20)),
        Err(StoreError::Invalid("POLICY_REVISION_CONFLICT"))
    ));
    assert!(matches!(
        first.apply_authority_policy(1, false, "campaign-20", Decimal::from(20)),
        Err(StoreError::Invalid("POLICY_REVISION_CONFLICT"))
    ));
    first
        .apply_authority_policy(3, false, "campaign-20", Decimal::from(10))
        .unwrap();
    let policy = first.policy().unwrap();
    assert_eq!(policy.0, 3);
    assert!(!policy.1);
    assert_eq!(policy.3, Decimal::from(10));
    assert_eq!(
        first.account_summary().unwrap()["campaign"]["limit_usd"],
        "10"
    );
    assert_eq!(second.policy().unwrap().0, 1);
    assert_eq!(second.policy().unwrap().3, Decimal::from(20));

    let path = first.path.clone();
    let meta = first.meta.clone();
    drop(first);
    let recovered =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(recovered.policy().unwrap().0, 3);
    assert!(!recovered.policy().unwrap().1);
    assert_eq!(recovered.policy().unwrap().3, Decimal::from(10));
    assert_eq!(second.policy().unwrap().0, 1);
}

/// One CI execution passes the actual committed Worker mcpApi CAS history to the
/// native SQLite policy owner. Fake identities only; no live account or WSS claim.
#[test]
#[ignore = "requires OKX_A2_TRACE_PATH generated by the actual Worker composition"]
fn central_worker_cas_trace_native_sql_readback_and_restart() {
    let trace_path = std::env::var("OKX_A2_TRACE_PATH")
        .expect("actual Worker acceptance run must publish OKX_A2_TRACE_PATH");
    let document: serde_json::Value =
        serde_json::from_slice(&std::fs::read(trace_path).unwrap()).unwrap();
    assert_eq!(document["schema"], "okx.a2.worker-sql-cas-trace/v1");
    assert_eq!(
        document["source"],
        "mcpApi.fetch+account_governance_store.transaction"
    );
    let events = document["events"].as_array().unwrap();
    let stages = [
        ("connect", 1_i64, false, "1"),
        ("pause", 2, true, "1"),
        ("stale_resume_rejected", 2, true, "1"),
        ("resume", 3, false, "1"),
        ("set_limits", 4, false, "0.5"),
        ("stale_limits_rejected", 4, false, "0.5"),
        ("set_limits_replay", 4, false, "0.5"),
        ("final", 4, false, "0.5"),
    ];
    assert_eq!(events.len(), stages.len());
    assert_eq!(
        events
            .iter()
            .map(|v| v["frame_count"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 3, 4, 5, 6, 6, 7, 7],
        "stale governance CAS must emit no WSS frame"
    );
    let account_key = "okx:demo:".to_string() + &"a".repeat(64);
    let meta = AccountMeta {
        initialize_operation_id: "fixture-worker-connect".into(),
        release_id: "fixture".into(),
        account_key: account_key.clone(),
        installation_id: "installation_test".into(),
        initialized_at: 1,
        opening_source: "synthetic-flat".into(),
    };
    let flat = || OpeningSnapshot {
        opening_equity_usd: Decimal::from(100),
        source_ref: "synthetic-flat".into(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(100))],
    };
    let mut first = AccountStore::initialize_flat(path(), meta.clone(), &flat()).unwrap();
    let other = AccountMeta {
        account_key: "okx:demo:synthetic-other-account".into(),
        installation_id: "independent-account".into(),
        ..meta.clone()
    };
    let mut second = AccountStore::initialize_flat(path(), other, &flat()).unwrap();
    second
        .apply_authority_policy(1, false, "unrelated-campaign", Decimal::from(20))
        .unwrap();
    for (event, (stage, revision, paused, per_order)) in events.iter().zip(stages) {
        assert_eq!(event["stage"].as_str(), Some(stage));
        // Use the exact outgoing Durable Object policy_snapshot, not a
        // copied central readback. The Rust protocol crate validates the same
        // frame independently before the native SQL owner reads its state.
        let frame = &event["frame"];
        assert_eq!(frame["type"], "policy_snapshot");
        assert_eq!(frame["schema"], "okx.direct-transport.frame/v2");
        assert_eq!(frame["session_id"], "session_test_012345");
        assert_eq!(frame["connection_generation"], 4);
        assert_eq!(frame["snapshot"]["session_generation"], 4);
        assert!(
            frame["snapshot"]["issued_at_ms"]
                .as_u64()
                .is_some_and(|v| v > 0)
        );
        let state = &frame["snapshot"]["state"];
        assert_eq!(*state, event["governance"]);
        assert_eq!(state["schema"], "okx.governance/v3");
        assert_eq!(state["installation_id"], meta.installation_id);
        assert_eq!(state["revision"].as_i64(), Some(revision));
        assert_eq!(state["campaign_loss_limit_usd"], "10");
        let accounts = state["accounts"].as_array().unwrap();
        assert_eq!(accounts.len(), 1);
        let row = &accounts[0];
        assert_eq!(row["account_key"], account_key);
        assert_eq!(row["id"], "fixture_a");
        assert_eq!(row["identity_verified"], true);
        assert_eq!(row["enabled"], true);
        assert_eq!(row["policy_revision"].as_i64(), Some(revision));
        assert_eq!(row["explicit_pause"].as_bool(), Some(paused));
        assert_eq!(
            row["limits"]["max_order_loss_usdt"].as_str(),
            Some(per_order)
        );
        let campaign = state["campaign_id"].as_str().unwrap();
        // This is the same AccountStore::apply_authority_policy method used by
        // TradeService on a delivered governance frame, not a shadow ledger.
        first
            .apply_authority_policy(revision, paused, campaign, Decimal::from(10))
            .unwrap();
        let saved = first.policy().unwrap();
        assert_eq!(saved.0, revision, "{stage} revision");
        assert_eq!(saved.1, paused, "{stage} pause");
        assert_eq!(saved.2, campaign, "{stage} campaign");
        assert_eq!(saved.3, Decimal::from(10), "{stage} ceiling");
        assert_eq!(
            first.account_summary().unwrap()["paused"].as_bool(),
            Some(paused)
        );
        assert_eq!(second.policy().unwrap().0, 1);
        assert_eq!(second.policy().unwrap().3, Decimal::from(20));
    }
    let (revision, paused, campaign, ceiling, _) = first.policy().unwrap();
    assert_eq!(revision, 4);
    assert!(!paused);
    assert!(matches!(
        first.apply_authority_policy(revision, true, &campaign, ceiling),
        Err(StoreError::Invalid("POLICY_REVISION_CONFLICT"))
    ));
    assert!(matches!(
        first.apply_authority_policy(revision - 1, paused, &campaign, ceiling),
        Err(StoreError::Invalid("POLICY_REVISION_CONFLICT"))
    ));
    assert!(matches!(
        first.apply_authority_policy(revision + 1, paused, "different-campaign", ceiling),
        Err(StoreError::Invalid("CAMPAIGN_ID_IMMUTABLE"))
    ));
    let file_path = first.path.clone();
    drop(first);
    let recovered =
        AccountStore::open_existing(file_path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(recovered.policy().unwrap().0, 4);
    assert_eq!(recovered.policy().unwrap().3, Decimal::from(10));
    assert!(!recovered.policy().unwrap().1);
    assert_eq!(second.policy().unwrap().0, 1);
    println!(
        "a2_actual_do_policy_frame_to_native_sql PASS 8 committed stages and 7 WSS policy frames; central ownership, replay, stale rejection, native SQL restart/isolation, 0 real exchange POST; not physical WSS"
    );
}

#[test]
fn immutable_strategy_versions_bind_old_intent_and_cas_start_stop() {
    let mut s = store();
    let old = intent(&mut s);
    reserve(&mut s);
    let mut instance = s.instance("manual").unwrap().unwrap();
    let original = s.strategy_definition("manual").unwrap();
    instance.parameters = "{\"kind\":\"manual\",\"threshold\":2}".into();
    instance.policy_revision = 2;
    let receipt = s
        .mutate_instance(
            "update-version",
            "hash-update",
            "update",
            &instance,
            Some(1),
            4,
        )
        .unwrap();
    let updated: StrategyInstance = serde_json::from_str(&receipt.result_ref).unwrap();
    assert_ne!(updated.definition_version, original.version);
    assert_eq!(
        s.intent(&old.intent_id)
            .unwrap()
            .unwrap()
            .plan
            .strategy_definition_version,
        original.version
    );
    assert_eq!(
        s.definition_version(&original.definition_id, &original.version)
            .unwrap()
            .unwrap(),
        original
    );
    assert!(matches!(
        s.begin_post(&old.intent_id, 1, 5),
        Err(StoreError::Invalid("STRATEGY_VERSION_CHANGED"))
    ));
    assert!(
        s.connection
            .execute(
                "UPDATE strategy_definition_versions SET parameters='{}'",
                []
            )
            .is_err()
    );
    let mut started = updated.clone();
    started.policy_revision = 3;
    started.enabled = true;
    let start = s
        .mutate_instance("start-version", "hash-start", "start", &started, Some(2), 6)
        .unwrap();
    assert_eq!(s.instance("manual").unwrap().unwrap().policy_revision, 3);
    assert!(
        s.mutate_instance(
            "stale-update",
            "hash-stale",
            "update",
            &instance,
            Some(2),
            7
        )
        .is_err()
    );
    assert_eq!(
        s.mutate_instance("start-version", "hash-start", "start", &started, Some(2), 7)
            .unwrap()
            .result_ref,
        start.result_ref
    );
    assert!(
        s.claim_strategy_decision("manual", "same-bar", "input", 1, 7)
            .unwrap()
    );
    let mut changed = s.instance("manual").unwrap().unwrap();
    changed.parameters = "{\"kind\":\"manual\",\"threshold\":3}".into();
    changed.policy_revision = 4;
    s.mutate_instance(
        "update-second",
        "hash-second",
        "update",
        &changed,
        Some(3),
        8,
    )
    .unwrap();
    assert!(
        s.claim_strategy_decision("manual", "same-bar", "input", 1, 9)
            .unwrap()
    );
    let mut stopped = s.instance("manual").unwrap().unwrap();
    stopped.policy_revision = 5;
    s.mutate_instance("stop-version", "hash-stop", "stop", &stopped, Some(4), 10)
        .unwrap();
    assert_eq!(s.instance("manual").unwrap().unwrap().policy_revision, 5);
    assert!(!s.instance("manual").unwrap().unwrap().enabled);
    let status = s.strategy_status("manual", 10).unwrap();
    assert_eq!(status["limits"]["policy_revision"], 5);
    assert!(status["positions"].as_array().unwrap().is_empty());
}

#[test]
fn strategy_status_bounds_owned_positions_and_uses_instance_index() {
    let s = store();
    for n in 0..51 {
        s.connection.execute("INSERT INTO position_lots VALUES(?1,'manual',NULL,NULL,'BTC-USDT-SWAP','long','1','100',?2)",params![format!("lot-{n:03}"),n]).unwrap();
    }
    let status = s.strategy_status("manual", 60).unwrap();
    assert_eq!(status["positions"].as_array().unwrap().len(), 50);
    assert_eq!(status["positions_partial"], true);
    assert_eq!(status["positions"][0]["instance_id"], "manual");
    let plan:String=s.connection.query_row("EXPLAIN QUERY PLAN SELECT lot_id FROM position_lots WHERE instance_id='manual' AND remaining_qty<>'0' ORDER BY instrument_id,side,opened_at,lot_id LIMIT 51",[],|r|r.get(3)).unwrap();
    assert!(plan.contains("idx_lots_instance_active"));
}

#[test]
fn recovery_history_age_is_separate_from_current_evidence_freshness() {
    let mut s = store();
    let now = 20000;
    for (source, part) in [
        ("fills", "SPOT"),
        ("fills", "SWAP"),
        ("fills", "FUTURES"),
        ("fills", "OPTION"),
        ("bills", "account"),
        ("recovery_audit", "account"),
        ("orders", "account"),
        ("account_snapshot", "account"),
    ] {
        let watermark = if matches!(source, "orders" | "account_snapshot") {
            now
        } else {
            10000
        };
        s.ingest_page(&IngestPage {
            cursor: Some(SourceCursor {
                source: source.into(),
                stream_partition: part.into(),
                cursor: String::new(),
                watermark,
                generation: 1,
                coverage_gaps: String::new(),
            }),
            ..Default::default()
        })
        .unwrap();
    }
    assert!(s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET watermark=9999 WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(!s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET watermark=10000,generation=2 WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(!s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET generation=1,coverage_gaps='missing' WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(!s.recovery_snapshot(now).unwrap().coverage_complete);
    s.connection
        .execute(
            "UPDATE source_cursors SET coverage_gaps='' WHERE source='bills'",
            [],
        )
        .unwrap();
    assert!(s.recovery_snapshot(now).unwrap().coverage_complete);
    assert!(!s.recovery_snapshot(now + 5001).unwrap().coverage_complete);
}

#[test]
fn strategy_crash_pending_decision_to_intent() {
    let mut s = store();
    owned_fill(&mut s, "a", "fixture-filled", 100, "long", 3);
    let version = s.strategy_definition("a").unwrap();
    assert!(
        s.claim_strategy_decision("a", "bar-next", "inputs-next", 1, 7)
            .unwrap()
    );
    // Simulate a process crash before the plan and intent were committed.
    let p = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &meta.account_key, &meta.installation_id).unwrap();
    assert!(
        s.claim_strategy_decision("a", "bar-next", "inputs-next", 1, 8)
            .unwrap()
    );
    assert!(
        s.instances()
            .unwrap()
            .iter()
            .find(|i| i.instance_id == "a")
            .unwrap()
            .last_decision_key
            .as_deref()
            != Some("bar-next")
    );
    let plan = IntentPlan {
        strategy_definition_id: version.definition_id.clone(),
        strategy_definition_version: version.version.clone(),
        strategy_parameters_digest: version.parameters_digest.clone(),
        operation: "open".into(),
        side: "buy".into(),
        position_side: "long".into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(100),
        stop_price: Some(Decimal::from(90)),
        strategy_instance: "a".into(),
        planned_loss_usd: Decimal::ONE,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::ONE,
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: format!("strategy:a:{}:bar-next", version.version),
        instance_id: Some("a".into()),
        decision_at: 9,
        input_digest: "inputs-next".into(),
        policy_revision: 1,
        code_version: "strategy-bar/v1".into(),
    };
    let intent = NewIntent {
        plan,
        intent_id: "f07-next".into(),
        idempotency_key: "bar-next".into(),
        request_hash: "f07-body".into(),
        client_order_id: "f07-client".into(),
        deadline: 1000,
        plan_hash: "f07-plan".into(),
        policy_revision: 1,
        risk_increase: true,
        instrument_id: "BTC".into(),
    };
    s.prepare(&decision, &intent).unwrap();
    let decisions: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM decisions WHERE decision_id=?1",
            [&decision.decision_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(decisions, 1);
    let bound: String = s
        .connection
        .query_row(
            "SELECT decision_id FROM intents WHERE idempotency_key='bar-next'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(bound, decision.decision_id);
    assert!(
        !s.claim_strategy_decision("a", "bar-next", "inputs-next", 1, 10)
            .unwrap()
    );
    assert_eq!(
        s.instances()
            .unwrap()
            .iter()
            .find(|i| i.instance_id == "a")
            .unwrap()
            .last_decision_key
            .as_deref(),
        Some("bar-next")
    );
}

#[test]
fn strategy_policy_retry_keeps_immutable_lineage_and_one_intent() {
    let mut s = store();
    owned_fill(&mut s, "a", "fixture-filled", 100, "long", 3);
    let definition = s.strategy_definition("a").unwrap();
    let base = format!("strategy:a:{}:bar-revision", definition.version);
    assert_eq!(
        s.claim_strategy_decision_with_id("a", "bar-revision", "inputs", 1, 7)
            .unwrap(),
        Some(base.clone())
    );
    let p = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut s = AccountStore::open_existing(p, &meta.account_key, &meta.installation_id).unwrap();
    // Revision changes after durable claim but before any intent or POST.
    let updated = s
        .claim_strategy_decision_with_id("a", "bar-revision", "inputs", 2, 8)
        .unwrap()
        .unwrap();
    assert_ne!(updated, base);
    assert!(updated.contains(":policy:2"));
    assert_eq!(
        s.claim_strategy_decision_with_id("a", "bar-revision", "inputs", 2, 9)
            .unwrap(),
        Some(updated.clone())
    );
    let old: (String, i64) = s
        .connection
        .query_row(
            "SELECT input_digest,policy_revision FROM decisions WHERE decision_id=?1",
            [&base],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(old, ("inputs".into(), 1));
    assert!(
        s.connection
            .execute(
                "UPDATE decisions SET policy_revision=2 WHERE decision_id=?1",
                [&base],
            )
            .is_err()
    );
    let successor: String = s.connection.query_row(
        "SELECT result_ref FROM operation_receipts WHERE operation_id=?1 AND kind='strategy_policy_retry'",
        [format!("strategy-policy-retry:{base}")],
        |r| r.get(0),
    ).unwrap();
    assert_eq!(successor, updated);
    // A third central revision appends again, never mutating either ancestor.
    let final_id = s
        .claim_strategy_decision_with_id("a", "bar-revision", "inputs", 3, 10)
        .unwrap()
        .unwrap();
    assert_ne!(final_id, updated);
    let plan = IntentPlan {
        strategy_definition_id: definition.definition_id,
        strategy_definition_version: definition.version,
        strategy_parameters_digest: definition.parameters_digest,
        operation: "open".into(),
        side: "buy".into(),
        position_side: "long".into(),
        quantity: Decimal::ONE,
        limit_price: Decimal::from(100),
        stop_price: Some(Decimal::from(90)),
        strategy_instance: "a".into(),
        planned_loss_usd: Decimal::ONE,
        source_generation: 1,
        private_source_generation: 1,
        contract_value: Decimal::ONE,
        inverse: false,
        settlement_currency: "USDT".into(),
    };
    let decision = Decision {
        decision_id: final_id.clone(),
        instance_id: Some("a".into()),
        decision_at: 11,
        input_digest: "inputs".into(),
        policy_revision: 3,
        code_version: "strategy-bar/policy-retry-v1".into(),
    };
    let intent = NewIntent {
        plan,
        intent_id: "f07-revision".into(),
        idempotency_key: "bar-revision".into(),
        request_hash: "f07-revision-body".into(),
        client_order_id: "f07-revision-client".into(),
        deadline: 1000,
        plan_hash: "f07-revision-plan".into(),
        policy_revision: 3,
        risk_increase: true,
        instrument_id: "BTC".into(),
    };
    s.prepare(&decision, &intent).unwrap();
    let count: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM intents WHERE idempotency_key='bar-revision'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert!(
        !s.claim_strategy_decision("a", "bar-revision", "inputs", 4, 12)
            .unwrap()
    );
    assert_eq!(
        s.connection
            .query_row(
                "SELECT decision_id FROM intents WHERE idempotency_key='bar-revision'",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap(),
        final_id
    );
    // The SQL owner has prepared exactly one intent and no authorized POST.
    assert!(
        s.connection
            .query_row(
                "SELECT begin_post_at FROM intents WHERE idempotency_key='bar-revision'",
                [],
                |r| r.get::<_, Option<i64>>(0),
            )
            .unwrap()
            .is_none()
    );
    let decisions: i64 = s
        .connection
        .query_row(
            "SELECT count(*) FROM decisions WHERE decision_id=?1 OR
         (decision_id >= (?1 || ':retry:') AND decision_id < (?1 || ':retry;'))",
            [&base],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(decisions, 3);
}

#[test]
fn manual_preview_never_creates_instance_or_changes_sql() {
    let mut s = store();
    let changes = |s: &AccountStore| -> i64 {
        s.connection
            .query_row("SELECT total_changes()", [], |r| r.get(0))
            .unwrap()
    };
    // The canonical store already seeds manual with a foreign-key owner.
    // Preview must not modify this existing account's SQL, policy or STOP.
    assert!(s.instance("manual").unwrap().is_some());
    let before = changes(&s);
    let policy = s.policy().unwrap();
    let stops = s.stops().unwrap();
    let planned = s.manual_definition_preview().unwrap();
    assert_eq!(changes(&s), before);
    assert_eq!(s.policy().unwrap(), policy);
    assert_eq!(s.stops().unwrap(), stops);
    assert!(s.instance("manual").unwrap().is_some());
    let created = s.ensure_manual_instance(1234).unwrap();
    assert_eq!(planned.version, created.version);
    assert_eq!(planned.parameters_digest, created.parameters_digest);
    assert_eq!(changes(&s), before);
    let again = s.manual_definition_preview().unwrap();
    assert_eq!(again.version, created.version);
    assert_eq!(changes(&s), before);
}
/// One real SQL owner and three concurrent decision submissions. "chatgpt" is
/// only a synthetic *unverified external* label: this test does NOT assert
/// authenticated chat approval or a live MCP/venue execution.
#[tokio::test]
async fn three_decision_sources_share_sql_owner_and_fail_closed() {
    let mut seeded = store();
    owned_fill(&mut seeded, "autonomous", "autonomous-seed", 100, "long", 3);
    let strategy_decision = seeded
        .claim_strategy_decision_with_id("autonomous", "bar-10", "market-inputs", 1, 7)
        .unwrap()
        .unwrap();
    let path = seeded.path.clone();
    let meta = seeded.meta.clone();
    drop(seeded);
    let sql = crate::AccountStoreHandle::open_existing(
        path,
        meta.account_key.clone(),
        meta.installation_id.clone(),
    )
    .await
    .unwrap();

    let mut pending = Vec::new();
    for (origin, instance, claim) in [
        ("human", "manual", None),
        ("chatgpt", "manual", None),
        ("autonomous", "autonomous", Some(strategy_decision.as_str())),
    ] {
        let sql = sql.clone();
        let origin = origin.to_owned();
        let instance = instance.to_owned();
        let claim = claim.map(str::to_owned);
        pending.push(tokio::spawn(async move {
            sql.call(
                std::time::Instant::now() + Duration::from_secs(5),
                move |s| {
                    let definition = s.strategy_definition(&instance)?;
                    let id = format!("three_sources_{origin}");
                    let decision = Decision {
                        decision_id: claim.unwrap_or_else(|| format!("external_{origin}")),
                        instance_id: Some(instance.clone()),
                        decision_at: 9,
                        input_digest: if origin == "autonomous" {
                            "market-inputs".into()
                        } else {
                            "unverified-external-request".into()
                        },
                        policy_revision: 1,
                        code_version: "three-sources-fixture".into(),
                    };
                    let request = NewIntent {
                        plan: IntentPlan {
                            strategy_definition_id: definition.definition_id,
                            strategy_definition_version: definition.version,
                            strategy_parameters_digest: definition.parameters_digest,
                            contract_value: Decimal::ONE,
                            inverse: false,
                            settlement_currency: "USDT".into(),
                            position_side: "long".into(),
                            private_source_generation: 1,
                            operation: "open".into(),
                            side: "buy".into(),
                            quantity: Decimal::ONE,
                            limit_price: Decimal::from(100),
                            stop_price: Some(Decimal::from(90)),
                            strategy_instance: instance.clone(),
                            planned_loss_usd: Decimal::ONE,
                            source_generation: 1,
                        },
                        intent_id: id.clone(),
                        idempotency_key: format!("idempotency_{origin}"),
                        request_hash: format!("hash_{origin}"),
                        client_order_id: format!("client_{origin}"),
                        deadline: 1000,
                        plan_hash: format!("plan_{origin}"),
                        policy_revision: 1,
                        risk_increase: true,
                        instrument_id: "BTC-USDT-SWAP".into(),
                    };
                    assert_eq!(s.prepare(&decision, &request)?.intent_id, id);
                    // Same request cannot produce a second SQL intent.
                    assert_eq!(s.prepare(&decision, &request)?.intent_id, id);
                    s.reserve(
                        &CentralReceipt {
                            reservation_key: format!("synthetic_risk_{origin}"),
                            account_key: s.meta.account_key.clone(),
                            installation_id: s.meta.installation_id.clone(),
                            instance_id: Some(instance),
                            intent_id: id.clone(),
                            plan_hash: request.plan_hash,
                            policy_revision: 1,
                            receipt_hash: format!("synthetic_receipt_{origin}"),
                            requested_loss_usd: Decimal::ONE,
                            conversion_digest: "synthetic-fx".into(),
                        },
                        11,
                    )?;
                    assert_eq!(s.intent(&id)?.unwrap().state, IntentState::Reserved);
                    Ok(id)
                },
            )
            .await
        }));
    }
    let mut ids = Vec::new();
    for task in pending {
        ids.push(task.await.unwrap().unwrap());
    }
    assert_eq!(ids.len(), 3);
    let sql_view = sql.clone();
    sql_view
        .call(
            std::time::Instant::now() + Duration::from_secs(5),
            move |s| {
                let count: i64 = s.connection.query_row(
                    "SELECT count(*) FROM intents WHERE intent_id LIKE 'three_sources_%'",
                    [],
                    |r| r.get(0),
                )?;
                assert_eq!(count, 3);
                // One central revision applies to every source, regardless
                // of synthetic external labels or internal strategy claims.
                s.apply_authority_policy(2, true, "campaign-20", Decimal::from(20))?;
                for id in &ids {
                    assert!(matches!(
                        s.begin_post(id, 1, 20),
                        Err(StoreError::Invalid("POLICY_CHANGED"))
                    ));
                    let stored = s.intent(id)?.unwrap();
                    assert_eq!(stored.state, IntentState::Reserved);
                    let began: Option<i64> = s.connection.query_row(
                        "SELECT begin_post_at FROM intents WHERE intent_id=?1",
                        [id],
                        |r| r.get(0),
                    )?;
                    assert!(began.is_none(), "no source may cross the POST fence");
                }
                Ok(())
            },
        )
        .await
        .unwrap();
}

// K6 V-02: one SQLite owner observes two partial entry fills, a full exit,
// fill/bill echoes and funding, including a reopen. No synthetic PnL owner.
#[test]
fn k6_v02_sqlite_partial_fills_close_fees_funding_and_replay() {
    let mut s = store();
    let definition = s.ensure_manual_instance(1).unwrap();
    let q = Decimal::new(9, 2);
    for (name, operation, side, risk_increase) in [
        ("v02-open", "open", "buy", true),
        ("v02-close", "close", "sell", false),
    ] {
        let decision = Decision {
            decision_id: format!("decision:{name}"),
            instance_id: Some("manual".into()),
            decision_at: 2,
            input_digest: format!("input:{name}"),
            policy_revision: 1,
            code_version: "fixture-v02".into(),
        };
        let request = NewIntent {
            plan: IntentPlan {
                strategy_definition_id: definition.definition_id.clone(),
                strategy_definition_version: definition.version.clone(),
                strategy_parameters_digest: definition.parameters_digest.clone(),
                operation: operation.into(),
                side: side.into(),
                position_side: "long".into(),
                quantity: q,
                limit_price: Decimal::from(if risk_increase { 101 } else { 102 }),
                stop_price: if risk_increase {
                    Some(Decimal::from(95))
                } else {
                    None
                },
                strategy_instance: "manual".into(),
                planned_loss_usd: if risk_increase {
                    Decimal::ONE
                } else {
                    Decimal::ZERO
                },
                source_generation: 1,
                private_source_generation: 1,
                contract_value: Decimal::ONE,
                inverse: false,
                settlement_currency: "USDT".into(),
            },
            intent_id: name.into(),
            idempotency_key: name.into(),
            request_hash: format!("hash:{name}"),
            client_order_id: format!("client:{name}"),
            deadline: 100,
            plan_hash: format!("plan:{name}"),
            policy_revision: 1,
            risk_increase,
            instrument_id: "BTC-USDT-SWAP".into(),
        };
        s.prepare(&decision, &request).unwrap();
    }
    let open_a = Fill {
        fill_id: "v02-fill-a".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        trade_id: "v02-trade-a".into(),
        exchange_order_id: "v02-order-open".into(),
        intent_id: Some("v02-open".into()),
        time: 3,
        qty: Decimal::new(4, 2),
        price: Decimal::from(100),
        fee: Decimal::new(-4, 3),
        currency: "USDT".into(),
        side: "buy".into(),
    };
    let open_b = Fill {
        fill_id: "v02-fill-b".into(),
        trade_id: "v02-trade-b".into(),
        time: 4,
        qty: Decimal::new(5, 2),
        price: Decimal::from(101),
        fee: Decimal::new(-505, 5),
        ..open_a.clone()
    };
    let exit = Fill {
        fill_id: "v02-fill-exit".into(),
        trade_id: "v02-trade-exit".into(),
        exchange_order_id: "v02-order-close".into(),
        intent_id: Some("v02-close".into()),
        time: 6,
        qty: q,
        price: Decimal::from(102),
        fee: Decimal::new(-918, 5),
        side: "sell".into(),
        ..open_a.clone()
    };
    for fill in [&open_a, &open_b] {
        s.ingest_attributed_fill(
            fill.clone(),
            FillEffect::Open {
                side: "long".into(),
            },
            None,
        )
        .unwrap();
    }
    assert_eq!(
        s.active_lots("BTC-USDT-SWAP")
            .unwrap()
            .iter()
            .map(|v| v.remaining_qty)
            .sum::<Decimal>(),
        q,
    );
    // The bill and fill point at the same economic event: exactly one cost.
    let bill = FinanceEntry {
        entry_id: "v02-bill-a".into(),
        economic_event_key: "fill:BTC-USDT-SWAP:v02-trade-a:v02-order-open".into(),
        component: "FEE".into(),
        allocation_leg: "account".into(),
        amount: Decimal::new(-4, 3),
        currency: "USDT".into(),
        instance_id: Some("manual".into()),
        source_ref: "bill:v02-bill-a".into(),
        time: 3,
        reverses_entry_id: None,
    };
    let funding = FinanceEntry {
        entry_id: "v02-funding".into(),
        economic_event_key: "funding:v02".into(),
        component: "FUNDING".into(),
        allocation_leg: "account".into(),
        amount: Decimal::new(-2, 3),
        currency: "USDT".into(),
        instance_id: Some("manual".into()),
        source_ref: "exchange:funding:v02".into(),
        time: 5,
        reverses_entry_id: None,
    };
    s.ingest_page(&IngestPage {
        finance: vec![bill.clone(), funding.clone()],
        ..Default::default()
    })
    .unwrap();
    s.ingest_attributed_fill(exit.clone(), FillEffect::Reduce, None)
        .unwrap();
    let path = s.path.clone();
    let account = s.meta.clone();
    drop(s);
    let mut s =
        AccountStore::open_existing(path, &account.account_key, &account.installation_id).unwrap();
    for fill in [&open_a, &open_b] {
        s.ingest_attributed_fill(
            fill.clone(),
            FillEffect::Open {
                side: "long".into(),
            },
            None,
        )
        .unwrap();
    }
    s.ingest_attributed_fill(exit, FillEffect::Reduce, None)
        .unwrap();
    s.ingest_page(&IngestPage {
        finance: vec![bill, funding],
        ..Default::default()
    })
    .unwrap();
    assert!(
        s.active_lots("BTC-USDT-SWAP").unwrap().is_empty(),
        "V-02 must finish FLAT, never duplicate a reduced lot"
    );
    let account = s.finance_totals(1, 10, None).unwrap();
    let manual = s.finance_totals(1, 10, Some("manual")).unwrap();
    for (component, expected) in [
        ("REALIZED_PNL", Decimal::new(13, 2)),
        ("FEE", Decimal::new(-1823, 5)),
        ("FUNDING", Decimal::new(-2, 3)),
    ] {
        let key = ("USDT".to_owned(), component.to_owned());
        assert_eq!(account.get(&key), Some(&expected), "account {component}");
        assert_eq!(manual.get(&key), Some(&expected), "manual {component}");
    }
    let net = account
        .iter()
        .filter(|((ccy, component), _)| {
            ccy == "USDT" && ["REALIZED_PNL", "FEE", "FUNDING"].contains(&component.as_str())
        })
        .map(|(_, amount)| *amount)
        .sum::<Decimal>();
    assert_eq!(net, Decimal::new(109770, 6));
    s.integrity_check().unwrap();
}

#[test]
fn p02_manual_and_account_nav_do_not_read_strategy_revision_sources() {
    use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

    // The test authorizer is scoped to the isolated T0 fixture, not to the
    // installed account database. Deny exactly the as-of revision inputs.
    fn deny_revision_inputs(ctx: AuthContext<'_>) -> Authorization {
        let forbidden = match ctx.action {
            AuthAction::Read { table_name, .. } => [
                "finance_entries",
                "fills",
                "bills",
                "position_lots",
                "cash_flows",
            ]
            .contains(&table_name),
            _ => false,
        };
        if forbidden {
            Authorization::Deny
        } else {
            Authorization::Allow
        }
    }

    let mut s = store();
    s.connection.authorizer(Some(deny_revision_inputs)).unwrap();
    let manual = s
        .nav_summary(1, 4, &["manual".into()], &["USD".into()])
        .unwrap();
    assert_eq!(manual.len(), 1);
    assert_eq!(manual[0].scope_id, "strategy:manual");
    assert_eq!(manual[0].coverage, "NOT_APPLICABLE");
    let account_only = s.nav_summary(1, 4, &[], &["USD".into()]).unwrap();
    assert!(account_only.iter().any(|n| n.scope_id == "account"));
    s.connection
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .unwrap();

    // An ordinary strategy still requires the exact as-of revision source.
    owned_fill(
        &mut s,
        "revision_gate",
        "revision-gate-seed",
        100,
        "long",
        3,
    );
    s.connection.authorizer(Some(deny_revision_inputs)).unwrap();
    assert_eq!(
        s.nav_summary(1, 4, &["manual".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "NOT_APPLICABLE"
    );
    assert!(matches!(
        s.nav_summary(1, 4, &["revision_gate".into()], &["USD".into()]),
        Err(StoreError::Sql(_))
    ));
    s.connection
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .unwrap();
    assert_eq!(
        s.nav_summary(1, 4, &["revision_gate".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "PARTIAL"
    );
}

#[test]
fn p02_cash_flow_totals_cache_is_commit_scoped_and_rebuilt_after_reopen() {
    let mut s = store();
    assert!(s.cash_flow_totals_cache.borrow().is_none());
    let opening = s.cash_flow_totals().unwrap();
    assert_eq!(
        opening[&("USDT".into(), "OPENING".into())],
        Decimal::from(100)
    );
    assert_eq!(s.cash_flow_totals().unwrap(), opening);
    assert!(s.cash_flow_totals_cache.borrow().is_some());
    let deposit = CashFlow {
        event_id: "cash-cache-deposit".into(),
        kind: "DEPOSIT".into(),
        currency: "USDT".into(),
        amount: Decimal::from(7),
        time: 3,
        source_ref: "verified-cash".into(),
    };
    s.ingest_page(&IngestPage {
        cash_flows: vec![deposit.clone()],
        ..Default::default()
    })
    .unwrap();
    assert!(s.cash_flow_totals_cache.borrow().is_none());
    let with_deposit = s.cash_flow_totals().unwrap();
    assert_eq!(
        with_deposit[&("USDT".into(), "DEPOSIT".into())],
        Decimal::from(7)
    );
    s.ingest_page(&IngestPage {
        cash_flows: vec![deposit.clone()],
        ..Default::default()
    })
    .unwrap();
    assert!(
        s.cash_flow_totals_cache.borrow().is_some(),
        "idempotent replay must not invalidate"
    );
    assert_eq!(s.cash_flow_totals().unwrap(), with_deposit);
    let conflicting = CashFlow {
        amount: Decimal::from(9),
        ..deposit
    };
    assert!(matches!(
        s.ingest_page(&IngestPage {
            cash_flows: vec![conflicting],
            ..Default::default()
        }),
        Err(StoreError::Invalid("IMMUTABLE_CASH_FLOW_CONFLICT"))
    ));
    assert_eq!(s.cash_flow_totals().unwrap(), with_deposit);
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let reopened =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert!(reopened.cash_flow_totals_cache.borrow().is_none());
    assert_eq!(reopened.cash_flow_totals().unwrap(), with_deposit);
}

#[test]
fn p02_account_backed_internal_capital_is_conserved_and_not_strategy_nav() {
    let mut s = store();
    assert!(s.latest_capital_assignment_time.borrow().is_none());
    s.capital_state(1).unwrap();
    assert_eq!(*s.latest_capital_assignment_time.borrow(), Some(None));
    owned_fill(&mut s, "a", "p02-a", 100, "long", 3);
    owned_fill(&mut s, "b", "p02-b", 100, "long", 4);
    let assignment =
        |key: &str, instance: &str, delta: i64, time: i64| AuthorizedCapitalAssignment {
            account_key: "okx:real:verified".into(),
            installation_id: "installation".into(),
            nav_source_ref: "authenticated-flat".into(),
            operation_id: key.into(),
            request_hash: format!("hash:{key}"),
            instance_id: instance.into(),
            delta_usd: Decimal::from(delta),
            authority_revision: 1,
            authority_receipt: format!("governance:rev1:{key}"),
            time,
        };
    let a = assignment("assign-a", "a", 30, 4);
    let b = assignment("assign-b", "b", 40, 4);
    let first = s.commit_authorized_capital_assignment(&a).unwrap();
    assert!(s.latest_capital_assignment_time.borrow().is_none());
    assert_eq!(
        s.commit_authorized_capital_assignment(&a)
            .unwrap()
            .result_ref,
        first.result_ref
    );
    assert!(matches!(
        s.commit_authorized_capital_assignment(&assignment("over", "b", 71, 4)),
        Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_EXCEEDS_AVAILABLE"))
    ));
    s.commit_authorized_capital_assignment(&b).unwrap();
    let state = s.capital_state(4).unwrap();
    assert_eq!(*s.latest_capital_assignment_time.borrow(), Some(Some(4)));
    assert_eq!(state.account_nav_usd, Decimal::from(100));
    assert_eq!(state.assigned_usd, Decimal::from(70));
    assert_eq!(state.unattributed_residual_usd, Decimal::from(30));
    assert_eq!(state.strategy_principal_usd["a"], Decimal::from(30));
    assert_eq!(state.strategy_principal_usd["b"], Decimal::from(40));
    assert_eq!(
        state.account_nav_usd,
        state.assigned_usd + state.unattributed_residual_usd
    );
    assert_eq!(
        s.usd_component_total("INTERNAL_CAPITAL_USD").unwrap(),
        Decimal::ZERO
    );
    // No NAV, source watermark or fake profits are generated by principal.
    assert_eq!(
        s.nav_summary(1, 4, &["a".into()], &["USD".into()]).unwrap()[0].coverage,
        "PARTIAL"
    );
    assert!(!s.financial_risk(4, Some("a")).unwrap().nav_complete);
    assert!(
        s.commit_authorized_capital_assignment(&assignment("takeback", "a", -10, 4))
            .is_ok()
    );
    assert!(matches!(
        s.commit_authorized_capital_assignment(&assignment("over-release", "a", -21, 4)),
        Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_EXCEEDS_AVAILABLE"))
    ));
    assert!(matches!(
        s.commit_authorized_capital_assignment(&assignment("manual", "manual", 1, 4)),
        Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"))
    ));
    let bogus = IngestPage {
        finance: vec![FinanceEntry {
            entry_id: "bogus".into(),
            economic_event_key: "fake".into(),
            component: "INTERNAL_CAPITAL_USD".into(),
            allocation_leg: "strategy:a".into(),
            amount: Decimal::from(99),
            currency: "USD".into(),
            instance_id: Some("a".into()),
            source_ref: "forged".into(),
            time: 4,
            reverses_entry_id: None,
        }],
        ..Default::default()
    };
    assert!(matches!(
        s.ingest_page(&bogus),
        Err(StoreError::Invalid(
            "CAPITAL_WRITE_REQUIRES_AUTHORIZED_OPERATION"
        ))
    ));
    let p = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut restored =
        AccountStore::open_existing(p, &meta.account_key, &meta.installation_id).unwrap();
    assert!(restored.latest_capital_assignment_time.borrow().is_none());
    let restored_state = restored.capital_state(4).unwrap();
    assert_eq!(
        *restored.latest_capital_assignment_time.borrow(),
        Some(Some(4))
    );
    assert_eq!(restored_state.assigned_usd, Decimal::from(60));
    assert_eq!(restored_state.unattributed_residual_usd, Decimal::from(40));
    assert_eq!(
        restored
            .commit_authorized_capital_assignment(&a)
            .unwrap()
            .result_ref,
        first.result_ref
    );
    assert!(matches!(
        restored.commit_authorized_capital_assignment(&AuthorizedCapitalAssignment {
            request_hash: "changed".into(),
            ..a
        }),
        Err(StoreError::Invalid("IDEMPOTENCY_CONFLICT"))
    ));
}
#[test]
fn p02_internal_capital_rejects_incomplete_or_stale_account_nav() {
    let mut s = store();
    owned_fill(&mut s, "a", "p02-a2", 100, "long", 3);
    let a = AuthorizedCapitalAssignment {
        account_key: "okx:real:verified".into(),
        installation_id: "installation".into(),
        nav_source_ref: "authenticated-flat".into(),
        operation_id: "capital-stale".into(),
        request_hash: "hash-stale".into(),
        instance_id: "a".into(),
        delta_usd: Decimal::ONE,
        authority_revision: 1,
        authority_receipt: "governance:rev1".into(),
        time: 6003,
    };
    assert!(matches!(
        s.commit_authorized_capital_assignment(&a),
        Err(StoreError::Invalid(
            "ACCOUNT_CAPITAL_SOURCE_INCOMPLETE_OR_STALE"
        ))
    ));
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 6003,
            source_version: "missing-sources".into(),
            equity: Decimal::from(100),
            pnl: Decimal::ZERO,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "PARTIAL".into(),
            mark_source: "no-proof".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        s.commit_authorized_capital_assignment(&a),
        Err(StoreError::Invalid(
            "ACCOUNT_CAPITAL_SOURCE_INCOMPLETE_OR_STALE"
        ))
    ));
}

#[test]
fn p02_capital_is_isolated_per_account_and_identity_bound() {
    let mut a = store();
    owned_fill(&mut a, "isolation", "a-isolated", 100, "long", 3);
    let second_meta = AccountMeta {
        initialize_operation_id: "connect-demo".into(),
        release_id: "release-demo".into(),
        account_key: "okx:demo:independent".into(),
        installation_id: "installation-demo".into(),
        initialized_at: 1,
        opening_source: "authenticated-demo-flat".into(),
    };
    let second_snapshot = OpeningSnapshot {
        opening_equity_usd: Decimal::from(50),
        source_ref: second_meta.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(50))],
    };
    let mut b =
        AccountStore::initialize_flat(path(), second_meta.clone(), &second_snapshot).unwrap();
    b.apply_authority_policy(1, false, "campaign-demo", Decimal::from(20))
        .unwrap();
    b.ensure_manual_instance(1).unwrap();
    owned_fill(&mut b, "isolation", "b-isolated", 100, "long", 3);
    let input = AuthorizedCapitalAssignment {
        account_key: a.meta().account_key.clone(),
        installation_id: a.meta().installation_id.clone(),
        nav_source_ref: a.meta().opening_source.clone(),
        operation_id: "cross-account-proof".into(),
        request_hash: "account-a-hash".into(),
        instance_id: "isolation".into(),
        delta_usd: Decimal::from(20),
        authority_revision: 1,
        authority_receipt: "authorized-account-a".into(),
        time: 4,
    };
    a.commit_authorized_capital_assignment(&input).unwrap();
    assert!(matches!(
        b.commit_authorized_capital_assignment(&input),
        Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"))
    ));
    let other = AuthorizedCapitalAssignment {
        account_key: second_meta.account_key.clone(),
        installation_id: second_meta.installation_id.clone(),
        nav_source_ref: second_meta.opening_source.clone(),
        operation_id: "demo-capital-assignment".into(),
        request_hash: "account-b-hash".into(),
        instance_id: "isolation".into(),
        delta_usd: Decimal::from(15),
        authority_revision: 1,
        authority_receipt: "authorized-account-b".into(),
        time: 4,
    };
    b.commit_authorized_capital_assignment(&other).unwrap();
    assert_eq!(a.capital_state(4).unwrap().assigned_usd, Decimal::from(20));
    assert_eq!(b.capital_state(4).unwrap().assigned_usd, Decimal::from(15));
    assert_eq!(
        a.capital_state(4).unwrap().unattributed_residual_usd,
        Decimal::from(80)
    );
    assert_eq!(
        b.capital_state(4).unwrap().unattributed_residual_usd,
        Decimal::from(35)
    );
    assert_eq!(
        a.usd_component_total("INTERNAL_CAPITAL_USD").unwrap(),
        Decimal::ZERO
    );
    assert_eq!(
        b.usd_component_total("INTERNAL_CAPITAL_USD").unwrap(),
        Decimal::ZERO
    );
    assert_eq!(
        a.nav_summary(1, 4, &["isolation".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "PARTIAL"
    );
    assert_eq!(
        b.nav_summary(1, 4, &["isolation".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "PARTIAL"
    );
    let db = b.path.clone();
    drop(b);
    let mut b =
        AccountStore::open_existing(db, &second_meta.account_key, &second_meta.installation_id)
            .unwrap();
    assert_eq!(b.capital_state(4).unwrap().assigned_usd, Decimal::from(15));
    assert!(matches!(
        b.commit_authorized_capital_assignment(&input),
        Err(StoreError::Invalid("INVALID_CAPITAL_ASSIGNMENT"))
    ));
}

#[test]
fn p02_account_loss_can_be_repaired_by_releasing_internal_capital() {
    let mut s = store();
    owned_fill(&mut s, "loss-strategy", "loss-a", 100, "long", 3);
    let first = AuthorizedCapitalAssignment {
        account_key: s.meta().account_key.clone(),
        installation_id: s.meta().installation_id.clone(),
        nav_source_ref: s.meta().opening_source.clone(),
        operation_id: "p02-initial-capital".into(),
        request_hash: "initial-hash".into(),
        instance_id: "loss-strategy".into(),
        delta_usd: Decimal::from(70),
        authority_revision: 1,
        authority_receipt: "governance:rev1".into(),
        time: 4,
    };
    s.commit_authorized_capital_assignment(&first).unwrap();
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 5,
            source_version: "venue-update".into(),
            equity: Decimal::from(60),
            pnl: Decimal::from(-40),
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "venue:loss".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let before = s.capital_state(5).unwrap();
    assert_eq!(before.assigned_usd, Decimal::from(70));
    assert_eq!(before.unattributed_residual_usd, Decimal::from(-10));
    assert_eq!(before.coverage, "PARTIAL");
    assert_eq!(
        before.reason_code.as_deref(),
        Some("ACCOUNT_CAPITAL_OVERASSIGNED")
    );
    let mut adjustment = AuthorizedCapitalAssignment {
        account_key: first.account_key.clone(),
        installation_id: first.installation_id.clone(),
        nav_source_ref: "venue:loss".into(),
        operation_id: "p02-denied-increase".into(),
        request_hash: "increase-hash".into(),
        instance_id: "loss-strategy".into(),
        delta_usd: Decimal::ONE,
        authority_revision: 1,
        authority_receipt: "governance:rev1".into(),
        time: 5,
    };
    assert!(matches!(
        s.commit_authorized_capital_assignment(&adjustment),
        Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_EXCEEDS_AVAILABLE"))
    ));
    adjustment.operation_id = "p02-repair".into();
    adjustment.request_hash = "release-hash".into();
    adjustment.delta_usd = Decimal::from(-15);
    s.commit_authorized_capital_assignment(&adjustment).unwrap();
    let after = s.capital_state(5).unwrap();
    assert_eq!(after.assigned_usd, Decimal::from(55));
    assert_eq!(after.unattributed_residual_usd, Decimal::from(5));
    assert_eq!(after.coverage, "COMPLETE");
    assert_eq!(
        after.account_nav_usd,
        after.assigned_usd + after.unattributed_residual_usd
    );
    assert_eq!(
        s.usd_component_total("INTERNAL_CAPITAL_USD").unwrap(),
        Decimal::ZERO
    );
    assert!(
        !s.financial_risk(5, Some("loss-strategy"))
            .unwrap()
            .nav_complete
    );
}

fn p02_completed_cursors(s: &mut AccountStore, time: i64) {
    let mut events = Vec::new();
    for (source, part) in [
        ("fills", "SPOT"),
        ("fills", "SWAP"),
        ("fills", "FUTURES"),
        ("fills", "OPTION"),
        ("bills", "account"),
        ("account_snapshot", "account"),
    ] {
        events.push(SourceCursor {
            source: source.into(),
            stream_partition: part.into(),
            cursor: String::new(),
            watermark: time,
            generation: 1,
            coverage_gaps: String::new(),
        });
    }
    for cursor in events {
        s.ingest_page(&IngestPage {
            cursor: Some(cursor),
            ..Default::default()
        })
        .unwrap();
    }
}
fn p02_valuation_witness(s: &AccountStore, time: i64, source: &str) -> StrategyValuationEvidence {
    StrategyValuationEvidence {
        account_key: s.meta().account_key.clone(),
        account_nav_source: source.into(),
        as_of: time,
        conversion: UsdConversionEvidence {
            observed_at: time,
            source_digest: "authenticated-fx".into(),
            usd_per_currency: std::collections::BTreeMap::from([
                ("USD".into(), Decimal::ONE),
                ("USDT".into(), Decimal::ONE),
            ]),
        },
        venue_positions: Vec::new(),
    }
}
#[test]
fn p02_valuation_requires_converged_sources_even_with_valid_principal() {
    let mut s = store();
    let strategy = StrategyInstance {
        instance_id: "alpha".into(),
        definition_id: "fixture".into(),
        definition_version: "1".into(),
        parameters: "{}".into(),
        enabled: false,
        policy_revision: 1,
        budget: Decimal::from(10),
        last_decision_key: None,
    };
    s.mutate_instance(
        "valuation-create",
        "valuation-hash",
        "create",
        &strategy,
        None,
        2,
    )
    .unwrap();
    let assign = AuthorizedCapitalAssignment {
        account_key: s.meta().account_key.clone(),
        installation_id: s.meta().installation_id.clone(),
        nav_source_ref: s.meta().opening_source.clone(),
        operation_id: "valuation-fund".into(),
        request_hash: "valuation-fund-hash".into(),
        instance_id: "alpha".into(),
        delta_usd: Decimal::from(20),
        authority_revision: 1,
        authority_receipt: "governance".into(),
        time: 3,
    };
    s.commit_authorized_capital_assignment(&assign).unwrap();
    assert!(matches!(
        s.reconcile_strategy_valuation(&p02_valuation_witness(&s, 1, "authenticated-flat")),
        Err(StoreError::Invalid("STRATEGY_NAV_HISTORY_INCOMPLETE"))
    ));
    p02_completed_cursors(&mut s, 4);
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 4,
            source_version: "verified".into(),
            equity: Decimal::from(100),
            pnl: Decimal::ZERO,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "account-witness".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let mut witness = p02_valuation_witness(&s, 4, "account-witness");
    let valid = s.reconcile_strategy_valuation(&witness).unwrap();
    assert_eq!(valid.coverage, "COMPLETE");
    assert_eq!(valid.strategies[0].capital_usd, Decimal::from(20));
    assert_eq!(valid.strategies[0].equity_usd, Decimal::from(20));
    assert_eq!(valid.attributed_usd, Decimal::from(20));
    assert_eq!(valid.unattributed_usd, Decimal::from(80));
    assert_eq!(
        valid.account_nav_usd,
        valid.attributed_usd + valid.unattributed_usd
    );
    // Force a cursor failure after strategy NAV rows were tentatively written.
    // Neither NAV nor receipt may escape the transaction on rollback.
    let previous_cursor = SourceCursor {
        source: "strategy_valuation".into(),
        stream_partition: "account".into(),
        cursor: String::new(),
        watermark: 4,
        generation: 1,
        coverage_gaps: "BEFORE_VALUATION".into(),
    };
    s.ingest_page(&IngestPage {
        cursor: Some(previous_cursor.clone()),
        ..Default::default()
    })
    .unwrap();
    let rejected = SourceCursor {
        generation: 0,
        ..previous_cursor.clone()
    };
    assert!(matches!(
        s.commit_reconciled_strategy_nav_with_cursor(&witness, &rejected),
        Err(StoreError::Invalid("SOURCE_CURSOR_REGRESSION"))
    ));
    let strategy_points: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM nav_points WHERE scope_id='strategy:alpha'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(strategy_points, 0);
    assert_eq!(
        s.cursor("strategy_valuation", "account")
            .unwrap()
            .unwrap()
            .coverage_gaps,
        "BEFORE_VALUATION"
    );
    let accepted = SourceCursor {
        coverage_gaps: String::new(),
        ..previous_cursor
    };
    let published = s
        .commit_reconciled_strategy_nav_with_cursor(&witness, &accepted)
        .unwrap();
    assert_eq!(published, valid);
    assert_eq!(
        s.cursor("strategy_valuation", "account")
            .unwrap()
            .unwrap()
            .coverage_gaps,
        ""
    );
    assert_eq!(
        s.commit_reconciled_strategy_nav_with_cursor(&witness, &accepted)
            .unwrap(),
        valid
    );
    assert_eq!(s.commit_reconciled_strategy_nav(&witness).unwrap(), valid);
    let nav = s
        .nav_summary(1, 4, &["alpha".into()], &["USD".into()])
        .unwrap();
    assert_eq!(nav[0].coverage, "COMPLETE");
    assert_eq!(nav[0].opening_equity, Some(Decimal::ZERO));
    assert_eq!(nav[0].current_equity, Some(Decimal::from(20)));
    assert_eq!(nav[0].flows, Some(Decimal::from(20)));
    assert_eq!(nav[0].pnl, Some(Decimal::ZERO));
    assert!(matches!(
        s.ingest_page(&IngestPage {
            nav: vec![NavPoint {
                scope_id: "strategy:alpha".into(),
                time: 4,
                source_version: "strategy-reconciled-v1".into(),
                equity: Decimal::from(999),
                pnl: Decimal::ZERO,
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: "forged".into()
            }],
            ..Default::default()
        }),
        Err(StoreError::Invalid("STRATEGY_NAV_REQUIRES_RECONCILIATION"))
    ));

    assert!(!s.financial_risk(4, Some("alpha")).unwrap().nav_complete);
    assert_eq!(
        s.nav_summary(1, 4, &["alpha".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "COMPLETE"
    );
    witness.conversion.usd_per_currency.clear();
    assert!(s.reconcile_strategy_valuation(&witness).is_ok()); // Flat books need no FX conversion.
    witness.account_nav_source = "forged".into();
    assert!(matches!(
        s.reconcile_strategy_valuation(&witness),
        Err(StoreError::Invalid("STRATEGY_NAV_ACCOUNT_SOURCE_MISMATCH"))
    ));
    let witness = p02_valuation_witness(&s, 4, "account-witness");
    assert!(matches!(
        s.ingest_page(&IngestPage {
            finance: vec![FinanceEntry {
                entry_id: "forged-fee".into(),
                economic_event_key: "fake-fee".into(),
                component: "FEE".into(),
                allocation_leg: "account".into(),
                amount: Decimal::from(-1),
                currency: "USD".into(),
                instance_id: Some("alpha".into()),
                source_ref: "unverified:fee".into(),
                time: 4,
                reverses_entry_id: None,
            }],
            ..Default::default()
        }),
        Err(StoreError::Invalid(
            "STRATEGY_NAV_ECONOMIC_SOURCE_UNVERIFIED"
        ))
    ));
    // Rejected before entry/totals/cursor commit; the prior valuation is intact.
    assert!(s.reconcile_strategy_valuation(&witness).is_ok());
    s.ingest_page(&IngestPage {
        cursor: Some(SourceCursor {
            source: "strategy_valuation".into(),
            stream_partition: "account".into(),
            cursor: String::new(),
            watermark: 4,
            generation: 1,
            coverage_gaps: "STRATEGY_NAV_ECONOMIC_SOURCE_UNVERIFIED".into(),
        }),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        s.nav_summary(1, 4, &["alpha".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "PARTIAL"
    );
}
#[test]
fn p02_valuation_rejects_missing_mark_and_uses_contract_mechanics() {
    let mut s = store();
    owned_fill(&mut s, "alpha", "valuation-fill", 100, "long", 3);
    let grant = AuthorizedCapitalAssignment {
        account_key: s.meta().account_key.clone(),
        installation_id: s.meta().installation_id.clone(),
        nav_source_ref: s.meta().opening_source.clone(),
        operation_id: "valuation-grant".into(),
        request_hash: "valuation-grant-hash".into(),
        instance_id: "alpha".into(),
        delta_usd: Decimal::from(20),
        authority_revision: 1,
        authority_receipt: "governance".into(),
        time: 4,
    };
    s.commit_authorized_capital_assignment(&grant).unwrap();
    p02_completed_cursors(&mut s, 5);
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 5,
            source_version: "verified".into(),
            equity: Decimal::from(101),
            pnl: Decimal::ONE,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "account-witness".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let mut evidence = p02_valuation_witness(&s, 5, "account-witness");
    assert!(matches!(
        s.reconcile_strategy_valuation(&evidence),
        Err(StoreError::Invalid("STRATEGY_NAV_MARK_MISSING"))
    ));
    evidence.venue_positions.push(ValuationPosition {
        instrument_id: "BTC".into(),
        signed_contracts: Decimal::ONE,
        mark_price: Decimal::from(110),
    });
    let v = s.reconcile_strategy_valuation(&evidence).unwrap();
    assert_eq!(v.strategies[0].unrealized_usd, Decimal::new(1, 1));
    assert_eq!(v.strategies[0].equity_usd, Decimal::new(201, 1));
    assert_eq!(v.unattributed_usd, Decimal::new(809, 1));
    assert_eq!(v.account_nav_usd, v.attributed_usd + v.unattributed_usd);
    evidence.conversion.usd_per_currency.remove("USDT");
    assert!(matches!(
        s.reconcile_strategy_valuation(&evidence),
        Err(StoreError::Invalid("STRATEGY_NAV_FX_MISSING"))
    ));
    evidence.venue_positions[0].signed_contracts = Decimal::from(2);
    assert!(matches!(
        s.reconcile_strategy_valuation(&evidence),
        Err(StoreError::Invalid("STRATEGY_NAV_FX_MISSING"))
    ));
    evidence
        .conversion
        .usd_per_currency
        .insert("USDT".into(), Decimal::ONE);
    evidence.venue_positions[0].signed_contracts = Decimal::ONE;
    // Existing fill identity does not permit inventing a different fee.
    assert!(matches!(
        s.ingest_page(&IngestPage {
            finance: vec![FinanceEntry {
                entry_id: "fee-mismatch".into(),
                economic_event_key: "fill:BTC:valuation-fill:o:valuation-fill".into(),
                component: "FEE".into(),
                allocation_leg: "account".into(),
                amount: Decimal::from(-1),
                currency: "USDT".into(),
                instance_id: Some("alpha".into()),
                source_ref: "fill:f:valuation-fill".into(),
                time: 5,
                reverses_entry_id: None,
            }],
            ..Default::default()
        }),
        Err(StoreError::Invalid("STRATEGY_NAV_FEE_SOURCE_MISMATCH"))
    ));
    assert!(s.reconcile_strategy_valuation(&evidence).is_ok());
}

#[test]
fn f1_current_strategy_valuation_uses_verified_totals_after_large_history_and_restart() {
    let mut s = store();
    let strategy = StrategyInstance {
        instance_id: "alpha".into(),
        definition_id: "fixture".into(),
        definition_version: "1".into(),
        parameters: "{}".into(),
        enabled: false,
        policy_revision: 1,
        budget: Decimal::from(10),
        last_decision_key: None,
    };
    s.mutate_instance("f1-create", "f1-create", "create", &strategy, None, 2)
        .unwrap();
    s.commit_authorized_capital_assignment(&AuthorizedCapitalAssignment {
        account_key: s.meta().account_key.clone(),
        installation_id: s.meta().installation_id.clone(),
        nav_source_ref: s.meta().opening_source.clone(),
        operation_id: "f1-capital".into(),
        request_hash: "f1-capital".into(),
        instance_id: "alpha".into(),
        delta_usd: Decimal::from(20),
        authority_revision: 1,
        authority_receipt: "governance".into(),
        time: 4,
    })
    .unwrap();
    let mut page = IngestPage {
        bills: vec![Bill {
            bill_id: "f1-bill".into(),
            kind: "FEE".into(),
            time: 7,
            amount: Decimal::NEGATIVE_ONE,
            currency: "USDT".into(),
            economic_event_key: "bill:f1-bill".into(),
        }],
        finance: vec![FinanceEntry {
            entry_id: "f1-attributed-fee".into(),
            economic_event_key: "bill:f1-bill".into(),
            component: "FEE".into(),
            allocation_leg: "account".into(),
            amount: Decimal::NEGATIVE_ONE,
            currency: "USDT".into(),
            instance_id: Some("alpha".into()),
            source_ref: "bill:f1-bill".into(),
            time: 7,
            reverses_entry_id: None,
        }],
        ..Default::default()
    };
    // The large immutable history is unrelated to the one active strategy.
    // NAV must depend on current projections, not traverse these rows.
    for n in 0..2000 {
        let mut e = fee(&format!("f1-unallocated-{n}"), "unattributed");
        e.economic_event_key = format!("unattributed-event:{n}");
        e.instance_id = None;
        page.finance.push(e);
    }
    s.ingest_page(&page).unwrap();
    p02_completed_cursors(&mut s, 10);
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 10,
            source_version: "f1-live".into(),
            equity: Decimal::from(100),
            pnl: Decimal::ZERO,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "f1-account".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let witness = p02_valuation_witness(&s, 10, "f1-account");
    let result = s.reconcile_strategy_valuation(&witness).unwrap();
    assert_eq!(result.strategies[0].realized_usd, Decimal::NEGATIVE_ONE);
    assert_eq!(result.strategies[0].equity_usd, Decimal::from(19));
    assert_eq!(result.unattributed_usd, Decimal::from(81));
    // The provenance receipt and totals survive process replacement.
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let s = AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(s.reconcile_strategy_valuation(&witness).unwrap(), result);
    let sealed = s.cursor("finance_provenance", "account").unwrap().unwrap();
    assert!(sealed.watermark >= 2000);
}

#[test]
fn f1_fill_fee_with_bill_id_is_verified_before_bill_history_and_never_doubled() {
    let mut s = store();
    owned_fill(&mut s, "alpha", "f1-seed", 100, "long", 3);
    let fee = FinanceEntry {
        entry_id: "f1-early-fill-fee".into(),
        economic_event_key: "bill:f1-later-bill".into(),
        component: "FEE".into(),
        allocation_leg: "account".into(),
        amount: Decimal::new(-1, 2),
        currency: "USDT".into(),
        instance_id: Some("alpha".into()),
        source_ref: "fill:BTC:f1-trade:f1-order".into(),
        time: 4,
        reverses_entry_id: None,
    };
    let first = IngestPage {
        fills: vec![Fill {
            fill_id: "BTC:f1-trade:f1-order".into(),
            instrument_id: "BTC".into(),
            trade_id: "f1-trade".into(),
            exchange_order_id: "f1-order".into(),
            intent_id: Some("f1-seed".into()),
            time: 4,
            qty: Decimal::ONE,
            price: Decimal::from(100),
            fee: fee.amount,
            currency: fee.currency.clone(),
            side: "buy".into(),
        }],
        finance: vec![fee.clone()],
        ..Default::default()
    };
    // The authenticated fill carries an exchange bill ID, but the separate
    // bill page has not yet arrived. It must not deadlock fills-first capture.
    s.ingest_page(&first).unwrap();
    s.ingest_page(&first).unwrap();
    let later = IngestPage {
        bills: vec![Bill {
            bill_id: "f1-later-bill".into(),
            kind: "FEE".into(),
            time: 5,
            amount: fee.amount,
            currency: "USDT".into(),
            economic_event_key: "bill:f1-later-bill".into(),
        }],
        finance: vec![FinanceEntry {
            entry_id: "f1-bill-alias".into(),
            source_ref: "bill:f1-later-bill".into(),
            time: 5,
            ..fee.clone()
        }],
        ..Default::default()
    };
    s.ingest_page(&later).unwrap();
    s.ingest_page(&later).unwrap();
    let total: String = s
        .connection
        .query_row(
            "SELECT amount FROM instance_finance_totals
             WHERE instance_id='alpha' AND component='FEE' AND currency='USDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(total, "-0.01");
    let conflicting_fee = FinanceEntry {
        entry_id: "f1-duplicate-fill-fee".into(),
        economic_event_key: "bill:another-bill".into(),
        ..fee
    };
    assert!(matches!(
        s.ingest_page(&IngestPage {
            finance: vec![conflicting_fee],
            ..Default::default()
        }),
        Err(StoreError::Invalid("FINANCE_FILL_SOURCE_REUSED"))
    ));
    assert_eq!(
        s.finance_history(1, 10, 0, "", 100)
            .unwrap()
            .iter()
            .filter(
                |entry| entry.component == "FEE" && entry.instance_id.as_deref() == Some("alpha"),
            )
            .count(),
        1
    );
    // A matching original is not authority to invent a profitable reversal.
    let unproven = FinanceEntry {
        entry_id: "f1-forged-reversal".into(),
        economic_event_key: "correction:forged".into(),
        component: "FEE".into(),
        allocation_leg: "account".into(),
        amount: Decimal::new(1, 2),
        currency: "USDT".into(),
        instance_id: Some("alpha".into()),
        source_ref: "forged:reversal".into(),
        time: 6,
        reverses_entry_id: Some("f1-early-fill-fee".into()),
    };
    assert!(matches!(
        s.ingest_page(&IngestPage {
            finance: vec![unproven.clone()],
            ..Default::default()
        }),
        Err(StoreError::Invalid(
            "STRATEGY_NAV_ECONOMIC_SOURCE_UNVERIFIED"
        ))
    ));
    let verified_reversal = FinanceEntry {
        entry_id: "f1-verified-reversal".into(),
        economic_event_key: "bill:f1-correction".into(),
        source_ref: "bill:f1-correction".into(),
        ..unproven
    };
    let correction = IngestPage {
        bills: vec![Bill {
            bill_id: "f1-correction".into(),
            kind: "FEE_REVERSAL".into(),
            time: 6,
            amount: verified_reversal.amount,
            currency: verified_reversal.currency.clone(),
            economic_event_key: verified_reversal.economic_event_key.clone(),
        }],
        finance: vec![verified_reversal],
        ..Default::default()
    };
    s.ingest_page(&correction).unwrap();
    s.ingest_page(&correction).unwrap();
    let balance: String = s
        .connection
        .query_row(
            "SELECT amount FROM instance_finance_totals
             WHERE instance_id='alpha' AND component='FEE' AND currency='USDT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(balance, "0");
}

#[test]
fn p02_two_accounts_two_strategies_fees_marks_restart_conserve_nav() {
    let mut real = store();
    let demo_meta = AccountMeta {
        initialize_operation_id: "demo-2x2-init".into(),
        release_id: "release-demo-2x2".into(),
        account_key: "okx:demo:verified-2x2".into(),
        installation_id: "demo-2x2-installation".into(),
        initialized_at: 1,
        opening_source: "demo-2x2-opening".into(),
    };
    let demo_snapshot = OpeningSnapshot {
        opening_equity_usd: Decimal::from(50),
        source_ref: demo_meta.opening_source.clone(),
        observed_at: 1,
        positions: 0,
        orders: 0,
        conditional_orders: 0,
        liabilities: Decimal::ZERO,
        balances: vec![("USDT".into(), Decimal::from(50))],
    };
    let mut demo =
        AccountStore::initialize_flat(path(), demo_meta.clone(), &demo_snapshot).unwrap();
    demo.apply_authority_policy(1, false, "campaign-demo", Decimal::from(20))
        .unwrap();
    demo.ensure_manual_instance(1).unwrap();
    for (s, left, right) in [
        (&mut real, "real_a", "real_b"),
        (&mut demo, "demo_a", "demo_b"),
    ] {
        owned_fill(s, left, &format!("{left}-fill"), 100, "long", 3);
        owned_fill(s, right, &format!("{right}-fill"), 100, "long", 4);
        let (first, second) = if left == "real_a" { (20, 30) } else { (10, 15) };
        for (id, amount, time) in [(left, first, 5), (right, second, 6)] {
            s.commit_authorized_capital_assignment(&AuthorizedCapitalAssignment {
                account_key: s.meta().account_key.clone(),
                installation_id: s.meta().installation_id.clone(),
                nav_source_ref: s.meta().opening_source.clone(),
                operation_id: format!("capital-{id}"),
                request_hash: format!("hash-{id}"),
                instance_id: id.into(),
                delta_usd: Decimal::from(amount),
                authority_revision: 1,
                authority_receipt: format!("governance-{id}"),
                time,
            })
            .unwrap();
        }
        let fee = if left == "real_a" {
            Decimal::from(-1)
        } else {
            Decimal::new(-5, 1)
        };
        let bill_id = format!("fee-bill-{left}");
        s.ingest_page(&IngestPage {
            bills: vec![Bill {
                bill_id: bill_id.clone(),
                kind: "trade-fee".into(),
                time: 7,
                amount: fee,
                currency: "USDT".into(),
                economic_event_key: format!("bill:{bill_id}"),
            }],
            finance: vec![FinanceEntry {
                entry_id: format!("fee-{left}"),
                economic_event_key: format!("bill:{bill_id}"),
                component: "FEE".into(),
                allocation_leg: "account".into(),
                amount: fee,
                currency: "USDT".into(),
                instance_id: Some(left.into()),
                source_ref: format!("bill:{bill_id}"),
                time: 7,
                reverses_entry_id: None,
            }],
            ..Default::default()
        })
        .unwrap();
        p02_completed_cursors(s, 10);
        let account_nav = if left == "real_a" {
            Decimal::new(992, 1)
        } else {
            Decimal::new(497, 1)
        };
        s.ingest_page(&IngestPage {
            nav: vec![NavPoint {
                scope_id: "account".into(),
                time: 10,
                source_version: "2x2".into(),
                equity: account_nav,
                pnl: account_nav
                    - if left == "real_a" {
                        Decimal::from(100)
                    } else {
                        Decimal::from(50)
                    },
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: "2x2-venue".into(),
            }],
            ..Default::default()
        })
        .unwrap();
        let mut proof = p02_valuation_witness(s, 10, "2x2-venue");
        proof.venue_positions.push(ValuationPosition {
            instrument_id: "BTC".into(),
            signed_contracts: Decimal::from(2),
            mark_price: Decimal::from(110),
        });
        let checked = s.commit_reconciled_strategy_nav(&proof).unwrap();
        assert_eq!(checked.coverage, "COMPLETE");
        assert_eq!(checked.strategies.len(), 2);
        assert_eq!(
            checked.account_nav_usd,
            checked.attributed_usd + checked.unattributed_usd
        );
        assert_eq!(
            checked.unattributed_usd,
            if left == "real_a" {
                Decimal::from(50)
            } else {
                Decimal::from(25)
            }
        );
        assert_eq!(checked.strategies[0].realized_usd, fee);
        assert_eq!(checked.strategies[0].unrealized_usd, Decimal::new(1, 1));
        assert_eq!(checked.strategies[1].unrealized_usd, Decimal::new(1, 1));
        assert_eq!(checked.strategies[1].realized_usd, Decimal::ZERO);
        for id in [left, right] {
            let nav = s.nav_summary(1, 10, &[id.into()], &["USD".into()]).unwrap();
            assert_eq!(nav.len(), 1);
            assert_eq!(nav[0].coverage, "COMPLETE");
            assert_eq!(nav[0].opening_equity, Some(Decimal::ZERO));
            assert_eq!(
                nav[0].pnl,
                Some(if id == left {
                    fee + Decimal::new(1, 1)
                } else {
                    Decimal::new(1, 1)
                })
            );
            assert_eq!(
                nav[0].current_equity,
                Some(nav[0].flows.unwrap() + nav[0].pnl.unwrap())
            );
            assert!(!s.financial_risk(10, Some(id)).unwrap().nav_complete);
        }
        assert_eq!(s.commit_reconciled_strategy_nav(&proof).unwrap(), checked);
    }
    let realsql = real.path.clone();
    let realmeta = real.meta.clone();
    let demosql = demo.path.clone();
    drop(real);
    drop(demo);
    let reloaded_real =
        AccountStore::open_existing(realsql, &realmeta.account_key, &realmeta.installation_id)
            .unwrap();
    let reloaded_demo =
        AccountStore::open_existing(demosql, &demo_meta.account_key, &demo_meta.installation_id)
            .unwrap();
    assert_eq!(
        reloaded_real
            .nav_summary(1, 10, &["real_a".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "COMPLETE"
    );
    assert_eq!(
        reloaded_demo
            .nav_summary(1, 10, &["demo_a".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "COMPLETE"
    );
    assert_eq!(
        reloaded_real.capital_state(10).unwrap().assigned_usd,
        Decimal::from(50)
    );
    assert_eq!(
        reloaded_demo.capital_state(10).unwrap().assigned_usd,
        Decimal::from(25)
    );
}

#[test]
fn p02_strategy_nav_expires_on_new_zero_fee_fill_or_new_account_mark() {
    let mut s = store();
    let strategy = StrategyInstance {
        instance_id: "witness".into(),
        definition_id: "fixture".into(),
        definition_version: "1".into(),
        parameters: "{}".into(),
        enabled: false,
        policy_revision: 1,
        budget: Decimal::from(10),
        last_decision_key: None,
    };
    s.mutate_instance(
        "witness-create",
        "witness-hash",
        "create",
        &strategy,
        None,
        2,
    )
    .unwrap();
    s.commit_authorized_capital_assignment(&AuthorizedCapitalAssignment {
        account_key: s.meta().account_key.clone(),
        installation_id: s.meta().installation_id.clone(),
        nav_source_ref: s.meta().opening_source.clone(),
        operation_id: "witness-capital".into(),
        request_hash: "witness-capital-hash".into(),
        instance_id: "witness".into(),
        delta_usd: Decimal::from(10),
        authority_revision: 1,
        authority_receipt: "governance".into(),
        time: 3,
    })
    .unwrap();
    p02_completed_cursors(&mut s, 4);
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 4,
            source_version: "witness-4".into(),
            equity: Decimal::from(100),
            pnl: Decimal::ZERO,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "mark-4".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let witness = p02_valuation_witness(&s, 4, "mark-4");
    s.commit_reconciled_strategy_nav(&witness).unwrap();
    let read = |s: &AccountStore, t| {
        s.nav_summary(1, t, &["witness".into()], &["USD".into()])
            .unwrap()[0]
            .coverage
            .clone()
    };
    assert_eq!(read(&s, 4), "COMPLETE");
    let stable = s.strategy_valuation_revision().unwrap();
    let finance_only = s.finance_revision().unwrap();
    // A later mark-to-market witness changes economic NAV without a finance
    // entry. The old strategy witness must not remain current/COMPLETE.
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 5,
            source_version: "witness-5".into(),
            equity: Decimal::from(101),
            pnl: Decimal::ONE,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "mark-5".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert_ne!(s.strategy_valuation_revision().unwrap(), stable);
    assert_eq!(s.strategy_valuation_revision_as_of(4).unwrap(), stable);
    assert_eq!(read(&s, 5), "PARTIAL");
    assert_eq!(read(&s, 4), "COMPLETE");
    // A zero-fee fill may change positions while finance_entries stays
    // unchanged. This makes the old *historical* revision invalid too.
    s.ingest_page(&IngestPage {
        fills: vec![Fill {
            fill_id: "zero-fee-source".into(),
            instrument_id: "BTC".into(),
            trade_id: "zero-trade".into(),
            exchange_order_id: "zero-order".into(),
            intent_id: None,
            time: 4,
            qty: Decimal::ONE,
            price: Decimal::from(100),
            fee: Decimal::ZERO,
            currency: "USDT".into(),
            side: "buy".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert_eq!(s.finance_revision().unwrap(), finance_only);
    assert_ne!(s.strategy_valuation_revision().unwrap(), stable);
    assert_eq!(read(&s, 4), "PARTIAL");
    assert_eq!(read(&s, 5), "PARTIAL");
}

#[test]
fn p02_strategy_nav_window_uses_separate_source_revisions_at_both_endpoints() {
    let mut s = store();
    let strategy = StrategyInstance {
        instance_id: "window".into(),
        definition_id: "fixture".into(),
        definition_version: "1".into(),
        parameters: "{}".into(),
        enabled: false,
        policy_revision: 1,
        budget: Decimal::from(10),
        last_decision_key: None,
    };
    s.mutate_instance("window-create", "window-hash", "create", &strategy, None, 2)
        .unwrap();
    s.commit_authorized_capital_assignment(&AuthorizedCapitalAssignment {
        account_key: s.meta().account_key.clone(),
        installation_id: s.meta().installation_id.clone(),
        nav_source_ref: s.meta().opening_source.clone(),
        operation_id: "window-capital".into(),
        request_hash: "window-capital-hash".into(),
        instance_id: "window".into(),
        delta_usd: Decimal::from(10),
        authority_revision: 1,
        authority_receipt: "governance".into(),
        time: 3,
    })
    .unwrap();
    p02_completed_cursors(&mut s, 5);
    // Different legitimate account marks change the immutable strategy
    // source revision. The historical opening must retain its own witness.
    for (time, mark, account_equity) in [(4, "window-mark-4", 100), (5, "window-mark-5", 101)] {
        s.ingest_page(&IngestPage {
            nav: vec![NavPoint {
                scope_id: "account".into(),
                time,
                source_version: format!("window-source-{time}"),
                equity: Decimal::from(account_equity),
                pnl: Decimal::ZERO,
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: mark.into(),
            }],
            ..Default::default()
        })
        .unwrap();
        s.commit_reconciled_strategy_nav(&p02_valuation_witness(&s, time, mark))
            .unwrap();
    }
    let checked = s
        .nav_summary(4, 5, &["window".into()], &["USD".into()])
        .unwrap();
    assert_eq!(checked[0].coverage, "COMPLETE");
    assert_eq!(checked[0].opening_as_of, Some(4));
    assert_eq!(checked[0].current_as_of, Some(5));
    assert_eq!(checked[0].opening_equity, Some(Decimal::from(10)));
    assert_eq!(checked[0].current_equity, Some(Decimal::from(10)));
    assert_eq!(checked[0].flows, Some(Decimal::ZERO));
    assert_eq!(checked[0].pnl, Some(Decimal::ZERO));

    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut restored =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(
        restored
            .nav_summary(4, 5, &["window".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "COMPLETE"
    );
    // An as-yet unreconciled future mark invalidates that future window,
    // but must not retroactively spoil a previously certified interval.
    restored
        .ingest_page(&IngestPage {
            nav: vec![NavPoint {
                scope_id: "account".into(),
                time: 6,
                source_version: "window-source-6".into(),
                equity: Decimal::from(102),
                pnl: Decimal::ZERO,
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: "window-mark-6".into(),
            }],
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        restored
            .nav_summary(4, 5, &["window".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "COMPLETE"
    );
    assert_eq!(
        restored
            .nav_summary(4, 6, &["window".into()], &["USD".into()])
            .unwrap()[0]
            .coverage,
        "PARTIAL"
    );
}

#[test]
fn p02_same_timestamp_late_bill_replays_new_nav_revision_without_rewriting_history() {
    let mut s = store();
    let strategy = StrategyInstance {
        instance_id: "late".into(),
        definition_id: "fixture".into(),
        definition_version: "1".into(),
        parameters: "{}".into(),
        enabled: false,
        policy_revision: 1,
        budget: Decimal::from(10),
        last_decision_key: None,
    };
    s.mutate_instance("late-create", "late-hash", "create", &strategy, None, 2)
        .unwrap();
    s.commit_authorized_capital_assignment(&AuthorizedCapitalAssignment {
        account_key: s.meta().account_key.clone(),
        installation_id: s.meta().installation_id.clone(),
        nav_source_ref: s.meta().opening_source.clone(),
        operation_id: "late-capital".into(),
        request_hash: "late-capital-hash".into(),
        instance_id: "late".into(),
        delta_usd: Decimal::from(20),
        authority_revision: 1,
        authority_receipt: "governance".into(),
        time: 3,
    })
    .unwrap();
    p02_completed_cursors(&mut s, 4);
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 4,
            source_version: "witness".into(),
            equity: Decimal::from(100),
            pnl: Decimal::ZERO,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "witness-source".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let witness = p02_valuation_witness(&s, 4, "witness-source");
    let first = s.commit_reconciled_strategy_nav(&witness).unwrap();
    assert_eq!(first.strategies[0].equity_usd, Decimal::from(20));
    let read = |s: &AccountStore| {
        s.nav_summary(1, 4, &["late".into()], &["USD".into()])
            .unwrap()[0]
            .coverage
            .clone()
    };
    assert_eq!(read(&s), "COMPLETE");
    let first_version = s.strategy_valuation_revision().unwrap();
    // Delayed authenticated bill arrives without adding attributable PnL.
    // The SQL receipt of the first calculation must remain immutable.
    s.ingest_page(&IngestPage {
        bills: vec![Bill {
            bill_id: "late-bill".into(),
            kind: "2:0".into(),
            time: 4,
            amount: Decimal::ZERO,
            currency: "USDT".into(),
            economic_event_key: "bill:late-bill".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert_ne!(s.strategy_valuation_revision().unwrap(), first_version);
    assert_eq!(read(&s), "PARTIAL");
    let second = s.commit_reconciled_strategy_nav(&witness).unwrap();
    assert_eq!(first, second);
    assert_eq!(read(&s), "COMPLETE");
    let versions: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM nav_points WHERE scope_id='strategy:late' AND time=4
         AND source_version LIKE 'strategy-reconciled-v1:revision:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(versions, 2);
    assert_eq!(s.commit_reconciled_strategy_nav(&witness).unwrap(), second);
    // Same timestamp, corrected source and venue equity. This is a separate
    // immutable account NAV witness and must allow a third strategy revision.
    let second_version = s.strategy_valuation_revision().unwrap();
    s.ingest_page(&IngestPage {
        nav: vec![NavPoint {
            scope_id: "account".into(),
            time: 4,
            source_version: "zz-witness-corrected".into(),
            equity: Decimal::from(101),
            pnl: Decimal::ONE,
            flows: Decimal::ZERO,
            currency: "USD".into(),
            coverage: "COMPLETE".into(),
            mark_source: "witness-corrected".into(),
        }],
        ..Default::default()
    })
    .unwrap();
    assert_ne!(s.strategy_valuation_revision().unwrap(), second_version);
    assert_eq!(read(&s), "PARTIAL");
    let corrected = p02_valuation_witness(&s, 4, "witness-corrected");
    let third = s.commit_reconciled_strategy_nav(&corrected).unwrap();
    assert_eq!(third.account_nav_usd, Decimal::from(101));
    assert_eq!(third.strategies[0].equity_usd, Decimal::from(20));
    assert_eq!(third.unattributed_usd, Decimal::from(81));
    assert_eq!(read(&s), "COMPLETE");
    assert_eq!(s.commit_reconciled_strategy_nav(&corrected).unwrap(), third);
    let versions: i64 = s
        .connection
        .query_row(
            "SELECT COUNT(*) FROM nav_points WHERE scope_id='strategy:late'
         AND time=4 AND source_version LIKE 'strategy-reconciled-v1:revision:%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(versions, 3);
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let restored =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(read(&restored), "COMPLETE");
}

#[test]
fn p02_historical_strategy_principal_does_not_look_ahead_after_later_allocation() {
    let mut s = store();
    for id in ["past", "future"] {
        let strategy = StrategyInstance {
            instance_id: id.into(),
            definition_id: "fixture".into(),
            definition_version: "1".into(),
            parameters: "{}".into(),
            enabled: false,
            policy_revision: 1,
            budget: Decimal::from(10),
            last_decision_key: None,
        };
        s.mutate_instance(
            &format!("{id}-create"),
            &format!("{id}-hash"),
            "create",
            &strategy,
            None,
            2,
        )
        .unwrap();
    }
    let assignment = |id: &str, time: i64, amount: i64| AuthorizedCapitalAssignment {
        account_key: "okx:real:verified".into(),
        installation_id: "installation".into(),
        nav_source_ref: if time < 6 {
            "historical-4"
        } else {
            "historical-8"
        }
        .into(),
        operation_id: format!("historical-{id}"),
        request_hash: format!("historical-hash-{id}"),
        instance_id: id.into(),
        delta_usd: Decimal::from(amount),
        authority_revision: 1,
        authority_receipt: format!("governance-{id}"),
        time,
    };
    // First allocation is covered by the initial source.
    let mut earlier = assignment("past", 3, 20);
    earlier.nav_source_ref = s.meta().opening_source.clone();
    s.commit_authorized_capital_assignment(&earlier).unwrap();
    p02_completed_cursors(&mut s, 8);
    for (time, source) in [(4, "historical-4"), (8, "historical-8")] {
        s.ingest_page(&IngestPage {
            nav: vec![NavPoint {
                scope_id: "account".into(),
                time,
                source_version: format!("as-of-{time}"),
                equity: Decimal::from(100),
                pnl: Decimal::ZERO,
                flows: Decimal::ZERO,
                currency: "USD".into(),
                coverage: "COMPLETE".into(),
                mark_source: source.into(),
            }],
            ..Default::default()
        })
        .unwrap();
    }
    let second = assignment("future", 6, 30);
    // At t=6 the as-of account NAV source must be the preceding t=4 witness.
    let mut second = second;
    second.nav_source_ref = "historical-4".into();
    s.commit_authorized_capital_assignment(&second).unwrap();
    let past = s.capital_state(4).unwrap();
    assert_eq!(past.assigned_usd, Decimal::from(20));
    assert_eq!(past.unattributed_residual_usd, Decimal::from(80));
    assert!(past.strategy_principal_usd.contains_key("past"));
    assert!(!past.strategy_principal_usd.contains_key("future"));
    let present = s.capital_state(8).unwrap();
    assert_eq!(present.assigned_usd, Decimal::from(50));
    assert_eq!(present.unattributed_residual_usd, Decimal::from(50));
    assert_eq!(present.strategy_principal_usd["future"], Decimal::from(30));
    let witness = p02_valuation_witness(&s, 4, "historical-4");
    let historical = s.reconcile_strategy_valuation(&witness).unwrap();
    assert_eq!(historical.strategies.len(), 1);
    assert_eq!(historical.strategies[0].instance_id, "past");
    assert_eq!(historical.unattributed_usd, Decimal::from(80));
    assert_eq!(
        historical.account_nav_usd,
        historical.attributed_usd + historical.unattributed_usd
    );
    let stale = assignment("future-late", 5, 1);
    assert!(matches!(
        s.commit_authorized_capital_assignment(&stale),
        Err(StoreError::Invalid("CAPITAL_ASSIGNMENT_OUT_OF_ORDER"))
    ));
    assert_eq!(
        s.commit_authorized_capital_assignment(&earlier)
            .unwrap()
            .kind,
        "CAPITAL_ASSIGNMENT"
    );
    let p = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let restored =
        AccountStore::open_existing(p, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(
        restored.capital_state(4).unwrap().assigned_usd,
        Decimal::from(20)
    );
    assert_eq!(
        restored.capital_state(8).unwrap().assigned_usd,
        Decimal::from(50)
    );
}

#[test]
fn p02_existing_v2_rebuilds_strategy_nav_revision_lookup_without_rewriting_facts() {
    let s = store();
    let path = s.path.clone();
    let meta = s.meta.clone();
    let before: i64 = s
        .connection
        .query_row("SELECT count(*) FROM nav_points", [], |r| r.get(0))
        .unwrap();
    s.connection
        .execute_batch("DROP INDEX idx_nav_scope_version_time")
        .unwrap();
    drop(s);
    let recovered =
        AccountStore::open_existing(&path, &meta.account_key, &meta.installation_id).unwrap();
    let version: i64 = recovered
        .connection
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    assert_eq!(version, 2);
    let after: i64 = recovered
        .connection
        .query_row("SELECT count(*) FROM nav_points", [], |r| r.get(0))
        .unwrap();
    assert_eq!(after, before);
    let mut plan = recovered.connection.prepare(
        "EXPLAIN QUERY PLAN SELECT EXISTS(SELECT 1 FROM nav_points n \
         JOIN operation_receipts o ON o.operation_id='strategy-nav:' || n.scope_id || ':' || n.time || ':' || n.source_version \
         WHERE n.scope_id=?1 AND n.time<=?2 AND n.source_version=?3 \
         AND o.kind='STRATEGY_NAV_RECONCILED')"
    ).unwrap();
    let details = plan
        .query_map(
            rusqlite::params![
                "strategy:ema",
                12345_i64,
                "strategy-reconciled-v1:revision:test"
            ],
            |r| r.get::<_, String>(3),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        details
            .iter()
            .any(|d| d.contains("idx_nav_scope_version_time")),
        "{details:?}"
    );
}

#[tokio::test]
async fn sql_job_timing_is_returned_with_its_own_result() {
    let existing = store();
    let path = existing.path.clone();
    let account = existing.meta.account_key.clone();
    let installation = existing.meta.installation_id.clone();
    drop(existing);
    let handle = crate::AccountStoreHandle::open_existing(path, account.clone(), installation)
        .await
        .unwrap();
    let (first, timing) = handle
        .call_with_timing(std::time::Instant::now() + Duration::from_secs(3), |s| {
            Ok(s.meta().account_key.clone())
        })
        .await
        .unwrap();
    assert_eq!(first, account);
    let counters = handle.sql_timing();
    assert_eq!(counters.observed_jobs, 1);
    assert_eq!(counters.last_queue_wait_ms, timing.queue_wait_ms);
    assert_eq!(counters.last_sql_owner_ms, timing.sql_owner_ms);
    assert_eq!(
        handle
            .call(std::time::Instant::now() - Duration::from_millis(1), |_s| {
                Ok(())
            })
            .await
            .unwrap_err()
            .to_string(),
        "ACCOUNT_QUEUE_DEADLINE"
    );
    assert_eq!(handle.sql_timing().observed_jobs, 1);
    let (second, second_timing) = handle
        .call_with_timing(std::time::Instant::now() + Duration::from_secs(3), |_s| {
            Ok(2_u8)
        })
        .await
        .unwrap();
    assert_eq!(second, 2);
    assert_eq!(handle.sql_timing().observed_jobs, 2);
    assert_eq!(
        handle.sql_timing().last_sql_owner_ms,
        second_timing.sql_owner_ms
    );
    // The first job owns its immutable timing even after later work.
    assert_eq!(first, account);
    assert_eq!(counters.last_sql_owner_ms, timing.sql_owner_ms);
}
