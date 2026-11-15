//! Read the original plan/identity and lots without crossing account namespaces.
use super::*;

fn seed(source: &mut AccountStore, plan_hash: &str, qty: i64, price: i64) {
    intent_with_plan_hash(source, plan_hash);
    acknowledged_entry_intent(source);
    source
        .ingest_attributed_fill(
            Fill {
                fill_id: "same-fill".into(),
                instrument_id: "BTC-USDT-SWAP".into(),
                trade_id: "same-trade".into(),
                exchange_order_id: "venue-order".into(),
                intent_id: Some("intent".into()),
                time: 5,
                qty: Decimal::from(qty),
                price: Decimal::from(price),
                fee: Decimal::ZERO,
                currency: "USDT".into(),
                side: "buy".into(),
            },
            FillEffect::Open {
                side: "long".into(),
            },
            None,
        )
        .unwrap();
}

#[test]
fn risk_book_contract_trade_identity_lookup_and_lots_are_scoped_read_only() {
    let mut a = store_with_account("trade-read-A");
    let mut b = store_with_account("trade-read-B");
    seed(&mut a, "plan-A", 1, 100);
    seed(&mut b, "plan-B", 2, 200);
    let original = a
        .lookup_trade_intent(&TradeIntentLookup::IdempotencyKey("request".into()))
        .unwrap()
        .unwrap();
    let ap = a.path.clone();
    let bp = b.path.clone();
    drop(a);
    drop(b);
    let sa = Connection::open_with_flags(ap, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let sb = Connection::open_with_flags(bp, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let file = path();
    let mut db = Connection::open(&file).unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    super::super::sql::unified_financial::importer::import_stopped_accounts_into_empty_finance(
        &mut db,
        "installation",
        &[&sa, &sb],
    )
    .unwrap();
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .unwrap();
    let before = tx.total_changes();
    for (account, hash, qty, price) in [
        ("trade-read-A", "plan-A", 1, 100),
        ("trade-read-B", "plan-B", 2, 200),
    ] {
        let scope = Some(("installation", account));
        for lookup in [
            TradeIntentLookup::IntentId("intent".into()),
            TradeIntentLookup::IdempotencyKey("request".into()),
            TradeIntentLookup::ClientOrderId("stableclient".into()),
            TradeIntentLookup::ExchangeOrderId("venue-order".into()),
            TradeIntentLookup::Reference("request".into()),
            TradeIntentLookup::Reference("stableclient".into()),
            TradeIntentLookup::Reference("venue-order".into()),
            TradeIntentLookup::Reference("intent".into()),
        ] {
            let found = AccountStore::lookup_trade_intent_sql(&tx, scope, &lookup)
                .unwrap()
                .unwrap();
            assert_eq!(found.plan_hash, hash);
            assert_eq!(found.intent_id, "intent");
            assert_eq!(found.state, IntentState::Acknowledged);
            if account == "trade-read-A" {
                assert_eq!(
                    serde_json::to_value(&found).unwrap(),
                    serde_json::to_value(&original).unwrap()
                );
            }
        }
        assert!(
            AccountStore::lookup_trade_intent_sql(
                &tx,
                scope,
                &TradeIntentLookup::Reference("missing".into())
            )
            .unwrap()
            .is_none()
        );
        let lots = AccountStore::active_lots_sql(&tx, scope, "BTC-USDT-SWAP").unwrap();
        assert_eq!(lots.len(), 1);
        assert_eq!(lots[0].remaining_qty, Decimal::from(qty));
        assert_eq!(lots[0].cost, Decimal::from(price));
        let census = AccountStore::census_snapshot_sql(
            &tx,
            "installation",
            account,
            AccountCensusRequest {
                positions: vec![("BTC-USDT-SWAP".into(), "long".into())],
                client_order_ids: vec!["stableclient".into(), "missing".into()],
                terminal_peer_release_intent: None,
                limit: 100,
            },
            10,
        )
        .unwrap();
        assert_eq!(census.observed_at_ms, 10);
        assert_eq!(
            census.positions[0].lots[0].remaining_qty,
            Decimal::from(qty)
        );
        assert_eq!(census.positions[0].owned_proofs.len(), 1);
        assert_eq!(census.pending_orders[0].as_ref().unwrap().plan_hash, hash);
        assert!(census.pending_orders[1].is_none());
        assert_eq!(census.intents.len(), 1);
        assert_eq!(census.intents[0].0.plan_hash, hash);
        assert_eq!(census.pending_central, vec!["intent"]);
        assert!(census.terminal_peer_sources.is_empty());
        assert_eq!(
            census.policy,
            AccountStore::account_policy_sql(&tx, scope).unwrap()
        );
    }
    assert!(
        AccountStore::lookup_trade_intent_sql(
            &tx,
            Some(("installation", "missing")),
            &TradeIntentLookup::IntentId("intent".into())
        )
        .is_err()
    );
    assert!(matches!(
        AccountStore::find_intent_sql(
            &tx,
            Some(("installation", "trade-read-A")),
            "intent_id OR 1=1",
            "intent"
        ),
        Err(StoreError::Invalid("INTENT_IDENTITY_COLUMN_INVALID"))
    ));
    assert_eq!(
        tx.total_changes(),
        before,
        "lookups cannot create facts or advance financial state"
    );
    tx.commit().unwrap();
    drop(db);
    let db = Connection::open(file).unwrap();
    assert_eq!(
        AccountStore::lookup_trade_intent_sql(
            &db,
            Some(("installation", "trade-read-B")),
            &TradeIntentLookup::Reference("request".into())
        )
        .unwrap()
        .unwrap()
        .plan_hash,
        "plan-B"
    );
}
