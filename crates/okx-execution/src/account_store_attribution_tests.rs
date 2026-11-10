//! #501: one plan/decision/reservation owner at the real account SQL boundary.
use super::*;

fn request(
    s: &mut AccountStore,
    owner: &str,
    decision_owner: Option<&str>,
) -> (Decision, NewIntent) {
    if owner != "manual" {
        s.mutate_instance(
            "create-owner",
            "create-body",
            "create",
            &StrategyInstance {
                instance_id: owner.into(),
                definition_id: "fixture".into(),
                definition_version: "1".into(),
                parameters: "{}".into(),
                enabled: true,
                policy_revision: 1,
                budget: Decimal::from(10),
                last_decision_key: None,
            },
            None,
            2,
        )
        .unwrap();
    }
    let definition = s.strategy_definition(owner).unwrap();
    (
        Decision {
            decision_id: "owner-decision".into(),
            instance_id: decision_owner.map(str::to_owned),
            decision_at: 3,
            input_digest: "owner-input".into(),
            policy_revision: 1,
            code_version: "owner-test".into(),
        },
        NewIntent {
            plan: IntentPlan {
                strategy_definition_id: definition.definition_id,
                strategy_definition_version: definition.version,
                strategy_parameters_digest: definition.parameters_digest,
                strategy_instance: owner.into(),
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
                planned_loss_usd: Decimal::from(5),
                source_generation: 1,
            },
            intent_id: "owner-intent".into(),
            idempotency_key: "owner-request".into(),
            request_hash: "owner-body".into(),
            client_order_id: "owner-client".into(),
            deadline: 100,
            plan_hash: "owner-plan".into(),
            policy_revision: 1,
            risk_increase: true,
            instrument_id: "BTC-USDT-SWAP".into(),
        },
    )
}

fn receipt(s: &AccountStore, owner: Option<&str>) -> CentralReceipt {
    CentralReceipt {
        reservation_key: "owner-reservation".into(),
        account_key: s.meta.account_key.clone(),
        installation_id: s.meta.installation_id.clone(),
        instance_id: owner.map(str::to_owned),
        intent_id: "owner-intent".into(),
        plan_hash: "owner-plan".into(),
        policy_revision: 1,
        receipt_hash: "owner-receipt".into(),
        requested_loss_usd: Decimal::from(5),
        conversion_digest: "owner-fx".into(),
    }
}

fn counts(s: &AccountStore) -> Vec<i64> {
    [
        "decisions",
        "intents",
        "intent_plans",
        "risk_receipts",
        "finance_entries",
        "operation_receipts",
    ]
    .into_iter()
    .map(|table| {
        s.connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    })
    .collect()
}

/// Independent rows accepted by the old NULL/named-plan contract. Seed an
/// existing database, not an alternate implementation of the operation under
/// test. Immutable triggers remain enabled; no historical row is rewritten.
fn historical_named_rows(s: &mut AccountStore, i: &NewIntent, reserved: bool) {
    let tx = s.connection.transaction().unwrap();
    tx.execute(
        "INSERT INTO decisions VALUES('owner-decision',NULL,3,'owner-input',1,'owner-test')",
        [],
    )
    .unwrap();
    tx.execute("INSERT INTO intents(intent_id,idempotency_key,request_hash,decision_id,state,revision,client_order_id,deadline,plan_hash,policy_revision,risk_increase,instrument_id,created_at,updated_at)
        VALUES('owner-intent','owner-request','owner-body','owner-decision',?1,?2,'owner-client',100,'owner-plan',1,1,'BTC-USDT-SWAP',3,3)",
        rusqlite::params![if reserved { "RESERVED" } else { "PREPARED" }, i64::from(reserved)]).unwrap();
    tx.execute("INSERT INTO intent_plans VALUES('owner-intent','open','buy','1','100','95','strategy-a','5',1,'long',1,'1',0,'USDT',?1,?2,?3)",
        rusqlite::params![i.plan.strategy_definition_id, i.plan.strategy_definition_version, i.plan.strategy_parameters_digest]).unwrap();
    if reserved {
        tx.execute("INSERT INTO risk_receipts VALUES('owner-reservation','owner-intent',?1,?2,NULL,'owner-plan',1,'owner-receipt','5','owner-fx','HELD')",
            rusqlite::params![s.meta.account_key, s.meta.installation_id]).unwrap();
    }
    tx.commit().unwrap();
}

#[test]
fn attribution_prepare_rejects_missing_named_owner_without_mutation() {
    let mut s = store();
    let (d, i) = request(&mut s, "strategy-a", None);
    let before = counts(&s);
    assert!(matches!(
        s.prepare(&d, &i),
        Err(StoreError::Invalid("STRATEGY_OWNER_MISMATCH"))
    ));
    assert_eq!(counts(&s), before);
    assert!(s.by_idempotency(&i.idempotency_key).unwrap().is_none());
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let s = AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(counts(&s), before);
    assert!(s.intent(&i.intent_id).unwrap().is_none());
}

#[test]
fn attribution_prepare_rejects_other_explicit_owner_without_mutation() {
    for owner in ["manual", "strategy-a"] {
        let mut s = store();
        let (d, i) = request(&mut s, owner, Some("strategy-b"));
        let before = counts(&s);
        assert!(matches!(
            s.prepare(&d, &i),
            Err(StoreError::Invalid("STRATEGY_OWNER_MISMATCH"))
        ));
        assert_eq!(counts(&s), before);
    }
}

#[test]
fn attribution_manual_and_strategy_keep_one_owner_through_reserve_and_reopen() {
    // NULL/manual remains the explicitly supported historical SQL representation.
    // Public Execute uses Some("manual"); no persisted NULL is rewritten.
    for (owner, decision_owner) in [
        ("manual", None),
        ("manual", Some("manual")),
        ("strategy-a", Some("strategy-a")),
    ] {
        let mut s = store();
        let (d, i) = request(&mut s, owner, decision_owner);
        s.prepare(&d, &i).unwrap();
        let r = receipt(&s, decision_owner);
        s.reserve(&r, 4).unwrap();
        let before = counts(&s);
        assert_eq!(s.prepare(&d, &i).unwrap().intent_id, i.intent_id);
        s.reserve(&r, 5).unwrap();
        assert_eq!(counts(&s), before);
        let path = s.path.clone();
        let meta = s.meta.clone();
        drop(s);
        let mut s =
            AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
        assert_eq!(
            s.central_receipt(&i.intent_id)
                .unwrap()
                .unwrap()
                .instance_id
                .as_deref(),
            decision_owner
        );
        assert_eq!(
            s.intent(&i.intent_id)
                .unwrap()
                .unwrap()
                .plan
                .strategy_instance,
            owner
        );
        s.reserve(&r, 6).unwrap();
        assert_eq!(counts(&s), before);
    }
}

#[test]
fn attribution_reserve_rejects_receipt_owner_and_incoherent_legacy_plan() {
    for malformed_plan in [false, true] {
        let mut s = store();
        let (d, i) = request(&mut s, "strategy-a", Some("strategy-a"));
        if malformed_plan {
            historical_named_rows(&mut s, &i, false);
        } else {
            s.prepare(&d, &i).unwrap();
        }
        let r = receipt(
            &s,
            if malformed_plan {
                None
            } else {
                Some("strategy-b")
            },
        );
        let before = counts(&s);
        assert!(matches!(
            s.reserve(&r, 4),
            Err(StoreError::Invalid("CENTRAL_INSTANCE_MISMATCH"))
        ));
        assert_eq!(counts(&s), before);
        assert_eq!(
            s.intent(&i.intent_id).unwrap().unwrap().state,
            IntentState::Prepared
        );
        assert!(s.central_receipt(&i.intent_id).unwrap().is_none());
        let path = s.path.clone();
        let meta = s.meta.clone();
        drop(s);
        let s =
            AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
        assert_eq!(counts(&s), before);
        assert_eq!(
            s.intent(&i.intent_id).unwrap().unwrap().state,
            IntentState::Prepared
        );
    }
}

#[test]
fn attribution_existing_receipt_replays_before_new_owner_validation() {
    let mut s = store();
    let (mut d, i) = request(&mut s, "strategy-a", None);
    historical_named_rows(&mut s, &i, true);
    let r = receipt(&s, None);
    // A historical incoherent plan must remain inspectable/replayable. New
    // validation must never rewrite its owner, mint money or re-reserve it.
    d.instance_id = Some("different-caller".into());
    let before = counts(&s);
    let path = s.path.clone();
    let meta = s.meta.clone();
    drop(s);
    let mut s =
        AccountStore::open_existing(path, &meta.account_key, &meta.installation_id).unwrap();
    assert_eq!(
        s.prepare(&d, &i).unwrap().plan.strategy_instance,
        "strategy-a"
    );
    s.reserve(&r, 5).unwrap();
    assert_eq!(counts(&s), before);
    assert!(
        s.central_receipt(&i.intent_id)
            .unwrap()
            .unwrap()
            .instance_id
            .is_none()
    );
}
