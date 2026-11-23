use super::*;

#[test]
fn t10_08_later_live_stop_get_cannot_clear_durable_protection_incident() {
    for reason in [
        "PROTECTION_PENDING_OR_FAILED",
        "MANAGED_AGGREGATE_PROTECTION_UNVERIFIED",
    ] {
        assert!(protection_incident_still_unresolved(Some(reason), false));
        assert!(
            !protection_incident_still_unresolved(Some(reason), true),
            "independently signed flat position is a different proof"
        );
    }
    assert!(!protection_incident_still_unresolved(None, false));
    assert!(!protection_incident_still_unresolved(
        Some("UNRELATED_LOCAL_ADMISSION_DENIAL"),
        false
    ));
}

#[tokio::test]
async fn t10_08_emergency_policy_is_bounded_before_financial_capture() {
    let first =
        bounded_protection_before_finance(Instant::now(), Duration::from_millis(50), async {
            Ok(json!({"kind":"source_bound_close"}))
        })
        .await;
    assert_eq!(first["state"], "attempted");
    assert_eq!(first["action"]["kind"], "source_bound_close");
    assert_eq!(first["phase"], "before_financial_capture");

    // A deferred source/operation cannot consume the whole finance
    // deadline before we report a fail-closed protective blocker.
    let began = Instant::now();
    let blocked = bounded_protection_before_finance(began, Duration::from_millis(25), async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(json!({"kind":"source_bound_close"}))
    })
    .await;
    assert_eq!(blocked["state"], "blocked");
    assert_eq!(
        blocked["reason_code"],
        "EMERGENCY_ACTION_DEADLINE_OR_OUTCOME_UNKNOWN"
    );
    assert_eq!(blocked["retry_create"], false);
    assert!(began.elapsed() < Duration::from_millis(200));

    let local =
        bounded_protection_before_finance(Instant::now(), Duration::from_millis(50), async {
            Err("ACCOUNT_MUTATION_BUSY".to_owned())
        })
        .await;
    assert_eq!(local["state"], "blocked");
    assert_eq!(local["reason_code"], "ACCOUNT_MUTATION_BUSY");
    assert_eq!(local["retry_create"], false);
}

#[test]
fn t10_08_emergency_close_requires_same_intent_signed_sql_lots() {
    let mut i = StoredIntent {
        intent_id: "intent-a".into(),
        client_order_id: "client-a".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_hash: "a".repeat(64),
        idempotency_key: "key-a".into(),
        request_hash: "b".repeat(64),
        state: IntentState::Filled,
        revision: 2,
        deadline: 20,
        policy_revision: 1,
        risk_increase: true,
        exchange_order_id: Some("venue-a".into()),
        incident: Some("PROTECTION_PENDING_OR_FAILED".into()),
        plan: okx_trade_execution::IntentPlan {
            quantity: Decimal::ONE,
            side: "buy".into(),
            position_side: "long".into(),
            ..Default::default()
        },
    };
    let one = Decimal::ONE;
    let half = Decimal::new(5, 1);
    assert_eq!(
        verified_emergency_owned_exposure(&i, one, one, [("long", half)]),
        Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into()),
        "a late fill must be in the same intent's SQL lots before Close"
    );
    assert_eq!(
        verified_emergency_owned_exposure(&i, one, one, [("long", one)]),
        Ok(one)
    );
    assert_eq!(
        verified_emergency_owned_exposure(&i, one, one, std::iter::empty()),
        Err("NO_ATTRIBUTED_POSITION_TO_CLOSE".into())
    );
    assert_eq!(
        verified_emergency_owned_exposure(&i, half, one, [("long", one)]),
        Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
    );
    assert_eq!(
        verified_emergency_owned_exposure(&i, one, half, [("long", one)]),
        Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
    );
    assert_eq!(
        verified_emergency_owned_exposure(&i, one, one, [("short", half)]),
        Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
    );
    assert_eq!(
        verified_emergency_owned_exposure(&i, one, Decimal::ZERO, [("long", half)]),
        Err("EMERGENCY_EXPOSURE_SOURCE_UNVERIFIED".into())
    );
    i.plan.position_side = "short".into();
    assert_eq!(
        verified_emergency_owned_exposure(&i, one, one, [("long", half)]),
        Err("EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH".into())
    );
}

#[test]
fn t09_public_ack_without_fill_bill_evidence_cannot_complete_finance() {
    let missing = json!({"state":"incomplete","reason_code":"ENTRY_FILL_EVIDENCE_EMPTY"});
    let bill_pending = json!({"state":"native_fills_verified_bill_pending"});
    let verified = json!({"state":"verified_native_fill_and_bill_fee_sources"});
    assert!(entry_fill_finance_pending(true, false, &missing));
    assert!(entry_fill_finance_pending(true, false, &bill_pending));
    assert!(!entry_fill_finance_pending(true, false, &verified));
    // A venue-confirmed zero-fill cancellation is a distinct terminal
    // proof, and still needs the separate central_release pending gate.
    assert!(!entry_fill_finance_pending(true, true, &missing));
    assert!(!entry_fill_finance_pending(false, false, &missing));
}

#[test]
fn e2_terminal_cursors_never_trust_invalid_exchange_order_time() {
    assert_eq!(terminal_order_update_time(None), Ok(None));
    let order =
        |t: &str| serde_json::from_value::<okx_api::TradeOrderDetails>(json!({"uTime":t})).unwrap();
    assert_eq!(terminal_order_update_time(Some(&order("16"))), Ok(Some(16)));
    for invalid in ["", "invalid", "0", "-1", "9223372036854775808"] {
        assert_eq!(
            terminal_order_update_time(Some(&order(invalid))),
            Err("TERMINAL_ORDER_UPDATE_TIME_INVALID".into())
        );
    }
}

#[test]
fn e1_native_fills_require_independent_exact_venue_quantity_and_time() {
    let intent = StoredIntent {
        intent_id: "intent".into(),
        client_order_id: "client".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_hash: "a".repeat(64),
        idempotency_key: "idem".into(),
        request_hash: "request".into(),
        state: IntentState::Filled,
        revision: 1,
        deadline: 15,
        policy_revision: 1,
        risk_increase: true,
        exchange_order_id: Some("venue-order".into()),
        incident: None,
        plan: okx_trade_execution::IntentPlan {
            quantity: Decimal::ONE,
            side: "buy".into(),
            ..Default::default()
        },
    };
    let source = okx_trade_execution::IntentFillCostEvidence {
        intent_id: intent.intent_id.clone(),
        plan_hash: intent.plan_hash.clone(),
        exchange_order_id: "venue-order".into(),
        reservation_key: "central-key".into(),
        fill_count: 2,
        filled_quantity: Decimal::ONE,
        covered_through: 10,
        gross_notional_usdt: Decimal::from(94),
        conservative_fee_usdt: Decimal::new(3, 2),
        source_digest: "b".repeat(64),
    };
    let order = okx_api::TradeOrderDetails {
        instrument_id: intent.instrument_id.clone(),
        order_id: source.exchange_order_id.clone(),
        client_order_id: intent.client_order_id.clone(),
        side: "buy".into(),
        position_side: "long".into(),
        trade_mode: "cross".into(),
        order_type: "limit".into(),
        price: "100".into(),
        size: "1".into(),
        accumulated_fill_size: "1".into(),
        average_fill_price: "94".into(),
        state: "filled".into(),
        creation_time_ms: "3".into(),
        update_time_ms: "10".into(),
    };
    assert!(verify_venue_fill_coverage(&intent, &source, &order).is_ok());
    assert_eq!(verified_order_readback(&intent, &order), Ok(Decimal::ONE));
    assert_eq!(
        verified_terminal_order_update_time(&intent, Some(&order)),
        Ok(10)
    );
    assert_eq!(
        verified_terminal_order_update_time(&intent, None),
        Err("TERMINAL_ORDER_READBACK_MISSING".into())
    );
    let mut changed = order.clone();
    changed.state = "partially_filled".into();
    assert_eq!(
        verified_terminal_order_update_time(&intent, Some(&changed)),
        Err("TERMINAL_ORDER_STATE_UNVERIFIED".into())
    );
    changed = order.clone();
    changed.order_id = "other".into();
    assert_eq!(
        verified_terminal_order_update_time(&intent, Some(&changed)),
        Err("TERMINAL_ORDER_IDENTITY_MISMATCH".into())
    );
    changed = order.clone();
    changed.client_order_id = "wrong-client".into();
    assert_eq!(
        verified_order_readback(&intent, &changed),
        Err("ORDER_READBACK_IDENTITY_MISMATCH".into())
    );
    changed = order.clone();
    changed.order_id = "other-venue-order".into();
    assert_eq!(
        verified_order_readback(&intent, &changed),
        Err("ORDER_READBACK_IDENTITY_MISMATCH".into())
    );
    changed = order.clone();
    changed.size = "2".into();
    assert_eq!(
        verified_order_readback(&intent, &changed),
        Err("ORDER_READBACK_PLAN_MISMATCH".into())
    );
    changed = order.clone();
    changed.accumulated_fill_size = "1.1".into();
    assert_eq!(
        verified_order_readback(&intent, &changed),
        Err("ORDER_READBACK_FILLED_SIZE_INVALID".into())
    );
    assert_eq!(
        verified_terminal_order_update_time(&intent, Some(&changed)),
        Err("TERMINAL_ORDER_IDENTITY_OR_SIZE_UNVERIFIED".into())
    );
    let mut changed = order.clone();
    changed.accumulated_fill_size = "0.9".into();
    assert_eq!(
        verify_venue_fill_coverage(&intent, &source, &changed),
        Err("ENTRY_FILL_VENUE_QUANTITY_MISMATCH".into())
    );
    changed = order.clone();
    changed.update_time_ms = "11".into();
    assert_eq!(
        verify_venue_fill_coverage(&intent, &source, &changed),
        Err("ENTRY_FILL_VENUE_TIME_UNCOVERED".into())
    );
    changed = order.clone();
    changed.order_id = "other".into();
    assert_eq!(
        verify_venue_fill_coverage(&intent, &source, &changed),
        Err("ENTRY_FILL_VENUE_IDENTITY_MISMATCH".into())
    );
    changed = order.clone();
    changed.client_order_id = "other".into();
    assert_eq!(
        verify_venue_fill_coverage(&intent, &source, &changed),
        Err("ENTRY_FILL_VENUE_IDENTITY_MISMATCH".into())
    );
    changed = order.clone();
    changed.update_time_ms = "invalid".into();
    assert_eq!(
        verify_venue_fill_coverage(&intent, &source, &changed),
        Err("ENTRY_FILL_VENUE_TIME_UNVERIFIED".into())
    );
    changed = order;
    changed.state = "unknown".into();
    assert_eq!(
        verify_venue_fill_coverage(&intent, &source, &changed),
        Err("ENTRY_FILL_VENUE_STATE_UNKNOWN".into())
    );
}

#[test]
fn e2_protective_readback_requires_exact_venue_exposure_and_opposite_side() {
    let one = Decimal::ONE;
    let half = dec("0.5").unwrap();
    assert!(protection_covers_live_position(
        "buy", "sell", one, one, "1"
    ));
    assert!(protection_covers_live_position(
        "sell", "buy", one, one, "1"
    ));
    assert!(!protection_covers_live_position(
        "buy", "buy", one, one, "1"
    ));
    assert!(!protection_covers_live_position(
        "buy", "close", one, one, "1"
    ));
    assert!(!protection_covers_live_position(
        "buy", "sell", one, half, "1"
    ));
    assert!(!protection_covers_live_position(
        "buy", "sell", one, one, "0.5"
    ));
    assert!(!protection_covers_live_position(
        "buy", "sell", one, one, "invalid"
    ));
    assert!(!protection_covers_live_position(
        "buy",
        "sell",
        Decimal::ZERO,
        one,
        "1"
    ));
}

#[test]
fn t09_4_add_attached_stop_must_cover_exact_increment_not_old_position() {
    let add = dec("0.2").unwrap();
    let net = dec("0.5").unwrap();
    assert!(protection_covers_managed_increment(
        "buy", "sell", add, net, "0.2"
    ));
    assert!(!protection_covers_managed_increment(
        "buy", "sell", add, net, "0.5"
    ));
    assert!(!protection_covers_managed_increment(
        "buy", "sell", add, net, "0.19"
    ));
    assert!(!protection_covers_managed_increment(
        "buy", "buy", add, net, "0.2"
    ));
    assert!(!protection_covers_managed_increment(
        "buy",
        "sell",
        Decimal::ZERO,
        net,
        "0.2"
    ));
    assert!(!protection_covers_managed_increment(
        "buy",
        "sell",
        dec("0.6").unwrap(),
        net,
        "0.6"
    ));
}

#[test]
fn t10_09_position_ahead_of_sql_lot_requires_signed_old_stop_and_stays_pending() {
    // A causal partial Reduce can appear in venue positions before its
    // immutable fill/bill updates the SQL lot. No new protection defect
    // is proved by the difference alone; a missing or wrong stop still
    // fails closed and the existing durable incident is never cleared.
    let intent = StoredIntent {
        intent_id: "original".into(),
        client_order_id: "c".repeat(32),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_hash: "a".repeat(64),
        idempotency_key: "original-key".into(),
        request_hash: "original-request".into(),
        state: IntentState::Cancelled,
        revision: 3,
        deadline: 100,
        policy_revision: 1,
        risk_increase: true,
        exchange_order_id: Some("original-order".into()),
        incident: None,
        plan: okx_trade_execution::IntentPlan {
            side: "buy".into(),
            position_side: "long".into(),
            quantity: dec("0.1").unwrap(),
            stop_price: Some(dec("900").unwrap()),
            ..Default::default()
        },
    };
    let original = okx_api::TradeAlgoOrderDetails {
        instrument_id: intent.instrument_id.clone(),
        algo_order_id: "900000004".into(),
        client_order_id: format!("s{}", "c".repeat(31)),
        state: "live".into(),
        stop_loss_trigger_price_type: "mark".into(),
        stop_loss_trigger_price: "900".into(),
        size: "0.04".into(),
        side: "sell".into(),
        position_side: "long".into(),
        ..serde_json::from_value::<okx_api::TradeAlgoOrderDetails>(json!({})).unwrap()
    };
    let sql_lot = dec("0.04").unwrap();
    let newer_venue_position = dec("0.02").unwrap();
    assert!(signed_position_below_sql_lot_with_live_old_stop(
        &intent,
        sql_lot,
        newer_venue_position,
        &original
    ));
    assert!(!protection_covers_managed_increment(
        "buy",
        "sell",
        sql_lot,
        newer_venue_position,
        &original.size
    ));
    for bad_size in ["0.02", "0.05", "invalid"] {
        let mut invalid = original.clone();
        invalid.size = bad_size.into();
        assert!(!signed_position_below_sql_lot_with_live_old_stop(
            &intent,
            sql_lot,
            newer_venue_position,
            &invalid
        ));
    }
    let mut missing = original.clone();
    missing.state = "canceled".into();
    assert!(!signed_position_below_sql_lot_with_live_old_stop(
        &intent,
        sql_lot,
        newer_venue_position,
        &missing
    ));
    let mut alien = original.clone();
    alien.client_order_id = "other-account-stop".into();
    assert!(!signed_position_below_sql_lot_with_live_old_stop(
        &intent,
        sql_lot,
        newer_venue_position,
        &alien
    ));
    assert!(!signed_position_below_sql_lot_with_live_old_stop(
        &intent,
        sql_lot,
        Decimal::ZERO,
        &original
    ));
    assert!(!signed_position_below_sql_lot_with_live_old_stop(
        &intent, sql_lot, sql_lot, &original
    ));
    assert!(!signed_position_below_sql_lot_with_live_old_stop(
        &intent,
        sql_lot,
        dec("0.05").unwrap(),
        &original
    ));
}

#[test]
fn e2_shared_protection_check_is_strict_about_live_venue_state_and_size() {
    let intent = StoredIntent {
        intent_id: "intent".into(),
        client_order_id: "c".repeat(32),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_hash: "a".repeat(64),
        idempotency_key: "key".into(),
        request_hash: "req".into(),
        state: IntentState::Filled,
        revision: 1,
        deadline: 100,
        policy_revision: 1,
        risk_increase: true,
        exchange_order_id: Some("order".into()),
        incident: None,
        plan: okx_trade_execution::IntentPlan {
            side: "buy".into(),
            position_side: "long".into(),
            quantity: Decimal::ONE,
            stop_price: Some(dec("96").unwrap()),
            ..Default::default()
        },
    };
    let p = okx_api::TradeAlgoOrderDetails {
        instrument_id: "BTC-USDT-SWAP".into(),
        algo_order_id: "algo".into(),
        client_order_id: format!("s{}", "c".repeat(31)),
        state: "live".into(),
        stop_loss_trigger_price_type: "mark".into(),
        stop_loss_trigger_price: "96".into(),
        size: "1".into(),
        side: "sell".into(),
        position_side: "net".into(),
        ..serde_json::from_value::<okx_api::TradeAlgoOrderDetails>(json!({})).unwrap()
    };
    assert!(verified_live_protection(&intent, Decimal::ONE, &p).is_ok());
    let mut changed = p.clone();
    changed.size = "0.5".into();
    assert_eq!(
        verified_live_protection(&intent, Decimal::ONE, &changed),
        Err("PROTECTION_LIVE_SOURCE_UNVERIFIED".into())
    );
    changed = p.clone();
    changed.state = "pause".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.side = "buy".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.stop_loss_trigger_price_type = "last".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.stop_loss_trigger_price = "95".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.client_order_id = "wrong".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.position_side = "short".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.position_side.clear();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.position_side = "long".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_ok());
    changed = p.clone();
    changed.failure_code = "failure".into();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    changed = p.clone();
    changed.algo_order_id.clear();
    assert!(verified_live_protection(&intent, Decimal::ONE, &changed).is_err());
    assert!(verified_live_protection(&intent, Decimal::ZERO, &p).is_err());
    let mut short_client = intent;
    short_client.client_order_id = "short".into();
    assert_eq!(
        verified_live_protection(&short_client, Decimal::ONE, &p),
        Err("PROTECTION_CLIENT_ID_INVALID".into())
    );
}

#[test]
fn e2_position_source_does_not_treat_malformed_exposure_as_flat() {
    assert_eq!(
        verified_position_quantity(["0", "0"].into_iter()),
        Ok(Decimal::ZERO)
    );
    assert_eq!(
        verified_position_quantity(["0.5", "-0.25"].into_iter()),
        Ok(dec("0.75").unwrap())
    );
    assert_eq!(
        verified_position_quantity(["0", "unreadable"].into_iter()),
        Err("POSITION_QUANTITY_UNVERIFIED".into())
    );
    assert_eq!(
        verified_position_quantity(["79228162514264337593543950335", "1"].into_iter()),
        Err("POSITION_QUANTITY_OVERFLOW".into())
    );
}

#[test]
fn e2_position_direction_is_identical_for_net_status_census_and_recovery() {
    let one = Decimal::ONE;
    let half = dec("0.5").unwrap();
    assert_eq!(
        verified_venue_position_direction("net", "1"),
        Ok(Some(("long", one)))
    );
    assert_eq!(
        verified_venue_position_direction("net", "-0.5"),
        Ok(Some(("short", half)))
    );
    assert_eq!(
        verified_intent_position_quantity("long", [("net", "0.25"), ("net", "0.75")]),
        Ok(one)
    );
    assert_eq!(
        verified_intent_position_quantity("short", [("net", "-0.5")]),
        Ok(half)
    );
    assert_eq!(
        verified_intent_position_quantity("long", [("net", "-0.5")]),
        Err("VENUE_POSITION_OPPOSITE_EXPOSURE_UNVERIFIED".into())
    );
    assert_eq!(
        verified_intent_position_quantity("short", [("long", "0")]),
        Ok(Decimal::ZERO)
    );
    assert_eq!(
        verified_venue_position_direction("long", "1"),
        Ok(Some(("long", one)))
    );
    assert_eq!(
        verified_venue_position_direction("short", "0.5"),
        Ok(Some(("short", half)))
    );
    for side in ["long", "short", "net"] {
        assert_eq!(verified_venue_position_direction(side, "0"), Ok(None));
    }
    assert_eq!(
        verified_venue_position_direction("long", "-1"),
        Err("VENUE_POSITION_SIDE_QUANTITY_MISMATCH".into())
    );
    assert_eq!(
        verified_venue_position_direction("unknown", "0"),
        Err("VENUE_POSITION_SIDE_UNVERIFIED".into())
    );
    assert_eq!(
        verified_venue_position_direction("unknown", "1"),
        Err("VENUE_POSITION_SIDE_UNVERIFIED".into())
    );
    assert_eq!(
        verified_venue_position_direction("net", "bad"),
        Err("VENUE_POSITION_QUANTITY_UNVERIFIED".into())
    );
}

#[test]
fn e2_cleanup_does_not_remove_protection_for_any_unverified_hedge_side() {
    let instrument = "BTC-USDT-SWAP";
    assert!(
        verify_instrument_is_flat(
            instrument,
            [
                (instrument, "0"),
                (instrument, "0"),
                ("ETH-USDT-SWAP", "20"),
            ]
        )
        .is_ok()
    );
    assert_eq!(
        verify_instrument_is_flat(instrument, [(instrument, "0"), (instrument, "-0.2"),]),
        Err("PROTECTION_STILL_REQUIRED".into())
    );
    assert_eq!(
        verify_instrument_is_flat(instrument, [(instrument, "unreadable")]),
        Err("POSITION_QUANTITY_UNVERIFIED".into())
    );
    assert_eq!(
        verify_instrument_is_flat(
            instrument,
            [
                (instrument, "79228162514264337593543950335"),
                (instrument, "1"),
            ]
        ),
        Err("POSITION_QUANTITY_OVERFLOW".into())
    );
}

#[test]
fn e2_replace_requires_independent_terminal_zero_fill_order_readback() {
    let intent = StoredIntent {
        intent_id: "intent".into(),
        client_order_id: "client".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_hash: "a".repeat(64),
        idempotency_key: "idem".into(),
        request_hash: "request".into(),
        state: IntentState::Cancelled,
        revision: 2,
        deadline: 40,
        policy_revision: 1,
        risk_increase: true,
        exchange_order_id: Some("venue".into()),
        incident: None,
        plan: okx_trade_execution::IntentPlan {
            quantity: Decimal::ONE,
            side: "buy".into(),
            ..Default::default()
        },
    };
    let order = okx_api::TradeOrderDetails {
        instrument_id: intent.instrument_id.clone(),
        order_id: "venue".into(),
        client_order_id: "client".into(),
        side: "buy".into(),
        position_side: "long".into(),
        trade_mode: "cross".into(),
        order_type: "limit".into(),
        price: "100".into(),
        size: "1".into(),
        accumulated_fill_size: "0".into(),
        average_fill_price: "".into(),
        state: "canceled".into(),
        creation_time_ms: "1".into(),
        update_time_ms: "2".into(),
    };
    assert!(verify_empty_canceled_order_for_replace(&intent, &order).is_ok());
    let mut partial = order.clone();
    partial.accumulated_fill_size = "0.1".into();
    assert_eq!(
        verify_empty_canceled_order_for_replace(&intent, &partial),
        Err("REPLACE_PARTIAL_FILL_REQUIRES_EXPLICIT_CLOSE".into())
    );
    let mut nonterminal = order.clone();
    nonterminal.state = "live".into();
    assert_eq!(
        verify_empty_canceled_order_for_replace(&intent, &nonterminal),
        Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
    );
    let mut wrong_id = order.clone();
    wrong_id.order_id = "other".into();
    assert_eq!(
        verify_empty_canceled_order_for_replace(&intent, &wrong_id),
        Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
    );
    let mut missing_time = order.clone();
    missing_time.update_time_ms = "0".into();
    assert_eq!(
        verify_empty_canceled_order_for_replace(&intent, &missing_time),
        Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
    );
    let mut malformed = order;
    malformed.accumulated_fill_size = "unreadable".into();
    assert_eq!(
        verify_empty_canceled_order_for_replace(&intent, &malformed),
        Err("REPLACE_CANCELED_READBACK_UNPROVEN".into())
    );
}

#[test]
fn e2_close_lots_require_one_side_positive_amounts_and_exact_sum() {
    assert_eq!(
        verified_close_lot_exposure([("long", dec("0.2").unwrap()), ("long", dec("0.3").unwrap())])
            .map(|(_, quantity)| quantity),
        Ok(dec("0.5").unwrap())
    );
    assert!(verified_close_lot_exposure([("short", Decimal::ONE)]).is_ok());
    assert_eq!(
        verified_close_lot_exposure(std::iter::empty()),
        Err("NO_ATTRIBUTED_POSITION_TO_CLOSE".into())
    );
    assert_eq!(
        verified_close_lot_exposure([("long", Decimal::ONE), ("short", Decimal::ONE)]),
        Err("CLOSE_LOT_MIXED_DIRECTIONS".into())
    );
    assert_eq!(
        verified_close_lot_exposure([("long", Decimal::ZERO)]),
        Err("CLOSE_LOT_QUANTITY_UNVERIFIED".into())
    );
    assert_eq!(
        verified_close_lot_exposure([("long", -Decimal::ONE)]),
        Err("CLOSE_LOT_QUANTITY_UNVERIFIED".into())
    );
    assert_eq!(
        verified_close_lot_exposure([("unknown", Decimal::ONE)]),
        Err("CLOSE_LOT_SIDE_UNATTESTED".into())
    );
    assert_eq!(
        verified_close_lot_exposure([("long", Decimal::MAX), ("long", Decimal::ONE)]),
        Err("CLOSE_LOT_QUANTITY_OVERFLOW".into())
    );
}

#[test]
fn e1_status_never_promotes_known_quote_into_all_in_entry_permission() {
    let empty = entry_cost_status(None, None).unwrap();
    assert_eq!(empty["all_in_entry_cost_proven"], false);
    assert!(empty["entry_budget_upper_reporting"].is_null());
    assert_eq!(empty["reason_codes"][0], "ENTRY_QUOTE_NOT_RECORDED");
    let quote = okx_trade_execution::EntryQuoteEvidence {
        schema: "okx.entry-known-quote/v1".into(),
        account_key: "okx:real:test".into(),
        installation_id: "install".into(),
        intent_id: "intent".into(),
        plan_hash: "a".repeat(64),
        policy_revision: 1,
        instrument_id: "BTC-USDT-SWAP".into(),
        quantity: dec("0.01").unwrap(),
        limit_price: dec("100").unwrap(),
        contract_value: Decimal::ONE,
        fee_rate: dec("0.0005").unwrap(),
        maker_rate: "-0.0002".into(),
        taker_rate: "0.0005".into(),
        fee_level: "Lv1".into(),
        fee_exchange_at: 1,
        usd_per_usdt: Decimal::ONE,
        notional_usdt: Decimal::ONE,
        fee_usdt: dec("0.0005").unwrap(),
        total_usdt: dec("1.0005").unwrap(),
        total_usd: dec("1.0005").unwrap(),
        fee_generation: format!("sha256:{}", "b".repeat(64)),
        reference_generation: "ref".into(),
        fee_received_at: 1,
        conversion_digest: "c".repeat(64),
        conversion_received_at: 1,
        observed_at: 1,
    };
    let view = entry_cost_status(Some(quote.clone()), None).unwrap();
    assert_eq!(view["state"], "historical_known_cost_only");
    assert_eq!(view["known_entry_total_usd"], "1.0005");
    assert_eq!(view["all_in_entry_cost_proven"], false);
    assert!(view["entry_budget_upper_reporting"].is_null());
    assert_eq!(view["reason_codes"].as_array().unwrap().len(), 5);
    let admitted = okx_trade_execution::EntryAdmissionEvidence {
        schema: "okx.entry-admission/v2".into(),
        account_key: quote.account_key.clone(),
        installation_id: quote.installation_id.clone(),
        intent_id: quote.intent_id.clone(),
        plan_hash: quote.plan_hash.clone(),
        quote_digest: "c".repeat(64),
        admission_usd_per_usdt: Decimal::ONE,
        other_entry_costs_upper_usd: Decimal::ONE,
        entry_upper_usd: Decimal::new(20005, 4),
        entry_proof_digest: "d".repeat(64),
        observed_at: quote.observed_at,
    };
    let shown = entry_cost_status(Some(quote.clone()), Some(admitted.clone())).unwrap();
    assert_eq!(shown["all_in_entry_cost_proven"], true);
    assert_eq!(shown["entry_budget_upper_reporting"], "2.0005");
    assert_eq!(shown["authorization"], "COST_PROOF_ONLY_NOT_POST_AUTHORITY");
    let mut mismatched = admitted;
    mismatched.plan_hash = "different".into();
    assert_eq!(
        entry_cost_status(Some(quote.clone()), Some(mismatched)).unwrap_err(),
        "ENTRY_ADMISSION_READBACK_MISMATCH"
    );
    let mut tampered = quote;
    tampered.total_usd = Decimal::ONE;
    assert_eq!(
        entry_cost_status(Some(tampered), None).unwrap_err(),
        "ENTRY_QUOTE_READBACK_MISMATCH"
    );
}
