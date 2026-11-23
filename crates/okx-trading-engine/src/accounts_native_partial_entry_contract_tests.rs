//! Fresh Native partial fills settle only independently linked signed fees.
//! Partial protection is not claimed. At the full quantity, public Status
//! verifies the original attached stop and keeps live exposure reserved.
use super::*;
use rust_decimal::Decimal;

use super::filled_fixture::*;

#[tokio::test]
async fn risk_book_contract_native_partial_sources_missing_bill_then_terminal_status_and_replay() {
    let mut fixture = fixture().await;
    let (market_stop, market_task, _socket, _public) = opened_market(&mut fixture).await;
    let account = fixture._account.clone();
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":now}])));
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &now);
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":INSTRUMENT,"tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900",
                "slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: envelope(json!([{"ordId":OPEN_ORDER,
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    fixture.feed.arm_lost_next_create_ack();
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let first = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .expect("original Native Execute before independently observed partial fills");
    assert_eq!(
        first["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{first}"
    );
    let entry = first["intent_id"].as_str().unwrap().to_owned();
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 2);
    // Ordinary periodic capture remains enabled throughout admission. Only
    // after Create do the named capture/Status phases own this finite feed.
    let timer_capture = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let initial = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let original = fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_trade_execution::TradeIntentLookup::Reference(entry.clone()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    let stop_client = format!("s{}", &original.client_order_id[..31]);

    // Recover the lost ACK through the production readback before history
    // capture. The real venue ID now attributes both fill and eventual bill.
    let partial_at = jiff::Timestamp::now().as_millisecond();
    order_cumulative(
        &fixture.feed,
        OPEN_ORDER,
        "buy",
        partial_at,
        "partially_filled",
        "0.04",
    );
    let (partial, observed) = fixture
        .trade
        .observe_intent_order(&account, original)
        .await
        .unwrap();
    assert_eq!(partial.state, okx_trade_execution::IntentState::PartiallyFilled);
    assert_eq!(observed.unwrap().accumulated_fill_size, "0.04");

    // 0.04 * 0.01 * 1000 = 0.4 USD, plus the real 0.0002 fee.
    // A raw fill's fee and balance movement cannot invent its absent bill.
    snapshot(&fixture.feed, "99.9998", "0.04");
    history_unbilled_amount(
        &fixture.feed,
        (OPEN_ORDER, "native-partial-first", "buy", partial_at),
        "0.04",
        "-0.0002",
    );
    capture(&fixture).await;
    let through = covered_through(&fixture).await;
    let missing_fee = fixture
        .owner
        .intent_fee_bill_evidence(
            Instant::now() + Duration::from_secs(3),
            fixture.config.account_key.wire(),
            entry.clone(),
            through,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(missing_fee, okx_trade_execution::StoreError::Invalid(reason) if reason.contains("BILL")),
        "an absent signed bill must remain pending, never a zero fee"
    );
    let missing_settlement = fixture
        .owner
        .bind_partial_entry_settlement(
            Instant::now() + Duration::from_secs(3),
            fixture.config.account_key.wire(),
            entry.clone(),
            partial_at,
            Decimal::new(4, 2),
            through,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(missing_settlement, okx_trade_execution::StoreError::Invalid(reason) if reason.contains("BILL")),
        "source settlement cannot bypass the missing independent bill"
    );
    let unbilled = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(unbilled.consumed_entry_usd, "0");
    assert_eq!(unbilled.reserved_entry_usd, initial.reserved_entry_usd);
    assert_eq!(unbilled.reserved_loss_usd, initial.reserved_loss_usd);
    assert_live_exposure(&fixture, &entry, Decimal::new(4, 2)).await;

    // The independently signed linked bill arrives on the next capture.
    // The unchanged fill is an exact source replay, not a second execution.
    snapshot(&fixture.feed, "99.9998", "0.04");
    history_amount(
        &fixture.feed,
        Some((OPEN_ORDER, "native-partial-first", "buy", partial_at)),
        "0.04",
        "-0.0002",
    );
    capture(&fixture).await;
    let proof = fixture
        .owner
        .bind_partial_entry_settlement(
            Instant::now() + Duration::from_secs(3),
            fixture.config.account_key.wire(),
            entry.clone(),
            partial_at,
            Decimal::new(4, 2),
            covered_through(&fixture).await,
        )
        .await
        .expect("original partial source receipt after the linked bill arrives");
    assert_eq!(proof.cumulative_entry_usd, Decimal::new(4002, 4));
    let current = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let settled = fixture
        .owner
        .settle_bound_entry(
            okx_trade_execution::account_worker::UnifiedFinanceBoundSettlement {
                campaign: fixture.campaign.clone(),
                reservation_key: proof.reservation_key.clone(),
                plan_sha256: proof.plan_hash.clone(),
                expected_sequence: current.book_sequence,
                cumulative_entry_usd: proof.cumulative_entry_usd.normalize().to_string(),
                settlement_revision: proof.settlement_revision,
                source_digest: proof.source_digest.clone(),
            },
        )
        .await
        .expect("same native owner verifies source proof and debits the partial increment");
    assert_eq!(settled.cumulative_entry_usd, "0.4002");
    let first_spend = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(first_spend.consumed_entry_usd, "0.4002");
    assert_live_exposure(&fixture, &entry, Decimal::new(4, 2)).await;

    // A distinct second event supplies only the incremental 0.06 contracts
    // and its own 0.0003 bill. Total entry spend is exactly 1.0005 USD.
    let final_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9995", "0.1");
    history_amount(
        &fixture.feed,
        Some((OPEN_ORDER, "native-partial-second", "buy", final_at)),
        "0.06",
        "-0.0003",
    );
    capture(&fixture).await;
    let filled = terminal_status(&fixture, &entry, &stop_client, final_at).await;
    assert_entry_settled(&filled);
    assert_eq!(filled["data"]["intent"]["state"], "FILLED", "{filled}");
    assert_eq!(
        filled["data"]["central_release"]["reason_code"], "INTENT_EXPOSURE_REMAINS",
        "{filled}"
    );
    let all_spend = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(all_spend.consumed_entry_usd, "1.0005");
    assert_live_exposure(&fixture, &entry, Decimal::new(1, 1)).await;

    // Repeat the exact final raw fill and bill, then public Status. Neither
    // immutable ingestion nor settlement replay may debit entry capacity twice.
    snapshot(&fixture.feed, "99.9995", "0.1");
    history_amount(
        &fixture.feed,
        Some((OPEN_ORDER, "native-partial-second", "buy", final_at)),
        "0.06",
        "-0.0003",
    );
    capture(&fixture).await;
    let replay = terminal_status(&fixture, &entry, &stop_client, final_at).await;
    assert_entry_settled(&replay);
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        all_spend
    );
    assert_live_exposure(&fixture, &entry, Decimal::new(1, 1)).await;
    let (risk, stops) = fixture
        .owner
        .financial_readiness(
            fixture.config.account_key.wire(),
            None,
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(risk.nav_complete);
    assert_eq!(
        risk.daily_loss_usd,
        Decimal::new(5, 4),
        "two independently billed fees only"
    );
    assert_eq!(stops, 0);
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().len(),
        1,
        "one Create despite ACK loss and replay"
    );
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());

    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}

/// Each call declares one actual public Status assessment: order, live stop,
/// initial signed account snapshot, then its original monetary capture turn.
async fn terminal_status(
    fixture: &NativeFixture,
    entry: &str,
    stop_client: &str,
    at: i64,
) -> Value {
    order(&fixture.feed, OPEN_ORDER, "buy", at);
    protection(&fixture.feed, stop_client);
    snapshot(&fixture.feed, "99.9995", "0.1");
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(&fixture.feed, None);
    let result = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: entry.into(),
            },
            None,
        )
        .await
        .expect("public Native terminal Status owns source-backed settlement and full-size stop");
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "exact named Status transcript: {result}"
    );
    result
}

async fn covered_through(fixture: &NativeFixture) -> i64 {
    let account = fixture.config.account_key.wire();
    let fills = fixture
        .owner
        .source_cursor(account.clone(), "fills".into(), "SWAP".into())
        .await
        .unwrap()
        .unwrap();
    let bills = fixture
        .owner
        .source_cursor(account, "bills".into(), "account".into())
        .await
        .unwrap()
        .unwrap();
    assert!(fills.cursor.is_empty() && bills.cursor.is_empty());
    assert!(fills.coverage_gaps.is_empty() && bills.coverage_gaps.is_empty());
    fills.watermark.min(bills.watermark)
}

fn assert_entry_settled(status: &Value) {
    assert_eq!(
        status["data"]["entry_fill_evidence"]["state"], "verified_native_fill_and_bill_fee_sources",
        "{status}"
    );
    assert_eq!(
        status["data"]["incremental_entry_settlement"]["state"], "verified_or_no_change",
        "{status}"
    );
}

async fn assert_live_exposure(fixture: &NativeFixture, entry: &str, expected: Decimal) {
    let account = fixture.config.account_key.wire();
    let lots = fixture
        .owner
        .active_lots(
            account.clone(),
            INSTRUMENT.into(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(!lots.is_empty());
    assert!(
        lots.iter()
            .all(|lot| lot.intent_id.as_deref() == Some(entry))
    );
    assert_eq!(
        lots.iter().map(|lot| lot.remaining_qty).sum::<Decimal>(),
        expected
    );
    assert!(
        fixture
            .owner
            .reservation_receipt(
                account.clone(),
                entry.into(),
                okx_trade_execution::CentralReceiptView::ActiveExposure,
                Instant::now() + Duration::from_secs(3)
            )
            .await
            .unwrap()
            .is_some(),
        "live lot must retain original central exposure reservation"
    );
    let stopped = fixture
        .owner
        .stopped_account_summary(fixture.campaign.clone(), account)
        .await
        .unwrap();
    assert_eq!(stopped.active_reservations, 1);
    let summary = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert!(summary.reserved_loss_usd.parse::<Decimal>().unwrap() > Decimal::ZERO);
}
