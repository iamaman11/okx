//! Independent signed late fees debit once; rebates never restore entry cap.
use super::filled_fixture::*;
use super::*;
use okx_execution::account_worker::UnifiedFinanceBoundSettlement;
use rust_decimal::Decimal;

#[tokio::test]
async fn risk_book_contract_native_late_fee_rebate_and_bound_breach_use_immutable_revisions() {
    let mut fixture = fixture().await;
    let (market_stop, market_task, _socket, _public) = opened_market(&mut fixture).await;
    let entry = create(&fixture).await;
    let account = fixture._account.clone();
    let timer_capture = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let original = fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_execution::TradeIntentLookup::Reference(entry.clone()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    let partial_at = chrono::Utc::now().timestamp_millis();
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
    assert_eq!(partial.state, okx_execution::IntentState::PartiallyFilled);
    assert_eq!(observed.unwrap().accumulated_fill_size, "0.04");
    snapshot(&fixture.feed, "99.9998", "0.04");
    history_amount(
        &fixture.feed,
        Some((OPEN_ORDER, "fee-correction-original", "buy", partial_at)),
        "0.04",
        "-0.0002",
    );
    capture(&fixture).await;
    let first = bind(&fixture, &entry, partial_at).await;
    // External amounts: 0.04 contracts * 0.01 BTC * 1000 + 0.0002 fee.
    assert_eq!(first.cumulative_entry_usd, Decimal::new(4002, 4));
    assert_eq!(first.settlement_revision, 1);
    let first_request = request(&fixture, &first).await;
    let first_ack = fixture
        .owner
        .settle_bound_entry(first_request.clone())
        .await
        .unwrap();
    assert_eq!(
        fixture
            .owner
            .settle_bound_entry(first_request)
            .await
            .unwrap(),
        first_ack
    );
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(before.consumed_entry_usd, "0.4002");
    assert_eq!(before.entry_bound_breach_usd, "0");

    // The venue publishes a separate, order-linked -1 USDT bill after the
    // first source frontier. It is not another fill or a rewritten old fee.
    let late_at = chrono::Utc::now().timestamp_millis();
    assert!(late_at > first.source_covered_through_ms);
    snapshot(&fixture.feed, "98.9998", "0.04");
    adjustment(&fixture.feed, "fee-correction-late-debit", late_at, "-1");
    capture(&fixture).await;
    let corrected = bind(&fixture, &entry, partial_at).await;
    assert_eq!(corrected.venue_filled_quantity, Decimal::new(4, 2));
    assert_eq!(corrected.cumulative_entry_usd, Decimal::new(14002, 4));
    assert_eq!(corrected.settlement_revision, 2);
    assert_ne!(corrected.source_digest, first.source_digest);
    let correction_request = request(&fixture, &corrected).await;
    let corrected_ack = fixture
        .owner
        .settle_bound_entry(correction_request.clone())
        .await
        .unwrap();
    let breached = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(breached.consumed_entry_usd, "1.4002");
    // Frozen full-order permission: 0.1 * 0.01 * 1000 + 0.0005.
    assert_eq!(breached.entry_bound_breach_usd, "0.3997");
    assert_eq!(breached.reserved_entry_usd, "0");
    assert_eq!(breached.reserved_loss_usd, before.reserved_loss_usd);
    assert!(breached.reserved_loss_usd.parse::<Decimal>().unwrap() > Decimal::ZERO);

    // Replay the exact external bill and original settlement request. Neither
    // immutable ingestion nor a lost-reply replay may debit a second dollar.
    snapshot(&fixture.feed, "98.9998", "0.04");
    adjustment(&fixture.feed, "fee-correction-late-debit", late_at, "-1");
    capture(&fixture).await;
    assert_eq!(bind(&fixture, &entry, partial_at).await, corrected);
    assert_eq!(
        fixture
            .owner
            .settle_bound_entry(correction_request.clone())
            .await
            .unwrap(),
        corrected_ack
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        breached
    );

    // A real +1 USDT fee rebate restores account cash, but never releases
    // conservative consumed entry capacity or erases the durable breach.
    snapshot(&fixture.feed, "99.9998", "0.04");
    adjustment(
        &fixture.feed,
        "fee-correction-rebate",
        chrono::Utc::now().timestamp_millis(),
        "1",
    );
    capture(&fixture).await;
    assert_eq!(bind(&fixture, &entry, partial_at).await, corrected);
    assert_eq!(
        fixture
            .owner
            .settle_bound_entry(correction_request)
            .await
            .unwrap(),
        corrected_ack
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        breached
    );
    // The first immutable revision remains addressable and byte-identical.
    let old = account
        .execution
        .store
        .operation(
            Instant::now() + Duration::from_secs(3),
            crate::execution_store::OperationId::new(format!("entry-partial:{entry}:r1")).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<okx_execution::EntrySettlementEvidence>(&old.result_ref).unwrap(),
        first
    );

    let mut another = execute();
    if let TradeRequest::Execute {
        idempotency_key, ..
    } = &mut another
    {
        *idempotency_key = "native_open_after_fee_breach".into();
    }
    let denied = fixture
        .trade
        .trade(&fixture.config.alias, &another, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(
        denied["data"]["primary_reason"], "BUDGET_BOUND_BREACH",
        "{denied}"
    );
    assert_eq!(denied["data"]["send_state"], "NOT_STARTED", "{denied}");
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        breached
    );
    let lots = fixture
        .owner
        .active_lots(
            fixture.config.account_key.wire(),
            INSTRUMENT.into(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(lots.len(), 1);
    assert_eq!(lots[0].intent_id.as_deref(), Some(entry.as_str()));
    assert_eq!(lots[0].remaining_qty, Decimal::new(4, 2));
    assert!(
        fixture
            .owner
            .reservation_receipt(
                fixture.config.account_key.wire(),
                entry,
                okx_execution::CentralReceiptView::ActiveExposure,
                Instant::now() + Duration::from_secs(3)
            )
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .owner
            .installation_receipt(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
            .schema_version,
        8,
        "late fee/rebate ingestion must not change the canonical financial schema"
    );
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    // Stop the timer while its event transcript is still protected. Releasing
    // first can start an unplanned capture that retains the original owner.
    tokio::time::timeout(Duration::from_secs(3), fixture.registry.shutdown())
        .await
        .expect("source shutdown must not wait for the test-held capture gate");
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}

async fn bind(
    fixture: &NativeFixture,
    entry: &str,
    at: i64,
) -> okx_execution::EntrySettlementEvidence {
    let account = fixture.config.account_key.wire();
    let fills = fixture
        .owner
        .source_cursor(account.clone(), "fills".into(), "SWAP".into())
        .await
        .unwrap()
        .unwrap();
    let bills = fixture
        .owner
        .source_cursor(account.clone(), "bills".into(), "account".into())
        .await
        .unwrap()
        .unwrap();
    assert!(fills.cursor.is_empty() && bills.cursor.is_empty());
    assert!(fills.coverage_gaps.is_empty() && bills.coverage_gaps.is_empty());
    fixture
        .owner
        .bind_partial_entry_settlement(
            Instant::now() + Duration::from_secs(3),
            account,
            entry.into(),
            at,
            Decimal::new(4, 2),
            fills.watermark.min(bills.watermark),
        )
        .await
        .unwrap()
}

async fn request(
    fixture: &NativeFixture,
    proof: &okx_execution::EntrySettlementEvidence,
) -> UnifiedFinanceBoundSettlement {
    let book = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    UnifiedFinanceBoundSettlement {
        campaign: fixture.campaign.clone(),
        reservation_key: proof.reservation_key.clone(),
        plan_sha256: proof.plan_hash.clone(),
        expected_sequence: book.book_sequence,
        cumulative_entry_usd: proof.cumulative_entry_usd.normalize().to_string(),
        settlement_revision: proof.settlement_revision,
        source_digest: proof.source_digest.clone(),
    }
}

fn adjustment(feed: &RecordedAccountHttp, bill: &str, at: i64, fee: &str) {
    for partition in ["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"] {
        get(
            feed,
            "fixture-executor-a",
            "/api/v5/trade/fills-history",
            vec![("instType", partition)],
            json!([]),
        );
    }
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/account/bills-archive",
        vec![],
        json!([{"billId":bill,"instType":"SWAP","instId":INSTRUMENT,
            "ordId":OPEN_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__",
            "tradeId":"","ccy":"USDT","type":"2","subType":"3",
            "balChg":fee,"fee":fee,"pnl":"0","ts":at.to_string()}]),
    );
}

async fn create(fixture: &NativeFixture) -> String {
    let now = chrono::Utc::now().timestamp_millis().to_string();
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
    let sent = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(
        sent["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{sent}"
    );
    sent["intent_id"].as_str().unwrap().to_owned()
}
