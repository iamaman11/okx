//! Native replacements for retained signed-observation boundaries from T09/T10.
use super::filled_fixture::*;
use super::*;
use rust_decimal::Decimal;

#[tokio::test]
async fn native_order_observation_requires_exact_plan_and_replays_terminal_once() {
    let mut fixture = fixture().await;
    let (market_stop, market_task, _socket, _public) = opened_market(&mut fixture).await;
    let account = fixture._account.clone();
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    fixture.feed.expect_current_public_clock();
    record_fee(&fixture.feed, &at);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &at);
    record_fee(&fixture.feed, &at);
    record_leverage(&fixture.feed);
    fixture.feed.expect_post(RecordedPost { access_key: "fixture-executor-a".into(), path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: envelope(json!([{"ordId":OPEN_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])) });
    fixture.feed.arm_lost_next_create_ack();
    let mut command = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut command {
        *expires_in_ms = 10_000;
    }
    let first = fixture
        .trade
        .trade(&fixture.config.alias, &command, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(first["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN");
    let entry = first["intent_id"].as_str().unwrap().to_owned();
    let timer_capture = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let original = fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_trade_execution::TradeIntentLookup::IntentId(entry.clone()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.state, okx_trade_execution::IntentState::Unknown);
    let reservation = account
        .execution
        .store
        .central_receipt(Instant::now() + Duration::from_secs(3), entry.clone())
        .await
        .unwrap()
        .expect("original reservation");
    let source = |code: &str, rows: Value| RecordedGet {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        required_query: vec![
            ("instId".into(), INSTRUMENT.into()),
            ("clOrdId".into(), original.client_order_id.clone()),
        ],
        raw_response: serde_json::to_vec(&json!({"code":code,"msg":"","data":rows})).unwrap(),
    };
    fixture.feed.expect_get(source("51603", json!([])));
    assert_eq!(
        fixture
            .trade
            .observe_intent_order(&account, original.clone())
            .await
            .unwrap_err(),
        "ORDER_READBACK_INCOMPLETE_NO_RESUBMIT"
    );
    let row = json!({"instId":INSTRUMENT,"ordId":OPEN_ORDER,"clOrdId":original.client_order_id,
        "side":"buy","posSide":"long","state":"filled","sz":"0.1","accFillSz":"0.1",
        "uTime":jiff::Timestamp::now().as_millisecond().to_string()});
    let mut wrong = row.clone();
    wrong["sz"] = json!("0.2");
    fixture.feed.expect_get(source("0", json!([wrong])));
    assert_eq!(
        fixture
            .trade
            .observe_intent_order(&account, original.clone())
            .await
            .unwrap_err(),
        "ORDER_READBACK_PLAN_MISMATCH"
    );
    let before = fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_trade_execution::TradeIntentLookup::IntentId(entry.clone()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.state, okx_trade_execution::IntentState::Unknown);
    for _ in 0..2 {
        fixture.feed.expect_get(source("0", json!([row.clone()])));
    }
    let (filled, _) = fixture
        .trade
        .observe_intent_order(&account, before)
        .await
        .unwrap();
    assert_eq!(filled.state, okx_trade_execution::IntentState::Filled);
    assert_eq!(filled.exchange_order_id.as_deref(), Some(OPEN_ORDER));
    let (replayed, _) = fixture
        .trade
        .observe_intent_order(&account, filled.clone())
        .await
        .unwrap();
    assert_eq!(replayed.revision, filled.revision);
    let receipt = fixture
        .owner
        .operation(
            Instant::now() + Duration::from_secs(3),
            fixture.config.account_key.wire(),
            format!("terminal:{entry}"),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.kind, "TERMINAL");
    assert_eq!(receipt.result_ref, "authenticated-order-readback");
    assert!(
        fixture
            .owner
            .operation(
                Instant::now() + Duration::from_secs(3),
                fixture.config.account_key.wire(),
                format!("central-release:{}", reservation.reservation_key)
            )
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    market_stop.send(true).unwrap();
    market_task.await.unwrap();
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}

#[tokio::test]
async fn pending_orders_signed_empty_page_is_finite_and_never_inferred() {
    use okx_api::{AccountApi, Credentials, OkxRestClient};
    let feed = Arc::new(RecordedAccountHttp::default());
    feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/trade/orders-pending".into(),
        required_query: vec![("limit".into(), "100".into())],
        raw_response: br#"{"code":"0","msg":"","data":[]}"#.to_vec(),
    });
    let client = OkxRestClient::with_http_transport(
        OkxEnvironment::new(okx_api::Region::Global, false),
        Credentials::new(
            "fixture-observer-a".into(),
            "observer-secret".into(),
            "pass".into(),
        )
        .unwrap(),
        RateBudget::new(),
        feed.clone(),
    )
    .unwrap();
    let api = AccountApi::new(client);
    assert!(api.pending_orders().await.unwrap().is_empty());
    assert_eq!(feed.unconsumed_reads(), 0);
    assert!(
        api.pending_orders()
            .await
            .unwrap_err()
            .to_string()
            .contains("TRANSCRIPT_SOURCE_PAGE_UNEXPECTED_OR_EXHAUSTED")
    );
}

#[tokio::test]
async fn native_incomplete_balance_blocks_source_capture_then_signed_source_recovers() {
    let fixture = fixture().await;
    let account = fixture._account.clone();
    let gate = account.recorded_capture_gate.write().await;
    let before = fixture
        .owner
        .source_capture_state(
            fixture.config.account_key.wire(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    fixture.feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/account/balance".into(),
        required_query: vec![],
        raw_response: envelope(json!([])),
    });
    let posts = fixture.feed.outbound_posts.lock().unwrap().len();
    let reads = fixture.feed.requests.lock().unwrap().len();
    let failure = crate::source_capture::capture_account_sources_measured(
        &account.observer,
        &account.private_ws,
        &account.execution,
    )
    .await;
    assert!(
        failure.is_err(),
        "empty signed balance must not become a source proof"
    );
    assert!(
        fixture.feed.requests.lock().unwrap()[reads..]
            .iter()
            .any(|r| r.contains("/api/v5/account/balance"))
    );
    let blocked = fixture
        .owner
        .source_capture_state(
            fixture.config.account_key.wire(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(
        blocked.cursors.swap.as_ref().unwrap().watermark,
        before.cursors.swap.as_ref().unwrap().watermark
    );
    assert_eq!(
        blocked.cursors.bills.as_ref().unwrap().watermark,
        before.cursors.bills.as_ref().unwrap().watermark
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), posts);
    while jiff::Timestamp::now().as_millisecond()
        <= before
            .cursors
            .swap
            .as_ref()
            .unwrap()
            .watermark
            .max(before.cursors.bills.as_ref().unwrap().watermark)
    {
        tokio::task::yield_now().await;
    }
    snapshot(&fixture.feed, "100", "0");
    history(&fixture.feed, None);
    crate::source_capture::capture_account_sources_measured(
        &account.observer,
        &account.private_ws,
        &account.execution,
    )
    .await
    .expect("new signed balance and six history pages recover actual source owner");
    let after = fixture
        .owner
        .source_capture_state(
            fixture.config.account_key.wire(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(after.cursors.swap.unwrap().watermark > before.cursors.swap.unwrap().watermark);
    assert!(after.cursors.bills.unwrap().watermark > before.cursors.bills.unwrap().watermark);
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), posts);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    drop(gate);
    drop(account);
    fixture.finish().await;
}

#[tokio::test]
async fn native_stop_blocks_open_but_close_recovers_after_malformed_positions() {
    let mut fixture = fixture().await;
    let (market_stop, market_task, socket, public) = opened_market(&mut fixture).await;
    let account = fixture._account.clone();
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    // The signed public clock is observed at HTTP delivery, not when the
    // test queues its finite transcript before a potentially slow Execute.
    // The production five-second drift guard remains unchanged.
    fixture.feed.expect_current_public_clock();
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &now);
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    fixture.feed.expect_post(RecordedPost {access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__","tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response:envelope(json!([{"ordId":OPEN_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]))});
    fixture.feed.arm_lost_next_create_ack();
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let first = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .expect("original Native Execute before signed fill");
    assert_eq!(
        first["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{first}"
    );
    let entry = first["intent_id"].as_str().unwrap().to_owned();
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 2);
    // Admission retains ordinary periodic NAV capture. Only after the signed
    // POST do we serialize the timer against the finite fill event phases.
    // Status and explicit fill captures still call the original source owner.
    let timer_capture = account.recorded_capture_gate.write().await;
    // The two admission market observations are complete. Status inventories
    // do not publish prices. Close receives its own explicit external event.
    *fixture.market_boundary.market.lock().unwrap() = None;

    // Independent cashflow: 0.1 contracts * 0.01 BTC * 1000 = 1 USDT
    // notional; 0.0005 fee. Margin is not an equity debit.
    let opened_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(
        &fixture.feed,
        Some((OPEN_ORDER, "native-open-fill", "buy", opened_at)),
    );
    capture(&fixture).await;
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
    assert_eq!(lots[0].remaining_qty, Decimal::new(1, 1));
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
    order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
    protection(&fixture.feed, &stop_client);
    // Named public Status: protection observation, then monetary capture.
    snapshot(&fixture.feed, "99.9995", "0.1");
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(&fixture.feed, None);
    let filled = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: entry.clone(),
            },
            None,
        )
        .await
        .expect("Native filled Status and source-backed live protection");
    assert_eq!(filled["data"]["intent"]["state"], "FILLED", "{filled}");
    assert_eq!(
        filled["data"]["entry_fill_evidence"]["state"], "verified_native_fill_and_bill_fee_sources",
        "{filled}"
    );
    assert_eq!(
        filled["data"]["incremental_entry_settlement"]["state"], "verified_or_no_change",
        "{filled}"
    );
    assert_eq!(
        filled["data"]["central_release"]["reason_code"], "INTENT_EXPOSURE_REMAINS",
        "{filled}"
    );
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "all filled observations consumed"
    );
    let spent = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(spent.consumed_entry_usd, "1.0005");

    {
        let before_stop = fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap();
        fixture.authority.state.revision += 1;
        fixture.authority.state.accounts[0].policy_revision = fixture.authority.state.revision;
        fixture.authority.state.accounts[0].explicit_pause = true;
        fixture
            .trade
            .update_governance(&fixture.authority)
            .await
            .unwrap();
        let paused = fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap();
        assert_eq!(paused.consumed_entry_usd, before_stop.consumed_entry_usd);
        assert_eq!(paused.reserved_entry_usd, before_stop.reserved_entry_usd);
        assert_eq!(paused.reserved_loss_usd, before_stop.reserved_loss_usd);
        let post_count = fixture.feed.outbound_posts.lock().unwrap().len();
        let mut fresh_open = execute();
        if let TradeRequest::Execute {
            idempotency_key, ..
        } = &mut fresh_open
        {
            *idempotency_key = "native_new_open_after_stop".into();
        }
        let denied = fixture
            .trade
            .trade(&fixture.config.alias, &fresh_open, Some(&fixture.authority))
            .await
            .unwrap_err();
        assert_eq!(denied, "ACCOUNT_NEW_RISK_PAUSED");
        assert_eq!(
            fixture.feed.outbound_posts.lock().unwrap().len(),
            post_count
        );
        assert_eq!(
            fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap(),
            paused,
            "denied new risk cannot consume or release the existing position budget"
        );
    }

    close_market(&socket, &public).await;
    fixture.feed.expect_current_public_clock();
    snapshot(&fixture.feed, "99.9995", "0.1");
    let removed = fixture.feed.advance_recorded_route(
        "fixture-observer-a",
        "/api/v5/account/positions",
        vec![RecordedGet {
            access_key: "fixture-observer-a".into(),
            path: "/api/v5/account/positions".into(),
            required_query: vec![],
            raw_response: envelope(json!(null)),
        }],
    );
    assert!(removed > 0);
    let reads = fixture.feed.requests.lock().unwrap().len();
    let posts = fixture.feed.outbound_posts.lock().unwrap().len();
    let negative = TradeRequest::Close {
        idempotency_key: "native_stop_bad_positions_001".into(),
        reference: entry.clone(),
        expires_in_ms: 10_000,
    };
    match fixture
        .trade
        .trade(&fixture.config.alias, &negative, None)
        .await
    {
        Err(reason) => assert_ne!(
            reason, "ACCOUNT_NEW_RISK_PAUSED",
            "STOP must not itself block risk reduction"
        ),
        Ok(result) => {
            assert_ne!(result["status"], "accepted");
            assert_ne!(result["data"]["primary_reason"], "ACCOUNT_NEW_RISK_PAUSED");
        }
    }
    assert!(
        fixture.feed.requests.lock().unwrap()[reads..]
            .iter()
            .any(|r| r.contains("/api/v5/account/positions")),
        "actual malformed signed positions must be consumed"
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), posts);
    // First market publication advanced the book 12 to 13. The recovery
    // event must advance 13 to 14 and be observed by the actual parser.
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    let mut parsed = public.subscribe_market_updates();
    for frame in [
        json!({"arg":{"channel":"tickers","instId":INSTRUMENT},"data":[{"instType":"SWAP","instId":INSTRUMENT,"last":"1000","lastSz":"1","askPx":"1000","askSz":"10","bidPx":"999.9","bidSz":"10","ts":at}]}),
        json!({"arg":{"channel":"mark-price","instId":INSTRUMENT},"data":[{"instType":"SWAP","instId":INSTRUMENT,"markPx":"1000","ts":at}]}),
        json!({"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","idxPx":"1000","ts":at}]}),
        json!({"arg":{"channel":"books","instId":INSTRUMENT},"action":"update","data":[{"asks":[["1000","10","0","1"]],"bids":[["999.9","10","0","1"]],"seqId":14,"prevSeqId":13,"ts":at}]}),
    ] {
        socket.send_public_frame(frame.to_string());
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            parsed.changed().await.unwrap();
            if public
                .risk_fresh_snapshot(
                    INSTRUMENT,
                    jiff::Timestamp::now().as_millisecond() as u64,
                    2000,
                    180000,
                    jiff::Timestamp::now().to_string(),
                )
                .await
                .is_ok_and(|snapshot| snapshot.order_book.seq_id == Some(14))
            {
                break;
            }
        }
    })
    .await
    .expect("recovered Close market event reaches sequence 14");
    fixture.feed.expect_current_public_clock();
    snapshot(&fixture.feed, "99.9995", "0.1");
    snapshot(&fixture.feed, "99.9995", "0.1");
    fixture.feed.expect_post(RecordedPost { access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
        expected_body:json!({"instId":INSTRUMENT,"tdMode":"cross","clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell","posSide":"long","ordType":"ioc","sz":"0.1","px":"999"}),
        raw_response:envelope(json!([{"ordId":CLOSE_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])) });
    fixture.feed.arm_lost_next_create_ack();
    let close = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Close {
                idempotency_key: "native_stop_recovered_close_001".into(),
                reference: entry.clone(),
                expires_in_ms: 10_000,
            },
            None,
        )
        .await
        .expect("corrective Close remains permitted after fresh position source");
    assert_eq!(
        close["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{close}"
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), posts + 1);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}
