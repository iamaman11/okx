//! Fresh Native partial exposure: exact stops, late-fill fencing and one resize.
use super::filled_fixture::*;
use super::*;
use rust_decimal::Decimal;

#[tokio::test]
async fn risk_book_contract_native_safety_cancel_unavailable_ack_is_not_reposted_or_released() {
    let mut fixture = fixture().await;
    let (stop, market, _socket, _public) = opened_market(&mut fixture).await;
    let entry = create_entry(&fixture).await;
    let account = fixture._account.clone();
    let timer = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let original = intent(&fixture, &entry).await;
    let stop_client = format!("s{}", &original.client_order_id[..31]);
    let at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9998", "0.04");
    history_amount(
        &fixture.feed,
        Some((OPEN_ORDER, "cancel-loss-partial", "buy", at)),
        "0.04",
        "-0.0002",
    );
    capture(&fixture).await;
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let mut stopped_expected = serde_json::to_value(&before).unwrap();
    stopped_expected["book_sequence"] = json!(before.book_sequence + 1); // real protection STOP
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/cancel-order".into(),
        expected_body: json!({"instId":INSTRUMENT,"clOrdId":original.client_order_id}),
        // The exact POST has happened, but its response cannot be decoded.
        // This is an unavailable acknowledgement, never a negative grant.
        raw_response: vec![0xff],
    });
    for turn in 0..2 {
        // Each explicit safety assessment obtains its own real clock evidence
        // before the durable claim; the second claim must still refuse POST.
        fixture.feed.expect_public_clock(envelope(
            json!([{"ts":jiff::Timestamp::now().as_millisecond().to_string()}]),
        ));
        order_cumulative(
            &fixture.feed,
            OPEN_ORDER,
            "buy",
            at,
            "partially_filled",
            "0.04",
        );
        if turn == 0 {
            live_stop(&fixture.feed, &stop_client, "0.04", "canceled");
        }
        snapshot(&fixture.feed, "99.9998", "0.04");
        order_cumulative(
            &fixture.feed,
            OPEN_ORDER,
            "buy",
            at,
            "partially_filled",
            "0.04",
        );
        let result = status(&fixture, &entry).await;
        assert_eq!(result["status"], "partial", "{result}");
        assert_eq!(result["data"]["protection_incident"], true, "{result}");
        assert_eq!(result["data"]["financial_complete"], false, "{result}");
        assert_eq!(
            result["data"]["central_release"]["state"], "blocked",
            "{result}"
        );
        assert_eq!(
            result["data"]["compensation"]["reason_code"],
            "SAFETY_CANCEL_OUTCOME_UNKNOWN_NO_RESUBMIT",
            "{result}"
        );
        assert_eq!(
            fixture.feed.outbound_posts.lock().unwrap().len(),
            2,
            "entry Create plus one cancellation, never another Create/cancel"
        );
        assert_eq!(
            serde_json::to_value(
                fixture
                    .owner
                    .campaign_summary(fixture.campaign.clone())
                    .await
                    .unwrap()
            )
            .unwrap(),
            stopped_expected
        );
        let operation = account
            .execution
            .store
            .operation(
                Instant::now() + Duration::from_secs(3),
                crate::execution_store::OperationId::new(format!(
                    "safety_cancel_{}",
                    original.client_order_id
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        let operation = operation.expect("durable possible-send marker survives missing ACK");
        assert_eq!(operation.kind, "safety_cancel");
        assert_eq!(
            operation.result_ref, "POSTING",
            "unknown cancellation cannot manufacture terminal verification"
        );
    }
    assert_transcript_consumed(&fixture);
    drop(timer);
    drop(account);
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
}

#[tokio::test]
async fn risk_book_contract_native_partial_stop_and_late_fill_keep_durable_failure_fence() {
    let mut fixture = fixture().await;
    let (market_stop, market_task, _socket, _public) = opened_market(&mut fixture).await;
    let entry = create_entry(&fixture).await;
    let account = fixture._account.clone();
    let timer_capture = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let original = intent(&fixture, &entry).await;
    let stop_client = format!("s{}", &original.client_order_id[..31]);
    let partial_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9998", "0.04");
    history_amount(
        &fixture.feed,
        Some((OPEN_ORDER, "partial-protected-first", "buy", partial_at)),
        "0.04",
        "-0.0002",
    );
    capture(&fixture).await;

    // The original attached stop is accepted only at the actual partial fill,
    // with independent live venue evidence and the corresponding signed bill.
    order_cumulative(
        &fixture.feed,
        OPEN_ORDER,
        "buy",
        partial_at,
        "partially_filled",
        "0.04",
    );
    live_stop(&fixture.feed, &stop_client, "0.04", "live");
    status_finance(&fixture.feed, "99.9998", "0.04");
    let partial = status(&fixture, &entry).await;
    assert_eq!(
        partial["data"]["intent"]["state"], "PARTIALLY_FILLED",
        "{partial}"
    );
    assert_eq!(
        partial["data"]["protection"]["quantity"], "0.04",
        "{partial}"
    );
    assert_eq!(partial["data"]["protection_incident"], false, "{partial}");
    assert_eq!(
        partial["data"]["entry_fill_evidence"]["state"],
        "verified_native_fill_and_bill_fee_sources",
        "{partial}"
    );
    assert_eq!(
        partial["data"]["incremental_entry_settlement"]["state"], "verified_or_no_change",
        "{partial}"
    );
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(before.consumed_entry_usd, "0.4002");
    assert_reserved_lot(&fixture, &entry, Decimal::new(4, 2)).await;

    // A real failed stop creates a durable incident. The emergency readback
    // discovers a late fill before finance has ingested its additional 0.06.
    // Venue 0.1 versus SQL 0.04 cannot authorize a guessed safety Close.
    let late_at = jiff::Timestamp::now().as_millisecond();
    order_cumulative(
        &fixture.feed,
        OPEN_ORDER,
        "buy",
        partial_at,
        "partially_filled",
        "0.04",
    );
    live_stop(&fixture.feed, &stop_client, "0.04", "canceled");
    snapshot(&fixture.feed, "99.9998", "0.04");
    order_cumulative(&fixture.feed, OPEN_ORDER, "buy", late_at, "canceled", "0.1");
    let failed = status(&fixture, &entry).await;
    assert_failure_fence(&failed);
    assert_eq!(
        intent(&fixture, &entry).await.incident.as_deref(),
        Some("PROTECTION_PENDING_OR_FAILED")
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before
    );
    let readiness = fixture
        .trade
        .trade(&fixture.config.alias, &TradeRequest::Preflight {}, None)
        .await
        .unwrap();
    assert_eq!(readiness["state"], "blocked", "{readiness}");
    assert!(
        readiness["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "ACCOUNT_STOP"),
        "{readiness}"
    );

    // A later authenticated live GET of the old 0.04 stop cannot repair the
    // durable incident or cover the independently observed 0.1 late fill.
    live_stop(&fixture.feed, &stop_client, "0.04", "live");
    let later = account
        .execution
        .trade
        .algo_order_by_client_id(&stop_client)
        .await
        .unwrap();
    assert_eq!(later.state, "live");
    assert_eq!(later.size, "0.04");
    order_cumulative(&fixture.feed, OPEN_ORDER, "buy", late_at, "canceled", "0.1");
    snapshot(&fixture.feed, "99.9995", "0.1");
    order_cumulative(&fixture.feed, OPEN_ORDER, "buy", late_at, "canceled", "0.1");
    let replay = status(&fixture, &entry).await;
    assert_failure_fence(&replay);
    assert_eq!(
        intent(&fixture, &entry).await.incident.as_deref(),
        Some("PROTECTION_PENDING_OR_FAILED")
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before
    );

    // The account's independent original source owner can subsequently ingest
    // the late execution and its own bill. Capture alone cannot clear the
    // protection incident, settle/refund the entry or release live exposure.
    snapshot(&fixture.feed, "99.9995", "0.1");
    history_amount(
        &fixture.feed,
        Some((OPEN_ORDER, "partial-protected-late", "buy", late_at)),
        "0.06",
        "-0.0003",
    );
    crate::source_capture::capture_account_sources_measured(
        &account.observer,
        &account.private_ws,
        &account.execution,
    )
    .await
    .unwrap();
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
    assert_eq!(risk.daily_loss_usd, Decimal::new(5, 4));
    assert!(
        stops > 0,
        "independent finance ingest must preserve the safety STOP"
    );
    assert_reserved_lot(&fixture, &entry, Decimal::new(1, 1)).await;
    assert_eq!(
        intent(&fixture, &entry).await.incident.as_deref(),
        Some("PROTECTION_PENDING_OR_FAILED")
    );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().len(),
        1,
        "only original Create: no speculative safety POST or replay"
    );
    assert_transcript_consumed(&fixture);
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}

#[tokio::test]
async fn risk_book_contract_native_partial_reduce_stop_resize_needs_signed_readback_and_sends_once()
{
    let mut fixture = fixture().await;
    let (market_stop, market_task, socket, public) = opened_market(&mut fixture).await;
    let entry = create_entry(&fixture).await;
    let account = fixture._account.clone();
    let timer_capture = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let original = intent(&fixture, &entry).await;
    let stop_client = format!("s{}", &original.client_order_id[..31]);
    let opened_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(
        &fixture.feed,
        Some((OPEN_ORDER, "resize-entry", "buy", opened_at)),
    );
    capture(&fixture).await;
    order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
    live_stop(&fixture.feed, &stop_client, "0.1", "live");
    status_finance(&fixture.feed, "99.9995", "0.1");
    let opened = status(&fixture, &entry).await;
    assert_eq!(opened["data"]["protection"]["quantity"], "0.1", "{opened}");
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();

    close_market(&socket, &public).await;
    fixture.feed.expect_public_clock(envelope(json!([{
        "ts":jiff::Timestamp::now().as_millisecond().to_string()}])));
    snapshot(&fixture.feed, "99.9995", "0.1"); // reduction plan
    snapshot(&fixture.feed, "99.9995", "0.1"); // final signed position
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":INSTRUMENT,"tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell","posSide":"long",
            "ordType":"ioc","sz":"0.04","px":"999"}),
        raw_response: envelope(json!([{"ordId":CLOSE_ORDER,
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    fixture.feed.arm_lost_next_create_ack();
    let reduce: TradeRequest = serde_json::from_value(json!({
        "action":"execute","idempotency_key":"native_partial_reduce_resize",
        "operation":"reduce","strategy_instance":"manual","instrument":INSTRUMENT,
        "side":"long","sizing":{"contracts":"0.04"},
        "order":{"type":"ioc","max_slippage_bps":10},"protection":null,
        "expires_in_ms":10000}))
    .unwrap();
    let sent = fixture
        .trade
        .trade(&fixture.config.alias, &reduce, None)
        .await
        .unwrap();
    assert_eq!(
        sent["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{sent}"
    );
    let reduction = sent["intent_id"].as_str().unwrap();
    let reducing = intent(&fixture, reduction).await;
    let reduced_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9993", "0.06");
    history_amount(
        &fixture.feed,
        Some((CLOSE_ORDER, "resize-reduce", "sell", reduced_at)),
        "0.04",
        "-0.0002",
    );
    capture(&fixture).await;
    reduce_order(&fixture.feed, reduced_at);
    status_finance(&fixture.feed, "99.9993", "0.06");
    live_stop(&fixture.feed, &stop_client, "0.1", "live");
    fixture.feed.expect_public_clock(envelope(json!([{
        "ts":jiff::Timestamp::now().as_millisecond().to_string()}])));
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/amend-algos".into(),
        expected_body: json!({"instId":INSTRUMENT,"algoId":STOP,
            "reqId":reducing.client_order_id,"newSz":"0.06","cxlOnFail":false}),
        raw_response: envelope(json!([{"algoId":STOP,
            "reqId":reducing.client_order_id,"sCode":"0"}])),
    });
    let ack = status(&fixture, reduction).await;
    assert_eq!(
        ack["data"]["protection_adjustment"]["state"], "ack_source_pending",
        "{ack}"
    );
    // An amend ACK is not a smaller verified stop receipt.
    assert_eq!(
        account
            .execution
            .store
            .protection(Instant::now() + Duration::from_secs(3), entry.clone())
            .await
            .unwrap()
            .unwrap()
            .quantity,
        Decimal::new(1, 1)
    );

    // Even a signed GET retaining the old size must remain pending with no
    // second amend. A separate exact 0.06 GET finally commits verification.
    for (size, expected) in [
        ("0.1", "ack_source_pending"),
        ("0.06", "verified"),
        ("0.06", "verified"),
    ] {
        reduce_order(&fixture.feed, reduced_at);
        status_finance(&fixture.feed, "99.9993", "0.06");
        live_stop(&fixture.feed, &stop_client, size, "live");
        let readback = status(&fixture, reduction).await;
        assert_eq!(
            readback["data"]["protection_adjustment"]["state"], expected,
            "{readback}"
        );
    }
    assert_eq!(
        account
            .execution
            .store
            .protection(Instant::now() + Duration::from_secs(3), entry.clone())
            .await
            .unwrap()
            .unwrap()
            .quantity,
        Decimal::new(6, 2)
    );
    // The opening Status must use the independently verified smaller stop,
    // although its historical gross executed quantity remains 0.1.
    get(
        &fixture.feed,
        "fixture-executor-a",
        "/api/v5/trade/order",
        vec![
            ("instId", INSTRUMENT),
            ("clOrdId", &original.client_order_id),
        ],
        json!([{"instId":INSTRUMENT,"ordId":OPEN_ORDER,
            "clOrdId":original.client_order_id,"side":"buy","posSide":"long",
            "tdMode":"cross","ordType":"limit","px":"1000","sz":"0.1",
            "accFillSz":"0.1","avgPx":"1000","state":"filled",
            "cTime":opened_at.to_string(),"uTime":opened_at.to_string()}]),
    );
    live_stop(&fixture.feed, &stop_client, "0.06", "live");
    status_finance(&fixture.feed, "99.9993", "0.06");
    let remaining = status(&fixture, &entry).await;
    assert_eq!(
        remaining["data"]["protection"]["quantity"], "0.06",
        "{remaining}"
    );
    assert_eq!(
        remaining["data"]["protection_incident"], false,
        "{remaining}"
    );
    assert_eq!(
        remaining["data"]["central_release"]["reason_code"], "INTENT_EXPOSURE_REMAINS",
        "{remaining}"
    );
    assert_reserved_lot(&fixture, &entry, Decimal::new(6, 2)).await;
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before,
        "reducing exposure must not refund spent entry capacity or release the opening reservation"
    );
    {
        let posts = fixture.feed.outbound_posts.lock().unwrap();
        assert_eq!(
            posts.len(),
            3,
            "entry Create, reduce Create and exactly one stop amendment"
        );
        assert_eq!(
            posts
                .iter()
                .filter(|(_, path, _)| path == "/api/v5/trade/amend-algos")
                .count(),
            1
        );
    }
    assert_transcript_consumed(&fixture);
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}

async fn create_entry(fixture: &NativeFixture) -> String {
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
    let sent = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(
        sent["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{sent}"
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 2);
    sent["intent_id"].as_str().unwrap().to_owned()
}

async fn intent(fixture: &NativeFixture, id: &str) -> okx_execution::StoredIntent {
    fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_execution::TradeIntentLookup::Reference(id.into()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn status(fixture: &NativeFixture, entry: &str) -> Value {
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
        .unwrap();
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "exact named Status transcript: {result}"
    );
    result
}

fn live_stop(feed: &RecordedAccountHttp, client: &str, quantity: &str, state: &str) {
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/trade/order-algo",
        vec![("algoClOrdId", client)],
        json!([{
            "instId":INSTRUMENT,"algoId":STOP,"algoClOrdId":client,"state":state,
            "sz":quantity,"side":"sell","posSide":"long","tdMode":"cross",
            "ordType":"conditional","slTriggerPx":"900",
            "slTriggerPxType":"mark","slOrdPx":"-1"}]),
    );
}

fn reduce_order(feed: &RecordedAccountHttp, at: i64) {
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/trade/order",
        vec![("instId", INSTRUMENT)],
        json!([{"instId":INSTRUMENT,"ordId":CLOSE_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"sell","posSide":"long","tdMode":"cross","ordType":"ioc",
            "px":"999","sz":"0.04","accFillSz":"0.04","avgPx":"1000",
            "state":"filled","cTime":at.to_string(),"uTime":at.to_string()}]),
    );
}

fn status_finance(feed: &RecordedAccountHttp, equity: &str, quantity: &str) {
    snapshot(feed, equity, quantity); // authenticated exposure assessment
    snapshot(feed, equity, quantity); // original monetary capture turn
    history(feed, None);
}

fn assert_failure_fence(result: &Value) {
    assert_eq!(result["status"], "partial", "{result}");
    assert_eq!(result["data"]["protection_incident"], true, "{result}");
    assert_eq!(result["data"]["financial_complete"], false, "{result}");
    assert_eq!(
        result["data"]["compensation"]["state"], "blocked",
        "{result}"
    );
    assert_eq!(
        result["data"]["compensation"]["phase"], "before_financial_capture",
        "{result}"
    );
    assert_eq!(
        result["data"]["compensation"]["reason_code"], "EMERGENCY_EXPOSURE_SQL_VENUE_MISMATCH",
        "{result}"
    );
    assert_eq!(
        result["data"]["central_release"]["reason_code"], "PROTECTION_INCIDENT_UNRESOLVED",
        "{result}"
    );
}

async fn assert_reserved_lot(fixture: &NativeFixture, entry: &str, quantity: Decimal) {
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
    assert!(lots.iter().all(|lot| lot.intent_id.as_deref() == Some(entry)
        && lot.remaining_qty > Decimal::ZERO));
    assert_eq!(
        lots.iter().map(|lot| lot.remaining_qty).sum::<Decimal>(),
        quantity
    );
    assert!(
        fixture
            .owner
            .reservation_receipt(
                account,
                entry.into(),
                okx_execution::CentralReceiptView::ActiveExposure,
                Instant::now() + Duration::from_secs(3)
            )
            .await
            .unwrap()
            .is_some()
    );
}

fn assert_transcript_consumed(fixture: &NativeFixture) {
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
}
