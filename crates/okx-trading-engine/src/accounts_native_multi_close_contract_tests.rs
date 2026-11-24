//! Two real Close orders after an IOC partial execution, on one Native owner.
use super::filled_fixture::*;
use super::*;
use rust_decimal::Decimal;

const SECOND_CLOSE: &str = "native-second-close-002";

#[tokio::test]
async fn risk_book_contract_native_two_closes_use_remaining_lot_and_retire_partial_terminal() {
    let mut fixture = fixture().await;
    let (market_stop, market_task, socket, public) = opened_market(&mut fixture).await;
    let entry = create_entry(&fixture).await;
    let account = fixture._account.clone();
    let timer_capture = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
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
    let opened_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(
        &fixture.feed,
        Some((OPEN_ORDER, "multi-close-open", "buy", opened_at)),
    );
    capture(&fixture).await;
    order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
    protection(&fixture.feed, &stop_client);
    finance_turn(&fixture.feed, "99.9995", "0.1");
    let opened = status(&fixture, &entry).await;
    assert_eq!(
        opened["data"]["central_release"]["reason_code"], "INTENT_EXPOSURE_REMAINS",
        "{opened}"
    );
    let reserved = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(reserved.consumed_entry_usd, "1.0005");

    // The first Close is sized from the actual attributed 0.1 lot. Its IOC
    // order independently executes 0.04 and cancels the unexecuted remainder.
    close_market(&socket, &public).await;
    let first_request = close_request("native_multi_close_first", &entry);
    let first = send_close(
        &fixture,
        &first_request,
        CLOSE_ORDER,
        "0.1",
        "0.1",
        "99.9995",
    )
    .await;
    let first_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9993", "0.06");
    history_amount(
        &fixture.feed,
        Some((CLOSE_ORDER, "multi-close-first", "sell", first_at)),
        "0.04",
        "-0.0002",
    );
    capture(&fixture).await;
    close_order(
        &fixture.feed,
        CLOSE_ORDER,
        first_at,
        "0.1",
        "0.04",
        "canceled",
    );
    finance_turn(&fixture.feed, "99.9993", "0.06");
    let partial = status(&fixture, &first).await;
    assert_eq!(partial["data"]["intent"]["state"], "CANCELLED", "{partial}");
    assert_eq!(
        partial["data"]["central_release"]["reason_code"], "INTENT_EXPOSURE_REMAINS",
        "{partial}"
    );
    assert_lot(&fixture, &entry, Decimal::new(6, 2)).await;
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        reserved
    );
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 2);

    // A separate named public market update precedes the second Close. No
    // replay of the first Create can substitute for this exact 0.06 request.
    refresh_second_close_market(&socket, &public).await;
    let second_request = close_request("native_multi_close_second", &entry);
    let second = send_close(
        &fixture,
        &second_request,
        SECOND_CLOSE,
        "0.06",
        "0.06",
        "99.9993",
    )
    .await;
    let second_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.999", "0");
    history_amount(
        &fixture.feed,
        Some((SECOND_CLOSE, "multi-close-second", "sell", second_at)),
        "0.06",
        "-0.0003",
    );
    capture(&fixture).await;
    close_order(
        &fixture.feed,
        SECOND_CLOSE,
        second_at,
        "0.06",
        "0.06",
        "filled",
    );
    finance_turn(&fixture.feed, "99.999", "0");
    let flat = status(&fixture, &second).await;
    assert_eq!(
        flat["data"]["central_release"]["state"], "verified",
        "{flat}"
    );
    assert_lot(&fixture, &entry, Decimal::ZERO).await;

    // Only independently covered, billed terminal source can retire the old
    // local Close marker. The terminal canceled order really filled 0.04,
    // so its original 0.1 request cannot be used as the execution quantity.
    close_order(
        &fixture.feed,
        CLOSE_ORDER,
        first_at,
        "0.1",
        "0.04",
        "canceled",
    );
    finance_turn(&fixture.feed, "99.999", "0");
    let first_replay = fixture
        .trade
        .trade(&fixture.config.alias, &first_request, None)
        .await
        .unwrap();
    assert_eq!(
        first_replay["data"]["central_release"]["state"], "verified",
        "{first_replay}"
    );
    assert_eq!(
        first_replay["data"]["financial_complete"], true,
        "{first_replay}"
    );
    assert_eq!(
        fixture.feed.outbound_posts.lock().unwrap().len(),
        3,
        "original entry, first Close and second Close only"
    );

    // The stop belongs to the opening intent. It is canceled only after
    // authenticated flat readbacks, then the original reservation releases.
    cleanup(&fixture.feed, &stop_client);
    order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
    snapshot(&fixture.feed, "99.999", "0"); // initial Status
    snapshot(&fixture.feed, "99.999", "0"); // before stop cancellation
    snapshot(&fixture.feed, "99.999", "0"); // after signed cancellation ACK
    snapshot(&fixture.feed, "99.999", "0"); // financial capture
    snapshot(&fixture.feed, "99.999", "0"); // terminal cohort census
    history(&fixture.feed, None);
    let released = status(&fixture, &entry).await;
    assert_eq!(
        released["data"]["central_release"]["state"], "verified",
        "{released}"
    );
    assert_eq!(released["data"]["financial_complete"], true, "{released}");
    let final_book = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(final_book.consumed_entry_usd, "1.0005");
    assert_eq!(final_book.reserved_entry_usd, "0");
    assert_eq!(final_book.reserved_loss_usd, "0");
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
        Decimal::new(1, 3),
        "entry fee0.0005 + two independently billed exit fees0.0002+0.0003"
    );
    assert_eq!(stops, 0);
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 4);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
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

async fn send_close(
    fixture: &NativeFixture,
    request: &TradeRequest,
    order_id: &str,
    quantity: &str,
    position: &str,
    equity: &str,
) -> String {
    fixture.feed.expect_public_clock(envelope(json!([{
        "ts":jiff::Timestamp::now().as_millisecond().to_string()}])));
    snapshot(&fixture.feed, equity, position); // close plan
    snapshot(&fixture.feed, equity, position); // final signed position
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":INSTRUMENT,"tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell","posSide":"long",
            "ordType":"ioc","sz":quantity,"px":"999"}),
        raw_response: envelope(
            json!([{"ordId":order_id,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]),
        ),
    });
    fixture.feed.arm_lost_next_create_ack();
    let result = fixture
        .trade
        .trade(&fixture.config.alias, request, None)
        .await
        .unwrap();
    assert_eq!(
        result["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{result}"
    );
    assert_eq!(fixture.feed.unconsumed_reads(), 0, "{result}");
    result["intent_id"].as_str().unwrap().to_owned()
}

fn close_request(key: &str, entry: &str) -> TradeRequest {
    TradeRequest::Close {
        idempotency_key: key.into(),
        reference: entry.into(),
        expires_in_ms: 10_000,
    }
}

fn close_order(
    feed: &RecordedAccountHttp,
    order_id: &str,
    at: i64,
    planned: &str,
    filled: &str,
    state: &str,
) {
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/trade/order",
        vec![("instId", INSTRUMENT)],
        json!([{"instId":INSTRUMENT,"ordId":order_id,"clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"sell","posSide":"long","tdMode":"cross","ordType":"ioc",
            "px":"999","sz":planned,"accFillSz":filled,"avgPx":"1000","state":state,
            "cTime":at.to_string(),"uTime":at.to_string()}]),
    );
}

fn finance_turn(feed: &RecordedAccountHttp, equity: &str, position: &str) {
    snapshot(feed, equity, position);
    snapshot(feed, equity, position);
    history(feed, None);
}

async fn status(fixture: &NativeFixture, id: &str) -> Value {
    let result = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: id.into(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "exact named Status: {result}; denied={:?}",
        fixture.feed.denied_routes.lock().unwrap()
    );
    result
}

async fn assert_lot(fixture: &NativeFixture, entry: &str, expected: Decimal) {
    let lots = fixture
        .owner
        .active_lots(
            fixture.config.account_key.wire(),
            INSTRUMENT.into(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert!(
        lots.iter()
            .all(|lot| lot.intent_id.as_deref() == Some(entry))
    );
    assert_eq!(
        lots.iter().map(|lot| lot.remaining_qty).sum::<Decimal>(),
        expected
    );
}

async fn refresh_second_close_market(
    socket: &RecordedWsConnector,
    public: &okx_market_runtime::PublicWsHandle,
) {
    let at = jiff::Timestamp::now().as_millisecond().to_string();
    let mut parsed = public.subscribe_market_updates();
    for frame in [
        json!({"arg":{"channel":"tickers","instId":INSTRUMENT},"data":[{
            "instType":"SWAP","instId":INSTRUMENT,"last":"1000","lastSz":"1",
            "askPx":"1000","askSz":"10","bidPx":"999.9","bidSz":"10","ts":at}]}),
        json!({"arg":{"channel":"mark-price","instId":INSTRUMENT},"data":[{
            "instType":"SWAP","instId":INSTRUMENT,"markPx":"1000","ts":at}]}),
        json!({"arg":{"channel":"index-tickers","instId":"BTC-USDT"},"data":[{
            "instId":"BTC-USDT","idxPx":"1000","ts":at}]}),
        json!({"arg":{"channel":"books","instId":INSTRUMENT},"action":"update","data":[{
            "asks":[["1000","10","0","1"]],"bids":[["999.9","10","0","1"]],
            "seqId":14,"prevSeqId":13,"ts":at}]}),
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
                .is_ok()
            {
                break;
            }
        }
    })
    .await
    .expect("second Close's independently parsed fresh market event");
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
    let result = with_fresh_recorded_clock(
        &fixture.feed,
        fixture
            .trade
            .trade(&fixture.config.alias, &request, Some(&fixture.authority)),
    )
    .await
    .unwrap();
    assert_eq!(
        result["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{result}"
    );
    result["intent_id"].as_str().unwrap().to_owned()
}
