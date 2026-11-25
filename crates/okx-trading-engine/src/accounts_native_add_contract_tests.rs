//! Public Add admits exactly owned, protected exposure on the original owner.
//! Venue bytes independently describe the old position, incremental fill and
//! both stops; a fresh unmatched position cannot acquire a local lot by Add.
use super::filled_fixture::*;
use super::*;
use rust_decimal::Decimal;
use std::str::FromStr;

const ADD_ORDER: &str = "native-public-add-002";
const ADD_STOP: &str = "900000003";

#[tokio::test]
async fn risk_book_contract_native_public_add_preserves_original_lot_and_protects_increment() {
    public_add(false).await;
}

#[tokio::test]
async fn risk_book_contract_native_public_add_rejects_fresh_unowned_venue_increment() {
    public_add(true).await;
}

async fn public_add(external_increment: bool) {
    let mut fixture = fixture().await;
    let reference_http = Arc::new(AddReferenceHttp::new());
    let reference_client = okx_api::OkxPublicClient::with_http_transport(
        OkxEnvironment::new(okx_api::Region::Global, false),
        RateBudget::new(),
        reference_http.clone(),
    )
    .unwrap();
    let (market_stop, market_task, socket, public) =
        opened_market_with_public_client(&mut fixture, Some(reference_client)).await;
    let entry = create_entry(&fixture).await;
    let account = fixture._account.clone();
    // Admission used the ordinary capture task. From the first external fill
    // onward each named production capture owns its finite history transcript.
    let timer_capture = account.recorded_capture_gate.write().await;
    *fixture.market_boundary.market.lock().unwrap() = None;
    let original = lookup(&fixture, &entry).await;
    let original_stop_client = format!("s{}", &original.client_order_id[..31]);
    let opened_at = jiff::Timestamp::now().as_millisecond();
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(
        &fixture.feed,
        Some((OPEN_ORDER, "public-add-original", "buy", opened_at)),
    );
    capture(&fixture).await;
    entry_status(
        &fixture,
        &entry,
        OPEN_ORDER,
        STOP,
        &original_stop_client,
        opened_at,
        "99.9995",
        "0.1",
        None,
    )
    .await;
    let settlement_completed_at = jiff::Timestamp::now().as_millisecond();
    let old_lots = lots(&fixture).await;
    assert_eq!(old_lots.len(), 1);
    assert_owned_lot(&old_lots[0], &entry);
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(before.consumed_entry_usd, "1.0005");
    assert_eq!(before.reserved_entry_usd, "0");
    assert_eq!(before.reserved_loss_usd, "0.102");

    // Entry settlement changed the economic epoch after initial maintenance
    // stopped. Its own completed receipt must enter the production closed
    // history window before sequential new risk can use refreshed readiness.
    // Wait only until that exact event-time cutoff, then perform ONE capture
    // and ONE original maintenance refresh. No Status retry or fabricated READY.
    let history_closes_at = settlement_completed_at.checked_add(2_000).unwrap();
    let remaining_ms = history_closes_at
        .saturating_sub(jiff::Timestamp::now().as_millisecond())
        .max(0) as u64;
    if remaining_ms > 0 {
        tokio::time::sleep_until(tokio::time::Instant::now() + Duration::from_millis(remaining_ms))
            .await;
    }
    assert!(jiff::Timestamp::now().as_millisecond() >= history_closes_at);
    // This contract also exercises the real stale-reference branch. Await
    // the validity boundary of the independently observed original reference,
    // not an arbitrary delay or a synthetic instruments WS update. Capture
    // money AFTER both cutoffs so this wait cannot age the new NAV evidence.
    let reference_at = jiff::Timestamp::from_str(
        &public
            .instrument_rules(INSTRUMENT)
            .await
            .unwrap()
            .source_received_at,
    )
    .unwrap()
    .as_millisecond();
    let reference_expires_at = reference_at.checked_add(5_001).unwrap();
    let reference_remaining_ms = reference_expires_at
        .saturating_sub(jiff::Timestamp::now().as_millisecond())
        .max(0) as u64;
    if reference_remaining_ms > 0 {
        tokio::time::sleep_until(
            tokio::time::Instant::now() + Duration::from_millis(reference_remaining_ms),
        )
        .await;
    }
    assert!(jiff::Timestamp::now().as_millisecond() >= reference_expires_at);
    snapshot(&fixture.feed, "99.9995", "0.1");
    history(
        &fixture.feed,
        Some((OPEN_ORDER, "public-add-original", "buy", opened_at)),
    );
    capture(&fixture).await;
    let frontier = fixture
        .owner
        .source_capture_state(
            fixture.config.account_key.wire(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(frontier.account_key, fixture.config.account_key.wire());
    let minimum_watermark = [
        frontier.cursors.spot,
        frontier.cursors.margin,
        frontier.cursors.swap,
        frontier.cursors.futures,
        frontier.cursors.options,
        frontier.cursors.bills,
    ]
    .into_iter()
    .map(|cursor| {
        let cursor = cursor.expect("all six original history streams are required");
        assert!(cursor.generation > 0);
        assert!(cursor.cursor.is_empty() && cursor.coverage_gaps.is_empty());
        cursor.watermark
    })
    .min()
    .unwrap();
    let through = minimum_watermark.min(jiff::Timestamp::now().as_millisecond() - 2_000);
    assert!(through >= settlement_completed_at);
    fixture
        .owner
        .refresh_campaign_entry_readiness(
            Instant::now() + Duration::from_secs(3),
            fixture.config.account_key.wire(),
            fixture.campaign.clone(),
            i64::try_from(before.campaign_started_at_ms.unwrap()).unwrap(),
            through,
        )
        .await
        .expect(
            "one production maintenance refresh over closed original fill/bill/settlement facts",
        );
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before,
        "closed-window replay/readiness cannot debit the original entry twice"
    );
    assert_eq!(
        serde_json::to_value(lots(&fixture).await).unwrap(),
        serde_json::to_value(&old_lots).unwrap(),
        "replaying the same immutable fill cannot duplicate or rewrite its lot"
    );
    assert_finance(&fixture, Decimal::new(5, 4), 1).await;
    assert_consumed(&fixture);

    // Deliver a distinct parsed market event before the Add planner. The
    // shared boundary alone owns its books sequence: Open ended at seq12,
    // this event is seq13, Add's two inventories deliver seq14/15.
    fixture
        .market_boundary
        .market_updates_expected
        .store(if external_increment { 4 } else { 5 }, Ordering::SeqCst);
    *fixture.market_boundary.market.lock().unwrap() = Some((socket.clone(), public));
    fixture
        .market_boundary
        .publish_market_observation(Instant::now() + Duration::from_secs(2))
        .await;

    let request = add_request();
    assert!(request.valid());
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    // Negative Add stops before the mutation clock read. Positive Add
    // performs one signed Create with its own independent clock observation.
    if !external_increment {
        // Windows scheduling can exceed the 5s exchange-clock offset budget
        // between fixture setup and delivery of the one-shot public time GET.
        // The production clock client fetches a new sample for each mutation;
        // there is no cached clock TTL to wait out before this request.
        fixture.feed.expect_current_public_clock();
    }
    let clock_gets_before_add = fixture
        .feed
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|request| request.as_str() == "public:/api/v5/public/time")
        .count();
    record_fee(&fixture.feed, &now); // plan
    record_leverage(&fixture.feed); // plan
    record_leverage(&fixture.feed); // immutable quote
    record_fee(&fixture.feed, &now); // immutable quote
    let observed_position = if external_increment { "0.15" } else { "0.1" };
    snapshot(&fixture.feed, "99.9995", observed_position); // plan
    snapshot(&fixture.feed, "99.9995", observed_position); // entry quote
    inventory(&fixture.feed, &[(STOP, &original_stop_client)]); // pre-reserve census
    snapshot(&fixture.feed, "99.9995", observed_position); // pre-reserve census
    if !external_increment {
        snapshot(&fixture.feed, "99.9995", "0.1"); // reserve conversion
        snapshot(&fixture.feed, "99.9995", "0.1"); // post-HELD selected read
        record_fee(&fixture.feed, &now); // post-HELD reassessment
        record_leverage(&fixture.feed); // post-HELD reassessment
        inventory(&fixture.feed, &[(STOP, &original_stop_client)]); // BEGIN_POST census
        create_post(&fixture.feed, ADD_ORDER);
        fixture.feed.arm_lost_next_create_ack();
    }
    let add = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .expect("public Add makes its own source-backed admission decision");
    *fixture.market_boundary.market.lock().unwrap() = None;
    assert_eq!(
        fixture.market_boundary.updates.load(Ordering::SeqCst),
        if external_increment { 4 } else { 5 }
    );

    if external_increment {
        assert_eq!(add["data"]["state"], "REJECTED", "{add}");
        assert_eq!(add["data"]["send_state"], "NOT_STARTED", "{add}");
        assert_eq!(
            add["data"]["primary_reason"], "UNATTRIBUTED_EXCHANGE_POSITION",
            "{add}"
        );
        assert_eq!(add["data"]["risk_release_state"], "NOT_RESERVED", "{add}");
        assert_eq!(
            fixture
                .feed
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.as_str() == "public:/api/v5/public/time")
                .count(),
            clock_gets_before_add,
            "a rejected external Add cannot fetch a mutation clock"
        );
        assert_consumed(&fixture);
        assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
        assert_eq!(
            fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap(),
            before,
            "an unmatched signed 0.05 cannot reserve, consume or release money"
        );
        assert_eq!(
            serde_json::to_value(lots(&fixture).await).unwrap(),
            serde_json::to_value(&old_lots).unwrap(),
            "the external position cannot create or rewrite an attributed lot"
        );
        let denied_intent = lookup(&fixture, add["data"]["intent_id"].as_str().unwrap()).await;
        assert_eq!(
            denied_intent.state,
            okx_trade_execution::IntentState::Rejected
        );
        assert!(
            fixture
                .owner
                .unsent_before_reserve(
                    Instant::now() + Duration::from_secs(3),
                    fixture.config.account_key.wire(),
                    denied_intent.intent_id
                )
                .await
                .unwrap()
        );
        assert_finance(&fixture, Decimal::new(5, 4), 1).await;
    } else {
        // Diagnose the actual safe early-rejection reason *before* checking
        // leftover one-shot transcript pages or the expected Create POST.
        assert_eq!(
            add["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
            "positive Add did not reach its original Create; reason_code={} primary_reason={}",
            add["reason_code"], add["data"]["primary_reason"]
        );
        assert_eq!(
            fixture
                .feed
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.as_str() == "public:/api/v5/public/time")
                .count(),
            clock_gets_before_add + 1,
            "positive Add must fetch exactly one fresh production public clock"
        );
        assert_consumed(&fixture);
        let increment = add["intent_id"].as_str().unwrap().to_owned();
        assert_ne!(increment, entry);
        let added = lookup(&fixture, &increment).await;
        assert_eq!(added.plan.operation, "add");
        assert_eq!(added.plan.strategy_instance, "manual");
        assert_eq!(added.plan.quantity, Decimal::new(1, 1));
        let add_stop_client = format!("s{}", &added.client_order_id[..31]);
        assert_ne!(add_stop_client, original_stop_client);
        let added_at = jiff::Timestamp::now().as_millisecond();
        // Independent cashflow: each 0.1 * 0.01 BTC * 1000 entry costs
        // 1 USDT notional + 0.0005 fee. Total equity loses only the two fees.
        snapshot(&fixture.feed, "99.999", "0.2");
        history(
            &fixture.feed,
            Some((ADD_ORDER, "public-add-increment", "buy", added_at)),
        );
        capture(&fixture).await;
        let stops = [
            (STOP, original_stop_client.as_str()),
            (ADD_STOP, add_stop_client.as_str()),
        ];
        let filled_add = entry_status(
            &fixture,
            &increment,
            ADD_ORDER,
            ADD_STOP,
            &add_stop_client,
            added_at,
            "99.999",
            "0.2",
            Some(&stops),
        )
        .await;
        assert_eq!(filled_add["data"]["protection"]["algo_id"], ADD_STOP);
        let both_lots = lots(&fixture).await;
        assert_eq!(both_lots.len(), 2);
        let preserved = both_lots
            .iter()
            .find(|lot| lot.intent_id.as_deref() == Some(&entry))
            .unwrap();
        assert_eq!(
            serde_json::to_value(preserved).unwrap(),
            serde_json::to_value(&old_lots[0]).unwrap()
        );
        let new_lot = both_lots
            .iter()
            .find(|lot| lot.intent_id.as_deref() == Some(&increment))
            .unwrap();
        assert_owned_lot(new_lot, &increment);
        assert_ne!(new_lot.lot_id, preserved.lot_id);
        assert_ne!(new_lot.fill_id, preserved.fill_id);
        assert_eq!(
            both_lots
                .iter()
                .map(|lot| lot.remaining_qty)
                .sum::<Decimal>(),
            Decimal::new(2, 1)
        );
        let after = fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap();
        assert_eq!(after.consumed_entry_usd, "2.001");
        assert_eq!(after.reserved_entry_usd, "0");
        assert_eq!(after.reserved_loss_usd, "0.204");
        assert_finance(&fixture, Decimal::new(1, 3), 2).await;

        // Original Status must still verify its own .1 stop in the .2 venue
        // position, using the same complete independently signed two-stop census.
        let original_status = entry_status(
            &fixture,
            &entry,
            OPEN_ORDER,
            STOP,
            &original_stop_client,
            opened_at,
            "99.999",
            "0.2",
            Some(&stops),
        )
        .await;
        assert_eq!(original_status["data"]["protection"]["algo_id"], STOP);
        assert_eq!(
            fixture
                .owner
                .campaign_summary(fixture.campaign.clone())
                .await
                .unwrap(),
            after
        );
        assert_eq!(
            fixture.feed.outbound_posts.lock().unwrap().len(),
            2,
            "only the original and Add Creates; Status cannot send an emergency order"
        );
    }
    assert_consumed(&fixture);
    assert_eq!(
        reference_http.delivered.load(Ordering::SeqCst),
        2,
        "stale full reference uses exactly one actual SWAP and FUTURES REST read"
    );
    assert!(reference_http.pages.lock().unwrap().is_empty());
    market_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    drop(timer_capture);
    drop(account);
    fixture.finish().await;
}

// Two real public REST envelopes for one stale-reference refresh. This source
// never fabricates WS instrument events or supplies financial/private routes.
pub(super) struct AddReferenceHttp {
    pages: Mutex<std::collections::VecDeque<(&'static str, Vec<u8>)>>,
    delivered: AtomicUsize,
}

impl AddReferenceHttp {
    pub(super) fn new() -> Self {
        let instrument = json!({"instType":"SWAP","instId":INSTRUMENT,
            "instFamily":"BTC-USDT","uly":"BTC-USDT","state":"live","ruleType":"normal",
            "settleCcy":"USDT","tickSz":"0.1","lotSz":"0.01","minSz":"0.01",
            "maxLmtSz":"1000","maxMktSz":"1000","ctType":"linear","ctVal":"0.01",
            "ctValCcy":"BTC","groupId":"4","lever":"20"});
        Self {
            pages: Mutex::new(
                [
                    ("SWAP", envelope(json!([instrument]))),
                    ("FUTURES", envelope(json!([]))),
                ]
                .into(),
            ),
            delivered: AtomicUsize::new(0),
        }
    }
}

impl HttpTransport for AddReferenceHttp {
    fn send(&self, request: HttpRequest, _deadline: Instant) -> HttpSend<'_> {
        Box::pin(async move {
            assert_eq!(request.method(), "GET");
            assert_eq!(request.url().path(), "/api/v5/public/instruments");
            assert!(request.headers().get("OK-ACCESS-KEY").is_none());
            let (partition, body) = self
                .pages
                .lock()
                .unwrap()
                .pop_front()
                .expect("only one named complete public reference refresh");
            assert_eq!(
                request.url().query_pairs().collect::<Vec<_>>(),
                vec![("instType".into(), partition.into())]
            );
            self.delivered.fetch_add(1, Ordering::SeqCst);
            Ok(okx_api::RawHttpResponse {
                status: okx_api::HttpStatusCode::OK,
                headers: Default::default(),
                body,
            })
        })
    }
}

async fn create_entry(fixture: &NativeFixture) -> String {
    let now = jiff::Timestamp::now().as_millisecond().to_string();
    // Emit venue time at HTTP delivery, not before slow Windows RiskBook work:
    // the production mutation clock still enforces its strict 5s offset budget.
    fixture.feed.expect_current_public_clock();
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    record_leverage(&fixture.feed);
    record_fee(&fixture.feed, &now);
    record_fee(&fixture.feed, &now);
    record_leverage(&fixture.feed);
    create_post(&fixture.feed, OPEN_ORDER);
    fixture.feed.arm_lost_next_create_ack();
    let mut request = execute();
    if let TradeRequest::Execute { expires_in_ms, .. } = &mut request {
        *expires_in_ms = 10_000;
    }
    let result = fixture
        .trade
        .trade(&fixture.config.alias, &request, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(
        result["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{result}"
    );
    result["intent_id"].as_str().unwrap().to_owned()
}

fn add_request() -> TradeRequest {
    serde_json::from_value(json!({
        "action":"execute","idempotency_key":"native_public_add_002",
        "operation":"add","strategy_instance":"manual","instrument":INSTRUMENT,
        "side":"long","sizing":{"contracts":"0.10"},
        "order":{"type":"limit","limit_price":"1000","max_entry_usd":"10","max_slippage_bps":0},
        "protection":{"stop_price":"900"},"expires_in_ms":10000
    }))
    .unwrap()
}

fn create_post(feed: &RecordedAccountHttp, order_id: &str) {
    feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":INSTRUMENT,"tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "ordType":"limit","sz":"0.1","px":"1000",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: envelope(json!([{"ordId":order_id,"clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
}

// Exactly one complete production scan, not spare copies for arbitrary retries.
pub(super) fn inventory(feed: &RecordedAccountHttp, stops: &[(&str, &str)]) {
    for kind in ["SWAP", "FUTURES"] {
        for order_type in [
            "conditional",
            "oco",
            "trigger",
            "move_order_stop",
            "chase",
            "iceberg",
            "twap",
            "smart_iceberg",
        ] {
            let data = if kind == "SWAP" && order_type == "conditional" {
                Value::Array(stops.iter().map(|(id, client)| json!({
                    "instType":"SWAP","instId":INSTRUMENT,"algoId":id,"algoClOrdId":client,
                    "ordType":"conditional","state":"live","side":"sell","posSide":"long","tdMode":"cross",
                    "sz":"0.1","slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
                })).collect())
            } else {
                json!([])
            };
            get(
                feed,
                "fixture-observer-a",
                "/api/v5/trade/orders-algo-pending",
                vec![
                    ("instType", kind),
                    ("ordType", order_type),
                    ("limit", "100"),
                ],
                data,
            );
        }
    }
}

#[allow(clippy::too_many_arguments)] // Two concrete source-backed entry Status phases.
async fn entry_status(
    fixture: &NativeFixture,
    intent: &str,
    order_id: &str,
    stop_id: &str,
    stop_client: &str,
    at: i64,
    equity: &str,
    position: &str,
    aggregate: Option<&[(&str, &str)]>,
) -> Value {
    order(&fixture.feed, order_id, "buy", at);
    snapshot(&fixture.feed, equity, position); // initial signed exposure
    get(
        &fixture.feed,
        "fixture-executor-a",
        "/api/v5/trade/order-algo",
        vec![("algoClOrdId", stop_client)],
        json!([{
            "instId":INSTRUMENT,"algoId":stop_id,"algoClOrdId":stop_client,"state":"live","sz":"0.1",
            "side":"sell","posSide":"long","tdMode":"cross","ordType":"conditional",
            "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
        }]),
    );
    if let Some(stops) = aggregate {
        inventory(&fixture.feed, stops);
        snapshot(&fixture.feed, equity, position); // original full-cohort protection census
    }
    snapshot(&fixture.feed, equity, position); // original financial capture
    history(&fixture.feed, None);
    let status = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: intent.into(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(status["data"]["intent"]["state"], "FILLED", "{status}");
    assert_eq!(
        status["data"]["intent"]["plan"]["strategy_instance"], "manual",
        "{status}"
    );
    assert_eq!(
        status["data"]["entry_fill_evidence"]["state"], "verified_native_fill_and_bill_fee_sources",
        "{status}"
    );
    assert_eq!(
        status["data"]["incremental_entry_settlement"]["state"], "verified_or_no_change",
        "{status}"
    );
    assert_eq!(
        status["data"]["central_release"]["reason_code"], "INTENT_EXPOSURE_REMAINS",
        "{status}"
    );
    assert_eq!(status["data"]["protection"]["quantity"], "0.1", "{status}");
    assert_eq!(status["data"]["protection_incident"], false, "{status}");
    assert_eq!(
        status["data"]["protection_source_pending"], false,
        "{status}"
    );
    assert!(
        status["data"]["aggregate_protection_blocker"].is_null(),
        "{status}"
    );
    assert_consumed(fixture);
    status
}

async fn lookup(fixture: &NativeFixture, intent: &str) -> okx_trade_execution::StoredIntent {
    fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_trade_execution::TradeIntentLookup::Reference(intent.into()),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn lots(fixture: &NativeFixture) -> Vec<okx_trade_execution::StoredLot> {
    fixture
        .owner
        .active_lots(
            fixture.config.account_key.wire(),
            INSTRUMENT.into(),
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap()
}

fn assert_owned_lot(lot: &okx_trade_execution::StoredLot, intent: &str) {
    assert_eq!(lot.intent_id.as_deref(), Some(intent));
    assert_eq!(lot.instance_id.as_deref(), Some("manual"));
    assert_eq!(lot.instrument_id, INSTRUMENT);
    assert_eq!(lot.side, "long");
    assert_eq!(lot.remaining_qty, Decimal::new(1, 1));
    assert_eq!(lot.cost, Decimal::from(1000));
    assert!(lot.fill_id.is_some());
}

async fn assert_finance(fixture: &NativeFixture, fees: Decimal, reservations: u64) {
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
    assert_eq!(risk.daily_loss_usd, fees);
    assert_eq!(stops, 0);
    let account = fixture
        .owner
        .stopped_account_summary(fixture.campaign.clone(), fixture.config.account_key.wire())
        .await
        .unwrap();
    assert_eq!(account.active_reservations, reservations);
}

fn assert_consumed(fixture: &NativeFixture) {
    assert_eq!(
        fixture.feed.unconsumed_reads(),
        0,
        "denied={:?}",
        fixture.feed.denied_routes.lock().unwrap()
    );
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
}
