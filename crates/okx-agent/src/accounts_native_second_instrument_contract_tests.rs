//! A distinct permitted linear contract uses its own public spec and sources.
use super::filled_fixture::opened_market;
use super::*;
use rust_decimal::Decimal;

const INSTRUMENT: &str = "DOGE-USDT-SWAP";
const INDEX: &str = "DOGE-USDT";

#[tokio::test]
async fn risk_book_contract_native_public_second_instrument_spec_precision_and_permission() {
    let mut fixture = fixture().await;
    // Reuse production opening maintenance, without inventing a financial
    // opening or changing its closed history frontier for this instrument.
    let (initial_stop, initial_task, _, _) = opened_market(&mut fixture).await;
    initial_stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), initial_task)
        .await
        .unwrap()
        .unwrap();
    *fixture.market_boundary.market.lock().unwrap() = None;

    let (market, public, socket, reference_http) = second_market().await;
    let mut updates = public.subscribe_market_updates();
    let (stop, stopped) = watch::channel(false);
    let market_task = tokio::spawn(async move { market.run(stopped).await.unwrap() });
    public.demand_instrument(INSTRUMENT).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if public
                .risk_fresh_snapshot(
                    INSTRUMENT,
                    chrono::Utc::now().timestamp_millis() as u64,
                    2_000,
                    180_000,
                    chrono::Utc::now().to_rfc3339(),
                )
                .await
                .is_ok()
            {
                break;
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .expect("the second instrument must be parsed from its own raw WS subscriptions");
    fixture.trade = Arc::new(TradeService::new(
        fixture.registry.clone(),
        public.clone(),
        NativeRiskClient::local(NativeFinanceRisk::from_owner(fixture.owner.clone())),
    ));
    fixture
        .trade
        .update_governance(&fixture.authority)
        .await
        .unwrap();

    // The same supported product is still forbidden unless this account's
    // actual governance explicitly permits its different instrument ID.
    let no_permission = request("second_spec_permission_001", "0.10");
    refused(&fixture, &no_permission, "INSTRUMENT_NOT_ALLOWED").await;
    fixture.authority.state.revision += 1;
    fixture.authority.state.accounts[0].policy_revision = fixture.authority.state.revision;
    fixture.authority.state.accounts[0]
        .limits
        .allowed_instruments
        .push(INSTRUMENT.into());
    fixture
        .trade
        .update_governance(&fixture.authority)
        .await
        .unwrap();

    *fixture.market_boundary.market_quote.lock().unwrap() = Some(ExecuteMarketQuote {
        instrument: INSTRUMENT,
        index: INDEX,
        ask: "0.04005",
        bid: "0.04000",
    });
    *fixture.market_boundary.market.lock().unwrap() = Some((socket, public));
    // Two named negative planning quotes, one positive planning quote, then
    // the two actual pre-reserve/post-HELD inventory quote observations.
    fixture
        .market_boundary
        .market_updates_expected
        .store(5, Ordering::SeqCst);
    for (key, size, reason) in [
        (
            "second_spec_off_grid_002",
            "0.11",
            "QUANTITY_NOT_ON_VENUE_LOT",
        ),
        (
            "second_spec_subminimum_003",
            "0.05",
            "ENTRY_BUDGET_BELOW_VENUE_MINIMUM",
        ),
    ] {
        fixture
            .market_boundary
            .publish_market_observation(deadline())
            .await;
        fee(&fixture.feed);
        refused(&fixture, &request(key, size), reason).await;
    }

    fixture
        .market_boundary
        .publish_market_observation(deadline())
        .await;
    let now = chrono::Utc::now().timestamp_millis().to_string();
    fixture
        .feed
        .expect_public_clock(envelope(json!([{"ts":now}])));
    fee(&fixture.feed); // plan
    leverage(&fixture.feed); // plan
    leverage(&fixture.feed); // original entry quote
    fee(&fixture.feed); // original entry quote
    fee(&fixture.feed); // post-HELD quote
    leverage(&fixture.feed); // post-HELD quote
    fixture.feed.expect_post(RecordedPost {
        access_key: "fixture-executor-a".into(),
        path: "/api/v5/trade/order".into(),
        expected_body: json!({"instId":INSTRUMENT,"tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
            "ordType":"limit","sz":"0.1","px":"0.04005",
            "attachAlgoOrds":[{"attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"0.038",
                "slTriggerPxType":"mark","slOrdPx":"-1"}]}),
        raw_response: envelope(json!([{"ordId":"native-second-spec-open-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}])),
    });
    fixture.feed.arm_lost_next_create_ack();
    let positive = request("second_spec_permitted_004", "0.10");
    let result = fixture
        .trade
        .trade(&fixture.config.alias, &positive, Some(&fixture.authority))
        .await
        .expect("public Execute must authorize its actual permitted DOGE spec");
    assert_eq!(
        result["reason_code"], "EXCHANGE_POST_OUTCOME_UNKNOWN",
        "{result}"
    );
    let intent = fixture
        .owner
        .lookup_intent(
            fixture.config.account_key.wire(),
            okx_execution::TradeIntentLookup::Reference(
                result["intent_id"].as_str().unwrap().into(),
            ),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(intent.instrument_id, INSTRUMENT);
    assert_eq!(intent.state, okx_execution::IntentState::Unknown);
    assert_eq!(intent.plan.contract_value, Decimal::from(1_000));
    assert_eq!(intent.plan.quantity, Decimal::new(1, 1));
    assert_eq!(intent.plan.limit_price, Decimal::new(4005, 5));
    assert_eq!(intent.plan.stop_price, Some(Decimal::new(38, 3)));
    assert_eq!(intent.plan.planned_loss_usd, Decimal::new(21301, 5));
    let (quote, admission) = fixture
        .owner
        .entry_cost_evidence(
            deadline(),
            fixture.config.account_key.wire(),
            intent.intent_id.clone(),
        )
        .await
        .unwrap();
    let quote = quote.unwrap();
    // Independent dimensions: .1 contracts * 1000 DOGE * .04005 = 4.005 USDT;
    // fee4.005*.0005=.0020025 rounds UP to .002003, total4.007003 USD.
    assert_eq!(quote.notional_usdt, Decimal::new(4005, 3));
    assert_eq!(quote.fee_usdt, Decimal::new(2003, 6));
    assert_eq!(quote.total_usd, Decimal::new(4007003, 6));
    assert_eq!(admission.unwrap().entry_upper_usd, Decimal::new(4007003, 6));
    let reserved = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(reserved.reserved_entry_usd, "4.007003");
    assert_eq!(reserved.remaining_entry_usd, "5.992997");
    assert_eq!(reserved.reserved_loss_usd, "0.21301");
    assert_eq!(reserved.consumed_entry_usd, "0");
    assert_eq!(fixture.feed.outbound_posts.lock().unwrap().len(), 1);
    assert_eq!(fixture.market_boundary.updates.load(Ordering::SeqCst), 5);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert_eq!(fixture.feed.unconsumed_public_clock(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    assert_eq!(reference_http.reads.load(Ordering::SeqCst), 2);
    assert!(reference_http.pages.lock().unwrap().is_empty());
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), market_task)
        .await
        .unwrap()
        .unwrap();
    fixture.finish().await;
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn request(key: &str, size: &str) -> TradeRequest {
    let request = serde_json::from_value(json!({
        "action":"execute","idempotency_key":key,"operation":"open","strategy_instance":"manual",
        "instrument":INSTRUMENT,"side":"long","sizing":{"contracts":size},
        "order":{"type":"limit","limit_price":"0.04005","max_entry_usd":"10","max_slippage_bps":0},
        "protection":{"stop_price":"0.03800"},"expires_in_ms":10000
    }))
    .unwrap();
    assert!(TradeRequest::valid(&request));
    request
}

async fn refused(fixture: &NativeFixture, request: &TradeRequest, reason: &str) {
    let before = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();
    let result = fixture
        .trade
        .trade(&fixture.config.alias, request, Some(&fixture.authority))
        .await;
    assert_eq!(result.unwrap_err(), reason);
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        before
    );
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    let TradeRequest::Execute {
        idempotency_key, ..
    } = request
    else {
        unreachable!()
    };
    assert!(
        fixture
            .owner
            .lookup_intent(
                fixture.config.account_key.wire(),
                okx_execution::TradeIntentLookup::IdempotencyKey(idempotency_key.clone()),
                deadline()
            )
            .await
            .unwrap()
            .is_none(),
        "precision/permission refusal must precede intent preparation"
    );
    assert!(
        fixture
            .owner
            .active_lots(
                fixture.config.account_key.wire(),
                INSTRUMENT.into(),
                deadline()
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
}

fn fee(feed: &RecordedAccountHttp) {
    feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/account/trade-fee".into(),
        required_query: vec![
            ("instType".into(), "SWAP".into()),
            ("instFamily".into(), INDEX.into()),
        ],
        raw_response: envelope(json!([{"instType":"SWAP","level":"Lv1",
            "ts":chrono::Utc::now().timestamp_millis().to_string(),
            "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]}])),
    });
}
fn leverage(feed: &RecordedAccountHttp) {
    feed.expect_get(RecordedGet {
        access_key: "fixture-observer-a".into(),
        path: "/api/v5/account/leverage-info".into(),
        required_query: vec![
            ("instId".into(), INSTRUMENT.into()),
            ("mgnMode".into(), "cross".into()),
        ],
        raw_response: envelope(json!([
            {"instId":INSTRUMENT,"mgnMode":"cross","posSide":"long","lever":"2"},
            {"instId":INSTRUMENT,"mgnMode":"cross","posSide":"short","lever":"2"}])),
    });
}

struct SecondReferenceHttp {
    pages: Mutex<std::collections::VecDeque<(&'static str, Vec<u8>)>>,
    reads: AtomicUsize,
}
impl HttpTransport for SecondReferenceHttp {
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
                .expect("two original complete spec scopes only");
            assert_eq!(
                request.url().query_pairs().collect::<Vec<_>>(),
                vec![("instType".into(), partition.into())]
            );
            self.reads.fetch_add(1, Ordering::SeqCst);
            Ok(okx_api::RawHttpResponse {
                status: okx_api::HttpStatusCode::OK,
                headers: Default::default(),
                body,
            })
        })
    }
}

async fn second_market() -> (
    okx_runtime::PublicWsCoordinator,
    okx_runtime::PublicWsHandle,
    Arc<RecordedWsConnector>,
    Arc<SecondReferenceHttp>,
) {
    let instrument = json!({"instType":"SWAP","instId":INSTRUMENT,"instFamily":INDEX,"uly":INDEX,
        "state":"live","ruleType":"normal","settleCcy":"USDT","tickSz":"0.00005",
        "lotSz":"0.05","minSz":"0.10","maxLmtSz":"1000","maxMktSz":"1000",
        "ctType":"linear","ctVal":"1000","ctValCcy":"DOGE","groupId":"4","lever":"20"});
    let http = Arc::new(SecondReferenceHttp {
        pages: Mutex::new(
            [
                ("SWAP", envelope(json!([instrument]))),
                ("FUTURES", envelope(json!([]))),
            ]
            .into(),
        ),
        reads: AtomicUsize::new(0),
    });
    let environment = OkxEnvironment::new(okx_api::Region::Global, false);
    let rate = RateBudget::new();
    let client =
        okx_api::OkxPublicClient::with_http_transport(environment, rate.clone(), http.clone())
            .unwrap();
    let source_floor = chrono::Utc::now().to_rfc3339();
    let rows = okx_api::PublicDataApi::new(client.clone())
        .derivative_instruments()
        .await
        .unwrap();
    let reference = okx_observation::ReferenceRegistry::from_public(source_floor, rows).unwrap();
    let socket = Arc::new(RecordedWsConnector::default());
    let at = chrono::Utc::now().timestamp_millis();
    for (argument, data) in [
        (json!({"channel":"instruments","instType":"SWAP"}), None),
        (json!({"channel":"instruments","instType":"FUTURES"}), None),
        (
            json!({"channel":"tickers","instId":INSTRUMENT}),
            Some(json!({"instType":"SWAP","instId":INSTRUMENT,
            "last":"0.04005","lastSz":"1","askPx":"0.04005","askSz":"10","bidPx":"0.04000","bidSz":"10","ts":at.to_string()})),
        ),
        (
            json!({"channel":"mark-price","instId":INSTRUMENT}),
            Some(
                json!({"instType":"SWAP","instId":INSTRUMENT,"markPx":"0.04005","ts":at.to_string()}),
            ),
        ),
        (
            json!({"channel":"index-tickers","instId":INDEX}),
            Some(json!({"instId":INDEX,"idxPx":"0.04005","ts":at.to_string()})),
        ),
        (
            json!({"channel":"funding-rate","instId":INSTRUMENT}),
            Some(
                json!({"instType":"SWAP","instId":INSTRUMENT,"fundingRate":"0.0001",
            "fundingTime":at.to_string(),"nextFundingTime":(at+28_800_000).to_string(),"ts":at.to_string()}),
            ),
        ),
        (
            json!({"channel":"open-interest","instId":INSTRUMENT}),
            Some(
                json!({"instType":"SWAP","instId":INSTRUMENT,"oi":"1000","oiCcy":"1000000","oiUsd":"40050","ts":at.to_string()}),
            ),
        ),
        (
            json!({"channel":"books","instId":INSTRUMENT}),
            Some(json!({"asks":[["0.04005","10","0","1"]],
            "bids":[["0.04000","10","0","1"]],"seqId":10,"prevSeqId":-1,"ts":at.to_string()})),
        ),
    ] {
        let ack = json!({"event":"subscribe","arg":argument,"connId":"native-second-spec-market"})
            .to_string();
        let mut frames = vec![];
        if let Some(data) = data {
            let mut frame = json!({"arg":argument,"data":[data]});
            if argument["channel"] == "books" {
                frame["action"] = "snapshot".into();
            }
            frames.push(frame.to_string());
        }
        if argument["channel"] == "books" {
            frames.push(ack);
        } else {
            frames.insert(0, ack);
        }
        socket.expect_public_subscription(argument, frames);
    }
    let (coordinator, handle) =
        okx_runtime::PublicWsCoordinator::new_with_connector_and_public_client(
            environment,
            reference,
            rate,
            socket.clone(),
            client,
        );
    (coordinator, handle, socket, http)
}
