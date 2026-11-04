// T09-SPEC: a second genuine linear reference spec goes through the same
// public Rust TradeService, Native account SQL and persisted Worker RiskBook.
async fn t09_spec_second_linear_product(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    public_ws: okx_runtime::PublicWsHandle,
    connector: Arc<crate::accounts::recorded_support::RecordedWsConnector>,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    rate_budget: RateBudget,
) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
    use okx_protocol::trading::{
        OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
    };
    use serde_json::json;
    const INST:&str="ETH-USDT-SWAP";
    let stamp=chrono::Utc::now().timestamp_millis();
    let ts=stamp.to_string();
    let funding=(stamp+8*60*60*1000).to_string();
    // These are raw public venue frames for the dynamic second ticker, not a
    // second MarketSnapshot builder or a direct mutation of production state.
    let subscriptions=vec![
        (json!({"channel":"tickers","instId":INST}),
         json!({"instType":"SWAP","instId":INST,
           "last":"500","lastSz":"1","askPx":"500","askSz":"20",
           "bidPx":"499.5","bidSz":"20","ts":ts}),None),
        (json!({"channel":"mark-price","instId":INST}),
         json!({"instType":"SWAP","instId":INST,"markPx":"500","ts":ts}),None),
        (json!({"channel":"index-tickers","instId":"ETH-USDT"}),
         json!({"instId":"ETH-USDT","idxPx":"500","ts":ts}),None),
        (json!({"channel":"funding-rate","instId":INST}),
         json!({"instType":"SWAP","instId":INST,"fundingRate":"0.0001",
            "fundingTime":ts,"nextFundingTime":funding,"ts":ts}),None),
        (json!({"channel":"open-interest","instId":INST}),
         json!({"instType":"SWAP","instId":INST,
            "oi":"1000","oiCcy":"20","oiUsd":"10000","ts":ts}),None),
        (json!({"channel":"books","instId":INST}),
         json!({"asks":[["500","20","0","1"]],
            "bids":[["499.5","20","0","1"]],
            "seqId":10,"prevSeqId":-1,"ts":ts}),Some("snapshot")),
    ];
    let refresh_subscriptions=subscriptions.clone();
    for (arg,data,action) in subscriptions {
        let ack=json!({"event":"subscribe","arg":arg,"connId":"recorded-public"}).to_string();
        let book=arg["channel"]=="books";
        let mut frames=Vec::new();
        if !book { frames.push(ack.clone()); }
        let mut message=json!({"arg":arg,"data":[data]});
        if let Some(action)=action { message["action"]=action.into(); }
        frames.push(message.to_string());
        if book { frames.push(ack); }
        connector.expect_public_subscription(arg,frames);
    }
    public_ws.demand_instrument(INST).await.expect("T09-SPEC public instrument demand");
    let mut observed=public_ws.subscribe_market_updates();
    let market=tokio::time::timeout(Duration::from_secs(8),async {
        loop{
            if let Ok(m)=public_ws.risk_fresh_snapshot(
                INST,chrono::Utc::now().timestamp_millis() as u64,5000,5000,
                chrono::Utc::now().to_rfc3339()
            ).await {break m;}
            observed.changed().await.expect("T09-SPEC market coordinator exited");
        }
    }).await.expect("T09-SPEC public source deadline");
    assert_eq!(market.market.ticker.best_ask,"500");
    assert_eq!(connector.unconsumed_public_subscriptions(),0);
    let rules=public_ws.instrument_rules(INST).await.expect("T09-SPEC valid rules");
    assert_eq!(rules.instrument.tick_size,"0.5");
    assert_eq!(rules.instrument.lot_size,"0.05");
    assert_eq!(rules.instrument.min_size,"0.05");
    assert_eq!(rules.instrument.contract_value.as_deref(),Some("0.02"));
    assert_eq!(rules.instrument.contract_value_currency.as_deref(),Some("ETH"));
    let wrap=|data:serde_json::Value|serde_json::to_vec(&json!({
        "code":"0","msg":"","data":data
    })).unwrap();
    let fee=wrap(json!([{
        "instType":"SWAP","level":"Lv1","ts":ts,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/trade-fee",
        (0..12).map(|_|RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"ETH-USDT".into()),
            ],raw_response:fee.clone()
        }).collect()
    );
    let lev=wrap(json!([
        {"instId":INST,"mgnMode":"cross","posSide":"long","lever":"2"},
        {"instId":INST,"mgnMode":"cross","posSide":"short","lever":"2"}
    ]));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/leverage-info",
        (0..12).map(|_|RecordedGet{
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/leverage-info".into(),
            required_query:vec![
                ("instId".into(),INST.into()),
                ("mgnMode".into(),"cross".into())
            ],raw_response:lev.clone()
        }).collect()
    );
    feed.advance_recorded_public_clock(wrap(json!([{"ts":ts}])));
    let mk_preview=|contracts:&str,limit:&str|TradeRequest::Preview {
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:INST.into(),side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:contracts.into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some(limit.into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0
        },
        protection:Some(Protection::Price{stop_price:"450".into()}),
    };
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,4,"T09-SPEC only after two positive A orders");

    // The same newly discovered public metadata cannot grant account A the
    // right to trade ETH. Only B's separately governed permission allows it.
    let denied_a=trade.trade("fixture-a",&mk_preview("0.10","500"),None)
        .await.expect_err("T09-SPEC public registry must not authorize A");
    assert_eq!(denied_a,"INSTRUMENT_NOT_ALLOWED");

    // Invalid exact size is a source-validated rejection, never a rounding
    // to the next lot. A noncrossing price below the 500 ask must not silently
    // move outside the user's limit to the next 0.5 price tick.
    let bad_lot=trade.trade("fixture-b",&mk_preview("0.06","500"),None)
        .await.expect_err("T09-SPEC off-lot quantity must reject");
    assert_eq!(bad_lot,"QUANTITY_NOT_ON_VENUE_LOT");
    let bad_tick=trade.trade("fixture-b",&mk_preview("0.1","499.9"),None)
        .await.expect_err("T09-SPEC unsatisfiable price bound must reject");
    assert_eq!(bad_tick,"NO_EXECUTABLE_PRICE_WITHIN_SLIPPAGE");
    // A positive budget of 0.5 USD cannot buy the minimum 0.05
    // ETH contracts at 0.02 ETH/contract and the signed 500 ask.
    // Budget sizing exercises the MINIMUM gate independently of the
    // prior explicit Contracts off-lot rejection (0.06).
    let small_budget=TradeRequest::Preview {
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:INST.into(),side:PositionSide::Long,
        sizing:Sizing::EntryBudgetUsd{entry_budget_usd:"0.5".into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some("500".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"450".into()}),
    };
    assert!(small_budget.valid(),"T09-SPEC valid sub-minimum budget probe");
    let bad_min=trade.trade("fixture-b",&small_budget,None)
        .await.expect_err("T09-SPEC minimum must reject without a venue POST");
    assert_eq!(bad_min,"ENTRY_BUDGET_BELOW_VENUE_MINIMUM");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before);

    let until=std::time::Instant::now()+Duration::from_secs(15);
    let preview=loop {
        let result=tokio::time::timeout_at(until.into(),
            trade.trade("fixture-b",&mk_preview("0.10","500"),None)
        ).await.expect("T09-SPEC finite preview");
        match result {
            Ok(value)=>break value,
            Err(reason)=>{
                let remaining=until.saturating_duration_since(std::time::Instant::now());
                let defer=recorded_preview_local_defer(
                    &reason,&rate_budget,remaining
                ).unwrap_or_else(||panic!("T09-SPEC preview source failure: {reason}"));
                tokio::time::timeout_at(until.into(),tokio::time::sleep(defer))
                    .await.expect("T09-SPEC preview rate admission");
            }
        }
    };
    let quote=preview["data"]["entry_costs"]["entry_total_usd"]
        .as_str().expect("T09-SPEC preview exact USD");
    assert_eq!(quote,"1.0005");

    // The negative/preview probes above use finite LOCAL rate admission and
    // may outlive the original ETH market observation. Publish another
    // independently recorded set of RAW venue frames at this precise
    // pre-Execute boundary. The unchanged PublicWsCoordinator parser must
    // authenticate every channel's new exchange timestamp before risk can
    // be requested. No stale snapshot is rewritten or production TTL raised.
    let continuing_subscriptions=refresh_subscriptions.clone();
    let refreshed=chrono::Utc::now().timestamp_millis();
    let refreshed_at=refreshed.to_string();
    let mut updates=public_ws.subscribe_market_updates();
    for (arg,mut data,action) in refresh_subscriptions {
        data["ts"]=refreshed_at.clone().into();
        if data.get("fundingTime").is_some() {
            data["fundingTime"]=refreshed_at.clone().into();
            data["nextFundingTime"]=(refreshed+8*60*60*1000).to_string().into();
        }
        if action==Some("snapshot") {
            data["seqId"]=11.into();
        }
        let mut frame=json!({"arg":arg,"data":[data]});
        if let Some(kind)=action {frame["action"]=kind.into();}
        connector.send_public_frame(frame.to_string());
    }
    let fresh_market=tokio::time::timeout(Duration::from_secs(5),async {
        loop {
            if let Ok(source)=public_ws.risk_fresh_snapshot(
                INST,chrono::Utc::now().timestamp_millis() as u64,
                5000,5000,chrono::Utc::now().to_rfc3339()
            ).await {
                if source.market.ticker.exchange_timestamp_ms==refreshed_at
                    && source.market.mark_price.exchange_timestamp_ms==refreshed_at
                    && source.market.index_price.exchange_timestamp_ms==refreshed_at
                    && source.market.open_interest.exchange_timestamp_ms==refreshed_at
                    && source.market.funding.as_ref().is_some_and(|f|
                        f.exchange_timestamp_ms==refreshed_at)
                {
                    break source;
                }
            }
            updates.changed().await.expect("T09-SPEC public source coordinator exited");
        }
    }).await.expect("T09-SPEC all freshly signed ETH channels required");
    assert_eq!(fresh_market.market.ticker.best_ask,"500");
    feed.advance_recorded_public_clock(
        wrap(json!([{"ts":refreshed_at}]))
    );
    // Explicitly re-observe the signed fee source. The earlier negative
    // Preview and read-admission phases can consume its 5-second freshness
    // window. Never promote the prior fee timestamp into a new quote.
    let fee_at=chrono::Utc::now().timestamp_millis().to_string();
    let live_fee=wrap(json!([{
        "instType":"SWAP","level":"Lv1","ts":fee_at,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    feed.advance_recorded_route("fixture-observer-b",
        "/api/v5/account/trade-fee",
        (0..12).map(|_|RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"ETH-USDT".into()),
            ],raw_response:live_fee.clone(),
        }).collect()
    );

    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/order".into(),
        expected_body:json!({
            "instId":INST,"tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit",
            "sz":"0.1","px":"500",
            "attachAlgoOrds":[{
                "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark",
                "slTriggerPx":"450","slTriggerPxType":"mark","slOrdPx":"-1"
            }]
        }),
        raw_response:br#"{"code":"0","msg":"","data":[{"ordId":"recorded-eth-spec-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
    });
    let mut readback=Vec::new();
    for _ in 0..6 {
        readback.push(RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/trade/order".into(),
            required_query:vec![("instId".into(),INST.into())],
            raw_response:wrap(json!([{
                "instId":INST,"ordId":"recorded-eth-spec-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","tdMode":"cross","ordType":"limit",
                "px":"500","sz":"0.1","accFillSz":"0","avgPx":"",
                "state":"live","cTime":ts,"uTime":ts
            }]))
        });
    }
    for page in readback {feed.expect_get(page);}
    let request=TradeRequest::Execute {
        idempotency_key:"t09_spec_second_linear_open_0001".into(),
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:INST.into(),side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:"0.10".into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some("500".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0
        },
        protection:Some(Protection::Price{stop_price:"450".into()}),
        expires_in_ms:30_000,
    };
    // Execute includes a real campaign-opening read barrier and several
    // independent private REST admissions. A single 5-second ETH public
    // snapshot cannot cover an arbitrarily delayed transaction. Supply
    // successive monotonically sequenced, raw exchange-shaped WS messages
    // while the original Execute is in flight; all freshness and book
    // coherency still belong to the unmodified PublicWsCoordinator.
    // No RiskBook/SQL/market snapshot mutation is performed here.
    let (stop_tx,mut stop_rx)=tokio::sync::oneshot::channel::<()>();
    let publisher_connector=connector.clone();
    let publisher=tokio::spawn(async move {
        let mut sequence=11_i64;
        let mut interval=tokio::time::interval(Duration::from_millis(650));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _=&mut stop_rx=>break,
                _=interval.tick()=>{
                    sequence+=1;
                    let stamp=chrono::Utc::now().timestamp_millis();
                    for (arg,mut data,action) in continuing_subscriptions.iter().cloned() {
                        data["ts"]=stamp.to_string().into();
                        if data.get("fundingTime").is_some() {
                            data["fundingTime"]=stamp.to_string().into();
                            data["nextFundingTime"]=
                                (stamp+8*60*60*1000).to_string().into();
                        }
                        let mut frame=json!({"arg":arg,"data":[data]});
                        if action==Some("snapshot") {
                            frame["data"][0]["seqId"]=sequence.into();
                            frame["data"][0]["prevSeqId"]=(sequence-1).into();
                            frame["action"]="update".into();
                        }
                        publisher_connector.send_public_frame(frame.to_string());
                    }
                }
            }
        }
    });
    let execution=trade.trade("fixture-b",&request,None).await;
    let _=stop_tx.send(());
    publisher.await.expect("T09-SPEC RAW ETH frame publisher joined");
    let accepted=execution
        .expect("T09-SPEC public Execute must reach real Native Worker risk");
    let id=accepted["data"]["intent"]["intent_id"].as_str()
        .expect("T09-SPEC accepted intent id").to_owned();
    let resource=accounts.resolve("fixture-b").await.expect("T09-SPEC B SQL");
    let sql_id=id.clone();
    let (intent,central,fence)=resource.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| {
            let intent=s.intent(&sql_id)?.ok_or(
                okx_execution::StoreError::Invalid("T09_SPEC_SQL_INTENT_MISSING")
            )?;
            let central=s.active_central_receipt(&sql_id)?;
            let fence=s.post_fence_evidence(&sql_id)?;
            Ok((intent,central,fence))
        }
    ).await.expect("T09-SPEC B persisted SQL");
    assert_eq!(intent.state.as_str(),"ACKNOWLEDGED");
    assert_eq!(intent.exchange_order_id.as_deref(),Some("recorded-eth-spec-001"));
    assert_eq!(intent.plan.quantity,Decimal::new(1,1));
    assert_eq!(intent.plan.contract_value,Decimal::new(2,2));
    assert!(fence.0>0,"T09-SPEC BEGIN_POST must be durable");
    assert!(central.is_some(),"T09-SPEC Worker HELD receipt must be bound to B SQL");
    let mut summary_cmd=summary("risk_t09_spec_after_second_linear");
    summary_cmd.installation_id="installation_test".into();
    let worker=risk.request(summary_cmd).await.expect("T09-SPEC persisted Worker");
    let budget=worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(budget.reserved_usd,"8.002");
    assert_eq!(budget.consumed_usd,"1.0005");
    let posts_after=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_after,posts_before+1,"one signed ETH create only");
    serde_json::json!({
        "schema":"okx.t09-spec-second-linear-native-worker/v1",
        "account_uid":resource.configuration.account_key.verified_uid,
        "instrument":INST,"tick":"0.5","lot":"0.05","minimum":"0.05",
        "multiplier":"0.02","quote_entry_upper_usd":quote,
        "no_metadata_permission_expansion":denied_a,
        "invalid_lot_reason":bad_lot,"invalid_tick_reason":bad_tick,
        "invalid_min_size_reason":bad_min,
        "intent_id":id,"intent_state":intent.state.as_str(),
        "sql_revision":intent.revision,
        "sql_risk_held":central.is_some(),
        "begin_post_at_ms":fence.0,
        "venue_order_id":intent.exchange_order_id,
        "posts_before":posts_before,"posts_after":posts_after,
        "worker_budget":worker.budget,"worker_sequence":worker.risk_book_sequence,
    })
}
