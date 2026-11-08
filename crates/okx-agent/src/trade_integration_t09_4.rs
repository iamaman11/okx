
    // T09.4: a distinct, provenance-bound Add order is admitted ONLY through
    // the existing TradeService and a fully sourced managed-position census.
    // All venue responses are immutable raw transport inputs, not SQL writes.
    struct T09_4Context {
        accounts: Arc<AccountRuntimeRegistry>,
        trade: Arc<TradeService>,
        risk: NativeRiskClient,
        rate_budget: RateBudget,
        feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
        prior_client_id: String,
        campaign_id: String,
        native_origin: i64,
        first_fill_at_ms: i64,
        terminal_fill_at_ms: i64,
        need_new_market: tokio::sync::oneshot::Sender<()>,
        fresh_market_ready: tokio::sync::oneshot::Receiver<()>,
    }


    struct T09_4FillContext {
        accounts: Arc<AccountRuntimeRegistry>,
        trade: Arc<TradeService>,
        risk: NativeRiskClient,
        rate_budget: RateBudget,
        feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
        intent_id: String,
        client_id: String,
        old_client_id: String,
        first_fill_at_ms: i64,
        terminal_fill_at_ms: i64,
    }

    // FILLED is an exchange order state, NOT evidence that immutable fees
    // reached the native SQL owner and were settled with central RiskBook.
    // An opened position may legitimately keep central RELEASE pending; the
    // independent Worker budget oracle below still requires exact 6.003.
    fn t09_4_incremental_entry_proven(status:&serde_json::Value)->bool {
        status["data"]["intent"]["state"]=="FILLED"
            && status["data"]["entry_fill_evidence"]["state"]
                =="verified_native_fill_and_bill_fee_sources"
            && status["data"]["incremental_entry_settlement"]["state"]
                =="verified_or_no_change"
            && status["data"]["financial_error"].is_null()
            && status["data"]["protection_incident"]!=true
            && status["data"]["protection_source_pending"]!=true
    }

    #[test]
    fn t09_4_filled_is_not_central_entry_settlement_proof() {
        let mut status=serde_json::json!({"data":{
            "intent":{"state":"FILLED"},
            "entry_fill_evidence":{"state":"verified_native_fill_and_bill_fee_sources"},
            "incremental_entry_settlement":{"state":"verified_or_no_change"},
            "financial_error":null,
            "protection_incident":false
        }});
        assert!(t09_4_incremental_entry_proven(&status));
        status["data"]["incremental_entry_settlement"]["state"]=serde_json::json!("pending");
        assert!(!t09_4_incremental_entry_proven(&status));
        status["data"]["incremental_entry_settlement"]["state"]=serde_json::json!("not_evaluated");
        assert!(!t09_4_incremental_entry_proven(&status));
        status["data"]["incremental_entry_settlement"]["state"]=serde_json::json!("verified_or_no_change");
        status["data"]["entry_fill_evidence"]["state"]=serde_json::json!("native_fills_verified_bill_pending");
        assert!(!t09_4_incremental_entry_proven(&status));
        status["data"]["entry_fill_evidence"]["state"]=serde_json::json!("verified_native_fill_and_bill_fee_sources");
        status["data"]["financial_error"]=serde_json::json!("ACCOUNT_SOURCE_NOT_READY");
        assert!(!t09_4_incremental_entry_proven(&status));
        status["data"]["financial_error"]=serde_json::Value::Null;
        status["data"]["protection_incident"]=serde_json::json!(true);
        assert!(!t09_4_incremental_entry_proven(&status));
        status["data"]["protection_incident"]=serde_json::json!(false);
        status["data"]["protection_source_pending"]=serde_json::json!(true);
        assert!(!t09_4_incremental_entry_proven(&status),
            "stale position source is not a proven managed protection census");
    }

    // The only injected material is the venue's raw signed GET history,
    // position, protective algo and order details. The same production
    // account capture, SQL lot owner, TradeService and Worker settle them.
    async fn t09_4_fill_phase(input: T09_4FillContext) -> serde_json::Value {
        use crate::accounts::recorded_support::RecordedGet;
        let T09_4FillContext {
            accounts, trade, risk, rate_budget, feed, intent_id, client_id,
            old_client_id, first_fill_at_ms, terminal_fill_at_ms,
        } = input;
        let envelope=|v|serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"","data":v
        })).unwrap();
        let filled_at=chrono::Utc::now().timestamp_millis();
        let old_stop=format!("s{}",&old_client_id[..31]);
        let new_stop=format!("s{}",&client_id[..31]);
        let mut fills=Vec::new();
        let mut bills=Vec::new();
        for (event,order,size,charge,ts) in [
            ("t09-second-001","recorded-t09-second-001","0.12","-0.0006",first_fill_at_ms),
            ("t09-second-002","recorded-t09-second-001","0.18","-0.0009",terminal_fill_at_ms),
            ("t09-managed-add-001","recorded-t09-add-001","0.2","-0.001",filled_at),
        ] {
            fills.push(serde_json::json!({
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "tradeId":format!("{event}-trade"),"ordId":order,
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "billId":format!("{event}-bill"),
                "subType":"3","side":"buy","posSide":"long",
                "fillPx":"1000","fillSz":size,"fillPnl":"0",
                "fee":charge,"feeCcy":"USDT",
                "ts":ts.to_string(),"fillTime":ts.to_string()
            }));
            bills.push(serde_json::json!({
                "billId":format!("{event}-bill"),"instType":"SWAP",
                "instId":"BTC-USDT-SWAP","ordId":order,
                "tradeId":format!("{event}-trade"),
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "ccy":"USDT","type":"2","subType":"3",
                "balChg":charge,"fee":charge,"pnl":"0",
                "ts":ts.to_string()
            }));
        }
        let mut history=Vec::new();
        for _ in 0..40 {
            for part in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
                history.push(RecordedGet {
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/fills-history".into(),
                    required_query:vec![("instType".into(),part.into())],
                    raw_response:if part=="SWAP" {
                        envelope(serde_json::json!(fills))
                    } else {envelope(serde_json::json!([]))},
                });
            }
        }
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/fills-history",history
        );
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/account/bills-archive",
            (0..40).map(|_|RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/account/bills-archive".into(),
                required_query:vec![],
                raw_response:envelope(serde_json::json!(bills)),
            }).collect()
        );
        let positions=envelope(serde_json::json!([{
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "posSide":"long","pos":"0.5","mgnMode":"cross",
            "avgPx":"1000","markPx":"1000","uTime":filled_at.to_string()
        }]));
        // Existing native background account-capture is the only authority
        // that may adopt the next source phase. The public Status must not
        // interpret the 0.50 external position before its source snapshot
        // has been ingested as part of that existing capture cycle.
        let source_boundary=chrono::Utc::now().timestamp_millis();
        // T09.4: stage this 0.50 venue snapshot until its immutable
        // 0.20 fill and linked bill are committed by the real SQL owner.
        // Do not expose a position the native census cannot yet attribute.
        let mut signed_orders=Vec::new();
        for _ in 0..24 {
            for (order,quantity,at,client) in [
                ("recorded-t09-second-001","0.3",terminal_fill_at_ms,&old_client_id),
                ("recorded-t09-add-001","0.2",filled_at,&client_id)
            ] {
                signed_orders.push(RecordedGet {
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/order".into(),
                    required_query:vec![
                        ("instId".into(),"BTC-USDT-SWAP".into()),
                        ("clOrdId".into(),client.clone()),
                    ],
                    raw_response:envelope(serde_json::json!([{
                        "instId":"BTC-USDT-SWAP","ordId":order,
                        "clOrdId":"__REQUEST_CL_ORD_ID__",
                        "side":"buy","posSide":"long","tdMode":"cross",
                        "ordType":"limit","px":"1000","sz":quantity,
                        "accFillSz":quantity,"avgPx":"1000","state":"filled",
                        "cTime":at.to_string(),"uTime":at.to_string()
                    }])),
                });
            }
        }
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order",signed_orders
        );
        let mut signed_algos=Vec::new();
        for _ in 0..16 {
            for (stop,algo,quantity) in [
                (&old_stop,"900000002","0.3"),
                (&new_stop,"900000003","0.2"),
            ] {
                signed_algos.push(RecordedGet {
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/order-algo".into(),
                    required_query:vec![("algoClOrdId".into(),stop.clone())],
                    raw_response:envelope(serde_json::json!([{
                        "instId":"BTC-USDT-SWAP","algoId":algo,
                        "algoClOrdId":stop,"state":"live","sz":quantity,
                        "side":"sell","posSide":"long","tdMode":"cross",
                        "ordType":"conditional","slTriggerPx":"900",
                        "slTriggerPxType":"mark","slOrdPx":"-1"
                    }])),
                });
            }
        }
        // The independently signed attached stop must remain available
        // as soon as the venue ACK exists: TradeService may verify it before
        // the finance ingest catches up. This one-order readback is NOT the
        // full managed-position census. Aggregate position and both live
        // stops are separately gated by the immutable SQL fill/bill below.
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order-algo",signed_algos
        );
        let mut pending=Vec::new();
        for _ in 0..12 {
            for inst in ["SWAP","FUTURES"] {
                for kind in [
                    "conditional","oco","trigger","move_order_stop",
                    "chase","iceberg","twap","smart_iceberg"
                ] {
                    let list=if inst=="SWAP" && kind=="conditional" {
                        serde_json::json!([
                            {
                                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                "ordType":"conditional","algoId":"900000002",
                                "algoClOrdId":old_stop,"state":"live",
                                "side":"sell","posSide":"long","sz":"0.3",
                                "tdMode":"cross","slTriggerPx":"900",
                                "slTriggerPxType":"mark","slOrdPx":"-1"
                            },
                            {
                                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                                "ordType":"conditional","algoId":"900000003",
                                "algoClOrdId":new_stop,"state":"live",
                                "side":"sell","posSide":"long","sz":"0.2",
                                "tdMode":"cross","slTriggerPx":"900",
                                "slTriggerPxType":"mark","slOrdPx":"-1"
                            }
                        ])
                    } else {serde_json::json!([])};
                    pending.push(RecordedGet {
                        access_key:"fixture-observer-a".into(),
                        path:"/api/v5/trade/orders-algo-pending".into(),
                        required_query:vec![
                            ("instType".into(),inst.into()),
                            ("ordType".into(),kind.into()),
                            ("limit".into(),"100".into())
                        ],
                        raw_response:envelope(list),
                    });
                }
            }
        }
        // Both owned pending stops are published together at the barrier
        // below, so a concurrent census never sees a premature new stop.
        let owner=accounts.resolve("fixture-a").await.unwrap();
        let mut latest=String::from("NOT_OBSERVED");
        let sql=tokio::time::timeout(Duration::from_secs(16),async {
            loop {
                let id=intent_id.clone();
                let observed=owner.execution.store.call(
                    std::time::Instant::now()+Duration::from_secs(3),
                    move |s| {
                        let f=s.cursor("fills","SWAP")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_FILL_CURSOR_MISSING"))?;
                        let b=s.cursor("bills","account")?
                            .ok_or(okx_execution::StoreError::Invalid("T09_4_BILL_CURSOR_MISSING"))?;
                        // A committed fill/bill does not by itself certify
                        // that the SAME native SQL finance projection has
                        // written the corresponding live position lots.
                        // Publishing signed 0.50/2-stop observations before
                        // that projection makes an authentic aggregate
                        // protection census fail closed at a false frontier.
                        let lots=s.active_lots("BTC-USDT-SWAP")?;
                        let own_qty=lots.iter()
                            .filter(|l|l.intent_id.as_deref()==Some(id.as_str()))
                            .fold(Decimal::ZERO,|sum,l|sum+l.remaining_qty);
                        let prior_qty=lots.iter()
                            .filter(|l|l.intent_id.as_deref()!=Some(id.as_str()))
                            .fold(Decimal::ZERO,|sum,l|sum+l.remaining_qty);
                        let (intents,_pending)=s.risk_census_intents(100)?;
                        let unresolved=intents.iter()
                            .filter(|i|matches!(i.state,
                                okx_execution::IntentState::Posting |
                                okx_execution::IntentState::Unknown))
                            .map(|i|(i.intent_id.clone(),format!("{:?}",i.state)))
                            .collect::<Vec<_>>();
                        Ok((
                            s.intent_fill_cost_evidence(&id,f.watermark),
                            s.intent_fee_bill_evidence(&id,b.watermark),
                            f.watermark,b.watermark,f.coverage_gaps,b.coverage_gaps,
                            own_qty,prior_qty,unresolved
                        ))
                    }
                ).await;
                match observed {
                    Ok((Ok(f),Ok(b),fw,bw,fg,bg,own,prior,unresolved)) => {
                        latest=format!("fill={f:?} bill={b:?} fw={fw} bw={bw} own_lots={own} prior_lots={prior} unresolved={unresolved:?}");
                        if f.filled_quantity==Decimal::new(2,1)
                            && f.fill_count==1
                            && b.linked_bill_count==1
                            && b.conservative_verified_fee_usdt==Decimal::new(1,3)
                            && fw>=filled_at && bw>=filled_at
                            && fg.is_empty() && bg.is_empty()
                            && own==Decimal::new(2,1)
                            && prior==Decimal::new(3,1)
                            && unresolved.is_empty()
                        {
                            break serde_json::json!({
                                "fill_count":f.fill_count,
                                "quantity":f.filled_quantity.normalize().to_string(),
                                "fee_usdt":b.conservative_verified_fee_usdt.normalize().to_string(),
                                "bill_count":b.linked_bill_count,
                                "linked_bill_source_digest":b.source_digest,
                                "fill_watermark":fw,"bill_watermark":bw,
                                "sql_owned_add_lots":own.normalize().to_string(),
                                "sql_owned_prior_lots":prior.normalize().to_string(),
                                "sql_posting_or_unknown_intents":0
                            });
                        }
                    }
                    o => latest=format!("capture={o:?}"),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap_or_else(|_|panic!("T09_4_CAPTURE_UNPROVEN:{latest}"));
        // Only after native SQL has independently sourced the exact fill and
        // bill, projected 0.20 + 0.30 owned lots, and proved no unresolved
        // POSTING/UNKNOWN intent may the observer publish the 0.50
        // position and both live stops. No SQL/risk receipt is test-written.
        // The real background finance capture must not interleave a NEW
        // 0.50 position with the OLD single-stop inventory. That mixed
        // source generation legitimately triggers a protection incident
        // and an emergency venue POST, even though neither new venue
        // snapshot has yet been fully published. Atomically publish this
        // pair across the existing test-only capture lease; production
        // safety and both raw signed HTTP payloads remain unchanged.
        let inventory_phase_guard=owner.recorded_capture_gate.write().await;
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/account/positions",
            (0..64).map(|_|RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/positions".into(),
                required_query:vec![],raw_response:positions.clone(),
            }).collect()
        );
        // T09.4 is BEFORE the first protective Cancel. No cancel-dependent
        // inventory has been armed yet, so demanding a matching ACK here
        // was an invalid fixture transition (A1 repro: PHASE_MISSING_AT_RETIRE).
        // Publish the independent, immutable two-stop source only after the
        // genuine SQL fill/bill/lot frontier above. T09.6 will arm and retire
        // its own cancel-dependent inventory at the actual signed cancel ACK.
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/trade/orders-algo-pending",pending
        );
        drop(inventory_phase_guard);
        eprintln!("T09_4_CAUSAL_INVENTORY_AFTER_SQL_V1 fill=0.2 bill=0.001 position=0.5 stops=2");
        // The signed 0.50 position and two pending stops are now registered
        // on the real recorded HTTP source. Wait for the ordinary Native
        // account-capture loop to consume the position before asking
        // TradeService Status to evaluate managed protection. Status itself
        // must fetch the two signed stops; requiring a stop GET earlier
        // would deadlock the independent source owners. This is read-only,
        // time-bounded, and never certifies or invents a protected position.
        let mut last_snapshot=String::from("NOT_OBSERVED");
        tokio::time::timeout(Duration::from_secs(8),async {
            loop {
                let observed=owner.latest_snapshot.read().await.clone();
                let last_success=owner.source_capture_health.read().await.last_success_ms;
                let matching=observed.as_ref().map(|snap|
                    snap.positions.iter().filter(|p|
                        p.instrument_id=="BTC-USDT-SWAP"
                            && p.position_side=="long"
                    ).map(|p|p.position.as_str()).collect::<Vec<_>>()
                ).unwrap_or_default();
                if last_success.is_some_and(|ms|ms>=source_boundary)
                    && matching==vec!["0.5"]
                { break; }
                last_snapshot=format!(
                    "captured_after_phase={} signed_long_positions={matching:?}",
                    last_success.is_some_and(|ms|ms>=source_boundary)
                );
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        }).await.unwrap_or_else(|_|panic!(
            "T09_4_MANAGED_POSITION_NOT_CAPTURED_BEFORE_STATUS:{last_snapshot}"
        ));
        // An order may be FILLED before the account SQL and Worker finish
        // source-backed incremental settlement. Observe that distinction on
        // the SAME public TradeService Status: no direct SQL/Worker mutation,
        // no added venue POST, no invented receipt, no relaxed oracle/TTL.
        // Preserve the existing 15s bound and the first financial reason.
        let status={
            let deadline=std::time::Instant::now()+Duration::from_secs(15);
            let mut last=serde_json::Value::Null;
            let mut status_attempts=0_u32;
            loop {
                // Causal source boundary: the independently signed Add fill,
                // linked fee bill and both owned position lots have NOW been
                // durably proven above. The earlier raw /public/time responses
                // were recorded for admission, before an up-to-16s SQL wait;
                // reusing their old exchange timestamp for Status violates the
                // unchanged production clock-offset gate. At each explicitly
                // requested public Status attempt, record ONE new finite raw
                // external time reply BEFORE production requests it. No clock
                // bytes are minted by the HTTP handler or transformed after
                // delivery; previously delivered observations remain facts.
                let clock_ms=chrono::Utc::now().timestamp_millis();
                let retired=feed.advance_recorded_public_clock(envelope(
                    serde_json::json!([{"ts":clock_ms.to_string()}])
                ));
                assert!(retired <= if status_attempts==0 { 9 } else { 1 },
                    "T09.4 Status source phase may only retire old unused clock replies");
                status_attempts+=1;
                eprintln!(
                    "T09_4_STATUS_CLOCK_SOURCE_PHASE_V1:attempt={} retired={} exchange_ts={} clock_deliveries={}",
                    status_attempts, retired, clock_ms, feed.public_clock_deliveries()
                );
                let answer=tokio::time::timeout_at(deadline.into(),trade.trade(
                    "fixture-a",
                    &okx_protocol::trading::TradeRequest::Status {
                        reference:intent_id.clone()
                    },None
                )).await.unwrap_or_else(|_|panic!(
                    "T09_4_ENTRY_SETTLEMENT_STATUS_DEADLINE:{last}"
                ));
                match answer {
                    Ok(status)=>{
                        assert_eq!(status["data"]["intent"]["state"],"FILLED",
                            "T09_4_SOURCE_BACKED_STATUS_NOT_FILLED:{status}");
                        let actual_posts=feed.outbound_posts.lock().unwrap().len();
                        // Captures the first failed ordering, not a new
                        // business oracle. The same production Status/SQL/
                        // Worker remain authoritative. In particular, an
                        // emergency safety POST is an observable failure of
                        // this *positive* cohort, never reclassified as PASS.
                        let first_cause=if status["data"]["protection_incident"]==true
                            || actual_posts!=5 {
                            // Join the first unexpected transport attempt to
                            // its durable SQL owner before classifying an
                            // extra POST as emergency, duplicate or foreign.
                            let extra_posts=feed.outbound_posts.lock().unwrap().iter()
                                .skip(5).take(3).cloned().collect::<Vec<_>>();
                            let extra_clients=extra_posts.iter()
                                .filter_map(|(_,path,body)|{
                                    (path=="/api/v5/trade/order").then(||body
                                        .get("clOrdId")
                                        .and_then(serde_json::Value::as_str))
                                        .flatten()
                                        .map(str::to_owned)
                                }).collect::<Vec<_>>();
                            let sends=extra_posts.iter().map(|(actor,path,body)|
                                serde_json::json!({
                                    "actor":actor,"route":path,
                                    "client_id":body.get("clOrdId"),
                                    "side":body.get("side"),
                                    "position_side":body.get("posSide"),
                                    "quantity":body.get("sz"),
                                    "reduce_only":body.get("reduceOnly"),
                                    "order_type":body.get("ordType"),
                                    "algo":body.as_array()
                                        .and_then(|v|v.first())
                                        .and_then(|v|v.get("algoId"))
                                })
                            ).collect::<Vec<_>>();
                            let sql=owner.execution.store.call(
                                std::time::Instant::now()+Duration::from_secs(2),
                                move |s|{
                                    let extra_owners=extra_clients.iter().map(|client|{
                                        let linked=s.by_client_order_id(client)?;
                                        Ok(serde_json::json!({
                                            "client_id":client,
                                            "intent_id":linked.as_ref().map(|i|i.intent_id.as_str()),
                                            "operation_key":linked.as_ref()
                                                .map(|i|i.idempotency_key.as_str()),
                                            "risk_increase":linked.as_ref().map(|i|i.risk_increase),
                                            "state":linked.as_ref().map(|i|i.state.as_str()),
                                            "incident":linked.as_ref()
                                                .and_then(|i|i.incident.as_deref()),
                                        }))
                                    }).collect::<okx_execution::StoreResult<Vec<_>>>()?;
                                    let (members,pending)=s.risk_census_intents(100)?;
                                    let observed=members.iter().take(6).map(|member|{
                                        let protection=s.protection(&member.intent_id)?;
                                        let central=s.active_central_receipt(&member.intent_id)?;
                                        Ok(serde_json::json!({
                                            "id":member.intent_id.get(0..12),
                                            "state":member.state.as_str(),
                                            "revision":member.revision,
                                            "pending":pending.binary_search(&member.intent_id).is_ok(),
                                            "held":central.is_some(),
                                            "stop":protection.map(|p|p.algo_id),
                                        }))
                                    }).collect::<okx_execution::StoreResult<Vec<_>>>()?;
                                    Ok(serde_json::json!({
                                        "extra_outbound_sql_owners":extra_owners,
                                        "intents":observed,
                                    }))
                                }
                            ).await;
                            let mut cmd=summary("t09_4_first_cause_readonly");
                            cmd.installation_id="installation_test".into();
                            let central=tokio::time::timeout(
                                Duration::from_secs(2),risk.request(cmd)
                            ).await;
                            let recent=feed.requests.lock().unwrap().iter().rev()
                                .filter(|line|line.contains("orders-algo-pending"))
                                .take(3).cloned().collect::<Vec<_>>();
                            serde_json::json!({
                                "schema":"okx.t09-4-first-cause/v1",
                                "intent":intent_id.get(0..12),
                                "expected_stops":[old_stop,new_stop],
                                "posts":actual_posts,"extra":sends,
                                "source_boundary_ms":source_boundary,
                                "status_incident":status["data"]["intent"]["incident"],
                                "protection_incident":status["data"]["protection_incident"],
                                "financial_error":status["data"]["financial_error"],
                                "incremental":status["data"]["incremental_entry_settlement"]["state"],
                                "release":status["data"]["central_release"]["state"],
                                "sql":format!("{sql:?}").chars().take(850).collect::<String>(),
                                "risk_sequence":central.ok().and_then(Result::ok)
                                    .and_then(|r|r.risk_book_sequence),
                                "recent_algo_get":recent,
                            })
                        }else{serde_json::Value::Null};
                        assert_ne!(status["data"]["protection_incident"],true,
                            "T09_4_FIRST_CAUSE_PROTECTION_V1:{first_cause}");
                        assert_eq!(actual_posts,5,
                            "T09_4_FIRST_CAUSE_OUTBOUND_V1:{first_cause}");
                        if t09_4_incremental_entry_proven(&status) {
                            break status;
                        }
                        last=serde_json::json!({
                            "incremental_entry_settlement":status["data"]["incremental_entry_settlement"],
                            "entry_fill_evidence":status["data"]["entry_fill_evidence"]["state"],
                            "financial_error":status["data"]["financial_error"],
                            "central_release":status["data"]["central_release"],
                            "financial_complete":status["data"]["financial_complete"],
                            "quality":status["quality"],
                        });
                        // Open 0.50 lots can keep terminal RELEASE pending:
                        // only independent incremental SETTLE_ENTRY permits
                        // the central consumed-budget assertion below.
                        let remaining=deadline.saturating_duration_since(
                            std::time::Instant::now());
                        if remaining<=Duration::from_millis(250) {
                            panic!("T09_4_ENTRY_SETTLEMENT_NOT_PROVEN:{last}");
                        }
                        tokio::time::timeout_at(deadline.into(),
                            tokio::time::sleep(Duration::from_millis(250)))
                            .await.unwrap_or_else(|_|panic!(
                                "T09_4_ENTRY_SETTLEMENT_NOT_PROVEN:{last}"
                            ));
                    },
                    Err(reason)=>{
                        let remaining=deadline.saturating_duration_since(
                            std::time::Instant::now());
                        let delay=recorded_preview_local_defer(
                            &reason,&rate_budget,remaining).unwrap_or_else(||panic!(
                                "T09.4 non-local status read failure: {reason}; last={last}"
                            ));
                        tokio::time::timeout_at(deadline.into(),tokio::time::sleep(delay))
                            .await.unwrap_or_else(|_|panic!(
                                "T09_4_ENTRY_SETTLEMENT_LOCAL_RATE_DEADLINE:{last}"
                            ));
                    }
                }
            }
        };
        let mut command=summary("risk_t09_4_after_linked_fill");
        command.installation_id="installation_test".into();
        let worker=risk.request(command).await.unwrap();
        let budget=worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
        assert_eq!(budget.consumed_usd,"6.003",
            "Native settled Add must debit exactly 2.001 additional USD");
        assert_eq!(budget.reserved_usd,"0",
            "Worker must retain no unspent Add reservation after full fill");
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),5,
            "fill and bill capture can NEVER submit another venue POST");
        serde_json::json!({
            "add_fill_at_ms":filled_at,
            "intent_id":intent_id,
            "filled":sql,
            "trade_status":status,
            "worker_budget":worker.budget,
            "risk_book_sequence":worker.risk_book_sequence,
        })
    }

    async fn t09_4_managed_add(context: T09_4Context) -> serde_json::Value {
        let T09_4Context {
            accounts,
            trade,
            risk,
            rate_budget,
            feed,
            prior_client_id,
            campaign_id,
            native_origin,
            first_fill_at_ms,
            terminal_fill_at_ms,
            need_new_market,
            fresh_market_ready,
        } = context;
        use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
        use okx_protocol::trading::{
            OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
        };
        // The sole existing financial owner must commit both source frontiers
        // before an increase. Never infer freshness from the T09.3 status
        // response or add an independent importer.

        // Repeated signed history observations may contain the SAME immutable
        // fill/bill IDs. Their true event times never change; the existing
        // financial owner may capture them again to certify a fresh frontier.
        let raw_history=|v|serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"","data":v
        })).unwrap();
        let mut known_fills=Vec::new();
        let mut known_bills=Vec::new();
        for (idx,size,fee,event_ms) in [
            (1,"0.12","-0.0006",first_fill_at_ms),
            (2,"0.18","-0.0009",terminal_fill_at_ms)
        ] {
            let event=format!("t09-second-{idx:03}");
            known_fills.push(serde_json::json!({
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "tradeId":format!("{event}-trade"),
                "ordId":"recorded-t09-second-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "billId":format!("{event}-bill"),
                "subType":"3","side":"buy","posSide":"long",
                "fillPx":"1000","fillSz":size,"fillPnl":"0",
                "fee":fee,"feeCcy":"USDT",
                "ts":event_ms.to_string(),"fillTime":event_ms.to_string()
            }));
            known_bills.push(serde_json::json!({
                "billId":format!("{event}-bill"),
                "instType":"SWAP","instId":"BTC-USDT-SWAP",
                "ordId":"recorded-t09-second-001",
                "tradeId":format!("{event}-trade"),
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "ccy":"USDT","type":"2","subType":"3",
                "balChg":fee,"fee":fee,"pnl":"0","ts":event_ms.to_string()
            }));
        }
        let mut pages=Vec::new();
        for _ in 0..48 {
            for part in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
                pages.push(RecordedGet{
                    access_key:"fixture-executor-a".into(),
                    path:"/api/v5/trade/fills-history".into(),
                    required_query:vec![("instType".into(),part.into())],
                    raw_response:if part=="SWAP" {
                        raw_history(serde_json::json!(known_fills))
                    } else {raw_history(serde_json::json!([]))}
                });
            }
        }
        feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/fills-history",pages);
        feed.advance_recorded_route("fixture-executor-a","/api/v5/account/bills-archive",
            (0..48).map(|_|RecordedGet{
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/account/bills-archive".into(),
                required_query:vec![],
                raw_response:raw_history(serde_json::json!(known_bills))
            }).collect()
        );
        // The existing Native finance owner must have fresh signed positions
        // WHILE refreshing campaign source cursors, not only after it is ready.
        // It continues to observe the exact 0.30 filled, protected exposure.
        let wrap = |v|serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"","data":v
        })).unwrap();
        let ts=chrono::Utc::now().timestamp_millis().to_string();
        // A managed Add is a NEW authenticated financial frontier after
        // T09.3's terminal settlement. Never fall back to the old generic
        // 64-read baseline or silently reset its read counter. The existing
        // recorded transport publishes a finite, UID-bound raw balance page
        // for this distinct source cycle, just like position/fill/bill pages.
        let source_balance=wrap(serde_json::json!([{
            "totalEq":"100","adjEq":"100","uTime":ts,
            "details":[{
                "ccy":"USDT","eq":"100","liab":"0",
                "availEq":"100","availBal":"100","eqUsd":"100","uTime":ts
            }]
        }]));
        let retired_balance=feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/account/balance",
            (0..48).map(|_|RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/balance".into(),
                required_query:vec![],raw_response:source_balance.clone(),
            }).collect()
        );
        eprintln!(
            "T09_4_BALANCE_FRONTIER_V1 retired={retired_balance} new_pages=48"
        );
        let source_position=wrap(serde_json::json!([{
            "instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"long",
            "pos":"0.3","mgnMode":"cross","avgPx":"1000","markPx":"1000","uTime":ts
        }]));
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/account/positions",
            (0..64).map(|_|RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/positions".into(),
                required_query:vec![],raw_response:source_position.clone(),
            }).collect()
        );
        // Finance maintenance still observes the previously FILLED opening
        // until its complete source frontier is committed. Serve that exact
        // old venue order during readiness; only after the frontier is proven
        // may the recorded transport switch to the distinct Add order ID.
        let previous_order = wrap(serde_json::json!([{
            "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-second-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy",
            "posSide":"long","tdMode":"cross","ordType":"limit",
            "px":"1000","sz":"0.3","accFillSz":"0.3","avgPx":"1000",
            "state":"filled","cTime":terminal_fill_at_ms.to_string(),
            "uTime":terminal_fill_at_ms.to_string()
        }]));
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order",
            (0..48).map(|_| RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:previous_order.clone(),
            }).collect()
        );
        let mut last_readiness=String::from("NOT_OBSERVED");
        tokio::time::timeout(Duration::from_secs(16),async {
            loop {
                let through=chrono::Utc::now().timestamp_millis()-2_000;
                let mut ready=true;
                for alias in ["fixture-a","fixture-b"] {
                    let owner=accounts.resolve(alias).await.unwrap();
                    let campaign=campaign_id.clone();
                    match owner.execution.store.call(
                        std::time::Instant::now()+Duration::from_secs(3),
                        move |s|s.verify_campaign_entry_readiness(&campaign,native_origin,through)
                    ).await {
                        Ok(())=>{},
                        Err(reason)=>{
                            last_readiness=format!("{alias}:{reason}");
                            ready=false;break;
                        }
                    }
                }
                if ready {break}
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.unwrap_or_else(|_|panic!(
            "T09_4_SOURCE_FRONTIER_INCOMPLETE:{last_readiness}; strict_source_denials={:?}",
            feed.denied_routes.lock().unwrap().iter().rev().take(5).collect::<Vec<_>>()
        ));

        // The producer must publish NEW signed public quotes only AFTER
        // the slower financial frontier is proven, before a single Execute.
        // Avoid aging already signed public market and fee evidence while
        // awaiting unrelated Worker Summary. This is read-only risk evidence.
        let mut before=summary("risk_t09_4_before_managed_add");
        before.installation_id="installation_test".into();
        let observed=risk.request(before).await.unwrap();
        let start_budget=observed.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
        assert_eq!(start_budget.consumed_usd,"4.002");
        assert_eq!(start_budget.reserved_usd,"0");
        need_new_market.send(()).expect("T09.4 market request channel");
        fresh_market_ready.await.expect("T09.4 recorded market refresh missing");
        let signed_now=chrono::Utc::now().timestamp_millis().to_string();
        // 16 independently queried derivative conditional scopes, all
        // returned by signed Observer GET. Only the one previously owned,
        // live stop is present; an unknown algo would block Add.
        let old_stop=format!("s{}",&prior_client_id[..31]);
        let mut pages=Vec::new();
        for _ in 0..7 {
            for inst in ["SWAP","FUTURES"] {
                for kind in [
                    "conditional","oco","trigger","move_order_stop","chase",
                    "iceberg","twap","smart_iceberg"
                ] {
                    let data=if inst=="SWAP" && kind=="conditional" {
                        serde_json::json!([{
                            "instType":"SWAP","instId":"BTC-USDT-SWAP","ordType":"conditional",
                            "algoId":"900000002","algoClOrdId":old_stop,
                            "state":"live","side":"sell","posSide":"long","sz":"0.3",
                            "tdMode":"cross","slTriggerPx":"900",
                            "slTriggerPxType":"mark","slOrdPx":"-1"
                        }])
                    } else {serde_json::json!([])};
                    pages.push(RecordedGet {
                        access_key:"fixture-observer-a".into(),
                        path:"/api/v5/trade/orders-algo-pending".into(),
                        required_query:vec![
                            ("instType".into(),inst.into()),
                            ("ordType".into(),kind.into()),
                            ("limit".into(),"100".into())
                        ],
                        raw_response:wrap(data),
                    });
                }
            }
        }
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/trade/orders-algo-pending",pages
        );
        // The actual exchange fee generation and leverage are reread for
        // this new order; the T09.2 admission proof cannot be reused.
        let mut refreshed_fee_pages=Vec::new();
        for _ in 0..8 {
            refreshed_fee_pages.push(RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/trade-fee".into(),
                required_query:vec![
                    ("instType".into(),"SWAP".into()),
                    ("instFamily".into(),"BTC-USDT".into()),
                ],
                raw_response:wrap(serde_json::json!([{
                    "instType":"SWAP","level":"Lv1","ts":signed_now,
                    "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
                }])),
            });
            feed.expect_get(RecordedGet {
                access_key:"fixture-observer-a".into(),
                path:"/api/v5/account/leverage-info".into(),
                required_query:vec![
                    ("instId".into(),"BTC-USDT-SWAP".into()),
                    ("mgnMode".into(),"cross".into()),
                ],
                raw_response:wrap(serde_json::json!([
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                    {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
                ])),
            });
        }
        // A fresh exchange fee generation supersedes unused older signed
        // pages from T09.2. The production GET must never consume stale
        // leftover fixture bytes ahead of this new admission.
        feed.advance_recorded_route(
            "fixture-observer-a","/api/v5/account/trade-fee",refreshed_fee_pages
        );
        feed.advance_recorded_public_clock(wrap(serde_json::json!([{"ts":signed_now}])));
        // The same production Add admission reads exchange time before POST
        // and again after the central reserve. Provide finite recorded venue
        // replies, never a simulated wall-clock or live HTTP fallback.
        for _ in 0..8 {
            feed.expect_public_clock(wrap(serde_json::json!([{"ts":signed_now}])));
        }
        feed.expect_post(RecordedPost {
            access_key:"fixture-executor-a".into(),
            path:"/api/v5/trade/order".into(),
            expected_body:serde_json::json!({
                "instId":"BTC-USDT-SWAP","tdMode":"cross",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","ordType":"limit",
                "sz":"0.2","px":"1000",
                "attachAlgoOrds":[{
                    "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                    "tpTriggerPxType":"mark","slTriggerPx":"900",
                    "slTriggerPxType":"mark","slOrdPx":"-1"
                }]
            }),
            raw_response:br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t09-add-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
        });
        feed.advance_recorded_route(
            "fixture-executor-a","/api/v5/trade/order",
            (0..4).map(|_|RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:wrap(serde_json::json!([{
                    "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-add-001",
                    "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy",
                    "posSide":"long","tdMode":"cross","ordType":"limit",
                    "px":"1000","sz":"0.2","accFillSz":"0","avgPx":"",
                    "state":"live","cTime":signed_now,"uTime":signed_now
                }])),
            }).collect(),
        );
        let request=TradeRequest::Execute {
            idempotency_key:"t09_real_public_add_0001".into(),
            operation:TradeOperation::Add,
            strategy_instance:"manual".into(),instrument:"BTC-USDT-SWAP".into(),
            side:PositionSide::Long,sizing:Sizing::Contracts{contracts:"0.20".into()},
            order:OrderConstraints{
                order_type:"limit".into(),limit_price:Some("1000".into()),
                reference:None,max_entry_usd:Some("10".into()),
                max_slippage_bps:0,
            },
            protection:Some(Protection::Price{stop_price:"900".into()}),
            expires_in_ms:30_000,
        };

        assert!(request.valid());
        let response=match trade.trade("fixture-a",&request,None).await {
            Ok(response)=>response,
            Err(reason)=>{
                // Preserve the first Add denial with the durable intent
                // incidents and existing outbound attempts. A later Status
                // sample may clear the transient protection flag.
                let owner=accounts.resolve("fixture-a").await.unwrap();
                let sql=owner.execution.store.call(
                    std::time::Instant::now()+Duration::from_secs(2),
                    |s|{
                        let (members,_pending)=s.risk_census_intents(100)?;
                        let rows=members.into_iter().take(6).map(|i|
                            serde_json::json!({
                                "id":i.intent_id,
                                "state":i.state.as_str(),
                                "revision":i.revision,
                                "incident":i.incident,
                            })
                        ).collect::<Vec<_>>();
                        Ok(rows)
                    }
                ).await;
                // Read only the SAME SQL owner's campaign readiness
                // breakdown on definitive local no-send rejection. Distinguish
                // age, source-generation and economic-epoch conflicts instead
                // of inferring a cause from a later successful Status.
                #[cfg(debug_assertions)]
                let readiness={
                    let campaign=campaign_id.clone();
                    let through=chrono::Utc::now().timestamp_millis()-2_000;
                    owner.execution.store.call(
                        std::time::Instant::now()+Duration::from_secs(2),
                        move |s|s.campaign_readiness_diagnostic(&campaign,native_origin,through),
                    ).await.unwrap_or_else(
                        |error|format!("SQL_DIAGNOSTIC_UNAVAILABLE:{error}"),
                    )
                };
                #[cfg(not(debug_assertions))]
                let readiness=String::from("SQL_DIAGNOSTIC_DISABLED");
                let capture=owner.source_capture_health.read().await;
                let last_success_ms=capture.last_success_ms;
                let source_failure=capture.reason_code.clone();
                drop(capture);
                let posts=feed.outbound_posts.lock().unwrap().iter().rev()
                    .take(3).map(|(actor,path,body)|serde_json::json!({
                        "actor":actor,
                        "route":path,
                        "client_id":body.get("clOrdId"),
                        "side":body.get("side"),
                        "quantity":body.get("sz"),
                        "reduce_only":body.get("reduceOnly"),
                    })).collect::<Vec<_>>();
                panic!("T09_4_MANAGED_ADD_DENIED:{reason};first_cause={}",
                    serde_json::json!({
                        "sql":format!("{sql:?}").chars().take(720).collect::<String>(),
                        "posts":posts,
                        "last_source_ms":last_success_ms,
                        "source_failure":source_failure,
                        "campaign_readiness":readiness,
                    })
                );
            }
        };
        let id=response["data"]["intent"]["intent_id"].as_str()
            .unwrap_or_else(||panic!("T09_4_MANAGED_ADD_MISSING_INTENT_V1:{response}"))
            .to_owned();
        let owner=accounts.resolve("fixture-a").await.unwrap();
        let id_for_sql=id.clone();
        let (intent,reserve,post)=owner.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            move |s|Ok((
                s.intent(&id_for_sql)?.ok_or(okx_execution::StoreError::Invalid("T09_4_SQL_ADD_MISSING"))?,
                s.central_receipt(&id_for_sql)?.ok_or(okx_execution::StoreError::Invalid("T09_4_WORKER_RESERVE_MISSING"))?,
                s.post_fence_evidence(&id_for_sql)?,
            ))
        ).await.unwrap();
        assert_eq!(intent.exchange_order_id.as_deref(),Some("recorded-t09-add-001"));
        assert_eq!(intent.plan.quantity,Decimal::new(2,1));
        assert!(post.0>0);
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),5);
        let mut after=summary("risk_t09_4_add_ack_unsettled");
        after.installation_id="installation_test".into();
        let worker=risk.request(after).await.unwrap();
        println!("T09_4_NATIVE_ADD_ACK_V1:{}",serde_json::json!({
            "intent_id":id,"account_uid":owner.configuration.account_key.verified_uid,
            "sql_begin_post_at_ms":post.0,"central_receipt_hash":reserve.receipt_hash,
            "venue_order_id":intent.exchange_order_id,
            "worker_budget":worker.budget,"financial_complete":response["data"]["financial_complete"],
            "post_count":feed.outbound_posts.lock().unwrap().len()
        }));
        let settlement=t09_4_fill_phase(T09_4FillContext {
            accounts:accounts.clone(),trade:trade.clone(),risk:risk.clone(),
            rate_budget,feed:feed.clone(),intent_id:id.clone(),
            client_id:intent.client_order_id.clone(),
            old_client_id:prior_client_id.clone(),
            first_fill_at_ms,terminal_fill_at_ms,
        }).await;
        println!("T09_4_NATIVE_ADD_SETTLEMENT_V1:{settlement}");
        serde_json::json!({
            "intent_id":id,"client_order_id":intent.client_order_id,
            "central_receipt_hash":reserve.receipt_hash,
            "sql_begin_post_at_ms":post.0,
            "venue_order_id":"recorded-t09-add-001",
            "account_uid":owner.configuration.account_key.verified_uid,
            "settlement":settlement
        })
    }
