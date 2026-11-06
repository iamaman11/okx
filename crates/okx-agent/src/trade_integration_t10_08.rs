// T10-08 negative leg: the EXACT T09.3 SQL intent is already partially filled
// and has one signed USDT bill. A distinct signed venue stop response then
// reports FAILED. The native protective owner sends one bounded Cancel,
// receives a negative venue response classified as UNKNOWN, and returns a blocker without
// resending a trading mutation or releasing the Worker reservation.
// Another signed fill+bill phase must occur AFTER this event.
async fn t10_08_failed_stop_before_late_fill(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent_id: String,
    client_id: String,
    partial: serde_json::Value,
) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
    use okx_protocol::trading::TradeRequest;
    let raw = |value| serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":value
    })).expect("signed T10-08 fixture envelope");
    let at_ms=chrono::Utc::now().timestamp_millis();
    let at=at_ms.to_string();
    let first_posts=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(first_posts,4,"T10-08 stop failure follows four original creates/closes");
    let owner=accounts.resolve("fixture-a").await.expect("T10-08 original A SQL owner");
    // T09.3 already read this exact Native SQL cursor and verified both
    // fill and linked bill before this source event. Re-querying the
    // mutable cursor during another capture is not a new independent proof:
    // it can move between two SQL statements and lose coverage. Bind that
    // earlier verified phase to a fresh durable identity/reservation read.
    assert_eq!(partial["phase"],"partial");
    let partial_sql=&partial["sql"];
    assert_eq!(partial_sql["quantity"],"0.12");
    assert_eq!(partial_sql["fill_count"],1);
    assert_eq!(partial_sql["bill_count"],1);
    assert_eq!(partial_sql["fee_usdt"],"0.0006");
    assert_eq!(partial_sql["source_digest"].as_str().map(str::len),Some(64));
    let sql_id=intent_id.clone();
    let durable=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),move |sql| {
            let persisted=sql.intent(&sql_id)?.ok_or(okx_execution::StoreError::Invalid(
                "T10_08_ORIGINAL_SQL_INTENT_MISSING"))?;
            Ok(serde_json::json!({
                "intent_id":sql_id,
                "state":persisted.state.as_str(),
                "revision":persisted.revision,
                "central_held":sql.active_central_receipt(&persisted.intent_id)?.is_some(),
            }))
        }
    ).await.expect("T10-08 durable SQL intent/reservation readback");
    assert_eq!(durable["intent_id"],intent_id);
    assert_eq!(durable["state"],"PARTIALLY_FILLED");
    assert_eq!(durable["central_held"],true);
    let before_sql=serde_json::json!({
        "intent_id":intent_id,
        "state":durable["state"],
        "fill_quantity":partial_sql["quantity"],
        "fill_count":partial_sql["fill_count"],
        "bill_count":partial_sql["bill_count"],
        "fee_usdt":partial_sql["fee_usdt"],
        "fill_source_digest":partial_sql["source_digest"],
        "revision":durable["revision"],
        "central_held":durable["central_held"],
        "sql_cursor_phase":"t09_3_phase/partial",
    });

    // These are new time-indexed externally signed observations; the
    // recorded transport still strictly denies an unexpected GET or POST.
    let stop_client=format!("s{}",&client_id[..31]);
    let stopped=raw(serde_json::json!([{
        "instId":"BTC-USDT-SWAP","algoId":"900000002",
        "algoClOrdId":stop_client,"state":"canceled","sz":"0.12",
        "side":"sell","posSide":"long","tdMode":"cross",
        "ordType":"conditional","slTriggerPx":"900",
        "slTriggerPxType":"mark","slOrdPx":"-1",
        "failCode":"51008"
    }]));
    let retired_stop=feed.advance_recorded_route(
        "fixture-executor-a","/api/v5/trade/order-algo",
        (0..12).map(|_|RecordedGet {
            access_key:"fixture-executor-a".into(),
            path:"/api/v5/trade/order-algo".into(),
            required_query:vec![("algoClOrdId".into(),stop_client.clone())],
            raw_response:stopped.clone(),
        }).collect()
    );
    let entry=raw(serde_json::json!([{
        "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-second-001",
        "clOrdId":"__REQUEST_CL_ORD_ID__",
        "side":"buy","posSide":"long","tdMode":"cross",
        "ordType":"limit","px":"1000","sz":"0.3","accFillSz":"0.12",
        "avgPx":"1000","state":"partially_filled","cTime":at,"uTime":at
    }]));
    feed.advance_recorded_route(
        "fixture-executor-a","/api/v5/trade/order",
        (0..14).map(|_|RecordedGet {
            access_key:"fixture-executor-a".into(),
            path:"/api/v5/trade/order".into(),
            required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
            raw_response:entry.clone(),
        }).collect()
    );
    let positions=raw(serde_json::json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"long",
        "pos":"0.12","mgnMode":"cross","avgPx":"1000",
        "markPx":"1000","uTime":at
    }]));
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/positions",
        (0..24).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],raw_response:positions.clone(),
        }).collect()
    );
    let balance=raw(serde_json::json!([{
        "totalEq":"100","adjEq":"100","uTime":at,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":at}]
    }]));
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/balance",
        (0..24).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],raw_response:balance.clone(),
        }).collect()
    );
    // A signed negative sCode after a real POST is not treated as a safe
    // un-sent result by production. The possible send is SQL-claimed,
    // and the outcome is UNKNOWN/no-resubmit; no second request follows.
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/cancel-order".into(),
        expected_body:serde_json::json!({
            "instId":"BTC-USDT-SWAP","clOrdId":client_id
        }),
        raw_response:raw(serde_json::json!([{
            "ordId":"recorded-t09-second-001",
            "clOrdId":client_id,"sCode":"51008",
            "sMsg":"Order is no longer cancelable"
        }])),
    });
    let clock=raw(serde_json::json!([{"ts":at}]));
    feed.advance_recorded_public_clock(clock);
    let started=std::time::Instant::now();
    let observed=tokio::time::timeout(
        Duration::from_secs(8),trade.trade("fixture-a",&TradeRequest::Status {
            reference:intent_id.clone()
        },None)
    ).await.expect("T10-08 stop failure Status deadline")
      .expect("T10-08 must return a public status blocker");
    let posts=feed.outbound_posts.lock().unwrap();
    let delta=posts.len()-first_posts;
    let new_posts=posts.iter().skip(first_posts)
        .map(|(actor,path,body)|serde_json::json!({
            "actor":actor,"path":path,
            "client_order_id":body.get("clOrdId")
        })).collect::<Vec<_>>();
    drop(posts);
    assert_eq!(delta,1,
        "T10-08 one exact cancel attempt, not a second create: {observed}; posts={new_posts:?}");
    assert_eq!(new_posts[0]["actor"],"fixture-executor-a");
    assert_eq!(new_posts[0]["path"],"/api/v5/trade/cancel-order");
    assert_eq!(new_posts[0]["client_order_id"],client_id);
    assert_eq!(feed.unconsumed_posts(),0,
        "T10-08 must consume actual explicit cancel rejection, no replay");
    assert_eq!(observed["status"],"partial",
        "failed protection and rejected emergency Cancel cannot be Active");
    let protective=&observed["data"]["compensation"];
    assert_eq!(protective["state"],"blocked");
    assert_eq!(protective["retry_create"],false);
    assert_eq!(protective["phase"],"before_financial_capture");
    assert_eq!(protective["reason_code"],"SAFETY_CANCEL_OUTCOME_UNKNOWN_NO_RESUBMIT");
    let elapsed=protective["elapsed_ms"].as_u64().expect("T10-08 bounded timestamp");
    assert!(elapsed<=2000,"T10-08 emergency blocker exceeded SLO: {elapsed} ms");
    assert!(started.elapsed()>=Duration::from_millis(0));

    let id=intent_id.clone();
    let after_sql=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),move |sql|{
            let intent=sql.intent(&id)?.ok_or(okx_execution::StoreError::Invalid(
                "T10_08_INCIDENT_SQL_INTENT_MISSING"))?;
            let cancel=sql.operation(&format!("safety_cancel_{}",intent.client_order_id))?;
            let held=sql.active_central_receipt(&id)?.is_some();
            let stopped=sql.stops()?;
            Ok(serde_json::json!({
                "intent_id":id,"sql_state":intent.state.as_str(),
                "revision":intent.revision,"incident":intent.incident,
                "cancel_claim":cancel,
                "central_held":held,
                "risk_stop_latched":stopped.iter().any(|(id,_,_,_)|id==&format!("protection:{}",intent.intent_id)),
            }))
        }
    ).await.expect("T10-08 durable SQL incident/claim readback");
    assert_eq!(after_sql["intent_id"],intent_id);
    assert_eq!(after_sql["central_held"],true);
    assert_eq!(after_sql["risk_stop_latched"],true);
    assert_eq!(after_sql["cancel_claim"]["kind"],"safety_cancel");
    let mut cmd=summary("risk_t10_08_after_failed_stop");
    cmd.installation_id="installation_test".into();
    let worker=risk.request(cmd).await.expect("T10-08 persisted Worker risk budget");
    let budget=worker.budget.as_ref().expect("T10-08 Worker budget");
    assert_ne!(budget.entry_budget.as_ref().unwrap().reserved_usd,"0",
        "a rejected protective cancel cannot release still-open risk");
    serde_json::json!({
        "schema":"okx.t10-08-native-protection-failure/v1",
        "intent_id":intent_id,
        "account_uid":owner.configuration.account_key.verified_uid,
        "first_partial_sql":before_sql,
        "signed_stop_failure":{
            "algo_id":"900000002","state":"canceled","fail_code":"51008",
            "old_signed_pages_retired":retired_stop
        },
        "protected_action":protective,
        "elapsed_to_status_ms":started.elapsed().as_millis() as u64,
        "sql_after":after_sql,
        "worker_budget":budget,
        "worker_risk_book_sequence":worker.risk_book_sequence,
        "outbound_posts_before":first_posts,
        "outbound_posts_after":first_posts+delta,
        "outbound":new_posts,
        "late_fill_after_this_checkpoint":true,
    })
}


// The next event is not permission to guess venue safety. The original entry
// already has independently captured 0.30 contracts and 0.0015 USDT in linked
// bills. Reuse the existing public Close owner, then publish a NEW immutable
// venue fill/bill/flat frontier only after its authenticated POST acknowledgement.
fn t10_08_publish_signed_flat_after_close(
    feed: &crate::accounts::recorded_support::RecordedAccountHttp,
    first_at: i64,
    second_at: i64,
    close_at: i64,
) {
    use crate::accounts::recorded_support::RecordedGet;
    assert!(close_at >= second_at && second_at >= first_at,
        "T10-08 signed Close fill must follow the late opening fill");
    let raw = |v| serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":v
    })).expect("T10-08 signed external source");
    let mut fills=Vec::new();
    let mut bills=Vec::new();
    for (trade_id,bill_id,ord_id,side,sub_type,quantity,fee,at) in [
        ("t09-second-001-trade","t09-second-001-bill",
            "recorded-t09-second-001","buy","3","0.12","-0.0006",first_at),
        ("t09-second-002-trade","t09-second-002-bill",
            "recorded-t09-second-001","buy","3","0.18","-0.0009",second_at),
        ("t10-08-close-001-trade","t10-08-close-001-bill",
            "recorded-t10-08-close-030","sell","5","0.3","-0.0015",close_at),
    ] {
        fills.push(serde_json::json!({
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "tradeId":trade_id,"ordId":ord_id,"clOrdId":"__REQUEST_CL_ORD_ID__",
            "billId":bill_id,"subType":sub_type,"side":side,"posSide":"long",
            "fillPx":"1000","fillSz":quantity,"fillPnl":"0",
            "fee":fee,"feeCcy":"USDT",
            "ts":at.to_string(),"fillTime":at.to_string()
        }));
        bills.push(serde_json::json!({
            "billId":bill_id,"instType":"SWAP","instId":"BTC-USDT-SWAP",
            "ordId":ord_id,"tradeId":trade_id,
            "clOrdId":"__REQUEST_CL_ORD_ID__","ccy":"USDT",
            "type":"2","subType":sub_type,
            "balChg":fee,"fee":fee,"pnl":"0","ts":at.to_string()
        }));
    }
    let mut history=Vec::new();
    for _ in 0..32 {
        for part in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
            history.push(RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/fills-history".into(),
                required_query:vec![("instType".into(),part.into())],
                raw_response:if part=="SWAP" {raw(serde_json::json!(fills))}
                    else {raw(serde_json::json!([]))}
            });
        }
    }
    feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/fills-history",history);
    feed.advance_recorded_route("fixture-executor-a","/api/v5/account/bills-archive",
        (0..32).map(|_| RecordedGet {
            access_key:"fixture-executor-a".into(),
            path:"/api/v5/account/bills-archive".into(),
            required_query:vec![],
            raw_response:raw(serde_json::json!(bills)),
        }).collect()
    );
    // Keep immutable earlier T09.1 fills in their previously ingested SQL
    // history: re-publishing them with new timestamps would be a forged
    // correction. The three NEW/unchanged T09.3/T10-08 rows are separately
    // signed; total campaign fee of all five bills is 0.004 USDT.
    let observed=chrono::Utc::now().timestamp_millis().max(close_at);
    let flat_balance=raw(serde_json::json!([{
        "totalEq":"99.996","adjEq":"99.996","uTime":observed.to_string(),
        "details":[{
            "ccy":"USDT","eq":"99.996","liab":"0","availEq":"99.996",
            "availBal":"99.996","eqUsd":"99.996","uTime":observed.to_string()
        }]
    }]));
    feed.advance_recorded_route("fixture-observer-a","/api/v5/account/balance",
        (0..48).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],
            raw_response:flat_balance.clone(),
        }).collect()
    );
    feed.advance_recorded_route("fixture-observer-a","/api/v5/account/positions",
        (0..80).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],
            raw_response:raw(serde_json::json!([])),
        }).collect()
    );
    feed.advance_recorded_route("fixture-observer-a","/api/v5/trade/orders-pending",
        (0..48).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/trade/orders-pending".into(),
            required_query:vec![("limit".into(),"100".into())],
            raw_response:raw(serde_json::json!([])),
        }).collect()
    );
    let mut orders=Vec::new();
    for _ in 0..32 {
        for (ord_id,qty,side,order_type,price,at) in [
            ("recorded-t09-second-001","0.3","buy","limit","1000",second_at),
            ("recorded-t10-08-close-030","0.3","sell","ioc","999",close_at),
        ] {
            orders.push(RecordedGet {
                access_key:"fixture-executor-a".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:raw(serde_json::json!([{
                    "instId":"BTC-USDT-SWAP","ordId":ord_id,
                    "clOrdId":"__REQUEST_CL_ORD_ID__","side":side,"posSide":"long",
                    "tdMode":"cross","ordType":order_type,"px":price,
                    "sz":qty,"accFillSz":qty,"avgPx":"1000",
                    "state":"filled","cTime":at.to_string(),"uTime":at.to_string()
                }])),
            });
        }
    }
    feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/order",orders);
    // The failed 900000002 stop stays CANCELED. An empty pending-algo census
    // after the signed flat source is valid; a "live" reuse of that old algoId
    // is not a recovery event.
    let mut algo_pages=Vec::new();
    for _ in 0..16 {
        for inst_type in ["SWAP","FUTURES"] {
            for kind in [
                "conditional","oco","trigger","move_order_stop",
                "chase","iceberg","twap","smart_iceberg",
            ] {
                algo_pages.push(RecordedGet {
                    access_key:"fixture-observer-a".into(),
                    path:"/api/v5/trade/orders-algo-pending".into(),
                    required_query:vec![
                        ("instType".into(),inst_type.into()),
                        ("ordType".into(),kind.into()),
                        ("limit".into(),"100".into()),
                    ],
                    raw_response:raw(serde_json::json!([])),
                });
            }
        }
    }
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/trade/orders-algo-pending",algo_pages
    );
}

async fn t10_08_recover_signed_flat_after_late_fill(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    rate_budget: RateBudget,
    original_intent: String,
    client_id: String,
    first_at: i64,
    original_unrelated_worker_reserve: &str,
    terminal: &serde_json::Value,
) -> serde_json::Value {
    let sql=&terminal["sql"];
    assert_eq!(sql["quantity"],"0.3");
    assert_eq!(sql["fill_count"],2);
    assert_eq!(sql["bill_count"],2);
    assert_eq!(terminal["public_status_deferred_until_signed_close_sources"],true);
    let prior_posts=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(prior_posts,5,"T10-08 original Cancel is the only pre-recovery mutation");
    let mut risk_before=summary("risk_t10_08_before_safe_close");
    risk_before.installation_id="installation_test".into();
    let held=risk.request(risk_before).await.expect("Worker before Close");
    // T09.0 has an independently held Worker-only reservation before
    // any T09.1/T09.2 trading intent. It remains 4 USD throughout this
    // T10-08 scenario; never infer that an account-wide 4 is this intent.
    // T10-08 must prove its OWN reservation RELEASED with INSPECT below.
    assert_eq!(original_unrelated_worker_reserve,"4",
        "baseline non-T10-08 Worker grant must be independently observed");
    // Native immutable fills can become durable before the Worker has
    // applied their last SETTLE_ENTRY revision. The post-late-fill Worker
    // grant therefore may still include part of the 3.0015 USD second
    // entry, in addition to the independent 4 USD baseline. This is
    // bounded unsettled exposure, NEVER an extra spend or a reason to
    // release the unrelated 4 USD grant. The terminal checks below
    // require exact RELEASED original grant and retained baseline.
    let held_entry=&held.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    let reserved=Decimal::from_str_exact(&held_entry.reserved_usd)
        .expect("T10-08 Worker decimal reserve");
    let baseline=Decimal::from_str_exact(original_unrelated_worker_reserve)
        .expect("T10-08 independent baseline decimal");
    assert!(
        reserved>=baseline && reserved<=Decimal::new(70015,4),
        "T10-08 post-late-fill Worker grant must be between original independent baseline and pre-fill reserved amount: reserved={reserved} baseline={baseline}"
    );
    // Reuse T09.6's existing public Close + exact signed POST/ACK fixture.
    // The operation key is precisely the one used by emergency_protection,
    // so a later Status cannot create a second Close after the first ACK.
    let close=t09_6_close_owned_lot(T09_6CloseContext {
        accounts:accounts.clone(),
        trade:trade.clone(),
        rate_budget:rate_budget.clone(),
        feed:feed.clone(),
        opening_intent_id:original_intent.clone(),
        operation_key:format!("safety_close_{client_id}"),
        quantity:"0.3",
        venue_order_id:"recorded-t10-08-close-030",
        expected_post_count:6,
    }).await;
    assert_eq!(close["quantity"],"0.3");
    assert_eq!(close["venue_order_id"],"recorded-t10-08-close-030");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),6);
    let first_at=first_at.min(terminal["raw_history_event_at_ms"]
        .as_i64().expect("T10-08 signed late event time"));
    let second_at=terminal["raw_history_event_at_ms"]
        .as_i64().expect("T10-08 signed late event time");
    let close_at=close["close_at_ms"].as_str()
        .expect("T10-08 authenticated Close ACK timestamp")
        .parse::<i64>().expect("T10-08 Close time");
    t10_08_publish_signed_flat_after_close(&feed,first_at,second_at,close_at);

    // Independently captured close fill/bill must terminate the local Close
    // before the original central entry can release its reserve. Status is
    // read-only except the original's idempotent risk resolution; POST count
    // must never advance after the exact one source-backed Close send.
    let close_id=close["close_intent_id"].as_str()
        .expect("T10-08 durable public Close owner").to_owned();
    let close_status=t09_6_status_after_local_read_defer(
        &trade,&rate_budget,&feed,&close_id,0,"T10_08_CLOSE_TERMINAL"
    ).await;
    let until=std::time::Instant::now()+Duration::from_secs(20);
    let mut last=serde_json::Value::Null;
    let mut attempts=0_u32;
    while std::time::Instant::now()<until && attempts<8 {
        attempts+=1;
        let status=t09_6_status_after_local_read_defer(
            &trade,&rate_budget,&feed,&original_intent,0,"T10_08_ORIGINAL_FLAT"
        ).await;
        let complete=status["data"]["central_release"]["state"]=="verified"
            && status["data"]["protection_incident"]==false
            && status["data"]["protection_remediation"]["state"]=="verified_flat_no_orphans";
        last=status;
        if complete {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(last["data"]["central_release"]["state"],"verified",
        "T10-08 same-intent central Worker release not proven: {last}");
    assert_eq!(last["data"]["protection_remediation"]["state"],"verified_flat_no_orphans",
        "T10-08 stale live stop cannot clear incident: {last}");
    assert_eq!(last["data"]["protection_incident"],false);
    assert_eq!(close_status["data"]["intent"]["state"],"FILLED",
        "T10-08 needs independent signed terminal Close fill/bill");
    let owner=accounts.resolve("fixture-a").await.expect("T10-08 same Native SQL owner");
    let intent=original_intent.clone();
    let close_sql_id=close_id.clone();
    let post_sql=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| {
            let opened=s.intent(&intent)?.ok_or(okx_execution::StoreError::Invalid(
                "T10_08_ORIGINAL_SQL_AFTER_CLOSE_MISSING"
            ))?;
            let reduced=s.intent(&close_sql_id)?.ok_or(okx_execution::StoreError::Invalid(
                "T10_08_CLOSE_SQL_AFTER_CLOSE_MISSING"
            ))?;
            let lots=s.active_lots(&opened.instrument_id)?;
            let stop=s.stops()?.iter().any(|(id,_,_,_)|id==&format!("protection:{intent}"));
            let resolution=s.operation(&format!("protection-recovered:{intent}"))?;
            Ok(serde_json::json!({
                "original_intent_id":intent,"original_state":opened.state.as_str(),
                "original_revision":opened.revision,
                "original_incident":opened.incident,
                "close_intent_id":close_sql_id,
                "close_state":reduced.state.as_str(),
                "close_quantity":reduced.plan.quantity.to_string(),
                "close_risk_increase":reduced.risk_increase,
                "remaining_instrument_lots":lots.len(),
                "risk_stop_latched":stop,
                "central_still_held":s.active_central_receipt(&intent)?.is_some(),
                "original_central_reservation":s.central_receipt(&intent)?.map(|r|
                    serde_json::json!({
                        "reservation_key":r.reservation_key,
                        "plan_hash":r.plan_hash,
                        "account_key":r.account_key,
                    })),
                "recovery_receipt_kind":resolution.map(|r|r.kind),
            }))
        }
    ).await.expect("T10-08 post-close original SQL and central release readback");
    assert_eq!(post_sql["original_incident"],serde_json::Value::Null);
    assert_eq!(post_sql["risk_stop_latched"],false);
    assert_eq!(post_sql["central_still_held"],false);
    assert_eq!(post_sql["remaining_instrument_lots"],0);
    assert_eq!(post_sql["close_risk_increase"],false);
    assert_eq!(post_sql["close_quantity"],"0.3");
    assert_eq!(post_sql["recovery_receipt_kind"],"PROTECTION_INCIDENT_FLAT_RESOLVED");
    let release=&post_sql["original_central_reservation"];
    assert_eq!(release["account_key"],owner.configuration.account_key.wire());
    let reservation_key=release["reservation_key"].as_str()
        .expect("T10-08 original durable central reservation key").to_owned();
    let plan_hash=release["plan_hash"].as_str()
        .expect("T10-08 original durable plan hash").to_owned();
    // The Worker may retain other accounts' grants even after the affected
    // entry has closed. INSPECT the EXACT original grant instead of
    // inventing a global budget=0 acceptance requirement.
    let inspected=risk.request(RiskCommand {
        schema:RISK_COMMAND_SCHEMA_V1.into(),
        command_id:"risk_t10_08_inspect_original_released".into(),
        installation_id:"installation_test".into(),
        action:RiskCommandAction::Inspect {
            reservation_key:reservation_key.clone(),
            plan_sha256:plan_hash,
        },
    }).await.expect("T10-08 original Worker grant INSPECT");
    assert_eq!(inspected.status,RiskDecisionStatus::Observed);
    assert_eq!(inspected.reservation_key.as_deref(),Some(reservation_key.as_str()));
    assert_eq!(
        inspected.reservation_state,
        Some(okx_protocol::risk_authority::RiskReservationState::Released),
        "T10-08 must release its OWN grant, never an unrelated baseline grant"
    );
    let mut command=summary("risk_t10_08_after_flat");
    command.installation_id="installation_test".into();
    let worker=risk.request(command).await.expect("persisted Worker after T10-08 recovery");
    let final_budget=&worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(final_budget.reserved_usd,original_unrelated_worker_reserve,
        "baseline Worker grant must survive this intent's independently verified release");
    assert_eq!(final_budget.consumed_usd,"4.002");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),6,
        "T10-08 cannot resend Cancel/Create/Close after flat reconciliation");
    serde_json::json!({
        "schema":"okx.t10-08-native-flat-recovery/v1",
        "original_intent_id":original_intent,
        "close":close,
        "signed_close_event_at_ms":close_at,
        "signed_late_event_at_ms":second_at,
        "close_terminal_status":close_status,
        "original_terminal_status":last,
        "native_sql":post_sql,
        "worker_before":held.budget,
        "worker_after":worker.budget,
        "worker_original_reservation_state":"RELEASED",
        "worker_original_reservation_key":reservation_key,
        "independent_baseline_reserved_usd":original_unrelated_worker_reserve,
        "worker_sequence_before":held.risk_book_sequence,
        "worker_sequence_after":worker.risk_book_sequence,
        "outbound_posts_before":prior_posts,
        "outbound_posts_after":feed.outbound_posts.lock().unwrap().len(),
        "status_attempts":attempts,
        "no_venue_post_replayed":true,
    })
}
