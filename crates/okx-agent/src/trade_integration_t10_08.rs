// T10-08 negative leg: the EXACT T09.3 SQL intent is already partially filled
// and has one signed USDT bill. A distinct signed venue stop response then
// reports FAILED. The native protective owner sends one bounded Cancel,
// receives an explicit venue rejection, and returns a durable blocker without
// resending a trading mutation or releasing the Worker reservation.
// Another signed fill+bill phase must occur AFTER this event.
async fn t10_08_failed_stop_before_late_fill(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent_id: String,
    client_id: String,
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
    let sql_id=intent_id.clone();
    let before_sql=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),move |sql| {
            let persisted=sql.intent(&sql_id)?.ok_or(okx_execution::StoreError::Invalid(
                "T10_08_ORIGINAL_SQL_INTENT_MISSING"))?;
            let fills=sql.cursor("fills","SWAP")?.ok_or(
                okx_execution::StoreError::Invalid("T10_08_PARTIAL_FILL_CURSOR_MISSING"))?;
            let bills=sql.cursor("bills","account")?.ok_or(
                okx_execution::StoreError::Invalid("T10_08_PARTIAL_BILL_CURSOR_MISSING"))?;
            let f=sql.intent_fill_cost_evidence(&sql_id,fills.watermark)?;
            let b=sql.intent_fee_bill_evidence(&sql_id,bills.watermark)?;
            Ok(serde_json::json!({
                "intent_id":sql_id,
                "state":persisted.state.as_str(),
                "fill_quantity":f.filled_quantity.normalize().to_string(),
                "fill_count":f.fill_count,
                "bill_count":b.linked_bill_count,
                "fee_usdt":b.conservative_verified_fee_usdt.normalize().to_string(),
                "revision":persisted.revision,
                "central_held":sql.active_central_receipt(&persisted.intent_id)?.is_some(),
            }))
        }
    ).await.expect("T10-08 must read actual partial SQL");
    assert_eq!(before_sql["state"],"PARTIALLY_FILLED");
    assert_eq!(before_sql["fill_quantity"],"0.12");
    assert_eq!(before_sql["fill_count"],1);
    assert_eq!(before_sql["bill_count"],1);
    assert_eq!(before_sql["fee_usdt"],"0.0006");
    assert_eq!(before_sql["central_held"],true);

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
    // An actual venue negative cancel response is distinct from an
    // unobserved ACK: one signed request is permitted, NEVER a retry.
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
    assert_eq!(protective["reason_code"],"SAFETY_CANCEL_NOT_CONFIRMED_NO_RESUBMIT");
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
