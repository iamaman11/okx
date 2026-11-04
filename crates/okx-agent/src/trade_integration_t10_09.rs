// T10-09: the B account's authentic venue Cancel ACK is already persisted by
// T10-14. Introduce a NEW recorded signed B fill/bill/position observation
// AFTER that ACK. Only the production account capture, SQL and TradeService
// may ingest/interpret the facts; no test-owned finance state is written.
async fn t10_09_b_signed_late_fill_after_cancel(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent_id: String,
    client_id: String,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::trading::TradeRequest;
    use serde_json::json;

    let raw = |value: serde_json::Value| {
        serde_json::to_vec(&json!({"code":"0","msg":"","data":value})).unwrap()
    };
    let posts_before = feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before, 6, "late B fill must follow public Cancel ACK");

    let now_ms = chrono::Utc::now().timestamp_millis();
    let at = now_ms.to_string();
    let fill = json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP",
        "tradeId":"t10-09-b-late-trade-001",
        "ordId":"recorded-b1-b-001","clOrdId":"__REQUEST_CL_ORD_ID__",
        "billId":"t10-09-b-late-bill-001","subType":"3",
        "side":"buy","posSide":"long","fillPx":"1000","fillSz":"0.04",
        "fillPnl":"0","fee":"-0.0002","feeCcy":"USDT",
        "ts":at,"fillTime":at
    }]);
    let bills = json!([{
        "billId":"t10-09-b-late-bill-001","instType":"SWAP",
        "instId":"BTC-USDT-SWAP","ordId":"recorded-b1-b-001",
        "tradeId":"t10-09-b-late-trade-001",
        "clOrdId":"__REQUEST_CL_ORD_ID__",
        "ccy":"USDT","type":"2","subType":"3",
        "balChg":"-0.0002","fee":"-0.0002","pnl":"0","ts":at
    }]);
    let fill_wire = raw(fill);
    let bill_wire = raw(bills);
    let mut fill_reads = Vec::new();
    for _ in 0..8 {
        for partition in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
            fill_reads.push(RecordedGet {
                access_key: "fixture-executor-b".into(),
                path: "/api/v5/trade/fills-history".into(),
                required_query: vec![("instType".into(),partition.into())],
                raw_response: if partition == "SWAP" {
                    fill_wire.clone()
                } else {
                    raw(json!([]))
                },
            });
        }
    }
    // Account B has never had an execution fill before this cancel ACK.
    // These are FIRST registered signed B history routes, not phase
    // advancement of an already registered transcript. The strict finite
    // transport rejects unregistered advance_recorded_route by design.
    for page in fill_reads {
        feed.expect_get(page);
    }
    for _ in 0..8 {
        feed.expect_get(RecordedGet {
            access_key: "fixture-executor-b".into(),
            path: "/api/v5/account/bills-archive".into(),
            required_query: vec![],raw_response:bill_wire.clone(),
        });
    }
    // Late fill is NOT a terminal canceled order. The venue can still report
    // a partial position after acknowledging only the cancel request.
    let order = raw(json!([{
        "instId":"BTC-USDT-SWAP","ordId":"recorded-b1-b-001",
        "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy",
        "posSide":"long","tdMode":"cross","ordType":"limit",
        "px":"1000","sz":"0.1","accFillSz":"0.04",
        "avgPx":"1000","state":"partially_filled",
        "cTime":at,"uTime":at
    }]));
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/trade/order",
        (0..6).map(|_|RecordedGet {
            access_key: "fixture-executor-b".into(),
            path: "/api/v5/trade/order".into(),
            required_query: vec![("instId".into(),"BTC-USDT-SWAP".into())],
            raw_response:order.clone(),
        }).collect()
    );
    let positions = raw(json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP",
        "posSide":"long","pos":"0.04","mgnMode":"cross",
        "avgPx":"1000","markPx":"1000","uTime":at
    }]));
    feed.advance_recorded_route(
        "fixture-observer-b","/api/v5/account/positions",
        (0..12).map(|_|RecordedGet {
            access_key: "fixture-observer-b".into(),
            path: "/api/v5/account/positions".into(),
            required_query: vec![],raw_response:positions.clone(),
        }).collect()
    );
    let balance = raw(json!([{
        "totalEq":"100","adjEq":"100","uTime":at,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
          "availEq":"100","availBal":"100","eqUsd":"100","uTime":at}]
    }]));
    feed.advance_recorded_route(
        "fixture-observer-b","/api/v5/account/balance",
        (0..8).map(|_|RecordedGet {
            access_key: "fixture-observer-b".into(),
            path: "/api/v5/account/balance".into(),
            required_query: vec![],raw_response:balance.clone(),
        }).collect()
    );

    // This is the SAME verified B SQL owner as the earlier Cancel. The
    // account's actual capture loop must ingest its new signed economic fact.
    let owner = accounts.resolve("fixture-b").await.expect("T10-09 B owner");
    let target = intent_id.clone();
    let evidence = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let id = target.clone();
            let observed = owner.execution.store.call(
                std::time::Instant::now()+Duration::from_secs(3),
                move |sql| {
                    let fills = sql.cursor("fills","SWAP")?
                        .ok_or(okx_execution::StoreError::Invalid(
                            "T10_09_B_FILL_CURSOR_MISSING"))?;
                    let bills = sql.cursor("bills","account")?
                        .ok_or(okx_execution::StoreError::Invalid(
                            "T10_09_B_BILL_CURSOR_MISSING"))?;
                    let f = sql.intent_fill_cost_evidence(&id,fills.watermark)?;
                    let b = sql.intent_fee_bill_evidence(&id,bills.watermark)?;
                    let persisted = sql.intent(&id)?
                        .ok_or(okx_execution::StoreError::Invalid(
                            "T10_09_B_SQL_INTENT_MISSING"))?;
                    let held = sql.active_central_receipt(&id)?.is_some();
                    Ok(json!({
                        "quantity":f.filled_quantity.normalize().to_string(),
                        "fill_count":f.fill_count,
                        "bill_count":b.linked_bill_count,
                        "verified_fee_usdt":
                            b.conservative_verified_fee_usdt.normalize().to_string(),
                        "bill_digest":b.source_digest,
                        "fill_watermark":fills.watermark,
                        "bill_watermark":bills.watermark,
                        "fill_gaps":fills.coverage_gaps,
                        "bill_gaps":bills.coverage_gaps,
                        "sql_state":persisted.state.as_str(),
                        "sql_revision":persisted.revision,
                        "active_central_receipt":held,
                    }))
                }
            ).await;
            if let Ok(value) = observed {
                if value["quantity"]=="0.04"
                    && value["fill_count"]==1
                    && value["bill_count"]==1
                    && value["verified_fee_usdt"]=="0.0002"
                    && value["fill_gaps"]==json!([])
                    && value["bill_gaps"]==json!([])
                    && value["fill_watermark"].as_i64().unwrap_or(0)>=now_ms
                    && value["bill_watermark"].as_i64().unwrap_or(0)>=now_ms
                {
                    break value;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("T10_09_B_SIGNED_LATE_FILL_NOT_INGESTED");
    assert_eq!(evidence["active_central_receipt"],true,
        "Cancel ACK and late fill cannot release the B reservation");
    let status = trade.trade(
        "fixture-b",&TradeRequest::Status{reference:intent_id.clone()},None
    ).await.expect("T10-09 public B Status after late signed fill");
    assert_eq!(status["data"]["intent"]["intent_id"],intent_id);
    assert_ne!(status["data"]["intent"]["state"],"CANCELLED",
        "a cancel acknowledgement cannot suppress the late venue fill");
    let posts_after = feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_after,posts_before,"late fill cannot create venue POST");
    let mut cmd = summary("risk_t10_09_after_signed_late_b_fill");
    cmd.installation_id="installation_test".into();
    let worker = risk.request(cmd).await.expect("T10-09 real Worker readback");
    assert_ne!(worker.budget.as_ref().unwrap().entry_budget
        .as_ref().unwrap().reserved_usd,"0");
    json!({
        "schema":"okx.t10-09-late-signed-b-fill/v1",
        "same_b_intent":intent_id,
        "client_order_id":client_id,
        "account_uid":owner.configuration.account_key.verified_uid,
        "after_cancel_ack":true,
        "signed_fill_and_bill_ingested_by_native_sql":true,
        "sql":evidence,
        "public_status_state":status["data"]["intent"]["state"],
        "worker_budget":worker.budget,
        "worker_sequence":worker.risk_book_sequence,
        "posts_before":posts_before,"posts_after":posts_after,
    })
}
