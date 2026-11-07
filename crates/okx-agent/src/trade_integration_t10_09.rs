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
    // This is B's FIRST signed non-flat position source; there was no
    // earlier positions route to advance. Keep the first-page registration
    // strict and finite rather than installing a synthetic fallback.
    for _ in 0..12 {
        feed.expect_get(RecordedGet {
            access_key: "fixture-observer-b".into(),
            path: "/api/v5/account/positions".into(),
            required_query: vec![],raw_response:positions.clone(),
        });
    }
    // A newly executed partial 0.04 entry needs a separately authenticated
    // live protective stop of precisely its filled amount. A Cancel ACK
    // does not create this fact: this is an explicit later venue GET sample,
    // with its own known algo ID, not a synthetic SQL protection receipt.
    // Without it production correctly enters emergency_exit, potentially
    // sending a new safety Cancel; that is a distinct negative fixture.
    assert!(client_id.len() >= 31, "B client ID must bind the attached stop");
    let stop_client = format!("s{}", &client_id[..31]);
    let stop_id = "900000004";
    let signed_stop = raw(json!([{
        "instId":"BTC-USDT-SWAP",
        "algoId":stop_id,"algoClOrdId":stop_client,
        "state":"live","sz":"0.04",
        "side":"sell","posSide":"long","tdMode":"cross",
        "ordType":"conditional",
        "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
    }]));
    for _ in 0..6 {
        feed.expect_get(RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/trade/order-algo".into(),
            required_query:vec![("algoClOrdId".into(),stop_client.clone())],
            raw_response:signed_stop.clone(),
        });
    }
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
    let mut last_seen = json!({"state":"NO_SQL_READBACK_YET"});
    let captured = tokio::time::timeout(Duration::from_secs(15), async {
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
            match observed {
              Ok(value) => {
                if value["quantity"]=="0.04"
                    && value["fill_count"]==1
                    && value["bill_count"]==1
                    && value["verified_fee_usdt"]=="0.0002"
                    && value["fill_gaps"].as_str()==Some("")
                    && value["bill_gaps"].as_str()==Some("")
                    && value["fill_watermark"].as_i64().unwrap_or(0)>=now_ms
                    && value["bill_watermark"].as_i64().unwrap_or(0)>=now_ms
                {
                    break value;
                }
                last_seen = value;
              }
              Err(error) => {
                last_seen = json!({"sql_readback_error": format!("{error:?}")});
              }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await;
    let evidence = match captured {
        Ok(evidence) => evidence,
        Err(_) => {
            let health = owner.source_capture_health.read().await;
            let requests = feed.requests.lock().unwrap();
            let source_counts = |route: &str| requests.iter()
                .filter(|entry| entry.starts_with(route)).count();
            let denials = feed.denied_routes.lock().unwrap();
            eprintln!("T10_09_B_CAPTURE_TIMEOUT_V1:{}", json!({
                "schema":"okx.t10-09-signed-b-capture-timeout/v1",
                "signed_event_at_ms":now_ms,
                "last_sql":last_seen,
                "source_health":{
                    "last_attempt_ms":health.last_attempt_ms,
                    "last_success_ms":health.last_success_ms,
                    "last_failure_ms":health.last_failure_ms,
                    "reason_code":health.reason_code,
                },
                "signed_source_reads":{
                    "fills":source_counts("fixture-executor-b:/api/v5/trade/fills-history?"),
                    "bills":source_counts("fixture-executor-b:/api/v5/account/bills-archive?"),
                    "positions":source_counts("fixture-observer-b:/api/v5/account/positions?"),
                    "balance":source_counts("fixture-observer-b:/api/v5/account/balance?"),
                    "order":source_counts("fixture-executor-b:/api/v5/trade/order?"),
                },
                "denied_route_count":denials.len(),
                "denied_route_last":denials.last(),
            }));
            panic!("T10_09_B_SIGNED_LATE_FILL_NOT_INGESTED");
        }
    };
    assert_eq!(evidence["active_central_receipt"],true,
        "Cancel ACK and late fill cannot release the B reservation");
    // The async financial capture above may outlive the original B order
    // clock observation. Status now starts a distinct read-only source phase:
    // publish ONE signed exchange time at that real boundary, never a
    // clock-on-demand fallback or relaxed production freshness check.
    let status_clock = chrono::Utc::now().timestamp_millis().to_string();
    let retired_clock = feed.advance_recorded_public_clock(
        raw(json!([{"ts":status_clock}]))
    );
    assert!(retired_clock <= 1,
        "T10-09 cannot replace multiple unseen authenticated clock samples");
    let status = trade.trade(
        "fixture-b",&TradeRequest::Status{reference:intent_id.clone()},None
    ).await.expect("T10-09 public B Status after late signed fill");
    assert_eq!(status["data"]["intent"]["intent_id"],intent_id);
    assert_eq!(status["data"]["protection_incident"],false,
        "a separately signed live 0.04 stop must protect the B late fill");
    assert_eq!(status["data"]["protection"]["algo_id"],stop_id,
        "the SQL protection receipt must bind the observed venue algo ID");
    assert_eq!(status["data"]["protection"]["quantity"],"0.04",
        "remaining venue stop must cover exactly the signed partial fill");
    assert_ne!(status["data"]["intent"]["state"],"CANCELLED",
        "a cancel acknowledgement cannot suppress the late venue fill");
    // Read-only fail evidence: classify a possible post-cancel protective
    // action by the exact signed route and operation IDs. Do not permit an
    // extra create, suppress a cancellation, or pre-label a POST as safe.
    let unexpected_posts = {
        let sent = feed.outbound_posts.lock().unwrap();
        sent.iter().skip(posts_before)
            .map(|(actor, route, body)| json!({
                "account_actor": actor,
                "route": route,
                "client_order_id": body.get("clOrdId"),
                "algo_ids": body.as_array().map(|orders| orders.iter()
                    .filter_map(|order| order.get("algoId"))
                    .collect::<Vec<_>>()),
            }))
            .collect::<Vec<_>>()
    };
    let posts_after = feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_after,posts_before,
        "T10_09_POST_CANCEL_LATE_FILL_MUTATION_V1:{}",
        json!({
            "before":posts_before,"after":posts_after,
            "unexpected_posts":unexpected_posts,
            "public_status_state":status["data"]["intent"]["state"],
            "protection_incident":status["data"]["protection_incident"],
            "protection":status["data"]["protection"],
            "financial_error":status["data"]["financial_error"],
        }));
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
        "signed_late_at_ms":now_ms,
        "sql":evidence,
        "public_status_state":status["data"]["intent"]["state"],
        "worker_budget":worker.budget,
        "worker_sequence":worker.risk_book_sequence,
        "posts_before":posts_before,"posts_after":posts_after,
    })
}


// The separate Cancel ACK does not prove the still-open 0.06 can no longer
// execute. Verify a NEW, order-ID-bound terminal readback and use the normal
// public TradeService Reduce with the original B SQL owner and signed ACK.
// No fill, bill, position or Worker money is injected by this helper.
async fn t10_09_b_public_partial_reduce(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    original_intent_id: String,
) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
    use okx_protocol::trading::{
        OrderConstraints, PositionSide, Sizing, TradeOperation, TradeRequest,
    };
    use serde_json::json;
    let raw = |v| {
        serde_json::to_vec(&json!({"code":"0","msg":"","data":v}))
            .expect("T10-09 immutable OKX wire")
    };
    let before = feed.outbound_posts.lock().unwrap().len();
    assert_eq!(before, 6, "public Reduce must follow B Cancel ACK and late fill");
    let at = chrono::Utc::now().timestamp_millis().to_string();
    let canceled = raw(json!([{
        "instId":"BTC-USDT-SWAP","ordId":"recorded-b1-b-001",
        "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy",
        "posSide":"long","tdMode":"cross","ordType":"limit",
        "px":"1000","sz":"0.1","accFillSz":"0.04",
        "avgPx":"1000","state":"canceled","cTime":at,"uTime":at
    }]));
    feed.advance_recorded_route(
        "fixture-executor-b", "/api/v5/trade/order",
        (0..12).map(|_| RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/trade/order".into(),
            required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
            raw_response:canceled.clone(),
        }).collect(),
    );
    // At the independently observed terminal Cancel readback, the earlier
    // signed 0.04 stop remains LIVE. Publish its new authenticated GET event
    // BEFORE Status can re-verify the opening intent; without this source
    // production must retain a durable PROTECTION_PENDING_OR_FAILED incident.
    // Neither this response nor the event mutates Native SQL or RiskBook.
    let owner = accounts.resolve("fixture-b").await
        .expect("T10-09 original B Native SQL owner");
    let original = original_intent_id.clone();
    let (opening_client,preexisting_incident,central_held)=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |sql| {
            let opening=sql.intent(&original)?.ok_or(
                okx_execution::StoreError::Invalid(
                    "T10_09_ORIGINAL_OPENING_INTENT_MISSING"
                )
            )?;
            Ok((
                opening.client_order_id,
                opening.incident,
                sql.active_central_receipt(&original)?.is_some(),
            ))
        },
    ).await.expect("T10-09 original B source and risk identity");
    assert_eq!(preexisting_incident,None,
        "T10_09_PROTECTION_INCIDENT_BEFORE_TERMINAL_SOURCE");
    assert!(central_held,"T10-09 original B reservation must remain active");
    let stop_client=format!("s{}",opening_client.get(..31)
        .expect("T10-09 original B stop client identity"));
    let signed_live_stop=raw(json!([{
        "instId":"BTC-USDT-SWAP","algoId":"900000004",
        "algoClOrdId":stop_client,"state":"live","sz":"0.04",
        "side":"sell","posSide":"long","tdMode":"cross",
        "ordType":"conditional",
        "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1",
        "uTime":at
    }]));
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/trade/order-algo",
        (0..8).map(|_|RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/trade/order-algo".into(),
            required_query:vec![("algoClOrdId".into(),stop_client.clone())],
            raw_response:signed_live_stop.clone(),
        }).collect(),
    );
    feed.advance_recorded_public_clock(raw(json!([{"ts":at}])));
    let terminal = trade.trade(
        "fixture-b",
        &TradeRequest::Status { reference:original_intent_id.clone() },
        None,
    ).await.expect("T10-09 authenticated terminal B order readback");
    assert_eq!(terminal["data"]["intent"]["intent_id"], original_intent_id);
    assert_eq!(terminal["data"]["protection_incident"],false,
        "T10-09 B Cancel readback must not create an incident with a signed live stop");
    assert_eq!(terminal["data"]["protection"]["algo_id"],"900000004",
        "the still-live 0.04 stop must remain bound to the original");
    assert_ne!(terminal["data"]["central_release"]["state"], "verified",
        "a canceled order with 0.04 signed execution cannot release live exposure");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(), before,
        "terminal B order readback must not create a venue POST");
    let key = "t10_09_public_reduce_020_0001".to_owned();
    let at = chrono::Utc::now().timestamp_millis().to_string();
    // The original B balance/position GET pages were consumed by source
    // capture and the terminal Cancel Status. This is a NEW bounded external
    // observation at the Reduce boundary, not a demand-driven fallback.
    // No fee or position mutation is fabricated before the real Reduce fill.
    let balance = raw(json!([{
        "totalEq":"100","adjEq":"100","uTime":at,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":at}]
    }]));
    feed.advance_recorded_route(
        "fixture-observer-b", "/api/v5/account/balance",
        (0..48).map(|_| RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],
            raw_response:balance.clone(),
        }).collect(),
    );
    let unchanged_position = raw(json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP",
        "posSide":"long","pos":"0.04","mgnMode":"cross",
        "avgPx":"1000","markPx":"1000","uTime":at
    }]));
    feed.advance_recorded_route(
        "fixture-observer-b", "/api/v5/account/positions",
        (0..48).map(|_| RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],
            raw_response:unchanged_position.clone(),
        }).collect(),
    );
    feed.advance_recorded_public_clock(raw(json!([{"ts":at}])));
    feed.expect_public_clock(raw(json!([{
        "ts":chrono::Utc::now().timestamp_millis().to_string()
    }])));
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/order".into(),
        expected_body:json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell",
            "posSide":"long","ordType":"ioc","sz":"0.02","px":"999"
        }),
        raw_response:raw(json!([{
            "ordId":"recorded-t10-09-b-reduce-020",
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"
        }])),
    });
    feed.expect_get(RecordedGet {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/order".into(),
        required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
        raw_response:raw(json!([{
            "instId":"BTC-USDT-SWAP",
            "ordId":"recorded-t10-09-b-reduce-020",
            "clOrdId":"__REQUEST_CL_ORD_ID__","side":"sell",
            "posSide":"long","tdMode":"cross","ordType":"ioc",
            "px":"999","sz":"0.02","accFillSz":"0",
            "avgPx":"","state":"live","cTime":at,"uTime":at
        }])),
    });
    let response = trade.trade(
        "fixture-b",
        &TradeRequest::Execute {
            idempotency_key:key.clone(),
            operation:TradeOperation::Reduce,
            strategy_instance:"manual".into(),
            instrument:"BTC-USDT-SWAP".into(),
            side:PositionSide::Long,
            sizing:Sizing::Contracts { contracts:"0.02".into() },
            order:OrderConstraints {
                order_type:"ioc".into(),
                limit_price:Some("999".into()),
                reference:Some(original_intent_id.clone()),
                max_entry_usd:None,
                max_slippage_bps:50,
            },
            protection:None,
            expires_in_ms:30_000,
        },
        None,
    ).await.unwrap_or_else(|reason| {
        panic!("T10_09_REAL_PUBLIC_REDUCE_DENIED:{reason}")
    });
    let after = feed.outbound_posts.lock().unwrap().len();
    assert_eq!(after, before + 1,
        "T10-09 exactly one source-bound public Reduce POST, no reentry");
    assert_eq!(feed.unconsumed_posts(), 0,
        "T10-09 must consume signed venue ACK, not infer execution");
    let saved = owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |sql| {
            let i = sql.by_idempotency(&key)?.ok_or(
                okx_execution::StoreError::Invalid("T10_09_REDUCE_SQL_MISSING")
            )?;
            Ok(json!({
                "intent_id":i.intent_id,
                "state":i.state.as_str(),
                "exchange_order_id":i.exchange_order_id,
                "quantity":i.plan.quantity.normalize().to_string(),
                "operation":i.plan.operation,
                "risk_increase":i.risk_increase,
                "revision":i.revision,
            }))
        },
    ).await.expect("T10-09 original B SQL Reduce readback");
    assert_eq!(saved["exchange_order_id"], "recorded-t10-09-b-reduce-020");
    assert_eq!(saved["quantity"], "0.02");
    assert_eq!(saved["operation"], "reduce");
    assert_eq!(saved["risk_increase"], false);
    json!({
        "schema":"okx.t10-09-source-bound-partial-reduce/v1",
        "original_intent_id":original_intent_id,
        "original_terminal_order_state":terminal["data"]["intent"]["state"],
        "original_central_release":terminal["data"]["central_release"],
        "reduce":saved,
        "execute_status":response["status"],
        "outbound_posts_before":before,
        "outbound_posts_after":after,
        "close_fill_and_remnant_protection_pending":true,
    })
}


// The partial Reduce ACK is not a fill. Publish a distinct immutable B
// execution + bill and a fresh 0.02 signed position; only account capture
// and the ordinary SQL finance projection can change the owned lot.
async fn t10_09_b_signed_partial_reduce_finance(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    reduce: &serde_json::Value,
    original_late_at: i64,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::trading::TradeRequest;
    use serde_json::json;
    let raw = |v| serde_json::to_vec(&json!({"code":"0","msg":"","data":v}))
        .expect("T10-09 authenticated reduce finance source");
    let reduce_id = reduce["reduce"]["intent_id"]
        .as_str().expect("T10-09 public Reduce owner").to_owned();
    let at = chrono::Utc::now().timestamp_millis().max(original_late_at + 1);
    let first_at = original_late_at.to_string();
    let reduce_at = at.to_string();
    let fills = json!([
        {
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "tradeId":"t10-09-b-late-trade-001",
            "ordId":"recorded-b1-b-001","clOrdId":"__REQUEST_CL_ORD_ID__",
            "billId":"t10-09-b-late-bill-001","subType":"3",
            "side":"buy","posSide":"long","fillPx":"1000","fillSz":"0.04",
            "fillPnl":"0","fee":"-0.0002","feeCcy":"USDT",
            "ts":first_at,"fillTime":first_at
        },
        {
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "tradeId":"t10-09-b-reduce-trade-001",
            "ordId":"recorded-t10-09-b-reduce-020",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "billId":"t10-09-b-reduce-bill-001","subType":"5",
            "side":"sell","posSide":"long","fillPx":"1000","fillSz":"0.02",
            "fillPnl":"0","fee":"-0.0001","feeCcy":"USDT",
            "ts":reduce_at,"fillTime":reduce_at
        }
    ]);
    let bills = json!([
        {
            "billId":"t10-09-b-late-bill-001","instType":"SWAP",
            "instId":"BTC-USDT-SWAP","ordId":"recorded-b1-b-001",
            "tradeId":"t10-09-b-late-trade-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "ccy":"USDT","type":"2","subType":"3",
            "balChg":"-0.0002","fee":"-0.0002","pnl":"0","ts":first_at
        },
        {
            "billId":"t10-09-b-reduce-bill-001","instType":"SWAP",
            "instId":"BTC-USDT-SWAP","ordId":"recorded-t10-09-b-reduce-020",
            "tradeId":"t10-09-b-reduce-trade-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "ccy":"USDT","type":"2","subType":"5",
            "balChg":"-0.0001","fee":"-0.0001","pnl":"0","ts":reduce_at
        }
    ]);
    // Arm only B's exact signed amend response immediately after the public
    // Reduce ACK, BEFORE publishing the new fill/bill event. The independent
    // account capture and maintenance Status can legitimately observe that
    // financial event before this test's SQL polling loop completes; arming
    // after the poll would reject an authorized first POST as an unexpected
    // transport send and leave its production fence in POSTING. Merely
    // arming a raw response does not mutate a venue stop, SQL, or RiskBook.
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),7,
        "T10-09 recorded amend response belongs after the single public Reduce");
    let owner=accounts.resolve("fixture-b").await.expect("T10-09 B source owner");
    // First inspect the signed original 0.04 stop. An ACK for the shrink
    // can only arrive from the sole production TradeService mutation owner.
    // The recorded transport does not alter a live algo just because the
    // test later wants to observe a 0.02 stop.
    let original_client = reduce["original_intent_id"].as_str()
        .expect("T10-09 original opening intent id").to_owned();
    let reduce_client = reduce_id.clone();
    let (opening_client_id, reducing_client_id) = owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| {
            let opening=s.intent(&original_client)?.ok_or(
                okx_execution::StoreError::Invalid(
                    "T10_09_OPENING_CLIENT_ID_MISSING"
                )
            )?;
            let reduced=s.intent(&reduce_client)?.ok_or(
                okx_execution::StoreError::Invalid(
                    "T10_09_REDUCE_CLIENT_ID_MISSING"
                )
            )?;
            Ok((opening.client_order_id, reduced.client_order_id))
        },
    ).await.expect("T10-09 exact persisted source identity");
    let stop_client=format!("s{}",opening_client_id.get(..31)
        .expect("T10-09 original client ID must be 32 bytes"));
    let prior_live=raw(json!([{
        "instId":"BTC-USDT-SWAP",
        "algoId":"900000004","algoClOrdId":stop_client,
        "state":"live","sz":"0.04","side":"sell",
        "posSide":"long","tdMode":"cross","ordType":"conditional",
        "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
    }]));
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/trade/order-algo",
        (0..12).map(|_| RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/trade/order-algo".into(),
            required_query:vec![("algoClOrdId".into(),stop_client.clone())],
            raw_response:prior_live.clone(),
        }).collect(),
    );
    feed.expect_post(crate::accounts::recorded_support::RecordedPost {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/amend-algos".into(),
        expected_body:json!({
            "instId":"BTC-USDT-SWAP","algoId":"900000004",
            "reqId":reducing_client_id,
            "newSz":"0.02","cxlOnFail":false,
        }),
        raw_response:raw(json!([{
            "algoId":"900000004",
            "algoClOrdId":stop_client,
            "reqId":reducing_client_id,
            "sCode":"0",
        }])),
    });
    let mut pages=Vec::new();
    for _ in 0..32 {
        for partition in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
            pages.push(RecordedGet {
                access_key:"fixture-executor-b".into(),
                path:"/api/v5/trade/fills-history".into(),
                required_query:vec![("instType".into(),partition.into())],
                raw_response:if partition=="SWAP" {raw(fills.clone())}
                    else {raw(json!([]))},
            });
        }
    }
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/trade/fills-history",pages
    );
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/account/bills-archive",
        (0..32).map(|_| RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/account/bills-archive".into(),
            required_query:vec![],
            raw_response:raw(bills.clone()),
        }).collect(),
    );
    let position = raw(json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"long",
        "pos":"0.02","mgnMode":"cross","avgPx":"1000",
        "markPx":"1000","uTime":reduce_at
    }]));
    feed.advance_recorded_route(
        "fixture-observer-b","/api/v5/account/positions",
        (0..48).map(|_| RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],
            raw_response:position.clone(),
        }).collect(),
    );
    let balance = raw(json!([{
        "totalEq":"99.9997","adjEq":"99.9997","uTime":reduce_at,
        "details":[{"ccy":"USDT","eq":"99.9997","liab":"0",
            "availEq":"99.9997","availBal":"99.9997",
            "eqUsd":"99.9997","uTime":reduce_at}]
    }]));
    feed.advance_recorded_route(
        "fixture-observer-b","/api/v5/account/balance",
        (0..48).map(|_| RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],
            raw_response:balance.clone(),
        }).collect(),
    );
    let mut orders=Vec::new();
    for _ in 0..32 {
        for (order,side,kind,qty,total,when) in [
            ("recorded-b1-b-001","buy","limit","0.1","0.04",first_at.as_str()),
            ("recorded-t10-09-b-reduce-020","sell","ioc","0.02","0.02",reduce_at.as_str()),
        ] {
            orders.push(RecordedGet {
                access_key:"fixture-executor-b".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:raw(json!([{
                    "instId":"BTC-USDT-SWAP","ordId":order,
                    "clOrdId":"__REQUEST_CL_ORD_ID__","side":side,
                    "posSide":"long","tdMode":"cross","ordType":kind,
                    "px":if kind=="ioc" {"999"}else{"1000"},
                    "sz":qty,"accFillSz":total,"avgPx":"1000",
                    "state":if kind=="ioc" {"filled"}else{"canceled"},
                    "cTime":when,"uTime":when
                }])),
            });
        }
    }
    feed.advance_recorded_route("fixture-executor-b","/api/v5/trade/order",orders);
    let reduction=reduce_id.clone();
    let last=std::time::Instant::now()+Duration::from_secs(16);
    let mut pending=json!({"reason":"WAITING_FOR_SIGNED_SOURCE"});
    let signed=loop {
        assert!(std::time::Instant::now()<last,
            "T10_09_B_REDUCE_FINANCE_CAPTURE_MISSING:{pending}");
        let id=reduction.clone();
        let found=owner.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            move |s| {
                let active=s.active_lots("BTC-USDT-SWAP")?;
                let qty=active.iter().fold(Decimal::ZERO,|n,l|n+l.remaining_qty);
                let fills=s.cursor("fills","SWAP")?;
                let bills=s.cursor("bills","account")?;
                Ok(json!({
                    "remaining_lot_qty":qty.normalize().to_string(),
                    "remaining_lot_count":active.len(),
                    "fill_watermark":fills.as_ref().map(|c|c.watermark),
                    "bill_watermark":bills.as_ref().map(|c|c.watermark),
                    "fills_complete":fills.as_ref().is_some_and(|c|
                        c.generation>0&&c.coverage_gaps.is_empty()&&c.cursor.is_empty()),
                    "bills_complete":bills.as_ref().is_some_and(|c|
                        c.generation>0&&c.coverage_gaps.is_empty()&&c.cursor.is_empty()),
                    "reduce_intent":s.intent(&id)?.map(|i|i.state.as_str().to_owned()),
                }))
            }
        ).await;
        match found {
            Ok(read) if read["remaining_lot_qty"]=="0.02"
                && read["remaining_lot_count"]==1
                && read["fills_complete"]==true
                && read["bills_complete"]==true
                && read["fill_watermark"].as_i64().unwrap_or(0)>=at
                && read["bill_watermark"].as_i64().unwrap_or(0)>=at => break read,
            Ok(read)=>pending=read,
            Err(reason)=>pending=json!({"sql_error":format!("{reason:?}")}),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // The real TradeService, not the test, records the signed terminal
    // venue readback. A partial Reduce cannot release the local marker
    // while 0.02 remains in the venue, or B's original Worker grant.
    feed.advance_recorded_public_clock(raw(json!([{
        "ts":chrono::Utc::now().timestamp_millis().to_string()
    }])));
    let status=trade.trade(
        "fixture-b",&TradeRequest::Status{reference:reduce_id.clone()},None,
    ).await.expect("T10-09 native terminal Reduce Status");
    assert_eq!(status["data"]["intent"]["state"],"FILLED");
    // A concurrent source-capture/Status read can report
    // protection_adjustment=not_applicable while its financial snapshot is
    // temporarily incomplete. That transient readout cannot erase a venue
    // amend ACK that TradeService has already durably recorded. Prove the
    // exact original/reduction/remaining-size operation in Native SQL before
    // publishing ANY post-ACK stop source: a seen POST alone is insufficient.
    let shrink_key=format!(
        "protection-shrink:{}:0.02",
        reduce["original_intent_id"].as_str()
            .expect("T10-09 original persisted intent for durable amend")
    );
    let ack=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| Ok(s.operation(&shrink_key)?.map(|op| json!({
            "kind":op.kind,
            "result_ref":op.result_ref,
            "request_hash":op.request_hash,
        }))),
    ).await.expect("T10-09 durable Native protection amendment receipt");
    let ack=if let Some(receipt)=ack {receipt} else {
        // The first failed SQL prerequisite, not a later transport denial,
        // identifies whether this is an original-risk incident, owner
        // mismatch, missing central hold or a pending local operation.
        // Inspect only the existing persisted B owner; do not alter it.
        let opening_id=reduce["original_intent_id"].as_str()
            .expect("T10-09 opening SQL identity").to_owned();
        let reducing_id=reduce_id.clone();
        let sql_risk=owner.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            move |s| {
                let opening=s.intent(&opening_id)?;
                let reduction=s.intent(&reducing_id)?;
                let held=s.active_central_receipt(&opening_id)?.is_some();
                let stop=s.protection(&opening_id)?;
                let lots=s.active_lots("BTC-USDT-SWAP")?;
                Ok(json!({
                    "opening_state":opening.as_ref().map(|i|i.state.as_str()),
                    "opening_risk_increase":opening.as_ref().map(|i|i.risk_increase),
                    "opening_incident":opening.as_ref().and_then(|i|i.incident.clone()),
                    "central_hold_active":held,
                    "original_stop_qty":stop.as_ref().map(|p|p.quantity.normalize().to_string()),
                    "opening_instrument":opening.as_ref().map(|i|i.instrument_id.clone()),
                    "reduce_instrument":reduction.as_ref().map(|i|i.instrument_id.clone()),
                    "opening_strategy":opening.as_ref().map(|i|i.plan.strategy_instance.clone()),
                    "reduce_strategy":reduction.as_ref().map(|i|i.plan.strategy_instance.clone()),
                    "opening_side":opening.as_ref().map(|i|i.plan.position_side.clone()),
                    "reduce_side":reduction.as_ref().map(|i|i.plan.position_side.clone()),
                    "reduce_state":reduction.as_ref().map(|i|i.state.as_str()),
                    "reduce_operation":reduction.as_ref().map(|i|i.plan.operation.clone()),
                    "lots":lots.iter().map(|lot| json!({
                        "remaining_qty":lot.remaining_qty.normalize().to_string(),
                        "original_lot":lot.intent_id.as_deref()==Some(opening_id.as_str()),
                        "instance_id":lot.instance_id,
                        "side":lot.side,
                    })).collect::<Vec<_>>(),
                }))
            }
        ).await;
        panic!(
            "T10_09_AMEND_ACK_SQL_MISSING:status={} sql_risk={:?} expected_req_id={} last_post={:?} unconsumed_posts={} last_denial={:?}",
            status["data"]["protection_adjustment"],sql_risk,
            reducing_client_id,
            feed.outbound_posts.lock().unwrap().last().cloned(),
            feed.unconsumed_posts(),
            feed.denied_routes.lock().unwrap().last().cloned()
        );
    };
    assert_eq!(ack["kind"],"protection_shrink",
        "T10-09 B durable amend kind must be source-bound");
    assert_eq!(ack["result_ref"],"ACK_SOURCE_PENDING",
        "T10-09 neither the public Status nor an outbound send proves ACK: {ack}");
    assert_eq!(ack["request_hash"].as_str().map(str::len),Some(64),
        "T10-09 durable amendment must bind its exact request");
    assert_eq!(feed.unconsumed_posts(),0,
        "T10-09 exact signed B amend response must be consumed");

    assert_eq!(feed.outbound_posts.lock().unwrap().len(),8,
        "T10-09 sole amend-algos POST must follow the public Reduce");
    // At this exact boundary, B's original order has independently read
    // back CANCELED and the Reduce has a signed FILLED readback plus a
    // reconciled 0.02 lot. The next authenticated B orders-pending GET is
    // therefore a separate finite venue sample, not the transcript's
    // generic empty fallback. Only B's exact signer, route and query can
    // consume it; repeated polling never publishes a fresher phase.
    feed.expect_immutable_signed_phase_bounded(
        "t10-09-b-reduce-filled-pending-inventory",
        RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/trade/orders-pending".into(),
            required_query:vec![("limit".into(),"100".into())],
            raw_response:raw(json!([])),
        },
        24,
        Duration::from_secs(12),
    );
    // The next venue GET is independent of the amend ACK: the old source
    // remains 0.04 until the explicit post-ACK source handoff.
    let after_live=raw(json!([{
        "instId":"BTC-USDT-SWAP",
        "algoId":"900000004","algoClOrdId":stop_client,
        "state":"live","sz":"0.02","side":"sell",
        "posSide":"long","tdMode":"cross","ordType":"conditional",
        "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
    }]));
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/trade/order-algo",
        (0..16).map(|_| RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/trade/order-algo".into(),
            required_query:vec![("algoClOrdId".into(),stop_client.clone())],
            raw_response:after_live.clone(),
        }).collect(),
    );
    feed.advance_recorded_public_clock(raw(json!([{
        "ts":chrono::Utc::now().timestamp_millis().to_string()
    }])));
    let resized=trade.trade(
        "fixture-b",&TradeRequest::Status{reference:reduce_id.clone()},None,
    ).await.expect("T10-09 new independent signed 0.02 stop readback");
    assert_eq!(resized["data"]["protection_adjustment"]["state"],"verified",
        "a matching amend ACK alone cannot verify remaining protection");
    assert_eq!(resized["data"]["protection_adjustment"]["remaining_quantity"],"0.02");
    let b_pending_reads=feed.immutable_signed_phase_reads(
        "fixture-observer-b",
        "/api/v5/trade/orders-pending",
        "t10-09-b-reduce-filled-pending-inventory",
    );
    assert!(b_pending_reads > 0 && b_pending_reads <= 24,
        "T10-09 must consume the exact signed B orders-pending phase");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),8,
        "T10-09 verified stop readback must not amend twice");
    let proof=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| s.prove_local_partial_reduce(
            &reduce_id,"recorded-t10-09-b-reduce-020",Decimal::new(2,2),at,
        ),
    ).await.expect("T10-09 immutable local reduction finance proof");
    assert_eq!(proof.0,Decimal::new(-1,4));
    assert_eq!(proof.1,1);
    assert_eq!(proof.2.len(),64);
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),8,
        "signed stop verification must not retry Reduce or amend");
    json!({
        "schema":"okx.t10-09-signed-partial-reduce-finance/v1",
        "reduce_intent_id":reduce["reduce"]["intent_id"],
        "signed_reduce_at_ms":at,
        "sql":signed,
        "linked_fee_signed_usdt":proof.0.normalize().to_string(),
        "linked_fill_count":proof.1,
        "source_digest":proof.2,
        "reduce_status":status["data"]["intent"]["state"],
        "central_release_state":status["data"]["central_release"]["state"],
        "protection_amend_ack":json!({
            "state":"ack_source_pending",
            "source":"native_sql_durable_amend_ack",
            "kind":ack["kind"],
            "request_hash":ack["request_hash"],
        }),
        "protection_status_at_ack":status["data"]["protection_adjustment"],
        "protection_resized":resized["data"]["protection_adjustment"],
        "b_signed_pending_inventory_reads":b_pending_reads,
        "outbound_posts":feed.outbound_posts.lock().unwrap().len(),
    })
}


// The public B Close uses only the source-backed remaining SQL lot. A posted
// Close ACK is not a fill and does not authorize releasing the opening grant.
// Publish the third independent fill/bill and venue-flat snapshots only after
// the exact Close POST/ACK, then let ordinary Native capture and TradeService
// settle the original B reservation through the persisted Worker.
async fn t10_09_b_terminal_after_partial(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    rate_budget: RateBudget,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    opening_id: String,
    late_at: i64,
    partial_at: i64,
    a_order_venue_at: i64,
    a_finance_source_at_flat: &(impl Fn(&str) + Sync),
) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
    use okx_protocol::trading::TradeRequest;
    use serde_json::json;

    assert!(partial_at >= late_at, "T10-09 finance events must be ordered");
    let raw = |v:serde_json::Value| serde_json::to_vec(
        &json!({"code":"0","msg":"","data":v})
    ).expect("T10-09 independent signed close source");
    let before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(before,8,"B final Close follows exactly one Reduce and one stop amend");
    let clock=chrono::Utc::now().timestamp_millis().to_string();
    feed.advance_recorded_public_clock(raw(json!([{"ts":clock}])));
    feed.expect_public_clock(raw(json!([{
        "ts":chrono::Utc::now().timestamp_millis().to_string()
    }])));
    feed.advance_recorded_route(
        "fixture-observer-b","/api/v5/account/balance",
        (0..24).map(|_| RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],
            raw_response:raw(json!([{
                "totalEq":"99.9997","adjEq":"99.9997","uTime":clock,
                "details":[{"ccy":"USDT","eq":"99.9997","liab":"0",
                    "availEq":"99.9997","availBal":"99.9997",
                    "eqUsd":"99.9997","uTime":clock}]
            }])),
        }).collect()
    );
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/order".into(),
        expected_body:json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"sell","posSide":"long",
            "ordType":"ioc","sz":"0.02","px":"999"
        }),
        raw_response:raw(json!([{
            "ordId":"recorded-t10-09-b-final-close-020",
            "clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"
        }])),
    });
    feed.expect_get(RecordedGet {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/order".into(),
        required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
        raw_response:raw(json!([{
            "instId":"BTC-USDT-SWAP",
            "ordId":"recorded-t10-09-b-final-close-020",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"sell","posSide":"long","tdMode":"cross",
            "ordType":"ioc","px":"999","sz":"0.02","accFillSz":"0",
            "avgPx":"","state":"live","cTime":clock,"uTime":clock
        }])),
    });
    let operation_key="t10_09_public_final_close_020_0001".to_owned();
    let attempted=trade.trade(
        "fixture-b",
        &TradeRequest::Close {
            idempotency_key:operation_key.clone(),
            reference:opening_id.clone(),
            expires_in_ms:30_000,
        },None,
    ).await;
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),9,
        "T10-09 must send exactly one public source-backed 0.02 Close POST: {attempted:?}");
    assert_eq!(feed.unconsumed_posts(),0,
        "T10-09 Close must consume its exact authenticated venue ACK");
    let owner=accounts.resolve("fixture-b").await.expect("B persisted Native owner");
    let key=operation_key.clone();
    let close=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| {
            let i=s.by_idempotency(&key)?.ok_or(
                okx_execution::StoreError::Invalid("T10_09_CLOSE_SQL_NOT_PERSISTED")
            )?;
            Ok(json!({
                "intent_id":i.intent_id,
                "exchange_order_id":i.exchange_order_id,
                "client_order_id":i.client_order_id,
                "state":i.state.as_str(),
                "quantity":i.plan.quantity.normalize().to_string(),
                "risk_increase":i.risk_increase,
                "operation":i.plan.operation,
                "revision":i.revision,
            }))
        }
    ).await.expect("T10-09 persisted Close intent after single ACK");
    assert_eq!(close["quantity"],"0.02","the ordinary public Close must own the SQL remainder");
    assert_eq!(close["risk_increase"],false);
    assert_eq!(close["operation"],"close");
    assert_eq!(close["exchange_order_id"],"recorded-t10-09-b-final-close-020",
        "the single venue ACK must be durably bound to B Close");
    let close_id=close["intent_id"].as_str()
        .expect("T10-09 durable B Close intent").to_owned();

    // This THIRD B signed economic event occurs only after the persisted
    // Close ACK. Replaying the first two immutable events does not create
    // new economics; the last event is one exact 0.02 sell + -0.0001 fee.
    let at=chrono::Utc::now().timestamp_millis().max(partial_at+1);
    let mut fills=Vec::new();
    let mut bills=Vec::new();
    for (trade_id,bill_id,ord_id,side,sub_type,quantity,fee,when) in [
        ("t10-09-b-late-trade-001","t10-09-b-late-bill-001",
            "recorded-b1-b-001","buy","3","0.04","-0.0002",late_at),
        ("t10-09-b-reduce-trade-001","t10-09-b-reduce-bill-001",
            "recorded-t10-09-b-reduce-020","sell","5","0.02","-0.0001",partial_at),
        ("t10-09-b-final-trade-001","t10-09-b-final-bill-001",
            "recorded-t10-09-b-final-close-020","sell","5","0.02","-0.0001",at),
    ] {
        fills.push(json!({
            "instType":"SWAP","instId":"BTC-USDT-SWAP",
            "tradeId":trade_id,"billId":bill_id,"ordId":ord_id,
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "subType":sub_type,"side":side,"posSide":"long",
            "fillPx":"1000","fillSz":quantity,"fillPnl":"0",
            "fee":fee,"feeCcy":"USDT",
            "ts":when.to_string(),"fillTime":when.to_string()
        }));
        bills.push(json!({
            "billId":bill_id,"instType":"SWAP","instId":"BTC-USDT-SWAP",
            "ordId":ord_id,"tradeId":trade_id,
            "clOrdId":"__REQUEST_CL_ORD_ID__","ccy":"USDT",
            "type":"2","subType":sub_type,
            "balChg":fee,"fee":fee,"pnl":"0","ts":when.to_string()
        }));
    }
    let mut history=Vec::new();
    for _ in 0..32 {
        for part in ["SPOT","MARGIN","SWAP","FUTURES","OPTION"] {
            history.push(RecordedGet {
                access_key:"fixture-executor-b".into(),
                path:"/api/v5/trade/fills-history".into(),
                required_query:vec![("instType".into(),part.into())],
                raw_response:if part=="SWAP" {raw(json!(fills))}
                    else {raw(json!([]))},
            });
        }
    }
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/trade/fills-history",history
    );
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/account/bills-archive",
        (0..32).map(|_| RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/account/bills-archive".into(),
            required_query:vec![],
            raw_response:raw(json!(bills)),
        }).collect(),
    );
    feed.advance_recorded_route(
        "fixture-observer-b","/api/v5/account/positions",
        (0..80).map(|_| RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],
            raw_response:raw(json!([])),
        }).collect(),
    );
    feed.advance_recorded_route(
        "fixture-observer-b","/api/v5/account/balance",
        (0..48).map(|_| RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],
            raw_response:raw(json!([{
                "totalEq":"99.9996","adjEq":"99.9996","uTime":at.to_string(),
                "details":[{"ccy":"USDT","eq":"99.9996","liab":"0",
                    "availEq":"99.9996","availBal":"99.9996",
                    "eqUsd":"99.9996","uTime":at.to_string()}]
            }])),
        }).collect(),
    );
    let mut order_reads=Vec::new();
    for _ in 0..32 {
        for (id,side,kind,qty,filled,when) in [
            ("recorded-b1-b-001","buy","limit","0.1","0.04",late_at),
            ("recorded-t10-09-b-reduce-020","sell","ioc","0.02","0.02",partial_at),
            ("recorded-t10-09-b-final-close-020","sell","ioc","0.02","0.02",at),
        ] {
            order_reads.push(RecordedGet {
                access_key:"fixture-executor-b".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
                raw_response:raw(json!([{
                    "instId":"BTC-USDT-SWAP","ordId":id,
                    "clOrdId":"__REQUEST_CL_ORD_ID__",
                    "side":side,"posSide":"long","tdMode":"cross",
                    "ordType":kind,"sz":qty,"accFillSz":filled,"avgPx":"1000",
                    "px":if kind=="ioc" {"999"}else{"1000"},
                    "state":if kind=="ioc" {"filled"}else{"canceled"},
                    "cTime":when.to_string(),"uTime":when.to_string()
                }])),
            });
        }
    }
    feed.advance_recorded_route("fixture-executor-b","/api/v5/trade/order",order_reads);
    // The exchange separately reports its attached stop canceled after flat.
    // This is a NEW independent venue GET event, not a test-owned cancel
    // mutation and not a synthetic protection receipt made by TradeService.
    let original=opening_id.clone();
    let opening_client=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| Ok(s.intent(&original)?.ok_or(
            okx_execution::StoreError::Invalid("T10_09_OPENING_ID_MISSING")
        )?.client_order_id),
    ).await.expect("B original client identity for signed stop");
    let stop_client=format!("s{}",opening_client.get(..31)
        .expect("B stop must bind exact opening client"));
    feed.advance_recorded_route(
        "fixture-executor-b","/api/v5/trade/order-algo",
        (0..24).map(|_| RecordedGet {
            access_key:"fixture-executor-b".into(),
            path:"/api/v5/trade/order-algo".into(),
            required_query:vec![("algoClOrdId".into(),stop_client.clone())],
            raw_response:raw(json!([{
                "instId":"BTC-USDT-SWAP","algoId":"900000004",
                "algoClOrdId":stop_client,"state":"canceled",
                "sz":"0.02","side":"sell","posSide":"long",
                "tdMode":"cross","ordType":"conditional",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
            }])),
        }).collect(),
    );
    // A full, exact signed orphan inventory is required even though the
    // stop-specific GET reports canceled; no empty generic fallbacks count.
    let flat_inventory_event=std::time::Instant::now();
    for _ in 0..8 {
        for kind in [
            "conditional","oco","trigger","move_order_stop","chase",
            "iceberg","twap","smart_iceberg",
        ] {
            for inst_type in ["SWAP","FUTURES"] {
                feed.expect_get(RecordedGet {
                    access_key:"fixture-observer-b".into(),
                    path:"/api/v5/trade/orders-algo-pending".into(),
                    required_query:vec![
                        ("instType".into(),inst_type.into()),
                        ("ordType".into(),kind.into()),
                        ("limit".into(),"100".into()),
                    ],
                    raw_response:raw(json!([])),
                });
            }
        }
    }
    // This is a scheduling barrier, not a terminal settlement claim:
    // wait for captured flat lots and complete signed source cursors before
    // publishing the finite final pending-inventory phase. A Close ACK/plan
    // cannot prove fills. Below, production Status must settle the Close,
    // then the SAME SQL local-close finance proof checks fill, linked bill,
    // unique fee and its durable terminal receipt. Never use the Entry-only
    // intent_fill_cost_evidence for a risk-decreasing Close.
    let until=std::time::Instant::now()+Duration::from_secs(24);
    let mut last_finance=json!({"state":"WAITING_FOR_SIGNED_FLAT"});
    loop {
        assert!(std::time::Instant::now()<until,
            "T10_09_FINAL_SIGNED_FLAT_NOT_CAPTURED:{last_finance}");
        let id=close_id.clone();
        let observed=owner.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            move |s| {
                let fills=s.cursor("fills","SWAP")?;
                let bills=s.cursor("bills","account")?;
                let lots=s.active_lots("BTC-USDT-SWAP")?;
                let closing=s.intent(&id)?.ok_or(
                    okx_execution::StoreError::Invalid(
                        "T10_09_FINAL_CLOSE_INTENT_MISSING"
                    )
                )?;
                Ok(json!({
                    "close_plan_qty":closing.plan.quantity.normalize().to_string(),
                    "close_venue_ack":closing.exchange_order_id.as_deref()
                        ==Some("recorded-t10-09-b-final-close-020"),
                    "remaining_lots":lots.len(),
                    "fill_watermark":fills.as_ref().map(|v|v.watermark),
                    "bill_watermark":bills.as_ref().map(|v|v.watermark),
                    "fills_complete":fills.as_ref().is_some_and(|v|
                        v.generation>0&&v.coverage_gaps.is_empty()&&v.cursor.is_empty()),
                    "bills_complete":bills.as_ref().is_some_and(|v|
                        v.generation>0&&v.coverage_gaps.is_empty()&&v.cursor.is_empty()),
                }))
            },
        ).await;
        match observed {
            Ok(value) if value["close_plan_qty"]=="0.02"
                && value["close_venue_ack"]==true
                && value["remaining_lots"]==0
                && value["fills_complete"]==true
                && value["bills_complete"]==true
                && value["fill_watermark"].as_i64().unwrap_or(0)>=at
                && value["bill_watermark"].as_i64().unwrap_or(0)>=at => break,
            Ok(value)=>last_finance=value,
            Err(reason)=>last_finance=json!({"sql_error":format!("{reason:?}")}),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Terminal Worker RELEASE runs a full A+B cohort census, not merely
    // the closing B account. A's original signed equity is still 100 USDT:
    // B's independent Close cannot mint or debit account A. The finite A
    // bootstrap pages have been consumed by earlier real cohort reads.
    // Publish ONE new actor-A authenticated balance observation at the
    // verified B-flat / terminal-census boundary, preserving those literal
    // A economics. Do not copy B's 99.9996 balance into A or substitute a
    // cached, undated account snapshot for fresh source admission.
    // The FULL A+B terminal census must consume A's newly signed balance.
    // The test's real periodic finance source task can otherwise exhaust
    // those finite A observations between publication and B's Status.
    // Hold the EXISTING test-only A source-cycle gate for this single public
    // release boundary. No engine/risk/census path or production concurrency
    // is disabled, and no missing source is substituted.
    // SQL has independently captured B's final 0.02 fill+bill and zero
    // lots. Preserve the finite signed final pending inventory for the
    // public B Close/Entry Status and full-cohort release only: a timer-
    // driven B source cycle must not consume it in between. The ordinary
    // TradeService and census still execute their own authenticated GETs.
    let _b_terminal_capture_lease=tokio::time::timeout(
        until.saturating_duration_since(std::time::Instant::now()),
        owner.recorded_capture_gate.write()
    ).await.expect("T10_09_B_SOURCE_GATE_DEADLINE");
    let a_owner=accounts.resolve("fixture-a").await
        .expect("T10-09 A signed cohort source owner");
    let a_terminal_capture_lease=tokio::time::timeout(
        until.saturating_duration_since(std::time::Instant::now()),
        a_owner.recorded_capture_gate.write()
    ).await.expect("T10_09_A_SOURCE_GATE_DEADLINE");
    // The A order remained unfilled while B closed, so its authenticated
    // historical fill/bill bytes do not change. The EXISTING B1 transcript
    // owner publishes a new finite observation of those same A sources at
    // this verified B-flat event; not a synthetic finance/NAV mutation.
    a_finance_source_at_flat("T10_09_B_SIGNED_FLAT_TERMINAL_CENSUS");
    let a_balance_at=chrono::Utc::now().timestamp_millis().to_string();
    let a_balance=raw(json!([{
        "totalEq":"100","adjEq":"100","uTime":a_balance_at,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100",
            "uTime":a_balance_at}]
    }]));
    let retired_a_balance=feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/balance",
        (0..8).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],
            raw_response:a_balance.clone(),
        }).collect(),
    );
    eprintln!(
        "T10_09_A_TERMINAL_COHORT_BALANCE_SOURCE_V1:account=A observed_at={} retired_prior={}",
        a_balance_at,retired_a_balance
    );
    // The same complete terminal census also reads A's STILL-LIVE
    // unrelated Open order. Its signed create ACK and earlier independent
    // order GET already established this 0.30 pending obligation; B's
    // independent Close did not fill, cancel or resize it. A new A-owned
    // pending-orders GET event reports the UNCHANGED original venue cTime
    // and uTime, not a synthetic time derived from B's release request.
    assert!(a_order_venue_at>0 && a_order_venue_at<=late_at,
        "T10-09 A signed pending order must predate B's late fill");
    let (a_client,a_state,a_qty,a_open)=a_owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |sql| {
            let i=sql.by_exchange_order_id("recorded-t09-second-001")?
                .ok_or(okx_execution::StoreError::Invalid(
                    "T10_09_A_PENDING_SOURCE_INTENT_NOT_FOUND"
                ))?;
            Ok((i.client_order_id,i.state.as_str().to_owned(),
                i.plan.quantity,i.risk_increase))
        },
    ).await.expect("T10-09 A persisted independent pending order identity");
    assert_eq!(a_state,"ACKNOWLEDGED",
        "T10-09 A unrelated pending order must not turn terminal");
    assert_eq!(a_qty,Decimal::new(3,1));
    assert!(a_open,"T10-09 A remains an independent risk-increasing intent");
    let a_order_at=a_order_venue_at.to_string();
    let a_pending=raw(json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP",
        "ordId":"recorded-t09-second-001","clOrdId":a_client,
        "side":"buy","posSide":"long","tdMode":"cross",
        "ordType":"limit","px":"1000","sz":"0.3",
        "accFillSz":"0","avgPx":"","state":"live",
        "cTime":a_order_at,"uTime":a_order_at
    }]));
    let retired_a_pending=feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/trade/orders-pending",
        (0..8).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/trade/orders-pending".into(),
            required_query:vec![("limit".into(),"100".into())],
            raw_response:a_pending.clone(),
        }).collect(),
    );
    eprintln!(
        "T10_09_A_TERMINAL_COHORT_PENDING_SOURCE_V1:account=A ord_id=recorded-t09-second-001 original_venue_at={} retired_prior={}",
        a_order_venue_at,retired_a_pending
    );
    feed.advance_immutable_signed_phase_bounded(
        "t10-09-b-reduce-filled-pending-inventory",
        "t10-09-b-final-flat-pending-inventory",
        RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/trade/orders-pending".into(),
            required_query:vec![("limit".into(),"100".into())],
            raw_response:raw(json!([])),
        },
        24,Duration::from_secs(20),
    );

    // A venue-flat proof does not by itself make the account-wide NAV
    // complete. Terminal release checks the SAME normal financial_risk()
    // predicate on both A and B. If a prior NAV is PARTIAL/stale, run the
    // existing authenticated source capture once on that account under its
    // source-cycle owner; never insert a test NAV or bypass the census.
    for (label,account) in [("A",&a_owner),("B",&owner)] {
        let prior=account.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            |sql| sql.financial_risk(chrono::Utc::now().timestamp_millis(),None),
        ).await.expect("T10-09 original financial risk SQL owner");
        if !prior.nav_complete {
            let captured=tokio::time::timeout(
                until.saturating_duration_since(std::time::Instant::now()),
                crate::source_capture::capture_account_sources(
                    &account.observer,&account.private_ws,&account.execution
                ),
            ).await.unwrap_or_else(|_|panic!(
                "T10_09_TERMINAL_NAV_CAPTURE_DEADLINE:account={label} prior_complete={} prior_as_of={}",
                prior.nav_complete,prior.source_as_of
            ));
            captured.unwrap_or_else(|reason|panic!(
                "T10_09_TERMINAL_NAV_CAPTURE_FAILED:account={label} reason={reason} prior_complete={} prior_as_of={}",
                prior.nav_complete,prior.source_as_of
            ));
        }
        let final_risk=account.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            |sql| sql.financial_risk(chrono::Utc::now().timestamp_millis(),None),
        ).await.expect("T10-09 independently captured financial risk");
        assert!(final_risk.nav_complete,
            "T10_09_TERMINAL_NAV_NOT_COMPLETE:account={label} prior_complete={} prior_as_of={} final_as_of={}",
            prior.nav_complete,prior.source_as_of,final_risk.source_as_of);
    }

    let mut last=json!(null);
    let mut first_release_blocker=json!(null);
    let mut original_status=json!(null);
    let mut close_status=json!(null);
    let mut attempts=0_u32;
    while attempts<8 && std::time::Instant::now()<until {
        attempts+=1;
        let mut deferred=false;
        for (reference,opening) in [(close_id.as_str(),false),(opening_id.as_str(),true)] {
            let result=trade.trade(
                "fixture-b",&TradeRequest::Status{reference:reference.to_owned()},None
            ).await;
            let observed=match result {
                Ok(value)=>value,
                Err(reason)=>{
                    let remaining=until.saturating_duration_since(std::time::Instant::now());
                    let delay=recorded_preview_local_defer(
                        &reason,&rate_budget,remaining
                    ).unwrap_or_else(||panic!(
                        "T10_09_FINAL_STATUS_NONLOCAL_SOURCE:{reference}:{reason}:attempt={attempts}:previous_status={last}:signed_pending_reads={}:balance_reads={}",
                        feed.immutable_signed_phase_reads(
                            "fixture-observer-b","/api/v5/trade/orders-pending",
                            "t10-09-b-final-flat-pending-inventory"
                        ),
                        feed.balance_read_probe()
                    ));
                    tokio::time::sleep(delay).await;
                    feed.advance_recorded_public_clock(raw(json!([{
                        "ts":chrono::Utc::now().timestamp_millis().to_string()
                    }])));
                    deferred=true;
                    break;
                }
            };
            assert_eq!(feed.outbound_posts.lock().unwrap().len(),9,
                "T10-09 final status cannot replay Close or issue unseen cancels");
            if opening {original_status=observed} else {close_status=observed}
        }
        if deferred {continue}
        // No protection incident occurred in this normal Cancel→Reduce→
        // Close chain. Production reports remediation=not_evaluated in
        // that case: verified_flat_no_orphans is ONLY for recovering a
        // PREVIOUSLY persisted incident. Requiring it here caused endless
        // Status polling and exhaustion of a correct finite signed source.
        let complete=close_status["data"]["intent"]["state"]=="FILLED"
            && original_status["data"]["central_release"]["state"]=="verified"
            && original_status["data"]["protection_incident"]==false
            && original_status["data"]["remaining_exposure"]=="0"
            && original_status["data"]["financial_complete"]==true;
        if first_release_blocker.is_null()
            && original_status["data"]["central_release"]["state"]!="verified" {
            first_release_blocker=json!({
                "attempt":attempts,
                "release":original_status["data"]["central_release"],
                "b_signed_pending_reads":feed.immutable_signed_phase_reads(
                    "fixture-observer-b","/api/v5/trade/orders-pending",
                    "t10-09-b-final-flat-pending-inventory"
                ),
            });
        }
        last=json!({
            "first_release_blocker":first_release_blocker,
            "attempt":attempts,
            "close_intent":close_status["data"]["intent"]["state"],
            "original_release":original_status["data"]["central_release"],
            "protection":original_status["data"]["protection_remediation"],
            "financial_error":original_status["data"]["financial_error"],
        });
        if complete {break}
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(close_status["data"]["intent"]["state"],"FILLED",
        "T10-09 signed B Close fill/bill not settled: {last}");
    assert_eq!(original_status["data"]["central_release"]["state"],"verified",
        "T10-09 original B Worker grant not released: {last}");
    assert_eq!(original_status["data"]["protection_incident"],false,
        "T10-09 independent flat must not leave protective incident");
    assert_eq!(original_status["data"]["protection_remediation"]["state"],
        "not_evaluated",
        "T10-09 ordinary flat with no incident must NOT claim incident remediation");
    assert_eq!(original_status["data"]["remaining_exposure"],"0",
        "T10-09 normal flat requires zero owned original exposure");
    assert_eq!(original_status["data"]["financial_complete"],true,
        "T10-09 flat release requires complete signed finance");
    // B's original central RELEASE has now been witnessed. Restore A's
    // periodic source capture; the scheduling gate owns NO economic state.
    drop(a_terminal_capture_lease);
    // Audit the actual raw authenticated responses used by normal readers
    // after flat. Do not issue a ninth full census merely to inspect proof:
    // that consumed a new set of finite pages AFTER release was verified.
    // All 16 exact scopes must have delivered successful empty raw pages;
    // missing/old/foreign/nonempty sources fail independently of Status.
    let orphan_inventory=feed.delivered_flat_algo_inventory_since(
        "fixture-observer-b",flat_inventory_event,
    ).expect("T10-09 delivered signed complete risk-algo inventory");
    assert_eq!(orphan_inventory.len(),16,
        "T10-09 orphan inventory must cover every required algo kind");

    let o=opening_id.clone();
    let c=close_id.clone();
    let sql=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| {
            let opening=s.intent(&o)?.ok_or(
                okx_execution::StoreError::Invalid("T10_09_TERMINAL_ORIGINAL_MISSING")
            )?;
            let closing=s.intent(&c)?.ok_or(
                okx_execution::StoreError::Invalid("T10_09_TERMINAL_CLOSE_MISSING")
            )?;
            let fills=s.cursor("fills","SWAP")?.ok_or(
                okx_execution::StoreError::Invalid("T10_09_FINAL_FILL_CURSOR_MISSING")
            )?;
            let bills=s.cursor("bills","account")?.ok_or(
                okx_execution::StoreError::Invalid("T10_09_FINAL_BILL_CURSOR_MISSING")
            )?;
            // Local Close has a different signed finance contract than
            // risk-increasing Entry. This read-only witness is the exact
            // same validation called by production local terminal release.
            let (close_qty,close_fills,close_bills,close_fee,close_proof)=
                s.local_terminal_close_evidence(
                    &c,"recorded-t10-09-b-final-close-020",
                    Decimal::new(2,2),at
                )?;
            let released=s.operation(&format!("local-terminal-release:{c}"))?
                .ok_or(okx_execution::StoreError::Invalid(
                    "T10_09_LOCAL_CLOSE_PROOF_NOT_ACKED"
                ))?;
            if released.kind!="LOCAL_TERMINAL_RELEASE"
                || released.result_ref!=close_proof
                || released.request_hash!=closing.request_hash {
                return Err(okx_execution::StoreError::Invalid(
                    "T10_09_LOCAL_CLOSE_PROOF_MISMATCH"
                ));
            }
            let lots=s.active_lots("BTC-USDT-SWAP")?;
            let receipt=s.central_receipt(&o)?.ok_or(
                okx_execution::StoreError::Invalid("T10_09_ORIGINAL_CENTRAL_RECEIPT_MISSING")
            )?;
            Ok(json!({
                "original_intent_id":opening.intent_id,
                "original_revision":opening.revision,
                "close_intent_id":closing.intent_id,
                "close_revision":closing.revision,
                "close_state":closing.state.as_str(),
                "close_quantity":closing.plan.quantity.normalize().to_string(),
                "close_risk_increase":closing.risk_increase,
                "close_signed_fill_quantity":close_qty.normalize().to_string(),
                "close_signed_fill_count":close_fills,
                "close_signed_bill_count":close_bills,
                "close_signed_fee_usdt":close_fee.abs().normalize().to_string(),
                "close_proof_digest":close_proof,
                "close_terminal_receipt_kind":released.kind,
                "remaining_lots":lots.len(),
                "fill_watermark":fills.watermark,
                "bill_watermark":bills.watermark,
                "fill_complete":fills.coverage_gaps.is_empty()&&fills.cursor.is_empty(),
                "bill_complete":bills.coverage_gaps.is_empty()&&bills.cursor.is_empty(),
                "original_central_still_held":
                    s.active_central_receipt(&o)?.is_some(),
                "reservation_key":receipt.reservation_key,
                "plan_hash":receipt.plan_hash,
                "account_key":receipt.account_key,
            }))
        }
    ).await.expect("T10-09 immutable B SQL final receipt");
    assert_eq!(sql["remaining_lots"],0,"T10-09 no position flip or hidden remnant");
    assert_eq!(sql["close_signed_fill_quantity"],"0.02");
    assert_eq!(sql["close_signed_fill_count"],1);
    assert_eq!(sql["close_signed_bill_count"],1);
    assert_eq!(sql["close_signed_fee_usdt"],"0.0001");
    assert_eq!(sql["close_risk_increase"],false);
    assert_eq!(sql["original_central_still_held"],false);
    assert_eq!(sql["fill_complete"],true);
    assert_eq!(sql["bill_complete"],true);
    assert!(sql["fill_watermark"].as_i64().unwrap_or_default()>=at);
    assert!(sql["bill_watermark"].as_i64().unwrap_or_default()>=at);
    let reservation_key=sql["reservation_key"].as_str()
        .expect("B original reservation key").to_owned();
    assert_eq!(sql["account_key"],owner.configuration.account_key.wire());
    let inspected=risk.request(RiskCommand {
        schema:RISK_COMMAND_SCHEMA_V1.into(),
        command_id:"risk_t10_09_inspect_original_released".into(),
        installation_id:"installation_test".into(),
        action:RiskCommandAction::Inspect {
            reservation_key:reservation_key.clone(),
            plan_sha256:sql["plan_hash"].as_str().expect("B original plan hash").to_owned(),
        },
    }).await.expect("T10-09 persisted Worker original reservation INSPECT");
    assert_eq!(inspected.status,RiskDecisionStatus::Observed);
    assert_eq!(inspected.reservation_key.as_deref(),Some(reservation_key.as_str()));
    assert_eq!(inspected.reservation_state,
        Some(okx_protocol::risk_authority::RiskReservationState::Released),
        "T10-09 may not infer Worker release from SQL alone");
    let mut cmd=summary("risk_t10_09_after_b_final_flat");
    cmd.installation_id="installation_test".into();
    let worker=risk.request(cmd).await.expect("T10-09 same persisted Worker");
    let budget=worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(budget.consumed_usd,"1.4007",
        "T10-09 terminal B release cannot erase cumulative campaign entry spend");
    assert_eq!(budget.reserved_usd,"7.0015",
        "T10-09 exact B remainder grant must be released, preserving other grants");
    assert_eq!(budget.remaining_usd,"1.5978",
        "release of B unused 0.6003 must restore campaign available capacity");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),9,
        "T10-09 no re-entry, duplicate reduce, amend, or extra cancel POST");
    assert_eq!(feed.unconsumed_posts(),0);
    json!({
        "schema":"okx.t10-09-native-terminal-flat/v1",
        "opening_intent_id":opening_id,
        "close":close,
        "original_status":original_status,
        "close_status":close_status,
        "native_sql":sql,
        "signed_close_at_ms":at,
        "worker_after":worker.budget,
        "worker_risk_book_sequence":worker.risk_book_sequence,
        "signed_flat_orphan_inventory_complete":orphan_inventory.len()==16,
        "signed_flat_orphan_count":0,
        "signed_flat_orphan_source_scopes":orphan_inventory,
        "worker_original_reservation_state":"RELEASED",
        "worker_original_reservation_key":reservation_key,
        "outbound_posts_before":before,
        "outbound_posts_after":feed.outbound_posts.lock().unwrap().len(),
        "status_attempts":attempts,
    })
}
