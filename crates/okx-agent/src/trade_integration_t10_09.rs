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
    feed.advance_recorded_public_clock(raw(json!([{"ts":at}])));
    let terminal = trade.trade(
        "fixture-b",
        &TradeRequest::Status { reference:original_intent_id.clone() },
        None,
    ).await.expect("T10-09 authenticated terminal B order readback");
    assert_eq!(terminal["data"]["intent"]["intent_id"], original_intent_id);
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
    let owner = accounts.resolve("fixture-b").await
        .expect("T10-09 original B Native SQL owner");
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
    let owner=accounts.resolve("fixture-b").await.expect("T10-09 B source owner");
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
    assert_eq!(status["data"]["protection_adjustment"]["state"],"ack_source_pending",
        "T10_09_SHRINK_FIRST_BLOCKER_V3:{} expected_req_id={} last_post={:?} unconsumed_posts={} last_denial={:?}",
        status["data"]["protection_adjustment"],
        reducing_client_id,
        feed.outbound_posts.lock().unwrap().last().cloned(),
        feed.unconsumed_posts(),
        feed.denied_routes.lock().unwrap().last().cloned());
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),8,
        "T10-09 sole amend-algos POST must follow the public Reduce");
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
        "protection_amend_ack":status["data"]["protection_adjustment"],
        "protection_resized":resized["data"]["protection_adjustment"],
        "outbound_posts":feed.outbound_posts.lock().unwrap().len(),
    })
}
