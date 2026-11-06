// T10-05: two separately isolated production public Native Execute cohorts
// share the SAME Worker/SQL/venue code. One loses the create ACK AFTER send;
// the other receives an exact client-ID bound explicit venue rejection.
async fn t10_05_create_outcome(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    rate_budget: RateBudget,
    lost_ack: bool,
)->serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet,RecordedPost};
    use okx_protocol::trading::{
        OrderConstraints,PositionSide,Protection,Sizing,TradeOperation,TradeRequest
    };
    use serde_json::json;
    let mode=if lost_ack{"lost_ack"}else{"venue_rejected"};
    let key=if lost_ack{
        "t10_05_native_lost_ack_0001"
    }else{
        "t10_05_native_explicit_venue_reject_0001"
    };
    let order_id="recorded-t10-05-b-001";
    let stamp=chrono::Utc::now().timestamp_millis().to_string();
    let wrap=|rows:serde_json::Value|serde_json::to_vec(&json!({
        "code":"0","msg":"","data":rows
    })).unwrap();
    let balance=wrap(json!([{
        "totalEq":"100","adjEq":"100","uTime":stamp,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":stamp}]
    }]));
    // In this independent Native cohort B has no prior balance route.
    // Register one bounded immutable raw signed source rather than pretend
    // there was an earlier phase to retire (no finance/SQL test writes).
    feed.expect_immutable_signed_phase("t10-05-b-initial-balance",RecordedGet {
        access_key:"fixture-observer-b".into(),
        path:"/api/v5/account/balance".into(),
        required_query:vec![],raw_response:balance,
    });
    let fee=wrap(json!([{
        "instType":"SWAP","level":"Lv1","ts":stamp,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/trade-fee",
        (0..10).map(|_|RecordedGet{
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:fee.clone()
        }).collect()
    );
    feed.advance_recorded_public_clock(wrap(json!([{"ts":stamp}])));
    // Signed readback is tied by RecordedAccountHttp to the actual same
    // (account, clOrdId, ordId) ACK that was intentionally lost in delivery.
    if lost_ack{
        for _ in 0..12 {
            feed.expect_get(RecordedGet {
                access_key:"fixture-executor-b".into(),
                path:"/api/v5/trade/order".into(),
                required_query:vec![
                    ("instId".into(),"BTC-USDT-SWAP".into())
                ],
                raw_response:wrap(json!([{
                    "instId":"BTC-USDT-SWAP","ordId":order_id,
                    "clOrdId":"__REQUEST_CL_ORD_ID__",
                    "side":"buy","posSide":"long","tdMode":"cross",
                    "ordType":"limit","px":"1000","sz":"0.1",
                    "accFillSz":"0","avgPx":"","state":"live",
                    "cTime":stamp,"uTime":stamp
                }]))
            });
        }
    }
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/order".into(),
        expected_body:json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit",
            "sz":"0.1","px":"1000",
            "attachAlgoOrds":[{
                "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark",
                "slTriggerPx":"900","slTriggerPxType":"mark","slOrdPx":"-1"
            }]
        }),
        raw_response:if lost_ack {
            br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t10-05-b-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec()
        } else {
            br#"{"code":"0","msg":"","data":[{"ordId":"","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"51001","sMsg":"explicit venue rejection"}]}"#.to_vec()
        },
    });
    // The A order has already been authenticated and acknowledged. The
    // next event belongs to B, not another periodic A finance/source cycle.
    // A's real capture/Status owner continues during B's first create.
    // It reads the authenticated ACK-bound immutable order snapshot;
    // serializing A out of this test would hide the original race.
    let before_posts=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(before_posts,4);
    let mut before_risk=summary("risk_t10_05_before_b_create");
    before_risk.installation_id="installation_test".into();
    let budget_before=risk.request(before_risk).await.expect("T10-05 Worker prior to POST");
    assert_eq!(budget_before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
        "7.0015");
    // This positive Execute is a new authenticated fee/time observation
    // after the earlier Worker budget inspection. Retire only the exact
    // predecessor route/query/raw bytes. Do not alter tariff, risk limits,
    // source age policy or the venue POST itself.
    let execute_stamp=chrono::Utc::now().timestamp_millis().to_string();
    let observed_fee=wrap(json!([{
        "instType":"SWAP","level":"Lv1","ts":execute_stamp,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    let retired=feed.supersede_queued_signed_source_at_event(
        "t10-05-positive-execute","t10-05-b-fee-at-execute",
        RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],
            raw_response:observed_fee,
        }
    );
    assert!(retired>0,"T10-05 needs prior signed B fee source");
    feed.advance_recorded_public_clock(wrap(json!([{"ts":execute_stamp}])));
    // T10-05 enters after the authenticated B leverage observation. Repeated
    // risk-plan reads of the same unchanged signed leverage are observations
    // of one source phase, not new leverage events or additional GET pages.
    // Retire only identical old actor/route/query/raw bytes at this boundary.
    let retired_leverage = feed.supersede_queued_signed_source_at_event(
        "t10-05-after-existing-b-source",
        "t10-05-b-unchanged-leverage",
        RecordedGet {
            access_key: "fixture-observer-b".into(),
            path: "/api/v5/account/leverage-info".into(),
            required_query: vec![
                ("instId".into(), "BTC-USDT-SWAP".into()),
                ("mgnMode".into(), "cross".into()),
            ],
            raw_response: wrap(json!([
                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
                {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
            ])),
        },
    );
    assert!(
        retired_leverage > 0,
        "T10-05 must have an authentic prior B leverage source to hand off"
    );
    if lost_ack {
        feed.arm_lost_next_create_ack();
    }
    // The authenticated local config limiter is a real 5/2s user budget.
    // A LOCAL, durable NOT_STARTED result cannot exercise an ACK-loss fault.
    // Re-enter with a NEW key only after independently proving SQL no-send,
    // no active central receipt and the original Worker reservation released.
    // This is bounded by the original 18s public deadline, not a new TTL,
    // GET page, POST retry, or exemption from the concurrent A capture owner.
    let make=|key:&str|TradeRequest::Execute {
        idempotency_key:key.into(),operation:TradeOperation::Open,
        strategy_instance:"manual".into(),instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,sizing:Sizing::Contracts{contracts:"0.10".into()},
        order:OrderConstraints{
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    let owner=accounts.resolve("fixture-b").await.expect("T10-05 B SQL");
    let admission_deadline=std::time::Instant::now()+Duration::from_secs(18);
    let mut local_nosend_attempts=0usize;
    let mut fee_phase = "t10-05-b-fee-at-execute".to_owned();
    let (key,request,first)=loop {
        let attempt_key=if local_nosend_attempts==0 {
            key.to_owned()
        }else{
            format!("{key}_after_proven_local_nosend_{local_nosend_attempts}")
        };
        let request=make(&attempt_key);
        assert!(request.valid());
        let first=tokio::time::timeout_at(admission_deadline.into(),
            trade.trade("fixture-b",&request,None)
        ).await.expect("T10-05 original 18s public request deadline")
          .expect("T10-05 public Execute must produce a typed outcome");
        let posts=feed.outbound_posts.lock().unwrap().len();
        if posts==before_posts+1 {
            break (attempt_key,request,first);
        }
        assert_eq!(posts,before_posts,
            "T10-05 unexpected POST count before expected first create");
        assert_eq!(first["data"]["state"],"REJECTED",
            "only a final locally rejected no-send key may be replaced: {first}");
        assert_eq!(first["data"]["rejection_origin"],"LOCAL");
        assert_eq!(first["data"]["send_state"],"NOT_STARTED");
        let release=first["data"]["risk_release_state"].as_str()
            .expect("T10-05 explicit no-send release state");
        assert!(matches!(release,"RELEASED"|"NOT_RESERVED"),
            "no retry of unreleased or possibly submitted risk: {first}");
        let checked_key=attempt_key.clone();
        let (state,active,unsent_receipt,never_reserved,exchange_id)=
            owner.execution.store.call(
                std::time::Instant::now()+Duration::from_secs(3),
                move |sql| {
                    let i=sql.by_idempotency(&checked_key)?.ok_or(
                        okx_execution::StoreError::Invalid(
                            "T10_05_LOCAL_NOSEND_SQL_INTENT_MISSING"
                        )
                    )?;
                    Ok((i.state.as_str().to_owned(),
                        sql.active_central_receipt(&i.intent_id)?.is_some(),
                        sql.unsent_release_receipt(&i.intent_id)?.is_some(),
                        sql.unsent_before_reserve(&i.intent_id)?,
                        i.exchange_order_id))
                }
            ).await.expect("T10-05 durable no-send proof before new key");
        assert_eq!(state,"REJECTED");
        assert!(!active,"T10-05 cannot re-enter with active risk");
        assert!(exchange_id.is_none(),"T10-05 cannot replace an ACKed order");
        if release=="RELEASED" {
            assert!(unsent_receipt,"T10-05 real Worker RELEASE_UNSENT proof missing");
        }else{
            assert!(never_reserved,"T10-05 no-reserve proof missing");
        }
        let mut probe=summary("risk_t10_05_after_local_nosend");
        probe.installation_id="installation_test".into();
        let unchanged=risk.request(probe).await.expect("T10-05 actual Worker no-send budget");
        assert_eq!(unchanged.budget.as_ref().unwrap()
            .entry_budget.as_ref().unwrap().reserved_usd,"7.0015");
        let remaining=admission_deadline.saturating_duration_since(
            std::time::Instant::now()
        );
        let reason=first["data"]["primary_reason"].as_str().unwrap_or("");
        let delay=recorded_preview_local_defer(reason,&rate_budget,remaining)
            .unwrap_or_else(||panic!(
                "T10-05 no-send reason is not an authenticated LOCAL private GET defer: {first:?} rate={:?}",
                rate_budget.snapshot().last_throttle
            ));
        local_nosend_attempts+=1;
        assert!(local_nosend_attempts<=2,
            "T10-05 stopped after two independently proved LOCAL no-send keys");
        tokio::time::timeout_at(admission_deadline.into(),
            tokio::time::sleep(delay)
        ).await.expect("T10-05 original deadline for local no-send admission");
        // A completed, durably unsent intent is a causal boundary for the
        // next NEW key. A second authenticated fee observation has its own
        // exchange timestamp; repeating the first signed source would be
        // stale after real 5/2s local admission waits. Do not generate bytes
        // inside the GET handler or change the production freshness gate.
        let next_stamp=chrono::Utc::now().timestamp_millis().to_string();
        let next_fee=wrap(json!([{
            "instType":"SWAP","level":"Lv1","ts":next_stamp,
            "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
        }]));
        let next_phase=format!("t10-05-b-fee-after-local-nosend-{local_nosend_attempts}");
        feed.advance_immutable_signed_phase(
            &fee_phase, &next_phase,
            RecordedGet {
                access_key:"fixture-observer-b".into(),
                path:"/api/v5/account/trade-fee".into(),
                required_query:vec![
                    ("instType".into(),"SWAP".into()),
                    ("instFamily".into(),"BTC-USDT".into()),
                ],
                raw_response:next_fee,
            }
        );
        fee_phase=next_phase;
        feed.advance_recorded_public_clock(wrap(json!([{"ts":next_stamp}])));
    };
    let first_status=first["status"].as_str().unwrap_or("MISSING_STATUS").to_owned();
    let read=|| {
        let owner=owner.clone();
        let key=key.clone();
        async move {
            owner.execution.store.call(
                std::time::Instant::now()+Duration::from_secs(3),
                move |sql|{
                    let i=sql.by_idempotency(&key)?.ok_or(
                        okx_execution::StoreError::Invalid("T10_05_INTENT_ABSENT")
                    )?;
                    Ok((i.intent_id,i.state.as_str().to_owned(),i.revision,
                        i.client_order_id,i.exchange_order_id))
                }
            ).await
        }
    };
    let (intent_id,sql_initial_state,initial_revision,client_id,initial_order)=
        read().await.expect("T10-05 SQL after POST");
    assert!(initial_revision>0);
    let posts_after_first=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_after_first,before_posts+1,
        "T10-05 first CREATE absent: rate_throttle={:?} first_sql_state={sql_initial_state} public={first} signed_denials={:?} recent_signed_gets={:?}",
        rate_budget.snapshot().last_throttle,
        feed.denied_routes.lock().unwrap().iter().rev().take(4).cloned().collect::<Vec<_>>(),
        feed.requests.lock().unwrap().iter().rev().filter(|event|
            event.contains("fixture-observer-b:") || event.contains("fixture-executor-b:")
        ).take(8).cloned().collect::<Vec<_>>()
    );
    let (final_state,replayed_id,venue_order_id,sql_revision)=if lost_ack {
        assert!(feed.lost_create_ack_was_consumed(),
            "T10-05 fixture must have dropped exactly one post-send ACK");
        assert!(matches!(sql_initial_state.as_str(),"POSTING"|"UNKNOWN"),
            "the lost ACK must not fabricate an accepted/rejected venue status");
        assert!(initial_order.is_none());
        let repeated=trade.trade("fixture-b",&request,None).await
            .expect("T10-05 same-key replay must return a reference");
        let repeated_id=repeated["intent_id"].as_str()
            .or_else(||repeated["data"]["intent"]["intent_id"].as_str())
            .expect("T10-05 replay identity").to_owned();
        assert_eq!(repeated_id,intent_id);
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_after_first);
        for _ in 0..4 {
            let status=trade.trade("fixture-b",
                &TradeRequest::Status{reference:intent_id.clone()},None
            ).await.expect("T10-05 public Status readback");
            if status["data"]["intent"]["state"]=="ACKNOWLEDGED"{break;}
        }
        let (_id,state,revision,_client,order)=read().await
            .expect("T10-05 Native SQL readback after authenticated venue GET");
        assert_eq!(state,"ACKNOWLEDGED",
            "only exact venue order readback may resolve a lost ACK");
        assert_eq!(order.as_deref(),Some(order_id));
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_after_first);
        (state,repeated_id,order,revision)
    } else {
        assert_eq!(sql_initial_state,"REJECTED",
            "explicit signed venue reject must never be UNKNOWN");
        assert!(initial_order.is_none());
        assert_eq!(first["data"]["state"],"REJECTED");
        let repeated=trade.trade("fixture-b",&request,None).await
            .expect("T10-05 explicit rejection same-key replay");
        let repeated_id=repeated["intent_id"].as_str()
            .or_else(||repeated["data"]["intent_id"].as_str())
            .or_else(||repeated["data"]["intent"]["intent_id"].as_str())
            .expect("T10-05 replay rejected identity").to_owned();
        assert_eq!(repeated_id,intent_id);
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_after_first);
        (sql_initial_state.clone(),repeated_id,None,initial_revision)
    };
    assert_eq!(replayed_id,intent_id);
    let mut after_risk=summary("risk_t10_05_after_b_create");
    after_risk.installation_id="installation_test".into();
    let worker=risk.request(after_risk).await.expect("T10-05 persistent Worker after");
    assert_eq!(worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().consumed_usd,
        "1.0005","an ACK outcome never invents entry fills or fees");
    serde_json::json!({
        "schema":"okx.t10-05-native-post-outcome/v1",
        "mode":mode,
        "account_uid":owner.configuration.account_key.verified_uid,
        "intent_id":intent_id,"client_order_id":client_id,
        "first_public_status":first_status,
        "initial_state":sql_initial_state,
        "final_state":final_state,
        "sql_revision":sql_revision,
        "replay_intent_id":replayed_id,
        "venue_order_id":venue_order_id,
        "posts_before":before_posts,"posts_after":posts_after_first,
        "outbound_posts_after_replay":feed.outbound_posts.lock().unwrap().len(),
        "worker_budget_after":worker.budget,
        "worker_sequence":worker.risk_book_sequence,
        "locally_rejected_nosend_keys":local_nosend_attempts,
    })
}
