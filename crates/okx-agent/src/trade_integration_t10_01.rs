// T10-01: authenticated incomplete private balance must block the actual
// public Execute before any Native reservation/POST. Fresh complete signed
// pages may recover using a NEW request and exactly one Worker-held create.
async fn t10_01_partial_balance_denial_recovery(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    rate_budget: RateBudget,
    refresh_recorded_market: &impl Fn()->i64,
)->serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet,RecordedPost,RecordedPhaseBudget};
    use okx_protocol::trading::{
        OrderConstraints,PositionSide,Protection,Sizing,TradeOperation,TradeRequest
    };
    use serde_json::json;
    let wrap=|rows:serde_json::Value|serde_json::to_vec(&json!({
        "code":"0","msg":"","data":rows
    })).unwrap();
    // The prior T09 bootstrap may have finite old fee/leverage pages.
    // Seal B's real capture owner while atomically publishing this scenario's
    // new raw signed source event. No production finance is bypassed.
    let bootstrap_b=accounts.resolve("fixture-b").await
        .expect("T10-01 verified B source owner at event boundary");
    let source_boundary=bootstrap_b.recorded_capture_gate.write().await;
    // Declared before any transport reads: 90s at a two-second capture
    // cadence (45 reads) plus 19 direct Preview/Execute observations.
    // These 64 deliveries contain ONE immutable raw snapshot, not 64 pages.
    let budget=||RecordedPhaseBudget::new(64,Duration::from_secs(90));
    let empty=wrap(json!([]));
    let observed_digest=format!("{:x}",sha2::Sha256::digest(&empty));
    let route="/api/v5/account/balance";
    // An exchange response with an empty data array is INCOMPLETE, not
    // "zero margin". It must never be promoted to a valid account state.
    // B has no prior authenticated REST balance transcript in this
    // baseline. Register its FIRST finite source rather than pretending
    // that a phase already exists; a later actual source change uses advance.
    feed.expect_immutable_signed_phase("t10-01-b-incomplete",RecordedGet {
        access_key:"fixture-observer-b".into(),path:route.into(),
        required_query:vec![],raw_response:empty.clone(),
    });
    let initial_ts=chrono::Utc::now().timestamp_millis().to_string();
    let initial_fee=wrap(json!([{
        "instType":"SWAP","level":"Lv1","ts":initial_ts,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    // One immutable signed fee snapshot per event, not 12 arbitrary GET pages.
    let retired_fee_pages=feed.supersede_queued_signed_source_at_event(
        "t10-01-negative-source-arrived", "t10-01-b-fee-initial", RecordedGet {
        access_key:"fixture-observer-b".into(),
        path:"/api/v5/account/trade-fee".into(),
        required_query:vec![
            ("instType".into(),"SWAP".into()),
            ("instFamily".into(),"BTC-USDT".into()),
        ],raw_response:initial_fee.clone()
    });
    assert!(retired_fee_pages>0, "B fee must supersede a real old signed source");
    let leverage=wrap(json!([
        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
    ]));
    // Unchanged leverage source is visible identically to concurrent readers.
    let retired_leverage_pages=feed.supersede_queued_signed_source_at_event(
        "t10-01-negative-source-arrived", "t10-01-b-leverage-stable", RecordedGet {
        access_key:"fixture-observer-b".into(),
        path:"/api/v5/account/leverage-info".into(),
        required_query:vec![
            ("instId".into(),"BTC-USDT-SWAP".into()),
            ("mgnMode".into(),"cross".into()),
        ],raw_response:leverage.clone()
    });
    assert!(retired_leverage_pages>0,
        "B leverage must supersede a real old signed source");
    drop(source_boundary);
    let request=|id:&str,expires_in_ms:u64|TradeRequest::Execute {
        idempotency_key:id.into(),operation:TradeOperation::Open,
        strategy_instance:"manual".into(),instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,sizing:Sizing::Contracts{contracts:"0.1".into()},
        order:OrderConstraints{
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms,
    };
    let bad=request("t10_01_incomplete_private_balance_0001",6000);
    assert!(bad.valid());
    let mut before_cmd=summary("risk_t10_01_before_source_denial");
    before_cmd.installation_id="installation_test".into();
    let before=risk.request(before_cmd).await.expect("T10-01 Worker before");
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,4);
    let until=std::time::Instant::now()+Duration::from_secs(14);
    let denied=loop {
        let answer=tokio::time::timeout_at(until.into(),
            trade.trade("fixture-b",&bad,None)
        ).await.expect("T10-01 source-denial bound");
        match answer {
            Err(reason) => {
                if let Some(defer)=recorded_preview_local_defer(
                    &reason,&rate_budget,
                    until.saturating_duration_since(std::time::Instant::now())
                ) {
                    tokio::time::timeout_at(until.into(),tokio::time::sleep(defer))
                        .await.expect("T10-01 only retry typed LOCAL no-send defer");
                    continue;
                }
                break reason;
            },
            Ok(reply)=> {
                let origin=reply["data"]["rejection_origin"].as_str().unwrap_or("UNKNOWN");
                assert_ne!(reply["status"],"accepted",
                    "missing authenticated balance cannot produce success");
                break format!("LOCAL_INCOMPLETE_BALANCE:{origin}");
            }
        }
    };
    let after_incomplete=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(after_incomplete,posts_before,
        "no venue POST can be authorized without complete private balance");
    let owner=accounts.resolve("fixture-b").await.expect("T10-01 real B SQL");
    let bad_sql=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        |s|s.by_idempotency("t10_01_incomplete_private_balance_0001")
    ).await.expect("T10-01 B account-store query");
    assert!(bad_sql.as_ref().is_none_or(|i|
        !matches!(i.state.as_str(),"POSTING"|"ACKNOWLEDGED"|"FILLED")
    ));
    let mut denied_cmd=summary("risk_t10_01_after_incomplete");
    denied_cmd.installation_id="installation_test".into();
    let unchanged=risk.request(denied_cmd).await.expect("T10-01 Worker after denial");
    let b0=before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    let b1=unchanged.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(b0.consumed_usd,b1.consumed_usd);
    assert_eq!(b0.reserved_usd,b1.reserved_usd);
    assert_eq!(b0.remaining_usd,b1.remaining_usd);

    // Source recovery publishes raw signed venue bytes, not a test-owned
    // AccountSnapshot or a bypass of the production observer parser.
    // The incomplete-source refusal can consume a bounded real-time
    // deadline. The recovery uses a newly observed signed exchange stamp,
    // never a timestamp copied from before the failed attempt.
    let stamp=chrono::Utc::now().timestamp_millis().to_string();
    let fresh=wrap(json!([{
        "totalEq":"100","adjEq":"100","uTime":stamp,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":stamp}]
    }]));
    let fresh_digest=format!("{:x}",sha2::Sha256::digest(&fresh));
    let observed_incomplete_reads=feed.advance_immutable_signed_phase(
        "t10-01-b-incomplete","t10-01-b-fresh",RecordedGet {
            access_key:"fixture-observer-b".into(),path:route.into(),
            required_query:vec![],raw_response:fresh.clone(),
        });
    assert!(observed_incomplete_reads>0,
        "negative TradeService source must read the actual incomplete signed phase");
    let fee=wrap(json!([{
        "instType":"SWAP","level":"Lv1","ts":stamp,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    feed.advance_immutable_signed_phase(
        "t10-01-b-fee-initial","t10-01-b-fee-recovered",RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:fee.clone()
        });
    // Source recovery and entry-admission are TWO distinct causes.
    // Wait for BOTH ordinary production finance capture owners to publish
    // fresh SQL cursor/NAV results from the just-installed signed phase;
    // then hold the *next* periodic capture tick out of the entry's way.
    // Never write an account snapshot, finance frontier or Worker grant here.
    let owner_a=accounts.resolve("fixture-a").await
        .expect("T10-01 verified A source owner");
    let owner_b=accounts.resolve("fixture-b").await
        .expect("T10-01 verified B source owner");
    let published_after=chrono::Utc::now().timestamp_millis();
    tokio::time::timeout(Duration::from_secs(17),async{
        loop {
            let a=owner_a.source_capture_health.read().await.last_success_ms;
            let b=owner_b.source_capture_health.read().await.last_success_ms;
            if a.is_some_and(|t|t>=published_after)
                &&b.is_some_and(|t|t>=published_after) {break;}
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("T10-01 both SQL source owners must capture signed recovery");
    let _capture_a=owner_a.recorded_capture_gate.write().await;
    let _capture_b=owner_b.recorded_capture_gate.write().await;
    assert!(owner_a.source_capture_health.read().await.last_success_ms
        .is_some_and(|t|t>=published_after));
    assert!(owner_b.source_capture_health.read().await.last_success_ms
        .is_some_and(|t|t>=published_after));
    // After the real capture owners have completed, publish a NEW raw
    // exchange clock reply and connected market WS frames. Publishing
    // either BEFORE waiting can make the later Execute's clock/skew proof
    // stale: the production safety gate must reject that, not the test.
    let fresh_market_received_ms=refresh_recorded_market();
    let signed_clock_received_ms=chrono::Utc::now().timestamp_millis();
    assert!(signed_clock_received_ms>=fresh_market_received_ms);
    feed.advance_recorded_public_clock(
        wrap(json!([{"ts":signed_clock_received_ms.to_string()}]))
    );
    // The recovered attempt legitimately reads all Real account scopes under
    // the unchanged authenticated 5-per-2s private budget. Six seconds was
    // shorter than the bounded two-account source census; the witnessed
    // LOCAL no-send rejection was therefore a fixture deadline, not a missing
    // SQL intent. Preserve the 6s negative case and use a single 30s positive
    // Execute (never repeated) with complete fresh signed source evidence.
    let positive=request("t10_01_recovered_fresh_private_balance_0001",30_000);
    assert!(positive.valid());
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-b".into(),path:"/api/v5/trade/order".into(),
        expected_body:json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit",
            "sz":"0.1","px":"1000",
            "attachAlgoOrds":[{
                "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900",
                "slTriggerPxType":"mark","slOrdPx":"-1"
            }]
        }),
        raw_response:br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t10-01-b-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
    });
    for _ in 0..8 {
        feed.expect_get(RecordedGet{
            access_key:"fixture-executor-b".into(),path:"/api/v5/trade/order".into(),
            required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
            raw_response:wrap(json!([{
                "instId":"BTC-USDT-SWAP","ordId":"recorded-t10-01-b-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","tdMode":"cross",
                "ordType":"limit","px":"1000","sz":"0.1",
                "accFillSz":"0","avgPx":"","state":"live",
                "cTime":stamp,"uTime":stamp
            }]))
        });
    }
    let accepted=tokio::time::timeout(Duration::from_secs(35),
        trade.trade("fixture-b",&positive,None)
    ).await.expect("T10-01 fresh recovery deadline")
      .expect("T10-01 real Worker/Native source recovery must execute");
    // Worker/SQL and the authenticated venue ACK are separate from the
    // immediately available public Status. An incomplete local readback
    // after one acknowledged POST produces UNKNOWN/no-resubmit; never
    // retry a possibly sent Execute to manufacture an accepted wrapper.

    // A successful transport response may still be a LOCAL no-send REJECTED
    // outcome. Report its native origin/reason rather than masking the actual
    // first failure as a missing intent ID; never retry this Execute.
    if accepted["data"]["state"] == "REJECTED" {
        panic!(
            "T10_01_FIRST_RECOVERY_DENIAL:reply={accepted}:posts={}:denied_routes={:?}:last_throttle={:?}",
            feed.outbound_posts.lock().unwrap().len(),
            feed.denied_routes.lock().unwrap().iter().rev().take(8).cloned().collect::<Vec<_>>(),
            rate_budget.snapshot().last_throttle
        );
    }
    let id=accepted["data"]["intent"]["intent_id"].as_str()
        .or_else(||accepted["data"]["intent_id"].as_str())
        .or_else(||accepted["intent_id"].as_str())
        .unwrap_or_else(||panic!(
            "T10_01_RECOVERY_NO_DURABLE_ID:status={} reason={} state={} posts={}",
            accepted["status"],accepted["reason_code"],
            accepted["data"]["state"],feed.outbound_posts.lock().unwrap().len()
        )).to_owned();
    assert!(
        ["completed","partial","unknown"].contains(&accepted["status"].as_str().unwrap_or("")),
        "T10-01 public recovery requires typed outcome: {accepted}"
    );
    if accepted["status"]=="partial" {
        assert_eq!(accepted["quality"]["execution"],"authenticated_readback");
        assert_eq!(accepted["quality"]["financial"],"incomplete");
    }
    if accepted["status"]=="unknown" {
        assert_eq!(accepted["reason_code"],"ORDER_READBACK_INCOMPLETE_NO_RESUBMIT",
            "T10-01 cannot accept an unverified or possibly unacknowledged POST");
    }
    let sql_id=id.clone();
    let (intent,reserve)=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s|{
            let i=s.intent(&sql_id)?.ok_or(
                okx_execution::StoreError::Invalid("T10_01_RECOVERED_INTENT_ABSENT")
            )?;
            let r=s.active_central_receipt(&sql_id)?;
            Ok((i,r))
        }
    ).await.expect("T10-01 recovered SQL/Worker receipt");
    assert_eq!(intent.state.as_str(),"ACKNOWLEDGED");
    assert_eq!(intent.exchange_order_id.as_deref(),Some("recorded-t10-01-b-001"));
    assert!(reserve.is_some(),"T10-01 recovered B has actual central reserve");
    let posts_after=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_after,posts_before+1);
    let mut risk_cmd=summary("risk_t10_01_after_fresh_recovery");
    risk_cmd.installation_id="installation_test".into();
    let worker=risk.request(risk_cmd).await.expect("T10-01 persistent Worker");
    let entry=worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(entry.reserved_usd,"8.002");
    assert_eq!(entry.consumed_usd,"1.0005");
    json!({
        "schema":"okx.t10-01-native-private-source-recovery/v1",
        "account_uid":owner.configuration.account_key.verified_uid,
        "partial_source_digest":observed_digest,
        "fresh_source_digest":fresh_digest,
        "signed_source_events":feed.immutable_phase_trace.lock().unwrap().clone(),
        "retired_baseline_fee_pages":retired_fee_pages,
        "retired_baseline_leverage_pages":retired_leverage_pages,
        "incomplete_denial":denied,
        "sql_created_on_denial":bad_sql.is_some(),
        "posts_before":posts_before,
        "posts_on_incomplete":after_incomplete,
        "posts_after_recovery":posts_after,
        "fresh_intent_id":id,
        "fresh_sql_state":intent.state.as_str(),
        "fresh_sql_revision":intent.revision,
        "fresh_central_reserve":reserve.is_some(),
        "fresh_order_id":intent.exchange_order_id,
        "worker_budget_before":before.budget,
        "worker_budget_after":worker.budget,
        "worker_sequence":worker.risk_book_sequence,
    })
}
