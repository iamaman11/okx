// T10-07: one independent production Native/SQL/Worker cohort.
//
// The recorded boundary owns ONLY raw authenticated HTTP and public WS
// observations. Two verified SQL account owners, public TradeService, and a
// single persisted Worker RiskBook decide every dollar and state transition.
// The test cannot create a reservation or mint a SQL RELEASE receipt.
async fn t10_07_native_one_campaign(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    public_ws: okx_runtime::PublicWsHandle,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    _rate_budget: RateBudget,
    refresh_recorded_market: &impl Fn()->i64,
) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet, RecordedPost};
    use okx_protocol::trading::{
        OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
    };
    use serde_json::json;
    use std::sync::atomic::Ordering::SeqCst;

    // A raw WS publisher queues frames; production PublicWsCoordinator
    // processes them asynchronously. Never treat "published" as "FRESH".
    // Observe the actual connected coordinator's updated rules and risk
    // snapshot before asking the unchanged TradeService for admission.
    let wait_market=|event_ms:i64| {
        let ws=public_ws.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(4),async {
                loop {
                    let rules=ws.instrument_rules("BTC-USDT-SWAP").await
                        .expect("T10-07 production instrument rules");
                    let observed=chrono::DateTime::parse_from_rfc3339(
                        &rules.source_received_at).unwrap().timestamp_millis();
                    let ready=ws.risk_fresh_snapshot(
                        "BTC-USDT-SWAP",
                        chrono::Utc::now().timestamp_millis() as u64,
                        5_000,5_000,chrono::Utc::now().to_rfc3339()
                    ).await.is_ok();
                    if observed>=event_ms && ready { break; }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await.expect("T10_07_MARKET_EVENT_NOT_YET_ACCEPTED_BY_PRODUCTION_WS");
        }
    };
    let envelope=|rows:serde_json::Value|serde_json::to_vec(
        &json!({"code":"0","msg":"","data":rows})
    ).unwrap();
    let at=chrono::Utc::now().timestamp_millis().to_string();
    let fee=|value:&str, stamp:&str|envelope(json!([{
        "instType":"SWAP","level":"Lv1","ts":stamp,
        "feeGroup":[{"groupId":"4","maker":value,"taker":value}]
    }]));
    // Zero is a legitimate signed fee tier in this explicitly recorded
    // campaign. It gives an exact $4 upper entry bound for 0.4 contracts
    // at $1000 with the venue instrument's real 0.01 multiplier. The quote
    // and reservation amount MUST be computed by production, never injected.
    let leverage=envelope(json!([
        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
    ]));
    for label in ["a","b"] {
        let actor=format!("fixture-observer-{label}");
        let before=feed.supersede_queued_signed_source_at_event(
            "t10-07-new-campaign-source", &format!("t10-07-{label}-fee"),
            RecordedGet {
                access_key:actor.clone(),
                path:"/api/v5/account/trade-fee".into(),
                required_query:vec![
                    ("instType".into(),"SWAP".into()),
                    ("instFamily".into(),"BTC-USDT".into()),
                ],raw_response:fee("0",&at),
            }
        );
        assert!(before>0, "T10-07 fee handoff must retire actual old observed pages");
        let old_leverage=feed.supersede_queued_signed_source_at_event(
            "t10-07-new-campaign-source", &format!("t10-07-{label}-leverage"),
            RecordedGet {
                access_key:actor.clone(),
                path:"/api/v5/account/leverage-info".into(),
                required_query:vec![
                    ("instId".into(),"BTC-USDT-SWAP".into()),
                    ("mgnMode".into(),"cross".into()),
                ],raw_response:leverage.clone(),
            }
        );
        assert!(old_leverage>0);
        let balance=envelope(json!([{
            "totalEq":"100","adjEq":"100","uTime":at,
            "details":[{"ccy":"USDT","eq":"100","liab":"0",
              "availEq":"100","availBal":"100","eqUsd":"100","uTime":at}]
        }]));
        feed.expect_immutable_signed_phase(&format!("t10-07-{label}-balance"),
            RecordedGet {
                access_key:actor,
                path:"/api/v5/account/balance".into(),
                required_query:vec![],raw_response:balance,
            });
    }
    let fresh_market=refresh_recorded_market();
    wait_market(fresh_market).await;
    let clock=chrono::Utc::now().timestamp_millis();
    assert!(clock>=fresh_market);
    feed.advance_recorded_public_clock(envelope(json!([{"ts":clock.to_string()}])));
    let risk_summary=|id:&str| {
        let mut command=summary(id);
        command.installation_id="installation_test".into();
        command
    };
    let initial=risk.request(risk_summary("risk_t10_07_initial")).await.unwrap();
    let initial_budget=initial.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(initial_budget.limit_usd,"10");
    assert_eq!(initial_budget.reserved_usd,"0");
    assert_eq!(initial_budget.consumed_usd,"0");
    assert!(feed.outbound_posts.lock().unwrap().is_empty());

    let make=|id:&str| TradeRequest::Execute {
        idempotency_key:id.into(), operation:TradeOperation::Open,
        strategy_instance:"manual".into(),instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:"0.4".into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    const A_KEY:&str="t10_07_a_original_four_usd_0001";
    const B_KEY:&str="t10_07_b_original_four_usd_0001";
    const THIRD_KEY:&str="t10_07_a_third_four_usd_0002";
    let first=make(A_KEY);assert!(first.valid());
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/order".into(),
        expected_body:json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit",
            "sz":"0.4","px":"1000",
            "attachAlgoOrds":[{
                "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900",
                "slTriggerPxType":"mark","slOrdPx":"-1",
            }],
        }),
        raw_response:br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t10-07-a-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
    });
    feed.expect_get(RecordedGet {
        access_key:"fixture-executor-a".into(),
        path:"/api/v5/trade/order".into(),
        required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
        raw_response:envelope(json!([{
            "instId":"BTC-USDT-SWAP",
            "ordId":"recorded-t10-07-a-001",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","tdMode":"cross",
            "ordType":"limit","px":"1000","sz":"0.4",
            "accFillSz":"0","avgPx":"","state":"live","cTime":at,"uTime":at,
        }])),
    });

    // Stage A's genuine Worker HELD and SQL receipt, but keep its ORIGINAL
    // public Execute pending. Stage B the same way before the third begins.
    // This is a real three-way Native overlap, not a sequential B1 cohort.
    crate::trade_service::T10_07_A_SQL_RESERVE_PERSISTED.store(false,SeqCst);
    crate::trade_service::T10_07_RELEASE_A_SQL_RESERVE_HOLD.store(false,SeqCst);
    crate::trade_service::T10_07_HOLD_A_SQL_RESERVE.store(true,SeqCst);
    let first_pending=tokio::spawn({
        let trade=trade.clone();
        async move{trade.trade("fixture-a",&first,None).await}
    });
    tokio::time::timeout(Duration::from_secs(12),async {
        while !crate::trade_service::T10_07_A_SQL_RESERVE_PERSISTED.load(SeqCst) {
            assert!(!first_pending.is_finished(),
                "T10-07 A exited before original SQL-held Worker receipt");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("T10-07 A authentic SQL HELD deadline");
    let owner_a=accounts.resolve("fixture-a").await.unwrap();
    let owner_b=accounts.resolve("fixture-b").await.unwrap();
    let a_held=owner_a.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),|s|{
            let intent=s.by_idempotency(A_KEY)?
                .ok_or(okx_execution::StoreError::Invalid("T10_07_A_HELD_SQL_MISSING"))?;
            let receipt=s.central_receipt(&intent.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid("T10_07_A_HELD_RECEIPT_MISSING"))?;
            Ok((intent.state.as_str().to_owned(),intent.intent_id,receipt))
        }
    ).await.expect("T10-07 A SQL HELD readback");
    assert_eq!(a_held.0,"RESERVED");
    assert_eq!(a_held.1,a_held.2.intent_id);
    assert_eq!(a_held.2.account_key,owner_a.configuration.account_key.wire());
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),0,
        "A is HELD in SQL/Worker, but must still be before venue POST");

    // B receives the ORIGINAL authentic Worker HELD. Only AFTER the SQL
    // owner stores the matching central receipt do we hold its post-HELD
    // fence. A pre-SQL hold would correctly trip the cohort's unresolved
    // reserve invariant when the third Native Execute begins.
    crate::trade_service::T10_07_SQL_RESERVE_PERSISTED.store(false,SeqCst);
    crate::trade_service::T10_07_RELEASE_SQL_RESERVE_HOLD.store(false,SeqCst);
    crate::trade_service::T10_07_HOLD_SQL_RESERVE.store(true,SeqCst);
    let market_b=refresh_recorded_market();
    wait_market(market_b).await;
    let clock_b=chrono::Utc::now().timestamp_millis();
    feed.advance_recorded_public_clock(
        envelope(json!([{"ts":clock_b.to_string()}])));
    let b_fee_at=chrono::Utc::now().timestamp_millis().to_string();
    feed.advance_immutable_signed_phase("t10-07-b-fee","t10-07-b-before-held",
        RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:fee("0",&b_fee_at),
        });
    let b=make(B_KEY); assert!(b.valid());
    let pending=tokio::spawn({
        let trade=trade.clone();
        async move{trade.trade("fixture-b",&b,None).await}
    });
    let held_wait=tokio::time::timeout(Duration::from_secs(12),async {
        while !crate::trade_service::T10_07_SQL_RESERVE_PERSISTED.load(SeqCst) {
            assert!(!pending.is_finished(),
                "T10-07 B terminated before Native SQL confirmed its Worker HELD");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await;
    if held_wait.is_err() {
        let denied=feed.denied_routes.lock().unwrap().clone();
        panic!("T10_07_B_SQL_HELD_NOT_PERSISTED:posts={} denied_routes={denied:?}",
            feed.outbound_posts.lock().unwrap().len());
    }
    let persisted_b_held=owner_b.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),|s|{
            let intent=s.by_idempotency(B_KEY)?
                .ok_or(okx_execution::StoreError::Invalid("T10_07_B_HELD_SQL_MISSING"))?;
            let receipt=s.central_receipt(&intent.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid("T10_07_B_HELD_RECEIPT_MISSING"))?;
            Ok((intent.state.as_str().to_owned(),intent.intent_id,receipt))
        }
    ).await.expect("T10-07 B SQL-held owner readback");
    assert_eq!(persisted_b_held.0,"RESERVED");
    assert_eq!(persisted_b_held.1,persisted_b_held.2.intent_id);
    assert_eq!(persisted_b_held.2.account_key,owner_b.configuration.account_key.wire());
    // Third $4 Execute reaches the SAME Worker/SQL campaign while B's
    // original reservation remains HELD. There is no authorized extra POST.
    let stage=risk.request(risk_summary("risk_t10_07_two_held")).await.unwrap();
    let money=stage.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(money.reserved_usd,"8");
    assert_eq!(money.remaining_usd,"2");
    assert_eq!(money.consumed_usd,"0");
    assert!(!first_pending.is_finished() && !pending.is_finished(),
        "A and B must BOTH remain active Native Execute calls at third entrance");
    let posts_at_two_held=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_at_two_held,0,
        "two authentic Worker HELDs are not venue POST authorizations");
    let market_third=refresh_recorded_market();
    wait_market(market_third).await;
    let clock_third=chrono::Utc::now().timestamp_millis();
    feed.advance_recorded_public_clock(
        envelope(json!([{"ts":clock_third.to_string()}])));
    // A's original fee/balance exchange timestamps may have aged during
    // the two real SQL-backed RESERVE calls. The third public Execute
    // consumes new raw authenticated source events, not a stale fixture
    // or a production source-age exemption.
    let a_at=chrono::Utc::now().timestamp_millis().to_string();
    feed.advance_immutable_signed_phase(
        "t10-07-a-fee","t10-07-a-fee-for-third",RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:fee("0",&a_at),
        }
    );
    let balance_a=|stamp:&str|envelope(json!([{
        "totalEq":"100","adjEq":"100","uTime":stamp,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
          "availEq":"100","availBal":"100","eqUsd":"100","uTime":stamp}]
    }]));
    feed.advance_immutable_signed_phase(
        "t10-07-a-balance","t10-07-a-balance-for-third",RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],raw_response:balance_a(&a_at),
        }
    );
    let third=make(THIRD_KEY);assert!(third.valid());
    let native_three_way_active=
        !first_pending.is_finished() && !pending.is_finished();
    assert!(native_three_way_active,
        "three Native Execute requests must overlap at third public entrance");
    let third_result=tokio::time::timeout(Duration::from_secs(10),
        trade.trade("fixture-a",&third,None))
        .await.expect("T10-07 third public Execute deadline")
        .expect("T10-07 third public Execute returns typed central denial");
    let reason=third_result["data"]["primary_reason"].as_str().unwrap_or("").to_owned();
    assert_eq!(third_result["data"]["rejection_origin"],"CENTRAL",
        "third request must not be a test/LOCAL source failure: {third_result}");
    assert_eq!(reason,"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED");
    assert_eq!(third_result["data"]["send_state"],"NOT_STARTED");
    let denied_extra_posts=feed.outbound_posts.lock().unwrap().len()-posts_at_two_held;
    assert_eq!(denied_extra_posts,0,"central denial cannot emit a venue POST");
    let third_sql=owner_a.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        |s|s.by_idempotency(THIRD_KEY)
    ).await.expect("T10-07 third Native SQL readback")
        .expect("T10-07 third Native SQL rejected intent");
    assert_eq!(third_sql.state.as_str(),"REJECTED");

    let before_release=risk.request(risk_summary("risk_t10_07_before_release"))
        .await.expect("T10-07 Worker before original B release");
    assert_eq!(before_release.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,"8");
    let changed_at=chrono::Utc::now().timestamp_millis().to_string();
    let changed=fee("0.0005",&changed_at);
    let digest=format!("{:x}",sha2::Sha256::digest(&changed));
    let reads=feed.advance_immutable_signed_phase(
        "t10-07-b-before-held","t10-07-b-after-held",
        RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:changed,
        }
    );
    assert!(reads>0,"original B signed phase must be observed before Worker HELD");
    let market_release=refresh_recorded_market();
    wait_market(market_release).await;
    let release_posts_before=feed.outbound_posts.lock().unwrap().len();
    crate::trade_service::T10_07_RELEASE_SQL_RESERVE_HOLD.store(true,SeqCst);
    let b_result=tokio::time::timeout(Duration::from_secs(18),pending)
        .await.expect("T10-07 B source-backed RELEASE deadline")
        .expect("T10-07 B public Execute task");
    let b_reply=b_result.as_ref().ok();
    let release_state=b_reply.and_then(|v|v["data"]["risk_release_state"].as_str());
    assert_eq!(release_state,Some("RELEASED"),
        "B's original HELD must be released by Native SQL proof: {b_result:?}");
    let release_extra_posts=feed.outbound_posts.lock().unwrap().len()-release_posts_before;
    assert_eq!(release_extra_posts,0);

    let b_sql=owner_b.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),|s| {
            let intent=s.by_idempotency(B_KEY)?
                .ok_or(okx_execution::StoreError::Invalid("T10_07_ORIGINAL_B_SQL_MISSING"))?;
            let proof=s.unsent_release_receipt(&intent.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid("T10_07_B_SQL_UNSENT_PROOF_MISSING"))?;
            let receipt=s.central_receipt(&intent.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid("T10_07_B_RESERVATION_RECEIPT_MISSING"))?;
            let still_held=s.active_central_receipt(&intent.intent_id)?.is_some();
            Ok((intent.state.as_str().to_owned(),intent.intent_id,intent.plan_hash,
                intent.request_hash,intent.exchange_order_id,proof,receipt,still_held,
                intent.revision))
        }
    ).await.expect("T10-07 B original SQL owner readback");
    assert_eq!(b_sql.0,"REJECTED");
    assert!(b_sql.4.is_none() && !b_sql.7);
    assert_eq!(b_sql.5.intent_id,b_sql.1);
    assert_eq!(b_sql.5.account_key,owner_b.configuration.account_key.wire());
    assert_eq!(b_sql.5.plan_hash,b_sql.2);
    assert_eq!(b_sql.5.request_hash,b_sql.3);
    assert_eq!(b_sql.5.reservation_key,b_sql.6.reservation_key);
    let inspected=risk.request(RiskCommand {
        schema:RISK_COMMAND_SCHEMA_V1.into(),
        command_id:"risk_t10_07_inspect_original_b_release".into(),
        installation_id:"installation_test".into(),
        action:RiskCommandAction::Inspect {
            reservation_key:b_sql.5.reservation_key.clone(),
            plan_sha256:b_sql.2.clone(),
        },
    }).await.expect("T10-07 Worker INSPECT original B reservation");
    assert_eq!(inspected.status,RiskDecisionStatus::Observed);
    assert_eq!(inspected.reservation_state,
        Some(okx_protocol::risk_authority::RiskReservationState::Released));
    let after=risk.request(risk_summary("risk_t10_07_after_release"))
        .await.expect("T10-07 persistent Worker after");
    let after_entry=after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(after_entry.reserved_usd,"4");
    assert_eq!(after_entry.remaining_usd,"6");
    assert_eq!(after_entry.consumed_usd,"0");
    assert!(after.risk_book_sequence.unwrap_or(0)>
        before_release.risk_book_sequence.unwrap_or(0));
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),1);

    json!({
        "schema":"okx.t10-07-native-one-campaign/v1",
        "a_account_uid":owner_a.configuration.account_key.verified_uid,
        "b_account_uid":owner_b.configuration.account_key.verified_uid,
        "a_sql_state":a_sql.state.as_str(),
        "a_sql_revision":a_sql.revision,
        "b_sql_state":b_sql.0,
        "b_sql_revision":b_sql.8,
        "third_sql_state":third_sql.state.as_str(),
        "third_sql_revision":third_sql.revision,
        "b_intent_id":b_sql.1,"a_intent_id":a_sql.intent_id,
        "b_release_reservation_key":b_sql.5.reservation_key,
        "b_release_plan_sha256":b_sql.2,
        "signed_changed_fee_digest":digest,
        "denial_reason":reason,
        "denied_extra_posts":denied_extra_posts,
        "release_extra_posts":release_extra_posts,
        "sql_release_verified":true,
        "worker_release_verified":true,
        "worker_before_release":before_release.budget,
        "worker_after_release":after.budget,
        "worker_after_sequence":after.risk_book_sequence,
        "worker_two_held_sequence":stage.risk_book_sequence,
        "held_receipts":["HELD","HELD","DENIED"],
        "original_b_held_source_reads":reads,
        "total_venue_posts":feed.outbound_posts.lock().unwrap().len(),
    })
}
