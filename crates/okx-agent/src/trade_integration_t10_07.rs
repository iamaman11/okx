// Standalone T10-07: actual public Native A/B Execute, separate account SQL
// owners and ONE clean persisted Worker RiskBook. The fixture transports raw
// exchange evidence only; it does not manufacture reservations or SQL rows.
async fn t10_07_native_single_campaign(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    _rate_budget: RateBudget,
    refresh_recorded_market: &impl Fn() -> i64,
) -> serde_json::Value {
    use crate::accounts::recorded_support::{RecordedGet,RecordedPost,RecordedPhaseBudget};
    use okx_protocol::trading::{
        OrderConstraints,PositionSide,Protection,Sizing,TradeOperation,TradeRequest,
    };
    use serde_json::json;
    let wrap=|rows:serde_json::Value|serde_json::to_vec(
        &json!({"code":"0","msg":"","data":rows})
    ).unwrap();
    let read_risk=|command:&str|{
        let mut cmd=summary(command);
        cmd.installation_id="installation_test".into();
        cmd
    };
    let before=risk.request(read_risk("t10_07_before_native"))
        .await.expect("T10-07 real Worker before");
    let before_entry=before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(before_entry.consumed_usd,"0");
    assert_eq!(before_entry.reserved_usd,"0");
    assert_eq!(before_entry.remaining_usd,"10");

    // The ordinary production maintenance owner establishes both account
    // SQL campaign frontiers. No test-owned INSERT or Worker command can
    // substitute for this readiness proof.
    let (maintenance_shutdown,maintenance_rx)=watch::channel(false);
    let maintenance_trade=trade.clone();
    let maintenance=tokio::spawn(async move {
        maintenance_trade.run(maintenance_rx).await;
    });
    let campaign_id=before_entry.campaign_id.clone();
    let native_origin=i64::try_from(before.budget.as_ref().unwrap()
        .campaign_origin.as_ref().unwrap().started_at_ms).unwrap();
    let mut last_readiness=String::from("NOT_CHECKED");
    tokio::time::timeout(Duration::from_secs(14),async {
        loop {
            let through=chrono::Utc::now().timestamp_millis()-2_000;
            let mut ready=true;
            for alias in ["fixture-a","fixture-b"] {
                let owner=accounts.resolve(alias).await.unwrap();
                let campaign=campaign_id.clone();
                if let Err(error)=owner.execution.store.call(
                    std::time::Instant::now()+Duration::from_secs(2),
                    move |s|s.verify_campaign_entry_readiness(
                        &campaign,native_origin,through
                    ),
                ).await {
                    last_readiness=format!("{alias}:{error}");
                    ready=false;
                    break;
                }
            }
            if ready {break;}
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.unwrap_or_else(|_|panic!(
        "T10_07_NATIVE_SQL_CAMPAIGN_NOT_READY:{last_readiness}"
    ));

    let owner_a=accounts.resolve("fixture-a").await.unwrap();
    let owner_b=accounts.resolve("fixture-b").await.unwrap();
    assert_ne!(owner_a.configuration.account_key.verified_uid,
        owner_b.configuration.account_key.verified_uid);
    let account_keys=[
        owner_a.configuration.account_key.wire().to_owned(),
        owner_b.configuration.account_key.wire().to_owned(),
    ];
    // Freeze only the NEXT test-driven periodic source tick while installing
    // a new raw authenticated fee event. All production finance capture
    // resumes after this handoff; source age and OKX quota are unchanged.
    let gate_a=owner_a.recorded_capture_gate.write().await;
    let gate_b=owner_b.recorded_capture_gate.write().await;
    let source_budget=RecordedPhaseBudget::new(64,Duration::from_secs(90));
    let now=chrono::Utc::now().timestamp_millis().to_string();
    let fee=|rate:&str,time:&str|wrap(json!([{
        "instType":"SWAP","level":"Lv1","ts":time,
        "feeGroup":[{"groupId":"4","maker":rate,"taker":rate}]
    }]));
    for (label,phase) in [
        ("a","t10-07-a-fee-zero"),
        ("b","t10-07-b-fee-zero"),
    ] {
        let old=feed.supersede_queued_signed_source_at_event(
            "t10-07-native-campaign-fee-observed",phase,RecordedGet {
                access_key:format!("fixture-observer-{label}"),
                path:"/api/v5/account/trade-fee".into(),
                required_query:vec![
                    ("instType".into(),"SWAP".into()),
                    ("instFamily".into(),"BTC-USDT".into()),
                ],raw_response:fee("0",&now),
            },source_budget
        );
        assert!(old>0,"T10-07 requires previously observed real signed tariff");
    }
    drop(gate_b);
    drop(gate_a);

    let request=|id:&str|TradeRequest::Execute{
        idempotency_key:id.into(),operation:TradeOperation::Open,
        strategy_instance:"manual".into(),instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:"0.4".into()},
        order:OrderConstraints{
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    let a=request("t10_07_a_first_execute_0001");
    let b=request("t10_07_b_second_execute_0001");
    let third=request("t10_07_a_third_execute_0001");
    assert!(a.valid()&&b.valid()&&third.valid());
    // Only A's true create may be sent; B's HELD is held before the
    // post-fence while the third request competes for the shared cap.
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
        expected_body:json!({
            "instId":"BTC-USDT-SWAP","tdMode":"cross",
            "clOrdId":"__REQUEST_CL_ORD_ID__",
            "side":"buy","posSide":"long","ordType":"limit",
            "sz":"0.4","px":"1000",
            "attachAlgoOrds":[{
                "attachAlgoClOrdId":"__REQUEST_ATTACH_ALGO_ID__",
                "tpTriggerPxType":"mark","slTriggerPx":"900",
                "slTriggerPxType":"mark","slOrdPx":"-1"
            }]
        }),
        raw_response:br#"{"code":"0","msg":"","data":[{"ordId":"recorded-t10-07-a-001","clOrdId":"__REQUEST_CL_ORD_ID__","sCode":"0"}]}"#.to_vec(),
    });
    let order_time=chrono::Utc::now().timestamp_millis().to_string();
    for _ in 0..6 {
        feed.expect_get(RecordedGet {
            access_key:"fixture-executor-a".into(),
            path:"/api/v5/trade/order".into(),
            required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
            raw_response:wrap(json!([{
                "instId":"BTC-USDT-SWAP","ordId":"recorded-t10-07-a-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__",
                "side":"buy","posSide":"long","tdMode":"cross",
                "ordType":"limit","px":"1000","sz":"0.4",
                "accFillSz":"0","avgPx":"","state":"live",
                "cTime":order_time,"uTime":order_time
            }])),
        });
    }
    feed.advance_recorded_public_clock(wrap(json!([{
        "ts":chrono::Utc::now().timestamp_millis().to_string()
    }])));
    let posts_before=feed.outbound_posts.lock().unwrap().len();

    // This scoped market publisher consumes only recorded public WS frames.
    // It does not set a price, source timestamp or any financial authority.
    let scenario=async {
        let a_reply=tokio::time::timeout(Duration::from_secs(27),
            trade.trade("fixture-a",&a,None)
        ).await.expect("T10-07 A Execute deadline")
            .expect("T10-07 A actual public Execute");
        if a_reply["data"]["state"]=="REJECTED" {
            panic!("T10_07_FIRST_EXECUTE_DENIED:{a_reply}");
        }
        let a_sql=owner_a.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            |s|s.by_idempotency("t10_07_a_first_execute_0001")
        ).await.expect("T10-07 A SQL readback")
            .expect("T10-07 A real SQL intent absent");
        assert_eq!(a_sql.state.as_str(),"ACKNOWLEDGED");
        assert_eq!(a_sql.exchange_order_id.as_deref(),
            Some("recorded-t10-07-a-001"));
        let after_a=risk.request(read_risk("t10_07_after_a"))
            .await.expect("T10-07 Worker A");
        let entry_a=after_a.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
        assert_eq!(entry_a.reserved_usd,"4");
        assert_eq!(entry_a.consumed_usd,"0");

        T10_07_B_HELD_OBSERVED.store(false,std::sync::atomic::Ordering::SeqCst);
        T10_07_RELEASE_B_HELD_GATE.store(false,std::sync::atomic::Ordering::SeqCst);
        T10_07_ARM_B_HELD_GATE.store(true,std::sync::atomic::Ordering::SeqCst);
        let waiting_b=tokio::spawn({
            let trade=trade.clone();
            async move{trade.trade("fixture-b",&b,None).await}
        });
        tokio::time::timeout(Duration::from_secs(12),async {
            while !T10_07_B_HELD_OBSERVED.load(
                std::sync::atomic::Ordering::SeqCst
            ){
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("T10-07 authentic B Worker HELD not committed");

        // B still owns a real pending Rust Execute and SQL reservation.
        // A third distinct public Execute must be denied by the SAME Worker.
        let posts_before_third=feed.outbound_posts.lock().unwrap().len();
        let third_result=tokio::time::timeout(Duration::from_secs(13),
            trade.trade("fixture-a",&third,None)
        ).await.expect("T10-07 third concurrent Execute deadline");
        let third_text=match &third_result{
            Ok(reply)=>reply.to_string(),Err(reason)=>reason.clone(),
        };
        assert!(third_text.contains("CAMPAIGN_ENTRY_BUDGET_EXHAUSTED"),
            "T10-07 third must reach Worker shared cap: {third_text}");
        assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before_third,
            "T10-07 denied third must have no venue POST");
        let before_release=risk.request(read_risk("t10_07_before_release"))
            .await.expect("T10-07 Worker with two real HELD");
        let entry_two=before_release.budget.as_ref().unwrap()
            .entry_budget.as_ref().unwrap();
        assert_eq!(entry_two.reserved_usd,"8");
        assert_eq!(entry_two.remaining_usd,"2");
        assert_eq!(entry_two.consumed_usd,"0");

        // A new authenticated B fee source arrives ONLY after the true
        // Worker 8/2 readback. The unmodified production post-HELD fence
        // denies the old B quote and obtains SQL-backed RELEASE_UNSENT.
        let changed=fee("0.0001",&chrono::Utc::now().timestamp_millis().to_string());
        feed.advance_immutable_signed_phase(
            "t10-07-b-fee-zero","t10-07-b-fee-changed",RecordedGet {
                access_key:"fixture-observer-b".into(),
                path:"/api/v5/account/trade-fee".into(),
                required_query:vec![
                    ("instType".into(),"SWAP".into()),
                    ("instFamily".into(),"BTC-USDT".into()),
                ],raw_response:changed,
            },source_budget,
        );
        T10_07_RELEASE_B_HELD_GATE.store(true,std::sync::atomic::Ordering::SeqCst);
        let b_result=tokio::time::timeout(Duration::from_secs(15),waiting_b)
            .await.expect("T10-07 B post-HELD source fence")
            .expect("T10-07 B Native task join");
        let b_text=match &b_result{
            Ok(reply)=>reply.to_string(),Err(reason)=>reason.clone()
        };
        assert!(b_text.contains("POST_ENTRY_FEE_OR_SPEC_CHANGED")
            ||b_text.contains("POST_ENTRY_FEE_SOURCE_CHANGED"),
            "T10-07 source change must reject B before POST: {b_text}");
        let b_sql=owner_b.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            |s|{
                let i=s.by_idempotency("t10_07_b_second_execute_0001")?
                    .ok_or(okx_execution::StoreError::Invalid(
                        "T10_07_B_NATIVE_INTENT_ABSENT"))?;
                let release=s.unsent_release_receipt(&i.intent_id)?;
                let still_held=s.active_central_receipt(&i.intent_id)?.is_some();
                Ok((i,release,still_held))
            }
        ).await.expect("T10-07 B SQL release readback");
        assert_eq!(b_sql.0.state.as_str(),"REJECTED");
        assert!(b_sql.1.is_some(),"T10-07 B missing authentic SQL unsent receipt");
        assert!(!b_sql.2,"T10-07 released B cannot remain SQL HELD");
        let after=risk.request(read_risk("t10_07_after_release"))
            .await.expect("T10-07 authoritative Worker release");
        let e=after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
        assert_eq!(e.reserved_usd,"4");
        assert_eq!(e.remaining_usd,"6");
        assert_eq!(e.consumed_usd,"0");
        let posts_after=feed.outbound_posts.lock().unwrap().len();
        assert_eq!(posts_after,posts_before+1,"only Native A create was sent");
        serde_json::json!({
            "schema":"okx.t10-07-native-single-campaign/v1",
            "account_keys":account_keys,
            "two_real_sql_owners":true,
            "native_execute_count":3,
            "a_intent_id":a_sql.intent_id,
            "a_sql_revision":a_sql.revision,
            "a_sql_state":a_sql.state.as_str(),
            "b_intent_id":b_sql.0.intent_id,
            "b_sql_revision":b_sql.0.revision,
            "b_sql_state":b_sql.0.state.as_str(),
            "b_release_receipt_verified":b_sql.1.is_some(),
            "b_sql_active_central_receipt":b_sql.2,
            "worker_before_release":before_release.budget,
            "worker_after_release":after.budget,
            "worker_sequence":after.risk_book_sequence,
            "third_denial":"CAMPAIGN_ENTRY_BUDGET_EXHAUSTED",
            "third_extra_create_posts":0,
            "posts_before":posts_before,
            "posts_after":posts_after,
            "source_events":feed.immutable_phase_trace.lock().unwrap().clone()
        })
    };
    let publisher=async {
        loop {
            refresh_recorded_market();
            tokio::time::sleep(Duration::from_millis(750)).await;
        }
    };
    let result=tokio::select!{
        output=scenario=>output,
        _=publisher=>unreachable!("T10-07 publisher is bounded by scenario")
    };
    maintenance_shutdown.send(true).expect("T10-07 maintenance owner");
    tokio::time::timeout(Duration::from_secs(10),maintenance)
        .await.expect("T10-07 maintenance join deadline").unwrap();
    result
}
