// T10-15: mutate only an authenticated raw fee source after an actual
// Worker HELD commit and before the unmodified production post-HELD fence.
async fn t10_15_fee_source_after_worker_held(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    rate_budget: RateBudget,
)->serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::trading::{
        OrderConstraints,PositionSide,Protection,Sizing,TradeOperation,TradeRequest
    };
    let wrap=|rows:serde_json::Value|serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":rows
    })).unwrap();
    // T10-07 exercises this SAME native no-send owner after the second
    // account's positive order. The available campaign headroom is smaller;
    // use an ordinary 0.05-contract public request rather than bypassing
    // the entry cap. A second cohort cannot prove the shared release.
    let shared_b1=std::env::var("OKX_T10_07_NATIVE_PROBE").as_deref()==Ok("1");
    let contracts=if shared_b1{"0.05"}else{"0.1"};
    let quoted_before=if shared_b1{"0.50025"}else{"1.0005"};
    let quoted_after=if shared_b1{"0.5003"}else{"1.0006"};
    let held_before=if shared_b1{"8.002"}else{"7.0015"};
    let remaining_before=if shared_b1{"0.9975"}else{"1.998"};
    let expected_posts=if shared_b1{5}else{4};
    let stamp=chrono::Utc::now().timestamp_millis().to_string();
    let balance=wrap(serde_json::json!([{
        "totalEq":"100","adjEq":"100","uTime":stamp,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":stamp}]
    }]));
    let balance_pages=(0..16).map(|_|RecordedGet{
        access_key:"fixture-observer-b".into(),
        path:"/api/v5/account/balance".into(),
        required_query:vec![],raw_response:balance.clone(),
    }).collect::<Vec<_>>();
    if shared_b1 {
        // B1 previously installed this route in the SAME Native campaign.
        feed.advance_recorded_route("fixture-observer-b",
            "/api/v5/account/balance",balance_pages);
    } else {
        // B's first REST source: no earlier phase may be retired.
        for page in balance_pages { feed.expect_get(page); }
    }
    let fee=|rate:&str,at:&str|wrap(serde_json::json!([{
        "instType":"SWAP","level":"Lv1","ts":at,
        "feeGroup":[{"groupId":"4","maker":rate,"taker":rate}]
    }]));
    let initial=fee("0.0005",&stamp);
    let fee_pages=(0..12).map(|_|RecordedGet{
        access_key:"fixture-observer-b".into(),
        path:"/api/v5/account/trade-fee".into(),
        required_query:vec![
            ("instType".into(),"SWAP".into()),
            ("instFamily".into(),"BTC-USDT".into()),
        ],raw_response:initial.clone(),
    }).collect::<Vec<_>>();
    if shared_b1 {
        feed.advance_recorded_route("fixture-observer-b",
            "/api/v5/account/trade-fee",fee_pages);
    } else {
        for page in fee_pages { feed.expect_get(page); }
    }
    // The independent T10-15 test registers B's first signed leverage
    // source; the shared T10-07 cohort has ALREADY read B's leverage for
    // its accepted public order. Advance to a fresh, finite raw leverage
    // phase only after that real B ACK. Never rearm a source on demand
    // inside the HTTP handler or weaken the leverage identity oracle.
    let leverage=wrap(serde_json::json!([
        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"long","lever":"2"},
        {"instId":"BTC-USDT-SWAP","mgnMode":"cross","posSide":"short","lever":"2"}
    ]));
    let leverage_pages=(0..if shared_b1 {4} else {12}).map(|_|RecordedGet{
        access_key:"fixture-observer-b".into(),
        path:"/api/v5/account/leverage-info".into(),
        required_query:vec![
            ("instId".into(),"BTC-USDT-SWAP".into()),
            ("mgnMode".into(),"cross".into()),
        ],raw_response:leverage.clone(),
    }).collect::<Vec<_>>();
    if shared_b1 {
        feed.advance_recorded_route("fixture-observer-b",
            "/api/v5/account/leverage-info",leverage_pages);
    } else {
        for page in leverage_pages { feed.expect_get(page); }
    }
    feed.advance_recorded_public_clock(
        wrap(serde_json::json!([{"ts":stamp}]))
    );
    let sizing=Sizing::Contracts{contracts:contracts.into()};
    let order=OrderConstraints{
        order_type:"limit".into(),limit_price:Some("1000".into()),
        reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0
    };
    let preview=TradeRequest::Preview {
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,sizing:sizing.clone(),order:order.clone(),
        protection:Some(Protection::Price{stop_price:"900".into()}),
    };
    let until=std::time::Instant::now()+Duration::from_secs(12);
    let old=loop {
        let answer=tokio::time::timeout_at(until.into(),
            trade.trade("fixture-b",&preview,None)
        ).await.expect("T10-15 pre-HELD Preview deadline");
        match answer {
            Ok(value)=>break value,
            Err(reason)=>{
                let delay=recorded_preview_local_defer(&reason,&rate_budget,
                    until.saturating_duration_since(std::time::Instant::now())
                ).unwrap_or_else(||panic!("T10-15 initial Preview invalid: {reason}"));
                tokio::time::timeout_at(until.into(),tokio::time::sleep(delay))
                    .await.expect("T10-15 bounded local read defer");
            }
        }
    };
    let old_quote=old["data"]["entry_costs"]["entry_total_usd"]
        .as_str().expect("T10-15 original quoted amount").to_owned();
    assert_eq!(old_quote,quoted_before);
    let mut before_risk=summary("risk_t10_15_before_held");
    before_risk.installation_id="installation_test".into();
    let worker_before=risk.request(before_risk).await
        .expect("T10-15 actual Worker before");
    assert_eq!(worker_before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
        held_before);
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,expected_posts);
    T10_15_WORKER_HELD_COMMITTED.store(false,std::sync::atomic::Ordering::SeqCst);
    T10_15_SIGNED_SOURCE_CHANGED.store(false,std::sync::atomic::Ordering::SeqCst);
    T10_15_ARM_AFTER_HELD.store(true,std::sync::atomic::Ordering::SeqCst);
    let execute=TradeRequest::Execute {
        idempotency_key:"t10_15_fee_changed_between_held_and_post_0001".into(),
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,sizing,order,
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    let pending=tokio::spawn({
        let trade=trade.clone();
        async move{trade.trade("fixture-b",&execute,None).await}
    });
    tokio::time::timeout(Duration::from_secs(6),async{
        while !T10_15_WORKER_HELD_COMMITTED.load(
            std::sync::atomic::Ordering::SeqCst
        ){tokio::time::sleep(Duration::from_millis(10)).await;}
    }).await.expect("T10-15 actual Worker HELD not observed");
    let after_ms=chrono::Utc::now().timestamp_millis().to_string();
    let changed=fee("0.0006",&after_ms);
    let digest=format!("{:x}",sha2::Sha256::digest(&changed));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/trade-fee",
        (0..14).map(|_|RecordedGet{
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:changed.clone(),
        }).collect()
    );
    T10_15_SIGNED_SOURCE_CHANGED.store(true,std::sync::atomic::Ordering::SeqCst);
    let result=tokio::time::timeout(Duration::from_secs(15),pending)
        .await.expect("T10-15 source-changed Execute deadline")
        .expect("T10-15 public Execute task");
    // This must be an authentic production SQL-backed no-send release,
    // not merely a rejected plan with a held Worker reservation left behind.
    // The public Execute response is issued after the Rust owner requested
    // RELEASE_UNSENT and persisted its central acknowledgement.
    let release_reply=result.as_ref().ok()
        .and_then(|body|body["data"]["risk_release_state"].as_str());
    let reason=match &result{
        Ok(body)=>body["data"]["primary_reason"].as_str()
            .unwrap_or("NO_TYPED_REFUSAL").to_owned(),
        Err(reason)=>reason.clone(),
    };
    assert_eq!(release_reply,Some("RELEASED"),
        "T10-15 requires Native SQL -> Worker RELEASE_UNSENT, not a pending release: {result:?}");
    assert!(
        reason.contains("POST_ENTRY_FEE_OR_SPEC_CHANGED")
          ||reason.contains("POST_ENTRY_FEE_SOURCE_CHANGED"),
        "T10-15 must refuse a true post-HELD fee mismatch: {reason}"
    );
    let posts_after=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_after,posts_before,"old fee proof must never reach venue POST");
    let owner=accounts.resolve("fixture-b").await.expect("T10-15 B SQL owner");
    let row=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        |s| {
            let i=s.by_idempotency(
                "t10_15_fee_changed_between_held_and_post_0001"
            )?.ok_or(okx_execution::StoreError::Invalid("T10_15_SQL_INTENT_MISSING"))?;
            let unsent=s.unsent_release_receipt(&i.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid(
                    "T10_15_DURABLE_UNSENT_SQL_PROOF_MISSING"))?;
            let retained=s.central_receipt(&i.intent_id)?
                .ok_or(okx_execution::StoreError::Invalid(
                    "T10_15_RETAINED_RESERVATION_RECEIPT_MISSING"))?;
            let still_held=s.active_central_receipt(&i.intent_id)?.is_some();
            Ok((i.intent_id,i.state.as_str().to_owned(),i.revision,
                i.exchange_order_id,unsent,retained,still_held,
                i.plan_hash,i.request_hash))
        }
    ).await.expect("T10-15 real SQL readback");
    assert_eq!(row.1,"REJECTED");
    assert!(row.3.is_none());
    assert!(!row.6,"Native SQL must not count the released reservation as active");
    assert_eq!(row.4.intent_id,row.0);
    assert_eq!(row.4.account_key,owner.configuration.account_key.wire());
    assert_eq!(row.4.plan_hash,row.7);
    assert_eq!(row.4.request_hash,row.8);
    assert_eq!(row.4.reservation_key,row.5.reservation_key);
    assert_eq!(row.5.plan_hash,row.7);
    let inspect=risk.request(RiskCommand{
        schema:RISK_COMMAND_SCHEMA_V1.into(),
        command_id:"risk_t10_15_inspect_native_unsent_release".into(),
        installation_id:"installation_test".into(),
        action:RiskCommandAction::Inspect{
            reservation_key:row.4.reservation_key.clone(),
            plan_sha256:row.7.clone(),
        },
    }).await.expect("T10-15 Worker INSPECT after Native SQL release");
    assert_eq!(inspect.status,RiskDecisionStatus::Observed);
    assert_eq!(inspect.reservation_key.as_deref(),
        Some(row.4.reservation_key.as_str()));
    assert_eq!(inspect.reservation_state,
        Some(okx_protocol::risk_authority::RiskReservationState::Released),
        "Native's durable no-send release must be committed by the real Worker");
    // A newly entered preview must use the newly signed fee rate rather
    // than returning the old frozen plan. It creates no reserve or POST.
    let new_preview=TradeRequest::Preview {
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:contracts.into()},
        order:OrderConstraints{
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
    };
    // A read-only Preview may be deferred before sending a private GET
    // by the REAL authenticated-user quota. Retry ONLY that typed LOCAL
    // no-send admission, under one finite deadline. No Execute, risk
    // reservation, stale quote reuse or enlarged production rate policy.
    let preview_until=std::time::Instant::now()+Duration::from_secs(12);
    let next=loop {
        let answer=tokio::time::timeout_at(preview_until.into(),
            trade.trade("fixture-b",&new_preview,None)
        ).await.expect("T10-15 new signed Preview deadline");
        match answer {
            Ok(value)=>break value,
            Err(reason)=>{
                let delay=recorded_preview_local_defer(
                    &reason,&rate_budget,
                    preview_until.saturating_duration_since(std::time::Instant::now())
                ).unwrap_or_else(||panic!(
                    "T10-15 new Preview non-local refusal: {reason}"
                ));
                tokio::time::timeout_at(preview_until.into(),tokio::time::sleep(delay))
                    .await.expect("T10-15 bounded local no-send Preview defer");
            }
        }
    };
    let quote=next["data"]["entry_costs"]["entry_total_usd"]
        .as_str().expect("T10-15 refreshed source quote").to_owned();
    assert_eq!(quote,quoted_after);
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before);
    let mut after_risk=summary("risk_t10_15_after_fee_revision");
    after_risk.installation_id="installation_test".into();
    let after=risk.request(after_risk).await.expect("T10-15 Worker finance after");
    let budget_before=worker_before.budget.as_ref().unwrap()
        .entry_budget.as_ref().unwrap();
    let budget_after=after.budget.as_ref().unwrap()
        .entry_budget.as_ref().unwrap();
    assert_eq!(budget_after.consumed_usd,budget_before.consumed_usd);
    assert_eq!(budget_after.reserved_usd,budget_before.reserved_usd,
        "Native no-send must return its transient HELD allocation");
    assert_eq!(budget_after.remaining_usd,budget_before.remaining_usd);
    assert_eq!(budget_after.consumed_usd,"1.0005");
    assert_eq!(budget_after.reserved_usd,held_before);
    assert_eq!(budget_after.remaining_usd,remaining_before);
    assert!(after.risk_book_sequence.unwrap_or(0)>
        worker_before.risk_book_sequence.unwrap_or(0),
        "authentic central RELEASE must advance durable Worker sequence");
    serde_json::json!({
        "schema":"okx.t10-15-post-held-source-change/v1",
        "account_uid":owner.configuration.account_key.verified_uid,
        "old_quote_usd":old_quote,"new_quote_usd":quote,
        "signed_changed_fee_digest":digest,
        "first_denial":reason,
        "worker_held_witness":true,
        "same_campaign_as_native_b1":shared_b1,
        "sql_state":row.1,
        "sql_revision":row.2,"intent_id":row.0,
        "release_state":"RELEASED",
        "sql_unsent_receipt_verified":true,
        "sql_release_not_active":!row.6,
        "sql_reservation_key":row.4.reservation_key,
        "sql_plan_hash":row.7,
        "worker_release_verified":true,
        "worker_release_inspect_sequence":inspect.risk_book_sequence,
        "posts_before":posts_before,"posts_after":posts_after,
        "worker_before":worker_before.budget,
        "worker_after":after.budget,"worker_sequence":after.risk_book_sequence,
    })
}
