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
    let stamp=chrono::Utc::now().timestamp_millis().to_string();
    let balance=wrap(serde_json::json!([{
        "totalEq":"100","adjEq":"100","uTime":stamp,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":stamp}]
    }]));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/balance",
        (0..16).map(|_|RecordedGet{
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],raw_response:balance.clone(),
        }).collect()
    );
    let fee=|rate:&str,at:&str|wrap(serde_json::json!([{
        "instType":"SWAP","level":"Lv1","ts":at,
        "feeGroup":[{"groupId":"4","maker":rate,"taker":rate}]
    }]));
    let initial=fee("0.0005",&stamp);
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/trade-fee",
        (0..12).map(|_|RecordedGet{
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:initial.clone(),
        }).collect()
    );
    feed.advance_recorded_public_clock(
        wrap(serde_json::json!([{"ts":stamp}]))
    );
    let sizing=Sizing::Contracts{contracts:"0.1".into()};
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
    assert_eq!(old_quote,"1.0005");
    let mut before_risk=summary("risk_t10_15_before_held");
    before_risk.installation_id="installation_test".into();
    let worker_before=risk.request(before_risk).await
        .expect("T10-15 actual Worker before");
    assert_eq!(worker_before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
        "7.0015");
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,4);
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
    let reason=match result{
        Ok(body)=>body["data"]["primary_reason"].as_str()
            .unwrap_or("NO_TYPED_REFUSAL").to_owned(),
        Err(reason)=>reason,
    };
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
            Ok((i.intent_id,i.state.as_str().to_owned(),i.revision,i.exchange_order_id))
        }
    ).await.expect("T10-15 real SQL readback");
    assert!(row.3.is_none());
    assert!(!matches!(row.1.as_str(),"ACKNOWLEDGED"|"FILLED"|"POSTING"));
    // A newly entered preview must use the newly signed fee rate rather
    // than returning the old frozen plan. It creates no reserve or POST.
    let new_preview=TradeRequest::Preview {
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:"0.1".into()},
        order:OrderConstraints{
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
    };
    let next=trade.trade("fixture-b",&new_preview,None).await
        .expect("T10-15 fresh quote after signed fee revision");
    let quote=next["data"]["entry_costs"]["entry_total_usd"]
        .as_str().expect("T10-15 refreshed source quote").to_owned();
    assert_eq!(quote,"1.0006");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before);
    let mut after_risk=summary("risk_t10_15_after_fee_revision");
    after_risk.installation_id="installation_test".into();
    let after=risk.request(after_risk).await.expect("T10-15 Worker finance after");
    assert_eq!(after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().consumed_usd,
        "1.0005");
    serde_json::json!({
        "schema":"okx.t10-15-post-held-source-change/v1",
        "account_uid":owner.configuration.account_key.verified_uid,
        "old_quote_usd":old_quote,"new_quote_usd":quote,
        "signed_changed_fee_digest":digest,
        "first_denial":reason,
        "worker_held_witness":true,"sql_state":row.1,
        "sql_revision":row.2,"intent_id":row.0,
        "posts_before":posts_before,"posts_after":posts_after,
        "worker_before":worker_before.budget,
        "worker_after":after.budget,"worker_sequence":after.risk_book_sequence,
    })
}
