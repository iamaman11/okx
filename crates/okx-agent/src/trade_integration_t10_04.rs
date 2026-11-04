// T10-04: fault only the delivery of a REAL Worker HELD after the Worker
// has made its authoritative SQL-backed reservation. The next public Native
// B Execute cannot use a result with a foreign command ID as authorization.
async fn t10_04_foreign_worker_result(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::trading::{
        OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
    };
    let wrap=|rows:serde_json::Value|serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":rows
    })).unwrap();
    let fresh=chrono::Utc::now().timestamp_millis().to_string();
    let balance=wrap(serde_json::json!([{
        "totalEq":"100","adjEq":"100","uTime":fresh,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":fresh}]
    }]));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/balance",
        (0..16).map(|_|RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/balance".into(),
            required_query:vec![],raw_response:balance.clone(),
        }).collect());
    let fee=wrap(serde_json::json!([{
        "instType":"SWAP","level":"Lv1","ts":fresh,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/trade-fee",
        (0..8).map(|_|RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],raw_response:fee.clone(),
        }).collect());
    feed.advance_recorded_public_clock(
        wrap(serde_json::json!([{"ts":fresh}]))
    );
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,4,"T10-04 begins after A positive ACK, before all other probes");
    let mut before_cmd=summary("risk_t10_04_before_worker_fault");
    before_cmd.installation_id="installation_test".into();
    let before=risk.request(before_cmd).await.expect("T10-04 real Worker before");
    assert_eq!(before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().reserved_usd,
        "7.0015");

    T10_04_FOREIGN_COMMAND_INJECTED.store(
        false,std::sync::atomic::Ordering::SeqCst
    );
    T10_04_CORRUPT_NEXT_HELD_COMMAND.store(
        true,std::sync::atomic::Ordering::SeqCst
    );
    let candidate=TradeRequest::Execute{
        idempotency_key:"t10_04_public_b_foreign_worker_reply_0001".into(),
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,sizing:Sizing::Contracts{contracts:"0.1".into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    assert!(candidate.valid());
    let call=tokio::time::timeout(Duration::from_secs(18),
        trade.trade("fixture-b",&candidate,None)
    ).await.expect("T10-04 public B trade timeout");
    let (denial_status,denial_reason)=match call{
        Ok(reply)=>(
            reply["status"].as_str().unwrap_or("UNVERIFIED").to_owned(),
            reply["data"]["primary_reason"].as_str().unwrap_or("UNKNOWN").to_owned(),
        ),
        Err(reason)=>("LOCAL_ERROR".into(),reason),
    };
    assert!(T10_04_FOREIGN_COMMAND_INJECTED.load(
        std::sync::atomic::Ordering::SeqCst
    ),"T10-04 must observe a committed real Worker HELD before corruption");
    assert!(!T10_04_CORRUPT_NEXT_HELD_COMMAND.load(
        std::sync::atomic::Ordering::SeqCst
    ),"T10-04 injection was not consumed");
    assert_ne!(denial_status,"accepted","foreign Worker binding cannot allow POST");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before);
    let b=accounts.resolve("fixture-b").await.expect("T10-04 B SQL owner");
    let sql=b.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        |store| {
            let intent=store.by_idempotency(
                "t10_04_public_b_foreign_worker_reply_0001"
            )?.ok_or(okx_execution::StoreError::Invalid(
                "T10_04_PUBLIC_B_PREPARED_INTENT_MISSING"
            ))?;
            let receipt=store.active_central_receipt(&intent.intent_id)?;
            Ok((intent.state.as_str().to_owned(),intent.revision,
                intent.exchange_order_id,receipt.is_some(),intent.intent_id))
        }
    ).await.expect("T10-04 B persisted intent readback");
    assert!(!matches!(sql.0.as_str(),"ACKNOWLEDGED"|"FILLED"|"POSTING"),
        "unbound Worker reply must never authorize a possible create POST");
    assert!(sql.2.is_none());
    assert!(!sql.3,"foreign Worker ID cannot become native persisted receipt");
    serde_json::json!({
        "schema":"okx.t10-04-native-worker-binding/v1",
        "account_uid":b.configuration.account_key.verified_uid,
        "sql_state":sql.0,"sql_revision":sql.1,
        "sql_intent_id":sql.4,
        "sql_exchange_order_id":sql.2,
        "sql_has_central_receipt":sql.3,
        "fault_injected":true,
        "denial_status":denial_status,"denial_reason":denial_reason,
        "posts_before":posts_before,
        "posts_after":feed.outbound_posts.lock().unwrap().len(),
        "worker_budget_before":before.budget,
        "worker_sequence_before":before.risk_book_sequence,
    })
}
