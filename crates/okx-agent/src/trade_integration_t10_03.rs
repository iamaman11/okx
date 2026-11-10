// T10-03: a genuine Worker BeginPost is committed to its SQLite DO, then its
// WSS response is lost before Native receives it. Production TradeService
// must INSPECT the SAME central reserve, keep SQL uncertain, and do zero POSTs.
async fn t10_03_worker_committed_post_reply_lost(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::risk_authority::{
        RiskCommand, RiskCommandAction, RiskDecisionStatus, RiskReservationState,
        RISK_COMMAND_SCHEMA_V1,
    };
    use okx_protocol::trading::{
        OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest
    };
    use serde_json::json;
    let raw = |data:serde_json::Value|serde_json::to_vec(&json!({
        "code":"0","msg":"","data":data
    })).unwrap();
    let stamped=chrono::Utc::now().timestamp_millis().to_string();
    let balance=raw(json!([{
        "totalEq":"100","adjEq":"100","uTime":stamped,
        "details":[{"ccy":"USDT","eq":"100","liab":"0",
            "availEq":"100","availBal":"100","eqUsd":"100","uTime":stamped}]
    }]));
    // This is B's FIRST signed balance source in the independent T10-03
    // cohort. Advancing an unregistered route falsely fails the fixture
    // before the production Worker post-fence can be exercised.
    feed.expect_immutable_signed_phase("t10-03-b-initial-balance",RecordedGet {
        access_key:"fixture-observer-b".into(),
        path:"/api/v5/account/balance".into(),
        required_query:vec![],raw_response:balance,
    });
    let signed_fee=raw(json!([{
        "instType":"SWAP","level":"Lv1","ts":stamped,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    feed.advance_recorded_route("fixture-observer-b","/api/v5/account/trade-fee",
        (0..12).map(|_|RecordedGet {
            access_key:"fixture-observer-b".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into())
            ],raw_response:signed_fee.clone()
        }).collect());
    feed.advance_recorded_public_clock(raw(json!([{"ts":stamped}])));
    // The A order has already been authenticated and acknowledged. The
    // next event belongs to B, not another periodic A finance/source cycle.
    // A's real capture/Status owner must remain runnable while B enters.
    // Its already ACK-bound signed order is immutable until a new source
    // event; blocking A's owner here would hide the concurrency contract.
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,4,"T10-03 starts after a real A order ACK");
    let key="t10_03_worker_post_auth_response_lost_0001";
    // Source event: the public B Execute observes NEW signed fee/time
    // bytes. No background GET may renew this timestamp or synthesize an
    // exchange/SQL/Worker state. The predecessor must have identical
    // exact-route/query raw bytes, or this transition fails closed.
    let execute_stamp=chrono::Utc::now().timestamp_millis().to_string();
    let observed_fee=raw(json!([{
        "instType":"SWAP","level":"Lv1","ts":execute_stamp,
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    let retired=feed.supersede_queued_signed_source_at_event(
        "t10-03-positive-execute","t10-03-b-fee-at-execute",
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
    assert!(retired>0,"T10-03 needs prior signed B fee source");
    feed.advance_recorded_public_clock(raw(json!([{"ts":execute_stamp}])));
    T10_03_WORKER_POST_COMMITTED_REPLY_LOST.store(
        false,std::sync::atomic::Ordering::SeqCst
    );
    T10_03_DROP_NEXT_POST_AUTH.store(
        true,std::sync::atomic::Ordering::SeqCst
    );
    let request=TradeRequest::Execute {
        idempotency_key:key.into(),operation:TradeOperation::Open,
        strategy_instance:"manual".into(),instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,
        sizing:Sizing::Contracts{contracts:"0.1".into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    assert!(request.valid());
    let answer=tokio::time::timeout(Duration::from_secs(23),
        trade.trade("fixture-b",&request,None)
    ).await.expect("T10-03 public Native completion deadline");
    let status=match &answer {
        Ok(reply)=>reply["status"].as_str().unwrap_or("UNVERIFIED").to_owned(),
        Err(_)=>"LOCAL_ERROR".to_owned(),
    };
    assert!(T10_03_WORKER_POST_COMMITTED_REPLY_LOST.load(
        std::sync::atomic::Ordering::SeqCst
    ),"T10-03 must lose the actual Worker POST_AUTHORIZED reply: public={status} outcome={answer:?} posts={} signed_denials={:?}",
        feed.outbound_posts.lock().unwrap().len(),
        feed.denied_routes.lock().unwrap().iter().rev().take(4).cloned().collect::<Vec<_>>()
    );
    assert!(!T10_03_DROP_NEXT_POST_AUTH.load(
        std::sync::atomic::Ordering::SeqCst
    ),"T10-03 fault was not exercised");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before,
        "an unreceived post fence cannot authorize even one new venue POST");
    let b=accounts.resolve("fixture-b").await.expect("T10-03 Native B owner");
    let (intent_id,sql_state,sql_revision,exchange_order,central,plan_hash)=
        b.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            |store| {
                let i=store.by_idempotency(key)?.ok_or(
                    okx_execution::StoreError::Invalid("T10_03_SQL_INTENT_MISSING")
                )?;
                let receipt=store.active_central_receipt(&i.intent_id)?;
                Ok((i.intent_id,i.state.as_str().to_owned(),i.revision,
                    i.exchange_order_id,receipt,i.plan_hash))
            }
        ).await.expect("T10-03 saved SQL readback");
    assert!(sql_revision>0);
    assert!(!matches!(sql_state.as_str(),"POSTING"|"FILLED"|"ACKNOWLEDGED"),
        "unreceived authorization cannot fabricate a Native venue POST");
    assert!(exchange_order.is_none());
    let central=central.expect("T10-03 must retain persisted central receipt");
    let inspect=RiskCommand {
        schema:RISK_COMMAND_SCHEMA_V1.into(),
        command_id:"inspect_t10_03_after_lost_post_reply_0001".into(),
        installation_id:"installation_test".into(),
        action:RiskCommandAction::Inspect {
            reservation_key:central.reservation_key.clone(),
            plan_sha256:plan_hash.clone(),
        }
    };
    let observed=risk.request(inspect).await.expect(
        "T10-03 Worker INSPECT must survive a lost prior authorization reply"
    );
    assert_eq!(observed.status,RiskDecisionStatus::Observed);
    assert_eq!(observed.reservation_state,Some(RiskReservationState::Posted),
        "BeginPost was durable, so this is POSTED, not HELD or RELEASED");
    assert_eq!(observed.reservation_key.as_deref(),
        Some(central.reservation_key.as_str()));
    let mut summary_cmd=summary("risk_t10_03_after_lost_fence");
    summary_cmd.installation_id="installation_test".into();
    let summary=risk.request(summary_cmd).await.expect(
        "T10-03 actual persisted Worker summary"
    );
    let entry=summary.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(entry.consumed_usd,"1.0005");
    assert_eq!(entry.reserved_usd,"8.002",
        "unresolved POSTED risk cannot receive an invented timeout release");
    let replay=trade.trade("fixture-b",&request,None).await
        .expect("T10-03 same-key Native replay must not create a new intent");
    let replay_id=replay["intent_id"].as_str()
        .or_else(||replay["data"]["intent"]["intent_id"].as_str())
        .or_else(||replay["data"]["intent_id"].as_str())
        .expect("T10-03 same durable intent reference");
    assert_eq!(replay_id,intent_id);
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before);
    json!({
        "schema":"okx.t10-03-native-post-auth-lost/v1",
        "account_uid":b.configuration.account_key.verified_uid,
        "intent_id":intent_id,"replay_intent_id":replay_id,
        "sql_state":sql_state,"sql_revision":sql_revision,
        "sql_exchange_order_id":exchange_order,
        "active_sql_receipt":true,
        "reservation_key":central.reservation_key,
        "central_state":"POSTED",
        "central_sequence":observed.risk_book_sequence,
        "lost_post_authorization":true,
        "public_status":status,
        "posts_before":posts_before,
        "posts_after":feed.outbound_posts.lock().unwrap().len(),
        "worker_budget":summary.budget,
        "worker_sequence":summary.risk_book_sequence,
    })
}
