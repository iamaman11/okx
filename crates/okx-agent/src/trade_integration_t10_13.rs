// T10-13: Worker STOP via the real persisted governance CAS AFTER the
// source-bound fill. The new risk key is denied; stale reduction sources block
// without exchange POST, then the existing T09.1 Close runs after restored
// signed venue positions with the paused account remaining paused.
async fn t10_13_stop_and_source_blocker(
    handoff_path:&str,
    accounts:Arc<AccountRuntimeRegistry>,
    trade:Arc<TradeService>,
    risk:NativeRiskClient,
    feed:Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    opening_intent:String,
)->serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::trading::{
        OrderConstraints,PositionSide,Protection,Sizing,TradeOperation,TradeRequest,
    };
    use serde_json::json;
    std::fs::write(
        format!("{handoff_path}.t10-stop-after-fill-ready"),
        b"REAL_FILL_AND_BILL_ACCOUNT_STATUS_CAPTURED"
    ).expect("T10-13 signed fill handoff marker");
    // No test-local state change. Wait only for the actual Worker PolicySnapshot
    // to reach both Native governance and AccountStore owners.
    let caps=tokio::time::timeout(Duration::from_secs(18),async {
        loop {
            let state=trade.trade("fixture-a",&TradeRequest::Capabilities{},None)
                .await.expect("T10-13 Native capabilities");
            if state["governance"]["explicit_pause"]==true
                &&state["native_sql_policy"]["matches_governance"]==true
                &&state["governance"]["revision"]==json!(3)
                &&state["native_sql_policy"]["revision"]==json!(3) {
                break state;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("T10-13 persisted STOP was not delivered to Native SQL");
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,1,"T10-13 starts after one real opening POST");
    let attempt=TradeRequest::Execute {
        idempotency_key:"t10_13_new_claim_after_stop_0001".into(),
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,sizing:Sizing::Contracts{contracts:"0.1".into()},
        order:OrderConstraints{
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    assert!(attempt.valid());
    let denied=trade.trade("fixture-a",&attempt,None).await
        .expect_err("T10-13 STOP must deny any new risk claim");
    assert_eq!(denied,"ACCOUNT_NEW_RISK_PAUSED");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),posts_before);
    let owner=accounts.resolve("fixture-a").await.expect("T10-13 SQL owner");
    let unclaimed=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        |sql|sql.by_idempotency("t10_13_new_claim_after_stop_0001")
    ).await.expect("T10-13 new claim SQL check");
    assert!(unclaimed.is_none(),"STOP cannot create a latent prepared risk claim");

    let wrap=|rows:serde_json::Value|serde_json::to_vec(&json!({
        "code":"0","msg":"","data":rows
    })).unwrap();
    let malformed=wrap(json!(null));
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/positions",
        (0..8).map(|_|RecordedGet{
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],raw_response:malformed.clone(),
        }).collect(),
    );
    let old_posts=feed.outbound_posts.lock().unwrap().len();
    let reads_before={
        let log=feed.requests.lock().unwrap();
        log.iter().filter(|x|x.starts_with("fixture-observer-a:/api/v5/account/positions?")).count()
    };
    let stale=TradeRequest::Close {
        idempotency_key:"t10_13_unverified_reduce_source_0001".into(),
        reference:opening_intent.clone(),expires_in_ms:6000,
    };
    let negative=tokio::time::timeout(Duration::from_secs(9),
        trade.trade("fixture-a",&stale,None)
    ).await.expect("T10-13 stale reduction must terminate");
    let refusal=match negative{
        Err(reason)=>reason,
        Ok(value)=> {
            assert_ne!(value["status"],"accepted",
                "partial positions must never authorize a real reduction");
            value["data"]["primary_reason"].as_str().unwrap_or("SOURCE_UNVERIFIED").to_owned()
        }
    };
    let reads_after={
        let log=feed.requests.lock().unwrap();
        log.iter().filter(|x|x.starts_with("fixture-observer-a:/api/v5/account/positions?")).count()
    };
    assert!(reads_after>reads_before,
        "T10-13 negative reduction must actually consume malformed signed source");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),old_posts,
        "STOP recovery cannot send a reduction without positions source");
    assert_ne!(refusal,"ACCOUNT_NEW_RISK_PAUSED",
        "STOP is not the cause that blocks a risk-reducing order");
    // No SQL projection or risk budget is altered by the test. Only newly
    // authenticated raw venue positions are available to the ordinary Close.
    let stamp=chrono::Utc::now().timestamp_millis().to_string();
    let restored=wrap(json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"long",
        "pos":"0.1","mgnMode":"cross","avgPx":"1000","markPx":"1000",
        "uTime":stamp
    }]));
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/positions",
        (0..24).map(|_|RecordedGet{
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],raw_response:restored.clone(),
        }).collect(),
    );
    let opening_key=opening_intent.clone();
    let opening_sql_revision=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s|s.intent(&opening_key)?.map(|i|i.revision)
            .ok_or(okx_execution::StoreError::Invalid("T10_13_OPENING_SQL_MISSING"))
    ).await.expect("T10-13 original persisted SQL revision");
    assert!(opening_sql_revision>0);
    let mut inspect=summary("risk_t10_13_stop_before_close");
    inspect.installation_id="installation_test".into();
    let worker=risk.request(inspect).await.expect("T10-13 risk remains held for Close");
    assert!(worker.budget.as_ref().unwrap().reserved_loss_usd!="0",
        "STOP must not release the filled protected position");
    json!({
        "schema":"okx.t10-13-native-stop-after-fill/v1",
        "account_uid":owner.configuration.account_key.verified_uid,
        "intent_id":opening_intent,
        "opening_sql_revision":opening_sql_revision,
        "source_fault_reads":reads_after-reads_before,
        "stale_reduce_blocker":refusal,
        "stop_governance_revision":caps["governance"]["revision"],
        "native_sql_policy_matches":caps["native_sql_policy"]["matches_governance"],
        "new_claim_denial":denied,
        "posts_before":posts_before,
        "posts_after_denied_reduce":feed.outbound_posts.lock().unwrap().len(),
        "source_recovered_digest":format!("{:x}",sha2::Sha256::digest(&restored)),
        "worker_budget_before_close":worker.budget,
        "worker_sequence":worker.risk_book_sequence,
    })
}
