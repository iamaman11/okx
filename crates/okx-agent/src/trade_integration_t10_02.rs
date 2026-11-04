// T10-02: a verified managed 0.50 long cannot authorize an unrelated
// +0.20 venue position. TradeService must reject additional exposure against
// the SAME authenticated account, SQLite owner and persisted Worker RiskBook.
async fn t10_02_unowned_exposure_denied(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    managed_intent: String,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    use okx_protocol::trading::{
        OrderConstraints, PositionSide, Protection, Sizing, TradeOperation, TradeRequest,
    };
    let wrap = |data: serde_json::Value| serde_json::to_vec(&serde_json::json!({
        "code":"0","msg":"","data":data
    })).unwrap();
    // The previous managed Add changes durable SQL/financial frontiers.
    // An unrelated external venue position must NOT be hidden behind a
    // campaign-readiness timeout from that earlier positive cohort. Wait
    // READ-ONLY until the real background maintenance owner has reconciled
    // BOTH account SQL frontiers. Do not call refresh/seal here, do not
    // mutate financial state, and never grant risk in this test fixture.
    let mut readiness_cmd=summary("risk_t10_02_wait_for_post_add_maintenance");
    readiness_cmd.installation_id="installation_test".into();
    let before_external=risk.request(readiness_cmd).await
        .expect("T10-02 real Worker campaign before signed external exposure");
    let origin=before_external.budget.as_ref()
        .and_then(|b|b.campaign_origin.as_ref())
        .expect("T10-02 real Worker campaign origin");
    let campaign=origin.campaign_id.clone();
    let from=i64::try_from(origin.started_at_ms)
        .expect("T10-02 campaign origin must fit signed SQL time");
    let mut last_failure=String::from("NOT_CHECKED");
    tokio::time::timeout(Duration::from_secs(9),async {
        loop {
            let through=chrono::Utc::now().timestamp_millis()-2_000;
            let mut complete=true;
            for alias in ["fixture-a","fixture-b"] {
                let account=accounts.resolve(alias).await
                    .expect("T10-02 independent account owner");
                let campaign=campaign.clone();
                match account.execution.store.call(
                    std::time::Instant::now()+Duration::from_secs(2),
                    move |s|s.verify_campaign_entry_readiness(
                        &campaign,from,through
                    ),
                ).await {
                    Ok(())=>{},
                    Err(reason)=>{
                        last_failure=format!("{alias}:{reason}");
                        complete=false;
                        break;
                    }
                }
            }
            if complete {break}
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.unwrap_or_else(|_|panic!(
        "T10_02_POST_ADD_NATIVE_SQL_READINESS_UNVERIFIED:{last_failure}"
    ));
    // Only now inject the different 0.70 authenticated position. All
    // exchange timestamps and fee/public proof remain newly observed.
    let fresh_at = chrono::Utc::now().timestamp_millis();
    let position = wrap(serde_json::json!([{
        "instType":"SWAP","instId":"BTC-USDT-SWAP","posSide":"long",
        "pos":"0.7","mgnMode":"cross","avgPx":"1000","markPx":"1000",
        "uTime":fresh_at.to_string()
    }]));
    // This is an external raw signed venue record, NOT a test modification
    // of the local lot, strategy owner, active receipt or central budget.
    let source_digest = format!("{:x}",sha2::Sha256::digest(&position));
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/positions",
        (0..16).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/positions".into(),
            required_query:vec![],raw_response:position.clone(),
        }).collect(),
    );
    let fee = wrap(serde_json::json!([{
        "instType":"SWAP","level":"Lv1","ts":fresh_at.to_string(),
        "feeGroup":[{"groupId":"4","maker":"0.0005","taker":"0.0005"}]
    }]));
    feed.advance_recorded_route(
        "fixture-observer-a","/api/v5/account/trade-fee",
        (0..8).map(|_|RecordedGet {
            access_key:"fixture-observer-a".into(),
            path:"/api/v5/account/trade-fee".into(),
            required_query:vec![
                ("instType".into(),"SWAP".into()),
                ("instFamily".into(),"BTC-USDT".into()),
            ],
            raw_response:fee.clone(),
        }).collect(),
    );
    feed.advance_recorded_public_clock(
        wrap(serde_json::json!([{"ts":fresh_at.to_string()}]))
    );
    let owner=accounts.resolve("fixture-a").await.expect("T10-02 SQL owner");
    let managed_for_sql=managed_intent.clone();
    let (managed_state,managed_revision,owned_lots)=owner.execution.store.call(
        std::time::Instant::now()+Duration::from_secs(3),
        move |s| {
            let intent=s.intent(&managed_for_sql)?
                .ok_or(okx_execution::StoreError::Invalid("T10_02_MANAGED_INTENT_ABSENT"))?;
            let owned=s.active_lots("BTC-USDT-SWAP")?.into_iter()
                .filter(|lot|lot.side=="long")
                .fold(Decimal::ZERO,|sum,lot|sum+lot.remaining_qty);
            Ok((intent.state.as_str().to_owned(),intent.revision,
                owned.normalize().to_string()))
        }
    ).await.expect("T10-02 SQL readback");
    assert_eq!(managed_state,"FILLED");
    assert_eq!(owned_lots,"0.5","the SQL owner cannot invent external lots");
    assert!(managed_revision>0);

    let mut before_cmd=summary("risk_t10_02_before_external_position");
    before_cmd.installation_id="installation_test".into();
    let before=risk.request(before_cmd).await.expect("T10-02 persisted Worker before");
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,5,"T10-02 follows one real managed Add");
    let request=TradeRequest::Execute {
        idempotency_key:"t10_02_external_unowned_add_0001".into(),
        operation:TradeOperation::Add,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),
        side:PositionSide::Long,sizing:Sizing::Contracts{contracts:"0.1".into()},
        order:OrderConstraints {
            order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0,
        },
        protection:Some(Protection::Price{stop_price:"900".into()}),
        expires_in_ms:30_000,
    };
    assert!(request.valid());
    let answer=tokio::time::timeout(Duration::from_secs(15),
        trade.trade("fixture-a",&request,None)
    ).await.expect("T10-02 public Execute deadline");
    let (reason,state,origin,send_state)=match answer {
        Ok(reply)=>(
            reply["data"]["primary_reason"].as_str().unwrap_or("NO_TYPED_REASON").to_owned(),
            reply["status"].as_str().unwrap_or("UNVERIFIED").to_owned(),
            reply["data"]["rejection_origin"].as_str().unwrap_or("UNVERIFIED").to_owned(),
            reply["data"]["send_state"].as_str().unwrap_or("UNVERIFIED").to_owned(),
        ),
        Err(reason)=>(reason,"ERROR".into(),"LOCAL".into(),"NOT_STARTED".into()),
    };
    let posts_after=feed.outbound_posts.lock().unwrap().len();
    let mut after_cmd=summary("risk_t10_02_after_external_position");
    after_cmd.installation_id="installation_test".into();
    let after=risk.request(after_cmd).await.expect("T10-02 persisted Worker after");
    let before_budget=before.budget.as_ref().expect("T10-02 before budget");
    let after_budget=after.budget.as_ref().expect("T10-02 after budget");
    // Source as-of timestamps and revisions may advance during background
    // observations. Compare independent economic invariants rather than
    // pretending that a fresh read must be byte-identical to an old snapshot.
    assert_eq!(before_budget.campaign_limit_usd,after_budget.campaign_limit_usd);
    assert_eq!(before_budget.consumed_loss_usd,after_budget.consumed_loss_usd);
    assert_eq!(before_budget.reserved_loss_usd,after_budget.reserved_loss_usd);
    assert_eq!(before_budget.remaining_loss_usd,after_budget.remaining_loss_usd);
    let before_entry=before_budget.entry_budget.as_ref().expect("T10-02 entry budget");
    let after_entry=after_budget.entry_budget.as_ref().expect("T10-02 entry budget after");
    assert_eq!(before_entry.limit_usd,after_entry.limit_usd);
    assert_eq!(before_entry.consumed_usd,after_entry.consumed_usd);
    assert_eq!(before_entry.reserved_usd,after_entry.reserved_usd);
    assert_eq!(before_entry.remaining_usd,after_entry.remaining_usd);
    assert_eq!(posts_after,posts_before,
        "unowned external position cannot lead to any venue POST");
    assert!(
        reason.contains("UNATTRIBUTED_EXCHANGE_POSITION")
            || reason.contains("COHORT_UNMANAGED_EXTERNAL_EXPOSURE"),
        "T10-02 must deny for true unowned exposure, not stale source: {reason} / {state} / {origin} / {send_state}"
    );
    assert!(
        !matches!(state.as_str(),"complete"|"accepted"),
        "the public wrapper cannot mark an unowned-risk Execute admitted"
    );
    serde_json::json!({
        "schema":"okx.t10-02-unowned-native-worker/v1",
        "account_uid":owner.configuration.account_key.verified_uid,
        "managed_intent":managed_intent,
        "managed_state":managed_state,
        "managed_sql_revision":managed_revision,
        "sql_owned_lots":owned_lots,
        "venue_signed_position":"0.7",
        "source_digest":source_digest,
        "denial_reason":reason,"public_state":state,
        "rejection_origin":origin,"send_state":send_state,
        "posts_before":posts_before,"posts_after":posts_after,
        "worker_before":before_budget,"worker_after":after_budget,
        "worker_sequence_before":before.risk_book_sequence,
        "worker_sequence_after":after.risk_book_sequence,
    })
}
