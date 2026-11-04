// Recorded T10-14: real A SQLite owner blocked; B still reads and cancels
// its own ACKed order through the PUBLIC TradeService and the same Worker DO.
async fn t10_14_b_public_cancel_while_a_sql_held(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    b_intent: String,
    b_client: String,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedPost;
    use okx_protocol::trading::TradeRequest;

    let a_owner=accounts.resolve("fixture-a").await.expect("T10-14 account A");
    let a_key=a_owner.configuration.account_key.wire();
    let b_owner=accounts.resolve("fixture-b").await.expect("T10-14 account B");
    let b_key=b_owner.configuration.account_key.wire();
    assert_ne!(a_key,b_key);
    let posts_before=feed.outbound_posts.lock().unwrap().len();
    assert_eq!(posts_before,5,"T10-14 starts after two real A/B creates");
    feed.expect_post(RecordedPost {
        access_key:"fixture-executor-b".into(),
        path:"/api/v5/trade/cancel-order".into(),
        expected_body:serde_json::json!({
            "instId":"BTC-USDT-SWAP","clOrdId":b_client.clone()
        }),
        raw_response:serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"",
            "data":[{"ordId":"recorded-b1-b-001","clOrdId":b_client,"sCode":"0"}]
        })).unwrap(),
    });

    let (release_tx,release_rx)=std::sync::mpsc::channel::<()>();
    let (entered_tx,entered_rx)=tokio::sync::oneshot::channel::<()>();
    let a_task=tokio::spawn(async move {
        a_owner.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(17),
            move |_sql| {
                let _=entered_tx.send(());
                release_rx.recv_timeout(Duration::from_secs(15))
                    .map_err(|_|okx_execution::StoreError::Invalid("T10_14_A_GATE_NOT_RELEASED"))?;
                Ok(())
            }
        ).await
    });
    tokio::time::timeout(Duration::from_secs(3),entered_rx).await
        .expect("T10-14 A SQL must enter hold")
        .expect("T10-14 A SQL signal missing");
    let status=tokio::time::timeout(Duration::from_secs(5),
        trade.trade("fixture-b",&TradeRequest::Status{
            reference:b_intent.clone()
        },None)
    ).await;
    let read_posts=feed.outbound_posts.lock().unwrap().len();

    // This is a new recorded external time observation BEFORE the mutation;
    // the production clock skew and 5000ms age gates remain untouched.
    let observed=chrono::Utc::now().timestamp_millis().to_string();
    feed.advance_recorded_public_clock(
        serde_json::to_vec(&serde_json::json!({
            "code":"0","msg":"","data":[{"ts":observed}]
        })).unwrap()
    );
    let cancel=tokio::time::timeout(Duration::from_secs(6),
        trade.trade("fixture-b",&TradeRequest::Cancel{
            idempotency_key:"t10_14_b_owned_cancel_0001".into(),
            reference:b_intent.clone()
        },None)
    ).await;
    let posts_after=feed.outbound_posts.lock().unwrap().len();

    let sql_id=b_intent.clone();
    let b_sql=tokio::time::timeout(Duration::from_secs(3),
        b_owner.execution.store.call(
            std::time::Instant::now()+Duration::from_secs(3),
            move |sql| {
                let i=sql.intent(&sql_id)?.ok_or(
                    okx_execution::StoreError::Invalid("T10_14_B_INTENT_MISSING")
                )?;
                Ok((i.state.as_str().to_owned(),i.revision,
                    sql.active_central_receipt(&i.intent_id)?.is_some()))
            }
        )
    ).await;
    let mut cmd=summary("risk_t10_14_b_cancel_while_a_held");
    cmd.installation_id="installation_test".into();
    let worker=tokio::time::timeout(Duration::from_secs(3),risk.request(cmd)).await;

    // No ASSERT or early return is permitted before releasing A's worker.
    release_tx.send(()).expect("T10-14 release held A SQL");
    tokio::time::timeout(Duration::from_secs(4),a_task).await
        .expect("T10-14 A SQL join deadline").expect("T10-14 A SQL task join")
        .expect("T10-14 A SQL result");
    let observed_status=status.expect("T10-14 B Status cannot wait for A")
        .expect("T10-14 B Status production read");
    assert_eq!(observed_status["data"]["intent"]["intent_id"],b_intent);
    assert_eq!(read_posts,posts_before,"T10-14 B read cannot send a POST");
    let observed_cancel=cancel.expect("T10-14 B Cancel cannot wait for A")
        .expect("T10-14 B Cancel production operation");
    assert!(observed_cancel["status"]=="partial" || observed_cancel["status"]=="unknown",
        "a cancel ACK cannot invent terminal financial release: {observed_cancel}");
    assert_eq!(posts_after,posts_before+1,"T10-14 exactly one venue cancel");
    assert_eq!(feed.unconsumed_posts(),0,"T10-14 cancel ACK consumed");
    let (sql_state,sql_revision,held)=b_sql.expect("B SQL deadline while A held")
        .expect("B SQL independent owner");
    assert_eq!(sql_state,"ACKNOWLEDGED","cancel ACK is not terminal");
    assert!(held,"B risk remains HELD without independent terminal proof");
    let result=worker.expect("Worker must respond while A SQL held")
        .expect("Worker risk summary");
    let budget=result.budget.as_ref().expect("T10-14 budget");
    let entry=budget.entry_budget.as_ref().expect("T10-14 campaign entry");
    assert_eq!(entry.consumed_usd,"1.0005");
    assert_eq!(entry.reserved_usd,"8.002");
    assert_eq!(entry.remaining_usd,"0.9975");
    serde_json::json!({
        "schema":"okx.t10-14-native-worker-isolation/v1",
        "account_a":a_key,"account_b":b_key,
        "b_intent_id":b_intent,
        "b_status_intent_id":observed_status["data"]["intent"]["intent_id"],
        "a_sql_held_during_b_cancel":true,
        "b_status_while_a_held":true,
        "b_cancel_status":observed_cancel["status"],
        "b_cancel_ack_without_terminal_release":true,
        "a_sql_rejoined":true,
        "b_sql_state":sql_state,"b_sql_revision":sql_revision,
        "b_risk_held":held,"outbound_posts_before":posts_before,
        "read_posts":read_posts,"outbound_posts_after":posts_after,
        "new_b_cancel_posts":posts_after-posts_before,
        "worker_budget":budget,"worker_sequence":result.risk_book_sequence
    })
}
