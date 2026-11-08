// T10-10 uses only signed HTTP input and public reconciliation. All monetary
// witnesses are read back from the existing Native SQL and Worker owners.
async fn t10_10_unknown_fee_hold(
    accounts: Arc<AccountRuntimeRegistry>, trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent_id: String, event_at: i64,
) -> serde_json::Value {
    let owner=accounts.resolve("fixture-a").await.unwrap();
    let mut cmd=summary("risk_t10_10_unknown_fee_before");
    cmd.installation_id="installation_test".into();
    let before=risk.request(cmd).await.unwrap();
    let observed=tokio::time::timeout(Duration::from_secs(9),async {
        loop {
            let id=intent_id.clone();
            let row=owner.execution.store.call(
                std::time::Instant::now()+Duration::from_secs(3),move |s| {
                    let f=s.cursor("fills","SWAP")?.unwrap();
                    let b=s.cursor("bills","account")?.unwrap();
                    Ok((s.intent_fill_cost_evidence(&id,f.watermark),
                        s.intent_fee_bill_evidence(&id,b.watermark),f.watermark,b.watermark))
                }).await.unwrap();
            if let (Ok(fill),Err(okx_execution::StoreError::Invalid(reason)),fw,bw)=row {
                let source_complete = fill.fill_count==1 && fw>=event_at && bw>=event_at;
                if source_complete {
                    // The archive cursor is complete, but the mandatory
                    // fill-linked bill itself is absent from that archive.
                    assert_eq!(reason,"ENTRY_FEE_BILL_SOURCE_MISMATCH");
                    break serde_json::json!({"reason":reason,"fill_count":fill.fill_count,
                        "quantity":fill.filled_quantity.normalize().to_string(),
                        "fill_watermark":fw,"bill_watermark":bw});
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("T10_10_UNKNOWN_FEE_SOURCE_NOT_OBSERVED");
    let status=trade.trade("fixture-a",&okx_protocol::trading::TradeRequest::Status {
        reference:intent_id.clone(),
    },None).await.unwrap();
    assert_eq!(status["data"]["financial_complete"],false);
    let mut cmd=summary("risk_t10_10_unknown_fee_after");
    cmd.installation_id="installation_test".into();
    let after=risk.request(cmd).await.unwrap();
    let a=before.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    let b=after.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(a.consumed_usd,b.consumed_usd);
    assert_eq!(a.reserved_usd,b.reserved_usd);
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),4);
    serde_json::json!({"intent_id":intent_id,"source_event_at_ms":event_at,
        "sql":observed,"worker_before":before.budget,"worker_after":after.budget,
        "venue_posts":4,"financial_complete":false})
}
async fn t10_10_native_sql_economics(
    accounts: &Arc<AccountRuntimeRegistry>,
    intent_id: &str,
    fixed_window: Option<(i64, i64)>,
) -> serde_json::Value {
    let owner = accounts.resolve("fixture-a").await.expect("T10-10 SQL owner");
    let target = intent_id.to_owned();
    owner.execution.store.call(
        std::time::Instant::now() + Duration::from_secs(3),
        move |s| {
            let fills = s.cursor("fills", "SWAP")?.ok_or(
                okx_execution::StoreError::Invalid("T10_10_FILL_FRONTIER_MISSING")
            )?;
            let bills = s.cursor("bills", "account")?.ok_or(
                okx_execution::StoreError::Invalid("T10_10_BILL_FRONTIER_MISSING")
            )?;
            // Evidence digests are intentionally bound to the caller's
            // through-window, not merely to economic amounts. Comparing a
            // new moving watermark to an older window is a false claim of
            // duplicate money. Prove the SAME original signed fee/fill
            // window while separately observing that live cursors advanced.
            let (fill_through,bill_through)=fixed_window
                .unwrap_or((fills.watermark,bills.watermark));
            if fills.watermark<fill_through || bills.watermark<bill_through {
                return Err(okx_execution::StoreError::Invalid(
                    "T10_10_FIXED_PROOF_WINDOW_NOT_COVERED"
                ));
            }
            let fill = s.intent_fill_cost_evidence(&target, fill_through)?;
            let bill = s.intent_fee_bill_evidence(&target, bill_through)?;
            Ok(serde_json::json!({
                "intent_id":target,
                "fill_count":fill.fill_count,
                "bill_count":bill.linked_bill_count,
                "quantity":fill.filled_quantity.normalize().to_string(),
                "gross_notional_usdt":fill.gross_notional_usdt.normalize().to_string(),
                "verified_fee_usdt":bill.conservative_verified_fee_usdt.normalize().to_string(),
                "bill_source_digest":bill.source_digest,
                "fill_watermark":fills.watermark,
                "bill_watermark":bills.watermark,
                "fill_proof_through":fill_through,
                "bill_proof_through":bill_through,
                "finance_revision":s.finance_revision()?,
            }))
        }
    ).await.expect("T10-10 real persisted Native SQL finance readback")
}

async fn t10_10_late_released_fee(
    accounts: Arc<AccountRuntimeRegistry>, trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent_id: String, original_source_time: String,
) -> serde_json::Value {
    use crate::accounts::recorded_support::RecordedGet;
    let owner=accounts.resolve("fixture-a").await.unwrap();
    let read=|id:String| {
        let store=owner.execution.store.clone();
        async move { store.call(std::time::Instant::now()+Duration::from_secs(3),move |s| {
            let terminal=s.operation(&format!("entry-settlement:{id}"))?.unwrap();
            let proof:okx_execution::EntrySettlementEvidence=
                serde_json::from_str(&terminal.result_ref).unwrap();
            let late=s.operation(&format!("entry-late-head:{id}"))?;
            let admission=s.entry_admission(&id)?.unwrap();
            Ok(serde_json::json!({"terminal":proof,"late":late.map(|r|
                serde_json::from_str::<serde_json::Value>(&r.result_ref).unwrap()),
                "reservation_active":s.active_central_receipt(&id)?.is_some(),"entry_upper_usd":admission.entry_upper_usd,
                "pending":s.entry_late_pending(&id)?,"finance_revision":s.finance_revision()?}))
        }).await.unwrap() }
    };
    let before=read(intent_id.clone()).await;
    assert_eq!(before["reservation_active"],false);
    // SQL receipts preserve Decimal scale; compare exact amounts, not rendering.
    let amount=|value:&serde_json::Value| value.as_str().unwrap()
        .parse::<rust_decimal::Decimal>().expect("exact SQL decimal");
    assert_eq!(amount(&before["terminal"]["cumulative_entry_usd"]),Decimal::new(10005,4));
    assert_eq!(amount(&before["entry_upper_usd"]),Decimal::new(10005,4));
    let raw=|rows:serde_json::Value|serde_json::to_vec(
        &serde_json::json!({"code":"0","msg":"","data":rows})).unwrap();
    // Replay the original signed terminal venue fact; neither timestamp nor
    // amount is obtained from the monetary proof under test.
    feed.advance_recorded_route("fixture-executor-a","/api/v5/trade/order",
        (0..5).map(|_| RecordedGet {
            access_key:"fixture-executor-a".into(),path:"/api/v5/trade/order".into(),
            required_query:vec![("instId".into(),"BTC-USDT-SWAP".into())],
            raw_response:raw(serde_json::json!([{
                "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-opening-001",
                "clOrdId":"__REQUEST_CL_ORD_ID__","side":"buy","posSide":"long",
                "tdMode":"cross","ordType":"limit","px":"1000","sz":"0.1",
                "accFillSz":"0.1","avgPx":"1000","state":"filled",
                "cTime":original_source_time,"uTime":original_source_time
            }])),
        }).collect());
    let event_at=chrono::Utc::now().timestamp_millis();
    // Both orders were already terminal in the original independently signed
    // T09.3 inventory. The new late-bill event does not invent an order-state
    // change: advance the SAME account/route/query from the earlier bounded
    // phase, retaining its actual delivery count instead of mixing queued
    // pages with a new immutable source.
    let prior_pending_reads = feed.advance_immutable_signed_phase_bounded(
        "t09-3-terminal-pending-orders",
        "t10-10-late-bill-no-pending-orders",
        RecordedGet {access_key:"fixture-observer-a".into(),
            path:"/api/v5/trade/orders-pending".into(),
            required_query:vec![("limit".into(),"100".into())],
            raw_response:raw(serde_json::json!([]))},
        12,Duration::from_secs(12),
    );
    assert!(prior_pending_reads<=8,
        "T10-10 must transition from the finite eight-read terminal source");
    let bill=raw(serde_json::json!([{
        "billId":"t10-10-independent-late-fee-001","instType":"SWAP",
        "instId":"BTC-USDT-SWAP","ordId":"recorded-t09-opening-001",
        "tradeId":"","clOrdId":"__REQUEST_CL_ORD_ID__",
        "ccy":"USDT","type":"2","subType":"0","balChg":"-0.0001",
        "fee":"-0.0001","pnl":"0","ts":event_at.to_string()
    }]));
    let source_sha256=format!("{:x}",sha2::Sha256::digest(&bill));
    feed.advance_recorded_route("fixture-executor-a","/api/v5/account/bills-archive",
        (0..12).map(|_| RecordedGet {
            access_key:"fixture-executor-a".into(),path:"/api/v5/account/bills-archive".into(),
            required_query:vec![],raw_response:bill.clone(),
        }).collect());
    // Await the SQL ingestion boundary before one public reconciliation.
    // Polling Status would repeatedly capture the same account/network state.
    tokio::time::timeout(Duration::from_secs(9),async {
        loop {
            let sql=read(intent_id.clone()).await;
            if sql["pending"]==true {break;}
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("T10_10_LATE_BILL_NOT_INGESTED");
    let status=trade.trade("fixture-a",&okx_protocol::trading::TradeRequest::Status {
        reference:intent_id.clone(),
    },None).await.unwrap();
    let after=read(intent_id.clone()).await;
    assert_eq!(after["pending"],false,"late settlement remains pending: {status}");
    let mut cmd=summary("risk_t10_10_late_fee_observed");
    cmd.installation_id="installation_test".into();
    let worker=risk.request(cmd).await.unwrap();
    assert_eq!(worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap().consumed_usd,"4.0021");
    assert_eq!(amount(&after["late"]["cumulative_entry_usd"]),Decimal::new(10006,4));
    assert!(after["late"]["settlement_revision"].as_u64().unwrap()>
        before["terminal"]["settlement_revision"].as_u64().unwrap());
    assert_eq!(after["late"]["reservation_key"],before["terminal"]["reservation_key"]);
    let entry=worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(entry.reserved_usd,"4");
    assert_eq!(entry.bound_breach_usd,"0.0001");
    // The same source is still queued: public replay must neither debit the
    // late revision again nor release another intent's reserved four dollars.
    trade.trade("fixture-a",&okx_protocol::trading::TradeRequest::Status {
        reference:intent_id.clone(),
    },None).await.unwrap();
    let replay=read(intent_id.clone()).await;
    assert_eq!(replay["late"],after["late"]);
    let mut cmd=summary("risk_t10_10_late_revision_replay");
    cmd.installation_id="installation_test".into();
    let replay_worker=risk.request(cmd).await.unwrap();
    let replay_entry=replay_worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(replay_entry.consumed_usd,"4.0021");
    assert_eq!(replay_entry.reserved_usd,"4");
    assert_eq!(replay_entry.bound_breach_usd,"0.0001");
    use okx_protocol::trading::{TradeRequest,TradeOperation,Sizing,OrderConstraints,
        PositionSide,Protection};
    let denied=trade.trade("fixture-a",&TradeRequest::Execute {
        idempotency_key:"t10_10_after_actual_bound_breach_0001".into(),
        operation:TradeOperation::Open,strategy_instance:"manual".into(),
        instrument:"BTC-USDT-SWAP".into(),side:PositionSide::Long,
        sizing:Sizing::Contracts {contracts:"0.1".into()},
        order:OrderConstraints {order_type:"limit".into(),limit_price:Some("1000".into()),
            reference:None,max_entry_usd:Some("10".into()),max_slippage_bps:0},
        protection:Some(Protection::Price {stop_price:"900".into()}),expires_in_ms:30_000,
    },None).await.expect("T10-10 post-breach public Execute outcome");
    assert_eq!(denied["data"]["state"],"REJECTED","{denied}");
    assert_eq!(denied["data"]["send_state"],"NOT_STARTED","{denied}");
    assert_eq!(denied["data"]["primary_reason"],"BUDGET_BOUND_BREACH","{denied}");
    let mut cmd=summary("risk_t10_10_after_bound_denial");
    cmd.installation_id="installation_test".into();
    let denied_worker=risk.request(cmd).await.unwrap();
    let denied_entry=denied_worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(denied_entry.consumed_usd,"4.0021");
    assert_eq!(denied_entry.reserved_usd,"4");
    assert_eq!(denied_entry.bound_breach_usd,"0.0001");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(),4);
    serde_json::json!({"intent_id":intent_id,"source_bill_id":"t10-10-independent-late-fee-001",
        "source_event_at_ms":event_at,"source_sha256":source_sha256,
        "before":before,"after":after,"replay":replay,
        "worker_after":worker.budget,"worker_replay":replay_worker.budget,
        "worker_sequence_after":worker.risk_book_sequence,
        "worker_sequence_replay":replay_worker.risk_book_sequence,
        "worker_sequence_after_denial":denied_worker.risk_book_sequence,
        "new_risk_denial":denied,"worker_after_denial":denied_worker.budget,"venue_posts":4})
}

async fn t10_10_signed_duplicate_ingestion(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent_id: String,
    released_intent_id: String,
    original_source_time: String,
) -> serde_json::Value {
    let before = t10_10_native_sql_economics(&accounts, &intent_id, None).await;
    assert_eq!(before["fill_count"], 2);
    assert_eq!(before["bill_count"], 2);
    assert_eq!(before["quantity"], "0.3");
    assert_eq!(before["verified_fee_usdt"], "0.0015");
    let request_counts = || {
        let log = feed.requests.lock().unwrap();
        (
            log.iter().filter(|r|r.starts_with(
                "fixture-executor-a:/api/v5/trade/fills-history?")).count(),
            log.iter().filter(|r|r.starts_with(
                "fixture-executor-a:/api/v5/account/bills-archive?")).count(),
        )
    };
    let requests_before = request_counts();
    // T09.3 already published 12 finite, byte-identical source rounds.
    // Wait for the REAL finance capture loop to consume another raw GET from
    // each source; counting previously observed pages is not a replay.
    let requests_after = tokio::time::timeout(Duration::from_secs(9), async {
        loop {
            let count = request_counts();
            if count.0 > requests_before.0 && count.1 > requests_before.1 {
                break count;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }).await.expect("T10_10_SIGNED_REPLAY_NOT_CONSUMED_BY_FINANCE_OWNER");
    let status = trade.trade(
        "fixture-a",
        &okx_protocol::trading::TradeRequest::Status {
            reference: intent_id.clone(),
        }, None,
    ).await.expect("T10-10 public same-intent Status after duplicate capture");
    assert_eq!(status["data"]["intent"]["intent_id"], intent_id);
    let fill_through=before["fill_proof_through"].as_i64()
        .expect("T10-10 initial immutable fill proof cutoff");
    let bill_through=before["bill_proof_through"].as_i64()
        .expect("T10-10 initial immutable bill proof cutoff");
    let after = t10_10_native_sql_economics(
        &accounts, &intent_id, Some((fill_through,bill_through))
    ).await;
    assert!(after["fill_watermark"].as_i64().unwrap()>=fill_through);
    assert!(after["bill_watermark"].as_i64().unwrap()>=bill_through);
    assert_eq!(after["fill_proof_through"],before["fill_proof_through"]);
    assert_eq!(after["bill_proof_through"],before["bill_proof_through"]);
    for field in ["fill_count", "bill_count", "quantity", "gross_notional_usdt",
                  "verified_fee_usdt", "bill_source_digest"] {
        assert_eq!(after[field], before[field],
            "duplicate signed HTTP source must not debit again: {field}");
    }
    assert!(after["finance_revision"].as_i64().unwrap() >=
            before["finance_revision"].as_i64().unwrap(),
            "Native finance revision must not roll back after source replay");
    let mut check = summary("risk_t10_10_after_duplicate_signed_fills_bills");
    check.installation_id="installation_test".into();
    let worker = risk.request(check).await.expect("T10-10 actual Worker risk readback");
    let entry = worker.budget.as_ref().unwrap().entry_budget.as_ref().unwrap();
    assert_eq!(entry.consumed_usd, "4.002");
    assert_eq!(entry.reserved_usd, "4");
    assert_eq!(feed.outbound_posts.lock().unwrap().len(), 4,
        "duplicate signed fill/bill must not create a second order POST");
    let late=t10_10_late_released_fee(
        accounts,trade,risk,feed.clone(),released_intent_id,original_source_time
    ).await;
    serde_json::json!({
        "schema":"okx.t10-10-signed-finance-duplicate/v1",
        "same_native_sql_owner":true,
        "intent_id":intent_id,
        "source_reads_before": {
            "fills":requests_before.0,"bills":requests_before.1,
        },
        "source_reads_after": {
            "fills":requests_after.0,"bills":requests_after.1,
        },
        "before":before,"after":after,
        "worker_budget":worker.budget,
        "risk_book_sequence":worker.risk_book_sequence,
        "venue_posts":feed.outbound_posts.lock().unwrap().len(),
        "late_released_fee":late,
    })
}
