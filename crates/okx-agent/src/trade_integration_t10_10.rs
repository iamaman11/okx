// T10-10, first causal leg only: genuine repeated signed fills-history and
// bills-archive are ingested by the EXISTING account finance owner. No fake
// finance entry, SQL mutation, Worker settlement or terminal state is built
// here. Late independent fee/rebate and bound breach remain separate gates.
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

async fn t10_10_signed_duplicate_ingestion(
    accounts: Arc<AccountRuntimeRegistry>,
    trade: Arc<TradeService>,
    risk: NativeRiskClient,
    feed: Arc<crate::accounts::recorded_support::RecordedAccountHttp>,
    intent_id: String,
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
        "late_fee_rebate_source_proven":false,
        "upper_bound_breach_proven":false,
    })
}
