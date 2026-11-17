//! Extends the existing fresh public lifecycle AFTER its verified terminal
//! release. No synthetic release marker, separate database, or duplicate chain.
use super::filled_fixture::*;
use super::*;
use rust_decimal::Decimal;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}

fn adjustment(feed: &RecordedAccountHttp, bill: &str, at: i64, fee: &str) {
    for partition in ["SPOT", "MARGIN", "SWAP", "FUTURES", "OPTION"] {
        get(
            feed,
            "fixture-executor-a",
            "/api/v5/trade/fills-history",
            vec![("instType", partition)],
            json!([]),
        );
    }
    get(
        feed,
        "fixture-executor-a",
        "/api/v5/account/bills-archive",
        vec![],
        json!([{"billId":bill,"instType":"SWAP","instId":INSTRUMENT,
            "ordId":OPEN_ORDER,"clOrdId":"__REQUEST_CL_ORD_ID__","tradeId":"",
            "ccy":"USDT","type":"2","subType":"3","balChg":fee,
            "fee":fee,"pnl":"0","ts":at.to_string()}]),
    );
}

async fn operation(account: &AccountResources, id: String) -> okx_execution::OperationReceipt {
    account
        .execution
        .store
        .operation(
            deadline(),
            crate::execution_store::OperationId::new(id).unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn status(fixture: &NativeFixture, entry: &str, opened_at: i64, equity: &str) -> Value {
    // Exactly the ordinary Status observer and finance-capture snapshots.
    // The original completed cleanup receipt avoids another cancellation.
    order(&fixture.feed, OPEN_ORDER, "buy", opened_at);
    snapshot(&fixture.feed, equity, "0");
    snapshot(&fixture.feed, equity, "0");
    history(&fixture.feed, None);
    let status = fixture
        .trade
        .trade(
            &fixture.config.alias,
            &TradeRequest::Status {
                reference: entry.into(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(status["data"]["intent"]["intent_id"], entry, "{status}");
    assert_eq!(
        status["data"]["central_release"]["state"], "verified",
        "{status}"
    );
    assert_eq!(
        status["data"]["incremental_entry_settlement"]["state"], "verified_or_no_change",
        "{status}"
    );
    status
}

pub(super) async fn verify_post_release_correction(
    fixture: &NativeFixture,
    account: &AccountResources,
    entry: &str,
    opened_at: i64,
) -> Vec<(String, okx_execution::OperationReceipt)> {
    let key = fixture.config.account_key.wire();
    let original_terminal = operation(account, format!("entry-settlement:{entry}")).await;
    let original = serde_json::from_str::<okx_execution::EntrySettlementEvidence>(
        &original_terminal.result_ref,
    )
    .unwrap();
    assert_eq!(original.schema, "okx.entry-terminal-settlement/v1");
    assert_eq!(original.cumulative_entry_usd, Decimal::new(10005, 4));
    let release_key = format!("central-release:{}", original.reservation_key);
    let original_release = operation(account, release_key.clone()).await;
    assert_eq!(original_release.kind, "CENTRAL_RELEASE");
    let released = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(released.reserved_entry_usd, "0");
    assert_eq!(released.reserved_loss_usd, "0");
    assert_eq!(released.consumed_entry_usd, "1.0005");
    assert!(
        !account
            .execution
            .store
            .entry_late_pending(deadline(), entry.into())
            .await
            .unwrap()
    );
    let posts = fixture.feed.outbound_posts.lock().unwrap().clone();

    // An independently signed order-linked bill arrives AFTER the original
    // release, not as a rewrite of its fill or original terminal witness.
    let at = chrono::Utc::now().timestamp_millis();
    assert!(at > original.source_covered_through_ms);
    snapshot(&fixture.feed, "98.999", "0");
    adjustment(&fixture.feed, "native-post-release-fee", at, "-1");
    crate::source_capture::capture_account_sources_measured(
        &account.observer,
        &account.private_ws,
        &account.execution,
    )
    .await
    .unwrap();
    assert!(
        account
            .execution
            .store
            .entry_late_pending(deadline(), entry.into())
            .await
            .unwrap()
    );
    status(fixture, entry, opened_at, "98.999").await;
    assert!(
        !account
            .execution
            .store
            .entry_late_pending(deadline(), entry.into())
            .await
            .unwrap()
    );
    let late_key = format!("entry-late:{entry}:r{}", original.settlement_revision + 1);
    let late_receipt = operation(account, late_key.clone()).await;
    let late =
        serde_json::from_str::<okx_execution::EntrySettlementEvidence>(&late_receipt.result_ref)
            .unwrap();
    assert_eq!(late.schema, "okx.entry-late-settlement/v1");
    assert_eq!(late.settlement_revision, original.settlement_revision + 1);
    assert_eq!(late.cumulative_entry_usd, Decimal::new(20005, 4));
    assert_eq!(late.venue_filled_quantity, Decimal::new(1, 1));
    assert_eq!(late.reservation_key, original.reservation_key);
    assert_eq!(late.plan_hash, original.plan_hash);
    assert_eq!(late.venue_order_id, original.venue_order_id);
    assert_ne!(late.source_digest, original.source_digest);
    let corrected = fixture
        .owner
        .campaign_summary(fixture.campaign.clone())
        .await
        .unwrap();
    assert_eq!(corrected.consumed_entry_usd, "2.0005");
    assert_eq!(corrected.entry_bound_breach_usd, "1");
    assert_eq!(corrected.reserved_entry_usd, "0");
    assert_eq!(corrected.reserved_loss_usd, "0");
    assert_eq!(corrected.opening, released.opening);
    assert!(
        fixture
            .owner
            .active_lots(key.clone(), INSTRUMENT.into(), deadline())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .owner
            .reservation_receipt(
                key.clone(),
                entry.into(),
                okx_execution::CentralReceiptView::ActiveExposure,
                deadline()
            )
            .await
            .unwrap()
            .is_none()
    );

    // Same source ID and same immutable late revision cannot debit again.
    snapshot(&fixture.feed, "98.999", "0");
    adjustment(&fixture.feed, "native-post-release-fee", at, "-1");
    crate::source_capture::capture_account_sources_measured(
        &account.observer,
        &account.private_ws,
        &account.execution,
    )
    .await
    .unwrap();
    status(fixture, entry, opened_at, "98.999").await;
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        corrected
    );
    assert_eq!(
        serde_json::to_value(operation(account, late_key.clone()).await).unwrap(),
        serde_json::to_value(&late_receipt).unwrap()
    );

    // A separately signed positive rebate restores cash only. It cannot refund
    // consumed entry capacity, erase breach, or resurrect a released reserve.
    snapshot(&fixture.feed, "99.999", "0");
    adjustment(
        &fixture.feed,
        "native-post-release-rebate",
        chrono::Utc::now().timestamp_millis(),
        "1",
    );
    crate::source_capture::capture_account_sources_measured(
        &account.observer,
        &account.private_ws,
        &account.execution,
    )
    .await
    .unwrap();
    status(fixture, entry, opened_at, "99.999").await;
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        corrected
    );
    assert_eq!(
        serde_json::to_value(operation(account, late_key.clone()).await).unwrap(),
        serde_json::to_value(&late_receipt).unwrap()
    );
    assert_eq!(
        serde_json::to_value(operation(account, format!("entry-settlement:{entry}")).await)
            .unwrap(),
        serde_json::to_value(&original_terminal).unwrap()
    );
    assert_eq!(
        serde_json::to_value(operation(account, release_key.clone()).await).unwrap(),
        serde_json::to_value(&original_release).unwrap()
    );
    let mut fresh = execute();
    if let TradeRequest::Execute {
        idempotency_key, ..
    } = &mut fresh
    {
        *idempotency_key = "native_post_release_breach_new_open".into();
    }
    let denied = fixture
        .trade
        .trade(&fixture.config.alias, &fresh, Some(&fixture.authority))
        .await
        .unwrap();
    assert_eq!(
        denied["data"]["primary_reason"], "BUDGET_BOUND_BREACH",
        "{denied}"
    );
    assert_eq!(denied["data"]["send_state"], "NOT_STARTED");
    assert_eq!(
        fixture
            .owner
            .campaign_summary(fixture.campaign.clone())
            .await
            .unwrap(),
        corrected
    );
    assert_eq!(*fixture.feed.outbound_posts.lock().unwrap(), posts);
    assert_eq!(fixture.feed.unconsumed_reads(), 0);
    assert_eq!(fixture.feed.unconsumed_posts(), 0);
    assert!(fixture.feed.denied_routes.lock().unwrap().is_empty());
    vec![
        (format!("entry-settlement:{entry}"), original_terminal),
        (release_key, original_release),
        (late_key, late_receipt),
    ]
}

pub(super) async fn verify_reopened_correction(
    owner: &UnifiedFinanceHandle,
    campaign: &str,
    account: &str,
    receipts: Vec<(String, okx_execution::OperationReceipt)>,
) {
    let mut late = None;
    for (id, expected) in receipts {
        let actual = owner
            .operation(deadline(), account.into(), id.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&actual).unwrap(),
            serde_json::to_value(&expected).unwrap(),
            "reopened {id}"
        );
        if id.starts_with("entry-late:") {
            late = Some(
                serde_json::from_str::<okx_execution::EntrySettlementEvidence>(&actual.result_ref)
                    .unwrap(),
            );
        }
    }
    let proof = late.expect("same immutable post-release late revision survives reopen");
    let book = owner.campaign_summary(campaign.into()).await.unwrap();
    owner
        .settle_bound_entry(
            okx_execution::account_worker::UnifiedFinanceBoundSettlement {
                campaign: campaign.into(),
                reservation_key: proof.reservation_key,
                plan_sha256: proof.plan_hash,
                expected_sequence: book.book_sequence,
                cumulative_entry_usd: proof.cumulative_entry_usd.normalize().to_string(),
                settlement_revision: proof.settlement_revision,
                source_digest: proof.source_digest,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        owner.campaign_summary(campaign.into()).await.unwrap(),
        book,
        "post-reopen exact late replay must not debit again or refund capacity"
    );
}
