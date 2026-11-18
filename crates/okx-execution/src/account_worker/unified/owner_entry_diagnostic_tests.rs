//! Historical entry-source inspection across a cold owner restart.
use super::{
    UnifiedFinanceHandle,
    operation_diagnostics::{NativeEntrySourceFailure, NativeEntrySourceKind},
    owner_financial_reply_tests::{INSTALLATION, account, campaign},
    owner_reserve_reply_tests::{cleanup, setup},
};
use std::time::{Duration, Instant};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[tokio::test]
async fn risk_book_contract_entry_diagnostic_reopens_original_sources_without_financial_effect() {
    let (root, owner, _request) = setup().await;
    let intent = "reply_intent_001";
    let money_before =
        serde_json::to_value(owner.campaign_summary(campaign()).await.unwrap()).unwrap();
    // Inspect the original persisted evidence; this is not a source recapture.
    let first = owner
        .inspect_intent_operation(deadline(), account(), intent.into())
        .await
        .unwrap();
    let quote = first.entry_quote.unwrap();
    let admission = first.entry_admission.unwrap();
    // Synthetic component evidence exercises persistence only; the separate
    // public Execute test proves the actual failed-source producer.
    let fact = NativeEntrySourceFailure {
        installation_id: INSTALLATION.into(),
        account_key: account(),
        intent_id: intent.into(),
        plan_hash: quote.plan_hash.clone(),
        phase: "POST_HELD_SOURCE_CHECK".into(),
        reason: "ENTRY_FEE_EXCHANGE_TIME_STALE".into(),
        source_kind: NativeEntrySourceKind::FeeExchange,
        source_version: quote.fee_generation.clone(),
        reference_version: quote.reference_generation.clone(),
        source_at_ms: quote.observed_at - 5001,
        checked_at_ms: quote.observed_at,
        max_age_ms: 5000,
    };
    let expected_failure = serde_json::to_value(&fact).unwrap();
    owner
        .operation_receipt(
            deadline(),
            account(),
            format!("entry-source-failure:{intent}"),
            quote.plan_hash.clone(),
            "ENTRY_SOURCE_FAILURE".into(),
            serde_json::to_string(&fact).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(quote.reference_generation, "reply_reference");
    assert_eq!(quote.conversion_digest, "b".repeat(64));
    assert_eq!(quote.intent_id, intent);
    assert_eq!(quote.plan_hash, admission.plan_hash);
    assert!(first.begin_post_at_ms.is_none());
    assert!(first.reservation.is_none());
    assert_eq!(
        money_before,
        serde_json::to_value(owner.campaign_summary(campaign()).await.unwrap()).unwrap()
    );

    // Drop the sole handle and wait for its writer to terminate before reopen.
    let mut stopped = owner.termination_signal();
    drop(owner);
    if !*stopped.borrow() {
        tokio::time::timeout(Duration::from_secs(5), stopped.changed())
            .await
            .unwrap()
            .unwrap();
    }
    assert!(*stopped.borrow());
    let owner = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    let reopened = owner
        .inspect_intent_operation(deadline(), account(), intent.into())
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(reopened.entry_source_failure.as_ref().unwrap()).unwrap(),
        expected_failure
    );
    assert_eq!(reopened.entry_quote, Some(quote));
    assert_eq!(reopened.entry_admission, Some(admission));
    assert!(reopened.begin_post_at_ms.is_none());
    assert!(reopened.reservation.is_none());
    assert!(
        owner
            .inspect_intent_operation(
                deadline(),
                format!("okx:real:{}", "b".repeat(64)),
                intent.into()
            )
            .await
            .is_err()
    );
    assert_eq!(
        money_before,
        serde_json::to_value(owner.campaign_summary(campaign()).await.unwrap()).unwrap()
    );
    cleanup(root, owner).await;
}
