//! A real busy FIFO cannot turn a timed-out read into an absent intent. This
//! component uses original typed prepare; public cold-reopen diagnostics have
//! separate caller witnesses. No test-only public owner hook is introduced.
use super::{
    UnifiedFinanceHandle,
    owner_financial_reply_tests::{INSTALLATION, account, fresh},
};
use crate::{Decision, IntentPlan, NewIntent, StoreError};
use rust_decimal::Decimal;
use std::{
    future::Future,
    sync::mpsc,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
async fn close(owner: UnifiedFinanceHandle) {
    let mut signal = owner.termination_signal();
    drop(owner);
    tokio::time::timeout(Duration::from_secs(5), signal.wait_for(|v| *v))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn risk_book_contract_native_diagnostic_busy_deadline_preserves_identity_across_reopen() {
    let (root, owner) = fresh().await;
    let definition = owner
        .ensure_manual_instance(deadline(), account())
        .await
        .unwrap();
    let at = super::opening_ingress::owner_now_ms().unwrap();
    let id = "native_busy_diagnostic_001";
    owner
        .prepare_admission(
            deadline(),
            account(),
            Decision {
                decision_id: "native_busy_decision_001".into(),
                instance_id: None,
                decision_at: at,
                input_digest: "d".repeat(64),
                policy_revision: 1,
                code_version: "a".repeat(40),
            },
            NewIntent {
                intent_id: id.into(),
                idempotency_key: "native_busy_request_001".into(),
                request_hash: "f".repeat(64),
                client_order_id: "nativebusyclient001".into(),
                deadline: at + 30_000,
                plan_hash: "e".repeat(64),
                policy_revision: 1,
                risk_increase: true,
                instrument_id: "BTC-USDT-SWAP".into(),
                plan: IntentPlan {
                    strategy_definition_id: definition.definition_id,
                    strategy_definition_version: definition.version,
                    strategy_parameters_digest: definition.parameters_digest,
                    contract_value: Decimal::ONE,
                    settlement_currency: "USDT".into(),
                    position_side: "long".into(),
                    operation: "open".into(),
                    side: "buy".into(),
                    quantity: Decimal::ONE,
                    limit_price: Decimal::from(100),
                    strategy_instance: "manual".into(),
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    let before = owner
        .inspect_intent_operation(deadline(), account(), id.into())
        .await
        .unwrap();
    assert!(before.intent.is_some());
    let before = serde_json::to_value(before.intent).unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut blocker = Box::pin(owner.submit(move |_| {
        entered_tx.send(()).unwrap();
        release_rx
            .recv_timeout(Duration::from_secs(5))
            .map_err(|_| StoreError::Invalid("TEST_DIAGNOSTIC_BARRIER_TIMEOUT"))?;
        Ok(())
    }));
    assert!(matches!(
        blocker
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    entered_rx.await.unwrap();
    let until = Instant::now() + Duration::from_millis(20);
    // Exactly the public diagnostic's read-only call and bounded-await policy.
    // Timeout is uncertainty, never an empty successful SQL snapshot.
    let diagnostic = tokio::time::timeout_at(
        until.into(),
        owner.inspect_intent_operation(until, account(), id.into()),
    )
    .await;
    assert!(diagnostic.is_err());
    release_tx.send(()).unwrap();
    blocker.await.unwrap();
    let after = owner
        .inspect_intent_operation(deadline(), account(), id.into())
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(after.intent).unwrap(), before);
    assert!(after.begin_post_at_ms.is_none());
    assert!(after.reservation.is_none());
    close(owner).await;
    let reopened = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    let cold = reopened
        .inspect_intent_operation(deadline(), account(), id.into())
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(cold.intent).unwrap(), before);
    assert!(cold.begin_post_at_ms.is_none());
    assert!(cold.reservation.is_none());
    close(reopened).await;
    assert_eq!(
        root.parent().unwrap().canonicalize().unwrap(),
        std::env::temp_dir().canonicalize().unwrap()
    );
    assert!(
        root.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("okx-fresh-financial-reply-")
    );
    std::fs::remove_dir_all(root).unwrap();
}
