//! Production operation + actual account SQLite owner. Scripted risk replies
//! verify orchestration/identity, not Worker economics or OS-crash acceptance.
use super::*;
use crate::risk_bridge::OutboundRiskCommand;
use okx_execution::{
    AccountMeta, CentralReceipt, Decision, IntentPlan, IntentState, NewIntent, OpeningSnapshot,
};
use okx_protocol::risk_authority::{RISK_RESULT_SCHEMA_V1, RiskCommand, RiskResult};
use rust_decimal::Decimal;
use serde_json::json;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::mpsc;

static NEXT: AtomicUsize = AtomicUsize::new(0);
const ID: &str = "intent_unsent";
const KEY: &str = "reservation_unsent";
const INSTALLATION: &str = "installation_test";
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
fn expired() -> Instant {
    Instant::now() - Duration::from_secs(1)
}

struct Fixture {
    store: AccountStoreHandle,
    intent: StoredIntent,
    governance: RwLock<Option<CentralGovernanceSnapshot>>,
    path: PathBuf,
    account: String,
}
impl Fixture {
    async fn new(reject: bool, local: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "okx-unsent-release-{}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_micros(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("account.db");
        let account = format!("okx:real:{}", "a".repeat(64));
        let store = AccountStoreHandle::initialize_flat(
            path.clone(),
            AccountMeta {
                account_key: account.clone(),
                installation_id: INSTALLATION.into(),
                initialized_at: 1,
                opening_source: "fixture_flat".into(),
                initialize_operation_id: "initialize_fixture".into(),
                release_id: "a".repeat(40),
            },
            OpeningSnapshot {
                source_ref: "fixture_flat".into(),
                observed_at: 1,
                opening_equity_usd: Decimal::from(100),
                positions: 0,
                orders: 0,
                conditional_orders: 0,
                liabilities: Decimal::ZERO,
                balances: vec![("USD".into(), Decimal::from(100))],
            },
        )
        .await
        .unwrap();
        let wire = account.clone();
        let intent = store
            .call(deadline(), move |s| {
                s.apply_authority_policy(1, false, "campaign_fixture", Decimal::from(20))?;
                let definition = s.ensure_manual_instance(1)?;
                let saved = s.prepare(
                    &Decision {
                        decision_id: "decision_unsent".into(),
                        instance_id: None,
                        decision_at: 2,
                        input_digest: "d".repeat(64),
                        policy_revision: 1,
                        code_version: "fixture".into(),
                    },
                    &NewIntent {
                        intent_id: ID.into(),
                        idempotency_key: "command_unsent".into(),
                        request_hash: "b".repeat(64),
                        client_order_id: "c".repeat(32),
                        deadline: 100,
                        plan_hash: "d".repeat(64),
                        policy_revision: 1,
                        risk_increase: !local,
                        instrument_id: "BTC-USDT-SWAP".into(),
                        plan: IntentPlan {
                            strategy_definition_id: definition.definition_id,
                            strategy_definition_version: definition.version,
                            strategy_parameters_digest: definition.parameters_digest,
                            operation: if local { "close" } else { "open" }.into(),
                            side: "buy".into(),
                            position_side: "long".into(),
                            quantity: Decimal::ONE,
                            limit_price: Decimal::from(5),
                            stop_price: Some(Decimal::from(4)),
                            strategy_instance: "manual".into(),
                            planned_loss_usd: Decimal::ONE,
                            source_generation: 1,
                            private_source_generation: 1,
                            contract_value: Decimal::ONE,
                            inverse: false,
                            settlement_currency: "USDT".into(),
                        },
                    },
                )?;
                if local {
                    s.reserve(
                        &CentralReceipt {
                            reservation_key: format!("local:{ID}"),
                            account_key: wire,
                            installation_id: INSTALLATION.into(),
                            instance_id: None,
                            intent_id: ID.into(),
                            plan_hash: saved.plan_hash.clone(),
                            policy_revision: 1,
                            receipt_hash: "e".repeat(64),
                            requested_loss_usd: Decimal::ZERO,
                            conversion_digest: "f".repeat(64),
                        },
                        3,
                    )?;
                } else {
                    s.mark_reserve_attempt(ID, KEY, 3)?;
                }
                if reject {
                    s.reject_unsent(ID, "RESERVE_REPLY_LOST", 4)?;
                }
                Ok(s.intent(ID)?.unwrap())
            })
            .await
            .unwrap();
        let authority: CentralGovernanceSnapshot = serde_json::from_value(json!({
            "state":{"schema":"okx.governance/v3","revision":1,"installation_id":INSTALLATION,
                "campaign_id":format!("sha256:{}","a".repeat(64)),"campaign_loss_limit_usd":"20",
                "max_total_modelled_loss_usdt":"5","accounts":[]},
            "issued_at_ms":1792910000000_u64,"session_generation":673,
        }))
        .unwrap();
        assert!(authority.valid());
        Self {
            store,
            intent,
            governance: RwLock::new(Some(authority)),
            path,
            account,
        }
    }
    async fn release(
        &self,
        risk: &NativeRiskClient,
    ) -> Result<UnsentReleaseOutcome, UnsentReleaseError> {
        release_unsent(
            Environment::Real,
            &self.intent,
            UnsentReleaseDependencies {
                store: &self.store,
                risk,
                governance: &self.governance,
                sql_deadline: deadline,
            },
        )
        .await
    }
    async fn ack(&self) -> Option<String> {
        self.store
            .call(deadline(), |s| {
                Ok(s.operation(&format!("central-release:{KEY}"))?
                    .map(|r| r.result_ref))
            })
            .await
            .unwrap()
    }
    async fn reopen(self) -> Self {
        let Self {
            store,
            intent,
            governance,
            path,
            account,
        } = self;
        let mut ended = store.termination_signal();
        drop(store);
        tokio::time::timeout(Duration::from_secs(3), ended.wait_for(|v| *v))
            .await
            .unwrap()
            .unwrap();
        let store =
            AccountStoreHandle::open_existing(path.clone(), account.clone(), INSTALLATION.into())
                .await
                .unwrap();
        Self {
            store,
            intent,
            governance,
            path,
            account,
        }
    }
    async fn finish(self) {
        let mut ended = self.store.termination_signal();
        let root = self.path.parent().unwrap().to_path_buf();
        drop(self);
        tokio::time::timeout(Duration::from_secs(3), ended.wait_for(|v| *v))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root.parent(), Some(std::env::temp_dir().as_path()));
        assert!(
            root.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("okx-unsent-release-")
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[derive(Clone, Copy)]
enum Reply {
    Held,
    Posted,
    Released,
    Open,
    Absent,
    Updated,
    Stale,
    Denied,
    Lost,
    MissingSequence,
}
fn response(command: &RiskCommand, reply: Reply) -> RiskResult {
    let result = RiskResult {
        schema: RISK_RESULT_SCHEMA_V1.into(),
        command_id: command.command_id.clone(),
        status: match reply {
            Reply::Updated => RiskDecisionStatus::Updated,
            Reply::Absent | Reply::Stale | Reply::Denied => RiskDecisionStatus::Denied,
            _ => RiskDecisionStatus::Observed,
        },
        reason: match reply {
            Reply::Absent => Some("RISK_RESERVATION_NOT_FOUND".into()),
            Reply::Stale => Some("STALE_SESSION_OR_BOOK_SEQUENCE".into()),
            Reply::Denied => Some("TEST_RISK_DENIAL".into()),
            _ => None,
        },
        reservation_key: Some(KEY.into()),
        risk_book_sequence: if matches!(reply, Reply::MissingSequence) {
            None
        } else {
            Some(7)
        },
        policy_revision: Some(1),
        existing_modelled_loss_usdt: None,
        reservation_state: match reply {
            Reply::Held | Reply::MissingSequence => Some(RiskReservationState::Held),
            Reply::Posted => Some(RiskReservationState::Posted),
            Reply::Released => Some(RiskReservationState::Released),
            Reply::Open => Some(RiskReservationState::Open),
            _ => None,
        },
        budget: None,
        account_budget: None,
    };
    assert_eq!(result.valid(), !matches!(reply, Reply::MissingSequence));
    result
}
fn assert_command(command: &RiskCommand, expected: &str) {
    assert_eq!(command.installation_id, INSTALLATION);
    match &command.action {
        RiskCommandAction::Inspect {
            reservation_key,
            plan_sha256,
        } => {
            assert_eq!(expected, "inspect");
            assert_eq!(reservation_key, KEY);
            assert_eq!(plan_sha256, &"d".repeat(64));
        }
        RiskCommandAction::ReleaseUnsent {
            reservation_key,
            plan_sha256,
            expected_risk_book_sequence,
            proof,
        } => {
            assert_eq!(expected, "release");
            assert_eq!(reservation_key, KEY);
            assert_eq!(plan_sha256, &"d".repeat(64));
            assert_eq!(*expected_risk_book_sequence, 7);
            assert_eq!(proof.intent_id, ID);
            assert_eq!(proof.reservation_key, KEY);
            assert_eq!(proof.account_key, format!("okx:real:{}", "a".repeat(64)));
            assert_eq!(proof.request_hash, "b".repeat(64));
            assert_eq!(proof.reason, "RESERVE_REPLY_LOST");
            assert_eq!(proof.plan_hash, *plan_sha256);
            assert_eq!(proof.record_digest.len(), 64);
        }
        _ => panic!("unsent release must not reserve, post or settle"),
    }
    assert_eq!(command.command_id, format!("{expected}_{}", "c".repeat(32)));
}
fn scripted(
    steps: Vec<(&'static str, Reply)>,
) -> (NativeRiskClient, tokio::task::JoinHandle<Vec<RiskCommand>>) {
    let (tx, mut rx) = mpsc::channel::<OutboundRiskCommand>(16);
    let task = tokio::spawn(async move {
        let mut calls = Vec::new();
        for (action, reply) in steps {
            let outbound = rx.recv().await.expect("expected risk command");
            assert_command(&outbound.command, action);
            if !matches!(reply, Reply::Lost) {
                outbound
                    .return_to
                    .send(response(&outbound.command, reply))
                    .unwrap();
            }
            calls.push(outbound.command);
        }
        assert!(
            rx.recv().await.is_none(),
            "unexpected additional risk command"
        );
        calls
    });
    (NativeRiskClient::new(tx), task)
}
async fn finish_risk(
    risk: NativeRiskClient,
    task: tokio::task::JoinHandle<Vec<RiskCommand>>,
) -> Vec<RiskCommand> {
    drop(risk);
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn ur01_missing_sql_proof_does_not_contact_worker() {
    let f = Fixture::new(false, false).await;
    let (risk, task) = scripted(vec![]);
    let error = f.release(&risk).await.unwrap_err();
    assert_eq!(error.stage, UnsentReleaseStage::SqlProof);
    assert_eq!(error.reason_code, "DURABLE_UNSENT_SQL_PROOF_MISSING");
    assert_eq!(error.reservation_key, None);
    assert!(f.ack().await.is_none());
    assert!(finish_risk(risk, task).await.is_empty());
    f.finish().await;
}

#[tokio::test]
async fn ur02_authority_failure_keeps_known_identity_and_reason() {
    let f = Fixture::new(true, false).await;
    *f.governance.write().await = None;
    let (risk, task) = scripted(vec![]);
    let error = f.release(&risk).await.unwrap_err();
    assert_eq!(
        serde_json::to_value(&error).unwrap(),
        json!({"stage":"authority",
        "reason_code":"AUTHORITY_UNAVAILABLE","intent_id":ID,"reservation_key":KEY})
    );
    assert!(f.ack().await.is_none());
    finish_risk(risk, task).await;
    f.finish().await;
}

#[tokio::test]
async fn ur03_held_release_and_replay_keep_first_durable_ack() {
    let f = Fixture::new(true, false).await;
    let (risk, task) = scripted(vec![
        ("inspect", Reply::Held),
        ("release", Reply::Updated),
        ("inspect", Reply::Released),
    ]);
    assert_eq!(
        f.release(&risk).await.unwrap(),
        UnsentReleaseOutcome::CentralAcknowledged
    );
    let first = f.ack().await.unwrap();
    assert_eq!(
        f.release(&risk).await.unwrap(),
        UnsentReleaseOutcome::CentralAcknowledged
    );
    assert_eq!(f.ack().await, Some(first));
    let calls = finish_risk(risk, task).await;
    assert_eq!(calls.len(), 3);
    assert_eq!(
        f.store
            .call(deadline(), |s| s.reconcilable_intents(16))
            .await
            .unwrap()
            .len(),
        0
    );
    assert!(
        !f.store
            .call(deadline(), |s| s.begin_post(ID, 1, 6))
            .await
            .unwrap()
    );
    f.finish().await;
}

#[tokio::test]
async fn ur04_lost_release_reply_reopens_sql_and_inspects_without_resubmitting_release() {
    let f = Fixture::new(true, false).await;
    let (risk, task) = scripted(vec![("inspect", Reply::Held), ("release", Reply::Lost)]);
    let error = f.release(&risk).await.unwrap_err();
    assert_eq!(error.stage, UnsentReleaseStage::Release);
    assert_eq!(error.reason_code, "RISK_TRANSPORT_LOST_DO_NOT_REPLAY");
    assert!(f.ack().await.is_none());
    finish_risk(risk, task).await;
    let f = f.reopen().await;
    let (risk, task) = scripted(vec![("inspect", Reply::Released)]);
    f.release(&risk).await.unwrap();
    assert!(f.ack().await.is_some());
    assert_eq!(finish_risk(risk, task).await.len(), 1);
    f.finish().await;
}

#[tokio::test]
async fn ur05_stale_cas_reinspects_and_stops_after_four_attempts() {
    let f = Fixture::new(true, false).await;
    let (risk, task) = scripted(vec![
        ("inspect", Reply::Held),
        ("release", Reply::Stale),
        ("inspect", Reply::Held),
        ("release", Reply::Updated),
    ]);
    f.release(&risk).await.unwrap();
    assert_eq!(finish_risk(risk, task).await.len(), 4);
    f.finish().await;
    let f = Fixture::new(true, false).await;
    let (risk, task) =
        scripted(vec![("inspect", Reply::Held), ("release", Reply::Stale)].repeat(4));
    let error = f.release(&risk).await.unwrap_err();
    assert_eq!(error.stage, UnsentReleaseStage::Release);
    assert_eq!(error.reason_code, "CENTRAL_UNSENT_RELEASE_CAS_BUSY");
    assert!(f.ack().await.is_none());
    assert_eq!(finish_risk(risk, task).await.len(), 8);
    f.finish().await;
}

#[tokio::test]
async fn ur06_inspection_failures_never_release_or_acknowledge() {
    for (reply, reason) in [
        (Reply::Open, "UNSENT_CENTRAL_STATE_UNRESOLVED"),
        (Reply::Denied, "TEST_RISK_DENIAL"),
        (Reply::Lost, "RISK_TRANSPORT_LOST_DO_NOT_REPLAY"),
        (Reply::MissingSequence, "RISK_RESPONSE_IDENTITY_MISMATCH"),
    ] {
        let f = Fixture::new(true, false).await;
        let (risk, task) = scripted(vec![("inspect", reply)]);
        let error = f.release(&risk).await.unwrap_err();
        assert_eq!(error.stage, UnsentReleaseStage::Inspect);
        assert_eq!(error.reason_code, reason);
        assert!(f.ack().await.is_none());
        assert_eq!(finish_risk(risk, task).await.len(), 1);
        f.finish().await;
    }
}

#[tokio::test]
async fn ur07_posted_and_absent_reservations_preserve_existing_no_send_release_contract() {
    for reply in [Reply::Posted, Reply::Absent] {
        let f = Fixture::new(true, false).await;
        let (risk, task) = scripted(vec![("inspect", reply), ("release", Reply::Updated)]);
        f.release(&risk).await.unwrap();
        assert!(f.ack().await.is_some());
        finish_risk(risk, task).await;
        f.finish().await;
    }
}

#[tokio::test]
async fn ur08_sql_ack_failure_is_not_reported_as_worker_failure_or_success() {
    let f = Fixture::new(true, false).await;
    let sql = rusqlite::Connection::open(&f.path).unwrap();
    sql.execute_batch(
        "CREATE TRIGGER reject_test_ack BEFORE INSERT ON operation_receipts
        WHEN NEW.kind='CENTRAL_RELEASE' BEGIN SELECT RAISE(ABORT,'TEST_SQL_ACK_UNAVAILABLE'); END;",
    )
    .unwrap();
    let (risk, task) = scripted(vec![
        ("inspect", Reply::Held),
        ("release", Reply::Updated),
        ("inspect", Reply::Released),
    ]);
    let error = f.release(&risk).await.unwrap_err();
    assert_eq!(error.stage, UnsentReleaseStage::SqlAck);
    assert!(error.reason_code.contains("TEST_SQL_ACK_UNAVAILABLE"));
    assert!(f.ack().await.is_none());
    sql.execute_batch("DROP TRIGGER reject_test_ack").unwrap();
    drop(sql);
    f.release(&risk).await.unwrap();
    assert!(f.ack().await.is_some());
    assert_eq!(finish_risk(risk, task).await.len(), 3);
    f.finish().await;
}

#[tokio::test]
async fn ur09_local_and_demo_paths_never_contact_worker() {
    let f = Fixture::new(true, true).await;
    let (risk, task) = scripted(vec![]);
    assert_eq!(
        f.release(&risk).await.unwrap(),
        UnsentReleaseOutcome::LocalAcknowledged
    );
    assert!(
        f.store
            .call(deadline(), |s| s
                .operation(&format!("central-release:local:{ID}")))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        release_unsent(
            Environment::Demo,
            &f.intent,
            UnsentReleaseDependencies {
                store: &f.store,
                risk: &risk,
                governance: &f.governance,
                sql_deadline: expired,
            }
        )
        .await
        .unwrap(),
        UnsentReleaseOutcome::NotApplicable
    );
    assert!(finish_risk(risk, task).await.is_empty());
    f.finish().await;
}

#[tokio::test]
async fn ur10_concurrent_callers_share_sql_identity_and_resolve_stale_release_by_inspection() {
    let f = Fixture::new(true, false).await;
    let (tx, mut rx) = mpsc::channel::<OutboundRiskCommand>(16);
    let risk = NativeRiskClient::new(tx);
    let task = tokio::spawn(async move {
        // Both callers observe HELD before either can release. No sleeps and
        // no duplicated risk implementation: these are ordered response facts.
        let first = rx.recv().await.unwrap();
        let second = rx.recv().await.unwrap();
        for out in [first, second] {
            assert_command(&out.command, "inspect");
            out.return_to
                .send(response(&out.command, Reply::Held))
                .unwrap();
        }
        let mut calls = Vec::new();
        for reply in [Reply::Updated, Reply::Stale, Reply::Released] {
            let out = rx.recv().await.unwrap();
            assert_command(
                &out.command,
                if matches!(reply, Reply::Released) {
                    "inspect"
                } else {
                    "release"
                },
            );
            out.return_to.send(response(&out.command, reply)).unwrap();
            calls.push(out.command);
        }
        assert!(rx.recv().await.is_none());
        calls
    });
    let (a, b) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(f.release(&risk), f.release(&risk))
    })
    .await
    .unwrap();
    assert_eq!(a.unwrap(), UnsentReleaseOutcome::CentralAcknowledged);
    assert_eq!(b.unwrap(), UnsentReleaseOutcome::CentralAcknowledged);
    assert!(f.ack().await.is_some());
    assert_eq!(finish_risk(risk, task).await.len(), 3);
    assert!(
        f.store
            .call(deadline(), |s| s.reconcilable_intents(16))
            .await
            .unwrap()
            .is_empty()
    );
    f.finish().await;
}
