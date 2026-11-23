//! One real process death after Native prepare, before reserve or BeginPost.
use super::UnifiedFinanceHandle;
use super::owner_financial_reply_tests::{INSTALLATION, account};
use super::owner_reserve_reply_tests::{cleanup, setup_before_reserve_attempt};
use crate::{IntentState, TradeIntentLookup};
use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

const INTENT: &str = "reply_intent_001";
const CHILD: &str = "OKX_NATIVE_PREPARED_KILL_CHILD";
const READY: &str = "NATIVE_PREPARED_KILL_READY=";

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[tokio::test]
async fn risk_book_contract_native_os_kill_prepared_before_reserve_rejects_unsent() {
    if std::env::var_os(CHILD).is_some() {
        let (root, owner, _request) = setup_before_reserve_attempt().await;
        let prepared = owner
            .lookup_intent(
                account(),
                TradeIntentLookup::IntentId(INTENT.into()),
                deadline(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prepared.state, IntentState::Prepared);
        // setup has committed original prepare and quote evidence, not reserve.
        let counts = owner
            .submit(|db| {
                Ok((
                    db.query_row::<i64, _, _>("SELECT count(*) FROM risk_entries", [], |r| {
                        r.get(0)
                    })?,
                    db.query_row::<i64, _, _>("SELECT count(*) FROM risk_receipts", [], |r| {
                        r.get(0)
                    })?,
                    db.query_row::<Option<i64>, _, _>(
                        "SELECT begin_post_at FROM intents WHERE intent_id=?1",
                        [INTENT],
                        |r| r.get(0),
                    )?,
                ))
            })
            .await
            .unwrap();
        assert_eq!(counts, (0, 0, None));
        println!("{READY}{}", root.display());
        std::io::stdout().flush().unwrap();
        std::future::pending::<()>().await;
        unreachable!();
    }
    let test = concat!(
        module_path!(),
        "::risk_book_contract_native_os_kill_prepared_before_reserve_rejects_unsent"
    );
    let test = test.strip_prefix("okx_trade_execution::").unwrap();
    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test, "--nocapture"])
            .env(CHILD, "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some((_, path)) = line.split_once(READY) {
                let _ = tx.send(std::path::PathBuf::from(path.trim()));
                break;
            }
        }
    });
    let root = rx
        .recv_timeout(Duration::from_secs(25))
        .expect("child did not reach committed prepare");
    assert!(child.0.try_wait().unwrap().is_none());
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    reader.join().unwrap();
    let owner = UnifiedFinanceHandle::open_native(root.clone(), INSTALLATION.into())
        .await
        .unwrap();
    let original = owner
        .lookup_intent(
            account(),
            TradeIntentLookup::IntentId(INTENT.into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.state, IntentState::Prepared);
    assert!(
        owner
            .operation(deadline(), account(), format!("risk-reserve:{INTENT}"))
            .await
            .unwrap()
            .is_none()
    );
    let counts = owner
        .submit(|db| {
            Ok((
                db.query_row::<i64, _, _>("SELECT count(*) FROM risk_entries", [], |r| r.get(0))?,
                db.query_row::<i64, _, _>("SELECT count(*) FROM risk_receipts", [], |r| r.get(0))?,
                db.query_row::<Option<i64>, _, _>(
                    "SELECT begin_post_at FROM intents WHERE intent_id=?1",
                    [INTENT],
                    |r| r.get(0),
                )?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(counts, (0, 0, None));

    assert!(
        owner
            .unsent_release_receipt(deadline(), account(), INTENT.into())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        owner
            .reject_unsent(
                deadline(),
                account(),
                INTENT.into(),
                "RECOVERY_UNSENT".into()
            )
            .await
            .unwrap()
    );
    assert!(
        !owner
            .reject_unsent(
                deadline(),
                account(),
                INTENT.into(),
                "DUPLICATE_RECOVERY".into()
            )
            .await
            .unwrap()
    );
    assert!(
        owner
            .unsent_before_reserve(deadline(), account(), INTENT.into())
            .await
            .unwrap()
    );
    assert!(
        owner
            .unsent_release_receipt(deadline(), account(), INTENT.into())
            .await
            .unwrap()
            .is_none()
    );
    let recovered = owner
        .lookup_intent(
            account(),
            TradeIntentLookup::IntentId(INTENT.into()),
            deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.state, IntentState::Rejected);
    assert_eq!(recovered.client_order_id, original.client_order_id);
    assert_eq!(recovered.request_hash, original.request_hash);
    let counts = owner
        .submit(|db| {
            Ok((
                db.query_row::<i64, _, _>("SELECT count(*) FROM risk_entries", [], |r| r.get(0))?,
                db.query_row::<i64, _, _>("SELECT count(*) FROM risk_receipts", [], |r| r.get(0))?,
                db.query_row::<Option<i64>, _, _>(
                    "SELECT begin_post_at FROM intents WHERE intent_id=?1",
                    [INTENT],
                    |r| r.get(0),
                )?,
            ))
        })
        .await
        .unwrap();
    assert_eq!(counts, (0, 0, None));
    cleanup(root, owner).await;
}
