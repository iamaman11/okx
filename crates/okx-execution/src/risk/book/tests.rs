//! Real on-disk SQLite tests. No Cloudflare, simulated exchange or REAL account.
use super::store::*;
use okx_kernel::risk::{model::RiskProposal, money::RiskAmount, rules::RiskRuleError};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    path: PathBuf,
}
impl Fixture {
    fn new(label: &str) -> Self {
        let number = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "okx-riskbook-{label}-{}-{number}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("risk-book.sqlite");
        Self { root, path }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn amt(v: &str) -> RiskAmount {
    RiskAmount::parse(v).unwrap()
}
fn uid(first: char) -> String {
    first.to_string().repeat(64)
}
fn account(first: char) -> String {
    format!("okx:real:{}", uid(first))
}
fn opening() -> CampaignOpening {
    CampaignOpening {
        installation_id: "test-installation".to_owned(),
        campaign_id: format!("sha256:{}", uid('c')),
        policy_revision: 1,
        generation: 7,
        global_limit: amt("10"),
        loss_limit: amt("100"),
        entry_limit: amt("100"),
        accounts: ['a', 'b']
            .into_iter()
            .map(|letter| AccountPolicy {
                account_key: account(letter),
                account_limit: amt("10"),
                order_limit: amt("8"),
                strategy_id: "strategy_test".to_owned(),
                strategy_limit: amt("8"),
            })
            .collect(),
    }
}
fn census() -> VerifiedCensus {
    VerifiedCensus {
        complete: true,
        generation: 7,
        source_digest: uid('d'),
        generated_at_ms: 10_000,
        read_floor_ms: 9_995,
        occupied: BTreeMap::from([(account('a'), amt("0")), (account('b'), amt("0"))]),
    }
}
fn request(letter: char, intent: &str, loss: &str) -> ReserveInput {
    ReserveInput {
        venue: "okx".to_string(),
        uid: uid(letter),
        run_id: "run_000001".into(),
        intent_id: intent.to_owned(),
        strategy_id: "strategy_test".into(),
        instrument_id: "BTC-USDT-SWAP".into(),
        plan_sha256: uid('e'),
        entry_proof_digest: uid('f'),
        conversion_digest: uid('a'),
        conversion_observed_at_ms: 9_999,
        conversion_rate: amt("1"),
        proposal: RiskProposal {
            loss_usdt: amt(loss),
            loss_usd: amt(loss),
            entry_upper_usd: amt("1"),
        },
        policy_revision: 1,
        now_ms: 10_000,
    }
}
fn open(f: &Fixture) -> RiskBookStore {
    RiskBookStore::open_existing(&f.path, "test-installation", &opening().campaign_id).unwrap()
}
#[test]
fn risk_book_contract_sql_concurrent_accounts_share_one_budget() {
    let f = Fixture::new("two-accounts");
    RiskBookStore::initialize_fixture(&f.path, &opening()).unwrap();
    let mut s = open(&f);
    let first = s
        .reserve(&request('a', "intent0001", "6"), &census())
        .unwrap();
    assert_eq!(first.book_sequence, 1);
    assert!(!first.replayed);
    let second = s.reserve(&request('b', "intent0002", "6"), &census());
    assert!(matches!(
        second,
        Err(RiskStoreError::Denied(
            RiskRuleError::InsufficientAggregateOrAccountRisk
        ))
    ));
    let read = s.readback().unwrap();
    assert_eq!(read.global_reserved_usdt, "6");
    assert_eq!(read.reserved_loss_usd, "6");
    assert_eq!(read.reserved_entry_usd, "1");
    assert_eq!(read.book_sequence, 1);
    drop(s);
    let mut recovered = open(&f);
    assert_eq!(recovered.readback().unwrap(), read);
    assert_eq!(
        recovered
            .reserve(&request('b', "intent0002", "4"), &census())
            .unwrap()
            .book_sequence,
        2
    );
}
#[test]
fn risk_book_contract_sql_ack_lost_replay_is_read_only() {
    let f = Fixture::new("replay");
    RiskBookStore::initialize_fixture(&f.path, &opening()).unwrap();
    let mut s = open(&f);
    let input = request('a', "intent0003", "3");
    let key = input.reservation_key();
    let first = s.reserve(&input, &census()).unwrap();
    assert_eq!(first.reservation_key, key);
    // Simulate a response lost *after* SQLite COMMIT: only persisted state survives.
    drop(first);
    drop(s);
    let mut reopened = open(&f);
    let replay = reopened.reserve(&input, &census()).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.book_sequence, 1);
    let altered = request('a', "intent0003", "4");
    assert!(matches!(
        reopened.reserve(&altered, &census()),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    assert_eq!(reopened.readback().unwrap().global_reserved_usdt, "3");
}
#[test]
fn risk_book_contract_sql_single_writer_and_no_implicit_init() {
    let f = Fixture::new("lease");
    assert!(matches!(
        RiskBookStore::open_existing(&f.path, "test-installation", "missing"),
        Err(RiskStoreError::Uninitialized)
    ));
    RiskBookStore::initialize_fixture(&f.path, &opening()).unwrap();
    let store = open(&f);
    assert!(matches!(
        RiskBookStore::open_existing(&f.path, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::WriterBusy)
    ));
    drop(store);
    assert!(matches!(
        RiskBookStore::open_existing(&f.path, "other", &opening().campaign_id),
        Err(RiskStoreError::IdentityMismatch)
    ));
    assert!(open(&f).readback().is_ok());
    assert!(matches!(
        RiskBookStore::initialize_fixture(&f.path, &opening()),
        Err(RiskStoreError::Invalid("EXISTING_RISK_BOOK"))
    ));
}
#[test]
fn risk_book_contract_sql_unproven_source_and_stale_revision_fail_closed() {
    let f = Fixture::new("source");
    RiskBookStore::initialize_fixture(&f.path, &opening()).unwrap();
    let mut s = open(&f);
    let input = request('a', "intent0004", "1");
    let mut incomplete = census();
    incomplete.occupied.remove(&account('b'));
    assert!(matches!(
        s.reserve(&input, &incomplete),
        Err(RiskStoreError::CensusUnproven)
    ));
    let mut untrusted = census();
    untrusted.complete = false;
    assert!(matches!(
        s.reserve(&input, &untrusted),
        Err(RiskStoreError::CensusUnproven)
    ));
    let mut stale = census();
    stale.generation = 6;
    assert!(matches!(
        s.reserve(&input, &stale),
        Err(RiskStoreError::StaleGeneration)
    ));
    let mut revision = input.clone();
    revision.policy_revision = 2;
    assert!(matches!(
        s.reserve(&revision, &census()),
        Err(RiskStoreError::PolicyConflict)
    ));
    let mut stale_clock = input.clone();
    stale_clock.now_ms = 16_001;
    stale_clock.conversion_observed_at_ms = 16_000;
    assert!(matches!(
        s.reserve(&stale_clock, &census()),
        Err(RiskStoreError::CensusUnproven)
    ));
    assert_eq!(s.readback().unwrap().book_sequence, 0);
}

#[test]
fn risk_book_contract_sql_child_post_commit_wait() {
    let Ok(db) = std::env::var("OKX_510_TEST_KILL_DB") else {
        return;
    };
    let path = PathBuf::from(db);
    let mut book =
        RiskBookStore::open_existing(&path, "test-installation", &opening().campaign_id).unwrap();
    book.reserve(&request('a', "intentkill1", "3"), &census())
        .unwrap();
    use std::io::Write;
    println!("RISK_SQL_COMMITTED_WITHOUT_REPLY");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(60));
    unreachable!("parent must terminate this OS process after commit");
}

#[test]
fn risk_book_contract_sql_os_kill_after_durable_commit() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    let fixture = Fixture::new("physical-kill");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "book::tests::risk_book_contract_sql_child_post_commit_wait",
            "--nocapture",
        ])
        .env("OKX_510_TEST_KILL_DB", &fixture.path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line
                .unwrap_or_default()
                .contains("RISK_SQL_COMMITTED_WITHOUT_REPLY")
            {
                let _ = tx.send(());
                return;
            }
        }
    });
    let committed = rx.recv_timeout(Duration::from_secs(15));
    child.kill().unwrap();
    let _ = child.wait().unwrap();
    reader.join().unwrap();
    assert!(
        committed.is_ok(),
        "SQLite commit witness not observed before forced kill"
    );
    // No simulated task cancellation: the writer OS process was killed while
    // SQLite and its file lease remained open. WAL must be durable/reopenable.
    let mut recovered = open(&fixture);
    assert_eq!(recovered.readback().unwrap().book_sequence, 1);
    assert_eq!(recovered.readback().unwrap().global_reserved_usdt, "3");
    let replay = recovered
        .reserve(&request('a', "intentkill1", "3"), &census())
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(recovered.readback().unwrap().book_sequence, 1);
}

#[tokio::test]
async fn risk_book_contract_sql_bounded_owner_serializes_competitors() {
    use super::owner::RiskBookHandle;
    use std::time::Duration;
    let fixture = Fixture::new("tokio-owner");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let owner = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    assert_eq!(owner.diagnostic_snapshot().unwrap().book_sequence, 0);
    let first = owner.clone();
    let second = owner.clone();
    let (a, b) = tokio::join!(
        first.reserve(request('a', "intent0007", "6"), census()),
        second.reserve(request('b', "intent0008", "6"), census()),
    );
    let accepted = [a.as_ref().is_ok(), b.as_ref().is_ok()]
        .into_iter()
        .filter(|v| *v)
        .count();
    assert_eq!(
        accepted, 1,
        "one shared campaign cap; not one cap per account"
    );
    assert!(
        matches!(
            a,
            Err(RiskStoreError::Denied(
                RiskRuleError::InsufficientAggregateOrAccountRisk
            ))
        ) || matches!(
            b,
            Err(RiskStoreError::Denied(
                RiskRuleError::InsufficientAggregateOrAccountRisk
            ))
        )
    );
    assert_eq!(owner.readback().await.unwrap().book_sequence, 1);
    assert_eq!(owner.diagnostic_snapshot().unwrap().book_sequence, 1);
    let mut witness = owner.termination_signal();
    drop(first);
    drop(second);
    drop(owner);
    tokio::time::timeout(Duration::from_secs(2), witness.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(*witness.borrow());
    let mut reloaded = open(&fixture);
    assert_eq!(reloaded.readback().unwrap().global_reserved_usdt, "6");
}
