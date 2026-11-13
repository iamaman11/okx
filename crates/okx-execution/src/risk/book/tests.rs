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
        cumulative_loss_usd: BTreeMap::from([(account('a'), amt("0")), (account('b'), amt("0"))]),
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
            "risk_book_contract_sql_child_post_commit_wait",
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

#[test]
fn risk_book_contract_sql_child_writer_probe() {
    let Ok(db) = std::env::var("OKX_510_TEST_BUSY_DB") else {
        return;
    };
    let attempt = RiskBookStore::open_existing(
        &PathBuf::from(db),
        "test-installation",
        &opening().campaign_id,
    );
    assert!(matches!(attempt, Err(RiskStoreError::WriterBusy)));
}

#[test]
fn risk_book_contract_sql_cross_process_exclusive_lease() {
    let fixture = Fixture::new("cross-process");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let store = open(&fixture);
    let probe = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["risk_book_contract_sql_child_writer_probe", "--nocapture"])
        .env("OKX_510_TEST_BUSY_DB", &fixture.path)
        .output()
        .unwrap();
    assert!(
        probe.status.success() && String::from_utf8_lossy(&probe.stdout).contains("1 passed"),
        "independent OS writer probe was not executed and passed: stdout={} stderr={}",
        String::from_utf8_lossy(&probe.stdout),
        String::from_utf8_lossy(&probe.stderr)
    );
    drop(store);
    assert!(open(&fixture).readback().is_ok());
}

#[test]
fn risk_book_contract_sql_external_occupancy_counts_before_reserve() {
    let fixture = Fixture::new("external");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let mut attested = census();
    attested.occupied.insert(account('a'), amt("9"));
    assert!(matches!(
        store.reserve(&request('b', "intent0099", "2"), &attested),
        Err(RiskStoreError::Denied(
            RiskRuleError::InsufficientAggregateOrAccountRisk
        ))
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 0);
}

#[test]
fn risk_book_contract_sql_loss_source_monotonic_even_on_denied_reserve() {
    let fixture = Fixture::new("loss-monotonic");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let mut source = census();
    source.cumulative_loss_usd.insert(account('a'), amt("11"));
    let denied = store.reserve(&request('a', "intentloss1", "95"), &source);
    assert!(matches!(
        denied,
        Err(RiskStoreError::Denied(RiskRuleError::LossBudgetExhausted))
    ));
    let state = store.readback().unwrap();
    assert_eq!(
        state.book_sequence, 1,
        "verified loss alone increments sequence"
    );
    assert_eq!(state.consumed_loss_usd, "11");
    assert_eq!(state.global_reserved_usdt, "0");
    let accepted = store
        .reserve(&request('a', "intentloss2", "5"), &source)
        .unwrap();
    assert_eq!(accepted.book_sequence, 2);
    assert_eq!(store.readback().unwrap().consumed_loss_usd, "11");
    let mut regression = source.clone();
    regression
        .cumulative_loss_usd
        .insert(account('a'), amt("10"));
    assert!(matches!(
        store.reserve(&request('b', "intentloss3", "1"), &regression),
        Err(RiskStoreError::LossRegression)
    ));
    drop(store);
    let mut reopened = open(&fixture);
    assert_eq!(reopened.readback().unwrap().consumed_loss_usd, "11");
    assert_eq!(reopened.readback().unwrap().book_sequence, 2);
}

#[tokio::test]
async fn risk_book_contract_sql_denied_reserve_refreshes_owner_projection() {
    use super::owner::RiskBookHandle;
    let fixture = Fixture::new("loss-projection");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let owner = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let mut source = census();
    source.cumulative_loss_usd.insert(account('a'), amt("11"));
    let denial = owner
        .reserve(request('a', "intentloss4", "95"), source)
        .await;
    assert!(matches!(
        denial,
        Err(RiskStoreError::Denied(RiskRuleError::LossBudgetExhausted))
    ));
    let visible = owner.diagnostic_snapshot().unwrap();
    assert_eq!(visible.consumed_loss_usd, "11");
    assert_eq!(visible.book_sequence, 1);
    assert_eq!(visible.global_reserved_usdt, "0");
}

#[test]
fn risk_book_contract_sql_legacy_identity_and_safe_number_ranges() {
    let mut input = request('a', "intent0001", "1");
    assert_eq!(
        input.reservation_key(),
        format!("3:okx4:real64:{}10:run_00000110:intent0001", uid('a'))
    );
    input.strategy_id = "s1".into();
    assert!(
        input.validate().is_ok(),
        "legacy allows two-character strategy IDs"
    );
    input.strategy_id = "1s".into();
    assert!(matches!(input.validate(), Err(RiskStoreError::Invalid(_))));
    input.strategy_id = "strategy_test".into();
    input.policy_revision = 9_007_199_254_740_992;
    assert!(matches!(input.validate(), Err(RiskStoreError::Invalid(_))));
    input.policy_revision = 1;
    input.now_ms = 9_007_199_254_740_992;
    assert!(matches!(input.validate(), Err(RiskStoreError::Invalid(_))));
}

#[test]
fn risk_book_contract_sql_rejects_over_limit_other_account() {
    let fixture = Fixture::new("cohort-over-limit");
    let mut policy = opening();
    policy.global_limit = amt("30");
    RiskBookStore::initialize_fixture(&fixture.path, &policy).unwrap();
    let mut store = open(&fixture);
    let mut source = census();
    // A already violates its own 10 USDT cap; B's 1 USDT cannot be admitted.
    // Checking only B and the global 30 USDT cap would incorrectly grant HELD.
    source.occupied.insert(account('a'), amt("11"));
    assert!(matches!(
        store.reserve(&request('b', "intentcohort1", "1"), &source),
        Err(RiskStoreError::Denied(
            RiskRuleError::AccountAlreadyOverLimit
        ))
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 0);
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "0");
}

#[test]
fn risk_book_contract_sql_corrupted_reservation_aggregates_reject_reopen() {
    let fixture = Fixture::new("aggregate-corruption");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    store
        .reserve(&request('a', "intentcorrupt1", "6"), &census())
        .unwrap();
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "6");
    drop(store);

    // Deliberately damage only a materialized total in an isolated test DB.
    // The primary HELD reservation and account/strategy aggregates remain intact.
    let connection = rusqlite::Connection::open(&fixture.path).unwrap();
    connection
        .execute(
            "UPDATE risk_meta SET global_reserved_usdt='0' WHERE singleton=1",
            [],
        )
        .unwrap();
    drop(connection);

    assert!(matches!(
        RiskBookStore::open_existing(&fixture.path, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Corrupt("RESERVATION_AGGREGATES"))
    ));
}
