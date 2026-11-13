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
    assert_eq!(read.global_limit_usdt, "10");
    assert_eq!(read.global_reserved_usdt, "6");
    assert_eq!(read.loss_limit_usd, "100");
    assert_eq!(read.consumed_loss_usd, "0");
    assert_eq!(read.reserved_loss_usd, "6");
    assert_eq!(read.remaining_loss_usd, "94");
    assert_eq!(read.entry_limit_usd, "100");
    assert_eq!(read.consumed_entry_usd, "0");
    assert_eq!(read.reserved_entry_usd, "1");
    assert_eq!(read.entry_bound_breach_usd, "0");
    assert_eq!(read.remaining_entry_usd, "99");
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
    assert_eq!(state.remaining_loss_usd, "89");
    assert_eq!(state.global_reserved_usdt, "0");
    let accepted = store
        .reserve(&request('a', "intentloss2", "5"), &source)
        .unwrap();
    assert_eq!(accepted.book_sequence, 2);
    assert_eq!(store.readback().unwrap().consumed_loss_usd, "11");
    assert_eq!(store.readback().unwrap().remaining_loss_usd, "84");
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

#[test]
fn risk_book_contract_sql_release_requires_zero_reservations() {
    let fixture = Fixture::new("release-check");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let input = request('a', "intentrelease1", "6");
    let mut store = open(&fixture);
    store.reserve(&input, &census()).unwrap();
    drop(store);

    // A terminal label alone must never erase a still-funded obligation.
    // SQLite rejects the incomplete transition even if a caller bypasses Rust.
    let connection = rusqlite::Connection::open(&fixture.path).unwrap();
    let result = connection.execute(
        "UPDATE risk_entries SET state='RELEASED' WHERE reservation_key=?1",
        rusqlite::params![input.reservation_key()],
    );
    assert!(result.is_err());
    drop(connection);
    let mut reopened = open(&fixture);
    assert_eq!(reopened.readback().unwrap().global_reserved_usdt, "6");
}

#[test]
fn risk_book_contract_sql_old_schema_version_is_rejected() {
    let fixture = Fixture::new("schema-version");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let connection = rusqlite::Connection::open(&fixture.path).unwrap();
    connection.pragma_update(None, "user_version", 1).unwrap();
    drop(connection);

    assert!(matches!(
        RiskBookStore::open_existing(&fixture.path, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Corrupt("SCHEMA_OR_JOURNAL"))
    ));
}

#[test]
fn risk_book_contract_sql_account_observation_and_inspect_preserve_revision() {
    use okx_kernel::risk::model::RiskEntryState;
    let fixture = Fixture::new("readonly");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);

    let empty = store.account_readback(&account('b')).unwrap();
    assert_eq!(empty.active_reservations, 0);
    assert_eq!(empty.reserved_loss_usdt, "0");
    assert_eq!(empty.reserved_loss_usd, "0");

    let input = request('a', "intentread1", "6");
    let held = store.reserve(&input, &census()).unwrap();
    let account_a = store.account_readback(&account('a')).unwrap();
    assert_eq!(account_a.active_reservations, 1);
    assert_eq!(account_a.reserved_loss_usdt, "6");
    assert_eq!(account_a.reserved_loss_usd, "6");
    assert_eq!(account_a.cumulative_loss_usd, "0");
    assert_eq!(account_a.book_sequence, held.book_sequence);
    assert_eq!(account_a.policy_revision, 1);
    let account_b = store.account_readback(&account('b')).unwrap();
    assert_eq!(account_b.book_sequence, held.book_sequence);
    assert_eq!(account_b.active_reservations, 0);
    assert_eq!(account_b.reserved_loss_usdt, "0");
    let found = store
        .inspect(&input.reservation_key(), &input.plan_sha256)
        .unwrap()
        .unwrap();
    assert_eq!(found.reservation_key, input.reservation_key());
    assert_eq!(found.plan_sha256, input.plan_sha256);
    assert_eq!(found.state, RiskEntryState::Held);
    assert_eq!(found.reservation_sequence, 1);
    assert_eq!(found.current_book_sequence, 1);
    assert_eq!(found.policy_revision, 1);
    assert!(
        store
            .inspect(&input.reservation_key(), &uid('b'))
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .inspect(
                &request('b', "intentnever1", "1").reservation_key(),
                &uid('e')
            )
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        store.account_readback(&account('c')),
        Err(RiskStoreError::Invalid("ACCOUNT_NOT_ACTIVE"))
    ));
    assert!(matches!(
        store.inspect(&input.reservation_key(), "bad"),
        Err(RiskStoreError::Invalid("INSPECTION_IDENTITY"))
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 1);
    drop(store);

    let mut reopened = open(&fixture);
    assert_eq!(reopened.account_readback(&account('a')).unwrap(), account_a);
    assert_eq!(
        reopened
            .inspect(&input.reservation_key(), &input.plan_sha256)
            .unwrap(),
        Some(found)
    );
    assert_eq!(reopened.readback().unwrap().book_sequence, 1);
}

#[test]
fn risk_book_contract_sql_loss_source_total_and_presence_are_durable() {
    let fixture = Fixture::new("loss-primary");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let mut source = census();
    source.cumulative_loss_usd.insert(account('a'), amt("11"));
    assert!(matches!(
        store.reserve(&request('a', "intentloss5", "95"), &source),
        Err(RiskStoreError::Denied(RiskRuleError::LossBudgetExhausted))
    ));
    assert_eq!(store.readback().unwrap().consumed_loss_usd, "11");
    drop(store);
    let connection = rusqlite::Connection::open(&fixture.path).unwrap();
    connection
        .execute(
            "UPDATE risk_meta SET loss_consumed_usd='0' WHERE singleton=1",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        RiskBookStore::open_existing(&fixture.path, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"))
    ));

    let missing = Fixture::new("loss-row-missing");
    RiskBookStore::initialize_fixture(&missing.path, &opening()).unwrap();
    let connection = rusqlite::Connection::open(&missing.path).unwrap();
    connection
        .execute(
            "DELETE FROM risk_loss_source WHERE account_key=?1",
            rusqlite::params![account('a')],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        RiskBookStore::open_existing(&missing.path, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Corrupt("LOSS_SOURCE_AGGREGATES"))
    ));
}

#[tokio::test]
async fn risk_book_contract_sql_bounded_owner_answers_readonly_observations() {
    use super::owner::RiskBookHandle;
    use okx_kernel::risk::model::RiskEntryState;

    let fixture = Fixture::new("owner-readonly");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let owner = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let input = request('a', "intentowner1", "3");
    owner.reserve(input.clone(), census()).await.unwrap();
    let observed = owner.account_readback(account('a')).await.unwrap();
    assert_eq!(observed.active_reservations, 1);
    assert_eq!(observed.reserved_loss_usdt, "3");
    let entry = owner
        .inspect(input.reservation_key(), input.plan_sha256.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.state, RiskEntryState::Held);
    assert_eq!(owner.readback().await.unwrap().book_sequence, 1);
}

fn unsent(letter: char, intent: &str) -> UnsentSqlProof {
    let input = request(letter, intent, "1");
    UnsentSqlProof {
        reservation_key: input.reservation_key(),
        account_key: account(letter),
        intent_id: intent.to_owned(),
        plan_sha256: input.plan_sha256,
        request_hash: uid('b'),
        reason: "REJECTED_UNSENT".to_owned(),
        record_digest: uid('d'),
    }
}

#[test]
fn risk_book_contract_sql_durable_no_reserve_tombstone_prevents_future_grant() {
    use okx_kernel::risk::model::RiskEntryState;
    let fixture = Fixture::new("negative-fence");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let proof = unsent('a', "intentfence1");
    assert!(
        store
            .inspect(&proof.reservation_key, &proof.plan_sha256)
            .unwrap()
            .is_none()
    );
    let first = store.release_unsent(&proof, 0, 7).unwrap();
    assert_eq!(first.book_sequence, 1);
    assert!(!first.replayed);
    let fenced = store
        .inspect(&proof.reservation_key, &proof.plan_sha256)
        .unwrap()
        .unwrap();
    assert_eq!(fenced.state, RiskEntryState::Released);
    assert_eq!(fenced.reservation_sequence, 1);
    assert_eq!(fenced.current_book_sequence, 1);
    assert_eq!(fenced.policy_revision, 1);
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "0");
    assert_eq!(store.readback().unwrap().reserved_entry_usd, "0");
    let replay = store.release_unsent(&proof, 1, 7).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.book_sequence, 1);
    let altered = UnsentSqlProof {
        record_digest: uid('e'),
        ..proof.clone()
    };
    assert!(matches!(
        store.release_unsent(&altered, 1, 7),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    assert!(matches!(
        store.reserve(&request('a', "intentfence1", "1"), &census()),
        Err(RiskStoreError::AlreadyResolved)
    ));
    let mut changed_plan = request('a', "intentfence1", "1");
    changed_plan.plan_sha256 = uid('f');
    assert!(matches!(
        store.reserve(&changed_plan, &census()),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 1);
    drop(store);

    let mut recovered = open(&fixture);
    assert_eq!(
        recovered
            .inspect(&proof.reservation_key, &proof.plan_sha256)
            .unwrap(),
        Some(fenced)
    );
    assert!(matches!(
        recovered.reserve(&request('a', "intentfence1", "1"), &census()),
        Err(RiskStoreError::AlreadyResolved)
    ));
    assert_eq!(
        recovered
            .reserve(&request('b', "intentother1", "6"), &census())
            .unwrap()
            .book_sequence,
        2
    );
    assert_eq!(recovered.readback().unwrap().global_reserved_usdt, "6");
}

#[test]
fn risk_book_contract_sql_unsent_held_release_is_exact_and_durable() {
    use okx_kernel::risk::model::RiskEntryState;
    let fixture = Fixture::new("unsent-held");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let input = request('a', "intentunsent1", "6");
    let first = store.reserve(&input, &census()).unwrap();
    assert_eq!(first.book_sequence, 1);
    let proof = unsent('a', "intentunsent1");
    assert!(matches!(
        store.release_unsent(&proof, 0, 7),
        Err(RiskStoreError::StaleSequence)
    ));
    assert!(matches!(
        store.release_unsent(&proof, 1, 6),
        Err(RiskStoreError::StaleGeneration)
    ));
    let wrong = UnsentSqlProof {
        plan_sha256: uid('d'),
        ..proof.clone()
    };
    assert!(matches!(
        store.release_unsent(&wrong, 1, 7),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "6");
    let released = store.release_unsent(&proof, 1, 7).unwrap();
    assert_eq!(released.book_sequence, 2);
    assert!(!released.replayed);
    let replay = store.release_unsent(&proof, 2, 7).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.book_sequence, 2);
    let changed_reason = UnsentSqlProof {
        reason: "OTHER_SQL_REASON".into(),
        ..proof.clone()
    };
    assert!(matches!(
        store.release_unsent(&changed_reason, 2, 7),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 2);
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "0");
    assert_eq!(store.readback().unwrap().reserved_loss_usd, "0");
    assert_eq!(store.readback().unwrap().reserved_entry_usd, "0");
    assert_eq!(
        store
            .account_readback(&account('a'))
            .unwrap()
            .active_reservations,
        0
    );
    let entry = store
        .inspect(&proof.reservation_key, &proof.plan_sha256)
        .unwrap()
        .unwrap();
    assert_eq!(entry.state, RiskEntryState::Released);
    assert_eq!(entry.reservation_sequence, 1);
    assert_eq!(entry.current_book_sequence, 2);
    assert!(matches!(
        store.reserve(&input, &census()),
        Err(RiskStoreError::AlreadyResolved)
    ));
    drop(store);

    let mut reopened = open(&fixture);
    assert_eq!(
        reopened
            .inspect(&proof.reservation_key, &proof.plan_sha256)
            .unwrap(),
        Some(entry)
    );
    assert!(reopened.release_unsent(&proof, 2, 7).unwrap().replayed);
    assert_eq!(reopened.readback().unwrap().global_reserved_usdt, "0");
    assert_eq!(reopened.readback().unwrap().remaining_loss_usd, "100");
    assert_eq!(reopened.readback().unwrap().remaining_entry_usd, "100");
}

#[test]
fn risk_book_contract_sql_rejects_invalid_no_send_and_conflicting_fence() {
    let fixture = Fixture::new("negative-corrupt");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let mut proof = unsent('a', "intentnoproof1");
    proof.reservation_key.push_str("garbage");
    assert!(matches!(
        store.release_unsent(&proof, 0, 7),
        Err(RiskStoreError::Invalid("UNSENT_SQL_PROOF_INVALID"))
    ));
    let mut proof = unsent('a', "intentnoproof1");
    proof.request_hash = "not-a-digest".into();
    assert!(matches!(
        store.release_unsent(&proof, 0, 7),
        Err(RiskStoreError::Invalid("UNSENT_SQL_PROOF_INVALID"))
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 0);
    store
        .reserve(&request('a', "intentother2", "4"), &census())
        .unwrap();
    drop(store);
    // Out-of-band corruption must not allow both a positive reservation
    // and a negative no-reserve fence on the exact same durable identity.
    let conflict = unsent('a', "intentother2");
    let db = rusqlite::Connection::open(&fixture.path).unwrap();
    db.execute(
        "INSERT INTO risk_no_reserve(
           reservation_key,account_key,intent_id,plan_sha256,
           request_hash,record_digest,reason,policy_revision,book_sequence)
         VALUES(?1,?2,?3,?4,?5,?6,?7,1,2)",
        rusqlite::params![
            &conflict.reservation_key,
            &conflict.account_key,
            &conflict.intent_id,
            &conflict.plan_sha256,
            &conflict.request_hash,
            &conflict.record_digest,
            &conflict.reason,
        ],
    )
    .unwrap();
    drop(db);
    assert!(matches!(
        RiskBookStore::open_existing(&fixture.path, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Corrupt("NO_RESERVE_FENCE_CONFLICT"))
    ));
}

#[tokio::test]
async fn risk_book_contract_sql_owner_serializes_negative_fence() {
    use super::owner::RiskBookHandle;
    let fixture = Fixture::new("owner-unsent");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let owner = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let proof = unsent('a', "intentowner2");
    let first = owner.release_unsent(proof.clone(), 0, 7).await.unwrap();
    assert_eq!(first.book_sequence, 1);
    let replay = owner.release_unsent(proof.clone(), 1, 7).await.unwrap();
    assert!(replay.replayed);
    let observed = owner
        .inspect(proof.reservation_key.clone(), proof.plan_sha256.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.current_book_sequence, 1);
    assert_eq!(owner.readback().await.unwrap().global_reserved_usdt, "0");
}
