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
        // OS-process identity + a fresh time nonce isolates real SQLite/WAL
        // paths across independent test binaries and CI runner reuse. File
        // locks are still exclusive; no retry or weaker lock is permitted.
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "okx-riskbook-{label}-{}-{number}-{nonce}",
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
                entry_limit: amt("10"),
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
        native_incarnation: "f".repeat(32),
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
fn risk_book_contract_real_legacy_campaign_cannot_overlap_unified_finance_mode() {
    let fixture = Fixture::new("installation-mode");
    let root = fixture.root.join("runtime");
    let campaign = root
        .join("risk")
        .join("campaigns")
        .join(uid('c'))
        .join("risk-book.sqlite");
    std::fs::create_dir_all(campaign.parent().unwrap()).unwrap();
    RiskBookStore::initialize_fixture(&campaign, &opening()).unwrap();
    let mut store =
        RiskBookStore::open_existing(&campaign, "test-installation", &opening().campaign_id)
            .unwrap();
    assert_eq!(store.readback().unwrap().book_sequence, 0);
    assert!(matches!(
        crate::finance_owner_gate::unified_mode_for(&root),
        Err(crate::finance_owner_gate::AuthorityModeError::Busy)
    ));
    store.close();
    let unified = crate::finance_owner_gate::unified_mode_for(&root).unwrap();
    assert!(matches!(
        RiskBookStore::open_existing(&campaign, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Invalid("FINANCE_INSTALLATION_WRITER_BUSY"))
    ));
    drop(unified);
    // Staging the unified financial file is already a hard stop for the
    // original campaign, even before any authority/venue grant is enabled.
    std::fs::write(root.join("finance").join("finance.sqlite"), b"offline").unwrap();
    assert!(matches!(
        RiskBookStore::open_existing(&campaign, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Invalid("LEGACY_FINANCE_OWNER_DISABLED"))
    ));
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
    s.close();
    // The original owner must have physically released the OS lock before
    // another one opens; no sleep/retry may substitute for this observation.
    let lease_path = format!("{}.writer.lock", f.path.canonicalize().unwrap().display());
    let probe = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(lease_path)
        .unwrap();
    probe
        .try_lock()
        .expect("former RiskBook writer still owns the lease");
    probe.unlock().unwrap();
    drop(probe);
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
    s.close();
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
    store.close();
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
    store.close();
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
    store.close();
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
    store.close();

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
    store.close();

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
    // This exercises the original owner's explicitly ordered production
    // shutdown: close SQLite first, then unlock the exact native writer
    // lease before allowing the next owner to open. No retry or sleep.
    store.close();

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
    store.close();
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
    // The first response may be lost. Repeating the ORIGINAL request/sequence
    // after COMMIT must return its receipt, not a spurious stale-CAS error.
    let replay = store.release_unsent(&proof, 0, 7).unwrap();
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
    store.close();

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
    let replay = store.release_unsent(&proof, 1, 7).unwrap();
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
    store.close();

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
    store.close();
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

#[tokio::test]
async fn risk_book_contract_real_account_sql_unsent_proof_bridges_campaign_owner() {
    use super::owner::RiskBookHandle;
    use crate::{
        AccountMeta, AccountStoreHandle, Decision, IntentPlan, NewIntent, OpeningSnapshot,
    };
    use rust_decimal::Decimal;
    use std::time::{Duration, Instant};

    let fixture = Fixture::new("account-sql-to-campaign");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let account_handle = AccountStoreHandle::initialize_flat(
        fixture.root.join("account.sqlite"),
        AccountMeta {
            account_key: account('a'),
            installation_id: "test-installation".into(),
            initialized_at: 1,
            opening_source: "fixture_flat".into(),
            initialize_operation_id: "initialize_native_unsent".into(),
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
    let campaign = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let no_source = campaign
        .release_unsent_from_account_sql(
            &account_handle,
            "intentnative1".into(),
            0,
            7,
            Instant::now() + Duration::from_secs(3),
        )
        .await;
    assert!(matches!(no_source, Err(RiskStoreError::UnsentSqlUnproven)));
    assert_eq!(campaign.readback().await.unwrap().book_sequence, 0);

    let reservation = request('a', "intentnative1", "3");
    let key = reservation.reservation_key();
    let marked_key = key.clone();
    account_handle
        .call(Instant::now() + Duration::from_secs(3), move |s| {
            s.apply_authority_policy(1, false, "campaign_fixture", Decimal::from(100))?;
            let definition = s.ensure_manual_instance(1)?;
            s.prepare(
                &Decision {
                    decision_id: "decision_native_unsent".into(),
                    instance_id: None,
                    decision_at: 2,
                    input_digest: "d".repeat(64),
                    policy_revision: 1,
                    code_version: "fixture".into(),
                },
                &NewIntent {
                    intent_id: "intentnative1".into(),
                    idempotency_key: "command_native_unsent".into(),
                    request_hash: "b".repeat(64),
                    client_order_id: "c".repeat(32),
                    deadline: 100,
                    plan_hash: "e".repeat(64),
                    policy_revision: 1,
                    risk_increase: true,
                    instrument_id: "BTC-USDT-SWAP".into(),
                    plan: IntentPlan {
                        strategy_definition_id: definition.definition_id,
                        strategy_definition_version: definition.version,
                        strategy_parameters_digest: definition.parameters_digest,
                        operation: "open".into(),
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
            s.mark_reserve_attempt("intentnative1", &marked_key, 3)?;
            Ok(())
        })
        .await
        .unwrap();

    let held = campaign
        .reserve(reservation.clone(), census())
        .await
        .unwrap();
    assert_eq!(held.book_sequence, 1);
    // A reserve attempt without the durable SQL terminal reject is NOT
    // sufficient to return any of the campaign's reserved capacity.
    assert!(matches!(
        campaign
            .release_unsent_from_account_sql(
                &account_handle,
                "intentnative1".into(),
                1,
                7,
                Instant::now() + Duration::from_secs(3)
            )
            .await,
        Err(RiskStoreError::UnsentSqlUnproven)
    ));
    assert_eq!(campaign.readback().await.unwrap().global_reserved_usdt, "3");

    account_handle
        .call(Instant::now() + Duration::from_secs(3), |s| {
            s.reject_unsent("intentnative1", "NO_NATIVE_POST", 4)?;
            Ok(())
        })
        .await
        .unwrap();
    let released = campaign
        .release_unsent_from_account_sql(
            &account_handle,
            "intentnative1".into(),
            1,
            7,
            Instant::now() + Duration::from_secs(3),
        )
        .await
        .unwrap();
    assert_eq!(released.book_sequence, 2);
    assert_eq!(campaign.readback().await.unwrap().global_reserved_usdt, "0");
    assert_eq!(campaign.readback().await.unwrap().reserved_loss_usd, "0");
    assert_eq!(campaign.readback().await.unwrap().reserved_entry_usd, "0");

    let original = account_handle
        .call(Instant::now() + Duration::from_secs(3), |s| {
            s.unsent_release_receipt("intentnative1")?
                .ok_or(crate::StoreError::Invalid("SQL_PROOF_MISSING"))
        })
        .await
        .unwrap();
    let proof = UnsentSqlProof::from_account_receipt(original).unwrap();
    assert!(
        campaign
            .release_unsent_from_account_sql(
                &account_handle,
                "intentnative1".into(),
                2,
                7,
                Instant::now() + Duration::from_secs(3)
            )
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(proof.reservation_key, key);
    let observed = campaign
        .inspect(key, proof.plan_sha256)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        observed.state,
        okx_kernel::risk::model::RiskEntryState::Released
    );
}

#[test]
fn risk_book_contract_campaign_db_path_is_installation_scoped() {
    let fixture = Fixture::new("campaign-path");
    let campaign = opening().campaign_id;
    let selected = campaign_db_path(&fixture.root, &campaign).unwrap();
    assert_eq!(
        selected,
        fixture
            .root
            .join("risk")
            .join("campaigns")
            .join(uid('c'))
            .join("risk-book.sqlite")
    );
    assert!(
        !selected.exists(),
        "path derivation must never bootstrap a budget"
    );
    assert!(matches!(
        campaign_db_path(std::path::Path::new("relative"), &campaign),
        Err(RiskStoreError::Invalid("RUNTIME_ROOT_NOT_ABSOLUTE"))
    ));
    assert!(matches!(
        campaign_db_path(&fixture.root, "sha256:../../another-tenant"),
        Err(RiskStoreError::Invalid("CAMPAIGN_ID"))
    ));
    assert!(matches!(
        campaign_db_path(&fixture.root, &format!("sha256:{}", "A".repeat(64))),
        Err(RiskStoreError::Invalid("CAMPAIGN_ID"))
    ));
}

fn post_fence(input: &ReserveInput, sequence: u64) -> PostFenceInput {
    use okx_kernel::risk::model::{PostAccountRead, PostCausalProof};
    let mut fresh = census();
    fresh.generated_at_ms = 10_001;
    fresh.read_floor_ms = 9_996;
    PostFenceInput {
        reservation_key: input.reservation_key(),
        plan_sha256: input.plan_sha256.clone(),
        policy_revision: input.policy_revision,
        expected_sequence: sequence,
        now_ms: 10_002,
        source_proof_digest: uid('b'),
        census: fresh,
        causal: PostCausalProof {
            reserve_anchor: input.reservation_key(),
            native_incarnation: input.native_incarnation.clone(),
            elapsed_to_proof_ms: 2,
            account_reads: ['a', 'b']
                .into_iter()
                .map(|letter| PostAccountRead {
                    account_key: account(letter),
                    private_generation: 7,
                    source_read_id: uid(letter),
                    content_digest: uid('c'),
                    started_after_held_ms: 0,
                    completed_after_held_ms: 1,
                })
                .collect(),
        },
    }
}

#[test]
fn risk_book_contract_sql_post_fence_commits_once() {
    use okx_kernel::risk::model::RiskEntryState;
    let fixture = Fixture::new("post-fence");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let input = request('a', "intentpost1", "6");
    assert_eq!(store.reserve(&input, &census()).unwrap().book_sequence, 1);
    let post = post_fence(&input, 1);
    let grant = store.begin_post(&post).unwrap();
    assert_eq!(grant.reservation_key, input.reservation_key());
    assert_eq!(grant.book_sequence, 2);
    assert_eq!(grant.existing_modelled_loss_usdt, "0");
    let observed = store
        .inspect(&input.reservation_key(), &input.plan_sha256)
        .unwrap()
        .unwrap();
    assert_eq!(observed.state, RiskEntryState::Posted);
    assert_eq!(observed.current_book_sequence, 2);
    assert_eq!(observed.reservation_sequence, 1);
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "6");
    assert_eq!(store.readback().unwrap().reserved_loss_usd, "6");
    assert_eq!(store.readback().unwrap().reserved_entry_usd, "1");
    assert!(matches!(
        store.begin_post(&post),
        Err(RiskStoreError::StaleSequence)
    ));
    let mut replay = post.clone();
    replay.expected_sequence = 2;
    assert!(matches!(
        store.begin_post(&replay),
        Err(RiskStoreError::AlreadyResolved)
    ));
    assert!(matches!(
        store.release_unsent(&unsent('a', "intentpost1"), 2, 7),
        Err(RiskStoreError::AlreadyResolved)
    ));
    store.close();
    let db = rusqlite::Connection::open(&fixture.path).unwrap();
    let stored: (String, i64, String, String) = db
        .query_row(
            "SELECT state,post_sequence,post_proof_digest,post_source_digest
         FROM risk_entries WHERE reservation_key=?1",
            rusqlite::params![input.reservation_key()],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(stored, ("POSTED".into(), 2, uid('b'), uid('d')));
    drop(db);
    let mut reopened = open(&fixture);
    assert_eq!(
        reopened
            .inspect(&input.reservation_key(), &input.plan_sha256)
            .unwrap(),
        Some(observed)
    );
    assert!(matches!(
        reopened.begin_post(&replay),
        Err(RiskStoreError::AlreadyResolved)
    ));
    assert_eq!(reopened.readback().unwrap().book_sequence, 2);
}

#[test]
fn risk_book_contract_sql_post_fence_rejects_stale_causal_and_other_account_exposure() {
    let fixture = Fixture::new("post-negative");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let input = request('a', "intentpost2", "6");
    store.reserve(&input, &census()).unwrap();
    let post = post_fence(&input, 1);
    let mut stale = post.clone();
    stale.expected_sequence = 0;
    assert!(matches!(
        store.begin_post(&stale),
        Err(RiskStoreError::Invalid("POST_FENCE_IDENTITY"))
    ));
    stale.expected_sequence = 2;
    assert!(matches!(
        store.begin_post(&stale),
        Err(RiskStoreError::StaleSequence)
    ));
    let mut foreign = post.clone();
    foreign.plan_sha256 = uid('a');
    assert!(matches!(
        store.begin_post(&foreign),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    let mut policy = post.clone();
    policy.policy_revision = 2;
    assert!(matches!(
        store.begin_post(&policy),
        Err(RiskStoreError::PolicyConflict)
    ));
    let mut incarnation = post.clone();
    incarnation.causal.native_incarnation = "e".repeat(32);
    assert!(matches!(
        store.begin_post(&incarnation),
        Err(RiskStoreError::Denied(
            RiskRuleError::PostCausalProofUnproven
        ))
    ));
    let mut missing = post.clone();
    missing.causal.account_reads.pop();
    assert!(matches!(
        store.begin_post(&missing),
        Err(RiskStoreError::Denied(
            RiskRuleError::PostCausalProofUnproven
        ))
    ));
    let mut before_held = post.clone();
    before_held.census.generated_at_ms = 10_000;
    assert!(matches!(
        store.begin_post(&before_held),
        Err(RiskStoreError::CensusUnproven)
    ));
    let mut regressed_source = post.clone();
    regressed_source.census.read_floor_ms = 9_994;
    assert!(matches!(
        store.begin_post(&regressed_source),
        Err(RiskStoreError::CensusUnproven)
    ));
    let mut too_late = post.clone();
    too_late.now_ms = 15_001;
    too_late.causal.elapsed_to_proof_ms = 5_001;
    assert!(matches!(
        store.begin_post(&too_late),
        Err(RiskStoreError::CensusUnproven)
    ));
    let mut other_over = post.clone();
    other_over.census.occupied.insert(account('b'), amt("11"));
    assert!(matches!(
        store.begin_post(&other_over),
        Err(RiskStoreError::Denied(
            RiskRuleError::AccountAlreadyOverLimit
        ))
    ));
    let mut combined_over = post.clone();
    combined_over.census.occupied.insert(account('b'), amt("5"));
    assert!(matches!(
        store.begin_post(&combined_over),
        Err(RiskStoreError::Denied(
            RiskRuleError::InsufficientAggregateOrAccountRisk
        ))
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 1);
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "6");
    assert_eq!(
        store
            .inspect(&input.reservation_key(), &input.plan_sha256)
            .unwrap()
            .unwrap()
            .state,
        okx_kernel::risk::model::RiskEntryState::Held
    );
    let grant = store.begin_post(&post).unwrap();
    assert_eq!(grant.book_sequence, 2);
}

#[test]
fn risk_book_contract_sql_post_loss_high_water_is_durable_on_denial() {
    let fixture = Fixture::new("post-loss");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let input = request('a', "intentpost3", "6");
    store.reserve(&input, &census()).unwrap();
    let mut post = post_fence(&input, 1);
    post.census
        .cumulative_loss_usd
        .insert(account('a'), amt("95"));
    assert!(matches!(
        store.begin_post(&post),
        Err(RiskStoreError::Denied(RiskRuleError::LossBudgetExhausted))
    ));
    let read = store.readback().unwrap();
    assert_eq!(
        read.book_sequence, 2,
        "loss source high-water survives POST denial"
    );
    assert_eq!(read.consumed_loss_usd, "95");
    assert_eq!(read.reserved_loss_usd, "6");
    assert_eq!(read.remaining_loss_usd, "0");
    assert_eq!(read.global_reserved_usdt, "6");
    assert_eq!(
        store
            .inspect(&input.reservation_key(), &input.plan_sha256)
            .unwrap()
            .unwrap()
            .state,
        okx_kernel::risk::model::RiskEntryState::Held
    );
    store.close();
    let mut reopened = open(&fixture);
    assert_eq!(reopened.readback().unwrap(), read);
    assert!(matches!(
        reopened.begin_post(&post),
        Err(RiskStoreError::StaleSequence)
    ));
}

#[test]
fn risk_book_contract_sql_child_post_fence_committed_without_reply() {
    let Ok(db) = std::env::var("OKX_510_TEST_POST_KILL_DB") else {
        return;
    };
    let mut store = RiskBookStore::open_existing(
        &PathBuf::from(db),
        "test-installation",
        &opening().campaign_id,
    )
    .unwrap();
    let input = request('a', "intentkillpost1", "3");
    store.reserve(&input, &census()).unwrap();
    store.begin_post(&post_fence(&input, 1)).unwrap();
    use std::io::Write;
    println!("RISK_POST_FENCE_COMMITTED_WITHOUT_REPLY");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(60));
    unreachable!("the parent must OS kill this child after post-fence commit");
}

#[test]
fn risk_book_contract_sql_os_kill_after_post_fence_never_regrants() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    let fixture = Fixture::new("physical-post-fence");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "risk_book_contract_sql_child_post_fence_committed_without_reply",
            "--nocapture",
        ])
        .env("OKX_510_TEST_POST_KILL_DB", &fixture.path)
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
                .contains("RISK_POST_FENCE_COMMITTED_WITHOUT_REPLY")
            {
                let _ = tx.send(());
                return;
            }
        }
    });
    let committed = rx.recv_timeout(Duration::from_secs(15));
    child.kill().unwrap();
    assert!(
        !child.wait().unwrap().success(),
        "real post-fence writer must be OS-killed"
    );
    reader.join().unwrap();
    assert!(
        committed.is_ok(),
        "did not see independently committed post-fence marker"
    );
    let mut reopened = open(&fixture);
    let input = request('a', "intentkillpost1", "3");
    let observed = reopened
        .inspect(&input.reservation_key(), &input.plan_sha256)
        .unwrap()
        .unwrap();
    assert_eq!(
        observed.state,
        okx_kernel::risk::model::RiskEntryState::Posted
    );
    assert_eq!(observed.current_book_sequence, 2);
    assert_eq!(reopened.readback().unwrap().global_reserved_usdt, "3");
    let mut same_post = post_fence(&input, 2);
    same_post.now_ms = 10_003;
    assert!(matches!(
        reopened.begin_post(&same_post),
        Err(RiskStoreError::AlreadyResolved)
    ));
    assert_eq!(reopened.readback().unwrap().book_sequence, 2);
}

#[tokio::test]
async fn risk_book_contract_sql_tokio_owner_posts_one_original_grant() {
    use super::owner::RiskBookHandle;
    let fixture = Fixture::new("post-tokio");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let owner = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let input = request('a', "intentpostowner1", "3");
    owner.reserve(input.clone(), census()).await.unwrap();
    let first = owner.begin_post(post_fence(&input, 1)).await.unwrap();
    assert_eq!(first.book_sequence, 2);
    assert!(matches!(
        owner.begin_post(post_fence(&input, 2)).await,
        Err(RiskStoreError::AlreadyResolved)
    ));
    assert_eq!(owner.readback().await.unwrap().reserved_entry_usd, "1");
}

#[test]
fn risk_book_contract_sql_account_entry_limit_rejects_before_held() {
    let fixture = Fixture::new("account-entry-cap");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let mut input = request('a', "intententry1", "1");
    input.proposal.entry_upper_usd = amt("11");
    assert!(matches!(
        store.reserve(&input, &census()),
        Err(RiskStoreError::Denied(
            RiskRuleError::AccountEntryLimitExceeded
        ))
    ));
    assert_eq!(store.readback().unwrap().book_sequence, 0);
    assert_eq!(store.readback().unwrap().global_reserved_usdt, "0");
    assert_eq!(store.readback().unwrap().reserved_entry_usd, "0");
}

#[test]
fn risk_book_contract_sql_posted_requires_durable_proof_fields() {
    let fixture = Fixture::new("post-durable-check");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut store = open(&fixture);
    let input = request('a', "intentpostcheck1", "3");
    store.reserve(&input, &census()).unwrap();
    store.close();
    let db = rusqlite::Connection::open(&fixture.path).unwrap();
    let rejected = db.execute(
        "UPDATE risk_entries SET state='POSTED' WHERE reservation_key=?1",
        rusqlite::params![input.reservation_key()],
    );
    assert!(
        rejected.is_err(),
        "SQLite may not grant POST without a saved source receipt"
    );
    drop(db);
    let mut recovered = open(&fixture);
    assert_eq!(
        recovered
            .inspect(&input.reservation_key(), &input.plan_sha256)
            .unwrap()
            .unwrap()
            .state,
        okx_kernel::risk::model::RiskEntryState::Held
    );
    assert_eq!(recovered.readback().unwrap().book_sequence, 1);
}

fn partial_proof(
    input: &ReserveInput,
    revision: u64,
    cumulative: &str,
    digest_letter: char,
) -> PartialEntrySqlProof {
    use crate::EntrySettlementEvidence;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    PartialEntrySqlProof::from_account_receipt(EntrySettlementEvidence {
        schema: "okx.entry-partial-settlement/v1".into(),
        account_key: account('a'),
        intent_id: input.intent_id.clone(),
        plan_hash: input.plan_sha256.clone(),
        reservation_key: input.reservation_key(),
        venue_order_id: "venue-order-001".into(),
        venue_update_ms: 11_000,
        source_covered_through_ms: 11_000,
        venue_filled_quantity: Decimal::ONE,
        booked_notional_and_fee_usdt: Decimal::ONE,
        cumulative_entry_usd: Decimal::from_str(cumulative).unwrap(),
        settlement_revision: revision,
        source_digest: uid(digest_letter),
    })
    .unwrap()
}

#[test]
fn risk_book_contract_sql_partial_entry_consumes_reserved_budget_once() {
    let fixture = Fixture::new("entry-partial");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut s = open(&fixture);
    let input = request('a', "intentpartial1", "3");
    s.reserve(&input, &census()).unwrap();
    s.begin_post(&post_fence(&input, 1)).unwrap();
    let revision1 = partial_proof(&input, 1, "0.4", 'b');
    let r = s.settle_partial_entry(&revision1, 2, 7).unwrap();
    assert_eq!(r.book_sequence, 3);
    assert_eq!(r.cumulative_entry_usd, "0.4");
    assert_eq!(r.entry_bound_breach_usd, "0");
    assert!(!r.replayed);
    let b = s.readback().unwrap();
    assert_eq!(b.consumed_entry_usd, "0.4");
    assert_eq!(b.reserved_entry_usd, "0.6");
    assert_eq!(b.remaining_entry_usd, "99");
    assert_eq!(b.global_reserved_usdt, "3");
    assert_eq!(b.reserved_loss_usd, "3");
    // Lost ACK with an original stale sequence must not debit twice.
    let replay = s.settle_partial_entry(&revision1, 2, 7).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.book_sequence, 3);
    let changed_digest = partial_proof(&input, 1, "0.4", 'c');
    assert!(matches!(
        s.settle_partial_entry(&changed_digest, 3, 7),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    let changed_amount = partial_proof(&input, 1, "0.5", 'b');
    assert!(matches!(
        s.settle_partial_entry(&changed_amount, 3, 7),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    let revision2 = partial_proof(&input, 2, "0.75", 'c');
    let r2 = s.settle_partial_entry(&revision2, 3, 7).unwrap();
    assert_eq!(r2.book_sequence, 4);
    let b = s.readback().unwrap();
    assert_eq!(b.consumed_entry_usd, "0.75");
    assert_eq!(b.reserved_entry_usd, "0.25");
    assert_eq!(b.global_reserved_usdt, "3");
    s.close();
    let mut reopened = open(&fixture);
    assert_eq!(reopened.readback().unwrap(), b);
    assert!(
        reopened
            .settle_partial_entry(&revision2, 3, 7)
            .unwrap()
            .replayed
    );
    assert_eq!(reopened.readback().unwrap().book_sequence, 4);
    let further = partial_proof(&input, 3, "1", 'd');
    assert_eq!(
        reopened
            .settle_partial_entry(&further, 4, 7)
            .unwrap()
            .book_sequence,
        5
    );
    assert_eq!(reopened.readback().unwrap().reserved_entry_usd, "0");
    assert_eq!(reopened.readback().unwrap().consumed_entry_usd, "1");
}

#[test]
fn risk_book_contract_sql_account_outcome_open_and_unknown_keep_entry_debit_atomic() {
    // Each state has exactly the same existing RiskBook monetary owner:
    // post-ACK OPEN and lost-ACK UNKNOWN preserve the original reservation.
    // The verified ORIGINAL AccountStore receipt is represented by the
    // existing typed domain proof; this fixture does not grant venue POST.
    for (state, letter) in [("OPEN", 'o'), ("UNKNOWN", 'u')] {
        let fixture = Fixture::new("entry-after-outcome");
        RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
        let input = request('a', &format!("intentafter{letter}"), "3");
        let mut owner = open(&fixture);
        owner.reserve(&input, &census()).unwrap();
        owner.begin_post(&post_fence(&input, 1)).unwrap();
        owner.close();
        {
            let db = rusqlite::Connection::open(&fixture.path).unwrap();
            db.execute(
                "UPDATE risk_entries SET state=?1 WHERE reservation_key=?2
                   AND state='POSTED'",
                rusqlite::params![state, input.reservation_key()],
            )
            .unwrap();
            db.execute(
                "UPDATE risk_meta SET book_sequence=3 WHERE singleton=1
                 AND book_sequence=2",
                [],
            )
            .unwrap();
        }
        let mut owner = open(&fixture);
        let initial = owner.readback().unwrap();
        assert_eq!(initial.global_reserved_usdt, "3");
        assert_eq!(initial.reserved_entry_usd, "1");
        assert_eq!(initial.book_sequence, 3);
        let proof = partial_proof(&input, 1, "0.4", 'b');
        let accepted = owner.settle_partial_entry(&proof, 3, 7).unwrap();
        assert_eq!(accepted.book_sequence, 4);
        assert_eq!(accepted.cumulative_entry_usd, "0.4");
        assert_eq!(owner.readback().unwrap().entry_bound_breach_usd, "0");
        owner.close();
        let mut recovered = open(&fixture);
        assert_eq!(recovered.readback().unwrap().book_sequence, 4);
        assert_eq!(recovered.readback().unwrap().global_reserved_usdt, "3");
        assert_eq!(recovered.readback().unwrap().reserved_entry_usd, "0.6");
        assert_eq!(recovered.readback().unwrap().consumed_entry_usd, "0.4");
        let replay = recovered.settle_partial_entry(&proof, 3, 7).unwrap();
        assert!(replay.replayed);
        assert_eq!(recovered.readback().unwrap().book_sequence, 4);
        let next = partial_proof(&input, 2, "0.8", 'c');
        assert_eq!(
            recovered
                .settle_partial_entry(&next, 4, 7)
                .unwrap()
                .book_sequence,
            5
        );
        assert_eq!(recovered.readback().unwrap().reserved_entry_usd, "0.2");
        assert_eq!(recovered.readback().unwrap().consumed_entry_usd, "0.8");
    }
}

/// Deterministic domain proof used only in this original RiskBook money
/// fixture. Production cannot supply a proof: the single SQL transaction
/// retrieves the sealed AccountStore terminal/late revision itself.
fn fixture_final_proof(
    input: &ReserveInput,
    schema: &str,
    revision: u64,
    cumulative: &str,
    digest_letter: char,
) -> PartialEntrySqlProof {
    use crate::EntrySettlementEvidence;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    PartialEntrySqlProof::from_account_receipt(EntrySettlementEvidence {
        schema: schema.into(),
        account_key: account('a'),
        intent_id: input.intent_id.clone(),
        plan_hash: input.plan_sha256.clone(),
        reservation_key: input.reservation_key(),
        venue_order_id: "venue-order-001".into(),
        venue_update_ms: 11_000,
        source_covered_through_ms: 12_000,
        venue_filled_quantity: if cumulative == "0" {
            Decimal::ZERO
        } else {
            Decimal::ONE
        },
        booked_notional_and_fee_usdt: Decimal::from_str(cumulative).unwrap(),
        cumulative_entry_usd: Decimal::from_str(cumulative).unwrap(),
        settlement_revision: revision,
        source_digest: uid(digest_letter),
    })
    .unwrap()
}

#[test]
fn risk_book_contract_sql_zero_terminal_then_post_release_late_fee_is_one_budget() {
    // Real SQLite, ORIGINAL RiskBook exact-money algorithm. No production
    // fee source or Release permission is fabricated by this domain fixture.
    let fixture = Fixture::new("terminal-zero-late");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let input = request('a', "intentterminalzero", "3");
    let mut owner = open(&fixture);
    owner.reserve(&input, &census()).unwrap();
    owner.begin_post(&post_fence(&input, 1)).unwrap();
    owner.close();
    let mut db = rusqlite::Connection::open(&fixture.path).unwrap();
    let terminal = fixture_final_proof(&input, "okx.entry-terminal-settlement/v1", 1, "0", 'b');
    {
        let tx = db.transaction().unwrap();
        let receipt =
            RiskBookStore::settle_partial_entry_sql_transaction(&tx, None, &terminal, 2, 7)
                .unwrap();
        assert_eq!(receipt.book_sequence, 3);
        assert_eq!(receipt.cumulative_entry_usd, "0");
        assert!(!receipt.replayed);
        tx.commit().unwrap();
    }
    {
        let tx = db.transaction().unwrap();
        assert!(
            RiskBookStore::settle_partial_entry_sql_transaction(&tx, None, &terminal, 2, 7,)
                .unwrap()
                .replayed
        );
        tx.rollback().unwrap();
    }
    // The real source-verified terminal RELEASE has its own separate
    // owner/receipt and is not implemented here. Emulate only the already
    // committed final state in the *test* fixture to prove late fee math.
    {
        let tx = db.transaction().unwrap();
        tx.execute(
            "UPDATE risk_entries SET state='RELEASED',
                reserved_usdt='0',reserved_loss_usd='0',reserved_entry_usd='0',
                release_proof_digest=?1,release_request_hash=?2,
                release_reason='fixture-terminal-reconciled'
             WHERE reservation_key=?3",
            rusqlite::params![uid('c'), uid('d'), input.reservation_key()],
        )
        .unwrap();
        tx.execute(
            "UPDATE risk_meta SET
                global_reserved_usdt='0',loss_reserved_usd='0',
                entry_reserved_usd='0',book_sequence=4
             WHERE singleton=1 AND book_sequence=3",
            [],
        )
        .unwrap();
        tx.execute(
            "UPDATE risk_account_occupied SET reserved_usdt='0'
             WHERE account_key=?1",
            [account('a')],
        )
        .unwrap();
        tx.execute(
            "UPDATE risk_strategy_occupied SET reserved_usdt='0'
             WHERE account_key=?1",
            [account('a')],
        )
        .unwrap();
        tx.commit().unwrap();
    }
    let mut reopened = open(&fixture);
    let initial = reopened.readback().unwrap();
    assert_eq!(initial.book_sequence, 4);
    assert_eq!(initial.global_reserved_usdt, "0");
    assert_eq!(initial.reserved_entry_usd, "0");
    assert_eq!(initial.consumed_entry_usd, "0");
    let partial = partial_proof(&input, 2, "0.3", 'e');
    let tx = db.transaction().unwrap();
    assert!(matches!(
        RiskBookStore::settle_partial_entry_sql_transaction(&tx, None, &partial, 4, 7,),
        Err(RiskStoreError::AlreadyResolved)
    ));
    tx.rollback().unwrap();
    let late = fixture_final_proof(&input, "okx.entry-late-settlement/v1", 2, "0.3", 'f');
    {
        let tx = db.transaction().unwrap();
        let accepted =
            RiskBookStore::settle_partial_entry_sql_transaction(&tx, None, &late, 4, 7).unwrap();
        assert_eq!(accepted.book_sequence, 5);
        assert_eq!(accepted.cumulative_entry_usd, "0.3");
        tx.commit().unwrap();
    }
    // No second reservation was created and the original capital remains
    // RELEASED, although the immutable late economic delta is charged.
    reopened.close();
    let mut reopened = open(&fixture);
    let after = reopened.readback().unwrap();
    assert_eq!(after.book_sequence, 5);
    assert_eq!(after.global_reserved_usdt, "0");
    assert_eq!(after.reserved_loss_usd, "0");
    assert_eq!(after.reserved_entry_usd, "0");
    assert_eq!(after.consumed_entry_usd, "0.3");
    let db = rusqlite::Connection::open(&fixture.path).unwrap();
    assert_eq!(
        db.query_row::<String, _, _>(
            "SELECT state FROM risk_entries WHERE reservation_key=?1",
            [input.reservation_key()],
            |row| row.get(0)
        )
        .unwrap(),
        "RELEASED"
    );
    let mut db = db;
    let tx = db.transaction().unwrap();
    assert!(
        RiskBookStore::settle_partial_entry_sql_transaction(&tx, None, &late, 4, 7,)
            .unwrap()
            .replayed
    );
    let changed = fixture_final_proof(&input, "okx.entry-late-settlement/v1", 2, "0.4", 'a');
    assert!(matches!(
        RiskBookStore::settle_partial_entry_sql_transaction(&tx, None, &changed, 5, 7,),
        Err(RiskStoreError::IdempotencyConflict)
    ));
    tx.rollback().unwrap();
}

#[test]
fn risk_book_contract_sql_partial_entry_regression_and_breach_fail_closed() {
    let fixture = Fixture::new("entry-breach");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut s = open(&fixture);
    let input = request('a', "intentpartial2", "3");
    s.reserve(&input, &census()).unwrap();
    s.begin_post(&post_fence(&input, 1)).unwrap();
    let over = partial_proof(&input, 1, "2", 'b');
    assert_eq!(
        s.settle_partial_entry(&over, 2, 7)
            .unwrap()
            .entry_bound_breach_usd,
        "1"
    );
    let read = s.readback().unwrap();
    assert_eq!(read.consumed_entry_usd, "2");
    assert_eq!(read.reserved_entry_usd, "0");
    assert_eq!(read.entry_bound_breach_usd, "1");
    // The modelled loss remains in the same original reservation.
    assert_eq!(read.global_reserved_usdt, "3");
    assert!(matches!(
        s.reserve(&request('b', "intentblocked1", "1"), &census()),
        Err(RiskStoreError::Denied(RiskRuleError::BoundBreach))
    ));
    let regression = partial_proof(&input, 2, "1.5", 'c');
    assert!(matches!(
        s.settle_partial_entry(&regression, 3, 7),
        Err(RiskStoreError::Invalid(
            "ENTRY_SETTLEMENT_REVISION_CONFLICT"
        ))
    ));
    let skipped = partial_proof(&input, 3, "3", 'd');
    assert!(matches!(
        s.settle_partial_entry(&skipped, 3, 7),
        Err(RiskStoreError::Invalid(
            "ENTRY_SETTLEMENT_REVISION_CONFLICT"
        ))
    ));
    let wrong_epoch = partial_proof(&input, 2, "2.5", 'c');
    assert!(matches!(
        s.settle_partial_entry(&wrong_epoch, 3, 6),
        Err(RiskStoreError::StaleGeneration)
    ));
    assert_eq!(s.readback().unwrap(), read);
}

#[test]
fn risk_book_contract_sql_partial_entry_integrity_detects_forged_cache() {
    let fixture = Fixture::new("entry-integrity");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut s = open(&fixture);
    let input = request('a', "intentpartial3", "2");
    s.reserve(&input, &census()).unwrap();
    s.begin_post(&post_fence(&input, 1)).unwrap();
    s.settle_partial_entry(&partial_proof(&input, 1, "0.25", 'b'), 2, 7)
        .unwrap();
    s.close();
    let db = rusqlite::Connection::open(&fixture.path).unwrap();
    db.execute(
        "UPDATE risk_meta SET entry_consumed_usd='0' WHERE singleton=1",
        [],
    )
    .unwrap();
    drop(db);
    assert!(matches!(
        RiskBookStore::open_existing(&fixture.path, "test-installation", &opening().campaign_id),
        Err(RiskStoreError::Corrupt("ENTRY_CONSUMED_AGGREGATES"))
    ));
}

#[test]
fn risk_book_contract_sql_child_partial_entry_debit_commit_wait() {
    let Ok(db) = std::env::var("OKX_510_TEST_PARTIAL_KILL_DB") else {
        return;
    };
    let mut store = RiskBookStore::open_existing(
        &PathBuf::from(db),
        "test-installation",
        &opening().campaign_id,
    )
    .unwrap();
    let input = request('a', "intentkillentry1", "3");
    store.reserve(&input, &census()).unwrap();
    store.begin_post(&post_fence(&input, 1)).unwrap();
    store
        .settle_partial_entry(&partial_proof(&input, 1, "0.4", 'b'), 2, 7)
        .unwrap();
    use std::io::Write;
    println!("RISK_ENTRY_DEBIT_COMMITTED_WITHOUT_REPLY");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(60));
    unreachable!("parent must kill this process after the actual SQLite commit");
}

#[test]
fn risk_book_contract_sql_os_kill_after_partial_entry_debit_is_exact() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::Duration;

    let fixture = Fixture::new("physical-partial-debit");
    RiskBookStore::initialize_fixture(&fixture.path, &opening()).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "risk_book_contract_sql_child_partial_entry_debit_commit_wait",
            "--nocapture",
        ])
        .env("OKX_510_TEST_PARTIAL_KILL_DB", &fixture.path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if line
                .unwrap_or_default()
                .contains("RISK_ENTRY_DEBIT_COMMITTED_WITHOUT_REPLY")
            {
                let _ = tx.send(());
                return;
            }
        }
    });
    let committed = rx.recv_timeout(Duration::from_secs(15));
    child.kill().unwrap();
    assert!(
        !child.wait().unwrap().success(),
        "writer must be physically OS killed"
    );
    reader.join().unwrap();
    assert!(
        committed.is_ok(),
        "real partial-entry COMMIT witness missing"
    );
    let mut recovered = open(&fixture);
    let input = request('a', "intentkillentry1", "3");
    let read = recovered.readback().unwrap();
    assert_eq!(read.book_sequence, 3);
    assert_eq!(read.consumed_entry_usd, "0.4");
    assert_eq!(read.reserved_entry_usd, "0.6");
    assert_eq!(read.global_reserved_usdt, "3");
    assert_eq!(read.reserved_loss_usd, "3");
    let replay = recovered
        .settle_partial_entry(&partial_proof(&input, 1, "0.4", 'b'), 2, 7)
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.book_sequence, 3);
    assert_eq!(recovered.readback().unwrap(), read);
}

async fn source_verified_partial_fixture(
    label: &str,
) -> (
    Fixture,
    crate::AccountStoreHandle,
    ReserveInput,
    crate::EntrySettlementEvidence,
) {
    use crate::{
        AccountMeta, AccountStoreHandle, Bill, CentralReceipt, Decision, EntryAdmissionEvidence,
        EntryQuoteEvidence, Fill, FinanceEntry, IngestPage, IntentPlan, IntentState, NewIntent,
        OpeningSnapshot, SourceCursor,
    };
    use rust_decimal::Decimal;
    use sha2::{Digest, Sha256};
    use std::time::{Duration, Instant};

    let fixture = Fixture::new(label);
    let mut config = opening();
    config.entry_limit = amt("200");
    for account in &mut config.accounts {
        account.entry_limit = amt("200");
    }
    RiskBookStore::initialize_fixture(&fixture.path, &config).unwrap();
    let mut input = request('a', "intenttwosql1", "3");
    input.proposal.entry_upper_usd = amt("100.05");
    {
        let mut campaign = open(&fixture);
        campaign.reserve(&input, &census()).unwrap();
        campaign.begin_post(&post_fence(&input, 1)).unwrap();
    }
    let account_owner = AccountStoreHandle::initialize_flat(
        fixture.root.join("account.sqlite"),
        AccountMeta {
            account_key: account('a'),
            installation_id: "test-installation".into(),
            initialized_at: 1,
            opening_source: "fixture_flat".into(),
            initialize_operation_id: "native-entry-setup".into(),
            release_id: "a".repeat(40),
        },
        OpeningSnapshot {
            source_ref: "fixture_flat".into(),
            observed_at: 1,
            opening_equity_usd: Decimal::from(200),
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USDT".into(), Decimal::from(200))],
        },
    )
    .await
    .unwrap();
    let reservation = input.reservation_key();
    let intent_id = input.intent_id.clone();
    let plan_hash = input.plan_sha256.clone();
    let source = account_owner
        .call(Instant::now() + Duration::from_secs(5), move |s| {
            s.apply_authority_policy(1, false, "campaign-native-test", Decimal::from(200))?;
            let definition = s.ensure_manual_instance(1)?;
            let prepared = s.prepare(
                &Decision {
                    decision_id: "decision_native_partial".into(),
                    instance_id: None,
                    decision_at: 2,
                    input_digest: "e".repeat(64),
                    policy_revision: 1,
                    code_version: "rust".into(),
                },
                &NewIntent {
                    intent_id: intent_id.clone(),
                    idempotency_key: "command_native_partial".into(),
                    request_hash: "b".repeat(64),
                    client_order_id: "c".repeat(32),
                    deadline: 100,
                    plan_hash: plan_hash.clone(),
                    policy_revision: 1,
                    risk_increase: true,
                    instrument_id: "BTC-USDT-SWAP".into(),
                    plan: IntentPlan {
                        strategy_definition_id: definition.definition_id,
                        strategy_definition_version: definition.version,
                        strategy_parameters_digest: definition.parameters_digest,
                        contract_value: Decimal::ONE,
                        inverse: false,
                        settlement_currency: "USDT".into(),
                        position_side: "long".into(),
                        private_source_generation: 1,
                        operation: "open".into(),
                        side: "buy".into(),
                        quantity: Decimal::ONE,
                        limit_price: Decimal::from(100),
                        stop_price: Some(Decimal::from(95)),
                        strategy_instance: "manual".into(),
                        planned_loss_usd: Decimal::from(3),
                        source_generation: 1,
                    },
                },
            )?;
            let quote = EntryQuoteEvidence {
                schema: "okx.entry-known-quote/v1".into(),
                account_key: account('a'),
                installation_id: "test-installation".into(),
                intent_id: intent_id.clone(),
                plan_hash: plan_hash.clone(),
                policy_revision: 1,
                instrument_id: "BTC-USDT-SWAP".into(),
                quantity: Decimal::ONE,
                limit_price: Decimal::from(100),
                contract_value: Decimal::ONE,
                fee_rate: Decimal::new(5, 4),
                maker_rate: "-0.0002".into(),
                taker_rate: "0.0005".into(),
                fee_level: "Lv1".into(),
                fee_exchange_at: 1,
                usd_per_usdt: Decimal::ONE,
                notional_usdt: Decimal::from(100),
                fee_usdt: Decimal::new(5, 2),
                total_usdt: Decimal::new(10005, 2),
                total_usd: Decimal::new(10005, 2),
                fee_generation: format!(
                    "sha256:{:x}",
                    Sha256::digest(
                        serde_json::to_vec(&(
                            "BTC-USDT-SWAP",
                            "test_reference",
                            "Lv1",
                            "-0.0002",
                            "0.0005",
                            true,
                        ))
                        .unwrap()
                    )
                ),
                reference_generation: "test_reference".into(),
                fee_received_at: 1,
                conversion_digest: "b".repeat(64),
                conversion_received_at: 1,
                observed_at: 2,
            };
            s.bind_entry_quote(&quote)?;
            s.bind_entry_admission(&EntryAdmissionEvidence {
                schema: "okx.entry-admission/v2".into(),
                account_key: account('a'),
                installation_id: "test-installation".into(),
                intent_id: intent_id.clone(),
                plan_hash: plan_hash.clone(),
                quote_digest: format!("{:x}", Sha256::digest(serde_json::to_vec(&quote).unwrap())),
                admission_usd_per_usdt: Decimal::ONE,
                other_entry_costs_upper_usd: Decimal::ZERO,
                entry_upper_usd: quote.total_usd,
                entry_proof_digest: "a".repeat(64),
                observed_at: quote.observed_at,
            })?;
            s.reserve(
                &CentralReceipt {
                    reservation_key: reservation,
                    account_key: account('a'),
                    installation_id: "test-installation".into(),
                    instance_id: None,
                    intent_id: intent_id.clone(),
                    plan_hash,
                    policy_revision: 1,
                    receipt_hash: "f".repeat(64),
                    requested_loss_usd: Decimal::from(3),
                    conversion_digest: "b".repeat(64),
                },
                3,
            )?;
            assert!(s.begin_post(&intent_id, 1, 4)?);
            s.record_outcome(
                &intent_id,
                IntentState::Acknowledged,
                Some("venue-order"),
                "native-venue-ack",
                4,
            )?;
            let half = Decimal::new(5, 1);
            let fee = Decimal::new(-1, 2);
            let entry = FinanceEntry {
                entry_id: "bill:partial:FEE".into(),
                economic_event_key: "bill:partial".into(),
                component: "FEE".into(),
                allocation_leg: "account".into(),
                amount: fee,
                currency: "USDT".into(),
                instance_id: None,
                source_ref: "fill:partial".into(),
                time: 6,
                reverses_entry_id: None,
            };
            s.ingest_page(&IngestPage {
                fills: vec![Fill {
                    fill_id: "partial".into(),
                    instrument_id: "BTC-USDT-SWAP".into(),
                    trade_id: "native-trade-partial".into(),
                    exchange_order_id: "venue-order".into(),
                    intent_id: Some(intent_id.clone()),
                    time: 6,
                    qty: half,
                    price: Decimal::from(100),
                    fee,
                    currency: "USDT".into(),
                    side: "buy".into(),
                }],
                finance: vec![entry.clone()],
                cursor: Some(SourceCursor {
                    source: "fills".into(),
                    stream_partition: "SWAP".into(),
                    cursor: String::new(),
                    watermark: 7,
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })?;
            s.ingest_page(&IngestPage {
                bills: vec![Bill {
                    bill_id: "partial".into(),
                    kind: "1:trade".into(),
                    time: 7,
                    amount: fee,
                    currency: "USDT".into(),
                    economic_event_key: "bill:partial".into(),
                }],
                cursor: Some(SourceCursor {
                    source: "bills".into(),
                    stream_partition: "account".into(),
                    cursor: String::new(),
                    watermark: 7,
                    generation: 1,
                    coverage_gaps: String::new(),
                }),
                ..Default::default()
            })?;
            s.ingest_page(&IngestPage {
                finance: vec![FinanceEntry {
                    source_ref: "bill:partial".into(),
                    time: 7,
                    ..entry
                }],
                ..Default::default()
            })?;
            s.record_order_readback(
                &intent_id,
                "venue-order",
                IntentState::PartiallyFilled,
                half,
                7,
            )?;
            let proof = s.bind_partial_entry_settlement(&intent_id, 7, half, 7)?;
            assert_eq!(proof.cumulative_entry_usd, Decimal::new(5001, 2));
            assert_eq!(proof.settlement_revision, 1);
            assert_eq!(
                s.partial_entry_settlement_receipt(&intent_id)?,
                Some(proof.clone())
            );
            assert!(s.partial_entry_ack_pending(&intent_id)?);
            // The native journal only carries one original source-backed value.
            assert_eq!(prepared.plan_hash, proof.plan_hash);
            Ok(proof)
        })
        .await
        .unwrap();
    (fixture, account_owner, input, source)
}

#[tokio::test]
async fn risk_book_contract_real_source_two_sql_owners_commit_then_ack_once() {
    use super::owner::RiskBookHandle;
    use std::time::{Duration, Instant};

    let (fixture, account, input, source) = source_verified_partial_fixture("two-owner-ack").await;
    let campaign = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let missing = campaign
        .settle_partial_from_account_sql(
            &account,
            "not-sourced-intent".into(),
            2,
            7,
            Instant::now() + Duration::from_secs(5),
        )
        .await;
    assert!(matches!(missing, Err(RiskStoreError::EntrySqlUnproven)));
    assert_eq!(campaign.readback().await.unwrap().book_sequence, 2);

    let receipt = campaign
        .settle_partial_from_account_sql(
            &account,
            input.intent_id.clone(),
            2,
            7,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(!receipt.replayed);
    assert_eq!(receipt.book_sequence, 3);
    assert_eq!(receipt.cumulative_entry_usd, "50.01");
    let read = campaign.readback().await.unwrap();
    assert_eq!(read.consumed_entry_usd, "50.01");
    assert_eq!(read.reserved_entry_usd, "50.04");
    assert_eq!(read.global_reserved_usdt, "3");
    assert_eq!(read.reserved_loss_usd, "3");
    assert_eq!(source.reservation_key, input.reservation_key());

    let id = input.intent_id.clone();
    assert!(
        !account
            .call(Instant::now() + Duration::from_secs(5), move |s| {
                s.partial_entry_ack_pending(&id)
            })
            .await
            .unwrap()
    );
    assert!(
        campaign
            .settle_partial_from_account_sql(
                &account,
                input.intent_id.clone(),
                2,
                7,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap()
            .replayed
    );
    assert_eq!(campaign.readback().await.unwrap(), read);
}

#[tokio::test]
async fn risk_book_contract_real_source_two_sql_owners_recover_missing_ack() {
    use super::owner::RiskBookHandle;
    use std::time::{Duration, Instant};

    let (fixture, account, input, source) =
        source_verified_partial_fixture("two-owner-lost-ack").await;
    let verified = PartialEntrySqlProof::from_account_receipt(source).unwrap();
    {
        let mut original = open(&fixture);
        let accepted = original.settle_partial_entry(&verified, 2, 7).unwrap();
        assert_eq!(accepted.book_sequence, 3);
        assert!(!accepted.replayed);
    }
    // The campaign commit exists, but the native account ACK has not happened.
    let pending_id = input.intent_id.clone();
    assert!(
        account
            .call(Instant::now() + Duration::from_secs(5), move |s| {
                s.partial_entry_ack_pending(&pending_id)
            })
            .await
            .unwrap()
    );
    let campaign = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let before = campaign.readback().await.unwrap();
    assert_eq!(before.consumed_entry_usd, "50.01");
    assert_eq!(before.reserved_entry_usd, "50.04");
    // An earlier book CAS is valid for EXACT committed-source replay only;
    // the SQL ACK is then written in the original account owner.
    let recovered = campaign
        .settle_partial_from_account_sql(
            &account,
            input.intent_id.clone(),
            2,
            7,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(recovered.replayed);
    assert_eq!(recovered.book_sequence, 3);
    let id = input.intent_id.clone();
    assert!(
        !account
            .call(Instant::now() + Duration::from_secs(5), move |s| {
                s.partial_entry_ack_pending(&id)
            })
            .await
            .unwrap()
    );
    assert_eq!(campaign.readback().await.unwrap(), before);
}

#[test]
fn risk_book_contract_sql_child_two_owner_entry_committed_without_account_ack() {
    let Ok(campaign_path) = std::env::var("OKX_510_TWO_OWNER_KILL_CAMPAIGN") else {
        return;
    };
    let account_path = std::env::var("OKX_510_TWO_OWNER_KILL_ACCOUNT").unwrap();
    let account_source = crate::AccountStore::open_existing(
        PathBuf::from(&account_path),
        &account('a'),
        "test-installation",
    )
    .unwrap();
    let original = account_source
        .partial_entry_settlement_receipt("intenttwosql1")
        .unwrap()
        .unwrap();
    assert!(
        account_source
            .partial_entry_ack_pending("intenttwosql1")
            .unwrap()
    );
    let proof = PartialEntrySqlProof::from_account_receipt(original).unwrap();
    let mut campaign = RiskBookStore::open_existing(
        &PathBuf::from(campaign_path),
        "test-installation",
        &opening().campaign_id,
    )
    .unwrap();
    let accepted = campaign.settle_partial_entry(&proof, 2, 7).unwrap();
    assert_eq!(accepted.book_sequence, 3);
    assert!(!accepted.replayed);
    assert_eq!(campaign.readback().unwrap().consumed_entry_usd, "50.01");
    // Deliberately NEVER ACK the native source. The parent must crash us
    // here and recover both original SQLite/WAL files independently.
    use std::io::Write;
    println!("TWO_SQL_CAMPAIGN_COMMITTED_ACCOUNT_ACK_PENDING");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(60));
    unreachable!("real OS kill must interrupt before native ACK");
}

#[tokio::test]
async fn risk_book_contract_sql_real_two_owner_os_kill_after_campaign_before_account_ack() {
    use super::owner::RiskBookHandle;
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let (fixture, account_owner, input, _) =
        source_verified_partial_fixture("two-sql-os-kill").await;
    let mut termination = account_owner.termination_signal();
    drop(account_owner);
    tokio::time::timeout(Duration::from_secs(5), async {
        while !*termination.borrow() {
            termination.changed().await.unwrap();
        }
    })
    .await
    .expect("original native account writer must close before child opens it");

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "risk_book_contract_sql_child_two_owner_entry_committed_without_account_ack",
            "--nocapture",
        ])
        .env("OKX_510_TWO_OWNER_KILL_CAMPAIGN", &fixture.path)
        .env(
            "OKX_510_TWO_OWNER_KILL_ACCOUNT",
            fixture.root.join("account.sqlite"),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(output).lines() {
            if line
                .unwrap_or_default()
                .contains("TWO_SQL_CAMPAIGN_COMMITTED_ACCOUNT_ACK_PENDING")
            {
                let _ = ready_tx.send(());
                return;
            }
        }
    });
    let committed = ready_rx.recv_timeout(Duration::from_secs(15));
    child.kill().unwrap();
    assert!(
        !child.wait().unwrap().success(),
        "two-SQL source writer must be OS-killed"
    );
    reader.join().unwrap();
    assert!(
        committed.is_ok(),
        "child never committed original campaign debit"
    );

    let reopened_account = crate::AccountStoreHandle::open_existing(
        fixture.root.join("account.sqlite"),
        account('a'),
        "test-installation".into(),
    )
    .await
    .unwrap();
    let pending_id = input.intent_id.clone();
    assert!(
        reopened_account
            .call(Instant::now() + Duration::from_secs(5), move |s| {
                s.partial_entry_ack_pending(&pending_id)
            })
            .await
            .unwrap()
    );
    let campaign = RiskBookHandle::open_existing(
        fixture.path.clone(),
        "test-installation".into(),
        opening().campaign_id,
    )
    .await
    .unwrap();
    let before = campaign.readback().await.unwrap();
    assert_eq!(before.book_sequence, 3);
    assert_eq!(before.consumed_entry_usd, "50.01");
    assert_eq!(before.reserved_entry_usd, "50.04");
    assert_eq!(before.global_reserved_usdt, "3");
    let accepted = campaign
        .settle_partial_from_account_sql(
            &reopened_account,
            input.intent_id.clone(),
            2,
            7,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert!(accepted.replayed);
    assert_eq!(accepted.book_sequence, 3);
    let id = input.intent_id.clone();
    assert!(
        !reopened_account
            .call(Instant::now() + Duration::from_secs(5), move |s| {
                s.partial_entry_ack_pending(&id)
            })
            .await
            .unwrap()
    );
    assert_eq!(campaign.readback().await.unwrap(), before);
}

// One original account receipt constructor for the joint SQL/OS-kill witness.
// Test-only fixed inputs; production proofs are deliberately not fabricated.
fn unified_reserve_account_receipt(input: &ReserveInput) -> crate::CentralReceipt {
    crate::CentralReceipt {
        reservation_key: input.reservation_key(),
        account_key: format!("{}:real:{}", input.venue, input.uid),
        installation_id: "test-installation".into(),
        instance_id: Some("strategy_test".into()),
        intent_id: input.intent_id.clone(),
        plan_hash: input.plan_sha256.clone(),
        policy_revision: 1,
        receipt_hash: uid('b'),
        requested_loss_usd: rust_decimal::Decimal::from(6),
        conversion_digest: input.conversion_digest.clone(),
    }
}

#[test]
fn risk_book_contract_unified_reserve_child_commit_boundary() {
    let Ok(file) = std::env::var("OKX_510_UNIFIED_RESERVE_KILL_DB") else {
        return;
    };
    let phase = std::env::var("OKX_510_UNIFIED_RESERVE_KILL_PHASE").unwrap();
    let mut db = rusqlite::Connection::open(file).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    let input = request('a', "intentA0001", "6");
    let receipt = unified_reserve_account_receipt(&input);
    let tx = db
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    assert!(matches!(
        RiskBookStore::reserve_unified_account_transaction(
            &tx,
            "test-installation",
            &opening().campaign_id,
            &input,
            &census(),
            &receipt,
        )
        .unwrap(),
        ReserveDecision::Held(HeldReceipt {
            replayed: false,
            ..
        })
    ));
    match phase.as_str() {
        "BEFORE_COMMIT" => {} // Hold the live uncommitted original account + risk updates.
        "AFTER_COMMIT" => tx.commit().unwrap(),
        _ => panic!("unexpected physical Reserve kill boundary"),
    }
    use std::io::Write;
    println!("UNIFIED_RESERVE_510_{phase}_READY");
    std::io::stdout().flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(60));
    unreachable!("the parent must kill this actual Rust OS process")
}

fn unified_reserve_os_kill(path: &std::path::Path, phase: &str) {
    use std::{
        io::{BufRead, BufReader},
        process::{Command, Stdio},
        sync::mpsc,
        time::Duration,
    };
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "risk_book_contract_unified_reserve_child_commit_boundary",
            "--nocapture",
        ])
        .env("OKX_510_UNIFIED_RESERVE_KILL_DB", path)
        .env("OKX_510_UNIFIED_RESERVE_KILL_PHASE", phase)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stream = child.stdout.take().unwrap();
    let expected = format!("UNIFIED_RESERVE_510_{phase}_READY");
    let (sender, receiver) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stream).lines() {
            if line.unwrap_or_default().contains(&expected) {
                let _ = sender.send(());
                return;
            }
        }
    });
    let marker = receiver.recv_timeout(Duration::from_secs(15));
    let killed = child.kill();
    let exit = child.wait().unwrap();
    reader.join().unwrap();
    assert!(
        marker.is_ok(),
        "joint original AccountStore+RiskBook {phase} SQL witness not reached"
    );
    assert!(
        killed.is_ok() && !exit.success(),
        "physical child kill failed"
    );
}

#[test]
fn risk_book_contract_scoped_original_reserve_shared_budget_and_high_water_on_denial() {
    use rusqlite::{Connection, params};
    let f = Fixture::new("finance-scoped-reserve");
    let mut db = Connection::open(&f.path).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    db.pragma_update(None, "journal_mode", "WAL").unwrap();
    db.pragma_update(None, "synchronous", "FULL").unwrap();
    db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
        .unwrap();
    let campaign = opening().campaign_id;
    // Test-only source fixtures: original schema + FK-backed policy rows.
    // They are NOT an authenticated producer or permission for live trading.
    {
        let tx = db.transaction().unwrap();
        for letter in ['a', 'b'] {
            let key = account(letter);
            let intent = format!("intent{}0001", letter.to_ascii_uppercase());
            tx.execute(
                "INSERT INTO account_meta(
                installation_id,account_key,singleton,schema_version,initialized_at,
                opening_source,initialize_operation_id,release_id)
                VALUES('test-installation',?1,1,2,1,'test-only',?2,'release')",
                params![&key, format!("init-{letter}")],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO account_policy(
                installation_id,account_key,singleton,revision,paused,
                campaign_id,campaign_limit,campaign_loss)
                VALUES('test-installation',?1,1,1,0,?2,'10','0')",
                params![&key, &campaign],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO strategy_definition_versions(
                installation_id,account_key,definition_id,version,parameters,
                parameters_digest,created_at)
                VALUES('test-installation',?1,'definition','1','{}',?2,1)",
                params![&key, uid('f')],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO strategy_instances(
                installation_id,account_key,instance_id,definition_id,
                definition_version,parameters,enabled,policy_revision,budget,
                last_decision_key,parameters_digest)
                VALUES('test-installation',?1,'strategy_test','definition',
                '1','{}',1,1,'10',NULL,?2)",
                params![&key, uid('f')],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO decisions(
                installation_id,account_key,decision_id,instance_id,
                decision_at,input_digest,policy_revision,code_version)
                VALUES('test-installation',?1,?2,'strategy_test',1,?3,1,'rust')",
                params![&key, format!("decision-{letter}"), uid('b')],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO intents(
                installation_id,account_key,intent_id,idempotency_key,request_hash,
                decision_id,state,revision,client_order_id,deadline,plan_hash,
                policy_revision,risk_increase,instrument_id,created_at,updated_at)
                VALUES('test-installation',?1,?2,?3,?4,?5,'PREPARED',0,
                ?6,100000,?7,1,1,'BTC-USDT-SWAP',1,1)",
                params![
                    &key,
                    &intent,
                    format!("request-{letter}"),
                    uid('c'),
                    format!("decision-{letter}"),
                    format!("client-{letter}"),
                    uid('e')
                ],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO intent_plans(
                installation_id,account_key,intent_id,operation,side,quantity,
                limit_price,stop_price,strategy_instance,planned_loss_usd,
                source_generation,position_side,private_source_generation,
                contract_value,inverse,settlement_currency,strategy_definition_id,
                strategy_definition_version,strategy_parameters_digest)
                VALUES('test-installation',?1,?2,'open','buy','1','100','95',
                'strategy_test','6',1,'long',1,'1',0,'USDT','definition','1',?3)",
                params![&key, &intent, uid('f')],
            )
            .unwrap();
        }
        tx.execute(
            "INSERT INTO risk_meta(
            singleton,campaign_id,installation_id,policy_revision,generation,
            book_sequence,legacy_fenced,global_limit_usdt,global_reserved_usdt,
            loss_limit_usd,loss_consumed_usd,loss_reserved_usd,entry_limit_usd,
            entry_consumed_usd,entry_opening_consumed_usd,entry_reserved_usd,
            entry_bound_breach_usd)
            VALUES(1,?1,'test-installation',1,7,0,1,'10','0',
            '100','0','0','100','0','0','0','0')",
            [&campaign],
        )
        .unwrap();
        for letter in ['a', 'b'] {
            let key = account(letter);
            tx.execute(
                "INSERT INTO risk_account_occupied(
                installation_id,campaign_id,account_key,reserved_usdt)
                VALUES('test-installation',?1,?2,'0')",
                params![&campaign, &key],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO risk_policy_account(
                installation_id,campaign_id,account_key,limit_usdt,order_limit_usdt,
                entry_limit_usd)
                VALUES('test-installation',?1,?2,'10','8','10')",
                params![&campaign, &key],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO risk_loss_source(
                installation_id,campaign_id,account_key,cumulative_loss_usd,source_digest)
                VALUES('test-installation',?1,?2,'0',?3)",
                params![&campaign, &key, uid('d')],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO risk_policy_strategy(
                installation_id,campaign_id,account_key,strategy_id,limit_usdt)
                VALUES('test-installation',?1,?2,'strategy_test','8')",
                params![&campaign, &key],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO risk_strategy_occupied(
                installation_id,campaign_id,account_key,strategy_id,reserved_usdt)
                VALUES('test-installation',?1,?2,'strategy_test','0')",
                params![&campaign, &key],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    let original = request('a', "intentA0001", "6");
    let second = request('b', "intentB0001", "6");
    let receipt_a = unified_reserve_account_receipt(&original);
    let receipt_b = unified_reserve_account_receipt(&second);
    // Original account policy is the source of campaign ownership. Its
    // campaign, revision, paused flag and intent risk-increase must all agree
    // BEFORE staging risk capacity in this same SQL transaction.
    for mutation in [
        "UPDATE account_policy SET campaign_id='foreign-campaign'
         WHERE installation_id='test-installation' AND account_key=?1",
        "UPDATE account_policy SET revision=2
         WHERE installation_id='test-installation' AND account_key=?1",
        "UPDATE account_policy SET paused=1
         WHERE installation_id='test-installation' AND account_key=?1",
        "UPDATE intents SET risk_increase=0
         WHERE installation_id='test-installation' AND account_key=?1",
    ] {
        let tx = db.transaction().unwrap();
        assert_eq!(tx.execute(mutation, [account('a')]).unwrap(), 1);
        assert!(matches!(
            RiskBookStore::reserve_unified_account_transaction(
                &tx,
                "test-installation",
                &campaign,
                &original,
                &census(),
                &receipt_a
            ),
            Err(RiskStoreError::Invalid(
                "UNIFIED_ACCOUNT_CAMPAIGN_POLICY_UNPROVEN"
            ))
        ));
        tx.rollback().unwrap();
    }
    // The original account plan, not a caller-selected alternate RiskBook
    // strategy bucket, owns this reservation. Reject before touching any risk
    // high-water/cache, even when account and campaign identities are valid.
    {
        let mut wrong_bucket = original.clone();
        wrong_bucket.strategy_id = "strategy_other".into();
        let tx = db.transaction().unwrap();
        assert!(matches!(
            RiskBookStore::reserve_unified_account_transaction(
                &tx,
                "test-installation",
                &campaign,
                &wrong_bucket,
                &census(),
                &receipt_a,
            ),
            Err(RiskStoreError::Invalid(
                "UNIFIED_ACCOUNT_STRATEGY_OWNER_UNPROVEN"
            ))
        ));
        tx.rollback().unwrap();
    }
    {
        use crate::account_worker::{
            UnifiedFinanceWrite, UnifiedFinanceWriteReceipt,
            apply_original_unified_finance_transaction, native_reservation_receipt_sql,
        };
        let tx = db.transaction().unwrap();
        let receipt =
            native_reservation_receipt_sql(&tx, "test-installation", &campaign, &original).unwrap();
        let mut later = original.clone();
        later.now_ms += 1;
        later.native_incarnation = "e".repeat(32);
        let repeat =
            native_reservation_receipt_sql(&tx, "test-installation", &campaign, &later).unwrap();
        assert_eq!(receipt.receipt_hash, repeat.receipt_hash);
        assert_eq!(receipt.receipt_hash.len(), 64);
        assert_ne!(
            receipt.receipt_hash, receipt_a.receipt_hash,
            "Native identity is not a fabricated wire response hash"
        );
        let mut changed = original.clone();
        changed.plan_sha256 = if original.plan_sha256 == uid('a') {
            uid('b')
        } else {
            uid('a')
        };
        assert!(
            native_reservation_receipt_sql(&tx, "test-installation", &campaign, &changed).is_err()
        );
        assert!(native_reservation_receipt_sql(&tx, "foreign", &campaign, &original).is_err());
        for replay in [false, true] {
            let result = apply_original_unified_finance_transaction(
                &tx,
                "test-installation",
                UnifiedFinanceWrite::ReserveNative {
                    campaign: campaign.clone(),
                    input: original.clone(),
                    census: census(),
                },
            )
            .unwrap();
            assert!(
                matches!(result,UnifiedFinanceWriteReceipt::Risk(super::UnifiedRiskDecision::Reserved {replayed,..}) if replayed==replay)
            );
        }
        let saved: String = tx
            .query_row(
                "SELECT receipt_hash FROM risk_receipts WHERE account_key=?1 AND intent_id=?2",
                params![account('a'), original.intent_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(saved, receipt.receipt_hash);
        let rows: i64 = tx
            .query_row("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1);
        tx.rollback().unwrap();
    }
    // The AccountStore check runs AFTER RiskBook has staged a HELD reservation.
    // If the original owner proof disagrees, the caller must roll back BOTH
    // writes, not commit a risk-only reservation with no native receipt.
    {
        let mut foreign_owner = receipt_a.clone();
        foreign_owner.instance_id = None;
        let tx = db.transaction().unwrap();
        assert!(matches!(
            RiskBookStore::reserve_unified_account_transaction(
                &tx,
                "test-installation",
                &campaign,
                &original,
                &census(),
                &foreign_owner
            ),
            Err(RiskStoreError::Invalid("CENTRAL_INSTANCE_MISMATCH"))
        ));
        // Deliberately commit the OUTER transaction despite the error.
        // The joint operation itself must have rolled back its savepoint.
        tx.commit().unwrap();
    }
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_receipts", [], |r| r.get(0))
            .unwrap(),
        0
    );
    // The first uncommitted reserve is a real v6 SQL mutation: rollback must
    // eliminate both the reservation and all associated budget/cache writes.
    {
        let tx = db.transaction().unwrap();
        assert!(matches!(
            RiskBookStore::reserve_unified_account_transaction(
                &tx,
                "test-installation",
                &campaign,
                &original,
                &census(),
                &receipt_a
            )
            .unwrap(),
            ReserveDecision::Held(_)
        ));
        tx.rollback().unwrap();
    }
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_receipts", [], |r| r.get(0))
            .unwrap(),
        0
    );
    // Kill the REAL Rust writer process with BOTH original Reserve changes
    // staged but not committed. A cold process must observe no half-reserve.
    unified_reserve_os_kill(&f.path, "BEFORE_COMMIT");
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_receipts", [], |r| r.get(0))
            .unwrap(),
        0
    );
    // After the SAME single COMMIT, kill before the caller receives an ACK.
    // The original AccountStore and RiskBook receipt must recover together.
    unified_reserve_os_kill(&f.path, "AFTER_COMMIT");
    let read = |db: &Connection| {
        db.query_row(
            "SELECT global_reserved_usdt,loss_consumed_usd,book_sequence FROM risk_meta
             WHERE installation_id='test-installation' AND campaign_id=?1",
            [&campaign],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            },
        )
        .unwrap()
    };
    assert_eq!(read(&db), ("6".into(), "0".into(), 1));
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_receipts", [], |r| r.get(0))
            .unwrap(),
        1
    );
    let a_state: String = db
        .query_row(
            "SELECT state FROM intents WHERE installation_id='test-installation'
         AND account_key=?1 AND intent_id='intentA0001'",
            [account('a')],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(a_state, "RESERVED");
    // B is denied by the SAME cohort budget; its newer observed loss must
    // still be durable even though no second reservation can be created.
    let mut fresh = census();
    fresh.cumulative_loss_usd.insert(account('b'), amt("1"));
    {
        let tx = db.transaction().unwrap();
        assert!(matches!(
            RiskBookStore::reserve_unified_account_transaction(
                &tx,
                "test-installation",
                &campaign,
                &second,
                &fresh,
                &receipt_b
            )
            .unwrap(),
            ReserveDecision::Rejected(RiskStoreError::Denied(
                RiskRuleError::InsufficientAggregateOrAccountRisk
            ))
        ));
        tx.commit().unwrap();
    }
    assert_eq!(read(&db), ("6".into(), "1".into(), 2));
    let b_state: String = db
        .query_row(
            "SELECT state FROM intents WHERE installation_id='test-installation'
         AND account_key=?1 AND intent_id='intentB0001'",
            [account('b')],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(b_state, "PREPARED");
    drop(db);
    let mut db = Connection::open(&f.path).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    {
        let tx = db.transaction().unwrap();
        assert!(matches!(
            RiskBookStore::reserve_unified_account_transaction(
                &tx,
                "test-installation",
                &campaign,
                &original,
                &fresh,
                &receipt_a
            )
            .unwrap(),
            ReserveDecision::Held(HeldReceipt { replayed: true, .. })
        ));
        tx.rollback().unwrap();
    }
    assert_eq!(read(&db), ("6".into(), "1".into(), 2));
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_entries", [], |r| r.get(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_receipts", [], |r| r.get(0))
            .unwrap(),
        1
    );
    // Exercise two more ORIGINAL RiskBook financial operations against the
    // same installation-wide v6 SQL. The test explicitly stages POSTED in
    // its own uncommitted transaction; this is NOT a causal proof or an
    // authenticated production producer. Only the original arithmetic,
    // scope, CAS, replay and rollback are being proved here.
    let scope = Some(("test-installation", campaign.as_str()));
    {
        let tx = db.transaction().unwrap();
        assert_eq!(
            tx.execute(
                "UPDATE risk_entries SET state='POSTED',
                 post_authorized_at_ms=held_at_ms+1,
                 post_sequence=book_sequence+1,
                 post_proof_digest=?4,post_source_digest=?5
             WHERE installation_id=?1 AND campaign_id=?2
               AND reservation_key=?3 AND state='HELD'",
                rusqlite::params![
                    "test-installation",
                    &campaign,
                    original.reservation_key(),
                    uid('a'),
                    uid('b')
                ],
            )
            .unwrap(),
            1
        );
        // The caller cannot mint entry consumption: a correct POSTED risk
        // row alone is NOT a durable AccountStore fill/fee revision.
        assert!(matches!(
            RiskBookStore::settle_unified_entry_from_account_sql_transaction(
                &tx,
                "test-installation",
                &campaign,
                &account('a'),
                &original.intent_id,
                2,
                7,
                super::EntrySettlementSource::Partial,
            ),
            Err(RiskStoreError::EntrySqlUnproven)
        ));
        // Test-only staged AccountStore format: the original real producer is
        // exercised separately by source_verified_partial_fixture. Here all
        // v6 scoped JOINS, immutable revision, single TX and ACK are tested.
        let source = crate::EntrySettlementEvidence {
            schema: "okx.entry-partial-settlement/v1".into(),
            account_key: account('a'),
            intent_id: original.intent_id.clone(),
            plan_hash: original.plan_sha256.clone(),
            reservation_key: original.reservation_key(),
            venue_order_id: "venue-order-001".into(),
            venue_update_ms: 11_000,
            source_covered_through_ms: 11_000,
            venue_filled_quantity: rust_decimal::Decimal::ONE,
            booked_notional_and_fee_usdt: rust_decimal::Decimal::ONE,
            cumulative_entry_usd: rust_decimal::Decimal::new(4, 1),
            settlement_revision: 1,
            source_digest: uid('b'),
        };
        tx.execute(
            "UPDATE intents SET state='PARTIALLY_FILLED',begin_post_at=10001,
             exchange_order_id=?4 WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
            rusqlite::params![
                "test-installation",
                account('a'),
                &original.intent_id,
                &source.venue_order_id
            ],
        )
        .unwrap();
        tx.execute(
            "UPDATE risk_receipts SET state='POSTED' WHERE
             installation_id=?1 AND account_key=?2 AND intent_id=?3",
            rusqlite::params!["test-installation", account('a'), &original.intent_id],
        )
        .unwrap();
        let bytes = serde_json::to_string(&source).unwrap();
        for (id, kind) in [
            (
                format!("entry-partial-head:{}", source.intent_id),
                "ENTRY_PARTIAL_HEAD",
            ),
            (
                format!("entry-partial:{}:r1", source.intent_id),
                "ENTRY_PARTIAL_SETTLEMENT_PROVEN",
            ),
        ] {
            tx.execute(
                "INSERT INTO operation_receipts(
                   installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,?2,?3,?4,?5,?6,11000)",
                rusqlite::params!["test-installation",account('a'),id,kind,
                    &source.plan_hash,&bytes],
            ).unwrap();
        }
        let first = RiskBookStore::settle_unified_entry_from_account_sql_transaction(
            &tx,
            "test-installation",
            &campaign,
            &account('a'),
            &original.intent_id,
            2,
            7,
            super::EntrySettlementSource::Partial,
        )
        .unwrap();
        assert_eq!(first.book_sequence, 3);
        assert_eq!(first.cumulative_entry_usd, "0.4");
        assert!(!first.replayed);
        let repeated = RiskBookStore::settle_unified_entry_from_account_sql_transaction(
            &tx,
            "test-installation",
            &campaign,
            &account('a'),
            &original.intent_id,
            2,
            7,
            super::EntrySettlementSource::Partial,
        )
        .unwrap();
        assert!(repeated.replayed);
        let ack: String = tx
            .query_row(
                "SELECT kind FROM operation_receipts WHERE
             installation_id=?1 AND account_key=?2 AND operation_id=?3",
                rusqlite::params![
                    "test-installation",
                    account('a'),
                    format!("entry-partial-settled:{}:r1", source.intent_id),
                ],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(ack, "ENTRY_PARTIAL_SETTLED");
        let proof = PartialEntrySqlProof::from_account_receipt(source.clone()).unwrap();
        assert_eq!(repeated.book_sequence, 3);
        let changed = partial_proof(&original, 1, "0.5", 'b');
        assert!(matches!(
            RiskBookStore::settle_partial_entry_sql_transaction(&tx, scope, &changed, 3, 7,),
            Err(RiskStoreError::IdempotencyConflict)
        ));
        assert!(
            RiskBookStore::settle_partial_entry_sql_transaction(
                &tx,
                Some(("test-installation", "foreign-campaign")),
                &proof,
                2,
                7,
            )
            .is_err(),
            "a second campaign cannot debit the original campaign's entry"
        );
        let (consumed, reserved): (String, String) = tx
            .query_row(
                "SELECT entry_consumed_usd,entry_reserved_usd FROM risk_meta
             WHERE installation_id=?1 AND campaign_id=?2",
                rusqlite::params!["test-installation", &campaign],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((consumed.as_str(), reserved.as_str()), ("0.4", "0.6"));
        tx.rollback().unwrap();
    }
    assert_eq!(read(&db), ("6".into(), "1".into(), 2));
    assert_eq!(
        db.query_row::<String, _, _>(
            "SELECT state FROM risk_entries WHERE installation_id=?1
             AND campaign_id=?2 AND reservation_key=?3",
            rusqlite::params!["test-installation", &campaign, original.reservation_key()],
            |r| r.get(0),
        )
        .unwrap(),
        "HELD",
        "no POSTED/settlement proof escaped the caller-owned transaction"
    );

    // A no-send release must originate in the ORIGINAL AccountStore SQL,
    // not a fabricated caller proof. The synthetic receipt rows below are
    // scoped test fixtures: the real native REJECTED_UNSENT producer is
    // separately tested in the original account library. Nothing commits.
    let no_send = unsent('b', "intentB0001");
    {
        let tx = db.transaction().unwrap();
        assert!(matches!(
            RiskBookStore::release_unified_unsent_from_account_sql_transaction(
                &tx,
                "test-installation",
                &campaign,
                &account('b'),
                "intentB0001",
                2,
                7,
            ),
            Err(RiskStoreError::UnsentSqlUnproven)
        ));
        tx.execute(
            "UPDATE intents SET state='REJECTED',incident='REJECTED_UNSENT'
             WHERE installation_id=?1 AND account_key=?2 AND intent_id=?3",
            rusqlite::params!["test-installation", account('b'), "intentB0001"],
        )
        .unwrap();
        for (id, kind, result) in [
            (
                "risk-reserve:intentB0001",
                "RISK_RESERVE_ATTEMPT",
                no_send.reservation_key.as_str(),
            ),
            ("terminal:intentB0001", "REJECTED_UNSENT", "REJECTED_UNSENT"),
        ] {
            tx.execute(
                "INSERT INTO operation_receipts(
                  installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES(?1,?2,?3,?4,?5,?6,11000)",
                rusqlite::params![
                    "test-installation",account('b'),id,kind,uid('c'),result,
                ],
            ).unwrap();
        }
        let first = RiskBookStore::release_unified_unsent_from_account_sql_transaction(
            &tx,
            "test-installation",
            &campaign,
            &account('b'),
            "intentB0001",
            2,
            7,
        )
        .unwrap();
        assert_eq!(first.book_sequence, 3);
        assert!(!first.replayed);
        let repeated = RiskBookStore::release_unified_unsent_from_account_sql_transaction(
            &tx,
            "test-installation",
            &campaign,
            &account('b'),
            "intentB0001",
            2,
            7,
        )
        .unwrap();
        assert!(repeated.replayed);
        let observed = tx
            .query_row(
                "SELECT record_digest FROM risk_no_reserve
             WHERE installation_id=?1 AND campaign_id=?2 AND reservation_key=?3",
                rusqlite::params!["test-installation", &campaign, &no_send.reservation_key],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(observed.len(), 64);
        assert_ne!(observed, no_send.record_digest);
        assert!(
            RiskBookStore::release_unified_unsent_from_account_sql_transaction(
                &tx,
                "test-installation",
                "foreign-campaign",
                &account('b'),
                "intentB0001",
                2,
                7,
            )
            .is_err(),
            "no other campaign can grant or replay B's no-send fence"
        );
        tx.rollback().unwrap();
    }
    assert_eq!(read(&db), ("6".into(), "1".into(), 2));
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT COUNT(*) FROM risk_no_reserve", [], |r| r.get(0))
            .unwrap(),
        0,
        "test-only proof cannot persist without a trusted source and COMMIT"
    );
    // The same two-account v6 installation can now exercise a REAL one-file
    // capital refund from a separately accepted immutable original terminal
    // debit. These rows are synthetic SOURCE fixtures, not exchange proofs.
    // The production path cannot be activated from this test.
    let terminal = crate::EntrySettlementEvidence {
        schema: "okx.entry-terminal-settlement/v1".into(),
        account_key: account('a'),
        intent_id: original.intent_id.clone(),
        plan_hash: original.plan_sha256.clone(),
        reservation_key: original.reservation_key(),
        venue_order_id: "venue-terminal-a".into(),
        venue_update_ms: 10_002,
        source_covered_through_ms: 11_000,
        venue_filled_quantity: rust_decimal::Decimal::ONE,
        booked_notional_and_fee_usdt: rust_decimal::Decimal::ONE,
        cumulative_entry_usd: rust_decimal::Decimal::new(4, 1),
        settlement_revision: 1,
        source_digest: uid('b'),
    };
    let original_json = serde_json::to_string(&terminal).unwrap();
    {
        let tx = db.transaction().unwrap();
        tx.execute(
            "UPDATE risk_entries SET state='OPEN',
             post_authorized_at_ms=10001,post_sequence=book_sequence+1,
             post_proof_digest=?1,post_source_digest=?2,
             entry_settled_usd='0.4',entry_settlement_revision=1,
             entry_settlement_digest=?3,reserved_entry_usd='0.6'
             WHERE installation_id='test-installation' AND campaign_id=?4
               AND reservation_key=?5",
            params![
                uid('a'),
                uid('b'),
                &terminal.source_digest,
                &campaign,
                original.reservation_key()
            ],
        )
        .unwrap();
        tx.execute(
            "UPDATE risk_meta SET entry_consumed_usd='0.4',
               entry_reserved_usd='0.6'
             WHERE installation_id='test-installation' AND campaign_id=?1",
            [&campaign],
        )
        .unwrap();
        tx.execute(
            "UPDATE intents SET state='FILLED',begin_post_at=10001,
               exchange_order_id=?1
             WHERE installation_id='test-installation' AND account_key=?2
               AND intent_id=?3",
            params![&terminal.venue_order_id, account('a'), &original.intent_id],
        )
        .unwrap();
        tx.execute(
            "UPDATE risk_receipts SET state='OPEN' WHERE
             installation_id='test-installation' AND account_key=?1 AND intent_id=?2",
            params![account('a'), &original.intent_id],
        )
        .unwrap();
        for (operation, kind, hash, encoded, at) in [
            (
                format!("entry-settlement:{}", original.intent_id),
                "ENTRY_SETTLEMENT_PROVEN",
                terminal.plan_hash.clone(),
                original_json.clone(),
                10002,
            ),
            (
                format!("entry-terminal-settled:{}:r1", original.intent_id),
                "ENTRY_TERMINAL_SETTLED",
                terminal.plan_hash.clone(),
                terminal.source_digest.clone(),
                11000,
            ),
            (
                format!("terminal:{}", original.intent_id),
                "TERMINAL",
                uid('c'),
                "verified-test-venue-terminal".into(),
                10002,
            ),
        ] {
            tx.execute(
                "INSERT INTO operation_receipts(
                 installation_id,account_key,operation_id,kind,request_hash,result_ref,committed_at)
                 VALUES('test-installation',?1,?2,?3,?4,?5,?6)",
                params![account('a'), operation, kind, hash, encoded, at],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    }
    let mut flat = census();
    flat.cumulative_loss_usd.insert(account('b'), amt("1"));
    flat.read_floor_ms = 11_001;
    flat.generated_at_ms = 12_000;
    let input = TerminalResolveInput {
        account: account('a'),
        intent: original.intent_id.clone(),
        reservation: original.reservation_key(),
        plan_sha256: original.plan_sha256.clone(),
        generation: 7,
        expected_sequence: 2,
        now_ms: 12_001,
        census: flat.clone(),
    };
    // None of these negative paths may release capital, even if the outer
    // transaction is committed by a mistaken caller. They run before writes.
    {
        let tx = db.transaction().unwrap();
        let mut occupied = input.clone();
        occupied.census.occupied.insert(account('a'), amt("1"));
        assert!(matches!(
            RiskBookStore::resolve_unified_terminal_from_account_sql_transaction(
                &tx,
                "test-installation",
                &campaign,
                &occupied,
            ),
            Err(RiskStoreError::CensusUnproven)
        ));
        assert!(
            RiskBookStore::resolve_unified_terminal_from_account_sql_transaction(
                &tx,
                "test-installation",
                "another-campaign",
                &input,
            )
            .is_err()
        );
        tx.commit().unwrap();
    }
    assert_eq!(read(&db), ("6".into(), "1".into(), 2));
    // A missing original terminal settlement ACK is not proof of a debit.
    {
        let tx = db.transaction().unwrap();
        tx.execute(
            "DELETE FROM operation_receipts
             WHERE installation_id='test-installation' AND account_key=?1
               AND operation_id=?2",
            params![
                account('a'),
                format!("entry-terminal-settled:{}:r1", original.intent_id)
            ],
        )
        .unwrap();
        assert!(matches!(
            RiskBookStore::resolve_unified_terminal_from_account_sql_transaction(
                &tx,
                "test-installation",
                &campaign,
                &input,
            ),
            Err(RiskStoreError::Invalid(
                "TERMINAL_ENTRY_DEBIT_NOT_ACKNOWLEDGED"
            ))
        ));
        tx.rollback().unwrap();
    }
    {
        let tx = db.transaction().unwrap();
        let done = RiskBookStore::resolve_unified_terminal_from_account_sql_transaction(
            &tx,
            "test-installation",
            &campaign,
            &input,
        )
        .unwrap();
        assert!(!done.replayed);
        assert_eq!(done.book_sequence, 3);
        assert_eq!(
            tx.query_row::<(String, String, String, String, i64), _, _>(
                "SELECT global_reserved_usdt,loss_reserved_usd,
                        entry_reserved_usd,entry_consumed_usd,book_sequence
                 FROM risk_meta WHERE installation_id='test-installation' AND campaign_id=?1",
                [&campaign],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            )
            .unwrap(),
            ("0".into(), "0".into(), "0".into(), "0.4".into(), 3)
        );
        assert!(
            RiskBookStore::resolve_unified_terminal_from_account_sql_transaction(
                &tx,
                "test-installation",
                &campaign,
                &input,
            )
            .unwrap()
            .replayed
        );
        tx.commit().unwrap();
    }
    drop(db);
    let mut db = Connection::open(&f.path).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    let tx = db.transaction().unwrap();
    assert!(
        RiskBookStore::resolve_unified_terminal_from_account_sql_transaction(
            &tx,
            "test-installation",
            &campaign,
            &input,
        )
        .unwrap()
        .replayed
    );
    assert_eq!(
        tx.query_row::<String, _, _>(
            "SELECT state FROM risk_receipts
             WHERE installation_id='test-installation' AND account_key=?1 AND intent_id=?2",
            params![account('a'), &original.intent_id],
            |r| r.get(0)
        )
        .unwrap(),
        "RELEASED",
        "original AccountStore and campaign RiskBook must survive cold reopen together"
    );
    assert_eq!(
        tx.query_row::<String, _, _>(
            "SELECT state FROM risk_entries
             WHERE installation_id='test-installation' AND campaign_id=?1
               AND reservation_key=?2",
            params![&campaign, original.reservation_key()],
            |r| r.get(0)
        )
        .unwrap(),
        "RELEASED"
    );
    tx.rollback().unwrap();
}

#[test]
fn risk_book_contract_unified_original_opening_requires_exact_two_account_sql_sources() {
    use super::store::{NativeOpeningAccount, NativeOpeningSource};
    use crate::account_worker::{
        UnifiedFinanceWrite, UnifiedFinanceWriteReceipt, apply_original_unified_finance_transaction,
    };
    use rusqlite::{Connection, params};
    use sha2::{Digest, Sha256};

    let f = Fixture::new("original-entry-opening");
    let mut db = Connection::open(&f.path).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    db.execute_batch(include_str!("../../../schema/finance-unified-v1.sql"))
        .unwrap();
    let campaign = opening().campaign_id;
    let source_gen = [
        "fills/SPOT:1",
        "fills/MARGIN:1",
        "fills/SWAP:1",
        "fills/FUTURES:1",
        "fills/OPTION:1",
        "bills/account:1",
    ]
    .join("|");
    db.execute(
        "INSERT INTO risk_meta(
             singleton,campaign_id,installation_id,policy_revision,generation,
             book_sequence,legacy_fenced,global_limit_usdt,global_reserved_usdt,
             loss_limit_usd,loss_consumed_usd,loss_reserved_usd,entry_limit_usd,
             entry_consumed_usd,entry_opening_consumed_usd,entry_reserved_usd,
             entry_bound_breach_usd)
         VALUES(1,?1,'test-installation',1,7,0,1,'10','0',
                '100','0','0','10','0','0','0','0')",
        [&campaign],
    )
    .unwrap();
    for letter in ['a', 'b'] {
        let key = account(letter);
        db.execute(
            "INSERT INTO account_meta(
               installation_id,account_key,singleton,schema_version,initialized_at,
               opening_source,initialize_operation_id,release_id)
             VALUES('test-installation',?1,1,2,1,'test-only',?2,'test-release')",
            params![&key, format!("init-{letter}")],
        )
        .unwrap();
        db.execute(
            "INSERT INTO account_policy(
                installation_id,account_key,singleton,revision,paused,campaign_id,
                campaign_limit,campaign_loss)
             VALUES('test-installation',?1,1,1,0,?2,'10','0')",
            params![&key, &campaign],
        )
        .unwrap();
        db.execute(
            "INSERT INTO risk_account_occupied VALUES('test-installation',?1,?2,'0')",
            params![&campaign, &key],
        )
        .unwrap();
        db.execute(
            "INSERT INTO risk_policy_account VALUES('test-installation',?1,?2,'10','8','10')",
            params![&campaign, &key],
        )
        .unwrap();
        db.execute(
            "INSERT INTO risk_loss_source VALUES('test-installation',?1,?2,'0','')",
            params![&campaign, &key],
        )
        .unwrap();
        db.execute(
            "INSERT INTO risk_policy_strategy
             VALUES('test-installation',?1,?2,'strategy_test','8')",
            params![&campaign, &key],
        )
        .unwrap();
        db.execute(
            "INSERT INTO risk_strategy_occupied
             VALUES('test-installation',?1,?2,'strategy_test','0')",
            params![&campaign, &key],
        )
        .unwrap();
        db.execute(
            "INSERT INTO campaign_source_epoch
             VALUES('test-installation',?1,1,1)",
            [&key],
        )
        .unwrap();
        for (source, partition) in [
            ("fills", "SPOT"),
            ("fills", "MARGIN"),
            ("fills", "SWAP"),
            ("fills", "FUTURES"),
            ("fills", "OPTION"),
            ("bills", "account"),
        ] {
            db.execute(
                "INSERT INTO source_cursors(
                 installation_id,account_key,source,stream_partition,
                 cursor,watermark,generation,coverage_gaps)
                 VALUES('test-installation',?1,?2,?3,'',200,1,'')",
                params![&key, source, partition],
            )
            .unwrap();
        }
        db.execute(
            "INSERT INTO campaign_entry_readiness(
                installation_id,account_key,campaign_id,from_ms,covered_through_ms,
                source_revision,source_generations,reviewed_late_epoch)
             VALUES('test-installation',?1,?2,100,200,1,?3,0)",
            params![&key, &campaign, &source_gen],
        )
        .unwrap();
        db.execute(
            "INSERT INTO campaign_receipt_cursors
             VALUES('test-installation',?1,?2,0)",
            params![&key, &campaign],
        )
        .unwrap();
        db.execute(
            "INSERT INTO campaign_entry_totals
             VALUES('test-installation',?1,?2,100,200,'0')",
            params![&key, &campaign],
        )
        .unwrap();
        db.execute(
            "INSERT INTO campaign_entry_window_proofs
             VALUES('test-installation',?1,?2,200,'0',?3)",
            params![&key, &campaign, uid('f')],
        )
        .unwrap();
    }
    let mut source = NativeOpeningSource {
        campaign_id: campaign.clone(),
        limit_usd: "10".into(),
        consumed_usd: "0".into(),
        source_digest: String::new(),
        as_of_ms: 200,
        campaign_started_at_ms: 100,
        accounts: ['a', 'b']
            .into_iter()
            .map(|letter| NativeOpeningAccount {
                account_key: account(letter),
                history_from_ms: 100,
                history_through_ms: 200,
                consumed_entry_usd: "0".into(),
                order_history_complete: true,
                fill_history_complete: true,
                bill_history_complete: true,
                finance_provenance_sealed: true,
                source_digest: uid('f'),
            })
            .collect(),
    };
    let source_hash = |value: &NativeOpeningSource| -> String {
        format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    "okx.campaign-entry-opening/v1",
                    &value.campaign_id,
                    &value.limit_usd,
                    &value.consumed_usd,
                    value.campaign_started_at_ms,
                    value.as_of_ms,
                    &value.accounts,
                ))
                .unwrap()
            )
        )
    };
    source.source_digest = source_hash(&source);
    assert!(source.valid());
    let mut complete = census();
    complete.generated_at_ms = 201;
    complete.read_floor_ms = 201;
    let command = || UnifiedFinanceWrite::InstallEntryOpening {
        source: source.clone(),
        census: complete.clone(),
        now_ms: 202,
    };
    // Missing even one immutable AccountStore seal must never mint zero
    // opening, even if caller commits the outer transaction after denial.
    {
        let tx = db.transaction().unwrap();
        assert!(matches!(
            apply_original_unified_finance_transaction(&tx, "test-installation", command()),
            Err(crate::StoreError::Invalid(
                "UNIFIED_OPENING_ORIGINAL_SOURCE_UNVERIFIED"
            ))
        ));
        tx.commit().unwrap();
    }
    let seq: i64 = db
        .query_row(
            "SELECT book_sequence FROM risk_meta WHERE installation_id='test-installation'
         AND campaign_id=?1",
            [&campaign],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(seq, 0);
    for letter in ['a', 'b'] {
        let key = account(letter);
        db.execute(
            "INSERT INTO operation_receipts(
                installation_id,account_key,operation_id,kind,
                request_hash,result_ref,committed_at)
             VALUES('test-installation',?1,?2,'CAMPAIGN_ENTRY_OPENING_SEALED',?3,?3,200)",
            params![
                key,
                format!("campaign_entry_opening:{campaign}:{}", uid('f')),
                uid('f')
            ],
        )
        .unwrap();
    }
    {
        let tx = db.transaction().unwrap();
        tx.execute(
            "UPDATE campaign_source_epoch SET revision=2
             WHERE installation_id='test-installation' AND account_key=?1",
            [account('b')],
        )
        .unwrap();
        assert!(matches!(
            apply_original_unified_finance_transaction(&tx, "test-installation", command()),
            Err(crate::StoreError::Invalid(
                "UNIFIED_OPENING_ORIGINAL_SOURCE_UNVERIFIED"
            ))
        ));
        tx.rollback().unwrap();
    }
    // Native SQL rollback: neither an original source nor risk opening is
    // installed before an actual COMMIT by the sole installation writer.
    {
        use crate::account_worker::{ReportedOpeningCensus, install_reported_opening_sql};
        let reported = ReportedOpeningCensus {
            source_digest: complete.source_digest.clone(),
            generated_at_ms: 201,
            read_floor_ms: 201,
            occupied: complete
                .occupied
                .iter()
                .map(|(k, v)| (k.clone(), v.canonical().unwrap()))
                .collect(),
            cumulative_loss_usd: complete
                .cumulative_loss_usd
                .iter()
                .map(|(k, v)| (k.clone(), v.canonical().unwrap()))
                .collect(),
        };
        let tx = db.transaction().unwrap();
        let mut bad = reported.clone();
        bad.generated_at_ms = 9000;
        assert!(
            install_reported_opening_sql(&tx, "test-installation", &source, &bad, 202).is_err()
        );
        bad = reported.clone();
        bad.occupied.remove(&account('a'));
        assert!(
            install_reported_opening_sql(&tx, "test-installation", &source, &bad, 202).is_err()
        );
        bad = reported.clone();
        bad.occupied.insert(account('a'), "-1".into());
        assert!(
            install_reported_opening_sql(&tx, "test-installation", &source, &bad, 202).is_err()
        );
        assert!(
            install_reported_opening_sql(&tx, "foreign-installation", &source, &reported, 202)
                .is_err()
        );
        assert!(
            install_reported_opening_sql(&tx, "test-installation", &source, &reported, 6000)
                .is_err()
        );
        let result =
            install_reported_opening_sql(&tx, "test-installation", &source, &reported, 202)
                .unwrap();
        assert!(matches!(
            result,
            UnifiedFinanceWriteReceipt::Risk(super::UnifiedRiskDecision::EntryOpeningInstalled {
                replayed: false,
                ..
            })
        ));
        tx.rollback().unwrap();
    }
    {
        let tx = db.transaction().unwrap();
        let receipt =
            apply_original_unified_finance_transaction(&tx, "test-installation", command())
                .unwrap();
        assert!(matches!(
            receipt,
            UnifiedFinanceWriteReceipt::Risk(super::UnifiedRiskDecision::EntryOpeningInstalled {
                replayed: false,
                ..
            })
        ));
        tx.rollback().unwrap();
    }
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT book_sequence FROM risk_meta WHERE installation_id='test-installation'
         AND campaign_id=?1",
            [&campaign],
            |r| r.get(0),
        )
        .unwrap(),
        0
    );
    {
        let tx = db.transaction().unwrap();
        assert!(
            super::read_unified_campaign_opening(&tx, "test-installation", &campaign)
                .unwrap()
                .is_none()
        );
        tx.execute_batch("CREATE TEMP TRIGGER reject_opening_source BEFORE INSERT ON operation_receipts WHEN NEW.kind='CAMPAIGN_OPENING_SOURCE' BEGIN SELECT RAISE(ABORT,'fixture provenance write failure'); END").unwrap();
        assert!(
            apply_original_unified_finance_transaction(&tx, "test-installation", command())
                .is_err()
        );
        let seq: i64 = tx
            .query_row(
                "SELECT book_sequence FROM risk_meta WHERE campaign_id=?1",
                [&campaign],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(seq, 0);
        let markers:i64=tx.query_row("SELECT COUNT(*) FROM operation_receipts WHERE kind IN ('CAMPAIGN_OPENING_SOURCE','CAMPAIGN_OPENING_INSTALLED')",[],|r|r.get(0)).unwrap();
        assert_eq!(markers, 0);
        tx.execute_batch("DROP TRIGGER reject_opening_source")
            .unwrap();
        tx.commit().unwrap();
    }
    {
        let tx = db.transaction().unwrap();
        apply_original_unified_finance_transaction(&tx, "test-installation", command()).unwrap();
        tx.commit().unwrap();
    }
    drop(db);
    let mut db = Connection::open(&f.path).unwrap();
    db.pragma_update(None, "foreign_keys", "ON").unwrap();
    {
        let tx = db.transaction().unwrap();
        assert_eq!(
            super::read_unified_campaign_opening(&tx, "test-installation", &campaign).unwrap(),
            Some(source.clone())
        );
        assert!(
            super::read_unified_campaign_opening(&tx, "foreign-installation", &campaign).is_err()
        );
        let replay =
            apply_original_unified_finance_transaction(&tx, "test-installation", command())
                .unwrap();
        assert!(matches!(
            replay,
            UnifiedFinanceWriteReceipt::Risk(super::UnifiedRiskDecision::EntryOpeningInstalled {
                book_sequence: 1,
                replayed: true,
                ..
            })
        ));
        tx.commit().unwrap();
    }
    assert_eq!(
        db.query_row::<(String, String, i64), _, _>(
            "SELECT entry_opening_consumed_usd,entry_consumed_usd,book_sequence
         FROM risk_meta WHERE installation_id='test-installation' AND campaign_id=?1",
            [&campaign],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap(),
        ("0".into(), "0".into(), 1)
    );
    let mut forged = source.clone();
    forged.accounts[1].source_digest = uid('e');
    forged.source_digest = source_hash(&forged);
    let tx = db.transaction().unwrap();
    assert!(
        apply_original_unified_finance_transaction(
            &tx,
            "test-installation",
            UnifiedFinanceWrite::InstallEntryOpening {
                source: forged,
                census: complete,
                now_ms: 202,
            }
        )
        .is_err()
    );
    tx.rollback().unwrap();
}
