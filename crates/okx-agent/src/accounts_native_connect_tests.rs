//! Real Native registry/SQL path; only signed external HTTP bytes are recorded.
use super::*;
use crate::accounts::recorded_support::{RecordedAccountHttp, RecordedGet, prepared};
use okx_execution::account_worker::UnifiedFinanceHandle;
use rusqlite::{Connection, OpenFlags, params};

const INSTALLATION: &str = "installation-test";
fn campaign() -> String {
    format!("sha256:{}", "c".repeat(64))
}
fn deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
fn root() -> PathBuf {
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!(
        "okx-native-connect-{:032x}",
        u128::from_le_bytes(nonce)
    ))
}

async fn fresh() -> PathBuf {
    let root = root();
    initialize_installation(&root, INSTALLATION).unwrap();
    let owner = UnifiedFinanceHandle::initialize_new(
        root.clone(),
        okx_execution::NewFinanceInstallation {
            installation_id: INSTALLATION.into(),
            initialize_request_id: "initialize_native_connect_001".into(),
            source_tree: "a".repeat(40),
        },
    )
    .await
    .unwrap();
    close_owner(owner).await;
    root
}
async fn close_owner(owner: UnifiedFinanceHandle) {
    let mut closed = owner.termination_signal();
    drop(owner);
    if !*closed.borrow() {
        tokio::time::timeout(std::time::Duration::from_secs(3), closed.changed())
            .await
            .unwrap()
            .unwrap();
    }
}
async fn stop(registry: AccountRuntimeRegistry, owner: UnifiedFinanceHandle) {
    registry.shutdown().await;
    drop(registry);
    close_owner(owner).await;
}
fn read(root: &Path) -> Connection {
    Connection::open_with_flags(
        root.join("finance/finance.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap()
}
fn account_count(root: &Path) -> i64 {
    read(root)
        .query_row("SELECT COUNT(*) FROM account_meta", [], |r| r.get(0))
        .unwrap()
}
fn assert_original_opening(root: &Path, account: &str) {
    let db = read(root);
    let original: (String, String, String) = db
        .query_row(
            "SELECT m.installation_id,f.amount,n.equity FROM account_meta m
         JOIN cash_flows f ON f.installation_id=m.installation_id AND f.account_key=m.account_key
           AND f.event_id='opening:USDT' AND f.kind='OPENING'
         JOIN nav_points n ON n.installation_id=m.installation_id AND n.account_key=m.account_key
           AND n.scope_id='account' AND n.source_version='opening'
         WHERE m.account_key=?1",
            [account],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(original.0, INSTALLATION);
    assert_eq!(Decimal::from_str(&original.1).unwrap(), Decimal::from(100));
    assert_eq!(Decimal::from_str(&original.2).unwrap(), Decimal::from(100));
    let receipt:i64=db.query_row(
        "SELECT COUNT(*) FROM operation_receipts WHERE installation_id=?1 AND account_key=?2 AND kind='INITIALIZE'",
        params![INSTALLATION,account],|r|r.get(0)).unwrap();
    assert_eq!(receipt, 1, "one original account initialization receipt");
}

#[tokio::test]
async fn risk_book_contract_native_connect_two_accounts_and_reopen_one_original_sqlite() {
    let root = fresh().await;
    let budget = RateBudget::new();
    let feed = Arc::new(RecordedAccountHttp::default());
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    let mut configs = Vec::new();
    for label in ["a", "b"] {
        let (config, clients) = prepared(label, feed.clone(), budget.clone());
        registry
            .connect_prepared(config.clone(), clients)
            .await
            .unwrap();
        let resources = registry.resolve(&config.alias).await.unwrap();
        let (native, scope) = resources
            .execution
            .store
            .native_scope()
            .expect("production Native store");
        assert_eq!(native.installation_id(), INSTALLATION);
        assert_eq!(scope, config.account_key.wire());
        assert_eq!(resources.execution.uid(), config.account_key.verified_uid);
        drop(resources);
        assert_original_opening(&root, &config.account_key.wire());
        configs.push(config);
    }
    assert_eq!(account_count(&root), 2);
    assert_eq!(
        account_catalog_proto::load(&root).unwrap().accounts,
        configs
    );
    assert!(
        !root.join("accounts").exists(),
        "no account-local finance file or directory"
    );
    assert!(!root.join("risk").exists(), "no standalone campaign writer");
    let (_, duplicate_clients) = prepared("a", feed.clone(), budget.clone());
    assert!(
        registry
            .connect_prepared(configs[0].clone(), duplicate_clients)
            .await
            .is_err()
    );
    assert_eq!(
        account_count(&root),
        2,
        "duplicate connection cannot rewrite original opening money"
    );
    assert_original_opening(&root, &configs[0].account_key.wire());
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    assert!(matches!(
        owner.verify_live_authority(deadline()).await,
        Err(okx_execution::StoreError::Invalid(
            "FINANCE_CAMPAIGN_POLICY_REQUIRED"
        ))
    ));
    stop(registry, owner).await;
    // A restarted Engine owns a NEW process-local limiter. Reusing the old
    // limiter would test an extra connection burst, not cold reconstruction.
    let budget = RateBudget::new();
    let (reopened, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    for (label, config) in ["a", "b"].into_iter().zip(&configs) {
        let (_, clients) = prepared(label, feed.clone(), budget.clone());
        reopened
            .reconnect_prepared(config.clone(), clients)
            .await
            .unwrap();
        assert_original_opening(&root, &config.account_key.wire());
    }
    assert_eq!(
        account_count(&root),
        2,
        "restart cannot reinitialize or duplicate account facts"
    );
    configs.sort_by_key(|config| config.account_key.wire());
    assert_eq!(reopened.list().await, configs);
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    stop(reopened, owner).await;
}

#[tokio::test]
async fn risk_book_contract_native_connect_wrong_uid_does_not_create_account_or_publish_catalog() {
    let root = fresh().await;
    let budget = RateBudget::new();
    let feed = Arc::new(RecordedAccountHttp::default());
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    let (mut config, clients) = prepared("a", feed.clone(), budget);
    config.account_key.verified_uid = "f".repeat(64);
    let error = registry
        .connect_prepared(config, clients)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ACCOUNT_IDENTITY_MISMATCH"));
    assert_eq!(account_count(&root), 0);
    assert!(
        account_catalog_proto::load(&root)
            .unwrap()
            .accounts
            .is_empty()
    );
    assert!(registry.list().await.is_empty());
    assert!(!root.join("accounts").exists());
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    stop(registry, owner).await;
}

#[tokio::test]
async fn risk_book_contract_native_connect_recovers_committed_opening_before_catalog_publication() {
    let root = fresh().await;
    let feed = Arc::new(RecordedAccountHttp::default());
    let budget = RateBudget::new();
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    let (config, clients) = prepared("a", feed.clone(), budget);
    let uid = verify_client_pair(clients.environment, &clients.observer, &clients.executor)
        .await
        .unwrap();
    assert_eq!(uid, config.account_key.verified_uid);
    let observer = AccountBootstrapper::new(clients.observer);
    // Same production source/SQL operation, interrupted BEFORE catalog publish.
    crate::native_account_connection::ensure_account(&owner, &config, &observer, &uid, true)
        .await
        .unwrap();
    let original = owner
        .account_metadata(deadline(), config.account_key.wire())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(account_count(&root), 1);
    assert!(
        account_catalog_proto::load(&root)
            .unwrap()
            .accounts
            .is_empty()
    );
    stop(registry, owner).await;
    let budget = RateBudget::new();
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    let (_, clients) = prepared("a", feed.clone(), budget);
    registry
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap();
    assert_eq!(
        owner
            .account_metadata(deadline(), config.account_key.wire())
            .await
            .unwrap(),
        Some(original)
    );
    assert_original_opening(&root, &config.account_key.wire());
    assert_eq!(account_count(&root), 1);
    assert_eq!(
        account_catalog_proto::load(&root).unwrap().accounts,
        vec![config]
    );
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    stop(registry, owner).await;
}

#[tokio::test]
async fn risk_book_contract_native_connect_existing_liability_is_not_zeroed_into_flat_account() {
    let root = fresh().await;
    let budget = RateBudget::new();
    let feed = Arc::new(RecordedAccountHttp::default());
    // Exactly the two observed opening snapshots, no extra retries/pages or
    // inferred fake balance. The production parser receives original raw bytes.
    for _ in 0..2 {
        feed.expect_get(RecordedGet {
            access_key:"fixture-observer-a".into(),path:"/api/v5/account/balance".into(),required_query:vec![],
            raw_response:br#"{"code":"0","msg":"","data":[{"totalEq":"100","adjEq":"100","uTime":"1793460000000","details":[{"ccy":"USDT","eq":"100","liab":"1","availEq":"100","availBal":"100","eqUsd":"100","uTime":"1793460000000"}]}]}"#.to_vec(),
        });
    }
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    let (config, clients) = prepared("a", feed.clone(), budget);
    let error = registry
        .connect_prepared(config, clients)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ACCOUNT_NOT_FLAT"), "{error}");
    assert_eq!(account_count(&root), 0);
    assert!(
        account_catalog_proto::load(&root)
            .unwrap()
            .accounts
            .is_empty()
    );
    assert_eq!(
        feed.unconsumed_reads(),
        0,
        "both actual source observations were consumed"
    );
    assert!(!root.join("accounts").exists());
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    stop(registry, owner).await;
}

#[tokio::test]
async fn risk_book_contract_native_catalog_failure_retains_sql_and_retries_original_connection() {
    let root = fresh().await;
    let feed = Arc::new(RecordedAccountHttp::default());
    let budget = RateBudget::new();
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    let (config, clients) = prepared("a", feed.clone(), budget);
    // A real filesystem publication obstruction, not a mocked SQL failure.
    let canonical = root.join(account_catalog_proto::CATALOG_FILE);
    let saved = root.join("test-original-catalog.pb");
    std::fs::rename(&canonical, &saved).unwrap();
    std::fs::create_dir(&canonical).unwrap();
    let error = registry
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap_err();
    assert!(
        matches!(error, AgentError::AccountCatalogPublicationPending { .. }),
        "{error}"
    );
    assert_eq!(account_count(&root), 1);
    assert!(registry.list().await.is_empty());
    let original = owner
        .account_metadata(deadline(), config.account_key.wire())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        owner.verify_live_authority(deadline()).await,
        Err(okx_execution::StoreError::Invalid(
            "FINANCE_CAMPAIGN_POLICY_REQUIRED"
        ))
    ));
    // The retained original catalog is restored only as test teardown of the
    // deliberate obstruction. The application's recovery is an exact RPC retry.
    std::fs::remove_dir(&canonical).unwrap();
    std::fs::rename(&saved, &canonical).unwrap();
    let (_, clients) = prepared("a", feed.clone(), RateBudget::new());
    registry
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap();
    assert_eq!(
        owner
            .account_metadata(deadline(), config.account_key.wire())
            .await
            .unwrap(),
        Some(original)
    );
    assert_eq!(account_count(&root), 1);
    assert_eq!(registry.list().await, vec![config]);
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    stop(registry, owner).await;
}

#[cfg(windows)]
#[tokio::test]
async fn risk_book_contract_native_catalog_windows_reader_lock_preserves_original_connection() {
    use std::os::windows::fs::OpenOptionsExt;
    let root = fresh().await;
    let feed = Arc::new(RecordedAccountHttp::default());
    let budget = RateBudget::new();
    let (registry, owner) =
        AccountRuntimeRegistry::open_native_catalog(&root, budget.clone(), &campaign())
            .await
            .unwrap();
    let (config, clients) = prepared("a", feed.clone(), budget);
    let path = root.join(account_catalog_proto::CATALOG_FILE);
    let original_catalog = std::fs::read(&path).unwrap();
    // A real Windows reader sharing READ/WRITE but not DELETE deterministically
    // refuses atomic replacement. This is a physical boundary, not a guessed
    // explanation of every intermittent host publication error.
    let reader = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0x1 | 0x2)
        .open(&path)
        .unwrap();
    let failure = registry
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap_err();
    assert!(
        matches!(
            &failure,
            AgentError::AccountCatalogPublicationPending { .. }
        ),
        "{failure}"
    );
    assert!(
        failure
            .to_string()
            .contains("ACCOUNT_CATALOG_REPLACE_GUARD"),
        "{failure}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), original_catalog);
    assert!(registry.list().await.is_empty());
    assert_eq!(account_count(&root), 1);
    let original = owner
        .account_metadata(deadline(), config.account_key.wire())
        .await
        .unwrap()
        .unwrap();
    assert_original_opening(&root, &config.account_key.wire());
    drop(reader); // The external obstruction actually ends before exact retry.
    let (_, clients) = prepared("a", feed.clone(), RateBudget::new());
    registry
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap();
    assert_eq!(
        owner
            .account_metadata(deadline(), config.account_key.wire())
            .await
            .unwrap(),
        Some(original)
    );
    assert_eq!(account_count(&root), 1);
    assert_original_opening(&root, &config.account_key.wire());
    assert_eq!(registry.list().await, vec![config.clone()]);
    assert_eq!(
        account_catalog_proto::load(&root).unwrap().accounts,
        vec![config]
    );
    assert!(feed.outbound_posts.lock().unwrap().is_empty());
    stop(registry, owner).await;
}
