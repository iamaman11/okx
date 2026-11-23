//! Explicit, stopped-Engine operations for the new SQL format only.
use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    accounts::{build_release, key_hash, verify_credentials},
    okx_credentials::load_native_account_credentials,
};
use okx_api::{OkxRestClient, RateBudget};
use okx_ipc_contracts::{
    account_catalog_proto,
    accounts::{AccountKey, Environment, VerifiedAccountConfig, valid_reference},
};
use okx_trade_execution::AccountStore;
use okx_trade_execution::account_worker::UnifiedFinanceHandle;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
fn valid_operation(id: &str) -> bool {
    (8..=128).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}
fn error(code: &str) -> AgentError {
    AgentError::FinancialCaptureTask(code.into())
}
fn release(expected: &str) -> AgentResult<()> {
    if expected == "UNVERIFIED_LOCAL_BUILD" || build_release() != expected || expected.is_empty() {
        return Err(error("RELEASE_MISMATCH"));
    }
    Ok(())
}
fn engine_lease(root: &Path) -> AgentResult<File> {
    fs::create_dir_all(root)?;
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("engine.lock"))?;
    f.try_lock().map_err(|_| error("ENGINE_MUST_BE_STOPPED"))?;
    Ok(f)
}
fn write_new(path: &Path, bytes: &[u8]) -> AgentResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| error("STORAGE_PARENT_MISSING"))?;
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|_| error("STORAGE_NONCE_UNAVAILABLE"))?;
    let temp = parent.join(format!(".storage-publish-{:x}", u128::from_le_bytes(nonce)));
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    fs::hard_link(&temp, path)?;
    drop(f);
    fs::remove_file(&temp)?;
    #[cfg(not(windows))]
    File::open(parent)?.sync_all()?;
    Ok(())
}
// A clean epoch must never be initialized alongside unknown financial state.
// Only files belonging to the verified binary bootstrap and the engine lease
// may precede the first account catalog. A retry may also contain its own
// durable operation receipt. Existing SQL, WAL, backups and legacy metadata
// are *not* evidence of a clean installation.
fn require_clean_installation_root(root: &Path, retry: bool) -> AgentResult<()> {
    for item in fs::read_dir(root)? {
        let item = item?;
        let kind = item.file_type()?;
        let allowed = match item.file_name().to_str() {
            Some("engine.lock" | "okx-agent.exe" | "installed-agent.json") => kind.is_file(),
            Some("installation-operation.json") if retry => kind.is_file(),
            _ => false,
        };
        if !allowed {
            return Err(error("INSTALLATION_ROOT_NOT_CLEAN"));
        }
    }
    Ok(())
}

fn installation(root: &Path) -> AgentResult<String> {
    Ok(account_catalog_proto::load(root)?.installation_id)
}
fn catalog(root: &Path, key: &AccountKey) -> AgentResult<VerifiedAccountConfig> {
    if !key.valid() {
        return Err(error("ACCOUNT_KEY_INVALID"));
    }
    account_catalog_proto::load(root)?
        .accounts
        .into_iter()
        .find(|a| a.account_key == *key)
        .ok_or_else(|| error("ACCOUNT_CATALOG_INVALID"))
}
/// The original stopped AccountStore maintenance path uses the very same
/// cross-process installation shared mode lock as regular legacy SQL workers.
/// The guard must live until the command completes; after finance.sqlite
/// exists, no account-local backup/restore or reinitialization is valid.
fn legacy_account_maintenance_lease(
    root: &Path,
    key: &AccountKey,
) -> AgentResult<okx_trade_execution::LegacyAccountMaintenanceLease> {
    let account_sql = account_root(root, key).join("account.sqlite");
    okx_trade_execution::hold_legacy_account_maintenance(root, &account_sql)
        .map_err(AgentError::from)
}

fn parse_key(wire: &str) -> AgentResult<AccountKey> {
    let p = wire.split(':').collect::<Vec<_>>();
    if p.len() != 3 {
        return Err(error("ACCOUNT_KEY_INVALID"));
    }
    let environment = match p[1] {
        "real" => Environment::Real,
        "demo" => Environment::Demo,
        _ => return Err(error("ACCOUNT_KEY_INVALID")),
    };
    let key = AccountKey {
        venue: p[0].into(),
        environment,
        verified_uid: p[2].into(),
    };
    if !key.valid() {
        return Err(error("ACCOUNT_KEY_INVALID"));
    }
    Ok(key)
}
fn account_root(root: &Path, key: &AccountKey) -> PathBuf {
    root.join("accounts").join(key_hash(key))
}
fn reply(installation: &str, operation: &str, key: Option<&AccountKey>, extra: Value) -> Value {
    let mut v = json!({"status":"completed","installation_id":installation,"operation_id":operation,"release":build_release()});
    if let Some(key) = key {
        v["account_key"] = Value::String(key.wire());
    }
    if let Some(fields) = extra.as_object() {
        for (k, value) in fields {
            v[k] = value.clone();
        }
    }
    v
}
/// Explicit initialization shares the SAME writer used by Native runtime.
/// Exact retry reads the original receipt; it never imports/reset old state.
async fn initialize_finance(
    root: &Path,
    installation_id: &str,
    request_id: &str,
    source_tree: &str,
) -> AgentResult<Value> {
    if root.join(account_catalog_proto::CATALOG_FILE).exists() {
        let existing = account_catalog_proto::load(root)?;
        if existing.installation_id != installation_id {
            return Err(error("FINANCE_INSTALLATION_IDENTITY_MISMATCH"));
        }
        if !existing.accounts.is_empty() && !root.join("finance/finance.sqlite").exists() {
            return Err(error("FINANCE_DATABASE_MISSING_WITH_ACCOUNTS"));
        }
    }
    let owner = okx_trade_execution::account_worker::UnifiedFinanceHandle::initialize_new(
        root.canonicalize()?,
        okx_trade_execution::NewFinanceInstallation {
            installation_id: installation_id.to_owned(),
            initialize_request_id: request_id.to_owned(),
            source_tree: source_tree.to_owned(),
        },
    )
    .await?;
    let mut stopped = owner.termination_signal();
    let result = owner
        .installation_receipt(Instant::now() + Duration::from_secs(2))
        .await;
    drop(owner);
    if !*stopped.borrow() {
        stopped
            .changed()
            .await
            .map_err(|_| error("FINANCE_WORKER_SHUTDOWN_UNCONFIRMED"))?;
    }
    Ok(serde_json::to_value(result?)?)
}
pub async fn initialize_installation(
    root: &Path,
    installation_id: &str,
    expected_release: &str,
    operation_id: &str,
) -> AgentResult<Value> {
    release(expected_release)?;
    if (!valid_reference(installation_id) || installation_id.len() < 8)
        || !valid_operation(operation_id)
    {
        return Err(error("INITIALIZE_ID_INVALID"));
    }
    let _lease = engine_lease(root)?;
    let op_path = root.join("installation-operation.json");
    let expected = json!({"installation_id":installation_id,"operation_id":operation_id,"release":expected_release});
    if op_path.exists() {
        let old: Value = serde_json::from_slice(&fs::read(&op_path)?)?;
        if old != expected {
            return Err(error("INITIALIZE_OPERATION_CONFLICT"));
        }
        if root.join(account_catalog_proto::CATALOG_FILE).exists() {
            if installation(root)? != installation_id {
                return Err(error("INSTALLATION_MISMATCH"));
            }
        } else {
            require_clean_installation_root(root, true)?;
            crate::accounts::initialize_installation(root, installation_id)?;
        }
        let finance =
            initialize_finance(root, installation_id, operation_id, expected_release).await?;
        return Ok(reply(
            installation_id,
            operation_id,
            None,
            json!({"finance":finance}),
        ));
    }
    if root.join(account_catalog_proto::CATALOG_FILE).exists() {
        return Err(error("INSTALLATION_ALREADY_EXISTS"));
    }
    // Refuse before publishing any operation state: a missing catalog does
    // not make an existing runtime or financial database a clean root.
    require_clean_installation_root(root, false)?;
    write_new(&op_path, &serde_json::to_vec(&expected)?)?;
    crate::accounts::initialize_installation(root, installation_id)?;
    let finance = initialize_finance(root, installation_id, operation_id, expected_release).await?;
    Ok(reply(
        installation_id,
        operation_id,
        None,
        json!({"finance":finance}),
    ))
}
/// Explicit stopped-Engine account initialization on the ONE Native financial
/// owner. Exact retry is read from original SQL receipts without private HTTP,
/// new opening facts, or an account.sqlite fallback.
pub async fn initialize_account(
    root: &Path,
    key: &str,
    installation_id: &str,
    expected_release: &str,
    operation_id: &str,
) -> AgentResult<Value> {
    let parsed_key = parse_key(key)?;
    let key = &parsed_key;
    release(expected_release)?;
    if !valid_operation(operation_id) {
        return Err(error("INITIALIZE_ID_INVALID"));
    }
    let _engine = engine_lease(root)?;
    if installation(root)? != installation_id {
        return Err(error("INSTALLATION_MISMATCH"));
    }
    // Preserve the original global account identity exclusion across roots,
    // not only the stopped Engine's per-installation lease.
    let _identity_lease = crate::accounts::acquire_identity_lease(
        &crate::config::default_root().join("account-leases"),
        key,
    )?;
    let config = catalog(root, key)?;
    if config.connect_operation_id != operation_id {
        return Err(error("INITIALIZE_OPERATION_CONFLICT"));
    }
    // Fail closed if the finance file is absent, foreign, staged or from the
    // retired per-account layout. Never create an account-local SQL writer.
    let owner =
        UnifiedFinanceHandle::open_native(root.canonicalize()?, installation_id.to_owned()).await?;
    let mut stopped = owner.termination_signal();
    let result = initialize_account_on_owner(&owner, &config, expected_release).await;
    drop(owner);
    // Keep the installation lock until the sole SQL writer has closed.
    if !*stopped.borrow() {
        stopped
            .changed()
            .await
            .map_err(|_| error("FINANCE_WORKER_SHUTDOWN_UNCONFIRMED"))?;
    }
    result?;
    Ok(reply(installation_id, operation_id, Some(key), json!({})))
}

/// Same operation for public CLI and direct original-owner regression tests.
/// Existing receipt means exact retry, never a fresh exchange snapshot.
async fn initialize_account_on_owner(
    owner: &UnifiedFinanceHandle,
    config: &VerifiedAccountConfig,
    expected_release: &str,
) -> AgentResult<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    if let Some(meta) =
        crate::native_account_connection::original_connection(owner, config, deadline).await?
    {
        if meta.release_id != expected_release {
            return Err(error("INITIALIZE_OPERATION_CONFLICT"));
        }
        return Ok(());
    }
    let credentials = load_native_account_credentials(&config.credential_ref)?;
    let budget = RateBudget::new();
    let uid = verify_credentials(&credentials, budget.clone()).await?;
    if uid != config.account_key.verified_uid
        || credentials.environment.demo != (config.account_key.environment == Environment::Demo)
    {
        return Err(error("ACCOUNT_IDENTITY_MISMATCH"));
    }
    let observer = AccountBootstrapper::new(OkxRestClient::with_rate_budget(
        credentials.environment,
        credentials.observer,
        budget,
    )?);
    crate::native_account_connection::ensure_account(owner, config, &observer, &uid, true).await?;
    // Even if the original commit reply was lost, require the durable bound
    // identity and original CONNECT receipt before reporting completion.
    let confirmed = crate::native_account_connection::original_connection(
        owner,
        config,
        Instant::now() + Duration::from_secs(2),
    )
    .await?;
    if !matches!(confirmed, Some(ref meta) if meta.release_id == expected_release) {
        return Err(error("ACCOUNT_CONNECTION_RECEIPT_NOT_CONFIRMED"));
    }
    Ok(())
}

fn hash(path: &Path) -> AgentResult<String> {
    let mut f = File::open(path)?;
    let mut hash = Sha256::new();
    let mut bytes = [0u8; 65536];
    loop {
        let n = f.read(&mut bytes)?;
        if n == 0 {
            break;
        }
        hash.update(&bytes[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PathBuf {
        let mut n = [0u8; 16];
        getrandom::fill(&mut n).unwrap();
        let path = std::env::temp_dir().join(format!("okx-storage-{:x}", u128::from_le_bytes(n)));
        fs::create_dir_all(&path).unwrap();
        path
    }
    #[test]
    fn native_storage_requires_exclusive_stopped_engine() {
        let root = root();
        let first = engine_lease(&root).unwrap();
        assert!(engine_lease(&root).is_err());
        drop(first);
        assert!(engine_lease(&root).is_ok());
    }
    #[test]
    fn publication_never_overwrites_committed_operation() {
        let root = root();
        let target = root.join("operation.json");
        write_new(&target, b"committed").unwrap();
        assert!(write_new(&target, b"replacement").is_err());
        assert_eq!(fs::read(&target).unwrap(), b"committed");
        assert_eq!(
            hash(&target).unwrap(),
            format!("{:x}", Sha256::digest(b"committed"))
        );
    }
    #[test]
    fn clean_installation_guard_rejects_financial_residue_without_writing() {
        let root = root();
        let sql = root
            .join("accounts")
            .join("previous")
            .join("account.sqlite");
        fs::create_dir_all(sql.parent().unwrap()).unwrap();
        fs::write(&sql, b"original-sql-content").unwrap();
        let err = require_clean_installation_root(&root, false).unwrap_err();
        assert!(err.to_string().contains("INSTALLATION_ROOT_NOT_CLEAN"));
        assert_eq!(fs::read(&sql).unwrap(), b"original-sql-content");
        assert!(!root.join("installation-operation.json").exists());
        assert!(!root.join(account_catalog_proto::CATALOG_FILE).exists());
    }
    #[test]
    fn clean_installation_guard_distinguishes_new_epoch_and_retry() {
        let root = root();
        fs::write(root.join("engine.lock"), b"").unwrap();
        fs::write(root.join("okx-agent.exe"), b"verified-agent-by-owner").unwrap();
        fs::write(
            root.join("installed-agent.json"),
            b"verified-release-provenance",
        )
        .unwrap();
        assert!(require_clean_installation_root(&root, false).is_ok());

        fs::write(
            root.join("installation-operation.json"),
            b"durable-operation",
        )
        .unwrap();
        assert!(require_clean_installation_root(&root, true).is_ok());
        assert!(require_clean_installation_root(&root, false).is_err());

        // A legacy identity marker is an explicit conflict even on retry.
        fs::write(root.join("installation.json"), b"old-installation").unwrap();
        assert!(require_clean_installation_root(&root, true).is_err());
    }
    #[test]
    fn clean_installation_guard_rejects_unknown_files_and_links() {
        let root = root();
        fs::write(root.join("unexpected-backup"), b"do-not-touch").unwrap();
        assert!(require_clean_installation_root(&root, false).is_err());
        assert_eq!(
            fs::read(root.join("unexpected-backup")).unwrap(),
            b"do-not-touch"
        );
    }
    #[test]
    fn storage_keys_cannot_select_arbitrary_paths() {
        assert!(parse_key("../../other").is_err());
        assert!(parse_key(&format!("okx:real:{}", "a".repeat(64))).is_ok());
        assert!(parse_key(&format!("okx:demo:{}", "g".repeat(64))).is_err());
        assert!(release("UNVERIFIED_LOCAL_BUILD").is_err());
    }

    #[tokio::test]
    async fn risk_book_contract_native_initialize_account_replays_original_without_legacy_sql() {
        let root = root();
        let installation = "installation_test";
        crate::accounts::initialize_installation(&root, installation).unwrap();
        let config = VerifiedAccountConfig {
            account_key: AccountKey {
                venue: "okx".into(),
                environment: Environment::Real,
                verified_uid: "a".repeat(64),
            },
            alias: "replay-account".into(),
            credential_ref: "never-read-on-replay".into(),
            connect_operation_id: "connect_native_replay_001".into(),
            configuration_digest: "b".repeat(64),
        };
        account_catalog_proto::append(&root, &config).unwrap();
        let owner = UnifiedFinanceHandle::initialize_new(
            root.clone(),
            okx_trade_execution::NewFinanceInstallation {
                installation_id: installation.into(),
                initialize_request_id: "initialize_native_replay_001".into(),
                source_tree: "c".repeat(40),
            },
        )
        .await
        .unwrap();
        let release = "d".repeat(40);
        let opening = okx_trade_execution::OpeningSnapshot {
            opening_equity_usd: rust_decimal::Decimal::from(100),
            source_ref: "e".repeat(64),
            observed_at: 100,
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: rust_decimal::Decimal::ZERO,
            balances: vec![("USDT".into(), rust_decimal::Decimal::from(100))],
        };
        let meta = okx_trade_execution::AccountMeta {
            account_key: config.account_key.wire(),
            installation_id: installation.into(),
            initialized_at: opening.observed_at,
            opening_source: opening.source_ref.clone(),
            initialize_operation_id: config.connect_operation_id.clone(),
            release_id: release.clone(),
        };
        owner
            .initialize_account(
                Instant::now() + Duration::from_secs(2),
                meta.clone(),
                opening,
                config.configuration_digest.clone(),
            )
            .await
            .unwrap();
        let db = rusqlite::Connection::open_with_flags(
            root.join("finance/finance.sqlite"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let counts = || -> (i64, i64, i64) {
            (
                db.query_row("SELECT COUNT(*) FROM account_meta", [], |r| r.get(0))
                    .unwrap(),
                db.query_row("SELECT COUNT(*) FROM cash_flows", [], |r| r.get(0))
                    .unwrap(),
                db.query_row("SELECT COUNT(*) FROM operation_receipts", [], |r| r.get(0))
                    .unwrap(),
            )
        };
        assert_eq!(counts(), (1, 1, 3));
        initialize_account_on_owner(&owner, &config, &release)
            .await
            .unwrap();
        assert_eq!(counts(), (1, 1, 3), "replay cannot write a second opening");
        assert_eq!(
            owner
                .account_metadata(
                    Instant::now() + Duration::from_secs(2),
                    meta.account_key.clone()
                )
                .await
                .unwrap(),
            Some(meta)
        );
        assert!(
            initialize_account_on_owner(&owner, &config, "wrong-release")
                .await
                .unwrap_err()
                .to_string()
                .contains("INITIALIZE_OPERATION_CONFLICT")
        );
        let mut changed = config.clone();
        changed.configuration_digest = "f".repeat(64);
        assert!(
            initialize_account_on_owner(&owner, &changed, &release)
                .await
                .unwrap_err()
                .to_string()
                .contains("ACCOUNT_CONNECTION_IDEMPOTENCY_CONFLICT")
        );
        assert_eq!(counts(), (1, 1, 3));
        drop(db);
        let mut stopped = owner.termination_signal();
        drop(owner);
        if !*stopped.borrow() {
            stopped.changed().await.unwrap();
        }
        assert!(
            !root.join("accounts").exists(),
            "no legacy financial writer"
        );
    }

    #[tokio::test]
    async fn risk_book_contract_native_initialize_account_never_seeds_without_identity() {
        let root = root();
        let owner = UnifiedFinanceHandle::initialize_new(
            root.clone(),
            okx_trade_execution::NewFinanceInstallation {
                installation_id: "installation_test".into(),
                initialize_request_id: "initialize_native_denied_001".into(),
                source_tree: "a".repeat(40),
            },
        )
        .await
        .unwrap();
        let config = VerifiedAccountConfig {
            account_key: AccountKey {
                venue: "okx".into(),
                environment: Environment::Real,
                verified_uid: "c".repeat(64),
            },
            alias: "missing-credential".into(),
            credential_ref: "never-present-512".into(),
            connect_operation_id: "connect_missing_512".into(),
            configuration_digest: "d".repeat(64),
        };
        assert!(
            initialize_account_on_owner(&owner, &config, "e")
                .await
                .is_err()
        );
        assert!(
            owner
                .account_metadata(
                    Instant::now() + Duration::from_secs(2),
                    config.account_key.wire(),
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(!root.join("accounts").exists());
        let mut stopped = owner.termination_signal();
        drop(owner);
        if !*stopped.borrow() {
            stopped.changed().await.unwrap();
        }
    }
    #[tokio::test]
    async fn new_finance_command_adapter_replays_receipt_and_releases_the_same_writer() {
        let root = root();
        let tree = "a".repeat(40);
        let first = initialize_finance(&root, "installation_test", "initialize_test_001", &tree)
            .await
            .unwrap();
        assert_eq!(first["schema_version"], 8);
        let replay = initialize_finance(&root, "installation_test", "initialize_test_001", &tree)
            .await
            .unwrap();
        assert_eq!(replay, first);
        assert!(
            initialize_finance(&root, "installation_test", "different_request_002", &tree)
                .await
                .is_err()
        );
        let owner = okx_trade_execution::account_worker::UnifiedFinanceHandle::open_native(
            root.clone(),
            "installation_test".into(),
        )
        .await
        .unwrap();
        assert!(matches!(
            owner
                .verify_live_authority(Instant::now() + Duration::from_secs(2))
                .await,
            Err(okx_trade_execution::StoreError::Invalid(
                "FINANCE_ACCOUNT_BOOTSTRAP_REQUIRED"
            ))
        ));
        let mut stopped = owner.termination_signal();
        drop(owner);
        if !*stopped.borrow() {
            stopped.changed().await.unwrap();
        }
        // No account identity, credentials or exchange client was invented.
        assert!(!root.join(account_catalog_proto::CATALOG_FILE).exists());
        assert!(!root.join("accounts").exists());
    }
}
mod backup;
pub use backup::{backup_account, restore_account};
mod native_backup;
pub use native_backup::{backup_finance, restore_finance};
