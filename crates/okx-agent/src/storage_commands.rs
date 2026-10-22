//! Explicit, stopped-Engine operations for the new SQL format only.
use crate::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    accounts::{build_release, key_hash, verify_credentials},
    okx_credentials::load_native_account_credentials,
};
use okx_api::{OkxRestClient, RateBudget};
use okx_execution::{AccountMeta, AccountStore, AccountStoreHandle, OpeningSnapshot};
use okx_protocol::accounts::{AccountKey, Environment, VerifiedAccountConfig, valid_reference};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
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
fn installation(root: &Path) -> AgentResult<String> {
    let v: Value = serde_json::from_slice(&fs::read(root.join("installation.json"))?)?;
    if v["schema"] != "okx.installation/v1" {
        return Err(error("INSTALLATION_INVALID"));
    }
    let id = v["installation_id"]
        .as_str()
        .ok_or_else(|| error("INSTALLATION_INVALID"))?;
    if !valid_reference(id) {
        return Err(error("INSTALLATION_INVALID"));
    }
    Ok(id.into())
}
fn catalog(root: &Path, key: &AccountKey) -> AgentResult<VerifiedAccountConfig> {
    if !key.valid() {
        return Err(error("ACCOUNT_KEY_INVALID"));
    }
    let c: VerifiedAccountConfig = serde_json::from_slice(&fs::read(
        root.join("account-catalog")
            .join(format!("{}.json", key_hash(key))),
    )?)?;
    if !c.valid() || c.account_key != *key {
        return Err(error("ACCOUNT_CATALOG_INVALID"));
    }
    Ok(c)
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
        if root.join("installation.json").exists() {
            if installation(root)? != installation_id {
                return Err(error("INSTALLATION_MISMATCH"));
            }
            fs::create_dir_all(root.join("account-catalog"))?;
        } else {
            crate::accounts::initialize_installation(root, installation_id)?;
        }
        return Ok(reply(installation_id, operation_id, None, json!({})));
    }
    if root.join("installation.json").exists() {
        return Err(error("INSTALLATION_ALREADY_EXISTS"));
    }
    write_new(&op_path, &serde_json::to_vec(&expected)?)?;
    crate::accounts::initialize_installation(root, installation_id)?;
    Ok(reply(installation_id, operation_id, None, json!({})))
}
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
    let _lease = engine_lease(root)?;
    if installation(root)? != installation_id {
        return Err(error("INSTALLATION_MISMATCH"));
    }
    let config = catalog(root, key)?;
    if config.connect_operation_id != operation_id {
        return Err(error("INITIALIZE_OPERATION_CONFLICT"));
    }
    let dir = account_root(root, key);
    fs::create_dir_all(&dir)?;
    let database = dir.join("account.sqlite");
    if database.exists() {
        let store =
            AccountStoreHandle::open_existing(database, key.wire(), installation_id.into()).await?;
        let op = operation_id.to_string();
        let rel = expected_release.to_string();
        store
            .call(Instant::now() + Duration::from_secs(2), move |s| {
                if s.meta().initialize_operation_id != op || s.meta().release_id != rel {
                    return Err(okx_execution::StoreError::Invalid(
                        "INITIALIZE_OPERATION_CONFLICT",
                    ));
                }
                Ok(())
            })
            .await?;
        return Ok(reply(installation_id, operation_id, Some(key), json!({})));
    }
    let credentials = load_native_account_credentials(&config.credential_ref)?;
    let budget = RateBudget::new();
    let uid = verify_credentials(&credentials, budget.clone()).await?;
    if uid != key.verified_uid
        || credentials.environment.demo != (key.environment == Environment::Demo)
    {
        return Err(error("ACCOUNT_IDENTITY_MISMATCH"));
    }
    let observer = AccountBootstrapper::new(OkxRestClient::with_rate_budget(
        credentials.environment,
        credentials.observer,
        budget,
    )?);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let snapshot = loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(error("INITIAL_SNAPSHOT_UNSTABLE"));
        }
        let first = observer
            .snapshot()
            .await
            .map_err(|_| error("ACCOUNT_SOURCE_UNAVAILABLE"))?;
        let algos = observer
            .pending_derivative_risk_algos()
            .await
            .map_err(|_| error("ACCOUNT_ALGO_SOURCE_UNAVAILABLE"))?;
        let last = observer
            .snapshot()
            .await
            .map_err(|_| error("ACCOUNT_SOURCE_UNAVAILABLE"))?;
        if first.account_uid_fingerprint != uid || last.account_uid_fingerprint != uid {
            return Err(error("ACCOUNT_IDENTITY_MISMATCH"));
        }
        if first.balance != last.balance
            || first.positions != last.positions
            || first.pending_orders != last.pending_orders
        {
            continue;
        }
        if !algos.complete_within_bound {
            return Err(error("ACCOUNT_ALGO_COVERAGE_INCOMPLETE"));
        }
        let liabilities = last
            .balance
            .details
            .iter()
            .try_fold(Decimal::ZERO, |a, b| {
                let v = match b.liabilities.as_deref() {
                    Some(v) => Decimal::from_str(v).map_err(|_| error("LIABILITY_INVALID"))?,
                    None if last.account_level == "2" => Decimal::ZERO,
                    None => return Err(error("LIABILITY_UNAVAILABLE")),
                };
                a.checked_add(v).ok_or_else(|| error("MONEY_OVERFLOW"))
            })?;
        let positions = last
            .positions
            .iter()
            .filter(|p| Decimal::from_str(&p.position) != Ok(Decimal::ZERO))
            .count();
        let balances = last
            .balance
            .details
            .iter()
            .map(|b| {
                Ok((
                    b.currency.clone(),
                    Decimal::from_str(&b.equity).map_err(|_| error("BALANCE_INVALID"))?,
                ))
            })
            .collect::<AgentResult<Vec<_>>>()?;
        let observed_at = chrono::DateTime::parse_from_rfc3339(&last.source_received_at)
            .map_err(|_| error("SOURCE_TIMESTAMP_INVALID"))?
            .timestamp_millis();
        let source_ref = format!("{:x}", Sha256::digest(serde_json::to_vec(&last)?));
        break OpeningSnapshot {
            opening_equity_usd: Decimal::from_str(&last.balance.total_equity_usd)
                .map_err(|_| error("OPENING_USD_INVALID"))?,
            source_ref,
            observed_at,
            positions,
            orders: last.pending_orders.len(),
            conditional_orders: algos.rows,
            liabilities,
            balances,
        };
    };
    let meta = AccountMeta {
        account_key: key.wire(),
        installation_id: installation_id.into(),
        initialized_at: snapshot.observed_at,
        opening_source: snapshot.source_ref.clone(),
        initialize_operation_id: operation_id.into(),
        release_id: expected_release.into(),
    };
    let store = AccountStoreHandle::initialize_flat(database, meta, snapshot).await?;
    store
        .call(Instant::now() + Duration::from_secs(2), |s| {
            s.integrity_check()
        })
        .await?;
    Ok(reply(installation_id, operation_id, Some(key), json!({})))
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
    fn storage_keys_cannot_select_arbitrary_paths() {
        assert!(parse_key("../../other").is_err());
        assert!(parse_key(&format!("okx:real:{}", "a".repeat(64))).is_ok());
        assert!(parse_key(&format!("okx:demo:{}", "g".repeat(64))).is_err());
        assert!(release("UNVERIFIED_LOCAL_BUILD").is_err());
    }
}
mod backup;
pub use backup::{backup_account, restore_account};
