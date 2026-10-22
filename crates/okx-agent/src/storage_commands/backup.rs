use super::*;

pub async fn backup_account(root: &Path, key: &str) -> AgentResult<Value> {
    release(build_release())?;
    let root = root.to_path_buf();
    let key = parse_key(key)?;
    tokio::task::spawn_blocking(move || backup_stopped(&root, &key))
        .await
        .map_err(|_| error("BACKUP_WORKER_FAILED"))?
}

fn backup_stopped(root: &Path, key: &AccountKey) -> AgentResult<Value> {
    let _engine = engine_lease(root)?;
    let installation_id = installation(root)?;
    catalog(root, key)?;
    let directory = account_root(root, key);
    let store = AccountStore::open_existing(
        directory.join("account.sqlite"),
        &key.wire(),
        &installation_id,
    )?;
    let now = chrono::Utc::now().timestamp_millis();
    let snapshot_id = format!("account-{now}");
    let target = directory.join("backups").join(format!("{snapshot_id}.db"));
    fs::create_dir_all(
        target
            .parent()
            .ok_or_else(|| error("BACKUP_PARENT_MISSING"))?,
    )?;
    store.backup(&target)?;
    let manifest = json!({"schema_version":2,"account_key":key.wire(),
        "installation_id":installation_id,"time":now,"sha256":hash(&target)?,"release":build_release()});
    write_new(
        &target.with_extension("manifest.json"),
        &serde_json::to_vec(&manifest)?,
    )?;
    Ok(reply(
        &installation_id,
        &snapshot_id,
        Some(key),
        json!({"snapshot_id":snapshot_id}),
    ))
}

pub async fn restore_account(
    root: &Path,
    key: &str,
    snapshot_id: &str,
    expected_release: &str,
    operation_id: &str,
) -> AgentResult<Value> {
    release(expected_release)?;
    if !valid_operation(operation_id)
        || snapshot_id.len() > 100
        || !snapshot_id.starts_with("account-")
        || !snapshot_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(error("RESTORE_REFERENCE_INVALID"));
    }
    let root = root.to_path_buf();
    let key = parse_key(key)?;
    let snapshot_id = snapshot_id.to_string();
    let operation_id = operation_id.to_string();
    tokio::task::spawn_blocking(move || restore_stopped(&root, &key, &snapshot_id, &operation_id))
        .await
        .map_err(|_| error("RESTORE_WORKER_FAILED"))?
}

fn restore_stopped(
    root: &Path,
    key: &AccountKey,
    snapshot_id: &str,
    operation_id: &str,
) -> AgentResult<Value> {
    let _engine = engine_lease(root)?;
    let installation_id = installation(root)?;
    catalog(root, key)?;
    let directory = account_root(root, key);
    let snapshot = directory.join("backups").join(format!("{snapshot_id}.db"));
    let manifest: Value =
        serde_json::from_slice(&fs::read(snapshot.with_extension("manifest.json"))?)?;
    if manifest["schema_version"] != 2
        || manifest["account_key"] != key.wire()
        || manifest["installation_id"] != installation_id
        || manifest["sha256"] != hash(&snapshot)?
    {
        return Err(error("BACKUP_MANIFEST_MISMATCH"));
    }
    let backup = AccountStore::open_existing(&snapshot, &key.wire(), &installation_id)?;
    backup.integrity_check()?;
    if !backup.storage_recovery_required()? {
        return Err(error("STORAGE_RECOVERY_INCOMPLETE"));
    }
    drop(backup);

    let database = directory.join("account.sqlite");
    let marker = database.with_extension("restore.pending");
    let request =
        json!({"operation_id":operation_id,"snapshot_id":snapshot_id,"release":build_release()});
    if marker.exists() {
        let prior: Value = serde_json::from_slice(&fs::read(&marker)?)?;
        if prior != request {
            return Err(error("RESTORE_OPERATION_CONFLICT"));
        }
    } else {
        // A corrupt current DB is an explicit restore use case. The verified snapshot,
        // writer lease and recovery gate provide authority; no empty DB is synthesized.
        if database.exists() {
            let current = AccountStore::open_existing(&database, &key.wire(), &installation_id);
            if let Err(okx_execution::StoreError::Invalid(code)) = &current
                && matches!(
                    *code,
                    "ACCOUNT_IDENTITY_MISMATCH" | "ACCOUNT_WRITER_ALREADY_RUNNING"
                )
            {
                return Err(error(code));
            }
            if let Ok(current) = current {
                if let Some(prior) = current.operation(operation_id)? {
                    if prior.kind == "RESTORE" && prior.request_hash == snapshot_id {
                        return Ok(reply(
                            &installation_id,
                            operation_id,
                            Some(key),
                            json!({"reconcile_required":true}),
                        ));
                    }
                    return Err(error("RESTORE_OPERATION_CONFLICT"));
                }
                current.checkpoint()?;
            }
        }
        write_new(&marker, &serde_json::to_vec(&request)?)?;
    }
    let writer = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(format!("{}.writer.lock", database.display()))?;
    writer
        .try_lock()
        .map_err(|_| error("ACCOUNT_WRITER_ALREADY_RUNNING"))?;
    let staged = directory.join(format!("restore-{operation_id}.sqlite"));
    if staged.exists() {
        fs::remove_file(&staged)?;
    }
    fs::copy(&snapshot, &staged)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(&staged)?
        .sync_all()?;
    let quarantine = directory.join(format!("before-restore-{operation_id}"));
    fs::create_dir_all(&quarantine)?;
    for suffix in ["-wal", "-shm", ""] {
        let old = PathBuf::from(format!("{}{suffix}", database.display()));
        if old.exists() {
            let destination = quarantine.join(format!("account.sqlite{suffix}"));
            if destination.exists() {
                fs::remove_file(old)?;
            } else {
                fs::rename(old, destination)?;
            }
        }
    }
    fs::rename(staged, &database)?;
    #[cfg(not(windows))]
    File::open(&directory)?.sync_all()?;
    // The global Engine lease excludes starts during this final local commit.
    drop(writer);
    fs::remove_file(&marker)?;
    let outcome = (|| -> AgentResult<()> {
        let mut store = AccountStore::open_existing(&database, &key.wire(), &installation_id)?;
        store.operation_receipt(
            operation_id,
            snapshot_id,
            "RESTORE",
            "RECONCILE_REQUIRED",
            chrono::Utc::now().timestamp_millis(),
        )?;
        Ok(())
    })();
    if let Err(error) = outcome {
        write_new(&marker, &serde_json::to_vec(&request)?)?;
        return Err(error);
    }
    Ok(reply(
        &installation_id,
        operation_id,
        Some(key),
        json!({"reconcile_required":true}),
    ))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (PathBuf, AccountKey) {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let root =
            std::env::temp_dir().join(format!("okx-restore-{:x}", u128::from_le_bytes(nonce)));
        crate::accounts::initialize_installation(&root, "installation-fixture").unwrap();
        let key = AccountKey {
            venue: "okx".into(),
            environment: Environment::Real,
            verified_uid: "a".repeat(64),
        };
        let config = VerifiedAccountConfig {
            account_key: key.clone(),
            alias: "fixture".into(),
            credential_ref: "fixture-key".into(),
            connect_operation_id: "connect-operation".into(),
            configuration_digest: "b".repeat(64),
        };
        write_new(
            &root
                .join("account-catalog")
                .join(format!("{}.json", key_hash(&key))),
            &serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        let directory = account_root(&root, &key);
        fs::create_dir_all(&directory).unwrap();
        let opening = OpeningSnapshot {
            opening_equity_usd: Decimal::from(100),
            source_ref: "flat-authenticated".into(),
            observed_at: 1,
            positions: 0,
            orders: 0,
            conditional_orders: 0,
            liabilities: Decimal::ZERO,
            balances: vec![("USDT".into(), Decimal::from(100))],
        };
        let meta = AccountMeta {
            initialize_operation_id: "connect-operation".into(),
            release_id: "fixture".into(),
            account_key: key.wire(),
            installation_id: "installation-fixture".into(),
            initialized_at: 1,
            opening_source: opening.source_ref.clone(),
        };
        drop(
            AccountStore::initialize_flat(directory.join("account.sqlite"), meta, &opening)
                .unwrap(),
        );
        (root, key)
    }
    #[test]
    fn native_backup_restore_is_idempotent_and_reconciles_even_corrupt_current_db() {
        let (root, key) = fixture();
        let result = backup_stopped(&root, &key).unwrap();
        let snapshot = result["snapshot_id"].as_str().unwrap();
        let path = account_root(&root, &key).join("account.sqlite");
        fs::write(&path, b"corrupt-current-file").unwrap();
        let first = restore_stopped(&root, &key, snapshot, "restore-operation").unwrap();
        let second = restore_stopped(&root, &key, snapshot, "restore-operation").unwrap();
        assert_eq!(first, second);
        assert_eq!(first["reconcile_required"], true);
        let store =
            AccountStore::open_existing(&path, &key.wire(), "installation-fixture").unwrap();
        assert!(store.storage_recovery_required().unwrap());
        assert_eq!(store.opening_equity_usd().unwrap(), Decimal::from(100));
        assert!(store.operation("restore-operation").unwrap().is_some());
        assert!(!path.with_extension("restore.pending").exists());
    }
    #[test]
    fn restore_rejects_manifest_tampering_before_touching_live_db() {
        let (root, key) = fixture();
        let result = backup_stopped(&root, &key).unwrap();
        let snapshot = result["snapshot_id"].as_str().unwrap();
        let directory = account_root(&root, &key);
        let manifest = directory
            .join("backups")
            .join(format!("{snapshot}.manifest.json"));
        let mut m: Value = serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
        m["sha256"] = json!("wrong-hash");
        fs::write(manifest, serde_json::to_vec(&m).unwrap()).unwrap();
        let path = directory.join("account.sqlite");
        let prior = hash(&path).unwrap();
        assert!(restore_stopped(&root, &key, snapshot, "restore-operation").is_err());
        assert_eq!(hash(&path).unwrap(), prior);
    }
}
