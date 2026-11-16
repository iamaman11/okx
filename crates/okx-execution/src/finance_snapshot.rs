//! Stopped whole-installation snapshots. Original SQL identity and obligations
//! are preserved; restore never replaces an existing financial database.
use crate::{
    AccountStore, FinanceInstallationReceipt, StoreError, StoreResult, finance_owner_gate,
};
use rusqlite::{
    Connection, OpenFlags,
    backup::{Backup, StepResult},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinanceBackupIdentity {
    pub installation_id: String,
    pub initialize_request_id: String,
    pub request_sha256: String,
    pub source_tree: String,
    pub created_at_ms: i64,
    pub schema_version: u32,
}
impl From<FinanceInstallationReceipt> for FinanceBackupIdentity {
    fn from(r: FinanceInstallationReceipt) -> Self {
        Self {
            installation_id: r.installation_id,
            initialize_request_id: r.initialize_request_id,
            request_sha256: r.request_sha256,
            source_tree: r.source_tree,
            created_at_ms: r.created_at_ms,
            schema_version: r.schema_version,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinanceBackupManifest {
    pub format: String,
    pub identity: FinanceBackupIdentity,
    pub captured_at_ms: i64,
    pub accounts: Vec<String>,
    pub campaigns: Vec<String>,
    pub sha256: String,
}

fn invalid(code: &'static str) -> StoreError {
    StoreError::Invalid(code)
}
fn now() -> StoreResult<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .filter(|v| *v > 0)
        .ok_or(invalid("FINANCE_SNAPSHOT_CLOCK_INVALID"))
}
fn plain(path: &Path, directory: bool) -> StoreResult<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let linked = metadata.file_type().is_symlink();
    #[cfg(windows)]
    let linked = {
        use std::os::windows::fs::MetadataExt;
        linked || metadata.file_attributes() & 0x400 != 0
    };
    if linked || metadata.is_dir() != directory || (!directory && !metadata.is_file()) {
        return Err(invalid("FINANCE_SNAPSHOT_PATH_INVALID"));
    }
    Ok(true)
}
fn directory(path: &Path) -> StoreResult<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid("FINANCE_SNAPSHOT_PATH_INVALID"));
    }
    for ancestor in path.ancestors() {
        if !plain(ancestor, true)? {
            return Err(invalid("FINANCE_SNAPSHOT_PARENT_MISSING"));
        }
    }
    Ok(path.canonicalize()?)
}
fn lock(path: &Path) -> StoreResult<File> {
    plain(path, false)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.try_lock()
        .map_err(|_| invalid("UNIFIED_FINANCE_WRITER_BUSY"))?;
    Ok(file)
}
// Uses the exact same exclusive mode and writer lock as owner_startup. An
// engine.lock alone cannot exclude a directly opened UnifiedFinanceHandle.
fn stopped(root: &Path) -> StoreResult<(PathBuf, finance_owner_gate::AuthorityModeLease, File)> {
    let root = directory(root)?;
    for old in ["accounts", "risk", "installation.json", "account-catalog"] {
        if fs::symlink_metadata(root.join(old)).is_ok() {
            return Err(invalid("FINANCE_EXISTING_STATE_NOT_SUPPORTED"));
        }
    }
    let financial = root.join("finance");
    if !plain(&financial, true)? {
        return Err(invalid("FINANCE_RUNTIME_ROOT_MISSING"));
    }
    for entry in fs::read_dir(&financial)? {
        let entry = entry?;
        if !matches!(
            entry.file_name().to_str(),
            Some(
                "authority-mode.lock"
                    | "finance.sqlite.writer.lock"
                    | "finance.sqlite"
                    | "finance.sqlite-wal"
                    | "finance.sqlite-shm"
            )
        ) {
            return Err(invalid("FINANCE_UNKNOWN_DIRECTORY_ENTRY"));
        }
        plain(&entry.path(), false)?;
    }
    let mode = finance_owner_gate::unified_mode_for(&root).map_err(|e| match e {
        finance_owner_gate::AuthorityModeError::Io(e) => StoreError::Io(e),
        _ => invalid("UNIFIED_FINANCE_WRITER_BUSY"),
    })?;
    let writer = lock(&financial.join("finance.sqlite.writer.lock"))?;
    Ok((financial, mode, writer))
}
fn hash(path: &Path) -> StoreResult<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
fn readonly(path: &Path) -> StoreResult<Connection> {
    if !plain(path, false)? {
        return Err(invalid("FINANCE_SNAPSHOT_MISSING"));
    }
    // Reject unknown formats before SQLite can create or recover sidecars.
    let mut header = [0u8; 100];
    File::open(path)?.read_exact(&mut header)?;
    if &header[..16] != b"SQLite format 3\0"
        || u32::from_be_bytes(header[68..72].try_into().unwrap()) != 0x4f4b5846
    {
        return Err(invalid("FINANCE_FILE_FORMAT_UNKNOWN"));
    }
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.pragma_update(None, "foreign_keys", "ON")?;
    Ok(db)
}
fn facts(
    db: &Connection,
    installation: &str,
) -> StoreResult<(FinanceBackupIdentity, Vec<String>, Vec<String>)> {
    let identity = AccountStore::inspect_finance_installation(db, installation)?.into();
    let list = |sql| -> StoreResult<Vec<String>> {
        Ok(db
            .prepare(sql)?
            .query_map([installation], |r| r.get(0))?
            .collect::<Result<Vec<_>, _>>()?)
    };
    Ok((
        identity,
        list("SELECT account_key FROM account_meta WHERE installation_id=?1 ORDER BY account_key")?,
        list("SELECT campaign_id FROM risk_meta WHERE installation_id=?1 ORDER BY campaign_id")?,
    ))
}
fn cohort(actual: &[String], expected: &[String]) -> StoreResult<()> {
    let mut expected = expected.to_vec();
    expected.sort();
    if expected.windows(2).any(|w| w[0] == w[1]) || actual != expected {
        return Err(invalid("FINANCE_SNAPSHOT_CATALOG_MISMATCH"));
    }
    Ok(())
}
fn recovery_cutoffs(
    db: &Connection,
    installation: &str,
    captured_at: i64,
    accounts: usize,
) -> StoreResult<()> {
    let (total, valid): (i64, i64) = db.query_row(
        "SELECT COUNT(*),COALESCE(SUM(CASE WHEN requires_reconcile=1 AND backup_at>0 AND backup_at<=?2
         AND reconciled_evidence IS NULL THEN 1 ELSE 0 END),0)
         FROM storage_recovery WHERE installation_id=?1",
        rusqlite::params![installation, captured_at], |r| Ok((r.get(0)?,r.get(1)?)))?;
    if total != accounts as i64 || valid != total {
        return Err(invalid("FINANCE_SNAPSHOT_RECOVERY_COHORT_MISMATCH"));
    }
    Ok(())
}
fn snapshot_copy(source: &Connection, target: &Path) -> StoreResult<()> {
    // Reserve before SQLite opens it: no overwrite/create race on the target.
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)?;
    let mut db = Connection::open_with_flags(target, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    {
        let backup = Backup::new(source, &mut db)?;
        if !matches!(backup.step(-1)?, StepResult::Done) {
            return Err(invalid("FINANCE_SNAPSHOT_COPY_INCOMPLETE"));
        }
    }
    db.pragma_update(None, "foreign_keys", "ON")?;
    // A portable snapshot has no uncheckpointed changes outside its main file.
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(db);
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(target)?
        .sync_all()?;
    Ok(())
}
// SQLite leaves empty WAL/SHM files when the last connection was read-only.
// These are VERIFIED snapshot artifacts, never the installation source. A
// writable checkpoint/close lets SQLite finish its own portable-file cleanup;
// no sidecar (which could contain committed pages) is manually deleted.
fn seal_archive(path: &Path) -> StoreResult<()> {
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    let (busy, _, _): (i64, i64, i64) =
        db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    if busy != 0 {
        return Err(invalid("FINANCE_SNAPSHOT_CHECKPOINT_BUSY"));
    }
    db.close().map_err(|(_, error)| StoreError::from(error))?;
    for suffix in ["-wal", "-shm"] {
        let sidecar = path.with_file_name(format!(
            "{}{suffix}",
            path.file_name().unwrap().to_string_lossy()
        ));
        if fs::symlink_metadata(sidecar).is_ok() {
            return Err(invalid("FINANCE_SNAPSHOT_SIDECAR_UNEXPECTED"));
        }
    }
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?
        .sync_all()?;
    Ok(())
}
fn sync_directory(path: &Path) -> StoreResult<()> {
    #[cfg(not(windows))]
    File::open(path)?.sync_all()?;
    #[cfg(windows)]
    let _ = path;
    Ok(())
}

/// Caller holds the stopped Engine lease and supplies its verified catalog.
/// The source is read-only and includes committed WAL pages via SQLite backup.
pub fn backup_stopped_finance(
    root: &Path,
    installation: &str,
    expected_accounts: &[String],
    snapshot_dir: &Path,
) -> StoreResult<FinanceBackupManifest> {
    let (financial, _mode, _writer) = stopped(root)?;
    let source = readonly(&financial.join("finance.sqlite"))?;
    let (identity, accounts, campaigns) = facts(&source, installation)?;
    cohort(&accounts, expected_accounts)?;
    let parent = directory(
        snapshot_dir
            .parent()
            .ok_or(invalid("FINANCE_SNAPSHOT_PATH_INVALID"))?,
    )?;
    if parent.starts_with(&financial) {
        return Err(invalid("FINANCE_SNAPSHOT_PATH_INVALID"));
    }
    if !snapshot_dir.is_absolute() {
        return Err(invalid("FINANCE_SNAPSHOT_PATH_INVALID"));
    }
    fs::create_dir(snapshot_dir)?;
    let target = snapshot_dir.join("finance.sqlite");
    snapshot_copy(&source, &target)?;
    let copied = readonly(&target)?;
    if facts(&copied, installation)? != (identity.clone(), accounts.clone(), campaigns.clone()) {
        return Err(invalid("FINANCE_SNAPSHOT_IDENTITY_MISMATCH"));
    }
    drop(copied);
    let captured_at = now()?;
    // Bind the recovery threshold into the checksummed archive, never into
    // editable manifest metadata alone. An already unresolved earlier audit
    // must not be shortened by taking another backup of its current state.
    let mut archived = Connection::open_with_flags(&target, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    archived.pragma_update(None, "synchronous", "FULL")?;
    let tx = archived.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute(
        "UPDATE storage_recovery SET backup_at=CASE
        WHEN requires_reconcile=1 THEN MIN(COALESCE(backup_at,(SELECT initialized_at FROM account_meta a
            WHERE a.installation_id=storage_recovery.installation_id AND a.account_key=storage_recovery.account_key)),?1)
        ELSE ?1 END, requires_reconcile=1,reconciled_evidence=NULL WHERE installation_id=?2",
        rusqlite::params![captured_at, installation],
    )?;
    recovery_cutoffs(&tx, installation, captured_at, accounts.len())?;
    tx.commit()?;
    drop(archived);
    seal_archive(&target)?;
    let manifest = FinanceBackupManifest {
        format: "okx.finance-backup/v1".into(),
        identity,
        captured_at_ms: captured_at,
        accounts,
        campaigns,
        sha256: hash(&target)?,
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(snapshot_dir.join("manifest.json"))?;
    let bytes =
        serde_json::to_vec(&manifest).map_err(|_| invalid("FINANCE_SNAPSHOT_MANIFEST_INVALID"))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    sync_directory(snapshot_dir)?;
    sync_directory(&parent)?;
    Ok(manifest)
}

/// Restores the original installation only when its database is absent.
/// Existing/unknown files, WAL/SHM obligations and active writers are refused.
/// Every restored account requires ORIGINAL storage/source reconciliation.
pub fn restore_stopped_finance(
    root: &Path,
    installation: &str,
    expected_accounts: &[String],
    snapshot_dir: &Path,
) -> StoreResult<FinanceBackupManifest> {
    let (financial, _mode, _writer) = stopped(root)?;
    let target = financial.join("finance.sqlite");
    for name in ["finance.sqlite", "finance.sqlite-wal", "finance.sqlite-shm"] {
        if fs::symlink_metadata(financial.join(name)).is_ok() {
            return Err(invalid("FINANCE_RESTORE_TARGET_EXISTS"));
        }
    }
    directory(snapshot_dir)?;
    let manifest_path = snapshot_dir.join("manifest.json");
    if !plain(&manifest_path, false)? || fs::metadata(&manifest_path)?.len() > 32 * 1024 {
        return Err(invalid("FINANCE_SNAPSHOT_MANIFEST_INVALID"));
    }
    let manifest: FinanceBackupManifest = serde_json::from_slice(&fs::read(manifest_path)?)
        .map_err(|_| invalid("FINANCE_SNAPSHOT_MANIFEST_INVALID"))?;
    let source_path = snapshot_dir.join("finance.sqlite");
    if !plain(&source_path, false)? {
        return Err(invalid("FINANCE_SNAPSHOT_MISSING"));
    }
    for suffix in ["finance.sqlite-wal", "finance.sqlite-shm"] {
        if fs::symlink_metadata(snapshot_dir.join(suffix)).is_ok() {
            return Err(invalid("FINANCE_SNAPSHOT_SIDECAR_UNEXPECTED"));
        }
    }
    if manifest.format != "okx.finance-backup/v1"
        || manifest.captured_at_ms <= 0
        || manifest.captured_at_ms > now()?
        || manifest.identity.installation_id != installation
        || hash(&source_path)? != manifest.sha256
    {
        return Err(invalid("FINANCE_SNAPSHOT_MANIFEST_MISMATCH"));
    }
    cohort(&manifest.accounts, expected_accounts)?;
    // A published standalone backup is immutable input, including on read-only
    // media. Validate SQLite only on a private byte-for-byte staging copy:
    // opening a WAL-format archive directly can create WAL/SHM beside it.
    let staged = financial.join("finance.restore-staged.sqlite");
    let mut staged_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staged)?;
    let mut source_file = File::open(&source_path)?;
    std::io::copy(&mut source_file, &mut staged_file)?;
    staged_file.sync_all()?;
    drop(staged_file);
    drop(source_file);
    if hash(&staged)? != manifest.sha256 {
        return Err(invalid("FINANCE_SNAPSHOT_MANIFEST_MISMATCH"));
    }
    let restored = Connection::open_with_flags(&staged, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    restored.pragma_update(None, "foreign_keys", "ON")?;
    restored.pragma_update(None, "synchronous", "FULL")?;
    let (identity, accounts, campaigns) = facts(&restored, installation)?;
    if (identity, accounts.clone(), campaigns)
        != (
            manifest.identity.clone(),
            manifest.accounts.clone(),
            manifest.campaigns.clone(),
        )
    {
        return Err(invalid("FINANCE_SNAPSHOT_MANIFEST_MISMATCH"));
    }
    cohort(&accounts, expected_accounts)?;
    // Keep the original checksummed cutoffs. A later manifest timestamp or
    // later restore time cannot erase the missing post-snapshot history.
    recovery_cutoffs(
        &restored,
        installation,
        manifest.captured_at_ms,
        accounts.len(),
    )?;
    if FinanceBackupIdentity::from(AccountStore::inspect_finance_installation(
        &restored,
        installation,
    )?) != manifest.identity
    {
        return Err(invalid("FINANCE_SNAPSHOT_IDENTITY_MISMATCH"));
    }
    drop(restored);
    seal_archive(&staged)?;
    // Atomic no-replace publication, unlike rename-over-existing on Unix.
    fs::hard_link(&staged, &target)?;
    fs::remove_file(&staged)?;
    sync_directory(&financial)?;
    Ok(manifest)
}
