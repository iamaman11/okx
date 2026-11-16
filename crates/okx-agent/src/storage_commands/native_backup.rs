//! Stopped Native whole-finance maintenance. Catalog/credentials remain owned
//! by their existing components; a snapshot never creates another identity.
use super::*;
use okx_execution::finance_snapshot::{
    FinanceBackupManifest, backup_stopped_finance, restore_stopped_finance,
};

fn snapshot_id(id: &str) -> bool {
    id.strip_prefix("finance-").is_some_and(|v| {
        v.len() == 32
            && v.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
fn plain(path: &Path, directory: bool) -> AgentResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    let linked = metadata.file_type().is_symlink();
    #[cfg(windows)]
    let linked = {
        use std::os::windows::fs::MetadataExt;
        linked || metadata.file_attributes() & 0x400 != 0
    };
    if linked || metadata.is_dir() != directory || (!directory && !metadata.is_file()) {
        return Err(error("FINANCE_SNAPSHOT_PATH_INVALID"));
    }
    Ok(())
}
// These operations require an already initialized, recognizable installation.
// Unknown root entries are evidence, never permission to replace financial data.
fn known_root(root: &Path) -> AgentResult<()> {
    if !root.is_absolute() {
        return Err(error("FINANCE_SNAPSHOT_PATH_INVALID"));
    }
    for ancestor in root.ancestors() {
        plain(ancestor, true)?;
    }
    for item in fs::read_dir(root)? {
        let item = item?;
        let directory = match item.file_name().to_str() {
            Some("finance" | "finance-backups" | "analytics" | "account-leases") => true,
            Some(
                "engine.lock"
                | "account-catalog.pb"
                | "installation-operation.json"
                | "okx-agent.exe"
                | "installed-agent.json"
                | "okx-agent.stdout.log"
                | "okx-agent.stderr.log",
            ) => false,
            _ => return Err(error("FINANCE_MAINTENANCE_UNKNOWN_ROOT_ENTRY")),
        };
        plain(&item.path(), directory)?;
    }
    plain(&root.join(account_catalog_proto::CATALOG_FILE), false)?;
    plain(&root.join("installation-operation.json"), false)?;
    Ok(())
}
fn original_initialization(root: &Path, manifest: &FinanceBackupManifest) -> AgentResult<()> {
    let record: Value =
        serde_json::from_slice(&fs::read(root.join("installation-operation.json"))?)?;
    if record["installation_id"] != manifest.identity.installation_id
        || record["operation_id"] != manifest.identity.initialize_request_id
        || record["release"] != manifest.identity.source_tree
    {
        return Err(error("FINANCE_SNAPSHOT_SOURCE_IDENTITY_MISMATCH"));
    }
    Ok(())
}
fn accounts(root: &Path) -> AgentResult<(String, Vec<String>)> {
    let catalog = account_catalog_proto::load(root)?;
    Ok((
        catalog.installation_id,
        catalog
            .accounts
            .iter()
            .map(|a| a.account_key.wire())
            .collect(),
    ))
}

pub async fn backup_finance(root: &Path) -> AgentResult<Value> {
    release(build_release())?;
    let root = root.to_path_buf();
    tokio::task::spawn_blocking(move || backup(&root))
        .await
        .map_err(|_| error("BACKUP_WORKER_FAILED"))?
}
fn backup(root: &Path) -> AgentResult<Value> {
    known_root(root)?;
    let _engine = engine_lease(root)?;
    let (installation, accounts) = accounts(root)?;
    let backups = root.join("finance-backups");
    if !backups.exists() {
        fs::create_dir(&backups)?;
    }
    plain(&backups, true)?;
    let mut nonce = [0u8; 16];
    getrandom::fill(&mut nonce).map_err(|_| error("STORAGE_NONCE_UNAVAILABLE"))?;
    let id = format!("finance-{:032x}", u128::from_le_bytes(nonce));
    let manifest = backup_stopped_finance(root, &installation, &accounts, &backups.join(&id))?;
    original_initialization(root, &manifest)?;
    Ok(reply(
        &installation,
        &id,
        None,
        json!({"snapshot_id":id,"scope":"whole_installation",
        "accounts":manifest.accounts,"campaigns":manifest.campaigns,"sha256":manifest.sha256}),
    ))
}

pub async fn restore_finance(
    root: &Path,
    snapshot: &str,
    expected_release: &str,
    operation: &str,
) -> AgentResult<Value> {
    release(expected_release)?;
    if !snapshot_id(snapshot) || !valid_operation(operation) {
        return Err(error("RESTORE_REFERENCE_INVALID"));
    }
    let root = root.to_path_buf();
    let snapshot = snapshot.to_owned();
    let operation = operation.to_owned();
    tokio::task::spawn_blocking(move || restore(&root, &snapshot, &operation))
        .await
        .map_err(|_| error("RESTORE_WORKER_FAILED"))?
}
fn restore(root: &Path, snapshot: &str, operation: &str) -> AgentResult<Value> {
    known_root(root)?;
    let _engine = engine_lease(root)?;
    let (installation, accounts) = accounts(root)?;
    let directory = root.join("finance-backups").join(snapshot);
    plain(&root.join("finance-backups"), true)?;
    plain(&directory, true)?;
    plain(&directory.join("manifest.json"), false)?;
    if fs::metadata(directory.join("manifest.json"))?.len() > 32 * 1024 {
        return Err(error("FINANCE_SNAPSHOT_MANIFEST_INVALID"));
    }
    let manifest: FinanceBackupManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)?;
    original_initialization(root, &manifest)?;
    let result = restore_stopped_finance(root, &installation, &accounts, &directory)?;
    Ok(reply(
        &installation,
        operation,
        None,
        json!({"snapshot_id":snapshot,
        "scope":"whole_installation","accounts":result.accounts,"campaigns":result.campaigns,
        "reconcile_required":true,"source_readiness":"NOT_INFERRED"}),
    ))
}

#[cfg(test)]
#[path = "native_backup_tests.rs"]
mod tests;
