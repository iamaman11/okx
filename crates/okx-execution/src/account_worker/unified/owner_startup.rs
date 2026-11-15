//! Startup of the SAME bounded SQL owner. No import on the new Native path.
use crate::finance_owner_gate::{self, AuthorityModeError, AuthorityModeLease};
use crate::{AccountStore, NewFinanceInstallation, StoreError, StoreResult};
use rusqlite::{Connection, OpenFlags};
use std::io::Read;
use std::{
    fs::{self, File, OpenOptions},
    path::Path,
    time::Duration,
};

pub(super) enum Startup {
    Initialize(NewFinanceInstallation),
    Native,
    // Historical offline fixtures; never selected as a new Native fallback.
    StoppedImport {
        accounts: Vec<String>,
        campaigns: Vec<String>,
    },
}
impl Startup {
    fn missing(&self) -> StoreError {
        StoreError::Invalid(if matches!(self, Self::StoppedImport { .. }) {
            "UNIFIED_FINANCE_IMPORT_NOT_FOUND"
        } else {
            "FINANCE_INSTALLATION_NOT_INITIALIZED"
        })
    }
}

fn plain(path: &Path, directory: bool) -> StoreResult<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let linked = metadata.file_type().is_symlink();
    #[cfg(windows)]
    let linked = {
        use std::os::windows::fs::MetadataExt;
        linked || metadata.file_attributes() & 0x400 != 0
    };
    if linked || (directory && !metadata.is_dir()) || (!directory && !metadata.is_file()) {
        return Err(StoreError::Invalid("FINANCE_PATH_TYPE_INVALID"));
    }
    Ok(true)
}

fn lock(path: &Path) -> StoreResult<File> {
    if fs::symlink_metadata(path).is_ok() {
        plain(path, false)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(fs::TryLockError::WouldBlock) => {
            Err(StoreError::Invalid("UNIFIED_FINANCE_WRITER_BUSY"))
        }
        Err(fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

fn no_old_state(root: &Path) -> StoreResult<()> {
    for name in ["accounts", "risk", "installation.json", "account-catalog"] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => return Err(StoreError::Invalid("FINANCE_EXISTING_STATE_NOT_SUPPORTED")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn verify_existing(db: &Connection, installation: &str, mode: &Startup) -> StoreResult<()> {
    match mode {
        Startup::Initialize(request) => {
            let receipt = AccountStore::inspect_finance_installation(db, installation)?;
            if receipt.initialize_request_id != request.initialize_request_id
                || receipt.source_tree != request.source_tree
                || receipt.request_sha256 != request.request_sha256()?
            {
                return Err(StoreError::Invalid("FINANCE_INITIALIZE_CONFLICT"));
            }
        }
        Startup::Native => {
            AccountStore::inspect_finance_installation(db, installation)?;
        }
        Startup::StoppedImport {
            accounts,
            campaigns,
        } => {
            AccountStore::inspect_offline_finance_import(
                db,
                installation,
                &accounts.iter().map(String::as_str).collect::<Vec<_>>(),
                &campaigns.iter().map(String::as_str).collect::<Vec<_>>(),
            )?;
        }
    }
    Ok(())
}

fn verify_file_format(path: &Path) -> StoreResult<()> {
    let mut header = [0u8; 100];
    File::open(path)?
        .read_exact(&mut header)
        .map_err(|_| StoreError::Invalid("FINANCE_FILE_FORMAT_UNKNOWN"))?;
    let application_id = u32::from_be_bytes(header[68..72].try_into().unwrap());
    if &header[..16] != b"SQLite format 3\0" || application_id != 0x4f4b5846 {
        return Err(StoreError::Invalid("FINANCE_FILE_FORMAT_UNKNOWN"));
    }
    Ok(())
}

pub(super) fn open(
    root: &Path,
    installation: &str,
    mode: Startup,
) -> StoreResult<(Connection, File, AuthorityModeLease)> {
    for ancestor in root.ancestors() {
        plain(ancestor, true)?;
    }
    if !plain(root, true)? {
        return Err(StoreError::Invalid("FINANCE_RUNTIME_ROOT_MISSING"));
    }
    let root = root.canonicalize()?;
    let directory = root.join("finance");
    if !plain(&directory, true)? {
        if !matches!(mode, Startup::Initialize(_)) {
            return Err(mode.missing());
        }
        fs::create_dir(&directory)?;
    }
    if !matches!(mode, Startup::StoppedImport { .. }) {
        no_old_state(&root)?;
        for item in fs::read_dir(&directory)? {
            let item = item?;
            if !matches!(
                item.file_name().to_str(),
                Some(
                    "authority-mode.lock"
                        | "finance.sqlite.writer.lock"
                        | "finance.sqlite"
                        | "finance.sqlite-wal"
                        | "finance.sqlite-shm"
                )
            ) {
                return Err(StoreError::Invalid("FINANCE_UNKNOWN_DIRECTORY_ENTRY"));
            }
            plain(&item.path(), false)?;
        }
    }
    let mode_lease = finance_owner_gate::unified_mode_for(&root).map_err(|error| match error {
        AuthorityModeError::Io(error) => StoreError::Io(error),
        AuthorityModeError::Busy => StoreError::Invalid("UNIFIED_FINANCE_WRITER_BUSY"),
        AuthorityModeError::FinancePresent => StoreError::Invalid("UNIFIED_FINANCE_MODE_INVALID"),
    })?;
    let path = directory.join("finance.sqlite");
    let writer = lock(&directory.join("finance.sqlite.writer.lock"))?;
    let exists = plain(&path, false)?;
    for suffix in ["finance.sqlite-wal", "finance.sqlite-shm"] {
        if plain(&directory.join(suffix), false)? && !exists {
            return Err(StoreError::Invalid("FINANCE_ORPHAN_SIDECAR"));
        }
    }
    let creating = !exists && matches!(mode, Startup::Initialize(_));
    if !exists && !creating {
        return Err(mode.missing());
    }
    if creating {
        // No clobber and no SQLite auto-create over an unknown existing file.
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
    }
    if !creating {
        if !matches!(mode, Startup::StoppedImport { .. }) {
            verify_file_format(&path)?;
        }
        // Verify before writable open: do not recover/checkpoint a rejected DB.
        // READ_ONLY observes committed WAL; immutable=1 is deliberately not used.
        let readonly = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        readonly.pragma_update(None, "foreign_keys", "ON")?;
        verify_existing(&readonly, installation, &mode)?;
    }
    let mut db = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    db.busy_timeout(Duration::from_millis(250))?;
    db.pragma_update(None, "foreign_keys", "ON")?;
    match &mode {
        Startup::Initialize(request) if creating => {
            db.pragma_update(None, "synchronous", "FULL")?;
            // Publish only a file-format tag in the main SQLite header BEFORE
            // WAL is enabled; it survives an uncheckpointed initial COMMIT.
            // A tag alone cannot pass schema/identity/account verification.
            db.pragma_update(None, "application_id", 0x4f4b5846_i64)?;
            db.pragma_update(None, "journal_mode", "WAL")?;
            db.pragma_update(None, "synchronous", "FULL")?;
            AccountStore::initialize_finance_installation(&mut db, request)?;
        }
        _ => verify_existing(&db, installation, &mode)?,
    }
    Ok((db, writer, mode_lease))
}
