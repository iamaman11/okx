//! One installation-local OS authority-mode lock, shared by the ORIGINAL
//! account/campaign SQL owners and exclusive to the unified finance writer.
//! This does NOT coordinate with Cloudflare and does NOT renew an owner epoch.
//! A one-time stopped RB09 legacy-finance disable/import is still required.
//!
//! Old mode: multiple account/campaign owners may each hold a shared lease.
//! New mode: the single installation SQLite owner holds the exclusive lease.
//! The locks protect against a simultaneous native start in this code version;
//! the stopped legacy Cloudflare/older-binary transition is an external gate.
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub(crate) enum AuthorityModeError {
    Io(io::Error),
    Busy,
    FinancePresent,
}

impl From<io::Error> for AuthorityModeError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// The filesystem lock itself is the durable process-liveness witness.
/// Dropping this RAII holder always attempts to release it. No retry/sleep,
/// lease timeouts, Cloudflare check, or automatic owner promotion.
pub(crate) struct AuthorityModeLease {
    file: File,
}

impl Drop for AuthorityModeLease {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Recognize the TWO original production layouts; arbitrary standalone
/// source fixtures remain valid for tests and offline source validation.
fn installation_for_legacy(path: &Path) -> Option<PathBuf> {
    match path.file_name()?.to_str()? {
        "account.sqlite" => {
            let account = path.parent()?;
            let accounts = account.parent()?;
            if accounts.file_name()?.to_str()? != "accounts" {
                return None;
            }
            accounts.parent().map(Path::to_path_buf)
        }
        "risk-book.sqlite" => {
            let campaign = path.parent()?;
            let campaigns = campaign.parent()?;
            let risk = campaigns.parent()?;
            if campaigns.file_name()?.to_str()? != "campaigns"
                || risk.file_name()?.to_str()? != "risk"
            {
                return None;
            }
            risk.parent().map(Path::to_path_buf)
        }
        _ => None,
    }
}

/// The existing installation mode lock file; path is canonicalized to avoid
/// acquiring distinct locks via aliases of the same installation directory.
fn gate(root: &Path) -> Result<(PathBuf, File), AuthorityModeError> {
    let canonical = root.canonicalize()?;
    let directory = canonical.join("finance");
    fs::create_dir_all(&directory)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join("authority-mode.lock"))?;
    Ok((directory, file))
}

fn finance_file_present(directory: &Path) -> Result<bool, AuthorityModeError> {
    match fs::symlink_metadata(directory.join("finance.sqlite")) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// A legacy account/campaign writer must not run once finance.sqlite exists,
/// even if it is still OFFLINE_NOT_AUTHORIZED. The old file is only a source
/// for stopped migration, never an independent live writer after import.
pub(crate) fn legacy_mode_for(
    path: &Path,
) -> Result<Option<AuthorityModeLease>, AuthorityModeError> {
    let Some(root) = installation_for_legacy(path) else {
        return Ok(None);
    };
    let (directory, file) = gate(&root)?;
    // Acquire BEFORE checking existence. A unified owner cannot enter in
    // between the check and the legacy connection startup.
    match file.try_lock_shared() {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            return Err(AuthorityModeError::Busy);
        }
        Err(error) => return Err(error.into()),
    }
    if finance_file_present(&directory)? {
        let _ = file.unlock();
        return Err(AuthorityModeError::FinancePresent);
    }
    Ok(Some(AuthorityModeLease { file }))
}

/// Only the ONE new Rust writer may hold this exclusive installation lease.
/// The existing exact-path `finance.sqlite.writer.lock` is an independent
/// defense against two openers for the same SQLite, not a second authority.
/// The old per-account/per-campaign writer cannot overlap this mode.
pub(crate) fn unified_mode_for(
    root: &Path,
) -> Result<AuthorityModeLease, AuthorityModeError> {
    let (_directory, file) = gate(root)?;
    match file.try_lock() {
        Ok(()) => Ok(AuthorityModeLease { file }),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            Err(AuthorityModeError::Busy)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn one_installation_native_mode_is_exclusive_no_sleep_no_retry() {
        let name = format!(
            "okx-510-native-mode-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let root = std::env::temp_dir().join(name);
        let account = root.join("accounts").join("a".repeat(64)).join("account.sqlite");
        let campaign = root
            .join("risk")
            .join("campaigns")
            .join("b".repeat(64))
            .join("risk-book.sqlite");
        fs::create_dir_all(account.parent().unwrap()).unwrap();
        fs::create_dir_all(campaign.parent().unwrap()).unwrap();
        let first = legacy_mode_for(&account).unwrap().unwrap();
        let second = legacy_mode_for(&campaign).unwrap().unwrap();
        assert!(matches!(
            unified_mode_for(&root),
            Err(AuthorityModeError::Busy)
        ));
        drop(first);
        assert!(matches!(
            unified_mode_for(&root),
            Err(AuthorityModeError::Busy),
            "the other old campaign must still hold its own shared lease"
        ));
        drop(second);
        let owner = unified_mode_for(&root).unwrap();
        assert!(matches!(
            legacy_mode_for(&account),
            Err(AuthorityModeError::Busy)
        ));
        assert!(matches!(
            legacy_mode_for(&campaign),
            Err(AuthorityModeError::Busy)
        ));
        assert!(matches!(
            unified_mode_for(&root),
            Err(AuthorityModeError::Busy)
        ));
        drop(owner);
        let old = legacy_mode_for(&account).unwrap().unwrap();
        drop(old);
        // Once import created the new finance authority file, the OLD
        // writer never restarts, even if nobody currently holds the lock.
        fs::write(root.join("finance").join("finance.sqlite"), b"not-authorized").unwrap();
        assert!(matches!(
            legacy_mode_for(&account),
            Err(AuthorityModeError::FinancePresent)
        ));
        assert!(matches!(
            legacy_mode_for(&campaign),
            Err(AuthorityModeError::FinancePresent)
        ));
        // An offline import still needs its own immutable checksum check;
        // an exclusive MODE lease alone is never authority to trade.
        let owner = unified_mode_for(&root).unwrap();
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn os_kill_of_legacy_owner_releases_native_installation_mode() {
        use std::io::{BufRead, BufReader, Write};
        use std::process::{Command, Stdio};
        use std::sync::mpsc;
        use std::time::Duration;

        // The subprocess is this exact Rust test. It parks only AFTER the
        // real native shared lock is acquired, until the parent OS-kills it.
        if let Some(root) = std::env::var_os("OKX_510_NATIVE_MODE_CHILD_ROOT") {
            let path = PathBuf::from(root)
                .join("accounts")
                .join("a".repeat(64))
                .join("account.sqlite");
            let _lease = legacy_mode_for(&path).unwrap().unwrap();
            println!("OKX_510_NATIVE_MODE_CHILD_LOCKED");
            std::io::stdout().flush().unwrap();
            loop {
                std::thread::park();
            }
        }

        let root = std::env::temp_dir().join(format!(
            "okx-510-native-mode-os-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let path = root
            .join("accounts")
            .join("a".repeat(64))
            .join("account.sqlite");
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("finance_owner_gate::tests::os_kill_of_legacy_owner_releases_native_installation_mode")
            .arg("--nocapture")
            .env("OKX_510_NATIVE_MODE_CHILD_ROOT", &root)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap_or_default().contains("OKX_510_NATIVE_MODE_CHILD_LOCKED") {
                    let _ = ready_tx.send(());
                    return;
                }
            }
        });
        let ready = ready_rx.recv_timeout(Duration::from_secs(15));
        if ready.is_ok() {
            assert!(matches!(
                unified_mode_for(&root),
                Err(AuthorityModeError::Busy),
                "separate live OS process holds the original shared lease"
            ));
        }
        child.kill().unwrap();
        let status = child.wait().unwrap();
        reader.join().unwrap();
        assert!(ready.is_ok(), "child did not acquire real OS lease");
        assert!(!status.success(), "child was physically OS-killed");
        let unified = unified_mode_for(&root).unwrap();
        drop(unified);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn noninstallation_sql_fixture_cannot_mint_mode_or_finance_permission() {
        let arbitrary = std::env::temp_dir().join("standalone-finance-source.sqlite");
        assert!(legacy_mode_for(&arbitrary).unwrap().is_none());
    }
}
