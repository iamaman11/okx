use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use okx_github::{GitHubClient, OWNER_USER_ID, REPOSITORY_ID};
use okx_protocol::{
    HostControlOperation, HostControlResult as ProtocolControlResult, HostControlStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{HostControlError, HostControlResult, autostart, single_instance::SingleInstanceGuard};

const CONTROLLER_PATH: &str = r"C:\okx-control\okx-host-control.exe";
const STAGED_CONTROLLER_PATH: &str = r"C:\okx-control\update\okx-host-control.exe.staged";
const PENDING_PATH: &str = r"C:\okx-control\update\pending-controller-update.json";
const INSTALLED_PROVENANCE_PATH: &str = r"C:\okx-control\installed-controller.json";
const PENDING_SCHEMA_V1: &str = "okx.host-control.controller-update/v1";
const INSTALLED_SCHEMA_V1: &str = "okx.host-control.installed-controller/v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyPending {
    schema: String,
    repository_id: u64,
    run_id: u64,
    artifact_id: u64,
    source_head_sha: String,
    source_tree: String,
    rust_version: String,
    controller_sha256: String,
    staged_path: String,
    handoff_request_id: Option<String>,
    #[serde(default)]
    activator_pid: Option<u32>,
}

#[derive(Debug, Serialize)]
struct InstalledProvenance {
    schema: String,
    repository_id: u64,
    run_id: u64,
    artifact_id: u64,
    source_head_sha: String,
    source_tree: String,
    rust_version: String,
    controller_sha256: String,
}

pub async fn activate(
    github: &GitHubClient,
    parent_pid: u32,
    handoff_request_id: &str,
) -> HostControlResult<Value> {
    if Path::new(okx_host_launcher::ACTIVE_PATH).exists()
        || Path::new(okx_host_launcher::LAUNCHER_PATH).exists()
    {
        return Err(HostControlError::LegacyBootstrapDisabled);
    }
    if parent_pid == 0 || !valid_request_id(handoff_request_id) {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }

    wait_for_process_exit(parent_pid)?;
    let pending = load_pending()?;
    verify_pending(&pending)?;
    if pending.handoff_request_id.as_deref() != Some(handoff_request_id)
        || pending.activator_pid != Some(std::process::id())
    {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }
    if !durable_handoff_ack(github, handoff_request_id).await? {
        return Err(HostControlError::ControllerUpdateTerminalAckMissing);
    }

    let _guard = SingleInstanceGuard::acquire()?;
    autostart::ensure_legacy_policy_valid()?;
    install_controller(
        Path::new(STAGED_CONTROLLER_PATH),
        Path::new(CONTROLLER_PATH),
        &pending.controller_sha256,
    )?;

    save_json(
        Path::new(INSTALLED_PROVENANCE_PATH),
        &InstalledProvenance {
            schema: INSTALLED_SCHEMA_V1.to_owned(),
            repository_id: pending.repository_id,
            run_id: pending.run_id,
            artifact_id: pending.artifact_id,
            source_head_sha: pending.source_head_sha.clone(),
            source_tree: pending.source_tree.clone(),
            rust_version: pending.rust_version.clone(),
            controller_sha256: pending.controller_sha256.clone(),
        },
    )?;
    fs::remove_file(PENDING_PATH)?;
    drop(_guard);
    let scheduler = autostart::run_legacy_now()?;

    Ok(json!({
        "activated": true,
        "migration_only": true,
        "controller_sha256": pending.controller_sha256,
        "source_head_sha": pending.source_head_sha,
        "source_tree": pending.source_tree,
        "scheduler": scheduler
    }))
}

fn load_pending() -> HostControlResult<LegacyPending> {
    let value: LegacyPending = serde_json::from_slice(&fs::read(PENDING_PATH)?)?;
    if value.schema != PENDING_SCHEMA_V1
        || value.repository_id != REPOSITORY_ID
        || value.run_id == 0
        || value.artifact_id == 0
        || !lower_hex(&value.source_head_sha, 40)
        || !lower_hex(&value.source_tree, 40)
        || !lower_hex(&value.controller_sha256, 64)
        || value.rust_version.trim().is_empty()
        || value.staged_path != STAGED_CONTROLLER_PATH
        || value
            .handoff_request_id
            .as_deref()
            .is_none_or(valid_request_id)
            == false
    {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }
    Ok(value)
}

fn verify_pending(value: &LegacyPending) -> HostControlResult<()> {
    let path = Path::new(STAGED_CONTROLLER_PATH);
    if !path.is_file() {
        return Err(HostControlError::ControllerUpdateNotStaged);
    }
    if sha256_file(path)? != value.controller_sha256 {
        return Err(HostControlError::ControllerUpdateHashMismatch);
    }
    Ok(())
}

async fn durable_handoff_ack(github: &GitHubClient, request_id: &str) -> HostControlResult<bool> {
    let comments = github.issue_comments(12).await?;
    Ok(comments.iter().any(|comment| {
        if comment.user_id != OWNER_USER_ID {
            return false;
        }
        let Ok(result) = serde_json::from_str::<ProtocolControlResult>(&comment.body) else {
            return false;
        };
        result.request_id == request_id
            && result.status == HostControlStatus::Pass
            && result.operation == HostControlOperation::HandoffControllerUpdate
            && result.validate().is_ok()
    }))
}

fn install_controller(staged: &Path, canonical: &Path, expected: &str) -> HostControlResult<()> {
    let parent = canonical
        .parent()
        .ok_or(HostControlError::ControllerUpdateStateInvalid)?;
    fs::create_dir_all(parent)?;
    let new_path = parent.join("okx-host-control.exe.new");
    let backup = parent.join("okx-host-control.exe.previous");
    fs::copy(staged, &new_path)?;
    if sha256_file(&new_path)? != expected {
        let _ = fs::remove_file(&new_path);
        return Err(HostControlError::ControllerUpdateHashMismatch);
    }

    if backup.exists() {
        fs::remove_file(&backup)?;
    }
    if canonical.exists() {
        fs::rename(canonical, &backup)?;
    }
    if let Err(error) = fs::rename(&new_path, canonical) {
        if backup.exists() && !canonical.exists() {
            let _ = fs::rename(&backup, canonical);
        }
        return Err(error.into());
    }
    if sha256_file(canonical)? != expected {
        return Err(HostControlError::ControllerUpdateHashMismatch);
    }
    Ok(())
}

fn save_json<T: Serialize>(path: &Path, value: &T) -> HostControlResult<()> {
    let parent = path
        .parent()
        .ok_or(HostControlError::ControllerUpdateStateInvalid)?;
    fs::create_dir_all(parent)?;
    let temp = PathBuf::from(format!("{}.tmp", path.display()));
    let mut file = File::create(&temp)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    drop(file);
    atomic_replace(&temp, path)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }
    let source_w = wide(source);
    let destination_w = wide(destination);
    let ok = unsafe {
        MoveFileExW(
            source_w.as_ptr(),
            destination_w.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)?;
    Ok(())
}

#[cfg(windows)]
fn wait_for_process_exit(pid: u32) -> HostControlResult<()> {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, WAIT_OBJECT_0},
        System::Threading::{INFINITE, OpenProcess, WaitForSingleObject},
    };
    const PROCESS_SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE_ACCESS, 0, pid) };
    if handle.is_null() {
        return Ok(());
    }
    let result = unsafe { WaitForSingleObject(handle, INFINITE) };
    unsafe { CloseHandle(handle) };
    if result == WAIT_OBJECT_0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(not(windows))]
fn wait_for_process_exit(_pid: u32) -> HostControlResult<()> {
    Err(HostControlError::UnsupportedPlatform)
}

fn sha256_file(path: &Path) -> HostControlResult<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_request_id(value: &str) -> bool {
    value.starts_with("ctl_")
        && (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_bridge_is_permanently_guarded_by_launcher_root() {
        let source = include_str!("legacy_bootstrap.rs");
        assert!(source.contains("okx_host_launcher::ACTIVE_PATH"));
        assert!(source.contains("LegacyBootstrapDisabled"));
        assert!(source.contains("migration_only"));
    }
}
