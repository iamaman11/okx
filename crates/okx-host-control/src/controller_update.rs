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

use crate::{HostControlError, HostControlResult, autostart};

pub const CONTROLLER_PATH: &str = r"C:\okx-control\okx-host-control.exe";
pub const UPDATE_ROOT: &str = r"C:\okx-control\update";
pub const STAGED_CONTROLLER_PATH: &str = r"C:\okx-control\update\okx-host-control.exe.staged";
const PENDING_PATH: &str = r"C:\okx-control\update\pending-controller-update.json";
const INSTALLED_PROVENANCE_PATH: &str = r"C:\okx-control\installed-controller.json";
const PENDING_SCHEMA_V1: &str = "okx.host-control.controller-update/v1";
const INSTALLED_SCHEMA_V1: &str = "okx.host-control.installed-controller/v1";

#[derive(Debug, Clone)]
pub struct ControllerUpdateCandidate {
    pub run_id: u64,
    pub artifact_id: u64,
    pub source_head_sha: String,
    pub source_tree: String,
    pub rust_version: String,
    pub controller_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingControllerUpdate {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledControllerProvenance {
    schema: String,
    repository_id: u64,
    run_id: u64,
    artifact_id: u64,
    source_head_sha: String,
    source_tree: String,
    rust_version: String,
    controller_sha256: String,
}

impl PendingControllerUpdate {
    fn from_candidate(candidate: ControllerUpdateCandidate) -> Self {
        Self {
            schema: PENDING_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            run_id: candidate.run_id,
            artifact_id: candidate.artifact_id,
            source_head_sha: candidate.source_head_sha,
            source_tree: candidate.source_tree,
            rust_version: candidate.rust_version,
            controller_sha256: candidate.controller_sha256,
            staged_path: STAGED_CONTROLLER_PATH.to_owned(),
            handoff_request_id: None,
        }
    }

    fn valid(&self) -> bool {
        self.schema == PENDING_SCHEMA_V1
            && self.repository_id == REPOSITORY_ID
            && self.run_id != 0
            && self.artifact_id != 0
            && lower_hex(&self.source_head_sha, 40)
            && lower_hex(&self.source_tree, 40)
            && lower_hex(&self.controller_sha256, 64)
            && !self.rust_version.trim().is_empty()
            && self.staged_path == STAGED_CONTROLLER_PATH
            && self
                .handoff_request_id
                .as_deref()
                .is_none_or(valid_control_request_id)
    }

    fn installed(&self) -> InstalledControllerProvenance {
        InstalledControllerProvenance {
            schema: INSTALLED_SCHEMA_V1.to_owned(),
            repository_id: self.repository_id,
            run_id: self.run_id,
            artifact_id: self.artifact_id,
            source_head_sha: self.source_head_sha.clone(),
            source_tree: self.source_tree.clone(),
            rust_version: self.rust_version.clone(),
            controller_sha256: self.controller_sha256.clone(),
        }
    }
}

impl InstalledControllerProvenance {
    fn valid(&self) -> bool {
        self.schema == INSTALLED_SCHEMA_V1
            && self.repository_id == REPOSITORY_ID
            && self.run_id != 0
            && self.artifact_id != 0
            && lower_hex(&self.source_head_sha, 40)
            && lower_hex(&self.source_tree, 40)
            && lower_hex(&self.controller_sha256, 64)
            && !self.rust_version.trim().is_empty()
    }
}

pub fn stage(candidate: ControllerUpdateCandidate, bytes: &[u8]) -> HostControlResult<Value> {
    if sha256_bytes(bytes) != candidate.controller_sha256 {
        return Err(HostControlError::ArtifactHashMismatch);
    }

    let pending = PendingControllerUpdate::from_candidate(candidate);
    if let Some(existing) = load_pending()? {
        verify_staged(&existing)?;
        if same_candidate(&existing, &pending) {
            return Ok(json!({
                "staged": true,
                "disposition": "EXISTING",
                "run_id": existing.run_id,
                "artifact_id": existing.artifact_id,
                "source_head_sha": existing.source_head_sha,
                "source_tree": existing.source_tree,
                "controller_sha256": existing.controller_sha256,
                "handoff_request_id": existing.handoff_request_id
            }));
        }
        return Err(HostControlError::ControllerUpdateConflict);
    }

    fs::create_dir_all(UPDATE_ROOT)?;
    let staged = PathBuf::from(STAGED_CONTROLLER_PATH);
    let temp = PathBuf::from(format!("{STAGED_CONTROLLER_PATH}.tmp"));
    fs::write(&temp, bytes)?;
    replace_file(&temp, &staged)?;

    if sha256_file(&staged)? != pending.controller_sha256 {
        return Err(HostControlError::ControllerUpdateHashMismatch);
    }

    save_json(Path::new(PENDING_PATH), &pending)?;

    Ok(json!({
        "staged": true,
        "disposition": "CREATED",
        "run_id": pending.run_id,
        "artifact_id": pending.artifact_id,
        "source_head_sha": pending.source_head_sha,
        "source_tree": pending.source_tree,
        "controller_sha256": pending.controller_sha256
    }))
}

pub fn prepare_handoff(request_id: &str) -> HostControlResult<Value> {
    if !valid_control_request_id(request_id) {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }

    let mut pending = load_pending()?.ok_or(HostControlError::ControllerUpdateNotStaged)?;
    verify_staged(&pending)?;
    if pending
        .handoff_request_id
        .as_deref()
        .is_some_and(|stored| stored != request_id)
    {
        return Err(HostControlError::ControllerUpdateConflict);
    }
    pending.handoff_request_id = Some(request_id.to_owned());
    save_json(Path::new(PENDING_PATH), &pending)?;

    let task = match autostart::install_controller_update_activation() {
        Ok(task) => task,
        Err(error) => {
            pending.handoff_request_id = None;
            save_json(Path::new(PENDING_PATH), &pending)?;
            return Err(error);
        }
    };
    Ok(json!({
        "handoff_prepared": true,
        "activation": public_pending(&pending),
        "scheduler": task
    }))
}

pub async fn activate(github: &GitHubClient) -> HostControlResult<Value> {
    let mut pending = load_pending()?.ok_or(HostControlError::ControllerUpdateNotStaged)?;
    verify_staged(&pending)?;
    let request_id = pending
        .handoff_request_id
        .as_deref()
        .ok_or(HostControlError::ControllerUpdateStateInvalid)?
        .to_owned();

    if !durable_handoff_ack(github, &request_id).await? {
        autostart::restore_controller_action()?;
        pending.handoff_request_id = None;
        save_json(Path::new(PENDING_PATH), &pending)?;
        return Err(HostControlError::ControllerUpdateTerminalAckMissing);
    }

    let staged = Path::new(STAGED_CONTROLLER_PATH);
    let canonical = Path::new(CONTROLLER_PATH);
    if canonical
        .is_file()
        .then(|| sha256_file(canonical))
        .transpose()?
        .as_deref()
        != Some(pending.controller_sha256.as_str())
    {
        install_controller(staged, canonical, &pending.controller_sha256)?;
    }

    save_json(Path::new(INSTALLED_PROVENANCE_PATH), &pending.installed())?;

    let scheduler = autostart::restore_controller_action()?;
    fs::remove_file(PENDING_PATH)?;

    Ok(json!({
        "activated": true,
        "controller_sha256": pending.controller_sha256,
        "source_head_sha": pending.source_head_sha,
        "source_tree": pending.source_tree,
        "scheduler": scheduler
    }))
}

pub fn abort() -> HostControlResult<Value> {
    autostart::ensure_policy_valid()?;

    let Some(pending) = load_pending()? else {
        return Ok(json!({
            "aborted": false,
            "disposition": "NO_PENDING_UPDATE"
        }));
    };

    if Path::new(PENDING_PATH).exists() {
        fs::remove_file(PENDING_PATH)?;
    }
    if Path::new(STAGED_CONTROLLER_PATH).exists() {
        fs::remove_file(STAGED_CONTROLLER_PATH)?;
    }

    Ok(json!({
        "aborted": true,
        "disposition": "PENDING_UPDATE_CLEARED",
        "run_id": pending.run_id,
        "artifact_id": pending.artifact_id,
        "source_head_sha": pending.source_head_sha,
        "source_tree": pending.source_tree,
        "controller_sha256": pending.controller_sha256,
        "handoff_request_id": pending.handoff_request_id
    }))
}

pub fn status_value() -> Value {
    let pending = match load_pending() {
        Ok(Some(value)) => match verify_staged(&value) {
            Ok(()) => json!({
                "state": "STAGED",
                "reason": "HASH_MATCH",
                "update": public_pending(&value)
            }),
            Err(error) => json!({
                "state": "MISMATCH",
                "reason": error.code(),
                "diagnostic": error.to_string()
            }),
        },
        Ok(None) => json!({"state":"NONE","reason":"NO_PENDING_UPDATE"}),
        Err(error) => json!({
            "state":"UNKNOWN",
            "reason":error.code(),
            "diagnostic":error.to_string()
        }),
    };

    json!({
        "installed_controller": installed_status(),
        "pending_update": pending
    })
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

fn verify_staged(value: &PendingControllerUpdate) -> HostControlResult<()> {
    if !value.valid() {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }
    let path = Path::new(STAGED_CONTROLLER_PATH);
    if !path.is_file() {
        return Err(HostControlError::ControllerUpdateNotStaged);
    }
    if sha256_file(path)? != value.controller_sha256 {
        return Err(HostControlError::ControllerUpdateHashMismatch);
    }
    Ok(())
}

fn load_pending() -> HostControlResult<Option<PendingControllerUpdate>> {
    let path = Path::new(PENDING_PATH);
    if !path.exists() {
        return Ok(None);
    }
    let value: PendingControllerUpdate = serde_json::from_slice(&fs::read(path)?)?;
    if !value.valid() {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }
    Ok(Some(value))
}

fn installed_status() -> Value {
    let path = Path::new(INSTALLED_PROVENANCE_PATH);
    let binary = Path::new(CONTROLLER_PATH);
    if !path.exists() {
        return json!({
            "state": "UNKNOWN",
            "reason": "PROVENANCE_MISSING",
            "binary_present": binary.is_file()
        });
    }

    let result: HostControlResult<InstalledControllerProvenance> = (|| {
        let value: InstalledControllerProvenance = serde_json::from_slice(&fs::read(path)?)?;
        if !value.valid() {
            return Err(HostControlError::ControllerUpdateStateInvalid);
        }
        Ok(value)
    })();

    match result {
        Ok(record) if binary.is_file() => match sha256_file(binary) {
            Ok(hash) => json!({
                "state": if hash == record.controller_sha256 { "VERIFIED" } else { "MISMATCH" },
                "reason": if hash == record.controller_sha256 { "HASH_MATCH" } else { "BINARY_HASH_MISMATCH" },
                "binary_sha256": hash,
                "provenance": record
            }),
            Err(error) => json!({
                "state":"UNKNOWN",
                "reason":error.code(),
                "diagnostic":error.to_string()
            }),
        },
        Ok(record) => json!({
            "state":"MISMATCH",
            "reason":"BINARY_MISSING",
            "provenance":record
        }),
        Err(error) => json!({
            "state":"UNKNOWN",
            "reason":error.code(),
            "diagnostic":error.to_string()
        }),
    }
}

fn public_pending(value: &PendingControllerUpdate) -> Value {
    json!({
        "run_id": value.run_id,
        "artifact_id": value.artifact_id,
        "source_head_sha": value.source_head_sha,
        "source_tree": value.source_tree,
        "rust_version": value.rust_version,
        "controller_sha256": value.controller_sha256,
        "handoff_request_id": value.handoff_request_id
    })
}

async fn durable_handoff_ack(github: &GitHubClient, request_id: &str) -> HostControlResult<bool> {
    let comments = github.issue_comments(12).await?;
    Ok(comments
        .iter()
        .any(|comment| durable_ack_body(comment.user_id, &comment.body, request_id)))
}

fn durable_ack_body(user_id: u64, body: &str, request_id: &str) -> bool {
    if user_id != OWNER_USER_ID {
        return false;
    }
    let Ok(result) = serde_json::from_str::<ProtocolControlResult>(body) else {
        return false;
    };
    result.request_id == request_id
        && result.status == HostControlStatus::Pass
        && result.operation == HostControlOperation::HandoffControllerUpdate
        && result.validate().is_ok()
}

fn same_candidate(left: &PendingControllerUpdate, right: &PendingControllerUpdate) -> bool {
    left.repository_id == right.repository_id
        && left.run_id == right.run_id
        && left.artifact_id == right.artifact_id
        && left.source_head_sha == right.source_head_sha
        && left.source_tree == right.source_tree
        && left.rust_version == right.rust_version
        && left.controller_sha256 == right.controller_sha256
}

fn valid_control_request_id(value: &str) -> bool {
    value.starts_with("ctl_")
        && (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
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

fn replace_file(source: &Path, destination: &Path) -> HostControlResult<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)?;
    Ok(())
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
    replace_file(source, destination)
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
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
    Ok(hex(&digest.finalize()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updater_paths_are_fixed_outside_mutable_repo() {
        assert!(Path::new(STAGED_CONTROLLER_PATH).starts_with(r"C:\okx-control"));
        assert!(!Path::new(STAGED_CONTROLLER_PATH).starts_with(r"C:\okx\"));
    }

    #[test]
    fn durable_ack_requires_owner_pass_and_exact_handoff_operation() {
        let request_id = "ctl_controller_update_0123456789";
        let pass = ProtocolControlResult {
            schema: okx_protocol::HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            operation: HostControlOperation::HandoffControllerUpdate,
            status: HostControlStatus::Pass,
            observed_at: "2026-09-29T12:00:00.000Z".to_owned(),
            details: Some(json!({"handoff_prepared": true})),
            failure: None,
        };
        let pass_json = serde_json::to_string(&pass).expect("serialize");
        assert!(durable_ack_body(OWNER_USER_ID, &pass_json, request_id));
        assert!(!durable_ack_body(OWNER_USER_ID + 1, &pass_json, request_id));

        let mut wrong_operation = pass.clone();
        wrong_operation.operation = HostControlOperation::Status;
        let wrong_json = serde_json::to_string(&wrong_operation).expect("serialize");
        assert!(!durable_ack_body(OWNER_USER_ID, &wrong_json, request_id));

        let mut failed = pass;
        failed.status = HostControlStatus::Fail;
        let failed_json = serde_json::to_string(&failed).expect("serialize");
        assert!(!durable_ack_body(OWNER_USER_ID, &failed_json, request_id));
    }

    #[test]
    fn candidate_produces_valid_pending_state() {
        let pending = PendingControllerUpdate::from_candidate(ControllerUpdateCandidate {
            run_id: 1,
            artifact_id: 2,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            rust_version: "rustc 1.95.0".to_owned(),
            controller_sha256: "c".repeat(64),
        });
        assert!(pending.valid());
    }
}
