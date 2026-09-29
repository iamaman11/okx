use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use okx_github::{GitHubClient, OWNER_USER_ID, REPOSITORY_ID};
use okx_protocol::{
    HostControlOperation, HostControlResult as ProtocolControlResult, HostControlStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{HostControlError, HostControlResult, autostart};

const ROOT_MIGRATION_PATH: &str = r"C:\okx-control\root-migration.json";
const ROOT_MIGRATION_SCHEMA_V1: &str = "okx.host-control.root-migration/v1";
#[cfg(windows)]
const ROOT_MIGRATION_MUTEX: &str = r"Local\iamaman11-okx-root-migration";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessIdentity {
    pid: u32,
    creation_time_100ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootMigration {
    schema: String,
    repository_id: u64,
    request_id: String,
    parent: ProcessIdentity,
    controller_sha256: String,
    launcher_sha256: String,
    terminal_ack: bool,
}

impl RootMigration {
    fn validate(&self) -> HostControlResult<()> {
        if self.schema != ROOT_MIGRATION_SCHEMA_V1
            || self.repository_id != REPOSITORY_ID
            || !valid_request_id(&self.request_id)
            || self.parent.pid == 0
            || self.parent.creation_time_100ns == 0
            || !lower_hex(&self.controller_sha256, 64)
            || !lower_hex(&self.launcher_sha256, 64)
        {
            return Err(HostControlError::ControllerUpdateStateInvalid);
        }
        Ok(())
    }
}

pub fn prepare(
    request_id: &str,
    controller_sha256: &str,
    launcher_sha256: &str,
) -> HostControlResult<Value> {
    if !valid_request_id(request_id)
        || !lower_hex(controller_sha256, 64)
        || !lower_hex(launcher_sha256, 64)
    {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }
    let parent = current_process_identity()?;
    let next = RootMigration {
        schema: ROOT_MIGRATION_SCHEMA_V1.to_owned(),
        repository_id: REPOSITORY_ID,
        request_id: request_id.to_owned(),
        parent,
        controller_sha256: controller_sha256.to_owned(),
        launcher_sha256: launcher_sha256.to_owned(),
        terminal_ack: false,
    };

    if let Some(existing) = load()? {
        if existing.request_id == next.request_id
            && existing.controller_sha256 == next.controller_sha256
            && existing.launcher_sha256 == next.launcher_sha256
        {
            return Ok(json!({
                "prepared": true,
                "disposition": "EXISTING",
                "request_id": existing.request_id,
                "terminal_ack": existing.terminal_ack
            }));
        }
        return Err(HostControlError::ControllerUpdateConflict);
    }

    save(&next)?;
    Ok(json!({
        "prepared": true,
        "disposition": "CREATED",
        "request_id": next.request_id,
        "parent_pid": next.parent.pid,
        "parent_creation_time_100ns": next.parent.creation_time_100ns,
        "terminal_ack": false
    }))
}

pub fn mark_terminal_ack(request_id: &str) -> HostControlResult<Value> {
    let mut migration = load()?.ok_or(HostControlError::ControllerUpdateNotStaged)?;
    if migration.request_id != request_id {
        return Err(HostControlError::ControllerUpdateConflict);
    }
    if !migration.terminal_ack {
        migration.terminal_ack = true;
        save(&migration)?;
    }
    Ok(json!({
        "request_id": request_id,
        "terminal_ack": true
    }))
}

pub async fn recover_terminal_ack(github: &GitHubClient) -> HostControlResult<bool> {
    let Some(migration) = load()? else {
        return Ok(false);
    };
    if migration.terminal_ack {
        return Ok(true);
    }

    let comments = github.issue_comments(12).await?;
    if comments.iter().any(|comment| {
        durable_ack_body(
            comment.user_id,
            &comment.body,
            &migration.request_id,
        )
    }) {
        mark_terminal_ack(&migration.request_id)?;
        return Ok(true);
    }
    Ok(false)
}

pub fn spawn_migrator() -> HostControlResult<Value> {
    let migration = load()?.ok_or(HostControlError::ControllerUpdateNotStaged)?;
    if !migration.terminal_ack {
        return Err(HostControlError::ControllerUpdateTerminalAckMissing);
    }

    let root = Path::new(r"C:\okx-control");
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("root-migration.stdout.log"))?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("root-migration.stderr.log"))?;
    let child = Command::new(std::env::current_exe()?)
        .arg("migrate-launcher-root")
        .arg("--request-id")
        .arg(&migration.request_id)
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()?;

    Ok(json!({
        "spawned": true,
        "migration_pid": child.id(),
        "request_id": migration.request_id
    }))
}

pub fn activate(request_id: &str) -> HostControlResult<Value> {
    let _guard = RootMigrationGuard::acquire()?;
    let migration = load()?.ok_or(HostControlError::ControllerUpdateNotStaged)?;
    migration.validate()?;
    if migration.request_id != request_id || !migration.terminal_ack {
        return Err(HostControlError::ControllerUpdateStateInvalid);
    }

    wait_for_exact_process_exit(migration.parent)?;

    let launcher = Path::new(okx_host_launcher::LAUNCHER_PATH);
    if !launcher.is_file()
        || okx_host_launcher::sha256_file(launcher)? != migration.launcher_sha256
    {
        return Err(HostControlError::ControllerUpdateHashMismatch);
    }

    let status = autostart::status_value()?;
    let canonical = status
        .get("policy_valid")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !canonical {
        autostart::ensure_legacy_policy_valid()?;
        autostart::install()?;
        autostart::ensure_policy_valid()?;
    }

    if Path::new(ROOT_MIGRATION_PATH).exists() {
        fs::remove_file(ROOT_MIGRATION_PATH)?;
    }
    let scheduler = autostart::run_now()?;

    Ok(json!({
        "migrated": true,
        "request_id": request_id,
        "launcher_sha256": migration.launcher_sha256,
        "controller_sha256": migration.controller_sha256,
        "scheduler": scheduler
    }))
}

pub fn pending_value() -> Value {
    match load() {
        Ok(Some(value)) => json!({
            "state": "PENDING",
            "request_id": value.request_id,
            "terminal_ack": value.terminal_ack,
            "parent_pid": value.parent.pid,
            "parent_creation_time_100ns": value.parent.creation_time_100ns,
            "controller_sha256": value.controller_sha256,
            "launcher_sha256": value.launcher_sha256
        }),
        Ok(None) => json!({"state":"NONE"}),
        Err(error) => json!({"state":"INVALID","error":error.to_string()}),
    }
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
        && matches!(
            result.operation,
            HostControlOperation::InstallLauncherRoot { .. }
        )
        && result.validate().is_ok()
}

fn load() -> HostControlResult<Option<RootMigration>> {
    let path = Path::new(ROOT_MIGRATION_PATH);
    if !path.exists() {
        return Ok(None);
    }
    let value: RootMigration = serde_json::from_slice(&fs::read(path)?)?;
    value.validate()?;
    Ok(Some(value))
}

fn save(value: &RootMigration) -> HostControlResult<()> {
    value.validate()?;
    let path = Path::new(ROOT_MIGRATION_PATH);
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
fn current_process_identity() -> HostControlResult<ProcessIdentity> {
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::Threading::{GetCurrentProcessId, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION},
    };
    let pid = unsafe { GetCurrentProcessId() };
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        return Err(std::io::Error::last_os_error().into());
    }
    let creation = process_creation_time(handle);
    unsafe { CloseHandle(handle) };
    Ok(ProcessIdentity {
        pid,
        creation_time_100ns: creation?,
    })
}

#[cfg(not(windows))]
fn current_process_identity() -> HostControlResult<ProcessIdentity> {
    Err(HostControlError::UnsupportedPlatform)
}

#[cfg(windows)]
fn process_creation_time(
    handle: windows_sys::Win32::Foundation::HANDLE,
) -> HostControlResult<u64> {
    use windows_sys::Win32::{
        Foundation::FILETIME,
        System::Threading::GetProcessTimes,
    };
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    let ok = unsafe {
        GetProcessTimes(
            handle,
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(((creation.dwHighDateTime as u64) << 32) | creation.dwLowDateTime as u64)
}

#[cfg(windows)]
fn wait_for_exact_process_exit(identity: ProcessIdentity) -> HostControlResult<()> {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, WAIT_OBJECT_0},
        System::Threading::{
            INFINITE, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, SYNCHRONIZE,
            WaitForSingleObject,
        },
    };
    let handle = unsafe {
        OpenProcess(
            SYNCHRONIZE | PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            identity.pid,
        )
    };
    if handle.is_null() {
        return Ok(());
    }
    let creation = process_creation_time(handle)?;
    if creation != identity.creation_time_100ns {
        unsafe { CloseHandle(handle) };
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
fn wait_for_exact_process_exit(_identity: ProcessIdentity) -> HostControlResult<()> {
    Err(HostControlError::UnsupportedPlatform)
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

fn valid_request_id(value: &str) -> bool {
    value.starts_with("ctl_")
        && (16..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn lower_hex(value: &str, len: usize) -> bool {
    value.len() == len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(windows)]
struct RootMigrationGuard {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl RootMigrationGuard {
    fn acquire() -> HostControlResult<Self> {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::{
            Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError},
            System::Threading::CreateMutexW,
        };
        let name: Vec<u16> = std::ffi::OsStr::new(ROOT_MIGRATION_MUTEX)
            .encode_wide()
            .chain(Some(0))
            .collect();
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error().into());
        }
        if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
            unsafe { CloseHandle(handle) };
            return Err(HostControlError::ControllerUpdateConflict);
        }
        Ok(Self { handle })
    }
}

#[cfg(windows)]
impl Drop for RootMigrationGuard {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
        }
    }
}

#[cfg(not(windows))]
struct RootMigrationGuard;

#[cfg(not(windows))]
impl RootMigrationGuard {
    fn acquire() -> HostControlResult<Self> {
        Err(HostControlError::UnsupportedPlatform)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_state_is_fixed_outside_workspace() {
        assert!(Path::new(ROOT_MIGRATION_PATH).starts_with(r"C:\okx-control"));
        assert!(!Path::new(ROOT_MIGRATION_PATH).starts_with(r"C:\okx\"));
    }

    #[test]
    fn durable_ack_requires_exact_install_root_operation() {
        let request_id = "ctl_root_migration_012345";
        let pass = ProtocolControlResult {
            schema: okx_protocol::HOST_CONTROL_RESULT_SCHEMA_V1.to_owned(),
            request_id: request_id.to_owned(),
            operation: HostControlOperation::InstallLauncherRoot {
                run_id: 1,
                artifact_id: 2,
                expected_source_tree: "a".repeat(40),
            },
            status: HostControlStatus::Pass,
            observed_at: "2026-09-29T20:00:00.000Z".to_owned(),
            details: Some(json!({"root_materialized": true})),
            failure: None,
        };
        let body = serde_json::to_string(&pass).expect("serialize");
        assert!(durable_ack_body(OWNER_USER_ID, &body, request_id));

        let mut wrong = pass;
        wrong.operation = HostControlOperation::HandoffControllerUpdate;
        let body = serde_json::to_string(&wrong).expect("serialize");
        assert!(!durable_ack_body(OWNER_USER_ID, &body, request_id));
    }
}
