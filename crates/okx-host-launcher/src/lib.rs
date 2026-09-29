use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const REPOSITORY_ID: u64 = 1_388_071_566;
pub const CONTROL_ROOT: &str = r"C:\okx-control";
pub const LAUNCHER_PATH: &str = r"C:\okx-control\okx-host-launcher.exe";
pub const VERSIONS_ROOT: &str = r"C:\okx-control\versions";
pub const ACTIVE_PATH: &str = r"C:\okx-control\active-controller.json";
pub const STAGED_PATH: &str = r"C:\okx-control\staged-controller.json";
pub const PENDING_PATH: &str = r"C:\okx-control\activation\pending.json";
pub const READY_PATH: &str = r"C:\okx-control\activation\ready.json";
pub const LAST_RESULT_PATH: &str = r"C:\okx-control\activation\last-result.json";

const ACTIVE_SCHEMA_V1: &str = "okx.host-launcher.active/v1";
const STAGED_SCHEMA_V1: &str = "okx.host-launcher.staged/v1";
const ACTIVATION_SCHEMA_V1: &str = "okx.host-launcher.activation/v1";
const READY_SCHEMA_V1: &str = "okx.host-launcher.ready/v1";
const RESULT_SCHEMA_V1: &str = "okx.host-launcher.activation-result/v1";
const CONTROLLER_MUTEX: &str = r"Local\iamaman11-okx-host-control";
const LAUNCHER_MUTEX: &str = r"Local\iamaman11-okx-host-launcher";

#[derive(Debug, Error)]
pub enum LauncherError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("launcher state is invalid")]
    InvalidState,
    #[error("launcher root is not installed")]
    RootNotInstalled,
    #[error("launcher root already exists with different immutable content")]
    RootConflict,
    #[error("controller version SHA-256 mismatch")]
    HashMismatch,
    #[error("another controller activation is already pending")]
    ActivationConflict,
    #[error("no staged controller update exists")]
    NotStaged,
    #[error("controller readiness proof is invalid")]
    InvalidReadiness,
    #[error("launcher is already running")]
    AlreadyRunning,
    #[error("Windows launcher runtime is unavailable on this platform")]
    UnsupportedPlatform,
    #[error("controller process launch failed")]
    ControllerLaunch,
}

pub type LauncherResult<T> = Result<T, LauncherError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerVersion {
    pub repository_id: u64,
    pub run_id: u64,
    pub artifact_id: u64,
    pub source_head_sha: String,
    pub source_tree: String,
    pub rust_version: String,
    pub controller_sha256: String,
}

impl ControllerVersion {
    pub fn validate(&self) -> LauncherResult<()> {
        if self.repository_id != REPOSITORY_ID
            || self.run_id == 0
            || self.artifact_id == 0
            || !lower_hex(&self.source_head_sha, 40)
            || !lower_hex(&self.source_tree, 40)
            || !lower_hex(&self.controller_sha256, 64)
            || self.rust_version.trim().is_empty()
        {
            return Err(LauncherError::InvalidState);
        }
        Ok(())
    }

    pub fn binary_path(&self) -> LauncherResult<PathBuf> {
        self.validate()?;
        Ok(Path::new(VERSIONS_ROOT)
            .join(&self.controller_sha256)
            .join("okx-host-control.exe"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveController {
    pub schema: String,
    pub repository_id: u64,
    pub active: ControllerVersion,
    pub previous: Option<ControllerVersion>,
}

impl ActiveController {
    fn new(active: ControllerVersion, previous: Option<ControllerVersion>) -> Self {
        Self {
            schema: ACTIVE_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            active,
            previous,
        }
    }

    fn validate(&self) -> LauncherResult<()> {
        if self.schema != ACTIVE_SCHEMA_V1 || self.repository_id != REPOSITORY_ID {
            return Err(LauncherError::InvalidState);
        }
        self.active.validate()?;
        if let Some(previous) = &self.previous {
            previous.validate()?;
            if previous.controller_sha256 == self.active.controller_sha256 {
                return Err(LauncherError::InvalidState);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StagedController {
    schema: String,
    repository_id: u64,
    candidate: ControllerVersion,
}

impl StagedController {
    fn new(candidate: ControllerVersion) -> Self {
        Self {
            schema: STAGED_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            candidate,
        }
    }

    fn validate(&self) -> LauncherResult<()> {
        if self.schema != STAGED_SCHEMA_V1 || self.repository_id != REPOSITORY_ID {
            return Err(LauncherError::InvalidState);
        }
        self.candidate.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationRecord {
    pub schema: String,
    pub repository_id: u64,
    pub request_id: String,
    pub old_controller_pid: u32,
    pub previous: ControllerVersion,
    pub candidate: ControllerVersion,
    pub terminal_ack: bool,
}

impl ActivationRecord {
    fn validate(&self) -> LauncherResult<()> {
        if self.schema != ACTIVATION_SCHEMA_V1
            || self.repository_id != REPOSITORY_ID
            || !valid_request_id(&self.request_id)
            || self.old_controller_pid == 0
        {
            return Err(LauncherError::InvalidState);
        }
        self.previous.validate()?;
        self.candidate.validate()?;
        if self.previous.controller_sha256 == self.candidate.controller_sha256 {
            return Err(LauncherError::InvalidState);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadyRecord {
    schema: String,
    repository_id: u64,
    request_id: String,
    controller_sha256: String,
    controller_pid: u32,
}

impl ReadyRecord {
    fn validate_for(&self, pending: &ActivationRecord) -> LauncherResult<()> {
        if self.schema != READY_SCHEMA_V1
            || self.repository_id != REPOSITORY_ID
            || self.request_id != pending.request_id
            || self.controller_sha256 != pending.candidate.controller_sha256
            || self.controller_pid == 0
        {
            return Err(LauncherError::InvalidReadiness);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationResult {
    schema: String,
    repository_id: u64,
    request_id: String,
    outcome: String,
    candidate_sha256: String,
    previous_sha256: String,
    reason: String,
}

pub fn install_root(
    launcher_bytes: &[u8],
    launcher_sha256: &str,
    active: ControllerVersion,
    controller_bytes: &[u8],
) -> LauncherResult<Value> {
    active.validate()?;
    if !lower_hex(launcher_sha256, 64) || sha256_bytes(launcher_bytes) != launcher_sha256 {
        return Err(LauncherError::HashMismatch);
    }
    if sha256_bytes(controller_bytes) != active.controller_sha256 {
        return Err(LauncherError::HashMismatch);
    }

    fs::create_dir_all(CONTROL_ROOT)?;
    match Path::new(LAUNCHER_PATH).is_file() {
        true if sha256_file(Path::new(LAUNCHER_PATH))? == launcher_sha256 => {}
        true => return Err(LauncherError::RootConflict),
        false => write_atomic_bytes(Path::new(LAUNCHER_PATH), launcher_bytes)?,
    }

    stage_version(&active, controller_bytes)?;

    match load_active()? {
        Some(existing) if existing.active.controller_sha256 == active.controller_sha256 => {}
        Some(_) => return Err(LauncherError::RootConflict),
        None => save_json(
            Path::new(ACTIVE_PATH),
            &ActiveController::new(active.clone(), None),
        )?,
    }

    Ok(json!({
        "installed": true,
        "launcher_path": LAUNCHER_PATH,
        "launcher_sha256": launcher_sha256,
        "active_controller_sha256": active.controller_sha256,
        "scheduler_action_required": "okx-host-launcher.exe run"
    }))
}

pub fn stage_update(candidate: ControllerVersion, bytes: &[u8]) -> LauncherResult<Value> {
    candidate.validate()?;
    let active = load_active()?.ok_or(LauncherError::RootNotInstalled)?;
    if candidate.controller_sha256 == active.active.controller_sha256 {
        return Err(LauncherError::ActivationConflict);
    }
    if load_pending()?.is_some() {
        return Err(LauncherError::ActivationConflict);
    }
    if sha256_bytes(bytes) != candidate.controller_sha256 {
        return Err(LauncherError::HashMismatch);
    }

    if let Some(existing) = load_staged()? {
        if existing.candidate == candidate {
            verify_version(&candidate)?;
            return Ok(json!({
                "staged": true,
                "disposition": "EXISTING",
                "candidate": candidate
            }));
        }
        return Err(LauncherError::ActivationConflict);
    }

    stage_version(&candidate, bytes)?;
    save_json(
        Path::new(STAGED_PATH),
        &StagedController::new(candidate.clone()),
    )?;
    Ok(json!({
        "staged": true,
        "disposition": "CREATED",
        "candidate": candidate
    }))
}

pub fn prepare_activation(request_id: &str, old_controller_pid: u32) -> LauncherResult<Value> {
    if !valid_request_id(request_id) || old_controller_pid == 0 {
        return Err(LauncherError::InvalidState);
    }

    if let Some(existing) = load_pending()? {
        if existing.request_id == request_id && existing.old_controller_pid == old_controller_pid {
            return Ok(json!({
                "handoff_prepared": true,
                "disposition": "EXISTING",
                "activation": existing
            }));
        }
        return Err(LauncherError::ActivationConflict);
    }

    let active = load_active()?.ok_or(LauncherError::RootNotInstalled)?;
    let staged = load_staged()?.ok_or(LauncherError::NotStaged)?;
    verify_version(&active.active)?;
    verify_version(&staged.candidate)?;
    if active.active.controller_sha256 == staged.candidate.controller_sha256 {
        return Err(LauncherError::ActivationConflict);
    }

    let pending = ActivationRecord {
        schema: ACTIVATION_SCHEMA_V1.to_owned(),
        repository_id: REPOSITORY_ID,
        request_id: request_id.to_owned(),
        old_controller_pid,
        previous: active.active,
        candidate: staged.candidate,
        terminal_ack: false,
    };
    pending.validate()?;
    if Path::new(READY_PATH).exists() {
        fs::remove_file(READY_PATH)?;
    }
    save_json(Path::new(PENDING_PATH), &pending)?;

    Ok(json!({
        "handoff_prepared": true,
        "disposition": "CREATED",
        "activation": pending
    }))
}

pub fn mark_terminal_ack(request_id: &str) -> LauncherResult<Value> {
    let mut pending = load_pending()?.ok_or(LauncherError::NotStaged)?;
    if pending.request_id != request_id {
        return Err(LauncherError::ActivationConflict);
    }
    if !pending.terminal_ack {
        pending.terminal_ack = true;
        save_json(Path::new(PENDING_PATH), &pending)?;
    }
    Ok(json!({
        "request_id": request_id,
        "terminal_ack": true
    }))
}

pub fn pending_activation() -> LauncherResult<Option<ActivationRecord>> {
    load_pending()
}

pub fn abort_update() -> LauncherResult<Value> {
    if let Some(pending) = load_pending()? {
        if pending.terminal_ack {
            return Err(LauncherError::ActivationConflict);
        }
        fs::remove_file(PENDING_PATH)?;
    }
    if Path::new(STAGED_PATH).exists() {
        fs::remove_file(STAGED_PATH)?;
    }
    if Path::new(READY_PATH).exists() {
        fs::remove_file(READY_PATH)?;
    }
    Ok(json!({
        "aborted": true,
        "disposition": "STAGED_OR_UNACKED_ACTIVATION_CLEARED"
    }))
}

pub fn status_value() -> Value {
    let active = load_active()
        .map(|value| {
            value.map_or_else(
                || json!({"state":"NONE"}),
                |v| json!({"state":"READY","value":v}),
            )
        })
        .unwrap_or_else(|error| json!({"state":"INVALID","error":error.to_string()}));
    let staged = load_staged()
        .map(|value| {
            value.map_or_else(
                || json!({"state":"NONE"}),
                |v| json!({"state":"STAGED","value":v}),
            )
        })
        .unwrap_or_else(|error| json!({"state":"INVALID","error":error.to_string()}));
    let pending = load_pending()
        .map(|value| {
            value.map_or_else(
                || json!({"state":"NONE"}),
                |v| json!({"state":"PENDING","value":v}),
            )
        })
        .unwrap_or_else(|error| json!({"state":"INVALID","error":error.to_string()}));
    let last_result = load_optional::<ActivationResult>(Path::new(LAST_RESULT_PATH))
        .map(|value| {
            value.map_or_else(
                || json!({"state":"NONE"}),
                |v| json!({"state":"RECORDED","value":v}),
            )
        })
        .unwrap_or_else(|error| json!({"state":"INVALID","error":error.to_string()}));

    json!({
        "schema": "okx.host-launcher.status/v1",
        "launcher_present": Path::new(LAUNCHER_PATH).is_file(),
        "active": active,
        "staged": staged,
        "pending": pending,
        "last_result": last_result
    })
}

pub fn signal_controller_ready(request_id: &str) -> LauncherResult<Value> {
    let pending = load_pending()?.ok_or(LauncherError::NotStaged)?;
    if !pending.terminal_ack || pending.request_id != request_id {
        return Err(LauncherError::InvalidReadiness);
    }

    let current = std::env::current_exe()?;
    if sha256_file(&current)? != pending.candidate.controller_sha256 {
        return Err(LauncherError::InvalidReadiness);
    }

    let ready = ReadyRecord {
        schema: READY_SCHEMA_V1.to_owned(),
        repository_id: REPOSITORY_ID,
        request_id: request_id.to_owned(),
        controller_sha256: pending.candidate.controller_sha256.clone(),
        controller_pid: std::process::id(),
    };
    save_json(Path::new(READY_PATH), &ready)?;
    signal_ready_event(request_id)?;

    Ok(json!({
        "request_id": request_id,
        "controller_pid": ready.controller_pid,
        "controller_sha256": ready.controller_sha256,
        "ready": true
    }))
}

pub fn run() -> LauncherResult<Value> {
    let _guard = LauncherGuard::acquire()?;
    let active = load_active()?.ok_or(LauncherError::RootNotInstalled)?;
    verify_version(&active.active)?;

    let Some(pending) = load_pending()? else {
        if controller_running()? {
            return Ok(json!({
                "schema": "okx.host-launcher.run/v1",
                "disposition": "CONTROLLER_ALREADY_RUNNING",
                "active_controller_sha256": active.active.controller_sha256
            }));
        }
        let child = spawn_controller(&active.active, None)?;
        return Ok(json!({
            "schema": "okx.host-launcher.run/v1",
            "disposition": "ACTIVE_CONTROLLER_STARTED",
            "controller_pid": child.id(),
            "active_controller_sha256": active.active.controller_sha256
        }));
    };

    pending.validate()?;
    if !pending.terminal_ack {
        if controller_running()? {
            return Ok(json!({
                "schema": "okx.host-launcher.run/v1",
                "disposition": "WAITING_FOR_TERMINAL_ACK",
                "request_id": pending.request_id
            }));
        }
        verify_version(&pending.previous)?;
        let child = spawn_controller(&pending.previous, None)?;
        return Ok(json!({
            "schema": "okx.host-launcher.run/v1",
            "disposition": "PREVIOUS_CONTROLLER_RESTARTED_WAITING_FOR_ACK",
            "controller_pid": child.id(),
            "request_id": pending.request_id
        }));
    }

    if let Some(ready) = load_ready()? {
        if ready.validate_for(&pending).is_ok()
            && process_is_running(ready.controller_pid)?
            && active.active.controller_sha256 == pending.candidate.controller_sha256
        {
            commit_activation(&pending)?;
            return Ok(json!({
                "schema": "okx.host-launcher.run/v1",
                "disposition": "ACTIVATION_COMMITTED_FROM_DURABLE_READY",
                "request_id": pending.request_id,
                "controller_pid": ready.controller_pid,
                "controller_sha256": pending.candidate.controller_sha256
            }));
        }
    }

    if active.active.controller_sha256 == pending.candidate.controller_sha256 {
        if controller_running()? {
            return Ok(json!({
                "schema": "okx.host-launcher.run/v1",
                "disposition": "PROVISIONAL_CONTROLLER_RUNNING_AWAITING_READY",
                "request_id": pending.request_id
            }));
        }
        return rollback_activation(&pending, "PROVISIONAL_CONTROLLER_NOT_READY");
    }

    if active.active.controller_sha256 != pending.previous.controller_sha256 {
        return Err(LauncherError::InvalidState);
    }

    wait_for_process_exit(pending.old_controller_pid)?;
    if controller_running()? {
        return Err(LauncherError::ActivationConflict);
    }

    verify_version(&pending.candidate)?;
    save_json(
        Path::new(ACTIVE_PATH),
        &ActiveController::new(pending.candidate.clone(), Some(pending.previous.clone())),
    )?;
    if Path::new(READY_PATH).exists() {
        fs::remove_file(READY_PATH)?;
    }

    let ready_event = ReadyEvent::create(&pending.request_id)?;
    let mut child = spawn_controller(&pending.candidate, Some(&pending.request_id))?;
    match ready_event.wait_with_child(&mut child)? {
        ReadyWait::Ready => {
            let ready = load_ready()?.ok_or(LauncherError::InvalidReadiness)?;
            ready.validate_for(&pending)?;
            if ready.controller_pid != child.id() || !process_is_running(ready.controller_pid)? {
                return rollback_activation(&pending, "READY_PROCESS_NOT_RUNNING");
            }
            commit_activation(&pending)?;
            Ok(json!({
                "schema": "okx.host-launcher.run/v1",
                "disposition": "ACTIVATION_COMMITTED",
                "request_id": pending.request_id,
                "controller_pid": ready.controller_pid,
                "controller_sha256": pending.candidate.controller_sha256
            }))
        }
        ReadyWait::ChildExited => rollback_activation(&pending, "CANDIDATE_EXITED_BEFORE_READY"),
    }
}

fn rollback_activation(pending: &ActivationRecord, reason: &str) -> LauncherResult<Value> {
    verify_version(&pending.previous)?;
    save_json(
        Path::new(ACTIVE_PATH),
        &ActiveController::new(pending.previous.clone(), None),
    )?;
    save_result(pending, "ROLLED_BACK", reason)?;
    clear_activation_files()?;
    let child = spawn_controller(&pending.previous, None)?;
    Ok(json!({
        "schema": "okx.host-launcher.run/v1",
        "disposition": "ACTIVATION_ROLLED_BACK",
        "reason": reason,
        "request_id": pending.request_id,
        "controller_pid": child.id(),
        "active_controller_sha256": pending.previous.controller_sha256
    }))
}

fn commit_activation(pending: &ActivationRecord) -> LauncherResult<()> {
    save_result(pending, "COMMITTED", "READY_PROOF_ACCEPTED")?;
    clear_activation_files()
}

fn save_result(pending: &ActivationRecord, outcome: &str, reason: &str) -> LauncherResult<()> {
    save_json(
        Path::new(LAST_RESULT_PATH),
        &ActivationResult {
            schema: RESULT_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            request_id: pending.request_id.clone(),
            outcome: outcome.to_owned(),
            candidate_sha256: pending.candidate.controller_sha256.clone(),
            previous_sha256: pending.previous.controller_sha256.clone(),
            reason: reason.to_owned(),
        },
    )
}

fn clear_activation_files() -> LauncherResult<()> {
    for path in [PENDING_PATH, STAGED_PATH, READY_PATH] {
        if Path::new(path).exists() {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn stage_version(version: &ControllerVersion, bytes: &[u8]) -> LauncherResult<()> {
    version.validate()?;
    if sha256_bytes(bytes) != version.controller_sha256 {
        return Err(LauncherError::HashMismatch);
    }
    let path = version.binary_path()?;
    if path.is_file() {
        return if sha256_file(&path)? == version.controller_sha256 {
            Ok(())
        } else {
            Err(LauncherError::HashMismatch)
        };
    }
    write_atomic_bytes(&path, bytes)?;
    verify_version(version)
}

fn verify_version(version: &ControllerVersion) -> LauncherResult<()> {
    let path = version.binary_path()?;
    if !path.is_file() || sha256_file(&path)? != version.controller_sha256 {
        return Err(LauncherError::HashMismatch);
    }
    Ok(())
}

fn load_active() -> LauncherResult<Option<ActiveController>> {
    let value = load_optional::<ActiveController>(Path::new(ACTIVE_PATH))?;
    if let Some(value) = &value {
        value.validate()?;
    }
    Ok(value)
}

fn load_staged() -> LauncherResult<Option<StagedController>> {
    let value = load_optional::<StagedController>(Path::new(STAGED_PATH))?;
    if let Some(value) = &value {
        value.validate()?;
    }
    Ok(value)
}

fn load_pending() -> LauncherResult<Option<ActivationRecord>> {
    let value = load_optional::<ActivationRecord>(Path::new(PENDING_PATH))?;
    if let Some(value) = &value {
        value.validate()?;
    }
    Ok(value)
}

fn load_ready() -> LauncherResult<Option<ReadyRecord>> {
    load_optional::<ReadyRecord>(Path::new(READY_PATH))
}

fn load_optional<T: for<'de> Deserialize<'de>>(path: &Path) -> LauncherResult<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
}

fn save_json<T: Serialize>(path: &Path, value: &T) -> LauncherResult<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    write_atomic_bytes(path, &bytes)
}

fn write_atomic_bytes(path: &Path, bytes: &[u8]) -> LauncherResult<()> {
    let parent = path.parent().ok_or(LauncherError::InvalidState)?;
    fs::create_dir_all(parent)?;
    let temp = PathBuf::from(format!("{}.tmp", path.display()));
    let mut file = File::create(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    atomic_replace(&temp, path)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> LauncherResult<()> {
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
fn atomic_replace(source: &Path, destination: &Path) -> LauncherResult<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)?;
    Ok(())
}

fn spawn_controller(
    version: &ControllerVersion,
    activation_request_id: Option<&str>,
) -> LauncherResult<Child> {
    verify_version(version)?;
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(Path::new(CONTROL_ROOT).join("controller.stdout.log"))?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(Path::new(CONTROL_ROOT).join("controller.stderr.log"))?;

    let mut command = Command::new(version.binary_path()?);
    command.arg("run");
    if let Some(request_id) = activation_request_id {
        command.arg("--activation-request-id").arg(request_id);
    }
    command
        .current_dir(CONTROL_ROOT)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|_| LauncherError::ControllerLaunch)
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

pub fn sha256_bytes(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

pub fn sha256_file(path: &Path) -> LauncherResult<String> {
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

#[cfg(windows)]
fn controller_running() -> LauncherResult<bool> {
    named_mutex_exists(CONTROLLER_MUTEX)
}

#[cfg(not(windows))]
fn controller_running() -> LauncherResult<bool> {
    Ok(false)
}

#[cfg(windows)]
fn named_mutex_exists(name: &str) -> LauncherResult<bool> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::{
        Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError},
        System::Threading::CreateMutexW,
    };

    let name: Vec<u16> = std::ffi::OsStr::new(name)
        .encode_wide()
        .chain(Some(0))
        .collect();
    let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
    if handle.is_null() {
        return Err(std::io::Error::last_os_error().into());
    }
    let exists = unsafe { GetLastError() } == ERROR_ALREADY_EXISTS;
    unsafe { CloseHandle(handle) };
    Ok(exists)
}

#[cfg(windows)]
fn process_is_running(pid: u32) -> LauncherResult<bool> {
    use windows_sys::Win32::{
        Foundation::{CloseHandle, WAIT_TIMEOUT},
        System::Threading::{OpenProcess, WaitForSingleObject},
    };
    const PROCESS_SYNCHRONIZE_ACCESS: u32 = 0x0010_0000;
    let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE_ACCESS, 0, pid) };
    if handle.is_null() {
        return Ok(false);
    }
    let result = unsafe { WaitForSingleObject(handle, 0) };
    unsafe { CloseHandle(handle) };
    Ok(result == WAIT_TIMEOUT)
}

#[cfg(not(windows))]
fn process_is_running(_pid: u32) -> LauncherResult<bool> {
    Ok(false)
}

#[cfg(windows)]
fn wait_for_process_exit(pid: u32) -> LauncherResult<()> {
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
fn wait_for_process_exit(_pid: u32) -> LauncherResult<()> {
    Err(LauncherError::UnsupportedPlatform)
}

struct LauncherGuard {
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
}

impl LauncherGuard {
    fn acquire() -> LauncherResult<Self> {
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            use windows_sys::Win32::{
                Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError},
                System::Threading::CreateMutexW,
            };
            let name: Vec<u16> = std::ffi::OsStr::new(LAUNCHER_MUTEX)
                .encode_wide()
                .chain(Some(0))
                .collect();
            let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
            if handle.is_null() {
                return Err(std::io::Error::last_os_error().into());
            }
            if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
                unsafe { CloseHandle(handle) };
                return Err(LauncherError::AlreadyRunning);
            }
            Ok(Self { handle })
        }
        #[cfg(not(windows))]
        {
            Ok(Self {})
        }
    }
}

#[cfg(windows)]
impl Drop for LauncherGuard {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
        }
    }
}

enum ReadyWait {
    Ready,
    ChildExited,
}

struct ReadyEvent {
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
}

impl ReadyEvent {
    fn create(request_id: &str) -> LauncherResult<Self> {
        if !valid_request_id(request_id) {
            return Err(LauncherError::InvalidState);
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            use windows_sys::Win32::System::Threading::CreateEventW;
            let name = ready_event_name(request_id);
            let name: Vec<u16> = std::ffi::OsStr::new(&name)
                .encode_wide()
                .chain(Some(0))
                .collect();
            let handle = unsafe { CreateEventW(std::ptr::null(), 1, 0, name.as_ptr()) };
            if handle.is_null() {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(Self { handle })
        }
        #[cfg(not(windows))]
        {
            Err(LauncherError::UnsupportedPlatform)
        }
    }

    fn wait_with_child(&self, child: &mut Child) -> LauncherResult<ReadyWait> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::{
                Foundation::{HANDLE, WAIT_FAILED, WAIT_OBJECT_0},
                System::Threading::{INFINITE, WaitForMultipleObjects},
            };
            let handles = [self.handle, child.as_raw_handle() as HANDLE];
            let result = unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) };
            if result == WAIT_OBJECT_0 {
                Ok(ReadyWait::Ready)
            } else if result == WAIT_OBJECT_0 + 1 {
                Ok(ReadyWait::ChildExited)
            } else if result == WAIT_FAILED {
                Err(std::io::Error::last_os_error().into())
            } else {
                Err(LauncherError::InvalidState)
            }
        }
        #[cfg(not(windows))]
        {
            let _ = child;
            Err(LauncherError::UnsupportedPlatform)
        }
    }
}

#[cfg(windows)]
impl Drop for ReadyEvent {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
        }
    }
}

fn ready_event_name(request_id: &str) -> String {
    format!(r"Local\iamaman11-okx-controller-ready-{request_id}")
}

#[cfg(windows)]
fn signal_ready_event(request_id: &str) -> LauncherResult<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::Threading::{EVENT_MODIFY_STATE, OpenEventW, SetEvent},
    };
    let name = ready_event_name(request_id);
    let name: Vec<u16> = std::ffi::OsStr::new(&name)
        .encode_wide()
        .chain(Some(0))
        .collect();
    let handle = unsafe { OpenEventW(EVENT_MODIFY_STATE, 0, name.as_ptr()) };
    if handle.is_null() {
        return Ok(());
    }
    let ok = unsafe { SetEvent(handle) };
    unsafe { CloseHandle(handle) };
    if ok == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn signal_ready_event(_request_id: &str) -> LauncherResult<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(hash: char) -> ControllerVersion {
        ControllerVersion {
            repository_id: REPOSITORY_ID,
            run_id: 11,
            artifact_id: 22,
            source_head_sha: "a".repeat(40),
            source_tree: "b".repeat(40),
            rust_version: "rustc 1.95.0".to_owned(),
            controller_sha256: hash.to_string().repeat(64),
        }
    }

    #[test]
    fn version_path_is_content_addressed_and_fixed() {
        let value = version('c');
        assert_eq!(
            value.binary_path().expect("path"),
            Path::new(VERSIONS_ROOT)
                .join("c".repeat(64))
                .join("okx-host-control.exe")
        );
        assert!(!value.binary_path().expect("path").starts_with(r"C:\okx\"));
    }

    #[test]
    fn active_rejects_same_previous_and_active() {
        let value = version('c');
        let active = ActiveController::new(value.clone(), Some(value));
        assert!(active.validate().is_err());
    }

    #[test]
    fn activation_requires_distinct_exact_versions() {
        let pending = ActivationRecord {
            schema: ACTIVATION_SCHEMA_V1.to_owned(),
            repository_id: REPOSITORY_ID,
            request_id: "ctl_launcher_test_012345".to_owned(),
            old_controller_pid: 42,
            previous: version('c'),
            candidate: version('d'),
            terminal_ack: false,
        };
        assert!(pending.validate().is_ok());

        let mut invalid = pending;
        invalid.candidate = invalid.previous.clone();
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn request_ids_are_bounded_and_allowlisted() {
        assert!(valid_request_id("ctl_launcher_test_012345"));
        assert!(!valid_request_id("launcher test"));
        assert!(!valid_request_id("ctl_x"));
    }

    #[test]
    fn launcher_root_has_no_mutable_workspace_path() {
        for path in [
            CONTROL_ROOT,
            LAUNCHER_PATH,
            VERSIONS_ROOT,
            ACTIVE_PATH,
            STAGED_PATH,
            PENDING_PATH,
            READY_PATH,
            LAST_RESULT_PATH,
        ] {
            assert!(!Path::new(path).starts_with(r"C:\okx\"));
        }
    }
}
