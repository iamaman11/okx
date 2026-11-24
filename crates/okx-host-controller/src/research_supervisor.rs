//! CONTROL-owned Research process lifecycle, separate from the trading Engine.
//! No finance credentials, ledger handles, strategy signals or Engine state are
//! reachable from this module. It never starts a worker from an unchecked path.
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Stdio},
    time::{Duration, Instant},
};
#[cfg(not(windows))]
use std::fs::File;

use okx_host_protocol::{
    HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1, ResearchCatalogMode, ResearchRuntimePhase,
    ResearchRuntimeStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    HostControlError, HostControlResult,
    artifact::resolve_verified_installed_research,
    background_process::hidden_command,
    desired::atomic_replace,
    job::AgentJob,
};

const RESEARCH_DESIRED_SCHEMA: &str = "okx.host-control.research-desired/v1";
const STATE_FILE: &str = "research-state.json";
const MAX_STATE_BYTES: u64 = 4096;
const RESEARCH_RUNTIME_ID: &str = "windows-primary";
// The authenticated Edge origin is fixed by the installation, never supplied
// through an MCP/CONTROL command or a writable Research state file.
const RESEARCH_EDGE_WSS_URL: &str = "wss://okx-cloudflare-mcp.okx-794.workers.dev/runtime";
const RETRY_SECONDS: [u64; 5] = [1, 5, 15, 30, 60];
const STABLE_PROCESS_SECONDS: u64 = 30;
const HOST_JOB_ASSIGNED_MARKER: &[u8] = b"RESEARCH_HOST_JOB_ASSIGNED\n";

fn valid_sha(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn fresh_scope() -> HostControlResult<String> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| HostControlError::Random(e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn fresh_incarnation() -> HostControlResult<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| HostControlError::Random(e.to_string()))?;
    Ok(format!(
        "ctl_{}",
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchDesiredState {
    schema: String,
    desired_running: bool,
    expected_worker_sha256: Option<String>,
    host_scope: Option<String>,
    process_generation: u64,
}

impl ResearchDesiredState {
    fn stopped() -> Self {
        Self {
            schema: RESEARCH_DESIRED_SCHEMA.into(),
            desired_running: false,
            expected_worker_sha256: None,
            host_scope: None,
            process_generation: 0,
        }
    }

    fn valid(&self) -> bool {
        self.schema == RESEARCH_DESIRED_SCHEMA
            && self
                .expected_worker_sha256
                .as_deref()
                .is_none_or(valid_sha)
            && self.host_scope.as_deref().is_none_or(valid_sha)
            && (self.expected_worker_sha256.is_some() == self.host_scope.is_some())
            && (!self.desired_running || self.expected_worker_sha256.is_some())
            && (self.process_generation == 0 || self.host_scope.is_some())
    }
}

#[derive(Debug, Clone)]
struct ResearchStateStore {
    path: PathBuf,
}

impl ResearchStateStore {
    fn new(control_root: &Path) -> Self {
        Self {
            path: control_root.join(STATE_FILE),
        }
    }

    fn read(&self) -> HostControlResult<ResearchDesiredState> {
        let metadata = match fs::symlink_metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                validate_state_ancestors(
                    self.path.parent().ok_or(HostControlError::ResearchStateCorrupt)?,
                    true,
                )?;
                return Ok(ResearchDesiredState::stopped());
            }
            Err(error) => return Err(error.into()),
        };
        if !metadata.is_file() || is_reparse(&metadata) || metadata.len() > MAX_STATE_BYTES {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        validate_state_ancestors(
            self.path.parent().ok_or(HostControlError::ResearchStateCorrupt)?,
            false,
        )?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(0x0020_0000); // FILE_FLAG_OPEN_REPARSE_POINT
        }
        let mut file = options.open(&self.path)?;
        let opened = file.metadata()?;
        if !opened.is_file() || is_reparse(&opened) || opened.len() > MAX_STATE_BYTES {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        let mut bytes = Vec::new();
        file.by_ref()
            .take(MAX_STATE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_STATE_BYTES {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        let state: ResearchDesiredState = serde_json::from_slice(&bytes)
            .map_err(|_| HostControlError::ResearchStateCorrupt)?;
        if !state.valid() {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        Ok(state)
    }

    fn save(&self, state: &ResearchDesiredState) -> HostControlResult<()> {
        if !state.valid() {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        let parent = self
            .path
            .parent()
            .ok_or(HostControlError::ResearchStateCorrupt)?;
        validate_state_ancestors(parent, true)?;
        fs::create_dir_all(parent)?;
        validate_state_ancestors(parent, false)?;
        match fs::symlink_metadata(&self.path) {
            Ok(metadata) if metadata.is_file() && !is_reparse(&metadata) => {}
            Ok(_) => return Err(HostControlError::ResearchStateCorrupt),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        // Unique same-directory temporary files prevent a crash-stale `.tmp`
        // from permanently blocking all future Research lifecycle writes.
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce)
            .map_err(|error| HostControlError::Random(error.to_string()))?;
        let suffix: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let mut temp_name = self.path.as_os_str().to_os_string();
        temp_name.push(format!(".{suffix}.tmp"));
        let temp = PathBuf::from(temp_name);
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temp)?;
        let result = (|| -> HostControlResult<()> {
            file.write_all(&serde_json::to_vec(state)?)?;
            file.sync_all()?;
            drop(file);
            atomic_replace(&temp, &self.path)?;
            #[cfg(not(windows))]
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }
}

/// The Research desired state must never be redirected by a junction or link
/// along its fixed CONTROL-root path. Missing ancestors are allowed only before
/// the first atomic save, never when reading an existing state file.
fn validate_state_ancestors(parent: &Path, allow_missing: bool) -> HostControlResult<()> {
    for ancestor in parent.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.is_dir() && !is_reparse(&metadata) => {}
            Err(error)
                if allow_missing && error.kind() == std::io::ErrorKind::NotFound => {}
            _ => return Err(HostControlError::ResearchStateCorrupt),
        }
    }
    Ok(())
}

fn is_reparse(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

/// One Research process and one independently owned Windows Job. Its desired
/// state and SQLite root have no dependency on Engine desired state or scope.
pub struct ResearchSupervisor {
    store: ResearchStateStore,
    runtime_root: PathBuf,
    desired: ResearchDesiredState,
    state_error: Option<String>,
    child: Option<Child>,
    job: Option<AgentJob>,
    active_sha256: Option<String>,
    started_at: Option<Instant>,
    restart_attempt: usize,
    retry_at: Option<Instant>,
    last_error: Option<String>,
    #[cfg(test)]
    terminate_fail_once: bool,
}

impl ResearchSupervisor {
    pub fn canonical() -> HostControlResult<Self> {
        Self::at(
            PathBuf::from(okx_host_launcher::CONTROL_ROOT),
            PathBuf::from(okx_host_launcher::RUNTIME_ROOT),
        )
    }

    /// Explicit roots support process/rollback acceptance without ever
    /// touching the real C:\okx installation. Production uses `canonical`.
    pub fn at(control_root: PathBuf, runtime_root: PathBuf) -> HostControlResult<Self> {
        let store = ResearchStateStore::new(&control_root);
        let (desired, state_error) = match store.read() {
            Ok(desired) => (desired, None),
            Err(error) => (
                ResearchDesiredState::stopped(),
                Some(error.code().to_owned()),
            ),
        };
        Ok(Self {
            store,
            runtime_root,
            desired,
            state_error,
            child: None,
            job: None,
            active_sha256: None,
            started_at: None,
            restart_attempt: 0,
            retry_at: None,
            last_error: None,
            #[cfg(test)]
            terminate_fail_once: false,
        })
    }

    fn persist(&mut self, next: ResearchDesiredState) -> HostControlResult<()> {
        if self.state_error.is_some() {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        self.store.save(&next)?;
        self.desired = next;
        Ok(())
    }

    fn running(&mut self) -> HostControlResult<bool> {
        let exited = match self.child.as_mut() {
            Some(child) => child.try_wait()?.is_some(),
            None => false,
        };
        if exited {
            if let Some(mut child) = self.child.take() {
                let _ = child.wait();
            }
            self.job.take();
            self.active_sha256 = None;
            self.started_at = None;
            self.last_error = Some("RESEARCH_PROCESS_EXITED".into());
            if self.desired.desired_running {
                self.schedule_restart();
            }
        }
        Ok(self.child.is_some())
    }

    fn schedule_restart(&mut self) {
        let seconds = RETRY_SECONDS[self.restart_attempt.min(RETRY_SECONDS.len() - 1)];
        self.restart_attempt = self.restart_attempt.saturating_add(1);
        self.retry_at = Some(Instant::now() + Duration::from_secs(seconds));
    }

    fn retry_in_ms(&self) -> Option<u64> {
        self.retry_at.map(|at| {
            at.saturating_duration_since(Instant::now())
                .as_millis()
                .min(600_000) as u64
        })
    }

    fn launch(&mut self) -> HostControlResult<u32> {
        if !cfg!(windows) {
            return Err(HostControlError::UnsupportedPlatform);
        }
        let expected = self
            .desired
            .expected_worker_sha256
            .clone()
            .ok_or(HostControlError::ResearchStateCorrupt)?;
        let scope = self
            .desired
            .host_scope
            .clone()
            .ok_or(HostControlError::ResearchStateCorrupt)?;
        let image = resolve_verified_installed_research(&self.runtime_root)?;
        if image.sha256 != expected
            || okx_host_launcher::sha256_file(&image.binary)? != image.sha256
        {
            return Err(HostControlError::ResearchIdentityMismatch);
        }
        let generation = self
            .desired
            .process_generation
            .checked_add(1)
            .ok_or(HostControlError::ResearchGenerationExhausted)?;
        let incarnation = fresh_incarnation()?;
        // Never pass a bearer, Engine account identity or private credential in
        // argv/env. Research reads its own CredMan target after child startup.
        let mut command = hidden_command(&image.binary);
        command
            .env_clear()
            .arg("--edge-runtime")
            .arg("--root")
            .arg(self.runtime_root.join("research").join("edge-catalog"))
            .arg("--scope")
            .arg(&scope)
            .env("OKX_RESEARCH_EDGE_WSS_URL", RESEARCH_EDGE_WSS_URL)
            .env("OKX_RESEARCH_RUNTIME_ID", RESEARCH_RUNTIME_ID)
            .env("OKX_RESEARCH_HOST_SCOPE", &scope)
            .env("OKX_RESEARCH_INCAR", incarnation)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // Avoid inheriting Engine/private credentials or diagnostic overrides
        // from CONTROL. Only Windows account and OS process basics pass through.
        for key in [
            "SystemRoot", "WINDIR", "USERPROFILE", "APPDATA", "LOCALAPPDATA",
            "ProgramData", "TEMP", "TMP", "PATH", "HOMEDRIVE", "HOMEPATH",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let job = AgentJob::new()?;
        let mut child = command.spawn()?;
        job.assign(&mut child)?;
        // The Research executable waits at this bounded stdin gate BEFORE it
        // self-assigns its memory Job. Job assignment must therefore precede
        // any Research runtime startup, and the marker must end with EOF.
        let gate_result = (|| -> std::io::Result<()> {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "research startup gate missing")
            })?;
            stdin.write_all(HOST_JOB_ASSIGNED_MARKER)?;
            stdin.flush()?;
            drop(stdin);
            Ok(())
        })();
        if let Err(error) = gate_result {
            let _ = job.terminate();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error.into());
        }
        let pid = child.id();
        let mut next = self.desired.clone();
        next.process_generation = generation;
        if let Err(error) = self.store.save(&next) {
            let _ = job.terminate();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        self.desired = next;
        self.child = Some(child);
        self.job = Some(job);
        self.active_sha256 = Some(image.sha256);
        self.started_at = Some(Instant::now());
        self.last_error = None;
        self.retry_at = None;
        Ok(pid)
    }

    pub fn start(&mut self, expected_installed_worker_sha256: &str) -> HostControlResult<Value> {
        if !cfg!(windows) {
            return Err(HostControlError::UnsupportedPlatform);
        }
        if self.state_error.is_some() || !valid_sha(expected_installed_worker_sha256) {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        let image = resolve_verified_installed_research(&self.runtime_root)?;
        if image.sha256 != expected_installed_worker_sha256 {
            return Err(HostControlError::ResearchIdentityMismatch);
        }
        if self.running()? {
            if self.active_sha256.as_deref() != Some(expected_installed_worker_sha256) {
                return Err(HostControlError::ResearchIdentityMismatch);
            }
            let next = ResearchDesiredState {
                desired_running: true,
                ..self.desired.clone()
            };
            self.persist(next)?;
            return self.status();
        }
        let scope = match &self.desired.host_scope {
            Some(scope) => scope.clone(),
            None => fresh_scope()?,
        };
        let mut next = self.desired.clone();
        next.desired_running = true;
        next.expected_worker_sha256 = Some(image.sha256);
        next.host_scope = Some(scope);
        self.persist(next)?;
        match self.launch() {
            Ok(_) => self.status(),
            Err(error) => {
                self.last_error = Some(error.code().into());
                self.schedule_restart();
                Err(error)
            }
        }
    }

    pub fn stop(
        &mut self,
        expected_active_worker_sha256: &str,
        expected_process_generation: u64,
    ) -> HostControlResult<Value> {
        if self.state_error.is_some() {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        if !self.desired.desired_running
            || self.desired.expected_worker_sha256.as_deref()
                != Some(expected_active_worker_sha256)
            || self.desired.process_generation != expected_process_generation
        {
            return Err(HostControlError::ResearchIdentityMismatch);
        }
        let mut next = self.desired.clone();
        next.desired_running = false;
        self.persist(next)?;
        if let Err(error) = self.terminate_owned() {
            self.last_error = Some("RESEARCH_TERMINATION_UNCONFIRMED".into());
            return Err(error);
        }
        self.retry_at = None;
        self.restart_attempt = 0;
        self.status()
    }

    fn terminate_owned(&mut self) -> HostControlResult<()> {
        #[cfg(test)]
        if std::mem::take(&mut self.terminate_fail_once) {
            return Err(HostControlError::JobAssignment("injected termination failure".into()));
        }
        // Never `take()` either kernel ownership handle until the kill path
        // succeeds; a transient TerminateJobObject error is retried on tick.
        if let Some(child) = self.child.as_mut()
            && child.try_wait()?.is_none()
        {
            if let Some(job) = &self.job {
                job.terminate()?;
            } else {
                child.kill()?;
            }
        }
        if let Some(job) = &self.job
            && !job.is_empty()?
        {
            job.terminate()?;
        }
        if let Some(child) = self.child.as_mut() {
            child.wait()?;
        }
        self.child.take();
        self.job.take();
        self.active_sha256 = None;
        self.started_at = None;
        self.last_error = None;
        Ok(())
    }

    pub fn shutdown(&mut self) -> HostControlResult<()> {
        // Controller shutdown must not rewrite desired state: autostart will
        // recover this independent process if Research was desired RUNNING.
        self.terminate_owned()
    }

    pub fn reconcile(&mut self) -> HostControlResult<()> {
        let running = self.running()?;
        if !self.desired.desired_running {
            if running || self.job.is_some() {
                if let Err(error) = self.terminate_owned() {
                    self.last_error = Some("RESEARCH_TERMINATION_UNCONFIRMED".into());
                    return Err(error);
                }
            }
            return Ok(());
        }
        if self.state_error.is_some() {
            return Ok(());
        }
        if running {
            if self.started_at.is_some_and(|at| at.elapsed() >= Duration::from_secs(STABLE_PROCESS_SECONDS)) {
                self.restart_attempt = 0;
                self.retry_at = None;
            }
            return Ok(());
        }
        if self.retry_at.is_some_and(|at| at > Instant::now()) {
            return Ok(());
        }
        if let Err(error) = self.launch() {
            self.last_error = Some(error.code().into());
            self.schedule_restart();
            return Err(error);
        }
        Ok(())
    }

    pub fn status(&mut self) -> HostControlResult<Value> {
        // A readback must not spawn a child or write its desired state.
        let running = self.running()?;
        let (installed, image_issue) = match resolve_verified_installed_research(&self.runtime_root) {
            Ok(image) => (Some(image), None),
            Err(HostControlError::ArtifactVerification("installed research pointer missing")) => {
                (None, Some("RESEARCH_NOT_INSTALLED".to_owned()))
            }
            Err(HostControlError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound
                    && matches!(
                        fs::symlink_metadata(&self.runtime_root),
                        Err(missing) if missing.kind() == std::io::ErrorKind::NotFound
                    ) =>
            {
                (None, Some("RESEARCH_NOT_INSTALLED".to_owned()))
            }
            Err(_) => (None, Some("RESEARCH_IMAGE_INVALID".to_owned())),
        };
        let mismatched = self.desired.desired_running
            && installed.as_ref().map(|image| image.sha256.as_str())
                != self.desired.expected_worker_sha256.as_deref();
        let phase = if self.state_error.is_some()
            || (self.job.is_some() && !running)
            || image_issue.as_deref() == Some("RESEARCH_IMAGE_INVALID")
            || mismatched
        {
            ResearchRuntimePhase::InvalidProvenance
        } else if running {
            ResearchRuntimePhase::RunningUnverified
        } else if !self.desired.desired_running {
            ResearchRuntimePhase::Stopped
        } else if self.retry_at.is_some_and(|at| at > Instant::now()) {
            ResearchRuntimePhase::RestartWait
        } else {
            ResearchRuntimePhase::Starting
        };
        let status = ResearchRuntimeStatus {
            schema: HOST_RESEARCH_RUNTIME_STATUS_SCHEMA_V1.into(),
            desired_running: self.desired.desired_running,
            process_generation: self.desired.process_generation,
            pid: self.child.as_ref().map(Child::id),
            active_worker_sha256: self.active_sha256.clone(),
            installed_worker_sha256: installed.map(|image| image.sha256),
            phase,
            catalog_mode: ResearchCatalogMode::HostIndependent,
            retry_in_ms: self.retry_in_ms(),
            last_error: self.state_error.clone()
                .or_else(|| mismatched.then(|| "RESEARCH_IMAGE_MISMATCH".into()))
                .or_else(|| self.last_error.clone())
                .or(image_issue),
        };
        status.validate()?;
        Ok(serde_json::to_value(status)?)
    }

    pub fn verified_installed_image(&self) -> HostControlResult<PathBuf> {
        Ok(resolve_verified_installed_research(&self.runtime_root)?.binary)
    }

    /// Credential rollover requires an explicitly stopped Research process.
    /// Never replace the CredMan secret underneath a live Edge session.
    pub fn ensure_stopped_for_provision(&mut self) -> HostControlResult<()> {
        if self.state_error.is_some() {
            return Err(HostControlError::ResearchStateCorrupt);
        }
        if self.desired.desired_running
            || self.running()?
            || self.job.is_some()
        {
            return Err(HostControlError::ResearchIdentityMismatch);
        }
        Ok(())
    }
}

impl Drop for ResearchSupervisor {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_state_is_strict_and_independent_of_engine_state() {
        let root = std::env::temp_dir().join(format!("okx-control-research-state-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let store = ResearchStateStore::new(&root);
        assert_eq!(store.read().unwrap(), ResearchDesiredState::stopped());
        let state = ResearchDesiredState {
            schema: RESEARCH_DESIRED_SCHEMA.into(),
            desired_running: true,
            expected_worker_sha256: Some("a".repeat(64)),
            host_scope: Some("b".repeat(64)),
            process_generation: 7,
        };
        store.save(&state).unwrap();
        assert_eq!(store.read().unwrap(), state);
        fs::write(root.join("engine-state.pb"), b"corrupted engine state").unwrap();
        assert_eq!(store.read().unwrap(), state);
        fs::write(root.join(STATE_FILE), b"{\"schema\":\"invalid\"}").unwrap();
        assert!(matches!(store.read(), Err(HostControlError::ResearchStateCorrupt)));
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn research_supervisor_sleeper_child() {
        if std::env::var_os("OKX_RESEARCH_SUPERVISOR_TEST_CHILD").is_some() {
            std::thread::sleep(Duration::from_secs(25));
        }
    }

    #[cfg(windows)]
    #[test]
    fn failed_research_stop_preserves_job_and_reconciles_termination() {
        let root = std::env::temp_dir().join(format!(
            "okx-research-stop-fault-{}-{}",
            std::process::id(),
            fresh_scope().unwrap()
        ));
        fs::create_dir_all(&root).unwrap();
        let mut owner = ResearchSupervisor::at(root.clone(), root.join("runtime")).unwrap();
        let sha = "c".repeat(64);
        owner.persist(ResearchDesiredState {
            schema: RESEARCH_DESIRED_SCHEMA.into(),
            desired_running: true,
            expected_worker_sha256: Some(sha.clone()),
            host_scope: Some("d".repeat(64)),
            process_generation: 1,
        }).unwrap();

        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "research_supervisor::tests::research_supervisor_sleeper_child"])
            .env("OKX_RESEARCH_SUPERVISOR_TEST_CHILD", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn().unwrap();
        let job = AgentJob::new().unwrap();
        job.assign(&mut child).unwrap();
        owner.active_sha256 = Some(sha.clone());
        owner.child = Some(child);
        owner.job = Some(job);
        owner.terminate_fail_once = true;

        assert!(owner.stop(&sha, 1).is_err());
        assert!(!owner.desired.desired_running);
        assert!(owner.child.is_some());
        assert!(owner.job.is_some());
        let status: ResearchRuntimeStatus = serde_json::from_value(owner.status().unwrap()).unwrap();
        assert_eq!(status.phase, ResearchRuntimePhase::RunningUnverified);
        assert!(status.pid.is_some());
        assert_eq!(status.last_error.as_deref(), Some("RESEARCH_TERMINATION_UNCONFIRMED"));

        owner.reconcile().unwrap();
        assert!(owner.child.is_none());
        assert!(owner.job.is_none());
        assert_eq!(owner.desired.process_generation, 1);
        assert_eq!(owner.store.read().unwrap().desired_running, false);
        drop(owner);
        fs::remove_dir_all(root).unwrap();
    }
}
