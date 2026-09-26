use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use okx_protocol::HostControlOperation;
use serde_json::{Value, json};

use crate::{
    HostControlError, HostControlResult,
    auth::load_native_github_token,
    autostart,
    desired::{AgentDesired, DesiredStateStore},
    job::AgentJob,
};

const CANONICAL_ROOT: &str = r"C:\okx";
const RUNTIME_ROOT: &str = r"C:\okx-runtime";
const AGENT_MAILBOX_ISSUE: &str = "10";
const HEALTHY_AGENT_SECS: u64 = 30;
const RESTART_BACKOFF_SECS: [u64; 5] = [1, 5, 15, 30, 60];
const ALLOWED_REMOTES: &[&str] = &[
    "https://github.com/iamaman11/okx",
    "https://github.com/iamaman11/okx.git",
    "git@github.com:iamaman11/okx.git",
];

pub struct HostExecutor {
    repo_root: PathBuf,
    agent_child: Option<Child>,
    agent_started_at: Option<Instant>,
    agent_job: AgentJob,
    desired_store: DesiredStateStore,
    desired_agent: AgentDesired,
    desired_error: Option<String>,
    restart_attempt: usize,
    next_restart_at: Option<Instant>,
    last_reconcile: String,
}

impl HostExecutor {
    pub fn canonical() -> HostControlResult<Self> {
        let desired_store = DesiredStateStore::canonical();
        let (desired_agent, desired_error) = match desired_store.load() {
            Ok(desired) => (desired, None),
            Err(error) => (AgentDesired::Stopped, Some(error.to_string())),
        };

        Ok(Self {
            repo_root: PathBuf::from(CANONICAL_ROOT),
            agent_child: None,
            agent_started_at: None,
            agent_job: AgentJob::new()?,
            desired_store,
            desired_agent,
            desired_error,
            restart_attempt: 0,
            next_restart_at: None,
            last_reconcile: "NOT_RUN".to_owned(),
        })
    }

    pub fn execute(&mut self, operation: HostControlOperation) -> HostControlResult<Value> {
        match operation {
            HostControlOperation::Status => self.status(),
            HostControlOperation::Sync => self.sync(),
            HostControlOperation::BuildAgent => self.build_agent(),
            HostControlOperation::DeployAgent { .. } => Err(HostControlError::InvalidExecutionPath),
            HostControlOperation::TestWorkspace => self.test_workspace(),
            HostControlOperation::InitAgentIdentity => self.init_agent_identity(),
            HostControlOperation::AgentIdentity => self.agent_identity(),
            HostControlOperation::BootstrapAgentGithubToken => self.bootstrap_agent_github_token(),
            HostControlOperation::StartAgent => self.start_agent(),
            HostControlOperation::StopAgent => self.stop_agent(),
            HostControlOperation::RestartAgent => self.restart_agent(),
            HostControlOperation::InstallAutostart => autostart::install(),
            HostControlOperation::AutostartStatus => autostart::status_value(),
            HostControlOperation::AcceptanceKillAgent => self.acceptance_kill_agent(),
            HostControlOperation::TransportStatus => self.transport_status(),
        }
    }

    pub fn shutdown(&mut self) {
        let _ = self.terminate_agent_owned();
    }

    pub fn reconcile_desired(&mut self) -> HostControlResult<Value> {
        let running = self.agent_is_running()?;

        if let Some(error) = self.desired_error.clone() {
            if running {
                self.terminate_agent_owned()?;
            }
            self.last_reconcile = "DEGRADED_DESIRED_STATE".to_owned();
            return Ok(json!({
                "disposition": self.last_reconcile.clone(),
                "desired": AgentDesired::Stopped,
                "error": error
            }));
        }

        match self.desired_agent {
            AgentDesired::Stopped => {
                if running {
                    self.terminate_agent_owned()?;
                    self.last_reconcile = "STOPPED_TO_DESIRED".to_owned();
                } else {
                    self.last_reconcile = "NOOP_STOPPED".to_owned();
                }
            }
            AgentDesired::Running => {
                if running {
                    if self.agent_started_at.is_some_and(|started| {
                        started.elapsed() >= Duration::from_secs(HEALTHY_AGENT_SECS)
                    }) {
                        self.restart_attempt = 0;
                        self.next_restart_at = None;
                    }
                    self.last_reconcile = "READY_RUNNING".to_owned();
                } else if self.restart_due() {
                    match self.start_agent_process() {
                        Ok(pid) => {
                            self.last_reconcile = format!("RESTORED_AGENT_PID_{pid}");
                        }
                        Err(error) => {
                            self.schedule_restart();
                            self.last_reconcile = format!("RESTART_FAILED_{}", error.code());
                            return Err(error);
                        }
                    }
                } else {
                    self.last_reconcile = "WAITING_RESTART_BACKOFF".to_owned();
                }
            }
        }

        Ok(json!({
            "disposition": self.last_reconcile,
            "desired": self.desired_agent,
            "running": self.agent_is_running()?,
            "restart_attempt": self.restart_attempt,
            "retry_in_ms": self.retry_in_ms()
        }))
    }

    fn status(&mut self) -> HostControlResult<Value> {
        let repo_present = self.repo_root.join(".git").is_dir();
        let mut head = None;
        let mut branch = None;
        let mut clean = None;
        let mut origin = None;

        if repo_present {
            origin = self.git(&["remote", "get-url", "origin"]).ok();
            head = self.git(&["rev-parse", "HEAD"]).ok();
            branch = self.git(&["rev-parse", "--abbrev-ref", "HEAD"]).ok();
            clean = self
                .git(&["status", "--porcelain"])
                .ok()
                .map(|value| value.trim().is_empty());
        }

        let running = self.agent_is_running()?;

        Ok(json!({
            "repo_root": CANONICAL_ROOT,
            "repo_present": repo_present,
            "repository": "iamaman11/okx",
            "head": head,
            "branch": branch,
            "clean": clean,
            "origin": origin,
            "cargo_available": command_available("cargo"),
            "agent_binary_present": self.agent_binary().is_file(),
            "agent_owned_running": running,
            "agent_desired": self.desired_agent,
            "desired_state_path": self.desired_store.path(),
            "desired_state_error": self.desired_error.clone(),
            "job_object_owned": true,
            "restart_attempt": self.restart_attempt,
            "retry_in_ms": self.retry_in_ms(),
            "last_reconcile": self.last_reconcile.clone()
        }))
    }

    fn sync(&mut self) -> HostControlResult<Value> {
        self.require_agent_stopped()?;
        self.assert_repo(true, false)?;

        self.git(&["fetch", "--prune", "origin", "main"])?;
        let branch = self.git(&["rev-parse", "--abbrev-ref", "HEAD"])?;
        if branch != "main" {
            self.git(&["switch", "main"])?;
        }
        self.git(&["merge", "--ff-only", "origin/main"])?;

        Ok(json!({
            "head": self.git(&["rev-parse", "HEAD"])?,
            "branch": self.git(&["rev-parse", "--abbrev-ref", "HEAD"])?,
            "clean": self.git(&["status", "--porcelain"])?.trim().is_empty()
        }))
    }

    fn test_workspace(&mut self) -> HostControlResult<Value> {
        self.require_agent_stopped()?;
        self.assert_synced_main()?;
        self.run_logged(
            "cargo",
            &["test", "--workspace"],
            "host-control-cargo-test.log",
            "cargo test",
        )?;

        Ok(json!({
            "head": self.git(&["rev-parse", "HEAD"])?,
            "workspace_tests": "PASS"
        }))
    }

    fn build_agent(&mut self) -> HostControlResult<Value> {
        self.require_agent_stopped()?;
        self.assert_synced_main()?;
        self.run_logged(
            "cargo",
            &["build", "--release", "-p", "okx-agent"],
            "host-control-cargo-build.log",
            "cargo build",
        )?;

        let local_binary = self.local_build_agent_binary();
        if !local_binary.is_file() {
            return Err(HostControlError::AgentBinaryMissing);
        }

        let bytes = fs::read(local_binary)?;
        self.install_verified_agent(&bytes)?;

        Ok(json!({
            "head": self.git(&["rev-parse", "HEAD"])?,
            "agent_binary_present": true,
            "deployment": "LOCAL_BOOTSTRAP_ONLY"
        }))
    }

    fn init_agent_identity(&mut self) -> HostControlResult<Value> {
        self.require_agent_binary()?;

        if let Ok(identity) = self.read_agent_identity() {
            return Ok(json!({
                "disposition": "EXISTING",
                "identity": identity
            }));
        }

        let output = self.agent_output(&["init-key"])?;
        let identity: Value = serde_json::from_slice(&output.stdout)?;

        Ok(json!({
            "disposition": "CREATED",
            "identity": identity
        }))
    }

    fn agent_identity(&mut self) -> HostControlResult<Value> {
        Ok(json!({
            "identity": self.read_agent_identity()?
        }))
    }

    fn bootstrap_agent_github_token(&mut self) -> HostControlResult<Value> {
        self.require_agent_binary()?;
        let token = load_native_github_token()?;

        let mut child = Command::new(self.agent_binary())
            .arg("set-github-token")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let mut stdin = child.stdin.take().ok_or(HostControlError::CommandFailed(
            "okx-agent set-github-token",
        ))?;
        stdin.write_all(token.as_bytes())?;
        stdin.write_all(b"\n")?;
        drop(stdin);

        let status = child.wait()?;
        if !status.success() {
            return Err(HostControlError::CommandFailed(
                "okx-agent set-github-token",
            ));
        }

        Ok(json!({
            "stored": true,
            "destination": "Windows Credential Manager"
        }))
    }

    fn start_agent(&mut self) -> HostControlResult<Value> {
        self.require_agent_binary()?;
        if self.agent_is_running()? {
            self.persist_desired(AgentDesired::Running)?;
            return Ok(json!({
                "disposition": "ALREADY_RUNNING",
                "desired": self.desired_agent
            }));
        }

        let pid = self.start_agent_process()?;
        if let Err(error) = self.persist_desired(AgentDesired::Running) {
            let _ = self.terminate_agent_owned();
            return Err(error);
        }
        self.restart_attempt = 0;
        self.next_restart_at = None;

        Ok(json!({
            "disposition": "STARTED",
            "pid": pid,
            "mailbox_issue": 10,
            "desired": self.desired_agent
        }))
    }

    fn stop_agent(&mut self) -> HostControlResult<Value> {
        self.persist_desired(AgentDesired::Stopped)?;
        let was_running = self.agent_is_running()?;
        if was_running {
            self.terminate_agent_owned()?;
        }
        self.restart_attempt = 0;
        self.next_restart_at = None;

        Ok(json!({
            "disposition": if was_running { "STOPPED" } else { "ALREADY_STOPPED" },
            "desired": self.desired_agent
        }))
    }

    fn restart_agent(&mut self) -> HostControlResult<Value> {
        let _ = self.terminate_agent_owned();
        let pid = self.start_agent_process()?;
        if let Err(error) = self.persist_desired(AgentDesired::Running) {
            let _ = self.terminate_agent_owned();
            return Err(error);
        }
        self.restart_attempt = 0;
        self.next_restart_at = None;

        Ok(json!({
            "disposition": "RESTARTED",
            "pid": pid,
            "desired": self.desired_agent
        }))
    }

    fn acceptance_kill_agent(&mut self) -> HostControlResult<Value> {
        if !self.agent_is_running()? {
            return Ok(json!({
                "disposition": "NO_OWNED_AGENT",
                "desired": self.desired_agent
            }));
        }

        self.terminate_agent_owned()?;
        if self.desired_agent == AgentDesired::Running {
            self.restart_attempt = 0;
            self.next_restart_at = Some(Instant::now() + Duration::from_secs(1));
        }

        Ok(json!({
            "disposition": "OWNED_AGENT_TERMINATED",
            "desired": self.desired_agent,
            "retry_in_ms": self.retry_in_ms()
        }))
    }

    fn transport_status(&mut self) -> HostControlResult<Value> {
        Ok(json!({
            "host": self.status()?,
            "identity": self.read_agent_identity()?,
            "autostart": autostart::status_value()?
        }))
    }

    fn read_agent_identity(&self) -> HostControlResult<Value> {
        self.require_agent_binary()?;
        let output = self.agent_output(&["identity"])?;
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn agent_output(&self, args: &[&str]) -> HostControlResult<std::process::Output> {
        let output = Command::new(self.agent_binary())
            .args(args)
            .current_dir(&self.repo_root)
            .output()?;

        if !output.status.success() {
            return Err(HostControlError::CommandFailed("okx-agent"));
        }

        Ok(output)
    }

    pub fn fetch_origin_main_tree(&self) -> HostControlResult<String> {
        self.assert_repo(false, false)?;
        self.git(&["fetch", "--prune", "origin", "main"])?;
        self.git(&["rev-parse", "origin/main^{tree}"])
    }

    pub fn install_verified_agent(&mut self, bytes: &[u8]) -> HostControlResult<()> {
        let should_restore = self.desired_agent == AgentDesired::Running;
        if self.agent_is_running()? {
            self.terminate_agent_owned()?;
        }

        fs::create_dir_all(self.runtime_dir())?;

        let current = self.agent_binary();
        let staging = self.runtime_dir().join("okx-agent.exe.new");
        let backup = self.runtime_dir().join("okx-agent.exe.previous");

        fs::write(&staging, bytes)?;

        if backup.exists() {
            fs::remove_file(&backup)?;
        }
        if current.exists() {
            fs::rename(&current, &backup)?;
        }

        if let Err(error) = fs::rename(&staging, &current) {
            if backup.exists() && !current.exists() {
                let _ = fs::rename(&backup, &current);
            }
            return Err(error.into());
        }

        if should_restore {
            if let Err(error) = self.start_agent_process() {
                self.schedule_restart();
                return Err(error);
            }
        }

        Ok(())
    }

    fn start_agent_process(&mut self) -> HostControlResult<u32> {
        self.require_agent_binary()?;
        fs::create_dir_all(self.runtime_dir())?;

        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.runtime_dir().join("okx-agent.stdout.log"))?;
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.runtime_dir().join("okx-agent.stderr.log"))?;

        let mut child = Command::new(self.agent_binary())
            .args([
                "run",
                "--mailbox-issue",
                AGENT_MAILBOX_ISSUE,
                "--poll-seconds",
                "2",
            ])
            .current_dir(&self.repo_root)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;

        self.agent_job.assign(&mut child)?;
        let pid = child.id();
        self.agent_child = Some(child);
        self.agent_started_at = Some(Instant::now());
        self.next_restart_at = None;
        Ok(pid)
    }

    fn terminate_agent_owned(&mut self) -> HostControlResult<()> {
        let Some(mut child) = self.agent_child.take() else {
            self.agent_started_at = None;
            return Ok(());
        };

        if child.try_wait()?.is_none() {
            child.kill()?;
            child.wait()?;
        }
        self.agent_started_at = None;
        Ok(())
    }

    fn persist_desired(&mut self, desired: AgentDesired) -> HostControlResult<()> {
        self.desired_store.save(desired)?;
        self.desired_agent = desired;
        self.desired_error = None;
        Ok(())
    }

    fn schedule_restart(&mut self) {
        let index = self.restart_attempt.min(RESTART_BACKOFF_SECS.len() - 1);
        let delay = RESTART_BACKOFF_SECS[index];
        self.restart_attempt = self.restart_attempt.saturating_add(1);
        self.next_restart_at = Some(Instant::now() + Duration::from_secs(delay));
    }

    fn restart_due(&self) -> bool {
        self.next_restart_at
            .is_none_or(|when| Instant::now() >= when)
    }

    fn retry_in_ms(&self) -> Option<u128> {
        self.next_restart_at
            .map(|when| when.saturating_duration_since(Instant::now()).as_millis())
    }

    fn assert_synced_main(&self) -> HostControlResult<()> {
        self.assert_repo(true, true)?;
        self.git(&["fetch", "--prune", "origin", "main"])?;

        let head = self.git(&["rev-parse", "HEAD"])?;
        let origin_main = self.git(&["rev-parse", "origin/main"])?;
        if head != origin_main {
            return Err(HostControlError::MainNotSynced);
        }

        Ok(())
    }

    fn assert_repo(&self, require_clean: bool, require_main: bool) -> HostControlResult<()> {
        if !self.repo_root.join(".git").is_dir() {
            return Err(HostControlError::RepositoryMissing);
        }

        let remote = self.git(&["remote", "get-url", "origin"])?;
        if !ALLOWED_REMOTES.contains(&remote.as_str()) {
            return Err(HostControlError::OriginMismatch);
        }

        if require_clean && !self.git(&["status", "--porcelain"])?.trim().is_empty() {
            return Err(HostControlError::RepositoryDirty);
        }

        if require_main && self.git(&["rev-parse", "--abbrev-ref", "HEAD"])? != "main" {
            return Err(HostControlError::BranchMismatch);
        }

        Ok(())
    }

    fn require_agent_binary(&self) -> HostControlResult<()> {
        if self.agent_binary().is_file() {
            Ok(())
        } else {
            Err(HostControlError::AgentBinaryMissing)
        }
    }

    fn require_agent_stopped(&mut self) -> HostControlResult<()> {
        if self.agent_is_running()? {
            Err(HostControlError::AgentRunning)
        } else {
            Ok(())
        }
    }

    fn agent_is_running(&mut self) -> HostControlResult<bool> {
        let Some(child) = self.agent_child.as_mut() else {
            return Ok(false);
        };

        if child.try_wait()?.is_some() {
            self.agent_child = None;
            self.agent_started_at = None;
            if self.desired_agent == AgentDesired::Running {
                self.schedule_restart();
            }
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn git(&self, args: &[&str]) -> HostControlResult<String> {
        let output = Command::new("git")
            .arg("-C")
            .arg(&self.repo_root)
            .args(args)
            .output()?;

        if !output.status.success() {
            return Err(HostControlError::CommandFailed("git"));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    fn run_logged(
        &self,
        program: &str,
        args: &[&str],
        log_name: &str,
        command_name: &'static str,
    ) -> HostControlResult<()> {
        fs::create_dir_all(self.runtime_dir())?;
        let stdout = File::create(self.runtime_dir().join(log_name))?;
        let stderr = stdout.try_clone()?;

        let status = Command::new(program)
            .args(args)
            .current_dir(&self.repo_root)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .status()?;

        if status.success() {
            Ok(())
        } else {
            Err(HostControlError::CommandFailed(command_name))
        }
    }

    fn local_build_agent_binary(&self) -> PathBuf {
        self.repo_root
            .join("target")
            .join("release")
            .join("okx-agent.exe")
    }

    fn agent_binary(&self) -> PathBuf {
        PathBuf::from(RUNTIME_ROOT).join("okx-agent.exe")
    }

    fn runtime_dir(&self) -> PathBuf {
        PathBuf::from(RUNTIME_ROOT)
    }
}

fn command_available(command: &str) -> bool {
    Command::new(command)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

pub fn install_current_executable(destination: &Path) -> HostControlResult<Value> {
    let source = std::env::current_exe()?;
    if !source.is_file() {
        return Err(HostControlError::InvalidInstallSource);
    }

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }

    if source != destination {
        fs::copy(&source, destination)?;
    }

    Ok(json!({
        "installed": true,
        "destination": destination
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_root_is_fixed() {
        let executor = HostExecutor::canonical().expect("executor");
        assert_eq!(executor.repo_root, PathBuf::from(r"C:\okx"));
    }

    #[test]
    fn remote_allowlist_is_narrow() {
        assert!(ALLOWED_REMOTES.contains(&"https://github.com/iamaman11/okx.git"));
        assert!(!ALLOWED_REMOTES.contains(&"https://github.com/other/okx.git"));
    }

    #[test]
    fn restart_backoff_is_bounded() {
        assert_eq!(RESTART_BACKOFF_SECS, [1, 5, 15, 30, 60]);
    }
}
