use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use okx_protocol::HostControlOperation;
use serde_json::{Value, json};

use crate::{HostControlError, HostControlResult, auth::load_native_github_token};

const CANONICAL_ROOT: &str = r"C:\okx";
const AGENT_MAILBOX_ISSUE: &str = "10";
const ALLOWED_REMOTES: &[&str] = &[
    "https://github.com/iamaman11/okx",
    "https://github.com/iamaman11/okx.git",
    "git@github.com:iamaman11/okx.git",
];

pub struct HostExecutor {
    repo_root: PathBuf,
    agent_child: Option<Child>,
}

impl HostExecutor {
    pub fn canonical() -> Self {
        Self {
            repo_root: PathBuf::from(CANONICAL_ROOT),
            agent_child: None,
        }
    }

    pub fn execute(&mut self, operation: HostControlOperation) -> HostControlResult<Value> {
        match operation {
            HostControlOperation::Status => self.status(),
            HostControlOperation::Sync => self.sync(),
            HostControlOperation::BuildAgent => self.build_agent(),
            HostControlOperation::TestWorkspace => self.test_workspace(),
            HostControlOperation::InitAgentIdentity => self.init_agent_identity(),
            HostControlOperation::AgentIdentity => self.agent_identity(),
            HostControlOperation::BootstrapAgentGithubToken => {
                self.bootstrap_agent_github_token()
            }
            HostControlOperation::StartAgent => self.start_agent(),
            HostControlOperation::StopAgent => self.stop_agent(),
            HostControlOperation::RestartAgent => self.restart_agent(),
            HostControlOperation::TransportStatus => self.transport_status(),
        }
    }

    pub fn shutdown(&mut self) {
        let _ = self.stop_agent();
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
            "agent_owned_running": running
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

        if !self.agent_binary().is_file() {
            return Err(HostControlError::AgentBinaryMissing);
        }

        Ok(json!({
            "head": self.git(&["rev-parse", "HEAD"])?,
            "agent_binary_present": true
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
            return Ok(json!({
                "disposition": "ALREADY_RUNNING"
            }));
        }

        fs::create_dir_all(self.runtime_dir())?;
        let stdout = File::create(self.runtime_dir().join("okx-agent.stdout.log"))?;
        let stderr = File::create(self.runtime_dir().join("okx-agent.stderr.log"))?;

        let child = Command::new(self.agent_binary())
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

        let pid = child.id();
        self.agent_child = Some(child);

        Ok(json!({
            "disposition": "STARTED",
            "pid": pid,
            "mailbox_issue": 10
        }))
    }

    fn stop_agent(&mut self) -> HostControlResult<Value> {
        let Some(mut child) = self.agent_child.take() else {
            return Ok(json!({
                "disposition": "ALREADY_STOPPED"
            }));
        };

        if child.try_wait()?.is_none() {
            child.kill()?;
            child.wait()?;
        }

        Ok(json!({
            "disposition": "STOPPED"
        }))
    }

    fn restart_agent(&mut self) -> HostControlResult<Value> {
        let stop = self.stop_agent()?;
        let start = self.start_agent()?;
        Ok(json!({
            "stop": stop,
            "start": start
        }))
    }

    fn transport_status(&mut self) -> HostControlResult<Value> {
        Ok(json!({
            "host": self.status()?,
            "identity": self.read_agent_identity()?
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

    fn agent_binary(&self) -> PathBuf {
        self.repo_root.join("target").join("release").join("okx-agent.exe")
    }

    fn runtime_dir(&self) -> PathBuf {
        self.repo_root.join(".runtime")
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
        let executor = HostExecutor::canonical();
        assert_eq!(executor.repo_root, PathBuf::from(r"C:\okx"));
    }

    #[test]
    fn remote_allowlist_is_narrow() {
        assert!(ALLOWED_REMOTES.contains(&"https://github.com/iamaman11/okx.git"));
        assert!(!ALLOWED_REMOTES.contains(&"https://github.com/other/okx.git"));
    }
}
