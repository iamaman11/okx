use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, Stdio},
    time::{Duration, Instant},
};

use okx_protocol::HostControlOperation;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

use crate::{
    HostControlError, HostControlResult,
    auth::load_native_github_token,
    autostart,
    background_process::hidden_command,
    controller_update,
    desired::{AgentDesired, AgentDesiredState, AgentProfile, DesiredStateStore},
    job::AgentJob,
    provenance::InstalledAgentProvenanceStore,
};

const CANONICAL_ROOT: &str = r"C:\okx";
const RUNTIME_ROOT: &str = r"C:\okx-runtime";
const AGENT_MAILBOX_ISSUE: &str = "10";
const DEMO_MAILBOX_ISSUE: &str = "234";
const DEMO_RUNTIME_ROOT: &str = r"C:\okx-runtime\demo";
const AGENT_CLOUDFLARE_WS_URL: &str = "wss://okx-cloudflare-mcp.okx-794.workers.dev/runtime";
const AGENT_CLOUDFLARE_RUNTIME_ID: &str = "windows-primary";
const HOST_CONTROL_CAPABILITIES_SCHEMA_V1: &str = "okx.host-control.capabilities/v1";
const HEALTHY_AGENT_SECS: u64 = 30;
const RESTART_BACKOFF_SECS: [u64; 5] = [1, 5, 15, 30, 60];
const WORKSPACE_STATUS_MAX_CHANGES: usize = 16;
const WORKSPACE_STATUS_MAX_CHANGE_BYTES: usize = 512;
const RUNTIME_DIAGNOSTIC_TAIL_BYTES: u64 = 64 * 1024;
const ALLOWED_REMOTES: &[&str] = &[
    "https://github.com/iamaman11/okx",
    "https://github.com/iamaman11/okx.git",
];

#[derive(Clone, Copy, Debug)]
struct AgentLaunchEvidence {
    profile: AgentProfile,
    pid: u32,
    stdout_offset: u64,
    stderr_offset: u64,
}

pub struct HostExecutor {
    repo_root: PathBuf,
    agent_child: Option<Child>,
    agent_started_at: Option<Instant>,
    agent_launch: Option<AgentLaunchEvidence>,
    agent_job: AgentJob,
    desired_store: DesiredStateStore,
    desired_agent: AgentDesired,
    desired_profile: AgentProfile,
    running_profile: Option<AgentProfile>,
    desired_error: Option<String>,
    restart_attempt: usize,
    next_restart_at: Option<Instant>,
    last_reconcile: String,
}

impl HostExecutor {
    pub fn canonical() -> HostControlResult<Self> {
        let desired_store = DesiredStateStore::canonical();
        let (desired_state, desired_error) = match desired_store.load() {
            Ok(desired) => (desired, None),
            Err(error) => (AgentDesiredState::stopped(), Some(error.to_string())),
        };

        Ok(Self {
            repo_root: PathBuf::from(CANONICAL_ROOT),
            agent_child: None,
            agent_started_at: None,
            agent_launch: None,
            agent_job: AgentJob::new()?,
            desired_store,
            desired_agent: desired_state.agent,
            desired_profile: desired_state.profile,
            running_profile: None,
            desired_error,
            restart_attempt: 0,
            next_restart_at: None,
            last_reconcile: "NOT_RUN".to_owned(),
        })
    }

    pub fn execute(&mut self, operation: HostControlOperation) -> HostControlResult<Value> {
        self.profile_transition_gate(&operation)?;
        match operation {
            HostControlOperation::Status => self.status(),
            HostControlOperation::Sync => self.sync(),
            HostControlOperation::BuildAgent => Err(HostControlError::InvalidExecutionPath),
            HostControlOperation::DeployAgent { .. }
            | HostControlOperation::InstallLauncherRoot { .. }
            | HostControlOperation::UpgradeLauncherRoot { .. }
            | HostControlOperation::StageControllerUpdate { .. }
            | HostControlOperation::HandoffControllerUpdate => {
                Err(HostControlError::InvalidExecutionPath)
            }
            HostControlOperation::AbortControllerUpdate => controller_update::abort(),
            HostControlOperation::ControllerUpdateStatus => Ok(controller_update::status_value()),
            HostControlOperation::WorkspaceStatus => self.workspace_status(),
            HostControlOperation::TestWorkspace => self.test_workspace(),
            HostControlOperation::InitAgentIdentity => self.init_agent_identity(),
            HostControlOperation::AgentIdentity => self.agent_identity(),
            HostControlOperation::BootstrapAgentGithubToken => self.bootstrap_agent_github_token(),
            HostControlOperation::ProvisionCloudflareRuntimeToken => {
                self.provision_cloudflare_runtime_token()
            }
            HostControlOperation::StartAgent => self.start_agent(),
            HostControlOperation::StartDemoAcceptance => self.start_demo_acceptance(),
            HostControlOperation::RestoreProductionAgent => self.restore_production_agent(),
            HostControlOperation::StopAgent => self.stop_agent(),
            HostControlOperation::RestartAgent => self.restart_agent(),
            HostControlOperation::InstallAutostart => autostart::install(),
            HostControlOperation::AutostartStatus => autostart::status_value(),
            HostControlOperation::HandoffToAutostart
            | HostControlOperation::AcceptanceCrashController => {
                Err(HostControlError::InvalidExecutionPath)
            }
            HostControlOperation::AcceptanceKillAgent => self.acceptance_kill_agent(),
            HostControlOperation::AcceptanceFailNextControllerActivation => {
                Ok(okx_host_launcher::arm_fail_next_activation()?)
            }
            HostControlOperation::TransportStatus => self.transport_status(),
        }
    }

    /// Single common gate for both ordinary CONTROL dispatch and privileged
    /// runtime operations (release install, launcher and controller handoff).
    /// A corrupt file is not an absent file: its read returns an error.
    pub fn profile_transition_gate(
        &self,
        operation: &HostControlOperation,
    ) -> HostControlResult<()> {
        if matches!(
            operation,
            HostControlOperation::Status
                | HostControlOperation::TransportStatus
                | HostControlOperation::ControllerUpdateStatus
                | HostControlOperation::AgentIdentity
                | HostControlOperation::AutostartStatus
        ) {
            return Ok(());
        }
        if self.desired_store.is_pending()? {
            return Err(HostControlError::ProfileTransitionPending);
        }
        Ok(())
    }

    pub fn shutdown(&mut self) {
        let _ = self.terminate_agent_owned();
    }

    pub fn reconcile_desired(&mut self) -> HostControlResult<Value> {
        // A pending profile journal outlives controller crashes. Never
        // implicitly stop or respawn either owner across an unknown handoff.
        if self.desired_store.is_pending()? {
            self.last_reconcile = "BLOCKED_UNFINALIZED_PROFILE_TRANSITION".to_owned();
            return Ok(json!({
                "disposition": self.last_reconcile,
                "reason": HostControlError::ProfileTransitionPending.code(),
                "desired_profile": self.desired_profile,
            }));
        }
        let running = self.agent_is_running()?;

        if let Some(error) = self.desired_error.clone() {
            // Corrupt desired state cannot certify a safe exit. Leave any
            // already owned live process supervising its venue protection.
            self.last_reconcile = "DEGRADED_DESIRED_STATE".to_owned();
            return Ok(json!({
                "disposition": self.last_reconcile.clone(),
                "desired": AgentDesired::Stopped,
                "error": error
            }));
        }

        // A mismatch must never terminate a still-running owner. The desired state
        // can be restored after a controller crash, so this guard belongs in
        // automatic reconciliation as well as in the manual profile command.
        // No verified native quiescence witness exists yet: fail closed.
        if running && self.running_profile != Some(self.desired_profile) {
            self.last_reconcile = "BLOCKED_PROFILE_HANDOFF_NO_QUIESCENCE_PROOF".to_owned();
            return Ok(json!({
                "disposition": self.last_reconcile,
                "reason": HostControlError::ProfileHandoffProofRequired.code(),
                "running_profile": self.running_profile,
                "desired_profile": self.desired_profile
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
                    match self.start_agent_process(self.desired_profile) {
                        Ok(pid) => {
                            self.last_reconcile =
                                format!("RESTORED_{:?}_AGENT_PID_{pid}", self.desired_profile);
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
        let agent_binary = self.agent_binary();
        let installed_agent =
            InstalledAgentProvenanceStore::canonical().status_value(&agent_binary);
        // Read-only release identity: the controller must not mistake an
        // independently updated agent for the same tested release set.
        // Same-profile recovery is deliberately not gated by this diagnostic.
        let launcher = okx_host_launcher::status_value();
        let current_controller_sha256 = std::env::current_exe()
            .ok()
            .and_then(|path| okx_host_launcher::sha256_file(&path).ok());
        let release_parity = controller_agent_release_parity(
            &installed_agent,
            &launcher,
            current_controller_sha256.as_deref(),
        );
        let owned_launch = if running {
            self.agent_launch.as_ref()
        } else {
            None
        };

        Ok(json!({
            "workspace": {
                "repo_root": CANONICAL_ROOT,
                "repo_present": repo_present,
                "repository": "iamaman11/okx",
                "head": head,
                "branch": branch,
                "clean": clean,
                "origin": origin,
                "cargo_available": command_available("cargo")
            },
            "installed_agent": installed_agent,
            "release_parity": release_parity,
            "agent_binary_present": agent_binary.is_file(),
            "agent_owned_running": running,
            "agent_pid": if running { self.agent_child.as_ref().map(Child::id) } else { None },
            "agent_desired": self.desired_agent,
            "desired_profile": self.desired_profile,
            "running_profile": self.running_profile,
            "desired_state_path": self.desired_store.path(),
            "desired_state_error": self.desired_error.clone(),
            "profile_transition": self.desired_store.status_value(),
            "job_object_owned": true,
            "restart_attempt": self.restart_attempt,
            "retry_in_ms": self.retry_in_ms(),
            "last_reconcile": self.last_reconcile.clone(),
            "runtime_diagnostics": {
                "production": runtime_log_summary(AgentProfile::Production, owned_launch),
                "demo_acceptance": runtime_log_summary(AgentProfile::DemoAcceptance, owned_launch)
            }
        }))
    }

    fn workspace_status(&self) -> HostControlResult<Value> {
        self.assert_repo(false, false)?;
        self.git(&["fetch", "--prune", "origin", "main"])?;

        let head = self.git(&["rev-parse", "HEAD"])?;
        let branch = self.git(&["rev-parse", "--abbrev-ref", "HEAD"])?;
        let origin_main = self.git(&["rev-parse", "origin/main"])?;
        let divergence =
            self.git(&["rev-list", "--left-right", "--count", "HEAD...origin/main"])?;
        let status = self.git(&["status", "--porcelain=v1"])?;
        let change_lines = status
            .lines()
            .filter(|line| !line.trim().is_empty())
            .collect::<Vec<_>>();
        let changes_total = change_lines.len();
        let changes = change_lines
            .iter()
            .take(WORKSPACE_STATUS_MAX_CHANGES)
            .map(|line| bounded_utf8(line, WORKSPACE_STATUS_MAX_CHANGE_BYTES))
            .collect::<Vec<_>>();
        let truncated = changes_total > changes.len()
            || change_lines
                .iter()
                .take(WORKSPACE_STATUS_MAX_CHANGES)
                .any(|line| line.len() > WORKSPACE_STATUS_MAX_CHANGE_BYTES);

        Ok(json!({
            "repo_root": CANONICAL_ROOT,
            "branch": branch,
            "head": head,
            "origin_main": origin_main,
            "divergence_left_right": divergence,
            "clean": changes_total == 0,
            "changes_total": changes_total,
            "changes_returned": changes.len(),
            "changes": changes,
            "changes_truncated": truncated
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

        let mut child = hidden_command(self.agent_binary())
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

    fn provision_cloudflare_runtime_token(&mut self) -> HostControlResult<Value> {
        self.require_agent_binary()?;

        let mut random = [0u8; 32];
        getrandom::fill(&mut random)
            .map_err(|error| HostControlError::Random(error.to_string()))?;

        let mut token = String::with_capacity(64);
        for byte in random {
            use std::fmt::Write as _;
            write!(&mut token, "{byte:02x}")
                .map_err(|error| HostControlError::Random(error.to_string()))?;
        }

        let token_sha256 = format!("{:x}", Sha256::digest(token.as_bytes()));
        let mut child = match hidden_command(self.agent_binary())
            .arg("set-cloudflare-token")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                token.zeroize();
                return Err(error.into());
            }
        };

        let write_result = (|| -> std::io::Result<()> {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "okx-agent set-cloudflare-token stdin unavailable",
                )
            })?;
            stdin.write_all(token.as_bytes())?;
            stdin.write_all(b"\n")?;
            Ok(())
        })();
        token.zeroize();
        write_result?;

        let status = child.wait()?;
        if !status.success() {
            return Err(HostControlError::CommandFailed(
                "okx-agent set-cloudflare-token",
            ));
        }

        Ok(json!({
            "stored": true,
            "destination": "Windows Credential Manager",
            "runtime_id": AGENT_CLOUDFLARE_RUNTIME_ID,
            "runtime_token_sha256": token_sha256
        }))
    }

    fn start_agent(&mut self) -> HostControlResult<Value> {
        self.switch_agent_profile(AgentProfile::Production)
    }

    fn start_demo_acceptance(&mut self) -> HostControlResult<Value> {
        self.switch_agent_profile(AgentProfile::DemoAcceptance)
    }

    fn restore_production_agent(&mut self) -> HostControlResult<Value> {
        self.switch_agent_profile(AgentProfile::Production)
    }

    fn switch_agent_profile(&mut self, profile: AgentProfile) -> HostControlResult<Value> {
        if self.desired_store.is_pending()? {
            return Err(HostControlError::ProfileTransitionPending);
        }
        // The CONTROL command is not a flat-account witness. Do not persist a
        // different desired profile before proof, even if the child is down:
        // old exchange exposure or an UNKNOWN durable intent may survive.
        let running = self.agent_is_running()?;
        ensure_profile_unchanged(self.desired_profile, self.running_profile, running, profile)?;
        self.require_agent_binary()?;
        if running && self.running_profile == Some(profile) {
            self.persist_desired(AgentDesiredState {
                agent: AgentDesired::Running,
                profile,
            })?;
            return Ok(json!({
                "disposition": "ALREADY_RUNNING",
                "desired": self.desired_agent,
                "profile": profile,
                "mailbox_issue": mailbox_issue(profile),
                "runtime_root": runtime_dir(profile),
                "cloudflare_attached": true
            }));
        }

        self.persist_desired(AgentDesiredState {
            agent: AgentDesired::Running,
            profile,
        })?;
        if running {
            self.terminate_agent_owned()?;
        }

        let pid = match self.start_agent_process(profile) {
            Ok(pid) => pid,
            Err(error) => {
                // A failed Demo launch is NOT authority to start Production.
                // Keep the committed environment for same-profile recovery.
                self.schedule_restart();
                return Err(error);
            }
        };
        self.restart_attempt = 0;
        self.next_restart_at = None;

        Ok(json!({
            "disposition": "STARTED",
            "pid": pid,
            "desired": self.desired_agent,
            "profile": profile,
            "mailbox_issue": mailbox_issue(profile),
            "runtime_root": runtime_dir(profile),
            "cloudflare_attached": true
        }))
    }

    fn stop_agent(&mut self) -> HostControlResult<Value> {
        self.profile_transition_gate(&HostControlOperation::StopAgent)?;
        let was_running = self.agent_is_running()?;
        ensure_profile_unchanged(
            self.desired_profile,
            self.running_profile,
            was_running,
            self.desired_profile,
        )?;
        // STOP is not a change of environment; preserve the Demo identity so
        // a later START/RESTART cannot silently become Production.
        self.persist_desired(AgentDesiredState::stopped_on(self.desired_profile))?;
        if was_running {
            self.terminate_agent_owned()?;
        }
        self.restart_attempt = 0;
        self.next_restart_at = None;

        Ok(json!({
            "disposition": if was_running { "STOPPED" } else { "ALREADY_STOPPED" },
            "desired": self.desired_agent,
            "profile": self.desired_profile
        }))
    }

    fn restart_agent(&mut self) -> HostControlResult<Value> {
        if self.desired_store.is_pending()? {
            return Err(HostControlError::ProfileTransitionPending);
        }
        let profile = self.desired_profile;
        let running = self.agent_is_running()?;
        ensure_profile_unchanged(self.desired_profile, self.running_profile, running, profile)?;
        self.persist_desired(AgentDesiredState {
            agent: AgentDesired::Running,
            profile,
        })?;
        // A failed termination is not proof that the previous owner exited.
        // Never launch a second agent on this uncertain result.
        self.terminate_agent_owned()?;
        let pid = match self.start_agent_process(profile) {
            Ok(pid) => pid,
            Err(error) => {
                self.schedule_restart();
                return Err(error);
            }
        };
        self.restart_attempt = 0;
        self.next_restart_at = None;

        Ok(json!({
            "disposition": "RESTARTED",
            "pid": pid,
            "desired": self.desired_agent,
            "profile": profile,
            "mailbox_issue": mailbox_issue(profile),
            "runtime_root": runtime_dir(profile),
            "cloudflare_attached": true
        }))
    }

    fn acceptance_kill_agent(&mut self) -> HostControlResult<Value> {
        self.profile_transition_gate(&HostControlOperation::AcceptanceKillAgent)?;
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
            "autostart": autostart::status_value()?,
            "controller_update": controller_update::status_value(),
            "controller_capabilities": {
                "schema": HOST_CONTROL_CAPABILITIES_SCHEMA_V1,
                "verified_self_update": true,
                "bounded_control_results": true,
                "bounded_workspace_status": true,
                "self_update_acceptance_marker": "production-baseline-v1",
                "scheduler_action_handoff": false,
                "transient_activator_handoff": false,
                "immutable_launcher_root": true,
                "controller_self_overwrite": false,
                "scheduler_mutation_during_update": false,
                "abort_controller_update": true,
                "acceptance_fail_next_controller_activation": true,
                "background_process_windows": "CREATE_NO_WINDOW",
                "launcher_console_subsystem": false,
                "remote_launcher_root_upgrade": true,
                "launcher": okx_host_launcher::status_value(),
                "cloudflare_direct_transport": {
                    "configured": true,
                    "runtime_id": AGENT_CLOUDFLARE_RUNTIME_ID,
                    "ws_url": AGENT_CLOUDFLARE_WS_URL,
                    "github_fallback_preserved": true
                },
                "agent_profiles": {
                    "production": {
                        "runtime_root": RUNTIME_ROOT,
                        "mailbox_issue": 10,
                        "cloudflare_attached": true
                    },
                    "demo_acceptance": {
                        "runtime_root": DEMO_RUNTIME_ROOT,
                        "mailbox_issue": 234,
                        "cloudflare_attached": true,
                        "direct_transport_read_only": true,
                        "mutation_acceptance_explicit": true
                    },
                    "single_owner": true
                }
            }
        }))
    }

    fn read_agent_identity(&self) -> HostControlResult<Value> {
        self.require_agent_binary()?;
        let output = self.agent_output(&["identity"])?;
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn agent_output(&self, args: &[&str]) -> HostControlResult<std::process::Output> {
        let output = hidden_command(self.agent_binary())
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
        if self.desired_store.is_pending()? {
            return Err(HostControlError::ProfileTransitionPending);
        }
        let should_restore = self.desired_agent == AgentDesired::Running;
        let running = self.agent_is_running()?;
        ensure_profile_unchanged(
            self.desired_profile,
            self.running_profile,
            running,
            self.desired_profile,
        )?;
        if running {
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

        if should_restore && let Err(error) = self.start_agent_process(self.desired_profile) {
            self.schedule_restart();
            return Err(error);
        }

        Ok(())
    }

    fn start_agent_process(&mut self, profile: AgentProfile) -> HostControlResult<u32> {
        // No normal lifecycle may spawn a second account owner while an
        // earlier profile transition is frozen or consumed.
        if self.desired_store.is_pending()? {
            return Err(HostControlError::ProfileTransitionPending);
        }
        self.require_agent_binary()?;
        let runtime_dir = runtime_dir(profile);
        fs::create_dir_all(&runtime_dir)?;

        let stdout = OpenOptions::new()
            .create(true)
            .append(true)
            .open(runtime_dir.join("okx-agent.stdout.log"))?;
        let stderr = OpenOptions::new()
            .create(true)
            .append(true)
            .open(runtime_dir.join("okx-agent.stderr.log"))?;

        // Capture append-only log boundaries before launching the owned child.
        // Old profile tails must never be presented as this process's errors.
        let stdout_offset = stdout.metadata()?.len();
        let stderr_offset = stderr.metadata()?.len();
        let mut child = hidden_command(self.agent_binary())
            .args(agent_args(profile))
            .current_dir(&self.repo_root)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;

        self.agent_job.assign(&mut child)?;
        let pid = child.id();
        self.agent_child = Some(child);
        self.agent_started_at = Some(Instant::now());
        self.agent_launch = Some(AgentLaunchEvidence {
            profile,
            pid,
            stdout_offset,
            stderr_offset,
        });
        self.running_profile = Some(profile);
        self.next_restart_at = None;
        Ok(pid)
    }

    fn terminate_agent_owned(&mut self) -> HostControlResult<()> {
        let Some(mut child) = self.agent_child.take() else {
            self.agent_started_at = None;
            self.agent_launch = None;
            return Ok(());
        };

        if child.try_wait()?.is_none() {
            child.kill()?;
            child.wait()?;
        }
        self.agent_started_at = None;
        self.agent_launch = None;
        self.running_profile = None;
        Ok(())
    }

    fn persist_desired(&mut self, state: AgentDesiredState) -> HostControlResult<()> {
        self.desired_store.save(state)?;
        self.desired_agent = state.agent;
        self.desired_profile = state.profile;
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
            self.agent_launch = None;
            self.running_profile = None;
            if self.desired_agent == AgentDesired::Running {
                self.schedule_restart();
            }
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn git(&self, args: &[&str]) -> HostControlResult<String> {
        let output = direct_git_command(&self.repo_root, args).output()?;

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

        let status = hidden_command(program)
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
        PathBuf::from(RUNTIME_ROOT).join("okx-agent.exe")
    }

    fn runtime_dir(&self) -> PathBuf {
        PathBuf::from(RUNTIME_ROOT)
    }
}

const PRODUCTION_AGENT_ARGS: &[&str] = &[
    "run",
    "--mailbox-issue",
    AGENT_MAILBOX_ISSUE,
    "--cloudflare-ws-url",
    AGENT_CLOUDFLARE_WS_URL,
    "--cloudflare-runtime-id",
    AGENT_CLOUDFLARE_RUNTIME_ID,
];

const DEMO_ACCEPTANCE_AGENT_ARGS: &[&str] = &[
    "--root",
    DEMO_RUNTIME_ROOT,
    "--demo",
    "run",
    "--mailbox-issue",
    DEMO_MAILBOX_ISSUE,
    "--demo-mutation-acceptance",
    "--cloudflare-ws-url",
    AGENT_CLOUDFLARE_WS_URL,
    "--cloudflare-runtime-id",
    AGENT_CLOUDFLARE_RUNTIME_ID,
];

fn agent_args(profile: AgentProfile) -> &'static [&'static str] {
    match profile {
        AgentProfile::Production => PRODUCTION_AGENT_ARGS,
        AgentProfile::DemoAcceptance => DEMO_ACCEPTANCE_AGENT_ARGS,
    }
}

fn read_log_tail(path: &Path) -> std::io::Result<(u64, String)> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let start = size.saturating_sub(RUNTIME_DIAGNOSTIC_TAIL_BYTES);
    file.seek(SeekFrom::Start(start))?;
    let mut bytes = Vec::with_capacity((size - start) as usize);
    file.read_to_end(&mut bytes)?;
    Ok((size, String::from_utf8_lossy(&bytes).into_owned()))
}

fn last_runtime_event(stdout: &str) -> Option<Value> {
    stdout.lines().rev().find_map(|line| {
        let value: Value = serde_json::from_str(line).ok()?;
        if value.get("schema")?.as_str()? != "okx.agent.runtime/v1" {
            return None;
        }
        Some(json!({
            "state": value.get("state").and_then(Value::as_str),
            "root": value.get("root").and_then(Value::as_str),
            "mailbox_issue": value.get("mailbox_issue")
        }))
    })
}

fn extract_okx_api_code(line: &str) -> Option<String> {
    if let Some(rest) = line.split("OKX API error ").nth(1) {
        return rest
            .split(':')
            .next()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned);
    }
    let rest = line.split("code:").nth(1)?;
    let trimmed = rest.trim_start().trim_start_matches('"');
    let end = trimmed.find(['"', ',', '}']).unwrap_or(trimmed.len());
    let code = trimmed[..end].trim();
    (!code.is_empty()).then(|| code.to_owned())
}

fn fixed_error_code(line: &str) -> Option<&'static str> {
    const CODES: &[(&str, &str)] = &[
        ("EmptySourceTimestamp", "empty_source_timestamp"),
        ("UnsupportedInstrumentType", "unsupported_instrument_type"),
        ("MissingRequiredField", "missing_required_field"),
        ("DuplicateInstrument", "duplicate_instrument"),
        (
            "UnsupportedUpcomingParameter",
            "unsupported_upcoming_parameter",
        ),
        ("MalformedUpcomingParameter", "malformed_upcoming_parameter"),
        ("Serialization(", "serialization"),
        ("IdentityNotFound", "identity_not_found"),
        ("InvalidPrivateKeyLength", "invalid_private_key_length"),
        ("GithubTokenNotFound", "github_token_not_found"),
        ("InvalidGithubToken", "invalid_github_token"),
        (
            "DemoOkxCredentialsNotFound",
            "demo_observer_credential_not_found",
        ),
        (
            "InvalidDemoOkxCredentials",
            "invalid_demo_observer_credential",
        ),
        (
            "DemoExecutorOkxCredentialsNotFound",
            "demo_executor_credential_not_found",
        ),
        (
            "InvalidDemoExecutorOkxCredentials",
            "invalid_demo_executor_credential",
        ),
        (
            "DemoMutationAcceptanceRequiresDemoEnvironment",
            "demo_mutation_requires_demo",
        ),
        ("InvalidMailboxIssue", "invalid_mailbox_issue"),
        ("InvalidPollInterval", "invalid_poll_interval"),
    ];
    CODES
        .iter()
        .find_map(|(needle, code)| line.contains(needle).then_some(*code))
}

fn missing_required_field(line: &str) -> Option<&'static str> {
    ["instId", "state", "tickSz", "lotSz", "minSz"]
        .into_iter()
        .find(|field| {
            line.contains(&format!(r#"field: "{field}""#))
                || line.contains(&format!("required field '{field}'"))
        })
}

fn fatal_error_summary(stderr: &str) -> Option<Value> {
    let line = stderr.lines().rev().find(|line| line.contains("Error:"))?;

    let (class, code) = if line.contains("Reference(") || line.contains("reference data error") {
        ("reference_data", fixed_error_code(line))
    } else if line.contains("SecretStore(") || line.contains("native secret store error") {
        ("native_secret_store", Some("secret_store"))
    } else if line.contains("IdentityNotFound")
        || line.contains("InvalidPrivateKeyLength")
        || line.contains("agent identity")
    {
        ("identity", fixed_error_code(line))
    } else if line.contains("GithubTokenNotFound") || line.contains("InvalidGithubToken") {
        ("github_token", fixed_error_code(line))
    } else if line.contains("Github(") || line.contains("GitHub transport error") {
        ("github", None)
    } else if line.contains("Okx(Api")
        || line.contains("OKX API error")
        || line.contains("Api { code:")
    {
        ("okx_api", None)
    } else if line.contains("Okx(Config") || line.contains("configuration error") {
        ("okx_config", None)
    } else if line.contains("Okx(Response") || line.contains("OKX response error") {
        ("okx_response", None)
    } else if line.contains("Okx(Http") || line.contains("HTTP error") || line.contains("Http(") {
        ("http", None)
    } else if line.contains("Json(") || line.contains("JSON error") {
        ("json", None)
    } else if line.contains("Io(") || line.contains("I/O error") {
        ("io", None)
    } else if line.contains("DemoOkxCredentialsNotFound")
        || line.contains("InvalidDemoOkxCredentials")
        || line.contains("DemoExecutorOkxCredentialsNotFound")
        || line.contains("InvalidDemoExecutorOkxCredentials")
    {
        ("demo_credentials", fixed_error_code(line))
    } else if line.contains("DemoMutationAcceptanceRequiresDemoEnvironment")
        || line.contains("InvalidMailboxIssue")
        || line.contains("InvalidPollInterval")
    {
        ("startup_config", fixed_error_code(line))
    } else if line.contains("ResearchSession") || line.contains("research session") {
        ("research_session", None)
    } else {
        ("other", fixed_error_code(line))
    };

    Some(json!({
        "class": class,
        "code": code,
        "missing_field": if code == Some("missing_required_field") {
            missing_required_field(line)
        } else {
            None
        },
        "okx_api_code": if class == "okx_api" { extract_okx_api_code(line) } else { None }
    }))
}

fn read_owned_launch_tail(path: &Path, start_offset: u64) -> Option<(String, bool)> {
    let mut file = File::open(path).ok()?;
    let size = file.metadata().ok()?.len();
    // Rotation, replacement, deletion, or truncation invalidates the lease.
    if size < start_offset {
        return None;
    }
    let start = start_offset.max(size.saturating_sub(RUNTIME_DIAGNOSTIC_TAIL_BYTES));
    file.seek(SeekFrom::Start(start)).ok()?;
    let expected_bytes = size - start;
    let mut bytes = Vec::with_capacity(expected_bytes as usize);
    let received_bytes = file.take(expected_bytes).read_to_end(&mut bytes).ok()?;
    if received_bytes as u64 != expected_bytes {
        return None;
    }
    let mut tail = String::from_utf8_lossy(&bytes).into_owned();
    if start > start_offset {
        // A tail can start mid-line. Do not parse such a fragment as an event.
        tail = tail
            .split_once('\n')
            .map_or(String::new(), |(_, rest)| rest.to_owned());
    }
    Some((tail, start == start_offset))
}

fn current_launch_summary(root: &Path, launch: &AgentLaunchEvidence) -> Value {
    let stdout = read_owned_launch_tail(&root.join("okx-agent.stdout.log"), launch.stdout_offset);
    let stderr = read_owned_launch_tail(&root.join("okx-agent.stderr.log"), launch.stderr_offset);
    let fully_observed = stdout.as_ref().is_some_and(|(_, complete)| *complete)
        && stderr.as_ref().is_some_and(|(_, complete)| *complete);
    let stdout_text = stdout.as_ref().map_or("", |(text, _)| text.as_str());
    let stderr_text = stderr.as_ref().map_or("", |(text, _)| text.as_str());
    json!({
        "pid": launch.pid,
        "profile": launch.profile,
        "log_evidence": {
            "scope": "owned_launch_file_offsets",
            "current_process_scoped": stdout.is_some() && stderr.is_some(),
            "current_startup_attributed": fully_observed,
            "complete_since_launch": fully_observed
        },
        // A cropped / rotated log cannot establish absence of an earlier error.
        "last_runtime_event": if fully_observed { last_runtime_event(stdout_text) } else { None },
        "startup_markers": if fully_observed {
            Some(json!({
                "reference_registry_ready": stderr_text.contains("reference registry ready"),
                "mailbox_repository_verified": stderr_text.contains("mailbox repository identity verified"),
                "observer_credential_missing": stderr_text.contains("OKX observer credential for the selected environment is not provisioned"),
                "executor_credential_missing": stderr_text.contains("OKX executor credential for the selected environment is not provisioned")
            }))
        } else {
            None
        },
        "fatal_error": if fully_observed { fatal_error_summary(stderr_text) } else { None }
    })
}

fn runtime_log_summary(profile: AgentProfile, launch: Option<&AgentLaunchEvidence>) -> Value {
    let root = runtime_dir(profile);
    let mut retained = runtime_log_summary_from_root(root.clone());
    retained["current_process"] = launch
        .filter(|value| value.profile == profile)
        .map_or(Value::Null, |value| current_launch_summary(&root, value));
    retained
}

fn runtime_log_summary_from_root(root: PathBuf) -> Value {
    let stdout_path = root.join("okx-agent.stdout.log");
    let stderr_path = root.join("okx-agent.stderr.log");
    let stdout = read_log_tail(&stdout_path).ok();
    let stderr = read_log_tail(&stderr_path).ok();
    let stdout_text = stdout.as_ref().map(|(_, text)| text.as_str()).unwrap_or("");
    let stderr_text = stderr.as_ref().map(|(_, text)| text.as_str()).unwrap_or("");

    json!({
        "root": root,
        // These retained per-profile files are append-only across agent restarts.
        // They are not scoped to the current PID/launch. Never treat a prior
        // fatal error or startup marker as proof of a current runtime failure.
        "log_evidence": {
            "scope": "retained_profile_file_tail",
            "current_process_scoped": false,
            "current_startup_attributed": false
        },
        "stdout_present": stdout.is_some(),
        "stdout_bytes": stdout.as_ref().map(|(size, _)| *size),
        "stderr_present": stderr.is_some(),
        "stderr_bytes": stderr.as_ref().map(|(size, _)| *size),
        "last_runtime_event": last_runtime_event(stdout_text),
        "startup_markers": {
            "reference_registry_ready": stderr_text.contains("reference registry ready"),
            "mailbox_repository_verified": stderr_text.contains("mailbox repository identity verified"),
            "observer_credential_missing": stderr_text.contains("OKX observer credential for the selected environment is not provisioned"),
            "executor_credential_missing": stderr_text.contains("OKX executor credential for the selected environment is not provisioned")
        },
        "fatal_error": fatal_error_summary(stderr_text)
    })
}

fn mailbox_issue(profile: AgentProfile) -> u64 {
    match profile {
        AgentProfile::Production => 10,
        AgentProfile::DemoAcceptance => 234,
    }
}

fn runtime_dir(profile: AgentProfile) -> PathBuf {
    match profile {
        AgentProfile::Production => PathBuf::from(RUNTIME_ROOT),
        AgentProfile::DemoAcceptance => PathBuf::from(DEMO_RUNTIME_ROOT),
    }
}

fn bounded_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }

    let mut end = max_bytes.min(value.len());
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn command_available(command: &str) -> bool {
    hidden_command(command)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Until the current agent can attest a fresh quiescent account+ledger state,
/// a raw CONTROL operation cannot change the bound exchange environment.
/// This is intentionally narrower than live trading admission.
fn ensure_profile_unchanged(
    committed: AgentProfile,
    running: Option<AgentProfile>,
    process_alive: bool,
    requested: AgentProfile,
) -> HostControlResult<()> {
    if committed != requested
        || running.is_some_and(|active| active != requested)
        || (process_alive && running != Some(requested))
    {
        return Err(HostControlError::ProfileHandoffProofRequired);
    }
    Ok(())
}

/// One read-only provenance check, not a profile-switch authority.
/// Versions can legitimately differ while serving the same profile, but a
/// cross-profile transition must never infer a tested release pair from
/// separately installed binaries. No runtime, ledger or network mutation.
fn controller_agent_release_parity(
    agent: &Value,
    launcher: &Value,
    running_controller_sha256: Option<&str>,
) -> Value {
    let mut blockers = Vec::<&str>::new();
    let agent_tree = agent
        .pointer("/provenance/source_tree")
        .and_then(Value::as_str);
    let controller_tree = launcher
        .pointer("/active/value/active/source_tree")
        .and_then(Value::as_str);
    let expected_controller_sha256 = launcher
        .pointer("/active/value/active/controller_sha256")
        .and_then(Value::as_str);

    if agent.pointer("/state").and_then(Value::as_str) != Some("VERIFIED") {
        blockers.push("AGENT_BINARY_OR_PROVENANCE_UNVERIFIED");
    }
    if launcher.pointer("/active/state").and_then(Value::as_str) != Some("READY")
        || launcher
            .pointer("/entrypoint_is_launcher")
            .and_then(Value::as_bool)
            != Some(true)
    {
        blockers.push("ACTIVE_CONTROLLER_UNVERIFIED");
    }
    if launcher.pointer("/pending/state").and_then(Value::as_str) != Some("NONE") {
        blockers.push("CONTROLLER_ACTIVATION_INCOMPLETE");
    }
    if controller_tree.is_none() || agent_tree.is_none() || controller_tree != agent_tree {
        blockers.push("CONTROLLER_AGENT_SOURCE_TREE_MISMATCH");
    }
    if expected_controller_sha256.is_none()
        || expected_controller_sha256 != running_controller_sha256
    {
        blockers.push("RUNNING_CONTROLLER_BINARY_MISMATCH");
    }

    json!({
        "schema": "okx.host-control.release-parity/v1",
        "state": if blockers.is_empty() { "MATCHED" } else { "BLOCKED" },
        "source_tree": if blockers.is_empty() { agent_tree } else { None },
        "blockers": blockers,
        "handoff_admission": "DENIED_NO_CONTROL_BOUND_ONE_TIME_FENCE",
        "same_profile_recovery_permitted": true
    })
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

/// Git is a verification tool, not a separate transport authority. Its origin/main
/// HTTPS reads must stay direct even when the Windows operator shell is proxied.
/// Keep the override process-local; never modify user or system Git configuration.
fn direct_git_command(repo_root: &Path, args: &[&str]) -> std::process::Command {
    let mut command = hidden_command("git");
    command
        .args([
            "-c",
            "http.proxy=",
            "-c",
            "http.https://github.com/iamaman11/okx.proxy=",
            "-c",
            "http.https://github.com/iamaman11/okx.git.proxy=",
        ])
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        );

    for name in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
        "GIT_PROXY_COMMAND",
        "GIT_CONFIG_COUNT",
    ] {
        command.env_remove(name);
    }

    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_commands_ignore_shell_proxy_and_global_git_configuration() {
        let command = direct_git_command(
            Path::new(r"C:\okx"),
            &["fetch", "--prune", "origin", "main"],
        );
        let arguments = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        for setting in [
            "http.proxy=",
            "http.https://github.com/iamaman11/okx.proxy=",
            "http.https://github.com/iamaman11/okx.git.proxy=",
        ] {
            assert!(
                arguments
                    .windows(2)
                    .any(|pair| pair[0] == "-c" && pair[1] == setting)
            );
        }

        for name in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
            "GIT_PROXY_COMMAND",
            "GIT_CONFIG_COUNT",
        ] {
            assert!(
                command.get_envs().any(|(key, value)| {
                    let same_name = if cfg!(windows) {
                        key.to_string_lossy().eq_ignore_ascii_case(name)
                    } else {
                        key == std::ffi::OsStr::new(name)
                    };
                    same_name && value.is_none()
                }),
                "proxy or inherited Git override not removed: {name}"
            );
        }

        assert!(command.get_envs().any(|(key, value)| {
            key == std::ffi::OsStr::new("GIT_CONFIG_NOSYSTEM")
                && value == Some(std::ffi::OsStr::new("1"))
        }));
        assert!(
            ALLOWED_REMOTES
                .iter()
                .all(|remote| remote.starts_with("https://"))
        );
    }

    #[test]
    fn controller_agent_release_parity_detects_drift_without_authorizing_handoff() {
        let hash = "c".repeat(64);
        let tree = "a".repeat(40);
        let agent = json!({
            "state": "VERIFIED",
            "provenance": {"source_tree": tree}
        });
        let launcher = json!({
            "active": {"state": "READY", "value": {"active": {
                "source_tree": tree, "controller_sha256": hash
            }}},
            "pending": {"state": "NONE"},
            "entrypoint_is_launcher": true
        });
        let matched = controller_agent_release_parity(&agent, &launcher, Some(&hash));
        assert_eq!(matched["state"], "MATCHED");
        assert_eq!(
            matched["handoff_admission"],
            "DENIED_NO_CONTROL_BOUND_ONE_TIME_FENCE"
        );
        assert_eq!(matched["same_profile_recovery_permitted"], true);

        let mut old_controller = launcher.clone();
        old_controller["active"]["value"]["active"]["source_tree"] = json!("b".repeat(40));
        let drift = controller_agent_release_parity(&agent, &old_controller, Some(&hash));
        assert_eq!(drift["state"], "BLOCKED");
        assert!(
            drift["blockers"]
                .as_array()
                .unwrap()
                .contains(&json!("CONTROLLER_AGENT_SOURCE_TREE_MISMATCH"))
        );

        let wrong_binary =
            controller_agent_release_parity(&agent, &launcher, Some(&"d".repeat(64)));
        assert_eq!(wrong_binary["state"], "BLOCKED");
        assert!(
            wrong_binary["blockers"]
                .as_array()
                .unwrap()
                .contains(&json!("RUNNING_CONTROLLER_BINARY_MISMATCH"))
        );

        let mut pending = launcher.clone();
        pending["pending"]["state"] = json!("PENDING");
        assert_eq!(
            controller_agent_release_parity(&agent, &pending, Some(&hash))["state"],
            "BLOCKED"
        );

        let missing = controller_agent_release_parity(
            &json!({"state": "UNKNOWN"}),
            &json!({"active": {"state": "NONE"}}),
            None,
        );
        assert_eq!(missing["state"], "BLOCKED");
    }

    #[test]
    fn profile_handoff_requires_real_quiescence_not_a_control_request() {
        use AgentProfile::{DemoAcceptance as Demo, Production as Prod};

        assert!(ensure_profile_unchanged(Demo, Some(Demo), true, Demo).is_ok());
        assert!(ensure_profile_unchanged(Demo, None, false, Demo).is_ok());
        assert!(ensure_profile_unchanged(Prod, Some(Prod), true, Prod).is_ok());
        for (committed, running, process_alive, requested) in [
            (Demo, Some(Demo), true, Prod),
            (Demo, None, false, Prod),
            (Prod, Some(Prod), true, Demo),
            (Prod, None, false, Demo),
            // A stale persisted target can never stop the still-running source
            // merely because reconcile_desired() sees a profile mismatch.
            (Prod, Some(Demo), true, Prod),
            (Demo, Some(Prod), true, Demo),
            // An observed live process with untrusted/unknown profile identity
            // is NOT equivalent to a stopped agent.
            (Demo, None, true, Demo),
            (Prod, None, true, Prod),
        ] {
            assert!(matches!(
                ensure_profile_unchanged(committed, running, process_alive, requested),
                Err(HostControlError::ProfileHandoffProofRequired)
            ));
        }
    }

    #[test]
    fn raw_profile_switch_refuses_before_binary_or_desired_state_mutation() {
        let mut host = HostExecutor::canonical().expect("controller");
        host.desired_agent = AgentDesired::Running;
        host.desired_profile = AgentProfile::DemoAcceptance;
        assert!(matches!(
            host.execute(HostControlOperation::RestoreProductionAgent),
            Err(HostControlError::ProfileHandoffProofRequired)
        ));
        assert_eq!(host.desired_profile, AgentProfile::DemoAcceptance);
        assert_eq!(host.desired_agent, AgentDesired::Running);
    }

    #[test]
    fn crash_pending_profile_transition_prevents_reconcile_and_manual_restart() {
        use crate::desired::FrozenSourceWitness;
        let path = std::env::temp_dir().join(format!(
            "okx-control-pending-handoff-{}.json",
            std::process::id(),
        ));
        let _ = fs::remove_file(&path);

        let mut host = HostExecutor::canonical().expect("controller");
        host.desired_store = DesiredStateStore::at(path.clone());
        host.desired_store
            .save(AgentDesiredState::demo_acceptance_running())
            .expect("initial single owner");
        host.desired_profile = AgentProfile::DemoAcceptance;
        host.desired_agent = AgentDesired::Running;

        let ticket = FrozenSourceWitness {
            sequence: 1,
            source: AgentProfile::DemoAcceptance,
            destination: AgentProfile::Production,
            source_pid: 321,
            launch_lease_sha256: "a".repeat(64),
            account_uid_fingerprint: "b".repeat(64),
            private_ws_generation: 7,
            ledger_manifest_sha256: "c".repeat(64),
            source_tree: "d".repeat(40),
            agent_sha256: "e".repeat(64),
            controller_sha256: "f".repeat(64),
            source_observed_at_ms: 10_000,
            issued_at_ms: 10_500,
            expires_at_ms: 40_500,
        };
        host.desired_store
            .stage_frozen(ticket, 11_000)
            .expect("atomic source fence");
        let disposition = host.reconcile_desired().expect("bounded blocked readback");
        assert_eq!(
            disposition["disposition"],
            "BLOCKED_UNFINALIZED_PROFILE_TRANSITION"
        );
        assert_eq!(host.desired_profile, AgentProfile::DemoAcceptance);
        assert_eq!(host.desired_agent, AgentDesired::Running);
        assert!(matches!(
            host.execute(HostControlOperation::RestoreProductionAgent),
            Err(HostControlError::ProfileTransitionPending)
        ));
        assert!(matches!(
            host.execute(HostControlOperation::RestartAgent),
            Err(HostControlError::ProfileTransitionPending)
        ));
        // All lateral entrypoints must remain fenced, not just RestartAgent.
        for operation in [
            HostControlOperation::StopAgent,
            HostControlOperation::AcceptanceKillAgent,
            HostControlOperation::HandoffControllerUpdate,
            HostControlOperation::StageControllerUpdate {
                run_id: 1,
                artifact_id: 1,
                expected_source_tree: "d".repeat(40),
            },
            HostControlOperation::HandoffToAutostart,
            HostControlOperation::Sync,
            HostControlOperation::InstallAutostart,
            HostControlOperation::ProvisionCloudflareRuntimeToken,
        ] {
            assert!(matches!(
                host.profile_transition_gate(&operation),
                Err(HostControlError::ProfileTransitionPending)
            ));
        }
        assert!(matches!(
            host.install_verified_agent(b"should-not-be-written"),
            Err(HostControlError::ProfileTransitionPending)
        ));
        // Read-only status remains available for diagnosis and repair.
        for operation in [
            HostControlOperation::Status,
            HostControlOperation::TransportStatus,
            HostControlOperation::ControllerUpdateStatus,
            HostControlOperation::AgentIdentity,
            HostControlOperation::AutostartStatus,
        ] {
            assert!(host.profile_transition_gate(&operation).is_ok());
        }
        let _ = fs::remove_file(&path);
    }

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
    fn remote_local_build_is_not_a_valid_execution_path() {
        let mut executor = HostExecutor::canonical().expect("executor");
        let error = executor
            .execute(HostControlOperation::BuildAgent)
            .expect_err("remote local build must fail closed");
        assert!(matches!(error, HostControlError::InvalidExecutionPath));
    }

    #[test]
    fn cloudflare_transport_endpoint_is_production_okx_account_only() {
        assert_eq!(
            AGENT_CLOUDFLARE_WS_URL,
            "wss://okx-cloudflare-mcp.okx-794.workers.dev/runtime"
        );
        assert!(!AGENT_CLOUDFLARE_WS_URL.contains("pvisakp"));
    }

    #[test]
    fn restart_backoff_is_bounded() {
        assert_eq!(RESTART_BACKOFF_SECS, [1, 5, 15, 30, 60]);
    }

    #[test]
    fn production_agent_launch_uses_agent_owned_data_poll_default() {
        let args = agent_args(AgentProfile::Production);
        assert!(!args.contains(&"--poll-seconds"));
        assert!(args.contains(&"--cloudflare-ws-url"));
        assert!(args.contains(&"--mailbox-issue"));
        assert!(!args.contains(&"--demo"));
        assert_eq!(mailbox_issue(AgentProfile::Production), 10);
        assert_eq!(
            runtime_dir(AgentProfile::Production),
            PathBuf::from(RUNTIME_ROOT)
        );
    }

    #[test]
    fn runtime_log_summary_returns_only_structured_bounded_diagnostics() {
        let root =
            std::env::temp_dir().join(format!("okx-runtime-diagnostics-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let stdout = root.join("stdout.log");
        let stderr = root.join("stderr.log");
        fs::write(
            &stdout,
            concat!(
                "noise\n",
                "{\"schema\":\"okx.agent.runtime/v1\",\"state\":\"DEGRADED_MAILBOX\",",
                "\"root\":\"C:\\\\okx-runtime\\\\demo\",\"key_id\":\"agent-key-1\",",
                "\"public_key\":\"public\",\"mailbox_issue\":234}\n"
            ),
        )
        .expect("stdout");
        fs::write(
            &stderr,
            "reference registry ready generation=x instruments=1\nError: Okx(Api { code: \"50101\", message: \"redacted in summary\" })\n",
        )
        .expect("stderr");

        let (_, stdout_tail) = read_log_tail(&stdout).expect("tail");
        let (_, stderr_tail) = read_log_tail(&stderr).expect("tail");
        let event = last_runtime_event(&stdout_tail).expect("event");
        assert_eq!(event["state"], "DEGRADED_MAILBOX");
        assert_eq!(event["mailbox_issue"], 234);
        assert!(event.get("public_key").is_none());

        let fatal = fatal_error_summary(&stderr_tail).expect("fatal");
        assert_eq!(fatal["class"], "okx_api");
        assert_eq!(fatal["okx_api_code"], "50101");
        assert!(!fatal.to_string().contains("redacted in summary"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn retained_log_error_cannot_be_misreported_as_current_runtime_failure() {
        let root = std::env::temp_dir().join(format!(
            "okx-retained-log-provenance-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        fs::write(
            root.join("okx-agent.stdout.log"),
            "{\"schema\":\"okx.agent.runtime/v1\",\"state\":\"READY\"}\n",
        )
        .expect("stdout");
        fs::write(
            root.join("okx-agent.stderr.log"),
            "Error: Reference(MissingRequiredField { instrument_id: \"NO_EXCHANGE_MUTATION\", field: \"instId\" })\n",
        )
        .expect("stderr");

        let summary = runtime_log_summary_from_root(root.clone());
        assert_eq!(
            summary["log_evidence"]["scope"],
            "retained_profile_file_tail"
        );
        assert_eq!(summary["log_evidence"]["current_process_scoped"], false);
        assert_eq!(summary["log_evidence"]["current_startup_attributed"], false);
        assert_eq!(summary["last_runtime_event"]["state"], "READY");
        assert_eq!(summary["fatal_error"]["code"], "missing_required_field");
        assert_eq!(summary["fatal_error"]["missing_field"], "instId");
        assert!(!summary.to_string().contains("NO_EXCHANGE_MUTATION"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn current_launch_diagnostics_exclude_history_and_fail_closed_on_log_loss() {
        let root = std::env::temp_dir().join(format!(
            "okx-owned-launch-diagnostics-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let stdout = root.join("okx-agent.stdout.log");
        let stderr = root.join("okx-agent.stderr.log");
        fs::write(&stdout, "old runtime event\n").expect("stdout");
        fs::write(
            &stderr,
            "Error: Reference(MissingRequiredField { instrument_id: \"OLD\", field: \"instId\" })\n",
        )
        .expect("old stderr");
        let launch = AgentLaunchEvidence {
            profile: AgentProfile::DemoAcceptance,
            pid: 321,
            stdout_offset: fs::metadata(&stdout).expect("stdout metadata").len(),
            stderr_offset: fs::metadata(&stderr).expect("stderr metadata").len(),
        };
        fs::OpenOptions::new()
            .append(true)
            .open(&stderr)
            .expect("stderr append")
            .write_all(b"reference registry ready\n")
            .expect("new stderr");

        let fresh = current_launch_summary(&root, &launch);
        assert_eq!(fresh["pid"], 321);
        assert_eq!(fresh["log_evidence"]["complete_since_launch"], true);
        assert_eq!(fresh["startup_markers"]["reference_registry_ready"], true);
        assert!(fresh["fatal_error"].is_null());
        assert!(!fresh.to_string().contains("OLD"));
        // The previously accepted retained profile diagnostics are still visible
        // as historical, separately labelled evidence.
        let retained = runtime_log_summary_from_root(root.clone());
        assert_eq!(retained["fatal_error"]["missing_field"], "instId");
        assert_eq!(retained["log_evidence"]["current_process_scoped"], false);

        fs::OpenOptions::new()
            .append(true)
            .open(&stderr)
            .expect("stderr append")
            .write_all(
                b"Error: Reference(MissingRequiredField { instrument_id: \"PRIVATE\", field: \"tickSz\" })\n",
            )
            .expect("new fatal");
        let failed = current_launch_summary(&root, &launch);
        assert_eq!(failed["fatal_error"]["missing_field"], "tickSz");
        assert!(!failed.to_string().contains("PRIVATE"));

        fs::write(&stderr, b"x").expect("rotated");
        let unavailable = current_launch_summary(&root, &launch);
        assert_eq!(unavailable["log_evidence"]["complete_since_launch"], false);
        assert!(unavailable["startup_markers"].is_null());
        assert!(unavailable["fatal_error"].is_null());

        // The bounded tail cannot claim to cover the complete startup.
        let oversized = launch.stderr_offset + RUNTIME_DIAGNOSTIC_TAIL_BYTES + 8;
        fs::write(&stderr, vec![b'x'; oversized as usize]).expect("oversized");
        let truncated = current_launch_summary(&root, &launch);
        assert_eq!(truncated["log_evidence"]["complete_since_launch"], false);
        assert!(truncated["fatal_error"].is_null());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn fatal_error_summary_classifies_early_startup_variants_without_messages() {
        let cases = [
            (
                "Error: Reference(MissingRequiredField { instrument_id: \"secret\", field: \"ctVal\" })",
                "reference_data",
                Some("missing_required_field"),
            ),
            (
                "Error: SecretStore(\"sensitive platform text\")",
                "native_secret_store",
                Some("secret_store"),
            ),
            (
                "Error: IdentityNotFound(\"agent-key-1\")",
                "identity",
                Some("identity_not_found"),
            ),
            (
                "Error: GithubTokenNotFound",
                "github_token",
                Some("github_token_not_found"),
            ),
            (
                "Error: DemoExecutorOkxCredentialsNotFound",
                "demo_credentials",
                Some("demo_executor_credential_not_found"),
            ),
            (
                "Error: DemoMutationAcceptanceRequiresDemoEnvironment",
                "startup_config",
                Some("demo_mutation_requires_demo"),
            ),
        ];

        for (line, class, code) in cases {
            let summary = fatal_error_summary(line).expect("summary");
            assert_eq!(summary["class"], class);
            assert_eq!(summary["code"].as_str(), code);
            assert!(!summary.to_string().contains("sensitive platform text"));
            assert!(!summary.to_string().contains("instrument_id"));
            assert!(!summary.to_string().contains("agent-key-1"));
        }
    }

    #[test]
    fn missing_required_field_diagnostic_exposes_only_allowlisted_schema_field() {
        let summary = fatal_error_summary(
            r#"Error: Reference(MissingRequiredField { instrument_id: "SENSITIVE-ID", field: "tickSz" })"#,
        )
        .expect("summary");

        assert_eq!(summary["class"], "reference_data");
        assert_eq!(summary["code"], "missing_required_field");
        assert_eq!(summary["missing_field"], "tickSz");
        assert!(!summary.to_string().contains("SENSITIVE-ID"));

        let unknown = fatal_error_summary(
            r#"Error: Reference(MissingRequiredField { instrument_id: "SENSITIVE-ID", field: "futureField" })"#,
        )
        .expect("summary");
        assert!(unknown["missing_field"].is_null());
        assert!(!unknown.to_string().contains("futureField"));
    }

    #[test]
    fn demo_acceptance_launch_isolated_from_production_transport_and_state() {
        let args = agent_args(AgentProfile::DemoAcceptance);
        assert!(args.contains(&"--demo"));
        assert!(args.contains(&"--demo-mutation-acceptance"));
        assert!(args.contains(&"--root"));
        assert!(args.contains(&DEMO_RUNTIME_ROOT));
        assert!(args.contains(&DEMO_MAILBOX_ISSUE));
        assert!(args.contains(&"--cloudflare-ws-url"));
        assert!(args.contains(&"--cloudflare-runtime-id"));
        assert!(args.contains(&AGENT_CLOUDFLARE_WS_URL));
        assert_eq!(mailbox_issue(AgentProfile::DemoAcceptance), 234);
        assert_eq!(
            runtime_dir(AgentProfile::DemoAcceptance),
            PathBuf::from(DEMO_RUNTIME_ROOT)
        );
        assert_ne!(
            runtime_dir(AgentProfile::DemoAcceptance),
            runtime_dir(AgentProfile::Production)
        );
    }

    #[test]
    fn workspace_diagnostic_text_is_byte_bounded_and_utf8_safe() {
        let value = "é".repeat(400);
        let bounded = bounded_utf8(&value, WORKSPACE_STATUS_MAX_CHANGE_BYTES);
        assert!(bounded.len() <= WORKSPACE_STATUS_MAX_CHANGE_BYTES);
        assert!(std::str::from_utf8(bounded.as_bytes()).is_ok());
    }

    #[test]
    fn workspace_diagnostic_count_is_intentionally_small() {
        assert_eq!(WORKSPACE_STATUS_MAX_CHANGES, 16);
        assert_eq!(WORKSPACE_STATUS_MAX_CHANGE_BYTES, 512);
    }
}
