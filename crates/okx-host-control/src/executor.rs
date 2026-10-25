use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};

use okx_protocol::{
    HostControlOperation, NATIVE_CHILD_CONTROL_MAX_BYTES, NATIVE_CHILD_CONTROL_PREFIX,
    NATIVE_CHILD_CONTROL_SCHEMA_V1, NativeChildOperation, NativeChildReply, NativeChildRequest,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

use crate::{
    HostControlError, HostControlResult,
    account_catalog::AccountCatalogStore,
    auth::load_native_github_token,
    autostart,
    background_process::hidden_command,
    controller_update,
    desired::{AgentDesired, AgentDesiredState, DesiredStateStore},
    job::AgentJob,
    provenance::InstalledAgentProvenanceStore,
};

const CANONICAL_ROOT: &str = r"C:\okx";
const RUNTIME_ROOT: &str = r"C:\okx-runtime";
const AGENT_MAILBOX_ISSUE: &str = "10";
const AGENT_CLOUDFLARE_WS_URL: &str = "wss://okx-cloudflare-mcp.okx-794.workers.dev/runtime";
const AGENT_CLOUDFLARE_RUNTIME_ID: &str = "windows-primary";
const HOST_CONTROL_CAPABILITIES_SCHEMA_V1: &str = "okx.host-control.capabilities/v1";
const HEALTHY_AGENT_SECS: u64 = 30;
const RESTART_BACKOFF_SECS: [u64; 5] = [1, 5, 15, 30, 60];
const WORKSPACE_STATUS_MAX_CHANGES: usize = 16;
const WORKSPACE_STATUS_MAX_CHANGE_BYTES: usize = 512;
const RUNTIME_DIAGNOSTIC_TAIL_BYTES: u64 = 64 * 1024;
const NATIVE_INITIALIZATION_TIMEOUT: Duration = Duration::from_secs(300);
const NATIVE_REPLY_MAX_BYTES: u64 = 4096;

/// Run the existing installed Agent exactly once, without a pipe deadlock.
/// Windows waits on the native process handle; an ephemeral OS Job owns every
fn bounded_native_output(command: &mut Command, timeout: Duration) -> HostControlResult<Vec<u8>> {
    #[cfg(not(windows))]
    {
        let _ = (command, timeout);
        Err(HostControlError::UnsupportedPlatform)
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::WaitForSingleObject;

        let millis: u32 = timeout
            .as_millis()
            .try_into()
            .map_err(|_| HostControlError::ControlResponseBudgetInvariant)?;
        if millis == 0 {
            return Err(HostControlError::ControlResponseBudgetInvariant);
        }
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).map_err(|error| HostControlError::Random(error.to_string()))?;
        let token = nonce
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let output_path = std::env::temp_dir().join(format!(
            "okx-native-operation-{}-{token}.receipt",
            std::process::id()
        ));
        let mut receipt = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&output_path)?;
        let result = (|| -> HostControlResult<Vec<u8>> {
            let job = AgentJob::new()?;
            let mut child = command
                .stdout(Stdio::from(receipt.try_clone()?))
                .stderr(Stdio::null())
                .spawn()?;
            job.assign(&mut child)?;
            let outcome = unsafe { WaitForSingleObject(child.as_raw_handle() as _, millis) };
            if outcome != WAIT_OBJECT_0 {
                let error = std::io::Error::last_os_error();
                // The temporary KILL_ON_JOB_CLOSE Job also owns descendants.
                let _ = child.kill();
                let _ = child.wait();
                if outcome == WAIT_TIMEOUT {
                    return Err(HostControlError::NativeStorageOutcomeUnknown);
                }
                return Err(error.into());
            }
            if !child.wait()?.success() {
                return Err(HostControlError::NativeStorageOutcomeUnknown);
            }
            if receipt.metadata()?.len() > NATIVE_REPLY_MAX_BYTES {
                return Err(HostControlError::ControlResponseBudgetInvariant);
            }
            receipt.rewind()?;
            let mut bytes = Vec::new();
            receipt.read_to_end(&mut bytes)?;
            Ok(bytes)
        })();
        drop(receipt);
        // an owner marker, restart signal, authorization or replay target.
        let _ = fs::remove_file(output_path);
        result
    }
}

const ALLOWED_REMOTES: &[&str] = &[
    "https://github.com/iamaman11/okx",
    "https://github.com/iamaman11/okx.git",
];

#[derive(Clone, Debug)]
struct AgentLaunchEvidence {
    pid: u32,
    incarnation: String,
    stdout_offset: u64,
    stderr_offset: u64,
    stdout_health: okx_host_launcher::logging::LogHealth,
    stderr_health: okx_host_launcher::logging::LogHealth,
}

struct NativeChildPipe {
    input: ChildStdin,
    replies: Receiver<NativeChildReply>,
}

pub struct HostExecutor {
    native_child_pipe: Option<NativeChildPipe>,
    repo_root: PathBuf,
    agent_child: Option<Child>,
    agent_started_at: Option<Instant>,
    agent_launch: Option<AgentLaunchEvidence>,
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
        let (desired_state, desired_error) = match desired_store.load() {
            Ok(desired) => (desired, None),
            Err(error) => (AgentDesiredState::stopped(), Some(error.to_string())),
        };

        Ok(Self {
            native_child_pipe: None,
            repo_root: PathBuf::from(CANONICAL_ROOT),
            agent_child: None,
            agent_started_at: None,
            agent_launch: None,
            agent_job: AgentJob::new()?,
            desired_store,
            desired_agent: desired_state.agent,
            desired_error,
            restart_attempt: 0,
            next_restart_at: None,
            last_reconcile: "NOT_RUN".to_owned(),
        })
    }

    pub fn execute(&mut self, operation: HostControlOperation) -> HostControlResult<Value> {
        self.lifecycle_gate(&operation)?;
        match operation {
            HostControlOperation::Status => self.status(),
            HostControlOperation::AccountCatalog => AccountCatalogStore::canonical().status(),
            HostControlOperation::Sync => self.sync(),
            HostControlOperation::BuildAgent => Err(HostControlError::InvalidExecutionPath),
            HostControlOperation::DeployAgent { .. }
            | HostControlOperation::InstallLauncherRoot { .. }
            | HostControlOperation::UpgradeLauncherRoot { .. }
            | HostControlOperation::StageControllerUpdate { .. }
            | HostControlOperation::HandoffControllerUpdate
            | HostControlOperation::HandoffToAutostart
            | HostControlOperation::AcceptanceCrashController => {
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
            HostControlOperation::StopAgent => self.stop_agent(),
            HostControlOperation::RestartAgent => self.restart_agent(),
            HostControlOperation::InitializeInstallation {
                installation_id,
                expected_release,
                operation_id,
            } => self.initialize_native(None, &installation_id, &expected_release, &operation_id),
            HostControlOperation::InitializeAccount {
                account_key,
                installation_id,
                expected_release,
                operation_id,
            } => self.initialize_native(
                Some(&account_key),
                &installation_id,
                &expected_release,
                &operation_id,
            ),
            HostControlOperation::BackupAccount { account_key } => {
                self.storage_maintenance("backup-account", &account_key, None, None, None)
            }
            HostControlOperation::RestoreAccount {
                account_key,
                snapshot_id,
                expected_release,
                operation_id,
            } => self.storage_maintenance(
                "restore-account",
                &account_key,
                Some(&snapshot_id),
                Some(&expected_release),
                Some(&operation_id),
            ),
            HostControlOperation::InstallAutostart => autostart::install(),
            HostControlOperation::AutostartStatus => autostart::status_value(),
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
    pub fn lifecycle_gate(&self, operation: &HostControlOperation) -> HostControlResult<()> {
        if matches!(
            operation,
            HostControlOperation::Status
                | HostControlOperation::AccountCatalog
                | HostControlOperation::TransportStatus
                | HostControlOperation::ControllerUpdateStatus
                | HostControlOperation::AgentIdentity
                | HostControlOperation::AutostartStatus
                | HostControlOperation::StopAgent
        ) {
            return Ok(());
        }
        self.desired_store.load()?;
        Ok(())
    }

    pub fn shutdown(&mut self) {
        let _ = self.terminate_agent_owned();
    }

    pub fn reconcile_desired(&mut self) -> HostControlResult<Value> {
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
                            self.last_reconcile = format!("RESTORED_ENGINE_PID_{pid}");
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
            // Catalog errors must be visible but cannot alter the lifecycle owner.
            "account_catalog": match AccountCatalogStore::canonical().status_summary() {
                Ok(value) => value,
                Err(_) => json!({"schema":"okx.account-catalog/v1","status":"CORRUPT_OR_UNAVAILABLE","selectable":false})
            },
            "agent_binary_present": agent_binary.is_file(),
            "agent_owned_running": running,
            "agent_pid": if running { self.agent_child.as_ref().map(Child::id) } else { None },
            "agent_incarnation": if running { self.agent_launch.as_ref().map(|e| e.incarnation.as_str()) } else { None },
            "agent_desired": self.desired_agent,
            "desired_state_path": self.desired_store.path(),
            "desired_state_error": self.desired_error.clone(),
            "job_object_owned": true,
            "native_child_control_pipe_attached": running && self.native_child_pipe.is_some(),
            "restart_attempt": self.restart_attempt,
            "retry_in_ms": self.retry_in_ms(),
            "last_reconcile": self.last_reconcile.clone(),
            "runtime_diagnostics": {
                "engine": runtime_log_summary(owned_launch)
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
        if self.agent_is_running()? {
            self.persist_desired(AgentDesiredState::running())?;
            return Ok(json!({"disposition":"ALREADY_RUNNING"}));
        }
        self.require_agent_binary()?;
        self.persist_desired(AgentDesiredState::running())?;
        let pid = match self.start_agent_process() {
            Ok(pid) => pid,
            Err(e) => {
                self.schedule_restart();
                return Err(e);
            }
        };
        self.restart_attempt = 0;
        self.next_restart_at = None;
        Ok(json!({"disposition":"STARTED","pid":pid,"runtime_root":RUNTIME_ROOT}))
    }

    /// Stop decisions and drain the owned process before terminating its Job.
    fn stop_agent(&mut self) -> HostControlResult<Value> {
        self.persist_desired(AgentDesiredState::stopped())?;
        self.terminate_agent_owned()?;
        self.restart_attempt = 0;
        self.next_restart_at = None;
        Ok(json!({"disposition":"STOPPED","auto_close":false}))
    }

    fn restart_agent(&mut self) -> HostControlResult<Value> {
        self.persist_desired(AgentDesiredState::running())?;
        self.terminate_agent_owned()?;
        let pid = self.start_agent_process()?;
        Ok(json!({"disposition":"RESTARTED","pid":pid}))
    }

    /// Fresh native ledger genesis with the existing CONTROL as sole owner. It is
    /// impossible to enter this branch while CONTROL desires an active Agent,
    /// while its Job contains a process, or while release provenance differs.
    /// A crash leaves the desired state STOPPED, not an automatic restart.
    fn initialize_native(
        &mut self,
        account: Option<&str>,
        installation: &str,
        release: &str,
        operation: &str,
    ) -> HostControlResult<Value> {
        self.require_storage_stopped()?;
        self.require_release(release)?;
        if let Some(key) = account {
            AccountCatalogStore::canonical().require_account(key, installation)?;
        }
        let mut command = hidden_command(self.agent_binary());
        command
            .arg("--root")
            .arg(RUNTIME_ROOT)
            .arg(if account.is_some() {
                "initialize-account"
            } else {
                "initialize-installation"
            })
            .arg("--installation-id")
            .arg(installation)
            .arg("--expected-release")
            .arg(release)
            .arg("--operation-id")
            .arg(operation);
        if let Some(key) = account {
            command.arg("--account-key").arg(key);
        }
        let bytes = bounded_native_output(&mut command, NATIVE_INITIALIZATION_TIMEOUT)?;
        let reply: Value = serde_json::from_slice(&bytes)?;
        if reply["installation_id"] != installation
            || reply["operation_id"] != operation
            || reply["release"] != release
            || reply["status"] != "completed"
            || account.is_some_and(|key| reply["account_key"] != key)
        {
            return Err(HostControlError::ArtifactVerification(
                "native initialization receipt binding invalid",
            ));
        }
        Ok(reply)
    }
    fn require_storage_stopped(&mut self) -> HostControlResult<()> {
        self.require_agent_stopped()?;
        if self.desired_agent != AgentDesired::Stopped
            || !self.agent_job.is_empty()?
            || self.agent_child.is_some()
            || self.native_child_pipe.is_some()
        {
            return Err(HostControlError::AgentRunning);
        }
        Ok(())
    }
    fn require_release(&mut self, release: &str) -> HostControlResult<()> {
        self.require_agent_binary()?;
        let status = self.status()?;
        if status["release_parity"]["state"] != "MATCHED"
            || status["release_parity"]["source_tree"] != release
        {
            return Err(HostControlError::ArtifactVerification(
                "native operation requires exact matched ReleaseSet",
            ));
        }
        Ok(())
    }
    fn storage_maintenance(
        &mut self,
        command_name: &str,
        key: &str,
        snapshot: Option<&str>,
        release: Option<&str>,
        operation: Option<&str>,
    ) -> HostControlResult<Value> {
        self.require_storage_stopped()?;
        let c = AccountCatalogStore::canonical().load()?;
        AccountCatalogStore::canonical().require_account(key, &c.installation_id)?;
        if let Some(r) = release {
            self.require_release(r)?;
        }
        let mut command = hidden_command(self.agent_binary());
        command
            .arg("--root")
            .arg(RUNTIME_ROOT)
            .arg(command_name)
            .arg("--account-key")
            .arg(key);
        if let Some(s) = snapshot {
            command.arg("--snapshot-id").arg(s);
        }
        if let Some(r) = release {
            command.arg("--expected-release").arg(r);
        }
        if let Some(o) = operation {
            command.arg("--operation-id").arg(o);
        }
        Ok(serde_json::from_slice(&bounded_native_output(
            &mut command,
            NATIVE_INITIALIZATION_TIMEOUT,
        )?)?)
    }

    fn acceptance_kill_agent(&mut self) -> HostControlResult<Value> {
        self.lifecycle_gate(&HostControlOperation::AcceptanceKillAgent)?;
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
                "engine": {"runtime_root":RUNTIME_ROOT,"mailbox_issue":10,"cloudflare_attached":true,"single_owner":true}
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
        let should_restore = self.desired_agent == AgentDesired::Running;
        let running = self.agent_is_running()?;
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

        if should_restore && let Err(error) = self.start_agent_process() {
            self.schedule_restart();
            return Err(error);
        }

        Ok(())
    }

    fn start_agent_process(&mut self) -> HostControlResult<u32> {
        self.start_agent_process_inner()
    }

    fn start_agent_process_inner(&mut self) -> HostControlResult<u32> {
        if self.agent_child.is_some() || !self.agent_job.is_empty()? {
            return Err(HostControlError::AgentRunning);
        }
        self.require_agent_binary()?;
        let runtime_dir = PathBuf::from(RUNTIME_ROOT);
        fs::create_dir_all(&runtime_dir)?;

        let stdout = okx_host_launcher::logging::RotatingLog::open(
            runtime_dir.join("okx-agent.stdout.log"),
        )?;
        let stderr = okx_host_launcher::logging::RotatingLog::open(
            runtime_dir.join("okx-agent.stderr.log"),
        )?;
        let stdout_offset = stdout.offset();
        let stderr_offset = stderr.offset();
        let stdout_health = stdout.health();
        let stderr_health = stderr.health();
        let args = ENGINE_AGENT_ARGS;
        // Native CONTROL owns one random identity per child; WSS reconnects
        // do not change it, while a new child necessarily gets a new value.
        let mut incarnation_bytes = [0u8; 16];
        getrandom::fill(&mut incarnation_bytes)
            .map_err(|error| HostControlError::Random(error.to_string()))?;
        let incarnation = format!(
            "ctl_{}",
            incarnation_bytes
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let mut child = hidden_command(self.agent_binary())
            .args(args)
            .arg("--native-child-control-stdio")
            .env("OKX_CONTROL_AGENT_INCAR", &incarnation)
            .current_dir(&self.repo_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        self.agent_job.assign(&mut child)?;
        let pid = child.id();
        let child_input = child
            .stdin
            .take()
            .ok_or(HostControlError::InvalidExecutionPath)?;
        let child_stdout = child
            .stdout
            .take()
            .ok_or(HostControlError::InvalidExecutionPath)?;
        okx_host_launcher::logging::spawn_drain(child_stdout, stdout);
        let child_stderr = child
            .stderr
            .take()
            .ok_or(HostControlError::InvalidExecutionPath)?;
        let (reply_tx, reply_rx) = mpsc::channel();
        // The original stderr bytes feed typed child replies independently of
        // the redacted rotating diagnostic sink; sink failures never stop drain.
        let _stderr_reader =
            std::thread::spawn(move || drain_native_stderr(child_stderr, stderr, reply_tx));
        self.native_child_pipe = Some(NativeChildPipe {
            input: child_input,
            replies: reply_rx,
        });
        self.agent_child = Some(child);
        self.agent_started_at = Some(Instant::now());
        self.agent_launch = Some(AgentLaunchEvidence {
            pid,
            incarnation,
            stdout_offset,
            stderr_offset,
            stdout_health,
            stderr_health,
        });
        self.next_restart_at = None;
        Ok(pid)
    }

    fn terminate_agent_owned(&mut self) -> HostControlResult<()> {
        let mut pipe = self.native_child_pipe.take();
        let Some(mut child) = self.agent_child.take() else {
            self.agent_started_at = None;
            self.agent_launch = None;
            return Ok(());
        };
        if child.try_wait()?.is_none() {
            let deadline = Instant::now() + Duration::from_secs(10);
            if let Some(p) = pipe.as_mut() {
                let request_id = format!(
                    "{:x}",
                    Sha256::digest(
                        format!("shutdown:{}:{:?}", child.id(), Instant::now()).as_bytes()
                    )
                );
                let request = NativeChildRequest {
                    schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.into(),
                    request_id: request_id.clone(),
                    operation: NativeChildOperation::Shutdown {
                        drain_timeout_ms: 10000,
                    },
                };
                if let Ok(mut bytes) = serde_json::to_vec(&request) {
                    bytes.push(b'\n');
                    let _ = p.input.write_all(&bytes);
                    let _ = p.input.flush();
                }
            }
            while Instant::now() < deadline && child.try_wait()?.is_none() {
                if let Some(p) = pipe.as_ref() {
                    while p.replies.try_recv().is_ok() {}
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            if child.try_wait()?.is_none() {
                self.agent_job.terminate()?;
                child.wait()?;
            }
        }
        drop(pipe);
        if !self.agent_job.is_empty()? {
            self.agent_job.terminate()?;
        }
        self.agent_started_at = None;
        self.agent_launch = None;
        Ok(())
    }

    fn persist_desired(&mut self, state: AgentDesiredState) -> HostControlResult<()> {
        self.desired_store.save(state)?;
        self.desired_agent = state.agent;
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
        // Separate line collectors prevent stdout/stderr fragments from
        // interleaving around a credential label before redaction.
        let out_log =
            okx_host_launcher::logging::RotatingLog::open(self.runtime_dir().join(log_name))?;
        let err_log = okx_host_launcher::logging::RotatingLog::open(
            self.runtime_dir().join(format!("{log_name}.stderr")),
        )?;
        let mut child = hidden_command(program)
            .args(args)
            .current_dir(&self.repo_root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or(HostControlError::InvalidExecutionPath)?;
        let stderr = child
            .stderr
            .take()
            .ok_or(HostControlError::InvalidExecutionPath)?;
        let out_thread = okx_host_launcher::logging::spawn_drain(stdout, out_log);
        let err_thread = okx_host_launcher::logging::spawn_drain(stderr, err_log);
        let status = child.wait()?;
        let _ = out_thread.join();
        let _ = err_thread.join();

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

const ENGINE_AGENT_ARGS: &[&str] = &[
    "run",
    "--mailbox-issue",
    AGENT_MAILBOX_ISSUE,
    "--cloudflare-ws-url",
    AGENT_CLOUDFLARE_WS_URL,
    "--cloudflare-runtime-id",
    AGENT_CLOUDFLARE_RUNTIME_ID,
];

fn drain_native_stderr(
    mut child_stderr: impl Read,
    mut stderr_log: okx_host_launcher::logging::RotatingLog,
    reply_tx: mpsc::Sender<NativeChildReply>,
) {
    let mut pending = Vec::<u8>::new();
    let mut overflow = false;
    let mut block = [0u8; 4096];
    loop {
        let count = match child_stderr.read(&mut block) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        stderr_log.append(&block[..count]);
        for byte in &block[..count] {
            if *byte == b'\n' {
                if !overflow && pending.starts_with(NATIVE_CHILD_CONTROL_PREFIX.as_bytes()) {
                    let payload = &pending[NATIVE_CHILD_CONTROL_PREFIX.len()..];
                    if let Ok(reply) = serde_json::from_slice::<NativeChildReply>(payload) {
                        let _ = reply_tx.send(reply);
                    }
                }
                pending.clear();
                overflow = false;
            } else if pending.len() < NATIVE_CHILD_CONTROL_MAX_BYTES {
                pending.push(*byte);
            } else {
                overflow = true;
            }
        }
    }
    stderr_log.finish();
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
        && stderr.as_ref().is_some_and(|(_, complete)| *complete)
        && launch.stdout_health.complete()
        && launch.stderr_health.complete();
    let stdout_text = stdout.as_ref().map_or("", |(text, _)| text.as_str());
    let stderr_text = stderr.as_ref().map_or("", |(text, _)| text.as_str());
    json!({
        "pid": launch.pid,
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

fn runtime_log_summary(launch: Option<&AgentLaunchEvidence>) -> Value {
    let root = PathBuf::from(RUNTIME_ROOT);
    let mut retained = runtime_log_summary_from_root(root.clone());
    retained["current_process"] = launch.map_or(Value::Null, |v| current_launch_summary(&root, v));
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
        // They are not scoped to the current PID/launch. Never treat a prior
        // fatal error or startup marker as proof of a current runtime failure.
        "log_evidence": {
            "scope":"retained_engine_file_tail",
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
        "process_recovery_permitted": true
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
    fn restart_backoff_is_bounded() {
        assert_eq!(RESTART_BACKOFF_SECS, [1, 5, 15, 30, 60]);
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
            "retained_engine_file_tail"
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
            pid: 321,
            incarnation: "ctl_00000000000000000000000000000001".into(),
            stdout_offset: fs::metadata(&stdout).expect("stdout metadata").len(),
            stderr_offset: fs::metadata(&stderr).expect("stderr metadata").len(),
            stdout_health: Default::default(),
            stderr_health: Default::default(),
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
    fn native_cutover_child_has_bounded_output_and_one_terminal_result() {
        let mut command = Command::new("cmd");
        command.arg("/C").arg("echo BOUNDED-CUTOVER-RECEIPT");
        let bytes = bounded_native_output(&mut command, Duration::from_secs(15))
            .expect("native completed once");
        assert_eq!(
            String::from_utf8(bytes).expect("UTF-8 receipt").trim(),
            "BOUNDED-CUTOVER-RECEIPT"
        );
    }

    #[cfg(windows)]
    #[test]
    fn timed_out_native_cutover_is_typed_unknown_not_replayed() {
        let mut command = Command::new("cmd");
        command.arg("/C").arg("ping -n 30 127.0.0.1 >nul");
        let error = bounded_native_output(&mut command, Duration::from_millis(30))
            .expect_err("the child must be terminated and its outcome uncertain");
        assert_eq!(error.code(), "NATIVE_STORAGE_OUTCOME_UNKNOWN");
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
    #[test]
    fn rotating_redacted_stderr_preserves_child_shutdown_reply() {
        let root = std::env::temp_dir().join(format!(
            "okx-child-log-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("stderr.log");
        let reply = NativeChildReply {
            schema: NATIVE_CHILD_CONTROL_SCHEMA_V1.into(),
            request_id: "shutdown_test_0001".into(),
            outcome: "DRAINED".into(),
        };
        let input = format!(
            "apiKey=NEVER_PERSIST_THIS\n{}{}\n",
            NATIVE_CHILD_CONTROL_PREFIX,
            serde_json::to_string(&reply).unwrap()
        );
        let (tx, rx) = mpsc::channel();
        drain_native_stderr(
            std::io::Cursor::new(input),
            okx_host_launcher::logging::RotatingLog::open(&path).unwrap(),
            tx,
        );
        assert!(rx.recv().unwrap().valid_for("shutdown_test_0001"));
        let text = fs::read_to_string(path).unwrap();
        assert!(!text.contains("NEVER_PERSIST_THIS"));
        assert!(text.contains("redacted"));
    }
}
