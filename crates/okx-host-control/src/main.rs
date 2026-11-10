use std::{
    io::{self, Read},
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};
use okx_github::GitHubClient;
use okx_host_control::{
    HostControlResult,
    auth::{load_native_github_token, store_native_github_token},
    executor::{HostExecutor, install_current_executable},
    legacy_bootstrap, root_migration,
    runtime::{DEFAULT_CONTROL_POLL_SECONDS, process_pending, run_until_shutdown},
    single_instance::SingleInstanceGuard,
};
use zeroize::Zeroize;

const INSTALL_PATH: &str = okx_host_launcher::ENTRYPOINT_PATH;

#[derive(Debug, Parser)]
#[command(name = "okx-host-control")]
#[command(about = "Outbound-only native Windows host control plane for iamaman11/okx")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect local installation presence without starting services or reading secrets.
    Inspect,

    /// Copy this trusted binary outside the optional C:\okx\workspace worktree.
    Install,

    /// Store the GitHub control token from stdin in Windows Credential Manager.
    SetGithubToken,

    /// One-shot staged activator for a previously verified controller update.
    ActivateControllerUpdate {
        #[arg(long)]
        parent_pid: u32,
        #[arg(long)]
        handoff_request_id: String,
    },

    /// One-shot post-parent launcher-root migration. Migration-only.
    MigrateLauncherRoot {
        #[arg(long)]
        request_id: String,
    },

    /// Process pending typed requests once and exit.
    Once,

    /// Poll GitHub control issue #12 over outbound HTTPS.
    Run {
        #[arg(long, default_value_t = DEFAULT_CONTROL_POLL_SECONDS)]
        poll_seconds: u64,
        #[arg(long)]
        activation_request_id: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(Cli::parse()).await?;
    Ok(())
}

async fn run(cli: Cli) -> HostControlResult<()> {
    match cli.command {
        Command::Inspect => {
            let receipt = inspect_installation_paths(
                Path::new(okx_host_launcher::CONTROL_ROOT),
                Path::new(okx_host_launcher::RUNTIME_ROOT),
            );
            println!("{}", serde_json::to_string_pretty(&receipt)?);
        }
        Command::Install => {
            if Path::new(okx_host_launcher::ACTIVE_PATH).exists() {
                return Err(okx_host_control::HostControlError::LegacyBootstrapDisabled);
            }
            let result = install_current_executable(&PathBuf::from(INSTALL_PATH))?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::SetGithubToken => {
            let mut token = read_stdin()?;
            while matches!(token.as_bytes().last(), Some(b'\r' | b'\n')) {
                token.pop();
            }
            let result = store_native_github_token(&token);
            token.zeroize();
            result?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.host-control.github-token/v1",
                    "stored": true
                })
            );
        }
        Command::ActivateControllerUpdate {
            parent_pid,
            handoff_request_id,
        } => {
            let token = load_native_github_token()?;
            let github = GitHubClient::new(token, "iamaman11-okx-host-control/0.1")?;
            github.verify_repository_identity().await?;
            let result =
                legacy_bootstrap::activate(&github, parent_pid, &handoff_request_id).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::MigrateLauncherRoot { request_id } => {
            let result = root_migration::activate(&request_id)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::Once => {
            let token = load_native_github_token()?;
            let github = GitHubClient::new(token, "iamaman11-okx-host-control/0.1")?;
            github.verify_repository_identity().await?;
            let mut executor = HostExecutor::canonical()?;
            let processed = process_pending(&github, &mut executor).await?;
            executor.shutdown();
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.host-control.once/v1",
                    "processed": processed
                })
            );
        }
        Command::Run {
            poll_seconds,
            activation_request_id,
        } => {
            let _single_instance = SingleInstanceGuard::acquire()?;
            let _log_capture = okx_host_launcher::stdio_capture::ProcessLogCapture::install(
                okx_host_launcher::CONTROL_ROOT,
            )?;
            let result = async {
                let token = load_native_github_token()?;
                let github = GitHubClient::new(token, "iamaman11-okx-host-control/0.1")?;
                let mut executor = HostExecutor::canonical()?;
                if let Some(request_id) = activation_request_id.as_deref() {
                    okx_host_control::autostart::ensure_policy_valid()?;
                    if okx_host_launcher::consume_fail_next_activation(request_id)? {
                        return Err(
                            okx_host_control::HostControlError::AcceptanceActivationFailureInjected,
                        );
                    }
                    okx_host_launcher::signal_controller_ready(request_id)?;
                }
                run_until_shutdown(&github, &mut executor, poll_seconds).await
            }
            .await;
            if let Err(error) = &result {
                eprintln!("Error: {error}");
            }
            result?;
        }
    }

    Ok(())
}

fn read_stdin() -> HostControlResult<String> {
    let mut payload = String::new();
    io::stdin().read_to_string(&mut payload)?;
    Ok(payload)
}

// Deliberately a local inspection, not an Engine health or RiskBook assertion.
// In particular it never runs HostExecutor, polls Issue #12, or mutates state.
fn inspect_installation_paths(control: &Path, runtime: &Path) -> serde_json::Value {
    let controller = control.join("okx-host-control.exe").is_file();
    let launcher = control.join("okx-host-launcher.exe").is_file();
    let agent = runtime.join("okx-agent.exe").is_file();
    let provenance = runtime.join("installed-agent.json").is_file();
    let catalog = runtime.join("account-catalog.pb").is_file();
    let existing_financial_state = runtime.join("accounts").exists()
        || runtime.join("installation.json").exists()
        || runtime.join("account-catalog").exists();
    let state = if !catalog && existing_financial_state {
        "BLOCKED_EXISTING_STATE_WITHOUT_CATALOG"
    } else if !catalog {
        "UNINITIALIZED_LOCAL_IDENTITY"
    } else {
        "IDENTITY_FILE_PRESENT_NOT_VERIFIED"
    };
    let next = match state {
        "BLOCKED_EXISTING_STATE_WITHOUT_CATALOG" => {
            "ISOLATE_OR_VERIFY_EXISTING_STATE_BEFORE_ANY_CLEAN_INITIALIZE"
        }
        "UNINITIALIZED_LOCAL_IDENTITY" => "VERIFY_CLEAN_ROOT_AND_RELEASE_BEFORE_INITIALIZE",
        _ => "VERIFY_ACCOUNT_SQL_AND_CENTRAL_RISK_BEFORE_START",
    };
    serde_json::json!({
        "schema": "okx.windows.local-inspection/v1",
        "state": state,
        "controller_present": controller,
        "launcher_present": launcher,
        "agent_present": agent,
        "agent_provenance_present": provenance,
        "account_catalog_present": catalog,
        "existing_financial_or_legacy_identity_present": existing_financial_state,
        "verification": "PRESENCE_ONLY",
        "trading_permission": "NOT_VERIFIED",
        "next": next
    })
}

#[cfg(test)]
mod inspection_tests {
    use super::*;
    use std::fs;

    #[test]
    fn local_inspection_is_read_only_and_blocks_legacy_financial_state() {
        let root = std::env::temp_dir().join(format!(
            "okx-inspect-{}-legacy", std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let control = root.join("control");
        let runtime = root.join("runtime");
        fs::create_dir_all(runtime.join("accounts").join("existing")).unwrap();
        let db = runtime.join("accounts").join("existing").join("account.sqlite");
        fs::write(&db, b"original").unwrap();

        let receipt = inspect_installation_paths(&control, &runtime);
        assert_eq!(receipt["state"], "BLOCKED_EXISTING_STATE_WITHOUT_CATALOG");
        assert_eq!(receipt["verification"], "PRESENCE_ONLY");
        assert_eq!(receipt["trading_permission"], "NOT_VERIFIED");
        assert_eq!(fs::read(db).unwrap(), b"original");
        assert!(!runtime.join("account-catalog.pb").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn local_inspection_never_equates_file_presence_with_account_or_risk_proof() {
        let root = std::env::temp_dir().join(format!(
            "okx-inspect-{}-new", std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let control = root.join("control");
        let runtime = root.join("runtime");
        fs::create_dir_all(&control).unwrap();
        fs::create_dir_all(&runtime).unwrap();
        let empty = inspect_installation_paths(&control, &runtime);
        assert_eq!(empty["state"], "UNINITIALIZED_LOCAL_IDENTITY");

        for p in [
            control.join("okx-host-control.exe"),
            control.join("okx-host-launcher.exe"),
            runtime.join("okx-agent.exe"),
            runtime.join("installed-agent.json"),
            runtime.join("account-catalog.pb"),
        ] {
            fs::write(p, b"synthetic-test-only").unwrap();
        }
        let present = inspect_installation_paths(&control, &runtime);
        assert_eq!(present["state"], "IDENTITY_FILE_PRESENT_NOT_VERIFIED");
        assert_eq!(present["trading_permission"], "NOT_VERIFIED");
        assert_eq!(present["launcher_present"], true);
        fs::remove_dir_all(root).unwrap();
    }
}
