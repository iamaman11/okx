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
    /// Inspect local installation without starting services, reading SQL or contacting GitHub.
    Inspect {
        #[arg(long)]
        control_root: Option<PathBuf>,
        #[arg(long)]
        runtime_root: Option<PathBuf>,
    },

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
        Command::Inspect {
            control_root,
            runtime_root,
        } => {
            let control =
                control_root.unwrap_or_else(|| PathBuf::from(okx_host_launcher::CONTROL_ROOT));
            let runtime =
                runtime_root.unwrap_or_else(|| PathBuf::from(okx_host_launcher::RUNTIME_ROOT));
            let receipt = inspect_installation_paths(&control, &runtime);
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

// Offline consistency only. The manifest is not a trust anchor by itself:
// the release bootstrap must independently verify the GitHub release before
// placing it here. No SQL, credentials, GitHub requests or process starts.
fn local_release_integrity(control: &Path, runtime: &Path) -> &'static str {
    let manifest_path = control.join("manifest.json");
    if !manifest_path.is_file() {
        return "MANIFEST_MISSING";
    }
    let Ok(bytes) = std::fs::read(&manifest_path) else {
        return "MANIFEST_UNREADABLE";
    };
    if bytes.len() > 64 * 1024 {
        return "MANIFEST_INVALID";
    }
    let Ok(manifest) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
        return "MANIFEST_INVALID";
    };
    if manifest["schema"] != "okx.windows.bundle/v1"
        || manifest["repository_id"] != okx_host_launcher::REPOSITORY_ID
        || !lower_hex(manifest["source_tree"].as_str(), 40)
        || !lower_hex(manifest["source_head_sha"].as_str(), 40)
    {
        return "MANIFEST_INVALID";
    }
    for (name, file) in [
        ("okx-agent.exe", runtime.join("okx-agent.exe")),
        (
            "okx-host-launcher.exe",
            control.join("okx-host-launcher.exe"),
        ),
    ] {
        let declared = manifest["files"][name]["sha256"].as_str();
        if !lower_hex(declared, 64)
            || okx_host_launcher::sha256_file(&file).ok().as_deref() != declared
        {
            return "BINARY_HASH_MISMATCH";
        }
    }
    let controller = manifest["files"]["okx-host-control.exe"]["sha256"].as_str();
    let launcher = manifest["files"]["okx-host-launcher.exe"]["sha256"].as_str();
    let entrypoint_hash = okx_host_launcher::sha256_file(&control.join("okx-host-control.exe"));
    if !lower_hex(controller, 64)
        || !matches!(
            entrypoint_hash.ok().as_deref(),
            Some(hash) if Some(hash) == controller || Some(hash) == launcher
        )
    {
        return "BINARY_HASH_MISMATCH";
    }
    "LOCAL_HASHES_MATCH_NOT_REMOTE_ATTESTED"
}

fn lower_hex(value: Option<&str>, len: usize) -> bool {
    value.is_some_and(|value| {
        value.len() == len
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

fn inspect_installation_paths(control: &Path, runtime: &Path) -> serde_json::Value {
    let controller = control.join("okx-host-control.exe").is_file();
    let launcher = control.join("okx-host-launcher.exe").is_file();
    let agent_path = runtime.join("okx-agent.exe");
    let agent = agent_path.is_file();
    let provenance = runtime.join("installed-agent.json").is_file();
    let catalog = runtime.join("account-catalog.pb").is_file();
    let existing_financial_state = runtime.join("accounts").exists()
        || runtime.join("installation.json").exists()
        || runtime.join("account-catalog").exists();
    let release_integrity = local_release_integrity(control, runtime);
    let agent_status = okx_host_control::provenance::InstalledAgentProvenanceStore::at(
        runtime.join("installed-agent.json"),
    )
    .status_value(&agent_path);
    let agent_provenance_state = agent_status["state"].as_str().unwrap_or("UNKNOWN");
    let state = if !catalog && existing_financial_state {
        "BLOCKED_EXISTING_STATE_WITHOUT_CATALOG"
    } else if !controller || !launcher || !agent {
        "BLOCKED_RELEASE_FILES_MISSING"
    } else if release_integrity != "LOCAL_HASHES_MATCH_NOT_REMOTE_ATTESTED" {
        "BLOCKED_RELEASE_NOT_VERIFIED"
    } else if agent_provenance_state != "VERIFIED" {
        "BLOCKED_AGENT_PROVENANCE"
    } else if !catalog {
        "UNINITIALIZED_LOCAL_IDENTITY"
    } else {
        "IDENTITY_FILE_PRESENT_NOT_VERIFIED"
    };
    let next = match state {
        "BLOCKED_EXISTING_STATE_WITHOUT_CATALOG" => {
            "ISOLATE_OR_VERIFY_EXISTING_STATE_BEFORE_ANY_CLEAN_INITIALIZE"
        }
        "BLOCKED_RELEASE_FILES_MISSING" | "BLOCKED_RELEASE_NOT_VERIFIED" => {
            "INSTALL_EXACT_VERIFIED_RELEASE_BEFORE_INITIALIZE"
        }
        "BLOCKED_AGENT_PROVENANCE" => "RECONCILE_ACCEPTED_RELEASE_PROVENANCE",
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
        "agent_provenance_state": agent_provenance_state,
        "local_release_integrity": release_integrity,
        "account_catalog_present": catalog,
        "existing_financial_or_legacy_identity_present": existing_financial_state,
        "verification": "LOCAL_FILES_ONLY_NO_REMOTE_RISK_OR_SQL",
        "safe_start": "READ_ONLY_INSPECTION_ONLY",
        "trading_permission": "NEW_RISK_BLOCKED",
        "central_risk": "UNKNOWN",
        "next": next
    })
}

#[cfg(test)]
mod inspection_tests {
    use super::*;
    use okx_host_control::provenance::{InstalledAgentProvenance, InstalledAgentProvenanceStore};
    use std::fs;

    fn test_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("okx-inspect-{}-{name}", std::process::id()))
    }

    #[test]
    fn local_inspection_is_read_only_and_blocks_legacy_financial_state() {
        let root = test_root("legacy");
        let _ = fs::remove_dir_all(&root);
        let control = root.join("control");
        let runtime = root.join("runtime");
        fs::create_dir_all(runtime.join("accounts").join("existing")).unwrap();
        let db = runtime
            .join("accounts")
            .join("existing")
            .join("account.sqlite");
        fs::write(&db, b"original").unwrap();

        let receipt = inspect_installation_paths(&control, &runtime);
        assert_eq!(receipt["state"], "BLOCKED_EXISTING_STATE_WITHOUT_CATALOG");
        assert_eq!(receipt["safe_start"], "READ_ONLY_INSPECTION_ONLY");
        assert_eq!(receipt["trading_permission"], "NEW_RISK_BLOCKED");
        assert_eq!(fs::read(db).unwrap(), b"original");
        assert!(!runtime.join("account-catalog.pb").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn local_inspection_requires_manifest_hashes_and_provenance_before_identity() {
        let root = test_root("new");
        let _ = fs::remove_dir_all(&root);
        let control = root.join("control");
        let runtime = root.join("runtime");
        fs::create_dir_all(&control).unwrap();
        fs::create_dir_all(&runtime).unwrap();
        let empty = inspect_installation_paths(&control, &runtime);
        assert_eq!(empty["state"], "BLOCKED_RELEASE_FILES_MISSING");

        for p in [
            control.join("okx-host-control.exe"),
            control.join("okx-host-launcher.exe"),
            runtime.join("okx-agent.exe"),
            runtime.join("account-catalog.pb"),
        ] {
            fs::write(p, b"synthetic-test-only").unwrap();
        }
        let no_manifest = inspect_installation_paths(&control, &runtime);
        assert_eq!(no_manifest["state"], "BLOCKED_RELEASE_NOT_VERIFIED");
        assert_eq!(no_manifest["trading_permission"], "NEW_RISK_BLOCKED");

        let hash = okx_host_launcher::sha256_file(&runtime.join("okx-agent.exe")).unwrap();
        let controller_hash =
            okx_host_launcher::sha256_file(&control.join("okx-host-control.exe")).unwrap();
        let launcher_hash =
            okx_host_launcher::sha256_file(&control.join("okx-host-launcher.exe")).unwrap();
        fs::write(
            control.join("manifest.json"),
            serde_json::to_vec(&serde_json::json!({
                "schema": "okx.windows.bundle/v1",
                "repository_id": okx_host_launcher::REPOSITORY_ID,
                "source_tree": "a".repeat(40),
                "source_head_sha": "b".repeat(40),
                "files": {
                    "okx-agent.exe": { "sha256": hash },
                    "okx-host-control.exe": { "sha256": controller_hash },
                    "okx-host-launcher.exe": { "sha256": launcher_hash }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let without_provenance = inspect_installation_paths(&control, &runtime);
        assert_eq!(without_provenance["state"], "BLOCKED_AGENT_PROVENANCE");
        let provenance = InstalledAgentProvenance::verified(
            1,
            1,
            "b".repeat(40),
            "a".repeat(40),
            "test-rust".into(),
            hash,
        );
        InstalledAgentProvenanceStore::at(runtime.join("installed-agent.json"))
            .save(&provenance)
            .unwrap();
        let present = inspect_installation_paths(&control, &runtime);
        assert_eq!(present["state"], "IDENTITY_FILE_PRESENT_NOT_VERIFIED");
        assert_eq!(
            present["local_release_integrity"],
            "LOCAL_HASHES_MATCH_NOT_REMOTE_ATTESTED"
        );
        assert_eq!(present["central_risk"], "UNKNOWN");
        fs::write(runtime.join("okx-agent.exe"), b"tampered").unwrap();
        let bad = inspect_installation_paths(&control, &runtime);
        assert_eq!(bad["state"], "BLOCKED_RELEASE_NOT_VERIFIED");
        fs::remove_dir_all(root).unwrap();
    }
}
