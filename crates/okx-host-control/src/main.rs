use std::{
    io::{self, Read},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use okx_github::GitHubClient;
use okx_host_control::{
    HostControlResult,
    auth::{
        load_native_github_token, migrate_github_token_to_machine, store_native_github_token,
    },
    executor::{HostExecutor, install_current_executable},
    runtime::{process_pending, run_until_shutdown},
    service::{install_service, run_service_dispatcher, start_service},
};
use zeroize::Zeroize;

const INSTALL_PATH: &str = r"C:\okx-control\okx-host-control.exe";

#[derive(Debug, Parser)]
#[command(name = "okx-host-control")]
#[command(about = "Outbound-only native Windows host control plane for iamaman11/okx")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Copy this trusted binary outside the mutable C:\okx worktree.
    Install,

    /// Store the GitHub control token from stdin in Windows Credential Manager.
    SetGithubToken,

    /// Copy current user-scoped controller and agent secrets to the service-safe machine store.
    PrepareServiceSecrets,

    /// Install the Windows SCM service. Requires elevation.
    InstallService,

    /// Start the installed Windows SCM service. Requires service control rights.
    StartService,

    /// Internal SCM entry point. Do not invoke manually.
    Service,

    /// Process pending typed requests once and exit.
    Once,

    /// Poll GitHub control issue #12 over outbound HTTPS.
    Run {
        #[arg(long, default_value_t = 2)]
        poll_seconds: u64,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if matches!(cli.command, Command::Service) {
        run_service_dispatcher()?;
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run(cli))?;
    Ok(())
}

async fn run(cli: Cli) -> HostControlResult<()> {
    match cli.command {
        Command::Install => {
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
        Command::PrepareServiceSecrets => {
            migrate_github_token_to_machine()?;
            let executor = HostExecutor::canonical();
            let agent = executor.migrate_agent_machine_secrets()?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.host-control.machine-secrets/v1",
                    "migrated": true,
                    "agent": agent
                })
            );
        }
        Command::InstallService => {
            install_service()?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.host-control.service/v1",
                    "installed": true,
                    "service": "okx-host-control"
                })
            );
        }
        Command::StartService => {
            start_service()?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.host-control.service/v1",
                    "started": true,
                    "service": "okx-host-control"
                })
            );
        }
        Command::Service => unreachable!("service mode is dispatched before Tokio startup"),
        Command::Once => {
            let token = load_native_github_token()?;
            let github = GitHubClient::new(token, "iamaman11-okx-host-control/0.1")?;
            github.verify_repository_identity().await?;
            let mut executor = HostExecutor::canonical();
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
        Command::Run { poll_seconds } => {
            let token = load_native_github_token()?;
            let github = GitHubClient::new(token, "iamaman11-okx-host-control/0.1")?;
            let mut executor = HostExecutor::canonical();
            run_until_shutdown(&github, &mut executor, poll_seconds).await?;
        }
    }

    Ok(())
}

fn read_stdin() -> HostControlResult<String> {
    let mut payload = String::new();
    io::stdin().read_to_string(&mut payload)?;
    Ok(payload)
}
