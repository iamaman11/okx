use clap::{Parser, Subcommand};
use okx_agent::{
    AgentError, AgentResult,
    accounts::AccountRuntimeRegistry,
    analytics::AnalyticsService,
    cloudflare_auth::{load_native_cloudflare_token, store_native_cloudflare_token},
    cloudflare_transport::CloudflareTransportConfig,
    config::{AgentConfig, default_root},
    github_auth::{load_native_github_token, store_native_github_token},
    github_mailbox::GitHubMailboxClient,
    identity::{
        default_key_id, initialize_native_identity, load_native_identity, load_native_private_key,
    },
    market_bootstrap::MarketBootstrapper,
    query::DispatchContext,
    reference_bootstrap::bootstrap_reference,
    risk_bridge::NativeRiskClient,
    runtime::{EngineContext, run_engine},
    trade_service::TradeService,
};
use okx_api::{OkxEnvironment, OkxPublicClient, RateBudget, Region};
use okx_protocol::{DirectRuntimeProfile, MailboxEnvelope};
use okx_runtime::PublicWsCoordinator;
use std::{
    fs::OpenOptions,
    io::{self, Read},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};
use zeroize::{Zeroize, Zeroizing};
#[derive(Parser)]
#[command(name = "okx-agent")]
struct Cli {
    #[arg(long,global=true,default_value_os_t=default_root())]
    root: PathBuf,
    #[arg(long,global=true,default_value=default_key_id())]
    key_id: String,
    #[arg(long, global = true, default_value = "global")]
    region: String,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    InitKey,
    Identity,
    SetGithubToken,
    SetCloudflareToken,
    SetAccountCredentials {
        #[arg(long)]
        credential_ref: String,
    },
    InitializeInstallation {
        #[arg(long)]
        installation_id: String,
        #[arg(long)]
        expected_release: String,
        #[arg(long)]
        operation_id: String,
    },
    InitializeAccount {
        #[arg(long)]
        account_key: String,
        #[arg(long)]
        installation_id: String,
        #[arg(long)]
        expected_release: String,
        #[arg(long)]
        operation_id: String,
    },
    BackupAccount {
        #[arg(long)]
        account_key: String,
    },
    RestoreAccount {
        #[arg(long)]
        account_key: String,
        #[arg(long)]
        snapshot_id: String,
        #[arg(long)]
        expected_release: String,
        #[arg(long)]
        operation_id: String,
    },
    Once {
        #[arg(long)]
        input: Option<PathBuf>,
    },
    Run {
        #[arg(long)]
        mailbox_issue: Option<u64>,
        #[arg(long, default_value_t = 15)]
        poll_seconds: u64,
        #[arg(long)]
        cloudflare_ws_url: Option<String>,
        #[arg(long, default_value = "windows-primary")]
        cloudflare_runtime_id: String,
        #[arg(long)]
        native_child_control_stdio: bool,
    },
}
fn input() -> AgentResult<Zeroizing<String>> {
    let mut text = Zeroizing::new(String::new());
    io::stdin().take(65537).read_to_string(&mut text)?;
    if text.len() > 65536 {
        return Err(AgentError::InvalidOkxCredentials);
    }
    Ok(text)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(Cli::parse()).await?;
    Ok(())
}
async fn run(cli: Cli) -> AgentResult<()> {
    let config = AgentConfig::new(cli.root, cli.key_id);
    let env = OkxEnvironment::new(Region::from_str(&cli.region)?, false);
    match cli.command {
        Command::InitKey => println!(
            "{}",
            serde_json::to_string(&initialize_native_identity(&config.key_id)?)?
        ),
        Command::Identity => println!(
            "{}",
            serde_json::to_string(&load_native_identity(&config.key_id)?)?
        ),
        Command::SetGithubToken => {
            let mut s = input()?;
            while s.ends_with(['\r', '\n']) {
                s.pop();
            }
            store_native_github_token(&s)?;
            println!("{{\"stored\":true}}");
        }
        Command::SetCloudflareToken => {
            let mut s = input()?;
            while s.ends_with(['\r', '\n']) {
                s.pop();
            }
            store_native_cloudflare_token(&s)?;
            println!("{{\"stored\":true}}");
        }
        Command::SetAccountCredentials { credential_ref } => {
            let s = input()?;
            okx_agent::okx_credentials::store_native_account_credentials(
                &credential_ref,
                s.as_bytes(),
            )?;
            println!("{{\"stored\":true}}");
        }
        Command::InitializeInstallation {
            installation_id,
            expected_release,
            operation_id,
        } => {
            let result = okx_agent::storage_commands::initialize_installation(
                &config.root,
                &installation_id,
                &expected_release,
                &operation_id,
            )
            .await?;
            println!("{}", result);
        }
        Command::InitializeAccount {
            account_key,
            installation_id,
            expected_release,
            operation_id,
        } => {
            let result = okx_agent::storage_commands::initialize_account(
                &config.root,
                &account_key,
                &installation_id,
                &expected_release,
                &operation_id,
            )
            .await?;
            println!("{}", result);
        }
        Command::BackupAccount { account_key } => {
            println!(
                "{}",
                okx_agent::storage_commands::backup_account(&config.root, &account_key).await?
            );
        }
        Command::RestoreAccount {
            account_key,
            snapshot_id,
            expected_release,
            operation_id,
        } => {
            println!(
                "{}",
                okx_agent::storage_commands::restore_account(
                    &config.root,
                    &account_key,
                    &snapshot_id,
                    &expected_release,
                    &operation_id
                )
                .await?
            );
        }
        command => {
            // Existing installation is required. Startup never creates financial storage.
            let _engine_lease = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(config.root.join("engine.lock"))?;
            _engine_lease
                .try_lock()
                .map_err(|_| AgentError::FinancialCaptureTask("ENGINE_ALREADY_RUNNING".into()))?;
            let budget = RateBudget::new();
            let accounts =
                Arc::new(AccountRuntimeRegistry::open(&config.root, budget.clone()).await?);
            let public_client = OkxPublicClient::with_rate_budget(env, budget.clone())?;
            let reference = bootstrap_reference(public_client.clone()).await?;
            let (public, public_ws) =
                PublicWsCoordinator::new_with_rate_budget(env, reference, budget);
            let market = MarketBootstrapper::new(public_client);
            let analytics = AnalyticsService::new(config.root.join("analytics"), market.clone())
                .map_err(AgentError::ResearchSessionTask)?;
            let (risk_tx, risk) = tokio::sync::mpsc::channel(32);
            let trade = Arc::new(TradeService::new(
                accounts.clone(),
                public_ws.clone(),
                NativeRiskClient::new(risk_tx),
            ));
            trade.set_analytics(analytics.clone()).await;
            let dispatch = DispatchContext {
                accounts,
                analytics,
                public_ws,
                trade,
                market,
            };
            let private_key = Zeroizing::new(load_native_private_key(&config.key_id)?);
            match command {
                Command::Once { input: path } => {
                    let mut data = match path {
                        Some(path) => Zeroizing::new(std::fs::read_to_string(path)?),
                        None => input()?,
                    };
                    let envelope: MailboxEnvelope = serde_json::from_str(&data)?;
                    data.zeroize();
                    let reply = okx_agent::once::process_once_now(
                        &envelope,
                        &config.key_id,
                        &private_key,
                        &dispatch,
                    )
                    .await?;
                    println!("{}", serde_json::to_string(&reply)?);
                    dispatch.accounts.shutdown().await;
                }
                Command::Run {
                    mailbox_issue,
                    poll_seconds,
                    cloudflare_ws_url,
                    cloudflare_runtime_id,
                    native_child_control_stdio,
                } => {
                    if !(1..=60).contains(&poll_seconds) {
                        return Err(AgentError::InvalidPollInterval);
                    }
                    let transport = cloudflare_ws_url
                        .map(|url| {
                            CloudflareTransportConfig::new(
                                url,
                                cloudflare_runtime_id,
                                load_native_cloudflare_token()?,
                                DirectRuntimeProfile::Engine,
                            )
                        })
                        .transpose()?;
                    let mailbox = mailbox_issue
                        .map(|issue| {
                            GitHubMailboxClient::new(
                                issue,
                                load_native_github_token()?,
                                &config.root,
                            )
                        })
                        .transpose()?;
                    run_engine(EngineContext {
                        dispatch,
                        public,
                        transport,
                        risk,
                        mailbox,
                        private_key,
                        key_id: config.key_id,
                        poll_seconds,
                        native_child_control_stdio,
                    })
                    .await?;
                }
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}
