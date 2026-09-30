use std::{
    fs,
    io::{self, Read},
    path::PathBuf,
    str::FromStr,
};

use chrono::Utc;

use clap::{Parser, Subcommand};
use okx_agent::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    cloudflare_auth::{load_native_cloudflare_token, store_native_cloudflare_token},
    cloudflare_transport::CloudflareTransportConfig,
    config::{AgentConfig, default_root},
    execution_runtime::ExecutionRuntime,
    github_auth::{load_native_github_token, store_native_github_token},
    github_mailbox::GitHubMailboxClient,
    identity::{
        default_key_id, initialize_native_identity, load_native_identity, load_native_private_key,
    },
    market_bootstrap::MarketBootstrapper,
    okx_credentials::{
        load_native_executor_okx_credentials, load_native_okx_credentials,
        store_native_executor_okx_credentials, store_native_okx_credentials,
    },
    once::{ObservationQueryContext, process_once_now},
    reference_bootstrap::bootstrap_reference,
    runtime::{MailboxRuntimeContext, run_mailbox_until_shutdown, run_until_shutdown},
};
use okx_api::{OkxEnvironment, OkxPublicClient, OkxRestClient, Region};
use okx_protocol::MailboxEnvelope;
use okx_runtime::{PrivateWsCoordinator, PrivateWsHandle, PublicWsCoordinator};
use zeroize::Zeroize;

const DEFAULT_DATA_POLL_SECONDS: u64 = 2;

#[derive(Debug, Parser)]
#[command(name = "okx-agent")]
#[command(about = "Native long-lived OKX observation agent runtime")]
struct Cli {
    #[arg(long, global = true, default_value_os_t = default_root())]
    root: PathBuf,

    #[arg(long, global = true, default_value = default_key_id())]
    key_id: String,

    #[arg(long, global = true, default_value = "global")]
    region: String,

    #[arg(long, global = true)]
    demo: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create the long-lived X25519 agent identity in the native Windows credential store.
    InitKey,

    /// Print only the public agent identity.
    Identity,

    /// Store the GitHub mailbox token from stdin in Windows Credential Manager.
    SetGithubToken,

    /// Store the Cloudflare direct-transport runtime token from stdin.
    SetCloudflareToken,

    /// Store the read-only OKX observer credential payload from stdin.
    SetOkxCredentials,

    /// Store the separate Read + Trade OKX executor credential payload from stdin.
    SetExecutorOkxCredentials,

    /// Process one encrypted mailbox envelope from a file or stdin.
    Once {
        #[arg(long)]
        input: Option<PathBuf>,
    },

    /// Start the native Tokio lifecycle shell. Optionally attach the GitHub mailbox.
    Run {
        #[arg(long)]
        mailbox_issue: Option<u64>,

        #[arg(long, default_value_t = DEFAULT_DATA_POLL_SECONDS)]
        poll_seconds: u64,

        /// Optional outbound Cloudflare runtime WebSocket. GitHub mailbox remains active when set.
        #[arg(long)]
        cloudflare_ws_url: Option<String>,

        #[arg(long, default_value = "windows-primary")]
        cloudflare_runtime_id: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(Cli::parse()).await?;
    Ok(())
}

async fn run(cli: Cli) -> AgentResult<()> {
    let config = AgentConfig::new(cli.root, cli.key_id.clone());
    let environment = OkxEnvironment::new(Region::from_str(&cli.region)?, cli.demo);

    match cli.command {
        Command::InitKey => {
            let identity = initialize_native_identity(&config.key_id)?;
            println!("{}", serde_json::to_string_pretty(&identity)?);
        }
        Command::Identity => {
            let identity = load_native_identity(&config.key_id)?;
            println!("{}", serde_json::to_string_pretty(&identity)?);
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
                    "schema": "okx.agent.github-token/v1",
                    "stored": true
                })
            );
        }
        Command::SetCloudflareToken => {
            let mut token = read_stdin()?;
            while matches!(token.as_bytes().last(), Some(b'\r' | b'\n')) {
                token.pop();
            }
            let result = store_native_cloudflare_token(&token);
            token.zeroize();
            result?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.agent.cloudflare-token/v1",
                    "stored": true
                })
            );
        }
        Command::SetOkxCredentials => {
            let mut payload = read_stdin()?;
            let result = store_native_okx_credentials(&payload);
            payload.zeroize();
            result?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.agent.okx-credentials/v1",
                    "stored": true
                })
            );
        }
        Command::SetExecutorOkxCredentials => {
            let mut payload = read_stdin()?;
            let result = store_native_executor_okx_credentials(&payload);
            payload.zeroize();
            result?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.agent.executor-okx-credentials/v1",
                    "stored": true
                })
            );
        }
        Command::Once { input } => {
            let payload = read_input(input)?;
            let envelope: MailboxEnvelope = serde_json::from_str(&payload)?;
            let mut private_key = load_native_private_key(&config.key_id)?;
            let public_client = OkxPublicClient::new(environment)?;
            let reference = bootstrap_reference(public_client.clone()).await?;
            let market = MarketBootstrapper::new(public_client);
            let account = optional_account_bootstrapper(environment);
            let response = process_once_now(
                &envelope,
                &config.key_id,
                &private_key,
                ObservationQueryContext::standalone_with_account(
                    &reference,
                    &market,
                    account.as_ref(),
                ),
            )
            .await;
            private_key.zeroize();
            println!("{}", serde_json::to_string_pretty(&response?)?);
        }
        Command::Run {
            mailbox_issue,
            poll_seconds,
            cloudflare_ws_url,
            cloudflare_runtime_id,
        } => {
            let identity = load_native_identity(&config.key_id)?;
            if let Some(mailbox_issue) = mailbox_issue {
                let token = load_native_github_token()?;
                let mailbox = GitHubMailboxClient::new(mailbox_issue, token, &config.root)?;
                let public_client = OkxPublicClient::new(environment)?;
                let reference = bootstrap_reference(public_client.clone()).await?;
                eprintln!(
                    "reference registry ready generation={} instruments={}",
                    reference.generation().as_str(),
                    reference.len()
                );
                let market = MarketBootstrapper::new(public_client);
                let (account, private_ws_coordinator, private_ws) =
                    optional_private_components(environment);
                let (public_ws_coordinator, public_ws) =
                    PublicWsCoordinator::new(environment, reference);
                let execution = optional_execution_runtime(&config, environment);
                let cloudflare = cloudflare_ws_url.and_then(|ws_url| {
                    match load_native_cloudflare_token() {
                        Ok(token) => match CloudflareTransportConfig::new(
                            ws_url,
                            cloudflare_runtime_id.clone(),
                            token,
                        ) {
                            Ok(config) => Some(config),
                            Err(error) => {
                                eprintln!(
                                    "Cloudflare direct transport disabled; GitHub fallback remains active: {error}"
                                );
                                None
                            }
                        },
                        Err(error) => {
                            eprintln!(
                                "Cloudflare direct transport credential unavailable; GitHub fallback remains active: {error}"
                            );
                            None
                        }
                    }
                });
                let mut private_key = load_native_private_key(&config.key_id)?;
                let result = run_mailbox_until_shutdown(
                    MailboxRuntimeContext {
                        config: &config,
                        identity: &identity,
                        mailbox: &mailbox,
                        mailbox_issue,
                        agent_private_key: &private_key,
                        public_ws: &public_ws,
                        market: &market,
                        account: account.as_ref(),
                        private_ws: private_ws.as_ref(),
                        execution: execution.as_ref(),
                        cloudflare: cloudflare.as_ref(),
                    },
                    public_ws_coordinator,
                    private_ws_coordinator,
                    poll_seconds,
                )
                .await;
                private_key.zeroize();
                result?;
            } else {
                run_until_shutdown(&config, &identity).await?;
            }
        }
    }

    Ok(())
}

fn optional_execution_runtime(
    config: &AgentConfig,
    environment: OkxEnvironment,
) -> Option<ExecutionRuntime> {
    let credentials = match load_native_executor_okx_credentials() {
        Ok(value) => value,
        Err(AgentError::ExecutorOkxCredentialsNotFound) => {
            eprintln!(
                "OKX executor credential not provisioned; execution operations are NOT_READY"
            );
            return None;
        }
        Err(error) => {
            eprintln!("OKX executor credential unavailable: {error}");
            return None;
        }
    };

    let observed_at_ms = Utc::now().timestamp_millis().max(1) as u64;
    match ExecutionRuntime::new(&config.root, environment, credentials, observed_at_ms) {
        Ok(value) => Some(value),
        Err(error) => {
            eprintln!("execution runtime unavailable: {error}");
            None
        }
    }
}

fn optional_private_components(
    environment: OkxEnvironment,
) -> (
    Option<AccountBootstrapper>,
    Option<PrivateWsCoordinator>,
    Option<PrivateWsHandle>,
) {
    match load_native_okx_credentials() {
        Ok(credentials) => match OkxRestClient::new(environment, credentials.clone()) {
            Ok(client) => {
                let account = AccountBootstrapper::new(client);
                let (private_ws_coordinator, private_ws) =
                    PrivateWsCoordinator::new(environment, credentials);
                (
                    Some(account),
                    Some(private_ws_coordinator),
                    Some(private_ws),
                )
            }
            Err(error) => {
                eprintln!("OKX observer REST client unavailable: {error}");
                (None, None, None)
            }
        },
        Err(AgentError::OkxCredentialsNotFound) => {
            eprintln!("OKX observer credential not provisioned; private queries are NOT_READY");
            (None, None, None)
        }
        Err(error) => {
            eprintln!("OKX observer credential unavailable: {error}");
            (None, None, None)
        }
    }
}

fn optional_account_bootstrapper(environment: OkxEnvironment) -> Option<AccountBootstrapper> {
    match load_native_okx_credentials() {
        Ok(credentials) => match OkxRestClient::new(environment, credentials) {
            Ok(client) => Some(AccountBootstrapper::new(client)),
            Err(error) => {
                eprintln!("OKX observer REST client unavailable: {error}");
                None
            }
        },
        Err(AgentError::OkxCredentialsNotFound) => {
            eprintln!("OKX observer credential not provisioned; private queries are NOT_READY");
            None
        }
        Err(error) => {
            eprintln!("OKX observer credential unavailable: {error}");
            None
        }
    }
}

fn read_input(input: Option<PathBuf>) -> AgentResult<String> {
    match input {
        Some(path) => Ok(fs::read_to_string(path)?),
        None => read_stdin(),
    }
}

fn read_stdin() -> AgentResult<String> {
    let mut payload = String::new();
    io::stdin().read_to_string(&mut payload)?;
    Ok(payload)
}
