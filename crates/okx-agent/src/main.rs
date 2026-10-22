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
    execution_runtime::{ExecutionRuntime, ExecutionRuntimeRegistry},
    github_auth::{load_native_github_token, store_native_github_token},
    github_mailbox::GitHubMailboxClient,
    identity::{
        default_key_id, initialize_native_identity, load_native_identity, load_native_private_key,
    },
    market_bootstrap::MarketBootstrapper,
    okx_credentials::{
        load_native_executor_okx_credentials_for_environment,
        load_native_okx_credentials_for_environment, store_native_demo_executor_okx_credentials,
        store_native_demo_okx_credentials, store_native_executor_okx_credentials,
        store_native_okx_credentials,
    },
    once::{ObservationQueryContext, process_once_now},
    reference_bootstrap::bootstrap_reference,
    runtime::{MailboxRuntimeContext, run_mailbox_until_shutdown, run_until_shutdown},
};
use okx_api::{OkxEnvironment, OkxPublicClient, OkxRestClient, RateBudget, Region};
use okx_execution::{ExecutionLedgerStore, LedgerEnvironment, LedgerVenue, SqliteAccountIdentity};
use okx_protocol::{DirectRuntimeProfile, MailboxEnvelope};
use okx_runtime::{PrivateWsCoordinator, PrivateWsHandle, PublicWsCoordinator};
use zeroize::Zeroize;

const DEFAULT_DATA_POLL_SECONDS: u64 = 15;

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

    /// Store the read-only OKX Demo observer credential payload from stdin.
    SetDemoOkxCredentials,

    /// Store the separate Read + Trade OKX Demo executor credential payload from stdin.
    SetDemoExecutorOkxCredentials,

    /// Single fresh SQL owner installation. Never imports v14 data.
    /// CONTROL must be STOPPED; exchange verification is read-only.
    StorageInitialize {
        #[arg(long)]
        expected_uid_fingerprint: String,
    },

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

        /// Explicitly admit bounded OKX Demo mutation acceptance. Requires --demo.
        #[arg(long)]
        demo_mutation_acceptance: bool,

        /// Native CONTROL policy-selected Production executor, not a pilot mode.
        #[arg(long)]
        production_trading: bool,

        /// Parent-owned anonymous stdin/stderr pipe, never a network listener.
        #[arg(long)]
        native_child_control_stdio: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run(Cli::parse()).await?;
    Ok(())
}

async fn run(cli: Cli) -> AgentResult<()> {
    let config = AgentConfig::new(cli.root, cli.key_id.clone());
    // Account venue is Demo for physical acceptance or Production for read-only.
    // Market observation, reference registry and research always use one
    // production-public source. Never couple signal truth to the account switch.
    let environment = OkxEnvironment::new(Region::from_str(&cli.region)?, cli.demo);
    let market_environment = environment.production_public();

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
        Command::SetDemoOkxCredentials => {
            let mut payload = read_stdin()?;
            let result = store_native_demo_okx_credentials(&payload);
            payload.zeroize();
            result?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.agent.demo-okx-credentials/v1",
                    "stored": true
                })
            );
        }
        Command::SetDemoExecutorOkxCredentials => {
            let mut payload = read_stdin()?;
            let result = store_native_demo_executor_okx_credentials(&payload);
            payload.zeroize();
            result?;
            println!(
                "{}",
                serde_json::json!({
                    "schema": "okx.agent.demo-executor-okx-credentials/v1",
                    "stored": true
                })
            );
        }
        Command::StorageInitialize {
            expected_uid_fingerprint,
        } => {
            let observer_credentials = load_native_okx_credentials_for_environment(environment)?;
            let observer = OkxRestClient::with_rate_budget(
                environment,
                observer_credentials,
                RateBudget::new(),
            )?;
            let account = AccountBootstrapper::new(observer);
            let snapshot = account
                .snapshot()
                .await
                .map_err(|e| AgentError::FinancialCaptureTask(e.to_string()))?;
            let facts = account
                .ledger_facts(&snapshot)
                .await
                .map_err(|e| AgentError::FinancialCaptureTask(e.to_string()))?;
            let algo = account
                .pending_derivative_risk_algos()
                .await
                .map_err(|e| AgentError::FinancialCaptureTask(e.to_string()))?;
            // A fresh financial baseline is possible only when the venue,
            // not the old local manifest, independently proves no live risk.
            if facts.summary.authority.account_uid_fingerprint != expected_uid_fingerprint
                || facts.summary.open_positions != 0
                || facts.summary.pending_orders != 0
                || !algo.complete_within_bound
                || algo.rows != 0
                || algo.received_at_ms == 0
            {
                return Err(
                    okx_execution::ExecutionLedgerError::CapitalReservationUnattested.into(),
                );
            }
            let account_identity = SqliteAccountIdentity {
                venue: LedgerVenue::Okx,
                environment: if environment.demo {
                    LedgerEnvironment::Demo
                } else {
                    LedgerEnvironment::Production
                },
                account_id: if environment.demo { "demo" } else { "real" }.to_owned(),
                uid_fingerprint: expected_uid_fingerprint,
                bound_at_ms: u64::try_from(Utc::now().timestamp_millis()).unwrap_or_default(),
            };
            let chosen =
                ExecutionLedgerStore::initialize_fresh_account(&config.root, &account_identity)?;
            let proof = chosen.attest_storage()?;
            let bound = chosen
                .bound_native_account_identity()?
                .ok_or(okx_execution::ExecutionLedgerError::AccountScopeMismatch)?;
            if bound != account_identity {
                return Err(okx_execution::ExecutionLedgerError::AccountScopeMismatch.into());
            }
            println!(
                "{}",
                serde_json::json!({
                    "schema":"okx.native-storage-initialization/v1",
                    "owner":"SQLITE",
                    "format":proof.on_disk_schema,
                    "integrity":proof.integrity,
                    "manifest_sha256":proof.manifest_sha256,
                    "account_binding":{
                        "venue":bound.venue.as_str(),
                        "environment":if environment.demo {"demo"}else{"production"},
                        "account_id":bound.account_id,
                        "uid_fingerprint":bound.uid_fingerprint,
                        "immutable":true
                    },
                    "trading_authorized":false
                })
            );
        }
        Command::Once { input } => {
            let payload = read_input(input)?;
            let envelope: MailboxEnvelope = serde_json::from_str(&payload)?;
            let mut private_key = load_native_private_key(&config.key_id)?;
            let rate_budget = RateBudget::new();
            let public_client =
                OkxPublicClient::with_rate_budget(market_environment, rate_budget.clone())?;
            let reference = bootstrap_reference(public_client.clone()).await?;
            let market = MarketBootstrapper::new(public_client);
            let account = optional_account_bootstrapper(environment, rate_budget);
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
            demo_mutation_acceptance,
            production_trading,
            native_child_control_stdio,
        } => {
            if demo_mutation_acceptance && !environment.demo {
                return Err(AgentError::DemoMutationAcceptanceRequiresDemoEnvironment);
            }
            if production_trading && (environment.demo || !native_child_control_stdio) {
                return Err(AgentError::ProductionPolicyUnavailable);
            }
            let identity = load_native_identity(&config.key_id)?;
            if let Some(mailbox_issue) = mailbox_issue {
                let token = load_native_github_token()?;
                let mailbox = GitHubMailboxClient::new(mailbox_issue, token, &config.root)?;
                let rate_budget = RateBudget::new();
                let public_client =
                    OkxPublicClient::with_rate_budget(market_environment, rate_budget.clone())?;
                let reference = bootstrap_reference(public_client.clone()).await?;
                eprintln!(
                    "reference registry ready generation={} instruments={}",
                    reference.generation().as_str(),
                    reference.len()
                );
                let market = MarketBootstrapper::new(public_client);
                let (account, private_ws_coordinator, private_ws) =
                    optional_private_components(environment, rate_budget.clone());
                let (public_ws_coordinator, public_ws) = PublicWsCoordinator::new_with_rate_budget(
                    market_environment,
                    reference,
                    rate_budget.clone(),
                );
                let candidate_execution = optional_execution_runtime(
                    &config,
                    environment,
                    rate_budget.clone(),
                    demo_mutation_acceptance,
                    production_trading,
                )
                .map(std::sync::Arc::new);
                // Exactly one registered native account executor for the
                // selected CONTROL identity. A missing SQL account never
                // becomes a legacy runtime or a second transport.
                let mut account_runtimes = ExecutionRuntimeRegistry::new();
                let execution = match candidate_execution {
                    Some(owner) => {
                        let key = account_runtimes
                            .register_existing(owner)
                            .map_err(AgentError::from)?;
                        account_runtimes.get(&key)
                    }
                    None => None,
                };
                let cloudflare = cloudflare_ws_url.and_then(|ws_url| {
                    match load_native_cloudflare_token() {
                        Ok(token) => match CloudflareTransportConfig::new(
                            ws_url,
                            cloudflare_runtime_id.clone(),
                            token,
                            if environment.demo {
                                DirectRuntimeProfile::DemoAcceptance
                            } else {
                                DirectRuntimeProfile::Production
                            },
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
                        native_child_control_stdio,
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
    rate_budget: RateBudget,
    demo_mutation_acceptance: bool,
    production_trading: bool,
) -> Option<ExecutionRuntime> {
    let credentials = match load_native_executor_okx_credentials_for_environment(environment) {
        Ok(value) => value,
        Err(
            AgentError::ExecutorOkxCredentialsNotFound
            | AgentError::DemoExecutorOkxCredentialsNotFound,
        ) => {
            eprintln!(
                "OKX executor credential for the selected environment is not provisioned; execution operations are NOT_READY"
            );
            return None;
        }
        Err(error) => {
            eprintln!("OKX executor credential unavailable: {error}");
            return None;
        }
    };

    let observed_at_ms = Utc::now().timestamp_millis().max(1) as u64;
    let runtime = if production_trading {
        ExecutionRuntime::new_production(
            &config.root,
            environment,
            credentials,
            observed_at_ms,
            rate_budget,
        )
    } else if demo_mutation_acceptance {
        ExecutionRuntime::new_demo_acceptance(
            &config.root,
            environment,
            credentials,
            observed_at_ms,
            rate_budget,
        )
    } else {
        ExecutionRuntime::new(
            &config.root,
            environment,
            credentials,
            observed_at_ms,
            rate_budget,
        )
    };
    match runtime {
        Ok(value) => Some(value),
        Err(error) => {
            eprintln!("execution runtime unavailable: {error}");
            None
        }
    }
}

fn optional_private_components(
    environment: OkxEnvironment,
    rate_budget: RateBudget,
) -> (
    Option<AccountBootstrapper>,
    Option<PrivateWsCoordinator>,
    Option<PrivateWsHandle>,
) {
    match load_native_okx_credentials_for_environment(environment) {
        Ok(credentials) => match OkxRestClient::with_rate_budget(
            environment,
            credentials.clone(),
            rate_budget.clone(),
        ) {
            Ok(client) => {
                let account = AccountBootstrapper::new(client);
                let (private_ws_coordinator, private_ws) =
                    PrivateWsCoordinator::new_with_rate_budget(
                        environment,
                        credentials,
                        rate_budget,
                    );
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
        Err(AgentError::OkxCredentialsNotFound | AgentError::DemoOkxCredentialsNotFound) => {
            eprintln!(
                "OKX observer credential for the selected environment is not provisioned; private queries are NOT_READY"
            );
            (None, None, None)
        }
        Err(error) => {
            eprintln!("OKX observer credential unavailable: {error}");
            (None, None, None)
        }
    }
}

fn optional_account_bootstrapper(
    environment: OkxEnvironment,
    rate_budget: RateBudget,
) -> Option<AccountBootstrapper> {
    match load_native_okx_credentials_for_environment(environment) {
        Ok(credentials) => {
            match OkxRestClient::with_rate_budget(environment, credentials, rate_budget) {
                Ok(client) => Some(AccountBootstrapper::new(client)),
                Err(error) => {
                    eprintln!("OKX observer REST client unavailable: {error}");
                    None
                }
            }
        }
        Err(AgentError::OkxCredentialsNotFound | AgentError::DemoOkxCredentialsNotFound) => {
            eprintln!(
                "OKX observer credential for the selected environment is not provisioned; private queries are NOT_READY"
            );
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
