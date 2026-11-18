use clap::{Parser, Subcommand};
use okx_agent::{
    AgentError, AgentResult,
    account_bootstrap::AccountBootstrapper,
    cloudflare_auth::{load_native_cloudflare_token, store_native_cloudflare_token},
    cloudflare_transport::CloudflareTransportConfig,
    config::{AgentConfig, default_root},
    github_auth::{load_native_github_token, store_native_github_token},
    github_mailbox::GitHubMailboxClient,
    identity::{
        default_key_id, initialize_native_identity, load_native_identity, load_native_private_key,
    },
    market_bootstrap::MarketBootstrapper,
    okx_credentials::load_native_account_credentials,
    query::DispatchContext,
    research_client::ResearchClient,
    runtime::{EngineContext, run_engine},
};
use okx_api::{OkxEnvironment, OkxPublicClient, OkxRestClient, RateBudget, Region};
use okx_protocol::{
    DirectRuntimeProfile, MailboxEnvelope, account_catalog_proto, accounts::Environment,
};
use okx_runtime::PublicWsCoordinator;
use std::{
    fs::OpenOptions,
    io::{self, Read},
    path::{Path, PathBuf},
    str::FromStr,
    time::{Duration, Instant},
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
    /// Exact immutable single campaign expected for the stopped imported
    /// finance.sqlite. Enables read-only native Run/Once, never live trading.
    #[arg(long, global = true)]
    stopped_finance_campaign: Option<String>,
    /// New Native finance requires explicit initialization and verified
    /// account sources; selecting this route never creates financial state.
    #[arg(long, global = true, conflicts_with = "stopped_finance_campaign")]
    native_finance_campaign: Option<String>,
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
    /// One-shot authenticated REST census; does not open the Engine or finance SQL.
    InspectVenue {
        #[arg(long)]
        account: String,
    },
    BackupAccount {
        #[arg(long)]
        account_key: String,
    },
    /// Back up the stopped installation's complete financial SQLite.
    BackupFinance {},
    /// Restore a missing financial SQLite, preserving identity and requiring reconciliation.
    RestoreFinance {
        #[arg(long)]
        snapshot_id: String,
        #[arg(long)]
        expected_release: String,
        #[arg(long)]
        operation_id: String,
    },
    /// Inspect a verified stopped 47-table finance import. No trading,
    /// account writers, Cloudflare session or financial mutation is started.
    InspectFinance {
        #[arg(long)]
        installation_id: String,
        #[arg(long = "expected-account", required = true)]
        expected_accounts: Vec<String>,
        #[arg(long = "expected-campaign", required = true)]
        expected_campaigns: Vec<String>,
        #[arg(long)]
        campaign: String,
        #[arg(long)]
        account: Option<String>,
        #[arg(long)]
        reservation_key: Option<String>,
        #[arg(long)]
        plan_sha256: Option<String>,
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
/// Uses only the existing named observer credential. The one-shot executable
/// never acquires engine.lock, starts AccountRuntimeRegistry, or writes SQL.
/// Counts are venue GET facts, not proof that local intents are clear.
async fn inspect_venue(root: &Path, account: &str) -> AgentResult<serde_json::Value> {
    let catalog = account_catalog_proto::load(root)?;
    let entry = catalog
        .accounts
        .iter()
        .find(|entry| entry.alias == account || entry.account_key.wire() == account)
        .ok_or_else(|| AgentError::FinancialCaptureTask("VENUE_ACCOUNT_UNAVAILABLE".into()))?;
    let credentials = load_native_account_credentials(&entry.credential_ref)?;
    let expected_demo = entry.account_key.environment == Environment::Demo;
    if credentials.environment.demo != expected_demo {
        return Err(AgentError::FinancialCaptureTask(
            "VENUE_ENVIRONMENT_MISMATCH".into(),
        ));
    }
    let client = OkxRestClient::new(credentials.environment, credentials.observer)?;
    let observer = AccountBootstrapper::new(client);
    let deadline = Instant::now() + Duration::from_secs(45);
    // snapshot_until validates that the credential is strictly read-only and
    // fetches authenticated account/positions/ordinary-orders from the venue.
    let snapshot = observer
        .snapshot_until(deadline)
        .await
        .map_err(|_| AgentError::FinancialCaptureTask("VENUE_SNAPSHOT_INCOMPLETE".into()))?;
    if snapshot.account_uid_fingerprint != entry.account_key.verified_uid {
        return Err(AgentError::FinancialCaptureTask(
            "VENUE_IDENTITY_MISMATCH".into(),
        ));
    }
    let algos = observer
        .pending_derivative_risk_algos_until(deadline)
        .await
        .map_err(|_| AgentError::FinancialCaptureTask("VENUE_ALGO_INVENTORY_INCOMPLETE".into()))?;
    // Additional unaffiliated-asset coverage: omit instType to enumerate all
    // documented algo types across SPOT/MARGIN/FUTURES/SWAP. This detects
    // orders capable of creating future exposure outside this Agent's SWAP policy.
    let account_algos = observer
        .pending_account_algo_risk_until(deadline)
        .await
        .map_err(|_| {
            AgentError::FinancialCaptureTask("VENUE_ACCOUNT_ALGO_INVENTORY_INCOMPLETE".into())
        })?;
    let complete_derivative = algos.complete_within_bound && algos.scopes_checked == 16;
    let complete_account = account_algos.complete_within_bound && account_algos.scopes_checked == 8;
    let complete = complete_derivative && complete_account;
    let state = if !complete {
        "INCOMPLETE"
    } else if snapshot.positions.is_empty()
        && snapshot.pending_orders.is_empty()
        && algos.rows == 0
        && account_algos.rows == 0
    {
        "ACCOUNT_EXPOSURE_EMPTY"
    } else {
        "EXPOSURE_PRESENT"
    };
    Ok(serde_json::json!({
        "schema": "okx.venue-opening-inventory/v1",
        "installation_id": catalog.installation_id,
        "account": entry.account_key.wire(),
        "state": state,
        "complete_derivative_algo_inventory": complete_derivative,
        "complete_account_algo_inventory": complete_account,
        "position_rows": snapshot.positions.len(),
        "pending_order_rows": snapshot.pending_orders.len(),
        "pending_derivative_algo_rows": algos.rows,
        "derivative_algo_scopes_checked": algos.scopes_checked,
        "pending_account_algo_rows": account_algos.rows,
        "account_algo_scopes_checked": account_algos.scopes_checked,
        "account_algo_received_at_ms": account_algos.received_at_ms,
        "snapshot_received_at": snapshot.source_received_at,
        "algo_received_at_ms": algos.received_at_ms,
        "observer_permissions": snapshot.api_key_permissions,
        "authenticated_get_only": true,
        "spot_algo_inventory": "INCLUDED_IN_ACCOUNT_ALGO_CENSUS",
        "local_unknown_intents": "NOT_EVALUATED",
        "safe_to_restart": false
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Process entrypoint only. stdout remains reserved for command responses.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .map_err(|_| std::io::Error::other("TRACING_SUBSCRIBER_INIT_FAILED"))?;
    run(Cli::parse()).await?;
    Ok(())
}
async fn run(cli: Cli) -> AgentResult<()> {
    let config = AgentConfig::new(cli.root, cli.key_id);
    let stopped_finance_campaign = cli.stopped_finance_campaign;
    let native_finance_campaign = cli.native_finance_campaign;
    let stopped_finance_mode = stopped_finance_campaign.is_some();
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
        Command::InspectFinance {
            installation_id,
            expected_accounts,
            expected_campaigns,
            campaign,
            account,
            reservation_key,
            plan_sha256,
        } => {
            let request = okx_agent::finance_inspection::StoppedFinanceInspectionRequest {
                installation_id,
                expected_accounts,
                expected_campaigns,
                campaign,
                account,
                reservation_key,
                plan_sha256,
            };
            let result = okx_agent::finance_inspection::inspect(&config.root, request).await?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Command::InspectVenue { account } => {
            let result = inspect_venue(&config.root, &account).await?;
            println!("{}", serde_json::to_string(&result)?);
        }
        Command::BackupFinance {} => {
            println!(
                "{}",
                okx_agent::storage_commands::backup_finance(&config.root).await?
            );
        }
        Command::RestoreFinance {
            snapshot_id,
            expected_release,
            operation_id,
        } => {
            println!(
                "{}",
                okx_agent::storage_commands::restore_finance(
                    &config.root,
                    &snapshot_id,
                    &expected_release,
                    &operation_id,
                )
                .await?
            );
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
            // Authenticate the existing host identity before source, network, or worker startup.
            let private_key = Zeroizing::new(load_native_private_key(&config.key_id)?);
            use sha2::{Digest, Sha256};
            let public_key = okx_protocol::crypto::public_key_from_private(*private_key);
            let scope = format!("{:x}", Sha256::digest(public_key));
            // Existing installation only: startup never creates financial storage.
            std::fs::create_dir_all(&config.root)?;
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
            let selection = okx_agent::finance_startup::select(
                &config.root,
                budget.clone(),
                stopped_finance_mode,
                stopped_finance_campaign.or(native_finance_campaign),
            )
            .await;
            let public_client = OkxPublicClient::with_rate_budget(env, budget.clone())?;
            let (public, public_ws) =
                PublicWsCoordinator::new_uninitialized(env, budget, public_client.clone());
            let market = MarketBootstrapper::new(public_client);
            let research = ResearchClient::installed(
                config.root.clone(),
                config.root.join("analytics"),
                okx_protocol::research_ipc::ResearchScope::new(scope.clone())
                    .map_err(|reason| AgentError::ResearchSessionTask(reason.into()))?,
                market.clone(),
            );
            let finance = selection.finish(public_ws.clone(), market.clone()).await;
            let dispatch = DispatchContext {
                finance,
                research,
                public_ws,
                market,
            };
            match command {
                Command::Once { input: path } => {
                    let mut data = match path {
                        Some(path) => Zeroizing::new(std::fs::read_to_string(path)?),
                        None => input()?,
                    };
                    let envelope: MailboxEnvelope = serde_json::from_str(&data)?;
                    data.zeroize();
                    let (public_stop, public_shutdown) = tokio::sync::watch::channel(false);
                    let public_task = tokio::spawn(public.run(public_shutdown));
                    let result = okx_agent::once::process_once_now(
                        &envelope,
                        &config.key_id,
                        &private_key,
                        &dispatch,
                    )
                    .await;
                    let _ = public_stop.send(true);
                    let mut public_task = public_task;
                    if tokio::time::timeout(std::time::Duration::from_secs(2), &mut public_task)
                        .await
                        .is_err()
                    {
                        public_task.abort();
                        let _ = public_task.await;
                    }
                    dispatch.research.shutdown().await;
                    dispatch.finance.shutdown().await;
                    println!("{}", serde_json::to_string(&result?)?);
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
                                scope.clone(),
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

#[cfg(test)]
mod native_finance_startup_tests {
    use super::*;
    use okx_protocol::{
        account_catalog_proto,
        accounts::{AccountKey, Environment, VerifiedAccountConfig},
    };
    use sha2::Digest;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn root() -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "okx-510-engine-native-startup-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn startup(root: PathBuf, campaign: Option<String>) -> Cli {
        Cli {
            root,
            key_id: "agent-key-1".into(),
            region: "global".into(),
            stopped_finance_campaign: campaign,
            native_finance_campaign: None,
            command: Command::Run {
                mailbox_issue: None,
                poll_seconds: 15,
                cloudflare_ws_url: None,
                cloudflare_runtime_id: "windows-primary".into(),
                native_child_control_stdio: false,
            },
        }
    }

    // Financial unavailability no longer aborts the research host. The public
    // dispatch contract separately proves research availability and exact denial.
    async fn select_cli(cli: Cli) -> okx_agent::finance_startup::FinanceSelection {
        let stopped = cli.stopped_finance_campaign.is_some();
        okx_agent::finance_startup::select(
            &cli.root,
            RateBudget::new(),
            stopped,
            cli.stopped_finance_campaign.or(cli.native_finance_campaign),
        )
        .await
    }

    fn assert_corrupt(result: okx_agent::finance_startup::FinanceSelection) {
        use okx_agent::finance_startup::{FinanceSelection, FinanceStartupCause};
        match result {
            FinanceSelection::Failed(failure) => {
                assert_eq!(failure.code(), "FINANCE_FILE_FORMAT_UNKNOWN");
                assert!(matches!(
                    failure.cause(),
                    FinanceStartupCause::Owner(AgentError::Store(
                        okx_execution::StoreError::Invalid("FINANCE_FILE_FORMAT_UNKNOWN")
                    ))
                ));
            }
            _ => panic!("corrupt finance must not open an owner"),
        }
    }

    #[tokio::test]
    async fn finance_file_without_explicit_campaign_cannot_start_legacy_run() {
        let path = root();
        account_catalog_proto::initialize(&path, "installation-test").unwrap();
        let catalog = path.join(account_catalog_proto::CATALOG_FILE);
        let before_catalog = std::fs::read(&catalog).unwrap();
        let finance = path.join("finance").join("finance.sqlite");
        std::fs::create_dir_all(finance.parent().unwrap()).unwrap();
        std::fs::write(&finance, b"sealed-finance-sentinel").unwrap();
        assert_corrupt(select_cli(startup(path.clone(), None)).await);
        assert_eq!(std::fs::read(&finance).unwrap(), b"sealed-finance-sentinel");
        assert_eq!(std::fs::read(catalog).unwrap(), before_catalog);
        assert!(!path.join("accounts").exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn default_startup_without_finance_never_creates_legacy_or_remote_owner() {
        let path = root();
        assert!(matches!(
            select_cli(startup(path.clone(), None)).await,
            okx_agent::finance_startup::FinanceSelection::Missing
        ));
        assert!(!path.join("accounts").exists());
        assert!(!path.join("finance").exists());
        assert!(!path.join(account_catalog_proto::CATALOG_FILE).exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn explicit_native_mode_without_finance_file_never_falls_back_to_cloudflare() {
        let path = root();
        assert!(matches!(
            select_cli(startup(path.clone(), Some("campaign-X".into()))).await,
            okx_agent::finance_startup::FinanceSelection::Missing
        ));
        assert!(!path.join("accounts").exists());
        assert!(!path.join("finance").exists());
        assert!(!path.join(account_catalog_proto::CATALOG_FILE).exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn live_native_selection_cannot_initialize_or_use_legacy_without_finance() {
        let path = root();
        let mut cli = startup(path.clone(), None);
        cli.native_finance_campaign = Some("campaign-X".into());
        assert!(matches!(
            select_cli(cli).await,
            okx_agent::finance_startup::FinanceSelection::Missing
        ));
        assert!(!path.join("accounts").exists());
        assert!(!path.join("finance").exists());
        assert!(!path.join(account_catalog_proto::CATALOG_FILE).exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn stopped_and_native_finance_modes_are_mutually_exclusive() {
        assert!(
            Cli::try_parse_from([
                "okx-agent",
                "--stopped-finance-campaign",
                "campaign-X",
                "--native-finance-campaign",
                "campaign-X",
                "run",
            ])
            .is_err()
        );
    }

    #[tokio::test]
    async fn stopped_native_run_uses_catalog_roster_and_never_opens_legacy_account_writer() {
        let path = root();
        account_catalog_proto::initialize(&path, "installation-test").unwrap();
        let account = VerifiedAccountConfig {
            account_key: AccountKey {
                venue: "okx".into(),
                environment: Environment::Real,
                verified_uid: "a".repeat(64),
            },
            alias: "account_one".into(),
            credential_ref: "credential_one".into(),
            connect_operation_id: "connect_1234".into(),
            configuration_digest: "b".repeat(64),
        };
        account_catalog_proto::append(&path, &account).unwrap();
        let finance = path.join("finance").join("finance.sqlite");
        std::fs::create_dir_all(finance.parent().unwrap()).unwrap();
        // Invalid SQL must fail at the ORIGINAL unified import checker.
        // No native mode may quietly fall back to a legacy account writer,
        // Cloudflare financial request or unauthorized empty bootstrap.
        std::fs::write(&finance, b"not-a-valid-financial-import").unwrap();
        let catalog = path.join(account_catalog_proto::CATALOG_FILE);
        let before_catalog = std::fs::read(&catalog).unwrap();
        let result = select_cli(startup(path.clone(), Some("campaign-X".into()))).await;
        match result {
            okx_agent::finance_startup::FinanceSelection::Failed(failure) => {
                assert_eq!(failure.code(), "FINANCE_STORAGE_CORRUPT");
                assert!(matches!(failure.cause(),
                    okx_agent::finance_startup::FinanceStartupCause::Owner(
                        AgentError::Store(okx_execution::StoreError::Sql(
                            rusqlite::Error::SqliteFailure(error, _)
                        ))
                    ) if error.code == rusqlite::ErrorCode::NotADatabase));
            }
            _ => panic!("corrupt stopped import must not open an owner"),
        }
        assert_eq!(std::fs::read(catalog).unwrap(), before_catalog);
        assert_eq!(
            std::fs::read(&finance).unwrap(),
            b"not-a-valid-financial-import"
        );
        let legacy_account_sql = path
            .join("accounts")
            .join(format!(
                "{:x}",
                sha2::Sha256::digest(account.account_key.wire().as_bytes())
            ))
            .join("account.sqlite");
        assert!(!legacy_account_sql.exists());
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod venue_inspection_cli_contract_tests {
    use super::*;

    #[test]
    fn explicit_readonly_venue_command_is_parseable() {
        let cli = Cli::try_parse_from([
            "okx-agent",
            "--root",
            "fixture-runtime",
            "inspect-venue",
            "--account",
            "real_acceptance",
        ])
        .unwrap();
        assert!(matches!(
            cli.command,
            Command::InspectVenue { account } if account == "real_acceptance"
        ));
    }
}
