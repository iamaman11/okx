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
    native_finance_risk::NativeFinanceRisk,
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
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            // Runtime MODE SELECTION occurs before any account SQL resource
            // is opened. A finance.sqlite can only open via the single
            // native owner; a legacy Cloudflare finance fallback is never
            // constructed for the same installation.
            let finance_present =
                match std::fs::symlink_metadata(config.root.join("finance").join("finance.sqlite"))
                {
                    Ok(_) => true,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
                    Err(_) => {
                        return Err(AgentError::FinancialCaptureTask(
                            "FINANCE_AUTHORITY_STATUS_UNAVAILABLE".into(),
                        ));
                    }
                };
            let (accounts, native_owner) = if finance_present {
                let campaign = stopped_finance_campaign
                    .as_deref()
                    .or(native_finance_campaign.as_deref())
                    .unwrap_or("");
                if stopped_finance_mode && campaign.is_empty() {
                    return Err(AgentError::FinancialCaptureTask(
                        "FINANCE_STOPPED_CAMPAIGN_REQUIRED".into(),
                    ));
                }
                let (registry, owner) = if stopped_finance_mode {
                    AccountRuntimeRegistry::open_stopped_finance(
                        &config.root,
                        budget.clone(),
                        campaign,
                    )
                    .await?
                } else {
                    AccountRuntimeRegistry::open_native_finance(
                        &config.root,
                        budget.clone(),
                        campaign,
                    )
                    .await?
                };
                (Arc::new(registry), owner)
            } else {
                // Absence of the canonical SQLite is a stopped installation,
                // never permission to construct a remote financial owner.
                return Err(AgentError::FinancialCaptureTask(if stopped_finance_mode {
                    "FINANCE_STOPPED_IMPORT_REQUIRED".into()
                } else {
                    "FINANCE_INSTALLATION_NOT_INITIALIZED".into()
                }));
            };
            let public_client = OkxPublicClient::with_rate_budget(env, budget.clone())?;
            let reference = bootstrap_reference(public_client.clone()).await?;
            let (public, public_ws) =
                PublicWsCoordinator::new_with_rate_budget(env, reference, budget);
            let market = MarketBootstrapper::new(public_client);
            let analytics = AnalyticsService::new(config.root.join("analytics"), market.clone())
                .map_err(AgentError::ResearchSessionTask)?;
            // Financial authority is the single local SQL owner. Cloudflare
            // transports authenticated product commands, never risk grants.
            let campaign = stopped_finance_campaign.or(native_finance_campaign);
            let backend = match campaign {
                Some(campaign) => NativeFinanceRisk::new(
                    native_owner,
                    accounts.installation_id().to_owned(),
                    campaign,
                )
                .map_err(|reason| AgentError::FinancialCaptureTask(reason.into()))?,
                None => NativeFinanceRisk::from_owner(native_owner),
            };
            let risk_client = if stopped_finance_mode {
                NativeRiskClient::local_stopped(backend)
            } else {
                NativeRiskClient::local(backend)
            };
            let trade = Arc::new(TradeService::new(
                accounts.clone(),
                public_ws.clone(),
                risk_client,
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

    #[tokio::test]
    async fn finance_file_without_explicit_campaign_cannot_start_legacy_run() {
        let path = root();
        let finance = path.join("finance").join("finance.sqlite");
        std::fs::create_dir_all(finance.parent().unwrap()).unwrap();
        std::fs::write(&finance, b"sealed-finance-sentinel").unwrap();
        let result = run(startup(path.clone(), None)).await;
        assert!(
            result.is_err(),
            "unknown financial file must never start or initialize"
        );
        assert_eq!(std::fs::read(&finance).unwrap(), b"sealed-finance-sentinel");
        assert!(!path.join("accounts").exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn default_startup_without_finance_never_creates_legacy_or_remote_owner() {
        let path = root();
        assert!(matches!(run(startup(path.clone(), None)).await,
            Err(AgentError::FinancialCaptureTask(ref reason))
                if reason == "FINANCE_INSTALLATION_NOT_INITIALIZED"));
        assert!(!path.join("accounts").exists());
        assert!(!path.join("finance").exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn explicit_native_mode_without_finance_file_never_falls_back_to_cloudflare() {
        let path = root();
        let result = run(startup(path.clone(), Some("campaign-X".into()))).await;
        assert!(matches!(
            result,
            Err(AgentError::FinancialCaptureTask(ref reason))
                if reason == "FINANCE_STOPPED_IMPORT_REQUIRED"
        ));
        assert!(!path.join("accounts").exists());
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn live_native_selection_cannot_initialize_or_use_legacy_without_finance() {
        let path = root();
        let mut cli = startup(path.clone(), None);
        cli.native_finance_campaign = Some("campaign-X".into());
        assert!(matches!(run(cli).await,
            Err(AgentError::FinancialCaptureTask(ref reason)) if reason=="FINANCE_INSTALLATION_NOT_INITIALIZED"));
        assert!(!path.join("accounts").exists());
        assert!(!path.join("finance").exists());
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
        let result = run(startup(path.clone(), Some("campaign-X".into()))).await;
        assert!(result.is_err());
        assert!(!matches!(
            result,
            Err(AgentError::FinancialCaptureTask(ref reason))
                if reason == "FINANCE_CUTOVER_NOT_ACCEPTED"
        ));
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
