//! Engine owner-boundary tests: actual Native Finance, no Research child or IPC.
use super::*;
use crate::accounts::{initialize_installation, recorded_support};
use crate::trade_service::TradeService;
use okx_api::{HttpSend, HttpTransport, OkxEnvironment, RateBudget};
use okx_ipc_contracts::{AGENT_REQUEST_SCHEMA_V1, InstrumentTypeFilter};
use std::sync::Arc;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

#[derive(Default)]
struct ForbiddenNetwork(AtomicUsize);
impl HttpTransport for ForbiddenNetwork {
    fn send(&self, _: okx_api::HttpRequest, _: Instant) -> HttpSend<'_> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            Err(okx_api::OkxError::Response(
                "PUBLIC_TEST_NETWORK_FORBIDDEN".into(),
            ))
        })
    }
}

struct Fixture {
    root: PathBuf,
    context: DispatchContext,
    _market: crate::market_bootstrap::MarketBootstrapper,
    // Keep the actual public websocket command receiver alive but offline.
    _coordinator: okx_market_runtime::PublicWsCoordinator,
    network: Arc<ForbiddenNetwork>,
    finance_owner: okx_trade_execution::account_worker::UnifiedFinanceHandle,
}
impl Fixture {
    async fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "okx-query-dispatch-{}-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        initialize_installation(&root, "query-dispatch-installation").unwrap();
        let initial_owner =
            okx_trade_execution::account_worker::UnifiedFinanceHandle::initialize_new(
                root.clone(),
                okx_trade_execution::NewFinanceInstallation {
                    installation_id: "query-dispatch-installation".into(),
                    initialize_request_id: "initialize_readonly_dispatch_001".into(),
                    source_tree: "a".repeat(40),
                },
            )
            .await
            .unwrap();
        let mut initial_closed = initial_owner.termination_signal();
        drop(initial_owner);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            initial_closed.wait_for(|v| *v),
        )
        .await
        .unwrap()
        .unwrap();
        let (accounts, finance_owner) =
            crate::accounts::tests::open_native_test_registry(&root, RateBudget::new()).await;
        let accounts = Arc::new(accounts);
        let environment = OkxEnvironment::new(okx_api::Region::Global, false);
        let network = Arc::new(ForbiddenNetwork::default());
        let market = crate::market_bootstrap::MarketBootstrapper::new(
            okx_api::OkxPublicClient::with_http_transport(
                environment,
                RateBudget::new(),
                network.clone(),
            )
            .unwrap(),
        );
        let (coordinator, public_ws) = okx_market_runtime::PublicWsCoordinator::new_with_connector(
            environment,
            okx_observation::ReferenceRegistry::from_public("fixture", vec![]).unwrap(),
            RateBudget::new(),
            Arc::new(recorded_support::OfflineSocket),
        );
        let trade = Arc::new(TradeService::new(
            accounts.clone(),
            public_ws,
            crate::risk_bridge::NativeRiskClient::local(
                crate::native_finance_risk::NativeFinanceRisk::from_owner(finance_owner.clone()),
            ),
        ));
        Self {
            root,
            context: DispatchContext {
                finance: crate::finance_dispatch::FinanceDispatch::ready(accounts, trade),
            },
            _market: market,
            _coordinator: coordinator,
            network,
            finance_owner,
        }
    }

    async fn request(&self, operation: AgentOperation, account: Option<&str>) -> AgentResponse {
        let jobs_before = self.finance_owner.sql_timing().observed_jobs;
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: "query-dispatch-request-001".into(),
            account: account.map(str::to_owned),
            operation,
        };
        let response = dispatch(&self.context, &request).await;
        assert_eq!(
            self.finance_owner.sql_timing().observed_jobs,
            jobs_before,
            "Engine read-only public dispatch cannot enqueue a financial write"
        );
        response
    }

    async fn finish(self) {
        assert_eq!(
            self.network.0.load(Ordering::Relaxed),
            0,
            "Engine public handoff must not fetch from a venue"
        );
        assert!(
            self.finance_owner
                .configured_campaign(Instant::now() + std::time::Duration::from_secs(3))
                .await
                .unwrap()
                .is_none(),
            "Research handoff must not install a financial policy"
        );
        self.context.finance.accounts().unwrap().shutdown().await;
        let root = self.root.clone();
        let mut closed = self.finance_owner.termination_signal();
        drop(self);
        tokio::time::timeout(std::time::Duration::from_secs(5), closed.wait_for(|v| *v))
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn rejection(response: AgentResponse, code: &str) {
    assert_eq!(response.status, AgentResponseStatus::Rejected);
    assert_eq!(response.quality, DataQuality::NotReady);
    assert!(response.result.is_none());
    assert_eq!(response.failure.unwrap().code, code);
}

#[tokio::test]
async fn trading_and_strategy_requests_reach_finance_owner_not_research_fallback() {
    use okx_ipc_contracts::trading::{StrategyRequest, TradeRequest};

    // No account, network, Research process or mutation. Distinct finance
    // failure proves that a normal financial request cannot hit Research.
    let ctx = DispatchContext {
        finance: crate::finance_dispatch::FinanceDispatch::missing(),
    };
    for operation in [
        AgentOperation::Trade {
            request: TradeRequest::Capabilities {},
        },
        AgentOperation::Strategies {
            request: StrategyRequest::List {},
        },
    ] {
        let request = AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: "engine_route_guard_001".into(),
            account: Some("test-account".into()),
            operation,
        };
        rejection(
            dispatch(&ctx, &request).await,
            "FINANCE_INSTALLATION_NOT_INITIALIZED",
        );
    }
}

// These are the Engine's *shipping* Research-owner handoff operations. The
// retired Engine-only child route and public Market shim were never shipping
// routes; Research now tests their positive semantics in its owner crate.
#[tokio::test]
async fn engine_rejects_every_research_operation_without_network_or_finance_work() {
    use okx_ipc_contracts::trading::{DiagnosticsRequest, DiagnosticsView};
    let fixture = Fixture::new().await;
    let operations = [
        AgentOperation::MarketCapabilities,
        AgentOperation::MarketMicrostructure {
            instrument: "BTC-USDT-SWAP".into(),
            size_contracts: Some("1".into()),
        },
        AgentOperation::FindInstruments {
            asset: "BTC".into(),
            settle_currency: Some("USDT".into()),
            instrument_type: Some(InstrumentTypeFilter::Swap),
        },
        AgentOperation::MarketOverview {
            instrument: "BTC-USDT-SWAP".into(),
        },
        AgentOperation::Diagnostics {
            request: DiagnosticsRequest {
                view: Some(DiagnosticsView::Research),
                reference: Some("deployment".into()),
                account: None,
                cycle_id: None,
                limit: None,
            },
        },
        AgentOperation::Diagnostics {
            request: DiagnosticsRequest {
                view: Some(DiagnosticsView::Research),
                reference: Some("a".repeat(64)),
                account: None,
                cycle_id: None,
                limit: None,
            },
        },
    ];
    for operation in operations {
        let response = fixture.request(operation.clone(), None).await;
        assert!(!response.failure.as_ref().unwrap().retryable);
        rejection(response, "RESEARCH_OWNER_REQUIRED");
        for finance in [
            crate::finance_dispatch::FinanceDispatch::missing(),
            crate::finance_dispatch::FinanceDispatch::invalid(),
        ] {
            let request = AgentRequest {
                schema: AGENT_REQUEST_SCHEMA_V1.into(),
                request_id: "query-dispatch-offline-001".into(),
                account: None,
                operation: operation.clone(),
            };
            rejection(
                dispatch(&DispatchContext { finance }, &request).await,
                "RESEARCH_OWNER_REQUIRED",
            );
        }
    }
    assert!(!fixture.root.join("research").exists());
    assert!(!fixture.root.join("analytics").exists());
    fixture.finish().await;
}

// An independent Engine-owned finance witness. Unlike the retired Research
// child tests, this compares actual Native Finance files under a live writer.
#[tokio::test]
async fn research_owner_handoff_cannot_change_native_finance_files_or_enqueue_writes() {
    use okx_ipc_contracts::trading::{DiagnosticsRequest, DiagnosticsView};

    let fixture = Fixture::new().await;
    let files_before = crate::accounts::tests::read_only_fixture_files(&fixture.root);
    let jobs_before = fixture.finance_owner.sql_timing().observed_jobs;
    for operation in [
        AgentOperation::MarketCapabilities,
        AgentOperation::MarketMicrostructure {
            instrument: "BTC-USDT-SWAP".into(),
            size_contracts: Some("1".into()),
        },
        AgentOperation::FindInstruments {
            asset: "BTC".into(),
            settle_currency: Some("USDT".into()),
            instrument_type: Some(InstrumentTypeFilter::Swap),
        },
        AgentOperation::MarketOverview {
            instrument: "BTC-USDT-SWAP".into(),
        },
        AgentOperation::Diagnostics {
            request: DiagnosticsRequest {
                view: Some(DiagnosticsView::Research),
                reference: Some("deployment".into()),
                account: None,
                cycle_id: None,
                limit: None,
            },
        },
        AgentOperation::Diagnostics {
            request: DiagnosticsRequest {
                view: Some(DiagnosticsView::Research),
                reference: Some("a".repeat(64)),
                account: None,
                cycle_id: None,
                limit: None,
            },
        },
    ] {
        rejection(
            fixture.request(operation, None).await,
            "RESEARCH_OWNER_REQUIRED",
        );
        assert_eq!(
            fixture.finance_owner.sql_timing().observed_jobs,
            jobs_before,
            "Research handoff must not enqueue a Native financial operation"
        );
        assert_eq!(
            crate::accounts::tests::read_only_fixture_files(&fixture.root),
            files_before,
            "Research handoff must preserve original durable finance files"
        );
    }
    assert!(!fixture.root.join("research").exists());
    fixture.finish().await;
}

#[tokio::test]
async fn malformed_finance_catalog_remains_unchanged_during_research_handoff_denial() {
    use okx_ipc_contracts::trading::{DiagnosticsRequest, DiagnosticsView, TradeRequest};

    // Unlike a FinanceDispatch::invalid() synthetic state, this creates the
    // real installation/catalog and a corrupt original finance.sqlite, then
    // exercises the genuine startup path without repairing its financial data.
    let root = std::env::temp_dir().join(format!(
        "okx-research-invalid-finance-boundary-{}-{}",
        std::process::id(),
        Timestamp::now().as_nanosecond()
    ));
    initialize_installation(&root, "research-denial-invalid-finance").unwrap();
    let finance_file = root.join("finance/finance.sqlite");
    std::fs::create_dir(root.join("finance")).unwrap();
    std::fs::write(
        &finance_file,
        b"invalid financial input must stay unchanged",
    )
    .unwrap();
    let catalog = root.join("account-catalog.pb");
    let original_finance = std::fs::read(&finance_file).unwrap();
    let original_catalog = std::fs::read(&catalog).unwrap();

    let environment = OkxEnvironment::new(okx_api::Region::Global, false);
    let forbidden = Arc::new(ForbiddenNetwork::default());
    let client = okx_api::OkxPublicClient::with_http_transport(
        environment,
        RateBudget::new(),
        forbidden.clone(),
    )
    .unwrap();
    let market = crate::market_bootstrap::MarketBootstrapper::new(client.clone());
    let (_coordinator, public_ws) = okx_market_runtime::PublicWsCoordinator::new_uninitialized(
        environment,
        RateBudget::new(),
        client,
    );
    let finance = crate::finance_startup::select(&root, RateBudget::new(), false, None)
        .await
        .finish(public_ws, market)
        .await;
    let context = DispatchContext { finance };
    let request = |operation, account: Option<&str>| AgentRequest {
        schema: AGENT_REQUEST_SCHEMA_V1.into(),
        request_id: "research_invalid_finance_001".into(),
        account: account.map(str::to_owned),
        operation,
    };
    for operation in [
        AgentOperation::MarketCapabilities,
        AgentOperation::FindInstruments {
            asset: "BTC".into(),
            settle_currency: Some("USDT".into()),
            instrument_type: Some(InstrumentTypeFilter::Swap),
        },
        AgentOperation::MarketOverview {
            instrument: "BTC-USDT-SWAP".into(),
        },
        AgentOperation::Diagnostics {
            request: DiagnosticsRequest {
                view: Some(DiagnosticsView::Research),
                reference: Some("deployment".into()),
                account: None,
                cycle_id: None,
                limit: None,
            },
        },
    ] {
        rejection(
            dispatch(&context, &request(operation, None)).await,
            "RESEARCH_OWNER_REQUIRED",
        );
    }
    rejection(
        dispatch(
            &context,
            &request(
                AgentOperation::Trade {
                    request: TradeRequest::Status {
                        reference: "missing-intent".into(),
                    },
                },
                Some("test-account"),
            ),
        )
        .await,
        "FINANCE_FILE_FORMAT_UNKNOWN",
    );
    assert_eq!(forbidden.0.load(Ordering::Relaxed), 0);
    assert_eq!(std::fs::read(&finance_file).unwrap(), original_finance);
    assert_eq!(std::fs::read(&catalog).unwrap(), original_catalog);
    assert!(!root.join("research").exists());
    drop(context);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn capabilities_and_mailbox_use_public_dispatch_without_external_effects() {
    let fixture = Fixture::new().await;
    // Research now owns positive MarketCapabilities. The shipping Engine may
    // only refuse that action while its own mailbox remains available.
    rejection(
        fixture
            .request(AgentOperation::MarketCapabilities, None)
            .await,
        "RESEARCH_OWNER_REQUIRED",
    );
    let mailbox = fixture
        .request(AgentOperation::MailboxTelemetry, None)
        .await;
    assert_eq!(mailbox.status, AgentResponseStatus::Completed);
    assert_eq!(
        mailbox.result.unwrap(),
        json!({"contract":"okx.product/v1","installation_id":"query-dispatch-installation"})
    );
    fixture.finish().await;
}

#[tokio::test]
async fn account_summary_checks_account_and_source_at_public_boundary() {
    let fixture = Fixture::new().await;
    rejection(
        fixture.request(AgentOperation::AccountSummary, None).await,
        "EXPLICIT_ACCOUNT_REQUIRED",
    );
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some("unknown-account"))
            .await,
        "ACCOUNT_UNAVAILABLE",
    );
    let feed = Arc::new(recorded_support::RecordedAccountHttp::default());
    let (config, clients) = recorded_support::prepared("a", feed, RateBudget::new());
    fixture
        .context
        .finance
        .accounts()
        .unwrap()
        .connect_prepared(config.clone(), clients)
        .await
        .unwrap();
    // Freeze the source producer before varying its published input. This
    // tests the public consumer, not an exchange observer's freshness proof.
    fixture.context.finance.accounts().unwrap().shutdown().await;
    let owner = fixture
        .context
        .finance
        .accounts()
        .unwrap()
        .resolve(&config.account_key.wire())
        .await
        .unwrap();
    let mut terminated = owner.execution.store.termination_signal();
    let original = owner.observer.snapshot().await.unwrap();
    *owner.latest_snapshot.write().await = None;
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some(&config.alias))
            .await,
        "WAITING_ACCOUNT_EVIDENCE",
    );
    let mut snapshot = original.clone();
    snapshot.source_received_at = "2020-01-01T00:00:00Z".into();
    *owner.latest_snapshot.write().await = Some(snapshot.clone());
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some(&config.alias))
            .await,
        "WAITING_ACCOUNT_EVIDENCE",
    );
    snapshot.source_received_at = "invalid-time".into();
    *owner.latest_snapshot.write().await = Some(snapshot.clone());
    rejection(
        fixture
            .request(AgentOperation::AccountSummary, Some(&config.alias))
            .await,
        "ACCOUNT_SOURCE_TIME_INVALID",
    );
    snapshot.source_received_at = Timestamp::now().to_string();
    *owner.latest_snapshot.write().await = Some(snapshot.clone());
    let response = fixture
        .request(AgentOperation::AccountSummary, Some(&config.alias))
        .await;
    assert_eq!(response.status, AgentResponseStatus::Completed);
    let result = response.result.unwrap();
    assert_eq!(result["account"], config.account_key.wire());
    assert_eq!(result["source_received_at"], snapshot.source_received_at);
    assert_eq!(
        result["total_equity_usd"],
        json!(original.balance.total_equity_usd)
    );
    assert_eq!(fixture.network.0.load(Ordering::Relaxed), 0);
    assert!(
        fixture
            .finance_owner
            .configured_campaign(Instant::now() + std::time::Duration::from_secs(3))
            .await
            .unwrap()
            .is_none()
    );
    drop(owner);
    fixture.context.finance.accounts().unwrap().shutdown().await;
    let root = fixture.root.clone();
    drop(fixture);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !*terminated.borrow_and_update() {
            terminated.changed().await.unwrap();
        }
    })
    .await
    .expect("actual SQL owner must terminate before deleting its files");
    std::fs::remove_dir_all(root).unwrap();
}
