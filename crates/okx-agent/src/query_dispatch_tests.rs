//! Public routing tests: real owners, with delivery replaced only at HTTP/WS.
use super::*;
use crate::accounts::{initialize_installation, recorded_support};
use crate::research_test_support::client as research_test_client;
use crate::trade_service::TradeService;
use okx_api::{HttpSend, HttpTransport, OkxEnvironment, RateBudget};
use okx_protocol::{AGENT_REQUEST_SCHEMA_V1, InstrumentTypeFilter};
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
    // Keep the actual command receiver alive without starting a network task.
    _coordinator: okx_runtime::PublicWsCoordinator,
    network: Arc<ForbiddenNetwork>,
    finance_owner: okx_execution::account_worker::UnifiedFinanceHandle,
}
impl Fixture {
    async fn new(reference: okx_observation::ReferenceRegistry) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "okx-query-dispatch-{}-{}-{}",
            std::process::id(),
            Timestamp::now().as_nanosecond(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        initialize_installation(&root, "query-dispatch-installation").unwrap();
        let initial_owner = okx_execution::account_worker::UnifiedFinanceHandle::initialize_new(
            root.clone(),
            okx_execution::NewFinanceInstallation {
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
        let analytics = research_test_client(root.join("research"), market.clone()).unwrap();
        let (coordinator, public_ws) = okx_runtime::PublicWsCoordinator::new_with_connector(
            environment,
            reference,
            RateBudget::new(),
            Arc::new(recorded_support::OfflineSocket),
        );
        let trade = Arc::new(TradeService::new(
            accounts.clone(),
            public_ws.clone(),
            crate::risk_bridge::NativeRiskClient::local(
                crate::native_finance_risk::NativeFinanceRisk::from_owner(finance_owner.clone()),
            ),
        ));
        Self {
            root,
            context: DispatchContext {
                research: analytics,
                public_ws,
                finance: crate::finance_dispatch::FinanceDispatch::ready(accounts, trade),
                market,
            },
            _coordinator: coordinator,
            network,
            finance_owner,
        }
    }
    async fn request(&self, operation: AgentOperation, account: Option<&str>) -> AgentResponse {
        let finance_jobs_before = self.finance_owner.sql_timing().observed_jobs;
        let response = dispatch(
            &self.context,
            &AgentRequest {
                schema: AGENT_REQUEST_SCHEMA_V1.into(),
                request_id: "query-dispatch-request-001".into(),
                account: account.map(str::to_owned),
                operation,
            },
        )
        .await;
        assert_eq!(
            self.finance_owner.sql_timing().observed_jobs,
            finance_jobs_before,
            "read-only public dispatch cannot enqueue financial work"
        );
        response
    }
    async fn finish(self) {
        self.context.research.shutdown().await;
        assert_eq!(
            self.network.0.load(Ordering::Relaxed),
            0,
            "read-only fixture must not fetch external data"
        );
        assert!(
            self.finance_owner
                .configured_campaign(Instant::now() + std::time::Duration::from_secs(3),)
                .await
                .unwrap()
                .is_none(),
            "read-only dispatch cannot install financial policy"
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
fn empty_reference() -> okx_observation::ReferenceRegistry {
    okx_observation::ReferenceRegistry::from_public("fixture", vec![]).unwrap()
}
fn rejection(response: AgentResponse, code: &str) {
    assert_eq!(response.status, AgentResponseStatus::Rejected);
    assert_eq!(response.quality, DataQuality::NotReady);
    assert!(response.result.is_none());
    assert_eq!(response.failure.unwrap().code, code);
}

#[tokio::test]
async fn research_without_finance_uses_real_worker_and_cannot_create_financial_authority() {
    use okx_protocol::{
        analytics::ResearchRequest,
        trading::{DiagnosticsRequest, DiagnosticsView, TradeRequest},
    };
    for invalid in [false, true] {
        let root = std::env::temp_dir().join(format!(
            "okx-research-finance-boundary-{}-{}-{invalid}",
            std::process::id(),
            Timestamp::now().as_nanosecond()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let finance_file = root.join("finance/finance.sqlite");
        if invalid {
            initialize_installation(&root, "research-boundary-installation").unwrap();
            std::fs::create_dir(root.join("finance")).unwrap();
            std::fs::write(
                &finance_file,
                b"invalid financial input must stay unchanged",
            )
            .unwrap();
        }
        let before = invalid.then(|| std::fs::read(&finance_file).unwrap());
        let catalog = root.join("account-catalog.pb");
        let catalog_before = invalid.then(|| std::fs::read(&catalog).unwrap());
        let env = OkxEnvironment::new(okx_api::Region::Global, false);
        let network = Arc::new(ForbiddenNetwork::default());
        let client =
            okx_api::OkxPublicClient::with_http_transport(env, RateBudget::new(), network.clone())
                .unwrap();
        let market = crate::market_bootstrap::MarketBootstrapper::new(client.clone());
        let (_coordinator, public_ws) =
            okx_runtime::PublicWsCoordinator::new_uninitialized(env, RateBudget::new(), client);
        let finance = crate::finance_startup::select(&root, RateBudget::new(), false, None)
            .await
            .finish(public_ws.clone(), market.clone())
            .await;
        let context = DispatchContext {
            finance,
            research: research_test_client(root.join("research"), market.clone()).unwrap(),
            public_ws,
            market,
        };
        let request = |operation, account: Option<&str>| AgentRequest {
            schema: AGENT_REQUEST_SCHEMA_V1.into(),
            request_id: "research-only-host-001".into(),
            account: account.map(str::to_owned),
            operation,
        };
        let response = dispatch(
            &context,
            &request(
                AgentOperation::Research {
                    request: ResearchRequest::Capabilities {},
                },
                None,
            ),
        )
        .await;
        assert_eq!(
            response.status,
            AgentResponseStatus::Completed,
            "{response:?}"
        );
        let denied = dispatch(
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
        .await;
        rejection(
            denied,
            if invalid {
                "FINANCE_FILE_FORMAT_UNKNOWN"
            } else {
                "FINANCE_INSTALLATION_NOT_INITIALIZED"
            },
        );
        let diagnostic = DiagnosticsRequest {
            view: Some(DiagnosticsView::Research),
            reference: Some("a".repeat(64)),
            account: None,
            cycle_id: None,
            limit: None,
        };
        let response = dispatch(
            &context,
            &request(
                AgentOperation::Diagnostics {
                    request: diagnostic.clone(),
                },
                None,
            ),
        )
        .await;
        assert_eq!(response.status, AgentResponseStatus::Completed);
        assert_eq!(
            response.result.unwrap()["reason_code"],
            "UNKNOWN_NOT_RETAINED"
        );
        let denied = dispatch(
            &context,
            &request(
                AgentOperation::Diagnostics {
                    request: diagnostic,
                },
                Some("test-account"),
            ),
        )
        .await;
        rejection(denied, "RESEARCH_DIAGNOSTICS_ACCOUNT_NOT_APPLICABLE");
        assert_eq!(network.0.load(Ordering::Relaxed), 0);
        context.research.shutdown().await;
        if let Some(bytes) = before {
            assert_eq!(std::fs::read(&finance_file).unwrap(), bytes);
            assert_eq!(std::fs::read(&catalog).unwrap(), catalog_before.unwrap());
        } else {
            assert!(!root.join("finance").exists());
            assert!(!catalog.exists());
        }
        assert!(!root.join("installation.json").exists());
        drop(context);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn capabilities_and_mailbox_use_public_dispatch_without_external_effects() {
    let fixture = Fixture::new(empty_reference()).await;
    let capabilities = fixture
        .request(AgentOperation::MarketCapabilities, None)
        .await;
    assert_eq!(capabilities.request_id, "query-dispatch-request-001");
    assert_eq!(capabilities.status, AgentResponseStatus::Completed);
    assert_eq!(capabilities.quality, DataQuality::Fresh);
    let data = capabilities.result.unwrap();
    assert_eq!(data["schema"], "okx.market.capabilities/v1");
    assert_eq!(data["limits"]["max_page_rows"], 50);
    assert!(
        data["operators"]
            .as_array()
            .unwrap()
            .iter()
            .any(|op| op["name"] == "returns" && op["unit"] == "ratio")
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
async fn research_public_dispatch_progress_decision_and_locked_catalog_have_zero_financial_effects()
{
    use crate::research_client::{ResearchClient, ResearchClientConfig};
    use okx_protocol::{
        analytics::{ResearchDecisionStatus, ResearchRequest},
        research_ipc::ResearchScope,
        trading::{DiagnosticsRequest, DiagnosticsView},
    };
    let mut fixture = Fixture::new(empty_reference()).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let executable = std::env::var_os("OKX_RESEARCH_SERVICE_TEST_EXE")
        .map(PathBuf::from)
        .expect("real worker executable must be built before process contracts");
    use sha2::{Digest, Sha256};
    let expected_worker_sha256 =
        format!("{:x}", Sha256::digest(std::fs::read(&executable).unwrap()));
    let research_root = fixture.root.join("public-research");
    fixture.context.research = ResearchClient::new(
        ResearchClientConfig {
            executable,
            root: research_root.clone(),
            scope: ResearchScope::new("query-dispatch-installation".into()).unwrap(),
        },
        crate::research_dispatch_tests::candle_source(calls.clone()),
    );
    let mut request = crate::research_dispatch_tests::grid("public-dispatch-progress");
    // A decision is scoped to one experiment, so use one instrument at this boundary.
    if let ResearchRequest::Grid { instruments, .. } = &mut request {
        instruments.truncate(1);
    }
    let queued = fixture
        .request(
            AgentOperation::Research {
                request: request.clone(),
            },
            None,
        )
        .await;
    assert_eq!(queued.status, AgentResponseStatus::Completed, "{queued:?}");
    let id = queued.result.unwrap()["id"].as_str().unwrap().to_owned();
    let complete = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let response = fixture
                .request(
                    AgentOperation::Research {
                        request: ResearchRequest::Status { job_id: id.clone() },
                    },
                    None,
                )
                .await;
            assert_eq!(
                response.status,
                AgentResponseStatus::Completed,
                "{response:?}"
            );
            let job = response.result.unwrap();
            match job["state"].as_str() {
                Some("completed") => break job,
                Some("failed" | "cancelled" | "interrupted") => {
                    panic!("research did not complete: {job}")
                }
                _ => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("bounded public research completion");
    assert_eq!(complete["origin_invocation"], "query-dispatch-request-001");
    assert_eq!(
        complete["producer"]["binary_sha256"],
        expected_worker_sha256
    );
    assert_eq!(complete["candidate_progress"]["completed_candidates"], 64);
    assert_eq!(complete["candidate_progress"]["total_work_units"], 193);
    let result = fixture
        .request(
            AgentOperation::Research {
                request: ResearchRequest::Result {
                    job_id: id.clone(),
                    cursor: None,
                    limit: 1,
                    view: okx_protocol::analytics::ResearchResultView::Summary,
                },
            },
            None,
        )
        .await;
    assert_eq!(result.status, AgentResponseStatus::Completed, "{result:?}");
    let result = result.result.unwrap();
    assert_eq!(result["metadata"]["job_id"], id);
    assert_eq!(
        result["metadata"]["producer"]["binary_sha256"],
        expected_worker_sha256
    );
    let result_id = complete["artifact"].as_str().unwrap().to_owned();
    let record = ResearchRequest::RecordDecision {
        job_id: id.clone(),
        result_id,
        expected_decision_id: None,
        name: "Independent hypothesis".into(),
        hypothesis: "Test a parameter grid".into(),
        status: ResearchDecisionStatus::Deferred,
        reason: "Await review".into(),
    };
    let recorded = fixture
        .request(
            AgentOperation::Research {
                request: record.clone(),
            },
            None,
        )
        .await;
    assert_eq!(
        recorded.status,
        AgentResponseStatus::Completed,
        "{recorded:?}"
    );
    let decision = recorded.result.unwrap();
    assert_eq!(decision["trading_authority"], false);
    assert_eq!(
        fixture
            .request(AgentOperation::Research { request: record }, None)
            .await
            .result
            .unwrap(),
        decision
    );
    let calls_before = calls.load(Ordering::Relaxed);
    let catalog =
        rusqlite::Connection::open(research_root.join("research-catalog.sqlite")).unwrap();
    catalog.execute_batch("BEGIN IMMEDIATE").unwrap();
    let changes_before = catalog.total_changes();
    let request = DiagnosticsRequest {
        reference: Some(id.clone()),
        account: None,
        view: Some(DiagnosticsView::Research),
        cycle_id: None,
        limit: None,
    };
    let observed = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        fixture.request(
            AgentOperation::Diagnostics {
                request: request.clone(),
            },
            None,
        ),
    )
    .await
    .expect("read-only diagnostic waited for catalog writer");
    assert_eq!(
        observed.status,
        AgentResponseStatus::Completed,
        "{observed:?}"
    );
    assert_eq!(observed.result.unwrap()["job"]["state"], "completed");
    assert_eq!(catalog.total_changes(), changes_before);
    let refused = fixture
        .request(
            AgentOperation::Diagnostics { request },
            Some("another-account"),
        )
        .await;
    rejection(refused, "RESEARCH_DIAGNOSTICS_ACCOUNT_NOT_APPLICABLE");
    catalog.execute_batch("ROLLBACK").unwrap();
    drop(catalog);
    assert_eq!(calls.load(Ordering::Relaxed), calls_before);
    fixture.context.research.shutdown().await;
    let mut replay_request = crate::research_dispatch_tests::grid("public-dispatch-progress");
    if let ResearchRequest::Grid { instruments, .. } = &mut replay_request {
        instruments.truncate(1);
    }
    let replay = fixture
        .request(
            AgentOperation::Research {
                request: replay_request,
            },
            None,
        )
        .await;
    assert_eq!(replay.result.unwrap()["id"], id);
    fixture.finish().await;
}

#[tokio::test]
async fn instrument_search_routes_type_filter_before_public_limit() {
    let instrument = |id: String, kind: &str| {
        serde_json::from_value(json!({
            "instId":id,"instType":kind,"state":"live","baseCcy":"BTC","ctValCcy":"BTC",
            "settleCcy":"USDT","tickSz":"0.1","lotSz":"1","minSz":"1"
        }))
        .unwrap()
    };
    let mut instruments = (0..60)
        .map(|i| instrument(format!("A-SWAP-{i:02}"), "SWAP"))
        .collect::<Vec<_>>();
    instruments.extend((0..55).map(|i| instrument(format!("Z-FUTURE-{i:02}"), "FUTURES")));
    let fixture = Fixture::new(
        okx_observation::ReferenceRegistry::from_public("fixture", instruments).unwrap(),
    )
    .await;
    let response = fixture
        .request(
            AgentOperation::FindInstruments {
                asset: "BTC".into(),
                settle_currency: Some("USDT".into()),
                instrument_type: Some(InstrumentTypeFilter::Futures),
            },
            None,
        )
        .await;
    assert_eq!(response.status, AgentResponseStatus::Completed);
    let data = response.result.unwrap();
    let rows = data["instruments"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    assert!(rows.iter().all(|row| {
        row["instrument_id"]
            .as_str()
            .unwrap()
            .starts_with("Z-FUTURE-")
    }));
    rejection(
        fixture
            .request(
                AgentOperation::FindInstruments {
                    asset: "".into(),
                    settle_currency: None,
                    instrument_type: None,
                },
                None,
            )
            .await,
        "INVALID_COMMAND",
    );
    fixture.finish().await;
}

#[tokio::test]
async fn account_summary_checks_account_and_source_at_public_boundary() {
    let fixture = Fixture::new(empty_reference()).await;
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
    // Freeze the source producer before varying its published input. This tests
    // the public consumer, not the exchange observer's separate freshness proof.
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
            .configured_campaign(Instant::now() + std::time::Duration::from_secs(3),)
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

#[tokio::test]
async fn overview_without_reference_rejects_unavailable_source_instead_of_empty_success() {
    let fixture = Fixture::new(empty_reference()).await;
    rejection(
        fixture
            .request(
                AgentOperation::MarketOverview {
                    instrument: "BTC-USDT-SWAP".into(),
                },
                None,
            )
            .await,
        "MARKET_SOURCE_UNAVAILABLE",
    );
    rejection(
        fixture
            .request(
                AgentOperation::MarketMicrostructure {
                    instrument: "BTC-USDT-SWAP".into(),
                    size_contracts: None,
                },
                None,
            )
            .await,
        "MICROSTRUCTURE_FRESH_BOOK_REQUIRED",
    );
    fixture.finish().await;
}

fn two_position_scenario() -> okx_protocol::trading::PortfolioScenarioRequest {
    use okx_protocol::trading::{
        PortfolioScenarioRequest, PositionSide, ScenarioCorrelation, ScenarioPosition,
    };
    PortfolioScenarioRequest {
        equity_usd: "100".into(),
        positions: vec![
            ScenarioPosition {
                instrument: "BTC-USDT-SWAP".into(),
                side: PositionSide::Long,
                notional_usd: "100".into(),
                margin_usd: "10".into(),
                price_shock_bps: -1000,
                volatility_bps: 300,
                liquidity_cost_bps: 100,
            },
            ScenarioPosition {
                instrument: "ETH-USDT-SWAP".into(),
                side: PositionSide::Short,
                notional_usd: "200".into(),
                margin_usd: "20".into(),
                price_shock_bps: 500,
                volatility_bps: 200,
                liquidity_cost_bps: 50,
            },
        ],
        correlations: vec![ScenarioCorrelation {
            left: 0,
            right: 1,
            rho: 0.0,
        }],
    }
}

// SQLite coordination bytes are not a durable financial-state fingerprint.
use crate::accounts::tests::read_only_fixture_files as fixture_artifacts;

#[tokio::test]
async fn portfolio_scenario_public_dispatch_returns_independent_virtual_risk_values() {
    let fixture = Fixture::new(empty_reference()).await;
    let before = fixture_artifacts(&fixture.root);
    let response = fixture
        .request(
            AgentOperation::PortfolioScenario {
                request: two_position_scenario(),
            },
            None,
        )
        .await;
    assert_eq!(response.request_id, "query-dispatch-request-001");
    assert_eq!(response.status, AgentResponseStatus::Completed);
    assert_eq!(response.quality, DataQuality::Fresh);
    assert!(response.failure.is_none());
    let data = response.result.unwrap();
    assert_eq!(data["schema"], "okx.portfolio.scenario/v1");
    assert_eq!(data["quality"], "assumed");
    assert_eq!(data["execution"], "virtual_read_only");
    assert_eq!(data["provenance"], "caller_supplied");
    assert_eq!(data["exchange_post"], false);
    assert_eq!(data["venue_margin_verified"], false);
    assert_eq!(data["gross_notional_usd"], "300");
    assert_eq!(data["margin_usd"], "30");
    assert_eq!(data["gross_leverage_ratio"], "3");
    assert_eq!(data["stress_price_pnl_usd"], "-20");
    assert_eq!(data["liquidity_loss_usd"], "2");
    assert_eq!(data["net_stress_pnl_usd"], "-22");
    assert_eq!(data["stressed_equity_usd"], "78");
    assert_eq!(data["drawdown_ratio"], "0.22");
    // Independent 3-4-5 case: uncorrelated dollar volatilities 3 and 4
    // have variance 25 and standard deviation 5. No VaR is exposed by this DTO.
    assert_eq!(data["correlated_volatility_usd"], 5.0);
    assert_eq!(data["correlation_basis"], "caller_supplied_psd");
    assert_eq!(
        data["positions"],
        json!([
            {"instrument":"BTC-USDT-SWAP","side":"long","price_pnl_usd":"-10","liquidity_loss_usd":"1","source":"caller_supplied"},
            {"instrument":"ETH-USDT-SWAP","side":"short","price_pnl_usd":"-10","liquidity_loss_usd":"1","source":"caller_supplied"}
        ])
    );
    assert_eq!(fixture_artifacts(&fixture.root), before);
    fixture.finish().await;
}

#[tokio::test]
async fn portfolio_scenario_public_dispatch_rejects_non_psd_and_invalid_parameters() {
    use okx_protocol::trading::ScenarioCorrelation;
    let fixture = Fixture::new(empty_reference()).await;
    let before = fixture_artifacts(&fixture.root);
    let mut non_psd = two_position_scenario();
    non_psd.positions.push(non_psd.positions[0].clone());
    // Pairwise-valid correlations cannot all coexist: the first two perfect
    // positive relationships require the third relationship to be positive.
    non_psd.correlations = vec![
        ScenarioCorrelation {
            left: 0,
            right: 1,
            rho: 1.0,
        },
        ScenarioCorrelation {
            left: 0,
            right: 2,
            rho: 1.0,
        },
        ScenarioCorrelation {
            left: 1,
            right: 2,
            rho: -1.0,
        },
    ];
    assert!(non_psd.valid());
    rejection(
        fixture
            .request(AgentOperation::PortfolioScenario { request: non_psd }, None)
            .await,
        "CORRELATION_NOT_PSD",
    );
    let mut invalid = two_position_scenario();
    invalid.positions[0].price_shock_bps = -10001;
    rejection(
        fixture
            .request(AgentOperation::PortfolioScenario { request: invalid }, None)
            .await,
        "INVALID_COMMAND",
    );
    let mut invalid = two_position_scenario();
    invalid.correlations[0].rho = 1.01;
    rejection(
        fixture
            .request(AgentOperation::PortfolioScenario { request: invalid }, None)
            .await,
        "INVALID_COMMAND",
    );
    assert_eq!(fixture_artifacts(&fixture.root), before);
    fixture.finish().await;
}
